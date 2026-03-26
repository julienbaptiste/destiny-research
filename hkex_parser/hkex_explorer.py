"""
HKEX OMD-D Explorer — Series Definitions scanner

Purpose
-------
Scan the Series Definition binary channels (MC101, MC151, MC201, MC301) for a
given date and produce a complete inventory of all instruments available in the
HKEX FBD-NSOM dataset.

This is a diagnostic / discovery tool, NOT part of the ingestion pipeline.
Run it once per new dataset to:
  1. Get the full product list and decide what to extract
  2. Confirm NumberOfDecimalsPrice for each product (resolves the open question
     in CONTEXT.md regarding HSI/MHI/HHI/MCH price denominator)
  3. Identify what lives in MC321 (Partition 3 — unknown products)
  4. Get the OrderbookID → Symbol mapping for sanity checks

Usage
-----
    python hkex_explorer.py --date 20260203 --raw-dir /media/julien/HDD/data/raw/provider=hkex/binaries

Output
------
  - Console: summary table grouped by ClassCode
  - CSV: hkex_instruments_YYYYMMDD.csv (full detail, all fields)

Both outputs go to the current directory unless --out-dir is specified.

Architecture note
-----------------
This script is intentionally standalone — no imports from ingestion/ or
hkex_parser/. It only depends on the stdlib + pyarrow for the CSV write.
Keeps it runnable as a one-off diagnostic without activating the full venv.
"""

import argparse
import mmap
import struct
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.csv as pa_csv


# ─────────────────────────────────────────────────────────────────────────────
# Binary protocol constants (mirrors messages.py — duplicated intentionally
# to keep this script self-contained)
# ─────────────────────────────────────────────────────────────────────────────

RECORD_LENGTH_SIZE  = 2    # RecLen field before each record (Big Endian)
PACKET_HEADER_SIZE  = 16   # PktSize(2) + MsgCount(1) + Filler(1) + SeqNum(4) + SendTime(8)
MESSAGE_HEADER_SIZE = 4    # MsgSize(2) + MsgType(2) — both Little Endian
RECORD_HEADER_SIZE  = RECORD_LENGTH_SIZE + PACKET_HEADER_SIZE  # 18 bytes total

# Message type IDs relevant to Series Definitions
MSG_COMMODITY_DEF    = 301
MSG_CLASS_DEF        = 302
MSG_SERIES_DEF_BASE  = 303   # Primary source: OrderbookID + Symbol + FinancialProduct + NumberOfDecimalsPrice
MSG_SERIES_DEF_EXT   = 304   # Secondary: OrderbookID + Symbol + CommodityCode + ContractSize
MSG_COMBINATION_DEF  = 305   # Combo/spread strategies — captured for completeness

# FinancialProduct values (SeriesDefinitionBase field at payload offset 36)
FINANCIAL_PRODUCT_NAMES = {
    1:  "Option",
    2:  "Forward",
    3:  "Future",
    4:  "FRA",
    5:  "Cash",
    10: "SyntheticBoxLeg",
    11: "Combination",
    14: "Warrant",
}

# Channel → Partition mapping (from spec section 1)
# Used to track which file each instrument came from
CHANNEL_PARTITION = {
    "MC101": "Partition1",
    "MC201": "Partition2",
    "MC301": "Partition3",
    "MC151": "OAPI",
}


# ─────────────────────────────────────────────────────────────────────────────
# Instrument record — one row per OrderbookID
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Instrument:
    """
    Represents one instrument (series) extracted from the Series Definition files.

    Fields are populated from whichever message type provided the data.
    A single OrderbookID may appear in multiple channels — we keep the first
    occurrence from MSG_SERIES_DEF_BASE (303) and use MSG_SERIES_DEF_EXT (304)
    only as a fallback.
    """
    orderbook_id:          int          # Unique key — referenced by all order/trade messages
    symbol:                str          # Full symbol string, e.g. "HSIH26"
    class_code:            str          # First 3 chars of symbol, e.g. "HSI"
    financial_product:     int          # Raw value from spec (3=Future, 1=Option, etc.)
    financial_product_str: str          # Human-readable label
    n_decimals_price:      int          # NumberOfDecimalsPrice — price divisor = 10^n
    expiration_date:       str          # YYYYMMDD string (from SeriesDefBase) or "" if unknown
    is_future:             bool         # True if financial_product == 3
    source_msg:            int          # Message type that populated this record (303 or 304)
    source_channel:        str          # Channel filename prefix, e.g. "MC101"
    source_partition:      str          # Human-readable partition label


# ─────────────────────────────────────────────────────────────────────────────
# Low-level binary readers
# ─────────────────────────────────────────────────────────────────────────────

def _read_rec_header(mm: mmap.mmap, offset: int) -> tuple[int, int, int]:
    """
    Read the 18-byte record header at `offset`.

    Returns (total_record_size, msg_count, send_time).
    total_record_size = RecLen (Big Endian uint16) + 2 bytes for the RecLen field itself.
    send_time is the PacketHeader SendTime (uint64 LE, nanoseconds UTC).

    RecLen is Big Endian despite what the spec says — validated empirically
    in the original parser. PktSize is also BE. Everything else is LE.
    """
    rec_len   = struct.unpack_from('>H', mm, offset)[0]      # offset 0, Big Endian
    msg_count = struct.unpack_from('<B', mm, offset + 4)[0]  # offset 4
    # send_time at offset 10 within the record header (after RecLen+PktSize+MsgCount+Filler+SeqNum)
    # Layout: [0-1]=RecLen [2-3]=PktSize [4]=MsgCount [5]=Filler [6-9]=SeqNum [10-17]=SendTime
    return rec_len + 2, msg_count, 0  # send_time unused in explorer


def _read_msg_header(mm: mmap.mmap, offset: int) -> tuple[int, int]:
    """
    Read the 4-byte message header at `offset`.

    Returns (msg_size, msg_type), both Little Endian uint16.
    msg_size includes the 4-byte header itself → payload = msg_size - 4 bytes.
    """
    msg_size = struct.unpack_from('<H', mm, offset)[0]
    msg_type = struct.unpack_from('<H', mm, offset + 2)[0]
    return msg_size, msg_type


def _parse_series_def_base(payload: bytes) -> Optional[dict]:
    """
    Parse a SeriesDefinitionBase (303) payload.

    Layout (offsets from start of payload, i.e. after the 4-byte MsgHeader):
      [0-3]   OrderbookID          Uint32 LE
      [4-35]  Symbol               String 32  (ASCII, space-padded)
      [36]    FinancialProduct     Uint8
      [37-38] NumberOfDecimalsPrice Uint16 LE  ← key field for price normalisation
      [39]    NumberOfLegs         Uint8       (ignored)
      [40-43] StrikePrice          Int32 LE
      [44-51] ExpirationDate       String 8    YYYYMMDD
      [52-53] DecimalInStrikePrice Uint16 LE   (ignored)
      [54]    PutOrCall            Uint8
      [55]    Filler               1 byte
    Total payload: 56 bytes (MsgSize=60 including the 4-byte header)

    Returns None if payload is too short to parse safely.
    """
    if len(payload) < 56:
        return None

    try:
        orderbook_id   = struct.unpack_from('<I', payload, 0)[0]
        symbol         = payload[4:36].decode('ascii', errors='replace').strip()
        fin_product    = struct.unpack_from('<B', payload, 36)[0]
        n_dec_price    = struct.unpack_from('<H', payload, 37)[0]
        expiry_raw     = payload[44:52].decode('ascii', errors='replace').strip()
        # ExpirationDate is YYYYMMDD string — validate it looks like a date
        expiry = expiry_raw if expiry_raw.isdigit() and len(expiry_raw) == 8 else ""

        return {
            "orderbook_id":      orderbook_id,
            "symbol":            symbol,
            "class_code":        symbol[:3] if symbol else "",
            "financial_product": fin_product,
            "n_decimals_price":  n_dec_price,
            "expiration_date":   expiry,
            "source_msg":        MSG_SERIES_DEF_BASE,
        }
    except struct.error:
        return None


def _parse_series_def_ext(payload: bytes) -> Optional[dict]:
    """
    Parse a SeriesDefinitionExtended (304) payload.

    Layout (offsets from start of payload):
      [0-3]   OrderBookID      Uint32 LE
      [4-35]  Symbol           String 32
      [36]    Country          Uint8
      [37]    Market           Uint8
      [38]    InstrumentGroup  Uint8
      [39]    Modifier         Uint8
      [40-41] CommodityCode    Uint16 LE
      [42-43] ExpirationDate   Uint16 LE  (encoded, not YYYYMMDD)
      [44-51] StrikePrice      Int32 LE + 4 bytes filler
      [52-59] ContractSize     Int64 LE
    Total payload: 100 bytes (MsgSize=104 including header)

    Note: FinancialProduct is NOT present in message 304 — this is why
    SeriesDefinitionBase (303) is the preferred source. We set fin_product=-1
    (unknown) for 304-only instruments.

    Returns None if payload is too short.
    """
    if len(payload) < 44:
        return None

    try:
        orderbook_id = struct.unpack_from('<I', payload, 0)[0]
        symbol       = payload[4:36].decode('ascii', errors='replace').strip()

        return {
            "orderbook_id":      orderbook_id,
            "symbol":            symbol,
            "class_code":        symbol[:3] if symbol else "",
            "financial_product": -1,          # not available in msg 304
            "n_decimals_price":  -1,          # not available in msg 304
            "expiration_date":   "",          # encoded differently, not decoded here
            "source_msg":        MSG_SERIES_DEF_EXT,
        }
    except struct.error:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Main scanner
# ─────────────────────────────────────────────────────────────────────────────

def scan_channel(filepath: Path, channel: str) -> dict[int, Instrument]:
    """
    Scan one Series Definition binary file and return a dict of Instruments.

    Key = OrderbookID (int).
    Only processes MSG_SERIES_DEF_BASE (303) and MSG_SERIES_DEF_EXT (304).
    All other message types (301 CommodityDef, 302 ClassDef, 305 CombinationDef)
    are silently skipped — not needed for the product inventory.

    Uses mmap for zero-copy file access — critical for the larger files
    (MC151 at ~4 MB is still small, but the pattern must be consistent with
    the production parser for correctness).
    """
    instruments: dict[int, Instrument] = {}
    file_size = filepath.stat().st_size
    partition = CHANNEL_PARTITION.get(channel, channel)

    with open(filepath, 'rb') as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            offset = 0

            while offset + RECORD_HEADER_SIZE <= file_size:
                # Read record header (18 bytes)
                total, msg_count, _ = _read_rec_header(mm, offset)

                # Skip malformed records (same byte-level recovery as production parser)
                if total < RECORD_HEADER_SIZE or offset + total > file_size:
                    offset += 1
                    continue

                msg_offset = offset + RECORD_HEADER_SIZE  # first message starts at byte 18

                for _ in range(msg_count):
                    # Ensure there's room for a message header
                    if msg_offset + MESSAGE_HEADER_SIZE > offset + total:
                        break

                    msg_size, msg_type = _read_msg_header(mm, msg_offset)

                    # Skip malformed message
                    if msg_size < MESSAGE_HEADER_SIZE:
                        break
                    msg_end = msg_offset + msg_size
                    if msg_end > offset + total:
                        break

                    # Payload starts after the 4-byte message header
                    payload = mm[msg_offset + MESSAGE_HEADER_SIZE: msg_end]

                    parsed = None
                    if msg_type == MSG_SERIES_DEF_BASE:
                        parsed = _parse_series_def_base(payload)
                    elif msg_type == MSG_SERIES_DEF_EXT:
                        parsed = _parse_series_def_ext(payload)
                    # 301, 302, 305 — skip silently

                    if parsed and parsed["orderbook_id"] > 0:
                        oid = parsed["orderbook_id"]
                        # Priority: keep MSG_SERIES_DEF_BASE (303) over 304
                        # If we already have a 303 entry, don't overwrite with 304
                        if oid not in instruments or (
                            instruments[oid].source_msg == MSG_SERIES_DEF_EXT
                            and parsed["source_msg"] == MSG_SERIES_DEF_BASE
                        ):
                            fin = parsed["financial_product"]
                            instruments[oid] = Instrument(
                                orderbook_id          = oid,
                                symbol                = parsed["symbol"],
                                class_code            = parsed["class_code"],
                                financial_product     = fin,
                                financial_product_str = FINANCIAL_PRODUCT_NAMES.get(fin, f"Unknown({fin})"),
                                n_decimals_price      = parsed["n_decimals_price"],
                                expiration_date       = parsed["expiration_date"],
                                is_future             = (fin == 3),
                                source_msg            = parsed["source_msg"],
                                source_channel        = channel,
                                source_partition      = partition,
                            )

                    msg_offset = msg_end

                offset += total

    return instruments


def scan_all_channels(
    raw_dir: Path,
    date_str: str,
) -> dict[int, Instrument]:
    """
    Scan all 4 Series Definition channels for a given date.

    Channels are processed in priority order:
      1. MC101, MC201, MC301 — contain SeriesDefinitionBase (303) with FinancialProduct
         and NumberOfDecimalsPrice — these are the authoritative source
      2. MC151 — contains SeriesDefinitionExtended (304) — used to fill gaps

    An instrument found in MC101/201/301 via msg 303 will NOT be overwritten
    by MC151 msg 304 (lower priority). The merge logic is in scan_channel().

    Returns merged dict[OrderbookID → Instrument].
    """
    all_instruments: dict[int, Instrument] = {}

    # Channel scan order: Base partitions first (303 = authoritative), OAPI last (304 = fallback)
    channel_order = [
        ("MC101", f"MC101_All_{date_str}"),
        ("MC201", f"MC201_All_{date_str}"),
        ("MC301", f"MC301_All_{date_str}"),
        ("MC151", f"MC151_All_{date_str}"),
    ]

    for channel, filename in channel_order:
        filepath = raw_dir / filename
        if not filepath.exists():
            print(f"  [SKIP] {filename} not found", file=sys.stderr)
            continue

        file_mb = filepath.stat().st_size / 1e6
        print(f"  Scanning {filename} ({file_mb:.1f} MB)...")
        ch_instruments = scan_channel(filepath, channel)

        # Merge: Base partitions (303) take priority over OAPI (304)
        new_count = 0
        upgrade_count = 0
        for oid, instr in ch_instruments.items():
            if oid not in all_instruments:
                all_instruments[oid] = instr
                new_count += 1
            elif (all_instruments[oid].source_msg == MSG_SERIES_DEF_EXT
                  and instr.source_msg == MSG_SERIES_DEF_BASE):
                # Upgrade 304 → 303: replace with richer data
                all_instruments[oid] = instr
                upgrade_count += 1

        print(f"    → {len(ch_instruments):,} instruments "
              f"(+{new_count:,} new, {upgrade_count:,} upgraded from 304→303)")

    return all_instruments


# ─────────────────────────────────────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(instruments: dict[int, Instrument]) -> None:
    """
    Print a console summary grouped by ClassCode.

    For each class: total series count, future count, option count,
    NumberOfDecimalsPrice values observed (should be unique per class),
    and earliest/latest expiration dates.
    """
    # Group by class_code
    by_class: dict[str, list[Instrument]] = {}
    for instr in instruments.values():
        cc = instr.class_code or "UNKNOWN"
        by_class.setdefault(cc, []).append(instr)

    print("\n" + "═" * 90)
    print(f"  HKEX INSTRUMENT INVENTORY — {len(instruments):,} total series")
    print("═" * 90)
    print(f"  {'ClassCode':<12} {'Total':>6} {'Futures':>8} {'Options':>8} "
          f"{'Other':>6} {'DecPrice':>9} {'Expiries':>8}  {'Source'}")
    print("─" * 90)

    for cc in sorted(by_class.keys()):
        instrs = by_class[cc]
        n_futures = sum(1 for i in instrs if i.financial_product == 3)
        n_options = sum(1 for i in instrs if i.financial_product == 1)
        n_other   = len(instrs) - n_futures - n_options

        # Collect unique NumberOfDecimalsPrice values (excluding -1 = unknown)
        dec_values = sorted({i.n_decimals_price for i in instrs if i.n_decimals_price >= 0})
        dec_str = ",".join(str(d) for d in dec_values) if dec_values else "?"

        # Collect expiration dates
        expiries = sorted({i.expiration_date for i in instrs if i.expiration_date})
        exp_str = f"{expiries[0]}…{expiries[-1]}" if len(expiries) > 1 else (expiries[0] if expiries else "—")

        # Source channels that contributed
        sources = sorted({i.source_channel for i in instrs})
        src_str = "+".join(sources)

        print(f"  {cc:<12} {len(instrs):>6,} {n_futures:>8,} {n_options:>8,} "
              f"{n_other:>6,} {dec_str:>9} {exp_str:>24}  {src_str}")

    print("═" * 90)

    # Highlight the 4 target products specifically
    targets = ["HSI", "MHI", "HHI", "MCH"]
    print("\n  TARGET PRODUCTS (HSI / MHI / HHI / MCH):")
    print("─" * 90)
    for cc in targets:
        if cc not in by_class:
            print(f"  {cc}: *** NOT FOUND ***")
            continue
        instrs = by_class[cc]
        futures = [i for i in instrs if i.financial_product == 3]
        if futures:
            dec = futures[0].n_decimals_price
            price_divisor = 10 ** dec if dec >= 0 else "?"
            print(f"  {cc}: {len(futures)} futures | NumberOfDecimalsPrice={dec} "
                  f"→ price divisor = {price_divisor} "
                  f"(raw Int32 / {price_divisor} = actual price)")
            # Show active contracts (non-expired relative to 2026)
            active = sorted(
                [i for i in futures if i.expiration_date >= "20260101"],
                key=lambda x: x.expiration_date
            )
            for i in active[:6]:  # max 6 to keep output compact
                print(f"    OrderbookID={i.orderbook_id:>8}  symbol={i.symbol:<20} "
                      f"expiry={i.expiration_date}  channel={i.source_channel}")
        else:
            print(f"  {cc}: no futures found")

    print("═" * 90)


def write_csv(instruments: dict[int, Instrument], out_path: Path) -> None:
    """
    Write the full instrument inventory to a CSV file via PyArrow.

    All fields from the Instrument dataclass are included.
    Sorted by class_code then symbol for readability.
    """
    rows = sorted(instruments.values(), key=lambda i: (i.class_code, i.symbol))
    dicts = [asdict(r) for r in rows]

    # Build PyArrow table from list of dicts
    schema = pa.schema([
        ("orderbook_id",          pa.int64()),
        ("symbol",                pa.string()),
        ("class_code",            pa.string()),
        ("financial_product",     pa.int32()),
        ("financial_product_str", pa.string()),
        ("n_decimals_price",      pa.int32()),
        ("expiration_date",       pa.string()),
        ("is_future",             pa.bool_()),
        ("source_msg",            pa.int32()),
        ("source_channel",        pa.string()),
        ("source_partition",      pa.string()),
    ])

    table = pa.Table.from_pylist(dicts, schema=schema)
    pa_csv.write_csv(table, str(out_path))
    print(f"\n  CSV written → {out_path}  ({len(rows):,} rows)")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="HKEX FBD-NSOM Series Definition explorer — lists all available instruments"
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Date in YYYYMMDD format, e.g. 20260203",
    )
    parser.add_argument(
        "--raw-dir",
        required=True,
        type=Path,
        help="Directory containing the binary files MC101_All_YYYYMMDD etc.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("."),
        help="Output directory for the CSV file (default: current directory)",
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Skip CSV output, print summary only",
    )
    args = parser.parse_args()

    if not args.raw_dir.exists():
        print(f"ERROR: --raw-dir does not exist: {args.raw_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"\nHKEX Explorer — date={args.date}")
    print(f"  raw-dir : {args.raw_dir}")

    # Scan all 4 channels
    instruments = scan_all_channels(args.raw_dir, args.date)
    print(f"\n  Total unique instruments found: {len(instruments):,}")

    # Console summary
    print_summary(instruments)

    # CSV output
    if not args.no_csv:
        args.out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = args.out_dir / f"hkex_instruments_{args.date}.csv"
        write_csv(instruments, csv_path)


if __name__ == "__main__":
    main()