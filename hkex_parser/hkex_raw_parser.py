"""
HKEX OMD-D Raw Parser — produces raw Parquet files from binary channels.

═══════════════════════════════════════════════════════════════════════════════
ROLE IN THE PIPELINE
═══════════════════════════════════════════════════════════════════════════════

This is the first stage of the HKEX ingestion pipeline:

  Binary channels (MC121, MC221, MC321, MC167)
      ↓
  [hkex_raw_parser.py]  ← YOU ARE HERE
      ↓
  data/raw/provider=hkex/product=HSI/year=2026/month=02/
      hkex-hsi_20260203_orders.parquet   ← all order events, all fields
      hkex-hsi_20260203_trades.parquet   ← all trade events, all fields
      ↓
  [hkex_adapter.py]  → normalized MBO parquet (merge + schema mapping)

RAW = every field from the binary spec, nothing discarded.
Filtering by product type, contract, or session happens downstream.

═══════════════════════════════════════════════════════════════════════════════
WHAT IS PRESERVED IN RAW (vs the old parser.py)
═══════════════════════════════════════════════════════════════════════════════

Orders (Add 330, Modify 331, Delete 332, Clear 335, COP 364):
  + lot_type          (uint8)  — Round/Odd/Block lot
  + order_type        (uint16) — bitmask: FAK, implied, short-sell, etc.
  + orderbook_position (uint32) — queue rank at price level
  + symbol            (str)    — full instrument name e.g. "HSIH26"
  + msg_type          (int)    — raw message type (330/331/332/335/364)

Trades (Trade 350):
  + combo_group_id    (uint32) — links combo and leg trades
  + trade_condition   (uint16) — late trade, crossing, off-market bitmask
  + deal_info         (uint16) — reported trade flag
  + symbol            (str)    — full instrument name

═══════════════════════════════════════════════════════════════════════════════
MERGE KEY FOR NORMALISATION
═══════════════════════════════════════════════════════════════════════════════

The hkex_adapter.py will merge orders + trades into a single chronological
MBO stream. The sort key is:

    (send_time_ns, seq_num, intra_packet_rank)

  send_time_ns      : PacketHeader.SendTime — ~1ms precision, shared by all
                      messages in the same packet (same seq_num)
  seq_num           : PacketHeader.SeqNum — monotonically increasing per channel
  intra_packet_rank : message index within the packet (0, 1, 2...) — preserved
                      as `msg_index` in the raw parquet so the adapter can
                      reconstruct exact intra-packet ordering without re-parsing

Both files include seq_num and msg_index for this purpose.

═══════════════════════════════════════════════════════════════════════════════
MEMORY STRATEGY
═══════════════════════════════════════════════════════════════════════════════

MC221 (HSI/HHI) ~ 7 GB, MC121 (MHI/MCH) ~ 2.7 GB — cannot load into RAM.

  1. mmap: file mapped to virtual address space, kernel pages in on demand.
     Equivalent to sequential read but with O(1) random access if needed.
  2. Batch flush: rows accumulated in Python lists, flushed to Parquet every
     BATCH_SIZE rows (~100 MB peak RAM per writer at 500k rows).
  3. WriterPool: one ParquetWriter per (product, kind) opened lazily — no
     empty files for products with no activity that day.

═══════════════════════════════════════════════════════════════════════════════
CHANNELS AND PRODUCT ROUTING
═══════════════════════════════════════════════════════════════════════════════

  MC121 → Partition 1  (MHI, MCH, single-stock futures from MC101)
  MC221 → Partition 2  (HSI, HHI, HTI, PHS, PHH, PTE, ... from MC201)
  MC321 → Partition 3  (CHN, CIN, CJP, LR*, GDR, ... from MC301)
  MC167 → Block trades (all products)

The parser scans all channels and routes each message to the correct product
file via the orderbook_id → (symbol, class_code) mapping built from the
Series Definition channels (MC101/201/301/151).

═══════════════════════════════════════════════════════════════════════════════
CLI
═══════════════════════════════════════════════════════════════════════════════

  python hkex_raw_parser.py --date 20260203 \\
      --bin-dir /media/julien/HDD/data/raw/provider=hkex/binaries/year=2026/month=02/day=03 \\
      --out-dir /media/julien/HDD/data/raw/provider=hkex \\
      [--products HSI MHI HHI MCH]   # optional filter; default = all

Output:
  <out-dir>/product=HSI/year=2026/month=02/hkex-hsi_20260203_orders.parquet
  <out-dir>/product=HSI/year=2026/month=02/hkex-hsi_20260203_trades.parquet
  ... (one pair per product that had activity that day)
"""

import argparse
import logging
import mmap
import struct
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import DATA_RAW, hkex_bin_dir, HKEX_CHANNELS_SERIES_DEFS, HKEX_CHANNELS_ORDER_BOOK, HKEX_CHANNELS_BLOCK_TRADES

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Protocol constants — mirrors messages.py, duplicated for self-containment
# ─────────────────────────────────────────────────────────────────────────────

RECORD_LENGTH_SIZE  = 2
PACKET_HEADER_SIZE  = 16
MESSAGE_HEADER_SIZE = 4
RECORD_HEADER_SIZE  = RECORD_LENGTH_SIZE + PACKET_HEADER_SIZE  # 18 bytes

# Message type IDs
MSG_SERIES_DEF_BASE  = 303
MSG_SERIES_DEF_EXT   = 304
MSG_ADD_ORDER        = 330
MSG_MODIFY_ORDER     = 331
MSG_DELETE_ORDER     = 332
MSG_ORDERBOOK_CLEAR  = 335
MSG_TRADE            = 350
MSG_COP              = 364   # Calculated Opening Price

# Messages that go into the orders file
ORDER_MSG_TYPES = {MSG_ADD_ORDER, MSG_MODIFY_ORDER, MSG_DELETE_ORDER,
                   MSG_ORDERBOOK_CLEAR, MSG_COP}

# Flush buffer to Parquet every N rows per (product, kind).
# 500k rows × ~250 bytes ≈ 125 MB peak RAM per writer — well within 16 GB.
BATCH_SIZE = 500_000

# Log progress every N records processed across all channels
PROGRESS_INTERVAL = 5_000_000


# ─────────────────────────────────────────────────────────────────────────────
# PyArrow schemas — raw, all fields preserved
# ─────────────────────────────────────────────────────────────────────────────

# Orders schema: ADD (330), MODIFY (331), DELETE (332), CLEAR (335), COP (364)
# Fields absent for a given msg_type are stored as 0 / null — see _make_order_row.
#
# Merge sort key for normalisation: (send_time_ns, seq_num, msg_index)
#   send_time_ns : PacketHeader.SendTime — shared by all msgs in same packet
#   seq_num      : PacketHeader.SeqNum — monotone per channel
#   msg_index    : 0-based position of this msg within its packet
ORDERS_SCHEMA = pa.schema([
    # ── Timing & sequencing ──────────────────────────────────────────────────
    ("send_time_ns",       pa.int64()),   # PacketHeader.SendTime, UTC ns, ~1ms precision
    ("seq_num",            pa.int64()),   # PacketHeader.SeqNum — for merge sort
    ("msg_index",          pa.int32()),   # 0-based position within packet — for merge sort

    # ── Routing ──────────────────────────────────────────────────────────────
    ("msg_type",           pa.int32()),   # 330/331/332/335/364 — raw, no conversion
    ("orderbook_id",       pa.int64()),   # raw OrderbookID from message
    ("symbol",             pa.string()),  # e.g. "HSIH26" — from Series Def mapping
    ("class_code",         pa.string()),  # e.g. "HSI" — first 3 chars of symbol

    # ── Order fields ─────────────────────────────────────────────────────────
    ("order_id",           pa.int64()),   # stable across Add/Modify/Delete; 0 for Clear/COP
    ("price",              pa.int32()),   # raw Int32; NumberOfDecimalsPrice=0 → actual price
    ("quantity",           pa.int32()),   # lots; 0 for Delete/Clear
    ("side",               pa.int8()),    # 0=Bid, 1=Offer; -1 for Clear/COP (no side)

    # ── Fields absent from old parser — preserved here ───────────────────────
    ("lot_type",           pa.int8()),    # 0=Undef,1=Odd,2=Round,3=Block,4=AON; 0 for non-Add
    ("order_type",         pa.int16()),   # bitmask: 8192=implied,1024=FAK,32=undisclosed,...
    ("orderbook_position", pa.int32()),   # queue rank at price level; 0 for Delete/Clear/COP
])

# Trades schema: TRADE (350)
# Also used for block trades from MC167 — same message format.
TRADES_SCHEMA = pa.schema([
    # ── Timing & sequencing ──────────────────────────────────────────────────
    ("send_time_ns",    pa.int64()),   # PacketHeader.SendTime
    ("seq_num",         pa.int64()),   # PacketHeader.SeqNum — for merge sort
    ("msg_index",       pa.int32()),   # 0-based position within packet — for merge sort
    ("trade_time_ns",   pa.int64()),   # Trade.TradeTime — matching engine ts, ~10ms precision

    # ── Routing ──────────────────────────────────────────────────────────────
    ("orderbook_id",    pa.int64()),
    ("symbol",          pa.string()),
    ("class_code",      pa.string()),

    # ── Trade fields ─────────────────────────────────────────────────────────
    ("order_id",        pa.int64()),   # aggressor order ID; 0 if not available
    ("price",           pa.int32()),   # execution price, raw Int32
    ("quantity",        pa.int64()),   # lots — Uint64 in spec (large for combo legs)
    ("trade_id",        pa.int64()),   # unique match ID — use for deduplication
    ("side",            pa.int8()),    # 0=N/A, 2=Buy aggressor, 3=Sell aggressor

    # ── Fields absent from old parser — preserved here ───────────────────────
    ("deal_type",       pa.int8()),    # bitmask: 1=Printable, 2=Cross, 4=Reported
    ("combo_group_id",  pa.int32()),   # links combo and leg trades; 0 for simple trades
    ("trade_condition", pa.int16()),   # bitmask: 2=InternalCross, 16=OffMarket, ...
    ("deal_info",       pa.int16()),   # bitmask: 1=ReportedTrade
])


# ─────────────────────────────────────────────────────────────────────────────
# Path helpers — Hive-style partitioning
# ─────────────────────────────────────────────────────────────────────────────

def _product_dir(out_dir: Path, class_code: str, date_str: str) -> Path:
    """
    Build the output directory for a (product, date) pair.

    Example:
      out_dir='.../.../provider=hkex', class_code='HSI', date_str='20260203'
      → '.../provider=hkex/product=HSI/year=2026/month=02/'
    """
    year  = date_str[:4]
    month = date_str[4:6]
    d = out_dir / f"product={class_code}" / f"year={year}" / f"month={month}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _parquet_paths(out_dir: Path, class_code: str,
                   date_str: str) -> tuple[Path, Path]:
    """
    Return (orders_path, trades_path) for a (product, date) pair.

    Naming convention: hkex-<lowercase_class_code>_YYYYMMDD_{orders|trades}.parquet
    Example: hkex-hsi_20260203_orders.parquet
    """
    d = _product_dir(out_dir, class_code, date_str)
    cc_lower = class_code.lower()
    return (
        d / f"hkex-{cc_lower}_{date_str}_orders.parquet",
        d / f"hkex-{cc_lower}_{date_str}_trades.parquet",
    )


# ─────────────────────────────────────────────────────────────────────────────
# WriterPool — lazy multi-product Parquet writers
# ─────────────────────────────────────────────────────────────────────────────

class _WriterPool:
    """
    Manages one ParquetWriter per (class_code, 'orders'|'trades'), opened lazily.

    Lazy opening: a writer is only created when the first row for that
    (product, kind) arrives. Products with no activity that day produce no files.

    Batch flush: each buffer is flushed to disk every BATCH_SIZE rows to keep
    peak RAM bounded regardless of file size.
    """

    def __init__(self, out_dir: Path, date_str: str,
                 filter_products: Optional[set[str]] = None):
        self._out_dir  = out_dir
        self._date_str = date_str
        # If set, only products in this set are written — others are silently skipped
        self._filter   = filter_products  # None → write all products

        # (class_code, kind) → open ParquetWriter
        self._writers: dict[tuple[str, str], pq.ParquetWriter] = {}
        # (class_code, kind) → list[dict] accumulation buffer
        self._buffers: dict[tuple[str, str], list[dict]] = {}

    def _get_writer(self, class_code: str, kind: str) -> pq.ParquetWriter:
        """Return existing writer or create a new one (lazy init)."""
        key = (class_code, kind)
        if key not in self._writers:
            orders_path, trades_path = _parquet_paths(
                self._out_dir, class_code, self._date_str
            )
            schema = ORDERS_SCHEMA if kind == "orders" else TRADES_SCHEMA
            path   = orders_path   if kind == "orders" else trades_path
            self._writers[key] = pq.ParquetWriter(path, schema,
                                                   compression="snappy")
            self._buffers[key] = []
            logger.debug("Opened writer: %s", path)
        return self._writers[key]

    def _flush(self, class_code: str, kind: str) -> None:
        """Convert buffer to Arrow Table and write as a Parquet row group."""
        key = (class_code, kind)
        buf = self._buffers.get(key, [])
        if not buf:
            return
        schema = ORDERS_SCHEMA if kind == "orders" else TRADES_SCHEMA
        self._writers[key].write_table(
            pa.Table.from_pylist(buf, schema=schema)
        )
        self._buffers[key] = []

    def append(self, class_code: str, kind: str, row: dict) -> None:
        """
        Append one row to the buffer for (class_code, kind).
        Flushes automatically when BATCH_SIZE is reached.
        Silently drops rows for products not in filter_products (if set).
        """
        if self._filter is not None and class_code not in self._filter:
            return
        key = (class_code, kind)
        self._get_writer(class_code, kind)
        self._buffers[key].append(row)
        if len(self._buffers[key]) >= BATCH_SIZE:
            self._flush(class_code, kind)

    def close_all(self) -> dict[str, tuple[Path, Path]]:
        """
        Flush all remaining buffers and close all writers.

        MUST be called (ideally in a try/finally) — without it, Parquet
        footers are not written and files are unreadable.

        Returns dict[class_code → (orders_path, trades_path)] for products
        that had at least one row written.
        """
        for class_code, kind in list(self._writers.keys()):
            self._flush(class_code, kind)
        for writer in self._writers.values():
            writer.close()

        products = {cc for cc, _ in self._writers}
        return {
            cc: _parquet_paths(self._out_dir, cc, self._date_str)
            for cc in products
        }


# ─────────────────────────────────────────────────────────────────────────────
# HKEXRawParser
# ─────────────────────────────────────────────────────────────────────────────

class HKEXRawParser:
    """
    Parses HKEX FBD-NSOM binary channels into raw Parquet files.

    Two-pass approach:
      Pass 1 — Series Definitions (MC101/201/301/151):
               builds _orderbook_map: OrderbookID → (symbol, class_code)
               Also stores NumberOfDecimalsPrice per orderbook_id for reference.

      Pass 2 — Market Data (MC221/121/321 + MC167):
               decodes order/trade messages, resolves class_code via the map,
               routes rows to the correct WriterPool slot.

    Parameters
    ----------
    filter_products : list[str] | None
        ClassCodes to extract, e.g. ['HSI', 'MHI']. None = extract all.
    """

    def __init__(self, filter_products: Optional[list[str]] = None):
        # filter_products as a set for O(1) lookup
        self._filter = set(filter_products) if filter_products else None

        # Primary mapping: OrderbookID → (symbol, class_code)
        # Built in pass 1, consumed millions of times in pass 2 → must be fast
        self._ob_map: dict[int, tuple[str, str]] = {}

        # Stats for reporting and sanity checks
        self._stats: dict[str, int] = {
            "series_defs":   0,
            "records":       0,
            "add_orders":    0,
            "mod_orders":    0,
            "del_orders":    0,
            "clears":        0,
            "cops":          0,
            "trades":        0,
            "skipped":       0,   # unknown orderbook_id or filtered product
            "parse_errors":  0,   # malformed payloads (silent)
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Low-level binary helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _read_rec_header(mm: mmap.mmap, offset: int) -> tuple[int, int, int, int]:
        """
        Read 18-byte record header at offset.

        Returns (total_record_size, msg_count, seq_num, send_time_ns).

        RecLen and PktSize are Big Endian (validated empirically — spec says LE
        but real data is BE for these two fields). Everything else is LE.
        """
        rec_len    = struct.unpack_from(">H", mm, offset)[0]      # BE
        msg_count  = struct.unpack_from("<B", mm, offset + 4)[0]  # LE (1 byte)
        seq_num    = struct.unpack_from("<I", mm, offset + 6)[0]  # LE uint32
        send_time  = struct.unpack_from("<Q", mm, offset + 10)[0] # LE uint64
        return rec_len + 2, msg_count, seq_num, send_time

    @staticmethod
    def _read_msg_header(mm: mmap.mmap, offset: int) -> tuple[int, int]:
        """
        Read 4-byte message header at offset.
        Returns (msg_size, msg_type), both LE uint16.
        msg_size includes these 4 bytes → payload = msg_size - 4.
        """
        msg_size = struct.unpack_from("<H", mm, offset)[0]
        msg_type = struct.unpack_from("<H", mm, offset + 2)[0]
        return msg_size, msg_type

    # ─────────────────────────────────────────────────────────────────────────
    # Pass 1 — Series Definitions
    # ─────────────────────────────────────────────────────────────────────────

    def _parse_series_defs(self, filepath: Path) -> None:
        """
        Scan a Series Definition channel and populate _ob_map.

        Processes MSG_SERIES_DEF_BASE (303) and MSG_SERIES_DEF_EXT (304).
        303 takes priority over 304 — never overwrites a 303 entry with a 304.

        Uses byte-level recovery on malformed records (advance 1 byte, retry)
        — same strategy as the original parser.py.
        """
        file_size = filepath.stat().st_size

        with open(filepath, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                offset = 0

                while offset + RECORD_HEADER_SIZE <= file_size:
                    total, msg_count, _, _ = self._read_rec_header(mm, offset)

                    if total < RECORD_HEADER_SIZE or offset + total > file_size:
                        offset += 1
                        continue

                    msg_offset = offset + RECORD_HEADER_SIZE

                    for _ in range(msg_count):
                        if msg_offset + MESSAGE_HEADER_SIZE > offset + total:
                            break
                        msg_size, msg_type = self._read_msg_header(mm, msg_offset)
                        if msg_size < MESSAGE_HEADER_SIZE:
                            break
                        msg_end = msg_offset + msg_size
                        if msg_end > offset + total:
                            break

                        payload = mm[msg_offset + MESSAGE_HEADER_SIZE: msg_end]

                        try:
                            if msg_type == MSG_SERIES_DEF_BASE:
                                self._register_series_base(payload)
                            elif msg_type == MSG_SERIES_DEF_EXT:
                                self._register_series_ext(payload)
                        except Exception:
                            self._stats["parse_errors"] += 1

                        msg_offset = msg_end

                    offset += total

    def _register_series_base(self, payload: bytes) -> None:
        """
        Extract OrderbookID + Symbol from SeriesDefinitionBase (303).

        Layout (payload offsets, after 4-byte MsgHeader):
          [0-3]   OrderbookID           Uint32 LE
          [4-35]  Symbol                String 32
          [36]    FinancialProduct      Uint8
          [37-38] NumberOfDecimalsPrice Uint16 LE  (stored for reference, not used in raw)

        Priority: 303 always wins over 304 — once an ID is registered via 303,
        it is never overwritten.
        """
        if len(payload) < 39:
            return
        oid    = struct.unpack_from("<I", payload, 0)[0]
        symbol = payload[4:36].decode("ascii", errors="replace").strip()
        if oid == 0 or not symbol:
            return

        # Only register from 303 if not already registered (303 takes priority)
        # We store source as the symbol string (non-empty = registered)
        if oid not in self._ob_map:
            self._ob_map[oid] = (symbol, symbol[:3])
            self._stats["series_defs"] += 1
        # If already registered via 303, keep existing — no overwrite

    def _register_series_ext(self, payload: bytes) -> None:
        """
        Extract OrderbookID + Symbol from SeriesDefinitionExtended (304).

        Used only as fallback when 303 has not registered this ID.
        Layout:
          [0-3]   OrderBookID  Uint32 LE
          [4-35]  Symbol       String 32
        """
        if len(payload) < 36:
            return
        oid    = struct.unpack_from("<I", payload, 0)[0]
        symbol = payload[4:36].decode("ascii", errors="replace").strip()
        if oid == 0 or not symbol:
            return

        # 304 only fills gaps — never overwrites a 303 entry
        if oid not in self._ob_map:
            self._ob_map[oid] = (symbol, symbol[:3])
            self._stats["series_defs"] += 1

    # ─────────────────────────────────────────────────────────────────────────
    # Pass 2 — Market Data
    # ─────────────────────────────────────────────────────────────────────────

    def _parse_market_data(self, filepath: Path, pool: _WriterPool) -> None:
        """
        Stream-parse a Full Order Book channel and route rows to the WriterPool.

        Iterates records sequentially via mmap. For each record:
          1. Read RecordHeader (send_time_ns, seq_num, msg_count)
          2. For each message in the record:
             a. Decode msg_type
             b. Parse payload → row dict (returns None if unknown/filtered/malformed)
             c. pool.append(class_code, kind, row)

        msg_index (0-based position within the packet) is tracked per record
        and stored in each row — required for exact merge-sort in the adapter.
        """
        file_size     = filepath.stat().st_size
        last_progress = 0

        with open(filepath, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                offset = 0

                while offset + RECORD_HEADER_SIZE <= file_size:
                    self._stats["records"] += 1

                    if self._stats["records"] - last_progress >= PROGRESS_INTERVAL:
                        pct = offset / file_size * 100
                        logger.info("  %.1f%%  (records=%s  trades=%s)",
                                    pct,
                                    f"{self._stats['records']:,}",
                                    f"{self._stats['trades']:,}")
                        last_progress = self._stats["records"]

                    total, msg_count, seq_num, send_time = \
                        self._read_rec_header(mm, offset)

                    if total < RECORD_HEADER_SIZE or offset + total > file_size:
                        offset += 1
                        continue

                    msg_offset = offset + RECORD_HEADER_SIZE

                    for msg_index in range(msg_count):
                        if msg_offset + MESSAGE_HEADER_SIZE > offset + total:
                            break
                        msg_size, msg_type = self._read_msg_header(mm, msg_offset)
                        if msg_size < MESSAGE_HEADER_SIZE:
                            break
                        msg_end = msg_offset + msg_size
                        if msg_end > offset + total:
                            break

                        payload = mm[msg_offset + MESSAGE_HEADER_SIZE: msg_end]

                        try:
                            if msg_type in ORDER_MSG_TYPES:
                                row, class_code = self._make_order_row(
                                    payload, msg_type, send_time, seq_num, msg_index
                                )
                                if row is not None:
                                    pool.append(class_code, "orders", row)
                            elif msg_type == MSG_TRADE:
                                row, class_code = self._make_trade_row(
                                    payload, send_time, seq_num, msg_index
                                )
                                if row is not None:
                                    pool.append(class_code, "trades", row)
                            # All other types (100, 301-305, 320, 321, 322, 323...)
                            # are silently ignored — not part of order book data

                        except Exception:
                            self._stats["parse_errors"] += 1

                        msg_offset = msg_end

                    offset += total

    # ─────────────────────────────────────────────────────────────────────────
    # Row builders
    # ─────────────────────────────────────────────────────────────────────────

    def _resolve_orderbook(self, orderbook_id: int) -> tuple[bool, str, str]:
        """
        Look up (symbol, class_code) for an OrderbookID.

        Returns (found, symbol, class_code).
        found=False if the ID is unknown (Series Def not seen) or filtered out.
        """
        entry = self._ob_map.get(orderbook_id)
        if entry is None:
            self._stats["skipped"] += 1
            return False, "", ""
        symbol, class_code = entry
        if self._filter is not None and class_code not in self._filter:
            self._stats["skipped"] += 1
            return False, "", ""
        return True, symbol, class_code

    def _make_order_row(
        self,
        payload: bytes,
        msg_type: int,
        send_time: int,
        seq_num: int,
        msg_index: int,
    ) -> tuple[Optional[dict], str]:
        """
        Build a raw order row dict from a payload.

        Handles all 5 order message types:
          330 Add Order    — full fields including lot_type, order_type, position
          331 Modify Order — same layout as Add (price/qty/side can change)
          332 Delete Order — no price/qty/lot_type/order_type (set to 0/-1)
          335 Clear        — only orderbook_id (side=-1, all others 0)
          364 COP          — orderbook_id + price + quantity (no order_id/side)

        Returns (row_dict, class_code) or (None, '') if skipped/malformed.
        """
        # All order messages start with orderbook_id at offset 0
        if len(payload) < 4:
            return None, ""

        orderbook_id = struct.unpack_from("<I", payload, 0)[0]
        found, symbol, class_code = self._resolve_orderbook(orderbook_id)
        if not found:
            return None, ""

        # Base row — fields common to all order message types
        row: dict = {
            "send_time_ns":       send_time,
            "seq_num":            seq_num,
            "msg_index":          msg_index,
            "msg_type":           msg_type,
            "orderbook_id":       orderbook_id,
            "symbol":             symbol,
            "class_code":         class_code,
            # Defaults — overridden below per message type
            "order_id":           0,
            "price":              0,
            "quantity":           0,
            "side":               -1,
            "lot_type":           0,
            "order_type":         0,
            "orderbook_position": 0,
        }

        if msg_type in (MSG_ADD_ORDER, MSG_MODIFY_ORDER):
            # Layout (payload offsets after MsgHeader):
            #   [0-3]   OrderbookID        Uint32 LE  — already read
            #   [4-11]  OrderID            Uint64 LE
            #   [12-15] Price              Int32  LE
            #   [16-19] Quantity           Uint32 LE
            #   [20]    Side               Uint8      0=Bid, 1=Offer
            #   [21]    LotType            Uint8
            #   [22-23] OrderType          Uint16 LE
            #   [24-27] OrderBookPosition  Uint32 LE
            # Total payload: 28 bytes (MsgSize=32 including header)
            if len(payload) < 28:
                return None, ""
            row["order_id"]           = struct.unpack_from("<Q", payload, 4)[0]
            row["price"]              = struct.unpack_from("<i", payload, 12)[0]
            row["quantity"]           = struct.unpack_from("<I", payload, 16)[0]
            row["side"]               = struct.unpack_from("<B", payload, 20)[0]
            row["lot_type"]           = struct.unpack_from("<B", payload, 21)[0]
            row["order_type"]         = struct.unpack_from("<H", payload, 22)[0]
            row["orderbook_position"] = struct.unpack_from("<I", payload, 24)[0]

            if msg_type == MSG_ADD_ORDER:
                self._stats["add_orders"] += 1
            else:
                self._stats["mod_orders"] += 1

        elif msg_type == MSG_DELETE_ORDER:
            # Layout:
            #   [0-3]   OrderbookID  Uint32 LE  — already read
            #   [4-11]  OrderID      Uint64 LE
            #   [12]    Side         Uint8
            #   [13]    Filler       1 byte
            # Total payload: 14 bytes (MsgSize=18)
            if len(payload) < 13:
                return None, ""
            row["order_id"] = struct.unpack_from("<Q", payload, 4)[0]
            row["side"]     = struct.unpack_from("<B", payload, 12)[0]
            # price=0, quantity=0, lot_type=0, order_type=0, position=0
            self._stats["del_orders"] += 1

        elif msg_type == MSG_ORDERBOOK_CLEAR:
            # Layout:
            #   [0-3]   OrderbookID  Uint32 LE  — already read
            # Total payload: 4 bytes (MsgSize=8)
            # No order_id, no price, no side — all defaults (0 / -1)
            self._stats["clears"] += 1

        elif msg_type == MSG_COP:
            # Layout:
            #   [0-3]   OrderbookID            Uint32 LE  — already read
            #   [4-7]   CalculatedOpeningPrice Int32  LE
            #   [8-11]  Filler                 4 bytes
            #   [12-19] Quantity               Uint64 LE
            # Total payload: 20 bytes (MsgSize=24)
            if len(payload) >= 8:
                row["price"] = struct.unpack_from("<i", payload, 4)[0]
            if len(payload) >= 20:
                # Quantity is Uint64 but capped to int32 range in schema;
                # COP quantities are always small (indicative volume at COP price)
                qty = struct.unpack_from("<Q", payload, 12)[0]
                row["quantity"] = min(qty, 2**31 - 1)
            # side=-1 (no side for COP), no order_id
            self._stats["cops"] += 1

        return row, class_code

    def _make_trade_row(
        self,
        payload: bytes,
        send_time: int,
        seq_num: int,
        msg_index: int,
    ) -> tuple[Optional[dict], str]:
        """
        Build a raw trade row dict from a Trade (350) payload.

        Layout (payload offsets after MsgHeader):
          [0-3]   OrderbookID    Uint32 LE  — already resolved
          [4-11]  OrderID        Uint64 LE  — aggressor (0 if not available)
          [12-15] Price          Int32  LE
          [16-23] TradeID        Uint64 LE
          [24-27] ComboGroupID   Uint32 LE  — 0 for simple trades
          [28]    Side           Uint8      — 0=N/A, 2=Buy, 3=Sell
          [29]    DealType       Uint8      — bitmask: 1=Printable, 2=Cross, 4=Reported
          [30-31] TradeCondition Uint16 LE  — bitmask: 2=InternalCross, 16=OffMarket
          [32-33] DealInfo       Uint16 LE  — bitmask: 1=ReportedTrade
          [34-35] Filler         2 bytes
          [36-43] Quantity       Uint64 LE
          [44-51] TradeTime      Uint64 LE  — matching engine ts, UTC ns, ~10ms
        Total payload: 52 bytes (MsgSize=56)

        Returns (row_dict, class_code) or (None, '') if skipped/malformed.
        """
        if len(payload) < 52:
            return None, ""

        orderbook_id = struct.unpack_from("<I", payload, 0)[0]
        found, symbol, class_code = self._resolve_orderbook(orderbook_id)
        if not found:
            return None, ""

        row = {
            "send_time_ns":    send_time,
            "seq_num":         seq_num,
            "msg_index":       msg_index,
            "trade_time_ns":   struct.unpack_from("<Q", payload, 44)[0],
            "orderbook_id":    orderbook_id,
            "symbol":          symbol,
            "class_code":      class_code,
            "order_id":        struct.unpack_from("<Q", payload, 4)[0],
            "price":           struct.unpack_from("<i", payload, 12)[0],
            "quantity":        struct.unpack_from("<Q", payload, 36)[0],  # Uint64
            "trade_id":        struct.unpack_from("<Q", payload, 16)[0],
            "side":            struct.unpack_from("<B", payload, 28)[0],
            "deal_type":       struct.unpack_from("<B", payload, 29)[0],
            "combo_group_id":  struct.unpack_from("<I", payload, 24)[0],
            "trade_condition": struct.unpack_from("<H", payload, 30)[0],
            "deal_info":       struct.unpack_from("<H", payload, 32)[0],
        }

        self._stats["trades"] += 1
        return row, class_code

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def parse_daily(
        self,
        bin_dir: Path,
        date_str: str,
        out_dir: Path,
    ) -> dict[str, tuple[Path, Path]]:
        """
        Full two-pass parse for one trading day.

        Pass 1: scan Series Definition channels → populate _ob_map
        Pass 2: scan Full Order Book channels → write raw Parquet files

        Parameters
        ----------
        bin_dir  : directory containing MC*_All_YYYYMMDD binary files
        date_str : date in YYYYMMDD format, e.g. '20260203'
        out_dir  : root of the raw Parquet tree (provider=hkex level)

        Returns
        -------
        dict[class_code → (orders_path, trades_path)]
        Only products that had at least one row are present.
        """
        pool = _WriterPool(out_dir, date_str, self._filter)

        try:
            # ── Pass 1: Series Definitions ────────────────────────────────────
            # Process in priority order: MC101/201/301 (msg 303) before MC151 (msg 304)
            # This ensures NumberOfDecimalsPrice and FinancialProduct-aware symbols
            # take precedence over the OAPI fallback.
            def_channels = [
                f"MC101_All_{date_str}",
                f"MC201_All_{date_str}",
                f"MC301_All_{date_str}",
                f"MC151_All_{date_str}",
            ]
            logger.info("[Pass 1] Series Definitions")
            for ch_name in def_channels:
                ch_path = bin_dir / ch_name
                if not ch_path.exists():
                    logger.warning("  SKIP %s (not found)", ch_name)
                    continue
                before = self._stats["series_defs"]
                self._parse_series_defs(ch_path)
                added = self._stats["series_defs"] - before
                logger.info("  %s → +%s instruments", ch_name, f"{added:,}")

            logger.info("  Total orderbook IDs mapped: %s",
                        f"{len(self._ob_map):,}")

            # ── Pass 2: Full Order Book channels ──────────────────────────────
            # MC221 first (larger, HSI/HHI), then MC121 (MHI/MCH), then MC321, MC167
            data_channels = [
                (f"MC221_All_{date_str}", "Partition2 (HSI/HHI)"),
                (f"MC121_All_{date_str}", "Partition1 (MHI/MCH)"),
                (f"MC321_All_{date_str}", "Partition3 (CHN/LR/...)"),
                (f"MC167_All_{date_str}", "BlockTrades"),
            ]
            logger.info("[Pass 2] Market Data")
            for ch_name, label in data_channels:
                ch_path = bin_dir / ch_name
                if not ch_path.exists():
                    logger.warning("  SKIP %s (not found)", ch_name)
                    continue
                size_gb = ch_path.stat().st_size / 1e9
                logger.info("  %s  (%s)  %.2f GB", ch_name, label, size_gb)
                before_add   = self._stats["add_orders"]
                before_trade = self._stats["trades"]
                self._parse_market_data(ch_path, pool)
                logger.info("    → add_orders=+%s  trades=+%s",
                            f"{self._stats['add_orders'] - before_add:,}",
                            f"{self._stats['trades'] - before_trade:,}")

        finally:
            # Always close writers — even on exception — to avoid corrupt files
            result = pool.close_all()

        # ── Summary ───────────────────────────────────────────────────────────
        logger.info("Done. %d product(s) written:", len(result))
        for cc, (op, tp) in sorted(result.items()):
            o_mb = op.stat().st_size / 1e6 if op.exists() else 0
            t_mb = tp.stat().st_size / 1e6 if tp.exists() else 0
            logger.info("  %-8s orders=%.0f MB  trades=%.0f MB", cc, o_mb, t_mb)
        logger.info("Stats: %s", self._stats)

        return result

    def get_stats(self) -> dict:
        """Return a copy of the internal stats dict."""
        return self._stats.copy()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="HKEX FBD-NSOM Raw Parser — binary channels → raw Parquet"
    )
    ap.add_argument(
        "--date", required=True,
        help="Trading date YYYY-MM-DD, e.g. 2026-02-03",
    )
    ap.add_argument(
        "--products", nargs="*", default=None,
        help="ClassCodes to extract, e.g. HSI MHI HHI MCH. Default: all",
    )
    ap.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING"],
    )
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    # Derive paths from config — no hardcoding in CLI
    bin_dir = hkex_bin_dir(args.date)
    out_dir = DATA_RAW / "provider=HKEX" / "venue=HKEX"
    date_str = args.date.replace("-", "")  # '2026-02-03' → '20260203'

    if not bin_dir.exists():
        logger.error("Binary directory not found: %s", bin_dir)
        logger.error("Expected structure: binaries/provider=HKEX/product=OMDD/year=YYYY/month=MM/day=DD/")
        sys.exit(1)

    filter_str = ", ".join(args.products) if args.products else "ALL"
    logger.info("HKEX Raw Parser — date=%s  products=%s", args.date, filter_str)
    logger.info("bin-dir : %s", bin_dir)
    logger.info("out-dir : %s", out_dir)

    parser = HKEXRawParser(filter_products=args.products)
    parser.parse_daily(
        bin_dir  = bin_dir,
        date_str = date_str,
        out_dir  = out_dir,
    )


if __name__ == "__main__":
    main()