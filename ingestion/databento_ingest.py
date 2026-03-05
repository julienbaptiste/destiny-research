"""
Databento MBO Ingestion Pipeline v2
=====================================
Converts Databento DBN files (MBO schema) to Hive-partitioned Parquet files.

Strategy:
    1. Write full DBN file to a temp Parquet using Databento's native to_parquet()
       with price_type="fixed" (raw int64 fixedpoint 1e-9) and pretty_ts=False
       (raw uint64 nanoseconds). This avoids all Python-level row iteration.
    2. Read temp Parquet via PyArrow, filter by action into orders / trades tables.
    3. Write two output files per day in Hive-partitioned layout.

Output convention (consistent with HKEX parser):
    data/market_data/product=FDAX/year=2025/month=05/FDAX_20250502_orders.parquet
    data/market_data/product=FDAX/year=2025/month=05/FDAX_20250502_trades.parquet

Split logic:
    - orders : action in {A=Add, C=Cancel, M=Modify, R=Clear}
    - trades : action in {T=Trade, F=Fill}

Price encoding:
    - Stored as raw int64 fixedpoint (1e-9 precision), e.g. 22799000000000 = 22799.0
    - Divide by 1e9 at analysis time: price_float = df["price"] / 1e9
    - Never converted to float at rest — lossless storage

Timestamp encoding:
    - ts_recv  : uint64 nanoseconds UTC — primary sort key (monotonic)
    - ts_event : uint64 nanoseconds UTC — exchange clock (use for lead-lag, latency)
    - pretty_ts=False preserves raw integers throughout

Usage:
    # Single file
    python databento_ingest.py \\
        --input ~/downloads/xeur-eobi-20250502.mbo.dbn.zst \\
        --output ~/destiny-research/data/market_data \\
        --product FDAX

    # Batch directory (--resume skips already-converted dates)
    python databento_ingest.py \\
        --input-dir ~/downloads/FDAX_2025/ \\
        --output ~/destiny-research/data/market_data \\
        --product FDAX --resume

Dependencies: databento>=0.50, pyarrow>=12
"""

import argparse
import logging
import re
import sys
import tempfile
from pathlib import Path
from typing import Iterator

import databento as db
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# MBO action codes — single char strings as written by Databento native to_parquet
TRADE_ACTIONS: frozenset[str] = frozenset({"T", "F"})             # Trade, Fill
ORDER_ACTIONS: frozenset[str] = frozenset({"A", "C", "M", "R"})   # Add, Cancel, Modify, Reset

# Columns to keep — drop Databento internal fields (rtype, publisher_id, instrument_id)
# symbol is kept: useful for multi-contract files (continuous + front month in same DBN)
OUTPUT_COLUMNS: list[str] = [
    "ts_recv",      # uint64 ns UTC — primary sort key
    "ts_event",     # uint64 ns UTC — exchange clock
    "ts_in_delta",  # int32 ns     — feed latency proxy (ts_recv - ts_exchange_send)
    "sequence",     # uint32       — venue sequence number (gap detection per channel)
    "order_id",     # uint64       — unique order identifier (stable across A/C/M)
    "price",        # int64        — fixedpoint 1e-9 (divide by 1e9 at analysis time)
    "size",         # uint32       — quantity in lots
    "action",       # string       — A/C/M/R/T/F
    "side",         # string       — A=Ask, B=Bid, N=None
    "flags",        # uint8        — bitmask (F_LAST=0x80, F_SNAPSHOT=0x20, ...)
    "channel_id",   # uint8        — venue channel / partition ID
    "symbol",       # string       — resolved instrument symbol (e.g. FDAX, NIY)
]

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def build_output_path(root: Path, product: str, date_str: str, kind: str) -> Path:
    """
    Build Hive-partitioned output path.

    build_output_path(root, "FDAX", "20250502", "orders")
    → root/product=FDAX/year=2025/month=05/FDAX_20250502_orders.parquet
    """
    year  = date_str[:4]
    month = date_str[4:6]
    directory = root / f"product={product}" / f"year={year}" / f"month={month}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{product}_{date_str}_{kind}.parquet"


def extract_date_from_filename(path: Path) -> str | None:
    """
    Extract YYYYMMDD from a Databento DBN filename.
    Handles: glbx-mdp3-20250103.mbo.dbn.zst, NIY_20250103.mbo.dbn.zst, etc.
    Returns None if no 8-digit date string is found.
    """
    match = re.search(r"(\d{8})", path.name)
    return match.group(1) if match else None


# ---------------------------------------------------------------------------
# Core: single DBN file → orders.parquet + trades.parquet
# ---------------------------------------------------------------------------

def convert_dbn_file(
    dbn_path: Path,
    output_root: Path,
    product: str,
    date_str: str,
    overwrite: bool = False,
) -> dict[str, int]:
    """
    Convert one Databento MBO DBN file to two Parquet files (orders + trades).

    Pipeline:
        1. Databento native to_parquet() → temp file
           (raw int64 prices, uint64 timestamps, no Python row iteration)
        2. PyArrow read → select columns → filter by action
        3. Sort by ts_recv → write orders + trades to Hive-partitioned Parquet

    Memory: temp file is written to disk, PyArrow reads it memory-mapped.
    Peak RAM is bounded by the largest single table (orders ~ 80% of rows).

    Returns {"orders": N, "trades": M}, or {"orders": -1, "trades": -1} if skipped.
    """
    out_orders = build_output_path(output_root, product, date_str, "orders")
    out_trades = build_output_path(output_root, product, date_str, "trades")

    # Resume mode: skip if both outputs already exist
    if not overwrite and out_orders.exists() and out_trades.exists():
        log.info("  SKIP  %s (outputs already exist)", date_str)
        return {"orders": -1, "trades": -1}

    log.info("Converting  %s  →  %s", dbn_path.name, date_str)

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "raw.parquet"

        # Step 1: Databento native write — fastest possible path
        # price_type="fixed" → raw int64 fixedpoint, no float conversion
        # pretty_ts=False    → raw uint64 ns, no pandas Timestamp objects
        # map_symbols=True   → resolve instrument_id → symbol string
        store = db.DBNStore.from_file(str(dbn_path))
        store.to_parquet(
            tmp_path,
            price_type="fixed",
            pretty_ts=False,
            map_symbols=True,
        )

        # Step 2: read via PyArrow (memory-mapped, zero-copy where possible)
        table = pq.read_table(tmp_path)

        # Select output columns (skip unavailable ones defensively)
        available = set(table.schema.names)
        cols = [c for c in OUTPUT_COLUMNS if c in available]
        table = table.select(cols)

        # Step 3: filter by action into orders / trades
        action_col = table.column("action")
        order_mask = pc.is_in(action_col, value_set=pa.array(list(ORDER_ACTIONS)))
        trade_mask = pc.is_in(action_col, value_set=pa.array(list(TRADE_ACTIONS)))

        orders_table = table.filter(order_mask).sort_by("ts_recv")
        trades_table = table.filter(trade_mask).sort_by("ts_recv")

        # Step 4: write final output files
        pq.write_table(orders_table, str(out_orders), compression="snappy", version="2.6")
        pq.write_table(trades_table, str(out_trades), compression="snappy", version="2.6")

    counts = {"orders": len(orders_table), "trades": len(trades_table)}
    log.info(
        "  OK    %s  orders=%d  trades=%d",
        date_str, counts["orders"], counts["trades"],
    )
    return counts


# ---------------------------------------------------------------------------
# Batch mode
# ---------------------------------------------------------------------------

def iter_dbn_files(input_dir: Path) -> Iterator[Path]:
    """Yield all DBN / DBN.ZST files in a directory, sorted chronologically."""
    files: list[Path] = []
    for pattern in ("*.dbn", "*.dbn.zst"):
        files.extend(input_dir.glob(pattern))
    yield from sorted(files)


def run_batch(
    input_dir: Path,
    output_root: Path,
    product: str,
    resume: bool,
) -> None:
    """Convert all DBN files in input_dir to split Parquet, optionally resuming."""
    files = list(iter_dbn_files(input_dir))
    if not files:
        log.error("No DBN files found in %s", input_dir)
        sys.exit(1)

    log.info("Batch: %d files in %s", len(files), input_dir)

    total_orders = total_trades = skipped = errors = 0

    for idx, dbn_path in enumerate(files, 1):
        date_str = extract_date_from_filename(dbn_path)
        if date_str is None:
            log.warning(
                "[%d/%d] Cannot infer date from %s — skipping",
                idx, len(files), dbn_path.name,
            )
            continue

        log.info("[%d/%d] %s", idx, len(files), dbn_path.name)
        try:
            counts = convert_dbn_file(
                dbn_path=dbn_path,
                output_root=output_root,
                product=product,
                date_str=date_str,
                overwrite=not resume,
            )
            if counts["orders"] < 0:
                skipped += 1
            else:
                total_orders += counts["orders"]
                total_trades += counts["trades"]
        except Exception as exc:
            log.error("  ERROR %s: %s", dbn_path.name, exc, exc_info=True)
            errors += 1

    log.info(
        "Batch complete — files=%d  orders=%d  trades=%d  skipped=%d  errors=%d",
        len(files), total_orders, total_trades, skipped, errors,
    )
    if errors:
        log.warning("%d file(s) failed — check logs above", errors)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Databento MBO DBN files to Hive-partitioned Parquet (orders + trades split).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--input",     type=Path, help="Single DBN input file.")
    grp.add_argument("--input-dir", type=Path, help="Directory of DBN files (batch mode).")

    parser.add_argument("--output",    type=Path, required=True, help="Root output directory.")
    parser.add_argument("--product",   type=str,  required=True, help="Product ticker (e.g. FDAX, NIY, ES).")
    parser.add_argument("--date",      type=str,  default=None,  help="Date override YYYYMMDD (single-file mode).")
    parser.add_argument("--resume",    action="store_true",       help="Skip already-converted dates.")
    parser.add_argument("--overwrite", action="store_true",       help="Overwrite existing Parquet files.")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    output_root = args.output.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    if args.input_dir:
        input_dir = args.input_dir.expanduser().resolve()
        if not input_dir.is_dir():
            log.error("Not a directory: %s", input_dir)
            sys.exit(1)
        run_batch(
            input_dir=input_dir,
            output_root=output_root,
            product=args.product,
            resume=args.resume,
        )
    else:
        dbn_path = args.input.expanduser().resolve()
        if not dbn_path.exists():
            log.error("File not found: %s", dbn_path)
            sys.exit(1)
        date_str = args.date or extract_date_from_filename(dbn_path)
        if date_str is None:
            log.error("Cannot infer date from %s — use --date YYYYMMDD", dbn_path.name)
            sys.exit(1)
        convert_dbn_file(
            dbn_path=dbn_path,
            output_root=output_root,
            product=args.product,
            date_str=date_str,
            overwrite=args.overwrite,
        )


if __name__ == "__main__":
    main()
