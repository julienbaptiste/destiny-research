"""
clean_cme_orders.py — CME Globex MBO orders cleaning pipeline.

Mirrors clean_eurex_orders.py architecture, adapted for CME MDP3 specifics:

Filtering rules (applied in order):
  Rule 1 — Exclude F_SNAPSHOT recovery block  (flags & 32 = 0)
  Rule 2 — Exclude sentinel prices             (price != INT64_MAX)
  Rule 3 — Front-month symbol filter           (symbol = dominant outright)

Session filter (applied after rules):
  RTH:      09:30–16:00 ET (DST-aware, 13:30–20:00 or 14:30–21:00 UTC)
  Full:     exclude maintenance window only (21:00–22:00 UTC)

Key CME differences vs Eurex:
  - No implied legs at negative prices — all negatives are calendar spread
    instruments filtered by Rule 3 (symbol filter)
  - No outlier price band needed — spread instruments are a distinct symbol
  - F_SNAPSHOT is a clean block at ts=session_open, not warm-up crossed books
  - flags=UTINYINT (uint8) — cast-safe for bitmask operations in DuckDB

Output: {product}_YYYYMMDD_orders_clean.parquet
  Hive-partitioned: product= / year= / month=
  Sorted by ts_recv (required for LOB reconstruction)
  row_group_size=500_000 (optimised for sequential tick-by-tick scan)

Usage:
    python clean_cme_orders.py --product ES --from-date 2025-10-01 --to-date 2025-10-31
    python clean_cme_orders.py --product ES --from-date 2025-10-01 --dry-run
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from cme_config import (
    AVAILABLE_PRODUCTS,
    F_SNAPSHOT,
    INT64_MAX,
    PRODUCT_CONFIG,
    front_month_subquery,
    rth_utc_bounds,
    session_where_clause,
    ts_utc_expr,
)

# ── Repository layout (relative to destiny-research root) ─────────────────────
DATA_ROOT    = Path("data/market_data")
OUTPUT_ROOT  = Path("data/clean")

# ── Output schema ──────────────────────────────────────────────────────────────
# Subset of columns retained from raw Parquet — same as Eurex cleaner.
# Hive partition columns (product, year, month) are dropped from the payload
# since they are encoded in the directory path.
KEEP_COLS = [
    "ts_recv",       # UBIGINT ns UTC — primary sort key, monotonic
    "ts_event",      # UBIGINT ns UTC — exchange clock (use for lead-lag)
    "ts_in_delta",   # INT32  ns     — ts_recv - ts_event (network latency proxy)
    "sequence",      # UINT32        — CME sequence number within channel
    "order_id",      # UBIGINT       — unique order identifier
    "price",         # BIGINT        — fixed-point 1e-9 (e.g. 5800000000000 = 5800.0)
    "size",          # UINT32        — number of contracts
    "action",        # VARCHAR       — A=Add C=Cancel M=Modify R=Replace
    "side",          # VARCHAR       — B=Bid A=Ask
    "flags",         # UTINYINT      — bitmask (F_LAST=128, F_SNAPSHOT=32, ...)
    "channel_id",    # UTINYINT      — CME channel identifier
    "symbol",        # VARCHAR       — front-month contract (e.g. ESZ5)
]

# ── PyArrow output schema ──────────────────────────────────────────────────────
# Explicit schema ensures consistent types across all products and dates.
OUTPUT_SCHEMA = pa.schema([
    pa.field("ts_recv",     pa.uint64()),
    pa.field("ts_event",    pa.uint64()),
    pa.field("ts_in_delta", pa.int32()),
    pa.field("sequence",    pa.uint32()),
    pa.field("order_id",    pa.uint64()),
    pa.field("price",       pa.int64()),
    pa.field("size",        pa.uint32()),
    pa.field("action",      pa.string()),
    pa.field("side",        pa.string()),
    pa.field("flags",       pa.uint8()),
    pa.field("channel_id",  pa.uint8()),
    pa.field("symbol",      pa.string()),
])


# ── Path helpers ───────────────────────────────────────────────────────────────

def raw_orders_path(product: str, d: date) -> Path:
    """Return the path to the raw orders Parquet file for a given date."""
    return (
        DATA_ROOT
        / f"product={product}"
        / f"year={d.year}"
        / f"month={d.month:02d}"
        / f"{product}_{d.strftime('%Y%m%d')}_orders.parquet"
    )


def clean_orders_path(product: str, d: date) -> Path:
    """Return the path for the clean orders output Parquet file."""
    return (
        OUTPUT_ROOT
        / f"product={product}"
        / f"year={d.year}"
        / f"month={d.month:02d}"
        / f"{product}_{d.strftime('%Y%m%d')}_orders_clean.parquet"
    )


# ── Core cleaning logic ────────────────────────────────────────────────────────

def _resolve_front_month(con: duckdb.DuckDBPyConnection, path: str, date_str: str) -> str:
    """Resolve the front-month symbol for a given day.

    Uses the dominant symbol by event count among non-snapshot, non-sentinel,
    outright-only rows (symbols without '-').

    Args:
        con:      DuckDB connection
        path:     path to raw orders Parquet file
        date_str: date label for error messages

    Returns:
        Front-month symbol string (e.g. 'ESZ5')

    Raises:
        ValueError: if the dominant symbol contains '-' (calendar spread)
        ValueError: if no valid symbol found (empty file after filters)
    """
    q = f"""
    SELECT symbol, COUNT(*) AS n
    FROM '{path}'
    WHERE flags & {F_SNAPSHOT} = 0
      AND price != {INT64_MAX}
      AND symbol NOT LIKE '%-%%'     -- pre-filter: outright instruments only
    GROUP BY symbol
    ORDER BY n DESC
    LIMIT 1
    """
    result = con.execute(q).fetchone()

    if result is None:
        raise ValueError(f"[{date_str}] No valid outright rows found — file may be empty or all snapshot")

    symbol = result[0]

    # Guard: dominant symbol must never be a calendar spread
    # This should be impossible after the NOT LIKE filter, but we assert anyway
    if "-" in symbol:
        raise ValueError(
            f"[{date_str}] Front-month resolved to spread instrument '{symbol}' — "
            "this should not happen. Check symbol filter logic."
        )

    return symbol


def clean_day(
    con: duckdb.DuckDBPyConnection,
    product: str,
    cfg: dict,
    d: date,
    use_rth: bool,
    dry_run: bool,
) -> dict:
    """Clean a single day's orders file.

    Args:
        con:     DuckDB connection (shared, in-memory)
        product: product code (e.g. 'ES')
        cfg:     product config dict from PRODUCT_CONFIG
        d:       target date
        use_rth: True = RTH only, False = full electronic session
        dry_run: if True, run diagnostics but do not write output

    Returns:
        dict with cleaning statistics for this day.
    """
    date_str  = d.strftime("%Y-%m-%d")
    raw_path  = raw_orders_path(product, d)
    out_path  = clean_orders_path(product, d)
    path_str  = str(raw_path)

    stats = {
        "date":          date_str,
        "product":       product,
        "front_month":   None,
        "n_raw":         0,
        "n_snapshot":    0,
        "n_sentinel":    0,
        "n_non_front":   0,
        "n_out_session": 0,
        "n_clean":       0,
        "pct_excluded":  0.0,
        "status":        "ok",
        "error":         None,
    }

    # ── Check raw file exists ──────────────────────────────────────────────────
    if not raw_path.exists():
        stats["status"] = "missing"
        stats["error"]  = f"Raw file not found: {raw_path}"
        return stats

    # ── Step 0: resolve front month ────────────────────────────────────────────
    try:
        front_month = _resolve_front_month(con, path_str, date_str)
    except ValueError as e:
        stats["status"] = "error"
        stats["error"]  = str(e)
        return stats

    stats["front_month"] = front_month

    # ── Step 1: count raw rows ─────────────────────────────────────────────────
    stats["n_raw"] = con.execute(f"SELECT COUNT(*) FROM '{path_str}'").fetchone()[0]

    # ── Step 2: diagnostic counts per exclusion rule ───────────────────────────
    diag_q = f"""
    SELECT
        -- Rule 1: F_SNAPSHOT recovery block
        COUNT(*) FILTER (WHERE flags & {F_SNAPSHOT} > 0)
            AS n_snapshot,

        -- Rule 2: sentinel price (market/stop orders without limit price)
        COUNT(*) FILTER (
            WHERE flags & {F_SNAPSHOT} = 0
              AND price = {INT64_MAX}
        ) AS n_sentinel,

        -- Rule 3: non-front-month (spreads + back months)
        COUNT(*) FILTER (
            WHERE flags & {F_SNAPSHOT} = 0
              AND price != {INT64_MAX}
              AND symbol != '{front_month}'
        ) AS n_non_front

    FROM '{path_str}'
    """
    diag = con.execute(diag_q).fetchone()
    stats["n_snapshot"]  = diag[0]
    stats["n_sentinel"]  = diag[1]
    stats["n_non_front"] = diag[2]

    # ── Step 3: session filter count ──────────────────────────────────────────
    # Count rows that pass rules 1-3 but fall outside the session window.
    # This tells us how many pre-market / post-market / maintenance rows exist.
    rth_start, rth_end = rth_utc_bounds(d, cfg)
    session_clause = session_where_clause(cfg, use_rth, d)
    utc_expr = ts_utc_expr("ts_recv")

    out_session_q = f"""
    SELECT COUNT(*)
    FROM '{path_str}'
    WHERE flags & {F_SNAPSHOT} = 0
      AND price != {INT64_MAX}
      AND symbol = '{front_month}'
      AND NOT ({session_clause})
    """
    stats["n_out_session"] = con.execute(out_session_q).fetchone()[0]

    # ── Step 4: build clean dataset ────────────────────────────────────────────
    cols_sql = ", ".join(KEEP_COLS)
    clean_q = f"""
    SELECT {cols_sql}
    FROM '{path_str}'
    WHERE flags & {F_SNAPSHOT} = 0      -- Rule 1: exclude snapshot recovery block
      AND price != {INT64_MAX}          -- Rule 2: exclude sentinel market orders
      AND symbol = '{front_month}'      -- Rule 3: front-month outright only
      AND {session_clause}              -- Session: RTH or full electronic
    ORDER BY ts_recv                    -- mandatory for LOB state machine
    """

    stats["n_clean"] = con.execute(
        f"SELECT COUNT(*) FROM ({clean_q})"
    ).fetchone()[0]

    n_excluded = stats["n_raw"] - stats["n_clean"]
    stats["pct_excluded"] = round(n_excluded * 100.0 / max(stats["n_raw"], 1), 4)

    # ── Step 5: write output (unless dry-run) ─────────────────────────────────
    if not dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)

        # Stream via PyArrow RecordBatchReader for memory efficiency.
        # DuckDB → Arrow zero-copy, no pandas round-trip.
        rel = con.execute(clean_q)
        reader = rel.fetch_record_batch(rows_per_batch=500_000)

        writer = pq.ParquetWriter(
            str(out_path),
            schema=OUTPUT_SCHEMA,
            compression="snappy",
        )
        try:
            while True:
                try:
                    batch = reader.read_next_batch()
                    # Cast to explicit schema to enforce types (DuckDB infers UTINYINT
                    # as uint8 but explicit cast avoids surprises across DuckDB versions)
                    table = pa.Table.from_batches([batch], schema=OUTPUT_SCHEMA)
                    # row_group_size passed here (not in constructor) — PyArrow ≥ 23
                    writer.write_table(table, row_group_size=500_000)
                except StopIteration:
                    break
        finally:
            writer.close()

    return stats


# ── Validation query (post-write sanity check) ────────────────────────────────

def validate_day(
    con: duckdb.DuckDBPyConnection,
    product: str,
    cfg: dict,
    d: date,
) -> dict:
    """Run post-write sanity checks on the clean output file.

    Checks:
      - Row count > 0
      - No snapshot rows leaked through (flags & 32 = 0 on all rows)
      - No sentinel prices (price != INT64_MAX on all rows)
      - No calendar spread symbols (no '-' in symbol)
      - ts_recv is monotonically non-decreasing (required for LOB state machine)
      - Tick size violations (price % tick_fp != 0)

    Returns:
        dict with validation results. 'ok' key is True if all checks pass.
    """
    out_path = clean_orders_path(product, d)
    path_str = str(out_path)
    tick_fp  = cfg["tick_size_fp"]
    date_str = d.strftime("%Y-%m-%d")

    if not out_path.exists():
        return {"date": date_str, "ok": False, "error": "Output file not found"}

    q = f"""
    WITH base AS (
        SELECT
            flags,
            price,
            symbol,
            ts_recv,
            -- compute previous ts_recv for monotonicity check
            LAG(ts_recv) OVER (ORDER BY ts_recv) AS prev_ts_recv
        FROM '{path_str}'
    )
    SELECT
        COUNT(*)                                                  AS n_rows,
        -- Rule 1 check: no snapshot should remain
        COUNT(*) FILTER (WHERE flags & {F_SNAPSHOT} > 0)         AS n_snapshot_leaked,
        -- Rule 2 check: no sentinel prices
        COUNT(*) FILTER (WHERE price = {INT64_MAX})               AS n_sentinel_leaked,
        -- Rule 3 check: no spread symbols
        COUNT(*) FILTER (WHERE symbol LIKE '%-%%')                AS n_spread_leaked,
        -- Tick size check
        COUNT(*) FILTER (
            WHERE price > 0 AND price % {tick_fp} != 0
        )                                                         AS n_tick_violations,
        -- Monotonicity check: count rows where ts_recv < previous ts_recv
        COUNT(*) FILTER (
            WHERE prev_ts_recv IS NOT NULL AND ts_recv < prev_ts_recv
        )                                                         AS n_ts_inversions
    FROM base
    """
    row = con.execute(q).fetchone()
    result = {
        "date":               date_str,
        "n_rows":             row[0],
        "n_snapshot_leaked":  row[1],
        "n_sentinel_leaked":  row[2],
        "n_spread_leaked":    row[3],
        "n_tick_violations":  row[4],
        "n_ts_inversions":    row[5],
    }
    result["ok"] = (
        result["n_rows"]             > 0
        and result["n_snapshot_leaked"]  == 0
        and result["n_sentinel_leaked"]  == 0
        and result["n_spread_leaked"]    == 0
        and result["n_tick_violations"]  == 0
        and result["n_ts_inversions"]    == 0
    )
    return result


# ── CLI ────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean CME Globex MBO orders — session filter + symbol filter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Clean ES October 2025 — RTH only
  python clean_cme_orders.py --product ES --from-date 2025-10-01 --to-date 2025-10-31

  # Full electronic session (no RTH filter)
  python clean_cme_orders.py --product ES --from-date 2025-10-01 --to-date 2025-10-31 --full-session

  # Dry run — diagnostics only, no output written
  python clean_cme_orders.py --product ES --from-date 2025-10-01 --dry-run

  # Skip post-write validation (faster batch)
  python clean_cme_orders.py --product ES --from-date 2025-10-01 --to-date 2025-10-31 --no-validate
        """,
    )
    parser.add_argument(
        "--product", required=True, choices=AVAILABLE_PRODUCTS,
        help="Product code (e.g. ES)",
    )
    parser.add_argument(
        "--from-date", required=True,
        help="Start date inclusive (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--to-date", default=None,
        help="End date inclusive (YYYY-MM-DD). Defaults to --from-date.",
    )
    parser.add_argument(
        "--full-session", action="store_true", default=False,
        help="Use full electronic session instead of RTH only.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print diagnostics only — do not write output files.",
    )
    parser.add_argument(
        "--no-validate", action="store_true", default=False,
        help="Skip post-write validation (faster for large batches).",
    )
    parser.add_argument(
        "--resume", action="store_true", default=False,
        help="Skip dates where clean output already exists.",
    )
    return parser.parse_args()


def _date_range(from_date: date, to_date: date) -> list[date]:
    """Return list of dates from from_date to to_date inclusive."""
    dates = []
    current = from_date
    while current <= to_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def main() -> None:
    args = _parse_args()

    product  = args.product
    cfg      = PRODUCT_CONFIG[product]
    use_rth  = not args.full_session
    dry_run  = args.dry_run
    validate = not args.no_validate and not dry_run
    resume   = args.resume

    from_date = datetime.strptime(args.from_date, "%Y-%m-%d").date()
    to_date   = datetime.strptime(args.to_date,   "%Y-%m-%d").date() \
                if args.to_date else from_date

    session_label = "RTH" if use_rth else "full electronic session"
    print(f"{'═' * 60}")
    print(f"  clean_cme_orders.py")
    print(f"  Product:  {product} ({cfg['description']})")
    print(f"  Session:  {session_label}")
    print(f"  Range:    {from_date} → {to_date}")
    print(f"  Dry run:  {dry_run}")
    print(f"  Validate: {validate}")
    print(f"  Resume:   {resume}")
    print(f"{'═' * 60}")

    dates = _date_range(from_date, to_date)

    # Shared DuckDB connection — reused across all dates for performance
    con = duckdb.connect()

    all_stats    = []
    all_val      = []
    n_ok         = 0
    n_skipped    = 0
    n_missing    = 0
    n_error      = 0

    for d in dates:
        date_str = d.strftime("%Y-%m-%d")

        # Resume: skip if output already exists
        if resume and not dry_run:
            out = clean_orders_path(product, d)
            if out.exists():
                print(f"  [{date_str}] SKIP (already exists)")
                n_skipped += 1
                continue

        # Skip weekends — CME is closed Saturday and Sunday
        if d.weekday() >= 5:
            print(f"  [{date_str}] SKIP (weekend)")
            n_skipped += 1
            continue

        # Clean
        stats = clean_day(con, product, cfg, d, use_rth, dry_run)
        all_stats.append(stats)

        if stats["status"] == "missing":
            print(f"  [{date_str}] MISSING — {stats['error']}")
            n_missing += 1
            continue

        if stats["status"] == "error":
            print(f"  [{date_str}] ERROR   — {stats['error']}")
            n_error += 1
            continue

        # Print per-day summary
        print(
            f"  [{date_str}] {stats['front_month']:6s} | "
            f"raw={stats['n_raw']:>10,} | "
            f"snap={stats['n_snapshot']:>6,} | "
            f"sent={stats['n_sentinel']:>4,} | "
            f"non_fm={stats['n_non_front']:>8,} | "
            f"out_sess={stats['n_out_session']:>8,} | "
            f"clean={stats['n_clean']:>10,} | "
            f"excl={stats['pct_excluded']:.2f}%"
            + (" [DRY]" if dry_run else "")
        )

        # Validate
        if validate:
            val = validate_day(con, product, cfg, d)
            all_val.append(val)
            status = "PASS" if val["ok"] else "FAIL"
            if not val["ok"]:
                print(f"    ⚠ VALIDATION {status}: {val}")
            else:
                print(f"    ✓ validation PASS ({val['n_rows']:,} rows)")

        n_ok += 1

    con.close()

    # ── Final summary ──────────────────────────────────────────────────────────
    print(f"{'═' * 60}")
    print(f"  Done. ok={n_ok} | skipped={n_skipped} | missing={n_missing} | error={n_error}")

    if all_stats:
        total_raw   = sum(s["n_raw"]   for s in all_stats if s["status"] == "ok")
        total_clean = sum(s["n_clean"] for s in all_stats if s["status"] == "ok")
        total_excl  = total_raw - total_clean
        pct_excl    = round(total_excl * 100.0 / max(total_raw, 1), 3)
        print(f"  Total raw:   {total_raw:>12,}")
        print(f"  Total clean: {total_clean:>12,}")
        print(f"  Excluded:    {total_excl:>12,}  ({pct_excl:.3f}%)")

    if all_val:
        n_pass = sum(1 for v in all_val if v["ok"])
        n_fail = sum(1 for v in all_val if not v["ok"])
        print(f"  Validation:  {n_pass}/{len(all_val)} PASS  |  {n_fail} FAIL")
        if n_fail > 0:
            sys.exit(1)

    print(f"{'═' * 60}")


if __name__ == "__main__":
    main()