"""
clean_cme_trades.py — CME Globex MBO trades cleaning pipeline.

Mirrors clean_cme_orders.py architecture, adapted for the trades stream.

Filtering rules:
  Rule 1 — Front-month symbol filter  (symbol = dominant outright)
  Rule 2 — Exclude sentinel prices    (price != INT64_MAX)
  Rule 3 — Session filter             (RTH or full electronic)

Key CME trades specifics vs Eurex:
  - flags=0 on ALL trades (T and F) — F_LAST is NOT set on CME trades.
    The Eurex is_unpaired_aggressor flag (based on flags=128) does not apply here.
  - No F_SNAPSHOT on trades (confirmed from exploration: 0 snapshot rows).
    Rule 1 snapshot exclusion from orders cleaner is therefore a no-op on trades,
    but we keep the sentinel + symbol + session filters for consistency.
  - T/F pairing cannot use F_LAST. Grouping by (ts_event, price) is the correct
    approach for Phase 3 feature engineering — not needed for cleaning.
  - Ratio T/F ≈ 2.45 on ES (363K T / 889K F on 2025-10-01): many trades sweep
    multiple passive resting orders — icebergs and multi-level sweeps common on ES.
  - No negative prices on front-month trades — calendar spread trades appear under
    their own spread symbol (e.g. ESZ5-ESH6) and are filtered by the symbol rule.

Output: {product}_YYYYMMDD_trades_clean.parquet
  Hive-partitioned: product= / year= / month=
  Sorted by ts_recv (required for LOB state machine fill matching)
  row_group_size=500_000 (passed to write_table, not ParquetWriter constructor)

Usage:
    python clean_cme_trades.py --product ES --from-date 2025-10-01 --to-date 2025-10-31
    python clean_cme_trades.py --product ES --from-date 2025-10-01 --dry-run
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
    rth_utc_bounds,
    session_where_clause,
    ts_utc_expr,
)

# ── Repository layout ──────────────────────────────────────────────────────────
DATA_ROOT   = Path("data/market_data")
OUTPUT_ROOT = Path("data/market_data")

# ── Output schema ──────────────────────────────────────────────────────────────
# Identical to orders schema — no extra columns needed.
# Unlike Eurex (where we added is_unpaired_aggressor), CME trades are all
# flags=0 and the T/F pairing logic belongs in Phase 3, not the cleaner.
KEEP_COLS = [
    "ts_recv",
    "ts_event",
    "ts_in_delta",
    "sequence",
    "order_id",
    "price",
    "size",
    "action",
    "side",
    "flags",
    "channel_id",
    "symbol",
]

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

def raw_trades_path(product: str, d: date) -> Path:
    return (
        DATA_ROOT
        / f"product={product}"
        / f"year={d.year}"
        / f"month={d.month:02d}"
        / f"{product}_{d.strftime('%Y%m%d')}_trades.parquet"
    )


def clean_trades_path(product: str, d: date) -> Path:
    return (
        OUTPUT_ROOT
        / f"product={product}"
        / f"year={d.year}"
        / f"month={d.month:02d}"
        / f"{product}_{d.strftime('%Y%m%d')}_trades_clean.parquet"
    )


# ── Front-month resolution ─────────────────────────────────────────────────────

def _resolve_front_month(con: duckdb.DuckDBPyConnection, path: str, date_str: str) -> str:
    """Resolve front-month symbol from the trades file for a given day.

    Uses the same logic as the orders cleaner: dominant symbol by event count
    among outright-only rows (no '-' in symbol), sentinel prices excluded.

    Consistency with orders_clean is guaranteed when both cleaners run on the
    same date range and the same raw data (roll dates identical in both streams).

    Args:
        con:      DuckDB connection
        path:     path to raw trades Parquet file
        date_str: date label for error messages

    Returns:
        Front-month symbol string (e.g. 'ESZ5')

    Raises:
        ValueError: if dominant symbol is a spread or file is empty
    """
    q = f"""
    SELECT symbol, COUNT(*) AS n
    FROM '{path}'
    WHERE price != {INT64_MAX}
      AND symbol NOT LIKE '%-%%'     -- outright instruments only
    GROUP BY symbol
    ORDER BY n DESC
    LIMIT 1
    """
    result = con.execute(q).fetchone()

    if result is None:
        raise ValueError(
            f"[{date_str}] No valid outright rows found in trades file"
        )

    symbol = result[0]
    if "-" in symbol:
        raise ValueError(
            f"[{date_str}] Front-month resolved to spread '{symbol}' — "
            "check symbol filter logic."
        )
    return symbol


# ── Core cleaning logic ────────────────────────────────────────────────────────

def clean_day(
    con:     duckdb.DuckDBPyConnection,
    product: str,
    cfg:     dict,
    d:       date,
    use_rth: bool,
    dry_run: bool,
) -> dict:
    """Clean a single day's trades file.

    Args:
        con:     DuckDB connection
        product: product code (e.g. 'ES')
        cfg:     product config dict from PRODUCT_CONFIG
        d:       target date
        use_rth: True = RTH only, False = full electronic session
        dry_run: if True, compute stats but do not write output

    Returns:
        dict with cleaning statistics.
    """
    date_str = d.strftime("%Y-%m-%d")
    raw_path = raw_trades_path(product, d)
    out_path = clean_trades_path(product, d)
    path_str = str(raw_path)

    stats = {
        "date":         date_str,
        "product":      product,
        "front_month":  None,
        "n_raw":        0,
        "n_snapshot":   0,     # expected 0 on CME trades — diagnostic only
        "n_sentinel":   0,
        "n_non_front":  0,
        "n_out_session": 0,
        "n_t":          0,     # aggressor trade count
        "n_f":          0,     # passive fill count
        "tf_ratio":     0.0,   # F/T ratio (iceberg / sweep indicator)
        "n_clean":      0,
        "pct_excluded": 0.0,
        "status":       "ok",
        "error":        None,
    }

    if not raw_path.exists():
        stats["status"] = "missing"
        stats["error"]  = f"Raw file not found: {raw_path}"
        return stats

    # ── Resolve front month ────────────────────────────────────────────────────
    try:
        front_month = _resolve_front_month(con, path_str, date_str)
    except ValueError as e:
        stats["status"] = "error"
        stats["error"]  = str(e)
        return stats

    stats["front_month"] = front_month

    # ── Raw count ─────────────────────────────────────────────────────────────
    stats["n_raw"] = con.execute(
        f"SELECT COUNT(*) FROM '{path_str}'"
    ).fetchone()[0]

    # ── Diagnostic counts per exclusion rule ───────────────────────────────────
    diag_q = f"""
    SELECT
        -- Snapshot check: should always be 0 on CME trades (all flags=0)
        COUNT(*) FILTER (WHERE flags & {F_SNAPSHOT} > 0)
            AS n_snapshot,

        -- Sentinel prices: market/stop orders without limit price
        COUNT(*) FILTER (WHERE price = {INT64_MAX})
            AS n_sentinel,

        -- Non-front-month: spreads + back months
        COUNT(*) FILTER (
            WHERE price != {INT64_MAX}
              AND symbol != '{front_month}'
        ) AS n_non_front,

        -- T/F action split (before session filter — full day view)
        COUNT(*) FILTER (WHERE action = 'T') AS n_t,
        COUNT(*) FILTER (WHERE action = 'F') AS n_f

    FROM '{path_str}'
    """
    diag = con.execute(diag_q).fetchone()
    stats["n_snapshot"]  = diag[0]
    stats["n_sentinel"]  = diag[1]
    stats["n_non_front"] = diag[2]
    stats["n_t"]         = diag[3]
    stats["n_f"]         = diag[4]
    stats["tf_ratio"]    = round(diag[4] / max(diag[3], 1), 3)

    # ── Session filter count ───────────────────────────────────────────────────
    session_clause = session_where_clause(cfg, use_rth, d)
    out_session_q = f"""
    SELECT COUNT(*)
    FROM '{path_str}'
    WHERE price != {INT64_MAX}
      AND symbol = '{front_month}'
      AND NOT ({session_clause})
    """
    stats["n_out_session"] = con.execute(out_session_q).fetchone()[0]

    # ── Build clean dataset ────────────────────────────────────────────────────
    cols_sql = ", ".join(KEEP_COLS)
    clean_q = f"""
    SELECT {cols_sql}
    FROM '{path_str}'
    WHERE price != {INT64_MAX}          -- Rule 1: exclude sentinel prices
      AND symbol = '{front_month}'      -- Rule 2: front-month outright only
      AND {session_clause}              -- Rule 3: session filter (RTH or full)
    ORDER BY ts_recv                    -- monotonic sort for LOB fill matching
    """

    stats["n_clean"] = con.execute(
        f"SELECT COUNT(*) FROM ({clean_q})"
    ).fetchone()[0]

    n_excluded = stats["n_raw"] - stats["n_clean"]
    stats["pct_excluded"] = round(n_excluded * 100.0 / max(stats["n_raw"], 1), 4)

    # ── Write output ───────────────────────────────────────────────────────────
    if not dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)

        rel    = con.execute(clean_q)
        reader = rel.fetch_record_batch(rows_per_batch=500_000)

        # row_group_size passed to write_table (not ParquetWriter constructor)
        writer = pq.ParquetWriter(
            str(out_path),
            schema=OUTPUT_SCHEMA,
            compression="snappy",
        )
        try:
            while True:
                try:
                    batch = reader.read_next_batch()
                    table = pa.Table.from_batches([batch], schema=OUTPUT_SCHEMA)
                    writer.write_table(table, row_group_size=500_000)
                except StopIteration:
                    break
        finally:
            writer.close()

    return stats


# ── Validation ────────────────────────────────────────────────────────────────

def validate_day(
    con:     duckdb.DuckDBPyConnection,
    product: str,
    cfg:     dict,
    d:       date,
) -> dict:
    """Post-write sanity checks on the clean trades output.

    Checks:
      - Row count > 0
      - No snapshot rows (should always pass — CME trades have flags=0)
      - No sentinel prices
      - No spread symbols
      - Tick size compliance
      - ts_recv monotonically non-decreasing

    Returns:
        dict with check results. 'ok' key is True iff all checks pass.
    """
    out_path = clean_trades_path(product, d)
    path_str = str(out_path)
    tick_fp  = cfg["tick_size_fp"]
    date_str = d.strftime("%Y-%m-%d")

    if not out_path.exists():
        return {"date": date_str, "ok": False, "error": "Output file not found"}

    # Use CTE + LAG for monotonicity — DuckDB does not allow window functions
    # inside COUNT(*) FILTER aggregates directly.
    q = f"""
    WITH base AS (
        SELECT
            flags,
            price,
            symbol,
            ts_recv,
            LAG(ts_recv) OVER (ORDER BY ts_recv) AS prev_ts_recv
        FROM '{path_str}'
    )
    SELECT
        COUNT(*)                                                  AS n_rows,
        COUNT(*) FILTER (WHERE flags & {F_SNAPSHOT} > 0)         AS n_snapshot_leaked,
        COUNT(*) FILTER (WHERE price = {INT64_MAX})               AS n_sentinel_leaked,
        COUNT(*) FILTER (WHERE symbol LIKE '%-%%')                AS n_spread_leaked,
        COUNT(*) FILTER (
            WHERE price > 0 AND price % {tick_fp} != 0
        )                                                         AS n_tick_violations,
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
        result["n_rows"]            > 0
        and result["n_snapshot_leaked"] == 0
        and result["n_sentinel_leaked"] == 0
        and result["n_spread_leaked"]   == 0
        and result["n_tick_violations"] == 0
        and result["n_ts_inversions"]   == 0
    )
    return result


# ── CLI ────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean CME Globex MBO trades — symbol filter + session filter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python clean_cme_trades.py --product ES --from-date 2025-10-01 --to-date 2025-10-31
  python clean_cme_trades.py --product ES --from-date 2025-10-01 --dry-run
  python clean_cme_trades.py --product ES --from-date 2025-10-01 --to-date 2025-10-31 --full-session
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
        help="Skip post-write validation.",
    )
    parser.add_argument(
        "--resume", action="store_true", default=False,
        help="Skip dates where clean output already exists.",
    )
    return parser.parse_args()


def _date_range(from_date: date, to_date: date) -> list[date]:
    dates, current = [], from_date
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
    print(f"  clean_cme_trades.py")
    print(f"  Product:  {product} ({cfg['description']})")
    print(f"  Session:  {session_label}")
    print(f"  Range:    {from_date} → {to_date}")
    print(f"  Dry run:  {dry_run}")
    print(f"  Validate: {validate}")
    print(f"  Resume:   {resume}")
    print(f"{'═' * 60}")

    dates = _date_range(from_date, to_date)
    con   = duckdb.connect()

    all_stats = []
    n_ok = n_skipped = n_missing = n_error = 0

    for d in dates:
        date_str = d.strftime("%Y-%m-%d")

        if resume and not dry_run and clean_trades_path(product, d).exists():
            print(f"  [{date_str}] SKIP (already exists)")
            n_skipped += 1
            continue

        if d.weekday() >= 5:
            print(f"  [{date_str}] SKIP (weekend)")
            n_skipped += 1
            continue

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

        print(
            f"  [{date_str}] {stats['front_month']:6s} | "
            f"raw={stats['n_raw']:>8,} | "
            f"snap={stats['n_snapshot']:>3,} | "
            f"sent={stats['n_sentinel']:>3,} | "
            f"non_fm={stats['n_non_front']:>7,} | "
            f"out_sess={stats['n_out_session']:>7,} | "
            f"T={stats['n_t']:>7,} F={stats['n_f']:>7,} F/T={stats['tf_ratio']:.2f} | "
            f"clean={stats['n_clean']:>8,} | "
            f"excl={stats['pct_excluded']:.2f}%"
            + (" [DRY]" if dry_run else "")
        )

        if validate:
            val    = validate_day(con, product, cfg, d)
            status = "PASS" if val["ok"] else "FAIL"
            if not val["ok"]:
                print(f"    ⚠ VALIDATION {status}: {val}")
            else:
                print(f"    ✓ validation PASS ({val['n_rows']:,} rows)")

        n_ok += 1

    con.close()

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"{'═' * 60}")
    print(f"  Done. ok={n_ok} | skipped={n_skipped} | missing={n_missing} | error={n_error}")

    ok_stats = [s for s in all_stats if s["status"] == "ok"]
    if ok_stats:
        total_raw   = sum(s["n_raw"]   for s in ok_stats)
        total_clean = sum(s["n_clean"] for s in ok_stats)
        total_excl  = total_raw - total_clean
        pct_excl    = round(total_excl * 100.0 / max(total_raw, 1), 3)
        total_t     = sum(s["n_t"] for s in ok_stats)
        total_f     = sum(s["n_f"] for s in ok_stats)
        print(f"  Total raw:   {total_raw:>10,}")
        print(f"  Total clean: {total_clean:>10,}")
        print(f"  Excluded:    {total_excl:>10,}  ({pct_excl:.3f}%)")
        print(f"  Total T:     {total_t:>10,}")
        print(f"  Total F:     {total_f:>10,}")
        print(f"  F/T ratio:   {total_f / max(total_t, 1):.3f}")

    print(f"{'═' * 60}")


if __name__ == "__main__":
    main()