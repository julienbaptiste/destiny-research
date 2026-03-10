# cleaning/clean_eurex_orders.py
#
# Phase 2 — Generic Eurex orders cleaner (FDAX / FESX / FSMI / any future Eurex product)
#
# Usage:
#   python clean_eurex_orders.py --product FDAX
#   python clean_eurex_orders.py --product FESX
#   python clean_eurex_orders.py --product FSMI
#   python clean_eurex_orders.py --product FDAX --from-date 20250502 --to-date 20250509
#   python clean_eurex_orders.py --product FDAX --dry-run
#
# Input:  data/market_data/product=<PRODUCT>/year=YYYY/month=MM/<PRODUCT>_YYYYMMDD_orders.parquet
# Output: data/clean/product=<PRODUCT>/year=YYYY/month=MM/<PRODUCT>_YYYYMMDD_orders_clean.parquet
#
# Filtering rules (Eurex EOBI, all products):
#   1. Session filter: Eurex core session 08:00-22:00 CET, weekdays only
#   2. Sentinel prices (INT64_MAX): exclude — market/stop orders without limit price
#   3. Implied spread legs, negative prices: exclude A/M/C where price <= 0
#      (Eurex synthetic implied-in/implied-out orders on calendar spreads)
#   4. Implied spread legs, positive prices: exclude A/M where price deviates
#      more than 10% from daily median bootstrapped on price > PRICE_FLOOR_FP.
#      The floor (1000 pts) ensures spread-leg differentials don't contaminate the median.
#   5. Back months: front month only — most frequent symbol per day.
#      Prevents multi-contract LOB corruption when multiple expiries coexist in raw file.
#
# No snapshot available for Eurex EOBI — LOB reconstruction starts from zero at
# session open. Databento snapshots only exist for CME Globex MDP 3.0.
#
# Product-specific config (tick size) is driven by PRODUCT_CONFIG dict below.
# To add a new Eurex product: add one entry to PRODUCT_CONFIG.

import argparse
import duckdb
import pyarrow.parquet as pq
from pathlib import Path

# ---------------------------------------------------------------------------
# PRODUCT CONFIG
# ---------------------------------------------------------------------------
# tick_size_fp : tick size in fixed-point int64 (price * 1e9)
#   FDAX : 0.5 pt  → 500_000_000
#   FESX : 1 pt    → 1_000_000_000
#   FSMI : 1 pt    → 1_000_000_000
# price_floor_fp : floor for median bootstrapping — must be above any calendar
#   spread differential but well below any realistic outright price.
#   1000 pts * 1e9 = 1_000_000_000_000 works for all three products.

PRODUCT_CONFIG = {
    "FDAX": {
        "tick_size_fp":    500_000_000,
        "price_floor_fp":  1_000_000_000_000,
        "description":     "DAX Future (Eurex)",
    },
    "FESX": {
        "tick_size_fp":    1_000_000_000,
        "price_floor_fp":  1_000_000_000_000,
        "description":     "Euro Stoxx 50 Future (Eurex)",
    },
    "FSMI": {
        "tick_size_fp":    1_000_000_000,
        "price_floor_fp":  1_000_000_000_000,
        "description":     "SMI Future (Eurex)",
    },
}

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

# Eurex core session boundaries — same for all products
SESSION_START_LOCAL = "08:00:00"
SESSION_END_LOCAL   = "22:00:00"
LOCAL_TZ            = "Europe/Paris"

# Databento sentinel: market/stop orders without limit price
INT64_MAX = 9_223_372_036_854_775_807

# Outlier band for implied spread leg filter (Rule 4)
# 10% is conservative — no Eurex equity index future moves 10% intraday
OUTLIER_BAND = 0.10

# ---------------------------------------------------------------------------
# ARGUMENT PARSING
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Clean Eurex MBO orders — session filter + implied leg removal"
    )
    parser.add_argument(
        "--product", required=True, choices=list(PRODUCT_CONFIG.keys()),
        help="Product to clean (e.g. FDAX, FESX, FSMI)"
    )
    parser.add_argument(
        "--from-date", default=None,
        help="Start date inclusive, format YYYYMMDD (default: all available)"
    )
    parser.add_argument(
        "--to-date", default=None,
        help="End date inclusive, format YYYYMMDD (default: all available)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run all diagnostics but do not write any output files"
    )
    parser.add_argument(
        "--data-root", default="data/market_data",
        help="Root directory for raw market data (default: data/market_data)"
    )
    parser.add_argument(
        "--output-root", default="data/clean",
        help="Root directory for clean output (default: data/clean)"
    )
    return parser.parse_args()

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def ts_local_expr(tz: str) -> str:
    """
    DuckDB expression to convert ts_recv (UBIGINT nanoseconds UTC) to local TIMESTAMPTZ.
    Encapsulated here so the conversion pattern is consistent across all queries.
    Pattern: CAST to BIGINT first (UBIGINT arithmetic not supported), divide by 1e9,
    convert to UTC timestamp, then shift to local tz.
    """
    return f"timezone('{tz}', to_timestamp(CAST(ts_recv AS BIGINT) / 1e9))"


def session_where_clause(tz: str, start: str, end: str, date_filter: str = "") -> str:
    """
    Build the WHERE clause for session filtering.
    date_filter: optional extra AND clause (e.g. "AND ts_local::DATE = '2025-05-02'")
    Uses ISODOW (1=Mon..7=Sun, ISO standard) rather than DAYOFWEEK (ambiguous).
    """
    return f"""
        ISODOW({ts_local_expr(tz)}) BETWEEN 1 AND 5
        AND {ts_local_expr(tz)}::TIME >= '{start}'::TIME
        AND {ts_local_expr(tz)}::TIME <  '{end}'::TIME
        {date_filter}
    """

# ---------------------------------------------------------------------------
# DIAGNOSTIC STEPS (run once over full dataset before writing)
# ---------------------------------------------------------------------------

def run_diagnostics(con: duckdb.DuckDBPyConnection, glob: str, cfg: dict, product: str):
    """
    Full diagnostic pass: raw stats, session stats, sanity checks, flags audit.
    These are informational — output is printed, nothing is written to disk.
    """
    tick_fp    = cfg["tick_size_fp"]
    floor_fp   = cfg["price_floor_fp"]

    print(f"\n{'='*60}")
    print(f"  DIAGNOSTICS — {product}  ({cfg['description']})")
    print(f"{'='*60}")

    # ------------------------------------------------------------------
    # Raw stats
    # ------------------------------------------------------------------
    print("\n[Diag 1] Raw stats (no filter)")
    raw = con.execute(f"""
        SELECT
            COUNT(*)                                        AS total_events,
            MIN(price) / 1e9                                AS price_min_pts,
            MAX(price) / 1e9                                AS price_max_pts,
            MIN(ts_recv)                                    AS ts_recv_min,
            MAX(ts_recv)                                    AS ts_recv_max,
            SUM(CASE WHEN action = 'A' THEN 1 ELSE 0 END)  AS n_add,
            SUM(CASE WHEN action = 'C' THEN 1 ELSE 0 END)  AS n_cancel,
            SUM(CASE WHEN action = 'M' THEN 1 ELSE 0 END)  AS n_modify,
            SUM(CASE WHEN action = 'R' THEN 1 ELSE 0 END)  AS n_clear
        FROM read_parquet('{glob}', hive_partitioning=true)
    """).fetchdf()
    print(raw.T.to_string())

    # ------------------------------------------------------------------
    # Session stats
    # ------------------------------------------------------------------
    print(f"\n[Diag 2] Session filter: {SESSION_START_LOCAL}-{SESSION_END_LOCAL} {LOCAL_TZ}")
    ts_local = ts_local_expr(LOCAL_TZ)
    sess = con.execute(f"""
        SELECT
            COUNT(*)                                              AS total_in_session,
            COUNT(*) FILTER (WHERE action = 'A')                 AS n_add,
            COUNT(*) FILTER (WHERE action = 'C')                 AS n_cancel,
            COUNT(*) FILTER (WHERE action = 'M')                 AS n_modify,
            COUNT(*) FILTER (WHERE action = 'R')                 AS n_clear,
            COUNT(DISTINCT ({ts_local})::DATE)                   AS trading_days
        FROM read_parquet('{glob}', hive_partitioning=true)
        WHERE {session_where_clause(LOCAL_TZ, SESSION_START_LOCAL, SESSION_END_LOCAL)}
    """).fetchdf()
    print(sess.T.to_string())

    # ------------------------------------------------------------------
    # Sanity checks
    # ------------------------------------------------------------------
    print("\n[Diag 3] Sanity checks")
    sanity = con.execute(f"""
        WITH in_session AS (
            SELECT * FROM read_parquet('{glob}', hive_partitioning=true)
            WHERE {session_where_clause(LOCAL_TZ, SESSION_START_LOCAL, SESSION_END_LOCAL)}
        ),
        -- Bootstrap median on outright prices only (floor filters spread legs)
        price_stats AS (
            SELECT APPROX_QUANTILE(price, 0.5) AS price_median
            FROM in_session
            WHERE action IN ('A', 'M')
              AND price > {floor_fp}::BIGINT
              AND price != {INT64_MAX}
        )
        SELECT
            SUM(CASE WHEN price = {INT64_MAX}
                 THEN 1 ELSE 0 END)                          AS price_sentinel,
            SUM(CASE WHEN price <= 0
                      AND price != {INT64_MAX}
                      AND action IN ('A','M')
                 THEN 1 ELSE 0 END)                          AS price_nonpositive,
            SUM(CASE WHEN action IN ('A','M')
                      AND price != {INT64_MAX}
                      AND price > 0
                      AND (price % {tick_fp}) != 0
                 THEN 1 ELSE 0 END)                          AS tick_violations,
            SUM(CASE WHEN action IN ('A','M')
                      AND price != {INT64_MAX}
                      AND price > 0
                      AND ABS(price - p.price_median) > {OUTLIER_BAND} * p.price_median
                 THEN 1 ELSE 0 END)                          AS price_outliers,
            SUM(CASE WHEN size <= 0
                      AND action IN ('A','M')
                 THEN 1 ELSE 0 END)                          AS size_nonpositive,
            SUM(CASE WHEN ts_event > ts_recv
                 THEN 1 ELSE 0 END)                          AS ts_event_after_recv,
            SUM(CASE WHEN ABS(CAST(ts_recv AS BIGINT) - CAST(ts_event AS BIGINT))
                          > 1_000_000_000
                 THEN 1 ELSE 0 END)                          AS ts_delta_over_1s,
            COUNT(*)                                         AS total_checked
        FROM in_session
        CROSS JOIN price_stats p
    """).fetchdf()

    total = sanity["total_checked"].iloc[0]
    checks = [
        "price_sentinel", "price_nonpositive", "tick_violations",
        "price_outliers", "size_nonpositive", "ts_event_after_recv", "ts_delta_over_1s"
    ]
    print(f"  {'Check':<28} {'Count':>10} {'Rate':>8}")
    print(f"  {'-'*50}")
    for c in checks:
        count = int(sanity[c].iloc[0])
        rate  = count / total * 100 if total > 0 else 0
        print(f"  {c:<28} {count:>10,} {rate:>7.4f}%")
    print(f"  Total in session: {int(total):,}")

    # ------------------------------------------------------------------
    # Flags distribution (top 15 combinations)
    # ------------------------------------------------------------------
    print("\n[Diag 4] Flags distribution (top 15)")
    flags = con.execute(f"""
        SELECT
            flags,
            (flags & 128) > 0   AS f_last,
            (flags & 64)  > 0   AS f_tob,
            (flags & 32)  > 0   AS f_snapshot,
            (flags & 8)   > 0   AS f_bad_ts_recv,
            (flags & 4)   > 0   AS f_maybe_bad_book,
            action,
            COUNT(*)            AS n_records
        FROM read_parquet('{glob}', hive_partitioning=true)
        WHERE {session_where_clause(LOCAL_TZ, SESSION_START_LOCAL, SESSION_END_LOCAL)}
        GROUP BY flags, f_last, f_tob, f_snapshot, f_bad_ts_recv, f_maybe_bad_book, action
        ORDER BY n_records DESC
        LIMIT 15
    """).fetchdf()
    print(flags.to_string())

    # ------------------------------------------------------------------
    # F_BAD_TS_RECV in-session audit
    # ------------------------------------------------------------------
    print("\n[Diag 5] F_BAD_TS_RECV in-session (flags & 8 > 0)")
    bad_ts = con.execute(f"""
        SELECT
            action,
            flags,
            price / 1e9     AS price_pts,
            COUNT(*)        AS n_records
        FROM read_parquet('{glob}', hive_partitioning=true)
        WHERE {session_where_clause(LOCAL_TZ, SESSION_START_LOCAL, SESSION_END_LOCAL)}
          AND (flags & 8) > 0
        GROUP BY action, flags, price_pts
        ORDER BY n_records DESC
    """).fetchdf()
    if bad_ts.empty:
        print("  None found in session window.")
    else:
        print(bad_ts.to_string())


# ---------------------------------------------------------------------------
# FRONT MONTH MAP
# ---------------------------------------------------------------------------

def build_front_month_map(
    con: duckdb.DuckDBPyConnection,
    glob: str,
    from_date: str | None,
    to_date: str | None,
) -> dict:
    """
    Build a {trade_date -> front_month_symbol} map.
    Front month = most frequent symbol by event count on each day.
    Works automatically through roll dates: the incoming contract becomes
    the most frequent symbol on roll day and remains so until next roll.

    Date filtering is applied if from_date / to_date are provided.
    """
    date_filter = ""
    if from_date:
        date_filter += f" AND {ts_local_expr(LOCAL_TZ)}::DATE >= '{from_date[:4]}-{from_date[4:6]}-{from_date[6:]}'"
    if to_date:
        date_filter += f" AND {ts_local_expr(LOCAL_TZ)}::DATE <= '{to_date[:4]}-{to_date[4:6]}-{to_date[6:]}'"

    result = con.execute(f"""
        SELECT
            ({ts_local_expr(LOCAL_TZ)})::DATE  AS trade_date,
            MODE(symbol)                        AS front_month_symbol
        FROM read_parquet('{glob}', hive_partitioning=true)
        WHERE {session_where_clause(LOCAL_TZ, SESSION_START_LOCAL, SESSION_END_LOCAL)}
          {date_filter}
        GROUP BY trade_date
        ORDER BY trade_date
    """).fetchdf()

    return dict(zip(result["trade_date"], result["front_month_symbol"]))


# ---------------------------------------------------------------------------
# TRADING DAYS DISCOVERY
# ---------------------------------------------------------------------------

def get_trading_days(
    con: duckdb.DuckDBPyConnection,
    glob: str,
    from_date: str | None,
    to_date: str | None,
) -> list[dict]:
    """
    Discover distinct trading days present in raw data after session filter.
    Returns list of dicts with keys: trade_date, year, month, date_str.
    """
    date_filter = ""
    if from_date:
        date_filter += f" AND {ts_local_expr(LOCAL_TZ)}::DATE >= '{from_date[:4]}-{from_date[4:6]}-{from_date[6:]}'"
    if to_date:
        date_filter += f" AND {ts_local_expr(LOCAL_TZ)}::DATE <= '{to_date[:4]}-{to_date[4:6]}-{to_date[6:]}'"

    rows = con.execute(f"""
        SELECT DISTINCT
            ({ts_local_expr(LOCAL_TZ)})::DATE                   AS trade_date,
            YEAR({ts_local_expr(LOCAL_TZ)})                     AS year,
            MONTH({ts_local_expr(LOCAL_TZ)})                    AS month
        FROM read_parquet('{glob}', hive_partitioning=true)
        WHERE {session_where_clause(LOCAL_TZ, SESSION_START_LOCAL, SESSION_END_LOCAL)}
          {date_filter}
        ORDER BY trade_date
    """).fetchdf()

    return [
        {
            "trade_date": row["trade_date"],
            "year":       int(row["year"]),
            "month":      int(row["month"]),
            "date_str":   row["trade_date"].strftime("%Y%m%d"),
        }
        for _, row in rows.iterrows()
    ]


# ---------------------------------------------------------------------------
# WRITE ONE DAY
# ---------------------------------------------------------------------------

def write_day_clean(
    con:          duckdb.DuckDBPyConnection,
    glob:         str,
    product:      str,
    date_info:    dict,
    front_month:  str,
    output_root:  Path,
    cfg:          dict,
    dry_run:      bool,
) -> dict:
    """
    Apply all cleaning rules for a single trading day and write output Parquet.
    Returns a stats dict: date, n_clean, n_excluded, pct_excluded, skipped, error.
    """
    trade_date = date_info["trade_date"]
    year       = date_info["year"]
    month      = date_info["month"]
    date_str   = date_info["date_str"]
    floor_fp   = cfg["price_floor_fp"]

    out_dir  = output_root / f"year={year}" / f"month={month:02d}"
    out_path = out_dir / f"{product}_{date_str}_orders_clean.parquet"

    if out_path.exists() and not dry_run:
        return {"date": date_str, "skipped": True}

    # Count raw events in session for this day (for exclusion rate reporting)
    n_raw = con.execute(f"""
        SELECT COUNT(*) AS n
        FROM read_parquet('{glob}', hive_partitioning=true)
        WHERE {session_where_clause(
            LOCAL_TZ, SESSION_START_LOCAL, SESSION_END_LOCAL,
            f"AND {ts_local_expr(LOCAL_TZ)}::DATE = '{trade_date}'"
        )}
    """).fetchone()[0]

    # Full cleaning query for this day:
    # CTE price_stats bootstraps daily median on outright prices only.
    # All five rules applied in WHERE of 'clean' CTE.
    result = con.execute(f"""
        WITH in_session AS (
            SELECT *
            FROM read_parquet('{glob}', hive_partitioning=true)
            WHERE {session_where_clause(
                LOCAL_TZ, SESSION_START_LOCAL, SESSION_END_LOCAL,
                f"AND {ts_local_expr(LOCAL_TZ)}::DATE = '{trade_date}'"
            )}
        ),
        price_stats AS (
            -- Bootstrap median on outright prices (floor excludes spread leg differentials)
            SELECT APPROX_QUANTILE(price, 0.5) AS price_median
            FROM in_session
            WHERE action IN ('A', 'M')
              AND price > {floor_fp}::BIGINT
              AND price != {INT64_MAX}
        ),
        clean AS (
            SELECT
                ts_recv, ts_event, ts_in_delta, sequence, order_id,
                price, size, action, side, flags, channel_id, symbol
            FROM in_session, price_stats
            WHERE
                -- Rule 2: exclude sentinel prices (market/stop without limit)
                price != {INT64_MAX}
                -- Rule 3: exclude negative-price implied spread legs on A/M/C
                AND NOT (price <= 0 AND action IN ('A', 'M', 'C'))
                -- Rule 4: exclude positive-price implied spread legs (calendar differentials)
                -- 10% band — no Eurex equity index future moves 10% intraday
                AND NOT (
                    action IN ('A', 'M')
                    AND price > 0
                    AND ABS(price - price_median) > {OUTLIER_BAND} * price_median
                )
                -- Rule 5: front month only — prevents multi-expiry LOB corruption
                AND symbol = '{front_month}'
        )
        SELECT * FROM clean
        ORDER BY ts_recv  -- monotonic sort required by LOB state machine
    """)

    arrow_table = result.fetch_arrow_table()
    n_clean     = arrow_table.num_rows
    n_excluded  = n_raw - n_clean
    pct_excl    = round(100.0 * n_excluded / n_raw, 4) if n_raw > 0 else 0.0

    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            arrow_table,
            str(out_path),
            compression="snappy",
            row_group_size=500_000,  # tuned for sequential tick-by-tick LOB scan
        )

    tag = "[DRY]" if dry_run else "[OK]"
    print(
        f"  {tag} {date_str} ({front_month}) — "
        f"{n_clean:,} clean / {n_excluded:,} excluded ({pct_excl}%) "
        f"→ {out_path.name}"
    )

    return {
        "date":        date_str,
        "n_clean":     n_clean,
        "n_excluded":  n_excluded,
        "pct_excluded": pct_excl,
        "skipped":     False,
    }


# ---------------------------------------------------------------------------
# VALIDATION: post-write raw vs clean comparison
# ---------------------------------------------------------------------------

def validate_output(
    con:         duckdb.DuckDBPyConnection,
    glob_raw:    str,
    output_root: Path,
    product:     str,
):
    """
    Cross-check total raw (session-filtered) vs total clean event counts.
    Prints a one-line summary table.
    """
    glob_clean = str(output_root / "**" / f"*_orders_clean.parquet")

    result = con.execute(f"""
        WITH raw AS (
            SELECT COUNT(*) AS n_raw
            FROM read_parquet('{glob_raw}', hive_partitioning=true)
            WHERE {session_where_clause(LOCAL_TZ, SESSION_START_LOCAL, SESSION_END_LOCAL)}
        ),
        clean AS (
            SELECT COUNT(*) AS n_clean
            FROM read_parquet('{glob_clean}', hive_partitioning=false)
        )
        SELECT
            raw.n_raw,
            clean.n_clean,
            raw.n_raw - clean.n_clean                                    AS n_excluded,
            ROUND(100.0 * (raw.n_raw - clean.n_clean) / raw.n_raw, 4)   AS pct_excluded
        FROM raw, clean
    """).fetchdf()

    print(f"\n[Validation] {product} — raw vs clean totals")
    print(result.T.to_string())


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    args    = parse_args()
    product = args.product
    cfg     = PRODUCT_CONFIG[product]

    data_root   = Path(args.data_root)   / f"product={product}"
    output_root = Path(args.output_root) / f"product={product}"

    if not data_root.exists():
        raise FileNotFoundError(f"Raw data directory not found: {data_root}")

    glob = str(data_root / "**" / "*_orders.parquet")

    # Verify files exist before doing anything
    order_files = sorted(data_root.rglob("*_orders.parquet"))
    if not order_files:
        raise FileNotFoundError(f"No order files found under {data_root}")

    print(f"\n{'='*60}")
    print(f"  clean_eurex_orders.py")
    print(f"  Product   : {product}  ({cfg['description']})")
    print(f"  Tick size : {cfg['tick_size_fp'] / 1e9:.1f} pt")
    print(f"  Files     : {len(order_files)} raw order files")
    print(f"  Dry run   : {args.dry_run}")
    if args.from_date:
        print(f"  From date : {args.from_date}")
    if args.to_date:
        print(f"  To date   : {args.to_date}")
    print(f"{'='*60}")

    con = duckdb.connect()

    # Diagnostics pass — always runs, regardless of dry_run
    run_diagnostics(con, glob, cfg, product)

    # Build front month map for date range
    front_month_map = build_front_month_map(con, glob, args.from_date, args.to_date)
    print(f"\n[Front months] {front_month_map}")

    # Discover trading days
    trading_days = get_trading_days(con, glob, args.from_date, args.to_date)
    print(f"\n[Processing] {len(trading_days)} trading days")

    all_stats = []
    total_clean    = 0
    total_excluded = 0

    for day in trading_days:
        date_str    = day["date_str"]
        front_month = front_month_map.get(day["trade_date"])

        if front_month is None:
            print(f"  [SKIP] {date_str} — no front month found")
            all_stats.append({"date": date_str, "skipped": True})
            continue

        try:
            stats = write_day_clean(
                con, glob, product, day, front_month, output_root, cfg, args.dry_run
            )
            all_stats.append(stats)
            if not stats.get("skipped"):
                total_clean    += stats["n_clean"]
                total_excluded += stats["n_excluded"]
        except Exception as e:
            print(f"  [ERROR] {date_str}: {e}")
            all_stats.append({"date": date_str, "error": str(e)})

    # Summary
    print(f"\n{'='*60}")
    print(f"  SUMMARY — {product}")
    print(f"{'='*60}")
    n_ok      = sum(1 for s in all_stats if not s.get("skipped") and "error" not in s)
    n_skipped = sum(1 for s in all_stats if s.get("skipped"))
    n_errors  = sum(1 for s in all_stats if "error" in s)
    print(f"  Days processed : {n_ok}")
    print(f"  Days skipped   : {n_skipped}")
    print(f"  Errors         : {n_errors}")
    if total_clean + total_excluded > 0:
        pct = round(100.0 * total_excluded / (total_clean + total_excluded), 4)
        print(f"  Total clean    : {total_clean:,}")
        print(f"  Total excluded : {total_excluded:,} ({pct}%)")

    # Post-write validation (skip in dry-run — no output files exist)
    if not args.dry_run and n_ok > 0:
        validate_output(con, glob, output_root, product)


if __name__ == "__main__":
    main()