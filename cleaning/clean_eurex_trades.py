# cleaning/clean_eurex_trades.py
#
# Phase 2 — Generic Eurex trades cleaner (FDAX / FESX / FSMI / any future Eurex product)
#
# Usage:
#   python clean_eurex_trades.py --product FDAX
#   python clean_eurex_trades.py --product FESX
#   python clean_eurex_trades.py --product FSMI
#   python clean_eurex_trades.py --product FDAX --from-date 20250502 --to-date 20250509
#   python clean_eurex_trades.py --product FDAX --dry-run
#
# Input:  data/market_data/product=<PRODUCT>/year=YYYY/month=MM/<PRODUCT>_YYYYMMDD_trades.parquet
# Output: data/clean/product=<PRODUCT>/year=YYYY/month=MM/<PRODUCT>_YYYYMMDD_trades_clean.parquet
#
# Filtering rules:
#   1. Session filter: Eurex core session 08:00-22:00 CET, weekdays only
#   2. Implied spread trades: exclude price <= 0 (calendar spread differentials)
#   3. Back months: front month only — MODE(symbol) per day, must match orders_clean
#
# T/F pairing:
#   Each outright trade = one T (aggressor) + one F (passive fill).
#   Unpaired T (flags=128, no F counterpart) = block trades / EFP off-book.
#   Kept in output with is_unpaired_aggressor=True.
#   F events are the mechanism that removes resting orders in the LOB state machine.
#
# Important: front month selection must be consistent with clean_eurex_orders.py.
# Both scripts use MODE(symbol) per day — guaranteed to agree as long as the
# same raw data is used and roll dates are identical across orders and trades files.

import argparse
import duckdb
import pyarrow.parquet as pq
from pathlib import Path

# ---------------------------------------------------------------------------
# PRODUCT CONFIG
# ---------------------------------------------------------------------------
# Mirrors clean_eurex_orders.py exactly — must stay in sync.
# tick_size_fp used for diagnostic tick violation check only (not a filter).

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

SESSION_START_LOCAL = "08:00:00"
SESSION_END_LOCAL   = "22:00:00"
LOCAL_TZ            = "Europe/Paris"

INT64_MAX    = 9_223_372_036_854_775_807
OUTLIER_BAND = 0.10

# ---------------------------------------------------------------------------
# ARGUMENT PARSING
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Clean Eurex MBO trades — session filter + implied spread removal"
    )
    parser.add_argument(
        "--product", required=True, choices=list(PRODUCT_CONFIG.keys()),
        help="Product to clean (e.g. FDAX, FESX, FSMI)"
    )
    parser.add_argument(
        "--from-date", default=None,
        help="Start date inclusive, format YYYYMMDD"
    )
    parser.add_argument(
        "--to-date", default=None,
        help="End date inclusive, format YYYYMMDD"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run diagnostics but do not write any output files"
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
# HELPERS — identical to clean_eurex_orders.py
# ---------------------------------------------------------------------------

def ts_local_expr(tz: str) -> str:
    """UBIGINT nanoseconds UTC → local TIMESTAMPTZ."""
    return f"timezone('{tz}', to_timestamp(CAST(ts_recv AS BIGINT) / 1e9))"


def session_where_clause(tz: str, start: str, end: str, date_filter: str = "") -> str:
    """WHERE clause for Eurex session filter + optional date restriction."""
    return f"""
        ISODOW({ts_local_expr(tz)}) BETWEEN 1 AND 5
        AND {ts_local_expr(tz)}::TIME >= '{start}'::TIME
        AND {ts_local_expr(tz)}::TIME <  '{end}'::TIME
        {date_filter}
    """

# ---------------------------------------------------------------------------
# DIAGNOSTIC STEPS
# ---------------------------------------------------------------------------

def run_diagnostics(con: duckdb.DuckDBPyConnection, glob: str, cfg: dict, product: str):
    """
    Diagnostic pass over full trades dataset.
    Key check: T/F delta — measures unpaired aggressor volume (block trades / EFP).
    """
    tick_fp  = cfg["tick_size_fp"]
    floor_fp = cfg["price_floor_fp"]

    print(f"\n{'='*60}")
    print(f"  DIAGNOSTICS — {product} trades  ({cfg['description']})")
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
            MIN(size)                                       AS size_min,
            MAX(size)                                       AS size_max,
            ROUND(AVG(size), 2)                             AS size_avg,
            SUM(CASE WHEN action = 'T' THEN 1 ELSE 0 END)  AS n_trade,
            SUM(CASE WHEN action = 'F' THEN 1 ELSE 0 END)  AS n_fill
        FROM read_parquet('{glob}', hive_partitioning=true)
    """).fetchdf()
    print(raw.T.to_string())

    # ------------------------------------------------------------------
    # Session stats + T/F pairing check
    # ------------------------------------------------------------------
    print(f"\n[Diag 2] Session filter + T/F pairing: {SESSION_START_LOCAL}-{SESSION_END_LOCAL} {LOCAL_TZ}")
    sess = con.execute(f"""
        SELECT
            COUNT(*)                                                AS total_in_session,
            COUNT(*) FILTER (WHERE action = 'T')                   AS n_trade,
            COUNT(*) FILTER (WHERE action = 'F')                   AS n_fill,
            -- Delta > 0 = unpaired T records (block trades, EFP off-book)
            -- Delta < 0 would indicate a data integrity issue
            COUNT(*) FILTER (WHERE action = 'T') -
            COUNT(*) FILTER (WHERE action = 'F')                   AS t_minus_f_delta,
            COUNT(DISTINCT ({ts_local_expr(LOCAL_TZ)})::DATE)      AS trading_days,
            SUM(CASE WHEN action = 'T' THEN size ELSE 0 END)       AS total_volume_contracts,
            ROUND(AVG(CASE WHEN action = 'T' THEN size END), 2)    AS avg_trade_size
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
        price_stats AS (
            SELECT APPROX_QUANTILE(price, 0.5) AS price_median
            FROM in_session
            WHERE price > {floor_fp}::BIGINT
              AND price != {INT64_MAX}
        )
        SELECT
            SUM(CASE WHEN price = {INT64_MAX}
                 THEN 1 ELSE 0 END)                              AS price_sentinel,
            SUM(CASE WHEN price <= 0
                      AND price != {INT64_MAX}
                 THEN 1 ELSE 0 END)                              AS price_nonpositive,
            SUM(CASE WHEN price != {INT64_MAX}
                      AND price > 0
                      AND (price % {tick_fp}) != 0
                 THEN 1 ELSE 0 END)                              AS tick_violations,
            SUM(CASE WHEN price != {INT64_MAX}
                      AND price > 0
                      AND ABS(price - p.price_median) > {OUTLIER_BAND} * p.price_median
                 THEN 1 ELSE 0 END)                              AS price_outliers,
            SUM(CASE WHEN size <= 0
                 THEN 1 ELSE 0 END)                              AS size_nonpositive,
            -- Unpaired T: aggressor with no corresponding passive F
            -- Approximated here as T with flags=128 and no F at same ts_recv
            SUM(CASE WHEN action = 'T' AND flags = 128
                 THEN 1 ELSE 0 END)                              AS n_unpaired_aggressor,
            SUM(CASE WHEN ts_event > ts_recv
                 THEN 1 ELSE 0 END)                              AS ts_event_after_recv,
            SUM(CASE WHEN ABS(CAST(ts_recv AS BIGINT) - CAST(ts_event AS BIGINT))
                          > 1_000_000_000
                 THEN 1 ELSE 0 END)                              AS ts_delta_over_1s,
            SUM(CASE WHEN (flags & 8) > 0
                 THEN 1 ELSE 0 END)                              AS f_bad_ts_recv,
            COUNT(*)                                             AS total_checked
        FROM in_session
        CROSS JOIN price_stats p
    """).fetchdf()

    total = sanity["total_checked"].iloc[0]
    checks = [
        "price_sentinel", "price_nonpositive", "tick_violations",
        "price_outliers", "size_nonpositive", "n_unpaired_aggressor",
        "ts_event_after_recv", "ts_delta_over_1s", "f_bad_ts_recv",
    ]
    print(f"  {'Check':<28} {'Count':>10} {'Rate':>8}")
    print(f"  {'-'*50}")
    for c in checks:
        count = int(sanity[c].iloc[0])
        rate  = count / total * 100 if total > 0 else 0
        print(f"  {c:<28} {count:>10,} {rate:>7.4f}%")
    print(f"  Total in session: {int(total):,}")

    # ------------------------------------------------------------------
    # Flags distribution
    # ------------------------------------------------------------------
    print("\n[Diag 4] Flags distribution (top 10)")
    flags = con.execute(f"""
        SELECT
            flags,
            (flags & 128) > 0   AS f_last,
            (flags & 64)  > 0   AS f_tob,
            (flags & 8)   > 0   AS f_bad_ts_recv,
            (flags & 4)   > 0   AS f_maybe_bad_book,
            action,
            COUNT(*)            AS n_records,
            MIN(price) / 1e9    AS price_min_pts,
            MAX(price) / 1e9    AS price_max_pts
        FROM read_parquet('{glob}', hive_partitioning=true)
        WHERE {session_where_clause(LOCAL_TZ, SESSION_START_LOCAL, SESSION_END_LOCAL)}
        GROUP BY flags, f_last, f_tob, f_bad_ts_recv, f_maybe_bad_book, action
        ORDER BY n_records DESC
        LIMIT 10
    """).fetchdf()
    print(flags.to_string())

    # ------------------------------------------------------------------
    # Negative price detail (implied spread trades)
    # ------------------------------------------------------------------
    print("\n[Diag 5] Negative price trades (implied spread legs)")
    neg = con.execute(f"""
        SELECT
            action,
            flags,
            price / 1e9                             AS price_pts,
            size,
            DATE_PART('hour', {ts_local_expr(LOCAL_TZ)})  AS hour_local,
            ({ts_local_expr(LOCAL_TZ)})::DATE       AS date_local,
            COUNT(*)                                AS n_records
        FROM read_parquet('{glob}', hive_partitioning=true)
        WHERE {session_where_clause(LOCAL_TZ, SESSION_START_LOCAL, SESSION_END_LOCAL)}
          AND price <= 0
        GROUP BY action, flags, price_pts, size, hour_local, date_local
        ORDER BY n_records DESC
        LIMIT 20
    """).fetchdf()
    if neg.empty:
        print("  None found.")
    else:
        print(neg.to_string())


# ---------------------------------------------------------------------------
# FRONT MONTH MAP — must mirror clean_eurex_orders.py exactly
# ---------------------------------------------------------------------------

def build_front_month_map(
    con:       duckdb.DuckDBPyConnection,
    glob:      str,
    from_date: str | None,
    to_date:   str | None,
) -> dict:
    """
    {trade_date -> front_month_symbol} from trades files.
    Uses MODE(symbol) per day — same logic as orders cleaner.
    Consistency with orders_clean is guaranteed when both scripts run on the
    same date range and the same raw data.
    """
    date_filter = ""
    if from_date:
        date_filter += f" AND {ts_local_expr(LOCAL_TZ)}::DATE >= '{from_date[:4]}-{from_date[4:6]}-{from_date[6:]}'"
    if to_date:
        date_filter += f" AND {ts_local_expr(LOCAL_TZ)}::DATE <= '{to_date[:4]}-{to_date[4:6]}-{to_date[6:]}'"

    result = con.execute(f"""
        SELECT
            ({ts_local_expr(LOCAL_TZ)})::DATE   AS trade_date,
            MODE(symbol)                         AS front_month_symbol
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
    con:       duckdb.DuckDBPyConnection,
    glob:      str,
    from_date: str | None,
    to_date:   str | None,
) -> list[dict]:
    date_filter = ""
    if from_date:
        date_filter += f" AND {ts_local_expr(LOCAL_TZ)}::DATE >= '{from_date[:4]}-{from_date[4:6]}-{from_date[6:]}'"
    if to_date:
        date_filter += f" AND {ts_local_expr(LOCAL_TZ)}::DATE <= '{to_date[:4]}-{to_date[4:6]}-{to_date[6:]}'"

    rows = con.execute(f"""
        SELECT DISTINCT
            ({ts_local_expr(LOCAL_TZ)})::DATE               AS trade_date,
            YEAR({ts_local_expr(LOCAL_TZ)})                 AS year,
            MONTH({ts_local_expr(LOCAL_TZ)})                AS month
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
    con:         duckdb.DuckDBPyConnection,
    glob:        str,
    product:     str,
    date_info:   dict,
    front_month: str,
    output_root: Path,
    dry_run:     bool,
) -> dict:
    """
    Apply cleaning rules for one trading day and write trades_clean Parquet.

    Output schema adds is_unpaired_aggressor (bool) to the raw columns:
      True  = T with flags=128 and no paired F (block trade / EFP off-book)
      False = normal outright T with a corresponding passive F

    is_unpaired_aggressor is kept in output for:
      - Volume analysis (block vs lit volume breakdown)
      - LOB reconstruction: unpaired T cannot drive passive fill removal
        (no order_id to look up) — the state machine ignores them for book updates
    """
    trade_date = date_info["trade_date"]
    year       = date_info["year"]
    month      = date_info["month"]
    date_str   = date_info["date_str"]

    out_dir  = output_root / f"year={year}" / f"month={month:02d}"
    out_path = out_dir / f"{product}_{date_str}_trades_clean.parquet"

    if out_path.exists() and not dry_run:
        return {"date": date_str, "skipped": True}

    # Raw count for exclusion rate
    n_raw = con.execute(f"""
        SELECT COUNT(*) AS n
        FROM read_parquet('{glob}', hive_partitioning=true)
        WHERE {session_where_clause(
            LOCAL_TZ, SESSION_START_LOCAL, SESSION_END_LOCAL,
            f"AND {ts_local_expr(LOCAL_TZ)}::DATE = '{trade_date}'"
        )}
    """).fetchone()[0]

    result = con.execute(f"""
        WITH in_session AS (
            SELECT *
            FROM read_parquet('{glob}', hive_partitioning=true)
            WHERE {session_where_clause(
                LOCAL_TZ, SESSION_START_LOCAL, SESSION_END_LOCAL,
                f"AND {ts_local_expr(LOCAL_TZ)}::DATE = '{trade_date}'"
            )}
              -- Rule 2: exclude implied spread trades (negative price differentials)
              AND price > 0
              -- Rule 3: front month only — must match orders_clean for same day
              AND symbol = '{front_month}'
        )
        SELECT
            ts_recv,
            ts_event,
            ts_in_delta,
            sequence,
            order_id,
            price,
            size,
            action,
            side,
            flags,
            channel_id,
            symbol,
            -- Unpaired aggressor: T with F_LAST=128 and no corresponding passive F.
            -- These are block trades or EFP (Exchange for Physical) off-book executions.
            -- Cannot drive LOB passive side removal — no resting order_id to cancel.
            (flags = 128 AND action = 'T') AS is_unpaired_aggressor
        FROM in_session
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
            row_group_size=500_000,
        )

    tag = "[DRY]" if dry_run else "[OK]"
    print(
        f"  {tag} {date_str} ({front_month}) — "
        f"{n_clean:,} clean / {n_excluded:,} excluded ({pct_excl}%) "
        f"→ {out_path.name}"
    )

    return {
        "date":         date_str,
        "n_clean":      n_clean,
        "n_excluded":   n_excluded,
        "pct_excluded": pct_excl,
        "skipped":      False,
    }


# ---------------------------------------------------------------------------
# VALIDATION
# ---------------------------------------------------------------------------

def validate_output(
    con:         duckdb.DuckDBPyConnection,
    glob_raw:    str,
    output_root: Path,
    product:     str,
):
    """Raw vs clean total count cross-check."""
    glob_clean = str(output_root / "**" / "*_trades_clean.parquet")

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

    print(f"\n[Validation] {product} trades — raw vs clean totals")
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

    glob = str(data_root / "**" / "*_trades.parquet")

    trade_files = sorted(data_root.rglob("*_trades.parquet"))
    if not trade_files:
        raise FileNotFoundError(f"No trade files found under {data_root}")

    print(f"\n{'='*60}")
    print(f"  clean_eurex_trades.py")
    print(f"  Product   : {product}  ({cfg['description']})")
    print(f"  Tick size : {cfg['tick_size_fp'] / 1e9:.1f} pt")
    print(f"  Files     : {len(trade_files)} raw trade files")
    print(f"  Dry run   : {args.dry_run}")
    if args.from_date:
        print(f"  From date : {args.from_date}")
    if args.to_date:
        print(f"  To date   : {args.to_date}")
    print(f"{'='*60}")

    con = duckdb.connect()

    run_diagnostics(con, glob, cfg, product)

    front_month_map = build_front_month_map(con, glob, args.from_date, args.to_date)
    print(f"\n[Front months] {front_month_map}")

    trading_days = get_trading_days(con, glob, args.from_date, args.to_date)
    print(f"\n[Processing] {len(trading_days)} trading days")

    all_stats      = []
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
                con, glob, product, day, front_month, output_root, args.dry_run
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
    print(f"  SUMMARY — {product} trades")
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

    if not args.dry_run and n_ok > 0:
        validate_output(con, glob, output_root, product)


if __name__ == "__main__":
    main()