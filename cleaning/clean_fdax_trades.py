# cleaning/clean_fdax_trades.py
#
# Phase 2 - Day 1: FDAX trades sanity checks & clean Parquet writer
# Mirror of clean_fdax.py applied to trades files (actions T and F only)
#
# Eurex session: 08:00 - 22:00 CET (Mon-Fri)
# Tick size: 0.5 index point = 500_000_000 in fixed-point int64 (1e-9 scale)
# Price encoding: raw int64 fixedpoint 1e-9 (e.g. 22799_000_000_000 = 22799.0)
#
# Filtering rules applied (Step T7):
#   1. Session filter: Eurex core session 08:00-22:00 CET, weekdays only
#   2. Implied spread trades: exclude price <= 0 (same logic as orders)
#   3. Back month contracts: front month filter (most frequent symbol per day)
#      Must be consistent with orders_clean — same front_month per day.
#
# T/F pairing notes:
#   Each outright trade generates exactly one T (aggressor) + one F (passive fill).
#   Exception: unpaired T (flags=128, no F counterpart) = block trades / EFP off-book.
#   Unpaired T kept but flagged via is_unpaired_aggressor column.
#   F events drive passive side removal in LOB reconstruction.

import duckdb
import pyarrow.parquet as pq
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

DATA_ROOT   = Path("data/market_data/product=FDAX")
OUTPUT_ROOT = Path("data/clean/product=FDAX")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# FDAX tick size in fixed-point int64 (0.5 point * 1e9 = 500_000_000)
TICK_SIZE_FP = 500_000_000

# Price floor for median bootstrapping (same as orders cleaner)
PRICE_FLOOR_FP = 1_000_000_000_000

SESSION_START_LOCAL = "08:00:00"
SESSION_END_LOCAL   = "22:00:00"
LOCAL_TZ            = "Europe/Paris"

INT64_MAX = 9_223_372_036_854_775_807

# ---------------------------------------------------------------------------
# CONNECT
# ---------------------------------------------------------------------------

con = duckdb.connect()

glob_pattern_trades = str(DATA_ROOT / "**" / "*_trades.parquet")

trade_files = sorted(DATA_ROOT.rglob("*_trades.parquet"))
print(f"[Discovery] Found {len(trade_files)} FDAX trade files")

if not trade_files:
    raise FileNotFoundError(f"No trade files found under {DATA_ROOT}")

# ---------------------------------------------------------------------------
# STEP T1 — RAW STATS
# ---------------------------------------------------------------------------

print("\n[Step T1] Raw stats (no filter)")

raw_stats = con.execute(f"""
    SELECT
        COUNT(*)                                        AS total_events,
        COUNT(DISTINCT action)                          AS distinct_actions,
        MIN(price) / 1e9                                AS price_min_pts,
        MAX(price) / 1e9                                AS price_max_pts,
        MIN(size)                                       AS size_min,
        MAX(size)                                       AS size_max,
        AVG(size)                                       AS size_avg,
        MIN(ts_recv)                                    AS ts_recv_min,
        MAX(ts_recv)                                    AS ts_recv_max,
        SUM(CASE WHEN action = 'T' THEN 1 ELSE 0 END)  AS n_trade,
        SUM(CASE WHEN action = 'F' THEN 1 ELSE 0 END)  AS n_fill
    FROM read_parquet('{glob_pattern_trades}', hive_partitioning=true)
""").fetchdf()

print(raw_stats.T)

# ---------------------------------------------------------------------------
# STEP T2 — SESSION FILTER
# ---------------------------------------------------------------------------

print(f"\n[Step T2] Session filter: {SESSION_START_LOCAL} - {SESSION_END_LOCAL} {LOCAL_TZ}")

session_stats = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            timezone('{LOCAL_TZ}',
                to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
            ) AS ts_local
        FROM read_parquet('{glob_pattern_trades}', hive_partitioning=true)
    )
    SELECT
        COUNT(*)                                            AS total_in_session,
        COUNT(*) FILTER (WHERE action = 'T')               AS n_trade,
        COUNT(*) FILTER (WHERE action = 'F')               AS n_fill,
        -- T and F should be equal for outright trades: one aggressor + one passive fill
        -- Delta > 0 indicates unpaired T records (block trades, EFP off-book)
        COUNT(*) FILTER (WHERE action = 'T') -
        COUNT(*) FILTER (WHERE action = 'F')               AS t_minus_f_delta,
        COUNT(DISTINCT CAST(ts_local AS DATE))              AS trading_days,
        SUM(CASE WHEN action = 'T' THEN size ELSE 0 END)   AS total_volume_contracts,
        AVG(CASE WHEN action = 'T' THEN size ELSE 0 END)   AS avg_trade_size
    FROM ts_converted
    WHERE ISODOW(ts_local) BETWEEN 1 AND 5
      AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
      AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
""").fetchdf()

print(session_stats.T)

# ---------------------------------------------------------------------------
# STEP T3 — SANITY CHECKS
# ---------------------------------------------------------------------------

print("\n[Step T3] Sanity checks")

sanity = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            timezone('{LOCAL_TZ}',
                to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
            ) AS ts_local
        FROM read_parquet('{glob_pattern_trades}', hive_partitioning=true)
    ),
    in_session AS (
        SELECT * FROM ts_converted
        WHERE ISODOW(ts_local) BETWEEN 1 AND 5
          AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
          AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
    ),
    -- Bootstrap median on outright prices only — consistent with orders cleaner
    price_stats AS (
        SELECT APPROX_QUANTILE(price, 0.5) AS price_median
        FROM in_session
        WHERE price > {PRICE_FLOOR_FP}::BIGINT
          AND price != {INT64_MAX}
    )
    SELECT
        -- PRICE CHECKS
        SUM(CASE WHEN price = {INT64_MAX}
             THEN 1 ELSE 0 END)                              AS price_sentinel,

        -- Implied spread trades: negative prices (calendar spread differentials)
        SUM(CASE WHEN price <= 0
                  AND price != {INT64_MAX}
             THEN 1 ELSE 0 END)                              AS price_nonpositive,

        -- Tick size violations
        SUM(CASE WHEN price != {INT64_MAX}
                  AND price > 0
                  AND (price % {TICK_SIZE_FP}) != 0
             THEN 1 ELSE 0 END)                              AS tick_size_violations,

        -- Extreme outliers (>10% from median) — positive spread legs
        SUM(CASE WHEN price != {INT64_MAX}
                  AND price > 0
                  AND ABS(price - p.price_median) > 0.1 * p.price_median
             THEN 1 ELSE 0 END)                              AS price_extreme_outliers,

        -- SIZE CHECKS
        SUM(CASE WHEN size <= 0 THEN 1 ELSE 0 END)          AS size_nonpositive,

        -- T/F PAIRING CHECK
        -- Count distinct ts_recv per action — should match for outright trades
        COUNT(DISTINCT CASE WHEN action = 'T' THEN ts_recv END) -
        COUNT(DISTINCT CASE WHEN action = 'F' THEN ts_recv END) AS unpaired_timestamps,

        -- TIMESTAMP CHECKS
        SUM(CASE WHEN ts_event > ts_recv THEN 1 ELSE 0 END)  AS ts_event_after_recv,

        SUM(CASE WHEN ABS(CAST(ts_recv AS BIGINT) - CAST(ts_event AS BIGINT))
                      > 1_000_000_000
             THEN 1 ELSE 0 END)                              AS ts_delta_over_1s,

        -- FLAGS AUDIT
        SUM(CASE WHEN (flags & 8) > 0 THEN 1 ELSE 0 END)   AS f_bad_ts_recv,
        SUM(CASE WHEN (flags & 4) > 0 THEN 1 ELSE 0 END)   AS f_maybe_bad_book,

        COUNT(*) AS total_checked

    FROM in_session
    CROSS JOIN price_stats p
""").fetchdf()

print(sanity.T)

# ---------------------------------------------------------------------------
# STEP T4 — FLAGS DISTRIBUTION
# ---------------------------------------------------------------------------

print("\n[Step T4] Flags distribution")

flags_dist = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            timezone('{LOCAL_TZ}',
                to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
            ) AS ts_local
        FROM read_parquet('{glob_pattern_trades}', hive_partitioning=true)
    ),
    in_session AS (
        SELECT * FROM ts_converted
        WHERE ISODOW(ts_local) BETWEEN 1 AND 5
          AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
          AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
    )
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
    FROM in_session
    GROUP BY flags, f_last, f_tob, f_bad_ts_recv, f_maybe_bad_book, action
    ORDER BY n_records DESC
""").fetchdf()

print(flags_dist.to_string())

# ---------------------------------------------------------------------------
# STEP T5 — QUALITY REPORT
# ---------------------------------------------------------------------------

print("\n[Step T5] Quality report")

total = sanity["total_checked"].iloc[0]
checks = [
    "price_sentinel",
    "price_nonpositive",
    "tick_size_violations",
    "price_extreme_outliers",
    "size_nonpositive",
    "unpaired_timestamps",
    "ts_event_after_recv",
    "ts_delta_over_1s",
    "f_bad_ts_recv",
    "f_maybe_bad_book",
]

print(f"{'Check':<30} {'Count':>10} {'Rate':>8}")
print("-" * 52)
for check in checks:
    count = sanity[check].iloc[0]
    rate  = count / total * 100 if total > 0 else 0
    print(f"{check:<30} {int(count):>10} {rate:>7.4f}%")

print(f"\nTotal trade events in session: {int(total):,}")

# ---------------------------------------------------------------------------
# STEP T6 — INVESTIGATE anomalies
# ---------------------------------------------------------------------------

print("\n[Step T6a] Unpaired T records (aggressor without passive fill)")

unpaired = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            timezone('{LOCAL_TZ}',
                to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
            ) AS ts_local
        FROM read_parquet('{glob_pattern_trades}', hive_partitioning=true)
    ),
    in_session AS (
        SELECT * FROM ts_converted
        WHERE ISODOW(ts_local) BETWEEN 1 AND 5
          AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
          AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
    )
    SELECT
        action,
        flags,
        price / 1e9                     AS price_pts,
        size,
        ts_local::TIME                  AS time_local,
        ts_local::DATE                  AS date_local,
        DATE_PART('hour', ts_local)     AS hour_local,
        COUNT(*)                        AS n_records
    FROM in_session
    -- flags=128 (F_LAST only) on T: aggressor with no paired F in same event burst
    -- These are likely block trades or EFP (Exchange for Physical) off-book trades
    WHERE flags = 128
      AND action = 'T'
    GROUP BY action, flags, price_pts, size, time_local, date_local, hour_local
    ORDER BY n_records DESC
    LIMIT 20
""").fetchdf()

print(unpaired.to_string())

print("\n[Step T6b] Non-positive prices on trades (implied spread trades)")

neg_prices = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            timezone('{LOCAL_TZ}',
                to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
            ) AS ts_local
        FROM read_parquet('{glob_pattern_trades}', hive_partitioning=true)
    ),
    in_session AS (
        SELECT * FROM ts_converted
        WHERE ISODOW(ts_local) BETWEEN 1 AND 5
          AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
          AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
    )
    SELECT
        action,
        flags,
        (flags & 128) > 0               AS f_last,
        price / 1e9                     AS price_pts,
        size,
        DATE_PART('hour', ts_local)     AS hour_local,
        ts_local::DATE                  AS date_local,
        COUNT(*)                        AS n_records
    FROM in_session
    WHERE price <= 0
    GROUP BY action, flags, f_last, price_pts, size, hour_local, date_local
    ORDER BY n_records DESC
    LIMIT 20
""").fetchdf()

print(neg_prices.to_string())

# ---------------------------------------------------------------------------
# STEP T7 — WRITE CLEAN TRADES PARQUET
# ---------------------------------------------------------------------------
#
# Filtering rules:
#   1. Session filter: 08:00-22:00 CET, weekdays only
#   2. Implied spread trades: exclude price <= 0
#   3. Back month contracts: front month filter (same logic as orders cleaner)
#
# is_unpaired_aggressor column:
#   True when flags=128 AND action=T — aggressor trade with no paired F record.
#   Kept in clean output for volume/aggressor analysis.
#   Cannot be used to drive LOB passive side removal (no order_id to look up).
#
# Output: data/clean/product=FDAX/year=YYYY/month=MM/FDAX_YYYYMMDD_trades_clean.parquet

print("\n[Step T7] Writing clean trades Parquet files")

# Get distinct trading days from trades files
trading_days_trades = con.execute(f"""
    SELECT DISTINCT
        CAST(timezone('{LOCAL_TZ}',
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
        ) AS DATE)                          AS trade_date,
        YEAR(timezone('{LOCAL_TZ}',
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
        ))                                  AS year,
        MONTH(timezone('{LOCAL_TZ}',
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
        ))                                  AS month
    FROM read_parquet('{glob_pattern_trades}', hive_partitioning=true)
    WHERE ISODOW(timezone('{LOCAL_TZ}',
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
        )) BETWEEN 1 AND 5
    ORDER BY trade_date
""").fetchdf()

# Build front month map from trades — most frequent symbol per day = front month.
# Must be consistent with orders cleaner: same expiry per day in both clean files.
front_months_trades = con.execute(f"""
    WITH ts_converted AS (
        SELECT *,
            timezone('{LOCAL_TZ}',
                to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
            ) AS ts_local
        FROM read_parquet('{glob_pattern_trades}', hive_partitioning=true)
    ),
    in_session AS (
        SELECT * FROM ts_converted
        WHERE ISODOW(ts_local) BETWEEN 1 AND 5
          AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
          AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
    )
    SELECT
        CAST(ts_local AS DATE)  AS trade_date,
        MODE(symbol)            AS front_month_symbol
    FROM in_session
    GROUP BY trade_date
    ORDER BY trade_date
""").fetchdf()

front_month_map_trades = dict(
    zip(front_months_trades["trade_date"], front_months_trades["front_month_symbol"])
)
print(f"Front month map (trades): {front_month_map_trades}")

total_clean = 0

for _, row in trading_days_trades.iterrows():
    trade_date  = row["trade_date"]
    year        = int(row["year"])
    month       = int(row["month"])
    date_str    = trade_date.strftime("%Y%m%d")

    out_dir  = OUTPUT_ROOT / f"year={year}" / f"month={month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"FDAX_{date_str}_trades_clean.parquet"

    if out_path.exists():
        print(f"  [SKIP] {date_str} — already exists")
        continue

    front_month = front_month_map_trades.get(trade_date)
    if front_month is None:
        print(f"  [SKIP] {date_str} — no front month found in trades")
        continue

    result = con.execute(f"""
        WITH ts_converted AS (
            SELECT
                *,
                timezone('{LOCAL_TZ}',
                    to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
                ) AS ts_local
            FROM read_parquet('{glob_pattern_trades}', hive_partitioning=true)
        ),
        in_session AS (
            SELECT * FROM ts_converted
            WHERE ISODOW(ts_local) BETWEEN 1 AND 5
              AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
              AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
              AND CAST(ts_local AS DATE) = '{trade_date}'
              -- Rule 2: exclude implied spread trades (negative price differentials)
              AND price > 0
              -- Rule 3: front month only — consistent with orders_clean for same day
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
            -- Unpaired aggressor flag: T with F_LAST but no paired F record
            -- Indicates block trade or EFP off-book execution
            (flags = 128 AND action = 'T') AS is_unpaired_aggressor
        FROM in_session
        ORDER BY ts_recv
    """)

    arrow_table = result.fetch_arrow_table()
    n_clean = arrow_table.num_rows

    pq.write_table(
        arrow_table,
        str(out_path),
        compression="snappy",
        row_group_size=500_000
    )

    total_clean += n_clean
    print(f"  [OK] {date_str} ({front_month}) — {n_clean:,} clean trades → {out_path.name}")

print(f"\n[Step T7] Done. Total clean trades written: {total_clean:,}")

# ---------------------------------------------------------------------------
# STEP T8 — VALIDATION: raw vs clean counts
# ---------------------------------------------------------------------------

print("\n[Step T8] Validation — raw vs clean trades")

glob_clean_trades = str(OUTPUT_ROOT / "**" / "*_trades_clean.parquet")

validation = con.execute(f"""
    WITH raw AS (
        SELECT COUNT(*) AS n_raw
        FROM read_parquet('{glob_pattern_trades}', hive_partitioning=true)
        WHERE ISODOW(timezone('{LOCAL_TZ}',
                to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
              )) BETWEEN 1 AND 5
          AND timezone('{LOCAL_TZ}',
                to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
              )::TIME >= '{SESSION_START_LOCAL}'::TIME
          AND timezone('{LOCAL_TZ}',
                to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
              )::TIME <  '{SESSION_END_LOCAL}'::TIME
    ),
    clean AS (
        SELECT COUNT(*) AS n_clean
        FROM read_parquet('{glob_clean_trades}', hive_partitioning=true)
    )
    SELECT
        raw.n_raw,
        clean.n_clean,
        raw.n_raw - clean.n_clean                                   AS n_excluded,
        ROUND(100.0 * (raw.n_raw - clean.n_clean) / raw.n_raw, 4)  AS pct_excluded
    FROM raw, clean
""").fetchdf()

print(validation.T)