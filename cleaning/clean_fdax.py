# cleaning/clean_fdax.py
#
# Phase 2 - Day 1: FDAX session reconstruction & sanity checks
# Tool: DuckDB (exploration/validation mode)
# Convention: timestamps in CET/CEST (Europe/Paris) for per-market analysis
#
# Eurex session: 08:00 - 22:00 CET (Mon-Fri)
# Tick size: 0.5 index point = 500_000_000 in fixed-point int64 (1e-9 scale)
# Price encoding: raw int64 fixedpoint 1e-9 (e.g. 22799_000_000_000 = 22799.0)
#
# Filtering rules applied (Step 8):
#   1. Session filter: Eurex core session 08:00-22:00 CET, weekdays only
#   2. Sentinel prices (INT64_MAX): exclude — market/stop orders without limit price
#   3. Implied spread legs negative prices: exclude A/M/C where price <= 0
#      (Eurex synthetic implied-in/implied-out orders on calendar spreads)
#   4. Implied spread legs positive prices: exclude A/M where price deviates
#      more than 10% from daily median (e.g. calendar spread differential ~364.5)
#      Median bootstrapped on price > 1000pts floor to avoid contamination.
#   5. Back month contracts: front month filter (most frequent symbol per day).
#      Prevents multi-contract LOB corruption (bid/ask from different expiries).
#
# No snapshot available for Eurex EOBI — Databento only provides MBO snapshots
# for CME Globex MDP 3.0. LOB reconstruction starts from zero at session open.

import duckdb
import pyarrow.parquet as pq
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIG — adjust paths to your local repo layout
# ---------------------------------------------------------------------------

DATA_ROOT   = Path("data/market_data/product=FDAX")
OUTPUT_ROOT = Path("data/clean/product=FDAX")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# FDAX tick size in fixed-point int64 (0.5 point * 1e9 = 500_000_000)
TICK_SIZE_FP = 500_000_000

# Price floor for median bootstrapping: excludes calendar spread legs
# 1000 points * 1e9 = 1_000_000_000_000 — no outright FDAX order is below 1000pts
PRICE_FLOOR_FP = 1_000_000_000_000

# Eurex core session boundaries in local time (CET = UTC+1, CEST = UTC+2)
# We filter via UTC conversion — DuckDB handles tz-aware casting natively
SESSION_START_LOCAL = "08:00:00"
SESSION_END_LOCAL   = "22:00:00"
LOCAL_TZ            = "Europe/Paris"

# Databento sentinel for "no price" (market/stop orders without limit price)
INT64_MAX = 9_223_372_036_854_775_807

# ---------------------------------------------------------------------------
# CONNECT — in-process DuckDB, no server needed
# ---------------------------------------------------------------------------

con = duckdb.connect()

# ---------------------------------------------------------------------------
# STEP 1 — DISCOVERY: list all FDAX order Parquet files
# ---------------------------------------------------------------------------

# Expected layout: product=FDAX/year=YYYY/month=MM/FDAX_YYYYMMDD_orders.parquet
order_files = sorted(DATA_ROOT.rglob("*_orders.parquet"))
print("\n[Step 1] List all FDAX order Parquet files")
print(f"[Discovery] Found {len(order_files)} FDAX order files")

if not order_files:
    raise FileNotFoundError(f"No order files found under {DATA_ROOT}")

# Build a glob pattern DuckDB can use directly — avoids loading file list into Python
# DuckDB supports ** glob for recursive directory scan
glob_pattern = str(DATA_ROOT / "**" / "*_orders.parquet")

# ---------------------------------------------------------------------------
# STEP 2 — RAW STATS: baseline before any filtering
# ---------------------------------------------------------------------------

print("\n[Step 2] Raw stats (no filter applied)")

raw_stats = con.execute(f"""
    SELECT
        COUNT(*)                                        AS total_events,
        COUNT(DISTINCT action)                          AS distinct_actions,
        -- Price range in fixed-point int64
        MIN(price)                                      AS price_min_fp,
        MAX(price)                                      AS price_max_fp,
        -- Convert to float points for readability (divide by 1e9)
        MIN(price) / 1e9                                AS price_min_pts,
        MAX(price) / 1e9                                AS price_max_pts,
        -- Timestamp range (raw uint64 nanoseconds UTC)
        MIN(ts_recv)                                    AS ts_recv_min,
        MAX(ts_recv)                                    AS ts_recv_max,
        -- Count by action type
        SUM(CASE WHEN action = 'A' THEN 1 ELSE 0 END)  AS n_add,
        SUM(CASE WHEN action = 'C' THEN 1 ELSE 0 END)  AS n_cancel,
        SUM(CASE WHEN action = 'M' THEN 1 ELSE 0 END)  AS n_modify,
        SUM(CASE WHEN action = 'R' THEN 1 ELSE 0 END)  AS n_clear,
        SUM(CASE WHEN action = 'T' THEN 1 ELSE 0 END)  AS n_trade,
        SUM(CASE WHEN action = 'F' THEN 1 ELSE 0 END)  AS n_fill
    FROM read_parquet('{glob_pattern}', hive_partitioning=true)
""").fetchdf()

print(raw_stats.T)

# ---------------------------------------------------------------------------
# STEP 3 — SESSION FILTER: keep only Eurex core session (08:00-22:00 CET)
# ---------------------------------------------------------------------------

print(f"\n[Step 3] Applying session filter: {SESSION_START_LOCAL} - {SESSION_END_LOCAL} {LOCAL_TZ}")

# DuckDB timestamp conversion pattern used throughout this file:
#   ts_recv is stored as UBIGINT nanoseconds UTC
#   CAST(ts_recv AS BIGINT) / 1e9  → seconds as DOUBLE
#   to_timestamp(...)              → TIMESTAMP (UTC)
#   timezone(LOCAL_TZ, ...)        → TIMESTAMPTZ in local time
#   ::TIME                         → TIME component for session filter
# Note: TIME() function does not exist in DuckDB — always use ::TIME cast

session_filtered = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)                          AS ts_utc,
            timezone('{LOCAL_TZ}', to_timestamp(CAST(ts_recv AS BIGINT) / 1e9))  AS ts_local
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
    )
    SELECT
        COUNT(*)                                              AS total_in_session,
        COUNT(*) FILTER (WHERE action = 'A')                 AS n_add,
        COUNT(*) FILTER (WHERE action = 'C')                 AS n_cancel,
        COUNT(*) FILTER (WHERE action = 'M')                 AS n_modify,
        COUNT(*) FILTER (WHERE action = 'R')                 AS n_clear,
        COUNT(DISTINCT DATE_TRUNC('day', ts_local))          AS trading_days
    FROM ts_converted
    WHERE
        -- Weekdays only: ISODOW returns 1=Mon ... 7=Sun (safer than DAYOFWEEK)
        ISODOW(ts_local) BETWEEN 1 AND 5
        -- Session hours: ::TIME cast for comparison
        AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
        AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
""").fetchdf()

print(session_filtered.T)

# ---------------------------------------------------------------------------
# STEP 4 — SANITY CHECKS: price, size, timestamp anomalies
# ---------------------------------------------------------------------------

print("\n[Step 4] Sanity checks")

sanity = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)                          AS ts_utc,
            timezone('{LOCAL_TZ}', to_timestamp(CAST(ts_recv AS BIGINT) / 1e9))  AS ts_local
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
    ),
    in_session AS (
        SELECT * FROM ts_converted
        WHERE ISODOW(ts_local) BETWEEN 1 AND 5
          AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
          AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
    ),
    -- Bootstrap median on outright prices only — floor at 1000pts excludes
    -- calendar spread legs (positive differentials like 364.5) from contaminating
    -- the median used in Rule 4 of the cleaning step
    price_stats AS (
        SELECT APPROX_QUANTILE(price, 0.5) AS price_median
        FROM in_session
        WHERE action IN ('A', 'M')
          AND price > {PRICE_FLOOR_FP}::BIGINT
          AND price != {INT64_MAX}
    )
    SELECT
        -- PRICE CHECKS
        -- Sentinel INT64_MAX: market/stop orders without limit price
        SUM(CASE WHEN price = {INT64_MAX} THEN 1 ELSE 0 END)
            AS price_sentinel,

        -- Non-positive prices on Add/Modify: implied calendar spread legs (negative differentials)
        SUM(CASE WHEN price <= 0
                  AND price != {INT64_MAX}
                  AND action IN ('A','M')
             THEN 1 ELSE 0 END)
            AS price_nonpositive,

        -- Tick size violations: FDAX tick = 0.5pt, all prices must be multiples
        SUM(CASE WHEN action IN ('A','M')
                  AND price != {INT64_MAX}
                  AND price > 0
                  AND (price % {TICK_SIZE_FP}) != 0
             THEN 1 ELSE 0 END)
            AS tick_size_violations,

        -- Extreme outliers: >10% from daily median — catches positive spread legs (e.g. 364.5)
        -- Cross-join with price_stats CTE to access median scalar
        SUM(CASE WHEN action IN ('A','M')
                  AND price != {INT64_MAX}
                  AND price > 0
                  AND ABS(price - p.price_median) > 0.1 * p.price_median
             THEN 1 ELSE 0 END)
            AS price_extreme_outliers,

        -- SIZE CHECKS
        SUM(CASE WHEN size <= 0 AND action IN ('A','M','T') THEN 1 ELSE 0 END)
            AS size_nonpositive,

        -- TIMESTAMP CHECKS
        -- ts_event > ts_recv: exchange timestamp after capture timestamp (clock skew)
        SUM(CASE WHEN ts_event > ts_recv THEN 1 ELSE 0 END)
            AS ts_event_after_recv,

        -- Extreme latency: |ts_recv - ts_event| > 1 second (network anomaly)
        SUM(CASE WHEN ABS(CAST(ts_recv AS BIGINT) - CAST(ts_event AS BIGINT))
                      > 1_000_000_000
             THEN 1 ELSE 0 END)
            AS ts_delta_over_1s,

        COUNT(*) AS total_checked

    FROM in_session
    CROSS JOIN price_stats p
""").fetchdf()

print(sanity.T)

# ---------------------------------------------------------------------------
# STEP 5 — QUALITY REPORT: summary with rejection rates
# ---------------------------------------------------------------------------

print("\n[Step 5] Quality report")

total = sanity["total_checked"].iloc[0]
checks = [
    "price_sentinel",
    "price_nonpositive",
    "tick_size_violations",
    "price_extreme_outliers",
    "size_nonpositive",
    "ts_event_after_recv",
    "ts_delta_over_1s",
]

print(f"{'Check':<30} {'Count':>10} {'Rate':>8}")
print("-" * 52)
for check in checks:
    count = sanity[check].iloc[0]
    rate  = count / total * 100 if total > 0 else 0
    print(f"{check:<30} {int(count):>10} {rate:>7.4f}%")

print(f"\nTotal events in session: {int(total):,}")

# ---------------------------------------------------------------------------
# STEP 6 — INVESTIGATE: non-positive price records
# ---------------------------------------------------------------------------

print("\n[Step 6] Investigating non-positive prices (implied spread legs)")

investigation = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)                          AS ts_utc,
            timezone('{LOCAL_TZ}', to_timestamp(CAST(ts_recv AS BIGINT) / 1e9))  AS ts_local
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
    ),
    in_session AS (
        SELECT * FROM ts_converted
        WHERE ISODOW(ts_local) BETWEEN 1 AND 5
          AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
          AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
    )
    SELECT
        action,
        side,
        flags,
        price,
        price / 1e9                         AS price_pts,
        size,
        price % 1000000000                  AS price_fp_remainder,
        ts_local::DATE                      AS trade_date,
        COUNT(*)                            AS n_records
    FROM in_session
    WHERE price <= 0
      AND price != {INT64_MAX}
    GROUP BY action, side, flags, price, size, ts_local::DATE
    ORDER BY n_records DESC
    LIMIT 50
""").fetchdf()

print(investigation.to_string())

print("\n[Step 6b] Time distribution of non-positive prices (by hour)")

time_dist = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)                          AS ts_utc,
            timezone('{LOCAL_TZ}', to_timestamp(CAST(ts_recv AS BIGINT) / 1e9))  AS ts_local
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
    ),
    in_session AS (
        SELECT * FROM ts_converted
        WHERE ISODOW(ts_local) BETWEEN 1 AND 5
          AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
          AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
    )
    SELECT
        DATE_PART('hour', ts_local)         AS hour_local,
        COUNT(*)                            AS n_nonpositive
    FROM in_session
    WHERE price <= 0
      AND price != {INT64_MAX}
    GROUP BY hour_local
    ORDER BY hour_local
""").fetchdf()

print(time_dist.to_string())

# ---------------------------------------------------------------------------
# STEP 7 — FLAGS AUDIT: distribution of all flag values in session
# ---------------------------------------------------------------------------

print("\n[Step 7] Flags distribution — full dataset in session")

flags_dist = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)                          AS ts_utc,
            timezone('{LOCAL_TZ}', to_timestamp(CAST(ts_recv AS BIGINT) / 1e9))  AS ts_local
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
    ),
    in_session AS (
        SELECT * FROM ts_converted
        WHERE ISODOW(ts_local) BETWEEN 1 AND 5
          AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
          AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
    )
    SELECT
        flags,
        -- Databento MBO flags (bitfield uint8):
        -- bit 7 (128) = F_LAST      last message in multi-message event
        -- bit 6 (64)  = F_TOB       top-of-book change
        -- bit 5 (32)  = F_SNAPSHOT  part of book snapshot (CME only, not Eurex)
        -- bit 4 (16)  = F_MBP       MBP-derived synthetic event
        -- bit 3 (8)   = F_BAD_TS_RECV  ts_recv is unreliable (set to generation time)
        -- bit 2 (4)   = F_MAYBE_BAD_BOOK  book state may be invalid
        (flags & 128) > 0           AS f_last,
        (flags & 64)  > 0           AS f_tob,
        (flags & 32)  > 0           AS f_snapshot,
        (flags & 16)  > 0           AS f_mbp,
        (flags & 8)   > 0           AS f_bad_ts_recv,
        (flags & 4)   > 0           AS f_maybe_bad_book,
        action,
        COUNT(*)                    AS n_records,
        MIN(price) / 1e9            AS price_min_pts,
        MAX(price) / 1e9            AS price_max_pts,
        AVG(price) / 1e9            AS price_avg_pts
    FROM in_session
    GROUP BY flags, f_last, f_tob, f_snapshot, f_mbp, f_bad_ts_recv, f_maybe_bad_book, action
    ORDER BY n_records DESC
""").fetchdf()

print(flags_dist.to_string())

# ---------------------------------------------------------------------------
# STEP 8 — WRITE CLEAN PARQUET: apply all filters and write clean events
# ---------------------------------------------------------------------------
#
# Filtering rules:
#   1. Session filter: Eurex core session 08:00-22:00 CET, weekdays only
#   2. Sentinel prices (INT64_MAX): exclude — market/stop orders without limit price
#   3. Implied spread legs negative prices: exclude A/M/C where price <= 0
#      (Eurex synthetic implied-in/implied-out orders on calendar spreads)
#   4. Implied spread legs positive prices: exclude A/M where price deviates
#      more than 10% from daily median (e.g. calendar spread differential ~364.5)
#      Median bootstrapped on price > 1000pts floor to avoid contamination.
#   5. Back month contracts: front month filter (most frequent symbol per day).
#      Prevents multi-contract LOB corruption (bid/ask from different expiries).
#
# Records kept:
#   - All legitimate Add/Cancel/Modify/cleaR with valid outright prices
#   - flags=0 Cancels (legitimate cancels without F_LAST — intermediate in burst)
#   - cleaR (R) events: book reset signals, required by LOB state machine
#
# Output convention (Hive-style partitioning):
#   data/clean/product=FDAX/year=YYYY/month=MM/FDAX_YYYYMMDD_orders_clean.parquet

print("\n[Step 8] Writing clean Parquet files")

# Get list of distinct trading days from raw data (local timezone date for naming)
trading_days = con.execute(f"""
    SELECT DISTINCT
        CAST(timezone('{LOCAL_TZ}',
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
        ) AS DATE)                              AS trade_date,
        YEAR(timezone('{LOCAL_TZ}',
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
        ))                                      AS year,
        MONTH(timezone('{LOCAL_TZ}',
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
        ))                                      AS month
    FROM read_parquet('{glob_pattern}', hive_partitioning=true)
    WHERE ISODOW(timezone('{LOCAL_TZ}',
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
        )) BETWEEN 1 AND 5
    ORDER BY trade_date
""").fetchdf()

print(f"Trading days to process: {len(trading_days)}")

# Build front month map: most frequent symbol per day = front month by definition.
# FDAX has quarterly expiries (Mar/Jun/Sep/Dec). Multiple back months appear in
# the same raw file. We keep only the front month to ensure a single consistent
# LOB per trading day. Roll is handled automatically — day of roll, the new front
# month becomes the most frequent symbol.
front_months = con.execute(f"""
    WITH ts_converted AS (
        SELECT *,
            timezone('{LOCAL_TZ}',
                to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
            ) AS ts_local
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
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

front_month_map = dict(
    zip(front_months["trade_date"], front_months["front_month_symbol"])
)
print(f"Front month map: {front_month_map}")

total_clean = 0

for _, row in trading_days.iterrows():
    trade_date  = row["trade_date"]
    year        = int(row["year"])
    month       = int(row["month"])
    date_str    = trade_date.strftime("%Y%m%d")

    out_dir  = OUTPUT_ROOT / f"year={year}" / f"month={month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"FDAX_{date_str}_orders_clean.parquet"

    # Resume capability — skip already written days
    if out_path.exists():
        print(f"  [SKIP] {date_str} — already exists")
        continue

    front_month = front_month_map.get(trade_date)
    if front_month is None:
        print(f"  [SKIP] {date_str} — no front month found")
        continue

    result = con.execute(f"""
        WITH ts_converted AS (
            SELECT
                *,
                timezone('{LOCAL_TZ}',
                    to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
                ) AS ts_local
            FROM read_parquet('{glob_pattern}', hive_partitioning=true)
        ),
        in_session AS (
            SELECT * FROM ts_converted
            WHERE ISODOW(ts_local) BETWEEN 1 AND 5
              AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
              AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
              AND CAST(ts_local AS DATE) = '{trade_date}'
        ),
        -- Bootstrap median on outright prices only (floor 1000pts excludes spread legs).
        -- Used by Rule 4 to filter positive-price implied calendar spread legs.
        price_stats AS (
            SELECT APPROX_QUANTILE(price, 0.5) AS price_median
            FROM in_session
            WHERE action IN ('A', 'M')
              AND price > {PRICE_FLOOR_FP}::BIGINT
              AND price != {INT64_MAX}
        ),
        clean AS (
            SELECT
                ts_recv, ts_event, ts_in_delta, sequence, order_id,
                price, size, action, side, flags, channel_id, symbol
            FROM in_session, price_stats
            WHERE
                -- Rule 2: exclude sentinel prices (market/stop orders without limit)
                price != {INT64_MAX}
                -- Rule 3: exclude implied spread legs with negative prices
                AND NOT (price <= 0 AND action IN ('A', 'M', 'C'))
                -- Rule 4: exclude implied spread legs with positive but unrealistic prices
                -- 10% band is conservative — FDAX never moves 2294 points intraday
                AND NOT (
                    action IN ('A', 'M')
                    AND price > 0
                    AND ABS(price - price_median) > 0.10 * price_median
                )
                -- Rule 5: front month only — excludes back months and calendar spreads
                AND symbol = '{front_month}'
        )
        SELECT * FROM clean
        ORDER BY ts_recv  -- monotonic sort key required for LOB reconstruction
    """)

    arrow_table = result.fetch_arrow_table()
    n_clean = arrow_table.num_rows

    pq.write_table(
        arrow_table,
        str(out_path),
        compression="snappy",
        row_group_size=500_000  # suitable for sequential tick-by-tick LOB scan
    )

    total_clean += n_clean
    print(f"  [OK] {date_str} ({front_month}) — {n_clean:,} clean events → {out_path.name}")

print(f"\n[Step 8] Done. Total clean events written: {total_clean:,}")

# ---------------------------------------------------------------------------
# STEP 9 — VALIDATION: compare raw vs clean event counts
# ---------------------------------------------------------------------------

print("\n[Step 9] Validation — raw vs clean")

validation = con.execute(f"""
    WITH raw AS (
        -- Raw count: session filter only, no cleaning rules
        SELECT COUNT(*) AS n_raw
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
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
        FROM read_parquet(
            '{str(OUTPUT_ROOT / "**" / "*_orders_clean.parquet")}',
            hive_partitioning=true
        )
    )
    SELECT
        raw.n_raw,
        clean.n_clean,
        raw.n_raw - clean.n_clean                                   AS n_excluded,
        ROUND(100.0 * (raw.n_raw - clean.n_clean) / raw.n_raw, 4)  AS pct_excluded
    FROM raw, clean
""").fetchdf()

print(validation.T)

# ---------------------------------------------------------------------------
# STEP 10 — F_BAD_TS_RECV audit: records with unreliable capture timestamps
# ---------------------------------------------------------------------------
#
# F_BAD_TS_RECV (flags & 8): ts_recv is set to snapshot generation time, not
# actual capture time. On Eurex EOBI these are cleaR book events at session
# boundaries (00:15 UTC, 06:00 UTC) — all outside our session window.
# Any F_BAD_TS_RECV remaining in-session warrants investigation.

print("\n[Step 10] F_BAD_TS_RECV audit (flags & 8 > 0, in-session only)")

bad_ts = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            timezone('{LOCAL_TZ}',
                to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
            ) AS ts_local
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
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
        (flags & 128) > 0   AS f_last,
        (flags & 64)  > 0   AS f_tob,
        (flags & 32)  > 0   AS f_snapshot,
        (flags & 16)  > 0   AS f_mbp,
        (flags & 8)   > 0   AS f_bad_ts_recv,
        (flags & 4)   > 0   AS f_maybe_bad_book,
        price / 1e9         AS price_pts,
        COUNT(*)            AS n_records
    FROM in_session
    WHERE (flags & 8) > 0
    GROUP BY action, flags, f_last, f_tob, f_snapshot, f_mbp,
             f_bad_ts_recv, f_maybe_bad_book, price_pts
    ORDER BY n_records DESC
""").fetchdf()

print(bad_ts.to_string())