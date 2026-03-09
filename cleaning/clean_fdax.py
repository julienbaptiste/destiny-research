# cleaning/clean_fdax.py
#
# Phase 2 - Day 1: FDAX session reconstruction & sanity checks
# Tool: DuckDB (exploration/validation mode)
# Convention: timestamps in CET/CEST (Europe/Paris) for per-market analysis
#
# Eurex session: 08:00 - 22:00 CET (Mon-Fri)
# Tick size: 0.5 index point = 500_000_000 in fixed-point int64 (1e-9 scale)
# Price encoding: raw int64 fixedpoint 1e-9 (e.g. 22799_000_000_000 = 22799.0)

import duckdb
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIG — adjust paths to your local repo layout
# ---------------------------------------------------------------------------

DATA_ROOT = Path("data/market_data/product=FDAX")
OUTPUT_ROOT = Path("data/clean/product=FDAX")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# FDAX tick size in fixed-point int64 (0.5 point * 1e9 = 500_000_000)
FDAX_TICK_SIZE_FP = 500_000_000

# Eurex core session boundaries in local time (CET = UTC+1, CEST = UTC+2)
# We filter in UTC then convert — DuckDB handles tz-aware casting natively
SESSION_START_LOCAL = "08:00:00"
SESSION_END_LOCAL   = "22:00:00"
LOCAL_TZ            = "Europe/Paris"

# ---------------------------------------------------------------------------
# CONNECT — in-process DuckDB, no server needed
# ---------------------------------------------------------------------------

con = duckdb.connect()  # in-memory connection, reads Parquet on demand

# Install/load the httpfs extension if needed (not required for local files)
# con.execute("INSTALL httpfs; LOAD httpfs;")

# ---------------------------------------------------------------------------
# STEP 1 — DISCOVERY: list all FDAX order Parquet files
# ---------------------------------------------------------------------------

# Glob all order files recursively under the product directory
# Expected layout: product=FDAX/year=YYYY/month=MM/FDAX_YYYYMMDD_orders.parquet
order_files = sorted(DATA_ROOT.rglob("*_orders.parquet"))
print("\n[Step 1] List all FDAX order Parquet files")
print(f"[Discovery] Found {len(order_files)} FDAX order files")

if not order_files:
    raise FileNotFoundError(f"No order files found under {DATA_ROOT}")

# Build a glob pattern DuckDB can use directly — avoids loading file list into Python
# DuckDB supports ** glob for recursive scan
glob_pattern = str(DATA_ROOT / "**" / "*_orders.parquet")

# ---------------------------------------------------------------------------
# STEP 2 — RAW STATS: baseline before any filtering
# ---------------------------------------------------------------------------

print("\n[Step 2] Raw stats (no filter applied)")

raw_stats = con.execute(f"""
    SELECT
        COUNT(*)                                    AS total_events,
        COUNT(DISTINCT action)                      AS distinct_actions,
        -- Price range in fixed-point int64
        MIN(price)                                  AS price_min_fp,
        MAX(price)                                  AS price_max_fp,
        -- Convert to float for readability (divide by 1e9)
        MIN(price) / 1e9                            AS price_min,
        MAX(price) / 1e9                            AS price_max,
        -- Timestamp range (raw uint64 nanoseconds UTC)
        MIN(ts_recv)                                AS ts_recv_min,
        MAX(ts_recv)                                AS ts_recv_max,
        -- Count by action type
        SUM(CASE WHEN action = 'A' THEN 1 ELSE 0 END) AS n_add,
        SUM(CASE WHEN action = 'C' THEN 1 ELSE 0 END) AS n_cancel,
        SUM(CASE WHEN action = 'M' THEN 1 ELSE 0 END) AS n_modify,
        SUM(CASE WHEN action = 'R' THEN 1 ELSE 0 END) AS n_replace,
        SUM(CASE WHEN action = 'T' THEN 1 ELSE 0 END) AS n_trade,
        SUM(CASE WHEN action = 'F' THEN 1 ELSE 0 END) AS n_fill
    FROM read_parquet('{glob_pattern}', hive_partitioning=true)
""").fetchdf()

print(raw_stats.T)  # transpose for readability

# ---------------------------------------------------------------------------
# STEP 3 — SESSION FILTER: keep only Eurex core session (08:00-22:00 CET)
# ---------------------------------------------------------------------------

print(f"\n[Step 3] Applying session filter: {SESSION_START_LOCAL} - {SESSION_END_LOCAL} {LOCAL_TZ}")

# DuckDB timezone conversion:
# ts_recv is int64 nanoseconds UTC
# epoch_ns() converts int64 ns to TIMESTAMPTZ natively
# ::TIME is the correct cast syntax (TIME() function does not exist in DuckDB)

session_filtered = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            -- Correct conversion: UBIGINT nanoseconds → TIMESTAMPTZ
            -- Step 1: cast UBIGINT to BIGINT (safe here, ts_recv < INT64_MAX for valid timestamps)
            -- Step 2: to_timestamp() takes seconds as DOUBLE — divide by 1e9
            -- Step 3: timezone() converts the resulting TIMESTAMPTZ to local time
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)               AS ts_utc,
            timezone('{LOCAL_TZ}', to_timestamp(CAST(ts_recv AS BIGINT) / 1e9))  AS ts_local
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
    )
    SELECT
        COUNT(*)                                              AS total_in_session,
        COUNT(*) FILTER (WHERE action = 'A')                 AS n_add,
        COUNT(*) FILTER (WHERE action = 'C')                 AS n_cancel,
        COUNT(*) FILTER (WHERE action = 'M')                 AS n_modify,
        COUNT(*) FILTER (WHERE action = 'T')                 AS n_trade,
        COUNT(DISTINCT DATE_TRUNC('day', ts_local))          AS trading_days
    FROM ts_converted
    WHERE
        -- Weekdays only: ISODOW returns 1=Mon ... 7=Sun (safer than DAYOFWEEK)
        ISODOW(ts_local) BETWEEN 1 AND 5
        -- Session hours: cast to TIME for comparison
        AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
        AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
""").fetchdf()

print(session_filtered.T)

# ---------------------------------------------------------------------------
# STEP 4 — SANITY CHECKS: price, size, timestamp anomalies
# ---------------------------------------------------------------------------

print("\n[Step 4] Sanity checks")

# INT64_MAX = 9_223_372_036_854_775_807 — Databento sentinel for "no price"
# (market orders, stop orders without limit price)
INT64_MAX = 9_223_372_036_854_775_807

sanity = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)                         AS ts_utc,
            timezone('{LOCAL_TZ}', to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)) AS ts_local
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
    ),
    in_session AS (
        SELECT * FROM ts_converted
        WHERE ISODOW(ts_local) BETWEEN 1 AND 5
          AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
          AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
    ),
    -- Pre-compute median price on valid limit orders only (exclude sentinel and non-positive)
    price_stats AS (
        SELECT APPROX_QUANTILE(price, 0.5) AS price_median
        FROM in_session
        WHERE action IN ('A', 'M')
          AND price > 0
          AND price != {INT64_MAX}
    )
    SELECT
        -- PRICE CHECKS
        SUM(CASE WHEN price = {INT64_MAX} THEN 1 ELSE 0 END)
            AS price_sentinel,

        SUM(CASE WHEN price <= 0
                  AND price != {INT64_MAX}
                  AND action IN ('A','M')
             THEN 1 ELSE 0 END)
            AS price_nonpositive,

        SUM(CASE WHEN action IN ('A','M')
                  AND price != {INT64_MAX}
                  AND price > 0
                  AND (price % {FDAX_TICK_SIZE_FP}) != 0
             THEN 1 ELSE 0 END)
            AS tick_size_violations,

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
        SUM(CASE WHEN ts_event > ts_recv THEN 1 ELSE 0 END)
            AS ts_event_after_recv,

        -- 7. Extreme network latency: |ts_recv - ts_event| > 1 second
        SUM(CASE WHEN ABS(CAST(ts_recv AS BIGINT) - CAST(ts_event AS BIGINT))
                      > 1_000_000_000
             THEN 1 ELSE 0 END)
            AS ts_delta_over_1s,

        COUNT(*) AS total_checked

    FROM in_session
    -- Cross-join: price_stats returns exactly one row — safe scalar access
    CROSS JOIN price_stats p
""").fetchdf()

print(sanity.T)

# ---------------------------------------------------------------------------
# STEP 5 — QUALITY REPORT: summary with rejection rates
# ---------------------------------------------------------------------------

print("\n[Step 5] Quality report")

total = sanity["total_checked"].iloc[0]
checks = [
    "price_nonpositive",
    "tick_size_violations",
    "price_extreme_outliers",
    "size_nonpositive",
    "ts_event_after_recv",
    "ts_delta_over_1s",
]

print(f"{'Check':<30} {'Count':>10} {'Rate':>8}")
print("-" * 50)
for check in checks:
    count = sanity[check].iloc[0]
    rate = count / total * 100 if total > 0 else 0
    print(f"{check:<30} {int(count):>10} {rate:>7.4f}%")

print(f"\nTotal events in session: {int(total):,}")

# ---------------------------------------------------------------------------
# STEP 6 — INVESTIGATE: price_nonpositive records
# ---------------------------------------------------------------------------

print("\n[Step 6] Investigating non-positive prices")

investigation = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)                         AS ts_utc,
            timezone('{LOCAL_TZ}', to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)) AS ts_local
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
        price / 1e9                         AS price_float,
        size,
        -- Check if price is a round number of fixed-point (might indicate encoding)
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

# Also check: are these concentrated at session open/close?
print("\n[Step 6b] Time distribution of non-positive prices")

time_dist = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)                         AS ts_utc,
            timezone('{LOCAL_TZ}', to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)) AS ts_local
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
    ),
    in_session AS (
        SELECT * FROM ts_converted
        WHERE ISODOW(ts_local) BETWEEN 1 AND 5
          AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
          AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
    )
    SELECT
        -- Bucket by hour to see if concentrated at open/close
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
# STEP 7 — FLAGS AUDIT: understand all flag values in the dataset
# ---------------------------------------------------------------------------

print("\n[Step 7] Flags distribution — full dataset in session")

flags_dist = con.execute(f"""
    WITH ts_converted AS (
        SELECT
            *,
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)                         AS ts_utc,
            timezone('{LOCAL_TZ}', to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)) AS ts_local
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
        -- Decode individual bits for readability
        -- Databento MBO flags (bitfield uint8):
        -- bit 7 (128) = F_LAST      last message in event
        -- bit 6 (64)  = F_TOB       top-of-book change
        -- bit 5 (32)  = F_SNAPSHOT  part of snapshot
        -- bit 4 (16)  = F_MBP       MBP-derived event
        -- bit 3 (8)   = reserved
        -- bit 2 (4)   = reserved
        -- bit 1 (2)   = reserved
        -- bit 0 (1)   = reserved
        (flags & 128) > 0           AS bit7_f_last,
        (flags & 64)  > 0           AS bit6_f_tob,
        (flags & 32)  > 0           AS bit5_snapshot,
        (flags & 16)  > 0           AS bit4_mbp,
        action,
        COUNT(*)                    AS n_records,
        -- Price stats for this flag combination
        MIN(price) / 1e9            AS price_min,
        MAX(price) / 1e9            AS price_max,
        AVG(price) / 1e9            AS price_avg
    FROM in_session
    GROUP BY flags, bit7_f_last, bit6_f_tob, bit5_snapshot, bit4_mbp, action
    ORDER BY n_records DESC
""").fetchdf()

print(flags_dist.to_string())

# ---------------------------------------------------------------------------
# STEP 8 — WRITE CLEAN PARQUET: apply all filters and write clean events
# ---------------------------------------------------------------------------
#
# Filters applied:
#   1. Session filter: Eurex core session 08:00-22:00 CET, weekdays only
#   2. Sentinel prices (INT64_MAX): exclude — market/stop orders without limit
#   3. Implied orders: exclude all A/C/M where price <= 0 (Eurex synthetic spread legs)
#   4. Replace records with sentinel (flags=8): excluded via rule 2
#
# Records kept:
#   - All legitimate Add/Cancel/Modify with price > 0
#   - flags=0 Cancels (legitimate cancels without F_LAST bit)
#
# Output convention (Hive-style partitioning, consistent with raw data layout):
#   data/clean/product=FDAX/year=YYYY/month=MM/FDAX_YYYYMMDD_orders_clean.parquet

print("\n[Step 8] Writing clean Parquet files")

INT64_MAX = 9_223_372_036_854_775_807

# Get list of distinct trading days from raw data
trading_days = con.execute(f"""
    SELECT DISTINCT
        -- Extract date in local timezone for file naming
        CAST(timezone('{LOCAL_TZ}',
            to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
        ) AS DATE)                              AS trade_date,
        -- Keep year/month for output partitioning
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

total_raw = 0
total_clean = 0

for _, row in trading_days.iterrows():
    trade_date  = row["trade_date"]   # e.g. 2025-05-02
    year        = int(row["year"])
    month       = int(row["month"])
    date_str    = trade_date.strftime("%Y%m%d")

    # Build output path — mirror Hive partitioning of raw data
    out_dir = OUTPUT_ROOT / f"year={year}" / f"month={month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"FDAX_{date_str}_orders_clean.parquet"

    # Skip if already written (resume capability)
    if out_path.exists():
        print(f"  [SKIP] {date_str} — already exists")
        continue

    # Query: filter to single day + apply all cleaning rules
    # Write via DuckDB COPY TO — zero Python row iteration, native Parquet writer
    result = con.execute(f"""
        WITH ts_converted AS (
            SELECT
                *,
                timezone('{LOCAL_TZ}',
                    to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)
                )                               AS ts_local
            FROM read_parquet('{glob_pattern}', hive_partitioning=true)
        ),
        clean AS (
            SELECT
                -- Drop ts_local helper column — not needed downstream
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
                symbol
            FROM ts_converted
            WHERE
                -- Session filter
                ISODOW(ts_local) BETWEEN 1 AND 5
                AND ts_local::TIME >= '{SESSION_START_LOCAL}'::TIME
                AND ts_local::TIME <  '{SESSION_END_LOCAL}'::TIME
                -- Single day
                AND CAST(ts_local AS DATE) = '{trade_date}'
                -- Rule 1: exclude sentinel prices
                AND price != {INT64_MAX}
                -- Rule 2: exclude implied orders (negative prices on A/M/C)
                AND NOT (price <= 0 AND action IN ('A', 'M', 'C'))
        )
        SELECT * FROM clean
        ORDER BY ts_recv  -- ensure monotonic sort key for LOB reconstruction
    """)

    # Fetch as PyArrow table — zero pandas overhead
    arrow_table = result.fetch_arrow_table()

    n_clean = arrow_table.num_rows

    # Write Parquet with snappy compression — good balance speed/size for tick data
    import pyarrow.parquet as pq
    pq.write_table(
        arrow_table,
        str(out_path),
        compression="snappy",
        # Preserve row group size suitable for sequential LOB scan
        row_group_size=500_000
    )

    total_raw += n_clean  # approximation — raw count not tracked per day here
    total_clean += n_clean

    print(f"  [OK] {date_str} — {n_clean:,} clean events → {out_path.name}")

print(f"\n[Step 8] Done. Total clean events written: {total_clean:,}")

# ---------------------------------------------------------------------------
# STEP 9 — VALIDATION: compare raw vs clean counts
# ---------------------------------------------------------------------------

print("\n[Step 9] Validation — raw vs clean")

validation = con.execute(f"""
    WITH raw AS (
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
              )::TIME < '{SESSION_END_LOCAL}'::TIME
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
        raw.n_raw - clean.n_clean           AS n_excluded,
        ROUND(100.0 * (raw.n_raw - clean.n_clean) / raw.n_raw, 4)
                                            AS pct_excluded
    FROM raw, clean
""").fetchdf()

print(validation.T)

# ---------------------------------------------------------------------------
# STEP 10 — F_BAD_TS_RECV audit: count all records with unreliable timestamps
# ---------------------------------------------------------------------------

print("\n[Step 10] F_BAD_TS_RECV audit (flags & 8 > 0)")

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
        -- Decode all flag bits present on these records
        flags,
        (flags & 128) > 0   AS f_last,
        (flags & 64)  > 0   AS f_tob,
        (flags & 32)  > 0   AS f_snapshot,
        (flags & 16)  > 0   AS f_mbp,
        (flags & 8)   > 0   AS f_bad_ts_recv,
        (flags & 4)   > 0   AS f_maybe_bad_book,
        price / 1e9         AS price_float,
        COUNT(*)            AS n_records
    FROM in_session
    -- Bitwise AND: catches flags=8 but also flags=9, 12, 136, etc.
    WHERE (flags & 8) > 0
    GROUP BY action, flags, f_last, f_tob, f_snapshot, f_mbp,
             f_bad_ts_recv, f_maybe_bad_book, price_float
    ORDER BY n_records DESC
""").fetchdf()

print(bad_ts.to_string())

# ---------------------------------------------------------------------------
# NEXT STEP (Jour 2): LOB level 1 reconstruction on clean events
# ---------------------------------------------------------------------------
# Once sanity check results are validated, we will:
# 1. Write clean filtered events to OUTPUT_ROOT as Parquet
# 2. Build the MBO state machine for bid/ask level 1 reconstruction
