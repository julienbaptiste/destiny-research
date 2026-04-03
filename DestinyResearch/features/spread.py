"""
spread.py — Bid-ask spread computation from MBP-1 data.

Atomic feature module — pure transformation, no I/O.
The caller is responsible for loading and filtering the table
(RTH or full-day) before passing it here.

Typical usage
-------------
import DestinyResearch as dr
from DestinyResearch.features.spread import compute_spread

contract  = dr.get_front_contract("ES", "2025-10-10")
tick_size = dr.get_info("ES")["tick_size_pts"]          # e.g. 0.25 for ES

# RTH only
tbl = dr.get_mbp1_rth(contract, "2025-10-10")
profile = compute_spread(tbl, tick_size_pts=tick_size, bin_minutes=30)

# Full day (no RTH filter)
tbl = dr.get_mbp1(contract, "2025-10-10")
profile = compute_spread(tbl, tick_size_pts=tick_size, bin_minutes=30)
"""

from __future__ import annotations

import duckdb
import pandas as pd
import pyarrow as pa


def compute_spread(
    table: pa.Table,
    tick_size_pts: float,
    bin_minutes: int = 30,
) -> pd.DataFrame:
    """Compute the intraday bid-ask spread profile from a MBP-1 Arrow table.

    Bins the session into fixed-width time intervals and computes spread
    statistics per bin. The spread is expressed in ticks (not points) to
    allow meaningful cross-market comparison.

    Spread definition
    -----------------
    spread_pts  = (ask_px_00 - bid_px_00) / 1e9
        MBP-1 prices are stored as fixed-point int64 (price * 1e9).
        Dividing by 1e9 converts to index points.

    spread_ticks = spread_pts / tick_size_pts
        Normalises by the product tick size so that one tick = 1.0
        regardless of the product. This makes cross-market comparison valid.

    Timestamp
    ---------
    Uses ts_event (exchange matching engine timestamp) rather than ts_recv
    (local receipt timestamp) for the time binning. ts_event reflects when
    the quote actually changed on the exchange — more meaningful for
    intraday seasonality analysis.

    Parameters
    ----------
    table : pa.Table
        MBP-1 Arrow table. Must contain columns:
            ts_event  (uint64 / int64, nanoseconds UTC epoch)
            bid_px_00 (int64, fixed-point price * 1e9)
            ask_px_00 (int64, fixed-point price * 1e9)
        Typically the output of dr.get_mbp1_rth() or dr.get_mbp1().
        No filtering is applied here — the caller decides the time scope.
    tick_size_pts : float
        Tick size in index points for the product (e.g. 0.25 for ES,
        0.5 for FDAX, 1.0 for HSI). Used to normalise spread to ticks.
        Retrieve via dr.get_info(product)["tick_size_pts"].
    bin_minutes : int
        Width of each time bin in minutes. Default 30 (standard in the
        microstructure literature). Must be a divisor of 60 or a multiple
        of 60 for clean hourly boundaries (e.g. 15, 30, 60).

    Returns
    -------
    pd.DataFrame with one row per time bin, columns:
        time_bin     : pd.Timestamp (UTC, start of the bin, date-naive)
                       Represents the time-of-day only — date is stripped
                       so that profiles from different days are directly
                       comparable and can be averaged across sessions.
        spread_mean  : float — mean spread over the bin, in ticks
        spread_std   : float — standard deviation of spread, in ticks
        spread_min   : float — minimum spread observed in the bin, in ticks
        spread_max   : float — maximum spread observed in the bin, in ticks
        n_obs        : int   — number of MBP-1 events in the bin

    Notes
    -----
    - Bins with zero observations (e.g. inside a lunch break that was not
      pre-filtered) will not appear in the output — DuckDB GROUP BY omits
      empty groups. The caller should handle gaps explicitly if needed
      (e.g. reindex with a fixed time grid and fill with NaN).
    - Spread values of 0 (bid == ask, which should not occur on a healthy
      LOB but can appear during auction phases or on HKEX around OrderbookClear
      events) are included in the aggregation. Filter upstream if needed.
    - Fixed-point conversion: prices are int64 stored as price * 1e9.
      The conversion (ask - bid) / 1e9 is done inside DuckDB before
      aggregation — no intermediate Python array is materialised.
    - BBO change filter: only rows where at least one of bid_px_00, ask_px_00,
      bid_sz_00, ask_sz_00 differs from the previous row are included in the
      aggregation. MBP-1 records a snapshot after every MBO event, including
      events that do not affect the best level (e.g. ADD/CANCEL at depth 2+).
      Without this filter, unchanged BBO states would be over-represented in
      periods of intense quoting behind the best, biasing the intraday profile.

    Example
    -------
    >>> profile = compute_spread(tbl, tick_size_pts=0.25, bin_minutes=30)
    >>> profile.head()
       time_bin  spread_mean  spread_std  spread_min  spread_max  n_obs
    0  01:00:00          1.0         0.0         1.0         2.0   4821
    1  01:30:00          1.1         0.3         1.0         3.0   5103
    """
    if tick_size_pts <= 0:
        raise ValueError(f"tick_size_pts must be positive, got {tick_size_pts}")
    if bin_minutes <= 0:
        raise ValueError(f"bin_minutes must be positive, got {bin_minutes}")

    # Validate required columns are present
    required = {"ts_event", "bid_px_00", "ask_px_00"}
    missing = required - set(table.schema.names)
    if missing:
        raise ValueError(
            f"Table is missing required columns: {missing}. "
            f"Available columns: {table.schema.names}"
        )

    if len(table) == 0:
        return pd.DataFrame(columns=[
            "time_bin", "spread_mean", "spread_std",
            "spread_min", "spread_max", "n_obs"
        ])

    # ------------------------------------------------------------------
    # DuckDB computation — zero Python-side data materialisation.
    #
    # Steps:
    #   1. Cast ts_event (uint64/int64 ns epoch) to DuckDB TIMESTAMPTZ
    #      via epoch_ns() — gives us a proper datetime for time binning.
    #   2. Compute spread in points: (ask - bid) / 1e9
    #   3. Normalise to ticks: spread_pts / tick_size_pts
    #   4. Bin by time-of-day using time_bucket() on the UTC timestamp.
    #      time_bucket() truncates to the nearest bin boundary — e.g. with
    #      30min bins, 01:47 UTC → 01:30 UTC.
    #   5. Aggregate per bin: mean, std, min, max, count.
    #   6. Extract time-of-day only (strip date) for cross-day comparability.
    #
    # Why DuckDB here rather than pandas?
    # The table can be large (ES: ~8M rows/day). Computing spread on every
    # row in Python would materialise a full float64 array (~64MB for 8M
    # rows). DuckDB computes the aggregation in a single vectorised pass
    # without building intermediate arrays.
    # ------------------------------------------------------------------

    bin_interval = f"INTERVAL '{bin_minutes} minutes'"

    sql = f"""
        WITH bbo_changes AS (
            -- Compute LAG values to detect BBO changes.
            -- MBP-1 emits a row for every MBO event, including events that
            -- do not affect the best level. Without this filter, unchanged
            -- BBO states would be over-represented in periods of intense
            -- quoting behind the best, biasing the spread average.
            SELECT
                ts_event,
                bid_px_00,
                ask_px_00,
                bid_sz_00,
                ask_sz_00,
                LAG(bid_px_00) OVER (ORDER BY ts_event)  AS prev_bid_px,
                LAG(ask_px_00) OVER (ORDER BY ts_event)  AS prev_ask_px,
                LAG(bid_sz_00) OVER (ORDER BY ts_event)  AS prev_bid_sz,
                LAG(ask_sz_00) OVER (ORDER BY ts_event)  AS prev_ask_sz
            FROM tbl
            WHERE ask_px_00 > bid_px_00
        ),
        base AS (
            -- Keep only rows where the BBO actually changed.
            -- OR prev_bid_px IS NULL: include first row (LAG returns NULL)
            SELECT
                to_timestamp(CAST(ts_event AS DOUBLE) / 1e9) AS ts_utc,
                (CAST(ask_px_00 AS DOUBLE)
                 - CAST(bid_px_00 AS DOUBLE))
                / {tick_size_pts}                            AS spread_ticks
            FROM bbo_changes
            WHERE bid_px_00 != prev_bid_px
               OR ask_px_00 != prev_ask_px
               OR bid_sz_00 != prev_bid_sz
               OR ask_sz_00 != prev_ask_sz
               OR prev_bid_px IS NULL
        ),
        binned AS (
            -- Bin by time-of-day (UTC)
            SELECT
                time_bucket({bin_interval}, ts_utc)          AS bin_ts,
                spread_ticks
            FROM base
        )
        SELECT
            bin_ts,
            AVG(spread_ticks)                                AS spread_mean,
            STDDEV_SAMP(spread_ticks)                        AS spread_std,
            MIN(spread_ticks)                                AS spread_min,
            MAX(spread_ticks)                                AS spread_max,
            COUNT(*)                                         AS n_obs
        FROM binned
        GROUP BY bin_ts
        ORDER BY bin_ts
    """

    con = duckdb.connect()
    # Register Arrow table as a DuckDB view — zero-copy, no data movement
    con.register("tbl", table)
    result = con.execute(sql).df()
    con.close()

    # ------------------------------------------------------------------
    # Post-processing: strip date from bin_ts to get time-of-day only.
    # This allows profiles from different dates to be averaged directly
    # without date misalignment.
    # ------------------------------------------------------------------
    result["bin_ts"] = pd.to_datetime(result["bin_ts"], utc=True)
    # Keep time-of-day as a Timestamp anchored on a fixed reference date
    # (1970-01-01) so that arithmetic and plotting work correctly.
    result["time_bin"] = result["bin_ts"].dt.time

    result = result.drop(columns=["bin_ts"])
    result = result[["time_bin", "spread_mean", "spread_std",
                     "spread_min", "spread_max", "n_obs"]]

    # Cast n_obs to int (DuckDB returns int64, ensure no float leakage)
    result["n_obs"] = result["n_obs"].astype(int)

    return result


def aggregate_spread_profiles(
    profiles: list[pd.DataFrame],
) -> pd.DataFrame:
    """Average spread profiles across multiple days.

    Takes a list of per-day DataFrames from compute_spread() and returns
    a single DataFrame with mean and standard deviation across days for
    each time bin. This is the cross-day average used for seasonality plots.

    Parameters
    ----------
    profiles : list of pd.DataFrame
        Each element is the output of compute_spread() for one trading day.
        All DataFrames must have the same time_bin grid (same bin_minutes,
        same session). Days with missing bins (e.g. early close) are handled
        gracefully — missing bins contribute NaN and are excluded from the
        cross-day mean via skipna=True.

    Returns
    -------
    pd.DataFrame with columns:
        time_bin        : time-of-day (same as input)
        spread_mean     : float — mean of daily spread_mean across days
        spread_std_across_days : float — std of daily spread_mean across days
                          (measures day-to-day variability, used for
                          confidence intervals in seasonality plots)
        spread_std_intraday    : float — mean of daily spread_std across days
                          (measures within-day variability)
        n_obs_mean      : float — mean number of observations per bin per day
        n_days          : int   — number of days that contributed to this bin

    Notes
    -----
    spread_std_across_days is the right metric for confidence intervals
    in seasonality plots: it captures how stable the pattern is across
    different trading days, which is what we want to show.
    """
    if not profiles:
        raise ValueError("profiles list is empty.")

    # Stack all daily profiles — one row per (day, time_bin)
    combined = pd.concat(profiles, ignore_index=True)

    agg = (
        combined
        .groupby("time_bin", sort=True)
        .agg(
            spread_mean=("spread_mean", "mean"),
            spread_std_across_days=("spread_mean", "std"),   # cross-day variability
            spread_std_intraday=("spread_std", "mean"),      # within-day variability
            n_obs_mean=("n_obs", "mean"),
            n_days=("n_obs", "count"),                       # days with data for this bin
        )
        .reset_index()
    )

    return agg