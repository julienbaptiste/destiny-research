"""
depth.py — Best bid/ask depth computation from MBP-1 data.

Atomic feature module — pure transformation, no I/O.
The caller is responsible for loading and filtering the table
(RTH or full-day) before passing it here.

Depth is defined as the number of contracts available at the best bid
and best ask (level 1 of the order book). It measures the immediate
liquidity available for execution without moving the price.

MBP-1 is the correct source for depth: it contains the reconstructed
book state after each event, including bid_sz_00 and ask_sz_00.

Typical usage
-------------
import DestinyResearch as dr
from DestinyResearch.features.depth import compute_depth

contract = dr.get_front_contract("ES", "2025-10-10")

# RTH only
tbl = dr.get_mbp1_rth(contract, "2025-10-10")
profile = compute_depth(tbl, bin_minutes=30)

# Full day (no RTH filter)
tbl = dr.get_mbp1(contract, "2025-10-10")
profile = compute_depth(tbl, bin_minutes=30)
"""

from __future__ import annotations

import duckdb
import pandas as pd
import pyarrow as pa


def compute_depth(
    table: pa.Table,
    bin_minutes: int = 30,
) -> pd.DataFrame:
    """Compute the intraday best-level depth profile from a MBP-1 Arrow table.

    Bins the session into fixed-width time intervals and computes bid and
    ask depth statistics per bin. Depth is expressed in number of contracts
    (the natural unit for bid_sz_00 / ask_sz_00 in MBP-1).

    Depth definition
    ----------------
    bid_depth = bid_sz_00 : number of contracts available at the best bid
    ask_depth = ask_sz_00 : number of contracts available at the best ask

    These are level-1 quantities only (MBP-1). For deeper book analysis,
    MBP-10 would be required — deferred to a later phase of the project.

    Timestamp
    ---------
    Uses ts_event (exchange matching engine timestamp) for time binning —
    consistent with compute_spread() and compute_volume().

    Parameters
    ----------
    table : pa.Table
        MBP-1 Arrow table. Must contain columns:
            ts_event  (uint64 / int64, nanoseconds UTC epoch)
            bid_sz_00 (uint32, number of contracts at best bid)
            ask_sz_00 (uint32, number of contracts at best ask)
        Typically the output of dr.get_mbp1_rth() or dr.get_mbp1().
        No filtering is applied here — the caller decides the time scope.
    bin_minutes : int
        Width of each time bin in minutes. Default 30.

    Returns
    -------
    pd.DataFrame with one row per time bin, columns:
        time_bin       : datetime.time (UTC, start of the bin)
                         Date-stripped for cross-day comparability.
        bid_depth_mean : float — mean bid depth over the bin, in contracts
        bid_depth_std  : float — std of bid depth within the bin
        ask_depth_mean : float — mean ask depth over the bin, in contracts
        ask_depth_std  : float — std of ask depth within the bin
        total_depth_mean : float — mean of (bid_sz_00 + ask_sz_00) per event
                           Convenient single metric for overall liquidity level.
        n_obs          : int   — number of MBP-1 events in the bin

    Notes
    -----
    - Depth of 0 on one side (empty best level) can occur during auction
      phases or around OrderbookClear events on HKEX. These rows are
      excluded from the aggregation (WHERE bid_sz_00 > 0 AND ask_sz_00 > 0)
      to avoid distorting the mean — they reflect book reconstruction
      artefacts rather than real market conditions.
    - bid_depth_std and ask_depth_std measure within-bin variability
      (how much depth fluctuates within a 30-min window on a single day).
      Cross-day variability is computed in aggregate_depth_profiles().
    - MBP-1 prices are stored as doubles in our reconstructed files
      (confirmed: bid_px_00 = 6788.0 in index points, not fixed-point).
      bid_sz_00 / ask_sz_00 are uint32 — direct contract counts, no
      conversion needed.
    - BBO change filter: only rows where at least one of bid_px_00, ask_px_00,
      bid_sz_00, ask_sz_00 differs from the previous row are included in the
      aggregation. MBP-1 records a snapshot after every MBO event, including
      events that do not affect the best level (e.g. ADD/CANCEL at depth 2+).
      Without this filter, unchanged BBO states would be over-represented in
      periods of intense quoting behind the best, biasing the intraday profile.

    Example
    -------
    >>> profile = compute_depth(tbl, bin_minutes=30)
    >>> profile.head()
       time_bin  bid_depth_mean  bid_depth_std  ask_depth_mean  ask_depth_std  total_depth_mean  n_obs
    0  13:30:00           412.3           85.2           398.7           79.1             811.0  794022
    1  14:00:00           389.1           91.4           401.2           88.3             790.3  873254
    """
    if bin_minutes <= 0:
        raise ValueError(f"bin_minutes must be positive, got {bin_minutes}")

    # Validate required columns
    required = {"ts_event", "bid_sz_00", "ask_sz_00"}
    missing = required - set(table.schema.names)
    if missing:
        raise ValueError(
            f"Table is missing required columns: {missing}. "
            f"Available columns: {table.schema.names}"
        )

    if len(table) == 0:
        return pd.DataFrame(columns=[
            "time_bin", "bid_depth_mean", "bid_depth_std",
            "ask_depth_mean", "ask_depth_std", "total_depth_mean", "n_obs"
        ])

    # ------------------------------------------------------------------
    # DuckDB computation — single pass over the MBP-1 stream.
    #
    # Steps:
    #   1. Convert ts_event (ns epoch int) to TIMESTAMPTZ via to_timestamp().
    #   2. Filter out rows with empty best level on either side.
    #   3. Compute total_depth = bid_sz_00 + ask_sz_00 per row.
    #   4. Bin by time-of-day using time_bucket().
    #   5. Aggregate per bin: mean + std for bid, ask, and total depth.
    # ------------------------------------------------------------------

    bin_interval = f"INTERVAL '{bin_minutes} minutes'"

    sql = f"""
        WITH bbo_changes AS (
            -- Compute LAG values to detect BBO changes
            SELECT
                ts_event,
                bid_sz_00,
                ask_sz_00,
                bid_px_00,
                ask_px_00,
                LAG(bid_sz_00) OVER (ORDER BY ts_event) AS prev_bid_sz,
                LAG(ask_sz_00) OVER (ORDER BY ts_event) AS prev_ask_sz,
                LAG(bid_px_00) OVER (ORDER BY ts_event) AS prev_bid_px,
                LAG(ask_px_00) OVER (ORDER BY ts_event) AS prev_ask_px
            FROM tbl
            WHERE bid_sz_00 > 0
              AND ask_sz_00 > 0
        ),
        base AS (
            -- Keep only rows where the BBO actually changed
            SELECT
                time_bucket(
                    {bin_interval},
                    to_timestamp(CAST(ts_event AS DOUBLE) / 1e9)
                )                                           AS bin_ts,
                CAST(bid_sz_00 AS DOUBLE)                   AS bid_depth,
                CAST(ask_sz_00 AS DOUBLE)                   AS ask_depth,
                CAST(bid_sz_00 AS DOUBLE)
                    + CAST(ask_sz_00 AS DOUBLE)             AS total_depth
            FROM bbo_changes
            WHERE bid_sz_00 != prev_bid_sz
               OR ask_sz_00 != prev_ask_sz
               OR bid_px_00 != prev_bid_px
               OR ask_px_00 != prev_ask_px
               -- First row has NULL LAG values — include it (COALESCE not needed,
               -- NULL != anything evaluates to NULL which is falsy, so the first
               -- row would be excluded. Force include via IS NULL check.)
               OR prev_bid_sz IS NULL
        )
        SELECT
            bin_ts,
            AVG(bid_depth)                                  AS bid_depth_mean,
            STDDEV_SAMP(bid_depth)                          AS bid_depth_std,
            AVG(ask_depth)                                  AS ask_depth_mean,
            STDDEV_SAMP(ask_depth)                          AS ask_depth_std,
            AVG(total_depth)                                AS total_depth_mean,
            COUNT(*)                                        AS n_obs
        FROM base
        GROUP BY bin_ts
        ORDER BY bin_ts
    """

    con = duckdb.connect()
    con.register("tbl", table)
    result = con.execute(sql).df()
    con.close()

    # Strip date — keep time-of-day only for cross-day comparability
    result["bin_ts"] = pd.to_datetime(result["bin_ts"], utc=True)
    result["time_bin"] = result["bin_ts"].dt.time

    result = result.drop(columns=["bin_ts"])
    result = result[[
        "time_bin",
        "bid_depth_mean", "bid_depth_std",
        "ask_depth_mean", "ask_depth_std",
        "total_depth_mean", "n_obs",
    ]]

    result["n_obs"] = result["n_obs"].astype(int)

    return result


def aggregate_depth_profiles(
    profiles: list[pd.DataFrame],
) -> pd.DataFrame:
    """Average depth profiles across multiple days.

    Takes a list of per-day DataFrames from compute_depth() and returns
    a single DataFrame with mean and standard deviation across days for
    each time bin.

    Parameters
    ----------
    profiles : list of pd.DataFrame
        Each element is the output of compute_depth() for one trading day.

    Returns
    -------
    pd.DataFrame with columns:
        time_bin                  : datetime.time — time-of-day (UTC)
        bid_depth_mean            : float — cross-day mean of bid depth
        bid_depth_std_across_days : float — cross-day std of bid depth
                                    Used for confidence intervals in plots.
        ask_depth_mean            : float — cross-day mean of ask depth
        ask_depth_std_across_days : float — cross-day std of ask depth
        total_depth_mean          : float — cross-day mean of total depth
        n_days                    : int   — number of days contributing to bin
    """
    if not profiles:
        raise ValueError("profiles list is empty.")

    combined = pd.concat(profiles, ignore_index=True)

    agg = (
        combined
        .groupby("time_bin", sort=True)
        .agg(
            bid_depth_mean=("bid_depth_mean", "mean"),
            bid_depth_std_across_days=("bid_depth_mean", "std"),
            ask_depth_mean=("ask_depth_mean", "mean"),
            ask_depth_std_across_days=("ask_depth_mean", "std"),
            total_depth_mean=("total_depth_mean", "mean"),
            n_days=("n_obs", "count"),
        )
        .reset_index()
    )

    return agg