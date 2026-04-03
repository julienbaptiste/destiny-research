"""
volume.py — Intraday trade volume computation from MBO data.

Atomic feature module — pure transformation, no I/O.
The caller is responsible for loading and filtering the table
(RTH or full-day) before passing it here.

MBO is the correct source for volume: it is the raw event stream where
each action is explicitly labeled. MBP-1 reflects the book state and
should not be used for action-based metrics.

Typical usage
-------------
import DestinyResearch as dr
from DestinyResearch.features.volume import compute_volume

contract = dr.get_front_contract("ES", "2025-10-10")

# RTH only
tbl = dr.get_mbo_rth(contract, "2025-10-10")
profile = compute_volume(tbl, bin_minutes=30)

# Full day (no RTH filter)
tbl = dr.get_mbo(contract, "2025-10-10")
profile = compute_volume(tbl, bin_minutes=30)
"""

from __future__ import annotations

import duckdb
import pandas as pd
import pyarrow as pa


def compute_volume(
    table: pa.Table,
    bin_minutes: int = 30,
) -> pd.DataFrame:
    """Compute the intraday trade volume profile from a MBO Arrow table.

    Bins the session into fixed-width time intervals and counts the number
    of trades (action='TRADE') per bin. Also reports total event count
    per bin (all actions) as a secondary activity metric.

    Timestamp
    ---------
    Uses ts_event (exchange matching engine timestamp) for time binning —
    consistent with compute_spread() and compute_depth().

    Parameters
    ----------
    table : pa.Table
        MBO Arrow table. Must contain columns:
            ts_event (uint64 / int64, nanoseconds UTC epoch)
            action   (string, e.g. 'ADD', 'CANCEL', 'TRADE', ...)
        Typically the output of dr.get_mbo_rth() or dr.get_mbo().
        No filtering is applied here — the caller decides the time scope.
    bin_minutes : int
        Width of each time bin in minutes. Default 30.

    Returns
    -------
    pd.DataFrame with one row per time bin, columns:
        time_bin    : datetime.time (UTC, start of the bin)
                      Date-stripped for cross-day comparability.
        n_trades    : int   — number of TRADE events in the bin
        n_events    : int   — total number of MBO events in the bin
                      (all actions: ADD, CANCEL, TRADE, FILL, etc.)
                      Useful for computing OTR in aggregate_volume_profiles.

    Notes
    -----
    - Partial fills on CME: each fill of a resting order generates a
      separate TRADE event in the MBO stream. n_trades therefore counts
      individual fill events, not unique aggressor orders. This is
      consistent with how n_trades is counted in get_contract_stats().
    - Bins with zero trades but non-zero events (e.g. pure quoting periods)
      will still appear in the output — the GROUP BY is on all events,
      not just trades. This avoids silent gaps in the profile.
    - Empty bins (no events at all) are not returned — DuckDB GROUP BY
      omits groups with no rows. Reindex with a fixed grid if needed.

    Example
    -------
    >>> profile = compute_volume(tbl, bin_minutes=30)
    >>> profile.head()
       time_bin  n_trades  n_events
    0  13:30:00      8423    712045
    1  14:00:00      9201    834120
    """
    if bin_minutes <= 0:
        raise ValueError(f"bin_minutes must be positive, got {bin_minutes}")

    # Validate required columns
    required = {"ts_event", "action"}
    missing = required - set(table.schema.names)
    if missing:
        raise ValueError(
            f"Table is missing required columns: {missing}. "
            f"Available columns: {table.schema.names}"
        )

    if len(table) == 0:
        return pd.DataFrame(columns=["time_bin", "n_trades", "n_events"])

    # ------------------------------------------------------------------
    # DuckDB computation — single pass over the MBO stream.
    #
    # Steps:
    #   1. Convert ts_event (ns epoch int) to TIMESTAMPTZ via to_timestamp().
    #   2. Bin by time-of-day using time_bucket().
    #   3. Count TRADE events and total events per bin in one GROUP BY pass.
    #      FILTER syntax avoids a second scan for the trade count.
    # ------------------------------------------------------------------

    bin_interval = f"INTERVAL '{bin_minutes} minutes'"

    sql = f"""
        SELECT
            time_bucket(
                {bin_interval},
                to_timestamp(CAST(ts_event AS DOUBLE) / 1e9)
            )                                               AS bin_ts,
            COUNT(*) FILTER (WHERE action = 'TRADE')        AS n_trades,
            COUNT(*)                                        AS n_events
        FROM tbl
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
    result = result[["time_bin", "n_trades", "n_events"]]

    result["n_trades"] = result["n_trades"].astype(int)
    result["n_events"] = result["n_events"].astype(int)

    return result


def aggregate_volume_profiles(
    profiles: list[pd.DataFrame],
) -> pd.DataFrame:
    """Average volume profiles across multiple days.

    Takes a list of per-day DataFrames from compute_volume() and returns
    a single DataFrame with mean and standard deviation across days for
    each time bin.

    Parameters
    ----------
    profiles : list of pd.DataFrame
        Each element is the output of compute_volume() for one trading day.
        All DataFrames should cover the same session (same bin_minutes,
        same RTH window). Missing bins are handled gracefully via skipna.

    Returns
    -------
    pd.DataFrame with columns:
        time_bin               : datetime.time — time-of-day (UTC)
        n_trades_mean          : float — mean trade count per bin across days
        n_trades_std           : float — std of trade count across days
                                 Used for confidence intervals in plots.
        n_events_mean          : float — mean total event count per bin
        n_days                 : int   — number of days contributing to this bin

    Notes
    -----
    n_trades_std captures day-to-day variability in volume at each
    time-of-day — it is the right metric for confidence intervals in
    seasonality plots. A wide confidence interval at a given bin means
    volume is unstable at that time across trading days (e.g. around
    scheduled macro releases).
    """
    if not profiles:
        raise ValueError("profiles list is empty.")

    combined = pd.concat(profiles, ignore_index=True)

    agg = (
        combined
        .groupby("time_bin", sort=True)
        .agg(
            n_trades_mean=("n_trades", "mean"),
            n_trades_std=("n_trades", "std"),    # cross-day variability
            n_events_mean=("n_events", "mean"),
            n_days=("n_trades", "count"),         # days with data for this bin
        )
        .reset_index()
    )

    return agg