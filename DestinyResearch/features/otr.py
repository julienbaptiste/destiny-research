"""
otr.py — Order-to-Trade Ratio computation from MBO data.

Atomic feature module — pure transformation, no I/O.
The caller is responsible for loading and filtering the table
(RTH or full-day) before passing it here.

OTR measures the ratio of order submissions to executed trades.
High OTR indicates intense algorithmic quoting activity (market making,
quote stuffing). Low OTR indicates more directional order flow.

MBO is the correct source for OTR: it contains the raw event stream
with explicit action labels (ADD, CANCEL, TRADE, ...). MBP-1 reflects
book state snapshots and should not be used for action-based metrics.

Typical usage
-------------
import DestinyResearch as dr
from DestinyResearch.features.otr import compute_otr

contract = dr.get_front_contract("NIY", "2025-03-10")

# RTH only (asia session)
tbl = dr.get_mbo_rth(contract, "2025-03-10")
profile = compute_otr(tbl, bin_minutes=30)

# Full day (no RTH filter)
tbl = dr.get_mbo(contract, "2025-03-10")
profile = compute_otr(tbl, bin_minutes=30)
"""

from __future__ import annotations

import duckdb
import pandas as pd
import pyarrow as pa


def compute_otr(
    table: pa.Table,
    bin_minutes: int = 30,
) -> pd.DataFrame:
    """Compute the intraday Order-to-Trade Ratio profile from a MBO Arrow table.

    Bins the session into fixed-width time intervals and computes OTR
    per bin. Also returns raw ADD and TRADE counts for transparency —
    the caller can recompute OTR or use the counts directly.

    OTR definition
    --------------
    OTR = n_add / n_trades per time bin.

    A high OTR means many orders are submitted per executed trade —
    characteristic of algorithmic market making or quote stuffing.
    A low OTR means most submitted orders result in a trade —
    characteristic of directional, informed order flow.

    Timestamp
    ---------
    Uses ts_event (exchange matching engine timestamp) for time binning —
    consistent with compute_spread(), compute_volume(), compute_depth().

    Parameters
    ----------
    table : pa.Table
        MBO Arrow table. Must contain columns:
            ts_event (uint64 / int64, nanoseconds UTC epoch)
            action   (string: 'ADD', 'CANCEL', 'TRADE', 'FILL', ...)
        Typically the output of dr.get_mbo_rth() or dr.get_mbo().
        No filtering is applied here — the caller decides the time scope.
    bin_minutes : int
        Width of each time bin in minutes. Default 30.

    Returns
    -------
    pd.DataFrame with one row per time bin, columns:
        time_bin  : datetime.time (UTC, start of the bin)
                    Date-stripped for cross-day comparability.
        n_add     : int   — number of ADD events in the bin
        n_trades  : int   — number of TRADE events in the bin
        n_events  : int   — total number of MBO events in the bin
        otr       : float — n_add / n_trades for the bin.
                    None if n_trades == 0 (avoids division by zero —
                    can occur in very quiet bins at session boundaries).

    Notes
    -----
    - Partial fills on CME: each fill of a resting order generates a
      separate TRADE event. n_trades counts individual fill events,
      consistent with get_contract_stats() and compute_volume().
    - Bins where n_trades == 0 are retained in the output with otr=None
      rather than dropped — the caller can filter or fill as needed.
      These typically appear at session boundaries (first/last bin).
    - No BBO change filter is applied here — OTR is computed on the raw
      MBO event stream where every ADD and TRADE is a discrete, meaningful
      event regardless of book state.

    Example
    -------
    >>> profile = compute_otr(tbl, bin_minutes=30)
    >>> profile.head()
       time_bin  n_add  n_trades  n_events    otr
    0  13:30:00  42031     47693    794022  21.38
    1  14:00:00  38912     42232    873254  19.42
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
        return pd.DataFrame(columns=["time_bin", "n_add", "n_trades", "n_events", "otr"])

    # ------------------------------------------------------------------
    # DuckDB computation — single pass over the MBO stream.
    #
    # Steps:
    #   1. Convert ts_event (ns epoch int) to TIMESTAMPTZ via to_timestamp().
    #   2. Bin by time-of-day using time_bucket().
    #   3. Count ADD, TRADE, and total events per bin in one GROUP BY pass
    #      using FILTER syntax (SQL:2003) — avoids multiple scans.
    #   4. Compute OTR as n_add / n_trades — guard against division by zero
    #      using NULLIF so bins with no trades return NULL.
    # ------------------------------------------------------------------

    bin_interval = f"INTERVAL '{bin_minutes} minutes'"

    sql = f"""
        SELECT
            time_bucket(
                {bin_interval},
                to_timestamp(CAST(ts_event AS DOUBLE) / 1e9)
            )                                               AS bin_ts,
            COUNT(*) FILTER (WHERE action = 'ADD')          AS n_add,
            COUNT(*) FILTER (WHERE action = 'TRADE')        AS n_trades,
            COUNT(*)                                        AS n_events,
            -- NULLIF guards against division by zero in empty bins
            CAST(COUNT(*) FILTER (WHERE action = 'ADD') AS DOUBLE)
                / NULLIF(COUNT(*) FILTER (WHERE action = 'TRADE'), 0)
                                                            AS otr
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
    result = result[["time_bin", "n_add", "n_trades", "n_events", "otr"]]

    result["n_add"]    = result["n_add"].astype(int)
    result["n_trades"] = result["n_trades"].astype(int)
    result["n_events"] = result["n_events"].astype(int)
    # otr stays float with potential None/NaN for zero-trade bins

    return result


def aggregate_otr_profiles(
    profiles: list[pd.DataFrame],
) -> pd.DataFrame:
    """Average OTR profiles across multiple days.

    Takes a list of per-day DataFrames from compute_otr() and returns
    a single DataFrame with mean and standard deviation across days for
    each time bin.

    Parameters
    ----------
    profiles : list of pd.DataFrame
        Each element is the output of compute_otr() for one trading day.

    Returns
    -------
    pd.DataFrame with columns:
        time_bin          : datetime.time — time-of-day (UTC)
        otr_mean          : float — cross-day mean OTR per bin
        otr_std           : float — cross-day std of OTR per bin
                            Used for confidence intervals in plots.
        n_add_mean        : float — cross-day mean ADD count per bin
        n_trades_mean     : float — cross-day mean TRADE count per bin
        n_days            : int   — number of days contributing to this bin

    Notes
    -----
    Bins where otr is None (zero trades) are excluded from the cross-day
    mean via skipna=True (pandas default for mean/std). n_days counts
    all days that had at least one event in the bin, regardless of trades.
    """
    if not profiles:
        raise ValueError("profiles list is empty.")

    combined = pd.concat(profiles, ignore_index=True)

    agg = (
        combined
        .groupby("time_bin", sort=True)
        .agg(
            otr_mean=("otr", "mean"),         # skipna=True by default
            otr_std=("otr", "std"),           # cross-day variability
            n_add_mean=("n_add", "mean"),
            n_trades_mean=("n_trades", "mean"),
            n_days=("n_events", "count"),
        )
        .reset_index()
    )

    return agg