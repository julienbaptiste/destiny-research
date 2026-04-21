"""
otr.py — Order-to-Trade Ratio computation from MBO data.

Atomic feature module — pure transformation, no I/O.
The caller is responsible for loading and filtering the table
(RTH or full-day) before passing it here.

OTR measures the ratio of order submissions to executed trades.
High OTR indicates intense algorithmic quoting activity (market making,
quote stuffing). Low OTR indicates more directional order flow.

Definition per Eurex regulation:
    OTR = (number of orders) / (number of trades)
    number of orders = ADD + MODIFY (where MODIFY = DELETE + ADD)
    number of trades = number of executions in the order book

Reference
---------
Eurex Order-to-Trade Ratio regulation:
https://www.eurex.com/ex-en/rules-regs/regulations/order-to-trade-ratio

MBO is the correct source for OTR: it contains the raw event stream
with explicit action labels (ADD, CANCEL, MODIFY, FILL, ...). MBP-1 reflects
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
    per bin. Also returns raw ADD, MODIFY, and FILL counts for transparency —
    the caller can recompute OTR or use the counts directly.

    OTR definition
    --------------
    OTR = (ADD + MODIFY) / FILL per time bin.

    Per Eurex regulation, a MODIFY counts as one order submission (internally
    it's a DELETE + ADD, but we count it once in the normalized data).

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
            action   (string: 'ADD', 'MODIFY', 'CANCEL', 'FILL', ...)
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
        n_modify  : int   — number of MODIFY events in the bin
        n_fill    : int   — number of FILL events in the bin
        n_events  : int   — total number of MBO events in the bin
        otr       : float — (n_add + n_modify) / n_fill for the bin.
                    None if n_fill == 0 (avoids division by zero —
                    can occur in very quiet bins at session boundaries).

    Notes
    -----
    - FILL events represent passive order executions in the order book.
      This is the correct denominator for OTR (per Eurex definition).
    - TRADE events are market data prints and should NOT be used for OTR.
    - On HKEX normalized data: TRADE → FILL → CANCEL triplet. Use FILL.
    - On Databento: FILL = partial fill events. Use FILL.
    - Partial fills: each fill of a resting order generates a separate
      FILL event. n_fill counts individual fill events, consistent with
      get_contract_stats() and compute_volume().
    - Bins where n_fill == 0 are retained in the output with otr=None
      rather than dropped — the caller can filter or fill as needed.
      These typically appear at session boundaries (first/last bin).
    - No BBO change filter is applied here — OTR is computed on the raw
      MBO event stream where every ADD, MODIFY, and FILL is a discrete,
      meaningful event regardless of book state.

    Example
    -------
    >>> profile = compute_otr(tbl, bin_minutes=30)
    >>> profile.head()
       time_bin  n_add  n_modify  n_fill  n_events    otr
    0  13:30:00  42031     3421    2047    794022  22.19
    1  14:00:00  38912     3102    1987    873254  21.14
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
        return pd.DataFrame(columns=["time_bin", "n_add", "n_modify", "n_fill", "n_events", "otr"])

    # ------------------------------------------------------------------
    # DuckDB computation — single pass over the MBO stream.
    #
    # Steps:
    #   1. Convert ts_event (ns epoch int) to TIMESTAMPTZ via to_timestamp().
    #   2. Bin by time-of-day using time_bucket().
    #   3. Count ADD, MODIFY, FILL, and total events per bin in one GROUP BY pass
    #      using FILTER syntax (SQL:2003) — avoids multiple scans.
    #   4. Compute OTR as (n_add + n_modify) / n_fill — guard against division
    #      by zero using NULLIF so bins with no fills return NULL.
    # ------------------------------------------------------------------

    bin_interval = f"INTERVAL '{bin_minutes} minutes'"

    sql = f"""
        SELECT
            time_bucket(
                {bin_interval},
                to_timestamp(CAST(ts_event AS DOUBLE) / 1e9)
            )                                               AS bin_ts,
            COUNT(*) FILTER (WHERE action = 'ADD')          AS n_add,
            COUNT(*) FILTER (WHERE action = 'MODIFY')       AS n_modify,
            COUNT(*) FILTER (WHERE action = 'FILL')         AS n_fill,
            COUNT(*)                                        AS n_events,
            -- OTR per Eurex definition: (ADD + MODIFY) / FILL
            -- NULLIF guards against division by zero in empty bins
            CAST(
                (COUNT(*) FILTER (WHERE action = 'ADD') + 
                 COUNT(*) FILTER (WHERE action = 'MODIFY'))
                AS DOUBLE
            ) / NULLIF(COUNT(*) FILTER (WHERE action = 'FILL'), 0)
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
    result = result[["time_bin", "n_add", "n_modify", "n_fill", "n_events", "otr"]]

    result["n_add"]    = result["n_add"].astype(int)
    result["n_modify"] = result["n_modify"].astype(int)
    result["n_fill"]   = result["n_fill"].astype(int)
    result["n_events"] = result["n_events"].astype(int)
    # otr stays float with potential None/NaN for zero-fill bins

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
        n_modify_mean     : float — cross-day mean MODIFY count per bin
        n_fill_mean       : float — cross-day mean FILL count per bin
        n_days            : int   — number of days contributing to this bin

    Notes
    -----
    Bins where otr is None (zero fills) are excluded from the cross-day
    mean via skipna=True (pandas default for mean/std). n_days counts
    all days that had at least one event in the bin, regardless of fills.
    """
    if not profiles:
        raise ValueError("profiles list is empty.")

    combined = pd.concat(profiles, ignore_index=True)

    agg = (
        combined
        .groupby("time_bin", sort=True)
        .agg(
            otr_mean=("otr", "mean"),             # skipna=True by default
            otr_std=("otr", "std"),               # cross-day variability
            n_add_mean=("n_add", "mean"),
            n_modify_mean=("n_modify", "mean"),
            n_fill_mean=("n_fill", "mean"),
            n_days=("n_events", "count"),
        )
        .reset_index()
    )

    return agg