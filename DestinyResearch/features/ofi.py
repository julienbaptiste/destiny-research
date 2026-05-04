"""
ofi.py — Order Flow Imbalance (OFI) computation from MBP-1 data.

Implements the Cont, Kukanov & Stoikov (2014) definition of OFI:
    OFI(t-Δ, t) = Σ [ ΔBid_size × I(bid_px unchanged) 
                    - ΔAsk_size × I(ask_px unchanged) ]

where the sum is over all MBP-1 updates in the interval [t-Δ, t].

OFI captures the net order flow pressure at the best bid/ask:
- Positive OFI → buying pressure (bid size increased or ask size decreased)
- Negative OFI → selling pressure (ask size increased or bid size decreased)

Typical usage
-------------
import DestinyResearch as dr
from DestinyResearch.features.ofi import compute_ofi_bucketed, align_ofi_midprice_returns

# Load one day MBP-1 (RTH)
cols = ["ts_recv", "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00"]
tbl = dr.get_mbp1_front_rth("ES", "2025-10-10", columns=cols)

# Compute OFI bucketed at 1s intervals
ofi_df = compute_ofi_bucketed(tbl, window_seconds=1)

# Align with forward returns (1s, 5s, 30s horizons)
analysis_df = align_ofi_midprice_returns(
    ofi_df, 
    forward_horizons=[1, 5, 30],
    tick_size_pts=0.25  # ES tick size
)

# IC computation (separate, in notebook)
ic_1s = analysis_df[["ofi", "fwd_return_1s"]].corr().iloc[0, 1]

Atomic feature module — pure transformation, no I/O.
The caller loads and filters MBP-1 data before passing it here.
"""

from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc


def compute_ofi_events(table: pa.Table) -> pa.Table:
    """Compute event-by-event OFI contributions from MBP-1 snapshots.
    
    Implements the Cont et al. (2014) OFI formula per event:
        ofi_contribution = ΔBid_size × I(bid_px unchanged) 
                         - ΔAsk_size × I(ask_px unchanged)
    
    where:
        ΔBid_size = bid_sz_00[i] - bid_sz_00[i-1]
        ΔAsk_size = ask_sz_00[i] - ask_sz_00[i-1]
        I(bid_px unchanged) = 1 if bid_px_00[i] == bid_px_00[i-1], else 0
        I(ask_px unchanged) = 1 if ask_px_00[i] == ask_px_00[i-1], else 0
    
    Price change filtering is critical: if the bid price moves up, the size change
    at the old price level is not meaningful — the book structure changed. OFI
    only measures flow *at the current best price*.
    
    This function is primarily for debugging and LOB inspection. For IC analysis,
    use compute_ofi_bucketed() which aggregates OFI over fixed time windows.
    
    Parameters
    ----------
    table : pa.Table
        MBP-1 Arrow table sorted by ts_recv (ascending). Must contain:
            ts_recv   : uint64 (nanoseconds UTC epoch)
            bid_px_00 : int64 (fixed-point price × 1e9)
            ask_px_00 : int64 (fixed-point price × 1e9)
            bid_sz_00 : uint32 (size in contracts)
            ask_sz_00 : uint32 (size in contracts)
        
        Typically the output of dr.get_mbp1_front_rth() with minimal columns.
        No additional filtering is applied here — caller controls the time scope.
    
    Returns
    -------
    pa.Table
        Original table with one additional column:
            ofi_contribution : int64
                Signed OFI contribution of this event. Sum over a time window
                gives the net order flow imbalance for that window.
        
        First row always has ofi_contribution = 0 (no previous state to diff).
    
    Notes
    -----
    - MBP-1 prices are fixed-point int64 (price × 1e9). We compare them directly
      without dividing by 1e9 since we only care about equality (px[i] == px[i-1]).
    - Sizes are uint32 in contracts. OFI is computed in contracts (not notional).
    - This function does NOT aggregate OFI over time — it returns one OFI value
      per MBP-1 update. Use compute_ofi_bucketed() for time-bucketed OFI.
    - MBP-1 snapshots where bid/ask/size are unchanged contribute zero to OFI
      by construction (ΔSize = 0). These rows are kept in the output but do not
      bias the aggregated OFI — filtering them would destabilize bucket alignment.
    
    Examples
    --------
    >>> import DestinyResearch as dr
    >>> from DestinyResearch.features.ofi import compute_ofi_events
    >>> 
    >>> cols = ["ts_recv", "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00"]
    >>> tbl = dr.get_mbp1_front_rth("ES", "2025-10-10", columns=cols)
    >>> ofi_tbl = compute_ofi_events(tbl)
    >>> 
    >>> # Inspect OFI around a specific timestamp
    >>> df = ofi_tbl.to_pandas()
    >>> df[(df.ts_recv >= target_ts - 1e9) & (df.ts_recv <= target_ts + 1e9)]
    """
    
    # Convert to pandas for diff() — pyarrow compute doesn't have diff yet
    # This is acceptable: the function is for debugging, not production batch processing
    df = table.to_pandas()
    
    # Compute price and size deltas
    df["bid_px_prev"] = df["bid_px_00"].shift(1)
    df["ask_px_prev"] = df["ask_px_00"].shift(1)
    df["bid_sz_prev"] = df["bid_sz_00"].shift(1)
    df["ask_sz_prev"] = df["ask_sz_00"].shift(1)
    
    # Price unchanged indicators
    df["bid_px_unchanged"] = (df["bid_px_00"] == df["bid_px_prev"]).astype(int)
    df["ask_px_unchanged"] = (df["ask_px_00"] == df["ask_px_prev"]).astype(int)
    
    # Size deltas (NaN on first row)
    df["delta_bid_sz"] = df["bid_sz_00"] - df["bid_sz_prev"]
    df["delta_ask_sz"] = df["ask_sz_00"] - df["ask_sz_prev"]
    
    # OFI contribution = ΔBid × I(bid_px unchanged) - ΔAsk × I(ask_px unchanged)
    df["ofi_contribution"] = (
        df["delta_bid_sz"].fillna(0) * df["bid_px_unchanged"]
        - df["delta_ask_sz"].fillna(0) * df["ask_px_unchanged"]
    ).astype("int64")
    
    # Drop temporary columns
    df = df.drop(columns=[
        "bid_px_prev", "ask_px_prev", "bid_sz_prev", "ask_sz_prev",
        "bid_px_unchanged", "ask_px_unchanged", "delta_bid_sz", "delta_ask_sz"
    ])
    
    return pa.Table.from_pandas(df, schema=table.schema.append(
        pa.field("ofi_contribution", pa.int64())
    ))


def compute_ofi_bucketed(
    table: pa.Table,
    window_seconds: int = 1,
) -> pd.DataFrame:
    """Aggregate OFI over fixed-width time windows using DuckDB.
    
    This is the primary OFI computation function for IC analysis. It buckets
    the MBP-1 stream into fixed-width intervals and computes the net OFI per bucket.
    
    Bucketing strategy
    ------------------
    Time bins are aligned to Unix epoch (1970-01-01 00:00:00 UTC):
        bucket_id = floor(ts_recv / (window_seconds × 1e9))
    
    This ensures deterministic bin boundaries across products and sessions.
    For intraday analysis, the absolute bucket_id is not meaningful — only the
    relative ordering matters. The returned DataFrame includes both bucket_id
    and bucket_start_ns for downstream join operations.
    
    OFI aggregation
    ---------------
    For each bucket:
        ofi = Σ [ ΔBid_size × I(bid_px unchanged) - ΔAsk_size × I(ask_px unchanged) ]
    
    where the sum is over all MBP-1 updates in that bucket.
    
    Additionally computed per bucket:
        - n_updates : number of MBP-1 snapshots (gauge of book activity)
        - mid_start : mid-price at bucket start (for forward returns alignment)
        - mid_end   : mid-price at bucket end (for forward returns alignment)
    
    Mid-price is computed as (bid_px_00 + ask_px_00) / 2 in index points (÷ 1e9).
    
    Parameters
    ----------
    table : pa.Table
        MBP-1 Arrow table sorted by ts_recv. Must contain:
            ts_recv   : uint64 (nanoseconds UTC epoch)
            bid_px_00 : int64 (fixed-point price × 1e9)
            ask_px_00 : int64 (fixed-point price × 1e9)
            bid_sz_00 : uint32 (size in contracts)
            ask_sz_00 : uint32 (size in contracts)
    
    window_seconds : int
        Width of each time bucket in seconds. Standard values:
            1   — baseline for IC analysis (Cont et al. 2014)
            5   — robustness check
            30  — robustness check
            60  — 1-minute bucket (standard in practitioner literature)
            300 — 5-minute bucket (institutional execution window)
    
    Returns
    -------
    pd.DataFrame
        One row per non-empty time bucket, columns:
            bucket_id       : int64 — bucket index (deterministic across products)
            bucket_start_ns : int64 — bucket start timestamp (Unix ns)
            ofi             : int64 — net order flow imbalance (contracts)
            n_updates       : int64 — number of MBP-1 snapshots in bucket
            mid_start       : float64 — mid-price at bucket start (index points)
            mid_end         : float64 — mid-price at bucket end (index points)
        
        Sorted by bucket_id ascending. Empty buckets (no MBP-1 updates) are not included.
    
    Notes
    -----
    - Uses DuckDB for memory-efficient aggregation. The MBP-1 table is scanned
      in Arrow format (zero-copy from PyArrow), bucketed in SQL, and the result
      is returned as a small Pandas DataFrame.
    - Peak RAM ≈ size of input table (Arrow) + result DataFrame (negligible).
    - For a typical RTH session: 1s buckets → ~23K rows, 5s → ~4.6K rows.
    - OFI is computed in contracts, not notional. For cross-market comparison,
      scale by contract multiplier and price level if needed (done in notebook).
    - MBP-1 snapshots where bid/ask/size are unchanged contribute zero to OFI
      but are counted in n_updates. This is correct: they indicate book activity
      (quoting behind the best) even if they don't change the OFI.
    
    Examples
    --------
    >>> import DestinyResearch as dr
    >>> from DestinyResearch.features.ofi import compute_ofi_bucketed
    >>> 
    >>> cols = ["ts_recv", "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00"]
    >>> tbl = dr.get_mbp1_front_rth("ES", "2025-10-10", columns=cols)
    >>> 
    >>> # 1s OFI buckets (baseline)
    >>> ofi_1s = compute_ofi_bucketed(tbl, window_seconds=1)
    >>> print(f"Mean OFI: {ofi_1s['ofi'].mean():.2f}, Std: {ofi_1s['ofi'].std():.2f}")
    >>> 
    >>> # 5s OFI buckets (robustness check)
    >>> ofi_5s = compute_ofi_bucketed(tbl, window_seconds=5)
    """
    
    # First compute event-level OFI contributions
    ofi_tbl = compute_ofi_events(table)
    
    # DuckDB aggregation query
    query = f"""
    WITH bucketed AS (
        SELECT
            CAST(ts_recv / ({window_seconds} * 1000000000) AS BIGINT) AS bucket_id,
            ts_recv,
            ofi_contribution,
            (bid_px_00 + ask_px_00) / 2.0e9 AS mid_price
        FROM ofi_tbl
    )
    SELECT
        bucket_id,
        bucket_id * {window_seconds} * 1000000000 AS bucket_start_ns,
        SUM(ofi_contribution) AS ofi,
        COUNT(*) AS n_updates,
        FIRST(mid_price) AS mid_start,
        LAST(mid_price) AS mid_end
    FROM bucketed
    GROUP BY bucket_id
    ORDER BY bucket_id
    """
    
    return duckdb.query(query).to_df()


def align_ofi_midprice_returns(
    ofi_df: pd.DataFrame,
    forward_horizons: list[int],
    tick_size_pts: float,
) -> pd.DataFrame:
    """Align OFI buckets with forward mid-price returns for IC analysis.
    
    For each OFI bucket at time t, compute forward returns over multiple horizons:
        return[t, t+Δ] = log(mid[t+Δ] / mid[t])
    
    where mid[t] is the mid-price at the end of bucket t, and mid[t+Δ] is the
    mid-price Δ seconds later (bucket_end_ns).
    
    Forward lookback validation
    ---------------------------
    For each horizon Δ, we check that mid[t+Δ] exists in the data. If the
    target bucket does not exist (session end, data gap), the return is set to NaN.
    
    No forward-filling across sessions. No extrapolation. If the data ends,
    returns for the last N buckets will be NaN (where N depends on the longest horizon).
    
    Log returns justification
    -------------------------
    We use log returns rather than arithmetic returns for three reasons:
    1. Symmetry: +10% and -10% moves have symmetric log returns (±0.0953),
       but asymmetric arithmetic returns (+0.1 vs -0.1).
    2. Outlier resistance: large price moves (e.g. on news) don't dominate the IC.
    3. Academic standard: Cont et al. (2014), Eisler et al. (2012), and most
       microstructure literature use log returns for OFI predictability analysis.
    
    Parameters
    ----------
    ofi_df : pd.DataFrame
        Output of compute_ofi_bucketed(). Must contain:
            bucket_id       : int64
            bucket_start_ns : int64
            ofi             : int64
            mid_end         : float64 (mid-price at bucket end, in index points)
    
    forward_horizons : list[int]
        List of forward horizons in seconds. For each horizon Δ, a column
        f"fwd_return_{Δ}s" is added to the output.
        
        Standard values:
            [1, 5, 30, 60, 300]  — 1s, 5s, 30s, 1min, 5min
        
        Horizons should match the bucketing windows used in compute_ofi_bucketed()
        for interpretability (e.g. if OFI was bucketed at 1s, use horizon=1s for IC).
    
    tick_size_pts : float
        Product tick size in index points (for debugging/validation only).
        Not used in the return computation — included for consistency with
        other feature modules and to enable tick-normalized returns if needed later.
    
    Returns
    -------
    pd.DataFrame
        ofi_df with additional columns:
            fwd_return_1s  : float64 — log return t → t+1s (if 1 in forward_horizons)
            fwd_return_5s  : float64 — log return t → t+5s (if 5 in forward_horizons)
            fwd_return_30s : float64 — log return t → t+30s (if 30 in forward_horizons)
            ...
        
        Returns are in log-space (dimensionless). To convert to basis points:
            return_bps = fwd_return_Xs * 10000
        
        Rows with NaN returns (session end, data gaps) are kept in the DataFrame.
        The IC computation in the notebook should drop NaNs before correlation.
    
    Notes
    -----
    - This function does NOT compute IC — it only aligns OFI with future returns.
      IC calculation (Spearman or Pearson) is done in the notebook, typically
      as a rolling 1-day IC to track alpha decay over time.
    - Forward return lookback is strict: if bucket t+Δ does not exist in ofi_df,
      the return is NaN. This is correct behavior at session boundaries.
    - For robustness checks: compute IC on multiple horizons (1s, 5s, 30s, 1min)
      to see if OFI alpha decays sub-minute or persists longer.
    
    Examples
    --------
    >>> from DestinyResearch.features.ofi import compute_ofi_bucketed, align_ofi_midprice_returns
    >>> 
    >>> # Compute 1s OFI buckets
    >>> ofi_df = compute_ofi_bucketed(tbl, window_seconds=1)
    >>> 
    >>> # Align with 1s, 5s, 30s forward returns
    >>> analysis_df = align_ofi_midprice_returns(
    ...     ofi_df,
    ...     forward_horizons=[1, 5, 30],
    ...     tick_size_pts=0.25  # ES
    ... )
    >>> 
    >>> # Compute IC (in notebook)
    >>> ic_1s = analysis_df[["ofi", "fwd_return_1s"]].corr().iloc[0, 1]
    >>> print(f"IC (1s horizon): {ic_1s:.4f}")
    """
    
    df = ofi_df.copy()
    
    # Bucket width in seconds (inferred from bucket_id spacing)
    # Assumes uniform bucketing — valid since compute_ofi_bucketed() uses fixed windows
    if len(df) < 2:
        raise ValueError("ofi_df must contain at least 2 rows to infer bucket width")
    
    bucket_width_s = int((df["bucket_start_ns"].iloc[1] - df["bucket_start_ns"].iloc[0]) / 1e9)
    
    for horizon_s in forward_horizons:
        # Number of buckets to shift forward
        shift_buckets = horizon_s // bucket_width_s
        
        if shift_buckets < 1:
            raise ValueError(
                f"Forward horizon {horizon_s}s is shorter than bucket width {bucket_width_s}s. "
                f"Increase horizon or decrease bucket width."
            )
        
        # Forward mid-price (shift mid_end by shift_buckets rows)
        df[f"mid_fwd_{horizon_s}s"] = df["mid_end"].shift(-shift_buckets)
        
        # Log return: log(mid[t+Δ] / mid[t])
        # If mid_fwd is NaN (session end / data gap), return is NaN
        df[f"fwd_return_{horizon_s}s"] = np.log(
            df[f"mid_fwd_{horizon_s}s"] / df["mid_end"]
        )
        
        # Drop the intermediate mid_fwd column (not needed in output)
        df = df.drop(columns=[f"mid_fwd_{horizon_s}s"])
    
    return df