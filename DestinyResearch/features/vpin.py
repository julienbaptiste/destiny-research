"""
vpin.py — Volume-Synchronized Probability of Informed Trading.

Atomic feature module — pure transformation, no I/O.
The caller is responsible for loading and filtering the table
(RTH or full-day) before passing it here.

VPIN measures the toxicity of order flow from the perspective of
liquidity providers. It quantifies adverse selection risk by measuring
the directional imbalance in trade flow over volume-synchronized buckets.

High VPIN indicates informed trading dominance (directional pressure,
news flow, adverse selection risk). Low VPIN indicates balanced noise
trading. VPIN is used for risk management, spread adjustment, and
liquidity provision decisions in market making.

Definition per Easley, López de Prado, O'Hara (2012)
----------------------------------------------------
1. Classify each trade as buy-side or sell-side initiated (aggressor side).
2. Partition trades into volume buckets of constant size V_bucket.
3. For each bucket i, compute order imbalance:
       OI[i] = |V_buy[i] - V_sell[i]| / V_total[i]
4. VPIN[i] = rolling mean of OI over n buckets (typically n=50).

VPIN ∈ [0, 1]:
    - VPIN = 0: perfect buy/sell balance (pure noise trading)
    - VPIN = 1: 100% unidirectional flow (extreme informed trading)
    - VPIN > 0.3-0.4: toxicity zone (empirical threshold)

Academic methodology — multi-day continuous calculation
-------------------------------------------------------
Per Easley et al. (2012) and Andersen & Bondarenko (2014), VPIN is
computed as a CONTINUOUS series across multiple days without daily resets.

Key principles:
1. RTH only: Regular trading hours are used (excludes overnight sessions).
   This is standard practice to ensure comparable liquidity regimes and
   participant bases across products.

2. Continuous bucketing: Volume buckets are numbered sequentially across
   all days in the analysis period. Day 1 produces buckets [0...N1],
   Day 2 produces buckets [N1+1...N2], etc.

3. Rolling window crosses session boundaries: VPIN[bucket i] uses the
   50-bucket window [i-49...i] regardless of whether these buckets span
   multiple trading days. Overnight gaps (no trading) are naturally
   handled — the window simply spans across them.

4. No daily reset: The toxicity regime persists between sessions. If the
   market closes with VPIN=0.45 (high toxicity), it typically reopens in
   a similar regime. Daily resets would create artificial discontinuities.

5. Warmup period: Only the first (window-1) buckets of the ENTIRE analysis
   period are in warmup (expanding window). All subsequent buckets, including
   those at the start of Day 2, Day 3, etc., have a full 50-bucket window.

Why this matters:
- Daily resets would make the first ~50 buckets of each day structurally
  biased low (incomplete window), creating false intraday patterns.
- VPIN measures regime persistence — informed trading doesn't reset at midnight.
- Cross-day comparability requires consistent window sizes.

This implementation follows the academic standard: get_vpin_range() computes
continuous VPIN across a date range. get_vpin() is provided for single-day
debugging only and includes a warning about its limitations.

Reference
---------
Easley, D., López de Prado, M., & O'Hara, M. (2012).
"Flow Toxicity and Liquidity in a High-frequency World."
The Review of Financial Studies, 25(5), 1457–1493.
    - Original VPIN paper. Uses E-mini S&P 500 continuous series over
      multiple months, RTH only, no daily resets.

Andersen, T. G., & Bondarenko, O. (2014).
"VPIN and the Flash Crash." Journal of Financial Markets, 17, 1–46.
    - Critique of VPIN's predictive power for crashes. Confirms the
      continuous multi-day methodology but shows lookahead bias issues
      in the original flash crash claims. VPIN remains useful as a
      descriptive toxicity measure, not a crash predictor.

VPIN vs OFI
-----------
OFI (Order Flow Imbalance):
    - Measures order book pressure (limit order submissions/cancellations).
    - Time-bucketed or tick-by-tick.
    - Predictive signal for forward returns (alpha).
    - Usage: directional trading signal.

VPIN (Volume-Synchronized Probability of Informed Trading):
    - Measures trade flow imbalance (executed trades, buy vs sell).
    - Volume-bucketed (normalized cross-market).
    - Toxicity / adverse selection measure.
    - Usage: risk management, spread widening, inventory control.

Both are complementary: OFI for taking positions, VPIN for protecting
against adverse selection.

Trade classification
--------------------
Uses TRADE events (market data prints), not FILL events.

TRADE vs FILL:
    - TRADE: atomic market execution (one per marketable order)
    - FILL: individual resting order fills (one TRADE can generate multiple FILLs)
    - TRADE.side: aggressor side (the liquidity taker)
    - FILL.side: passive side (the liquidity provider) — inverted from TRADE.side

VPIN uses TRADE events per academic methodology (Easley et al. 2012).

Side field interpretation for TRADE events:
    - side == 'ASK': aggressor lifted the ask → BUY-side initiated
    - side == 'BID': aggressor hit the bid → SELL-side initiated
    - side == 'NONE' or null: dropped (rare, logged as warning)

This aggressor flag is available from all exchanges in our normalized schema
(Databento CME/Eurex, HKEX OMD-D). More accurate than tick rule (Lee-Ready).

Volume bucketing
----------------
Buckets are synchronized on cumulative volume, not time. This makes VPIN
comparable across markets with different liquidity regimes.

Bucket size = bucket_pct × daily_total_volume (default 5%).
Typical result: ~20 buckets/day for constant liquidity markets,
~50-100 buckets/day for high-frequency markets.

Each bucket contains exactly V_bucket volume (last bucket may be smaller).

Typical usage
-------------
import DestinyResearch as dr
from DestinyResearch.features.vpin import get_vpin_range

# Multi-day continuous VPIN (recommended for research)
vpin_df = get_vpin_range("ES", "2025-10-01", "2025-10-31", 
                         bucket_pct=0.05, window=50)

# Single day VPIN (debugging only — has warmup artifacts)
vpin_df = get_vpin("ES", "2025-10-01", bucket_pct=0.05, window=50)
"""

from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc


def classify_trades(
    table: pa.Table,
) -> pa.Table:
    """Classify trades as buy-side or sell-side initiated via aggressor flag.

    Adds a 'signed_volume' column to the table:
        - side == 'ASK' (Ask aggressor)  → BUY  → signed_volume = +size
        - side == 'BID' (Bid aggressor)  → SELL → signed_volume = -size
        - side == 'NONE' or null         → row dropped

    Parameters
    ----------
    table : pa.Table
        MBO trades table. Must contain columns:
            action (string): 'TRADE' for market executions
            side   (string): 'ASK' or 'BID' — aggressor side
            size   (int64):  trade size in contracts

        Typically the output of dr.get_mbo_front_rth() filtered to
        action == 'TRADE'.

    Returns
    -------
    pa.Table
        Same schema as input plus 'signed_volume' (int64) column.
        Rows where side is 'NONE' or null are dropped.

    Notes
    -----
    - TRADE vs FILL: We use TRADE events (market data prints), not FILL events.
      TRADE represents the atomic market execution. FILL events represent the
      individual resting orders that were filled — a single TRADE can generate
      multiple FILLs. TRADE.side = aggressor side. FILL.side = passive side
      (inverted). Academic literature (Easley et al. 2012) uses trade prints.
    
    - Aggressor side interpretation (TRADE events):
        side == 'ASK' → aggressor lifted the ask → consumes ask liquidity
                     → BUY-side initiated (market buy)
        side == 'BID' → aggressor hit the bid → consumes bid liquidity
                     → SELL-side initiated (market sell)
    
    - This is consistent across Databento (CME, Eurex) and HKEX normalized schemas.
    
    - Rows with side == 'NONE' or null are rare (<0.1% empirically) and
      indicate exchange data anomalies or non-printable trades. These are
      dropped and logged as a warning if their proportion exceeds 1%.

    Example
    -------
    >>> tbl = dr.get_mbo_front_rth("ES", "2025-10-01")
    >>> trades = tbl.filter(pc.field("action") == "TRADE")
    >>> classified = classify_trades(trades)
    >>> classified.column_names
    [..., 'signed_volume']
    """
    # Validate required columns
    required = {"action", "side", "size"}
    missing = required - set(table.schema.names)
    if missing:
        raise ValueError(
            f"Table is missing required columns: {missing}. "
            f"Available columns: {table.schema.names}"
        )

    if len(table) == 0:
        # Return empty table with signed_volume column added
        schema_new = table.schema.append(pa.field("signed_volume", pa.int64()))
        return pa.table({name: pa.array([], type=field.type) 
                        for name, field in zip(schema_new.names, schema_new)})

    # Filter to TRADE events only
    mask_trade = pc.equal(table["action"], "TRADE")
    trades = table.filter(mask_trade)

    if len(trades) == 0:
        schema_new = trades.schema.append(pa.field("signed_volume", pa.int64()))
        return pa.table({name: pa.array([], type=field.type) 
                        for name, field in zip(schema_new.names, schema_new)})

    # ------------------------------------------------------------------
    # Classification logic (TRADE events):
    #   side == 'ASK' → aggressor lifted ask → BUY  → signed_volume = +size
    #   side == 'BID' → aggressor hit bid    → SELL → signed_volume = -size
    #   side == 'NONE' or null → drop
    # ------------------------------------------------------------------

    side_col = trades["side"]
    size_col = trades["size"]

    # Masks for valid side values
    mask_buy = pc.equal(side_col, "ASK")   # ASK aggressor → BUY-side initiated
    mask_sell = pc.equal(side_col, "BID")  # BID aggressor → SELL-side initiated
    mask_valid = pc.or_(mask_buy, mask_sell)

    # Check for invalid side values (None, '', or other)
    n_total = len(trades)
    n_valid = pc.sum(pc.cast(mask_valid, pa.int64())).as_py()
    n_invalid = n_total - n_valid

    if n_invalid > 0:
        pct_invalid = 100.0 * n_invalid / n_total
        if pct_invalid > 1.0:
            import warnings
            warnings.warn(
                f"classify_trades: {n_invalid}/{n_total} trades ({pct_invalid:.2f}%) "
                f"have invalid side field (not 'A' or 'B'). These trades are dropped. "
                f"Check data quality if this proportion is unexpectedly high.",
                UserWarning
            )

    # Filter to valid side only
    trades = trades.filter(mask_valid)
    side_col = trades["side"]
    size_col = trades["size"]
    mask_buy = pc.equal(side_col, "ASK")   # ASK aggressor → BUY
    mask_sell = pc.equal(side_col, "BID")  # BID aggressor → SELL

    # Cast size to int64 before negation (size is uint32, negate would wraparound)
    size_col_signed = pc.cast(size_col, pa.int64())

    # Compute signed_volume: +size for BUY, -size for SELL
    signed_volume = pc.if_else(
        mask_buy,
        size_col_signed,
        pc.negate(size_col_signed)
    )

    # Append signed_volume column to table
    trades = trades.append_column("signed_volume", signed_volume)

    return trades


def create_volume_buckets(
    trades_table: pa.Table,
    bucket_pct: float = 0.05,
) -> pd.DataFrame:
    """Partition trades into volume buckets of constant size.

    Volume bucketing normalizes VPIN across markets with different liquidity.
    Time buckets would bias VPIN (1 min during lunch ≠ 1 min at open).
    Volume buckets ensure each bucket represents the same "information flow"
    regardless of market speed.

    Parameters
    ----------
    trades_table : pa.Table
        Classified trades with 'signed_volume' column from classify_trades().
        Must contain:
            ts_event       (int64): exchange timestamp, nanoseconds UTC epoch
            signed_volume  (int64): +size for BUY, -size for SELL
    bucket_pct : float
        Target bucket size as percentage of total daily volume.
        Default 0.05 (5%) per Easley et al. (2012).
        Typical result: ~20 buckets/day for constant liquidity,
                        ~50-100 buckets/day for high-frequency markets.

    Returns
    -------
    pd.DataFrame with one row per bucket:
        bucket_id   : int          — sequential bucket number (0, 1, 2, ...)
        ts_last     : int64        — timestamp of last trade in bucket (ns)
        v_buy       : int64        — cumulative buy volume in bucket
        v_sell      : int64        — cumulative sell volume in bucket
        v_total     : int64        — total volume in bucket (v_buy + v_sell)
        n_trades    : int          — number of trades in bucket

    Notes
    -----
    - Bucket assignment: trades are assigned sequentially to buckets based
      on cumulative volume. When cumsum(|signed_volume|) crosses a bucket
      boundary, a new bucket starts.
    - Last bucket: may contain less than V_bucket if remaining volume < V_bucket.
    - ts_last: timestamp of the last trade in the bucket. Used for time series
      alignment with other metrics (OFI, spread, depth).
    - v_buy and v_sell are cumulative sums within each bucket. They always
      satisfy v_buy + v_sell == v_total.

    Example
    -------
    >>> classified = classify_trades(trades_tbl)
    >>> buckets = create_volume_buckets(classified, bucket_pct=0.05)
    >>> buckets.head()
       bucket_id            ts_last  v_buy  v_sell  v_total  n_trades
    0          0  1696118434123456789   4523    3891     8414       142
    1          1  1696118521987654321   4102    4312     8414       138
    """
    if bucket_pct <= 0 or bucket_pct > 1:
        raise ValueError(f"bucket_pct must be in (0, 1], got {bucket_pct}")

    # Validate required columns
    required = {"ts_event", "signed_volume"}
    missing = required - set(trades_table.schema.names)
    if missing:
        raise ValueError(
            f"Table is missing required columns: {missing}. "
            f"Available columns: {trades_table.schema.names}"
        )

    if len(trades_table) == 0:
        return pd.DataFrame(columns=[
            "bucket_id", "ts_last", "v_buy", "v_sell", "v_total", "n_trades"
        ])

    # ------------------------------------------------------------------
    # Volume bucketing via DuckDB.
    #
    # Steps:
    #   1. Compute total daily volume and bucket size.
    #   2. Assign bucket_id via floor(cumsum(abs(signed_volume)) / V_bucket).
    #   3. Group by bucket_id and aggregate:
    #      - ts_last: max(ts_event) in bucket
    #      - v_buy:   sum(signed_volume) where signed_volume > 0
    #      - v_sell:  sum(abs(signed_volume)) where signed_volume < 0
    #      - v_total: v_buy + v_sell
    #      - n_trades: count(*) in bucket
    #
    # DuckDB window functions allow this in a single pass without
    # materializing intermediate arrays.
    # ------------------------------------------------------------------

    # Compute total daily volume and bucket size
    total_volume = pc.sum(pc.abs(trades_table["signed_volume"])).as_py()
    bucket_size = int(total_volume * bucket_pct)

    if bucket_size == 0:
        raise ValueError(
            f"Computed bucket_size is zero. Total volume={total_volume}, "
            f"bucket_pct={bucket_pct}. Increase bucket_pct or check data."
        )

    con = duckdb.connect()
    con.register("trades", trades_table)

    sql = f"""
        WITH numbered AS (
            SELECT
                ts_event,
                signed_volume,
                ROW_NUMBER() OVER (ORDER BY ts_event) AS row_num
            FROM trades
        ),
        bucketed AS (
            SELECT
                ts_event,
                signed_volume,
                -- Assign bucket_id via cumulative volume
                CAST(
                    FLOOR(
                        SUM(ABS(signed_volume)) OVER (ORDER BY row_num) 
                        / {bucket_size}
                    ) AS INTEGER
                ) AS bucket_id
            FROM numbered
        )
        SELECT
            bucket_id,
            MAX(ts_event)                                           AS ts_last,
            SUM(signed_volume) FILTER (WHERE signed_volume > 0)     AS v_buy,
            SUM(ABS(signed_volume)) FILTER (WHERE signed_volume < 0) AS v_sell,
            SUM(ABS(signed_volume))                                 AS v_total,
            COUNT(*)                                                AS n_trades
        FROM bucketed
        GROUP BY bucket_id
        ORDER BY bucket_id
    """

    result = con.execute(sql).df()
    con.close()

    # Cast types explicitly (DuckDB may return float for sum of ints)
    result["bucket_id"] = result["bucket_id"].astype(int)
    result["ts_last"] = result["ts_last"].astype(np.int64)
    result["v_buy"] = result["v_buy"].fillna(0).astype(np.int64)
    result["v_sell"] = result["v_sell"].fillna(0).astype(np.int64)
    result["v_total"] = result["v_total"].astype(np.int64)
    result["n_trades"] = result["n_trades"].astype(int)

    return result


def compute_vpin(
    buckets_df: pd.DataFrame,
    window: int = 50,
) -> pd.DataFrame:
    """Compute rolling VPIN over volume buckets.

    VPIN[i] = rolling mean of order imbalance over n buckets.
    Order imbalance per bucket = |V_buy - V_sell| / V_total.

    Parameters
    ----------
    buckets_df : pd.DataFrame
        Output from create_volume_buckets(). Must contain:
            bucket_id : int
            v_buy     : int64 — buy volume in bucket
            v_sell    : int64 — sell volume in bucket
            v_total   : int64 — total volume in bucket
    window : int
        Rolling window size in buckets. Default 50 per Easley et al. (2012).
        Typical interpretation: if ~20 buckets/day, window=50 ≈ 2.5 days.

    Returns
    -------
    pd.DataFrame
        Same as buckets_df with two additional columns:
            order_imbalance : float — |v_buy - v_sell| / v_total per bucket
            vpin            : float — rolling mean(order_imbalance, window)

    Notes
    -----
    - order_imbalance ∈ [0, 1]:
        0 = perfect balance (v_buy == v_sell)
        1 = 100% unidirectional (all buy or all sell)
    - vpin ∈ [0, 1]: rolling average of order_imbalance.
    - First (window-1) buckets: expanding window is used to avoid NaN.
      E.g., bucket 0 has vpin = OI[0], bucket 1 has vpin = mean(OI[0], OI[1]),
      bucket 49 has vpin = mean(OI[0:49]), bucket 50+ has full window.
    - Division by zero: v_total == 0 should never occur (create_volume_buckets
      ensures v_total > 0), but if it does, order_imbalance = NaN and propagates.

    Example
    -------
    >>> buckets = create_volume_buckets(classified, bucket_pct=0.05)
    >>> vpin_df = compute_vpin(buckets, window=50)
    >>> vpin_df[["bucket_id", "order_imbalance", "vpin"]].head()
       bucket_id  order_imbalance      vpin
    0          0         0.073845  0.073845
    1          1         0.025123  0.049484
    2          2         0.112456  0.070475
    """
    if window <= 0:
        raise ValueError(f"window must be positive, got {window}")

    # Validate required columns
    required = {"bucket_id", "v_buy", "v_sell", "v_total"}
    missing = required - set(buckets_df.columns)
    if missing:
        raise ValueError(
            f"DataFrame is missing required columns: {missing}. "
            f"Available columns: {list(buckets_df.columns)}"
        )

    if len(buckets_df) == 0:
        return buckets_df.assign(order_imbalance=pd.Series(dtype=float),
                                 vpin=pd.Series(dtype=float))

    # ------------------------------------------------------------------
    # Order imbalance per bucket: |V_buy - V_sell| / V_total
    # ------------------------------------------------------------------
    df = buckets_df.copy()
    df["order_imbalance"] = (
        (df["v_buy"] - df["v_sell"]).abs() / df["v_total"]
    )

    # ------------------------------------------------------------------
    # VPIN: rolling mean of order_imbalance over window buckets.
    #
    # Use expanding window for first (window-1) buckets to avoid NaN warmup.
    # After warmup, use fixed rolling window.
    # ------------------------------------------------------------------
    df["vpin"] = (
        df["order_imbalance"]
        .rolling(window=window, min_periods=1)  # min_periods=1 → expanding warmup
        .mean()
    )

    return df


def get_vpin(
    product: str,
    date: str,
    session: str = "default",
    bucket_pct: float = 0.05,
    window: int = 50,
) -> pd.DataFrame:
    """Compute VPIN for a single trading day — FOR DEBUGGING ONLY.

    WARNING: This function computes VPIN on a single day with expanding
    warmup window. The first (window-1) buckets use an incomplete rolling
    window, and VPIN resets at the start of each day.

    For research and analysis, use get_vpin_range() instead, which computes
    continuous VPIN across multiple days following the academic methodology
    (Easley et al. 2012).

    This function is provided for quick single-day debugging and exploration only.

    Loads MBO trades RTH, classifies buy/sell via aggressor flag,
    creates volume buckets, computes rolling VPIN with expanding warmup.

    Parameters
    ----------
    product : str
        Product ticker (e.g., 'ES', 'NIY', 'HSI').
    date : str
        Trading date 'YYYY-MM-DD'.
    session : str
        RTH session ('default', 'asia', 'us'). Default 'default'.
        For NIY/NKD, use 'asia' for OSE session or 'us' for CME session.
    bucket_pct : float
        Volume bucket size as percentage of daily volume. Default 0.05 (5%).
    window : int
        VPIN rolling window in buckets. Default 50.

    Returns
    -------
    pd.DataFrame
        VPIN time series with columns:
            bucket_id        : int
            ts_last          : int64  — timestamp of last trade in bucket (ns)
            v_buy            : int64  — buy volume in bucket
            v_sell           : int64  — sell volume in bucket
            v_total          : int64  — total volume in bucket
            n_trades         : int    — number of trades in bucket
            order_imbalance  : float  — |v_buy - v_sell| / v_total
            vpin             : float  — rolling mean(order_imbalance, window)

    Notes
    -----
    - Empty session: if no trades exist for the date/session, returns empty
      DataFrame with correct schema.
    - Warmup period: first (window-1) buckets use expanding window (incomplete).
      E.g., bucket 0 has window=1, bucket 49 has window=50, bucket 50+ have
      full window=50. This is an artifact of single-day calculation.
    - VPIN interpretation:
        VPIN < 0.2: low toxicity, balanced noise trading
        VPIN 0.2-0.4: moderate toxicity, some directional pressure
        VPIN > 0.4: high toxicity, informed trading dominance
      (Thresholds are empirical and market-dependent.)
    - Memory: loads one day of MBO trades (~500 MB for ES) → peak RAM well
      within 16 GB budget. Buckets are ~100-200 rows → negligible.

    Example
    -------
    >>> # Quick single-day check (debugging)
    >>> vpin_df = get_vpin("ES", "2025-10-01")
    >>> vpin_df[["bucket_id", "vpin"]].tail()
        bucket_id      vpin
    95         95  0.287654
    96         96  0.291023
    97         97  0.285432
    98         98  0.278901
    99         99  0.273456
    """
    import DestinyResearch as dr

    # Load MBO trades RTH
    tbl = dr.get_mbo_front_rth(product, date, session=session)

    if len(tbl) == 0:
        return pd.DataFrame(columns=[
            "bucket_id", "ts_last", "v_buy", "v_sell", "v_total", 
            "n_trades", "order_imbalance", "vpin"
        ])

    # Step 1: Classify trades (adds signed_volume column)
    classified = classify_trades(tbl)

    if len(classified) == 0:
        return pd.DataFrame(columns=[
            "bucket_id", "ts_last", "v_buy", "v_sell", "v_total", 
            "n_trades", "order_imbalance", "vpin"
        ])

    # Step 2: Create volume buckets
    buckets = create_volume_buckets(classified, bucket_pct=bucket_pct)

    if len(buckets) == 0:
        return pd.DataFrame(columns=[
            "bucket_id", "ts_last", "v_buy", "v_sell", "v_total", 
            "n_trades", "order_imbalance", "vpin"
        ])

    # Step 3: Compute VPIN
    vpin_df = compute_vpin(buckets, window=window)

    return vpin_df


def get_vpin_range(
    product: str,
    start_date: str,
    end_date: str,
    session: str = "default",
    bucket_pct: float = 0.05,
    window: int = 50,
) -> pd.DataFrame:
    """Compute continuous VPIN over a date range — RECOMMENDED FOR RESEARCH.

    This function implements the academic standard methodology (Easley et al. 2012):
    - Loads RTH trades for all days in [start_date, end_date]
    - Creates volume buckets sequentially across all days (no daily reset)
    - Computes rolling VPIN with window=50 buckets spanning session boundaries
    - Only the first (window-1) buckets of the ENTIRE range are in warmup

    This produces a continuous VPIN series where toxicity regimes persist
    across overnight gaps, matching the way informed trading actually behaves
    in the market.

    Parameters
    ----------
    product : str
        Product ticker (e.g., 'ES', 'NIY', 'HSI').
    start_date : str
        Start date inclusive, 'YYYY-MM-DD'.
    end_date : str
        End date inclusive, 'YYYY-MM-DD'.
    session : str
        RTH session ('default', 'asia', 'us'). Default 'default'.
        For NIY/NKD, use 'asia' for OSE session or 'us' for CME session.
    bucket_pct : float
        Volume bucket size as percentage of DAILY volume. Default 0.05 (5%).
        Note: bucket size is computed per day, then trades are bucketed
        sequentially across all days. This normalizes for daily volume
        variations while maintaining continuous bucketing.
    window : int
        VPIN rolling window in buckets. Default 50.

    Returns
    -------
    pd.DataFrame
        Continuous VPIN time series with columns:
            date             : str    — trading date 'YYYY-MM-DD'
            bucket_id_global : int    — sequential bucket number across all days
            bucket_id_daily  : int    — bucket number within the day (0, 1, 2, ...)
            ts_last          : int64  — timestamp of last trade in bucket (ns)
            v_buy            : int64  — buy volume in bucket
            v_sell           : int64  — sell volume in bucket
            v_total          : int64  — total volume in bucket
            n_trades         : int    — number of trades in bucket
            order_imbalance  : float  — |v_buy - v_sell| / v_total
            vpin             : float  — rolling mean(order_imbalance, window)
            is_warmup        : bool   — True for first (window-1) buckets of range

    Notes
    -----
    - RTH only: Regular trading hours are used to ensure comparable liquidity
      and participant base across products. Overnight sessions are excluded.
    - Overnight gaps: The rolling window naturally spans overnight gaps. For
      example, if Day 1 ends at bucket 99 and Day 2 starts at bucket 100,
      VPIN[bucket 110] uses buckets [61...110], which includes 39 buckets
      from Day 1 and 11 buckets from Day 2. This is correct — toxicity
      regimes persist between sessions.
    - Warmup period: Only buckets 0 through (window-1) have is_warmup=True.
      All subsequent buckets, including those at the start of Day 2, Day 3, etc.,
      have a full 50-bucket window and is_warmup=False.
    - Bucket size normalization: bucket_pct is applied to each day's total
      volume separately to account for daily volume variations (e.g., holidays,
      news events). This ensures roughly consistent bucket counts per day while
      maintaining continuous sequential bucketing.
    - Memory: loads all days' MBO trades sequentially (one day at a time),
      concatenates buckets (few hundred rows per day → few thousand total).
      Peak RAM = single day MBO (~500 MB) + concatenated buckets (~1 MB).
      Well within 16 GB budget even for year-long ranges.
    - Empty days: if a day has no trades (holiday, data gap), it is skipped
      and the bucket sequence continues from the previous valid day.

    Example
    -------
    >>> # Multi-month continuous VPIN (recommended for research)
    >>> vpin_df = get_vpin_range("ES", "2025-10-01", "2025-10-31")
    >>> 
    >>> # Check warmup period
    >>> vpin_df[vpin_df["is_warmup"]]
       date  bucket_id_global  vpin  is_warmup
    0  2025-10-01             0  0.15       True
    ...
    48 2025-10-01            48  0.23       True
    >>> 
    >>> # Analyze per-day VPIN statistics (no warmup bias)
    >>> daily_stats = (
    ...     vpin_df[~vpin_df["is_warmup"]]
    ...     .groupby("date")["vpin"]
    ...     .agg(["mean", "max", "std"])
    ... )
    >>> 
    >>> # Plot continuous VPIN across all days
    >>> import matplotlib.pyplot as plt
    >>> plt.plot(vpin_df["bucket_id_global"], vpin_df["vpin"])
    >>> plt.xlabel("Bucket ID (continuous)")
    >>> plt.ylabel("VPIN")
    >>> plt.axhline(0.4, color="red", linestyle="--", label="High toxicity")
    >>> plt.legend()
    >>> plt.show()
    """
    import DestinyResearch as dr

    # Get available dates in range
    all_dates = dr.get_available_dates(product)
    dates = [d for d in all_dates if start_date <= d <= end_date]

    if not dates:
        return pd.DataFrame(columns=[
            "date", "bucket_id_global", "bucket_id_daily", "ts_last",
            "v_buy", "v_sell", "v_total", "n_trades",
            "order_imbalance", "vpin", "is_warmup"
        ])

    # ------------------------------------------------------------------
    # Load and classify trades for all days, create per-day buckets.
    # Bucket size is computed per day (bucket_pct × daily volume) to
    # normalize for daily volume variations, but bucket IDs are sequential
    # across all days (no reset).
    # ------------------------------------------------------------------

    all_buckets = []
    bucket_id_offset = 0

    for date in dates:
        # Load MBO trades RTH for this day
        tbl = dr.get_mbo_front_rth(product, date, session=session)

        if len(tbl) == 0:
            continue  # Skip empty days (holidays, data gaps)

        # Classify trades (adds signed_volume column)
        classified = classify_trades(tbl)

        if len(classified) == 0:
            continue  # Skip if all trades had invalid side

        # Create volume buckets for this day
        # Bucket size is computed from this day's volume, but bucket_id
        # is offset by the cumulative count from previous days
        buckets = create_volume_buckets(classified, bucket_pct=bucket_pct)

        if len(buckets) == 0:
            continue  # Should not happen, but handle gracefully

        # Add date and adjust bucket_id to be globally sequential
        buckets["date"] = date
        buckets["bucket_id_daily"] = buckets["bucket_id"]  # Save original per-day ID
        buckets["bucket_id_global"] = buckets["bucket_id"] + bucket_id_offset
        buckets = buckets.drop(columns=["bucket_id"])

        all_buckets.append(buckets)
        bucket_id_offset += len(buckets)

    if not all_buckets:
        return pd.DataFrame(columns=[
            "date", "bucket_id_global", "bucket_id_daily", "ts_last",
            "v_buy", "v_sell", "v_total", "n_trades",
            "order_imbalance", "vpin", "is_warmup"
        ])

    # ------------------------------------------------------------------
    # Concatenate all per-day buckets into a single continuous series.
    # Compute VPIN on this continuous series — the rolling window will
    # naturally span overnight gaps between sessions.
    # ------------------------------------------------------------------

    buckets_continuous = pd.concat(all_buckets, ignore_index=True)

    # Compute order imbalance per bucket
    buckets_continuous["order_imbalance"] = (
        (buckets_continuous["v_buy"] - buckets_continuous["v_sell"]).abs() 
        / buckets_continuous["v_total"]
    )

    # Compute VPIN as rolling mean over the continuous series
    # Use expanding window for first (window-1) buckets, then fixed window
    buckets_continuous["vpin"] = (
        buckets_continuous["order_imbalance"]
        .rolling(window=window, min_periods=1)
        .mean()
    )

    # Add warmup flag: True for first (window-1) buckets only
    buckets_continuous["is_warmup"] = (
        buckets_continuous["bucket_id_global"] < (window - 1)
    )

    # Reorder columns for readability
    buckets_continuous = buckets_continuous[[
        "date", "bucket_id_global", "bucket_id_daily", "ts_last",
        "v_buy", "v_sell", "v_total", "n_trades",
        "order_imbalance", "vpin", "is_warmup"
    ]]

    return buckets_continuous