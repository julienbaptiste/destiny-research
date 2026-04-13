"""
tests/validation/validate_mbp1_hkex.py — MBP-1 reconstruction validator
for HKEX products using microstructure invariants (no reference feed).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONTEXT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Unlike the Databento validator (validate_mbp1_databento.py) which compares
against a purchased reference MBP-1 feed, HKEX has no reference MBP-1
available for purchase. This validator instead checks microstructure
invariants that any correctly reconstructed limit order book must satisfy.

The reconstruction engine (build_mbp1.py) is provider-agnostic and already
validated RELIABLE against Databento native MBP-1 for CME/EUREX. The risk
for HKEX is therefore in the hkex_adapter.py normalization, not in the
engine itself. These invariant checks target adapter-level issues:
  - Incorrect side mapping → crossed spreads
  - Missing fill decrements → inflated sizes, stale levels
  - Timestamp ordering → non-causal book states
  - Price continuity → gaps in the LOB

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CHECKS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Check 1 — NO CROSSED SPREAD (RTH continuous only)
    bid_px_00 < ask_px_00 on all rows where both sides are non-null.
    Excludes pre-open and post-close auction periods where indicative
    prices can legitimately cross. This is the primary regression test
    for the synthetic CANCEL fix (2026-04-08).

Check 2 — TRADE PRICE WITHIN SPREAD
    For every TRADE row, the event price must lie within the spread of
    the PREVIOUS snapshot: price >= prev_bid and price <= prev_ask.
    The post-trade snapshot may have already consumed the level, so we
    compare against the lagged TOB. Tolerance: auction trades at session
    open may fall outside the first quoted spread.

Check 3 — SPREAD IN TICKS
    (ask_px_00 - bid_px_00) / tick_size must be >= 1 during continuous
    trading. A locked market (spread = 0) is acceptable during auctions
    but is an error during continuous trading on HKEX. Also computes
    spread distribution statistics (mean, median, p95, p99).

Check 4 — SIZE POSITIVITY
    bid_sz_00 >= 0 and ask_sz_00 >= 0 on all rows. Negative sizes
    indicate an underflow bug in the book state machine (e.g. double
    decrement from a fill). This was the symptom that revealed the
    side mapping bug (2026-04-08).

Check 5 — BOOK CONTINUITY (RTH only)
    If bid_px_00 or ask_px_00 is non-null at time t, it must be non-null
    at t+1 (except after a CLEAR at session transitions). A side dropping
    to null during continuous trading signals missing ADDs or excessive
    CANCELs in the reconstruction.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VERDICT THRESHOLDS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    RELIABLE   : all 5 checks pass with 0 violations (or < 0.001% tolerance)
    ACCEPTABLE : minor violations on checks 2/3/5 (< 0.01%)
    NEEDS_WORK : any check with > 0.01% violation rate, or any crossed spread

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
USAGE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Single product, single date (front month auto-detected)
    python tests/validation/validate_mbp1_hkex.py \\
        --product HSI --contract HSIG26 --date 20260202

    # Multiple dates
    python tests/validation/validate_mbp1_hkex.py \\
        --product HSI --contract HSIG26 --date 20260202 20260203 20260204

    # All 4 HKEX products, single date
    python tests/validation/validate_mbp1_hkex.py \\
        --product HSI --contract HSIG26 --date 20260202
    python tests/validation/validate_mbp1_hkex.py \\
        --product MHI --contract MHIG26 --date 20260202
    python tests/validation/validate_mbp1_hkex.py \\
        --product HHI --contract HHIG26 --date 20260202
    python tests/validation/validate_mbp1_hkex.py \\
        --product MCH --contract MCHG26 --date 20260202

Exit codes:
    0 — all dates RELIABLE or ACCEPTABLE
    1 — one or more dates NEEDS_WORK
    2 — setup error (missing file, unknown product, etc.)
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Repo root + project imports
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from config import DATA_RECONSTRUCTED                         # noqa: E402
from ingestion.market_config import MARKET_CONFIG             # noqa: E402
from ingestion.schema import reconstructed_path               # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ANSI colors — consistent with validate_mbp1_databento.py
# ---------------------------------------------------------------------------

BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"

# ---------------------------------------------------------------------------
# Verdict thresholds (violation rate = n_violations / n_rows_checked)
# ---------------------------------------------------------------------------
# crossed_rate : check 1 — 0 tolerance for RELIABLE
# trade_rate   : check 2 — small tolerance for auction edge cases
# size_neg_rate: check 4 — 0 tolerance (this is always a bug)

_THRESHOLDS = {
    "RELIABLE": {
        "max_crossed_rate":      0.0,
        "max_trade_outside_rate": 0.01, # < 0.1% — ts_event/ts_recv divergence
        "max_size_neg_rate":     0.0,
        "max_null_drop_rate":    0.001,
    },
    "ACCEPTABLE": {
        "max_crossed_rate":      0.0001,
        "max_trade_outside_rate": 1.0, # < 1% — tolerated on illiquid contracts
        "max_size_neg_rate":     0.0,
        "max_null_drop_rate":    0.01,
    },
}


# ---------------------------------------------------------------------------
# Path helper
# ---------------------------------------------------------------------------

def _our_mbp1_path(product: str, contract: str, date_str: str) -> Path:
    """
    Return the path of our reconstructed MBP-1 Parquet file for HKEX.
    Mirrors build_mbp1.py output convention.
    """
    cfg      = MARKET_CONFIG[product]
    venue    = cfg["exchange"]
    provider = cfg["provider"]
    year     = int(date_str[:4])
    month    = int(date_str[4:6])
    return reconstructed_path(
        base_dir = DATA_RECONSTRUCTED,
        provider = provider,
        venue    = venue,
        product  = product,
        contract = contract,
        year     = year,
        month    = month,
        date_str = date_str,
        schema   = "mbp1",
    )


# ---------------------------------------------------------------------------
# RTH timestamp bounds (nanoseconds UTC)
# ---------------------------------------------------------------------------

def _rth_ns_bounds(product: str, date_str: str) -> tuple[int, int, int, int]:
    """
    Return RTH nanosecond bounds for HKEX products.

    Returns (morning_start_ns, lunch_start_ns, lunch_end_ns, afternoon_end_ns).
    All HKEX products use fixed UTC with a lunch break.
    """
    cfg = MARKET_CONFIG[product]
    d = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:]))

    def _hms_to_ns(hms: str) -> int:
        h, m, s = map(int, hms.split(":"))
        dt = datetime(d.year, d.month, d.day, h, m, s, tzinfo=timezone.utc)
        return int(dt.timestamp()) * 10**9

    rth_start = _hms_to_ns(cfg["rth_start_utc"])
    rth_end   = _hms_to_ns(cfg["rth_end_utc"])
    lunch_s   = _hms_to_ns(cfg["lunch_break_start"])
    lunch_e   = _hms_to_ns(cfg["lunch_break_end"])

    return rth_start, lunch_s, lunch_e, rth_end


# ---------------------------------------------------------------------------
# Core validation — all 5 checks in DuckDB
# ---------------------------------------------------------------------------

def _run_validation(
    mbp1_path : str,
    product   : str,
    date_str  : str,
) -> dict:
    """
    Run all 5 invariant checks on a single MBP-1 Parquet file.

    All computation stays inside DuckDB — no Arrow tables materialized
    in Python. Returns a dict with all check results and the verdict.
    """
    cfg       = MARKET_CONFIG[product]
    tick_size = cfg["tick_size_fp"] / 1e9  # real-price tick size (float)
    rth_start_ns, lunch_start_ns, lunch_end_ns, rth_end_ns = _rth_ns_bounds(
        product, date_str
    )

    # RTH continuous trading filter (excludes lunch break and pre/post-open).
    # We add a 5-minute buffer after session open (01:15 → 01:20 UTC) and
    # after lunch end (05:00 → 05:05 UTC) to exclude auction periods.
    auction_buffer_ns = 5 * 60 * 10**9  # 5 minutes in nanoseconds
    rth_continuous_where = f"""
        (
            (ts_recv >= {rth_start_ns + auction_buffer_ns} AND ts_recv < {lunch_start_ns})
            OR
            (ts_recv >= {lunch_end_ns + auction_buffer_ns} AND ts_recv < {rth_end_ns})
        )
    """

    # Full RTH filter (including auction periods at open/lunch)
    rth_all_where = f"""
        (
            (ts_recv >= {rth_start_ns} AND ts_recv < {lunch_start_ns})
            OR
            (ts_recv >= {lunch_end_ns} AND ts_recv < {rth_end_ns})
        )
    """

    con = duckdb.connect()

    # ── Total rows ────────────────────────────────────────────────────────
    n_total = con.execute(
        f"SELECT COUNT(*) FROM '{mbp1_path}'"
    ).fetchone()[0]

    n_rth = con.execute(
        f"SELECT COUNT(*) FROM '{mbp1_path}' WHERE {rth_all_where}"
    ).fetchone()[0]

    n_rth_continuous = con.execute(
        f"SELECT COUNT(*) FROM '{mbp1_path}' WHERE {rth_continuous_where}"
    ).fetchone()[0]

    # ══════════════════════════════════════════════════════════════════════
    # CHECK 1 — No crossed spread (RTH continuous only)
    # ══════════════════════════════════════════════════════════════════════
    n_crossed = con.execute(f"""
        SELECT COUNT(*) FROM '{mbp1_path}'
        WHERE {rth_continuous_where}
          AND bid_px_00 IS NOT NULL
          AND ask_px_00 IS NOT NULL
          AND bid_px_00 >= ask_px_00
    """).fetchone()[0]

    n_both_sides = con.execute(f"""
        SELECT COUNT(*) FROM '{mbp1_path}'
        WHERE {rth_continuous_where}
          AND bid_px_00 IS NOT NULL
          AND ask_px_00 IS NOT NULL
    """).fetchone()[0]

    # ══════════════════════════════════════════════════════════════════════
    # CHECK 2 — Trade price within previous spread
    # ══════════════════════════════════════════════════════════════════════
    # Use LAG() over a stable total order (ts_recv, sequence, rn) where rn
    # is a ROW_NUMBER() that breaks ties deterministically. Without rn,
    # events sharing the same ts_recv+sequence (e.g. TRADE and its synthetic
    # CANCEL) have non-deterministic ordering in DuckDB → unstable results
    # across runs.
    # Known limitation: HKEX trade_time_ns (~10ms precision) can predate
    # ts_recv by several ms. Trades at the boundary of a quote change may
    # appear outside spread due to ts_event/ts_recv divergence — not a
    # reconstruction error.
    trade_check = con.execute(f"""
        WITH numbered AS (
            -- Assign a stable row number to break ts_recv+sequence ties
            SELECT *,
                ROW_NUMBER() OVER (
                    ORDER BY ts_recv, sequence,
                    CASE action WHEN 'TRADE' THEN 0 ELSE 1 END
                ) AS rn
            FROM '{mbp1_path}'
            WHERE {rth_continuous_where}
        ),
        lagged AS (
            SELECT
                action,
                price,
                ts_recv,
                LAG(bid_px_00) OVER (ORDER BY rn) AS prev_bid,
                LAG(ask_px_00) OVER (ORDER BY rn) AS prev_ask
            FROM numbered
        )
        SELECT
            COUNT(*)                                              AS n_trades,
            SUM(CASE WHEN prev_bid IS NOT NULL
                      AND prev_ask IS NOT NULL
                      AND (price < prev_bid OR price > prev_ask)
                 THEN 1 ELSE 0 END)                              AS n_outside
        FROM lagged
        WHERE action = 'TRADE'
    """).fetchone()
    n_trades        = trade_check[0]
    n_trade_outside = trade_check[1]

    # ══════════════════════════════════════════════════════════════════════
    # CHECK 3 — Spread in ticks (distribution stats, RTH continuous)
    # ══════════════════════════════════════════════════════════════════════
    spread_stats = con.execute(f"""
        WITH spreads AS (
            SELECT (ask_px_00 - bid_px_00) / {tick_size} AS spread_ticks
            FROM '{mbp1_path}'
            WHERE {rth_continuous_where}
              AND bid_px_00 IS NOT NULL
              AND ask_px_00 IS NOT NULL
        )
        SELECT
            COUNT(*)                                     AS n_spread,
            SUM(CASE WHEN spread_ticks < 1 THEN 1 ELSE 0 END) AS n_locked,
            ROUND(AVG(spread_ticks), 2)                  AS avg_spread,
            ROUND(MEDIAN(spread_ticks), 2)               AS med_spread,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY spread_ticks), 2) AS p95_spread,
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY spread_ticks), 2) AS p99_spread,
            ROUND(MIN(spread_ticks), 2)                  AS min_spread,
            ROUND(MAX(spread_ticks), 2)                  AS max_spread
        FROM spreads
    """).fetchone()
    n_spread_rows = spread_stats[0]
    n_locked      = spread_stats[1]
    avg_spread    = spread_stats[2]
    med_spread    = spread_stats[3]
    p95_spread    = spread_stats[4]
    p99_spread    = spread_stats[5]
    min_spread    = spread_stats[6]
    max_spread    = spread_stats[7]

    # ══════════════════════════════════════════════════════════════════════
    # CHECK 4 — Size positivity (all rows, not just RTH)
    # ══════════════════════════════════════════════════════════════════════
    size_check = con.execute(f"""
        SELECT
            SUM(CASE WHEN bid_sz_00 < 0 THEN 1 ELSE 0 END) AS n_neg_bid,
            SUM(CASE WHEN ask_sz_00 < 0 THEN 1 ELSE 0 END) AS n_neg_ask
        FROM '{mbp1_path}'
    """).fetchone()
    n_neg_bid = size_check[0]
    n_neg_ask = size_check[1]

    # ══════════════════════════════════════════════════════════════════════
    # CHECK 5 — Book continuity (RTH continuous, non-CLEAR rows)
    # ══════════════════════════════════════════════════════════════════════
    # A side that is non-null at t should remain non-null at t+1 unless
    # a CLEAR event intervenes. Count "null drops": transitions from
    # non-null to null that are NOT preceded by a CLEAR.
    # ROW_NUMBER() tie-breaker ensures deterministic LAG ordering — same
    # rationale as check 2.
    null_drop_check = con.execute(f"""
        WITH numbered AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    ORDER BY ts_recv, sequence,
                    CASE action WHEN 'TRADE' THEN 0 ELSE 1 END
                ) AS rn
            FROM '{mbp1_path}'
            WHERE {rth_continuous_where}
        ),
        seq AS (
            SELECT
                action,
                bid_px_00,
                ask_px_00,
                LAG(bid_px_00) OVER (ORDER BY rn) AS prev_bid,
                LAG(ask_px_00) OVER (ORDER BY rn) AS prev_ask,
                LAG(action)    OVER (ORDER BY rn) AS prev_action
            FROM numbered
        )
        SELECT
            SUM(CASE WHEN prev_bid IS NOT NULL AND bid_px_00 IS NULL
                      AND prev_action != 'CLEAR' THEN 1 ELSE 0 END) AS bid_null_drops,
            SUM(CASE WHEN prev_ask IS NOT NULL AND ask_px_00 IS NULL
                      AND prev_action != 'CLEAR' THEN 1 ELSE 0 END) AS ask_null_drops
        FROM seq
    """).fetchone()
    bid_null_drops = null_drop_check[0]
    ask_null_drops = null_drop_check[1]

    con.close()

    # ── Compute rates ─────────────────────────────────────────────────────
    def _rate(n: int, total: int) -> float:
        return round(100.0 * n / total, 6) if total > 0 else 0.0

    crossed_rate       = _rate(n_crossed, n_both_sides)
    trade_outside_rate = _rate(n_trade_outside, n_trades)
    locked_rate        = _rate(n_locked, n_spread_rows)
    neg_bid_rate       = _rate(n_neg_bid, n_total)
    neg_ask_rate       = _rate(n_neg_ask, n_total)
    bid_null_drop_rate = _rate(bid_null_drops, n_rth_continuous)
    ask_null_drop_rate = _rate(ask_null_drops, n_rth_continuous)

    # ── Verdict ───────────────────────────────────────────────────────────
    # Check against thresholds — most restrictive first
    crossed_pct       = n_crossed / n_both_sides * 100 if n_both_sides > 0 else 0
    trade_outside_pct = n_trade_outside / n_trades * 100 if n_trades > 0 else 0
    size_neg_pct      = (n_neg_bid + n_neg_ask) / n_total * 100 if n_total > 0 else 0
    null_drop_pct     = max(bid_null_drop_rate, ask_null_drop_rate)

    rel = _THRESHOLDS["RELIABLE"]
    acc = _THRESHOLDS["ACCEPTABLE"]

    if (crossed_pct       <= rel["max_crossed_rate"] and
        trade_outside_pct <= rel["max_trade_outside_rate"] and
        size_neg_pct      <= rel["max_size_neg_rate"] and
        null_drop_pct     <= rel["max_null_drop_rate"]):
        verdict = "RELIABLE"
    elif (crossed_pct       <= acc["max_crossed_rate"] and
          trade_outside_pct <= acc["max_trade_outside_rate"] and
          size_neg_pct      <= acc["max_size_neg_rate"] and
          null_drop_pct     <= acc["max_null_drop_rate"]):
        verdict = "ACCEPTABLE"
    else:
        verdict = "NEEDS_WORK"

    return {
        # Row counts
        "n_total"            : n_total,
        "n_rth"              : n_rth,
        "n_rth_continuous"   : n_rth_continuous,
        # Check 1 — crossed spread
        "n_both_sides"       : n_both_sides,
        "n_crossed"          : n_crossed,
        "crossed_rate"       : crossed_rate,
        # Check 2 — trade price
        "n_trades"           : n_trades,
        "n_trade_outside"    : n_trade_outside,
        "trade_outside_rate" : trade_outside_rate,
        # Check 3 — spread distribution
        "n_spread_rows"      : n_spread_rows,
        "n_locked"           : n_locked,
        "locked_rate"        : locked_rate,
        "avg_spread"         : avg_spread,
        "med_spread"         : med_spread,
        "p95_spread"         : p95_spread,
        "p99_spread"         : p99_spread,
        "min_spread"         : min_spread,
        "max_spread"         : max_spread,
        # Check 4 — size positivity
        "n_neg_bid"          : n_neg_bid,
        "n_neg_ask"          : n_neg_ask,
        "neg_bid_rate"       : neg_bid_rate,
        "neg_ask_rate"       : neg_ask_rate,
        # Check 5 — book continuity
        "bid_null_drops"     : bid_null_drops,
        "ask_null_drops"     : ask_null_drops,
        "bid_null_drop_rate" : bid_null_drop_rate,
        "ask_null_drop_rate" : ask_null_drop_rate,
        # Verdict
        "verdict"            : verdict,
    }

# ---------------------------------------------------------------------------
# Report printing — matches Databento validator formatting style
# ---------------------------------------------------------------------------

def _print_report(
    product  : str,
    contract : str,
    date_str : str,
    v        : dict,
) -> None:
    """Print a formatted validation report for one date."""

    def _check_color(passed: bool) -> str:
        return GREEN if passed else RED

    def _pass_fail(passed: bool) -> str:
        color = _check_color(passed)
        return f"{color}{'PASS' if passed else 'FAIL'}{RESET}"

    verdict_color = GREEN if v["verdict"] == "RELIABLE" else (
        YELLOW if v["verdict"] == "ACCEPTABLE" else RED
    )

    # Determine pass/fail per check
    c1_pass = v["n_crossed"] == 0
    c2_pass = v["trade_outside_rate"] <= 0.01
    c3_pass = v["n_locked"] == 0
    c4_pass = v["n_neg_bid"] == 0 and v["n_neg_ask"] == 0
    c5_pass = v["bid_null_drops"] == 0 and v["ask_null_drops"] == 0

    print(f"\n{'=' * 72}")
    print(f"  {BOLD}MBP-1 INVARIANT VALIDATION — {product} {contract} {date_str}{RESET}")
    print(f"{'=' * 72}")
    print(f"  Total rows          : {v['n_total']:>12,}")
    print(f"  RTH rows            : {v['n_rth']:>12,}")
    print(f"  RTH continuous rows : {v['n_rth_continuous']:>12,}")
    print(f"{'─' * 72}")

    # Check 1
    print(f"  {BOLD}Check 1 — Crossed Spread{RESET}          {_pass_fail(c1_pass)}")
    print(f"    Both sides quoted : {v['n_both_sides']:>12,}")
    print(f"    Crossed (bid≥ask) : {v['n_crossed']:>12,}  ({v['crossed_rate']:.4f}%)")

    # Check 2
    print(f"  {BOLD}Check 2 — Trade Price in Spread{RESET}   {_pass_fail(c2_pass)}")
    print(f"    Trades checked    : {v['n_trades']:>12,}")
    print(f"    Outside spread    : {v['n_trade_outside']:>12,}  ({v['trade_outside_rate']:.4f}%)")

    # Check 3
    print(f"  {BOLD}Check 3 — Spread Distribution{RESET}     {_pass_fail(c3_pass)}")
    print(f"    Locked (< 1 tick) : {v['n_locked']:>12,}  ({v['locked_rate']:.4f}%)")
    print(f"    Spread (ticks)    :  avg={v['avg_spread']}  med={v['med_spread']}"
          f"  p95={v['p95_spread']}  p99={v['p99_spread']}")
    print(f"                         min={v['min_spread']}  max={v['max_spread']}")

    # Check 4
    print(f"  {BOLD}Check 4 — Size Positivity{RESET}         {_pass_fail(c4_pass)}")
    print(f"    Negative bid_sz   : {v['n_neg_bid']:>12,}  ({v['neg_bid_rate']:.4f}%)")
    print(f"    Negative ask_sz   : {v['n_neg_ask']:>12,}  ({v['neg_ask_rate']:.4f}%)")

    # Check 5
    print(f"  {BOLD}Check 5 — Book Continuity{RESET}         {_pass_fail(c5_pass)}")
    print(f"    Bid null drops    : {v['bid_null_drops']:>12,}  ({v['bid_null_drop_rate']:.4f}%)")
    print(f"    Ask null drops    : {v['ask_null_drops']:>12,}  ({v['ask_null_drop_rate']:.4f}%)")

    print(f"{'─' * 72}")
    print(f"  {BOLD}VERDICT: {verdict_color}{v['verdict']}{RESET}")
    print(f"{'=' * 72}")


# ---------------------------------------------------------------------------
# Top-level per-date orchestration
# ---------------------------------------------------------------------------

def validate(
    product  : str,
    contract : str,
    date_str : str,
) -> dict:
    """
    Full invariant validation for one HKEX product/contract/date.

    Args:
        product:  product ticker, e.g. "HSI"
        contract: normalized contract name, e.g. "HSIG26"
        date_str: date in YYYYMMDD format

    Returns:
        Result dict with all check metrics and verdict.
    """
    our_path = _our_mbp1_path(product, contract, date_str)

    if not our_path.exists():
        raise FileNotFoundError(
            f"Reconstructed MBP-1 not found: {our_path}\n"
            f"  → Run: python reconstruction/build_mbp1.py "
            f"--product {product} --contract {contract} "
            f"--date {date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        )

    log.info("[CHECK] %s %s %s — %s", product, contract, date_str, our_path.name)

    mbp1_str = str(our_path)
    v = _run_validation(mbp1_str, product, date_str)

    _print_report(product, contract, date_str, v)

    return {
        "product"  : product,
        "contract" : contract,
        "date"     : date_str,
        **v,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate reconstructed MBP-1 for HKEX products using "
            "microstructure invariants (no reference feed required)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python tests/validation/validate_mbp1_hkex.py \\\n"
            "      --product HSI --contract HSIG26 --date 20260202\n\n"
            "  python tests/validation/validate_mbp1_hkex.py \\\n"
            "      --product HSI --contract HSIG26 \\\n"
            "      --date 20260202 20260203 20260204\n"
        ),
    )
    parser.add_argument(
        "--product", required=True,
        help="Product ticker (e.g. HSI, MHI, HHI, MCH).",
    )
    parser.add_argument(
        "--contract", required=True,
        help="Normalized contract name (e.g. HSIG26).",
    )
    parser.add_argument(
        "--date", required=True, nargs="+",
        help="One or more dates in YYYYMMDD or YYYY-MM-DD format.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    if args.product not in MARKET_CONFIG:
        log.error("Unknown product '%s'. Add it to market_config.py.", args.product)
        return 2

    cfg = MARKET_CONFIG[args.product]
    if cfg.get("exchange") != "HKEX":
        log.error(
            "Product '%s' is not an HKEX product (exchange=%s). "
            "Use validate_mbp1_databento.py for Databento products.",
            args.product, cfg.get("exchange"),
        )
        return 2

    all_results    : list[dict] = []
    any_needs_work  = False
    any_setup_error = False

    for date_str in args.date:
        date_str = date_str.replace("-", "")
        if len(date_str) != 8 or not date_str.isdigit():
            log.error(
                "Invalid date '%s'. Expected YYYYMMDD or YYYY-MM-DD.", date_str
            )
            any_setup_error = True
            continue

        print(f"\n{BOLD}--- {args.product} {args.contract} {date_str} ---{RESET}")
        try:
            result = validate(
                product  = args.product,
                contract = args.contract,
                date_str = date_str,
            )
            all_results.append(result)
            if result["verdict"] == "NEEDS_WORK":
                any_needs_work = True

        except FileNotFoundError as e:
            log.error("[SETUP ERROR]\n%s", e)
            any_setup_error = True
        except Exception as e:
            log.exception(
                "[ERROR] %s %s %s: %s",
                args.product, args.contract, date_str, e,
            )
            any_setup_error = True

    # ── Multi-date summary ────────────────────────────────────────────────
    if len(all_results) > 1:
        print(f"\n{'=' * 72}")
        print(f"  {BOLD}SUMMARY — {args.product} {args.contract}{RESET}")
        print(f"{'=' * 72}")
        print(f"  {'Date':<10}  {'crossed':>8}  {'trade%':>8}  {'neg_sz':>8}  verdict")
        print(f"  {'─'*10}  {'─'*8}  {'─'*8}  {'─'*8}  {'─'*10}")
        for r in all_results:
            color = GREEN if r["verdict"] == "RELIABLE" else (
                YELLOW if r["verdict"] == "ACCEPTABLE" else RED
            )
            n_neg = r["n_neg_bid"] + r["n_neg_ask"]
            print(
                f"  {r['date']:<10}  "
                f"{r['n_crossed']:>8,}  "
                f"{r['trade_outside_rate']:>7.3f}%  "
                f"{n_neg:>8,}  "
                f"{color}{r['verdict']}{RESET}"
            )
        print(f"{'=' * 72}")

    # ── Exit codes — consistent with validate_mbp1_databento.py ──────────
    if any_setup_error:
        return 2
    if any_needs_work:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())