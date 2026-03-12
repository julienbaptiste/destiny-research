# cleaning/validate_mbp1.py
#
# Validation script: compare our LOB1 reconstruction against Databento MBP-1.
#
# Methodology:
#   The MBP-1 feed emits one record per MBO event that impacts depth=0 (TOB).
#   Our LOB1 emits one row per MBO event (all depths). The MBP-1 has no Fill
#   events (F) — only A/C/M/T.
#
#   Within the same (ts_recv, sequence), multiple events can coexist (bursts).
#   A naive inner join on (ts_recv, sequence) produces many-to-many matches
#   with inflated row counts and meaningless match rates.
#
#   Correct approach: compare only on (ts_recv, sequence) pairs that are UNIQUE
#   in both datasets. This gives a clean 1-to-1 alignment on ~88% of events
#   (the remaining ~12% are in bursts and cannot be aligned without sub-sequence
#   ordering that differs between MBO and MBP-1).
#
#   On the unique-key subset, we compare 6 TOB columns:
#     bid_px_00, ask_px_00, bid_sz_00, ask_sz_00, bid_ct_00, ask_ct_00
#
#   Expected results (ES, validated 2025-10-01):
#     - Prices: >99.97% match (near-perfect)
#     - Sizes/Counts: >98.5% match (residual from intra-burst ordering)
#     - Full match: >97.5%
#
# Usage:
#   python validate_mbp1.py --product ES --date 20251001 --symbol ESZ5
#   python validate_mbp1.py --product ES --date 20251001 20251010 20251027 --symbol ESZ5
#
# Input:
#   Our LOB1:  data/lob1/product=<P>/year=YYYY/month=MM/<P>_YYYYMMDD_<sym>_lob1.parquet
#   MBP-1 ref: data/market_data_mbp_1/product=<P>/year=YYYY/month=MM/<P>_YYYYMMDD_MBP_1.parquet

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------
LOB1_ROOT = Path("data/lob1")
MBP1_ROOT = Path("data/market_data_mbp_1")


def _lob1_path(product: str, date_str: str, symbol: str) -> Path:
    year = date_str[:4]
    mon  = date_str[4:6]
    return (
        LOB1_ROOT / f"product={product}"
        / f"year={year}" / f"month={mon}"
        / f"{product}_{date_str}_{symbol}_lob1.parquet"
    )


def _mbp1_path(product: str, date_str: str) -> Path:
    year = date_str[:4]
    mon  = date_str[4:6]
    return (
        MBP1_ROOT / f"product={product}"
        / f"year={year}" / f"month={mon}"
        / f"{product}_{date_str}_MBP_1.parquet"
    )


# ---------------------------------------------------------------------------
# VALIDATION — single date
# ---------------------------------------------------------------------------

def validate(
    product: str,
    date_str: str,
    symbol: str,
    max_mismatches: int = 20,
) -> dict:
    """Compare our LOB1 against Databento MBP-1 for one instrument/day.

    Uses unique-key join: only compares events where (ts_recv, sequence)
    appears exactly once in both datasets. All computation stays in DuckDB
    (no large Arrow tables materialized in Python).

    Returns a dict with match rates and verdict.
    """
    lob1_path = _lob1_path(product, date_str, symbol)
    mbp1_path = _mbp1_path(product, date_str)

    if not lob1_path.exists():
        raise FileNotFoundError(f"LOB1 not found: {lob1_path}")
    if not mbp1_path.exists():
        raise FileNotFoundError(f"MBP-1 reference not found: {mbp1_path}")

    lob1_str = str(lob1_path)
    mbp1_str = str(mbp1_path)

    con = duckdb.connect()

    # ── Row counts ────────────────────────────────────────────────────────
    our_total    = con.execute(f"SELECT COUNT(*) FROM '{lob1_str}'").fetchone()[0]
    our_no_fill  = con.execute(f"SELECT COUNT(*) FROM '{lob1_str}' WHERE action != 'F'").fetchone()[0]
    ref_total    = con.execute(f"SELECT COUNT(*) FROM '{mbp1_str}' WHERE symbol = '{symbol}'").fetchone()[0]

    # ── Unique-key match rates (all computed in DuckDB) ───────────────────
    stats = con.execute(f"""
        WITH our_unique AS (
            SELECT ts_recv, sequence, action,
                   bid_px_00, ask_px_00, bid_sz_00, ask_sz_00,
                   bid_ct_00, ask_ct_00
            FROM '{lob1_str}'
            WHERE action != 'F'
            QUALIFY COUNT(*) OVER (PARTITION BY ts_recv, sequence) = 1
        ),
        ref_unique AS (
            SELECT ts_recv, sequence,
                   bid_px_00, ask_px_00, bid_sz_00, ask_sz_00,
                   bid_ct_00, ask_ct_00
            FROM '{mbp1_str}'
            WHERE symbol = '{symbol}'
            QUALIFY COUNT(*) OVER (PARTITION BY ts_recv, sequence) = 1
        ),
        joined AS (
            SELECT
                o.ts_recv, o.sequence,
                o.bid_px_00 AS our_bid_px, r.bid_px_00 AS ref_bid_px,
                o.ask_px_00 AS our_ask_px, r.ask_px_00 AS ref_ask_px,
                o.bid_sz_00 AS our_bid_sz, r.bid_sz_00 AS ref_bid_sz,
                o.ask_sz_00 AS our_ask_sz, r.ask_sz_00 AS ref_ask_sz,
                CAST(o.bid_ct_00 AS INT) AS our_bid_ct, CAST(r.bid_ct_00 AS INT) AS ref_bid_ct,
                CAST(o.ask_ct_00 AS INT) AS our_ask_ct, CAST(r.ask_ct_00 AS INT) AS ref_ask_ct
            FROM our_unique o
            INNER JOIN ref_unique r USING (ts_recv, sequence)
        )
        SELECT
            COUNT(*)                                                           AS n_joined,
            SUM(CASE WHEN our_bid_px = ref_bid_px THEN 1 ELSE 0 END)          AS bid_px,
            SUM(CASE WHEN our_ask_px = ref_ask_px THEN 1 ELSE 0 END)          AS ask_px,
            SUM(CASE WHEN our_bid_sz = ref_bid_sz THEN 1 ELSE 0 END)          AS bid_sz,
            SUM(CASE WHEN our_ask_sz = ref_ask_sz THEN 1 ELSE 0 END)          AS ask_sz,
            SUM(CASE WHEN our_bid_ct = ref_bid_ct THEN 1 ELSE 0 END)          AS bid_ct,
            SUM(CASE WHEN our_ask_ct = ref_ask_ct THEN 1 ELSE 0 END)          AS ask_ct,
            SUM(CASE WHEN our_bid_px = ref_bid_px AND our_ask_px = ref_ask_px
                      AND our_bid_sz = ref_bid_sz AND our_ask_sz = ref_ask_sz
                      AND our_bid_ct = ref_bid_ct AND our_ask_ct = ref_ask_ct
                 THEN 1 ELSE 0 END)                                            AS full_match,
            -- Size direction (price-correct subset only)
            SUM(CASE WHEN our_bid_px = ref_bid_px AND our_bid_sz > ref_bid_sz THEN 1 ELSE 0 END) AS bid_sz_high,
            SUM(CASE WHEN our_bid_px = ref_bid_px AND our_bid_sz < ref_bid_sz THEN 1 ELSE 0 END) AS bid_sz_low,
            SUM(CASE WHEN our_ask_px = ref_ask_px AND our_ask_sz > ref_ask_sz THEN 1 ELSE 0 END) AS ask_sz_high,
            SUM(CASE WHEN our_ask_px = ref_ask_px AND our_ask_sz < ref_ask_sz THEN 1 ELSE 0 END) AS ask_sz_low,
            -- Average delta magnitude (cast to INT to avoid UINT32 overflow)
            ROUND(AVG(CASE WHEN our_bid_px = ref_bid_px AND our_bid_sz != ref_bid_sz
                THEN ABS(CAST(our_bid_sz AS INT) - CAST(ref_bid_sz AS INT)) END), 2)  AS avg_bid_sz_delta,
            ROUND(AVG(CASE WHEN our_ask_px = ref_ask_px AND our_ask_sz != ref_ask_sz
                THEN ABS(CAST(our_ask_sz AS INT) - CAST(ref_ask_sz AS INT)) END), 2)  AS avg_ask_sz_delta
        FROM joined
    """).fetchone()

    n_joined    = stats[0]
    n_bid_px    = stats[1]
    n_ask_px    = stats[2]
    n_bid_sz    = stats[3]
    n_ask_sz    = stats[4]
    n_bid_ct    = stats[5]
    n_ask_ct    = stats[6]
    n_full      = stats[7]
    bid_sz_high = stats[8]
    bid_sz_low  = stats[9]
    ask_sz_high = stats[10]
    ask_sz_low  = stats[11]
    avg_bid_d   = stats[12]
    avg_ask_d   = stats[13]

    pct = lambda n: round(100.0 * n / n_joined, 4) if n_joined > 0 else 0.0

    # ── Print report ──────────────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"  MBP-1 VALIDATION — {product} {symbol} {date_str}")
    print(f"{'=' * 70}")
    print(f"  Our LOB1 total:     {our_total:>12,}")
    print(f"  Our LOB1 (excl F):  {our_no_fill:>12,}")
    print(f"  MBP-1 reference:    {ref_total:>12,}")
    print(f"  Unique-key joined:  {n_joined:>12,}  ({100*n_joined/ref_total:.1f}% of ref)")
    print(f"{'─' * 70}")
    print(f"  bid_px_00:   {n_bid_px:>10,} / {n_joined:,}  ({pct(n_bid_px):.4f}%)")
    print(f"  ask_px_00:   {n_ask_px:>10,} / {n_joined:,}  ({pct(n_ask_px):.4f}%)")
    print(f"  bid_sz_00:   {n_bid_sz:>10,} / {n_joined:,}  ({pct(n_bid_sz):.4f}%)")
    print(f"  ask_sz_00:   {n_ask_sz:>10,} / {n_joined:,}  ({pct(n_ask_sz):.4f}%)")
    print(f"  bid_ct_00:   {n_bid_ct:>10,} / {n_joined:,}  ({pct(n_bid_ct):.4f}%)")
    print(f"  ask_ct_00:   {n_ask_ct:>10,} / {n_joined:,}  ({pct(n_ask_ct):.4f}%)")
    print(f"{'─' * 70}")
    print(f"  FULL MATCH:  {n_full:>10,} / {n_joined:,}  ({pct(n_full):.4f}%)")
    print(f"{'─' * 70}")
    print(f"  Size direction (where price matches):")
    print(f"    bid_sz: +{bid_sz_high:,} high / -{bid_sz_low:,} low  (avg delta: {avg_bid_d})")
    print(f"    ask_sz: +{ask_sz_high:,} high / -{ask_sz_low:,} low  (avg delta: {avg_ask_d})")

    # ── First N mismatches ────────────────────────────────────────────────
    if max_mismatches > 0:
        mismatches = con.execute(f"""
            WITH our_unique AS (
                SELECT ts_recv, sequence, action, side, price, flags,
                       bid_px_00, ask_px_00, bid_sz_00, ask_sz_00, bid_ct_00, ask_ct_00
                FROM '{lob1_str}'
                WHERE action != 'F'
                QUALIFY COUNT(*) OVER (PARTITION BY ts_recv, sequence) = 1
            ),
            ref_unique AS (
                SELECT ts_recv, sequence,
                       bid_px_00 AS ref_bid_px, ask_px_00 AS ref_ask_px,
                       bid_sz_00 AS ref_bid_sz, ask_sz_00 AS ref_ask_sz,
                       bid_ct_00 AS ref_bid_ct, ask_ct_00 AS ref_ask_ct
                FROM '{mbp1_str}'
                WHERE symbol = '{symbol}'
                QUALIFY COUNT(*) OVER (PARTITION BY ts_recv, sequence) = 1
            )
            SELECT o.ts_recv, o.sequence, o.action, o.side, o.price/1e9 AS px,
                   o.bid_px_00/1e9 AS our_bid, ref_bid_px/1e9 AS ref_bid,
                   o.bid_sz_00 AS our_bid_sz, ref_bid_sz,
                   o.ask_px_00/1e9 AS our_ask, ref_ask_px/1e9 AS ref_ask,
                   o.ask_sz_00 AS our_ask_sz, ref_ask_sz
            FROM our_unique o
            JOIN ref_unique r USING (ts_recv, sequence)
            WHERE NOT (
                o.bid_px_00 = r.ref_bid_px AND o.ask_px_00 = r.ref_ask_px
                AND o.bid_sz_00 = r.ref_bid_sz AND o.ask_sz_00 = r.ref_ask_sz
                AND CAST(o.bid_ct_00 AS INT) = CAST(r.ref_bid_ct AS INT)
                AND CAST(o.ask_ct_00 AS INT) = CAST(r.ref_ask_ct AS INT)
            )
            ORDER BY o.ts_recv
            LIMIT {max_mismatches}
        """).fetchall()

        if mismatches:
            print(f"\n  First {len(mismatches)} mismatches:")
            for m in mismatches:
                print(f"    {m}")
        else:
            print(f"\n  PERFECT MATCH — 0 mismatches")

    con.close()

    # ── Build result dict ─────────────────────────────────────────────────
    result = {
        "product":        product,
        "symbol":         symbol,
        "date":           date_str,
        "our_total":      our_total,
        "our_no_fill":    our_no_fill,
        "ref_total":      ref_total,
        "n_joined":       n_joined,
        "n_full_match":   n_full,
        "full_rate":      pct(n_full),
        "bid_px_rate":    pct(n_bid_px),
        "ask_px_rate":    pct(n_ask_px),
        "bid_sz_rate":    pct(n_bid_sz),
        "ask_sz_rate":    pct(n_ask_sz),
        "bid_ct_rate":    pct(n_bid_ct),
        "ask_ct_rate":    pct(n_ask_ct),
    }

    # Verdict based on price match rate (primary) and size match rate (secondary)
    px_rate = min(pct(n_bid_px), pct(n_ask_px))
    sz_rate = min(pct(n_bid_sz), pct(n_ask_sz))

    if px_rate >= 99.9 and sz_rate >= 98.0:
        result["verdict"] = "RELIABLE"
    elif px_rate >= 99.5 and sz_rate >= 95.0:
        result["verdict"] = "ACCEPTABLE"
    else:
        result["verdict"] = "NEEDS_WORK"

    print(f"\n  VERDICT: {result['verdict']}  (px>{px_rate:.2f}%, sz>{sz_rate:.2f}%)")
    print(f"{'=' * 70}")

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Validate LOB1 against Databento MBP-1 reference",
    )
    parser.add_argument("--product", required=True, help="Product (e.g. ES)")
    parser.add_argument("--date", required=True, nargs="+", help="Date(s) YYYYMMDD")
    parser.add_argument("--symbol", required=True, help="Instrument symbol (e.g. ESZ5)")
    parser.add_argument("--max-mismatches", type=int, default=20,
                        help="Max mismatches to display per date (default 20)")
    args = parser.parse_args()

    all_results = []
    any_fail = False

    for date_str in args.date:
        try:
            result = validate(args.product, date_str, args.symbol, args.max_mismatches)
            all_results.append(result)
            if result["verdict"] == "NEEDS_WORK":
                any_fail = True
        except Exception as e:
            log.error(f"[ERROR] {date_str}: {e}", exc_info=True)
            any_fail = True

    # ── Summary across all dates ──────────────────────────────────────────
    if len(all_results) > 1:
        print(f"\n{'=' * 70}")
        print(f"  SUMMARY — {args.product} {args.symbol}")
        print(f"{'=' * 70}")
        for r in all_results:
            print(
                f"  {r['date']}  "
                f"px={min(r['bid_px_rate'], r['ask_px_rate']):.2f}%  "
                f"sz={min(r['bid_sz_rate'], r['ask_sz_rate']):.2f}%  "
                f"full={r['full_rate']:.2f}%  "
                f"{r['verdict']}"
            )
        print(f"{'=' * 70}")

    sys.exit(1 if any_fail else 0)


if __name__ == "__main__":
    main()