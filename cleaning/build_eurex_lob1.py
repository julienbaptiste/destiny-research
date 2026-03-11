# cleaning/build_eurex_lob1.py
#
# Phase 2 — Generic Eurex LOB Level 1 reconstruction (FDAX / FESX / FSMI)
#
# Usage:
#   python build_eurex_lob1.py --product FDAX
#   python build_eurex_lob1.py --product FESX
#   python build_eurex_lob1.py --product FSMI
#   python build_eurex_lob1.py --product FDAX --from-date 20250502 --to-date 20250509
#   python build_eurex_lob1.py --product FDAX --no-validate
#
# Input:  data/clean/product=<PRODUCT>/**/<PRODUCT>_YYYYMMDD_orders_clean.parquet
#         data/clean/product=<PRODUCT>/**/<PRODUCT>_YYYYMMDD_trades_clean.parquet
# Output: data/lob1/product=<PRODUCT>/year=YYYY/month=MM/<PRODUCT>_YYYYMMDD_lob1.parquet
#
# Architecture: MBO state machine — one row per event, TOB snapshot after each event.
#   - active_orders: dict[order_id -> (price_fp, size, side)]
#   - bid/ask SortedLists (negated prices for bid desc order) for O(log n) TOB
#   - size dicts [price_fp -> total_size] for O(1) level aggregation
#   - Events: orders (A/C/M/R) + trades (T/F) merged and sorted by ts_recv
#
# Memory: ~200-400 MB peak per day (FDAX ~2.4M events/day)
# Runtime: ~20-40s per day (pure Python state machine, single-threaded)
#
# Eurex EOBI specifics:
#   - No snapshot available — LOB builds from zero at session open.
#     First ~1-2 min of session have incomplete book (burn-in period).
#     Filter ts_recv > session_open + 120s in downstream analysis.
#   - Modify (M): size change only, never price (verified on FDAX/FESX/FSMI).
#     If price change detected at runtime -> treated as Cancel + Add.
#   - CleaR (R): full book reset. Rare in-session (reconnect / Eurex failover).
#
# Output schema (fixed-point int64 prices throughout, divide by 1e9 for points):
#   ts_recv, ts_event, sequence, order_id, action, side, price, size, flags,
#   bid_px, bid_sz, ask_px, ask_sz, mid_px, spread, tob_changed, source
#
# Product config (tick size, session times, URLs) is defined in eurex_config.py.
# To add a new Eurex product: add one entry to PRODUCT_CONFIG in eurex_config.py.

import argparse
import logging
import time
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pathlib import Path
from sortedcontainers import SortedList

from eurex_config import (
    PRODUCT_CONFIG,
    AVAILABLE_PRODUCTS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Fixed-point scale (all Databento prices: raw int64 * 1e-9 = points)
FP_SCALE = 1_000_000_000

# ---------------------------------------------------------------------------
# OUTPUT SCHEMA
# ---------------------------------------------------------------------------
# One row per input event (orders + trades merged).
# TOB columns reflect state AFTER the event has been applied.
# tob_changed = True if best_bid or best_ask changed vs previous row.
# Prices remain in fixed-point int64 — downstream divides by 1e9 as needed.

LOB1_SCHEMA = pa.schema([
    # event identity
    pa.field("ts_recv",     pa.uint64()),   # sort key, nanoseconds UTC
    pa.field("ts_event",    pa.uint64()),   # exchange timestamp
    pa.field("sequence",    pa.int64()),    # exchange sequence number
    pa.field("order_id",    pa.uint64()),   # Databento order_id (uint64)
    pa.field("action",      pa.string()),   # A/C/M/R/T/F
    pa.field("side",        pa.string()),   # B/A/N
    pa.field("price",       pa.int64()),    # event price, fixed-point
    pa.field("size",        pa.int32()),    # event size, contracts
    pa.field("flags",       pa.uint8()),    # Databento flags bitfield
    # TOB snapshot after this event
    pa.field("bid_px",      pa.int64()),    # best bid, fixed-point (null if empty)
    pa.field("bid_sz",      pa.int32()),    # total size at best bid
    pa.field("ask_px",      pa.int64()),    # best ask, fixed-point (null if empty)
    pa.field("ask_sz",      pa.int32()),    # total size at best ask
    pa.field("mid_px",      pa.int64()),    # (bid + ask) // 2, fixed-point
    pa.field("spread",      pa.int64()),    # ask - bid, fixed-point
    # derived
    pa.field("tob_changed", pa.bool_()),   # True if TOB changed vs prev row
    pa.field("source",      pa.string()),   # "order" or "trade"
])

# ---------------------------------------------------------------------------
# LOB STATE MACHINE
# ---------------------------------------------------------------------------

class LOBStateMachine:
    """
    MBO Level 1 state machine for a single trading day.

    State:
      active_orders : dict[order_id -> (price_fp, size, side)]
          Full resting book. Required for Cancel/Modify/Fill lookups.
      _bid_prices   : SortedList of negated price_fp (ascending = best bid first)
      _ask_prices   : SortedList of price_fp (ascending = best ask first)
      bid_size_at   : dict[price_fp -> total resting size]
      ask_size_at   : dict[price_fp -> total resting size]

    Complexity:
      Add/Cancel/Fill : O(log n) SortedList + O(1) dict
      TOB read        : O(1)
    """

    def __init__(self):
        self.active_orders = {}
        self._bid_prices   = SortedList()   # negated price_fp
        self._ask_prices   = SortedList()   # price_fp
        self.bid_size_at   = {}
        self.ask_size_at   = {}
        self.n_events      = 0
        self.n_warnings    = 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _add_to_side(self, price_fp: int, size: int, side: str):
        """Register size at a price level, inserting the level if new."""
        if side == 'B':
            if price_fp not in self.bid_size_at:
                self.bid_size_at[price_fp] = 0
                self._bid_prices.add(-price_fp)
            self.bid_size_at[price_fp] += size
        else:
            if price_fp not in self.ask_size_at:
                self.ask_size_at[price_fp] = 0
                self._ask_prices.add(price_fp)
            self.ask_size_at[price_fp] += size

    def _remove_from_side(self, price_fp: int, size: int, side: str):
        """
        Reduce size at a price level.
        Removes the level entirely when size reaches zero.
        Negative residual size indicates a data integrity issue (logged, clamped).
        """
        if side == 'B':
            size_dict  = self.bid_size_at
            price_list = self._bid_prices
            key        = -price_fp
        else:
            size_dict  = self.ask_size_at
            price_list = self._ask_prices
            key        = price_fp

        if price_fp not in size_dict:
            # Level not found — expected at session open during burn-in period
            # (book not yet fully populated). Silent debug log only.
            log.debug(f"_remove_from_side: {price_fp/FP_SCALE:.4f} not in {side} book")
            return

        size_dict[price_fp] -= size

        if size_dict[price_fp] <= 0:
            if size_dict[price_fp] < 0:
                log.warning(
                    f"Negative size at {price_fp/FP_SCALE:.4f} side={side} "
                    f"remaining={size_dict[price_fp]} — clamping to 0"
                )
                self.n_warnings += 1
            del size_dict[price_fp]
            try:
                price_list.remove(key)
            except ValueError:
                log.warning(f"_remove_from_side: key {key} not in SortedList")
                self.n_warnings += 1

    # ------------------------------------------------------------------
    # TOB access
    # ------------------------------------------------------------------

    def best_bid(self) -> tuple[int | None, int | None]:
        """Best bid (price_fp, size). (None, None) if book is empty."""
        if not self._bid_prices:
            return None, None
        px = -self._bid_prices[0]
        return px, self.bid_size_at.get(px, 0)

    def best_ask(self) -> tuple[int | None, int | None]:
        """Best ask (price_fp, size). (None, None) if book is empty."""
        if not self._ask_prices:
            return None, None
        px = self._ask_prices[0]
        return px, self.ask_size_at.get(px, 0)

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def apply_add(self, order_id: int, price_fp: int, size: int, side: str):
        """New resting order. Duplicate Add on reconnect boundary: update in place."""
        if order_id in self.active_orders:
            log.debug(f"Duplicate Add order_id={order_id} — updating in place")
            old_price, old_size, old_side = self.active_orders[order_id]
            self._remove_from_side(old_price, old_size, old_side)
        self.active_orders[order_id] = (price_fp, size, side)
        self._add_to_side(price_fp, size, side)

    def apply_cancel(self, order_id: int):
        """Full cancel — remove resting order from book."""
        if order_id not in self.active_orders:
            log.debug(f"Cancel unknown order_id={order_id} — skipping")
            return
        price_fp, size, side = self.active_orders.pop(order_id)
        self._remove_from_side(price_fp, size, side)

    def apply_modify(self, order_id: int, new_size: int, new_price_fp: int):
        """
        Eurex EOBI Modify: normally size change only, price unchanged.
        If price changes (unexpected): treat as Cancel + Add
        (price change = loss of queue priority on Eurex).
        """
        if order_id not in self.active_orders:
            log.debug(f"Modify unknown order_id={order_id} — skipping")
            return

        old_price, old_size, side = self.active_orders[order_id]

        if new_price_fp != old_price and new_price_fp > 0:
            log.warning(
                f"Modify with price change order_id={order_id} "
                f"{old_price/FP_SCALE:.4f} -> {new_price_fp/FP_SCALE:.4f} — Cancel+Add"
            )
            self.n_warnings += 1
            self._remove_from_side(old_price, old_size, side)
            self.active_orders[order_id] = (new_price_fp, new_size, side)
            self._add_to_side(new_price_fp, new_size, side)
        else:
            # Normal case: size delta only
            delta = new_size - old_size
            self.active_orders[order_id] = (old_price, new_size, side)
            if delta > 0:
                self._add_to_side(old_price, delta, side)
            elif delta < 0:
                self._remove_from_side(old_price, -delta, side)

    def apply_fill(self, order_id: int, fill_size: int):
        """
        Passive fill (action=F): reduce resting size.
        Full fill removes the order from the book.
        """
        if order_id not in self.active_orders:
            log.debug(f"Fill unknown order_id={order_id} — skipping")
            return

        price_fp, old_size, side = self.active_orders[order_id]
        new_size = old_size - fill_size

        if new_size <= 0:
            if new_size < 0:
                log.warning(
                    f"Overfill order_id={order_id} old={old_size} fill={fill_size}"
                )
                self.n_warnings += 1
            del self.active_orders[order_id]
            self._remove_from_side(price_fp, old_size, side)
        else:
            self.active_orders[order_id] = (price_fp, new_size, side)
            self._remove_from_side(price_fp, fill_size, side)

    def apply_clear(self):
        """
        CleaR (R): full book reset.
        Triggered at session start or on Eurex feed reconnection.
        All resting state is discarded — LOB burn-in restarts from zero.
        """
        log.info("CleaR (R) event — full LOB reset")
        self.active_orders.clear()
        self._bid_prices.clear()
        self._ask_prices.clear()
        self.bid_size_at.clear()
        self.ask_size_at.clear()

    def apply(self, action: str, order_id: int, price_fp: int,
              size: int, side: str) -> None:
        """Dispatch one MBO event. TOB is read by caller after return."""
        self.n_events += 1
        if   action == 'A': self.apply_add(order_id, price_fp, size, side)
        elif action == 'C': self.apply_cancel(order_id)
        elif action == 'M': self.apply_modify(order_id, size, price_fp)
        elif action == 'R': self.apply_clear()
        elif action == 'F': self.apply_fill(order_id, size)
        elif action == 'T': pass   # aggressor — no book change, TOB snapshot emitted


# ---------------------------------------------------------------------------
# LOAD & MERGE one day's events
# ---------------------------------------------------------------------------

def load_day_events(
    clean_root: Path,
    product:    str,
    date_str:   str,
) -> pa.Table:
    """
    Load orders_clean + trades_clean for one day, merge and sort by ts_recv.

    Databento MBO split convention:
      orders file : A / C / M / R
      trades file : T / F
    Both streams must be merged and sorted by ts_recv before the state machine
    can correctly apply passive fills (F) against the resting book.

    Secondary sort key: sequence (exchange sequence number) — breaks ts_recv ties
    deterministically. Same-nanosecond events from same exchange are ordered by
    their original sequence, preserving logical event order within a burst.
    """
    cols = [
        "ts_recv", "ts_event", "sequence", "order_id",
        "action", "side", "price", "size", "flags",
    ]

    orders_files = sorted(clean_root.rglob(f"{product}_{date_str}_orders_clean.parquet"))
    trades_files = sorted(clean_root.rglob(f"{product}_{date_str}_trades_clean.parquet"))

    if not orders_files:
        raise FileNotFoundError(f"No orders_clean for {product} {date_str}")

    orders_tbl = pq.read_table(orders_files[0], columns=cols)
    orders_tbl = orders_tbl.append_column(
        "source",
        pa.array(["order"] * len(orders_tbl),
                 type=pa.dictionary(pa.int8(), pa.string())),
    )

    if trades_files:
        trades_tbl = pq.read_table(trades_files[0], columns=cols)
        trades_tbl = trades_tbl.append_column(
            "source",
            pa.array(["trade"] * len(trades_tbl),
                     type=pa.dictionary(pa.int8(), pa.string())),
        )
        merged = pa.concat_tables([orders_tbl, trades_tbl])
    else:
        log.warning(f"No trades_clean for {product} {date_str} — orders only")
        merged = orders_tbl

    # Sort by (ts_recv ASC, sequence ASC) — deterministic within nanosecond ties
    sort_idx = pc.sort_indices(
        merged,
        sort_keys=[("ts_recv", "ascending"), ("sequence", "ascending")],
    )
    return merged.take(sort_idx)


# ---------------------------------------------------------------------------
# PROCESS ONE DAY
# ---------------------------------------------------------------------------

def process_day(
    clean_root:  Path,
    output_root: Path,
    product:     str,
    date_str:    str,
    tick_fp:     int,
) -> dict:
    """
    Run the MBO state machine on one day. Write lob1 Parquet. Return stats dict.

    Hot path: Arrow columns converted to Python lists once before the loop.
    Pre-allocated output lists avoid repeated append overhead.
    """
    t0   = time.time()
    year = date_str[:4]
    mon  = str(int(date_str[4:6]))   # strip leading zero for Hive path

    out_dir  = output_root / f"year={year}" / f"month={mon}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{product}_{date_str}_lob1.parquet"

    if out_path.exists():
        log.info(f"[SKIP] {date_str} — lob1 already exists")
        return {"date": date_str, "skipped": True}

    log.info(f"[START] {product} {date_str}")

    events   = load_day_events(clean_root, product, date_str)
    n_events = len(events)
    log.info(f"  {n_events:,} events loaded (orders + trades merged)")

    # Convert Arrow columns to Python lists once — faster than per-row Arrow access
    ts_recv_arr  = events.column("ts_recv").to_pylist()
    ts_event_arr = events.column("ts_event").to_pylist()
    seq_arr      = events.column("sequence").to_pylist()
    oid_arr      = events.column("order_id").to_pylist()
    action_arr   = events.column("action").to_pylist()
    side_arr     = events.column("side").to_pylist()
    price_arr    = events.column("price").to_pylist()
    size_arr     = events.column("size").to_pylist()
    flags_arr    = events.column("flags").to_pylist()
    source_arr   = events.column("source").to_pylist()

    # Pre-allocate output lists (avoids repeated list.append overhead)
    out_ts_recv  = [None] * n_events
    out_ts_event = [None] * n_events
    out_seq      = [None] * n_events
    out_oid      = [None] * n_events
    out_action   = [None] * n_events
    out_side     = [None] * n_events
    out_price    = [None] * n_events
    out_size     = [None] * n_events
    out_flags    = [None] * n_events
    out_bid_px   = [None] * n_events
    out_bid_sz   = [None] * n_events
    out_ask_px   = [None] * n_events
    out_ask_sz   = [None] * n_events
    out_mid_px   = [None] * n_events
    out_spread   = [None] * n_events
    out_tob_chg  = [None] * n_events
    out_source   = [None] * n_events

    lob          = LOBStateMachine()
    prev_bid_px  = None
    prev_ask_px  = None
    n_crossed    = 0
    n_no_tob     = 0
    n_tob_chg    = 0

    # -----------------------------------------------------------------------
    # Main event loop — hot path
    # -----------------------------------------------------------------------
    for i in range(n_events):
        action   = action_arr[i]
        order_id = oid_arr[i]
        price_fp = price_arr[i]
        size     = size_arr[i]
        side     = side_arr[i]

        lob.apply(action, order_id, price_fp, size, side)

        bid_px, bid_sz = lob.best_bid()
        ask_px, ask_sz = lob.best_ask()

        if bid_px is not None and ask_px is not None:
            mid_px = (bid_px + ask_px) // 2
            spread = ask_px - bid_px
            if spread <= 0:
                n_crossed += 1
                log.debug(
                    f"Crossed book ts_recv={ts_recv_arr[i]} "
                    f"bid={bid_px/FP_SCALE:.4f} ask={ask_px/FP_SCALE:.4f}"
                )
        else:
            mid_px = None
            spread = None
            n_no_tob += 1

        tob_changed = (bid_px != prev_bid_px) or (ask_px != prev_ask_px)
        if tob_changed:
            n_tob_chg += 1
        prev_bid_px = bid_px
        prev_ask_px = ask_px

        out_ts_recv[i]  = ts_recv_arr[i]
        out_ts_event[i] = ts_event_arr[i]
        out_seq[i]      = seq_arr[i]
        out_oid[i]      = oid_arr[i]
        out_action[i]   = action
        out_side[i]     = side
        out_price[i]    = price_fp
        out_size[i]     = size_arr[i]
        out_flags[i]    = flags_arr[i]
        out_bid_px[i]   = bid_px
        out_bid_sz[i]   = bid_sz
        out_ask_px[i]   = ask_px
        out_ask_sz[i]   = ask_sz
        out_mid_px[i]   = mid_px
        out_spread[i]   = spread
        out_tob_chg[i]  = tob_changed
        out_source[i]   = source_arr[i]

    # -----------------------------------------------------------------------
    # Build output table and write Parquet
    # -----------------------------------------------------------------------
    result = pa.table(
        {
            "ts_recv":     pa.array(out_ts_recv,  type=pa.uint64()),
            "ts_event":    pa.array(out_ts_event, type=pa.uint64()),
            "sequence":    pa.array(out_seq,      type=pa.int64()),
            "order_id":    pa.array(out_oid,      type=pa.uint64()),
            "action":      pa.array(out_action,   type=pa.string()),
            "side":        pa.array(out_side,     type=pa.string()),
            "price":       pa.array(out_price,    type=pa.int64()),
            "size":        pa.array(out_size,     type=pa.int32()),
            "flags":       pa.array(out_flags,    type=pa.uint8()),
            "bid_px":      pa.array(out_bid_px,   type=pa.int64()),
            "bid_sz":      pa.array(out_bid_sz,   type=pa.int32()),
            "ask_px":      pa.array(out_ask_px,   type=pa.int64()),
            "ask_sz":      pa.array(out_ask_sz,   type=pa.int32()),
            "mid_px":      pa.array(out_mid_px,   type=pa.int64()),
            "spread":      pa.array(out_spread,   type=pa.int64()),
            "tob_changed": pa.array(out_tob_chg,  type=pa.bool_()),
            "source":      pa.array(out_source,   type=pa.string()),
        },
        schema=LOB1_SCHEMA,
    )

    pq.write_table(
        result,
        str(out_path),
        compression="snappy",
        row_group_size=500_000,
    )

    elapsed = time.time() - t0
    stats = {
        "date":           date_str,
        "n_events":       n_events,
        "n_tob_changes":  n_tob_chg,
        "pct_tob_chg":    round(100.0 * n_tob_chg / n_events, 2) if n_events else 0,
        "n_crossed_book": n_crossed,
        "n_no_tob":       n_no_tob,
        "n_warnings":     lob.n_warnings,
        "elapsed_s":      round(elapsed, 1),
        "skipped":        False,
    }

    log.info(
        f"  [OK] {date_str} | {n_events:,} events | "
        f"TOB_chg={stats['pct_tob_chg']}% | "
        f"crossed={n_crossed} | no_tob={n_no_tob} | "
        f"warn={lob.n_warnings} | {elapsed:.1f}s"
    )
    return stats


# ---------------------------------------------------------------------------
# VALIDATION — DuckDB post-hoc checks on written lob1 file
# ---------------------------------------------------------------------------

def validate_lob1(
    output_root: Path,
    product:     str,
    date_str:    str,
    tick_fp:     int,
):
    """
    Sanity checks on the lob1 Parquet for one day:
      - crossed book count (bid >= ask)
      - negative / zero spread count
      - tick size violations (spread not multiple of tick_fp)
      - spread distribution (top 10 values)
    Prints results to stdout. Raises no exception — caller decides on severity.
    """
    glob = str(output_root / "**" / f"{product}_{date_str}_lob1.parquet")
    con  = duckdb.connect()

    checks = con.execute(f"""
        SELECT
            COUNT(*)                                                AS n_rows,
            SUM(CASE WHEN bid_px IS NOT NULL
                      AND ask_px IS NOT NULL THEN 1 ELSE 0 END)    AS n_tob_valid,
            SUM(CASE WHEN bid_px >= ask_px   THEN 1 ELSE 0 END)    AS n_crossed,
            SUM(CASE WHEN spread <= 0
                      AND bid_px IS NOT NULL THEN 1 ELSE 0 END)    AS n_neg_spread,
            SUM(CASE WHEN spread IS NOT NULL
                      AND (spread % {tick_fp}) != 0
                 THEN 1 ELSE 0 END)                                AS n_tick_violations,
            MIN(spread) / 1e9                                       AS spread_min_pts,
            MAX(spread) / 1e9                                       AS spread_max_pts,
            ROUND(AVG(spread) / 1e9, 4)                            AS spread_avg_pts,
            APPROX_QUANTILE(spread, 0.5) / 1e9                     AS spread_median_pts,
            SUM(CASE WHEN tob_changed THEN 1 ELSE 0 END)           AS n_tob_changed,
            ROUND(100.0 * SUM(CASE WHEN tob_changed THEN 1 ELSE 0 END)
                  / COUNT(*), 2)                                    AS pct_tob_changed
        FROM read_parquet('{glob}', hive_partitioning=false)
        WHERE bid_px IS NOT NULL AND ask_px IS NOT NULL
    """).fetchdf()

    log.info(f"\n  [Validation] {product} {date_str}")
    print(checks.T.to_string())

    spread_dist = con.execute(f"""
        SELECT
            spread / 1e9    AS spread_pts,
            COUNT(*)        AS n_rows,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM read_parquet('{glob}', hive_partitioning=false)
        WHERE spread IS NOT NULL
        GROUP BY spread_pts
        ORDER BY n_rows DESC
        LIMIT 10
    """).fetchdf()

    print(f"\n  Spread distribution (top 10):")
    print(spread_dist.to_string())


# ---------------------------------------------------------------------------
# ARGUMENT PARSING
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Eurex MBO LOB Level 1 reconstruction"
    )
    parser.add_argument(
        "--product", required=True, choices=AVAILABLE_PRODUCTS,
        help="Product to process"
    )
    parser.add_argument(
        "--from-date", default=None,
        help="Start date inclusive, format YYYYMMDD"
    )
    parser.add_argument(
        "--to-date", default=None,
        help="End date inclusive, format YYYYMMDD"
    )
    parser.add_argument(
        "--no-validate", action="store_true",
        help="Skip DuckDB validation step after each day (faster)"
    )
    parser.add_argument(
        "--clean-root", default="data/clean",
        help="Root directory for clean input (default: data/clean)"
    )
    parser.add_argument(
        "--output-root", default="data/lob1",
        help="Root directory for lob1 output (default: data/lob1)"
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    args    = parse_args()
    product = args.product
    cfg     = PRODUCT_CONFIG[product]
    tick_fp = cfg["tick_size_fp"]

    clean_root  = Path(args.clean_root)  / f"product={product}"
    output_root = Path(args.output_root) / f"product={product}"

    if not clean_root.exists():
        raise FileNotFoundError(f"Clean data directory not found: {clean_root}")

    # Discover dates from orders_clean filenames
    order_files = sorted(clean_root.rglob("*_orders_clean.parquet"))
    if not order_files:
        raise FileNotFoundError(f"No orders_clean files under {clean_root}")

    # Extract YYYYMMDD from filename: PRODUCT_YYYYMMDD_orders_clean.parquet
    all_dates = [f.stem.split("_")[1] for f in order_files]

    # Apply date range filter if provided
    if args.from_date:
        all_dates = [d for d in all_dates if d >= args.from_date]
    if args.to_date:
        all_dates = [d for d in all_dates if d <= args.to_date]

    print(f"\n{'='*60}")
    print(f"  build_eurex_lob1.py")
    print(f"  Product   : {product}  ({cfg['description']})")
    print(f"  Tick size : {tick_fp / 1e9:.1f} pt")
    print(f"  Days      : {len(all_dates)}")
    print(f"  Validate  : {not args.no_validate}")
    if args.from_date:
        print(f"  From date : {args.from_date}")
    if args.to_date:
        print(f"  To date   : {args.to_date}")
    print(f"{'='*60}\n")

    all_stats = []

    for date_str in all_dates:
        try:
            stats = process_day(clean_root, output_root, product, date_str, tick_fp)
            all_stats.append(stats)
            if not stats.get("skipped") and not args.no_validate:
                validate_lob1(output_root, product, date_str, tick_fp)
        except Exception as e:
            log.error(f"[ERROR] {date_str}: {e}", exc_info=True)
            all_stats.append({"date": date_str, "error": str(e)})

    # Final summary
    log.info(f"\n{'='*60}")
    log.info(f"  SUMMARY — {product}")
    log.info(f"{'='*60}")
    for s in all_stats:
        if s.get("skipped"):
            log.info(f"  {s['date']}  SKIPPED")
        elif "error" in s:
            log.info(f"  {s['date']}  ERROR: {s['error']}")
        else:
            log.info(
                f"  {s['date']}  {s['n_events']:>9,} events  "
                f"TOB_chg={s['pct_tob_chg']:>5.1f}%  "
                f"crossed={s['n_crossed_book']:>3}  "
                f"warn={s['n_warnings']:>3}  "
                f"{s['elapsed_s']}s"
            )

    n_ok      = sum(1 for s in all_stats if not s.get("skipped") and "error" not in s)
    n_skipped = sum(1 for s in all_stats if s.get("skipped"))
    n_errors  = sum(1 for s in all_stats if "error" in s)
    log.info(f"\n  Processed={n_ok}  Skipped={n_skipped}  Errors={n_errors}")


if __name__ == "__main__":
    # Requires: pip install sortedcontainers
    main()