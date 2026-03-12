# cleaning/build_cme_lob1.py
#
# Phase 2 — CME Globex LOB Level 1 reconstruction v2
#
# KEY CHANGE vs v1:
#   v1 operated on RTH-clean events only, with a fragile overnight warmup hack.
#   v2 operates on RAW MBO events for the full electronic session (22:00-21:00 UTC),
#   initializes via F_SNAPSHOT, reconstructs per-instrument, and outputs a TOB
#   snapshot after every event — mirroring Databento's MBP-1 feed structure.
#
#   Downstream filtering (RTH, front-month, etc.) happens in analysis/features.
#
# Usage:
#   python build_cme_lob1.py --product ES --from-date 20251001 --to-date 20251001
#   python build_cme_lob1.py --product ES --from-date 20251001 --to-date 20251031
#   python build_cme_lob1.py --product NIY --from-date 20250101 --to-date 20251231
#
# Input:
#   Raw MBO events: data/market_data/product=<P>/**/<P>_YYYYMMDD_orders.parquet
#                   data/market_data/product=<P>/**/<P>_YYYYMMDD_trades.parquet
#
# Output:
#   data/lob1/product=<P>/year=YYYY/month=MM/<P>_YYYYMMDD_<symbol>_lob1.parquet
#
# Output schema (aligned with Databento MBP-1 for bit-exact validation):
#   ts_recv, ts_event, sequence, order_id, action, side, price, size, flags,
#   bid_px_00, ask_px_00, bid_sz_00, ask_sz_00, bid_ct_00, ask_ct_00,
#   mid_px, spread, tob_changed, source, symbol
#
# Architecture:
#   - One LOBStateMachine per instrument per day
#   - F_SNAPSHOT records initialize the book state (replaces warmup hack)
#   - Aggressive Add detection: orders crossing the TOB are tracked but not
#     inserted into the resting book (they'll be matched by a Fill)
#   - Order count tracking (bid_ct_00, ask_ct_00) for MBP-1 parity
#
# Memory management:
#   - Raw events loaded via PyArrow (memory-mapped)
#   - Processing is per-instrument: only one instrument's events in memory at a time
#   - Output written incrementally per instrument per day
#

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from sortedcontainers import SortedList

from cme_config import (
    AVAILABLE_PRODUCTS,
    F_SNAPSHOT,
    INT64_MAX,
    PRODUCT_CONFIG,
    rth_utc_bounds,
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
# DIRECTORY LAYOUT
# ---------------------------------------------------------------------------
RAW_ROOT     = Path("data/market_data")       # split orders/trades Parquet (for analysis)
RAW_MBO_ROOT = Path("data/raw_market_data")   # unsplit MBO Parquet (native feed order)
LOB1_ROOT    = Path("data/lob1")           # v2 output — distinct from v1


# ---------------------------------------------------------------------------
# OUTPUT SCHEMA — aligned with Databento MBP-1 + our extras
# ---------------------------------------------------------------------------

LOB1_SCHEMA = pa.schema([
    pa.field("ts_recv",      pa.uint64()),
    pa.field("ts_event",     pa.uint64()),
    pa.field("sequence",     pa.uint32()),
    pa.field("order_id",     pa.uint64()),
    pa.field("action",       pa.string()),
    pa.field("side",         pa.string()),
    pa.field("price",        pa.int64()),
    pa.field("size",         pa.uint32()),
    pa.field("flags",        pa.uint8()),
    pa.field("symbol",       pa.string()),
    # TOB snapshot — naming aligned with Databento MBP-1
    pa.field("bid_px_00",    pa.int64()),
    pa.field("ask_px_00",    pa.int64()),
    pa.field("bid_sz_00",    pa.uint32()),
    pa.field("ask_sz_00",    pa.uint32()),
    pa.field("bid_ct_00",    pa.uint32()),
    pa.field("ask_ct_00",    pa.uint32()),
    # Derived fields (not in MBP-1, but useful for analysis)
    pa.field("mid_px",       pa.int64()),
    pa.field("spread",       pa.int64()),
    pa.field("tob_changed",  pa.bool_()),
    pa.field("source",       pa.string()),
])


# ---------------------------------------------------------------------------
# LOB STATE MACHINE v2
# ---------------------------------------------------------------------------

class LOBStateMachine:
    """
    MBO → Level 1 state machine with order count tracking.

    Tracks per price level:
      - size (total quantity)
      - count (number of distinct orders)

    active_orders: dict[order_id -> (price_fp, size, side, in_book)]
      in_book=True  → resting order, contributing to level aggregation
      in_book=False → aggressive Add, tracked for Fill matching only

    Output after each event: best bid/ask price, size, and order count.
    """

    def __init__(self):
        self.active_orders: dict[int, tuple[int, int, str, bool]] = {}

        # Price level aggregation — bid side uses negated keys for desc sort
        self._bid_prices = SortedList()   # negated prices for best-bid-first
        self._ask_prices = SortedList()   # natural order for best-ask-first

        # Size at each price level
        self.bid_size_at: dict[int, int] = {}
        self.ask_size_at: dict[int, int] = {}

        # Order count at each price level (new in v2)
        self.bid_count_at: dict[int, int] = {}
        self.ask_count_at: dict[int, int] = {}

        # Stats
        self.n_events          = 0
        self.n_warnings        = 0
        self.n_aggressive_adds = 0
        self.n_snapshot_events = 0
        self.n_overfills       = 0

    # ── Side helpers with count tracking ──────────────────────────────────

    def _add_to_side(self, price_fp: int, size: int, side: str):
        """Add an order's size to a price level, increment order count."""
        if side == 'B':
            size_dict, count_dict, price_list, key = (
                self.bid_size_at, self.bid_count_at, self._bid_prices, -price_fp
            )
        else:
            size_dict, count_dict, price_list, key = (
                self.ask_size_at, self.ask_count_at, self._ask_prices, price_fp
            )

        if price_fp not in size_dict:
            size_dict[price_fp] = 0
            count_dict[price_fp] = 0
            price_list.add(key)

        size_dict[price_fp] += size
        count_dict[price_fp] += 1

    def _remove_from_side(self, price_fp: int, size: int, side: str,
                          decrement_count: bool = True):
        """Remove size from a price level, optionally decrement order count.

        decrement_count=False is used for partial fills where the order
        remains active (count stays the same, only size decreases).
        """
        if side == 'B':
            size_dict, count_dict, price_list, key = (
                self.bid_size_at, self.bid_count_at, self._bid_prices, -price_fp
            )
        else:
            size_dict, count_dict, price_list, key = (
                self.ask_size_at, self.ask_count_at, self._ask_prices, price_fp
            )

        if price_fp not in size_dict:
            log.debug(f"_remove_from_side: {price_fp/FP_SCALE:.4f} not in {side} book")
            return

        size_dict[price_fp] -= size
        if decrement_count:
            count_dict[price_fp] -= 1

        # Clean up empty levels
        if size_dict[price_fp] <= 0:
            if size_dict[price_fp] < 0:
                log.warning(
                    f"Negative size at {price_fp/FP_SCALE:.4f} side={side} "
                    f"remaining={size_dict[price_fp]} — clamping to 0"
                )
                self.n_warnings += 1
            del size_dict[price_fp]
            # Also remove count entry and price from sorted list
            count_dict.pop(price_fp, None)
            try:
                price_list.remove(key)
            except ValueError:
                log.warning(f"_remove_from_side: key {key} not in SortedList")
                self.n_warnings += 1
        elif decrement_count and count_dict.get(price_fp, 0) <= 0:
            # Edge case: count went to 0 but size still positive
            # This shouldn't happen in normal operation but handle defensively
            if count_dict.get(price_fp, 0) < 0:
                log.warning(
                    f"Negative count at {price_fp/FP_SCALE:.4f} side={side} "
                    f"count={count_dict[price_fp]} — clamping to 0"
                )
                self.n_warnings += 1
                count_dict[price_fp] = 0

    # ── TOB accessors ─────────────────────────────────────────────────────

    def best_bid(self) -> tuple[int | None, int | None, int | None]:
        """Return (price, size, count) at best bid, or (None, None, None)."""
        if not self._bid_prices:
            return None, None, None
        px = -self._bid_prices[0]
        return px, self.bid_size_at.get(px, 0), self.bid_count_at.get(px, 0)

    def best_ask(self) -> tuple[int | None, int | None, int | None]:
        """Return (price, size, count) at best ask, or (None, None, None)."""
        if not self._ask_prices:
            return None, None, None
        px = self._ask_prices[0]
        return px, self.ask_size_at.get(px, 0), self.ask_count_at.get(px, 0)

    # ── Event handlers ────────────────────────────────────────────────────

    def apply_add(self, order_id: int, price_fp: int, size: int, side: str):
        """New order — always inserted as resting.

        Previous versions attempted aggressive Add detection (marking orders
        that cross the opposite TOB as non-resting). This caused systematic
        size deficits (~1.5%) because within a single (ts_recv, sequence) burst,
        Adds can arrive interleaved with Cancels/Fills on the opposite side.
        The TOB at the moment of the Add still shows the old price level that
        is being swept — so the Add appears to cross, but by the end of the
        burst the opposite side has moved and the Add is legitimately resting.

        The correct approach: insert all Adds as resting. If an Add truly is
        aggressive, the matching engine will emit a Fill that removes it from
        the book. No information is lost — the Fill handles it.
        """
        # Handle duplicate order_id (replace in place)
        if order_id in self.active_orders:
            log.debug(f"Duplicate Add order_id={order_id} — updating in place")
            old_price, old_size, old_side, old_in_book = self.active_orders[order_id]
            if old_in_book:
                self._remove_from_side(old_price, old_size, old_side)

        # Insert as resting — Fills will handle aggressive orders
        self.active_orders[order_id] = (price_fp, size, side, True)
        self._add_to_side(price_fp, size, side)

    def apply_cancel(self, order_id: int):
        """Cancel an existing order — remove from book if resting."""
        if order_id not in self.active_orders:
            return
        price_fp, size, side, in_book = self.active_orders.pop(order_id)
        if in_book:
            self._remove_from_side(price_fp, size, side)

    def apply_modify(self, order_id: int, new_size: int, new_price_fp: int):
        """Modify an existing order — price change = remove + re-add."""
        if order_id not in self.active_orders:
            return
        old_price, old_size, side, in_book = self.active_orders[order_id]

        if new_price_fp != old_price and new_price_fp > 0:
            # Price change: remove from old level, add to new level
            if in_book:
                self._remove_from_side(old_price, old_size, side)
            # Re-insert as resting (price change = new priority = resting)
            self.active_orders[order_id] = (new_price_fp, new_size, side, True)
            self._add_to_side(new_price_fp, new_size, side)
        else:
            # Size-only change: adjust delta in place
            delta = new_size - old_size
            self.active_orders[order_id] = (old_price, new_size, side, in_book)
            if in_book:
                if delta > 0:
                    # Size increase — add delta, no count change
                    if side == 'B':
                        self.bid_size_at[old_price] = self.bid_size_at.get(old_price, 0) + delta
                    else:
                        self.ask_size_at[old_price] = self.ask_size_at.get(old_price, 0) + delta
                elif delta < 0:
                    # Size decrease — remove delta, no count change
                    self._remove_from_side(old_price, -delta, side, decrement_count=False)

    def apply_fill(self, order_id: int, fill_size: int):
        """Fill (passive side) — reduce size, remove if fully filled."""
        if order_id not in self.active_orders:
            return
        price_fp, old_size, side, in_book = self.active_orders[order_id]
        new_size = old_size - fill_size

        if new_size <= 0:
            # Fully filled — remove order entirely
            if new_size < 0:
                # Expected with M→T→F→C ordering: a Modify may have resized
                # the order before the Fill applies for the original quantity.
                # The book-level size is clamped correctly via _remove_from_side.
                log.debug(f"Overfill order_id={order_id} old={old_size} fill={fill_size}")
                self.n_overfills += 1
            del self.active_orders[order_id]
            if in_book:
                # Remove remaining size and decrement count
                self._remove_from_side(price_fp, old_size, side, decrement_count=True)
        else:
            # Partial fill — reduce size, keep order (count unchanged)
            self.active_orders[order_id] = (price_fp, new_size, side, in_book)
            if in_book:
                self._remove_from_side(price_fp, fill_size, side, decrement_count=False)

    def apply_clear(self):
        """Clear (R event) — full LOB reset."""
        log.info("CleaR (R) event — full LOB reset")
        self.active_orders.clear()
        self._bid_prices.clear()
        self._ask_prices.clear()
        self.bid_size_at.clear()
        self.ask_size_at.clear()
        self.bid_count_at.clear()
        self.ask_count_at.clear()

    def apply_snapshot(self, order_id: int, price_fp: int, size: int, side: str):
        """Snapshot record — insert directly as resting order (no crossing check).

        F_SNAPSHOT records are sent by CME at session open to reconstruct
        the book state. They represent the true resting book — no aggressive
        detection needed because the matching engine already resolved all
        crossings before publishing the snapshot.
        """
        self.n_snapshot_events += 1

        # Remove existing order if present (snapshot may update)
        if order_id in self.active_orders:
            old_price, old_size, old_side, old_in_book = self.active_orders[order_id]
            if old_in_book:
                self._remove_from_side(old_price, old_size, old_side)

        # Insert as resting — snapshot orders are always in-book
        self.active_orders[order_id] = (price_fp, size, side, True)
        self._add_to_side(price_fp, size, side)

    def apply(self, action: str, order_id: int, price_fp: int,
              size: int, side: str, is_snapshot: bool = False) -> None:
        """Dispatch an event to the appropriate handler."""
        self.n_events += 1

        # Snapshot records bypass aggressive detection
        if is_snapshot and action == 'A':
            self.apply_snapshot(order_id, price_fp, size, side)
            return

        if   action == 'A': self.apply_add(order_id, price_fp, size, side)
        elif action == 'C': self.apply_cancel(order_id)
        elif action == 'M': self.apply_modify(order_id, size, price_fp)
        elif action == 'R': self.apply_clear()
        elif action == 'F': self.apply_fill(order_id, size)
        elif action == 'T': pass  # Trade aggressor — no book impact


# ---------------------------------------------------------------------------
# LOAD RAW EVENTS FOR ONE DAY (full session, single instrument)
# ---------------------------------------------------------------------------

def load_raw_day_events(
    product: str,
    date_str: str,
    symbol: str,
    con: duckdb.DuckDBPyConnection,
) -> pa.Table:
    """Load raw MBO events for one day, one instrument, full session.

    Primary source: unsplit MBO Parquet (data/raw_market_data/) which preserves
    the native feed ordering from the CME matching engine. This is critical for
    correct book reconstruction — within the same (ts_recv, sequence), the
    matching engine emits events in a specific order:
        T → F → C → M → A  (observed empirically on CME MDP3)
    The Trade fires, Fills execute against passive orders, residual Cancels
    clean up fully-filled orders (no-ops), then Modifies resize partials,
    and new Adds arrive last.

    Fallback: if unsplit MBO file is not found, merges split orders + trades
    files from data/market_data/ with action-priority-based ordering
    (T=0, F=1, C=2, M=3, A=4, R=5). This is an approximation — the unsplit
    file is strongly preferred.

    Filters:
      - symbol match (exact outright, no spreads)
      - price != INT64_MAX (exclude sentinel market/stop orders)
      - No session time filter (full electronic session)
      - No snapshot exclusion (snapshots are needed for book init)

    Memory: single DuckDB streaming query, peak RAM = output table only.

    Returns a PyArrow table preserving native event ordering.
    """
    year = date_str[:4]
    mon  = date_str[4:6]

    cols = "ts_recv, ts_event, sequence, order_id, action, side, price, size, flags"
    where = f"symbol = '{symbol}' AND price != {INT64_MAX}"

    # ── Primary: unsplit MBO file (native feed order) ─────────────────────
    mbo_dir = RAW_MBO_ROOT / f"product={product}" / f"year={year}" / f"month={mon}"
    mbo_path = mbo_dir / f"{product}_{date_str}_mbo.parquet"

    if mbo_path.exists():
        # The raw MBO file has all events in native feed order.
        # DuckDB reads Parquet row groups sequentially — no ORDER BY needed
        # for intra-(ts_recv, sequence) ordering since the file preserves it.
        # We only ORDER BY (ts_recv, sequence) to handle cross-message ordering;
        # DuckDB's stable sort preserves intra-key file order.
        q = f"""
            SELECT {cols},
                   CASE WHEN action IN ('T', 'F') THEN 'trade' ELSE 'order' END AS source
            FROM '{mbo_path}'
            WHERE {where}
            ORDER BY ts_recv, sequence
        """
        log.info(f"    Loading from unsplit MBO: {mbo_path.name}")
        return con.execute(q).fetch_arrow_table()

    # ── Fallback: split orders + trades (approximate ordering) ────────────
    raw_dir = RAW_ROOT / f"product={product}" / f"year={year}" / f"month={mon}"
    orders_path = raw_dir / f"{product}_{date_str}_orders.parquet"
    trades_path = raw_dir / f"{product}_{date_str}_trades.parquet"

    if not orders_path.exists():
        raise FileNotFoundError(
            f"Neither unsplit MBO ({mbo_path}) nor split orders ({orders_path}) found"
        )

    log.warning(
        f"    Unsplit MBO not found — falling back to split orders+trades "
        f"(approximate intra-burst ordering)"
    )

    # Action priority: mirrors observed CME feed order T→F→C→M→A→R
    action_priority = """
        CASE action
            WHEN 'T' THEN 0  WHEN 'F' THEN 1  WHEN 'C' THEN 2
            WHEN 'M' THEN 3  WHEN 'A' THEN 4  WHEN 'R' THEN 5
            ELSE 6
        END
    """

    has_trades = trades_path.exists()

    if has_trades:
        q = f"""
            SELECT {cols}, source
            FROM (
                SELECT {cols}, 'order' AS source, {action_priority} AS _ap
                FROM '{orders_path}' WHERE {where}
                UNION ALL
                SELECT {cols}, 'trade' AS source, {action_priority} AS _ap
                FROM '{trades_path}' WHERE {where}
            )
            ORDER BY ts_recv, sequence, _ap
        """
    else:
        q = f"""
            SELECT {cols}, 'order' AS source
            FROM (
                SELECT {cols}, {action_priority} AS _ap
                FROM '{orders_path}' WHERE {where}
            )
            ORDER BY ts_recv, sequence, _ap
        """

    return con.execute(q).fetch_arrow_table()


# ---------------------------------------------------------------------------
# DISCOVER INSTRUMENTS FOR A DAY
# ---------------------------------------------------------------------------

def discover_instruments(
    product: str,
    date_str: str,
    con: duckdb.DuckDBPyConnection,
    min_events: int = 100,
) -> list[str]:
    """Find all outright instruments for a given day with enough activity.

    Checks unsplit MBO file first, falls back to split orders file.

    Excludes:
      - Calendar spreads (symbol contains '-')
      - Instruments with fewer than min_events (noise)
      - Snapshot-only instruments

    Returns list of symbols sorted by event count descending (front month first).
    """
    year = date_str[:4]
    mon  = date_str[4:6]

    # Try unsplit MBO first
    mbo_path = (
        RAW_MBO_ROOT / f"product={product}" / f"year={year}" / f"month={mon}"
        / f"{product}_{date_str}_mbo.parquet"
    )
    if not mbo_path.exists():
        # Fall back to split orders
        mbo_path = (
            RAW_ROOT / f"product={product}" / f"year={year}" / f"month={mon}"
            / f"{product}_{date_str}_orders.parquet"
        )
    if not mbo_path.exists():
        return []

    q = f"""
        SELECT symbol, COUNT(*) AS n
        FROM '{mbo_path}'
        WHERE symbol NOT LIKE '%%-%%'
          AND price != {INT64_MAX}
          AND flags & {F_SNAPSHOT} = 0
        GROUP BY symbol
        HAVING COUNT(*) >= {min_events}
        ORDER BY n DESC
    """
    result = con.execute(q).fetchall()
    return [row[0] for row in result]


# ---------------------------------------------------------------------------
# PROCESS ONE DAY, ONE INSTRUMENT
# ---------------------------------------------------------------------------

def process_instrument_day(
    product: str,
    date_str: str,
    symbol: str,
    tick_fp: int,
    con: duckdb.DuckDBPyConnection,
    chunk_size: int = 2_000_000,
) -> dict:
    """Run the MBO state machine for one instrument on one day.

    Full electronic session. Snapshot events initialize the book.
    Outputs one Parquet file per instrument per day.

    Memory management: processes events in chunks of chunk_size rows.
    The LOB state machine persists across chunks (it's tiny — just dicts).
    Each chunk's output is written incrementally via ParquetWriter, then
    the chunk's Python lists are freed before loading the next chunk.

    Peak RAM ≈ 1 chunk of input (Arrow table) + 1 chunk of output (Python lists)
    ≈ 2 × chunk_size × ~100 bytes/row ≈ 400MB for 2M chunk_size.

    Returns stats dict.
    """
    t0   = time.time()
    year = date_str[:4]
    mon  = date_str[4:6]

    out_dir  = LOB1_ROOT / f"product={product}" / f"year={year}" / f"month={mon}"
    out_path = out_dir / f"{product}_{date_str}_{symbol}_lob1.parquet"

    log.info(f"  [START] {symbol} {date_str}")

    # ── Count total events first (cheap metadata query) ───────────────────
    events_full = load_raw_day_events(product, date_str, symbol, con)
    n_events = len(events_full)

    if n_events == 0:
        log.warning(f"  No events for {symbol} {date_str} — skipping")
        return {"date": date_str, "symbol": symbol, "skipped": True, "reason": "no_events"}

    log.info(f"    {n_events:,} raw events loaded — processing in chunks of {chunk_size:,}")

    # ── Initialize state machine + counters ───────────────────────────────
    lob = LOBStateMachine()
    prev_bid_px = None
    prev_ask_px = None
    n_crossed   = 0
    n_no_tob    = 0
    n_tob_chg   = 0

    # ── Streaming Parquet writer ──────────────────────────────────────────
    out_dir.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(str(out_path), LOB1_SCHEMA, compression="snappy")

    n_chunks = (n_events + chunk_size - 1) // chunk_size

    for chunk_idx in range(n_chunks):
        start = chunk_idx * chunk_size
        end   = min(start + chunk_size, n_events)
        chunk_len = end - start

        # Slice the Arrow table for this chunk
        chunk = events_full.slice(start, chunk_len)

        # Convert chunk to Python lists
        ts_recv_arr  = chunk.column("ts_recv").to_pylist()
        ts_event_arr = chunk.column("ts_event").to_pylist()
        seq_arr      = chunk.column("sequence").to_pylist()
        oid_arr      = chunk.column("order_id").to_pylist()
        action_arr   = chunk.column("action").to_pylist()
        side_arr     = chunk.column("side").to_pylist()
        price_arr    = chunk.column("price").to_pylist()
        size_arr     = chunk.column("size").to_pylist()
        flags_arr    = chunk.column("flags").to_pylist()
        source_arr   = chunk.column("source").to_pylist()

        # Pre-allocate output lists for this chunk
        out_ts_recv   = [None] * chunk_len
        out_ts_event  = [None] * chunk_len
        out_seq       = [None] * chunk_len
        out_oid       = [None] * chunk_len
        out_action    = [None] * chunk_len
        out_side      = [None] * chunk_len
        out_price     = [None] * chunk_len
        out_size      = [None] * chunk_len
        out_flags     = [None] * chunk_len
        out_bid_px    = [None] * chunk_len
        out_ask_px    = [None] * chunk_len
        out_bid_sz    = [None] * chunk_len
        out_ask_sz    = [None] * chunk_len
        out_bid_ct    = [None] * chunk_len
        out_ask_ct    = [None] * chunk_len
        out_mid_px    = [None] * chunk_len
        out_spread    = [None] * chunk_len
        out_tob_chg   = [None] * chunk_len
        out_source    = [None] * chunk_len

        # ── Process events in this chunk ──────────────────────────────────
        for i in range(chunk_len):
            action   = action_arr[i]
            order_id = oid_arr[i]
            price_fp = price_arr[i]
            size     = size_arr[i]
            side     = side_arr[i]
            flags    = flags_arr[i]

            is_snapshot = bool(flags & F_SNAPSHOT)
            lob.apply(action, order_id, price_fp, size, side, is_snapshot=is_snapshot)

            bid_px, bid_sz, bid_ct = lob.best_bid()
            ask_px, ask_sz, ask_ct = lob.best_ask()

            if bid_px is not None and ask_px is not None:
                mid_px = (bid_px + ask_px) // 2
                spread = ask_px - bid_px
                if spread <= 0:
                    n_crossed += 1
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
            out_ask_px[i]   = ask_px
            out_bid_sz[i]   = bid_sz
            out_ask_sz[i]   = ask_sz
            out_bid_ct[i]   = bid_ct
            out_ask_ct[i]   = ask_ct
            out_mid_px[i]   = mid_px
            out_spread[i]   = spread
            out_tob_chg[i]  = tob_changed
            out_source[i]   = source_arr[i]

        # ── Write this chunk to Parquet ───────────────────────────────────
        symbol_arr = [symbol] * chunk_len

        chunk_table = pa.table(
            {
                "ts_recv":     pa.array(out_ts_recv,  type=pa.uint64()),
                "ts_event":    pa.array(out_ts_event, type=pa.uint64()),
                "sequence":    pa.array(out_seq,      type=pa.uint32()),
                "order_id":    pa.array(out_oid,      type=pa.uint64()),
                "action":      pa.array(out_action,   type=pa.string()),
                "side":        pa.array(out_side,     type=pa.string()),
                "price":       pa.array(out_price,    type=pa.int64()),
                "size":        pa.array(out_size,     type=pa.uint32()),
                "flags":       pa.array(out_flags,    type=pa.uint8()),
                "symbol":      pa.array(symbol_arr,   type=pa.string()),
                "bid_px_00":   pa.array(out_bid_px,   type=pa.int64()),
                "ask_px_00":   pa.array(out_ask_px,   type=pa.int64()),
                "bid_sz_00":   pa.array(out_bid_sz,   type=pa.uint32()),
                "ask_sz_00":   pa.array(out_ask_sz,   type=pa.uint32()),
                "bid_ct_00":   pa.array(out_bid_ct,   type=pa.uint32()),
                "ask_ct_00":   pa.array(out_ask_ct,   type=pa.uint32()),
                "mid_px":      pa.array(out_mid_px,   type=pa.int64()),
                "spread":      pa.array(out_spread,   type=pa.int64()),
                "tob_changed": pa.array(out_tob_chg,  type=pa.bool_()),
                "source":      pa.array(out_source,   type=pa.string()),
            },
            schema=LOB1_SCHEMA,
        )
        writer.write_table(chunk_table)

        # Free chunk memory before next iteration
        del chunk, chunk_table
        del ts_recv_arr, ts_event_arr, seq_arr, oid_arr, action_arr
        del side_arr, price_arr, size_arr, flags_arr, source_arr
        del out_ts_recv, out_ts_event, out_seq, out_oid, out_action
        del out_side, out_price, out_size, out_flags, out_bid_px, out_ask_px
        del out_bid_sz, out_ask_sz, out_bid_ct, out_ask_ct
        del out_mid_px, out_spread, out_tob_chg, out_source, symbol_arr

        if n_chunks > 1:
            log.info(f"    chunk {chunk_idx+1}/{n_chunks} done ({end:,}/{n_events:,})")

    writer.close()

    # Free the full input table
    del events_full

    elapsed = time.time() - t0
    stats = {
        "date":              date_str,
        "symbol":            symbol,
        "n_events":          n_events,
        "n_snapshot":        lob.n_snapshot_events,
        "n_tob_changes":     n_tob_chg,
        "pct_tob_chg":       round(100.0 * n_tob_chg / n_events, 2) if n_events else 0,
        "n_crossed_book":    n_crossed,
        "n_no_tob":          n_no_tob,
        "n_aggressive_adds": lob.n_aggressive_adds,
        "n_overfills":       lob.n_overfills,
        "n_warnings":        lob.n_warnings,
        "elapsed_s":         round(elapsed, 1),
        "skipped":           False,
    }

    log.info(
        f"    [OK] {symbol} {date_str} | {n_events:,} events | "
        f"snap={lob.n_snapshot_events} | "
        f"TOB_chg={stats['pct_tob_chg']:.1f}% | "
        f"crossed={n_crossed} | no_tob={n_no_tob} | "
        f"agg_add={lob.n_aggressive_adds:,} | "
        f"overfill={lob.n_overfills:,} | "
        f"warn={lob.n_warnings} | {elapsed:.1f}s"
    )
    return stats


# ---------------------------------------------------------------------------
# PROCESS ONE DAY (all instruments)
# ---------------------------------------------------------------------------

def process_day(
    product: str,
    date_str: str,
    tick_fp: int,
    con: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Process all outright instruments for one day.

    Returns list of stats dicts (one per instrument).
    """
    instruments = discover_instruments(product, date_str, con)
    if not instruments:
        log.warning(f"No instruments found for {product} {date_str}")
        return [{"date": date_str, "skipped": True, "reason": "no_instruments"}]

    log.info(f"[DAY] {product} {date_str} — {len(instruments)} instruments: {instruments}")

    day_stats = []
    for symbol in instruments:
        try:
            stats = process_instrument_day(product, date_str, symbol, tick_fp, con)
            day_stats.append(stats)
        except Exception as e:
            log.error(f"  [ERROR] {symbol} {date_str}: {e}", exc_info=True)
            day_stats.append({"date": date_str, "symbol": symbol, "error": str(e)})

    return day_stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CME Globex MBO → LOB1 reconstruction v2 (full session, per-instrument)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Full electronic session reconstruction with F_SNAPSHOT initialization.
Replaces v1's RTH-only + warmup hack approach.

Output: one Parquet file per instrument per day in data/lob1/

Examples:
  python build_cme_lob1.py --product ES --from-date 20251001 --to-date 20251001
  python build_cme_lob1.py --product ES --from-date 20251001 --to-date 20251031
  python build_cme_lob1.py --product NIY --from-date 20250101 --to-date 20251231
        """,
    )
    parser.add_argument("--product", required=True, choices=AVAILABLE_PRODUCTS)
    parser.add_argument("--from-date", default=None, help="YYYYMMDD")
    parser.add_argument("--to-date", default=None, help="YYYYMMDD")
    parser.add_argument("--symbol", default=None, help="Single instrument (e.g. ESZ5). Default: all outrights.")
    parser.add_argument("--resume", action="store_true", help="Skip days where output already exists")
    return parser.parse_args()


def main() -> None:
    args    = _parse_args()
    product = args.product
    cfg     = PRODUCT_CONFIG[product]
    tick_fp = cfg["tick_size_fp"]

    # Discover available dates from raw data (prefer unsplit MBO, fall back to split)
    mbo_product_root = RAW_MBO_ROOT / f"product={product}"
    raw_product_root = RAW_ROOT / f"product={product}"

    mbo_files = sorted(mbo_product_root.rglob("*_mbo.parquet")) if mbo_product_root.exists() else []
    order_files = sorted(raw_product_root.rglob("*_orders.parquet")) if raw_product_root.exists() else []

    if mbo_files:
        all_dates = [f.stem.split("_")[1] for f in mbo_files]
        data_source = "unsplit MBO"
    elif order_files:
        all_dates = [f.stem.split("_")[1] for f in order_files]
        data_source = "split orders+trades (fallback)"
    else:
        raise FileNotFoundError(
            f"No raw data found for {product} in {mbo_product_root} or {raw_product_root}"
        )

    all_dates = [f.stem.split("_")[1] for f in order_files]
    if args.from_date:
        all_dates = [d for d in all_dates if d >= args.from_date]
    if args.to_date:
        all_dates = [d for d in all_dates if d <= args.to_date]

    print(f"\n{'=' * 70}")
    print(f"  build_cme_lob1.py  [Full session — per instrument]")
    print(f"  Product   : {product}  ({cfg['description']})")
    print(f"  Tick size : {tick_fp / 1e9:.4f} pt")
    print(f"  Data src  : {data_source}")
    print(f"  Days      : {len(all_dates)}")
    print(f"  Symbol    : {args.symbol or 'all outrights'}")
    print(f"  Resume    : {args.resume}")
    if args.from_date:
        print(f"  From date : {args.from_date}")
    if args.to_date:
        print(f"  To date   : {args.to_date}")
    print(f"{'=' * 70}\n")

    con = duckdb.connect()
    all_stats = []

    for date_str in all_dates:
        if args.symbol:
            # Single instrument mode
            year = date_str[:4]
            mon  = date_str[4:6]
            out_path = (
                LOB1_ROOT / f"product={product}"
                / f"year={year}" / f"month={mon}"
                / f"{product}_{date_str}_{args.symbol}_lob1.parquet"
            )
            if args.resume and out_path.exists():
                log.info(f"[SKIP] {args.symbol} {date_str} — already exists (resume)")
                all_stats.append({"date": date_str, "symbol": args.symbol, "skipped": True})
                continue

            try:
                stats = process_instrument_day(
                    product, date_str, args.symbol, tick_fp, con
                )
                all_stats.append(stats)
            except Exception as e:
                log.error(f"[ERROR] {args.symbol} {date_str}: {e}", exc_info=True)
                all_stats.append({"date": date_str, "symbol": args.symbol, "error": str(e)})
        else:
            # All instruments mode
            day_stats = process_day(product, date_str, tick_fp, con)
            all_stats.extend(day_stats)

    con.close()

    # ── Final summary ─────────────────────────────────────────────────────
    log.info(f"\n{'=' * 70}")
    log.info(f"  SUMMARY — {product} LOB1")
    log.info(f"{'=' * 70}")
    for s in all_stats:
        if s.get("skipped"):
            reason = s.get("reason", "resume")
            log.info(f"  {s['date']}  {s.get('symbol', '?'):>8s}  SKIPPED ({reason})")
        elif "error" in s:
            log.info(f"  {s['date']}  {s.get('symbol', '?'):>8s}  ERROR: {s['error']}")
        else:
            log.info(
                f"  {s['date']}  {s['symbol']:>8s}  "
                f"{s['n_events']:>10,} events  "
                f"snap={s['n_snapshot']:>4}  "
                f"TOB_chg={s['pct_tob_chg']:>5.1f}%  "
                f"crossed={s['n_crossed_book']:>5}  "
                f"no_tob={s['n_no_tob']:>6}  "
                f"agg_add={s['n_aggressive_adds']:>6,}  "
                f"warn={s['n_warnings']:>3}  "
                f"{s['elapsed_s']}s"
            )

    n_ok      = sum(1 for s in all_stats if not s.get("skipped") and "error" not in s)
    n_skipped = sum(1 for s in all_stats if s.get("skipped"))
    n_errors  = sum(1 for s in all_stats if "error" in s)
    log.info(f"\n  Processed={n_ok}  Skipped={n_skipped}  Errors={n_errors}")
    log.info(f"{'=' * 70}")

    if n_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()