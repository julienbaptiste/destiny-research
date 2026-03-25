"""
reconstruction/build_mbp1.py — MBO → MBP-1 reconstruction engine.

Reads normalized MBO Parquet files (output of ingestion pipeline) and
reconstructs the top-of-book (MBP-1) state for each day.

Output schema mirrors Databento's native MBP-1 format exactly, enabling
direct comparison via tests/validation/validate_mbp1.py:
    ts_event, ts_recv      — uint64 nanoseconds UTC
    action                 — string (ADD/CANCEL/MODIFY/TRADE/FILL/CLEAR/NONE)
    side                   — string (BID/ASK/NONE)
    price                  — float64 (fixed-point / FIXED_PRICE_SCALE)
    flags                  — uint8
    sequence               — uint32
    bid_px_00, ask_px_00   — float64 (real price = fixed-point / 1e9)
    bid_sz_00, ask_sz_00   — uint32
    bid_ct_00, ask_ct_00   — uint32

Emission rule:
    A snapshot row is emitted when ALL of the following hold:
      1. F_LAST (0x80) is set — the atomic event group is complete.
      2. At least one event in the group modified the book state
         (action ADD, CANCEL, MODIFY, or CLEAR).
    Additionally, TRADE events trigger an emission (post-trade TOB snapshot)
    to match the Databento MBP-1 reference format which includes T rows.

    F_SNAPSHOT events (warmup bootstrap) are applied to seed the book but
    do NOT emit output rows — they are not live market events.

Book state machine (adapted from Databento's official algorithm):
    - SortedDict bids (descending) / offers (ascending) keyed by price level
    - orders_by_id dict for O(1) lookup on CANCEL and MODIFY
    - F_TOB messages replace an entire book side (synthetic aggregate)
    - MODIFY with price change → loses queue priority
    - MODIFY with size increase → loses queue priority
    - MODIFY with unknown order_id → treated as ADD (GTC cross-session)
    - CANCEL to zero → removes order and level if empty

GTC orphan handling (EUREX):
    CANCEL/MODIFY for order_ids not in orders_by_id are silently tolerated
    (MODIFY falls back to ADD, CANCEL is skipped with a warning counter).
    These are GTC orders placed J-1 whose ADD is in the previous day's file.

Usage:
    # Single day
    python reconstruction/build_mbp1.py --product ES --contract ESZ25 --date 2025-10-01

    # Full month batch
    python reconstruction/build_mbp1.py --product ES --contract ESZ25 --month 2025-10

    # All contracts for a product/month (auto-discovers contracts from normalized dir)
    python reconstruction/build_mbp1.py --product ES --month 2025-10 --all-contracts

    # Overwrite existing outputs
    python reconstruction/build_mbp1.py --product ES --month 2025-10 --overwrite
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from itertools import takewhile
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from sortedcontainers import SortedDict

# ---------------------------------------------------------------------------
# Repo root + project imports
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT))

from config import DATA_NORMALIZED, DATA_RECONSTRUCTED  # noqa: E402
from ingestion.market_config import MARKET_CONFIG        # noqa: E402
from ingestion.schema import (                           # noqa: E402
    Action,
    Flags,
    Side,
    FIXED_PRICE_SCALE,
    UNDEF_PRICE,
    reconstructed_path,
)

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
# Output schema
# ---------------------------------------------------------------------------

# Mirrors Databento MBP-1 native schema for direct validation comparison.
# Prices are float64 (real values = fixed-point / FIXED_PRICE_SCALE).
# Timestamp columns are uint64 nanoseconds since Unix epoch.
MBP1_SCHEMA = pa.schema([
    pa.field("ts_event",   pa.uint64(),  nullable=False),
    pa.field("ts_recv",    pa.uint64(),  nullable=False),
    pa.field("action",     pa.string(),  nullable=False),
    pa.field("side",       pa.string(),  nullable=False),
    pa.field("price",      pa.float64(), nullable=False),  # triggering event price
    pa.field("flags",      pa.uint8(),   nullable=False),
    pa.field("sequence",   pa.uint32(),  nullable=False),
    pa.field("bid_px_00",  pa.float64(), nullable=True),   # None if no bid
    pa.field("ask_px_00",  pa.float64(), nullable=True),   # None if no ask
    pa.field("bid_sz_00",  pa.uint32(),  nullable=True),
    pa.field("ask_sz_00",  pa.uint32(),  nullable=True),
    pa.field("bid_ct_00",  pa.uint32(),  nullable=True),
    pa.field("ask_ct_00",  pa.uint32(),  nullable=True),
])

# Row group size for Parquet output — tuned for sequential scan in DuckDB
_ROW_GROUP_SIZE = 500_000

# ---------------------------------------------------------------------------
# Book state machine — adapted from Databento's official algorithm
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class _Order:
    """Single resting order in the book."""
    order_id : int
    price    : int    # fixed-point int64
    size     : int    # remaining size
    side     : str    # Side.BID or Side.ASK
    flags    : int    # original flags bitmask


@dataclass(slots=True)
class _PriceLevel:
    """Aggregated state of one price level."""
    price  : int
    size   : int = 0
    count  : int = 0   # number of non-F_TOB orders


@dataclass(slots=True)
class _LevelOrders:
    """
    All orders resting at one price level (preserves queue order).

    size and count are maintained incrementally on every add/remove —
    never recomputed from scratch. This eliminates the O(n) sum() call
    that was the primary bottleneck (458M enum.__and__ calls in profiling).

    count: number of non-F_TOB orders (real orders, excludes synthetic)
    size:  total resting size across all orders at this level
    """
    price  : int
    orders : list[_Order] = field(default_factory=list)
    size   : int = 0    # maintained incrementally
    count  : int = 0    # maintained incrementally (excludes F_TOB orders)

    def __bool__(self) -> bool:
        return bool(self.orders)

    @property
    def level(self) -> _PriceLevel:
        """Return a _PriceLevel snapshot. size/count are already up-to-date."""
        return _PriceLevel(price=self.price, size=self.size, count=self.count)

    def add_order(self, order: _Order, is_tob: bool) -> None:
        """Append order and update incremental aggregates."""
        self.orders.append(order)
        self.size += order.size
        if not is_tob:
            self.count += 1

    def remove_order(self, order: _Order, is_tob: bool) -> None:
        """Remove order and update incremental aggregates."""
        try:
            self.orders.remove(order)
        except ValueError:
            return
        self.size  -= order.size
        if not is_tob:
            self.count -= 1

    def update_size(self, old_size: int, new_size: int) -> None:
        """Update incremental size after an in-place modify (no priority change)."""
        self.size += new_size - old_size


class Book:
    """
    Single-instrument limit order book.

    Maintains full order-level state for MBO reconstruction.
    bids: SortedDict keyed by price (ascending) — peek last for best bid.
    offers: SortedDict keyed by price (ascending) — peek first for best ask.
    orders_by_id: dict for O(1) lookup on CANCEL/MODIFY.
    """

    __slots__ = ("orders_by_id", "offers", "bids", "_n_orphan_cancel",
                 "_n_orphan_modify")

    def __init__(self) -> None:
        self.orders_by_id   : dict[int, _Order]            = {}
        self.offers         : SortedDict[int, _LevelOrders] = SortedDict()
        self.bids           : SortedDict[int, _LevelOrders] = SortedDict()
        # Counters for GTC orphan events (EUREX cross-session orders)
        self._n_orphan_cancel : int = 0
        self._n_orphan_modify : int = 0

    # ------------------------------------------------------------------
    # TOB access
    # ------------------------------------------------------------------

    def best_bid(self) -> _PriceLevel | None:
        """Return the best bid level, or None if the bid side is empty."""
        if self.bids:
            lo = self.bids.peekitem(-1)[1]
            return _PriceLevel(price=lo.price, size=lo.size, count=lo.count)
        return None

    def best_ask(self) -> _PriceLevel | None:
        """Return the best ask level, or None if the ask side is empty."""
        if self.offers:
            lo = self.offers.peekitem(0)[1]
            return _PriceLevel(price=lo.price, size=lo.size, count=lo.count)
        return None

    # ------------------------------------------------------------------
    # Event application
    # ------------------------------------------------------------------

    def apply(self, event: dict) -> bool:
        """
        Apply one normalized MBO event to the book state.

        Returns True if the book state changed (TOB may have been affected),
        False if the event was ignored (TRADE, FILL, NONE).

        F_SNAPSHOT events must be applied (they seed the book) but the caller
        is responsible for suppressing output rows for warmup events.

        Args:
            event: normalized event dict from NORMALIZED_MBO_SCHEMA.

        Returns:
            True if book state was modified, False otherwise.
        """
        action = event["action"]
        side   = event["side"]
        flags  = event["flags"]

        # TRADE, FILL, NONE — no book state change
        if action in (Action.TRADE, Action.FILL, Action.NONE):
            return False

        # CLEAR — wipe the entire book
        if action == Action.CLEAR:
            self._clear()
            return True

        # F_TOB with UNDEF_PRICE — clear one side entirely
        if (flags & Flags.F_TOB) and event["price"] == UNDEF_PRICE:
            self._side_levels(side).clear()
            return True

        # Standard order-level actions require a valid side
        if side not in (Side.BID, Side.ASK):
            return False

        if action == Action.ADD:
            self._add(event)
        elif action == Action.CANCEL:
            self._cancel(event)
        elif action == Action.MODIFY:
            self._modify(event)
        else:
            return False

        return True

    # ------------------------------------------------------------------
    # Internal mutators
    # ------------------------------------------------------------------

    def _clear(self) -> None:
        """Hard reset — remove all resting orders from both sides."""
        self.orders_by_id.clear()
        self.offers.clear()
        self.bids.clear()

    def _add(self, event: dict) -> None:
        """Insert a new order (or replace a book side for F_TOB)."""
        price    = event["price"]
        size     = event["size"]
        side     = event["side"]
        order_id = event["order_id"]
        flags    = event["flags"]

        order = _Order(
            order_id=order_id,
            price=price,
            size=size,
            side=side,
            flags=flags,
        )

        if flags & Flags.F_TOB:
            # F_TOB ADD: replace the entire side with a single synthetic level.
            # order_id=0 on F_TOB messages — do NOT insert into orders_by_id.
            levels = self._side_levels(side)
            levels.clear()
            levels[price] = _LevelOrders(price=price, orders=[order])
        else:
            # Normal ADD: insert order at the end of its price level queue.
            level = self._get_or_insert_level(price, side)
            # Duplicate order_id guard — should not happen on clean data
            if order_id in self.orders_by_id:
                log.debug("Duplicate order_id=%d on ADD — overwriting", order_id)
            self.orders_by_id[order_id] = order
            level.orders.append(order)

    def _cancel(self, event: dict) -> None:
        """Partially or fully cancel a resting order."""
        order_id = event["order_id"]
        price    = event["price"]
        side     = event["side"]
        size     = event["size"]   # size being cancelled (delta)

        if order_id not in self.orders_by_id:
            # GTC orphan: ADD was in the previous day's file — skip silently
            self._n_orphan_cancel += 1
            return

        order = self.orders_by_id[order_id]
        level = self._get_level(price, side)
        if level is None:
            # Price level missing — data integrity issue, skip
            log.debug("CANCEL: level not found price=%d side=%s", price, side)
            return

        # Reduce the order's remaining size
        order.size = max(0, order.size - size)

        if order.size == 0:
            # Fully cancelled — remove from book
            self.orders_by_id.pop(order_id)
            try:
                level.orders.remove(order)
            except ValueError:
                pass
            # Remove the price level if it's now empty
            if not level:
                self._remove_level(price, side)

    def _modify(self, event: dict) -> None:
        """
        Modify the price and/or size of a resting order.

        Queue priority rules (standard exchange convention):
          - Price change → loses priority (moved to end of new level's queue)
          - Size increase → loses priority (moved to end of same level's queue)
          - Size decrease → keeps priority (updated in place)

        If order_id is not found (GTC cross-session orphan), treat as ADD.
        """
        order_id  = event["order_id"]
        new_price = event["price"]
        new_size  = event["size"]
        side      = event["side"]
        flags     = event["flags"]

        if order_id not in self.orders_by_id:
            # GTC orphan MODIFY: treat as ADD to seed the book state
            self._n_orphan_modify += 1
            self._add(event)
            return

        order = self.orders_by_id[order_id]
        level = self._get_level(order.price, order.side)
        if level is None:
            log.debug("MODIFY: level not found price=%d side=%s", order.price, side)
            self._add(event)
            return

        if order.price != new_price:
            # Price change: remove from current level, insert at end of new level
            try:
                level.orders.remove(order)
            except ValueError:
                pass
            if not level:
                self._remove_level(order.price, side)
            order.price = new_price
            order.size  = new_size
            order.flags = flags
            new_level = self._get_or_insert_level(new_price, side)
            new_level.orders.append(order)

        elif new_size > order.size:
            # Size increase: loses priority — move to end of same level
            try:
                level.orders.remove(order)
            except ValueError:
                pass
            order.size  = new_size
            order.flags = flags
            level.orders.append(order)

        else:
            # Size decrease (or no change): update in place, keep priority
            order.size  = new_size
            order.flags = flags

        self.orders_by_id[order_id] = order

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _side_levels(self, side: str) -> SortedDict:
        """Return the SortedDict for the given side."""
        if side == Side.ASK:
            return self.offers
        if side == Side.BID:
            return self.bids
        raise ValueError(f"Invalid side '{side}'")

    def _get_level(self, price: int, side: str) -> _LevelOrders | None:
        """Return the _LevelOrders for a (price, side) pair, or None."""
        levels = self._side_levels(side)
        return levels.get(price)

    def _get_or_insert_level(self, price: int, side: str) -> _LevelOrders:
        """Return the _LevelOrders for (price, side), inserting if absent."""
        levels = self._side_levels(side)
        if price not in levels:
            levels[price] = _LevelOrders(price=price)
        return levels[price]

    def _remove_level(self, price: int, side: str) -> None:
        """Remove an empty price level from the book."""
        levels = self._side_levels(side)
        levels.pop(price, None)


# ---------------------------------------------------------------------------
# Market — multi-instrument, multi-publisher container
# ---------------------------------------------------------------------------

class Market:
    """
    Container for all per-instrument, per-publisher Books.

    Keyed on (instrument_id, publisher_id) — same as Databento's official algo.
    For single-publisher files (typical normalized Parquet), publisher_id is
    constant but we keep the structure for correctness.
    """

    __slots__ = ("_books",)

    def __init__(self) -> None:
        # instrument_id → publisher_id → Book
        self._books: defaultdict[int, defaultdict[int, Book]] = defaultdict(
            lambda: defaultdict(Book)
        )

    def get_book(self, instrument_id: int, publisher_id: int) -> Book:
        return self._books[instrument_id][publisher_id]

    def apply(self, event: dict) -> bool:
        """Apply event to the appropriate book. Returns True if state changed."""
        book = self._books[event["instrument_id"]][event["publisher_id"]]
        return book.apply(event)

    def best_bid(self, instrument_id: int, publisher_id: int) -> _PriceLevel | None:
        return self._books[instrument_id][publisher_id].best_bid()

    def best_ask(self, instrument_id: int, publisher_id: int) -> _PriceLevel | None:
        return self._books[instrument_id][publisher_id].best_ask()


# ---------------------------------------------------------------------------
# Snapshot builder
# ---------------------------------------------------------------------------

_SCALE = float(FIXED_PRICE_SCALE)  # 1e9 — used for fixed-point → float conversion


def _apply_book(
    market   : Market,
    action   : str,
    side     : str,
    price    : int,
    size     : int,
    order_id : int,
    flags    : int,
    iid      : int,
    pid      : int,
) -> bool:
    """
    Apply one MBO event to the appropriate book in market.

    Standalone function (not a method) to avoid attribute lookup overhead
    in the hot loop. Takes individual fields rather than a dict.

    Returns True if book state changed, False if event was ignored.
    """
    book = market._books[iid][pid]

    # TRADE, FILL, NONE — no book state change
    if action == Action.TRADE or action == Action.FILL or action == Action.NONE:
        return False

    # CLEAR — wipe the entire book
    if action == Action.CLEAR:
        book._clear()
        return True

    # F_TOB with UNDEF_PRICE — clear one side entirely
    # Use int literals for bitmask ops — avoids enum.__and__ overhead
    if (flags & 0x01) and price == UNDEF_PRICE:
        book._side_levels(side).clear()
        return True

    # Standard order-level actions require a valid side
    if side != Side.BID and side != Side.ASK:
        return False

    # Build a minimal _Order-compatible event for the book mutators.
    # We pass individual fields to avoid dict allocation.
    if action == Action.ADD:
        _book_add(book, price, size, side, order_id, flags)
    elif action == Action.CANCEL:
        _book_cancel(book, price, size, side, order_id)
    elif action == Action.MODIFY:
        _book_modify(book, price, size, side, order_id, flags)
    else:
        return False

    return True


def _book_add(
    book     : Book,
    price    : int,
    size     : int,
    side     : str,
    order_id : int,
    flags    : int,
) -> None:
    """Insert a new order (or replace a book side for F_TOB)."""
    # Use int literal for F_TOB check — avoids enum overhead in hot path
    is_tob = bool(flags & 0x01)
    order  = _Order(order_id=order_id, price=price, size=size, side=side, flags=flags)

    if is_tob:
        # F_TOB ADD: replace the entire side with a single synthetic level.
        # order_id=0 on F_TOB messages — do NOT insert into orders_by_id.
        levels = book._side_levels(side)
        levels.clear()
        lo = _LevelOrders(price=price, size=size, count=0)  # F_TOB: count=0
        lo.orders.append(order)
        levels[price] = lo
    else:
        level = book._get_or_insert_level(price, side)
        book.orders_by_id[order_id] = order
        level.add_order(order, is_tob=False)


def _book_cancel(
    book     : Book,
    price    : int,
    size     : int,
    side     : str,
    order_id : int,
) -> None:
    """Partially or fully cancel a resting order."""
    if order_id not in book.orders_by_id:
        book._n_orphan_cancel += 1
        return
    order = book.orders_by_id[order_id]
    level = book._get_level(price, side)
    if level is None:
        return

    old_size   = order.size
    order.size = max(0, order.size - size)

    if order.size == 0:
        # Fully cancelled — remove from book
        book.orders_by_id.pop(order_id)
        is_tob = bool(order.flags & 0x01)
        level.size -= old_size      # ← utilise old_size, avant mutation
        if not is_tob:
            level.count -= 1
        try:
            level.orders.remove(order)
        except ValueError:
            pass
        if not level:
            book._remove_level(price, side)
    else:
        # Partial cancel — update incremental size in place
        level.update_size(old_size, order.size)


def _book_modify(
    book     : Book,
    price    : int,
    size     : int,
    side     : str,
    order_id : int,
    flags    : int,
) -> None:
    """Modify price and/or size of a resting order."""
    if order_id not in book.orders_by_id:
        book._n_orphan_modify += 1
        _book_add(book, price, size, side, order_id, flags)
        return

    order  = book.orders_by_id[order_id]
    level  = book._get_level(order.price, order.side)
    is_tob = bool(order.flags & 0x01)

    if level is None:
        _book_add(book, price, size, side, order_id, flags)
        return

    if order.price != price:
        # Price change: remove from current level, append to new level (priority lost)
        level.remove_order(order, is_tob=is_tob)
        if not level:
            book._remove_level(order.price, side)
        order.price = price
        order.size  = size
        order.flags = flags
        new_level = book._get_or_insert_level(price, side)
        new_level.add_order(order, is_tob=is_tob)

    elif size > order.size:
        # Size increase: loses priority — move to end of queue
        level.remove_order(order, is_tob=is_tob)
        order.size  = size
        order.flags = flags
        level.add_order(order, is_tob=is_tob)

    else:
        # Size decrease or no change: update in place, keep priority
        old_size   = order.size
        order.size  = size
        order.flags = flags
        level.update_size(old_size, size)

    book.orders_by_id[order_id] = order


# ---------------------------------------------------------------------------
# Core reconstruction loop
# ---------------------------------------------------------------------------

def reconstruct_day(
    mbo_file     : Path,
    out_file     : Path,
    product      : str,
    contract     : str,
) -> dict:
    """
    Reconstruct MBP-1 for one day from a normalized MBO Parquet file.

    Reads the MBO file in streaming batches (READ_BATCH_SIZE rows at a time)
    to cap memory usage. Each batch is extracted to Python lists, processed
    through the book state machine, and output rows are flushed to Parquet
    every WRITE_FLUSH_ROWS rows.

    Memory profile (ES ~7.4M rows/day, READ_BATCH_SIZE=100_000):
      - Arrow batch in memory: ~8MB at a time (vs ~600MB for full file)
      - Python column lists: ~10MB per batch (released after each batch)
      - Output row buffer: ~2MB at WRITE_FLUSH_ROWS=50_000

    Atomic event group handling across batch boundaries:
      Groups (F_LAST) are rarely split across batches in practice (event
      groups are typically 1-4 events). The group_state_changed and
      group_has_trade accumulators are maintained across batch iterations —
      only reset on F_LAST regardless of batch boundary.

    F_SNAPSHOT events (warmup bootstrap) are applied to seed the book but
    do not produce output rows.

    Args:
        mbo_file: path to normalized MBO Parquet file.
        out_file: path for the output MBP-1 Parquet file.
        product:  product ticker (e.g. 'ES') — used for logging only.
        contract: contract symbol (e.g. 'ESZ25') — used for logging only.

    Returns:
        Stats dict: n_events, n_rows_emitted, n_orphan_cancel, n_orphan_modify,
                    elapsed_seconds.
    """
    # Number of MBO rows read per Arrow batch — controls peak RAM per batch.
    # 100K rows ≈ 8MB Arrow + 10MB Python lists — safe on 16GB with other processes.
    READ_BATCH_SIZE = 100_000

    # Flush output rows to Parquet every N rows — bounds output buffer memory.
    WRITE_FLUSH_ROWS = 50_000

    t0 = time.perf_counter()

    # Pre-compute flag constants as plain ints — enum.__and__ was the #1 bottleneck
    # (458M calls to enum.__and__ / enum._get_value in profiling = 1150s wasted)
    _F_LAST     = 0x80   # Flags.F_LAST
    _F_SNAPSHOT = 0x20   # Flags.F_SNAPSHOT
    _TRADE      = Action.TRADE
    _FILL       = Action.FILL

    # State machine — persists across all batches for this day
    market         = Market()
    rows           : list[dict] = []
    n_events       = 0
    n_rows_emitted = 0

    # Group state accumulators — survive batch boundaries
    group_state_changed : bool = False
    group_has_trade     : bool = False

    # Output file — open before the batch loop
    out_file.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(out_file, MBP1_SCHEMA, compression="zstd")

    def _flush_output(rows: list[dict]) -> None:
        """Write accumulated output rows to Parquet and clear the buffer."""
        if not rows:
            return
        batch = pa.RecordBatch.from_pydict(
            {col: [r[col] for r in rows] for col in MBP1_SCHEMA.names},
            schema=MBP1_SCHEMA,
        )
        writer.write_batch(batch)

    def _col_from_batch(batch: pa.RecordBatch, name: str) -> list:
        """Extract a column from an Arrow RecordBatch as a Python list.
        Dictionary-encoded columns are cast to plain strings first."""
        arr = batch.column(name)
        if pa.types.is_dictionary(arr.type):
            arr = arr.cast(pa.string())
        return arr.to_pylist()

    try:
        pf = pq.ParquetFile(mbo_file)

        for arrow_batch in pf.iter_batches(batch_size=READ_BATCH_SIZE):
            batch_len = len(arrow_batch)
            n_events += batch_len

            # Extract all needed columns as Python lists — O(batch_size) allocation.
            # Released at end of each batch iteration (Python GC via local scope).
            c_ts_event      = _col_from_batch(arrow_batch, "ts_event")
            c_ts_recv       = _col_from_batch(arrow_batch, "ts_recv")
            c_action        = _col_from_batch(arrow_batch, "action")
            c_side          = _col_from_batch(arrow_batch, "side")
            c_price         = _col_from_batch(arrow_batch, "price")
            c_size          = _col_from_batch(arrow_batch, "size")
            c_order_id      = _col_from_batch(arrow_batch, "order_id")
            c_flags         = _col_from_batch(arrow_batch, "flags")
            c_sequence      = _col_from_batch(arrow_batch, "sequence")
            c_instrument_id = _col_from_batch(arrow_batch, "instrument_id")
            c_publisher_id  = _col_from_batch(arrow_batch, "publisher_id")

            # Release the Arrow batch immediately — column lists are sufficient
            del arrow_batch

            # --- Hot loop over this batch ---
            for i in range(batch_len):
                flags    = c_flags[i]
                is_snap  = bool(flags & _F_SNAPSHOT)
                is_last  = bool(flags & _F_LAST)
                action   = c_action[i]
                side     = c_side[i]
                price    = c_price[i]
                size     = c_size[i]
                order_id = c_order_id[i]
                iid      = c_instrument_id[i]
                pid      = c_publisher_id[i]

                # Apply to book state machine
                changed = _apply_book(
                    market, action, side, price, size, order_id, flags, iid, pid
                )

                if not is_snap:
                    if changed:
                        group_state_changed = True
                    if action == _TRADE or action == _FILL:
                        group_has_trade = True

                if is_last:
                    # Emit snapshot at end of atomic event group
                    if not is_snap and (group_state_changed or group_has_trade):
                        bid = market.best_bid(iid, pid)
                        ask = market.best_ask(iid, pid)
                        rows.append({
                            "ts_event"  : c_ts_event[i],
                            "ts_recv"   : c_ts_recv[i],
                            "action"    : action,
                            "side"      : side,
                            "price"     : price / _SCALE,
                            "flags"     : flags,
                            "sequence"  : c_sequence[i],
                            "bid_px_00" : bid.price / _SCALE if bid else None,
                            "ask_px_00" : ask.price / _SCALE if ask else None,
                            "bid_sz_00" : bid.size            if bid else None,
                            "ask_sz_00" : ask.size            if ask else None,
                            "bid_ct_00" : bid.count           if bid else None,
                            "ask_ct_00" : ask.count           if ask else None,
                        })
                        n_rows_emitted += 1

                        # Flush output buffer at threshold
                        if len(rows) >= WRITE_FLUSH_ROWS:
                            _flush_output(rows)
                            rows.clear()

                    group_state_changed = False
                    group_has_trade     = False

            # Column lists go out of scope here — GC reclaims ~10MB per batch

        # Flush any remaining output rows
        _flush_output(rows)

    finally:
        writer.close()

    # Aggregate orphan stats across all books in the market
    n_orphan_cancel = sum(
        b._n_orphan_cancel
        for books_by_pub in market._books.values()
        for b in books_by_pub.values()
    )
    n_orphan_modify = sum(
        b._n_orphan_modify
        for books_by_pub in market._books.values()
        for b in books_by_pub.values()
    )

    elapsed = time.perf_counter() - t0
    return {
        "n_events"       : n_events,
        "n_rows_emitted" : n_rows_emitted,
        "n_orphan_cancel": n_orphan_cancel,
        "n_orphan_modify": n_orphan_modify,
        "elapsed_seconds": round(elapsed, 2),
    }


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _normalized_dir(product: str, contract: str, year: int, month: int) -> Path:
    """Return the normalized data directory for a product/contract/month."""
    cfg      = MARKET_CONFIG[product]
    venue    = cfg["exchange"]
    provider = cfg["provider"]
    return (
        DATA_NORMALIZED
        / f"provider={provider}"
        / f"venue={venue}"
        / f"product={product}"
        / f"contract={contract}"
        / f"year={year}"
        / f"month={month:02d}"
    )


def _iter_mbo_files(
    product  : str,
    contract : str,
    year     : int,
    month    : int,
) -> Iterator[tuple[Path, str]]:
    """
    Yield (mbo_file_path, date_str) for all normalized MBO files for a
    given product/contract/year/month, sorted by date.

    date_str format: 'YYYYMMDD' (matches reconstructed_path convention).
    """
    norm_dir = _normalized_dir(product, contract, year, month)
    if not norm_dir.exists():
        return
    for p in sorted(norm_dir.glob(f"{contract}_*_mbo.parquet")):
    # Extract date as the first 8-digit numeric segment in the filename stem.
    # Works for both outrights (ESZ25_20251001_mbo) and
    # calendar spreads (ES_CAL_H26H27_20251001_mbo).
    # REMINDER: WATCH OUT FOR POTENTIAL NEW PRODUCTS (SHOULD WE REWRITE THIS PART?)
        match = re.search(r"(\d{8})", p.stem)
        if match:
            date_str = match.group(1)
            yield p, date_str


def _out_path(product: str, contract: str, year: int, month: int, date_str: str) -> Path:
    """Return the output path for a reconstructed MBP-1 Parquet file."""
    cfg   = MARKET_CONFIG[product]
    venue = cfg["exchange"]
    return reconstructed_path(
        base_dir  = DATA_RECONSTRUCTED,
        venue     = venue,
        product   = product,
        contract  = contract,
        year      = year,
        month     = month,
        date_str  = date_str,
        schema    = "mbp1",
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconstruct MBP-1 from normalized MBO Parquet files."
    )
    parser.add_argument(
        "--product", required=True,
        help="Product ticker (e.g. ES, FDAX, NIY).",
    )
    parser.add_argument(
        "--contract", default=None,
        help=(
            "Contract symbol (e.g. ESZ25). "
            "Required unless --all-contracts is set."
        ),
    )
    parser.add_argument(
        "--date", default=None,
        help="Single date YYYY-MM-DD. Mutually exclusive with --month.",
    )
    parser.add_argument(
        "--month", default=None,
        help="Month YYYY-MM. Processes all available days for that month.",
    )
    parser.add_argument(
        "--all-contracts", action="store_true",
        help=(
            "Auto-discover all contracts for this product/month from the "
            "normalized directory. Requires --month."
        ),
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Overwrite existing output files (default: skip).",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    if args.product not in MARKET_CONFIG:
        log.error("Unknown product '%s'. Add it to market_config.py.", args.product)
        return 1

    # Build the work list: list of (product, contract, year, month, date_str)
    work: list[tuple[str, str, int, int, str]] = []

    if args.date and args.month:
        log.error("--date and --month are mutually exclusive.")
        return 1

    if args.date:
        # Single day
        if not args.contract:
            log.error("--contract is required when --date is specified.")
            return 1
        try:
            d     = date.fromisoformat(args.date)
            d_str = d.strftime("%Y%m%d")
        except ValueError:
            log.error("Invalid date format '%s'. Expected YYYY-MM-DD.", args.date)
            return 1
        work.append((args.product, args.contract, d.year, d.month, d_str))

    elif args.month:
        try:
            month_date = datetime.strptime(args.month, "%Y-%m")
            year, month = month_date.year, month_date.month
        except ValueError:
            log.error("Invalid month format '%s'. Expected YYYY-MM.", args.month)
            return 1

        if args.all_contracts:
            # Auto-discover contracts from the normalized directory
            cfg      = MARKET_CONFIG[args.product]
            venue    = cfg["exchange"]
            provider = cfg["provider"]
            base = (
                DATA_NORMALIZED
                / f"provider={provider}"
                / f"venue={venue}"
                / f"product={args.product}"
            )
            contracts = sorted(
                p.name.replace("contract=", "")
                for p in base.glob("contract=*")
                if p.is_dir()
            )
            if not contracts:
                log.error("No contracts found under %s", base)
                return 1
            log.info("Auto-discovered contracts: %s", contracts)
        else:
            if not args.contract:
                log.error(
                    "--contract is required for --month (or use --all-contracts)."
                )
                return 1
            contracts = [args.contract]

        for contract in contracts:
            for mbo_file, date_str in _iter_mbo_files(
                args.product, contract, year, month
            ):
                work.append((args.product, contract, year, month, date_str))

    else:
        log.error("Specify either --date or --month.")
        return 1

    if not work:
        log.warning("No files to process.")
        return 0

    log.info("Reconstruction plan: %d file(s) to process.", len(work))

    n_ok = n_skip = n_fail = 0

    for product, contract, year, month, date_str in work:
        out = _out_path(product, contract, year, month, date_str)
        norm_dir = _normalized_dir(product, contract, year, month)
        mbo_file = norm_dir / f"{contract}_{date_str}_mbo.parquet"

        if not mbo_file.exists():
            log.warning("[SKIP] MBO file not found: %s", mbo_file)
            n_skip += 1
            continue

        if out.exists() and not args.overwrite:
            log.info("[SKIP] Output already exists: %s", out.name)
            n_skip += 1
            continue

        log.info("[START] %s %s %s", product, contract, date_str)
        try:
            stats = reconstruct_day(mbo_file, out, product, contract)
            log.info(
                "[DONE]  %s %s %s — "
                "events=%d  rows=%d  orphan_cancel=%d  orphan_modify=%d  %.1fs",
                product, contract, date_str,
                stats["n_events"],
                stats["n_rows_emitted"],
                stats["n_orphan_cancel"],
                stats["n_orphan_modify"],
                stats["elapsed_seconds"],
            )
            n_ok += 1
        except Exception:
            log.exception("[FAIL]  %s %s %s", product, contract, date_str)
            n_fail += 1

    log.info(
        "Summary: %d processed, %d skipped, %d failed.",
        n_ok, n_skip, n_fail,
    )
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())