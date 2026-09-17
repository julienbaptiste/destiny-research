"""
reconstruction/build_mbp1.py — canonical MBO -> MBP-1 reconstruction.

The engine is provider-neutral. It consumes the normalized event contract,
applies only state-changing actions to the resting book, and emits a consistent
MBP-1 snapshot only at F_LAST boundaries. TRADE/FILL are informational rows;
provider adapters must express deterministic book effects through ADD/CANCEL/
MODIFY/CLEAR.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
import logging
from pathlib import Path
import re
import sys
import time
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from sortedcontainers import SortedDict

from utils.logging_config import setup_logging

_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT))

from config import DATA_NORMALIZED, DATA_RECONSTRUCTED  # noqa: E402
from ingestion.market_config import MARKET_CONFIG  # noqa: E402
from ingestion.schema import (  # noqa: E402
    Action,
    Flags,
    Side,
    FIXED_PRICE_SCALE,
    UNDEF_PRICE,
    reconstructed_path,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# OUTPUT SCHEMA
# ---------------------------------------------------------------------------

MBP1_SCHEMA = pa.schema([
    pa.field("ts_event",   pa.uint64(),  nullable=False),
    pa.field("ts_recv",    pa.uint64(),  nullable=False),
    pa.field("action",     pa.string(),  nullable=False),
    pa.field("side",       pa.string(),  nullable=False),
    pa.field("price",      pa.float64(), nullable=False),
    pa.field("flags",      pa.uint8(),   nullable=False),
    pa.field("sequence",   pa.uint32(),  nullable=False),
    pa.field("subsequence", pa.uint16(), nullable=False),
    pa.field("bid_px_00",  pa.float64(), nullable=True),
    pa.field("ask_px_00",  pa.float64(), nullable=True),
    pa.field("bid_sz_00",  pa.uint32(),  nullable=True),
    pa.field("ask_sz_00",  pa.uint32(),  nullable=True),
    pa.field("bid_ct_00",  pa.uint32(),  nullable=True),
    pa.field("ask_ct_00",  pa.uint32(),  nullable=True),
])

_SCALE = float(FIXED_PRICE_SCALE)
_F_TOB = int(Flags.F_TOB)
_F_LAST = int(Flags.F_LAST)
_F_SNAPSHOT = int(Flags.F_SNAPSHOT)


# ---------------------------------------------------------------------------
# BOOK STATE
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class _Order:
    order_id: int
    price: int
    size: int
    side: str
    flags: int


@dataclass(slots=True)
class _PriceLevel:
    price: int
    size: int = 0
    count: int = 0


@dataclass(slots=True)
class _LevelOrders:
    price: int
    orders: list[_Order] = field(default_factory=list)
    size: int = 0
    count: int = 0

    def __bool__(self) -> bool:
        return bool(self.orders)

    def add_order(self, order: _Order, is_tob: bool) -> None:
        self.orders.append(order)
        self.size += order.size
        if not is_tob:
            self.count += 1

    def remove_order(self, order: _Order, is_tob: bool) -> None:
        try:
            self.orders.remove(order)
        except ValueError:
            return
        self.size -= order.size
        if not is_tob:
            self.count -= 1

    def update_size(self, old_size: int, new_size: int) -> None:
        self.size += new_size - old_size


class Book:
    """Single-instrument MBO book keyed by (order_id, side)."""

    __slots__ = (
        "orders_by_id",
        "offers",
        "bids",
        "_n_orphan_cancel",
        "_n_orphan_modify",
    )

    def __init__(self) -> None:
        self.orders_by_id: dict[tuple[int, str], _Order] = {}
        self.offers: SortedDict[int, _LevelOrders] = SortedDict()
        self.bids: SortedDict[int, _LevelOrders] = SortedDict()
        self._n_orphan_cancel = 0
        self._n_orphan_modify = 0

    def _side_levels(self, side: str) -> SortedDict[int, _LevelOrders]:
        if side == Side.ASK:
            return self.offers
        if side == Side.BID:
            return self.bids
        raise ValueError(f"Invalid resting side: {side!r}")

    def _get_level(self, price: int, side: str) -> _LevelOrders | None:
        return self._side_levels(side).get(price)

    def _get_or_insert_level(self, price: int, side: str) -> _LevelOrders:
        levels = self._side_levels(side)
        level = levels.get(price)
        if level is None:
            level = _LevelOrders(price=price)
            levels[price] = level
        return level

    def _remove_level(self, price: int, side: str) -> None:
        self._side_levels(side).pop(price, None)

    def clear(self) -> None:
        self.orders_by_id.clear()
        self.offers.clear()
        self.bids.clear()

    def clear_side(self, side: str) -> None:
        """Remove an entire quoted side including order-id state."""
        self._side_levels(side).clear()
        stale_keys = [key for key in self.orders_by_id if key[1] == side]
        for key in stale_keys:
            self.orders_by_id.pop(key, None)

    def best_bid(self) -> _PriceLevel | None:
        if not self.bids:
            return None
        level = self.bids.peekitem(-1)[1]
        return _PriceLevel(level.price, level.size, level.count)

    def best_ask(self) -> _PriceLevel | None:
        if not self.offers:
            return None
        level = self.offers.peekitem(0)[1]
        return _PriceLevel(level.price, level.size, level.count)


class Market:
    """Per-(instrument_id, publisher_id) book container."""

    __slots__ = ("_books",)

    def __init__(self) -> None:
        self._books: defaultdict[int, defaultdict[int, Book]] = defaultdict(
            lambda: defaultdict(Book)
        )

    def best_bid(self, instrument_id: int, publisher_id: int) -> _PriceLevel | None:
        return self._books[instrument_id][publisher_id].best_bid()

    def best_ask(self, instrument_id: int, publisher_id: int) -> _PriceLevel | None:
        return self._books[instrument_id][publisher_id].best_ask()


# ---------------------------------------------------------------------------
# STATE TRANSITIONS
# ---------------------------------------------------------------------------

def _book_add(
    book: Book,
    price: int,
    size: int,
    side: str,
    order_id: int,
    flags: int,
) -> None:
    is_tob = bool(flags & _F_TOB)
    order = _Order(order_id, price, size, side, flags)

    if is_tob:
        # F_TOB is a side replacement, not an order-level ADD.
        book.clear_side(side)
        level = _LevelOrders(price=price, size=size, count=0)
        level.orders.append(order)
        book._side_levels(side)[price] = level
        return

    level = book._get_or_insert_level(price, side)
    book.orders_by_id[(order_id, side)] = order
    level.add_order(order, is_tob=False)


def _book_cancel(
    book: Book,
    price: int,
    size: int,
    side: str,
    order_id: int,
) -> None:
    key = (order_id, side)
    order = book.orders_by_id.get(key)
    if order is None:
        book._n_orphan_cancel += 1
        return

    level = book._get_level(price, side)
    if level is None:
        book._n_orphan_cancel += 1
        return

    old_size = order.size
    order.size = max(0, order.size - size)
    if order.size > 0:
        level.update_size(old_size, order.size)
        return

    book.orders_by_id.pop(key, None)
    is_tob = bool(order.flags & _F_TOB)
    level.size -= old_size
    if not is_tob:
        level.count -= 1
    try:
        level.orders.remove(order)
    except ValueError:
        pass
    if not level:
        book._remove_level(price, side)


def _book_modify(
    book: Book,
    price: int,
    size: int,
    side: str,
    order_id: int,
    flags: int,
) -> None:
    key = (order_id, side)
    order = book.orders_by_id.get(key)
    if order is None:
        book._n_orphan_modify += 1
        _book_add(book, price, size, side, order_id, flags)
        return

    level = book._get_level(order.price, side)
    is_tob = bool(order.flags & _F_TOB)
    if level is None:
        book._n_orphan_modify += 1
        _book_add(book, price, size, side, order_id, flags)
        return

    if order.price != price:
        level.remove_order(order, is_tob=is_tob)
        if not level:
            book._remove_level(order.price, side)
        order.price = price
        order.size = size
        order.flags = flags
        book._get_or_insert_level(price, side).add_order(order, is_tob=is_tob)
    elif size > order.size:
        # Size increase loses queue priority.
        level.remove_order(order, is_tob=is_tob)
        order.size = size
        order.flags = flags
        level.add_order(order, is_tob=is_tob)
    else:
        # Size decrease retains queue priority.
        old_size = order.size
        order.size = size
        order.flags = flags
        level.update_size(old_size, size)

    book.orders_by_id[key] = order


def _apply_book(
    market: Market,
    action: str,
    side: str,
    price: int,
    size: int,
    order_id: int,
    flags: int,
    iid: int,
    pid: int,
) -> bool:
    """Apply one normalized row and return whether resting state changed."""
    book = market._books[iid][pid]

    if action in (Action.TRADE, Action.FILL, Action.NONE):
        return False

    if action == Action.CLEAR:
        book.clear()
        return True

    if flags & _F_TOB and price == UNDEF_PRICE:
        if side in Side.ORDER_SIDES:
            book.clear_side(side)
            return True
        return False

    if side not in Side.ORDER_SIDES:
        return False

    if action == Action.ADD:
        _book_add(book, price, size, side, order_id, flags)
    elif action == Action.CANCEL:
        _book_cancel(book, price, size, side, order_id)
    elif action == Action.MODIFY:
        _book_modify(book, price, size, side, order_id, flags)
    else:
        return False
    return True


# ---------------------------------------------------------------------------
# STREAMING RECONSTRUCTION
# ---------------------------------------------------------------------------

def reconstruct_day(
    mbo_file: Path,
    out_file: Path,
    product: str,
    contract: str,
) -> dict:
    """Reconstruct one normalized day in bounded memory."""
    read_batch_size = 100_000
    write_flush_rows = 50_000
    started = time.perf_counter()

    market = Market()
    output_rows: list[dict] = []
    n_events = 0
    n_rows_emitted = 0
    group_state_changed = False
    group_has_trade = False

    out_file.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(out_file, MBP1_SCHEMA, compression="zstd")

    def col(batch: pa.RecordBatch, name: str) -> list:
        array = batch.column(name)
        if pa.types.is_dictionary(array.type):
            array = array.cast(pa.string())
        return array.to_pylist()

    def flush() -> None:
        if not output_rows:
            return
        table = pa.Table.from_pylist(output_rows, schema=MBP1_SCHEMA)
        writer.write_table(table)
        output_rows.clear()

    try:
        parquet = pq.ParquetFile(mbo_file)
        for batch in parquet.iter_batches(batch_size=read_batch_size):
            batch_len = len(batch)
            n_events += batch_len

            c_ts_event = col(batch, "ts_event")
            c_ts_recv = col(batch, "ts_recv")
            c_action = col(batch, "action")
            c_side = col(batch, "side")
            c_price = col(batch, "price")
            c_size = col(batch, "size")
            c_order_id = col(batch, "order_id")
            c_flags = col(batch, "flags")
            c_sequence = col(batch, "sequence")
            c_subsequence = col(batch, "subsequence")
            c_instrument_id = col(batch, "instrument_id")
            c_publisher_id = col(batch, "publisher_id")
            del batch

            for index in range(batch_len):
                flags = int(c_flags[index])
                action = c_action[index]
                side = c_side[index]
                price = int(c_price[index])
                size = int(c_size[index])
                order_id = int(c_order_id[index])
                iid = int(c_instrument_id[index])
                pid = int(c_publisher_id[index])

                is_snapshot = bool(flags & _F_SNAPSHOT)
                is_last = bool(flags & _F_LAST)

                changed = _apply_book(
                    market,
                    action,
                    side,
                    price,
                    size,
                    order_id,
                    flags,
                    iid,
                    pid,
                )

                if not is_snapshot:
                    group_state_changed |= changed
                    group_has_trade |= action in (Action.TRADE, Action.FILL)

                if not is_last:
                    continue

                if not is_snapshot and (group_state_changed or group_has_trade):
                    bid = market.best_bid(iid, pid)
                    ask = market.best_ask(iid, pid)
                    output_rows.append(
                        {
                            "ts_event": int(c_ts_event[index]),
                            "ts_recv": int(c_ts_recv[index]),
                            "action": action,
                            "side": side,
                            "price": price / _SCALE,
                            "flags": flags,
                            "sequence": int(c_sequence[index]),
                            "subsequence": int(c_subsequence[index]),
                            "bid_px_00": bid.price / _SCALE if bid else None,
                            "ask_px_00": ask.price / _SCALE if ask else None,
                            "bid_sz_00": bid.size if bid else None,
                            "ask_sz_00": ask.size if ask else None,
                            "bid_ct_00": bid.count if bid else None,
                            "ask_ct_00": ask.count if ask else None,
                        }
                    )
                    n_rows_emitted += 1
                    if len(output_rows) >= write_flush_rows:
                        flush()

                group_state_changed = False
                group_has_trade = False

        flush()
    finally:
        writer.close()

    n_orphan_cancel = sum(
        book._n_orphan_cancel
        for books_by_publisher in market._books.values()
        for book in books_by_publisher.values()
    )
    n_orphan_modify = sum(
        book._n_orphan_modify
        for books_by_publisher in market._books.values()
        for book in books_by_publisher.values()
    )

    return {
        "n_events": n_events,
        "n_rows_emitted": n_rows_emitted,
        "n_orphan_cancel": n_orphan_cancel,
        "n_orphan_modify": n_orphan_modify,
        "elapsed_seconds": round(time.perf_counter() - started, 2),
    }


# ---------------------------------------------------------------------------
# PATH / DISCOVERY HELPERS
# ---------------------------------------------------------------------------

def _normalized_dir(product: str, contract: str, year: int, month: int) -> Path:
    cfg = MARKET_CONFIG[product]
    return (
        DATA_NORMALIZED
        / f"provider={cfg['provider']}"
        / f"venue={cfg['exchange']}"
        / f"product={product}"
        / f"contract={contract}"
        / f"year={year}"
        / f"month={month:02d}"
    )


def _iter_mbo_files(
    product: str,
    contract: str,
    year: int,
    month: int,
) -> Iterator[tuple[Path, str]]:
    directory = _normalized_dir(product, contract, year, month)
    if not directory.exists():
        return
    for path in sorted(directory.glob(f"{contract}_*_mbo.parquet")):
        match = re.search(r"(\d{8})", path.stem)
        if match:
            yield path, match.group(1)


def _out_path(
    product: str,
    contract: str,
    year: int,
    month: int,
    date_str: str,
) -> Path:
    cfg = MARKET_CONFIG[product]
    return reconstructed_path(
        DATA_RECONSTRUCTED,
        cfg["provider"],
        cfg["exchange"],
        product,
        contract,
        year,
        month,
        date_str,
        "mbp1",
    )


def _product_root(product: str) -> Path:
    cfg = MARKET_CONFIG[product]
    return (
        DATA_NORMALIZED
        / f"provider={cfg['provider']}"
        / f"venue={cfg['exchange']}"
        / f"product={product}"
    )


def _discover_contracts(product: str, year: int, month: int) -> list[str]:
    root = _product_root(product)
    return sorted(
        path.name.removeprefix("contract=")
        for path in root.glob("contract=*")
        if path.is_dir()
        and (path / f"year={year}" / f"month={month:02d}").exists()
    )


def _discover_year_months(product: str) -> list[tuple[int, int]]:
    pairs: set[tuple[int, int]] = set()
    for path in _product_root(product).glob("contract=*/year=*/month=*"):
        parts = {
            token.split("=", 1)[0]: token.split("=", 1)[1]
            for token in path.parts
            if "=" in token
        }
        try:
            pairs.add((int(parts["year"]), int(parts["month"])))
        except (KeyError, ValueError):
            continue
    return sorted(pairs)


def _resolve_contracts(
    args: argparse.Namespace,
    product: str,
    year: int,
    month: int,
) -> list[str] | None:
    if args.all_contracts or args.all_data:
        contracts = _discover_contracts(product, year, month)
        if not contracts:
            log.error("No contracts found for %s %d-%02d", product, year, month)
            return None
        return contracts
    if not args.contract:
        log.error("--contract is required unless discovery is enabled")
        return None
    return [args.contract]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconstruct MBP-1 from canonical MBO")
    parser.add_argument("--product", required=True, nargs="+")
    parser.add_argument("--contract")
    parser.add_argument("--date")
    parser.add_argument("--month")
    parser.add_argument("--year")
    parser.add_argument("--all-data", action="store_true")
    parser.add_argument("--all-contracts", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def _build_work(args: argparse.Namespace) -> list[tuple[str, str, int, int, str]] | None:
    mode_count = sum(bool(value) for value in (args.date, args.month, args.year, args.all_data))
    if mode_count != 1:
        log.error("Specify exactly one of --date/--month/--year/--all-data")
        return None

    work: list[tuple[str, str, int, int, str]] = []
    for product in args.product:
        if product not in MARKET_CONFIG:
            log.error("Unknown product: %s", product)
            return None

        if args.date:
            if not args.contract:
                log.error("--contract is required with --date")
                return None
            session_date = date.fromisoformat(args.date)
            work.append(
                (
                    product,
                    args.contract,
                    session_date.year,
                    session_date.month,
                    session_date.strftime("%Y%m%d"),
                )
            )
            continue

        if args.month:
            month_date = datetime.strptime(args.month, "%Y-%m")
            pairs = [(month_date.year, month_date.month)]
        elif args.year:
            target_year = int(args.year)
            pairs = [pair for pair in _discover_year_months(product) if pair[0] == target_year]
        else:
            pairs = _discover_year_months(product)

        for year, month in pairs:
            contracts = _resolve_contracts(args, product, year, month)
            if contracts is None:
                return None
            for contract in contracts:
                for _, date_str in _iter_mbo_files(product, contract, year, month):
                    work.append((product, contract, year, month, date_str))

    return work


def main() -> int:
    args = _parse_args()
    try:
        work = _build_work(args)
    except ValueError as exc:
        log.error("Invalid CLI value: %s", exc)
        return 1
    if work is None:
        return 1
    if not work:
        log.warning("No files to process")
        return 0

    n_ok = n_skip = n_fail = 0
    for product, contract, year, month, date_str in work:
        mbo_file = _normalized_dir(product, contract, year, month) / f"{contract}_{date_str}_mbo.parquet"
        out_file = _out_path(product, contract, year, month, date_str)

        if not mbo_file.exists():
            log.warning("[SKIP] missing %s", mbo_file)
            n_skip += 1
            continue
        if out_file.exists() and not args.overwrite:
            n_skip += 1
            continue

        try:
            stats = reconstruct_day(mbo_file, out_file, product, contract)
            log.info(
                "[DONE] %s %s %s events=%d rows=%d orphan_cancel=%d "
                "orphan_modify=%d %.1fs",
                product,
                contract,
                date_str,
                stats["n_events"],
                stats["n_rows_emitted"],
                stats["n_orphan_cancel"],
                stats["n_orphan_modify"],
                stats["elapsed_seconds"],
            )
            n_ok += 1
        except Exception:
            log.exception("[FAIL] %s %s %s", product, contract, date_str)
            n_fail += 1

    log.info("Summary: %d processed, %d skipped, %d failed", n_ok, n_skip, n_fail)
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    setup_logging()
    raise SystemExit(main())
