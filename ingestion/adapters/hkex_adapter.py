"""
hkex_adapter.py — HKEX OMD-D -> canonical Destiny MBO adapter.

The raw HKEX corpus stores order messages and trade messages in separate
Parquet files. This adapter deterministically merges those physical sources into
one provider-ordered event stream and translates native OMD-D semantics into the
canonical MBO contract.

Important semantic boundaries:
    - physical file separation is normalized away;
    - native Delete+Add remains CANCEL+ADD in canonical MBO;
    - a normal Trade(350) with order_id>0 becomes one atomic
      TRADE/FILL/CANCEL bundle with exactly one resting-state decrement;
    - printable order_id=0 trades are retained without fabricated queue identity;
    - inferred/synthetic/repaired state is carried in norm_flags, never in
      provider/control flags.
"""

from __future__ import annotations

from datetime import date
import os
from pathlib import Path
import re
from typing import Iterator

import duckdb

from .base import BaseAdapter, ContractInfo, SessionConfig
from ..schema import Action, Flags, NormFlags, Side, FIXED_PRICE_SCALE


VENUE = "HKEX"
PROVIDER = "hkex"

_MONTH_CODE: dict[str, int] = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}

_MSG_TYPE_TO_ACTION: dict[int, str] = {
    330: Action.ADD,
    331: Action.MODIFY,
    332: Action.CANCEL,
    335: Action.CLEAR,
    364: Action.NONE,
    350: Action.TRADE,
}

_ORDER_SIDE_MAP: dict[int, str] = {
    0: Side.BID,
    1: Side.ASK,
    -1: Side.NONE,
}

# HKEX Trade(350).Side identifies the resting order side.
_PASSIVE_SIDE_MAP: dict[int, str] = {
    2: Side.BID,
    3: Side.ASK,
}

# Canonical TRADE.side is aggressor side, therefore opposite the identified
# resting order side.
_AGGRESSOR_SIDE_MAP: dict[int, str] = {
    2: Side.ASK,
    3: Side.BID,
}

_RE_HKEX_FUTURE = re.compile(r"^([A-Z]{2,4})([FGHJKMNQUVXZ])(\d)$")

_DEFAULT_BATCH_SIZE_ROWS = 200_000


def _normalize_expiry_year(single_digit: str, session_date: date) -> int:
    digit = int(single_digit)
    current_year = session_date.year
    candidate = (current_year // 10) * 10 + digit
    if candidate < current_year - 2:
        candidate += 10
    return candidate


def _parse_hkex_symbol(
    symbol: str,
    session_date: date,
) -> tuple[str, str] | None:
    match = _RE_HKEX_FUTURE.match(symbol.strip())
    if not match:
        return None
    product, month_code, year_digit = match.groups()
    year = _normalize_expiry_year(year_digit, session_date)
    return product, f"{product}{month_code}{year % 100:02d}"


class HKEXAdapter(BaseAdapter):
    """Streaming adapter for parsed HKEX OMD-D order/trade Parquet files."""

    # Historical data layout uses provider=HKEX. Preserve it intentionally.
    PROVIDER = "HKEX"

    def __init__(self) -> None:
        super().__init__()
        self._orders_path: Path | None = None
        self._trades_path: Path | None = None
        self._symbol_map: dict[str, ContractInfo] = {}
        self._ob_map: dict[int, ContractInfo] = {}

        # (orderbook_id, order_id, side) -> (resting_size, price_fp)
        self._order_sizes: dict[tuple[int, int, str], tuple[int, int]] = {}

        self._n_unknown_symbol = 0
        self._n_clears = 0
        self._n_cops = 0
        self._n_mod_orders = 0
        self._n_synthetic_cancels = 0
        self._n_unresolved_deletes = 0
        self._n_order_id_zero_trades = 0
        self._n_non_printable_trades = 0

        self._batch_size_rows = _DEFAULT_BATCH_SIZE_ROWS
        self._last_sequence: int | None = None
        self._next_subsequence = 0

    # ------------------------------------------------------------------
    # SESSION LIFECYCLE
    # ------------------------------------------------------------------

    def _open(self, raw_source: object, config: SessionConfig) -> None:
        raw_dir = Path(raw_source)
        date_str = config.session_date.strftime("%Y%m%d")

        orders_files = sorted(raw_dir.glob(f"hkex-*_{date_str}_orders.parquet"))
        trades_files = sorted(raw_dir.glob(f"hkex-*_{date_str}_trades.parquet"))
        if not orders_files:
            raise FileNotFoundError(
                f"No HKEX orders parquet found for {date_str} in {raw_dir}"
            )

        self._orders_path = orders_files[0]
        self._trades_path = trades_files[0] if trades_files else None
        self._last_sequence = None
        self._next_subsequence = 0

    def _close(self) -> None:
        self._orders_path = None
        self._trades_path = None
        self._symbol_map.clear()
        self._ob_map.clear()
        self._order_sizes.clear()
        self._last_sequence = None
        self._next_subsequence = 0

    # ------------------------------------------------------------------
    # INSTRUMENT RESOLUTION
    # ------------------------------------------------------------------

    def resolve_contract(
        self,
        instrument_id: int,
        session_date: date,
    ) -> ContractInfo | None:
        return self._ob_map.get(instrument_id)

    def list_instruments(self) -> list[ContractInfo]:
        assert self._orders_path is not None, "open_session() not called"
        assert self._config is not None, "open_session() not called"

        con = duckdb.connect()
        try:
            rows = con.execute(
                f"""
                SELECT DISTINCT orderbook_id, symbol
                FROM read_parquet('{self._orders_path}')
                ORDER BY symbol, orderbook_id
                """
            ).fetchall()
        finally:
            con.close()

        results: list[ContractInfo] = []
        for orderbook_id, symbol in rows:
            info = self._build_contract_info(
                int(orderbook_id),
                str(symbol),
                self._config.session_date,
            )
            if info is None:
                continue
            results.append(info)
            self._symbol_map[str(symbol)] = info
            self._ob_map[int(orderbook_id)] = info
        return results

    def _build_contract_info(
        self,
        orderbook_id: int,
        symbol: str,
        session_date: date,
    ) -> ContractInfo | None:
        parsed = _parse_hkex_symbol(symbol, session_date)
        if parsed is None:
            self._n_unknown_symbol += 1
            return None

        product, contract = parsed
        tick_size = 0
        currency = "HKD"
        try:
            from ..market_config import MARKET_CONFIG

            cfg = MARKET_CONFIG.get(product)
            if cfg:
                tick_size = int(cfg["tick_size_fp"])
                currency = str(cfg.get("currency", "HKD"))
        except (ImportError, KeyError, TypeError):
            pass

        return ContractInfo(
            product=product,
            contract=contract,
            venue=VENUE,
            instrument_id=orderbook_id,
            is_spread=False,
            tick_size=tick_size,
            currency=currency,
        )

    # ------------------------------------------------------------------
    # RAW MERGE
    # ------------------------------------------------------------------

    def _iter_raw(self) -> Iterator[dict]:
        """Merge order and trade files in authoritative packet/message order."""
        assert self._orders_path is not None, "open_session() not called"

        orders_sql = f"""
            SELECT
                send_time_ns,
                seq_num,
                msg_index,
                msg_type,
                orderbook_id,
                symbol,
                class_code,
                order_id,
                price,
                CAST(quantity AS BIGINT) AS quantity,
                side,
                NULL::BIGINT AS trade_time_ns,
                NULL::BIGINT AS trade_id,
                NULL::TINYINT AS deal_type,
                NULL::INTEGER AS combo_group_id,
                'order' AS source
            FROM read_parquet('{self._orders_path}')
        """

        trades_sql = ""
        if self._trades_path is not None:
            trades_sql = f"""
                UNION ALL
                SELECT
                    send_time_ns,
                    seq_num,
                    msg_index,
                    350::INTEGER AS msg_type,
                    orderbook_id,
                    symbol,
                    class_code,
                    order_id,
                    price,
                    CAST(quantity AS BIGINT) AS quantity,
                    side,
                    trade_time_ns,
                    trade_id,
                    deal_type,
                    combo_group_id,
                    'trade' AS source
                FROM read_parquet('{self._trades_path}')
            """

        query = f"""
            SELECT *
            FROM (
                {orders_sql}
                {trades_sql}
            )
            ORDER BY seq_num, msg_index, send_time_ns, source
        """

        os.makedirs("/tmp/duckdb_hkex_spill", exist_ok=True)
        con = duckdb.connect()
        con.execute("SET memory_limit='6GB'")
        con.execute("SET temp_directory='/tmp/duckdb_hkex_spill'")
        reader = con.execute(query).fetch_record_batch(self._batch_size_rows)

        try:
            while True:
                try:
                    batch = reader.read_next_batch()
                except StopIteration:
                    break
                for row in batch.to_pylist():
                    yield row
        finally:
            con.close()

    # ------------------------------------------------------------------
    # ORDERING / SHADOW HELPERS
    # ------------------------------------------------------------------

    def _allocate_subsequence_range(self, sequence: int, count: int) -> list[int]:
        """Reserve collision-free normalized row indexes inside one sequence."""
        if self._last_sequence != sequence:
            self._last_sequence = sequence
            self._next_subsequence = 0

        start = self._next_subsequence
        end = start + count
        if end - 1 > 0xFFFF:
            raise OverflowError(f"subsequence overflow for HKEX sequence={sequence}")
        self._next_subsequence = end
        return list(range(start, end))

    @staticmethod
    def _shadow_key(
        orderbook_id: int,
        order_id: int,
        side: str,
    ) -> tuple[int, int, str]:
        return orderbook_id, order_id, side

    def _clear_orderbook_shadow(self, orderbook_id: int) -> None:
        keys = [key for key in self._order_sizes if key[0] == orderbook_id]
        for key in keys:
            self._order_sizes.pop(key, None)

    @staticmethod
    def _is_non_printable_combo_parent(raw_event: dict) -> bool:
        """Identify the explicit non-printable combo-parent representation.

        The current parsed corpus distinguishes the parent from printable leg
        trades through deal_type/combo_group_id. Crucially, order_id==0 alone is
        never used as an exclusion predicate.
        """
        combo_group_id = int(raw_event.get("combo_group_id") or 0)
        deal_type = int(raw_event.get("deal_type") or 0)
        return combo_group_id != 0 and deal_type == 0

    # ------------------------------------------------------------------
    # NORMALIZED ITERATION
    # ------------------------------------------------------------------

    def iter_events(self) -> Iterator[dict | None]:
        assert self._is_open, "open_session() must be called before iter_events()"
        for raw_event in self._iter_raw():
            result = self.translate(raw_event)
            if result is None:
                self._n_dropped += 1
                yield None
                continue
            for event in result:
                self._n_yielded += 1
                yield event

    def translate(self, raw_event: dict) -> list[dict] | None:
        orderbook_id = int(raw_event["orderbook_id"])
        info = self._ob_map.get(orderbook_id)
        if info is None:
            self._n_unknown_symbol += 1
            return None

        msg_type = int(raw_event["msg_type"])
        source = str(raw_event["source"])
        action = _MSG_TYPE_TO_ACTION.get(msg_type)
        if action is None:
            return None

        if msg_type == 364:
            self._n_cops += 1
            return None

        if msg_type == 350 and self._is_non_printable_combo_parent(raw_event):
            self._n_non_printable_trades += 1
            return None

        if msg_type == 331:
            self._n_mod_orders += 1
        elif msg_type == 335:
            self._n_clears += 1

        raw_side = int(raw_event.get("side", -1))
        order_id = int(raw_event.get("order_id", 0))
        raw_price = int(raw_event.get("price", 0))
        price_fp = raw_price * FIXED_PRICE_SCALE
        size = int(raw_event.get("quantity", 0))

        send_time = int(raw_event["send_time_ns"])
        trade_time_raw = raw_event.get("trade_time_ns")
        trade_time = int(trade_time_raw or 0)
        ts_event = trade_time if source == "trade" and trade_time > 0 else send_time
        ts_recv = send_time
        sequence = int(raw_event["seq_num"]) & 0xFFFFFFFF

        coarse = int(NormFlags.N_COARSE_TS)
        flags_final = int(Flags.F_LAST)

        # ------------------------------------------------------------------
        # NATIVE ORDER MESSAGES
        # ------------------------------------------------------------------
        if source != "trade":
            side = _ORDER_SIDE_MAP.get(raw_side, Side.NONE)
            norm_flags = coarse

            if msg_type == 335:
                self._clear_orderbook_shadow(orderbook_id)
                order_id = 0
                price_fp = 0
                size = 0

            elif msg_type == 330:
                key = self._shadow_key(orderbook_id, order_id, side)
                self._order_sizes[key] = (size, price_fp)

            elif msg_type == 331:
                key = self._shadow_key(orderbook_id, order_id, side)
                self._order_sizes[key] = (size, price_fp)

            elif msg_type == 332:
                key = self._shadow_key(orderbook_id, order_id, side)
                cached = self._order_sizes.pop(key, None)
                if cached is None:
                    # Do not invent size/price. The row remains observable and
                    # is marked so validator policy can account for it.
                    self._n_unresolved_deletes += 1
                    price_fp = 0
                    size = 0
                    norm_flags |= int(NormFlags.N_VALIDATION_ANOMALY)
                else:
                    size, price_fp = cached
                    norm_flags |= int(NormFlags.N_REPAIRED)
                    norm_flags |= int(NormFlags.N_INFERRED_SIZE)

            subsequence = self._allocate_subsequence_range(sequence, 1)[0]
            return [
                {
                    "ts_event": ts_event,
                    "ts_recv": ts_recv,
                    "venue": VENUE,
                    "product": info.product,
                    "contract": info.contract,
                    "action": action,
                    "side": side,
                    "price": price_fp,
                    "size": size,
                    "order_id": order_id,
                    "flags": flags_final,
                    "norm_flags": norm_flags,
                    "sequence": sequence,
                    "subsequence": subsequence,
                    "publisher_id": 0,
                    "instrument_id": orderbook_id,
                }
            ]

        # ------------------------------------------------------------------
        # TRADE 350
        # ------------------------------------------------------------------
        if order_id == 0:
            self._n_order_id_zero_trades += 1
            subsequence = self._allocate_subsequence_range(sequence, 1)[0]
            return [
                {
                    "ts_event": ts_event,
                    "ts_recv": ts_recv,
                    "venue": VENUE,
                    "product": info.product,
                    "contract": info.contract,
                    "action": Action.TRADE,
                    "side": Side.NONE,
                    "price": price_fp,
                    "size": size,
                    "order_id": 0,
                    "flags": flags_final,
                    "norm_flags": coarse,
                    "sequence": sequence,
                    "subsequence": subsequence,
                    "publisher_id": 0,
                    "instrument_id": orderbook_id,
                }
            ]

        passive_side = _PASSIVE_SIDE_MAP.get(raw_side)
        aggressor_side = _AGGRESSOR_SIDE_MAP.get(raw_side)
        if passive_side is None or aggressor_side is None:
            # A resting order is identified but its side is unsafe. Preserve the
            # print and avoid fabricating a book mutation.
            subsequence = self._allocate_subsequence_range(sequence, 1)[0]
            return [
                {
                    "ts_event": ts_event,
                    "ts_recv": ts_recv,
                    "venue": VENUE,
                    "product": info.product,
                    "contract": info.contract,
                    "action": Action.TRADE,
                    "side": Side.NONE,
                    "price": price_fp,
                    "size": size,
                    "order_id": order_id,
                    "flags": flags_final,
                    "norm_flags": coarse | int(NormFlags.N_VALIDATION_ANOMALY),
                    "sequence": sequence,
                    "subsequence": subsequence,
                    "publisher_id": 0,
                    "instrument_id": orderbook_id,
                }
            ]

        subsequences = self._allocate_subsequence_range(sequence, 3)
        common = {
            "ts_event": ts_event,
            "ts_recv": ts_recv,
            "venue": VENUE,
            "product": info.product,
            "contract": info.contract,
            "price": price_fp,
            "size": size,
            "order_id": order_id,
            "sequence": sequence,
            "publisher_id": 0,
            "instrument_id": orderbook_id,
        }

        trade_event = {
            **common,
            "action": Action.TRADE,
            "side": aggressor_side,
            "flags": 0,
            "norm_flags": coarse | int(NormFlags.N_INFERRED_SIDE),
            "subsequence": subsequences[0],
        }
        fill_event = {
            **common,
            "action": Action.FILL,
            "side": passive_side,
            "flags": 0,
            "norm_flags": coarse | int(NormFlags.N_SYNTHETIC),
            "subsequence": subsequences[1],
        }
        cancel_event = {
            **common,
            "action": Action.CANCEL,
            "side": passive_side,
            "flags": flags_final,
            "norm_flags": coarse | int(NormFlags.N_SYNTHETIC),
            "subsequence": subsequences[2],
        }

        self._n_synthetic_cancels += 1

        key = self._shadow_key(orderbook_id, order_id, passive_side)
        cached = self._order_sizes.get(key)
        if cached is not None:
            cached_size, cached_price = cached
            residual = max(0, cached_size - size)
            if residual > 0:
                self._order_sizes[key] = (residual, cached_price)
            else:
                self._order_sizes.pop(key, None)

        return [trade_event, fill_event, cancel_event]

    # ------------------------------------------------------------------
    # INFO / STATS
    # ------------------------------------------------------------------

    def is_warmup_event(self, normalized_event: dict) -> bool:
        return False

    def get_venue(self) -> str:
        return VENUE

    def get_stats(self) -> dict:
        stats = super().get_stats()
        stats.update(
            {
                "venue": VENUE,
                "n_instruments": len(self._ob_map),
                "n_unknown_symbol": self._n_unknown_symbol,
                "n_clears": self._n_clears,
                "n_cops": self._n_cops,
                "n_mod_orders": self._n_mod_orders,
                "n_synthetic_cancels": self._n_synthetic_cancels,
                "n_unresolved_deletes": self._n_unresolved_deletes,
                "n_order_id_zero_trades": self._n_order_id_zero_trades,
                "n_non_printable_trades": self._n_non_printable_trades,
            }
        )
        return stats
