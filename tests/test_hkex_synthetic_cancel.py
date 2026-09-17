"""Unit/characterization tests for the current HKEXAdapter trade handling.

These tests intentionally freeze the pre-R0 implementation before semantic
changes are made. They verify trade expansion, shadow-state decrements, raw
DeleteOrder resolution and composite order keys. They do not claim that the
current TRADE/FILL/CANCEL grouping is the final normative design.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.adapters.base import ContractInfo
from ingestion.adapters.hkex_adapter import HKEXAdapter
from ingestion.schema import Action, FIXED_PRICE_SCALE, Flags, Side


_OB_ID = 135_335_842
_OB_ID_2 = 135_335_999
_ORDER_ID = 8_108_676_166_385_258_219
_TS = 1_769_995_480_741_000_000
_SEQ = 4_101_677
_PRICE_RAW = 27_079
_PRICE_FP = _PRICE_RAW * FIXED_PRICE_SCALE
_SIZE = 5


def _make_adapter_with_contract(
    ob_id: int = _OB_ID,
    product: str = "HSI",
    contract: str = "HSIG26",
    ob_id_2: int | None = None,
    product2: str = "HHI",
    contract2: str = "HHIG26",
) -> HKEXAdapter:
    adapter = HKEXAdapter()
    adapter._ob_map[ob_id] = ContractInfo(
        product=product,
        contract=contract,
        venue="HKEX",
        instrument_id=ob_id,
        is_spread=False,
        tick_size=FIXED_PRICE_SCALE,
        currency="HKD",
    )
    if ob_id_2 is not None:
        adapter._ob_map[ob_id_2] = ContractInfo(
            product=product2,
            contract=contract2,
            venue="HKEX",
            instrument_id=ob_id_2,
            is_spread=False,
            tick_size=FIXED_PRICE_SCALE,
            currency="HKD",
        )
    return adapter


def _raw_add(
    ob_id: int = _OB_ID,
    order_id: int = _ORDER_ID,
    price: int = _PRICE_RAW,
    size: int = _SIZE,
    side: int = 0,
    seq: int = _SEQ,
    ts: int = _TS,
) -> dict:
    return {
        "source": "order",
        "msg_type": 330,
        "orderbook_id": ob_id,
        "symbol": "HSIG6",
        "class_code": "HSI",
        "order_id": order_id,
        "price": price,
        "quantity": size,
        "side": side,
        "send_time_ns": ts,
        "seq_num": seq,
        "msg_index": 0,
    }


def _raw_delete(
    ob_id: int = _OB_ID,
    order_id: int = _ORDER_ID,
    side: int = 0,
    seq: int = _SEQ + 1,
    ts: int = _TS + 1_000_000,
) -> dict:
    return {
        "source": "order",
        "msg_type": 332,
        "orderbook_id": ob_id,
        "symbol": "HSIG6",
        "class_code": "HSI",
        "order_id": order_id,
        "price": 0,
        "quantity": 0,
        "side": side,
        "send_time_ns": ts,
        "seq_num": seq,
        "msg_index": 0,
    }


def _raw_trade(
    ob_id: int = _OB_ID,
    order_id: int = _ORDER_ID,
    price: int = _PRICE_RAW,
    size: int = 2,
    trade_side: int = 2,
    seq: int = _SEQ + 10,
    ts: int = _TS + 10_000_000,
    trade_time_ns: int = 0,
    combo_group_id: int = 0,
) -> dict:
    return {
        "source": "trade",
        "msg_type": 350,
        "orderbook_id": ob_id,
        "symbol": "HSIG6",
        "class_code": "HSI",
        "order_id": order_id,
        "price": price,
        "quantity": size,
        "side": trade_side,
        "send_time_ns": ts,
        "seq_num": seq,
        "msg_index": 0,
        "trade_time_ns": trade_time_ns,
        "trade_id": 999,
        "deal_type": 0,
        "combo_group_id": combo_group_id,
    }


class TestSyntheticCancel:
    def test_trade_with_order_id_returns_three_events(self) -> None:
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(size=5, side=0))

        result = adapter.translate(_raw_trade(order_id=_ORDER_ID, size=2))

        assert result is not None
        assert [event["action"] for event in result] == [
            Action.TRADE,
            Action.FILL,
            Action.CANCEL,
        ]

    def test_current_trade_bundle_flag_pattern_is_characterized(self) -> None:
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(size=5, side=0))

        trade, fill, cancel = adapter.translate(_raw_trade(size=2))

        assert trade["flags"] & int(Flags.F_LAST)
        assert fill["flags"] == 0
        assert cancel["flags"] & int(Flags.F_LAST)

    def test_order_id_zero_trade_is_currently_dropped(self) -> None:
        adapter = _make_adapter_with_contract()
        assert adapter.translate(_raw_trade(order_id=0)) is None

    def test_trade_side_2_maps_passive_events_to_bid(self) -> None:
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(side=0))

        trade, fill, cancel = adapter.translate(_raw_trade(trade_side=2))

        assert trade["side"] == Side.ASK
        assert fill["side"] == Side.BID
        assert cancel["side"] == Side.BID

    def test_trade_side_3_maps_passive_events_to_ask(self) -> None:
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(side=1))

        trade, fill, cancel = adapter.translate(_raw_trade(trade_side=3))

        assert trade["side"] == Side.BID
        assert fill["side"] == Side.ASK
        assert cancel["side"] == Side.ASK

    def test_synthetic_cancel_carries_trade_price_size_and_order_id(self) -> None:
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(size=5, side=0))

        _, _, cancel = adapter.translate(_raw_trade(size=2))

        assert cancel["price"] == _PRICE_FP
        assert cancel["size"] == 2
        assert cancel["order_id"] == _ORDER_ID

    def test_synthetic_cancel_counter_increments(self) -> None:
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(size=10, side=0))

        adapter.translate(_raw_trade(size=3, seq=_SEQ + 10))
        adapter.translate(_raw_trade(size=2, seq=_SEQ + 20, ts=_TS + 20_000_000))

        assert adapter._n_synthetic_cancels == 2

    def test_partial_fill_updates_shadow_residual(self) -> None:
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(size=5, side=0))
        adapter.translate(_raw_trade(size=2))

        size, price = adapter._order_sizes[(_OB_ID, _ORDER_ID, Side.BID)]
        assert size == 3
        assert price == _PRICE_FP

    def test_full_fill_removes_shadow_order(self) -> None:
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(size=3, side=0))
        adapter.translate(_raw_trade(size=3))

        assert (_OB_ID, _ORDER_ID, Side.BID) not in adapter._order_sizes

    def test_two_partial_fills_can_fully_consume_order(self) -> None:
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(size=5, side=0))
        adapter.translate(_raw_trade(size=2, seq=_SEQ + 10))
        adapter.translate(_raw_trade(size=3, seq=_SEQ + 20, ts=_TS + 20_000_000))

        assert (_OB_ID, _ORDER_ID, Side.BID) not in adapter._order_sizes


class TestDeleteOrderResolution:
    def test_delete_after_add_resolves_price_and_size(self) -> None:
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(price=_PRICE_RAW, size=_SIZE, side=0))

        result = adapter.translate(_raw_delete(side=0))

        assert result is not None
        assert len(result) == 1
        cancel = result[0]
        assert cancel["action"] == Action.CANCEL
        assert cancel["price"] == _PRICE_FP
        assert cancel["size"] == _SIZE
        assert cancel["side"] == Side.BID

    def test_delete_removes_order_from_shadow_state(self) -> None:
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(side=0))
        adapter.translate(_raw_delete(side=0))

        assert (_OB_ID, _ORDER_ID, Side.BID) not in adapter._order_sizes

    def test_unknown_delete_emits_current_noop_cancel(self) -> None:
        adapter = _make_adapter_with_contract()
        result = adapter.translate(_raw_delete(order_id=_ORDER_ID + 1, side=0))

        assert result is not None
        cancel = result[0]
        assert cancel["action"] == Action.CANCEL
        assert cancel["price"] == 0
        assert cancel["size"] == 0

    def test_delete_after_partial_fill_uses_residual_size(self) -> None:
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(size=5, side=0))
        adapter.translate(_raw_trade(size=2, seq=_SEQ + 10))

        cancel = adapter.translate(_raw_delete(side=0, seq=_SEQ + 20))[0]
        assert cancel["size"] == 3
        assert cancel["price"] == _PRICE_FP

    def test_ask_delete_resolves_ask_side(self) -> None:
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(size=4, side=1))

        cancel = adapter.translate(_raw_delete(side=1))[0]
        assert cancel["side"] == Side.ASK
        assert cancel["size"] == 4
        assert cancel["price"] == _PRICE_FP


class TestCompositeShadowKeys:
    def test_same_order_id_bid_and_ask_are_independent(self) -> None:
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(price=27_079, size=2, side=0, seq=_SEQ))
        adapter.translate(_raw_add(price=27_087, size=3, side=1, seq=_SEQ + 1))

        bid_key = (_OB_ID, _ORDER_ID, Side.BID)
        ask_key = (_OB_ID, _ORDER_ID, Side.ASK)
        assert adapter._order_sizes[bid_key] == (2, 27_079 * FIXED_PRICE_SCALE)
        assert adapter._order_sizes[ask_key] == (3, 27_087 * FIXED_PRICE_SCALE)

    def test_delete_bid_does_not_affect_ask(self) -> None:
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(price=27_079, size=2, side=0, seq=_SEQ))
        adapter.translate(_raw_add(price=27_087, size=3, side=1, seq=_SEQ + 1))
        adapter.translate(_raw_delete(side=0, seq=_SEQ + 2))

        assert (_OB_ID, _ORDER_ID, Side.BID) not in adapter._order_sizes
        assert (_OB_ID, _ORDER_ID, Side.ASK) in adapter._order_sizes

    def test_same_order_id_across_contracts_is_independent(self) -> None:
        adapter = _make_adapter_with_contract(
            ob_id_2=_OB_ID_2,
            product2="HHI",
            contract2="HHIG26",
        )
        adapter.translate(
            _raw_add(ob_id=_OB_ID, price=27_079, size=3, side=0, seq=_SEQ)
        )
        adapter.translate(
            _raw_add(ob_id=_OB_ID_2, price=22_345, size=7, side=0, seq=_SEQ + 1)
        )

        hsi_key = (_OB_ID, _ORDER_ID, Side.BID)
        hhi_key = (_OB_ID_2, _ORDER_ID, Side.BID)
        assert adapter._order_sizes[hsi_key] == (3, 27_079 * FIXED_PRICE_SCALE)
        assert adapter._order_sizes[hhi_key] == (7, 22_345 * FIXED_PRICE_SCALE)

        adapter.translate(_raw_delete(ob_id=_OB_ID, side=0, seq=_SEQ + 2))
        assert hsi_key not in adapter._order_sizes
        assert hhi_key in adapter._order_sizes
