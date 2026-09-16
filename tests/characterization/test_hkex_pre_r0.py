"""Pre-R0 characterization tests for the current HKEX adapter behavior.

IMPORTANT: these assertions describe what the pipeline does at the pre-R0
baseline. They are not normative market-data semantics. Future R0 changes are
allowed to update these tests only when the behavior change is explicitly
reviewed and documented.
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from ingestion.adapters.base import ContractInfo  # noqa: E402
from ingestion.adapters.hkex_adapter import HKEXAdapter  # noqa: E402
from ingestion.schema import Action, FIXED_PRICE_SCALE, Flags, Side  # noqa: E402


_OB_ID = 135_335_842
_ORDER_ID = 8_108_676_166_385_258_219
_PRICE_RAW = 27_079
_TS = 1_769_995_480_741_000_000
_SEQ = 4_101_677


def _adapter() -> HKEXAdapter:
    adapter = HKEXAdapter()
    adapter._ob_map[_OB_ID] = ContractInfo(
        product="HSI",
        contract="HSIG26",
        venue="HKEX",
        instrument_id=_OB_ID,
        is_spread=False,
        tick_size=FIXED_PRICE_SCALE,
        currency="HKD",
    )
    return adapter


def _add(*, size: int = 5, side: int = 0, seq: int = _SEQ) -> dict:
    return {
        "source": "order",
        "msg_type": 330,
        "orderbook_id": _OB_ID,
        "symbol": "HSIG6",
        "class_code": "HSI",
        "order_id": _ORDER_ID,
        "price": _PRICE_RAW,
        "quantity": size,
        "side": side,
        "send_time_ns": _TS,
        "seq_num": seq,
        "msg_index": 0,
    }


def _trade(
    *,
    size: int = 2,
    side: int = 2,
    order_id: int = _ORDER_ID,
    seq: int = _SEQ + 10,
) -> dict:
    return {
        "source": "trade",
        "msg_type": 350,
        "orderbook_id": _OB_ID,
        "symbol": "HSIG6",
        "class_code": "HSI",
        "order_id": order_id,
        "price": _PRICE_RAW,
        "quantity": size,
        "side": side,
        "send_time_ns": _TS + 10_000_000,
        "seq_num": seq,
        "msg_index": 0,
        "trade_time_ns": 0,
        "trade_id": 999,
        "deal_type": 0,
        "combo_group_id": 0,
    }


def test_pre_r0_trade_expands_to_trade_fill_cancel() -> None:
    adapter = _adapter()
    adapter.translate(_add(size=5, side=0))

    events = adapter.translate(_trade(size=2, side=2))

    assert events is not None
    assert [event["action"] for event in events] == [
        Action.TRADE,
        Action.FILL,
        Action.CANCEL,
    ]


def test_pre_r0_trade_bundle_has_two_f_last_boundaries() -> None:
    """Freeze the current double-F_LAST behavior before R0 changes it."""
    adapter = _adapter()
    adapter.translate(_add(size=5, side=0))

    trade, fill, cancel = adapter.translate(_trade(size=2, side=2))

    assert trade["flags"] & int(Flags.F_LAST)
    assert fill["flags"] == 0
    assert cancel["flags"] & int(Flags.F_LAST)


def test_pre_r0_raw_side_2_maps_trade_ask_fill_cancel_bid() -> None:
    """Freeze current action-dependent side mapping without judging correctness."""
    adapter = _adapter()
    adapter.translate(_add(size=5, side=0))

    trade, fill, cancel = adapter.translate(_trade(size=2, side=2))

    assert trade["side"] == Side.ASK
    assert fill["side"] == Side.BID
    assert cancel["side"] == Side.BID


def test_pre_r0_partial_trade_updates_hkex_shadow_residual() -> None:
    adapter = _adapter()
    adapter.translate(_add(size=5, side=0))
    adapter.translate(_trade(size=2, side=2))

    residual_size, residual_price = adapter._order_sizes[(_OB_ID, _ORDER_ID, Side.BID)]
    assert residual_size == 3
    assert residual_price == _PRICE_RAW * FIXED_PRICE_SCALE


def test_pre_r0_order_id_zero_trade_is_dropped() -> None:
    adapter = _adapter()
    assert adapter.translate(_trade(order_id=0)) is None
