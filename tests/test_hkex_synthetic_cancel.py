"""
tests/test_hkex_synthetic_cancel.py — Unit tests for HKEXAdapter.translate()

Tests the synthetic CANCEL logic, shadow state management, and combo leg
handling introduced to fix the HKEX MBP-1 reconstruction bugs.

Usage:
    pytest tests/test_hkex_synthetic_cancel.py -v

Strategy:
    We instantiate HKEXAdapter directly and bypass open_session() by
    populating _ob_map manually with synthetic ContractInfo objects.
    translate() is called with raw event dicts that mirror the structure
    produced by _iter_raw() — no parquet files needed.

    This approach is consistent with the existing test_hkex_parser.py style:
    synthetic data constructed in Python, no dependency on real HKEX files.

Coverage:
    TestSyntheticCancel    — Trade (350) → [TRADE, CANCEL] or None
    TestDeleteOrderResolution — Delete (332) price/size resolved from shadow state
    TestComboLegs          — same order_id on BID and ASK, independent shadow entries
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Path setup — allow running from repo root without installing the package
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.adapters.hkex_adapter import HKEXAdapter
from ingestion.adapters.base import ContractInfo
from ingestion.schema import Action, Side, FIXED_PRICE_SCALE


# ---------------------------------------------------------------------------
# Constants used across tests
# ---------------------------------------------------------------------------

_OB_ID      = 135_335_842     # realistic HKEX orderbook_id (HSI front month)
_OB_ID_2    = 135_335_999     # second orderbook_id (different contract)
_ORDER_ID   = 8_108_676_166_385_258_219   # realistic HKEX order_id
_TS         = 1_769_995_480_741_000_000   # arbitrary nanosecond timestamp
_SEQ        = 4_101_677
_PRICE_RAW  = 27_079           # raw int32 price in index points (no decimals)
_PRICE_FP   = _PRICE_RAW * FIXED_PRICE_SCALE   # fixed-point int64
_SIZE       = 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_adapter_with_contract(
    ob_id   : int   = _OB_ID,
    product : str   = "HSI",
    contract: str   = "HSIG26",
    ob_id_2 : int | None = None,
    product2: str   = "HHI",
    contract2: str  = "HHIG26",
) -> HKEXAdapter:
    """
    Create an HKEXAdapter with _ob_map pre-populated.
    Bypasses open_session() — safe for unit testing translate() in isolation.
    """
    adapter = HKEXAdapter()

    info = ContractInfo(
        product       = product,
        contract      = contract,
        venue         = "HKEX",
        instrument_id = ob_id,
        is_spread     = False,
        tick_size     = 1_000_000_000,   # 1 index point
        currency      = "HKD",
    )
    adapter._ob_map[ob_id] = info

    if ob_id_2 is not None:
        info2 = ContractInfo(
            product       = product2,
            contract      = contract2,
            venue         = "HKEX",
            instrument_id = ob_id_2,
            is_spread     = False,
            tick_size     = 1_000_000_000,
            currency      = "HKD",
        )
        adapter._ob_map[ob_id_2] = info2

    return adapter


def _raw_add(
    ob_id    : int   = _OB_ID,
    order_id : int   = _ORDER_ID,
    price    : int   = _PRICE_RAW,
    size     : int   = _SIZE,
    side     : int   = 0,        # 0=BID, 1=ASK
    seq      : int   = _SEQ,
    ts       : int   = _TS,
) -> dict:
    """Synthetic raw AddOrder (msg_type=330) event dict."""
    return {
        "source"       : "order",
        "msg_type"     : 330,
        "orderbook_id" : ob_id,
        "symbol"       : "HSIG6",
        "class_code"   : "HSI",
        "order_id"     : order_id,
        "price"        : price,
        "quantity"     : size,
        "side"         : side,
        "send_time_ns" : ts,
        "seq_num"      : seq,
        "msg_index"    : 0,
    }


def _raw_delete(
    ob_id    : int   = _OB_ID,
    order_id : int   = _ORDER_ID,
    side     : int   = 0,        # 0=BID, 1=ASK (raw side on Delete)
    seq      : int   = _SEQ + 1,
    ts       : int   = _TS + 1_000_000,
) -> dict:
    """
    Synthetic raw DeleteOrder (msg_type=332) event dict.
    price=0, quantity=0 — matches actual HKEX raw feed behavior.
    """
    return {
        "source"       : "order",
        "msg_type"     : 332,
        "orderbook_id" : ob_id,
        "symbol"       : "HSIG6",
        "class_code"   : "HSI",
        "order_id"     : order_id,
        "price"        : 0,       # always 0 in raw HKEX feed
        "quantity"     : 0,       # always 0 in raw HKEX feed
        "side"         : side,
        "send_time_ns" : ts,
        "seq_num"      : seq,
        "msg_index"    : 0,
    }


def _raw_trade(
    ob_id         : int   = _OB_ID,
    order_id      : int   = _ORDER_ID,
    price         : int   = _PRICE_RAW,
    size          : int   = 2,
    trade_side    : int   = 2,   # 2=Buy passive (BID), 3=Sell passive (ASK)
    seq           : int   = _SEQ + 10,
    ts            : int   = _TS + 10_000_000,
    trade_time_ns : int   = 0,
    combo_group_id: int   = 0,
) -> dict:
    """
    Synthetic raw Trade (msg_type=350) event dict.
    trade_time_ns=0 → ts_event falls back to send_time_ns.
    combo_group_id=0 → standard outright trade.
    """
    return {
        "source"        : "trade",
        "msg_type"      : 350,
        "orderbook_id"  : ob_id,
        "symbol"        : "HSIG6",
        "class_code"    : "HSI",
        "order_id"      : order_id,
        "price"         : price,
        "quantity"      : size,
        "side"          : trade_side,
        "send_time_ns"  : ts,
        "seq_num"       : seq,
        "msg_index"     : 0,
        "trade_time_ns" : trade_time_ns,
        "trade_id"      : 999,
        "deal_type"     : 0,
        "combo_group_id": combo_group_id,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TestSyntheticCancel — Trade (350) behavior
# ═══════════════════════════════════════════════════════════════════════════════

class TestSyntheticCancel:
    """
    Verify that translate() emits [TRADE, synthetic CANCEL] for trades with
    order_id > 0, and None for combo leg trades with order_id == 0.
    """

    def test_trade_with_order_id_returns_two_events(self):
        """
        Trade (350) with order_id > 0 must return a two-element list:
        [TRADE event, synthetic CANCEL event].
        Per OMD-D spec §3.5: the passive resting order must be decremented.
        """
        adapter = _make_adapter_with_contract()

        # Register the resting order in shadow state first
        adapter.translate(_raw_add(size=5, side=0))   # ADD BID

        result = adapter.translate(_raw_trade(order_id=_ORDER_ID, size=2))

        assert result is not None
        assert len(result) == 2
        trade_event, cancel_event = result
        assert trade_event["action"]  == Action.TRADE
        assert cancel_event["action"] == Action.CANCEL

    def test_combo_leg_trade_order_id_zero_returns_none(self):
        """
        Trade (350) with order_id == 0 is a combo leg trade.
        Per design decision: dropped at normalization — no resting order,
        price is not constrained by the outright BBO.
        """
        adapter = _make_adapter_with_contract()
        result = adapter.translate(_raw_trade(order_id=0))
        assert result is None

    def test_synthetic_cancel_carries_passive_bid_side(self):
        """
        Trade with trade_side=2 (Buy Order passive) → synthetic CANCEL
        must carry Side.BID — the side of the resting order that was consumed.
        """
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(side=0))   # ADD BID

        result = adapter.translate(_raw_trade(trade_side=2, order_id=_ORDER_ID))
        _, cancel_event = result

        assert cancel_event["side"]  == Side.BID

    def test_synthetic_cancel_carries_passive_ask_side(self):
        """
        Trade with trade_side=3 (Sell Order passive) → synthetic CANCEL
        must carry Side.ASK.
        """
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(side=1))   # ADD ASK

        result = adapter.translate(
            _raw_trade(trade_side=3, order_id=_ORDER_ID, price=_PRICE_RAW)
        )
        _, cancel_event = result

        assert cancel_event["side"]  == Side.ASK

    def test_synthetic_cancel_carries_trade_price(self):
        """
        Synthetic CANCEL price must equal the trade execution price,
        which equals the resting order's limit price.
        """
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(price=_PRICE_RAW, side=0))

        result = adapter.translate(
            _raw_trade(price=_PRICE_RAW, order_id=_ORDER_ID, size=2)
        )
        _, cancel_event = result

        assert cancel_event["price"] == _PRICE_FP

    def test_synthetic_cancel_carries_traded_size(self):
        """
        Synthetic CANCEL size must equal the quantity traded — the delta
        to subtract from the resting order. Not the full order size.
        """
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(size=5, side=0))

        result = adapter.translate(
            _raw_trade(size=2, order_id=_ORDER_ID)
        )
        _, cancel_event = result

        assert cancel_event["size"]     == 2
        assert cancel_event["order_id"] == _ORDER_ID

    def test_synthetic_cancel_increments_counter(self):
        """_n_synthetic_cancels must be incremented for each Trade with order_id > 0."""
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(size=10, side=0))

        adapter.translate(_raw_trade(size=3, order_id=_ORDER_ID, seq=_SEQ+10))
        assert adapter._n_synthetic_cancels == 1

        # Simulate partial fill — order still resting, add another trade
        adapter.translate(
            _raw_trade(size=2, order_id=_ORDER_ID, seq=_SEQ+20, ts=_TS+20_000_000)
        )
        assert adapter._n_synthetic_cancels == 2

    def test_partial_fill_updates_shadow_state_residual(self):
        """
        After a partial fill, the shadow state must reflect the residual size.
        A subsequent Delete (332) should emit a CANCEL with the residual size.
        """
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(size=5, price=_PRICE_RAW, side=0))

        # Partial fill: trade 2 out of 5 → residual 3
        adapter.translate(_raw_trade(size=2, order_id=_ORDER_ID, seq=_SEQ+10))

        key = (_OB_ID, _ORDER_ID, Side.BID)
        residual_size, residual_price = adapter._order_sizes[key]
        assert residual_size  == 3
        assert residual_price == _PRICE_FP

    def test_full_fill_removes_from_shadow_state(self):
        """
        After a full fill (traded size == resting size), the order_id must
        be removed from the shadow state entirely.
        """
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(size=3, price=_PRICE_RAW, side=0))

        # Full fill
        adapter.translate(_raw_trade(size=3, order_id=_ORDER_ID, seq=_SEQ+10))

        key = (_OB_ID, _ORDER_ID, Side.BID)
        assert key not in adapter._order_sizes

    def test_two_partial_fills_sequential(self):
        """
        Two sequential partial fills must correctly decrement the shadow state.
        First trade: size=2 → residual 3.
        Second trade: size=3 → residual 0 → removed.
        """
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(size=5, price=_PRICE_RAW, side=0))

        adapter.translate(_raw_trade(size=2, order_id=_ORDER_ID, seq=_SEQ+10))
        adapter.translate(
            _raw_trade(size=3, order_id=_ORDER_ID, seq=_SEQ+20, ts=_TS+20_000_000)
        )

        key = (_OB_ID, _ORDER_ID, Side.BID)
        assert key not in adapter._order_sizes


# ═══════════════════════════════════════════════════════════════════════════════
# TestDeleteOrderResolution — Delete (332) shadow state resolution
# ═══════════════════════════════════════════════════════════════════════════════

class TestDeleteOrderResolution:
    """
    Verify that Delete (332) — which carries price=0 and quantity=0 in the
    raw HKEX feed — is correctly resolved from the shadow state.
    """

    def test_delete_after_add_resolves_price_and_size(self):
        """
        Delete (332) on a known order_id must produce a CANCEL event with
        the correct price and size resolved from the shadow state.
        The raw price=0 and quantity=0 must NOT appear in the output.
        """
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(price=_PRICE_RAW, size=_SIZE, side=0))

        result = adapter.translate(_raw_delete(side=0))

        assert result is not None
        assert len(result) == 1
        cancel = result[0]
        assert cancel["action"] == Action.CANCEL
        assert cancel["price"]  == _PRICE_FP       # resolved, not 0
        assert cancel["size"]   == _SIZE            # resolved, not 0
        assert cancel["side"]   == Side.BID

    def test_delete_removes_order_from_shadow_state(self):
        """
        After a Delete (332), the order must be removed from the shadow state.
        A subsequent Delete on the same order_id must be dropped.
        """
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(price=_PRICE_RAW, size=_SIZE, side=0))
        adapter.translate(_raw_delete(side=0))

        # Shadow state must be empty for this key
        key = (_OB_ID, _ORDER_ID, Side.BID)
        assert key not in adapter._order_sizes

    def test_delete_on_unknown_order_id_emits_noop_cancel(self):
        """
        Delete (332) on an order_id not in the shadow state (GTC from a previous
        session) emits a no-op CANCEL with price=0 and size=0 rather than being
        dropped. The CANCEL is harmless — _book_cancel() will increment
        _n_orphan_cancel and return without touching the book.
        """
        adapter = _make_adapter_with_contract()

        result = adapter.translate(_raw_delete(order_id=_ORDER_ID + 1, side=0))

        assert result is not None
        assert len(result) == 1
        cancel = result[0]
        assert cancel["action"] == Action.CANCEL
        assert cancel["price"]  == 0
        assert cancel["size"]   == 0

    def test_delete_after_partial_fill_resolves_residual(self):
        """
        When a partial fill has already decremented the shadow state,
        a subsequent Delete must use the residual size, not the original size.
        """
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(price=_PRICE_RAW, size=5, side=0))

        # Partial fill: 2 out of 5 traded → residual 3
        adapter.translate(_raw_trade(size=2, order_id=_ORDER_ID, seq=_SEQ+10))

        # Delete must emit CANCEL with residual size=3
        result = adapter.translate(_raw_delete(side=0, seq=_SEQ+20))
        assert result is not None
        cancel = result[0]
        assert cancel["size"]  == 3
        assert cancel["price"] == _PRICE_FP

    def test_delete_ask_side_resolves_correctly(self):
        """Delete (332) on an ASK order resolves with ASK side."""
        adapter = _make_adapter_with_contract()
        adapter.translate(_raw_add(price=_PRICE_RAW, size=4, side=1))  # ADD ASK

        result = adapter.translate(_raw_delete(side=1))  # DELETE ASK
        assert result is not None
        cancel = result[0]
        assert cancel["side"]  == Side.ASK
        assert cancel["size"]  == 4
        assert cancel["price"] == _PRICE_FP


# ═══════════════════════════════════════════════════════════════════════════════
# TestComboLegs — same order_id on BID and ASK simultaneously
# ═══════════════════════════════════════════════════════════════════════════════

class TestComboLegs:
    """
    HKEX combination orders use the same order_id for both legs — one on
    BID and one on ASK. The shadow state key (orderbook_id, order_id, side)
    must keep them independent.
    """

    def test_same_order_id_bid_ask_independent_shadow_entries(self):
        """
        ADD BID and ADD ASK with the same order_id must create two
        independent entries in the shadow state, keyed by side.
        """
        adapter = _make_adapter_with_contract()

        price_bid = 27_079
        price_ask = 27_087

        # Both legs share the same order_id
        adapter.translate(_raw_add(price=price_bid, size=2, side=0,
                                   order_id=_ORDER_ID, seq=_SEQ))
        adapter.translate(_raw_add(price=price_ask, size=3, side=1,
                                   order_id=_ORDER_ID, seq=_SEQ+1))

        key_bid = (_OB_ID, _ORDER_ID, Side.BID)
        key_ask = (_OB_ID, _ORDER_ID, Side.ASK)

        assert key_bid in adapter._order_sizes
        assert key_ask in adapter._order_sizes

        size_bid, fp_bid = adapter._order_sizes[key_bid]
        size_ask, fp_ask = adapter._order_sizes[key_ask]

        assert size_bid == 2
        assert fp_bid   == price_bid * FIXED_PRICE_SCALE
        assert size_ask == 3
        assert fp_ask   == price_ask * FIXED_PRICE_SCALE

    def test_delete_bid_leg_does_not_affect_ask_leg(self):
        """
        Deleting the BID leg of a combo order must not touch the ASK leg
        in the shadow state.
        """
        adapter = _make_adapter_with_contract()

        adapter.translate(_raw_add(price=27_079, size=2, side=0,
                                   order_id=_ORDER_ID, seq=_SEQ))
        adapter.translate(_raw_add(price=27_087, size=3, side=1,
                                   order_id=_ORDER_ID, seq=_SEQ+1))

        # Delete BID leg
        adapter.translate(_raw_delete(side=0, seq=_SEQ+2))

        key_bid = (_OB_ID, _ORDER_ID, Side.BID)
        key_ask = (_OB_ID, _ORDER_ID, Side.ASK)

        assert key_bid not in adapter._order_sizes   # BID removed
        assert key_ask in adapter._order_sizes        # ASK untouched

    def test_delete_bid_leg_resolves_bid_price(self):
        """
        Deleting the BID leg must produce a CANCEL with the BID price,
        not the ASK price.
        """
        adapter = _make_adapter_with_contract()

        price_bid = 27_079
        price_ask = 27_087

        adapter.translate(_raw_add(price=price_bid, size=2, side=0,
                                   order_id=_ORDER_ID, seq=_SEQ))
        adapter.translate(_raw_add(price=price_ask, size=3, side=1,
                                   order_id=_ORDER_ID, seq=_SEQ+1))

        result = adapter.translate(_raw_delete(side=0, seq=_SEQ+2))
        assert result is not None
        cancel = result[0]
        assert cancel["price"] == price_bid * FIXED_PRICE_SCALE
        assert cancel["size"]  == 2
        assert cancel["side"]  == Side.BID

    def test_cross_contract_same_order_id_independent(self):
        """
        Same order_id on two different contracts (different orderbook_id)
        must have fully independent shadow state entries.
        The orderbook_id is part of the composite key.
        """
        adapter = _make_adapter_with_contract(
            ob_id=_OB_ID, product="HSI", contract="HSIG26",
            ob_id_2=_OB_ID_2, product2="HHI", contract2="HHIG26",
        )

        price_hsi = 27_079
        price_hhi = 22_345

        # Same order_id, different contracts
        adapter.translate(_raw_add(ob_id=_OB_ID,   price=price_hsi,
                                   size=3, side=0, order_id=_ORDER_ID, seq=_SEQ))
        adapter.translate(_raw_add(ob_id=_OB_ID_2, price=price_hhi,
                                   size=7, side=0, order_id=_ORDER_ID, seq=_SEQ+1))

        key_hsi = (_OB_ID,   _ORDER_ID, Side.BID)
        key_hhi = (_OB_ID_2, _ORDER_ID, Side.BID)

        assert key_hsi in adapter._order_sizes
        assert key_hhi in adapter._order_sizes

        size_hsi, fp_hsi = adapter._order_sizes[key_hsi]
        size_hhi, fp_hhi = adapter._order_sizes[key_hhi]

        assert size_hsi == 3
        assert fp_hsi   == price_hsi * FIXED_PRICE_SCALE
        assert size_hhi == 7
        assert fp_hhi   == price_hhi * FIXED_PRICE_SCALE

    def test_delete_on_one_contract_does_not_affect_other(self):
        """
        Deleting order_id on HSI must not affect the same order_id on HHI.
        """
        adapter = _make_adapter_with_contract(
            ob_id=_OB_ID,   product="HSI", contract="HSIG26",
            ob_id_2=_OB_ID_2, product2="HHI", contract2="HHIG26",
        )

        adapter.translate(_raw_add(ob_id=_OB_ID,   price=27_079,
                                   size=3, side=0, order_id=_ORDER_ID, seq=_SEQ))
        adapter.translate(_raw_add(ob_id=_OB_ID_2, price=22_345,
                                   size=7, side=0, order_id=_ORDER_ID, seq=_SEQ+1))

        # Delete only HSI leg
        adapter.translate(_raw_delete(ob_id=_OB_ID, side=0, seq=_SEQ+2))

        key_hsi = (_OB_ID,   _ORDER_ID, Side.BID)
        key_hhi = (_OB_ID_2, _ORDER_ID, Side.BID)

        assert key_hsi not in adapter._order_sizes   # HSI removed
        assert key_hhi in adapter._order_sizes        # HHI untouched