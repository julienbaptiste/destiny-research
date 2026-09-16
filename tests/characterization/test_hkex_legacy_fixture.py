"""Replay synthetic legacy fixtures through the current HKEX adapter."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from ingestion.adapters.base import ContractInfo  # noqa: E402
from ingestion.adapters.hkex_adapter import HKEXAdapter  # noqa: E402
from ingestion.schema import FIXED_PRICE_SCALE, Side  # noqa: E402


_FIXTURE = _REPO_ROOT / "tests" / "fixtures" / "legacy_pre_r0" / "hkex_partial_fill.json"


def test_hkex_partial_fill_fixture_matches_pre_r0_behavior() -> None:
    payload = json.loads(_FIXTURE.read_text())
    contract = payload["contract"]
    add_raw, trade_raw = payload["raw_events"]
    expected = payload["expected_pre_r0"]

    adapter = HKEXAdapter()
    adapter._ob_map[contract["orderbook_id"]] = ContractInfo(
        product=contract["product"],
        contract=contract["contract"],
        venue="HKEX",
        instrument_id=contract["orderbook_id"],
        is_spread=False,
        tick_size=contract["tick_size_fp"],
        currency=contract["currency"],
    )

    add_events = adapter.translate(add_raw)
    assert add_events is not None
    assert len(add_events) == 1

    trade_events = adapter.translate(trade_raw)
    assert trade_events is not None
    assert [event["action"] for event in trade_events] == expected["trade_actions"]
    assert [event["side"] for event in trade_events] == expected["trade_sides"]
    assert [event["flags"] for event in trade_events] == expected["trade_flags"]

    key = (contract["orderbook_id"], add_raw["order_id"], Side.BID)
    residual_size, residual_price = adapter._order_sizes[key]
    assert residual_size == expected["passive_order_residual_size"]
    assert residual_price == expected["passive_order_price_fp"]
    assert residual_price == add_raw["price"] * FIXED_PRICE_SCALE
