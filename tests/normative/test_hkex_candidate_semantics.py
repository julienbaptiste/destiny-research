"""Focused invariants for R0.2 HKEX semantic normalization."""

from __future__ import annotations

from datetime import date

from ingestion.adapters.base import ContractInfo, SessionConfig
from ingestion.adapters.hkex_adapter import HKEXAdapter
from ingestion.schema import Action, NormFlags, Side, ValidationMode
from ingestion.validator import RejectReason, ValidatorState, validate_event


def _adapter() -> HKEXAdapter:
    adapter = HKEXAdapter()
    adapter._config = SessionConfig(
        session_date=date(2026, 2, 3),
        validation_mode=ValidationMode.STRICT,
    )
    adapter._ob_map[135335842] = ContractInfo(
        product="HSI",
        contract="HSIG26",
        venue="HKEX",
        instrument_id=135335842,
        currency="HKD",
    )
    return adapter


def test_unresolved_trade_keeps_print_but_marks_synthetic_mutation_anomalous() -> None:
    """A truncated-session trade must not silently fabricate a valid book mutation."""
    adapter = _adapter()
    events = adapter.translate(
        {
            "source": "trade",
            "msg_type": 350,
            "orderbook_id": 135335842,
            "order_id": 999,
            "price": 27_000,
            "quantity": 2,
            "side": 2,
            "deal_type": 1,
            "combo_group_id": 0,
            "send_time_ns": 1_770_066_500_001_000_000,
            "trade_time_ns": 1_770_066_500_001_000_000,
            "seq_num": 50,
            "msg_index": 0,
        }
    )

    assert events is not None
    assert [event["action"] for event in events] == [
        Action.TRADE,
        Action.FILL,
        Action.CANCEL,
    ]
    assert events[0]["side"] == Side.ASK
    assert events[0]["norm_flags"] & int(NormFlags.N_VALIDATION_ANOMALY) == 0

    for event in events[1:]:
        assert event["norm_flags"] & int(NormFlags.N_SYNTHETIC)
        assert event["norm_flags"] & int(NormFlags.N_VALIDATION_ANOMALY)

    state = ValidatorState(mode=ValidationMode.STRICT, warmup_mode=False)
    verdicts = [validate_event(event, state) for event in events]
    assert verdicts[0] == (True, None)
    assert verdicts[1] == (True, None)
    assert verdicts[2] == (False, RejectReason.ORPHAN_CANCEL)

    stats = adapter.get_stats()
    assert stats["n_unresolved_trade_orders"] == 1
    assert stats["n_trade_size_overrun"] == 0


def test_trade_size_overrun_is_auditable_even_with_known_order() -> None:
    """Trade quantity above known residual is not accepted as a clean decrement."""
    adapter = _adapter()
    adapter._order_sizes[(135335842, 101, Side.BID)] = (
        1,
        27_000_000_000_000,
    )

    events = adapter.translate(
        {
            "source": "trade",
            "msg_type": 350,
            "orderbook_id": 135335842,
            "order_id": 101,
            "price": 27_000,
            "quantity": 2,
            "side": 2,
            "deal_type": 1,
            "combo_group_id": 0,
            "send_time_ns": 1_770_066_500_001_000_000,
            "trade_time_ns": 1_770_066_500_001_000_000,
            "seq_num": 51,
            "msg_index": 0,
        }
    )

    assert events is not None
    assert events[-1]["action"] == Action.CANCEL
    assert events[-1]["norm_flags"] & int(NormFlags.N_VALIDATION_ANOMALY)
    assert adapter.get_stats()["n_trade_size_overrun"] == 1
