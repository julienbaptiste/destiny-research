"""Pre-R0 characterization for Databento flag remapping and validator quirks.

These tests freeze legacy behavior before R0.1 changes the normalized contract.
They are intentionally descriptive rather than normative.
"""

from __future__ import annotations

from datetime import date

import ingestion.adapters.databento_adapter as databento_module
from ingestion.adapters.base import ContractInfo, SessionConfig
from ingestion.adapters.databento_adapter import DatabentoAdapter
from ingestion.schema import Action, Flags, Side, ValidationMode
from ingestion.validator import ValidatorState, validate_event


class _FixtureMBOMsg:
    """Minimal attribute-based stand-in for Databento MBOMsg."""

    def __init__(self, **values: object) -> None:
        self.__dict__.update(values)


def _adapter(monkeypatch) -> DatabentoAdapter:
    """Build a deterministic adapter without opening a DBN file."""
    monkeypatch.setattr(databento_module.db, "MBOMsg", _FixtureMBOMsg)

    adapter = DatabentoAdapter()
    adapter._config = SessionConfig(
        session_date=date(2025, 10, 1),
        warmup_enabled=False,
        validation_mode=ValidationMode.STRICT,
    )
    adapter._venue = "CME"
    adapter._contract_cache[1001] = ContractInfo(
        product="ES",
        contract="ESZ25",
        venue="CME",
        instrument_id=1001,
        is_spread=False,
        tick_size=250_000_000,
        currency="USD",
    )
    return adapter


def _raw_message(*, flags: int) -> _FixtureMBOMsg:
    """Return one valid Databento-shaped ADD with selected raw flags."""
    return _FixtureMBOMsg(
        instrument_id=1001,
        publisher_id=1,
        ts_event=1_759_276_800_000_000_001,
        ts_recv=1_759_276_800_000_000_101,
        action="A",
        side="B",
        price=4_500_250_000_000,
        size=5,
        order_id=101,
        flags=flags,
        sequence=1,
    )


def test_pre_r0_databento_tob_flag_is_remapped_to_internal_bit_zero(monkeypatch) -> None:
    """Freeze legacy F_TOB 0x40 -> internal 0x01 remapping."""
    adapter = _adapter(monkeypatch)
    event = adapter.translate(_raw_message(flags=0x80 | 0x40))

    assert event is not None
    assert event["flags"] == int(Flags.F_LAST) | int(Flags.F_TOB)
    assert event["flags"] == 0x81


def test_pre_r0_bad_ts_recv_is_remapped_to_generic_bad_ts(monkeypatch) -> None:
    """Freeze legacy provider F_BAD_TS_RECV 0x08 -> internal 0x04 mapping."""
    adapter = _adapter(monkeypatch)
    event = adapter.translate(_raw_message(flags=0x80 | 0x08))

    assert event is not None
    assert event["flags"] == int(Flags.F_LAST) | int(Flags.F_BAD_TS)
    assert event["flags"] == 0x84


def _event(action: str, *, size: int, sequence: int) -> dict[str, object]:
    """Build one normalized pre-R0 order event for validator characterization."""
    return {
        "ts_event": 1_759_276_800_000_000_000 + sequence,
        "ts_recv": 1_759_276_800_000_000_100 + sequence,
        "venue": "CME",
        "product": "ES",
        "contract": "ESZ25",
        "action": action,
        "side": Side.BID,
        "price": 4_500_250_000_000,
        "size": size,
        "order_id": 101,
        "flags": int(Flags.F_LAST),
        "sequence": sequence,
        "publisher_id": 1,
        "instrument_id": 1001,
    }


def test_pre_r0_partial_cancel_removes_order_from_validator_shadow() -> None:
    """Freeze legacy shadow behavior: any CANCEL drops the tracked order entirely."""
    state = ValidatorState(mode=ValidationMode.STRICT, warmup_mode=False)

    assert validate_event(_event(Action.ADD, size=5, sequence=1), state) == (True, None)
    assert validate_event(_event(Action.CANCEL, size=2, sequence=2), state) == (True, None)
    assert state.get_order(101) is None

    # A later MODIFY is therefore treated as an accepted orphan/GTC-style event
    # and increments the overloaded warmup-skip counter even after warmup.
    assert validate_event(_event(Action.MODIFY, size=3, sequence=3), state) == (True, None)
    assert state.get_order(101) is not None
    assert state.n_warmup_skip == 1
