"""Canonical R0.1 schema and flag invariants."""

from __future__ import annotations

from datetime import date

import ingestion.adapters.databento_adapter as databento_module
from ingestion import market_config
from ingestion.adapters.base import ContractInfo, SessionConfig
from ingestion.adapters.databento_adapter import DatabentoAdapter
from ingestion.schema import Flags, NormFlags, NORMALIZED_MBO_SCHEMA, REJECTED_EVENTS_SCHEMA


class _FixtureMBOMsg:
    """Minimal attribute-based Databento message stand-in for adapter tests."""

    def __init__(self, **values: object) -> None:
        self.__dict__.update(values)


def test_canonical_provider_flag_assignments() -> None:
    """Freeze provider/control bits independently from normalization provenance."""
    assert int(Flags.F_LAST) == 0x80
    assert int(Flags.F_TOB) == 0x40
    assert int(Flags.F_SNAPSHOT) == 0x20
    assert int(Flags.F_MBP) == 0x10
    assert int(Flags.F_BAD_TS_RECV) == 0x08
    assert int(Flags.F_MAYBE_BAD_BOOK) == 0x04
    assert int(Flags.F_PUBLISHER_SPECIFIC) == 0x02

    provider_mask = 0
    for flag in Flags:
        provider_mask |= int(flag)
    assert provider_mask & 0x01 == 0


def test_databento_reserved_provider_bit_is_preserved(monkeypatch) -> None:
    """Representation normalization must not erase an upstream reserved bit."""
    adapter = DatabentoAdapter()
    adapter._config = SessionConfig(session_date=date(2025, 10, 1))
    adapter._venue = "CME"
    adapter._contract_cache[1001] = ContractInfo(
        product="ES",
        contract="ESZ25",
        venue="CME",
        instrument_id=1001,
    )
    monkeypatch.setattr(databento_module.db, "MBOMsg", _FixtureMBOMsg)

    event = adapter.translate(
        _FixtureMBOMsg(
            ts_event=1_759_276_800_000_000_001,
            ts_recv=1_759_276_800_000_000_101,
            action="A",
            side="B",
            price=4_500_250_000_000,
            size=5,
            order_id=101,
            flags=0x89,  # F_LAST | F_BAD_TS_RECV | upstream reserved bit 0x01
            sequence=1,
            publisher_id=1,
            instrument_id=1001,
        )
    )

    assert event is not None
    assert event["flags"] == 0x89
    assert event["norm_flags"] == 0


def test_normalization_provenance_bits_are_unique_and_bounded() -> None:
    values = [int(flag) for flag in NormFlags if flag is not NormFlags.NONE]
    assert len(values) == len(set(values))
    assert all(value > 0 and value <= 0xFFFF for value in values)
    assert all(value & (value - 1) == 0 for value in values), "Each norm flag must be one bit"


def test_canonical_mbo_field_order_and_types() -> None:
    assert NORMALIZED_MBO_SCHEMA.names == [
        "ts_event",
        "ts_recv",
        "venue",
        "product",
        "contract",
        "action",
        "side",
        "price",
        "size",
        "order_id",
        "flags",
        "norm_flags",
        "sequence",
        "subsequence",
        "publisher_id",
        "instrument_id",
    ]
    assert str(NORMALIZED_MBO_SCHEMA.field("flags").type) == "uint8"
    assert str(NORMALIZED_MBO_SCHEMA.field("norm_flags").type) == "uint16"
    assert str(NORMALIZED_MBO_SCHEMA.field("sequence").type) == "uint32"
    assert str(NORMALIZED_MBO_SCHEMA.field("subsequence").type) == "uint16"


def test_rejected_schema_preserves_candidate_provenance() -> None:
    assert "norm_flags" in REJECTED_EVENTS_SCHEMA.names
    assert "subsequence" in REJECTED_EVENTS_SCHEMA.names
    assert REJECTED_EVENTS_SCHEMA.names[-2:] == ["reject_reason", "mode"]


def test_market_config_shared_databento_bits_match_canonical_contract() -> None:
    """Detect drift while market_config still exposes legacy convenience constants."""
    assert market_config.F_LAST == int(Flags.F_LAST)
    assert market_config.F_TOB == int(Flags.F_TOB)
    assert market_config.F_SNAPSHOT == int(Flags.F_SNAPSHOT)
    assert market_config.F_MBP == int(Flags.F_MBP)
    assert market_config.F_BAD_TS_RECV == int(Flags.F_BAD_TS_RECV)
