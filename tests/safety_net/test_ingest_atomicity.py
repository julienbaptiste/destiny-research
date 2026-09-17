"""Failure-atomicity tests for canonical ingestion outputs."""

from __future__ import annotations

from datetime import date

import pytest

from ingestion import ingest as ingest_module
from ingestion.adapters.base import ContractInfo
from ingestion.schema import normalized_path, rejected_path, ValidationMode


class _FailingAdapter:
    """Minimal adapter that flushes rows before failing mid-session."""

    PROVIDER = "test"

    def __init__(self) -> None:
        self.info = ContractInfo(
            product="ES",
            contract="ESZ25",
            venue="CME",
            instrument_id=1001,
        )
        self.close_called = False

    def open_session(self, raw_source, config) -> None:
        return None

    def close_session(self) -> None:
        self.close_called = True

    def list_instruments(self) -> list[ContractInfo]:
        return [self.info]

    def resolve_contract(self, instrument_id: int, session_date: date) -> ContractInfo | None:
        return self.info if instrument_id == self.info.instrument_id else None

    def iter_events(self):
        event = {
            "ts_event": 1_759_276_800_000_000_001,
            "ts_recv": 1_759_276_800_000_000_101,
            "venue": "CME",
            "product": "ES",
            "contract": "ESZ25",
            "action": "ADD",
            "side": "BID",
            "price": 4_500_250_000_000,
            "size": 5,
            "order_id": 101,
            "flags": 0x80,
            "norm_flags": 0,
            "sequence": 1,
            "subsequence": 0,
            "publisher_id": 1,
            "instrument_id": 1001,
        }

        # With BATCH_SIZE=1 the first row is physically written immediately.
        yield dict(event)
        # The exact duplicate is rejected and physically flushes the audit file.
        yield dict(event)
        raise RuntimeError("synthetic mid-session failure")


def test_ingestion_failure_removes_partial_clean_and_rejected_outputs(
    tmp_path,
    monkeypatch,
) -> None:
    """A failed day must never leave readable partial Parquet as canonical data."""
    monkeypatch.setattr(ingest_module, "BATCH_SIZE", 1)

    raw_source = tmp_path / "raw.bin"
    raw_source.write_bytes(b"synthetic")
    normalized_root = tmp_path / "normalized"
    adapter = _FailingAdapter()
    session_date = date(2025, 10, 1)

    with pytest.raises(RuntimeError, match="synthetic mid-session failure"):
        ingest_module.ingest_file(
            adapter=adapter,
            raw_path=raw_source,
            normalized_dir=normalized_root,
            session_date=session_date,
            mode=ValidationMode.STRICT,
            verbose=False,
        )

    clean = normalized_path(
        base_dir=normalized_root,
        provider="test",
        venue="CME",
        product="ES",
        contract="ESZ25",
        year=2025,
        month=10,
        date_str="20251001",
    )
    rejected = rejected_path(
        base_dir=normalized_root,
        provider="test",
        venue="CME",
        product="ES",
        contract="ESZ25",
        year=2025,
        month=10,
        date_str="20251001",
    )

    assert not clean.exists()
    assert not rejected.exists()
    assert adapter.close_called is True
