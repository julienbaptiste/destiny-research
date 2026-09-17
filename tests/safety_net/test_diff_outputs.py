"""Mutation tests for the schema-aware baseline-vs-candidate differential runner."""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_THIS_DIR = Path(__file__).resolve().parent
_REGRESSION_DIR = _REPO_ROOT / "tests" / "regression"
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_THIS_DIR))
sys.path.insert(0, str(_REGRESSION_DIR))

from diff_outputs import apply_classification, compare_parquet_outputs  # noqa: E402
from ingestion.schema import NORMALIZED_MBO_SCHEMA  # noqa: E402


_LEGACY_MBO_SCHEMA = pa.schema(
    [
        field
        for field in NORMALIZED_MBO_SCHEMA
        if field.name not in {"norm_flags", "subsequence"}
    ]
)


def _row(sequence: int, *, size: int = 5) -> dict:
    return {
        "ts_event": 1_000_000_000 + sequence,
        "ts_recv": 1_000_000_100 + sequence,
        "venue": "CME",
        "product": "ES",
        "contract": "ESZ25",
        "action": "ADD",
        "side": "BID",
        "price": 4_500_250_000_000,
        "size": size,
        "order_id": 100 + sequence,
        "flags": 0x80,
        "norm_flags": 0,
        "sequence": sequence,
        "subsequence": 0,
        "publisher_id": 1,
        "instrument_id": 1001,
    }


def _write_mbo(path: Path, rows: list[dict], *, legacy: bool = False) -> None:
    schema = _LEGACY_MBO_SCHEMA if legacy else NORMALIZED_MBO_SCHEMA
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path, compression="zstd")


def test_identical_outputs_require_no_classification(tmp_path):
    baseline = tmp_path / "baseline.parquet"
    candidate = tmp_path / "candidate.parquet"
    rows = [_row(1), _row(2)]
    _write_mbo(baseline, rows)
    _write_mbo(candidate, rows)

    report = compare_parquet_outputs(baseline, candidate, kind="mbo", batch_size=1)

    assert report["status"] == "IDENTICAL"
    assert report["classification"] == "NO_CHANGE"
    assert report["differing_batches"] == []
    assert report["row_differences"] == []


def test_mutation_is_unclassified_and_localized(tmp_path):
    baseline = tmp_path / "baseline.parquet"
    candidate = tmp_path / "candidate.parquet"
    _write_mbo(baseline, [_row(1), _row(2), _row(3)])
    _write_mbo(candidate, [_row(1), _row(2, size=7), _row(3)])

    report = compare_parquet_outputs(baseline, candidate, kind="mbo", batch_size=1)

    assert report["status"] == "DIFFERENT"
    assert report["classification"] == "UNCLASSIFIED"
    assert len(report["differing_batches"]) == 1
    assert report["row_differences"][0]["row"] == 1
    assert report["row_differences"][0]["changed_fields"] == ["size"]
    assert report["row_differences"][0]["baseline"] == {"size": 5}
    assert report["row_differences"][0]["candidate"] == {"size": 7}


def test_schema_migration_reports_added_columns_without_losing_row_diagnostics(tmp_path):
    baseline = tmp_path / "legacy.parquet"
    candidate = tmp_path / "canonical.parquet"
    rows = [_row(1), _row(2)]
    _write_mbo(baseline, rows, legacy=True)
    _write_mbo(candidate, rows)

    report = compare_parquet_outputs(baseline, candidate, kind="mbo", batch_size=1)

    assert report["status"] == "DIFFERENT"
    assert report["classification"] == "UNCLASSIFIED"
    assert report["schema_evolution"]["baseline_missing_target_columns"] == [
        "norm_flags",
        "subsequence",
    ]
    assert report["schema_evolution"]["candidate_added_columns"] == [
        "norm_flags",
        "subsequence",
    ]
    # The common semantic fields are identical; only schema evolution differs.
    assert report["differing_batches"] == []
    assert report["row_differences"] == []


def test_expected_change_must_bind_exact_diff_signature(tmp_path):
    baseline = tmp_path / "baseline.parquet"
    candidate = tmp_path / "candidate.parquet"
    _write_mbo(baseline, [_row(1)])
    _write_mbo(candidate, [_row(1, size=9)])
    report = compare_parquet_outputs(baseline, candidate, kind="mbo")

    classified = apply_classification(
        report,
        {
            "diff_signature": report["diff_signature"],
            "classification": "EXPECTED_CHANGE",
            "decision_ref": "ADR-003",
            "note": "Synthetic self-test only.",
        },
    )
    assert classified["classification"] == "EXPECTED_CHANGE"
    assert classified["classification_detail"]["decision_ref"] == "ADR-003"


def test_classification_for_another_diff_is_rejected(tmp_path):
    baseline = tmp_path / "baseline.parquet"
    candidate = tmp_path / "candidate.parquet"
    _write_mbo(baseline, [_row(1)])
    _write_mbo(candidate, [_row(1, size=9)])
    report = compare_parquet_outputs(baseline, candidate, kind="mbo")

    with pytest.raises(ValueError, match="diff_signature"):
        apply_classification(
            report,
            {
                "diff_signature": "not-this-diff",
                "classification": "EXPECTED_CHANGE",
                "decision_ref": "ADR-003",
            },
        )
