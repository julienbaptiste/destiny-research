"""Code-only self-tests for R0 real-data canary provenance helpers."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

_THIS_DIR = Path(__file__).resolve().parent
_REGRESSION_DIR = _THIS_DIR.parent / "regression"
sys.path.insert(0, str(_THIS_DIR))
sys.path.insert(0, str(_REGRESSION_DIR))

from rerun_canary import (  # noqa: E402
    _compare_fingerprint,
    _compare_optional_parquet,
    _raw_input_manifest,
)
from shared.fingerprint import semantic_parquet_fingerprint  # noqa: E402


def _write_parquet(path: Path, values: list[int]) -> None:
    """Write a tiny deterministic parquet fixture for helper tests."""
    pq.write_table(pa.table({"value": pa.array(values, type=pa.int64())}), path)


def test_raw_input_manifest_hashes_databento_file(tmp_path):
    raw = tmp_path / "sample.dbn.zst"
    raw.write_bytes(b"destiny-canary-input")

    manifest = _raw_input_manifest(raw, "databento", "2025-10-01")

    assert manifest == [
        {
            "relative_path": raw.name,
            "size_bytes": len(b"destiny-canary-input"),
            "sha256": hashlib.sha256(b"destiny-canary-input").hexdigest(),
        }
    ]


def test_compare_fingerprint_reports_match_mismatch_and_no_reference(tmp_path):
    candidate = tmp_path / "candidate.parquet"
    reference = tmp_path / "reference.parquet"
    changed = tmp_path / "changed.parquet"
    missing = tmp_path / "missing.parquet"

    _write_parquet(candidate, [1, 2, 3])
    _write_parquet(reference, [1, 2, 3])
    _write_parquet(changed, [1, 2, 4])
    candidate_fp = semantic_parquet_fingerprint(candidate)

    assert _compare_fingerprint(candidate_fp, reference)["status"] == "MATCH"
    assert _compare_fingerprint(candidate_fp, changed)["status"] == "MISMATCH"
    assert _compare_fingerprint(candidate_fp, missing)["status"] == "NO_REFERENCE"


def test_compare_optional_parquet_treats_shared_absence_as_match(tmp_path):
    candidate = tmp_path / "candidate_rejected.parquet"
    reference = tmp_path / "reference_rejected.parquet"

    result = _compare_optional_parquet(candidate, None, reference)

    assert result["status"] == "MATCH"
    assert result["candidate_exists"] is False
    assert result["reference_exists"] is False
