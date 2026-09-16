"""Self-tests for the R0 deep Parquet fingerprint.

These tests validate the detector itself. In particular, a mutation after row
50k must leave the legacy 50k sample unchanged while changing the full-file
fingerprint.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT / "tests" / "regression"))

from shared.fingerprint import semantic_parquet_fingerprint  # noqa: E402


def _write_fixture(path: Path, values: list[int]) -> None:
    table = pa.table(
        {
            "ts_event": pa.array(range(len(values)), type=pa.uint64()),
            "action": pa.array(["ADD"] * len(values)),
            "value": pa.array(values, type=pa.int64()),
        }
    )
    pq.write_table(table, path, compression="zstd")


def test_full_fingerprint_is_deterministic(tmp_path: Path) -> None:
    path = tmp_path / "a.parquet"
    _write_fixture(path, list(range(2_000)))

    first = semantic_parquet_fingerprint(path)
    second = semantic_parquet_fingerprint(path)

    assert first["semantic_sha256"] == second["semantic_sha256"]
    assert first["row_count"] == 2_000
    assert first["rows_hashed"] == 2_000


def test_late_mutation_escapes_50k_sample_but_not_full_hash(tmp_path: Path) -> None:
    original_path = tmp_path / "original.parquet"
    mutated_path = tmp_path / "mutated.parquet"

    values = list(range(60_000))
    mutated = values.copy()
    mutated[55_000] = -1

    _write_fixture(original_path, values)
    _write_fixture(mutated_path, mutated)

    original_sample = semantic_parquet_fingerprint(original_path, max_rows=50_000)
    mutated_sample = semantic_parquet_fingerprint(mutated_path, max_rows=50_000)
    assert original_sample["semantic_sha256"] == mutated_sample["semantic_sha256"]

    original_full = semantic_parquet_fingerprint(original_path)
    mutated_full = semantic_parquet_fingerprint(mutated_path)
    assert original_full["semantic_sha256"] != mutated_full["semantic_sha256"]


def test_row_reordering_changes_semantic_hash(tmp_path: Path) -> None:
    original_path = tmp_path / "original.parquet"
    reordered_path = tmp_path / "reordered.parquet"

    values = list(range(1_000))
    reordered = values.copy()
    reordered[-2], reordered[-1] = reordered[-1], reordered[-2]

    _write_fixture(original_path, values)
    _write_fixture(reordered_path, reordered)

    assert (
        semantic_parquet_fingerprint(original_path)["semantic_sha256"]
        != semantic_parquet_fingerprint(reordered_path)["semantic_sha256"]
    )


def test_schema_change_changes_semantic_hash(tmp_path: Path) -> None:
    a_path = tmp_path / "a.parquet"
    b_path = tmp_path / "b.parquet"

    pq.write_table(pa.table({"value": pa.array([1, 2, 3], type=pa.int32())}), a_path)
    pq.write_table(pa.table({"value": pa.array([1, 2, 3], type=pa.int64())}), b_path)

    assert (
        semantic_parquet_fingerprint(a_path)["semantic_sha256"]
        != semantic_parquet_fingerprint(b_path)["semantic_sha256"]
    )
