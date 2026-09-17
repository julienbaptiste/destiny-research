"""Failure-atomicity tests for canonical MBP-1 reconstruction outputs."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from reconstruction import build_mbp1 as mbp1_module


class _EmptyParquetFile:
    """Synthetic input that completes without emitting any record batches."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def iter_batches(self, batch_size: int):
        return iter(())


class _FailingParquetFile:
    """Synthetic input that fails after the staged output writer is open."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def iter_batches(self, batch_size: int):
        raise RuntimeError("synthetic reconstruction read failure")


class _SingleBatchParquetFile:
    """Synthetic one-event input used to force the final flush path."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def iter_batches(self, batch_size: int):
        batch = pa.RecordBatch.from_pydict(
            {
                "ts_event": pa.array([1_759_276_800_000_000_001], type=pa.uint64()),
                "ts_recv": pa.array([1_759_276_800_000_000_101], type=pa.uint64()),
                "action": pa.array(["ADD"], type=pa.string()),
                "side": pa.array(["BID"], type=pa.string()),
                "price": pa.array([4_500_250_000_000], type=pa.int64()),
                "size": pa.array([5], type=pa.uint32()),
                "order_id": pa.array([101], type=pa.uint64()),
                "flags": pa.array([0x80], type=pa.uint8()),
                "sequence": pa.array([1], type=pa.uint32()),
                "subsequence": pa.array([0], type=pa.uint16()),
                "instrument_id": pa.array([1001], type=pa.uint32()),
                "publisher_id": pa.array([1], type=pa.uint16()),
            }
        )
        return iter((batch,))


class _SuccessfulWriter:
    """Minimal writer that materializes deterministic staged bytes on close."""

    created_paths: list[Path] = []

    def __init__(self, path: Path, schema, compression: str) -> None:
        self.path = Path(path)
        type(self).created_paths.append(self.path)
        self.path.write_bytes(b"staged")

    def write_table(self, table) -> None:
        return None

    def close(self) -> None:
        self.path.write_bytes(b"complete-mbp1")


class _WriteFailingWriter(_SuccessfulWriter):
    """Writer that fails after physically staging bytes during a flush."""

    created_paths: list[Path] = []

    def write_table(self, table) -> None:
        self.path.write_bytes(b"partial-mbp1")
        raise OSError("synthetic Parquet write failure")


class _CloseFailingWriter(_SuccessfulWriter):
    """Writer whose finalization leaves staged bytes and then fails."""

    created_paths: list[Path] = []

    def close(self) -> None:
        self.path.write_bytes(b"partial-mbp1")
        raise OSError("synthetic Parquet close failure")


def _staged_files(out_file: Path) -> list[Path]:
    """Return any unpublished sibling staging files for one canonical target."""
    return list(out_file.parent.glob(f".{out_file.name}.*.tmp"))


def test_reconstruct_day_preserves_existing_target_on_processing_failure(
    tmp_path,
    monkeypatch,
) -> None:
    """A mid-reconstruction failure must not clobber a known-good target."""
    out_file = tmp_path / "ESZ25_20251001_mbp1.parquet"
    out_file.write_bytes(b"known-good")

    monkeypatch.setattr(mbp1_module.pq, "ParquetFile", _FailingParquetFile)
    monkeypatch.setattr(mbp1_module.pq, "ParquetWriter", _SuccessfulWriter)

    with pytest.raises(RuntimeError, match="synthetic reconstruction read failure"):
        mbp1_module.reconstruct_day(
            tmp_path / "input.parquet",
            out_file,
            product="ES",
            contract="ESZ25",
        )

    assert out_file.read_bytes() == b"known-good"
    assert _staged_files(out_file) == []


def test_reconstruct_day_preserves_existing_target_on_flush_failure(
    tmp_path,
    monkeypatch,
) -> None:
    """A failed write_table call must leave only the prior canonical output."""
    out_file = tmp_path / "ESZ25_20251001_mbp1.parquet"
    out_file.write_bytes(b"known-good")

    monkeypatch.setattr(mbp1_module.pq, "ParquetFile", _SingleBatchParquetFile)
    monkeypatch.setattr(mbp1_module.pq, "ParquetWriter", _WriteFailingWriter)

    with pytest.raises(OSError, match="synthetic Parquet write failure"):
        mbp1_module.reconstruct_day(
            tmp_path / "input.parquet",
            out_file,
            product="ES",
            contract="ESZ25",
        )

    assert out_file.read_bytes() == b"known-good"
    assert _staged_files(out_file) == []


def test_reconstruct_day_preserves_existing_target_on_writer_close_failure(
    tmp_path,
    monkeypatch,
) -> None:
    """A failed Parquet finalization must never publish the staged file."""
    out_file = tmp_path / "ESZ25_20251001_mbp1.parquet"
    out_file.write_bytes(b"known-good")

    monkeypatch.setattr(mbp1_module.pq, "ParquetFile", _EmptyParquetFile)
    monkeypatch.setattr(mbp1_module.pq, "ParquetWriter", _CloseFailingWriter)

    with pytest.raises(OSError, match="synthetic Parquet close failure"):
        mbp1_module.reconstruct_day(
            tmp_path / "input.parquet",
            out_file,
            product="ES",
            contract="ESZ25",
        )

    assert out_file.read_bytes() == b"known-good"
    assert _staged_files(out_file) == []


def test_reconstruct_day_atomically_replaces_target_after_success(
    tmp_path,
    monkeypatch,
) -> None:
    """Only a fully closed staged output may replace the canonical target."""
    out_file = tmp_path / "ESZ25_20251001_mbp1.parquet"
    out_file.write_bytes(b"old-output")
    _SuccessfulWriter.created_paths.clear()

    monkeypatch.setattr(mbp1_module.pq, "ParquetFile", _EmptyParquetFile)
    monkeypatch.setattr(mbp1_module.pq, "ParquetWriter", _SuccessfulWriter)

    stats = mbp1_module.reconstruct_day(
        tmp_path / "input.parquet",
        out_file,
        product="ES",
        contract="ESZ25",
    )

    assert stats["n_events"] == 0
    assert stats["n_rows_emitted"] == 0
    assert out_file.read_bytes() == b"complete-mbp1"
    assert _staged_files(out_file) == []

    assert len(_SuccessfulWriter.created_paths) == 1
    staged_path = _SuccessfulWriter.created_paths[0]
    assert staged_path.parent == out_file.parent
    assert staged_path.name.startswith(f".{out_file.name}.")
    assert staged_path.suffix == ".tmp"
