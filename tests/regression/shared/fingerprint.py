"""Streaming fingerprints for deterministic regression checks.

The existing regression suite hashes the first 50k rows of selected columns.
That is useful for fast checks, but it can miss regressions occurring later in
large trading sessions. This module adds a deep fingerprint that scans the
entire Parquet file in bounded Arrow batches.

The implementation deliberately avoids pandas and never materializes a full
trading day in RAM. Dictionary-encoded columns are decoded to strings before
hashing so dictionary ordering does not affect the semantic fingerprint.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.parquet as pq


DEFAULT_BATCH_SIZE = 100_000
_FILE_CHUNK_SIZE = 8 * 1024 * 1024


def file_sha256(path: Path) -> str:
    """Hash the physical file bytes using bounded reads."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(_FILE_CHUNK_SIZE):
            h.update(chunk)
    return h.hexdigest()


def semantic_parquet_fingerprint(
    path: Path,
    columns: Iterable[str] | None = None,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_rows: int | None = None,
) -> dict[str, object]:
    """Return a deterministic semantic fingerprint for a Parquet file.

    The hash is sensitive to schema, row ordering and values. The scan is
    streaming: at most one Arrow batch plus one decoded column is materialized
    at a time. ``max_rows`` exists only for characterization/self-tests and
    reproduces the old sampled-hash behavior when set to 50k.

    Notes
    -----
    The batch size is part of the hash contract. Keep it fixed for a baseline.
    We also persist a physical file SHA-256 separately because it is extremely
    cheap and catches metadata/compression changes that leave logical values
    untouched.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_rows is not None and max_rows < 0:
        raise ValueError("max_rows must be non-negative or None")

    parquet = pq.ParquetFile(path)
    available = parquet.schema_arrow.names
    selected = list(columns) if columns is not None else list(available)
    missing = [name for name in selected if name not in available]
    if missing:
        raise ValueError(f"Missing columns in {path}: {missing}")

    # Include the selected logical schema in the digest. This catches a type
    # change even if the decoded Python values happen to look identical.
    logical_fields = [parquet.schema_arrow.field(name) for name in selected]
    logical_schema = pa.schema(logical_fields)

    h = hashlib.sha256()
    h.update(b"destiny-parquet-semantic-v1\n")
    h.update(str(logical_schema).encode("utf-8"))
    h.update(f"\nbatch_size={batch_size}\n".encode("ascii"))

    rows_hashed = 0
    batch_index = 0

    for batch in parquet.iter_batches(batch_size=batch_size, columns=selected):
        if max_rows is not None:
            remaining = max_rows - rows_hashed
            if remaining <= 0:
                break
            if len(batch) > remaining:
                batch = batch.slice(0, remaining)

        h.update(f"batch={batch_index};rows={len(batch)}\n".encode("ascii"))

        for idx, name in enumerate(selected):
            arr = batch.column(idx)
            if pa.types.is_dictionary(arr.type):
                arr = arr.cast(pa.string())

            # Hash one column at a time to keep peak Python memory bounded.
            # The normalized schema uses integer/string/float/null scalars whose
            # repr is deterministic within the captured Python/PyArrow runtime.
            values = arr.to_pylist()
            h.update(name.encode("utf-8"))
            h.update(b"\0")
            h.update(repr(values).encode("utf-8"))
            h.update(b"\n")
            del values

        rows_hashed += len(batch)
        batch_index += 1

        if max_rows is not None and rows_hashed >= max_rows:
            break

    return {
        "semantic_sha256": h.hexdigest(),
        "file_sha256": file_sha256(path),
        "row_count": parquet.metadata.num_rows,
        "rows_hashed": rows_hashed,
        "columns": selected,
        "batch_size": batch_size,
        "schema": str(logical_schema),
        "file_size_bytes": path.stat().st_size,
    }
