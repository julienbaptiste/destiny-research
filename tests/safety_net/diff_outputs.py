"""Bounded-memory semantic diff for baseline-vs-candidate Parquet outputs.

The runner first compares deep semantic fingerprints. When they differ, it scans
fixed-size Arrow batches in lockstep, hashes each batch, and materializes rows
only for differing batches. Reports are intentionally UNCLASSIFIED until a
human-authored classification manifest binds the exact diff signature to either
EXPECTED_CHANGE or UNEXPECTED_REGRESSION.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from itertools import zip_longest
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.schema import NORMALIZED_MBO_SCHEMA
from reconstruction.build_mbp1 import MBP1_SCHEMA
from shared.fingerprint import DEFAULT_BATCH_SIZE, semantic_parquet_fingerprint


_KIND_COLUMNS = {
    "mbo": list(NORMALIZED_MBO_SCHEMA.names),
    "mbp1": list(MBP1_SCHEMA.names),
}
_VALID_CLASSIFICATIONS = {"EXPECTED_CHANGE", "UNEXPECTED_REGRESSION"}


def _batch_hash(batch: pa.RecordBatch, columns: list[str]) -> str:
    """Hash one logical Arrow batch with dictionary values decoded to strings."""
    h = hashlib.sha256()
    h.update(f"rows={len(batch)}\n".encode("ascii"))
    for index, name in enumerate(columns):
        arr = batch.column(index)
        if pa.types.is_dictionary(arr.type):
            arr = arr.cast(pa.string())
        values = arr.to_pylist()
        h.update(name.encode("utf-8"))
        h.update(b"\0")
        h.update(repr(values).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _batch_rows(batch: pa.RecordBatch, columns: list[str]) -> list[dict[str, Any]]:
    """Materialize one already-identified differing batch for diagnostic output."""
    arrays = []
    for index in range(len(columns)):
        arr = batch.column(index)
        if pa.types.is_dictionary(arr.type):
            arr = arr.cast(pa.string())
        arrays.append(arr)
    table = pa.Table.from_arrays(arrays, names=columns)
    return table.to_pylist()


def _required_columns(kind: str) -> list[str]:
    try:
        return _KIND_COLUMNS[kind]
    except KeyError as exc:
        raise ValueError(f"Unsupported diff kind: {kind}") from exc


def _schema_check(path: Path, required: list[str]) -> dict[str, Any]:
    parquet = pq.ParquetFile(path)
    available = parquet.schema_arrow.names
    missing = [column for column in required if column not in available]
    return {
        "schema": str(parquet.schema_arrow),
        "missing_required_columns": missing,
    }


def _diff_signature(
    kind: str,
    baseline_fp: dict[str, object],
    candidate_fp: dict[str, object],
) -> str:
    """Create a stable signature that changes whenever either logical output changes."""
    payload = {
        "version": 1,
        "kind": kind,
        "baseline_semantic_sha256": baseline_fp["semantic_sha256"],
        "baseline_row_count": baseline_fp["row_count"],
        "baseline_schema": baseline_fp["schema"],
        "candidate_semantic_sha256": candidate_fp["semantic_sha256"],
        "candidate_row_count": candidate_fp["row_count"],
        "candidate_schema": candidate_fp["schema"],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _row_difference(
    absolute_row: int,
    baseline_row: dict[str, Any] | None,
    candidate_row: dict[str, Any] | None,
    columns: Iterable[str],
) -> dict[str, Any]:
    """Describe one positional row difference without guessing semantic intent."""
    if baseline_row is None:
        return {
            "row": absolute_row,
            "kind": "CANDIDATE_ONLY",
            "baseline": None,
            "candidate": candidate_row,
        }
    if candidate_row is None:
        return {
            "row": absolute_row,
            "kind": "BASELINE_ONLY",
            "baseline": baseline_row,
            "candidate": None,
        }

    changed_fields = [
        column for column in columns if baseline_row.get(column) != candidate_row.get(column)
    ]
    return {
        "row": absolute_row,
        "kind": "ROW_CHANGED",
        "changed_fields": changed_fields,
        "baseline": {column: baseline_row.get(column) for column in changed_fields},
        "candidate": {column: candidate_row.get(column) for column in changed_fields},
    }


def compare_parquet_outputs(
    baseline: Path,
    candidate: Path,
    *,
    kind: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_row_diffs: int = 50,
) -> dict[str, Any]:
    """Compare two MBO or MBP-1 outputs with bounded memory."""
    baseline = Path(baseline)
    candidate = Path(candidate)
    if not baseline.exists():
        raise FileNotFoundError(baseline)
    if not candidate.exists():
        raise FileNotFoundError(candidate)
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_row_diffs < 0:
        raise ValueError("max_row_diffs must be non-negative")

    columns = _required_columns(kind)
    baseline_schema = _schema_check(baseline, columns)
    candidate_schema = _schema_check(candidate, columns)
    missing = {
        "baseline": baseline_schema["missing_required_columns"],
        "candidate": candidate_schema["missing_required_columns"],
    }
    if missing["baseline"] or missing["candidate"]:
        raise ValueError(f"Required columns missing: {missing}")

    baseline_fp = semantic_parquet_fingerprint(
        baseline, columns=columns, batch_size=batch_size
    )
    candidate_fp = semantic_parquet_fingerprint(
        candidate, columns=columns, batch_size=batch_size
    )
    signature = _diff_signature(kind, baseline_fp, candidate_fp)

    identical = (
        baseline_fp["semantic_sha256"] == candidate_fp["semantic_sha256"]
        and baseline_fp["row_count"] == candidate_fp["row_count"]
        and baseline_fp["schema"] == candidate_fp["schema"]
    )
    report: dict[str, Any] = {
        "report_version": 1,
        "kind": kind,
        "baseline": baseline_fp,
        "candidate": candidate_fp,
        "diff_signature": signature,
        "status": "IDENTICAL" if identical else "DIFFERENT",
        "classification": "NO_CHANGE" if identical else "UNCLASSIFIED",
        "differing_batches": [],
        "row_differences": [],
        "row_differences_truncated": False,
    }
    if identical:
        return report

    baseline_pf = pq.ParquetFile(baseline)
    candidate_pf = pq.ParquetFile(candidate)
    baseline_batches = baseline_pf.iter_batches(batch_size=batch_size, columns=columns)
    candidate_batches = candidate_pf.iter_batches(batch_size=batch_size, columns=columns)

    row_offset = 0
    batch_index = 0
    for baseline_batch, candidate_batch in zip_longest(
        baseline_batches, candidate_batches, fillvalue=None
    ):
        baseline_rows_count = len(baseline_batch) if baseline_batch is not None else 0
        candidate_rows_count = len(candidate_batch) if candidate_batch is not None else 0

        baseline_hash = (
            _batch_hash(baseline_batch, columns) if baseline_batch is not None else None
        )
        candidate_hash = (
            _batch_hash(candidate_batch, columns) if candidate_batch is not None else None
        )

        if baseline_hash != candidate_hash:
            report["differing_batches"].append(
                {
                    "batch_index": batch_index,
                    "row_offset": row_offset,
                    "baseline_rows": baseline_rows_count,
                    "candidate_rows": candidate_rows_count,
                    "baseline_hash": baseline_hash,
                    "candidate_hash": candidate_hash,
                }
            )

            if len(report["row_differences"]) < max_row_diffs:
                baseline_rows = (
                    _batch_rows(baseline_batch, columns) if baseline_batch is not None else []
                )
                candidate_rows = (
                    _batch_rows(candidate_batch, columns) if candidate_batch is not None else []
                )
                for local_index, pair in enumerate(
                    zip_longest(baseline_rows, candidate_rows, fillvalue=None)
                ):
                    baseline_row, candidate_row = pair
                    if baseline_row == candidate_row:
                        continue
                    if len(report["row_differences"]) >= max_row_diffs:
                        report["row_differences_truncated"] = True
                        break
                    report["row_differences"].append(
                        _row_difference(
                            row_offset + local_index,
                            baseline_row,
                            candidate_row,
                            columns,
                        )
                    )

        row_offset += max(baseline_rows_count, candidate_rows_count)
        batch_index += 1

    if len(report["row_differences"]) >= max_row_diffs and report["differing_batches"]:
        report["row_differences_truncated"] = True

    return report


def apply_classification(
    report: dict[str, Any], classification: dict[str, Any]
) -> dict[str, Any]:
    """Bind a human classification to exactly one deterministic diff signature."""
    if report["status"] == "IDENTICAL":
        if classification:
            raise ValueError("Cannot classify an IDENTICAL report")
        return report

    expected_signature = report["diff_signature"]
    if classification.get("diff_signature") != expected_signature:
        raise ValueError(
            "Classification diff_signature does not match this report: "
            f"expected {expected_signature}, got {classification.get('diff_signature')}"
        )

    label = classification.get("classification")
    if label not in _VALID_CLASSIFICATIONS:
        raise ValueError(
            f"classification must be one of {sorted(_VALID_CLASSIFICATIONS)}, got {label}"
        )

    if label == "EXPECTED_CHANGE" and not classification.get("decision_ref"):
        raise ValueError("EXPECTED_CHANGE requires a non-empty decision_ref")

    classified = dict(report)
    classified["classification"] = label
    classified["classification_detail"] = {
        "decision_ref": classification.get("decision_ref"),
        "note": classification.get("note", ""),
    }
    return classified


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare baseline and candidate MBO/MBP1 Parquet outputs."
    )
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--kind", required=True, choices=sorted(_KIND_COLUMNS))
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--max-row-diffs", type=int, default=50)
    parser.add_argument("--classification", type=Path, default=None)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument(
        "--allow-unclassified",
        action="store_true",
        help="Return success for exploratory UNCLASSIFIED diffs. Never use in merge gates.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    report = compare_parquet_outputs(
        args.baseline,
        args.candidate,
        kind=args.kind,
        batch_size=args.batch_size,
        max_row_diffs=args.max_row_diffs,
    )

    if args.classification is not None:
        classification = json.loads(args.classification.read_text())
        report = apply_classification(report, classification)

    _write_json_atomic(args.report, report)

    print(f"Differential report: {args.report}")
    print(f"Status: {report['status']}")
    print(f"Classification: {report['classification']}")
    print(f"Diff signature: {report['diff_signature']}")
    print(f"Differing batches: {len(report['differing_batches'])}")
    print(f"Sampled row diffs: {len(report['row_differences'])}")

    if report["classification"] in {"NO_CHANGE", "EXPECTED_CHANGE"}:
        return 0
    if report["classification"] == "UNCLASSIFIED" and args.allow_unclassified:
        return 0
    if report["classification"] == "UNCLASSIFIED":
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
