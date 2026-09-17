"""Bounded-memory semantic diff for baseline-vs-candidate Parquet outputs.

The migration runner must compare a legacy baseline against a candidate whose
schema can intentionally evolve. Each side therefore receives a full logical
fingerprint, while row diagnostics operate on the canonical columns available
on both sides. Candidate omission of a required canonical column is an error;
baseline omissions are reported as schema evolution rather than rejected.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from itertools import zip_longest
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "tests" / "regression"))

from ingestion.schema import NORMALIZED_MBO_SCHEMA  # noqa: E402
from reconstruction.build_mbp1 import MBP1_SCHEMA  # noqa: E402
from shared.fingerprint import DEFAULT_BATCH_SIZE, semantic_parquet_fingerprint  # noqa: E402


_KIND_COLUMNS = {
    "mbo": list(NORMALIZED_MBO_SCHEMA.names),
    "mbp1": list(MBP1_SCHEMA.names),
}
_VALID_CLASSIFICATIONS = {"EXPECTED_CHANGE", "UNEXPECTED_REGRESSION"}


def _batch_hash(batch: pa.RecordBatch, columns: list[str]) -> str:
    h = hashlib.sha256()
    h.update(f"rows={len(batch)}\n".encode("ascii"))
    for index, name in enumerate(columns):
        array = batch.column(index)
        if pa.types.is_dictionary(array.type):
            array = array.cast(pa.string())
        values = array.to_pylist()
        h.update(name.encode("utf-8"))
        h.update(b"\0")
        h.update(repr(values).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _batch_rows(batch: pa.RecordBatch, columns: list[str]) -> list[dict[str, Any]]:
    arrays = []
    for index in range(len(columns)):
        array = batch.column(index)
        if pa.types.is_dictionary(array.type):
            array = array.cast(pa.string())
        arrays.append(array)
    return pa.Table.from_arrays(arrays, names=columns).to_pylist()


def _target_columns(kind: str) -> list[str]:
    try:
        return _KIND_COLUMNS[kind]
    except KeyError as exc:
        raise ValueError(f"Unsupported diff kind: {kind}") from exc


def _schema_evolution(
    baseline: Path,
    candidate: Path,
    kind: str,
) -> tuple[list[str], dict[str, Any]]:
    target = _target_columns(kind)
    baseline_schema = pq.ParquetFile(baseline).schema_arrow
    candidate_schema = pq.ParquetFile(candidate).schema_arrow
    baseline_names = list(baseline_schema.names)
    candidate_names = list(candidate_schema.names)

    candidate_missing = [name for name in target if name not in candidate_names]
    if candidate_missing:
        raise ValueError(
            f"Candidate is missing canonical {kind} columns: {candidate_missing}"
        )

    common = [
        name for name in target if name in baseline_names and name in candidate_names
    ]
    if not common:
        raise ValueError("No common canonical columns available for row diagnostics")

    report = {
        "baseline_schema": str(baseline_schema),
        "candidate_schema": str(candidate_schema),
        "baseline_missing_target_columns": [
            name for name in target if name not in baseline_names
        ],
        "candidate_missing_target_columns": candidate_missing,
        "candidate_added_columns": [
            name for name in candidate_names if name not in baseline_names
        ],
        "baseline_only_columns": [
            name for name in baseline_names if name not in candidate_names
        ],
        "comparison_columns": common,
    }
    return common, report


def _diff_signature(
    kind: str,
    baseline_fp: dict[str, object],
    candidate_fp: dict[str, object],
) -> str:
    payload = {
        "version": 2,
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
    """Compare legacy and candidate MBO/MBP1 outputs with bounded memory."""
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

    comparison_columns, schema_evolution = _schema_evolution(
        baseline, candidate, kind
    )

    # Full-file fingerprints intentionally use every column available on each
    # side. Schema additions therefore affect the diff signature even when all
    # legacy/common field values remain identical.
    baseline_fp = semantic_parquet_fingerprint(baseline, batch_size=batch_size)
    candidate_fp = semantic_parquet_fingerprint(candidate, batch_size=batch_size)
    signature = _diff_signature(kind, baseline_fp, candidate_fp)

    identical = (
        baseline_fp["semantic_sha256"] == candidate_fp["semantic_sha256"]
        and baseline_fp["row_count"] == candidate_fp["row_count"]
        and baseline_fp["schema"] == candidate_fp["schema"]
    )

    report: dict[str, Any] = {
        "report_version": 2,
        "kind": kind,
        "baseline": baseline_fp,
        "candidate": candidate_fp,
        "schema_evolution": schema_evolution,
        "diff_signature": signature,
        "status": "IDENTICAL" if identical else "DIFFERENT",
        "classification": "NO_CHANGE" if identical else "UNCLASSIFIED",
        "differing_batches": [],
        "row_differences": [],
        "row_differences_truncated": False,
    }
    if identical:
        return report

    baseline_batches = pq.ParquetFile(baseline).iter_batches(
        batch_size=batch_size,
        columns=comparison_columns,
    )
    candidate_batches = pq.ParquetFile(candidate).iter_batches(
        batch_size=batch_size,
        columns=comparison_columns,
    )

    row_offset = 0
    batch_index = 0
    for baseline_batch, candidate_batch in zip_longest(
        baseline_batches,
        candidate_batches,
        fillvalue=None,
    ):
        baseline_count = len(baseline_batch) if baseline_batch is not None else 0
        candidate_count = len(candidate_batch) if candidate_batch is not None else 0
        baseline_hash = (
            _batch_hash(baseline_batch, comparison_columns)
            if baseline_batch is not None
            else None
        )
        candidate_hash = (
            _batch_hash(candidate_batch, comparison_columns)
            if candidate_batch is not None
            else None
        )

        if baseline_hash != candidate_hash:
            report["differing_batches"].append(
                {
                    "batch_index": batch_index,
                    "row_offset": row_offset,
                    "baseline_rows": baseline_count,
                    "candidate_rows": candidate_count,
                    "baseline_hash": baseline_hash,
                    "candidate_hash": candidate_hash,
                }
            )

            if len(report["row_differences"]) < max_row_diffs:
                baseline_rows = (
                    _batch_rows(baseline_batch, comparison_columns)
                    if baseline_batch is not None
                    else []
                )
                candidate_rows = (
                    _batch_rows(candidate_batch, comparison_columns)
                    if candidate_batch is not None
                    else []
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
                            comparison_columns,
                        )
                    )

        row_offset += max(baseline_count, candidate_count)
        batch_index += 1

    if len(report["row_differences"]) >= max_row_diffs and report["differing_batches"]:
        report["row_differences_truncated"] = True

    return report


def apply_classification(
    report: dict[str, Any],
    classification: dict[str, Any],
) -> dict[str, Any]:
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
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temp.replace(path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare baseline and candidate MBO/MBP1 Parquet outputs."
    )
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--kind", required=True, choices=sorted(_KIND_COLUMNS))
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--max-row-diffs", type=int, default=50)
    parser.add_argument("--classification", type=Path)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--allow-unclassified", action="store_true")
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
        report = apply_classification(
            report,
            json.loads(args.classification.read_text()),
        )
    _write_json_atomic(args.report, report)

    print(f"Differential report: {args.report}")
    print(f"Status: {report['status']}")
    print(f"Classification: {report['classification']}")
    print(f"Diff signature: {report['diff_signature']}")
    print(f"Differing batches: {len(report['differing_batches'])}")
    print(f"Sampled row diffs: {len(report['row_differences'])}")
    print(
        "Schema additions: "
        f"{report['schema_evolution']['candidate_added_columns']}"
    )

    if report["classification"] in {"NO_CHANGE", "EXPECTED_CHANGE"}:
        return 0
    if report["classification"] == "UNCLASSIFIED" and args.allow_unclassified:
        return 0
    if report["classification"] == "UNCLASSIFIED":
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
