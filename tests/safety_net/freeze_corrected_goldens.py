"""Freeze corrected R0.1/R0.2 goldens from qualified isolated canaries.

The freezer never reruns ingestion or reconstruction. It accepts only the six
classified canaries, re-fingerprints every source Parquet file against the
reviewed evidence, and writes canonical goldens in a namespace separate from
the immutable pre-R0 legacy metrics.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from pathlib import Path
import sys
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

_REPO_ROOT = Path(__file__).resolve().parents[2]
_THIS_DIR = Path(__file__).resolve().parent
_REGRESSION_DIR = _REPO_ROOT / "tests" / "regression"
_DEFAULT_WORK_ROOT = Path("/tmp/destiny_r0_candidate_canary")
_DEFAULT_REGISTRY = _THIS_DIR / "classifications" / "r0_1_r0_2.json"
_DEFAULT_OUTPUT_ROOT = _REGRESSION_DIR / "corrected" / "r0_1_r0_2"
_REQUIRED_PRODUCTS = ("ES", "NIY", "FDAX", "FESX", "HSI", "MHI")
_GOLDEN_NAMESPACE = "corrected/r0_1_r0_2"
_SAMPLE_ROWS = 50_000

sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_THIS_DIR))
sys.path.insert(0, str(_REGRESSION_DIR))

from cases import NORMALIZATION_CASES  # noqa: E402
from classify_candidate_evidence import _validate_registry  # noqa: E402
from ingestion.market_config import MARKET_CONFIG  # noqa: E402
from ingestion.schema import normalized_path  # noqa: E402
from shared.fingerprint import semantic_parquet_fingerprint  # noqa: E402
from shared.metrics_mbo import (  # noqa: E402
    CHECKSUM_COLS as MBO_CHECKSUM_COLS,
    sha256_table_sample as mbo_sample_checksum,
)
from shared.metrics_mbp1 import (  # noqa: E402
    CHECKSUM_COLS as MBP1_CHECKSUM_COLS,
    sha256_table_sample as mbp1_sample_checksum,
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _json_text(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(_json_text(payload))
    temp.replace(path)


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(_REPO_ROOT))
    except ValueError:
        return str(path)


def _candidate_paths(work_root: Path, product: str) -> tuple[Path, Path, Path]:
    """Resolve isolated candidate MBO/rejected/MBP1 paths for one canary."""
    contract, date_str = NORMALIZATION_CASES[product]
    session_date = date.fromisoformat(date_str)
    cfg = MARKET_CONFIG[product]
    compact = date_str.replace("-", "")
    work_dir = work_root / f"{product}_{date_str}"

    mbo = normalized_path(
        work_dir / "normalized",
        cfg["provider"],
        cfg["exchange"],
        product,
        contract,
        session_date.year,
        session_date.month,
        compact,
    )
    rejected = mbo.with_name(f"{contract}_{compact}_rejected.parquet")
    mbp1 = work_dir / "reconstructed" / f"{contract}_{compact}_mbp1.parquet"
    return mbo, rejected, mbp1


def _assert_checksum_contract() -> None:
    """Fail before freezing if canonical ordering/provenance leaves the checksum."""
    required_mbo = {"norm_flags", "sequence", "subsequence"}
    missing_mbo = required_mbo - set(MBO_CHECKSUM_COLS)
    if missing_mbo:
        raise ValueError(f"MBO checksum contract missing fields: {sorted(missing_mbo)}")

    required_mbp1 = {"sequence", "subsequence"}
    missing_mbp1 = required_mbp1 - set(MBP1_CHECKSUM_COLS)
    if missing_mbp1:
        raise ValueError(f"MBP1 checksum contract missing fields: {sorted(missing_mbp1)}")


def _assert_fingerprint(path: Path, expected: dict[str, Any], label: str) -> dict[str, Any]:
    """Recompute a full bounded-memory fingerprint and require exact evidence match."""
    actual = semantic_parquet_fingerprint(path)
    keys = (
        "semantic_sha256",
        "file_sha256",
        "row_count",
        "rows_hashed",
        "columns",
        "batch_size",
        "schema",
        "file_size_bytes",
    )
    for key in keys:
        if actual.get(key) != expected.get(key):
            raise ValueError(
                f"{label}: qualified fingerprint mismatch for {key}: "
                f"actual={actual.get(key)!r} expected={expected.get(key)!r}"
            )
    return actual


def _sample_checksum(path: Path, columns: list[str], *, kind: str) -> tuple[str, int]:
    """Hash the first 50k canonical rows without materializing a trading day."""
    parquet = pq.ParquetFile(path)
    missing = [name for name in columns if name not in parquet.schema_arrow.names]
    if missing:
        raise ValueError(f"{kind}: source is missing corrected checksum columns: {missing}")

    batches: list[pa.RecordBatch] = []
    rows = 0
    for batch in parquet.iter_batches(batch_size=_SAMPLE_ROWS, columns=columns):
        take = min(_SAMPLE_ROWS - rows, len(batch))
        batches.append(batch.slice(0, take))
        rows += take
        if rows >= _SAMPLE_ROWS:
            break

    if not batches:
        return hashlib.sha256(b"").hexdigest(), 0

    table = pa.Table.from_batches(batches)
    checksum_fn = mbo_sample_checksum if kind == "mbo" else mbp1_sample_checksum
    return checksum_fn(table, _SAMPLE_ROWS, columns), len(table)


def _golden_payload(
    *,
    product: str,
    contract: str,
    date_str: str,
    kind: str,
    evidence_commit: str,
    diff_report: dict[str, Any],
    fingerprint: dict[str, Any],
    sample_checksum: str,
    sample_rows: int,
    checksum_columns: list[str],
    canary_report: dict[str, Any],
    rejected_fingerprint: dict[str, Any] | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "format_version": 1,
        "golden_namespace": _GOLDEN_NAMESPACE,
        "kind": kind,
        "product": product,
        "contract": contract,
        "date": date_str,
        "evidence_candidate_commit": evidence_commit,
        "diff_signature": diff_report["diff_signature"],
        "classification": diff_report["classification"],
        "decision_ref": diff_report.get("classification_detail", {}).get("decision_ref"),
        "checksum_columns": list(checksum_columns),
        "sample_rows_used": sample_rows,
        "sample_checksum_sha256": sample_checksum,
        "full_fingerprint": fingerprint,
    }

    if kind == "mbo":
        payload["rejected_fingerprint"] = rejected_fingerprint
        payload["validator_stats"] = canary_report.get("validator_stats")
        payload["reject_reason_distribution"] = canary_report.get(
            "reject_reason_distribution", {}
        )
    else:
        payload["reconstruction_stats"] = canary_report.get("reconstruction_stats")

    return payload


def _target_path(root: Path, kind: str, product: str, contract: str, date_str: str) -> Path:
    subdir = "normalization" if kind == "mbo" else "reconstruction"
    return root / subdir / f"{product}_{contract}_{date_str}_golden.json"


def _prepare_freeze(
    *,
    work_root: Path,
    registry_path: Path,
    output_root: Path,
) -> tuple[list[tuple[Path, dict[str, Any]]], dict[str, Any]]:
    """Validate all evidence before constructing the corrected golden set."""
    _assert_checksum_contract()

    summary_path = work_root / "candidate_qualification_classified_summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(summary_path)

    summary = _read_json(summary_path)
    registry = _read_json(registry_path)
    _validate_registry(registry)

    evidence_commit = summary.get("evidence_candidate_commit")
    if evidence_commit != registry["reviewed_against_candidate_commit"]:
        raise ValueError(
            "Classified summary commit does not match reviewed registry: "
            f"summary={evidence_commit!r} registry={registry['reviewed_against_candidate_commit']!r}"
        )

    products = summary.get("products")
    if tuple(products or ()) != _REQUIRED_PRODUCTS:
        raise ValueError(
            "Corrected golden freeze requires the ordered six-canary set: "
            f"expected={_REQUIRED_PRODUCTS} actual={products!r}"
        )

    payloads: list[tuple[Path, dict[str, Any]]] = []
    manifest_entries: dict[str, Any] = {}

    for product in _REQUIRED_PRODUCTS:
        product_summary = summary.get("canaries", {}).get(product, {})
        if product_summary.get("status") != "PASS_CLASSIFIED":
            raise ValueError(f"{product}: classified canary is not PASS_CLASSIFIED")

        contract, date_str = NORMALIZATION_CASES[product]
        mbo_path, rejected_path, mbp1_path = _candidate_paths(work_root, product)
        canary_report_path = work_root / f"{product}_{date_str}" / "canary_report.json"
        canary_report = _read_json(canary_report_path)
        if canary_report.get("checkout_commit") != evidence_commit:
            raise ValueError(f"{product}: canary checkout commit does not match evidence")

        reported_rejected = canary_report.get("rejected")
        if reported_rejected is None:
            if rejected_path.exists():
                raise ValueError(f"{product}: rejected file exists but canary reported none")
            rejected_fingerprint = None
        else:
            if not rejected_path.exists():
                raise FileNotFoundError(rejected_path)
            rejected_fingerprint = _assert_fingerprint(
                rejected_path,
                reported_rejected,
                f"{product}/rejected",
            )

        manifest_entries[product] = {}
        for kind, source_path, checksum_columns in (
            ("mbo", mbo_path, MBO_CHECKSUM_COLS),
            ("mbp1", mbp1_path, MBP1_CHECKSUM_COLS),
        ):
            diff_summary = product_summary.get("diffs", {}).get(kind, {})
            registry_entry = registry["classifications"][product][kind]
            if diff_summary.get("classification") != "EXPECTED_CHANGE":
                raise ValueError(f"{product}/{kind}: classification is not EXPECTED_CHANGE")
            if diff_summary.get("diff_signature") != registry_entry["diff_signature"]:
                raise ValueError(f"{product}/{kind}: classified summary signature is stale")

            report_path = Path(diff_summary.get("report", ""))
            if not report_path.exists():
                raise FileNotFoundError(report_path)
            report = _read_json(report_path)
            if report.get("classification") != "EXPECTED_CHANGE":
                raise ValueError(f"{product}/{kind}: detailed report is not EXPECTED_CHANGE")
            if report.get("diff_signature") != registry_entry["diff_signature"]:
                raise ValueError(f"{product}/{kind}: detailed report signature is stale")

            fingerprint = _assert_fingerprint(
                source_path,
                report["candidate"],
                f"{product}/{kind}",
            )
            checksum, sample_rows = _sample_checksum(
                source_path,
                list(checksum_columns),
                kind=kind,
            )
            payload = _golden_payload(
                product=product,
                contract=contract,
                date_str=date_str,
                kind=kind,
                evidence_commit=evidence_commit,
                diff_report=report,
                fingerprint=fingerprint,
                sample_checksum=checksum,
                sample_rows=sample_rows,
                checksum_columns=list(checksum_columns),
                canary_report=canary_report,
                rejected_fingerprint=rejected_fingerprint if kind == "mbo" else None,
            )
            target = _target_path(output_root, kind, product, contract, date_str)
            payloads.append((target, payload))
            manifest_entries[product][kind] = {
                "golden": _display_path(target),
                "diff_signature": report["diff_signature"],
                "decision_ref": payload["decision_ref"],
                "semantic_sha256": fingerprint["semantic_sha256"],
                "sample_checksum_sha256": checksum,
                "row_count": fingerprint["row_count"],
            }

    manifest = {
        "format_version": 1,
        "golden_namespace": _GOLDEN_NAMESPACE,
        "evidence_candidate_commit": evidence_commit,
        "reviewed_date": registry.get("reviewed_date"),
        "classification_registry": _display_path(registry_path),
        "products": list(_REQUIRED_PRODUCTS),
        "normalization_checksum_columns": list(MBO_CHECKSUM_COLS),
        "reconstruction_checksum_columns": list(MBP1_CHECKSUM_COLS),
        "entries": manifest_entries,
    }
    return payloads, manifest


def _preflight_targets(
    targets: list[tuple[Path, dict[str, Any]]],
    *,
    overwrite: bool,
) -> None:
    """Reject divergent existing goldens before writing any output."""
    for path, payload in targets:
        if not path.exists() or path.read_text() == _json_text(payload):
            continue
        if not overwrite:
            raise FileExistsError(
                f"Refusing to overwrite divergent corrected golden {path}. "
                "Use --overwrite only after a newly reviewed qualification."
            )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Freeze R0.1/R0.2 corrected goldens from classified canaries."
    )
    parser.add_argument("--work-root", type=Path, default=_DEFAULT_WORK_ROOT)
    parser.add_argument("--classification-registry", type=Path, default=_DEFAULT_REGISTRY)
    parser.add_argument("--output-root", type=Path, default=_DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace divergent corrected goldens only after a new reviewed qualification.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        payloads, manifest = _prepare_freeze(
            work_root=args.work_root,
            registry_path=args.classification_registry,
            output_root=args.output_root,
        )
        targets = payloads + [(args.output_root / "manifest.json", manifest)]
        _preflight_targets(targets, overwrite=args.overwrite)
        for path, payload in targets:
            _write_json_atomic(path, payload)
    except Exception as exc:
        print(f"Corrected golden freeze FAILED: {exc}")
        return 1

    print(f"Corrected golden namespace: {args.output_root}")
    for path, payload in payloads:
        print(
            f"[FROZEN] {_display_path(path)} | rows={payload['full_fingerprint']['row_count']} "
            f"| signature={payload['diff_signature'][:12]}..."
        )
    print(f"[FROZEN] {_display_path(args.output_root / 'manifest.json')}")
    print("R0.1/R0.2 corrected golden freeze PASSED.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
