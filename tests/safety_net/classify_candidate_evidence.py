"""Apply reviewed R0.1/R0.2 classifications to existing differential evidence.

This replay is intentionally cheap: it reuses the already-generated migration
reports and never reruns ingestion or reconstruction. Each classification is
bound to an exact diff signature, and the registry is bound to the candidate
commit that produced the evidence being reviewed.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
_THIS_DIR = Path(__file__).resolve().parent
_DEFAULT_REGISTRY = _THIS_DIR / "classifications" / "r0_1_r0_2.json"
_DEFAULT_WORK_ROOT = Path("/tmp/destiny_r0_candidate_canary")
_REQUIRED_PRODUCTS = {"ES", "NIY", "FDAX", "FESX", "HSI", "MHI"}
_KINDS = ("mbo", "mbp1")

sys.path.insert(0, str(_THIS_DIR))

from diff_outputs import apply_classification  # noqa: E402


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temp.replace(path)


def _git_head() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=_REPO_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _validate_registry(registry: dict[str, Any]) -> None:
    reviewed_commit = registry.get("reviewed_against_candidate_commit")
    if not isinstance(reviewed_commit, str) or len(reviewed_commit) != 40:
        raise ValueError("Classification registry requires a 40-char reviewed candidate commit")

    classifications = registry.get("classifications")
    if not isinstance(classifications, dict):
        raise ValueError("Classification registry is missing classifications")

    products = set(classifications)
    if products != _REQUIRED_PRODUCTS:
        raise ValueError(
            "Classification registry product set mismatch: "
            f"expected={sorted(_REQUIRED_PRODUCTS)} actual={sorted(products)}"
        )

    for product, by_kind in classifications.items():
        if set(by_kind) != set(_KINDS):
            raise ValueError(f"{product}: expected classifications for {_KINDS}")
        for kind, classification in by_kind.items():
            if classification.get("classification") != "EXPECTED_CHANGE":
                raise ValueError(f"{product}/{kind}: reviewed migration must be EXPECTED_CHANGE")
            signature = classification.get("diff_signature")
            if not isinstance(signature, str) or len(signature) != 64:
                raise ValueError(f"{product}/{kind}: invalid diff signature")
            if not classification.get("decision_ref"):
                raise ValueError(f"{product}/{kind}: missing decision_ref")


def classify_existing_evidence(
    *,
    work_root: Path,
    registry_path: Path,
) -> tuple[dict[str, Any], int]:
    """Classify an existing qualification run without regenerating candidate data."""
    source_summary_path = work_root / "candidate_qualification_summary.json"
    if not source_summary_path.exists():
        raise FileNotFoundError(source_summary_path)

    source_summary = _read_json(source_summary_path)
    registry = _read_json(registry_path)
    _validate_registry(registry)

    evidence_commit = source_summary.get("candidate_commit")
    reviewed_commit = registry["reviewed_against_candidate_commit"]
    if evidence_commit != reviewed_commit:
        raise ValueError(
            "Qualification evidence commit does not match reviewed classification commit: "
            f"evidence={evidence_commit!r} reviewed={reviewed_commit!r}"
        )

    products = source_summary.get("products")
    if set(products or []) != _REQUIRED_PRODUCTS:
        raise ValueError(
            "Existing qualification summary must contain exactly the six reviewed canaries"
        )

    classified_summary: dict[str, Any] = {
        "evidence_candidate_commit": evidence_commit,
        "classification_commit": _git_head(),
        "reviewed_date": registry.get("reviewed_date"),
        "classification_registry": str(registry_path),
        "products": products,
        "work_root": str(work_root),
        "canaries": {},
    }
    failures = 0

    for product in products:
        source_canary = source_summary.get("canaries", {}).get(product, {})
        source_diffs = source_canary.get("diffs", {})
        product_result: dict[str, Any] = {"diffs": {}}
        product_ok = True

        for kind in _KINDS:
            source_diff = source_diffs.get(kind)
            if not isinstance(source_diff, dict):
                raise ValueError(f"{product}/{kind}: missing source differential summary")

            report_path = Path(source_diff.get("report", ""))
            if not report_path.exists():
                raise FileNotFoundError(report_path)
            report = _read_json(report_path)

            classification = registry["classifications"][product][kind]
            classified = apply_classification(report, classification)
            classified_path = report_path.with_name(
                report_path.stem + "_classified" + report_path.suffix
            )
            _write_json_atomic(classified_path, classified)

            label = classified["classification"]
            if label not in {"NO_CHANGE", "EXPECTED_CHANGE"}:
                product_ok = False
                failures += 1

            product_result["diffs"][kind] = {
                "status": classified["status"],
                "classification": label,
                "diff_signature": classified["diff_signature"],
                "classification_detail": classified.get("classification_detail"),
                "report": str(classified_path),
            }

        product_result["status"] = "PASS_CLASSIFIED" if product_ok else "CLASSIFICATION_FAILED"
        classified_summary["canaries"][product] = product_result

    return classified_summary, failures


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply reviewed classifications to existing R0.1/R0.2 canary diffs."
    )
    parser.add_argument("--work-root", type=Path, default=_DEFAULT_WORK_ROOT)
    parser.add_argument(
        "--classification-registry",
        type=Path,
        default=_DEFAULT_REGISTRY,
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        summary, failures = classify_existing_evidence(
            work_root=args.work_root,
            registry_path=args.classification_registry,
        )
    except Exception as exc:
        print(f"Classification replay FAILED: {exc}")
        return 1

    output = args.work_root / "candidate_qualification_classified_summary.json"
    _write_json_atomic(output, summary)

    for product in summary["products"]:
        result = summary["canaries"][product]
        labels = ", ".join(
            f"{kind.upper()}={result['diffs'][kind]['classification']}"
            for kind in _KINDS
        )
        print(f"{product}: {result['status']} | {labels}")

    print(f"\nClassified qualification summary: {output}")
    if failures:
        print(f"Classification replay FAILED: {failures} reviewed diff(s) are blocking.")
        return 1

    print("R0.1/R0.2 reviewed migration differentials PASSED.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
