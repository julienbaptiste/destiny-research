"""Run consolidated R0.1/R0.2 candidate qualification.

Workflow:
    1. execute the fast code-only candidate preflight;
    2. rerun selected real-data canaries in isolated workspaces;
    3. generate schema-aware legacy-vs-candidate MBO/MBP1 differential reports.

Diffs intentionally remain UNCLASSIFIED. This command generates evidence; it
never decides that a production change is expected on the user's behalf.
"""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import subprocess
import sys

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "tests" / "regression"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from ingestion.market_config import MARKET_CONFIG  # noqa: E402
from ingestion.schema import normalized_path  # noqa: E402
from shared.metrics_mbo import mbo_path as reference_mbo_path  # noqa: E402
from shared.metrics_mbp1 import mbp1_path as reference_mbp1_path  # noqa: E402
from cases import NORMALIZATION_CASES  # noqa: E402
from diff_outputs import compare_parquet_outputs  # noqa: E402


_PRIMARY_PRODUCTS = ["ES", "FDAX", "HSI"]
_ALL_REQUIRED_PRODUCTS = ["ES", "NIY", "FDAX", "FESX", "HSI", "MHI"]


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temp.replace(path)


def _run(command: list[str]) -> int:
    print("\n$ " + " ".join(command), flush=True)
    return subprocess.run(command, cwd=_REPO_ROOT).returncode


def _candidate_paths(work_root: Path, product: str) -> tuple[Path, Path]:
    contract, date_str = NORMALIZATION_CASES[product]
    session_date = date.fromisoformat(date_str)
    cfg = MARKET_CONFIG[product]
    provider = cfg["provider"]
    venue = cfg["exchange"]
    compact = date_str.replace("-", "")
    work_dir = work_root / f"{product}_{date_str}"

    mbo = normalized_path(
        work_dir / "normalized",
        provider,
        venue,
        product,
        contract,
        session_date.year,
        session_date.month,
        compact,
    )
    mbp1 = work_dir / "reconstructed" / f"{contract}_{compact}_mbp1.parquet"
    return mbo, mbp1


def _generate_diffs(work_root: Path, product: str) -> dict[str, object]:
    contract, date_str = NORMALIZATION_CASES[product]
    candidate_mbo, candidate_mbp1 = _candidate_paths(work_root, product)
    baseline_mbo = reference_mbo_path(product, contract, date_str)
    baseline_mbp1 = reference_mbp1_path(product, contract, date_str)
    work_dir = work_root / f"{product}_{date_str}"

    result: dict[str, object] = {}
    for kind, baseline, candidate in (
        ("mbo", baseline_mbo, candidate_mbo),
        ("mbp1", baseline_mbp1, candidate_mbp1),
    ):
        if not baseline.exists():
            result[kind] = {
                "status": "NO_REFERENCE",
                "reference": str(baseline),
            }
            continue
        if not candidate.exists():
            result[kind] = {
                "status": "NO_CANDIDATE",
                "candidate": str(candidate),
            }
            continue

        report = compare_parquet_outputs(
            baseline,
            candidate,
            kind=kind,
            max_row_diffs=50,
        )
        report_path = work_dir / f"migration_diff_{kind}.json"
        _write_json(report_path, report)
        result[kind] = {
            "status": report["status"],
            "classification": report["classification"],
            "diff_signature": report["diff_signature"],
            "differing_batches": len(report["differing_batches"]),
            "sampled_row_diffs": len(report["row_differences"]),
            "schema_evolution": report["schema_evolution"],
            "report": str(report_path),
        }
    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run R0.1/R0.2 preflight, canaries and migration differentials."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--products",
        nargs="+",
        choices=sorted(NORMALIZATION_CASES),
        help="Explicit canary products. Default: ES FDAX HSI.",
    )
    group.add_argument(
        "--all-canaries",
        action="store_true",
        help="Run the six qualified provider-family canaries.",
    )
    parser.add_argument(
        "--work-root",
        type=Path,
        default=Path("/tmp/destiny_r0_candidate_canary"),
    )
    parser.add_argument("--skip-preflight", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    products = (
        _ALL_REQUIRED_PRODUCTS
        if args.all_canaries
        else args.products or _PRIMARY_PRODUCTS
    )

    if not args.skip_preflight:
        code = _run([sys.executable, "tests/safety_net/run_candidate_preflight.py"])
        if code != 0:
            print("\nCandidate preflight failed; real-data canaries were not started.")
            return code

    summary: dict[str, object] = {
        "products": products,
        "work_root": str(args.work_root),
        "canaries": {},
    }
    failures = 0

    for product in products:
        code = _run(
            [
                sys.executable,
                "tests/safety_net/rerun_canary.py",
                "--product",
                product,
                "--work-root",
                str(args.work_root),
            ]
        )
        if code != 0:
            failures += 1
            summary["canaries"][product] = {"status": "CANARY_FAILED", "exit_code": code}
            continue

        try:
            diffs = _generate_diffs(args.work_root, product)
        except Exception as exc:  # preserve other canaries while surfacing diagnostic failure
            failures += 1
            summary["canaries"][product] = {
                "status": "DIFF_FAILED",
                "error": repr(exc),
            }
            continue

        summary["canaries"][product] = {
            "status": "PASS_WITH_UNCLASSIFIED_DIFFS",
            "diffs": diffs,
        }
        print(f"\n=== {product} migration differential ===")
        for kind, result in diffs.items():
            print(
                f"{kind.upper()}: status={result.get('status')} "
                f"classification={result.get('classification')} "
                f"signature={result.get('diff_signature')}"
            )

    summary_path = args.work_root / "candidate_qualification_summary.json"
    _write_json(summary_path, summary)
    print(f"\nQualification summary: {summary_path}")

    if failures:
        print(f"Candidate qualification encountered {failures} execution failure(s).")
        return 1

    print("Candidate qualification execution PASSED.")
    print("Migration diffs remain UNCLASSIFIED until reviewed against ADR-003/ADR-004.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
