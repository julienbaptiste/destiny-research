"""Capture deep local fingerprints for the pre-R0 data pipeline.

This script never modifies market-data outputs. It reads the existing
normalized/reconstructed Parquet files and stores deterministic fingerprints
in a local JSON manifest. Vendor data itself is never copied into the repo.

Typical usage:
    python tests/safety_net/capture_baseline.py \
        --output /tmp/destiny_pre_r0_manifest.json
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "tests" / "regression"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from shared.fingerprint import semantic_parquet_fingerprint  # noqa: E402
from shared.metrics_mbo import mbo_path, rejected_path        # noqa: E402
from shared.metrics_mbp1 import mbp1_path                    # noqa: E402
from cases import NORMALIZATION_CASES, RECONSTRUCTION_CASES  # noqa: E402


BASELINE_COMMIT = "69ef65ca3c17df63a72e8fef371140b5b7bc0db0"


def _package_version(name: str) -> str | None:
    """Return an installed package version without making it mandatory."""
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def _git_head() -> str | None:
    """Return the current checkout SHA when git metadata is available."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=_REPO_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _capture(path: Path) -> dict[str, object]:
    """Fingerprint one Parquet output or record it as missing."""
    if not path.exists():
        return {
            "exists": False,
            "logical_name": path.name,
        }

    fp = semantic_parquet_fingerprint(path)
    fp["exists"] = True
    fp["logical_name"] = path.name
    return fp


def _capture_normalization() -> dict[str, object]:
    results: dict[str, object] = {}
    for product, (contract, date_str) in NORMALIZATION_CASES.items():
        results[product] = {
            "contract": contract,
            "date": date_str,
            "mbo": _capture(mbo_path(product, contract, date_str)),
            "rejected": _capture(rejected_path(product, contract, date_str)),
        }
    return results


def _capture_reconstruction() -> dict[str, object]:
    results: dict[str, object] = {}
    for product, (contract, date_str) in RECONSTRUCTION_CASES.items():
        results[product] = {
            "contract": contract,
            "date": date_str,
            "mbp1": _capture(mbp1_path(product, contract, date_str)),
        }
    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture deep pre-R0 fingerprints without modifying data outputs."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("/tmp/destiny_pre_r0_manifest.json"),
        help="Manifest path (default: /tmp/destiny_pre_r0_manifest.json).",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    manifest = {
        "manifest_version": 1,
        "baseline_name": "pre-r0",
        "baseline_commit": BASELINE_COMMIT,
        "captured_from_checkout": _git_head(),
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "runtime": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "pyarrow": _package_version("pyarrow"),
            "duckdb": _package_version("duckdb"),
            "pandas": _package_version("pandas"),
            "databento": _package_version("databento"),
        },
        "normalization": _capture_normalization(),
        "reconstruction": _capture_reconstruction(),
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = args.output.with_suffix(args.output.suffix + ".tmp")
    tmp_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    tmp_path.replace(args.output)

    missing = 0
    checked = 0
    for section in ("normalization", "reconstruction"):
        for payload in manifest[section].values():
            for key, value in payload.items():
                if key in {"contract", "date"}:
                    continue
                checked += 1
                if not value["exists"]:
                    missing += 1

    print(f"Baseline manifest written: {args.output}")
    print(f"Files checked: {checked} | missing: {missing}")
    print(f"Baseline code SHA: {BASELINE_COMMIT}")
    return 0 if missing == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
