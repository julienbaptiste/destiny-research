"""Run the code-only R0 safety-net preflight.

This command does not rerun paid-data pipelines. It validates the runtime,
fingerprint detector, canary provenance helpers, differential runner, normative
synthetic fixtures, pre-R0 legacy characterization and existing fast regression
goldens against outputs already present on disk.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _environment_precheck() -> int:
    """Fail fast with a concise diagnostic when the project runtime is wrong."""
    problems: list[str] = []
    if sys.version_info < (3, 11):
        problems.append(
            f"Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} "
            "detected; Destiny Research requires Python >= 3.11"
        )

    missing = [
        module
        for module in ("pyarrow", "duckdb")
        if importlib.util.find_spec(module) is None
    ]
    if missing:
        problems.append(f"missing required modules: {', '.join(missing)}")

    if problems:
        print("\n=== Environment precheck ===")
        print("[FAIL] Invalid Research runtime:")
        for problem in problems:
            print(f"  - {problem}")
        print(f"  interpreter: {sys.executable}")
        return 1

    print("\n=== Environment precheck ===")
    print(
        "[PASS] "
        f"Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} "
        f"| interpreter={sys.executable}"
    )
    return 0


def _run(label: str, cmd: list[str]) -> int:
    print(f"\n=== {label} ===")
    result = subprocess.run(cmd, cwd=_REPO_ROOT)
    return result.returncode


def main() -> int:
    if _environment_precheck() != 0:
        return 1

    suites = [
        (
            "Safety-net code-only self-tests",
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/safety_net/test_fingerprint.py",
                "tests/safety_net/test_canary_helpers.py",
                "tests/safety_net/test_diff_outputs.py",
                "tests/normative/test_normative_fixtures.py",
                "-v",
            ],
        ),
        (
            "Pre-R0 legacy characterization",
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/characterization/test_databento_pre_r0.py",
                "tests/test_hkex_synthetic_cancel.py",
                "tests/characterization/test_hkex_pre_r0.py",
                "tests/characterization/test_hkex_legacy_fixture.py",
                "tests/characterization/test_hkex_reconstruction_pre_r0.py",
                "tests/characterization/test_hkex_postprocess_pre_r0.py",
                "-v",
            ],
        ),
        (
            "Existing normalization/reconstruction goldens",
            [sys.executable, "tests/run_all_checks.py", "--skip-pipeline", "--verbose"],
        ),
    ]

    failures = 0
    for label, cmd in suites:
        code = _run(label, cmd)
        if code != 0:
            failures += 1
            print(f"[FAIL] {label} exited with {code}")
        else:
            print(f"[PASS] {label}")

    if failures:
        print(f"\nR0 preflight FAILED: {failures}/{len(suites)} suite(s) failed.")
        return 1

    print("\nR0 preflight PASSED.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
