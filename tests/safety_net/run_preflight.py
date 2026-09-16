"""Run the code-only R0 safety-net preflight.

This command does not rerun paid-data pipelines. It validates the fingerprint
detector, legacy HKEX characterization and existing fast regression goldens
against outputs already present on disk.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _run(label: str, cmd: list[str]) -> int:
    print(f"\n=== {label} ===")
    result = subprocess.run(cmd, cwd=_REPO_ROOT)
    return result.returncode


def main() -> int:
    suites = [
        (
            "Safety-net detector self-tests",
            [sys.executable, "-m", "pytest", "tests/safety_net/test_fingerprint.py", "-v"],
        ),
        (
            "HKEX pre-R0 characterization",
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/test_hkex_synthetic_cancel.py",
                "tests/characterization/test_hkex_pre_r0.py",
                "tests/characterization/test_hkex_legacy_fixture.py",
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
