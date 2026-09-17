"""Run the code-only R0.1/R0.2 migration candidate preflight.

This runner intentionally excludes pre-R0 characterization tests and historical
goldens whose purpose is to describe the legacy implementation. The candidate
merge gate is the canonical schema contract + all normative fixtures + safety-net
self-tests. Real-data canaries and baseline differentials are run only after this
fast code-only gate is green.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _environment_precheck() -> int:
    problems: list[str] = []
    if sys.version_info < (3, 11):
        problems.append(
            f"Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} "
            "detected; Destiny Research requires Python >= 3.11"
        )

    missing = [
        module
        for module in ("pyarrow", "duckdb", "sortedcontainers")
        if importlib.util.find_spec(module) is None
    ]
    if missing:
        problems.append(f"missing required modules: {', '.join(missing)}")

    print("\n=== Candidate environment precheck ===")
    if problems:
        print("[FAIL] Invalid Research runtime:")
        for problem in problems:
            print(f"  - {problem}")
        print(f"  interpreter: {sys.executable}")
        return 1

    print(
        "[PASS] "
        f"Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} "
        f"| interpreter={sys.executable}"
    )
    return 0


def _run(label: str, command: list[str]) -> int:
    print(f"\n=== {label} ===")
    return subprocess.run(command, cwd=_REPO_ROOT).returncode


def main() -> int:
    if _environment_precheck() != 0:
        return 1

    suites = [
        (
            "Compile changed Python surfaces",
            [
                sys.executable,
                "-m",
                "compileall",
                "-q",
                "ingestion",
                "reconstruction",
                "tests/safety_net",
                "tests/normative",
            ],
        ),
        (
            "Safety-net detector and failure-atomicity self-tests",
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/safety_net/test_fingerprint.py",
                "tests/safety_net/test_canary_helpers.py",
                "tests/safety_net/test_diff_outputs.py",
                "tests/safety_net/test_ingest_atomicity.py",
                "-v",
            ],
        ),
        (
            "Canonical schema and normative fixture matrix",
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/normative/test_canonical_schema.py",
                "tests/normative/test_hkex_candidate_semantics.py",
                "tests/normative/test_normative_fixtures.py",
                "-v",
            ],
        ),
    ]

    failures = 0
    for label, command in suites:
        code = _run(label, command)
        if code:
            failures += 1
            print(f"[FAIL] {label} exited with {code}")
        else:
            print(f"[PASS] {label}")

    if failures:
        print(f"\nR0.1/R0.2 candidate preflight FAILED: {failures}/{len(suites)} suite(s).")
        return 1

    print("\nR0.1/R0.2 candidate preflight PASSED.")
    print("Next gate: targeted real-data canaries + schema-aware baseline differentials.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
