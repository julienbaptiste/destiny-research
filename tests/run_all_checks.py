"""
tests/run_all_checks.py — Unified regression check runner.

Runs both normalization and reconstruction regression checks in sequence
and prints a consolidated summary. Designed to be run after any commit
that touches the ingestion or reconstruction pipelines.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WHEN TO RUN
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  After ANY commit that touches:
    ingestion/         → run --suite normalization (or both)
    reconstruction/    → run --suite reconstruction (or both)
    ingestion/schema.py, ingestion/market_config.py → run --suite both

  Typical workflow:
    1. Make your changes
    2. git add + git commit
    3. python tests/run_all_checks.py --skip-pipeline
    4. If FAILED: investigate, fix, re-commit

  Deep check (re-runs pipeline, ~10 min):
    python tests/run_all_checks.py  (without --skip-pipeline)
    → Use this when you want to verify the pipeline itself, not just
      the outputs already on disk.

  Single suite:
    python tests/run_all_checks.py --suite normalization --skip-pipeline
    python tests/run_all_checks.py --suite reconstruction --skip-pipeline

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXIT CODES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    0 — all suites passed
    1 — one or more regressions detected
    2 — setup error (missing golden files, pipeline failed, file not found)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
USAGE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Recommended — fast, uses outputs already on disk (~30s)
    python tests/run_all_checks.py --skip-pipeline

    # Deep check — re-runs pipelines before comparing (~10 min)
    python tests/run_all_checks.py

    # Single suite only
    python tests/run_all_checks.py --suite normalization --skip-pipeline
    python tests/run_all_checks.py --suite reconstruction --skip-pipeline

    # Verbose — print all metric comparisons, not just failures
    python tests/run_all_checks.py --skip-pipeline --verbose
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Repo root
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT))

from config import REPO_ROOT  # noqa: E402

# ---------------------------------------------------------------------------
# ANSI colours
# ---------------------------------------------------------------------------

BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"

# ---------------------------------------------------------------------------
# Suite definitions
# ---------------------------------------------------------------------------

# Each suite: (label, script_path_relative_to_repo_root)
_SUITES: dict[str, tuple[str, Path]] = {
    "normalization": (
        "MBO Normalization",
        REPO_ROOT / "tests" / "regression" / "normalization" / "check_regression.py",
    ),
    "reconstruction": (
        "MBP-1 Reconstruction",
        REPO_ROOT / "tests" / "regression" / "reconstruction" / "check_regression.py",
    ),
}


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run normalization and reconstruction regression checks.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Typical post-commit usage:\n"
            "  python tests/run_all_checks.py --skip-pipeline\n\n"
            "Deep check (re-runs pipelines):\n"
            "  python tests/run_all_checks.py\n"
        ),
    )
    parser.add_argument(
        "--suite",
        choices=["normalization", "reconstruction", "both"],
        default="both",
        help=(
            "Which suite(s) to run (default: both). "
            "Use 'normalization' after ingestion changes, "
            "'reconstruction' after build_mbp1.py changes."
        ),
    )
    parser.add_argument(
        "--skip-pipeline",
        action="store_true",
        help=(
            "Skip re-running the pipelines — compare outputs already on disk. "
            "Recommended for routine post-commit checks (~30s). "
            "Omit when you want to verify the pipeline itself (~10 min)."
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Pass --verbose to each suite — prints all metric comparisons.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Suite runner
# ---------------------------------------------------------------------------

def _run_suite(
    label        : str,
    script       : Path,
    skip_pipeline: bool,
    verbose      : bool,
) -> tuple[int, float]:
    """Run one check_regression.py suite as a subprocess.

    Streams output directly to stdout in real time — no buffering.
    Returns (exit_code, elapsed_seconds).
    """
    cmd = [sys.executable, str(script), "--skip-pipeline"] if skip_pipeline \
        else [sys.executable, str(script)]
    if verbose:
        cmd.append("--verbose")

    t0 = time.monotonic()
    # Use Popen to stream output live — important for long-running suites
    proc = subprocess.Popen(cmd, cwd=str(REPO_ROOT))
    proc.wait()
    elapsed = time.monotonic() - t0

    return proc.returncode, elapsed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    args = _parse_args()

    # Build the list of suites to run
    if args.suite == "both":
        suites = list(_SUITES.items())
    else:
        suites = [(args.suite, _SUITES[args.suite])]

    mode_str = "skip-pipeline (outputs on disk)" if args.skip_pipeline else "re-run pipelines"

    print(f"\n{BOLD}{'=' * 68}{RESET}")
    print(f"{BOLD}  Destiny Research — Regression Check Suite{RESET}")
    print(f"{BOLD}{'=' * 68}{RESET}")
    print(f"  Repo root : {REPO_ROOT}")
    print(f"  Mode      : {mode_str}")
    print(f"  Suites    : {', '.join(k for k, _ in suites)}")
    print(f"{BOLD}{'=' * 68}{RESET}\n")

    results: list[tuple[str, int, float]] = []  # (label, exit_code, elapsed)

    for suite_key, (label, script) in suites:

        if not script.exists():
            print(
                f"{RED}[MISSING]{RESET} {label} — "
                f"script not found: {script}\n"
                f"  Run generate_golden.py first to create the golden files.\n"
            )
            results.append((label, 2, 0.0))
            continue

        print(f"{BOLD}{'─' * 68}{RESET}")
        print(f"{BOLD}  Running: {label}{RESET}")
        print(f"{BOLD}{'─' * 68}{RESET}")

        exit_code, elapsed = _run_suite(
            label         = label,
            script        = script,
            skip_pipeline = args.skip_pipeline,
            verbose       = args.verbose,
        )
        results.append((label, exit_code, elapsed))

        # Small visual separator between suites
        if len(suites) > 1:
            print()

    # ── Consolidated summary ──────────────────────────────────────────────
    total_elapsed = sum(e for _, _, e in results)

    print(f"\n{BOLD}{'=' * 68}{RESET}")
    print(f"{BOLD}  SUMMARY{RESET}")
    print(f"{BOLD}{'=' * 68}{RESET}")

    overall_code = 0
    for label, code, elapsed in results:
        if code == 0:
            status = f"{GREEN}PASSED{RESET}"
        elif code == 1:
            status = f"{RED}FAILED{RESET}"
            overall_code = max(overall_code, 1)
        else:
            status = f"{YELLOW}SETUP ERROR{RESET}"
            overall_code = max(overall_code, 2)

        print(f"  {status}  {label:<30}  ({elapsed:.1f}s)")

    print(f"{BOLD}{'─' * 68}{RESET}")
    print(f"  Total time : {total_elapsed:.1f}s")

    if overall_code == 0:
        print(f"\n{GREEN}{BOLD}  ALL CHECKS PASSED ✓{RESET}\n")
    elif overall_code == 1:
        print(f"\n{RED}{BOLD}  REGRESSIONS DETECTED — do not merge{RESET}\n")
    else:
        print(f"\n{YELLOW}{BOLD}  SETUP ERRORS — run generate_golden.py first{RESET}\n")

    print(f"{BOLD}{'=' * 68}{RESET}\n")

    return overall_code


if __name__ == "__main__":
    sys.exit(main())