"""
tests/regression/eurex/check_regression.py

Regression checker for the Eurex cleaning pipeline.
Thin wrapper around shared/metrics.py — all heavy lifting is there.

Exit codes:
    0 — all checks passed
    1 — regression detected
    2 — setup error (golden file missing, pipeline failed, lob1 not found)

Usage:
    # Full check — re-runs pipeline then compares
    python tests/regression/eurex/check_regression.py

    # Quick mode — compare existing outputs, no pipeline re-run
    python tests/regression/eurex/check_regression.py --skip-pipeline

    # Single product, verbose output
    python tests/regression/eurex/check_regression.py --product FDAX --verbose
"""

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Repo root & shared import
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[3]
SHARED_DIR = REPO_ROOT / "tests" / "regression" / "shared"
sys.path.insert(0, str(SHARED_DIR.parent))

from shared.metrics import (  # noqa: E402
    BOLD, GREEN, RED, RESET, YELLOW,
    compare_metrics,
    extract_metrics,
    load_golden,
    run_pipeline,
)

# ---------------------------------------------------------------------------
# Eurex-specific configuration
# ---------------------------------------------------------------------------

PIPELINE_SCRIPT = REPO_ROOT / "cleaning" / "run_eurex_pipeline.py"

GOLDEN_CONFIG: dict[str, list[str]] = {
    "FDAX": ["2025-05-02", "2025-05-12", "2025-05-26"],
    "FESX": ["2025-05-02", "2025-05-12", "2025-05-26"],
    "FSMI": ["2025-05-02", "2025-05-12"],
}

GOLDEN_DIR = Path(__file__).resolve().parent / "golden"

PIPELINE_VERSION = "eurex_v1"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run regression checks on the Eurex cleaning pipeline."
    )
    parser.add_argument(
        "--product",
        choices=list(GOLDEN_CONFIG.keys()),
        default=None,
        help="Restrict to a single product (default: all)",
    )
    parser.add_argument(
        "--skip-pipeline",
        action="store_true",
        help="Skip re-running the pipeline (use existing outputs on disk)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print all metric comparisons, not just failures",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    config = GOLDEN_CONFIG
    if args.product:
        if args.product not in GOLDEN_CONFIG:
            print(f"ERROR: unknown product '{args.product}'", file=sys.stderr)
            return 2
        config = {args.product: GOLDEN_CONFIG[args.product]}

    print(f"{BOLD}Eurex Pipeline — Regression Check{RESET}")
    print(f"Repo root   : {REPO_ROOT}")
    print(f"Golden dir  : {GOLDEN_DIR.relative_to(REPO_ROOT)}")
    print(f"Mode        : {'skip-pipeline' if args.skip_pipeline else 're-run pipeline'}\n")

    total_checks = 0
    total_failures = 0
    setup_errors = 0

    for product, dates in config.items():
        for date_str in dates:
            print(f"{BOLD}--- {product} {date_str} ---{RESET}")

            # Load golden reference — abort this pair if missing
            golden = load_golden(GOLDEN_DIR, product, date_str)
            if golden is None:
                print(
                    f"  {YELLOW}[SKIP]{RESET} no golden file found"
                    f" — run generate_golden.py first"
                )
                setup_errors += 1
                continue

            # Optionally re-run the pipeline
            if not args.skip_pipeline:
                ok = run_pipeline(REPO_ROOT, PIPELINE_SCRIPT, product, date_str)
                if not ok:
                    print(f"  {RED}[ERROR]{RESET} pipeline failed — skipping comparison")
                    setup_errors += 1
                    continue

            # Extract current metrics
            current = extract_metrics(REPO_ROOT, product, date_str, PIPELINE_VERSION)
            if current.get("lob1_row_count") is None:
                print(
                    f"  {RED}[ERROR]{RESET} lob1 not found"
                    f" — pipeline may not have run for this date"
                )
                setup_errors += 1
                continue

            # Compare against golden
            failures = compare_metrics(golden, current, verbose=args.verbose)
            total_checks += 1

            n_metrics = len(golden) - 3  # exclude product/date/pipeline_version keys
            if failures:
                total_failures += 1
                print(f"  {RED}[FAIL]{RESET} {len(failures)} regression(s) detected:")
                for f in failures:
                    print(f"    {RED}✗ {f}{RESET}")
            else:
                print(f"  {GREEN}[PASS]{RESET} all {n_metrics} metrics match golden reference")

    # --- Summary ---
    print(f"\n{'=' * 60}")
    if setup_errors:
        print(f"{YELLOW}Setup errors : {setup_errors}{RESET}")

    if total_failures == 0 and setup_errors == 0:
        print(f"{GREEN}{BOLD}PASSED — {total_checks}/{total_checks} checks clean{RESET}")
        return 0
    elif total_failures > 0:
        print(
            f"{RED}{BOLD}FAILED — {total_failures}/{total_checks} checks"
            f" have regressions{RESET}"
        )
        return 1
    else:
        print(
            f"{YELLOW}INCOMPLETE — {setup_errors} setup error(s),"
            f" {total_checks} check(s) run{RESET}"
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())