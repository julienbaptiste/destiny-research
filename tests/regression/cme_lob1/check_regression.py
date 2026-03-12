"""
tests/regression/cme_lob1/check_regression.py

Regression checker for the CME LOB1 pipeline.
Thin wrapper around shared/metrics_lob1.py.

Exit codes:
    0 — all checks passed
    1 — regression detected
    2 — setup error (golden file missing, pipeline failed, lob1 not found)

Usage:
    # Full check — re-runs pipeline then compares
    python tests/regression/cme_lob1/check_regression.py

    # Quick mode — compare existing outputs, no pipeline re-run
    python tests/regression/cme_lob1/check_regression.py --skip-pipeline

    # Single product, verbose
    python tests/regression/cme_lob1/check_regression.py --product ES --verbose
"""

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Repo root & shared import
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "tests" / "regression"))

from shared.metrics_lob1 import (  # noqa: E402
    BOLD, GREEN, RED, RESET, YELLOW,
    compare_metrics,
    extract_metrics,
    load_golden,
    run_pipeline,
)

# ---------------------------------------------------------------------------
# CME LOB1 configuration (must match generate_golden.py)
# ---------------------------------------------------------------------------

PIPELINE_SCRIPT = REPO_ROOT / "cleaning" / "build_cme_lob1.py"

GOLDEN_CONFIG: dict[str, dict[str, list[str]]] = {
    "ES": {
        "ESZ5": ["2025-10-01", "2025-10-10", "2025-10-27"],
    },
}

GOLDEN_DIR = Path(__file__).resolve().parent / "golden"

PIPELINE_VERSION = "cme_lob1"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run regression checks on the CME LOB1 pipeline."
    )
    parser.add_argument(
        "--product", default=None,
        help="Restrict to a single product (default: all)",
    )
    parser.add_argument(
        "--skip-pipeline", action="store_true",
        help="Skip re-running the pipeline (use existing outputs on disk)",
    )
    parser.add_argument(
        "--verbose", action="store_true",
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

    print(f"{BOLD}CME LOB1 — Regression Check{RESET}")
    print(f"Repo root   : {REPO_ROOT}")
    print(f"Golden dir  : {GOLDEN_DIR.relative_to(REPO_ROOT)}")
    print(f"Mode        : {'skip-pipeline' if args.skip_pipeline else 're-run pipeline'}\n")

    total_checks = 0
    total_failures = 0
    setup_errors = 0

    for product, symbols_dates in config.items():
        for symbol, dates in symbols_dates.items():
            for date_str in dates:
                print(f"{BOLD}--- {product} {symbol} {date_str} ---{RESET}")

                # Load golden reference
                golden = load_golden(GOLDEN_DIR, product, date_str, symbol)
                if golden is None:
                    print(
                        f"  {YELLOW}[SKIP]{RESET} no golden file found"
                        f" — run generate_golden.py first"
                    )
                    setup_errors += 1
                    continue

                # Optionally re-run the pipeline
                if not args.skip_pipeline:
                    ok = run_pipeline(
                        REPO_ROOT, PIPELINE_SCRIPT, product, date_str, symbol
                    )
                    if not ok:
                        print(f"  {RED}[ERROR]{RESET} pipeline failed — skipping")
                        setup_errors += 1
                        continue

                # Extract current metrics
                current = extract_metrics(
                    REPO_ROOT, product, date_str, symbol, PIPELINE_VERSION
                )
                if current.get("lob1_row_count") is None:
                    print(f"  {RED}[ERROR]{RESET} lob1 not found — pipeline may not have run")
                    setup_errors += 1
                    continue

                # Compare against golden
                failures = compare_metrics(golden, current, verbose=args.verbose)
                total_checks += 1

                if failures:
                    total_failures += 1
                    print(f"  {RED}[FAIL]{RESET} {len(failures)} regression(s):")
                    for f in failures:
                        print(f"    {RED}✗ {f}{RESET}")
                else:
                    n_metrics = len([k for k in golden if k not in ("product", "symbol", "date", "pipeline_version")])
                    print(f"  {GREEN}[PASS]{RESET} all {n_metrics} metrics match golden")

    # --- Summary ---
    print(f"\n{'=' * 60}")
    if setup_errors:
        print(f"{YELLOW}Setup errors : {setup_errors}{RESET}")

    if total_failures == 0 and setup_errors == 0:
        print(f"{GREEN}{BOLD}PASSED — {total_checks}/{total_checks} checks clean{RESET}")
        return 0
    elif total_failures > 0:
        print(f"{RED}{BOLD}FAILED — {total_failures}/{total_checks} checks have regressions{RESET}")
        return 1
    else:
        print(f"{YELLOW}INCOMPLETE — {setup_errors} setup error(s), {total_checks} check(s) run{RESET}")
        return 2


if __name__ == "__main__":
    sys.exit(main())