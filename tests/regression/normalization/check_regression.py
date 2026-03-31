"""
tests/regression/normalization/check_regression.py

Regression checker for the MBO normalization pipeline.

Compares current pipeline outputs against golden reference files generated
by generate_golden.py. Designed to be run after any modification to the
normalization pipeline (ingest.py, databento_adapter.py, validator.py,
schema.py) to detect silent regressions.

Exit codes:
    0 — all checks passed
    1 — one or more regressions detected
    2 — setup error (golden file missing, pipeline failed, normalized file not found)

Usage:
    # Quick check — compare existing outputs, no pipeline re-run (recommended)
    python tests/regression/normalization/check_regression.py --skip-pipeline

    # Full check — re-runs pipeline then compares
    python tests/regression/normalization/check_regression.py

    # Single product, verbose output
    python tests/regression/normalization/check_regression.py --product ES --verbose --skip-pipeline

    # All products, verbose
    python tests/regression/normalization/check_regression.py --skip-pipeline --verbose
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Repo root + shared import
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "tests" / "regression"))

from config import GOLDEN_DIR_NORM, REPO_ROOT  # noqa: E402
from shared.metrics_mbo import (  # noqa: E402
    BOLD, GREEN, RED, RESET, YELLOW,
    compare_metrics,
    extract_metrics,
    load_golden,
    run_pipeline,
)

# ---------------------------------------------------------------------------
# Configuration — must stay in sync with generate_golden.py
# ---------------------------------------------------------------------------

GOLDEN_CONFIG: dict[str, tuple[str, str]] = {
    # product: (contract, date)
    ######### CME   PRODUCTS #########
    "ES":   ("ESZ25",   "2025-10-01"),
    "NIY":  ("NIYU25",  "2025-06-16"),
    "NKD":  ("NKDU25",  "2025-06-16"),
    ######### EUREX PRODUCTS #########
    "FDAX": ("FDAXM25", "2025-05-02"),
    "FESX": ("FESXM25", "2025-05-02"),
    "FSMI": ("FSMIM25", "2025-05-02"),
    ######### HKEX  PRODUCTS #########
    "HHI":  ("HHIG26",  "2026-02-03"),
    "HSI":  ("HSIG26",  "2026-02-03"),
    "MCH":  ("MCHG26",  "2026-02-03"),
    "MHI":  ("MHIG26",  "2026-02-03"),
}

GOLDEN_DIR = GOLDEN_DIR_NORM

PIPELINE_VERSION = "normalization_v1"


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run regression checks on the MBO normalization pipeline."
    )
    parser.add_argument(
        "--product", default=None,
        choices=list(GOLDEN_CONFIG.keys()),
        help="Restrict to a single product (default: all products).",
    )
    parser.add_argument(
        "--skip-pipeline", action="store_true",
        help=(
            "Skip re-running the ingestion pipeline. "
            "Use existing normalized outputs on disk. "
            "Recommended for routine checks — the pipeline is slow on 12-month datasets."
        ),
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print all metric comparisons, not just failures.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()

    # Build work list
    config: dict[str, tuple[str, str]]
    if args.product:
        if args.product not in GOLDEN_CONFIG:
            print(f"ERROR: unknown product '{args.product}'", file=sys.stderr)
            return 2
        config = {args.product: GOLDEN_CONFIG[args.product]}
    else:
        config = GOLDEN_CONFIG

    print(f"{BOLD}MBO Normalization — Regression Check{RESET}")
    print(f"Repo root    : {REPO_ROOT}")
    print(f"Golden dir   : {GOLDEN_DIR}")
    print(f"Mode         : {'skip-pipeline (existing outputs)' if args.skip_pipeline else 're-run pipeline'}")
    print(f"Products     : {', '.join(config.keys())}\n")

    total_checks  = 0
    total_failures = 0
    setup_errors  = 0

    for product, (contract, date_str) in config.items():
        print(f"{BOLD}--- {product} {contract} {date_str} ---{RESET}")

        # Load golden reference
        golden = load_golden(GOLDEN_DIR, product, contract, date_str)
        if golden is None:
            print(
                f"  {YELLOW}[SKIP]{RESET} no golden file found — "
                f"run generate_golden.py first"
            )
            setup_errors += 1
            continue

        # Optionally re-run the pipeline
        if not args.skip_pipeline:
            ok = run_pipeline(product, date_str)
            if not ok:
                print(f"  {RED}[ERROR]{RESET} pipeline failed — skipping comparison")
                setup_errors += 1
                continue

        # Extract current metrics
        current = extract_metrics(product, contract, date_str, PIPELINE_VERSION)

        if current.get("row_count") is None:
            print(
                f"  {RED}[ERROR]{RESET} normalized file not found — "
                f"pipeline may not have run for this date"
            )
            setup_errors += 1
            continue

        # Compare against golden
        failures = compare_metrics(golden, current, verbose=args.verbose)
        total_checks += 1

        # Print summary for this product/day
        n_metrics = len([
            k for k in golden
            if k not in ("product", "contract", "date", "pipeline_version", "sample_rows_used")
        ])

        if failures:
            total_failures += 1
            print(f"  {RED}[FAIL]{RESET} {len(failures)} regression(s) detected:")
            for f in failures:
                print(f"    {RED}✗ {f}{RESET}")
        else:
            print(
                f"  {GREEN}[PASS]{RESET} all {n_metrics} metrics match golden  "
                f"(rows={current['row_count']:,}  "
                f"rejected={current.get('rejected_count', '?')}  "
                f"warmup_skip={current.get('warmup_skip_count', '?')})"
            )

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    print(f"\n{'=' * 60}")

    if setup_errors:
        print(f"{YELLOW}Setup errors : {setup_errors}{RESET}")

    if total_failures == 0 and setup_errors == 0:
        print(f"{GREEN}{BOLD}PASSED — {total_checks}/{total_checks} checks clean{RESET}")
        return 0
    elif total_failures > 0:
        print(
            f"{RED}{BOLD}FAILED — {total_failures}/{total_checks} checks have regressions{RESET}"
        )
        return 1
    else:
        # setup errors only, no actual regressions detected on the checks that ran
        print(
            f"{YELLOW}INCOMPLETE — {setup_errors} setup error(s), "
            f"{total_checks} check(s) run{RESET}"
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())