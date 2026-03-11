"""
tests/regression/eurex/generate_golden.py

Generate golden reference files for the Eurex cleaning pipeline.
Thin wrapper around shared/metrics.py — all heavy lifting is there.

Run ONCE after a known-good pipeline state, then commit the golden/ directory.

Usage:
    python tests/regression/eurex/generate_golden.py
    python tests/regression/eurex/generate_golden.py --product FDAX
    python tests/regression/eurex/generate_golden.py --product FDAX --dates 2025-05-02 2025-05-12
    python tests/regression/eurex/generate_golden.py --skip-pipeline
"""

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Repo root & shared import
# ---------------------------------------------------------------------------

# tests/regression/eurex/ -> go up 3 levels to reach repo root
REPO_ROOT = Path(__file__).resolve().parents[3]
SHARED_DIR = REPO_ROOT / "tests" / "regression" / "shared"
sys.path.insert(0, str(SHARED_DIR.parent))  # make 'shared' importable as a package

from shared.metrics import (  # noqa: E402
    BOLD, GREEN, RED, RESET, YELLOW,
    extract_metrics,
    load_golden,
    run_pipeline,
    save_golden,
)

# ---------------------------------------------------------------------------
# Eurex-specific configuration
# ---------------------------------------------------------------------------

# Orchestrator script for the Eurex pipeline
PIPELINE_SCRIPT = REPO_ROOT / "cleaning" / "run_eurex_pipeline.py"
print(PIPELINE_SCRIPT)
# Golden dataset: 3 representative days per product
#   2025-05-02 : first trading day of May, clean warm-up session
#   2025-05-12 : Monday — test week-boundary behaviour
#   2025-05-26 : Monday before Pentecost (atypical close, DE public holiday next day)
GOLDEN_CONFIG: dict[str, list[str]] = {
    "FDAX": ["2025-05-02", "2025-05-12", "2025-05-26"],
    "FESX": ["2025-05-02", "2025-05-12", "2025-05-26"],
    "FSMI": ["2025-05-02", "2025-05-12"],  # 20 trading days only — 25/05 CH public holiday
}

GOLDEN_DIR = Path(__file__).resolve().parent / "golden"

# Human-readable version tag — bump manually after intentional breaking pipeline changes
PIPELINE_VERSION = "eurex_v1"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Eurex pipeline golden reference files."
    )
    parser.add_argument(
        "--product",
        choices=list(GOLDEN_CONFIG.keys()),
        default=None,
        help="Restrict to a single product (default: all)",
    )
    parser.add_argument(
        "--dates",
        nargs="+",
        default=None,
        help="Override dates for the selected product (requires --product)",
    )
    parser.add_argument(
        "--skip-pipeline",
        action="store_true",
        help="Skip re-running the pipeline (assume outputs already exist on disk)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # Build the (product, dates) map to process
    if args.product:
        config = {args.product: args.dates or GOLDEN_CONFIG[args.product]}
    elif args.dates:
        print("ERROR: --dates requires --product", file=sys.stderr)
        return 1
    else:
        config = GOLDEN_CONFIG

    print(f"{BOLD}Eurex Pipeline — Golden File Generator{RESET}")
    print(f"Repo root   : {REPO_ROOT}")
    print(f"Golden dir  : {GOLDEN_DIR.relative_to(REPO_ROOT)}")
    print(f"Version tag : {PIPELINE_VERSION}\n")

    errors: list[str] = []

    for product, dates in config.items():
        for date_str in dates:
            print(f"{BOLD}--- {product} {date_str} ---{RESET}")

            # Step 1: run the pipeline (unless skipped)
            if not args.skip_pipeline:
                ok = run_pipeline(REPO_ROOT, PIPELINE_SCRIPT, product, date_str)
                if not ok:
                    errors.append(f"{product} {date_str}: pipeline failed")
                    continue

            # Step 2: extract metrics from current outputs
            metrics = extract_metrics(REPO_ROOT, product, date_str, PIPELINE_VERSION)
            if metrics.get("lob1_row_count") is None:
                msg = f"{product} {date_str}: lob1 not found — pipeline may not have run"
                print(f"  {YELLOW}[WARN]{RESET} {msg}")
                errors.append(msg)
                continue

            # Step 3: write golden JSON
            out_path = save_golden(metrics, GOLDEN_DIR, product, date_str)
            print(f"  {GREEN}[golden]{RESET} written: {out_path.relative_to(REPO_ROOT)}")

    print()
    if errors:
        print(f"{RED}[DONE]{RESET} Completed with {len(errors)} error(s):")
        for e in errors:
            print(f"  - {e}")
        print(f"\nCommit {GOLDEN_DIR.relative_to(REPO_ROOT)} to Git once errors are resolved.")
        return 1

    print(f"{GREEN}{BOLD}[DONE]{RESET} All golden files generated successfully.")
    print(f"       git add {GOLDEN_DIR.relative_to(REPO_ROOT)}")
    print(f"       git commit -m 'test(regression): add Eurex pipeline golden files'")
    return 0


if __name__ == "__main__":
    sys.exit(main())