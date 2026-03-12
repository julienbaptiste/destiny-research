"""
tests/regression/cme_lob1/generate_golden.py

Generate golden reference files for the CME LOB1 pipeline.
Thin wrapper around shared/metrics_lob1.py.

Run ONCE after a known-good pipeline state, then commit the golden/ directory.

Usage:
    python tests/regression/cme_lob1/generate_golden.py
    python tests/regression/cme_lob1/generate_golden.py --skip-pipeline
    python tests/regression/cme_lob1/generate_golden.py --product ES --dates 2025-10-01
"""

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Repo root & shared import
# ---------------------------------------------------------------------------

# tests/regression/cme_lob1/ -> go up 3 levels to reach repo root
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "tests" / "regression"))

from shared.metrics_lob1 import (  # noqa: E402
    BOLD, GREEN, RED, RESET, YELLOW,
    extract_metrics,
    run_pipeline,
    save_golden,
)

# ---------------------------------------------------------------------------
# CME LOB1 configuration
# ---------------------------------------------------------------------------

# Builder script
PIPELINE_SCRIPT = REPO_ROOT / "cleaning" / "build_cme_lob1.py"

# Golden dataset: 3 dates × ESZ5, validated against MBP-1
#   2025-10-01 : first trading day of October (RELIABLE, px=99.98%, sz=98.61%)
#   2025-10-10 : high-volume day (RELIABLE, px=99.93%, sz=99.39%)
#   2025-10-27 : Monday post-weekend (ACCEPTABLE, px=99.95%, sz=97.50%)
GOLDEN_CONFIG: dict[str, dict[str, list[str]]] = {
    "ES": {
        "ESZ5": ["2025-10-01", "2025-10-10", "2025-10-27"],
    },
    # Uncomment when validated:
    # "NIY": {"NIYZ5": ["2025-06-16", ...]},
    # "NKD": {"NKDZ5": ["2025-06-16", ...]},
}

GOLDEN_DIR = Path(__file__).resolve().parent / "golden"

# Bump manually after intentional breaking pipeline changes
PIPELINE_VERSION = "cme_lob1"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate CME LOB1 pipeline golden reference files."
    )
    parser.add_argument(
        "--product", default=None,
        help="Restrict to a single product (default: all)",
    )
    parser.add_argument(
        "--dates", nargs="+", default=None,
        help="Override dates (requires --product)",
    )
    parser.add_argument(
        "--skip-pipeline", action="store_true",
        help="Skip re-running the pipeline (assume outputs already exist)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # Build the work list
    if args.product:
        if args.product not in GOLDEN_CONFIG:
            print(f"ERROR: unknown product '{args.product}'", file=sys.stderr)
            return 1
        symbols_dates = GOLDEN_CONFIG[args.product]
        if args.dates:
            # Override dates for all symbols of this product
            symbols_dates = {sym: args.dates for sym in symbols_dates}
        config = {args.product: symbols_dates}
    else:
        config = GOLDEN_CONFIG

    print(f"{BOLD}CME LOB1 — Golden File Generator{RESET}")
    print(f"Repo root   : {REPO_ROOT}")
    print(f"Golden dir  : {GOLDEN_DIR.relative_to(REPO_ROOT)}")
    print(f"Version tag : {PIPELINE_VERSION}\n")

    errors: list[str] = []

    for product, symbols_dates in config.items():
        for symbol, dates in symbols_dates.items():
            for date_str in dates:
                print(f"{BOLD}--- {product} {symbol} {date_str} ---{RESET}")

                # Step 1: optionally run the pipeline
                if not args.skip_pipeline:
                    ok = run_pipeline(
                        REPO_ROOT, PIPELINE_SCRIPT, product, date_str, symbol
                    )
                    if not ok:
                        errors.append(f"{product} {symbol} {date_str}: pipeline failed")
                        continue

                # Step 2: extract metrics
                metrics = extract_metrics(
                    REPO_ROOT, product, date_str, symbol, PIPELINE_VERSION
                )
                if metrics.get("lob1_row_count") is None:
                    msg = f"{product} {symbol} {date_str}: lob1 not found"
                    print(f"  {YELLOW}[WARN]{RESET} {msg}")
                    errors.append(msg)
                    continue

                # Step 3: write golden JSON
                out_path = save_golden(metrics, GOLDEN_DIR, product, date_str, symbol)
                print(f"  {GREEN}[golden]{RESET} written: {out_path.relative_to(REPO_ROOT)}")
                print(f"           rows={metrics['lob1_row_count']:,}  "
                      f"tob_chg={metrics.get('tob_change_count', '?')}  "
                      f"crossed={metrics.get('crossed_book_count', '?')}")

    print()
    if errors:
        print(f"{RED}[DONE]{RESET} Completed with {len(errors)} error(s):")
        for e in errors:
            print(f"  - {e}")
        return 1

    print(f"{GREEN}{BOLD}[DONE]{RESET} All golden files generated successfully.")
    print(f"       git add {GOLDEN_DIR.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())