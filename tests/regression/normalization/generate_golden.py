"""
tests/regression/normalization/generate_golden.py

Generate golden reference files for the MBO normalization pipeline.

Run ONCE after a known-good pipeline state, then commit the golden/ directory.
The golden files are JSON snapshots of statistical metrics extracted from the
normalized MBO Parquet outputs. They are used by check_regression.py to detect
silent regressions in the normalization pipeline.

One golden file per product/day — 1 day per product is sufficient to detect
algorithmic regressions in the normalization logic.

Usage:
    # Generate all golden files (assumes normalized outputs already on disk)
    python tests/regression/normalization/generate_golden.py --skip-pipeline

    # Re-run the ingestion pipeline first, then generate
    python tests/regression/normalization/generate_golden.py

    # Single product only
    python tests/regression/normalization/generate_golden.py --product ES --skip-pipeline
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Repo root + shared import
# ---------------------------------------------------------------------------

# tests/regression/normalization/ → go up 3 levels to reach repo root
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "tests" / "regression"))

from config import GOLDEN_DIR_NORM, REPO_ROOT  # noqa: E402
from shared.metrics_mbo import (  # noqa: E402
    BOLD, GREEN, RED, RESET, YELLOW,
    extract_metrics,
    run_pipeline,
    save_golden,
)

# ---------------------------------------------------------------------------
# Golden configuration
# ---------------------------------------------------------------------------
# One reference day per product — enough to catch algorithmic regressions.
# Contract is the front month for that date.
#
# Date selection rationale:
#   ES   2025-10-01 : first day of dataset, 100% clean (0 rejected, 0 warmup_skip)
#   FDAX 2025-05-02 : first trading day of dataset, ~1 rejected/day (duplicate),
#                     ~1000 warmup_skip/day (GTC overnight orders) — tests both paths
#   FESX 2025-05-02 : same raw file as FDAX, different OTR profile
#   FSMI 2025-05-02 : same batch, lowest volume product — good contrast
#   NIY  2025-06-16 : mid-year, post-rollover (NIYU25 front month), clean session
#   NKD  2025-06-16 : same day as NIY, crossed books structural property of NKD
#
# To add a product: append an entry with (contract, date).
# Contract must match exactly the symbol in the normalized Parquet filename.

# TO DO: MOVE THIS LIST IN A SEPARATE FILE SO IT CAN BE SHARED
# WITH check_regression.py. IT WILL ALSO BE EASIER TO ADD A NEW
# PRODUCT BY LISTING ALL THE STEPS TO DO IN THIS SEPARATE FILE
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

# Bump this tag manually after intentional breaking changes to the pipeline.
# The tag is stored in each golden file for traceability.
PIPELINE_VERSION = "normalization_v1"


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate MBO normalization pipeline golden reference files."
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
            "Assumes normalized outputs already exist on disk. "
            "Recommended when golden files are being regenerated after "
            "a known-good run."
        ),
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()

    # Build work list — single product or all
    config: dict[str, tuple[str, str]]
    if args.product:
        config = {args.product: GOLDEN_CONFIG[args.product]}
    else:
        config = GOLDEN_CONFIG

    print(f"{BOLD}MBO Normalization — Golden File Generator{RESET}")
    print(f"Repo root    : {REPO_ROOT}")
    print(f"Data norm    : {__import__('config').DATA_NORMALIZED}")
    print(f"Golden dir   : {GOLDEN_DIR}")
    print(f"Version tag  : {PIPELINE_VERSION}")
    print(f"Mode         : {'skip-pipeline (use existing outputs)' if args.skip_pipeline else 're-run pipeline'}")
    print(f"Products     : {', '.join(config.keys())}\n")

    errors: list[str] = []

    for product, (contract, date_str) in config.items():
        print(f"{BOLD}--- {product} {contract} {date_str} ---{RESET}")

        # Step 1: optionally re-run the ingestion pipeline
        if not args.skip_pipeline:
            ok = run_pipeline(product, date_str)
            if not ok:
                msg = f"{product} {contract} {date_str}: pipeline failed"
                errors.append(msg)
                print(f"  {RED}[ERROR]{RESET} {msg}")
                continue

        # Step 2: extract metrics from normalized output
        metrics = extract_metrics(product, contract, date_str, PIPELINE_VERSION)

        if metrics.get("row_count") is None:
            msg = f"{product} {contract} {date_str}: normalized file not found"
            print(f"  {YELLOW}[WARN]{RESET} {msg}")
            print(f"         Expected: data/normalized/.../  {contract}_*.parquet")
            errors.append(msg)
            continue

        # Step 3: write golden JSON
        out_path = save_golden(metrics, GOLDEN_DIR, product, contract, date_str)
        print(f"  {GREEN}[golden]{RESET} written : {out_path}")
        print(
            f"           rows={metrics['row_count']:,}"
            f"  rejected={metrics.get('rejected_count', '?')}"
            f"  warmup_skip={metrics.get('warmup_skip_count', '?')}"
            f"  checksum={metrics.get('sample_checksum_sha256', '?')[:12]}..."
        )

        # Print action distribution for quick sanity check
        if "action_distribution" in metrics:
            dist_str = "  ".join(
                f"{k}={v:,}" for k, v in metrics["action_distribution"].items()
            )
            print(f"           actions: {dist_str}")

    print()
    if errors:
        print(f"{RED}[DONE]{RESET} Completed with {len(errors)} error(s):")
        for e in errors:
            print(f"  - {e}")
        print()
        print("Tip: run with --skip-pipeline if normalized files already exist.")
        return 1

    print(
        f"{GREEN}{BOLD}[DONE]{RESET} All golden files generated successfully.\n"
        f"       Next: git add {GOLDEN_DIR}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())