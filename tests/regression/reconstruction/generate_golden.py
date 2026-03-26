"""
tests/regression/reconstruction/generate_golden.py

Generate golden reference files for the MBP-1 reconstruction pipeline.

Run ONCE after a known-good reconstruction state, then commit the golden/
directory. The golden files are JSON snapshots of statistical metrics
extracted from the reconstructed MBP-1 Parquet outputs. They are used by
check_regression.py to detect silent regressions in the reconstruction engine.

One golden file per product/contract/day — 1 day per product is sufficient
to detect algorithmic regressions in the reconstruction logic (state machine,
emission rules, _book_cancel, _book_modify, etc.).

Date selection rationale (mirrors normalization golden config for consistency):
    ES   2025-10-01 : first day of dataset, 100% clean, high volume (8.1M rows)
    FDAX 2025-05-14 : validated against Databento MBP-1 reference (99.9956%)
    FESX 2025-05-14 : same raw file as FDAX, higher volume than FDAX
    FSMI 2025-05-14 : same batch, lowest volume EUREX product
    NIY  2025-06-16 : post-rollover, clean session, 100% validated
    NKD  2025-06-16 : same day as NIY, structurally thin market

Usage:
    # Generate all golden files (reconstructed outputs must exist on disk)
    python tests/regression/reconstruction/generate_golden.py --skip-pipeline

    # Re-run reconstruction first, then generate
    python tests/regression/reconstruction/generate_golden.py

    # Single product only
    python tests/regression/reconstruction/generate_golden.py --product ES --skip-pipeline
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Repo root + shared import
# ---------------------------------------------------------------------------

# tests/regression/reconstruction/ → go up 3 levels to reach repo root
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "tests" / "regression"))

from config import GOLDEN_DIR_RECON, REPO_ROOT  # noqa: E402
from shared.metrics_mbp1 import (               # noqa: E402
    BOLD, GREEN, RED, RESET, YELLOW,
    extract_metrics,
    run_pipeline,
    save_golden,
)

# ---------------------------------------------------------------------------
# Golden configuration
# ---------------------------------------------------------------------------
# One reference day per product — enough to catch algorithmic regressions.
# Contract is the front month validated against Databento MBP-1 reference.
#
# To add a product: append an entry with (contract, date).
# Contract must match exactly the symbol in the reconstructed Parquet filename.

GOLDEN_CONFIG: dict[str, tuple[str, str]] = {
    # product: (contract, date)
    "ES":   ("ESZ25",   "2025-10-01"),
    "FDAX": ("FDAXM25", "2025-05-14"),
    "FESX": ("FESXM25", "2025-05-14"),
    "FSMI": ("FSMIM25", "2025-05-14"),
    "NIY":  ("NIYU25",  "2025-06-16"),
    "NKD":  ("NKDU25",  "2025-06-16"),
}

GOLDEN_DIR = GOLDEN_DIR_RECON

# Bump this tag manually after intentional breaking changes to the engine.
# The tag is stored in each golden file for traceability.
PIPELINE_VERSION = "reconstruction_v1"


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate MBP-1 reconstruction pipeline golden reference files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python tests/regression/reconstruction/generate_golden.py --skip-pipeline\n"
            "  python tests/regression/reconstruction/generate_golden.py --product ES --skip-pipeline\n"
        ),
    )
    parser.add_argument(
        "--product", default=None,
        choices=list(GOLDEN_CONFIG.keys()),
        help="Restrict to a single product (default: all products).",
    )
    parser.add_argument(
        "--skip-pipeline", action="store_true",
        help=(
            "Skip re-running the reconstruction pipeline. "
            "Assumes reconstructed MBP-1 outputs already exist on disk. "
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

    print(f"{BOLD}MBP-1 Reconstruction — Golden File Generator{RESET}")
    print(f"Repo root    : {REPO_ROOT}")
    print(f"Golden dir   : {GOLDEN_DIR}")
    print(f"Version tag  : {PIPELINE_VERSION}")
    print(f"Mode         : {'skip-pipeline (use existing outputs)' if args.skip_pipeline else 're-run pipeline'}")
    print(f"Products     : {', '.join(config.keys())}\n")

    errors: list[str] = []

    for product, (contract, date_str) in config.items():
        print(f"{BOLD}--- {product} {contract} {date_str} ---{RESET}")

        # Step 1: optionally re-run the reconstruction pipeline
        if not args.skip_pipeline:
            ok = run_pipeline(product, contract, date_str)
            if not ok:
                msg = f"{product} {contract} {date_str}: pipeline failed"
                errors.append(msg)
                print(f"  {RED}[ERROR]{RESET} {msg}")
                continue

        # Step 2: extract metrics from reconstructed output
        metrics = extract_metrics(product, contract, date_str, PIPELINE_VERSION)

        if metrics.get("row_count") is None:
            msg = f"{product} {contract} {date_str}: reconstructed file not found"
            print(f"  {YELLOW}[WARN]{RESET} {msg}")
            print(
                f"         Run: python -m reconstruction.build_mbp1 "
                f"--product {product} --contract {contract} --date {date_str}"
            )
            errors.append(msg)
            continue

        # Step 3: write golden JSON
        out_path = save_golden(metrics, GOLDEN_DIR, product, contract, date_str)
        print(f"  {GREEN}[golden]{RESET} written : {out_path}")
        print(
            f"           rows={metrics['row_count']:,}"
            f"  burst_rate={metrics.get('burst_rate', '?')}"
            f"  orphan_cancel={metrics.get('orphan_cancel', '?')}"
            f"  orphan_modify={metrics.get('orphan_modify', '?')}"
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
        print("Tip: run with --skip-pipeline if reconstructed files already exist.")
        return 1

    print(
        f"{GREEN}{BOLD}[DONE]{RESET} All golden files generated successfully.\n"
        f"       Next: git add {GOLDEN_DIR}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())