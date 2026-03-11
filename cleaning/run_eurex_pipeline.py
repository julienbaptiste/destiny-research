# cleaning/run_eurex_pipeline.py
#
# Phase 2 — Eurex cleaning pipeline orchestrator
#
# Runs the full cleaning pipeline for one or more Eurex products in sequence:
#   Step 1: clean_eurex_orders.py   — session filter + implied leg removal
#   Step 2: clean_eurex_trades.py   — session filter + implied spread removal
#   Step 3: build_eurex_lob1.py     — MBO state machine → TOB snapshots
#
# Usage:
#   python run_eurex_pipeline.py --products FDAX FESX FSMI
#   python run_eurex_pipeline.py --products FDAX --from-date 20250502 --to-date 20250509
#   python run_eurex_pipeline.py --products FESX FSMI --steps orders trades
#   python run_eurex_pipeline.py --products FDAX --dry-run
#   python run_eurex_pipeline.py --products FDAX --no-validate
#
# Each step is called as a subprocess — clean separation of concerns, each script
# can still be run independently. Failures in one product do not abort others.
# A cross-product summary is printed at the end.
#
# To add a new Eurex product to the pipeline: add one entry to eurex_config.py.
# This script will pick it up automatically via AVAILABLE_PRODUCTS.
#
# Exit code: 0 if all steps succeeded for all products, 1 if any step failed.

import argparse
import subprocess
import sys
import time

from eurex_config import AVAILABLE_PRODUCTS

# ---------------------------------------------------------------------------
# STEPS CONFIG
# ---------------------------------------------------------------------------

AVAILABLE_STEPS = ["orders", "trades", "lob1"]

# Map step name -> script path (relative to repo root — adjust if layout changes)
STEP_SCRIPTS = {
    "orders": "cleaning/clean_eurex_orders.py",
    "trades": "cleaning/clean_eurex_trades.py",
    "lob1":   "cleaning/build_eurex_lob1.py",
}

# ---------------------------------------------------------------------------
# ARGUMENT PARSING
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Eurex cleaning pipeline: orders → trades → lob1",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_eurex_pipeline.py --products FDAX FESX FSMI
  python run_eurex_pipeline.py --products FDAX --from-date 20250502 --to-date 20250509
  python run_eurex_pipeline.py --products FESX --steps orders trades
  python run_eurex_pipeline.py --products FDAX FESX FSMI --dry-run
        """,
    )
    parser.add_argument(
        "--products", nargs="+", required=True,
        choices=AVAILABLE_PRODUCTS,
        help="Products to process (e.g. FDAX FESX FSMI)",
    )
    parser.add_argument(
        "--steps", nargs="+", default=AVAILABLE_STEPS,
        choices=AVAILABLE_STEPS,
        help=f"Steps to run (default: all). Choices: {AVAILABLE_STEPS}",
    )
    parser.add_argument(
        "--from-date", default=None,
        help="Start date inclusive, format YYYYMMDD",
    )
    parser.add_argument(
        "--to-date", default=None,
        help="End date inclusive, format YYYYMMDD",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Pass --dry-run to orders and trades steps (no files written). lob1 is skipped.",
    )
    parser.add_argument(
        "--no-validate", action="store_true",
        help="Pass --no-validate to lob1 step (skip per-day DuckDB validation)",
    )
    parser.add_argument(
        "--data-root", default="data/market_data",
        help="Root for raw market data (default: data/market_data)",
    )
    parser.add_argument(
        "--output-root-clean", default="data/clean",
        help="Root for clean output (default: data/clean)",
    )
    parser.add_argument(
        "--output-root-lob1", default="data/lob1",
        help="Root for lob1 output (default: data/lob1)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# RUN ONE STEP
# ---------------------------------------------------------------------------

def run_step(
    step:    str,
    product: str,
    args:    argparse.Namespace,
) -> tuple[int, float]:
    """
    Build and execute the subprocess command for one pipeline step.
    Returns (returncode, elapsed_seconds).
    Output (stdout + stderr) is streamed directly to terminal in real time.
    """
    script = STEP_SCRIPTS[step]

    # Base command: run with the same Python interpreter as this process —
    # ensures the correct venv is used without requiring explicit activation.
    cmd = [sys.executable, script, "--product", product]

    # Date range — shared by all three scripts
    if args.from_date:
        cmd += ["--from-date", args.from_date]
    if args.to_date:
        cmd += ["--to-date", args.to_date]

    # Step-specific flags
    if step in ("orders", "trades"):
        cmd += ["--data-root",   args.data_root]
        cmd += ["--output-root", args.output_root_clean]
        if args.dry_run:
            cmd.append("--dry-run")

    elif step == "lob1":
        cmd += ["--clean-root",  args.output_root_clean]
        cmd += ["--output-root", args.output_root_lob1]
        if args.no_validate:
            cmd.append("--no-validate")
        # lob1 skipped entirely in dry-run — no clean files exist yet (or by design)
        if args.dry_run:
            print(f"  [DRY] lob1 skipped (--dry-run)")
            return 0, 0.0

    print(f"\n  $ {' '.join(cmd)}")
    t0 = time.time()

    # No stdout/stderr capture — subprocess logs stream to terminal in real time
    result = subprocess.run(cmd)

    elapsed = time.time() - t0
    return result.returncode, round(elapsed, 1)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    # Preserve canonical step order regardless of user input order
    steps_to_run = [s for s in AVAILABLE_STEPS if s in args.steps]

    print(f"\n{'='*60}")
    print(f"  run_eurex_pipeline.py")
    print(f"  Products  : {args.products}")
    print(f"  Steps     : {steps_to_run}")
    print(f"  Dry run   : {args.dry_run}")
    if args.from_date:
        print(f"  From date : {args.from_date}")
    if args.to_date:
        print(f"  To date   : {args.to_date}")
    print(f"{'='*60}")

    # results[product][step] = (returncode, elapsed_s)
    results: dict[str, dict[str, tuple[int, float]]] = {}
    pipeline_t0 = time.time()

    for product in args.products:
        results[product] = {}
        print(f"\n{'─'*60}")
        print(f"  PRODUCT: {product}")
        print(f"{'─'*60}")

        for step in steps_to_run:
            print(f"\n[{product}] Step: {step}")
            rc, elapsed = run_step(step, product, args)
            results[product][step] = (rc, elapsed)

            if rc != 0:
                print(
                    f"\n  [FAIL] {product} / {step} "
                    f"returned exit code {rc} — skipping remaining steps for {product}"
                )
                # Fail-fast per product: mark remaining steps as skipped
                remaining = steps_to_run[steps_to_run.index(step) + 1:]
                for skipped_step in remaining:
                    results[product][skipped_step] = ("SKIPPED", 0.0)
                break

    pipeline_elapsed = round(time.time() - pipeline_t0, 1)

    # ---------------------------------------------------------------------------
    # CROSS-PRODUCT SUMMARY TABLE
    # ---------------------------------------------------------------------------
    print(f"\n\n{'='*60}")
    print(f"  PIPELINE SUMMARY  ({pipeline_elapsed}s total)")
    print(f"{'='*60}")

    col_w  = 12
    header = f"  {'Product':<8}" + "".join(f"{s:>{col_w}}" for s in steps_to_run)
    print(header)
    print(f"  {'-'*8}" + f"{'-'*col_w}" * len(steps_to_run))

    any_failure = False
    for product in args.products:
        row = f"  {product:<8}"
        for step in steps_to_run:
            rc, elapsed = results[product].get(step, ("N/A", 0.0))
            if rc == 0:
                cell = f"OK ({elapsed}s)"
            elif rc == "SKIPPED":
                cell = "SKIPPED"
            else:
                cell = f"FAIL({rc})"
                any_failure = True
            row += f"{cell:>{col_w}}"
        print(row)

    print(f"{'='*60}\n")

    sys.exit(1 if any_failure else 0)


if __name__ == "__main__":
    main()