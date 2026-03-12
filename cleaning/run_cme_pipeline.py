# cleaning/run_cme_pipeline.py
#
# Phase 2 — CME Globex cleaning pipeline orchestrator
#
# Runs the full cleaning pipeline for one or more CME products in sequence:
#   Step 1: clean_cme_orders.py   — snapshot filter + symbol filter + session filter
#   Step 2: clean_cme_trades.py   — symbol filter + session filter
#   Step 3: build_cme_lob1.py     — MBO state machine → TOB snapshots (RTH, no snapshot)
#
# Usage:
#   python run_cme_pipeline.py --products ES
#   python run_cme_pipeline.py --products ES --from-date 2025-10-01 --to-date 2025-10-31
#   python run_cme_pipeline.py --products ES --steps orders trades
#   python run_cme_pipeline.py --products ES --dry-run
#   python run_cme_pipeline.py --products ES --no-validate
#
# Each step is called as a subprocess — clean separation of concerns, each script
# can still be run independently. Failures in one product do not abort others.
# A cross-product summary is printed at the end.
#
# Key CME differences vs Eurex orchestrator:
#   - Date format for orders/trades cleaners: YYYY-MM-DD (not YYYYMMDD)
#   - Date format for lob1 builder: YYYYMMDD (same as Eurex)
#   - orders/trades cleaners don't accept --data-root / --output-root
#     (paths are hardcoded in the scripts, same convention as Eurex)
#   - --full-session flag available for orders/trades (RTH is default)
#
# To add NIY/NKD: add entries in cme_config.py, this script picks them up
# automatically via AVAILABLE_PRODUCTS.
#
# Exit code: 0 if all steps succeeded for all products, 1 if any step failed.

import argparse
import subprocess
import sys
import time

from cme_config import AVAILABLE_PRODUCTS

# ---------------------------------------------------------------------------
# STEPS CONFIG
# ---------------------------------------------------------------------------

AVAILABLE_STEPS = ["orders", "trades", "lob1"]

# Map step name -> script path (relative to repo root)
STEP_SCRIPTS = {
    "orders": "cleaning/clean_cme_orders.py",
    "trades": "cleaning/clean_cme_trades.py",
    "lob1":   "cleaning/build_cme_lob1.py",
}

# ---------------------------------------------------------------------------
# ARGUMENT PARSING
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="CME Globex cleaning pipeline: orders → trades → lob1",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_cme_pipeline.py --products ES
  python run_cme_pipeline.py --products ES --from-date 2025-10-01 --to-date 2025-10-31
  python run_cme_pipeline.py --products ES --steps orders trades
  python run_cme_pipeline.py --products ES --dry-run
  python run_cme_pipeline.py --products ES --no-validate
  python run_cme_pipeline.py --products ES --full-session
        """,
    )
    parser.add_argument(
        "--products", nargs="+", required=True,
        choices=AVAILABLE_PRODUCTS,
        help="Products to process (e.g. ES)",
    )
    parser.add_argument(
        "--steps", nargs="+", default=AVAILABLE_STEPS,
        choices=AVAILABLE_STEPS,
        help=f"Steps to run (default: all). Choices: {AVAILABLE_STEPS}",
    )
    parser.add_argument(
        "--from-date", default=None,
        help="Start date inclusive (YYYY-MM-DD for orders/trades, auto-converted for lob1)",
    )
    parser.add_argument(
        "--to-date", default=None,
        help="End date inclusive (YYYY-MM-DD for orders/trades, auto-converted for lob1)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Pass --dry-run to orders and trades steps (no files written). lob1 is skipped.",
    )
    parser.add_argument(
        "--no-validate", action="store_true",
        help="Pass --no-validate to orders, trades, and lob1 steps",
    )
    parser.add_argument(
        "--full-session", action="store_true",
        help="Use full electronic session instead of RTH (passed to orders and trades)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# DATE FORMAT HELPERS
# ---------------------------------------------------------------------------

def _to_dash(date_str: str) -> str:
    """Ensure date is in YYYY-MM-DD format (orders/trades cleaners expect this)."""
    if date_str and "-" not in date_str:
        # Convert YYYYMMDD -> YYYY-MM-DD
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    return date_str


def _to_compact(date_str: str) -> str:
    """Ensure date is in YYYYMMDD format (lob1 builder expects this)."""
    if date_str and "-" in date_str:
        return date_str.replace("-", "")
    return date_str


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

    # Base command
    cmd = [sys.executable, script, "--product", product]

    if step in ("orders", "trades"):
        # CME orders/trades cleaners expect YYYY-MM-DD dates
        if args.from_date:
            cmd += ["--from-date", _to_dash(args.from_date)]
        if args.to_date:
            cmd += ["--to-date", _to_dash(args.to_date)]
        if args.full_session:
            cmd.append("--full-session")
        if args.dry_run:
            cmd.append("--dry-run")
        if args.no_validate:
            cmd.append("--no-validate")

    elif step == "lob1":
        # lob1 builder expects YYYYMMDD dates
        if args.from_date:
            cmd += ["--from-date", _to_compact(args.from_date)]
        if args.to_date:
            cmd += ["--to-date", _to_compact(args.to_date)]
        if args.no_validate:
            cmd.append("--no-validate")
        # lob1 skipped entirely in dry-run — no clean files exist yet
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

    session_label = "full electronic" if args.full_session else "RTH"
    print(f"\n{'='*60}")
    print(f"  run_cme_pipeline.py")
    print(f"  Products  : {args.products}")
    print(f"  Steps     : {steps_to_run}")
    print(f"  Session   : {session_label}")
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