"""
tests/regression/shared/metrics.py

Shared utilities for pipeline regression tests across all markets (Eurex, CME, HKEX, ...).

Provides:
  - Path resolution helpers (Hive-style Parquet convention)
  - Metric extraction from pipeline outputs (orders_clean, trades_clean, lob1)
  - SHA-256 checksum on a row sample
  - Golden file I/O (load / save JSON)
  - Metric comparison with configurable tolerances
  - Pipeline runner (subprocess wrapper)
  - Terminal colour helpers
"""

import hashlib
import json
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Number of rows sampled for the SHA-256 checksum — fast + discriminating
SAMPLE_ROWS = 50_000

# Columns used for the lob1 sample checksum.
# These are the columns most likely to silently regress after a pipeline change.
CHECKSUM_COLS = [
    "ts_recv",
    "bid_px",
    "ask_px",
    "bid_sz",
    "ask_sz",
    "mid_px",
    "spread",
    "tob_changed",
]

# Metrics that must match EXACTLY (zero tolerance).
# Row counts and checksums are fully deterministic on fixed input data.
EXACT_METRICS = [
    "orders_clean_row_count",
    "trades_clean_row_count",
    "trades_unpaired_aggressor_count",
    "lob1_row_count",
    "tob_change_count",
    "crossed_book_count",
    "lob1_sample_checksum_sha256",
]

# Metrics with a small relative tolerance (float-derived values).
# Spread metrics are raw int64 ticks — deterministic in practice, tolerance is precautionary.
TOLERANT_METRICS: dict[str, float] = {
    "tob_change_rate": 1e-6,
    "spread_median": 0.0,
    "spread_p25": 0.0,
    "spread_p75": 0.0,
    "spread_p95": 0.0,
}

# ANSI colour codes for terminal output
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


# ---------------------------------------------------------------------------
# Path resolution — Hive-style convention
# ---------------------------------------------------------------------------

def _hive_dir(repo_root: Path, data_root: str, product: str, date_str: str) -> Path:
    """
    Return the Hive-partitioned directory for a given data root, product and date.

    Convention (from CONTEXT.md section 12):
      orders_clean / trades_clean  → data/clean/product=X/year=Y/month=M/
      lob1                         → data/lob1/product=X/year=Y/month=M/
    """
    d = date.fromisoformat(date_str)
    return (
        repo_root
        / "data" / data_root
        / f"product={product}"
        / f"year={d.year}"
        / f"month={d.month:02d}"
    )


def lob1_path(repo_root: Path, product: str, date_str: str) -> Path:
    # lob1 lives under data/lob1/ — dedicated directory, output of build_eurex_lob1.py
    tag = date_str.replace("-", "")
    return _hive_dir(repo_root, "lob1", product, date_str) / f"{product}_{tag}_lob1.parquet"


def orders_clean_path(repo_root: Path, product: str, date_str: str) -> Path:
    tag = date_str.replace("-", "")
    return (
        _hive_dir(repo_root, "clean", product, date_str)
        / f"{product}_{tag}_orders_clean.parquet"
    )


def trades_clean_path(repo_root: Path, product: str, date_str: str) -> Path:
    tag = date_str.replace("-", "")
    return (
        _hive_dir(repo_root, "clean", product, date_str)
        / f"{product}_{tag}_trades_clean.parquet"
    )


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline(repo_root: Path, pipeline_script: Path, product: str, date_str: str) -> bool:
    """
    Run a cleaning pipeline script for one product/day via subprocess.

    Args:
        repo_root       : root of the destiny-research repo (used for display only)
        pipeline_script : absolute path to the orchestrator script
                          (e.g. cleaning/run_eurex_pipeline.py)
        product         : product ticker (e.g. "FDAX")
        date_str        : ISO date string (e.g. "2025-05-02")

    Returns True on success (exit code 0).
    """
    cmd = [
        sys.executable,
        str(pipeline_script),
        "--product", product,
        "--from-date", date_str,
        "--to-date", date_str,
    ]
    print(f"  [pipeline] {product} {date_str} ...", flush=True)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  {RED}[ERROR]{RESET} pipeline failed for {product} {date_str}")
        print(result.stderr[-2000:])  # last 2k chars to avoid flooding the terminal
        return False
    return True


# ---------------------------------------------------------------------------
# Checksum
# ---------------------------------------------------------------------------

def sha256_table_sample(table, n_rows: int, cols: list[str]) -> str:
    """
    Compute a SHA-256 fingerprint over the first n_rows of selected columns.

    Hashes the Python repr of each column's value list — deterministic and
    sensitive to both value changes and ordering changes (e.g. missing ORDER BY).
    """
    h = hashlib.sha256()
    actual_rows = min(n_rows, len(table))
    sliced = table.slice(0, actual_rows)
    for col in cols:
        if col not in sliced.schema.names:
            continue
        h.update(sliced.column(col).to_pylist().__repr__().encode())
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Metric extraction
# ---------------------------------------------------------------------------

def extract_metrics(
    repo_root: Path,
    product: str,
    date_str: str,
    pipeline_version: str = "unknown",
) -> dict[str, Any]:
    """
    Extract the statistical fingerprint from pipeline outputs for one product/day.

    Reads orders_clean, trades_clean, and lob1 Parquet files.
    Returns a dict suitable for JSON serialisation.

    Args:
        repo_root        : root of the destiny-research repo
        product          : product ticker
        date_str         : ISO date string
        pipeline_version : human-readable version tag (bump on intentional breaking changes)
    """
    metrics: dict[str, Any] = {
        "product": product,
        "date": date_str,
        "pipeline_version": pipeline_version,
    }

    # --- orders clean ---
    p = orders_clean_path(repo_root, product, date_str)
    if p.exists():
        tbl = pq.read_table(p)
        metrics["orders_clean_row_count"] = len(tbl)
        del tbl
    else:
        metrics["orders_clean_row_count"] = None

    # --- trades clean ---
    p = trades_clean_path(repo_root, product, date_str)
    if p.exists():
        tbl = pq.read_table(p)
        metrics["trades_clean_row_count"] = len(tbl)
        if "is_unpaired_aggressor" in tbl.schema.names:
            metrics["trades_unpaired_aggressor_count"] = (
                pc.sum(tbl.column("is_unpaired_aggressor")).as_py()
            )
        del tbl
    else:
        metrics["trades_clean_row_count"] = None

    # --- lob1 ---
    p = lob1_path(repo_root, product, date_str)
    if not p.exists():
        metrics["lob1_row_count"] = None
        return metrics

    tbl = pq.read_table(p)
    metrics["lob1_row_count"] = len(tbl)

    # TOB change rate
    if "tob_changed" in tbl.schema.names:
        n_tob = pc.sum(tbl.column("tob_changed")).as_py()
        metrics["tob_change_count"] = n_tob
        metrics["tob_change_rate"] = (
            round(n_tob / len(tbl), 6) if len(tbl) > 0 else 0.0
        )

    # Crossed books: bid >= ask on rows where both sides are populated
    if "bid_px" in tbl.schema.names and "ask_px" in tbl.schema.names:
        bid = tbl.column("bid_px").to_pylist()
        ask = tbl.column("ask_px").to_pylist()
        metrics["crossed_book_count"] = sum(
            1
            for b, a in zip(bid, ask)
            if b is not None and a is not None and b > 0 and a > 0 and b >= a
        )

    # Spread distribution (raw int64 ticks — no division)
    if "spread" in tbl.schema.names:
        spread_arr = tbl.column("spread").drop_null()
        spread_pos = spread_arr.filter(pc.greater(spread_arr, 0))
        if len(spread_pos) > 0:
            spread_np = np.array(spread_pos.to_pylist(), dtype=np.int64)
            metrics["spread_median"] = int(np.median(spread_np))
            metrics["spread_p25"]    = int(np.percentile(spread_np, 25))
            metrics["spread_p75"]    = int(np.percentile(spread_np, 75))
            metrics["spread_p95"]    = int(np.percentile(spread_np, 95))

    # SHA-256 checksum on first SAMPLE_ROWS rows of critical columns
    metrics["lob1_sample_checksum_sha256"] = sha256_table_sample(tbl, SAMPLE_ROWS, CHECKSUM_COLS)
    metrics["lob1_sample_rows_used"] = min(SAMPLE_ROWS, len(tbl))

    del tbl
    return metrics


# ---------------------------------------------------------------------------
# Golden file I/O
# ---------------------------------------------------------------------------

def save_golden(metrics: dict[str, Any], golden_dir: Path, product: str, date_str: str) -> Path:
    """Write metrics dict as a JSON golden file. Returns the path written."""
    golden_dir.mkdir(parents=True, exist_ok=True)
    out_path = golden_dir / f"{product}_{date_str}_metrics.json"
    with open(out_path, "w") as f:
        json.dump(metrics, f, indent=2)
    return out_path


def load_golden(golden_dir: Path, product: str, date_str: str) -> dict[str, Any] | None:
    """Load golden JSON for one product/day. Returns None if file not found."""
    path = golden_dir / f"{product}_{date_str}_metrics.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Metric comparison
# ---------------------------------------------------------------------------

def compare_metrics(
    golden: dict[str, Any],
    current: dict[str, Any],
    verbose: bool = False,
) -> list[str]:
    """
    Compare current metrics against golden reference.

    Returns a list of failure message strings.
    An empty list means all checks passed.

    Args:
        golden  : reference metrics loaded from golden JSON
        current : metrics extracted from current pipeline outputs
        verbose : if True, also print passing metrics to stdout
    """
    failures: list[str] = []
    passed:   list[str] = []

    # --- exact metrics ---
    for key in EXACT_METRICS:
        g_val = golden.get(key)
        c_val = current.get(key)

        if g_val is None and c_val is None:
            # Field not applicable for this product/day (e.g. no unpaired aggressors)
            continue
        if g_val is None or c_val is None:
            failures.append(f"{key}: golden={g_val}  current={c_val}  [one side missing]")
            continue
        if g_val != c_val:
            failures.append(f"{key}: golden={g_val}  current={c_val}  [EXACT MISMATCH]")
        else:
            passed.append(f"{key}: {c_val}  ✓")

    # --- tolerant metrics ---
    for key, rel_tol in TOLERANT_METRICS.items():
        g_val = golden.get(key)
        c_val = current.get(key)

        if g_val is None and c_val is None:
            continue
        if g_val is None or c_val is None:
            failures.append(f"{key}: golden={g_val}  current={c_val}  [one side missing]")
            continue

        if rel_tol == 0.0:
            ok = (g_val == c_val)
        else:
            denom = abs(g_val) if g_val != 0 else 1
            ok = abs(g_val - c_val) / denom <= rel_tol

        if not ok:
            pct = abs(g_val - c_val) / (abs(g_val) or 1) * 100
            failures.append(
                f"{key}: golden={g_val}  current={c_val}"
                f"  diff={pct:.4f}%  [EXCEEDS TOL {rel_tol}]"
            )
        else:
            passed.append(f"{key}: {c_val}  ✓")

    if verbose:
        for p in passed:
            print(f"    {GREEN}{p}{RESET}")

    return failures