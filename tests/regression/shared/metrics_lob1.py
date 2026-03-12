"""
tests/regression/shared/metrics_lob1.py

Shared utilities for LOB1 pipeline regression tests.

Adapted from metrics.py for the v2 schema:
  - Column names: bid_px_00, ask_px_00, bid_sz_00, ask_sz_00, bid_ct_00, ask_ct_00
  - Path convention: data/lob1/product=X/year=Y/month=MM/X_YYYYMMDD_<symbol>_lob1.parquet
  - Per-instrument files (one file per symbol per day)
  - No dependency on clean orders/trades (LOB1 reads from raw MBO)

Provides:
  - Path resolution (Hive-style, per-instrument)
  - Metric extraction from LOB1 Parquet output
  - SHA-256 checksum on row sample
  - Golden file I/O (JSON)
  - Metric comparison with configurable tolerances
  - Pipeline runner (subprocess wrapper for build_cme_lob1.py)
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

# Number of rows sampled for SHA-256 checksum
SAMPLE_ROWS = 50_000

# Columns used for the lob1 v2 sample checksum.
# Aligned with Databento MBP-1 schema + our derived fields.
CHECKSUM_COLS = [
    "ts_recv",
    "bid_px_00",
    "ask_px_00",
    "bid_sz_00",
    "ask_sz_00",
    "bid_ct_00",
    "ask_ct_00",
    "mid_px",
    "spread",
    "tob_changed",
]

# Metrics that must match EXACTLY (zero tolerance).
EXACT_METRICS = [
    "lob1_row_count",
    "tob_change_count",
    "crossed_book_count",
    "action_A_count",
    "action_C_count",
    "action_M_count",
    "action_T_count",
    "action_F_count",
    "lob1_sample_checksum_sha256",
]

# Metrics with small relative tolerance (float-derived).
TOLERANT_METRICS: dict[str, float] = {
    "tob_change_rate": 1e-6,
    "spread_median": 0.0,
    "spread_p25": 0.0,
    "spread_p75": 0.0,
    "spread_p95": 0.0,
}

# ANSI colour codes
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


# ---------------------------------------------------------------------------
# Path resolution — Hive-style, per-instrument
# ---------------------------------------------------------------------------

def lob1_path(repo_root: Path, product: str, date_str: str, symbol: str) -> Path:
    """Return path to LOB1 Parquet for one instrument/day.

    Convention:
      data/lob1/product=ES/year=2025/month=10/ES_20251001_ESZ5_lob1.parquet
    """
    d = date.fromisoformat(date_str)
    tag = date_str.replace("-", "")
    return (
        repo_root / "data" / "lob1"
        / f"product={product}"
        / f"year={d.year}"
        / f"month={d.month:02d}"
        / f"{product}_{tag}_{symbol}_lob1.parquet"
    )


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline(
    repo_root: Path,
    pipeline_script: Path,
    product: str,
    date_str: str,
    symbol: str,
) -> bool:
    """Run the LOB1 builder for one product/day/symbol via subprocess.

    Returns True on success (exit code 0).
    """
    tag = date_str.replace("-", "")
    cmd = [
        sys.executable,
        str(pipeline_script),
        "--product", product,
        "--from-date", tag,
        "--to-date", tag,
        "--symbol", symbol,
    ]
    print(f"  [pipeline] {product} {symbol} {date_str} ...", flush=True)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  {RED}[ERROR]{RESET} pipeline failed for {product} {symbol} {date_str}")
        print(result.stderr[-2000:])
        return False
    return True


# ---------------------------------------------------------------------------
# Checksum
# ---------------------------------------------------------------------------

def sha256_table_sample(table, n_rows: int, cols: list[str]) -> str:
    """Compute SHA-256 fingerprint over the first n_rows of selected columns."""
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
    symbol: str,
    pipeline_version: str = "unknown",
) -> dict[str, Any]:
    """Extract the statistical fingerprint from LOB1 output for one instrument/day.

    Returns a dict suitable for JSON serialisation.
    """
    metrics: dict[str, Any] = {
        "product": product,
        "symbol": symbol,
        "date": date_str,
        "pipeline_version": pipeline_version,
    }

    p = lob1_path(repo_root, product, date_str, symbol)
    if not p.exists():
        metrics["lob1_row_count"] = None
        return metrics

    tbl = pq.read_table(p)
    metrics["lob1_row_count"] = len(tbl)

    # Action distribution
    if "action" in tbl.schema.names:
        action_col = tbl.column("action").to_pylist()
        from collections import Counter
        action_counts = Counter(action_col)
        for act in ("A", "C", "M", "T", "F", "R"):
            metrics[f"action_{act}_count"] = action_counts.get(act, 0)
        del action_col, action_counts

    # TOB change rate
    if "tob_changed" in tbl.schema.names:
        n_tob = pc.sum(tbl.column("tob_changed")).as_py()
        metrics["tob_change_count"] = n_tob
        metrics["tob_change_rate"] = (
            round(n_tob / len(tbl), 6) if len(tbl) > 0 else 0.0
        )

    # Crossed books: bid_px_00 >= ask_px_00 where both populated
    if "bid_px_00" in tbl.schema.names and "ask_px_00" in tbl.schema.names:
        bid = tbl.column("bid_px_00")
        ask = tbl.column("ask_px_00")
        # Use PyArrow compute for memory efficiency (no .to_pylist() on full columns)
        both_valid = pc.and_(pc.is_valid(bid), pc.is_valid(ask))
        both_positive = pc.and_(pc.greater(bid, 0), pc.greater(ask, 0))
        mask = pc.and_(both_valid, both_positive)
        crossed = pc.greater_equal(bid.filter(mask), ask.filter(mask))
        metrics["crossed_book_count"] = pc.sum(crossed).as_py()

    # Spread distribution (raw int64 fixed-point)
    if "spread" in tbl.schema.names:
        spread_arr = tbl.column("spread").drop_null()
        spread_pos = spread_arr.filter(pc.greater(spread_arr, 0))
        if len(spread_pos) > 0:
            # Use PyArrow quantile for memory efficiency
            metrics["spread_median"] = int(pc.quantile(spread_pos, q=[0.5])[0].as_py())
            metrics["spread_p25"]    = int(pc.quantile(spread_pos, q=[0.25])[0].as_py())
            metrics["spread_p75"]    = int(pc.quantile(spread_pos, q=[0.75])[0].as_py())
            metrics["spread_p95"]    = int(pc.quantile(spread_pos, q=[0.95])[0].as_py())

    # SHA-256 checksum on first SAMPLE_ROWS of critical columns
    metrics["lob1_sample_checksum_sha256"] = sha256_table_sample(tbl, SAMPLE_ROWS, CHECKSUM_COLS)
    metrics["lob1_sample_rows_used"] = min(SAMPLE_ROWS, len(tbl))

    del tbl
    return metrics


# ---------------------------------------------------------------------------
# Golden file I/O
# ---------------------------------------------------------------------------

def save_golden(
    metrics: dict[str, Any],
    golden_dir: Path,
    product: str,
    date_str: str,
    symbol: str,
) -> Path:
    """Write metrics dict as a JSON golden file. Returns the path written."""
    golden_dir.mkdir(parents=True, exist_ok=True)
    tag = date_str.replace("-", "")
    out_path = golden_dir / f"{product}_{tag}_{symbol}_metrics.json"
    with open(out_path, "w") as f:
        json.dump(metrics, f, indent=2)
    return out_path


def load_golden(
    golden_dir: Path,
    product: str,
    date_str: str,
    symbol: str,
) -> dict[str, Any] | None:
    """Load golden JSON for one product/day/symbol. Returns None if not found."""
    tag = date_str.replace("-", "")
    path = golden_dir / f"{product}_{tag}_{symbol}_metrics.json"
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
    """Compare current metrics against golden reference.

    Returns a list of failure message strings. Empty list = all passed.
    """
    failures: list[str] = []
    passed:   list[str] = []

    # --- exact metrics ---
    for key in EXACT_METRICS:
        g_val = golden.get(key)
        c_val = current.get(key)

        if g_val is None and c_val is None:
            continue
        if g_val is None or c_val is None:
            failures.append(f"{key}: golden={g_val}  current={c_val}  [one side missing]")
            continue
        if g_val != c_val:
            failures.append(f"{key}: golden={g_val}  current={c_val}  [EXACT MISMATCH]")
        else:
            passed.append(f"{key}: {c_val}")

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
            passed.append(f"{key}: {c_val}")

    if verbose:
        for p in passed:
            print(f"    {GREEN}{p}{RESET}")

    return failures