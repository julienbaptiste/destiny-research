"""
tests/regression/shared/metrics_mbo.py

Shared utilities for regression testing of the MBO normalization pipeline.

Provides:
  - Path resolution for normalized MBO Parquet files (new Hive convention)
  - Metric extraction from normalized MBO outputs
  - SHA-256 checksum on a deterministic row sample
  - Golden file I/O (load / save JSON)
  - Metric comparison with configurable tolerances
  - Pipeline runner (subprocess wrapper around ingest.py CLI)
  - Terminal colour helpers

Metrics captured per product/day:
  Exact (zero tolerance — deterministic on fixed input):
    row_count, rejected_count, warmup_skip_count,
    action_distribution, side_distribution,
    price_min, price_max, ts_event_min, ts_event_max,
    sample_checksum_sha256

  Informational (not compared, stored for reference):
    sample_rows_used
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow.compute as pc
import pyarrow.parquet as pq

# config.py lives at repo root — tests/regression/shared/ is 3 levels deep
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))
from config import DATA_NORMALIZED, REPO_ROOT  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Number of rows sampled for the SHA-256 checksum.
# 50K rows is fast to read and highly discriminating for ordering/value regressions.
SAMPLE_ROWS = 50_000

# Columns used for the sample checksum — the columns most likely to regress
# silently after a pipeline change (parsing, dedup, flag filtering).
CHECKSUM_COLS = [
    "ts_event",
    "ts_recv",
    "action",
    "side",
    "price",
    "size",
    "order_id",
    "flags",
]

# Metrics that must match EXACTLY.
# All MBO metrics are deterministic on fixed input data (integer counts and
# fixed-point prices — no floating-point arithmetic involved).
EXACT_METRICS = [
    "row_count",
    "rejected_count",
    "warmup_skip_count",
    "price_min",
    "price_max",
    "ts_event_min",
    "ts_event_max",
    "sample_checksum_sha256",
]

# action_distribution and side_distribution are dicts — compared separately
# in compare_metrics() with exact equality on each key.

# ANSI colour codes for terminal output
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


# ---------------------------------------------------------------------------
# Path resolution — uses DATA_NORMALIZED from config.py
# ---------------------------------------------------------------------------
# data/normalized/provider=databento/venue=<VENUE>/product=<PRODUCT>/
#     contract=<CONTRACT>/year=<Y>/month=<MM>/
#     <CONTRACT>_<YYYYMMDD>_mbo.parquet
#     <CONTRACT>_<YYYYMMDD>_rejected.parquet
#
# The contract (front month symbol) must be supplied by the caller —
# it is not derivable from product + date without reading the instrument map.
# For golden file purposes we hardcode the front month per product/date
# in GOLDEN_CONFIG (generate_golden.py).
# ---------------------------------------------------------------------------

# Venue + provider mapping: product → (venue, provider)
# Mirrors ingestion/market_config.py — extend here when adding HKEX products.
_PRODUCT_VENUE: dict[str, tuple[str, str]] = {
    "ES":   ("CME",   "databento"),
    "NIY":  ("CME",   "databento"),
    "NKD":  ("CME",   "databento"),
    "FDAX": ("EUREX", "databento"),
    "FESX": ("EUREX", "databento"),
    "FSMI": ("EUREX", "databento"),
    # HKEX products — add once hkex_adapter.py is implemented
    "HHI": ("HKEX", "HKEX"),
    "HSI": ("HKEX", "HKEX"),
    "MCH": ("HKEX", "HKEX"),
    "MHI": ("HKEX", "HKEX"),
}


def normalized_dir(product: str, contract: str, date_str: str) -> Path:
    """Return the normalized Hive directory for a given product/contract/date.

    Builds the path from DATA_NORMALIZED (config.py) — no repo_root needed.

    Args:
        product:  product ticker (e.g. 'ES', 'FDAX').
        contract: front-month contract symbol (e.g. 'ESZ25', 'FDAXM25').
        date_str: ISO date string 'YYYY-MM-DD'.

    Returns:
        Absolute Path to the directory containing the normalized Parquet files.

    Raises:
        ValueError: if product is not in _PRODUCT_VENUE.
    """
    if product not in _PRODUCT_VENUE:
        raise ValueError(
            f"Unknown product '{product}'. "
            f"Add it to _PRODUCT_VENUE in metrics_mbo.py."
        )
    venue, provider = _PRODUCT_VENUE[product]
    d = date.fromisoformat(date_str)
    return (
        DATA_NORMALIZED
        / f"provider={provider}"
        / f"venue={venue}"
        / f"product={product}"
        / f"contract={contract}"
        / f"year={d.year}"
        / f"month={d.month:02d}"
    )


def mbo_path(product: str, contract: str, date_str: str) -> Path:
    """Return the absolute path to the normalized MBO Parquet file for one day."""
    tag = date_str.replace("-", "")
    return normalized_dir(product, contract, date_str) / f"{contract}_{tag}_mbo.parquet"


def rejected_path(product: str, contract: str, date_str: str) -> Path:
    """Return the absolute path to the rejected events Parquet file for one day."""
    tag = date_str.replace("-", "")
    return normalized_dir(product, contract, date_str) / f"{contract}_{tag}_rejected.parquet"


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline(product: str, date_str: str) -> bool:
    """Run ingest.py batch for one product/day via subprocess.

    Calls: python ingestion/ingest.py batch --provider databento
               --venue <VENUE> --product <PRODUCT>
               (--overwrite=False by default — existing outputs are skipped)

    For regression testing we assume the normalized file for date_str already
    exists on disk. Use --skip-pipeline in generate_golden.py / check_regression.py
    to skip this step entirely.

    Args:
        product:  product ticker.
        date_str: ISO date string (used for display only).

    Returns:
        True on success (exit code 0), False otherwise.
    """
    if product not in _PRODUCT_VENUE:
        print(f"  {RED}[ERROR]{RESET} Unknown product '{product}'", file=sys.stderr)
        return False

    venue, provider = _PRODUCT_VENUE[product]
    if provider == "hkex":
        # HKEX uses its own subcommand with --date instead of batch
        cmd = [
            sys.executable,
            "-m", "ingestion.ingest",
            "hkex",
            "--product", product,
            "--date",    date_str,
            "--mode",    "LOOSE",
        ]
    else:
        cmd = [
            sys.executable,
            "-m", "ingestion.ingest",
            "batch",
            "--provider", provider,
            "--venue",    venue,
            "--product",  product,
        ]
    print(f"  [pipeline] {product} {date_str} ...", flush=True)
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT))
    if result.returncode != 0:
        print(f"  {RED}[ERROR]{RESET} pipeline failed for {product} {date_str}")
        print(result.stderr[-2_000:])
        return False
    return True


# ---------------------------------------------------------------------------
# Checksum
# ---------------------------------------------------------------------------

def sha256_table_sample(table, n_rows: int, cols: list[str]) -> str:
    """Compute a SHA-256 fingerprint over the first n_rows of selected columns.

    Hashes the Python repr of each column's value list — deterministic and
    sensitive to both value changes and row-ordering changes (e.g. a missing
    ORDER BY in the pipeline would be caught immediately).

    Args:
        table:  PyArrow Table.
        n_rows: number of rows to sample from the start of the table.
        cols:   column names to include in the hash.

    Returns:
        Lowercase hex SHA-256 digest string.
    """
    h = hashlib.sha256()
    actual_rows = min(n_rows, len(table))
    sliced = table.slice(0, actual_rows)
    for col in cols:
        if col not in sliced.schema.names:
            # Column may not exist for all products (e.g. sequence on HKEX)
            continue
        # repr of Python list is deterministic for int/str/None types
        h.update(repr(sliced.column(col).to_pylist()).encode())
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Metric extraction
# ---------------------------------------------------------------------------

def extract_metrics(
    product: str,
    contract: str,
    date_str: str,
    pipeline_version: str = "unknown",
) -> dict[str, Any]:
    """Extract the statistical fingerprint from normalized MBO outputs for one day.

    Reads the MBO Parquet file and the rejected Parquet file (if present).
    Paths are resolved via DATA_NORMALIZED from config.py.
    Returns a dict suitable for JSON serialisation and golden file comparison.

    All metrics are deterministic on fixed input data:
      - Integer counts (row_count, rejected_count, warmup_skip_count)
      - Action/side distributions (dict of string → int)
      - Fixed-point price bounds (int64)
      - Timestamp bounds (uint64 nanoseconds)
      - SHA-256 checksum over first SAMPLE_ROWS rows

    Args:
        product:          product ticker (e.g. 'ES').
        contract:         front-month contract symbol (e.g. 'ESZ25').
        date_str:         ISO date string 'YYYY-MM-DD'.
        pipeline_version: version tag — bump manually after intentional breaking changes.

    Returns:
        Dict with all metrics. Missing files are recorded as None rather than raising.
    """
    metrics: dict[str, Any] = {
        "product":          product,
        "contract":         contract,
        "date":             date_str,
        "pipeline_version": pipeline_version,
    }

    # --- MBO normalized file ---
    mbo_p = mbo_path(product, contract, date_str)
    if not mbo_p.exists():
        # Record missing file — check_regression.py will report setup error
        metrics["row_count"] = None
        return metrics

    tbl = pq.ParquetFile(mbo_p).read()
    metrics["row_count"] = len(tbl)

    # Action distribution — exact count per action value
    # Expected keys: ADD, CANCEL, MODIFY, TRADE, FILL, CLEAR, NONE
    if "action" in tbl.schema.names:
        actions = tbl.column("action").to_pylist()
        dist: dict[str, int] = {}
        for a in actions:
            if a is not None:
                dist[a] = dist.get(a, 0) + 1
        # Sort by key for deterministic JSON serialisation
        metrics["action_distribution"] = dict(sorted(dist.items()))

    # Side distribution — BID / ASK / NONE
    if "side" in tbl.schema.names:
        sides = tbl.column("side").to_pylist()
        sdist: dict[str, int] = {}
        for s in sides:
            if s is not None:
                sdist[s] = sdist.get(s, 0) + 1
        metrics["side_distribution"] = dict(sorted(sdist.items()))

    # Price bounds (fixed-point int64) — excludes sentinel INT64_MAX
    if "price" in tbl.schema.names:
        INT64_MAX = 9_223_372_036_854_775_807
        price_col = tbl.column("price")
        # Filter out sentinel prices before computing bounds
        valid_mask = pc.not_equal(price_col, INT64_MAX)
        valid_prices = price_col.filter(valid_mask)
        if len(valid_prices) > 0:
            metrics["price_min"] = pc.min(valid_prices).as_py()
            metrics["price_max"] = pc.max(valid_prices).as_py()

    # Timestamp bounds (uint64 nanoseconds UTC)
    if "ts_event" in tbl.schema.names:
        ts_col = tbl.column("ts_event")
        metrics["ts_event_min"] = pc.min(ts_col).as_py()
        metrics["ts_event_max"] = pc.max(ts_col).as_py()

    # SHA-256 checksum on first SAMPLE_ROWS rows of critical columns
    metrics["sample_checksum_sha256"] = sha256_table_sample(tbl, SAMPLE_ROWS, CHECKSUM_COLS)
    metrics["sample_rows_used"] = min(SAMPLE_ROWS, len(tbl))  # informational only

    del tbl

    # --- Rejected file ---
    rej_p = rejected_path(product, contract, date_str)
    if rej_p.exists():
        rej_tbl = pq.ParquetFile(rej_p).read()
        metrics["rejected_count"] = len(rej_tbl)

        # warmup_skip_count: rows with reason == 'warmup_skip' (GTC orphan CANCELs)
        if "reason" in rej_tbl.schema.names:
            reasons = rej_tbl.column("reason").to_pylist()
            metrics["warmup_skip_count"] = sum(1 for r in reasons if r == "warmup_skip")
        else:
            metrics["warmup_skip_count"] = 0

        del rej_tbl
    else:
        # No rejected file means 0 rejections (ES case — fully clean)
        metrics["rejected_count"] = 0
        metrics["warmup_skip_count"] = 0

    return metrics


# ---------------------------------------------------------------------------
# Golden file I/O
# ---------------------------------------------------------------------------

def save_golden(
    metrics: dict[str, Any],
    golden_dir: Path,
    product: str,
    contract: str,
    date_str: str,
) -> Path:
    """Write metrics dict as a JSON golden file. Returns the path written.

    Filename convention: <PRODUCT>_<CONTRACT>_<DATE>_metrics.json
    e.g. ES_ESZ25_2025-10-01_metrics.json
    """
    golden_dir.mkdir(parents=True, exist_ok=True)
    out_path = golden_dir / f"{product}_{contract}_{date_str}_metrics.json"
    with open(out_path, "w") as f:
        json.dump(metrics, f, indent=2)
    return out_path


def load_golden(
    golden_dir: Path,
    product: str,
    contract: str,
    date_str: str,
) -> dict[str, Any] | None:
    """Load golden JSON for one product/contract/day. Returns None if not found."""
    path = golden_dir / f"{product}_{contract}_{date_str}_metrics.json"
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

    Returns a list of failure message strings.
    An empty list means all checks passed.

    Handles:
      - Scalar exact metrics (EXACT_METRICS list)
      - Dict metrics: action_distribution, side_distribution (key-by-key exact)

    Args:
        golden:  reference metrics loaded from golden JSON.
        current: metrics extracted from current pipeline outputs.
        verbose: if True, also print passing metrics to stdout.

    Returns:
        List of human-readable failure strings (empty = all passed).
    """
    failures: list[str] = []
    passed:   list[str] = []

    # --- scalar exact metrics ---
    for key in EXACT_METRICS:
        g_val = golden.get(key)
        c_val = current.get(key)

        if g_val is None and c_val is None:
            continue  # metric not applicable for this product/day
        if g_val is None or c_val is None:
            failures.append(
                f"{key}: golden={g_val}  current={c_val}  [one side missing]"
            )
            continue
        if g_val != c_val:
            failures.append(
                f"{key}: golden={g_val}  current={c_val}  [EXACT MISMATCH]"
            )
        else:
            passed.append(f"{key}: {c_val}  ✓")

    # --- dict metrics: action_distribution, side_distribution ---
    for dict_key in ("action_distribution", "side_distribution"):
        g_dict = golden.get(dict_key)
        c_dict = current.get(dict_key)

        if g_dict is None and c_dict is None:
            continue
        if g_dict is None or c_dict is None:
            failures.append(
                f"{dict_key}: golden={g_dict}  current={c_dict}  [one side missing]"
            )
            continue

        # Check all keys present in golden exist in current with exact counts
        all_keys = sorted(set(g_dict) | set(c_dict))
        dict_ok = True
        for k in all_keys:
            g_v = g_dict.get(k, 0)
            c_v = c_dict.get(k, 0)
            if g_v != c_v:
                failures.append(
                    f"{dict_key}[{k}]: golden={g_v}  current={c_v}  [EXACT MISMATCH]"
                )
                dict_ok = False

        if dict_ok:
            total = sum(g_dict.values())
            passed.append(f"{dict_key}: {dict(g_dict)}  total={total}  ✓")

    if verbose:
        for p in passed:
            print(f"    {GREEN}{p}{RESET}")

    return failures