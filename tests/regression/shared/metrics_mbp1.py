"""Shared metrics for MBP-1 reconstruction regression tests.

The fast suite keeps the historical 50k-row sample checksum, while the R0
safety net performs a separate full-file fingerprint. Metric extraction here is
bounded-memory and no longer pretends that orphan counters are observable when
they are not persisted by the reconstruction engine.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from config import DATA_RECONSTRUCTED, REPO_ROOT  # noqa: E402
from ingestion.schema import reconstructed_path  # noqa: E402


GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"
BOLD = "\033[1m"

SAMPLE_ROWS = 50_000
READ_BATCH_ROWS = 100_000
CHECKSUM_COLS = [
    "ts_event",
    "ts_recv",
    "action",
    "side",
    "price",
    "flags",
    "sequence",
    "subsequence",
    "bid_px_00",
    "ask_px_00",
    "bid_sz_00",
    "ask_sz_00",
    "bid_ct_00",
    "ask_ct_00",
]

# orphan_cancel/orphan_modify are intentionally absent until build_mbp1 persists
# them. Historical values were hard-coded zeros and therefore provided no
# regression protection.
EXACT_METRICS = [
    "row_count",
    "burst_rate",
    "bid_px_min",
    "bid_px_max",
    "ask_px_min",
    "ask_px_max",
    "ts_event_min",
    "ts_event_max",
    "sample_checksum_sha256",
]

_PRODUCT_VENUE: dict[str, tuple[str, str]] = {
    "ES": ("CME", "databento"),
    "NIY": ("CME", "databento"),
    "NKD": ("CME", "databento"),
    "FDAX": ("EUREX", "databento"),
    "FESX": ("EUREX", "databento"),
    "FSMI": ("EUREX", "databento"),
    "HSI": ("HKEX", "HKEX"),
    "MHI": ("HKEX", "HKEX"),
    "HHI": ("HKEX", "HKEX"),
    "MCH": ("HKEX", "HKEX"),
}


def mbp1_path(product: str, contract: str, date_str: str) -> Path:
    """Return the reconstructed MBP-1 Parquet path for one day."""
    if product not in _PRODUCT_VENUE:
        raise ValueError(
            f"Unknown product '{product}'. Add it to _PRODUCT_VENUE in metrics_mbp1.py."
        )

    venue, provider = _PRODUCT_VENUE[product]
    compact = date_str.replace("-", "")
    return reconstructed_path(
        base_dir=DATA_RECONSTRUCTED,
        provider=provider,
        venue=venue,
        product=product,
        contract=contract,
        year=int(compact[:4]),
        month=int(compact[4:6]),
        date_str=compact,
        schema="mbp1",
    )


def sha256_table_sample(table: pa.Table, n_rows: int, cols: list[str]) -> str:
    """Hash the first ``n_rows`` of selected columns."""
    h = hashlib.sha256()
    sliced = table.slice(0, min(n_rows, len(table)))
    for col in cols:
        if col not in sliced.schema.names:
            continue
        arr = sliced.column(col)
        if pa.types.is_dictionary(arr.type):
            arr = arr.cast(pa.string())
        h.update(repr(arr.to_pylist()).encode())
    return h.hexdigest()


def _update_min(current: float | int | None, candidate: float | int | None):
    if candidate is None:
        return current
    return candidate if current is None else min(current, candidate)


def _update_max(current: float | int | None, candidate: float | int | None):
    if candidate is None:
        return current
    return candidate if current is None else max(current, candidate)


def extract_metrics(
    product: str,
    contract: str,
    date_str: str,
    pipeline_version: str = "unknown",
) -> dict[str, Any]:
    """Extract deterministic MBP-1 metrics in bounded Arrow batches."""
    metrics: dict[str, Any] = {
        "product": product,
        "contract": contract,
        "date": date_str,
        "pipeline_version": pipeline_version,
    }

    path = mbp1_path(product, contract, date_str)
    if not path.exists():
        metrics["row_count"] = None
        return metrics

    parquet = pq.ParquetFile(path)
    metrics["row_count"] = parquet.metadata.num_rows

    action_dist: dict[str, int] = {}
    side_dist: dict[str, int] = {}
    bid_min = bid_max = ask_min = ask_max = None
    ts_min = ts_max = None
    sample_parts: list[pa.RecordBatch] = []
    sample_rows = 0

    columns = [name for name in CHECKSUM_COLS if name in parquet.schema_arrow.names]
    for batch in parquet.iter_batches(batch_size=READ_BATCH_ROWS, columns=columns):
        if "action" in batch.schema.names:
            arr = batch.column(batch.schema.get_field_index("action"))
            if pa.types.is_dictionary(arr.type):
                arr = arr.cast(pa.string())
            for value in arr.to_pylist():
                if value is not None:
                    action_dist[value] = action_dist.get(value, 0) + 1

        if "side" in batch.schema.names:
            arr = batch.column(batch.schema.get_field_index("side"))
            if pa.types.is_dictionary(arr.type):
                arr = arr.cast(pa.string())
            for value in arr.to_pylist():
                if value is not None:
                    side_dist[value] = side_dist.get(value, 0) + 1

        if "bid_px_00" in batch.schema.names:
            arr = batch.column(batch.schema.get_field_index("bid_px_00")).drop_null()
            if len(arr):
                bid_min = _update_min(bid_min, pc.min(arr).as_py())
                bid_max = _update_max(bid_max, pc.max(arr).as_py())

        if "ask_px_00" in batch.schema.names:
            arr = batch.column(batch.schema.get_field_index("ask_px_00")).drop_null()
            if len(arr):
                ask_min = _update_min(ask_min, pc.min(arr).as_py())
                ask_max = _update_max(ask_max, pc.max(arr).as_py())

        if "ts_event" in batch.schema.names:
            arr = batch.column(batch.schema.get_field_index("ts_event"))
            ts_min = _update_min(ts_min, pc.min(arr).as_py())
            ts_max = _update_max(ts_max, pc.max(arr).as_py())

        if sample_rows < SAMPLE_ROWS:
            take = min(SAMPLE_ROWS - sample_rows, len(batch))
            sample_parts.append(batch.slice(0, take))
            sample_rows += take

    metrics["action_distribution"] = dict(sorted(action_dist.items()))
    metrics["side_distribution"] = dict(sorted(side_dist.items()))
    metrics["bid_px_min"] = bid_min
    metrics["bid_px_max"] = bid_max
    metrics["ask_px_min"] = ask_min
    metrics["ask_px_max"] = ask_max
    metrics["ts_event_min"] = ts_min
    metrics["ts_event_max"] = ts_max

    # Compute burst rate directly in DuckDB without materializing the day.
    quoted_path = str(path).replace("'", "''")
    result = duckdb.execute(
        f"""
        SELECT
            COUNT(*) FILTER (
                WHERE (ts_recv, sequence) IN (
                    SELECT ts_recv, sequence
                    FROM read_parquet('{quoted_path}')
                    GROUP BY ts_recv, sequence
                    HAVING COUNT(*) > 1
                )
            ) * 1.0 / COUNT(*) AS burst_rate
        FROM read_parquet('{quoted_path}')
        """
    ).fetchone()
    metrics["burst_rate"] = (
        round(result[0], 6) if result and result[0] is not None else 0.0
    )

    if sample_parts:
        sample_table = pa.Table.from_batches(sample_parts)
        metrics["sample_checksum_sha256"] = sha256_table_sample(
            sample_table, SAMPLE_ROWS, CHECKSUM_COLS
        )
        metrics["sample_rows_used"] = len(sample_table)
    else:
        metrics["sample_checksum_sha256"] = hashlib.sha256(b"").hexdigest()
        metrics["sample_rows_used"] = 0

    # Explicitly unavailable until reconstruction persists these counters.
    metrics["orphan_cancel"] = None
    metrics["orphan_modify"] = None
    return metrics


def save_golden(
    metrics: dict[str, Any],
    golden_dir: Path,
    product: str,
    contract: str,
    date_str: str,
) -> Path:
    """Write a golden JSON file and return its path."""
    golden_dir.mkdir(parents=True, exist_ok=True)
    out_path = golden_dir / f"{product}_{contract}_{date_str}_metrics.json"
    with out_path.open("w") as fh:
        json.dump(metrics, fh, indent=2)
    return out_path


def load_golden(
    golden_dir: Path,
    product: str,
    contract: str,
    date_str: str,
) -> dict[str, Any] | None:
    """Load one golden JSON file."""
    path = golden_dir / f"{product}_{contract}_{date_str}_metrics.json"
    if not path.exists():
        return None
    with path.open() as fh:
        return json.load(fh)


def compare_metrics(
    golden: dict[str, Any],
    current: dict[str, Any],
    verbose: bool = False,
) -> list[str]:
    """Compare deterministic scalar/distribution metrics against a golden."""
    failures: list[str] = []
    passed: list[str] = []

    for key in EXACT_METRICS:
        g_val = golden.get(key)
        c_val = current.get(key)
        if g_val is None and c_val is None:
            continue
        if g_val is None or c_val is None:
            failures.append(f"{key}: golden={g_val} current={c_val} [one side missing]")
            continue
        if g_val != c_val:
            failures.append(f"{key}: golden={g_val} current={c_val} [EXACT MISMATCH]")
        else:
            passed.append(f"{key}: {c_val} ✓")

    for dict_key in ("action_distribution", "side_distribution"):
        g_dict = golden.get(dict_key)
        c_dict = current.get(dict_key)
        if g_dict is None and c_dict is None:
            continue
        if g_dict is None or c_dict is None:
            failures.append(
                f"{dict_key}: golden={g_dict} current={c_dict} [one side missing]"
            )
            continue

        dict_ok = True
        for key in sorted(set(g_dict) | set(c_dict)):
            g_val = g_dict.get(key, 0)
            c_val = c_dict.get(key, 0)
            if g_val != c_val:
                failures.append(
                    f"{dict_key}[{key}]: golden={g_val} current={c_val} [EXACT MISMATCH]"
                )
                dict_ok = False
        if dict_ok:
            passed.append(f"{dict_key}: {dict(g_dict)} total={sum(g_dict.values())} ✓")

    if verbose:
        for message in passed:
            print(f"    {GREEN}{message}{RESET}")
    return failures


def run_pipeline(product: str, contract: str, date_str: str) -> bool:
    """Re-run MBP-1 reconstruction for exactly one golden day."""
    if product not in _PRODUCT_VENUE:
        print(f"  {RED}[ERROR]{RESET} Unknown product '{product}'", file=sys.stderr)
        return False

    cmd = [
        sys.executable,
        "-m",
        "reconstruction.build_mbp1",
        "--product",
        product,
        "--contract",
        contract,
        "--date",
        date_str,
        "--overwrite",
    ]
    print(f"  [pipeline] {product} {contract} {date_str} ...", flush=True)
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT))
    if result.returncode != 0:
        print(f"  {RED}[ERROR]{RESET} pipeline failed for {product} {contract} {date_str}")
        print(result.stderr[-2_000:])
        return False
    return True
