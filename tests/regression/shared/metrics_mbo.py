"""Shared metrics for MBO normalization regression tests.

The fast golden suite remains intentionally lightweight, while the R0 safety
net adds a separate full-file semantic fingerprint. This module is responsible
for exact day selection, bounded-memory metric extraction and deterministic
legacy golden comparisons.

Candidate note: CHECKSUM_COLS includes the canonical R0.1 ordering/provenance
fields. Historical pre-R0 checksum goldens are therefore legacy evidence, not a
candidate merge gate; corrected goldens are generated only after differential
classification.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))
from config import DATA_NORMALIZED, DATA_RAW, REPO_ROOT  # noqa: E402


SAMPLE_ROWS = 50_000
READ_BATCH_ROWS = 100_000
CHECKSUM_COLS = [
    "ts_event",
    "ts_recv",
    "action",
    "side",
    "price",
    "size",
    "order_id",
    "flags",
    "norm_flags",
    "sequence",
    "subsequence",
]

# warmup_skip_count is deliberately not asserted here. The current pipeline
# increments it only in ValidatorState and does not persist it to Parquet, so
# historical golden values were synthetic zeros rather than observed data.
EXACT_METRICS = [
    "row_count",
    "rejected_count",
    "price_min",
    "price_max",
    "ts_event_min",
    "ts_event_max",
    "sample_checksum_sha256",
]

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"
BOLD = "\033[1m"


# Storage provider is case-sensitive because it is part of the Hive path.
_PRODUCT_VENUE: dict[str, tuple[str, str]] = {
    "ES": ("CME", "databento"),
    "NIY": ("CME", "databento"),
    "NKD": ("CME", "databento"),
    "FDAX": ("EUREX", "databento"),
    "FESX": ("EUREX", "databento"),
    "FSMI": ("EUREX", "databento"),
    "HHI": ("HKEX", "HKEX"),
    "HSI": ("HKEX", "HKEX"),
    "MCH": ("HKEX", "HKEX"),
    "MHI": ("HKEX", "HKEX"),
}


def normalized_dir(product: str, contract: str, date_str: str) -> Path:
    """Return the normalized Hive directory for one contract/day."""
    if product not in _PRODUCT_VENUE:
        raise ValueError(
            f"Unknown product '{product}'. Add it to _PRODUCT_VENUE in metrics_mbo.py."
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
    """Return the normalized MBO Parquet path for one day."""
    tag = date_str.replace("-", "")
    return normalized_dir(product, contract, date_str) / f"{contract}_{tag}_mbo.parquet"


def rejected_path(product: str, contract: str, date_str: str) -> Path:
    """Return the rejected-event Parquet path for one day."""
    tag = date_str.replace("-", "")
    return normalized_dir(product, contract, date_str) / f"{contract}_{tag}_rejected.parquet"


def _raw_databento_file(product: str, venue: str, provider: str, date_str: str) -> Path | None:
    """Resolve exactly one Databento raw MBO file for the requested golden day."""
    tag = date_str.replace("-", "")
    root = DATA_RAW / f"provider={provider}" / f"venue={venue}" / f"product={product}"
    if not root.exists():
        return None

    matches = sorted(root.rglob(f"*{tag}*.mbo.dbn.zst"))
    if len(matches) != 1:
        if len(matches) > 1:
            print(
                f"  {RED}[ERROR]{RESET} expected one raw file for {product} {date_str}, "
                f"found {len(matches)}: {matches}",
                file=sys.stderr,
            )
        return None
    return matches[0]


def run_pipeline(product: str, date_str: str) -> bool:
    """Re-run normalization for exactly the requested regression day.

    This intentionally avoids the old behavior where a 'deep' check invoked a
    full product batch and often skipped existing outputs. Only the golden day
    is overwritten.
    """
    if product not in _PRODUCT_VENUE:
        print(f"  {RED}[ERROR]{RESET} Unknown product '{product}'", file=sys.stderr)
        return False

    venue, storage_provider = _PRODUCT_VENUE[product]
    provider_cli = storage_provider.lower()

    if provider_cli == "hkex":
        cmd = [
            sys.executable,
            "-m",
            "ingestion.ingest",
            "hkex",
            "--product",
            product,
            "--date",
            date_str,
            "--mode",
            "LOOSE",
            "--overwrite",
        ]
    else:
        raw_path = _raw_databento_file(product, venue, storage_provider, date_str)
        if raw_path is None:
            print(
                f"  {RED}[ERROR]{RESET} raw Databento MBO file not uniquely resolved "
                f"for {product} {date_str}",
                file=sys.stderr,
            )
            return False
        cmd = [
            sys.executable,
            "-m",
            "ingestion.ingest",
            "file",
            str(raw_path),
            "--mode",
            "STRICT",
            "--overwrite",
        ]

    print(f"  [pipeline] {product} {date_str} ...", flush=True)
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT))
    if result.returncode != 0:
        print(f"  {RED}[ERROR]{RESET} pipeline failed for {product} {date_str}")
        print(result.stderr[-2_000:])
        return False
    return True


def sha256_table_sample(table: pa.Table, n_rows: int, cols: list[str]) -> str:
    """Hash the first ``n_rows`` of selected columns for the fast golden check."""
    h = hashlib.sha256()
    actual_rows = min(n_rows, len(table))
    sliced = table.slice(0, actual_rows)
    for col in cols:
        if col not in sliced.schema.names:
            continue
        arr = sliced.column(col)
        if pa.types.is_dictionary(arr.type):
            arr = arr.cast(pa.string())
        h.update(repr(arr.to_pylist()).encode())
    return h.hexdigest()


def _update_min(current: int | None, candidate: int | None) -> int | None:
    if candidate is None:
        return current
    return candidate if current is None else min(current, candidate)


def _update_max(current: int | None, candidate: int | None) -> int | None:
    if candidate is None:
        return current
    return candidate if current is None else max(current, candidate)


def extract_metrics(
    product: str,
    contract: str,
    date_str: str,
    pipeline_version: str = "unknown",
) -> dict[str, Any]:
    """Extract deterministic MBO metrics in bounded Arrow batches."""
    metrics: dict[str, Any] = {
        "product": product,
        "contract": contract,
        "date": date_str,
        "pipeline_version": pipeline_version,
    }

    mbo_p = mbo_path(product, contract, date_str)
    if not mbo_p.exists():
        metrics["row_count"] = None
        return metrics

    parquet = pq.ParquetFile(mbo_p)
    metrics["row_count"] = parquet.metadata.num_rows

    action_dist: dict[str, int] = {}
    side_dist: dict[str, int] = {}
    price_min: int | None = None
    price_max: int | None = None
    ts_min: int | None = None
    ts_max: int | None = None
    sample_parts: list[pa.RecordBatch] = []
    sample_rows = 0

    columns = [name for name in CHECKSUM_COLS if name in parquet.schema_arrow.names]

    for batch in parquet.iter_batches(batch_size=READ_BATCH_ROWS, columns=columns):
        action_arr = batch.column(batch.schema.get_field_index("action"))
        side_arr = batch.column(batch.schema.get_field_index("side"))
        if pa.types.is_dictionary(action_arr.type):
            action_arr = action_arr.cast(pa.string())
        if pa.types.is_dictionary(side_arr.type):
            side_arr = side_arr.cast(pa.string())

        for action in action_arr.to_pylist():
            if action is not None:
                action_dist[action] = action_dist.get(action, 0) + 1
        for side in side_arr.to_pylist():
            if side is not None:
                side_dist[side] = side_dist.get(side, 0) + 1

        price_arr = batch.column(batch.schema.get_field_index("price"))
        valid_prices = price_arr.filter(pc.not_equal(price_arr, 9_223_372_036_854_775_807))
        if len(valid_prices):
            price_min = _update_min(price_min, pc.min(valid_prices).as_py())
            price_max = _update_max(price_max, pc.max(valid_prices).as_py())

        ts_arr = batch.column(batch.schema.get_field_index("ts_event"))
        ts_min = _update_min(ts_min, pc.min(ts_arr).as_py())
        ts_max = _update_max(ts_max, pc.max(ts_arr).as_py())

        if sample_rows < SAMPLE_ROWS:
            take = min(SAMPLE_ROWS - sample_rows, len(batch))
            sample_parts.append(batch.slice(0, take))
            sample_rows += take

    metrics["action_distribution"] = dict(sorted(action_dist.items()))
    metrics["side_distribution"] = dict(sorted(side_dist.items()))
    metrics["price_min"] = price_min
    metrics["price_max"] = price_max
    metrics["ts_event_min"] = ts_min
    metrics["ts_event_max"] = ts_max

    if sample_parts:
        sample_table = pa.Table.from_batches(sample_parts)
        metrics["sample_checksum_sha256"] = sha256_table_sample(
            sample_table, SAMPLE_ROWS, CHECKSUM_COLS
        )
        metrics["sample_rows_used"] = len(sample_table)
    else:
        metrics["sample_checksum_sha256"] = hashlib.sha256(b"").hexdigest()
        metrics["sample_rows_used"] = 0

    rej_p = rejected_path(product, contract, date_str)
    if rej_p.exists():
        rejected = pq.ParquetFile(rej_p)
        metrics["rejected_count"] = rejected.metadata.num_rows
        reason_dist: dict[str, int] = {}
        if "reject_reason" in rejected.schema_arrow.names:
            for batch in rejected.iter_batches(
                batch_size=READ_BATCH_ROWS, columns=["reject_reason"]
            ):
                for reason in batch.column(0).to_pylist():
                    if reason is not None:
                        reason_dist[reason] = reason_dist.get(reason, 0) + 1
        metrics["reject_reason_distribution"] = dict(sorted(reason_dist.items()))
    else:
        metrics["rejected_count"] = 0
        metrics["reject_reason_distribution"] = {}

    # Not currently persisted by ingestion; explicitly mark unavailable rather
    # than manufacturing the historical zero that previously looked authoritative.
    metrics["warmup_skip_count"] = None
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
