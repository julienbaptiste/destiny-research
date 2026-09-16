"""Run one real-data R0 canary in an isolated temporary workspace.

The script reuses the production adapter, validator, ingestion and reconstruction
code, but writes outputs under ``--work-root`` instead of touching the user's
normalized/reconstructed corpus. It captures validator/adapter/reconstruction
diagnostics and deep fingerprints in one JSON report.

No vendor data is copied into the repository.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from collections import Counter
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "tests" / "regression"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import DATA_RAW  # noqa: E402
from ingestion import ingest as ingest_module  # noqa: E402
from ingestion.adapters.databento_adapter import DatabentoAdapter  # noqa: E402
from ingestion.adapters.hkex_adapter import HKEXAdapter  # noqa: E402
from ingestion.market_config import MARKET_CONFIG  # noqa: E402
from ingestion.schema import (  # noqa: E402
    ValidationMode,
    normalized_path,
    rejected_path,
)
from reconstruction.build_mbp1 import reconstruct_day  # noqa: E402
from shared.fingerprint import semantic_parquet_fingerprint  # noqa: E402
from cases import NORMALIZATION_CASES  # noqa: E402


BASELINE_COMMIT = "69ef65ca3c17df63a72e8fef371140b5b7bc0db0"


def _git_head() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=_REPO_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _resolve_raw(product: str, date_str: str) -> tuple[object, Path, str]:
    """Return (adapter, raw source, validation mode) for one canary day."""
    cfg = MARKET_CONFIG[product]
    venue = cfg["exchange"]
    provider = cfg["provider"]
    compact = date_str.replace("-", "")

    if provider.lower() == "databento":
        root = DATA_RAW / "provider=databento" / f"venue={venue}" / f"product={product}"
        matches = sorted(root.rglob(f"*{compact}*.mbo.dbn.zst")) if root.exists() else []
        if len(matches) != 1:
            raise RuntimeError(
                f"Expected exactly one raw Databento MBO file for {product} {date_str}; "
                f"found {len(matches)} under {root}"
            )
        return DatabentoAdapter(), matches[0], ValidationMode.STRICT

    if provider.lower() == "hkex":
        d = date.fromisoformat(date_str)
        root = (
            DATA_RAW
            / "provider=HKEX"
            / "venue=HKEX"
            / f"product={product}"
            / f"year={d.year}"
            / f"month={d.month:02d}"
        )
        if not root.exists():
            raise RuntimeError(f"HKEX raw directory not found: {root}")
        return HKEXAdapter(), root, ValidationMode.LOOSE

    raise RuntimeError(f"Unsupported provider for safety-net canary: {provider}")


def _reason_distribution(path: Path) -> dict[str, int]:
    """Count rejected reasons in bounded Arrow batches."""
    if not path.exists():
        return {}
    parquet = pq.ParquetFile(path)
    if "reject_reason" not in parquet.schema_arrow.names:
        return {}

    counts: Counter[str] = Counter()
    for batch in parquet.iter_batches(batch_size=100_000, columns=["reject_reason"]):
        arr = batch.column(0)
        if pa.types.is_dictionary(arr.type):
            arr = arr.cast(pa.string())
        counts.update(value for value in arr.to_pylist() if value is not None)
    return dict(sorted(counts.items()))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run an isolated real-data R0 normalization/reconstruction canary."
    )
    parser.add_argument("--product", required=True, choices=sorted(NORMALIZATION_CASES))
    parser.add_argument(
        "--work-root",
        type=Path,
        default=Path("/tmp/destiny_r0_canary"),
        help="Temporary output root; the selected product/date subdir is replaced.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional report path. Defaults inside the canary workspace.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    product = args.product
    contract, date_str = NORMALIZATION_CASES[product]
    session_date = date.fromisoformat(date_str)
    cfg = MARKET_CONFIG[product]

    adapter, raw_source, mode = _resolve_raw(product, date_str)
    provider = adapter.PROVIDER
    venue = cfg["exchange"]

    work_dir = args.work_root / f"{product}_{date_str}"
    if work_dir.exists():
        shutil.rmtree(work_dir)
    normalized_root = work_dir / "normalized"
    reconstructed_root = work_dir / "reconstructed"
    normalized_root.mkdir(parents=True, exist_ok=True)
    reconstructed_root.mkdir(parents=True, exist_ok=True)

    captured_validator: dict[str, dict[str, object]] = {}
    original_log_stats = ingest_module.log_stats

    def _capture_log_stats(state, state_contract: str, state_date: str) -> None:
        captured_validator[state_contract] = {
            "date": state_date,
            "mode": state.mode,
            "n_validated": state.n_validated,
            "n_rejected": state.n_rejected,
            "n_warmup_skip": state.n_warmup_skip,
        }
        original_log_stats(state, state_contract, state_date)

    ingest_module.log_stats = _capture_log_stats
    try:
        counts = ingest_module.ingest_file(
            adapter=adapter,
            raw_path=raw_source,
            normalized_dir=normalized_root,
            session_date=session_date,
            mode=mode,
            verbose=True,
        )
    finally:
        ingest_module.log_stats = original_log_stats

    compact = date_str.replace("-", "")
    mbo_file = normalized_path(
        base_dir=normalized_root,
        provider=provider,
        venue=venue,
        product=product,
        contract=contract,
        year=session_date.year,
        month=session_date.month,
        date_str=compact,
    )
    rejected_file = rejected_path(
        base_dir=normalized_root,
        provider=provider,
        venue=venue,
        product=product,
        contract=contract,
        year=session_date.year,
        month=session_date.month,
        date_str=compact,
    )

    if not mbo_file.exists():
        raise RuntimeError(
            f"Target normalized file was not produced: {mbo_file}. "
            f"Produced contract counts: {counts}"
        )

    pre_postprocess_fp = semantic_parquet_fingerprint(mbo_file)
    hkex_postprocess_stats = None

    if provider.lower() == "hkex":
        # The current historical production path post-processes HKEX Delete+Add
        # pairs into MODIFY. Keep that behavior in the legacy canary until R0
        # explicitly changes it.
        from ingestion.post_process_hkex_ingestion import process_file

        temp_processed = mbo_file.with_name(mbo_file.stem + "_postprocessed.parquet")
        hkex_postprocess_stats = process_file(
            mbo_path=mbo_file,
            processed_path=temp_processed,
            overwrite=True,
        )

    normalized_fp = semantic_parquet_fingerprint(mbo_file)
    rejected_fp = semantic_parquet_fingerprint(rejected_file) if rejected_file.exists() else None

    mbp1_file = reconstructed_root / f"{contract}_{compact}_mbp1.parquet"
    reconstruction_stats = reconstruct_day(
        mbo_file=mbo_file,
        out_file=mbp1_file,
        product=product,
        contract=contract,
    )
    reconstruction_fp = semantic_parquet_fingerprint(mbp1_file)

    report = {
        "report_version": 1,
        "baseline_commit": BASELINE_COMMIT,
        "checkout_commit": _git_head(),
        "product": product,
        "contract": contract,
        "date": date_str,
        "provider": provider,
        "venue": venue,
        "validation_mode": mode,
        "raw_source_name": Path(raw_source).name,
        "ingestion_contract_counts": counts,
        "adapter_stats": adapter.get_stats(),
        "validator_stats": captured_validator.get(contract),
        "reject_reason_distribution": _reason_distribution(rejected_file),
        "normalization_pre_hkex_postprocess": pre_postprocess_fp,
        "hkex_postprocess_stats": hkex_postprocess_stats,
        "normalization": normalized_fp,
        "rejected": rejected_fp,
        "reconstruction_stats": reconstruction_stats,
        "reconstruction": reconstruction_fp,
    }

    report_path = args.report or (work_dir / "canary_report.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = report_path.with_suffix(report_path.suffix + ".tmp")
    tmp.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    tmp.replace(report_path)

    print(f"Canary report: {report_path}")
    print(f"Normalized rows: {normalized_fp['row_count']}")
    print(f"Reconstructed rows: {reconstruction_fp['row_count']}")
    print(
        "Orphans: "
        f"cancel={reconstruction_stats['n_orphan_cancel']} "
        f"modify={reconstruction_stats['n_orphan_modify']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
