"""Run one real-data R0 canary in an isolated temporary workspace.

The script reuses the production adapter, validator, ingestion and reconstruction
code, but writes outputs under ``--work-root`` instead of touching the user's
normalized/reconstructed corpus. It captures raw-input provenance, runtime
versions, validator/adapter/reconstruction diagnostics, deep output fingerprints,
and same-date comparisons against the existing local reference corpus.

No vendor data is copied into the repository.
"""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
import sys
from collections import Counter
from datetime import date
from importlib import metadata
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
from shared.fingerprint import file_sha256, semantic_parquet_fingerprint  # noqa: E402
from shared.metrics_mbo import (  # noqa: E402
    mbo_path as reference_mbo_path,
    rejected_path as reference_rejected_path,
)
from shared.metrics_mbp1 import mbp1_path as reference_mbp1_path  # noqa: E402
from cases import NORMALIZATION_CASES  # noqa: E402


BASELINE_COMMIT = "69ef65ca3c17df63a72e8fef371140b5b7bc0db0"


def _git_head() -> str | None:
    """Return the current checkout commit when git metadata is available."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=_REPO_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _package_version(name: str) -> str | None:
    """Return an installed package version without making it mandatory."""
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def _runtime_manifest() -> dict[str, str | None]:
    """Capture the runtime versions that can affect canary output."""
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "pyarrow": _package_version("pyarrow"),
        "duckdb": _package_version("duckdb"),
        "pandas": _package_version("pandas"),
        "databento": _package_version("databento"),
        "databento-dbn": _package_version("databento-dbn"),
    }


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


def _relative_to_data_raw(path: Path) -> str:
    """Return a machine-independent path when the file is under DATA_RAW."""
    try:
        return str(path.resolve().relative_to(DATA_RAW.resolve()))
    except ValueError:
        return path.name


def _raw_input_files(raw_source: Path, provider: str, date_str: str) -> list[Path]:
    """Return exactly the raw files consumed by the selected session canary."""
    if provider.lower() == "databento":
        return [raw_source]

    if provider.lower() == "hkex":
        compact = date_str.replace("-", "")
        orders = sorted(raw_source.glob(f"hkex-*_{compact}_orders.parquet"))
        trades = sorted(raw_source.glob(f"hkex-*_{compact}_trades.parquet"))
        if not orders:
            raise RuntimeError(
                f"No HKEX orders parquet found for {date_str} under {raw_source}"
            )
        # Mirror HKEXAdapter._open(): the first orders file and, if present,
        # the first trades file are the actual inputs consumed by the adapter.
        files = [orders[0]]
        if trades:
            files.append(trades[0])
        return files

    raise RuntimeError(f"Unsupported provider for raw manifest: {provider}")


def _raw_input_manifest(raw_source: Path, provider: str, date_str: str) -> list[dict[str, object]]:
    """Hash raw inputs in streaming mode without copying vendor data."""
    manifest: list[dict[str, object]] = []
    for path in _raw_input_files(raw_source, provider, date_str):
        manifest.append(
            {
                "relative_path": _relative_to_data_raw(path),
                "size_bytes": path.stat().st_size,
                "sha256": file_sha256(path),
            }
        )
    return manifest


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


def _compare_fingerprint(
    candidate: dict[str, object],
    reference_path: Path,
) -> dict[str, object]:
    """Compare one candidate parquet fingerprint with a same-date local reference."""
    if not reference_path.exists():
        return {
            "status": "NO_REFERENCE",
            "reference_exists": False,
            "reference_logical_name": reference_path.name,
        }

    reference = semantic_parquet_fingerprint(reference_path)
    fields = ("semantic_sha256", "row_count", "schema")
    differences = {
        field: {
            "candidate": candidate.get(field),
            "reference": reference.get(field),
        }
        for field in fields
        if candidate.get(field) != reference.get(field)
    }
    return {
        "status": "MATCH" if not differences else "MISMATCH",
        "reference_exists": True,
        "reference_logical_name": reference_path.name,
        "reference": reference,
        "differences": differences,
    }


def _compare_optional_parquet(
    candidate_path: Path,
    candidate_fp: dict[str, object] | None,
    reference_path: Path,
) -> dict[str, object]:
    """Compare optional outputs where file absence can itself be the expected state."""
    candidate_exists = candidate_path.exists()
    reference_exists = reference_path.exists()

    if not candidate_exists and not reference_exists:
        return {
            "status": "MATCH",
            "candidate_exists": False,
            "reference_exists": False,
            "reference_logical_name": reference_path.name,
        }
    if candidate_exists != reference_exists:
        return {
            "status": "MISMATCH",
            "candidate_exists": candidate_exists,
            "reference_exists": reference_exists,
            "reference_logical_name": reference_path.name,
            "differences": {"existence": True},
        }

    assert candidate_fp is not None
    result = _compare_fingerprint(candidate_fp, reference_path)
    result["candidate_exists"] = True
    return result


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
    raw_source = Path(raw_source)

    # Hash raw files before running the pipeline so the report proves which
    # immutable input bytes produced the candidate outputs.
    raw_inputs = _raw_input_manifest(raw_source, provider, date_str)

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

    ref_mbo = reference_mbo_path(product, contract, date_str)
    ref_rejected = reference_rejected_path(product, contract, date_str)
    ref_mbp1 = reference_mbp1_path(product, contract, date_str)
    same_date_baseline = {
        "normalization": _compare_fingerprint(normalized_fp, ref_mbo),
        "rejected": _compare_optional_parquet(rejected_file, rejected_fp, ref_rejected),
        "reconstruction": _compare_fingerprint(reconstruction_fp, ref_mbp1),
    }

    report = {
        "report_version": 2,
        "baseline_commit": BASELINE_COMMIT,
        "checkout_commit": _git_head(),
        "runtime": _runtime_manifest(),
        "product": product,
        "contract": contract,
        "date": date_str,
        "provider": provider,
        "venue": venue,
        "validation_mode": mode,
        "raw_source_pattern": _relative_to_data_raw(raw_source),
        "raw_inputs": raw_inputs,
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
        "same_date_baseline": same_date_baseline,
    }

    report_path = args.report or (work_dir / "canary_report.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = report_path.with_suffix(report_path.suffix + ".tmp")
    tmp.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    tmp.replace(report_path)

    print(f"Canary report: {report_path}")
    print(f"Raw inputs hashed: {len(raw_inputs)}")
    print(f"Normalized rows: {normalized_fp['row_count']}")
    print(f"Reconstructed rows: {reconstruction_fp['row_count']}")
    print(
        "Same-date baseline: "
        f"MBO={same_date_baseline['normalization']['status']} "
        f"rejected={same_date_baseline['rejected']['status']} "
        f"MBP1={same_date_baseline['reconstruction']['status']}"
    )
    print(
        "Orphans: "
        f"cancel={reconstruction_stats['n_orphan_cancel']} "
        f"modify={reconstruction_stats['n_orphan_modify']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
