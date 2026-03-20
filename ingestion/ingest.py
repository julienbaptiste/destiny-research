"""
ingest.py — Ingestion orchestrator: RAW → NORMALIZED.

This is the top-level entry point for the ingestion pipeline.
It coordinates the adapter, validator, and Parquet writers to produce
normalized MBO parquet files from raw provider-specific feeds.

Pipeline per file:
    1. Open adapter session (load raw file, build instrument map)
    2. List all instruments in the file → create per-instrument writers
    3. Stream events through adapter.translate() → validator.validate_event()
    4. Route clean events to per-instrument Parquet writers
    5. Write rejected events to per-instrument audit logs
    6. Print end-of-session stats

Output layout (per instrument, per day):
    data/normalized/provider=databento/venue=CME/product=ES/contract=ESZ25/
        year=2025/month=10/ESZ25_20251027_mbo.parquet
    data/normalized/provider=databento/venue=CME/product=ES/contract=ESZ25/
        year=2025/month=10/ESZ25_20251027_rejected.parquet

Design decisions:
    - One ParquetWriter per (contract, rejected/clean) pair, opened lazily
      on first event for that contract. This avoids creating empty files
      for contracts that have zero events on a given day.
    - Events are buffered in memory (BATCH_SIZE rows) before flushing to
      Parquet. This amortizes the Arrow conversion cost.
    - Warmup events (F_SNAPSHOT) are written to the clean output with their
      flags intact. They are NOT filtered out — the reconstruction engine
      uses them to bootstrap book state. The validator runs in warmup_mode
      during this phase (orphan CANCELs silently ignored).
    - On CLEAR action: validator state is reset, warmup_mode stays False
      (mid-session CLEAR is not a warmup event).

Memory management:
    - Buffers are flushed every BATCH_SIZE events and cleared.
    - Peak memory per instrument = BATCH_SIZE × ~200 bytes ≈ 100MB at 500k.
    - With 10+ instruments per file, keep BATCH_SIZE ≤ 100_000 on 16GB RAM.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq

from adapters.base import BaseAdapter, ContractInfo, SessionConfig
from schema import (
    Action,
    Flags,
    ValidationMode,
    NORMALIZED_MBO_SCHEMA,
    REJECTED_EVENTS_SCHEMA,
    normalized_path,
    rejected_path,
)
from validator import ValidatorState, validate_event, print_stats


# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

# Number of events to buffer before flushing to Parquet.
# Tuned for 16GB RAM with up to ~15 simultaneous instrument writers.
# Each event dict ≈ 150-200 bytes → 100k × 200B × 15 instruments ≈ 300MB peak.
BATCH_SIZE: int = 100_000

# Parquet compression — zstd level 3 is a good balance for tick data:
# better ratio than snappy, faster than gzip, negligible CPU overhead.
PARQUET_COMPRESSION     : str = "zstd"
PARQUET_COMPRESSION_LVL : int = 3


# ---------------------------------------------------------------------------
# WRITER CONTEXT
# ---------------------------------------------------------------------------

class _WriterContext:
    """
    Holds open Parquet writers and event buffers for one contract.

    Opened lazily on first event — avoids empty files for contracts
    with no events on a given day.

    Flushed every BATCH_SIZE events and on close().
    """

    __slots__ = (
        "clean_writer", "rejected_writer",
        "clean_buf", "rejected_buf",
        "clean_path", "rejected_path",
        "n_clean", "n_rejected",
    )

    def __init__(
        self,
        clean_path    : Path,
        rejected_path : Path,
    ) -> None:
        self.clean_path    = clean_path
        self.rejected_path = rejected_path

        # Writers opened lazily on first write
        self.clean_writer    : pq.ParquetWriter | None = None
        self.rejected_writer : pq.ParquetWriter | None = None

        self.clean_buf    : list[dict] = []
        self.rejected_buf : list[dict] = []

        self.n_clean    : int = 0
        self.n_rejected : int = 0

    def _ensure_clean_writer(self) -> None:
        if self.clean_writer is None:
            self.clean_path.parent.mkdir(parents=True, exist_ok=True)
            self.clean_writer = pq.ParquetWriter(
                self.clean_path,
                NORMALIZED_MBO_SCHEMA,
                compression=PARQUET_COMPRESSION,
                compression_level=PARQUET_COMPRESSION_LVL,
            )

    def _ensure_rejected_writer(self) -> None:
        if self.rejected_writer is None:
            self.rejected_path.parent.mkdir(parents=True, exist_ok=True)
            self.rejected_writer = pq.ParquetWriter(
                self.rejected_path,
                REJECTED_EVENTS_SCHEMA,
                compression=PARQUET_COMPRESSION,
                compression_level=PARQUET_COMPRESSION_LVL,
            )

    def append_clean(self, event: dict) -> None:
        self.clean_buf.append(event)
        self.n_clean += 1
        if len(self.clean_buf) >= BATCH_SIZE:
            self.flush_clean()

    def append_rejected(self, row: dict) -> None:
        self.rejected_buf.append(row)
        self.n_rejected += 1
        if len(self.rejected_buf) >= BATCH_SIZE:
            self.flush_rejected()

    def flush_clean(self) -> None:
        if not self.clean_buf:
            return
        self._ensure_clean_writer()
        table = pa.Table.from_pylist(self.clean_buf, schema=NORMALIZED_MBO_SCHEMA)
        self.clean_writer.write_table(table)
        self.clean_buf.clear()

    def flush_rejected(self) -> None:
        if not self.rejected_buf:
            return
        self._ensure_rejected_writer()
        table = pa.Table.from_pylist(self.rejected_buf, schema=REJECTED_EVENTS_SCHEMA)
        self.rejected_writer.write_table(table)
        self.rejected_buf.clear()

    def close(self) -> None:
        """Flush remaining buffers and close writers."""
        self.flush_clean()
        self.flush_rejected()
        if self.clean_writer:
            self.clean_writer.close()
        if self.rejected_writer:
            self.rejected_writer.close()


# ---------------------------------------------------------------------------
# REJECTED ROW BUILDER
# ---------------------------------------------------------------------------

def _build_rejected_row(event: dict, reason: str, mode: str) -> dict:
    """
    Build a row for the rejected events audit log from a normalized event dict.
    All fields nullable — a malformed event may be missing some fields.
    """
    return {
        "ts_event"      : event.get("ts_event",      0),
        "ts_recv"       : event.get("ts_recv",        0),
        "venue"         : event.get("venue",          ""),
        "product"       : event.get("product",        ""),
        "contract"      : event.get("contract",       ""),
        "action"        : event.get("action",         None),
        "side"          : event.get("side",           None),
        "price"         : event.get("price",          None),
        "size"          : event.get("size",           None),
        "order_id"      : event.get("order_id",       None),
        "flags"         : event.get("flags",          None),
        "sequence"      : event.get("sequence",       None),
        "publisher_id"  : event.get("publisher_id",   None),
        "instrument_id" : event.get("instrument_id",  None),
        "reject_reason" : reason,
        "mode"          : mode,
    }


# ---------------------------------------------------------------------------
# CORE INGESTION FUNCTION
# ---------------------------------------------------------------------------

def ingest_file(
    adapter       : BaseAdapter,
    raw_path      : Path,
    normalized_dir: Path,
    session_date  : date,
    mode          : str = ValidationMode.STRICT,
    verbose       : bool = True,
) -> dict[str, int]:
    """
    Ingest one raw provider file into normalized Parquet files.

    Args:
        adapter:        Initialized adapter instance (DatabentoAdapter, etc.)
        raw_path:       Path to the raw provider file (.dbn.zst, etc.)
        normalized_dir: Root of the normalized data tree
                        (e.g. Path("/media/julien/HDD/data/normalized"))
        session_date:   Calendar date of this file (used for symbol resolution
                        and output path construction)
        mode:           ValidationMode.STRICT or ValidationMode.LOOSE
        verbose:        Print progress and stats to stdout

    Returns:
        dict: per-contract event counts {"ESZ25": 1_234_567, ...}

    Raises:
        FileNotFoundError: if raw_path does not exist
        AssertionError:    if adapter session lifecycle is violated
    """
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    provider  = adapter.PROVIDER
    date_str  = session_date.strftime("%Y%m%d")
    year      = session_date.year
    month     = session_date.month

    # ------------------------------------------------------------------
    # 1. Open adapter session
    # ------------------------------------------------------------------
    config = SessionConfig(
        session_date    = session_date,
        warmup_enabled  = True,   # always enable — adapter detects via F_SNAPSHOT
        validation_mode = mode,
    )
    adapter.open_session(raw_path, config)

    if verbose:
        print(f"[ingest] {provider} | {raw_path.name} | {session_date} | mode={mode}")

    # ------------------------------------------------------------------
    # 2. Pre-build instrument map from metadata
    #    Falls back to lazy resolution if list_instruments() returns empty.
    # ------------------------------------------------------------------
    instruments   : dict[int, ContractInfo] = {}  # instrument_id → ContractInfo
    writer_ctx    : dict[str, _WriterContext] = {} # contract → _WriterContext
    validator_states: dict[str, ValidatorState] = {} # contract → ValidatorState

    known_contracts = adapter.list_instruments()
    for info in known_contracts:
        _register_instrument(
            info, provider, normalized_dir, date_str, year, month,
            mode, instruments, writer_ctx, validator_states, verbose,
        )

    if verbose:
        print(f"[ingest]   {len(known_contracts)} instruments found in metadata")

    # ------------------------------------------------------------------
    # 3. Stream events
    # ------------------------------------------------------------------
    n_total   = 0
    n_dropped = 0  # None from adapter (heartbeats, non-MBO records)

    for event in adapter.iter_events():

        # adapter.translate() returns None for irrelevant records
        if event is None:
            n_dropped += 1
            continue

        n_total += 1
        instrument_id = event.get("instrument_id", 0)
        contract      = event.get("contract", "")

        # Lazy instrument registration — handles contracts not in metadata
        if instrument_id not in instruments:
            info = adapter.resolve_contract(instrument_id, session_date)
            if info is None:
                # Truly unknown instrument — skip silently
                n_dropped += 1
                continue
            _register_instrument(
                info, provider, normalized_dir, date_str, year, month,
                mode, instruments, writer_ctx, validator_states, verbose,
            )

        ctx   = writer_ctx[contract]
        vstate = validator_states[contract]

        # --- Warmup boundary detection ---
        # While F_SNAPSHOT is set, we are in warmup mode.
        # On the first non-snapshot event, signal warmup end to validator.
        flags     = event.get("flags", 0)
        is_warmup = bool(flags & Flags.F_SNAPSHOT)

        if not is_warmup and vstate.warmup_mode:
            vstate.warmup_end()
            if verbose:
                print(
                    f"[ingest]   {contract} warmup ended "
                    f"(ts_event={event['ts_event']})"
                )

        # --- Validate ---
        is_clean, reason = validate_event(event, vstate)

        if is_clean:
            ctx.append_clean(event)
        else:
            rejected_row = _build_rejected_row(event, reason, mode)
            ctx.append_rejected(rejected_row)

            if mode == ValidationMode.LOOSE:
                # In LOOSE mode: flag the event and keep it in clean output.
                # F_BAD_TS (0x04) repurposed as generic anomaly flag.
                flagged = dict(event)
                flagged["flags"] = flags | 0x04
                ctx.append_clean(flagged)

    # ------------------------------------------------------------------
    # 4. Close all writers and print stats
    # ------------------------------------------------------------------
    counts: dict[str, int] = {}

    for contract, ctx in writer_ctx.items():
        ctx.close()
        counts[contract] = ctx.n_clean

    if verbose:
        print(
            f"[ingest]   total={n_total:,} | "
            f"dropped(pre-validation)={n_dropped:,} | "
            f"instruments={len(writer_ctx)}"
        )
        for contract, vstate in validator_states.items():
            print_stats(vstate, contract, date_str)

    adapter.close_session()
    return counts


# ---------------------------------------------------------------------------
# BATCH INGESTION
# ---------------------------------------------------------------------------

def ingest_product(
    adapter       : BaseAdapter,
    raw_dir       : Path,
    normalized_dir: Path,
    mode          : str  = ValidationMode.STRICT,
    verbose       : bool = True,
    overwrite     : bool = False,
) -> None:
    """
    Ingest all raw files for a product directory.

    Scans raw_dir recursively for files matching the adapter's expected
    extension (*.dbn.zst for Databento). Processes files in chronological
    order (sorted by filename).

    Args:
        adapter:        Adapter instance — reused across files (re-opened
                        per file via open_session/close_session).
        raw_dir:        Product-level raw directory, e.g.:
                        /media/julien/HDD/data/raw/provider=databento/
                            venue=CME/product=ES/
        normalized_dir: Root normalized directory.
        mode:           ValidationMode.STRICT or LOOSE.
        verbose:        Print progress per file.
        overwrite:      If False, skip files whose normalized output already
                        exists. Allows resuming interrupted batch runs.
    """
    # Collect raw files sorted chronologically
    # Match only MBO files — exclude mbp1, mbp10, and other schemas
    # that may coexist in the same raw directory.
    raw_files = sorted(raw_dir.rglob("*.mbo.dbn.zst"))

    if not raw_files:
        print(f"[ingest] No .dbn.zst files found in {raw_dir}", file=sys.stderr)
        return

    print(f"[ingest] Found {len(raw_files)} files to process in {raw_dir}")

    for raw_path in raw_files:
        # Extract date from filename: "glbx-mdp3-20251027.mbo.dbn.zst"
        session_date = _extract_date_from_filename(raw_path)
        if session_date is None:
            print(f"[ingest] WARNING: cannot extract date from {raw_path.name} — skipping")
            continue

        # Skip if output already exists and overwrite=False.
        # Scope the search to the product subtree under normalized_dir —
        # NOT the full tree — because multiple products share the same
        # raw filename convention (e.g. xeur-eobi-20250502.mbo.dbn.zst
        # exists for FDAX, FESX and FSMI simultaneously).
        if not overwrite:
            date_str = session_date.strftime("%Y%m%d")
            # Extract provider/venue/product from raw_dir path components.
            # raw_dir is e.g. .../raw/provider=databento/venue=EUREX/product=FDAX/
            # We reconstruct the corresponding normalized product subtree.
            provider_part = next(
                (p for p in raw_dir.parts if p.startswith("provider=")), None
            )
            venue_part = next(
                (p for p in raw_dir.parts if p.startswith("venue=")), None
            )
            product_part = next(
                (p for p in raw_dir.parts if p.startswith("product=")), None
            )
            if provider_part and venue_part and product_part:
                # Scoped search: only look under this specific product subtree
                scoped_dir = (
                    normalized_dir
                    / provider_part
                    / venue_part
                    / product_part
                )
                existing = list(scoped_dir.rglob(f"*_{date_str}_mbo.parquet"))
            else:
                # Fallback: raw_dir structure doesn't match expected layout —
                # search the full normalized tree (may cause false skips on
                # same-named files across products, but better than crashing)
                existing = list(normalized_dir.rglob(f"*_{date_str}_mbo.parquet"))
            if existing:
                if verbose:
                    print(f"[ingest]   {raw_path.name} → already normalized, skipping")
                continue

        try:
            ingest_file(
                adapter        = adapter,
                raw_path       = raw_path,
                normalized_dir = normalized_dir,
                session_date   = session_date,
                mode           = mode,
                verbose        = verbose,
            )
        except Exception as exc:
            # Log error and continue — do not abort the batch
            print(
                f"[ingest] ERROR processing {raw_path.name}: {exc}",
                file=sys.stderr,
            )
            # Ensure adapter session is closed even on error
            try:
                adapter.close_session()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# INTERNAL HELPERS
# ---------------------------------------------------------------------------

def _register_instrument(
    info           : ContractInfo,
    provider       : str,
    normalized_dir : Path,
    date_str       : str,
    year           : int,
    month          : int,
    mode           : str,
    instruments    : dict,
    writer_ctx     : dict,
    validator_states: dict,
    verbose        : bool,
) -> None:
    """
    Register a new instrument: create writer context and validator state.
    Idempotent — safe to call multiple times for the same instrument.
    """
    if info.instrument_id in instruments:
        return

    instruments[info.instrument_id] = info

    # Only create writer/validator once per contract
    # (multiple instrument_ids can map to the same contract — rare but possible)
    if info.contract in writer_ctx:
        return

    # Build output paths
    clean_p = normalized_path(
        base_dir = normalized_dir,
        provider = provider,
        venue    = info.venue,
        product  = info.product,
        contract = info.contract,
        year     = year,
        month    = month,
        date_str = date_str,
    )
    rej_p = rejected_path(
        base_dir = normalized_dir,
        provider = provider,
        venue    = info.venue,
        product  = info.product,
        contract = info.contract,
        year     = year,
        month    = month,
        date_str = date_str,
    )

    writer_ctx[info.contract]      = _WriterContext(clean_p, rej_p)
    validator_states[info.contract] = ValidatorState(mode=mode, warmup_mode=True)

    if verbose:
        label = " [SPREAD]" if info.is_spread else ""
        print(f"[ingest]   registered {info.contract}{label} → {clean_p.name}")


def _extract_date_from_filename(path: Path) -> date | None:
    """
    Extract session date from a raw filename.

    Supported formats:
        "glbx-mdp3-20251027.mbo.dbn.zst"   → date(2025, 10, 27)
        "xeur-eobi-20250507.mbo.dbn.zst"   → date(2025, 5,  7)

    The date is assumed to be the 8-digit sequence in the filename stem.
    Returns None if no 8-digit date is found.
    """
    import re
    stem  = path.name  # full filename including all extensions
    match = re.search(r"(\d{8})", stem)
    if not match:
        return None
    try:
        return date(
            int(match.group(1)[:4]),
            int(match.group(1)[4:6]),
            int(match.group(1)[6:8]),
        )
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# CLI ENTRY POINT
# ---------------------------------------------------------------------------
#
# Two modes:
#
#   Single file:
#       python ingest.py file <path_to_file.mbo.dbn.zst> [--mode STRICT|LOOSE] [--overwrite]
#
#   Batch (all MBO files under a product or venue directory):
#       python ingest.py batch --provider databento --venue CME --product ES [--mode STRICT|LOOSE] [--overwrite]
#       python ingest.py batch --provider databento --venue CME              [--mode STRICT|LOOSE] [--overwrite]
#       python ingest.py batch --provider databento                          [--mode STRICT|LOOSE] [--overwrite]
#
# DATA_ROOT is read from environment variable DESTINY_DATA_ROOT, or defaults
# to /media/julien/HDD/data (can be overridden with --data-root).
#
# Examples:
#   python ingest.py file /media/julien/HDD/data/raw/.../glbx-mdp3-20251027.mbo.dbn.zst
#   python ingest.py batch --provider databento --venue CME --product ES
#   python ingest.py batch --provider databento --venue CME --product ES --overwrite
#   python ingest.py batch --provider databento --venue EUREX

import argparse
import os


# Default data root — override with DESTINY_DATA_ROOT env var or --data-root
_DEFAULT_DATA_ROOT = Path("/media/julien/HDD/data")


def _get_adapter(provider: str):
    """Return the correct adapter instance for a given provider string."""
    from adapters.databento_adapter import DatabentoAdapter
    adapters = {
        "databento": DatabentoAdapter,
        # "hkex": HKEXAdapter,   ← add here as new providers are implemented
    }
    if provider not in adapters:
        print(f"ERROR: unknown provider '{provider}'. Available: {list(adapters)}")
        sys.exit(1)
    return adapters[provider]()


def _resolve_data_root(args) -> Path:
    """Resolve data root from CLI arg or environment variable."""
    if hasattr(args, "data_root") and args.data_root:
        return Path(args.data_root)
    env = os.environ.get("DESTINY_DATA_ROOT")
    if env:
        return Path(env)
    return _DEFAULT_DATA_ROOT


def _cmd_file(args) -> None:
    """Single-file ingestion."""
    raw_path = Path(args.path)
    if not raw_path.exists():
        print(f"ERROR: file not found: {raw_path}")
        sys.exit(1)

    # Derive provider from path (provider=databento segment)
    provider = "databento"
    for part in raw_path.parts:
        if part.startswith("provider="):
            provider = part.split("=", 1)[1]
            break

    # Derive data root from path (walk up to find "data" directory)
    parts = raw_path.parts
    try:
        data_idx  = next(i for i, p in enumerate(parts) if p == "data")
        data_root = Path(*parts[:data_idx + 1])
    except StopIteration:
        data_root = _resolve_data_root(args)

    normalized_dir = data_root / "normalized"
    session_date   = _extract_date_from_filename(raw_path)

    if session_date is None:
        print(f"ERROR: cannot extract date from filename: {raw_path.name}")
        sys.exit(1)

    adapter = _get_adapter(provider)
    counts  = ingest_file(
        adapter        = adapter,
        raw_path       = raw_path,
        normalized_dir = normalized_dir,
        session_date   = session_date,
        mode           = args.mode,
        verbose        = True,
    )

    print("\n[ingest] Summary:")
    for contract, n in sorted(counts.items()):
        print(f"  {contract}: {n:,} clean events")


def _cmd_batch(args) -> None:
    """
    Batch ingestion — process all MBO files under a provider/venue/product tree.

    Scope is determined by which flags are provided:
        --product → single product directory
        --venue only → all products under that venue
        --provider only → all venues and products under that provider
    """
    data_root      = _resolve_data_root(args)
    raw_root       = data_root / "raw"
    normalized_dir = data_root / "normalized"

    # Build the raw directory path from provided filters
    raw_dir = raw_root / f"provider={args.provider}"
    if args.venue:
        raw_dir = raw_dir / f"venue={args.venue}"
    if args.product:
        raw_dir = raw_dir / f"product={args.product}"

    if not raw_dir.exists():
        print(f"ERROR: raw directory not found: {raw_dir}")
        sys.exit(1)

    # If --product is specified: single adapter batch run
    # Otherwise: one adapter run per product directory found
    if args.product:
        product_dirs = [raw_dir]
    else:
        # Find all product= directories under the resolved path
        product_dirs = sorted([
            d for d in raw_dir.rglob("product=*") if d.is_dir()
        ])
        if not product_dirs:
            # raw_dir itself might be the product level
            product_dirs = [raw_dir]

    print(f"[ingest] Batch scope: {raw_dir}")
    print(f"[ingest] Product directories: {len(product_dirs)}")

    for product_dir in product_dirs:
        adapter = _get_adapter(args.provider)
        ingest_product(
            adapter        = adapter,
            raw_dir        = product_dir,
            normalized_dir = normalized_dir,
            mode           = args.mode,
            verbose        = True,
            overwrite      = args.overwrite,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="ingest.py",
        description="Destiny ingestion pipeline — RAW → NORMALIZED",
    )
    parser.add_argument(
        "--data-root",
        help=f"Override data root (default: $DESTINY_DATA_ROOT or {_DEFAULT_DATA_ROOT})",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- file subcommand ---
    p_file = subparsers.add_parser("file", help="Ingest a single raw MBO file")
    p_file.add_argument("path", help="Path to .mbo.dbn.zst file")
    p_file.add_argument(
        "--mode", choices=["STRICT", "LOOSE"], default="STRICT",
        help="Validation mode (default: STRICT)",
    )
    p_file.add_argument(
        "--overwrite", action="store_true",
        help="Re-process even if normalized output already exists",
    )
    p_file.set_defaults(func=_cmd_file)

    # --- batch subcommand ---
    p_batch = subparsers.add_parser(
        "batch",
        help="Ingest all MBO files under a provider/venue/product directory",
    )
    p_batch.add_argument(
        "--provider", required=True,
        help="Provider name, e.g. databento",
    )
    p_batch.add_argument(
        "--venue", default=None,
        help="Venue filter, e.g. CME, EUREX (optional — all venues if omitted)",
    )
    p_batch.add_argument(
        "--product", default=None,
        help="Product filter, e.g. ES, FDAX (optional — all products if omitted)",
    )
    p_batch.add_argument(
        "--mode", choices=["STRICT", "LOOSE"], default="STRICT",
        help="Validation mode (default: STRICT)",
    )
    p_batch.add_argument(
        "--overwrite", action="store_true",
        help="Re-process files even if normalized output already exists",
    )
    p_batch.set_defaults(func=_cmd_batch)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()