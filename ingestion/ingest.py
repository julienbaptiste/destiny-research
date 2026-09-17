"""
ingest.py — streaming RAW -> canonical normalized MBO orchestrator.

The orchestrator is provider-agnostic. Adapters own source translation,
validator.py owns canonical validation policy, and this module owns bounded
Parquet buffering plus session lifecycle.
"""

from __future__ import annotations

import argparse
from datetime import date
import logging
import os
from pathlib import Path
import re
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from .adapters.base import BaseAdapter, ContractInfo, SessionConfig
from .schema import (
    Flags,
    ValidationMode,
    NORMALIZED_MBO_SCHEMA,
    REJECTED_EVENTS_SCHEMA,
    normalized_path,
    rejected_path,
)
from .validator import (
    ValidatorState,
    _build_rejected_row,
    log_stats,
    mark_validation_anomaly,
    validate_event,
)

log = logging.getLogger(__name__)

BATCH_SIZE = 100_000
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LVL = 3


class _WriterContext:
    """Bounded clean/rejected buffers and lazy Parquet writers for one contract."""

    __slots__ = (
        "clean_writer",
        "rejected_writer",
        "clean_buf",
        "rejected_buf",
        "clean_path",
        "rejected_path",
        "n_clean",
        "n_rejected",
    )

    def __init__(self, clean_path: Path, rejected_path_: Path) -> None:
        self.clean_path = clean_path
        self.rejected_path = rejected_path_
        self.clean_writer: pq.ParquetWriter | None = None
        self.rejected_writer: pq.ParquetWriter | None = None
        self.clean_buf: list[dict] = []
        self.rejected_buf: list[dict] = []
        self.n_clean = 0
        self.n_rejected = 0

    def _ensure_clean_writer(self) -> None:
        if self.clean_writer is not None:
            return
        self.clean_path.parent.mkdir(parents=True, exist_ok=True)
        self.clean_writer = pq.ParquetWriter(
            self.clean_path,
            NORMALIZED_MBO_SCHEMA,
            compression=PARQUET_COMPRESSION,
            compression_level=PARQUET_COMPRESSION_LVL,
        )

    def _ensure_rejected_writer(self) -> None:
        if self.rejected_writer is not None:
            return
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

    def append_rejected(self, event: dict) -> None:
        self.rejected_buf.append(event)
        self.n_rejected += 1
        if len(self.rejected_buf) >= BATCH_SIZE:
            self.flush_rejected()

    def flush_clean(self) -> None:
        if not self.clean_buf:
            return
        self._ensure_clean_writer()
        assert self.clean_writer is not None
        table = pa.Table.from_pylist(self.clean_buf, schema=NORMALIZED_MBO_SCHEMA)
        self.clean_writer.write_table(table)
        self.clean_buf.clear()

    def flush_rejected(self) -> None:
        if not self.rejected_buf:
            return
        self._ensure_rejected_writer()
        assert self.rejected_writer is not None
        table = pa.Table.from_pylist(self.rejected_buf, schema=REJECTED_EVENTS_SCHEMA)
        self.rejected_writer.write_table(table)
        self.rejected_buf.clear()

    def close(self) -> None:
        self.flush_clean()
        self.flush_rejected()
        if self.clean_writer is not None:
            self.clean_writer.close()
        if self.rejected_writer is not None:
            self.rejected_writer.close()


def ingest_file(
    adapter: BaseAdapter,
    raw_path: Path,
    normalized_dir: Path,
    session_date: date,
    mode: str = ValidationMode.STRICT,
    verbose: bool = True,
) -> dict[str, int]:
    """Stream one provider session into per-contract canonical Parquet files."""
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw source not found: {raw_path}")

    provider = adapter.PROVIDER
    date_str = session_date.strftime("%Y%m%d")
    year = session_date.year
    month = session_date.month

    config = SessionConfig(
        session_date=session_date,
        warmup_enabled=True,
        validation_mode=mode,
    )
    adapter.open_session(raw_path, config)

    instruments: dict[int, ContractInfo] = {}
    writer_ctx: dict[str, _WriterContext] = {}
    validator_states: dict[str, ValidatorState] = {}

    n_total = 0
    n_dropped = 0

    try:
        for info in adapter.list_instruments():
            _register_instrument(
                info,
                provider,
                normalized_dir,
                date_str,
                year,
                month,
                mode,
                instruments,
                writer_ctx,
                validator_states,
                verbose,
            )

        if verbose:
            log.info(
                "%s | %s | %s | mode=%s | metadata instruments=%d",
                provider,
                raw_path.name,
                session_date,
                mode,
                len(instruments),
            )

        for event in adapter.iter_events():
            if event is None:
                n_dropped += 1
                continue

            n_total += 1
            instrument_id = int(event.get("instrument_id", 0))
            contract = str(event.get("contract", ""))

            if instrument_id not in instruments:
                info = adapter.resolve_contract(instrument_id, session_date)
                if info is None:
                    n_dropped += 1
                    continue
                _register_instrument(
                    info,
                    provider,
                    normalized_dir,
                    date_str,
                    year,
                    month,
                    mode,
                    instruments,
                    writer_ctx,
                    validator_states,
                    verbose,
                )

            ctx = writer_ctx[contract]
            state = validator_states[contract]

            is_snapshot = bool(int(event.get("flags", 0)) & int(Flags.F_SNAPSHOT))
            if not is_snapshot and state.warmup_mode:
                state.warmup_end()
                if verbose:
                    log.debug(
                        "%s warmup ended at ts_event=%s",
                        contract,
                        event["ts_event"],
                    )

            is_clean, reason = validate_event(event, state)
            if is_clean:
                ctx.append_clean(event)
                continue

            ctx.append_rejected(_build_rejected_row(event, reason or "UNKNOWN", mode))
            if mode == ValidationMode.LOOSE:
                ctx.append_clean(mark_validation_anomaly(event))

    finally:
        # Always flush what was already accepted before propagating a hard error.
        for ctx in writer_ctx.values():
            ctx.close()
        adapter.close_session()

    counts = {contract: ctx.n_clean for contract, ctx in writer_ctx.items()}

    if verbose:
        log.info(
            "  total=%s | dropped(pre-validation)=%s | instruments=%d",
            f"{n_total:,}",
            f"{n_dropped:,}",
            len(writer_ctx),
        )
        for contract, state in validator_states.items():
            log_stats(state, contract, date_str)

    return counts


def ingest_product(
    adapter: BaseAdapter,
    raw_dir: Path,
    normalized_dir: Path,
    mode: str = ValidationMode.STRICT,
    verbose: bool = True,
    overwrite: bool = False,
) -> None:
    """Ingest all Databento-style MBO files below one product directory."""
    raw_files = sorted(raw_dir.rglob("*.mbo.dbn.zst"))
    if not raw_files:
        log.warning("No .mbo.dbn.zst files found in %s", raw_dir)
        return

    for raw_path in raw_files:
        session_date = _extract_date_from_filename(raw_path)
        if session_date is None:
            log.warning("Cannot extract date from %s; skipping", raw_path.name)
            continue

        if not overwrite and _normalized_day_exists(
            raw_dir,
            normalized_dir,
            session_date,
        ):
            if verbose:
                log.info("%s already normalized; skipping", raw_path.name)
            continue

        try:
            ingest_file(
                adapter=adapter,
                raw_path=raw_path,
                normalized_dir=normalized_dir,
                session_date=session_date,
                mode=mode,
                verbose=verbose,
            )
        except Exception:
            log.exception("Error processing %s", raw_path)
            if adapter._is_open:  # defensive cleanup after partial open
                try:
                    adapter.close_session()
                except Exception:
                    log.exception("Adapter cleanup failed")


def _register_instrument(
    info: ContractInfo,
    provider: str,
    normalized_dir: Path,
    date_str: str,
    year: int,
    month: int,
    mode: str,
    instruments: dict[int, ContractInfo],
    writer_ctx: dict[str, _WriterContext],
    validator_states: dict[str, ValidatorState],
    verbose: bool,
) -> None:
    if info.instrument_id in instruments:
        return

    instruments[info.instrument_id] = info
    if info.contract in writer_ctx:
        return

    clean_path = normalized_path(
        normalized_dir,
        provider,
        info.venue,
        info.product,
        info.contract,
        year,
        month,
        date_str,
    )
    rejected_file = rejected_path(
        normalized_dir,
        provider,
        info.venue,
        info.product,
        info.contract,
        year,
        month,
        date_str,
    )

    writer_ctx[info.contract] = _WriterContext(clean_path, rejected_file)
    validator_states[info.contract] = ValidatorState(mode=mode, warmup_mode=True)

    if verbose:
        spread_label = " [SPREAD]" if info.is_spread else ""
        log.debug("registered %s%s -> %s", info.contract, spread_label, clean_path)


def _extract_date_from_filename(path: Path) -> date | None:
    match = re.search(r"(\d{8})", path.name)
    if not match:
        return None
    try:
        return date.fromisoformat(
            f"{match.group(1)[0:4]}-{match.group(1)[4:6]}-{match.group(1)[6:8]}"
        )
    except ValueError:
        return None


def _normalized_day_exists(
    raw_dir: Path,
    normalized_dir: Path,
    session_date: date,
) -> bool:
    date_str = session_date.strftime("%Y%m%d")
    provider_part = next((part for part in raw_dir.parts if part.startswith("provider=")), None)
    venue_part = next((part for part in raw_dir.parts if part.startswith("venue=")), None)
    product_part = next((part for part in raw_dir.parts if part.startswith("product=")), None)

    if provider_part and venue_part and product_part:
        scoped = normalized_dir / provider_part / venue_part / product_part
    else:
        scoped = normalized_dir
    return scoped.exists() and any(scoped.rglob(f"*_{date_str}_mbo.parquet"))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

_DEFAULT_DATA_ROOT = Path("/media/julien/HDD/data")


def _get_adapter(provider: str) -> BaseAdapter:
    from .adapters.databento_adapter import DatabentoAdapter
    from .adapters.hkex_adapter import HKEXAdapter

    factories = {
        "databento": DatabentoAdapter,
        "hkex": HKEXAdapter,
        "HKEX": HKEXAdapter,
    }
    factory = factories.get(provider)
    if factory is None:
        raise ValueError(f"Unknown provider {provider!r}; available={sorted(factories)}")
    return factory()


def _resolve_data_root(args: argparse.Namespace) -> Path:
    if getattr(args, "data_root", None):
        return Path(args.data_root)
    env = os.environ.get("DESTINY_DATA_ROOT")
    return Path(env) if env else _DEFAULT_DATA_ROOT


def _cmd_file(args: argparse.Namespace) -> None:
    raw_path = Path(args.path)
    if not raw_path.exists():
        raise FileNotFoundError(raw_path)

    provider = "databento"
    for part in raw_path.parts:
        if part.startswith("provider="):
            provider = part.split("=", 1)[1]
            break

    session_date = _extract_date_from_filename(raw_path)
    if session_date is None:
        raise ValueError(f"Cannot extract date from {raw_path.name}")

    counts = ingest_file(
        adapter=_get_adapter(provider),
        raw_path=raw_path,
        normalized_dir=_resolve_data_root(args) / "normalized",
        session_date=session_date,
        mode=args.mode,
        verbose=True,
    )
    for contract, count in sorted(counts.items()):
        log.info("%s: %s clean events", contract, f"{count:,}")


def _cmd_batch(args: argparse.Namespace) -> None:
    data_root = _resolve_data_root(args)
    raw_dir = data_root / "raw" / f"provider={args.provider}"
    if args.venue:
        raw_dir /= f"venue={args.venue}"
    if args.product:
        raw_dir /= f"product={args.product}"
    if not raw_dir.exists():
        raise FileNotFoundError(raw_dir)

    product_dirs = [raw_dir] if args.product else sorted(
        directory for directory in raw_dir.rglob("product=*") if directory.is_dir()
    )
    if not product_dirs:
        product_dirs = [raw_dir]

    for product_dir in product_dirs:
        ingest_product(
            adapter=_get_adapter(args.provider),
            raw_dir=product_dir,
            normalized_dir=data_root / "normalized",
            mode=args.mode,
            verbose=True,
            overwrite=args.overwrite,
        )


def _discover_hkex_dates(raw_month_dir: Path, product: str) -> list[str]:
    dates: list[str] = []
    pattern = re.compile(rf"hkex-{re.escape(product.lower())}_(\d{{8}})_orders\.parquet$")
    for path in sorted(raw_month_dir.glob(f"hkex-{product.lower()}_*_orders.parquet")):
        match = pattern.match(path.name)
        if match:
            token = match.group(1)
            dates.append(f"{token[:4]}-{token[4:6]}-{token[6:8]}")
    return dates


def _cmd_hkex(args: argparse.Namespace) -> None:
    data_root = _resolve_data_root(args)
    normalized_dir = data_root / "normalized"

    for product in args.product:
        if args.date:
            dates = [args.date]
        else:
            year, month = args.month.split("-")
            raw_month_dir = (
                data_root
                / "raw"
                / "provider=HKEX"
                / "venue=HKEX"
                / f"product={product}"
                / f"year={year}"
                / f"month={month}"
            )
            dates = _discover_hkex_dates(raw_month_dir, product)
            if not dates:
                raise FileNotFoundError(f"No HKEX orders files in {raw_month_dir}")

        for date_str in dates:
            session_date = date.fromisoformat(date_str)
            raw_dir = (
                data_root
                / "raw"
                / "provider=HKEX"
                / "venue=HKEX"
                / f"product={product}"
                / f"year={session_date.year}"
                / f"month={session_date.month:02d}"
            )

            product_norm = normalized_dir / "provider=HKEX" / "venue=HKEX" / f"product={product}"
            token = session_date.strftime("%Y%m%d")
            existing = list(product_norm.rglob(f"*_{token}_mbo.parquet")) if product_norm.exists() else []
            if existing and not args.overwrite:
                log.info("SKIP %s %s: already normalized", product, date_str)
                continue

            counts = ingest_file(
                adapter=_get_adapter("hkex"),
                raw_path=raw_dir,
                normalized_dir=normalized_dir,
                session_date=session_date,
                mode=args.mode,
                verbose=True,
            )
            log.info(
                "%s %s -> %s clean events across %d contracts",
                product,
                date_str,
                f"{sum(counts.values()):,}",
                len(counts),
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Destiny RAW -> normalized MBO ingestion")
    parser.add_argument("--data-root")
    subparsers = parser.add_subparsers(dest="command", required=True)

    file_parser = subparsers.add_parser("file")
    file_parser.add_argument("path")
    file_parser.add_argument("--mode", choices=["STRICT", "LOOSE"], default="STRICT")
    file_parser.add_argument("--overwrite", action="store_true")
    file_parser.set_defaults(func=_cmd_file)

    batch_parser = subparsers.add_parser("batch")
    batch_parser.add_argument("--provider", required=True)
    batch_parser.add_argument("--venue")
    batch_parser.add_argument("--product")
    batch_parser.add_argument("--mode", choices=["STRICT", "LOOSE"], default="STRICT")
    batch_parser.add_argument("--overwrite", action="store_true")
    batch_parser.set_defaults(func=_cmd_batch)

    hkex_parser = subparsers.add_parser("hkex")
    hkex_parser.add_argument("--product", required=True, nargs="+")
    date_group = hkex_parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--date")
    date_group.add_argument("--month")
    hkex_parser.add_argument("--mode", choices=["STRICT", "LOOSE"], default="LOOSE")
    hkex_parser.add_argument("--overwrite", action="store_true")
    hkex_parser.set_defaults(func=_cmd_hkex)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    from utils.logging_config import setup_logging

    setup_logging()
    main()
