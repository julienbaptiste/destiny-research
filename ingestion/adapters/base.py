"""
base.py — Abstract base class for all provider-specific MBO adapters.

Every provider (Databento, HKEX, IBKR, ...) must implement a concrete
subclass of BaseAdapter. The ingestion orchestrator (ingest.py) works
exclusively against this interface — it has zero knowledge of any
specific provider format.

Adapter responsibilities:
    1. Parse raw provider-specific events into normalized event dicts
       (schema defined in schema.py — NORMALIZED_MBO_SCHEMA)
    2. Resolve instrument_id → (product, contract) via provider metadata
    3. Report the venue string for this provider/dataset
    4. Signal session boundaries (warmup start/end) to the orchestrator

What adapters must NOT do:
    - Apply validation logic (that is validator.py's job)
    - Write any files (that is ingest.py's job)
    - Maintain LOB state (that is the reconstruction engine's job)
    - Emit MBP snapshots (that is the reconstruction engine's job)

Translation contract:
    translate() receives one raw event (provider-specific object or dict)
    and returns either:
        - a normalized event dict conforming to NORMALIZED_MBO_SCHEMA fields
        - None  →  event should be silently dropped BEFORE validation
                   (e.g. heartbeats, metadata records, instrument defs)
    Only events that are semantically irrelevant to MBO reconstruction
    should return None here. Anomalous but potentially meaningful events
    must be translated and let the validator decide.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Iterator

import pyarrow as pa

from ingestion.schema import ValidationMode


# ---------------------------------------------------------------------------
# CONTRACT INFO
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ContractInfo:
    """
    Resolved contract metadata for a given instrument_id on a given date.

    Returned by BaseAdapter.resolve_contract().
    Immutable — built once per instrument_id during session initialization.

    Fields:
        product:       underlying product code,  e.g. "ES", "FDAX", "HSI"
        contract:      specific expiry symbol,   e.g. "ESZ25", "FDAXZ25"
        venue:         exchange venue code,      e.g. "CME", "EUREX", "HKEX"
        instrument_id: provider-native ID (stored in normalized parquet for
                       traceability, not used by reconstruction engine)
        is_spread:     True if this is a calendar spread instrument
                       e.g. ES_CAL_Z25H26
        tick_size:     minimum price increment in fixed-point int64
                       e.g. 250_000_000 for ES (0.25 pts × 1e9)
        currency:      ISO 4217 currency code, e.g. "USD", "EUR", "JPY"
    """
    product       : str
    contract      : str
    venue         : str
    instrument_id : int
    is_spread     : bool  = False
    tick_size     : int   = 0       # fixed-point, 0 = unknown
    currency      : str   = ""


# ---------------------------------------------------------------------------
# SESSION CONFIG
# ---------------------------------------------------------------------------

@dataclass
class SessionConfig:
    """
    Per-session configuration passed to the adapter at session open.

    session_date:    the calendar date being processed (UTC)
    warmup_enabled:  whether overnight warmup is required for this product.
                     If True, the adapter must yield warmup events before
                     RTH events, and call signal_warmup_end() at the
                     appropriate boundary.
    validation_mode: STRICT or LOOSE (propagated to ValidatorState)
    """
    session_date     : date
    warmup_enabled   : bool = False
    validation_mode  : str  = ValidationMode.STRICT


# ---------------------------------------------------------------------------
# ABSTRACT BASE ADAPTER
# ---------------------------------------------------------------------------

class BaseAdapter(ABC):
    """
    Abstract base class for provider-specific MBO feed adapters.

    Subclass this for each data provider. Override all abstract methods.
    The orchestrator (ingest.py) calls methods in this order:

        1. open_session(raw_source, config)
        2. iter_events()             ← yields normalized dicts one by one
        3. close_session()

    Thread safety: one adapter instance per session. Do not share instances
    across threads or sessions.
    """

    # Provider identifier — set as a class-level constant in each subclass.
    # Used for path construction and logging.
    # e.g. "databento", "hkex", "ibkr"
    PROVIDER: str = ""

    def __init__(self) -> None:
        # Populated during open_session() — available to all methods after.
        self._config    : SessionConfig | None = None
        self._is_open   : bool                 = False
        self._n_yielded : int                  = 0   # total events translated
        self._n_dropped : int                  = 0   # events returning None

    # ------------------------------------------------------------------
    # SESSION LIFECYCLE  (must be called in order by orchestrator)
    # ------------------------------------------------------------------

    def open_session(self, raw_source: object, config: SessionConfig) -> None:
        """
        Initialize adapter for a new session.

        Args:
            raw_source: provider-specific data source.
                        Databento: path to .dbn.zst file (str | Path)
                        HKEX:      path to parsed binary dump (str | Path)
                        IBKR:      live socket connection object
            config:     SessionConfig for this session

        Subclasses must call super().open_session() first, then initialize
        their provider-specific state (file handles, instrument maps, etc.).
        """
        self._config  = config
        self._is_open = True
        self._open(raw_source, config)

    def close_session(self) -> None:
        """
        Finalize and release all resources for this session.
        Subclasses must call super().close_session() last.
        """
        self._close()
        self._is_open   = False
        self._config    = None

    @abstractmethod
    def _open(self, raw_source: object, config: SessionConfig) -> None:
        """
        Provider-specific session initialization.
        Open file handles, load instrument maps, initialize state.
        """
        ...

    @abstractmethod
    def _close(self) -> None:
        """
        Provider-specific session teardown.
        Close file handles, flush buffers, release memory.
        """
        ...

    # ------------------------------------------------------------------
    # EVENT ITERATION  (core interface)
    # ------------------------------------------------------------------

    def iter_events(self) -> Iterator[dict | None]:
        """
        Iterate over all events in the session, yielding normalized dicts.

        Yields:
            dict  →  normalized event conforming to NORMALIZED_MBO_SCHEMA fields
            None  →  event should be dropped before validation (heartbeat, etc.)

        The orchestrator filters out None values before passing to the validator.
        Events are yielded in feed order — the validator handles sorting if needed.

        Subclasses implement _iter_raw() to yield raw provider events,
        and translate() to convert each one. This base class handles the
        iteration wrapper, counters, and None filtering signal.
        """
        assert self._is_open, "open_session() must be called before iter_events()"

        for raw_event in self._iter_raw():
            normalized = self.translate(raw_event)
            if normalized is None:
                self._n_dropped += 1
                yield None
            else:
                self._n_yielded += 1
                yield normalized

    @abstractmethod
    def _iter_raw(self) -> Iterator[object]:
        """
        Iterate over raw provider-specific events.
        Yield one raw event at a time — translate() handles conversion.
        No filtering here — yield everything including heartbeats,
        instrument defs, etc. translate() decides what to drop.
        """
        ...

    @abstractmethod
    def translate(self, raw_event: object) -> dict | None:
        """
        Translate one raw provider event to a normalized event dict.

        Returns:
            dict  →  normalized event (all NORMALIZED_MBO_SCHEMA fields present)
            None  →  drop this event before validation (heartbeat, metadata, etc.)

        The returned dict must contain ALL fields defined in NORMALIZED_MBO_SCHEMA.
        Missing fields will cause a schema error in validate_batch().

        Performance note:
            This is the hot path — called once per raw event.
            Avoid object allocation where possible. Reuse dicts if safe.
            On ES (10M events/day), even 1µs per call = 10s total.
        """
        ...

    # ------------------------------------------------------------------
    # INSTRUMENT RESOLUTION
    # ------------------------------------------------------------------

    @abstractmethod
    def resolve_contract(
        self,
        instrument_id : int,
        session_date  : date,
    ) -> ContractInfo | None:
        """
        Resolve a provider-native instrument_id to ContractInfo.

        Args:
            instrument_id: provider-native instrument identifier
            session_date:  date for which to resolve (instrument maps
                           can change over time due to expiry rolls)

        Returns:
            ContractInfo if resolution succeeds, None if instrument_id
            is unknown or not relevant (e.g. an equity in a futures feed).

        Called during session initialization to build the instrument map.
        May also be called lazily on first encounter of a new instrument_id.

        Subclasses should cache results — this may be called millions of
        times during iteration on a multi-instrument feed.
        """
        ...

    @abstractmethod
    def list_instruments(self) -> list[ContractInfo]:
        """
        Return all instruments present in the current raw source.

        Called by the orchestrator after open_session() to:
            1. Pre-build the instrument map
            2. Create output directories and Parquet writers
            3. Initialize one ValidatorState per instrument

        Must be called after open_session().
        """
        ...

    # ------------------------------------------------------------------
    # WARMUP BOUNDARY SIGNALING
    # ------------------------------------------------------------------

    @abstractmethod
    def is_warmup_event(self, normalized_event: dict) -> bool:
        """
        Return True if this normalized event is part of the warmup period
        and should not emit LOB rows.

        Called by the orchestrator on every event when warmup_enabled=True.
        When this method transitions from True → False, the orchestrator
        calls validator_state.warmup_end() for the relevant instrument.

        Implementation guidelines:
            - Databento: F_SNAPSHOT as a common way to detect it
            - HKEX: session flags or timestamp-based boundary
            - Return False always if warmup_enabled=False in config
        """
        ...

    # ------------------------------------------------------------------
    # VENUE / PROVIDER INFO
    # ------------------------------------------------------------------

    @abstractmethod
    def get_venue(self) -> str:
        """
        Return the venue code for this adapter's data source.
        e.g. "CME", "EUREX", "HKEX"
        Must be consistent with the venue field in NORMALIZED_MBO_SCHEMA.
        """
        ...

    # ------------------------------------------------------------------
    # STATS / DIAGNOSTICS
    # ------------------------------------------------------------------

    def get_stats(self) -> dict:
        """
        Return translation statistics for end-of-session logging.
        Subclasses may override to add provider-specific metrics.
        """
        return {
            "provider"   : self.PROVIDER,
            "n_yielded"  : self._n_yielded,
            "n_dropped"  : self._n_dropped,
        }

    def __repr__(self) -> str:
        status = "open" if self._is_open else "closed"
        return (
            f"{self.__class__.__name__}("
            f"provider={self.PROVIDER!r}, "
            f"status={status}, "
            f"n_yielded={self._n_yielded:,})"
        )