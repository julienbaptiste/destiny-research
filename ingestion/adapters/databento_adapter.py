"""
databento_adapter.py — Databento MBO feed adapter.

Translates Databento DBN MBOMsg records into normalized event dicts
conforming to NORMALIZED_MBO_SCHEMA (schema.py).

Responsibilities:
    - Parse .dbn.zst files via the databento-dbn library
    - Resolve instrument_id → ContractInfo via Databento InstrumentMap
    - Translate all MBOMsg fields to normalized schema
    - Detect warmup events via F_SNAPSHOT flag
    - Map Databento action/side chars to normalized strings

What this adapter does NOT do:
    - Validate events (validator.py)
    - Reconstruct the LOB (reconstruction engine)
    - Write any files (ingest.py)
    - Apply any RTH filtering

Databento-specific notes:

    F_SNAPSHOT (0x20):
        Set on messages that are replayed from a MBP snapshot at stream
        start to bootstrap the book state. These are warmup events — they
        represent the resting order book before our live event window.
        Warmup ends on the first message where F_SNAPSHOT is NOT set.

    F_TOB (0x01):
        Top-of-book synthetic messages. These carry order_id=0 and may
        carry UNDEF_PRICE to signal that a book side should be cleared.
        They replace an entire side, not a single order.
        Handled by the reconstruction engine — translated faithfully here.

    F_LAST (0x80):
        Last message in an atomic event group. A LOB snapshot should only
        be taken when this flag is set. Translated faithfully — the
        reconstruction engine enforces this rule.

    UNDEF_PRICE (INT64_MAX = 9_223_372_036_854_775_807):
        Databento sentinel for "no price defined". Valid only on F_TOB
        messages to signal side clearing. Passed through as-is — the
        validator and reconstruction engine handle the semantics.

    publisher_id:
        Databento feeds can have multiple publishers for the same
        instrument (e.g. CME direct + consolidated). We store publisher_id
        in the normalized schema for traceability. The reconstruction
        engine uses (instrument_id, publisher_id) as the book key —
        same as the official Databento algo.

    Symbology:
        Databento uses instrument_id (uint32) as the primary key.
        Symbol strings (e.g. "ESZ5") are resolved via InstrumentMap
        for a given date. We derive product and contract from the symbol:
            "ESZ5"    → product="ES",   contract="ESZ25"  (normalized expiry)
            "FESXZ5"  → product="FESX", contract="FESXZ25"
            "ES-Z5H6" → product="ES",   contract="ES_CAL_Z25H26" (spread)
        See _parse_symbol() for full normalization logic.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Iterator

import databento as db

from .base import BaseAdapter, ContractInfo, SessionConfig
from ingestion.schema import (
    Action,
    Flags,
    Side,
    UNDEF_PRICE,
    FIXED_PRICE_SCALE,
)


# ---------------------------------------------------------------------------
# VENUE MAPPING
# ---------------------------------------------------------------------------

# Databento dataset prefix → our canonical venue code.
# The dataset string is in the DBN metadata (e.g. "glbx.mdp3" → CME).
_DATASET_TO_VENUE: dict[str, str] = {
    "glbx.mdp3"  : "CME",     # CME Globex MDP3
    "xeur.eobi"  : "EUREX",   # Eurex EOBI
    "xnas.itch"  : "NASDAQ",
    "xnys.pillar": "NYSE",
    # Add new datasets here as needed
}


# ---------------------------------------------------------------------------
# SYMBOL NORMALIZATION
# ---------------------------------------------------------------------------

# Databento expiry month codes → standard month numbers
_MONTH_CODE: dict[str, int] = {
    "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
    "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12,
}

# Reverse: month number → expiry code (for contract naming)
_MONTH_TO_CODE: dict[int, str] = {v: k for k, v in _MONTH_CODE.items()}

# EUREX month name abbreviations (3-letter) → futures month code
# Used to parse EUREX spread symbols: "FDAX.S.JUN25.SEP25.SPD"
_EUREX_MONTH_NAME: dict[str, str] = {
    "JAN": "F", "FEB": "G", "MAR": "H", "APR": "J", "MAY": "K", "JUN": "M",
    "JUL": "N", "AUG": "Q", "SEP": "U", "OCT": "V", "NOV": "X", "DEC": "Z",
}

# ---------------------------------------------------------------------------
# Regex patterns — CME format
# ---------------------------------------------------------------------------

# CME outright future: e.g. "ESZ5", "FESXZ5", "NIYZ5"
#   Pattern: {PRODUCT}{MONTH_LETTER}{SINGLE_DIGIT_YEAR}
_RE_OUTRIGHT = re.compile(r"^([A-Z]+)([FGHJKMNQUVXZ])(\d)$")

# CME calendar spread: e.g. "ESM6-ESU6", "NIYZ5-NIYZ6"
#   Pattern: {PRODUCT}{MC}{Y}-{PRODUCT}{MC}{Y}
#   Both legs carry the full product code — product taken from leg 1.
_RE_SPREAD = re.compile(
    r"^([A-Z]+)([FGHJKMNQUVXZ])(\d)-[A-Z]+([FGHJKMNQUVXZ])(\d)$"
)

# ---------------------------------------------------------------------------
# Regex patterns — EUREX format
# ---------------------------------------------------------------------------

# EUREX outright future: e.g. "FDAX SI 20250620 CS"
#   Pattern: {PRODUCT} SI {YYYYMMDD} CS
#   The expiry date encodes the last trading day — we extract year+month only.
_RE_EUREX_OUTRIGHT = re.compile(
    r"^([A-Z]+)\s+SI\s+(\d{4})(\d{2})\d{2}\s+CS$"
)

# EUREX calendar spread: e.g. "FDAX.S.JUN25.SEP25.SPD"
#   Pattern: {PRODUCT}.S.{MMM}{YY}.{MMM}{YY}.SPD
#   Month names: JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC
_RE_EUREX_SPREAD = re.compile(
    r"^([A-Z]+)\.S\.([A-Z]{3})(\d{2})\.([A-Z]{3})(\d{2})\.SPD$"
)


def _normalize_expiry_year(single_digit_year: str, session_date: date) -> int:
    """
    Convert a single-digit expiry year to a 4-digit year.

    Databento uses single-digit year codes (e.g. "5" for 2025).
    We use the session date to disambiguate the decade.

    Rule: if the single-digit year < (current year % 10) - 1,
    assume next decade. This handles the edge case of trading
    contracts more than ~8 years out (rare for liquid futures).

    Examples (session_date in 2025):
        "5" → 2025,  "6" → 2026,  "7" → 2027,  "4" → 2034 (far expiry)
    """
    digit       = int(single_digit_year)
    current_yr  = session_date.year
    decade_base = (current_yr // 10) * 10
    candidate   = decade_base + digit
    # If the candidate year is more than 2 years in the past, bump a decade
    if candidate < current_yr - 2:
        candidate += 10
    return candidate


def _parse_symbol(
    symbol      : str,
    session_date: date,
) -> tuple[str, str, bool] | None:
    """
    Parse a Databento symbol string into (product, contract, is_spread).

    Returns None if the symbol cannot be parsed (e.g. index, equity,
    option, or unrecognized format) — the instrument will be skipped.

    Supported formats:

    CME (Globex MDP3):
        Outrights : "ESZ5"            → ("ES",   "ESZ25",             False)
                    "NIYZ5"           → ("NIY",  "NIYZ25",            False)
        Spreads   : "ESM6-ESU6"       → ("ES",   "ES_CAL_M26U26",     True)

    EUREX (EOBI):
        Outrights : "FDAX SI 20250620 CS"  → ("FDAX", "FDAXM25",      False)
                    "FESX SI 20250620 CS"  → ("FESX", "FESXM25",      False)
        Spreads   : "FDAX.S.JUN25.SEP25.SPD" → ("FDAX","FDAX_CAL_M25U25", True)

    Unrecognized symbols (options, indices, malformed) return None.
    """
    # Strip whitespace — some Databento symbols have trailing spaces
    symbol = symbol.strip()

    # --- CME outright future ---
    m = _RE_OUTRIGHT.match(symbol)
    if m:
        product    = m.group(1)
        month_code = m.group(2)
        year_digit = m.group(3)
        year_4     = _normalize_expiry_year(year_digit, session_date)
        contract   = f"{product}{month_code}{str(year_4)[-2:]}"
        return product, contract, False

    # --- CME calendar spread ---
    m = _RE_SPREAD.match(symbol)
    if m:
        product  = m.group(1)
        mc1, yd1 = m.group(2), m.group(3)
        mc2, yd2 = m.group(4), m.group(5)
        yr1      = _normalize_expiry_year(yd1, session_date)
        yr2      = _normalize_expiry_year(yd2, session_date)
        contract = (
            f"{product}_CAL_"
            f"{mc1}{str(yr1)[-2:]}"
            f"{mc2}{str(yr2)[-2:]}"
        )
        return product, contract, True

    # --- EUREX outright future ---
    # Format: "FDAX SI 20250620 CS"
    # We extract product from group 1, year from group 2, month from group 3.
    # The expiry day is ignored — we only need year+month for the contract code.
    m = _RE_EUREX_OUTRIGHT.match(symbol)
    if m:
        product    = m.group(1)
        year_4     = int(m.group(2))
        month_num  = int(m.group(3))
        month_code = _MONTH_TO_CODE.get(month_num)
        if month_code is None:
            return None  # malformed month
        contract = f"{product}{month_code}{str(year_4)[-2:]}"
        return product, contract, False

    # --- EUREX calendar spread ---
    # Format: "FDAX.S.JUN25.SEP25.SPD"
    # Groups: product, month_name_1, 2digit_year_1, month_name_2, 2digit_year_2
    m = _RE_EUREX_SPREAD.match(symbol)
    if m:
        product   = m.group(1)
        mn1, yd1  = m.group(2), m.group(3)
        mn2, yd2  = m.group(4), m.group(5)
        mc1       = _EUREX_MONTH_NAME.get(mn1)
        mc2       = _EUREX_MONTH_NAME.get(mn2)
        if mc1 is None or mc2 is None:
            return None  # unknown month name
        # 2-digit year from spread symbol is already 2-digit (e.g. "25")
        contract = f"{product}_CAL_{mc1}{yd1}{mc2}{yd2}"
        return product, contract, True

    # Unrecognized format — skip (options, indices, malformed symbols, etc.)
    return None


# ---------------------------------------------------------------------------
# DATABENTO ADAPTER
# ---------------------------------------------------------------------------

class DatabentoAdapter(BaseAdapter):
    """
    Adapter for Databento MBO (.dbn.zst) feed files.

    Usage:
        adapter = DatabentoAdapter()
        adapter.open_session(path_to_dbn_file, config)
        for event in adapter.iter_events():
            if event is not None:
                # pass to validator
        adapter.close_session()
    """

    PROVIDER = "databento"

    def __init__(self) -> None:
        super().__init__()

        # Populated in _open()
        self._store          : db.DBNStore | None        = None
        self._instrument_map : db.common.symbology.InstrumentMap | None = None
        self._venue          : str                       = ""

        # instrument_id → ContractInfo cache (built lazily during iteration)
        # Keyed by instrument_id (int) — one entry per instrument in the file.
        self._contract_cache : dict[int, ContractInfo | None] = {}

        # Warmup tracking:
        # _in_warmup is True while F_SNAPSHOT is set on incoming messages.
        # Transitions to False on first message without F_SNAPSHOT.
        # Per-instrument: instrument_id → bool
        self._in_warmup      : dict[int, bool] = {}

        # Stats extension
        self._n_snapshot_events : int = 0
        self._n_unknown_instr   : int = 0

    # ------------------------------------------------------------------
    # SESSION LIFECYCLE
    # ------------------------------------------------------------------

    def _open(self, raw_source: object, config: SessionConfig) -> None:
        """
        Load DBN store and build instrument map from file metadata.

        raw_source: str | Path — path to .dbn.zst file
        """
        self._store = db.DBNStore.from_file(raw_source)

        # InstrumentMap resolves instrument_id → symbol for a given date.
        # insert_metadata() populates it from the DBN file header.
        self._instrument_map = db.common.symbology.InstrumentMap()
        self._instrument_map.insert_metadata(self._store.metadata)

        # Derive venue from dataset string in metadata
        dataset     = self._store.metadata.dataset.lower()
        self._venue = _DATASET_TO_VENUE.get(dataset, dataset.upper())

    def _close(self) -> None:
        """Release DBN store and clear caches."""
        self._store          = None
        self._instrument_map = None
        self._contract_cache.clear()
        self._in_warmup.clear()

    # ------------------------------------------------------------------
    # INSTRUMENT RESOLUTION
    # ------------------------------------------------------------------

    def resolve_contract(
        self,
        instrument_id : int,
        session_date  : date,
    ) -> ContractInfo | None:
        """
        Resolve a Databento instrument_id to ContractInfo.

        Uses InstrumentMap for symbol lookup, then _parse_symbol() for
        product/contract derivation.

        Returns None for unresolvable or irrelevant instruments
        (options, indices, unknown symbols).

        Results are cached — safe to call repeatedly in the hot path.
        """
        # Check cache first — this is the hot path during iter_events()
        if instrument_id in self._contract_cache:
            return self._contract_cache[instrument_id]

        # Resolve symbol via Databento InstrumentMap
        symbol = self._instrument_map.resolve(instrument_id, session_date)
        if not symbol:
            self._contract_cache[instrument_id] = None
            return None

        return self._resolve_from_symbol(instrument_id, symbol, session_date)

    def _resolve_from_symbol(
        self,
        instrument_id : int,
        symbol        : str,
        session_date  : date,
    ) -> ContractInfo | None:
        """
        Build ContractInfo from a known (instrument_id, symbol) pair.
        Populates the cache. Called by both resolve_contract() and
        list_instruments() to avoid redundant InstrumentMap lookups.
        """
        parsed = _parse_symbol(symbol, session_date)
        if parsed is None:
            self._contract_cache[instrument_id] = None
            return None

        product, contract, is_spread = parsed

        info = ContractInfo(
            product       = product,
            contract      = contract,
            venue         = self._venue,
            instrument_id = instrument_id,
            is_spread     = is_spread,
            tick_size     = 0,
            currency      = "",
        )
        self._contract_cache[instrument_id] = info
        return info

    def list_instruments(self) -> list[ContractInfo]:
        """
        Return ContractInfo for all resolvable instruments in this file.

        Strategy: scan the InstrumentMap interval entries for the session date.
        metadata.mappings is a dict of symbol → list of intervals, each interval
        having an instrument_id. We invert this to get all instrument_ids active
        on the session date, then resolve each one.

        Unknown or irrelevant instruments (options, unrecognized formats)
        are excluded — resolve_contract() returns None for those.

        Falls back to a lazy scan approach if the metadata API is unavailable:
        in that case, list_instruments() returns an empty list and the
        orchestrator will populate the instrument map lazily during iter_events().
        """
        assert self._store is not None, "open_session() not called"

        session_date = self._config.session_date
        results      = []
        seen_ids     : set[int] = set()

        try:
            # metadata.mappings structure (observed from databento-dbn):
            #   dict[
            #       symbol_str,          e.g. "ESZ5", "ESM6-ESU6"
            #       list[dict{
            #           "start_date": date,
            #           "end_date":   date,
            #           "symbol":     str   # instrument_id encoded as string
            #       }]
            #   ]
            # The human-readable symbol is the KEY.
            # The instrument_id is interval["symbol"] cast to int.
            # We filter intervals that cover the session date.
            for human_symbol, intervals in self._store.metadata.mappings.items():
                for interval in intervals:
                    # Filter: only intervals active on session_date
                    if not (interval["start_date"] <= session_date
                            < interval["end_date"]):
                        continue
                    try:
                        iid = int(interval["symbol"])
                    except (ValueError, KeyError):
                        continue
                    if iid in seen_ids:
                        continue
                    seen_ids.add(iid)
                    # Use _resolve_from_symbol directly — we already have
                    # the human-readable symbol, no need for InstrumentMap lookup
                    info = self._resolve_from_symbol(iid, human_symbol, session_date)
                    if info is not None:
                        results.append(info)
        except (AttributeError, TypeError):
            # Metadata API may differ across databento-dbn versions.
            # Return empty list — orchestrator falls back to lazy resolution.
            pass

        return results

    # ------------------------------------------------------------------
    # EVENT ITERATION
    # ------------------------------------------------------------------

    def _iter_raw(self) -> Iterator[db.MBOMsg]:
        """Yield raw MBOMsg records from the DBN store."""
        assert self._store is not None, "open_session() not called"
        yield from self._store

    def translate(self, raw_event: db.MBOMsg) -> dict | None:
        """
        Translate one Databento MBOMsg to a normalized event dict.

        Returns None for:
            - Instruments that cannot be resolved (options, indices, etc.)
            - Non-MBO record types that may appear in the stream

        Handles F_SNAPSHOT warmup detection per instrument.
        """
        # Guard: only process MBOMsg records
        # DBN files may contain instrument definition records or statistics
        if not isinstance(raw_event, db.MBOMsg):
            return None

        instrument_id = raw_event.instrument_id
        session_date  = self._config.session_date

        # Resolve contract — skip unknown instruments
        info = self.resolve_contract(instrument_id, session_date)
        if info is None:
            self._n_unknown_instr += 1
            return None

        # --- Warmup detection via F_SNAPSHOT ---
        # F_SNAPSHOT (0x20) is set on messages replayed from a book snapshot
        # at stream start. These bootstrap the book state. We track the
        # per-instrument warmup state so the orchestrator can signal
        # validator_state.warmup_end() at the right moment.
        flags     = int(raw_event.flags)
        is_snap   = bool(flags & db.RecordFlags.F_SNAPSHOT)

        if is_snap:
            self._in_warmup[instrument_id] = True
            self._n_snapshot_events += 1
        elif self._in_warmup.get(instrument_id, True):
            # First non-snapshot message for this instrument → warmup ends
            self._in_warmup[instrument_id] = False

        # --- Action translation ---
        # Databento action is a single char: A/C/M/R/T/F/N
        action_raw = str(raw_event.action)
        action     = Action.DATABENTO_MAP.get(action_raw)
        if action is None:
            # Unknown action — drop with None (will be logged by orchestrator)
            return None

        # --- Side translation ---
        side_raw = str(raw_event.side)
        side     = Side.DATABENTO_MAP.get(side_raw, Side.NONE)

        # --- Flags normalization ---
        # We remap Databento RecordFlags bitmask to our canonical Flags bitmask.
        # Bit values are identical for F_LAST (0x80), F_TOB (0x01), F_SNAPSHOT (0x20).
        # F_BAD_TS_RECV (0x08 in Databento) → we keep it as-is for traceability.
        normalized_flags = 0
        if flags & db.RecordFlags.F_LAST:
            normalized_flags |= Flags.F_LAST
        if flags & db.RecordFlags.F_TOB:
            normalized_flags |= Flags.F_TOB
        if flags & db.RecordFlags.F_SNAPSHOT:
            normalized_flags |= Flags.F_SNAPSHOT
        if flags & 0x08:
            # F_BAD_TS_RECV — map to our F_BAD_TS flag
            normalized_flags |= Flags.F_BAD_TS

        return {
            # --- Timing ---
            "ts_event"      : int(raw_event.ts_event),
            "ts_recv"       : int(raw_event.ts_recv),

            # --- Instrument identification ---
            "venue"         : info.venue,
            "product"       : info.product,
            "contract"      : info.contract,

            # --- Event semantics ---
            "action"        : action,
            "side"          : side,

            # --- Order fields ---
            # price: already fixed-point int64 in Databento MBO — pass through.
            # UNDEF_PRICE is a valid value for F_TOB clear messages.
            "price"         : int(raw_event.price),
            "size"          : int(raw_event.size),
            "order_id"      : int(raw_event.order_id),

            # --- Control flags ---
            "flags"         : normalized_flags,
            "sequence"      : int(raw_event.sequence),

            # --- Provider traceability ---
            "publisher_id"  : int(raw_event.publisher_id),
            "instrument_id" : instrument_id,
        }

    # ------------------------------------------------------------------
    # WARMUP BOUNDARY
    # ------------------------------------------------------------------

    def is_warmup_event(self, normalized_event: dict) -> bool:
        """
        Return True if this event is part of the warmup period.

        Detection logic:
            F_SNAPSHOT set → warmup event (book bootstrap replay)
            F_SNAPSHOT not set → live event

        The per-instrument warmup state (_in_warmup) is updated during
        translate(). Here we simply check the normalized flags field.

        Note: once F_SNAPSHOT clears for an instrument, all subsequent
        events for that instrument are live — even if F_SNAPSHOT appears
        later (which would indicate a mid-session book reset, a separate
        concern handled by the CLEAR action).
        """
        return bool(normalized_event.get("flags", 0) & Flags.F_SNAPSHOT)

    # ------------------------------------------------------------------
    # VENUE INFO
    # ------------------------------------------------------------------

    def get_venue(self) -> str:
        return self._venue

    # ------------------------------------------------------------------
    # STATS
    # ------------------------------------------------------------------

    def get_stats(self) -> dict:
        stats = super().get_stats()
        stats.update({
            "n_snapshot_events" : self._n_snapshot_events,
            "n_unknown_instr"   : self._n_unknown_instr,
            "venue"             : self._venue,
            "n_instruments"     : len(self._contract_cache),
        })
        return stats