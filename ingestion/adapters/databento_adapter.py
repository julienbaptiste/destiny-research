"""
databento_adapter.py — Databento MBO -> canonical Destiny MBO adapter.

The adapter preserves Databento provider/control flags using their native bit
assignments and translates provider action/side codes into the canonical event
contract. Normalization provenance remains zero because no semantic inference is
required for ordinary Databento MBO rows.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Iterator

import databento as db

from .base import BaseAdapter, ContractInfo, SessionConfig
from ..schema import Action, Side, NormFlags


# ---------------------------------------------------------------------------
# VENUE MAPPING
# ---------------------------------------------------------------------------

_DATASET_TO_VENUE: dict[str, str] = {
    "glbx.mdp3": "CME",
    "xeur.eobi": "EUREX",
    "xnas.itch": "NASDAQ",
    "xnys.pillar": "NYSE",
}


# ---------------------------------------------------------------------------
# SYMBOL NORMALIZATION
# ---------------------------------------------------------------------------

_MONTH_CODE: dict[str, int] = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}
_MONTH_TO_CODE: dict[int, str] = {value: key for key, value in _MONTH_CODE.items()}

_EUREX_MONTH_NAME: dict[str, str] = {
    "JAN": "F",
    "FEB": "G",
    "MAR": "H",
    "APR": "J",
    "MAY": "K",
    "JUN": "M",
    "JUL": "N",
    "AUG": "Q",
    "SEP": "U",
    "OCT": "V",
    "NOV": "X",
    "DEC": "Z",
}

_RE_OUTRIGHT = re.compile(r"^([A-Z]+)([FGHJKMNQUVXZ])(\d)$")
_RE_SPREAD = re.compile(
    r"^([A-Z]+)([FGHJKMNQUVXZ])(\d)-[A-Z]+([FGHJKMNQUVXZ])(\d)$"
)
_RE_EUREX_OUTRIGHT = re.compile(
    r"^([A-Z]+)\s+SI\s+(\d{4})(\d{2})\d{2}\s+CS$"
)
_RE_EUREX_SPREAD = re.compile(
    r"^([A-Z]+)\.S\.([A-Z]{3})(\d{2})\.([A-Z]{3})(\d{2})\.SPD$"
)


def _normalize_expiry_year(single_digit_year: str, session_date: date) -> int:
    """Resolve Databento's single-digit futures year around the session decade."""
    digit = int(single_digit_year)
    current_year = session_date.year
    candidate = (current_year // 10) * 10 + digit
    if candidate < current_year - 2:
        candidate += 10
    return candidate


def _parse_symbol(
    symbol: str,
    session_date: date,
) -> tuple[str, str, bool] | None:
    """Return (product, canonical contract, is_spread) for supported futures."""
    symbol = symbol.strip()

    match = _RE_OUTRIGHT.match(symbol)
    if match:
        product, month_code, year_digit = match.groups()
        year = _normalize_expiry_year(year_digit, session_date)
        return product, f"{product}{month_code}{str(year)[-2:]}", False

    match = _RE_SPREAD.match(symbol)
    if match:
        product, month_1, year_1, month_2, year_2 = match.groups()
        year_1_full = _normalize_expiry_year(year_1, session_date)
        year_2_full = _normalize_expiry_year(year_2, session_date)
        contract = (
            f"{product}_CAL_"
            f"{month_1}{str(year_1_full)[-2:]}"
            f"{month_2}{str(year_2_full)[-2:]}"
        )
        return product, contract, True

    match = _RE_EUREX_OUTRIGHT.match(symbol)
    if match:
        product, year_text, month_text = match.groups()
        month_code = _MONTH_TO_CODE.get(int(month_text))
        if month_code is None:
            return None
        return product, f"{product}{month_code}{year_text[-2:]}", False

    match = _RE_EUREX_SPREAD.match(symbol)
    if match:
        product, month_name_1, year_1, month_name_2, year_2 = match.groups()
        month_1 = _EUREX_MONTH_NAME.get(month_name_1)
        month_2 = _EUREX_MONTH_NAME.get(month_name_2)
        if month_1 is None or month_2 is None:
            return None
        return product, f"{product}_CAL_{month_1}{year_1}{month_2}{year_2}", True

    return None


# ---------------------------------------------------------------------------
# ADAPTER
# ---------------------------------------------------------------------------

class DatabentoAdapter(BaseAdapter):
    """Streaming adapter for Databento DBN MBO files."""

    PROVIDER = "databento"

    def __init__(self) -> None:
        super().__init__()
        self._store: db.DBNStore | None = None
        self._instrument_map: db.common.symbology.InstrumentMap | None = None
        self._venue = ""
        self._contract_cache: dict[int, ContractInfo | None] = {}
        self._in_warmup: dict[int, bool] = {}
        self._n_snapshot_events = 0
        self._n_unknown_instr = 0
        self._last_sequence: int | None = None
        self._next_subsequence = 0

    # ------------------------------------------------------------------
    # SESSION LIFECYCLE
    # ------------------------------------------------------------------

    def _open(self, raw_source: object, config: SessionConfig) -> None:
        self._store = db.DBNStore.from_file(raw_source)
        self._instrument_map = db.common.symbology.InstrumentMap()
        self._instrument_map.insert_metadata(self._store.metadata)
        dataset = self._store.metadata.dataset.lower()
        self._venue = _DATASET_TO_VENUE.get(dataset, dataset.upper())
        self._last_sequence = None
        self._next_subsequence = 0

    def _close(self) -> None:
        self._store = None
        self._instrument_map = None
        self._contract_cache.clear()
        self._in_warmup.clear()
        self._last_sequence = None
        self._next_subsequence = 0

    # ------------------------------------------------------------------
    # INSTRUMENT RESOLUTION
    # ------------------------------------------------------------------

    def resolve_contract(
        self,
        instrument_id: int,
        session_date: date,
    ) -> ContractInfo | None:
        if instrument_id in self._contract_cache:
            return self._contract_cache[instrument_id]

        assert self._instrument_map is not None, "open_session() not called"
        symbol = self._instrument_map.resolve(instrument_id, session_date)
        if not symbol:
            self._contract_cache[instrument_id] = None
            return None
        return self._resolve_from_symbol(instrument_id, symbol, session_date)

    def _resolve_from_symbol(
        self,
        instrument_id: int,
        symbol: str,
        session_date: date,
    ) -> ContractInfo | None:
        parsed = _parse_symbol(symbol, session_date)
        if parsed is None:
            self._contract_cache[instrument_id] = None
            return None

        product, contract, is_spread = parsed
        info = ContractInfo(
            product=product,
            contract=contract,
            venue=self._venue,
            instrument_id=instrument_id,
            is_spread=is_spread,
            tick_size=0,
            currency="",
        )
        self._contract_cache[instrument_id] = info
        return info

    def list_instruments(self) -> list[ContractInfo]:
        assert self._store is not None, "open_session() not called"
        assert self._config is not None, "open_session() not called"

        session_date = self._config.session_date
        results: list[ContractInfo] = []
        seen_ids: set[int] = set()

        try:
            for human_symbol, intervals in self._store.metadata.mappings.items():
                for interval in intervals:
                    if not (
                        interval["start_date"] <= session_date < interval["end_date"]
                    ):
                        continue
                    try:
                        instrument_id = int(interval["symbol"])
                    except (ValueError, TypeError, KeyError):
                        continue
                    if instrument_id in seen_ids:
                        continue
                    seen_ids.add(instrument_id)
                    info = self._resolve_from_symbol(
                        instrument_id,
                        human_symbol,
                        session_date,
                    )
                    if info is not None:
                        results.append(info)
        except (AttributeError, TypeError):
            # Metadata API differs between databento-dbn versions. Lazy
            # resolution in translate() remains authoritative.
            pass

        return results

    # ------------------------------------------------------------------
    # EVENT ITERATION / TRANSLATION
    # ------------------------------------------------------------------

    def _iter_raw(self) -> Iterator[db.MBOMsg]:
        assert self._store is not None, "open_session() not called"
        yield from self._store

    def _allocate_subsequence(self, sequence: int) -> int:
        """Allocate a monotone uint16 row index within one provider sequence."""
        if self._last_sequence != sequence:
            self._last_sequence = sequence
            self._next_subsequence = 1
            return 0

        subsequence = self._next_subsequence
        if subsequence > 0xFFFF:
            raise OverflowError(f"subsequence overflow for Databento sequence={sequence}")
        self._next_subsequence += 1
        return subsequence

    def translate(self, raw_event: db.MBOMsg) -> dict | None:
        if not isinstance(raw_event, db.MBOMsg):
            return None
        assert self._config is not None, "open_session() not called"

        instrument_id = int(raw_event.instrument_id)
        info = self.resolve_contract(instrument_id, self._config.session_date)
        if info is None:
            self._n_unknown_instr += 1
            return None

        raw_flags = int(raw_event.flags) & 0xFF
        is_snapshot = bool(raw_flags & int(db.RecordFlags.F_SNAPSHOT))
        if is_snapshot:
            self._in_warmup[instrument_id] = True
            self._n_snapshot_events += 1
        elif self._in_warmup.get(instrument_id, True):
            self._in_warmup[instrument_id] = False

        action = Action.DATABENTO_MAP.get(str(raw_event.action))
        if action is None:
            return None
        side = Side.DATABENTO_MAP.get(str(raw_event.side), Side.NONE)

        # Representation normalization is lossless here: preserve every raw
        # provider/control bit. Canonical semantics only assign meanings to the
        # documented subset; reserved/provider-specific bits remain auditable.
        flags = raw_flags
        sequence = int(raw_event.sequence)

        return {
            "ts_event": int(raw_event.ts_event),
            "ts_recv": int(raw_event.ts_recv),
            "venue": info.venue,
            "product": info.product,
            "contract": info.contract,
            "action": action,
            "side": side,
            "price": int(raw_event.price),
            "size": int(raw_event.size),
            "order_id": int(raw_event.order_id),
            "flags": flags,
            "norm_flags": int(NormFlags.NONE),
            "sequence": sequence,
            "subsequence": self._allocate_subsequence(sequence),
            "publisher_id": int(raw_event.publisher_id),
            "instrument_id": instrument_id,
        }

    # ------------------------------------------------------------------
    # WARMUP / INFO / STATS
    # ------------------------------------------------------------------

    def is_warmup_event(self, normalized_event: dict) -> bool:
        # F_SNAPSHOT remains the native Databento 0x20 bit.
        return bool(int(normalized_event.get("flags", 0)) & 0x20)

    def get_venue(self) -> str:
        return self._venue

    def get_stats(self) -> dict:
        stats = super().get_stats()
        stats.update(
            {
                "n_snapshot_events": self._n_snapshot_events,
                "n_unknown_instr": self._n_unknown_instr,
                "venue": self._venue,
                "n_instruments": len(self._contract_cache),
            }
        )
        return stats
