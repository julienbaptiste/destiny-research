"""
hkex_adapter.py — HKEX OMD-D adapter: raw Parquet → normalized MBO stream.

═══════════════════════════════════════════════════════════════════════════════
ROLE IN THE PIPELINE
═══════════════════════════════════════════════════════════════════════════════

  data/raw/provider=HKEX/venue=HKEX/product=HSI/year=2026/month=02/
      hkex-hsi_20260203_orders.parquet
      hkex-hsi_20260203_trades.parquet
          ↓
  [HKEXAdapter]  ← YOU ARE HERE
          ↓
  data/normalized/provider=HKEX/venue=HKEX/product=HSI/contract=HSIH26/
      year=2026/month=02/HSIH26_20260203_mbo.parquet

═══════════════════════════════════════════════════════════════════════════════
KEY DESIGN DECISIONS
═══════════════════════════════════════════════════════════════════════════════

1. MERGE STRATEGY — orders + trades into a chronological MBO stream
   The raw files are separate (one for orders, one for trades). They must be
   merged in exact event order. The sort key is:
       (send_time_ns, seq_num, msg_index)
   send_time_ns is the PacketHeader.SendTime — shared by all messages in the
   same packet (same seq_num). msg_index is the 0-based position within the
   packet. Together they give an exact total ordering across both files.
   DuckDB UNION ALL + ORDER BY on the merged view handles this efficiently
   without loading both files entirely into RAM.

2. SYMBOL → CONTRACT RESOLUTION
   The raw parquet contains `symbol` (e.g. "HSIH6") and `class_code` (e.g. "HSI").
   We normalize the symbol to our contract convention:
       "HSIH6" → "HSIH26"   (4-digit year, e.g. H=March 2026)
       "MHIG6" → "MHIG26"   (G=February 2026)
   HKEX uses single-digit year codes identical to CME futures month codes.
   The same _normalize_expiry_year() logic from databento_adapter applies.

3. PRICE NORMALIZATION
   NumberOfDecimalsPrice = 0 for HSI/MHI/HHI/MCH (confirmed empirically).
   raw Int32 price = actual price in index points.
   Normalized price = raw_price * FIXED_PRICE_SCALE (1e9).
   Example: HSI price 26815 → 26_815_000_000_000 in fixed-point int64.

4. ACTION MAPPING
   HKEX msg_type → normalized Action:
       330 (AddOrder)      → ADD
       332 (DeleteOrder)   → CANCEL
       335 (OrderbookClear)→ CLEAR
       364 (COP)           → NONE   (informational, not a book event)
       350 (Trade)         → TRADE  + synthetic CANCEL (see §10 below)
   Note: ModifyOrder (331) is never observed in practice — HKEX emits
   Delete+Add pairs instead. Mapped to MODIFY if encountered.

5. SIDE MAPPING
   Orders (Add/Modify/Delete): raw int8  0=BID, 1=ASK, -1=NONE (Clear/COP)
   Trades: raw int8 0=NONE, 2=Buy aggressor, 3=Sell aggressor
   We normalize to BID/ASK/NONE strings.
   For trades: Buy aggressor → ASK (passive resting sell was hit),
               Sell aggressor → BID (passive resting buy was hit).
   This follows the standard convention: trade side = passive side = book side
   that was consumed. Same as Databento convention.

6. FLAGS
   HKEX has no F_SNAPSHOT, F_TOB, or F_LAST concept equivalent to Databento.
   - F_LAST: set on all events (HKEX packets contain 1 message typically,
     so every message is implicitly "last"). We set F_LAST=1 on all events.
     The reconstruction engine requires F_LAST to emit LOB snapshots.
   - F_BAD_TS: set because HKEX timestamp precision is ~10ms despite ns storage.
   - F_SNAPSHOT / F_TOB: never set for HKEX.
   The OrderbookClear (335) at session open/auction end effectively replaces
   the entire book — handled via Action.CLEAR, not F_TOB.

7. WARMUP
   HKEX has no equivalent to Databento's F_SNAPSHOT book bootstrap.
   No warmup mechanism is required — warmup_enabled=False in SessionConfig.
   The first message of the day starts from a known-empty book state after
   the OrderbookClear (335) messages at session open.

8. INSTRUMENT IDENTIFICATION
   instrument_id in the normalized schema = orderbook_id from HKEX Series Defs.
   publisher_id = 0 (HKEX is not a Databento publisher).

9. CONTRACT SPLIT
   The raw parquet for a given product (e.g. HSI) contains all contracts
   (front month, back months) mixed together. This adapter splits them by
   contract during iter_events(), routing each event to its contract's writer
   via the ContractInfo returned by resolve_contract().

10. SYNTHETIC CANCEL ON TRADE — passive order book decrement
    (added 2026-04-08, ref: HKEX OMD-D spec §3.5 Trade message)

    HKEX Trade (350) carries the order_id of the PASSIVE resting order
    (not the aggressor). Per the OMD-D specification:

        "If the OrderID within the Trade (350) message is non-zero then
         users must reduce the resting order identified by the 'Quantity'
         within the Trade (350) message. If the outstanding quantity is
         zero the order must be deleted."

    Unlike Databento (which emits explicit FILL events that the reconstruction
    engine can process), HKEX provides no separate message to decrement the
    resting order on a fill. Without compensation, the book accumulates stale
    liquidity after every trade → crossed spreads in the reconstructed MBP-1.

    Solution: for every Trade (350) with order_id > 0, the adapter emits TWO
    normalized events:
        1. TRADE  — the original trade event (unchanged)
        2. CANCEL — a synthetic cancel that decrements the passive resting
                    order by the traded quantity

    Trades with order_id == 0 are leg trades from combo-vs-combo executions
    (per OMD-D spec). They have no associated resting order to decrement →
    only the TRADE event is emitted, no synthetic CANCEL.

    The synthetic CANCEL uses the same timestamps, sequence, price, side,
    and order_id as the TRADE — the reconstruction engine's _book_cancel()
    handles partial and full cancels correctly.

    HKEX does NOT emit a separate DeleteOrder (332) after a fill. Deletes
    only occur on participant-initiated cancellations or as part of
    Delete+Add modify sequences. There is no risk of double-decrement.

═══════════════════════════════════════════════════════════════════════════════
USAGE (via ingest.py orchestrator)
═══════════════════════════════════════════════════════════════════════════════

    from ingestion.adapters.hkex_adapter import HKEXAdapter
    from ingestion.adapters.base import SessionConfig
    from datetime import date

    adapter = HKEXAdapter()
    config  = SessionConfig(session_date=date(2026, 2, 3))
    # raw_source = directory containing the raw parquet files for one product
    raw_source = Path(".../raw/provider=HKEX/venue=HKEX/product=HSI/year=2026/month=02")
    adapter.open_session(raw_source, config)
    for event in adapter.iter_events():
        if event is not None:
            # event is a normalized dict conforming to NORMALIZED_MBO_SCHEMA
            ...
    adapter.close_session()

═══════════════════════════════════════════════════════════════════════════════
ADDING HKEX TO ingest.py
═══════════════════════════════════════════════════════════════════════════════

In ingest.py _get_adapter(), add:
    from .adapters.hkex_adapter import HKEXAdapter
    adapters = {
        "databento": DatabentoAdapter,
        "hkex":      HKEXAdapter,    ← add this line
    }
"""

from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path
from typing import Iterator

import duckdb
import pyarrow as pa

from .base import BaseAdapter, ContractInfo, SessionConfig
from ..schema import (
    Action,
    Flags,
    Side,
    FIXED_PRICE_SCALE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

VENUE    : str = "HKEX"
PROVIDER : str = "hkex"

# HKEX futures month codes (identical to CME conventions)
_MONTH_CODE: dict[str, int] = {
    "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
    "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12,
}

# Raw HKEX msg_type → normalized Action string
# 331 (ModifyOrder) is included for completeness but never observed in practice.
_MSG_TYPE_TO_ACTION: dict[int, str] = {
    330: Action.ADD,
    331: Action.MODIFY,
    332: Action.CANCEL,
    335: Action.CLEAR,
    364: Action.NONE,    # Calculated Opening Price — informational only
    350: Action.TRADE,
}

# Raw order side (int8) → normalized Side string
# Orders: 0=Bid, 1=Offer, -1=no side (Clear/COP)
_ORDER_SIDE_MAP: dict[int, str] = {
    0:  Side.BID,
    1:  Side.ASK,
    -1: Side.NONE,
}

# Raw trade side (int8) → normalized Side string
# Trade side mapping for TRADE events (aggressor side)
# For FILL and CANCEL events, we'll invert this to get passive side
_TRADE_SIDE_MAP: dict[int, str] = {
    0: Side.NONE,
    2: Side.ASK,    # Buy aggressor → sold into the ASK (hit resting sell)
    3: Side.BID,    # Sell aggressor → bought from the BID (hit resting buy)
}

# Passive side mapping for FILL/CANCEL events (inverse of trade aggressor)
_PASSIVE_SIDE_MAP: dict[int, str] = {
    0: Side.NONE,
    2: Side.BID,    # Buy order resting → BID side
    3: Side.ASK,    # Sell order resting → ASK side
}

# Regex: HKEX futures symbol e.g. "HSIH6", "MHIG6", "HHIU6", "MCHZ6"
# Pattern: {ClassCode 2-3 chars}{MonthCode}{SingleDigitYear}
_RE_HKEX_FUTURE = re.compile(r"^([A-Z]{2,4})([FGHJKMNQUVXZ])(\d)$")

# Flags applied to all HKEX events:
#   F_LAST (0x80): set on all events — HKEX packets are typically single-message,
#                  so every event is the last (and only) event in its atomic group.
#                  The reconstruction engine requires F_LAST to emit LOB snapshots.
#   F_BAD_TS (0x04): set because HKEX clock precision is ~10ms despite ns storage.
_BASE_FLAGS: int = int(Flags.F_LAST) | int(Flags.F_BAD_TS)

# DuckDB batch size for the merged event stream.
# Larger batches = fewer Python round-trips but more RAM per fetch.
# 200k rows × ~300 bytes ≈ 60 MB per batch — safe within 16 GB.
_DUCKDB_BATCH_SIZE: int = 200_000

# In hkex_adapter.py module level — tune to control peak RAM:
#   50_000  → ~200-300 MB peak  (safe, slower)
#   200_000 → ~500-600 MB peak  (faster, still safe on 16 GB)
#   500_000 → ~1-1.5 GB peak    (previous behavior, fastest)
_DEFAULT_BATCH_SIZE_ROWS: int = 200_000


# ─────────────────────────────────────────────────────────────────────────────
# Symbol normalization helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_expiry_year(single_digit: str, session_date: date) -> int:
    """
    Convert a single-digit expiry year to a 4-digit year.

    Identical logic to databento_adapter._normalize_expiry_year().
    Uses the session date to disambiguate the decade.

    Examples (session_date = 2026-02-03):
        "6" → 2026,  "7" → 2027,  "5" → 2025 (back-adjusted if < current-2)
    """
    digit       = int(single_digit)
    current_yr  = session_date.year
    decade_base = (current_yr // 10) * 10
    candidate   = decade_base + digit
    if candidate < current_yr - 2:
        candidate += 10
    return candidate


def _parse_hkex_symbol(
    symbol      : str,
    session_date: date,
) -> tuple[str, str] | None:
    """
    Parse an HKEX futures symbol into (product, contract).

    Returns None if the symbol does not match the expected futures format
    (e.g. options have a different format with strike + P/C).

    Examples:
        "HSIH6"  → ("HSI",  "HSIH26")   H=March 2026
        "MHIG6"  → ("MHI",  "MHIG26")   G=February 2026
        "HHIU6"  → ("HHI",  "HHIU26")   U=September 2026
        "MCHZ6"  → ("MCH",  "MCHZ26")   Z=December 2026
    """
    m = _RE_HKEX_FUTURE.match(symbol.strip())
    if not m:
        return None

    product    = m.group(1)                                   # e.g. "HSI"
    month_code = m.group(2)                                   # e.g. "H"
    year_4d    = _normalize_expiry_year(m.group(3), session_date)
    year_2d    = year_4d % 100                                # e.g. 26
    contract   = f"{product}{month_code}{year_2d}"            # e.g. "HSIH26"

    return product, contract


# ─────────────────────────────────────────────────────────────────────────────
# HKEXAdapter
# ─────────────────────────────────────────────────────────────────────────────

class HKEXAdapter(BaseAdapter):
    """
    HKEX OMD-D adapter: raw Parquet orders+trades → normalized MBO stream.

    Implements BaseAdapter for the HKEX provider. The normalized output
    is identical in schema to the Databento adapter output — the
    reconstruction engine and feature engineering layer are provider-agnostic.

    raw_source (passed to open_session) must be a Path to the directory
    containing the raw parquet files for one product and one month:
        .../raw/provider=HKEX/venue=HKEX/product=HSI/year=2026/month=02/

    The adapter discovers the orders and trades files for the session_date
    from this directory automatically.
    """

    PROVIDER: str = "HKEX"

    def __init__(self) -> None:
        super().__init__()

        # Raw parquet file paths for the current session
        self._orders_path : Path | None = None
        self._trades_path : Path | None = None

        # Instrument map: symbol (e.g. "HSIH6") → ContractInfo
        # Built lazily during list_instruments() / first iter_events() call.
        # Keyed by symbol string (unique per product per session).
        self._symbol_map  : dict[str, ContractInfo] = {}

        # orderbook_id → ContractInfo (for fast lookup in translate())
        # orderbook_id is the HKEX-native instrument identifier stored in the
        # raw parquet. Multiple orderbook_ids can map to the same contract
        # (e.g. combination orders), but for futures they are 1:1.
        self._ob_map      : dict[int, ContractInfo] = {}

        # Stats specific to HKEX
        self._n_unknown_symbol    : int = 0
        self._n_clears            : int = 0
        self._n_cops              : int = 0
        self._n_mod_orders        : int = 0   # should be 0 — HKEX emits Del+Add
        self._n_synthetic_cancels : int = 0   # CANCEL events synthesized from Trade (350)

        # Shadow state: resting order sizes AND prices for DeleteOrder resolution.
        # Keyed by order_id → (resting_size_lots, price_fp).
        # price_fp is required because DeleteOrder (332) carries price=0 in the
        # raw feed — without the cached price, _book_cancel() finds no level and
        # silently skips the cancel, leaving phantom orders in the book.
        self._order_sizes : dict[int, tuple[int, int]] = {}

        # In HKEXAdapter.__init__(), add:
        self._batch_size_rows: int = _DEFAULT_BATCH_SIZE_ROWS

    # ─────────────────────────────────────────────────────────────────────────
    # BaseAdapter lifecycle
    # ─────────────────────────────────────────────────────────────────────────

    def _open(self, raw_source: object, config: SessionConfig) -> None:
        """
        Locate the raw parquet files for this session.

        raw_source must be a Path to the product/month directory.
        The date is extracted from config.session_date.

        File naming convention (from hkex_raw_parser.py):
            hkex-{product_lower}_{YYYYMMDD}_orders.parquet
            hkex-{product_lower}_{YYYYMMDD}_trades.parquet

        We discover the product name by scanning for matching files —
        the adapter does not need the product name as a separate input.
        """
        raw_dir  = Path(raw_source)
        date_str = config.session_date.strftime("%Y%m%d")

        # Discover orders and trades files for this date
        orders_files = sorted(raw_dir.glob(f"hkex-*_{date_str}_orders.parquet"))
        trades_files = sorted(raw_dir.glob(f"hkex-*_{date_str}_trades.parquet"))

        if not orders_files:
            raise FileNotFoundError(
                f"No HKEX orders parquet found for {date_str} in {raw_dir}"
            )
        if not trades_files:
            # No trades file — possible on holiday or zero-activity day.
            # We'll stream orders only; _iter_raw() handles None trades path.
            self._trades_path = None
        else:
            self._trades_path = trades_files[0]

        # Take the first match (there should be exactly one per date)
        self._orders_path = orders_files[0]

    def _close(self) -> None:
        """Release resources — DuckDB connections are closed after each query."""
        self._orders_path = None
        self._trades_path = None
        self._symbol_map.clear()
        self._ob_map.clear()
        self._order_sizes.clear()

    # ─────────────────────────────────────────────────────────────────────────
    # Instrument resolution
    # ─────────────────────────────────────────────────────────────────────────

    def resolve_contract(
        self,
        instrument_id : int,
        session_date  : date,
    ) -> ContractInfo | None:
        """
        Resolve an HKEX orderbook_id to a ContractInfo.

        instrument_id here = orderbook_id from the raw parquet.
        Results are cached in _ob_map after the first resolution.

        Returns None for unknown orderbook_ids (e.g. options, combos,
        or instruments outside our filter set).
        """
        return self._ob_map.get(instrument_id)

    def list_instruments(self) -> list[ContractInfo]:
        """
        Scan the raw orders parquet to discover all unique instruments
        present in this session, resolve each to ContractInfo, and
        populate _symbol_map and _ob_map caches.

        Called by the orchestrator after open_session() to pre-build
        the instrument map and create per-contract Parquet writers.

        Uses DuckDB for an efficient GROUP BY on the raw parquet —
        no need to scan the full file row by row.
        """
        assert self._orders_path is not None, "open_session() not called"
        session_date = self._config.session_date

        # DuckDB: get unique (orderbook_id, symbol) pairs from the orders file.
        # The trades file uses the same orderbook_id/symbol — orders file is
        # sufficient and faster (larger, but better for this kind of scan since
        # it has more distinct instruments represented).
        con = duckdb.connect()
        rows = con.execute(f"""
            SELECT DISTINCT orderbook_id, symbol
            FROM read_parquet('{self._orders_path}')
            ORDER BY symbol
        """).fetchall()
        con.close()

        results: list[ContractInfo] = []

        for orderbook_id, symbol in rows:
            info = self._build_contract_info(orderbook_id, symbol, session_date)
            if info is not None:
                results.append(info)
                self._symbol_map[symbol]     = info
                self._ob_map[orderbook_id]   = info

        return results

    def _build_contract_info(
        self,
        orderbook_id : int,
        symbol       : str,
        session_date : date,
    ) -> ContractInfo | None:
        """
        Build a ContractInfo from an HKEX symbol string and orderbook_id.

        Returns None if the symbol cannot be parsed (options have a different
        format — they include strike price and P/C indicator).

        tick_size is taken from market_config.MARKET_CONFIG if the product
        is listed there, otherwise defaults to 0 (unknown).
        """
        parsed = _parse_hkex_symbol(symbol, session_date)
        if parsed is None:
            self._n_unknown_symbol += 1
            return None

        product, contract = parsed

        # Look up tick_size from market_config if available
        tick_size = 0
        try:
            from ..market_config import MARKET_CONFIG
            cfg = MARKET_CONFIG.get(product)
            if cfg:
                tick_size = cfg["tick_size_fp"]
        except (ImportError, KeyError):
            pass

        # Look up currency from market_config if available
        currency = "HKD"   # default for all HKEX futures
        try:
            from ..market_config import MARKET_CONFIG
            cfg = MARKET_CONFIG.get(product)
            if cfg:
                currency = cfg.get("currency", "HKD")
        except (ImportError, KeyError):
            pass

        return ContractInfo(
            product       = product,
            contract      = contract,
            venue         = VENUE,
            instrument_id = orderbook_id,
            is_spread     = False,          # HKEX combination orders filtered out
            tick_size     = tick_size,
            currency      = currency,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Event iteration — core of the adapter
    # ─────────────────────────────────────────────────────────────────────────

    def _iter_raw(self) -> Iterator[dict]:
        """
        Yield raw event dicts from the merged orders+trades stream,
        sorted by seq_num (global OMD-D channel counter shared across
        both files — guarantees correct interleaving of orders and trades).

        Uses DuckDB UNION ALL + ORDER BY seq_num on the two parquet files.
        At ~750 MB total, DuckDB handles this comfortably within 4 GB limit
        without spilling. Results are streamed back in Arrow batches to
        avoid materializing the full result set in Python memory.
        """
        import duckdb
        import os

        assert self._orders_path is not None, "open_session() not called"

        BATCH_ROWS = getattr(self, "_batch_size_rows", 50_000)

        # Columns to select from each file — only what translate() needs.
        # Missing columns are filled with NULL and cast to the correct type.
        # Both sides of the UNION ALL must have identical column names and types.
        orders_sql = f"""
            SELECT
                send_time_ns,
                seq_num,
                msg_index,
                msg_type,
                orderbook_id,
                symbol,
                class_code,
                order_id,
                price,
                CAST(quantity AS BIGINT)    AS quantity,
                side,
                NULL::BIGINT                AS trade_time_ns,
                NULL::BIGINT                AS trade_id,
                NULL::TINYINT               AS deal_type,
                NULL::INTEGER               AS combo_group_id,
                'order'                     AS source
            FROM read_parquet('{self._orders_path}')
        """

        if self._trades_path is not None:
            trades_sql = f"""
            UNION ALL
            SELECT
                send_time_ns,
                seq_num,
                msg_index,
                350::INTEGER                AS msg_type,
                orderbook_id,
                symbol,
                class_code,
                order_id,
                price,
                CAST(quantity AS BIGINT)    AS quantity,
                side,
                trade_time_ns,
                trade_id,
                deal_type,
                combo_group_id,
                'trade'                     AS source
            FROM read_parquet('{self._trades_path}')
            """
        else:
            trades_sql = ""

        query = f"""
            SELECT *
            FROM (
                {orders_sql}
                {trades_sql}
            )
            ORDER BY seq_num
        """

        import os
        os.makedirs('/tmp/duckdb_hkex_spill', exist_ok=True)
        
        con = duckdb.connect()
        con.execute("SET memory_limit='6GB'")
        con.execute("SET temp_directory='/tmp/duckdb_hkex_spill'")

        # Stream results as Arrow record batches — avoids materializing
        # the full result set in Python memory.
        reader = con.execute(query).fetch_record_batch(BATCH_ROWS)

        try:
            while True:
                try:
                    batch = reader.read_next_batch()
                except StopIteration:
                    break
                for row in batch.to_pylist():
                    yield row
        finally:
            con.close()

    def iter_events(self) -> Iterator[dict | None]:
        """
        Override BaseAdapter.iter_events() to handle translate() returning
        a list of events (TRADE + synthetic CANCEL) instead of a single dict.

        For most event types, translate() returns a single-element list.
        For Trade (350) with order_id > 0, it returns [TRADE, CANCEL] — both
        events are yielded in sequence so the downstream pipeline (validator,
        writer) sees them as two separate normalized events.

        This override is HKEX-specific. The Databento adapter uses the default
        BaseAdapter.iter_events() which expects translate() → dict | None.
        """
        assert self._is_open, "open_session() must be called before iter_events()"

        for raw_event in self._iter_raw():
            result = self.translate(raw_event)
            if result is None:
                self._n_dropped += 1
                yield None
            else:
                # translate() returns list[dict] — unroll for the downstream pipeline
                for event in result:
                    self._n_yielded += 1
                    yield event

    def translate(self, raw_event: dict) -> list[dict] | None:
        """
        Translate one raw HKEX event dict to normalized MBO event(s).

        Returns:
            list[dict] — one or two normalized events:
                - Most event types: single-element list [event]
                - Trade (350) with order_id > 0: [TRADE, synthetic CANCEL]
                  (see module docstring §10 for rationale)
                - Trade (350) with order_id == 0: [TRADE] only
                  (combo leg trade, no resting order to decrement)
            None — event should be dropped:
                - Unknown orderbook_id (unresolved instrument)
                - COP (msg_type=364) — informational, not a book event
                - Trade with combo_group_id != 0 — combination leg, avoid
                  double-count (same logic as Databento non-printable filter)

        Handles all 5 order message types (330/331/332/335/364) and trades (350).
        """
        orderbook_id = raw_event["orderbook_id"]
        info         = self._ob_map.get(orderbook_id)

        if info is None:
            # Instrument not in our map (option, combo, or unknown)
            self._n_unknown_symbol += 1
            return None

        msg_type = raw_event["msg_type"]
        source   = raw_event["source"]

        # ── Action mapping ────────────────────────────────────────────────────
        action = _MSG_TYPE_TO_ACTION.get(msg_type, Action.NONE)

        # COP (364) — informational, not a book event → drop
        if msg_type == 364:
            self._n_cops += 1
            return None

        # Count rare message types for monitoring
        if msg_type == 331:
            self._n_mod_orders += 1
        elif msg_type == 335:
            self._n_clears += 1

        # ── Side mapping ──────────────────────────────────────────────────────
        raw_side = int(raw_event["side"])
        if source == "trade":
            side = _TRADE_SIDE_MAP.get(raw_side, Side.NONE)
        else:
            side = _ORDER_SIDE_MAP.get(raw_side, Side.NONE)

        # ── Price normalization ───────────────────────────────────────────────
        # NumberOfDecimalsPrice = 0 for HSI/MHI/HHI/MCH (confirmed empirically).
        # raw Int32 price in index points → multiply by FIXED_PRICE_SCALE (1e9).
        # Price = 0 on Delete (332) and Clear (335) in the raw feed — resolved
        # from shadow state for Delete (see order size tracking block below).
        raw_price = int(raw_event["price"])
        price_fp  = raw_price * FIXED_PRICE_SCALE   # fixed-point int64

        # ── Size normalization ────────────────────────────────────────────────
        # Orders: raw Uint32 quantity. Delete and Clear have quantity=0.
        # Trades: raw Uint64 quantity.
        size = int(raw_event["quantity"])

        # ── Timestamps ────────────────────────────────────────────────────────
        # ts_event: exchange-assigned time. For orders, use send_time_ns
        #           (PacketHeader.SendTime, ~1ms precision).
        #           For trades, prefer trade_time_ns (matching engine, ~10ms)
        #           if available and non-zero.
        # ts_recv: we set to send_time_ns — HKEX historical data has no
        #          separate receive timestamp (we are the receiver, not Databento).
        send_time = int(raw_event["send_time_ns"])
        if source == "trade":
            trade_time = int(raw_event["trade_time_ns"])
            ts_event   = trade_time if trade_time > 0 else send_time
        else:
            ts_event   = send_time
        ts_recv = send_time

        # ── Flags ─────────────────────────────────────────────────────────────
        # F_LAST | F_BAD_TS on all events (see module docstring §6).
        flags = _BASE_FLAGS

        # ── Sequence number ───────────────────────────────────────────────────
        # Use seq_num from PacketHeader — monotonically increasing per channel.
        # Cast to uint32 (mod 2^32) to match normalized schema type.
        sequence = int(raw_event["seq_num"]) & 0xFFFFFFFF

        # ── order_id ──────────────────────────────────────────────────────────
        # For Clear (335) and COP (364): order_id = 0 (no associated order).
        # For Delete (332): order_id is the ID of the order being cancelled.
        # For Trade (350): order_id is the PASSIVE resting order ID.
        #   Per OMD-D spec §3.5: "users must reduce the resting order
        #   identified by the 'Quantity' within the Trade (350) message."
        #   order_id = 0 on combo leg trades (no resting order to decrement).
        order_id = int(raw_event["order_id"])
        _key = (orderbook_id, order_id, side)  # side included — HKEX reuses order_id across sides (combo legs)
        
        # ── Order size + price shadow tracking (HKEX-specific) ───────────────
        # _order_sizes maps order_id → (resting_size, price_fp).
        #
        # Why we track price:
        #   DeleteOrder (332) carries price=0 AND quantity=0 in the raw feed.
        #   The reconstruction engine's _book_cancel() looks up the price level
        #   via book._get_level(price, side). With price=0 it finds no level and
        #   returns silently — the phantom order stays in the book indefinitely,
        #   eventually causing crossed spreads when the other side moves past it.
        #   We resolve both fields from shadow state before building the event.
        #
        # Why we track size:
        #   Same issue — with size=0 the delta subtraction is a no-op.
        #
        # Edge case: order_id absent from shadow on Delete/Trade.
        #   Can happen if the session starts mid-carnet (truncated raw file) or
        #   if a partial day's data is replayed. We log a warning and fall back
        #   to (0, 0) — the resulting CANCEL will be a no-op rather than a crash.
        if msg_type == 330:                          # AddOrder — register new resting order
            self._order_sizes[_key] = (size, price_fp)

        elif msg_type == 331:                        # ModifyOrder (rare) — update both fields
            self._order_sizes[_key] = (size, price_fp)

        elif msg_type == 332:                        # DeleteOrder — full cancel, resolve from shadow
            cached = self._order_sizes.pop(_key, None)
            if cached is None:
                # Order not seen in this session — stale cancel or truncated feed.
                # Emit a no-op CANCEL (size=0, price=0) rather than crashing.
                size     = 0
                price_fp = 0
            else:
                size, price_fp = cached   # overwrite raw zeros with tracked values                                   

        # ── Build the base normalized event ───────────────────────────────────
        event = {
            # ── Timing ────────────────────────────────────────────────────────
            "ts_event"      : ts_event,
            "ts_recv"       : ts_recv,

            # ── Instrument identification ─────────────────────────────────────
            "venue"         : VENUE,
            "product"       : info.product,
            "contract"      : info.contract,

            # ── Event semantics ───────────────────────────────────────────────
            "action"        : action,
            "side"          : side,

            # ── Order fields ──────────────────────────────────────────────────
            "price"         : price_fp,
            "size"          : size,
            "order_id"      : order_id,

            # ── Control flags ─────────────────────────────────────────────────
            "flags"         : flags,
            "sequence"      : sequence,

            # ── Provider traceability ─────────────────────────────────────────
            "publisher_id"  : 0,            # HKEX is not a Databento publisher
            "instrument_id" : orderbook_id, # HKEX OrderbookID — stable per series
        }

        if msg_type == 350 and order_id == 0:
            """
            logging.getLogger(__name__).warning(
                "[info] Trade with OrderID = 0 skipped (contract=%s) (ts_event=%d)", info.contract, ts_event
            )
            """
            return None   # combo leg trade — no resting order, price not meaningful for outright book

        # ── Synthetic CANCEL for trades (see module docstring §10) ────────────
        # When a Trade (350) has a non-zero order_id, the passive resting order
        # must be decremented by the traded quantity. HKEX does not emit a
        # separate fill or delete message for this — the Trade IS the signal.
        # We synthesize a CANCEL event to let the reconstruction engine
        # decrement the book correctly via its existing _book_cancel() logic.
        #
        # The synthetic CANCEL uses price_fp from the TRADE event itself (which
        # equals the resting order's limit price), so no shadow lookup is needed
        # here for price. We do update the shadow state to reflect the residual
        # size in case a defensive Delete (332) arrives later.
        if msg_type == 350 and order_id > 0:
            # Trade with resting order — emit TRADE + FILL + CANCEL
            # to match Databento format (see Databento example lines 9-11)
            
            # Get passive side (resting order side)
            passive_side = _PASSIVE_SIDE_MAP.get(raw_side, Side.NONE)
            
            # 1. FILL event (passive order consumed)
            fill_event = {
                "ts_event"      : ts_event,
                "ts_recv"       : ts_recv,
                "venue"         : VENUE,
                "product"       : info.product,
                "contract"      : info.contract,
                "action"        : Action.FILL,
                "side"          : passive_side,   # passive resting order side
                "price"         : price_fp,
                "size"          : size,
                "order_id"      : order_id,       # passive resting order
                "flags"         : 0,
                "sequence"      : sequence,
                "publisher_id"  : 0,
                "instrument_id" : orderbook_id,
            }
            
            # 2. CANCEL event (decrement passive order from book)
            cancel_event = {
                "ts_event"      : ts_event,
                "ts_recv"       : ts_recv,
                "venue"         : VENUE,
                "product"       : info.product,
                "contract"      : info.contract,
                "action"        : Action.CANCEL,
                "side"          : passive_side,   # same as FILL
                "price"         : price_fp,
                "size"          : size,
                "order_id"      : order_id,       # same as FILL
                "flags"         : flags,
                "sequence"      : sequence,
                "publisher_id"  : 0,
                "instrument_id" : orderbook_id,
            }
            
            self._n_synthetic_cancels += 1
            
            # Update shadow state with residual size after partial fill
            cached = self._order_sizes.get(_key)
            if cached is not None:
                cached_size, cached_price = cached
                residual = max(0, cached_size - size)
                if residual > 0:
                    self._order_sizes[_key] = (residual, cached_price)
                else:
                    # Fully filled — remove from shadow state
                    self._order_sizes.pop(_key, None)
            
            # Return all 3 events in order: TRADE (aggressor), FILL (passive), CANCEL (passive)
            # Note: event['side'] has already been set to aggressor side via _TRADE_SIDE_MAP
            return [event, fill_event, cancel_event]

        return [event]

    # ─────────────────────────────────────────────────────────────────────────
    # Warmup boundary — not applicable for HKEX
    # ─────────────────────────────────────────────────────────────────────────

    def is_warmup_event(self, normalized_event: dict) -> bool:
        """
        Always returns False — HKEX has no F_SNAPSHOT warmup equivalent.

        The OrderbookClear (335) messages at session open are modelled as
        Action.CLEAR events (which reset the validator shadow state), not
        as warmup events. The reconstruction engine handles CLEAR correctly.
        """
        return False

    # ─────────────────────────────────────────────────────────────────────────
    # Venue info
    # ─────────────────────────────────────────────────────────────────────────

    def get_venue(self) -> str:
        return VENUE

    # ─────────────────────────────────────────────────────────────────────────
    # Stats
    # ─────────────────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        stats = super().get_stats()
        stats.update({
            "venue"                : VENUE,
            "n_instruments"        : len(self._ob_map),
            "n_unknown_symbol"     : self._n_unknown_symbol,
            "n_clears"             : self._n_clears,
            "n_cops"               : self._n_cops,
            "n_mod_orders"         : self._n_mod_orders,
            "n_synthetic_cancels"  : self._n_synthetic_cancels,
        })
        return stats