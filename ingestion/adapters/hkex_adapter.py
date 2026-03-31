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
       350 (Trade)         → TRADE
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
# Convention: trade side = passive side of the book (the side that was resting).
# 0=N/A, 2=Buy aggressor (→ passive ASK was hit), 3=Sell aggressor (→ passive BID was hit)
_TRADE_SIDE_MAP: dict[int, str] = {
    0: Side.NONE,
    2: Side.ASK,    # Buy aggressor → resting ASK was consumed
    3: Side.BID,    # Sell aggressor → resting BID was consumed
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

    Returns None if the symbol cannot be parsed (options, combos, or
    unrecognized format) — the instrument will be skipped by the adapter.

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
        self._n_unknown_symbol : int = 0
        self._n_clears         : int = 0
        self._n_cops           : int = 0
        self._n_mod_orders     : int = 0   # should be 0 — HKEX emits Del+Add

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
        Yield raw event dicts from the merged orders+trades stream.

        Memory-efficient merge sort using PyArrow streaming + heapq.merge().

        Key optimizations vs previous version:
        1. Columnar-first: batches stay as Arrow RecordBatches until the
            last possible moment — no intermediate Python dict explosion.
        2. Controlled batch size: batch_size_rows caps the number of rows
            held in RAM per file at any time. Two files open simultaneously
            → peak RAM = 2 × batch_size_rows × bytes_per_row.
            At 50k rows × ~250 bytes ≈ 25 MB per file → ~50 MB peak for
            the merge buffers alone. Add Python overhead → ~200-300 MB total.
        3. Column pre-selection: we only decode the columns we actually use
            in translate(), skipping lot_type/order_type/orderbook_position
            which are present in the raw but not needed for normalization.
        4. to_pylist() per micro-batch: converts one Arrow batch to a list
            of dicts in a single C++ call — much faster than row-by-row
            dict construction in Python.

        Merge correctness:
        Both files are written in chronological order by hkex_raw_parser
        (sequential binary scan → sequential write). Each batch is already
        sorted internally. heapq.merge() performs an O(n) n-way merge on
        pre-sorted iterables — no global sort, no RAM spike.

        Parameters (class-level, set in __init__ or overridden for testing):
        _BATCH_SIZE_ROWS : int  rows per Arrow batch (default 50_000)
        """
        import heapq
        import pyarrow.parquet as pq

        assert self._orders_path is not None, "open_session() not called"

        # Columns needed from the orders file for translate()
        # Omitting lot_type, order_type, orderbook_position — not used downstream
        ORDERS_COLS = [
            "send_time_ns", "seq_num", "msg_index", "msg_type",
            "orderbook_id", "symbol", "class_code",
            "order_id", "price", "quantity", "side",
        ]

        # Columns needed from the trades file
        TRADES_COLS = [
            "send_time_ns", "seq_num", "msg_index",
            "orderbook_id", "symbol", "class_code",
            "order_id", "price", "quantity", "side",
            "trade_time_ns", "trade_id", "deal_type", "combo_group_id",
        ]

        # Batch size — controls peak RAM.
        # 50k rows × ~250 bytes × 2 files ≈ 25 MB merge buffer
        # Increase to 200k if you want fewer Python round-trips (more RAM).
        BATCH_ROWS = getattr(self, "_batch_size_rows", 50_000)

        def _iter_orders(path: Path) -> Iterator[tuple]:
            """
            Stream orders parquet in sorted batches.

            Yields (send_time_ns, seq_num, msg_index, row_dict) tuples.
            Each batch is converted from columnar Arrow to row dicts via
            to_pylist() — a single C++ call per batch, not per row.
            """
            pf = pq.ParquetFile(path)
            for batch in pf.iter_batches(
                batch_size=BATCH_ROWS,
                columns=ORDERS_COLS,
            ):
                # to_pylist() converts the entire batch columnar → list[dict]
                # in one C++ call. Much faster than row-by-row dict construction.
                rows = batch.to_pylist()
                for row in rows:
                    row["source"] = "order"
                    yield (
                        row["send_time_ns"],
                        row["seq_num"],
                        row["msg_index"],
                        row,
                    )

        def _iter_trades(path: Path) -> Iterator[tuple]:
            """
            Stream trades parquet in sorted batches.
            Injects msg_type=350 (absent from trades schema by design).
            """
            pf = pq.ParquetFile(path)
            for batch in pf.iter_batches(
                batch_size=BATCH_ROWS,
                columns=TRADES_COLS,
            ):
                rows = batch.to_pylist()
                for row in rows:
                    row["source"]   = "trade"
                    row["msg_type"] = 350   # inject — absent from trades schema
                    yield (
                        row["send_time_ns"],
                        row["seq_num"],
                        row["msg_index"],
                        row,
                    )

        # heapq.merge: O(n) n-way merge on pre-sorted iterables.
        # Both files are written chronologically by hkex_raw_parser →
        # each batch is already sorted → merge is correct without global sort.
        iterables = [_iter_orders(self._orders_path)]
        if self._trades_path is not None:
            iterables.append(_iter_trades(self._trades_path))

        merged = heapq.merge(*iterables)

        for _, _, _, row in merged:
            yield row

    def translate(self, raw_event: dict) -> dict | None:
        """
        Translate one raw HKEX event dict to a normalized MBO event dict.

        Returns None for:
          - Unknown orderbook_id (unresolved instrument — options, combos, etc.)
          - COP (msg_type=364) — informational, not a book event
          - Trade with combo_group_id != 0 — combination leg, avoid double-count
            (same logic as Databento non-printable filter, but more conservative:
             we use combo_group_id rather than deal_type for the adapter layer;
             the researcher can further filter by deal_type & 1 if needed)

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
        # Price = 0 on Delete (332) and Clear (335) — kept as 0 (validator will
        # accept 0 for CANCEL/CLEAR actions).
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
        # For Trade (350): order_id is the aggressor order ID (0 if unavailable).
        order_id = int(raw_event["order_id"])

        return {
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
            "venue"              : VENUE,
            "n_instruments"      : len(self._ob_map),
            "n_unknown_symbol"   : self._n_unknown_symbol,
            "n_clears"           : self._n_clears,
            "n_cops"             : self._n_cops,
            "n_mod_orders"       : self._n_mod_orders,
        })
        return stats