"""
schema.py — Canonical normalized MBO schema and shared constants.

This is the single source of truth for the internal data format.
All provider adapters MUST translate their raw feed into this schema.
The reconstruction engine ONLY consumes this schema — it has zero
knowledge of any specific provider (Databento, HKEX, IBKR, ...).

Price representation:
    All prices are stored as fixed-point int64, using Databento's scale
    factor: FIXED_PRICE_SCALE = 1_000_000_000 (1e9).
    i.e. a price of 4500.25 is stored as 4_500_250_000_000.
    HKEX and other providers must convert to this scale on ingestion.
    Never store prices as float — precision loss is unacceptable for
    tick-level microstructure research.

Timestamp representation:
    All timestamps are uint64 nanoseconds since Unix epoch.
    Exchange time  → ts_event
    Receiver time  → ts_recv
"""

from __future__ import annotations

from enum import IntFlag
from typing import Final

import pyarrow as pa


# ---------------------------------------------------------------------------
# PRICE CONSTANTS
# ---------------------------------------------------------------------------

# Fixed-point scale factor: 1 price unit = 1e-9 in real terms.
# All providers must normalize to this scale.
FIXED_PRICE_SCALE: Final[int] = 1_000_000_000

# Sentinel value for undefined price (used by Databento F_TOB messages
# to signal that a book side should be cleared). Equals INT64_MAX.
UNDEF_PRICE: Final[int] = 9_223_372_036_854_775_807


# ---------------------------------------------------------------------------
# FLAGS BITMASK
# ---------------------------------------------------------------------------

class Flags(IntFlag):
    """
    Bitmask flags carried on every normalized MBO event.

    These map directly to Databento's RecordFlags for Databento-sourced data.
    For other providers (HKEX, IBKR), the adapter is responsible for
    translating provider-specific flags into this canonical bitmask.

    Bits not listed here are reserved and should be set to 0 by adapters.
    """

    # Last message in the current event (atomic group).
    # A snapshot MUST only be taken when F_LAST is set —
    # otherwise the book is in a partial / inconsistent state.
    F_LAST      = 0x80

    # Top-of-book change. The message directly affects the BBO.
    # In Databento MBO feeds, F_TOB messages have special semantics:
    # they replace an entire book side rather than a single order.
    F_TOB       = 0x01

    # Message was replayed from a MBP snapshot (not a live order event).
    # Used during warmup — events with this flag should not emit LOB rows.
    F_SNAPSHOT  = 0x20

    # Message was received out of sequence (gap detected upstream).
    # Flag for audit purposes — do not drop, but track.
    F_BAD_TS    = 0x04


# ---------------------------------------------------------------------------
# ACTION ENUM
# ---------------------------------------------------------------------------

class Action:
    """
    Normalized action codes for MBO events.

    String constants (not IntEnum) to match the Parquet dictionary encoding.
    All adapters must map their provider-specific action codes to these values.

    Reconstruction engine rules:
        ADD    → insert order into book
        CANCEL → reduce or remove order from book
        MODIFY → update price and/or size of resting order
        CLEAR  → wipe the entire book (hard reset)
        TRADE  → ignored by reconstruction engine (no book state change)
        FILL   → ignored by reconstruction engine (no book state change)
        NONE   → ignored by reconstruction engine
    """
    ADD    : Final[str] = "ADD"
    CANCEL : Final[str] = "CANCEL"
    MODIFY : Final[str] = "MODIFY"
    CLEAR  : Final[str] = "CLEAR"
    TRADE  : Final[str] = "TRADE"
    FILL   : Final[str] = "FILL"
    NONE   : Final[str] = "NONE"

    # Set of actions that modify book state — used by validator
    BOOK_ACTIONS : Final[frozenset] = frozenset({ADD, CANCEL, MODIFY, CLEAR})

    # Set of actions that require a valid order_id
    ORDER_ID_REQUIRED : Final[frozenset] = frozenset({ADD, CANCEL, MODIFY})

    # Set of actions that require a valid price (not UNDEF_PRICE)
    PRICE_REQUIRED : Final[frozenset] = frozenset({ADD, MODIFY})

    # Set of actions that require a valid size > 0
    SIZE_REQUIRED : Final[frozenset] = frozenset({ADD, MODIFY})

    # Mapping from Databento raw action chars to normalized strings
    DATABENTO_MAP : Final[dict] = {
        "A": ADD,
        "C": CANCEL,
        "M": MODIFY,
        "R": CLEAR,
        "T": TRADE,
        "F": FILL,
        "N": NONE,
    }


# ---------------------------------------------------------------------------
# SIDE ENUM
# ---------------------------------------------------------------------------

class Side:
    """
    Normalized side codes for MBO events.

    String constants to match Parquet dictionary encoding.
    NONE is valid only for TRADE, FILL, and CLEAR actions.
    """
    BID  : Final[str] = "BID"
    ASK  : Final[str] = "ASK"
    NONE : Final[str] = "NONE"

    # Sides that indicate a resting order — used by validator
    ORDER_SIDES : Final[frozenset] = frozenset({BID, ASK})

    # Mapping from Databento raw side chars to normalized strings
    DATABENTO_MAP : Final[dict] = {
        "B": BID,
        "A": ASK,
        "N": NONE,
    }


# ---------------------------------------------------------------------------
# NORMALIZED MBO SCHEMA
# ---------------------------------------------------------------------------

# Dictionary encoding for low-cardinality string columns.
# Cardinality: action=7, side=3, venue~5, product~20, contract~100.
# This makes string comparisons as fast as integer comparisons in Arrow,
# with negligible storage overhead.
_DICT_INT8_STR  = pa.dictionary(pa.int8(),  pa.string())
_DICT_INT16_STR = pa.dictionary(pa.int16(), pa.string())

NORMALIZED_MBO_SCHEMA: Final[pa.Schema] = pa.schema([

    # --- Timing ---
    # ts_event: exchange-assigned timestamp, nanoseconds since Unix epoch.
    # ts_recv:  timestamp when the message was received by Databento's
    #           infrastructure (or our own gateway for HKEX).
    pa.field("ts_event",      pa.uint64(),       nullable=False),
    pa.field("ts_recv",       pa.uint64(),       nullable=False),

    # --- Instrument identification ---
    # venue and product are redundant with the file path, but included
    # for cross-product DuckDB queries without path reconstruction.
    # contract: specific expiry, e.g. "ESZ25", "FESXZ25", "HSIZ25"
    # For calendar spreads: "ES_CAL_Z25H26"
    pa.field("venue",         _DICT_INT8_STR,    nullable=False),
    pa.field("product",       _DICT_INT8_STR,    nullable=False),
    pa.field("contract",      _DICT_INT16_STR,   nullable=False),

    # --- Event semantics ---
    # action: ADD / CANCEL / MODIFY / CLEAR / TRADE / FILL / NONE
    # side:   BID / ASK / NONE
    pa.field("action",        _DICT_INT8_STR,    nullable=False),
    pa.field("side",          _DICT_INT8_STR,    nullable=False),

    # --- Order fields ---
    # price:    fixed-point int64 (scale = FIXED_PRICE_SCALE = 1e9).
    #           UNDEF_PRICE (INT64_MAX) is a valid sentinel for F_TOB clears.
    # size:     remaining size after the event (for CANCEL: delta cancelled).
    # order_id: exchange-assigned order identifier.
    #           For F_TOB synthetic messages: order_id = 0.
    pa.field("price",         pa.int64(),        nullable=False),
    pa.field("size",          pa.uint32(),       nullable=False),
    pa.field("order_id",      pa.uint64(),       nullable=False),

    # --- Control flags ---
    # Bitmask using the Flags IntFlag above.
    # F_LAST is the critical one: snapshot only when this bit is set.
    pa.field("flags",         pa.uint8(),        nullable=False),

    # sequence: exchange sequence number, used for:
    #   1. deduplication (ts_event, sequence, order_id, action)
    #   2. gap detection (non-contiguous sequences = missed messages)
    #   3. tie-breaking when ts_event values are equal
    pa.field("sequence",      pa.uint32(),       nullable=False),

    # --- Provider traceability ---
    # Retained for audit and debugging. Not used by reconstruction engine.
    # publisher_id: Databento publisher ID (0 for non-Databento sources)
    # instrument_id: provider-native instrument identifier
    #   (Databento instrument_id, HKEX stock code, etc.)
    pa.field("publisher_id",  pa.uint16(),       nullable=False),
    pa.field("instrument_id", pa.uint32(),       nullable=False),
])


# ---------------------------------------------------------------------------
# REJECTED EVENTS SCHEMA
# ---------------------------------------------------------------------------

# Schema for the per-day audit log of dropped / flagged events.
# Written by the validator to:
#   data/normalized/provider=X/venue=Y/product=Z/contract=W/year=Y/month=M/
#       <CONTRACT>_<DATE>_rejected.parquet
#
# Never delete these files — they are the audit trail for data quality.
REJECTED_EVENTS_SCHEMA: Final[pa.Schema] = pa.schema([
    pa.field("ts_event",      pa.uint64(),       nullable=False),
    pa.field("ts_recv",       pa.uint64(),       nullable=False),
    pa.field("venue",         pa.string(),       nullable=False),
    pa.field("product",       pa.string(),       nullable=False),
    pa.field("contract",      pa.string(),       nullable=False),
    pa.field("action",        pa.string(),       nullable=True),
    pa.field("side",          pa.string(),       nullable=True),
    pa.field("price",         pa.int64(),        nullable=True),
    pa.field("size",          pa.uint32(),       nullable=True),
    pa.field("order_id",      pa.uint64(),       nullable=True),
    pa.field("flags",         pa.uint8(),        nullable=True),
    pa.field("sequence",      pa.uint32(),       nullable=True),
    pa.field("publisher_id",  pa.uint16(),       nullable=True),
    pa.field("instrument_id", pa.uint32(),       nullable=True),
    # Human-readable rejection reason — queryable via DuckDB
    pa.field("reject_reason", pa.string(),       nullable=False),
    # Validation mode active when this event was rejected
    pa.field("mode",          pa.string(),       nullable=False),
])


# ---------------------------------------------------------------------------
# VALIDATION MODES
# ---------------------------------------------------------------------------

class ValidationMode:
    """
    STRICT: drop all anomalies — use for debugging, regression testing,
            and initial pipeline validation.
    LOOSE:  flag anomalies but keep them in the normalized output with
            a dedicated flag bit — use for production and research.
            Anomalous events are still written to the rejected log.
    """
    STRICT : Final[str] = "STRICT"
    LOOSE  : Final[str] = "LOOSE"


# ---------------------------------------------------------------------------
# PATH HELPERS
# ---------------------------------------------------------------------------

from pathlib import Path


def normalized_path(
    base_dir:  Path,
    provider:  str,
    venue:     str,
    product:   str,
    contract:  str,
    year:      int,
    month:     int,
    date_str:  str,   # "YYYYMMDD"
    suffix:    str = "mbo",
) -> Path:
    """
    Build the canonical path for a normalized Parquet file.

    Example:
        normalized_path(..., provider="databento", venue="CME",
                         product="ES", contract="ESZ25",
                         year=2025, month=10, date_str="20251027")
        →  base_dir/provider=databento/venue=CME/product=ES/
               contract=ESZ25/year=2025/month=10/ESZ25_20251027_mbo.parquet
    """
    return (
        base_dir
        / f"provider={provider}"
        / f"venue={venue}"
        / f"product={product}"
        / f"contract={contract}"
        / f"year={year}"
        / f"month={month:02d}"
        / f"{contract}_{date_str}_{suffix}.parquet"
    )


def reconstructed_path(
    base_dir:  Path,
    provider:  str,
    venue:     str,
    product:   str,
    contract:  str,
    year:      int,
    month:     int,
    date_str:  str,
    schema:    str,   # "mbp1", "mbp10", "bbo"
) -> Path:
    """
    Build the canonical path for a reconstructed LOB Parquet file.
    Provider is intentionally absent — reconstructed data is provider-agnostic.

    Example:
        reconstructed_path(..., venue="CME", product="ES",
                            contract="ESZ25", year=2025, month=10,
                            date_str="20251027", schema="mbp1")
        →  base_dir/provider=databento/venue=CME/product=ES/
               contract=ESZ25/year=2025/month=10/ESZ25_20251027_mbp1.parquet
    """
    return (
        base_dir
        / f"provider={provider}"
        / f"venue={venue}"
        / f"product={product}"
        / f"contract={contract}"
        / f"year={year}"
        / f"month={month:02d}"
        / f"{contract}_{date_str}_{schema}.parquet"
    )


def rejected_path(
    base_dir:  Path,
    provider:  str,
    venue:     str,
    product:   str,
    contract:  str,
    year:      int,
    month:     int,
    date_str:  str,
) -> Path:
    """
    Build the canonical path for the per-day rejected events audit log.
    Co-located with the normalized file for easy cross-reference.
    """
    return (
        base_dir
        / f"provider={provider}"
        / f"venue={venue}"
        / f"product={product}"
        / f"contract={contract}"
        / f"year={year}"
        / f"month={month:02d}"
        / f"{contract}_{date_str}_rejected.parquet"
    )