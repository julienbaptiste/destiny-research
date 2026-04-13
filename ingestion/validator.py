"""
validator.py — Provider-agnostic MBO event validator.

Operates exclusively on the normalized schema defined in schema.py.
Has zero knowledge of any specific provider (Databento, HKEX, IBKR...).

Two-pass design:
    1. Structural validation  — field-level checks (price, size, order_id...)
    2. Semantic validation    — book-state coherence checks (orphan cancel,
                                duplicate add, side consistency...)

The validator is stateful: it maintains a lightweight shadow of the active
order set (order_id → side, price) to detect semantic anomalies without
running the full LOB reconstruction. This shadow state is reset on CLEAR.

Validation modes (see schema.ValidationMode):
    STRICT  → anomalous events are dropped and written to the rejected log.
              Use for debugging and pipeline regression testing.
    LOOSE   → anomalous events are kept but flagged (reject log still written).
              Use for production and research — never silently lose data.

Warmup mode:
    During warmup (before the first clean RTH event), orphan CANCELs and
    MODIFYs for unknown order_ids are silently ignored — these refer to
    orders that were resting before the start of our data window.
    After warmup ends, orphan events are logged and handled per mode.

Output:
    validate_batch() returns two Arrow Tables:
        - clean events  (NORMALIZED_MBO_SCHEMA)
        - rejected events (REJECTED_EVENTS_SCHEMA)
    Both are written by the caller (ingestion orchestrator).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

import pyarrow as pa

from .schema import (
    Action,
    Flags,
    Side,
    ValidationMode,
    FIXED_PRICE_SCALE,
    UNDEF_PRICE,
    NORMALIZED_MBO_SCHEMA,
    REJECTED_EVENTS_SCHEMA,
)

import logging
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# REJECTION REASONS
# ---------------------------------------------------------------------------

class RejectReason:
    """
    Human-readable rejection reason codes.
    Stored in rejected_events.parquet — queryable via DuckDB.
    Keep these short and grep-friendly.
    """

    # Structural
    INVALID_PRICE_NEGATIVE      : Final[str] = "INVALID_PRICE_NEGATIVE"
    INVALID_PRICE_ZERO          : Final[str] = "INVALID_PRICE_ZERO"
    INVALID_SIZE_ZERO           : Final[str] = "INVALID_SIZE_ZERO"
    INVALID_ORDER_ID_ZERO       : Final[str] = "INVALID_ORDER_ID_ZERO"
    INVALID_ACTION              : Final[str] = "INVALID_ACTION"
    INVALID_SIDE                : Final[str] = "INVALID_SIDE"
    INVALID_SIDE_FOR_ACTION     : Final[str] = "INVALID_SIDE_FOR_ACTION"
    MISSING_PRICE_FOR_ACTION    : Final[str] = "MISSING_PRICE_FOR_ACTION"

    # Semantic — order book coherence
    ORPHAN_CANCEL               : Final[str] = "ORPHAN_CANCEL"
    ORPHAN_MODIFY               : Final[str] = "ORPHAN_MODIFY"
    DUPLICATE_ADD               : Final[str] = "DUPLICATE_ADD"
    SIDE_MISMATCH               : Final[str] = "SIDE_MISMATCH"
    PRICE_MISMATCH_ON_CANCEL    : Final[str] = "PRICE_MISMATCH_ON_CANCEL"

    # Timestamp
    TS_EVENT_ZERO               : Final[str] = "TS_EVENT_ZERO"
    TS_RECV_ZERO                : Final[str] = "TS_RECV_ZERO"

    # Deduplication
    DUPLICATE_EVENT             : Final[str] = "DUPLICATE_EVENT"


# ---------------------------------------------------------------------------
# SHADOW ORDER (lightweight state for semantic validation)
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class ShadowOrder:
    """
    Minimal order state tracked by the validator for semantic checks.
    We do NOT replicate the full LOB here — just enough to detect
    orphan cancels, duplicate adds, and side/price mismatches.
    """
    order_id : int
    side     : str    # BID or ASK
    price    : int    # fixed-point int64


# ---------------------------------------------------------------------------
# VALIDATOR STATE
# ---------------------------------------------------------------------------

@dataclass
class ValidatorState:
    """
    Per-instrument, per-session validator state.

    One ValidatorState instance per (contract, date) pair.
    Reset between sessions — do NOT reuse across days.

    warmup_mode:
        When True, orphan CANCELs and MODIFYs for unknown order_ids are
        silently ignored (pre-existing resting orders from before our
        data window). Set to False once warmup_end() is called explicitly
        by the ingestion orchestrator.
    """

    mode          : str  = ValidationMode.STRICT
    warmup_mode   : bool = True

    # Shadow active orders: order_id → ShadowOrder
    # Reset on every CLEAR event.
    _active_orders: dict[int, ShadowOrder] = field(default_factory=dict)

    # Deduplication set: (ts_event, sequence, order_id, action)
    # Bounded in practice — a single day of ES MBO is ~10M events,
    # but the set only stores the 4-tuple hash, not the full event.
    _seen_events  : set[tuple] = field(default_factory=set)

    # Stats — for end-of-day logging
    n_validated   : int = 0
    n_rejected    : int = 0
    n_warmup_skip : int = 0

    def warmup_end(self) -> None:
        """
        Signal end of warmup period.
        From this point, orphan events are logged and handled per mode.
        Also clears the dedup set — warmup events are not subject to dedup.
        """
        self.warmup_mode = False
        self._seen_events.clear()

    def reset(self) -> None:
        """Full reset — call on CLEAR event."""
        self._active_orders.clear()
        # Do NOT clear _seen_events here — dedup spans the full session.

    def register_add(self, order_id: int, side: str, price: int) -> None:
        self._active_orders[order_id] = ShadowOrder(order_id, side, price)

    def remove_order(self, order_id: int) -> None:
        self._active_orders.pop(order_id, None)

    def get_order(self, order_id: int) -> ShadowOrder | None:
        return self._active_orders.get(order_id)

    def is_duplicate(self, ts_event: int, sequence: int,
                     order_id: int, action: str,
                     price: int, size: int) -> bool:
        # Full 6-tuple key to avoid false positives on multi-leg atomic groups.
        # On CME MBO, the same order_id can appear multiple times in one F_LAST
        # group (e.g. partial fill causing multiple MODIFYs with same sequence
        # but different size). Adding price+size ensures only bit-for-bit
        # identical messages (true network duplicates) are flagged.
        key = (ts_event, sequence, order_id, action, price, size)
        if key in self._seen_events:
            return True
        self._seen_events.add(key)
        return False


# ---------------------------------------------------------------------------
# CORE VALIDATION LOGIC
# ---------------------------------------------------------------------------

def _validate_structural(event: dict) -> str | None:
    """
    Field-level structural checks.
    Returns a RejectReason string if invalid, None if clean.

    These checks are purely local — no state required.
    Order of checks matters: fail fast on the most common issues first.
    """
    action = event.get("action")
    side   = event.get("side")
    price  = event.get("price")
    size   = event.get("size")

    # --- Timestamps ---
    if event.get("ts_event", 0) == 0:
        return RejectReason.TS_EVENT_ZERO
    if event.get("ts_recv", 0) == 0:
        return RejectReason.TS_RECV_ZERO

    # --- Action ---
    valid_actions = {
        Action.ADD, Action.CANCEL, Action.MODIFY, Action.CLEAR,
        Action.TRADE, Action.FILL, Action.NONE,
    }
    if action not in valid_actions:
        return RejectReason.INVALID_ACTION

    # --- Side ---
    valid_sides = {Side.BID, Side.ASK, Side.NONE}
    if side not in valid_sides:
        return RejectReason.INVALID_SIDE

    # TRADE, FILL, NONE, CLEAR may carry side=NONE — that is valid.
    # ADD, CANCEL, MODIFY must have a real side.
    if action in Action.ORDER_ID_REQUIRED and side == Side.NONE:
        return RejectReason.INVALID_SIDE_FOR_ACTION

    # --- order_id ---
    if action in Action.ORDER_ID_REQUIRED:
        # order_id=0 is a sentinel for F_TOB synthetic messages only.
        # For real order events it is invalid.
        flags = event.get("flags", 0)
        if event.get("order_id", 0) == 0 and not (flags & Flags.F_TOB):
            return RejectReason.INVALID_ORDER_ID_ZERO

    # --- Price ---
    if action in Action.PRICE_REQUIRED:
        # UNDEF_PRICE is only valid on F_TOB messages — handled in semantic pass.
        flags = event.get("flags", 0)
        if not (flags & Flags.F_TOB):
            if price is None or price == UNDEF_PRICE:
                return RejectReason.MISSING_PRICE_FOR_ACTION
            if price <= 0:
                # For calendar spreads (_CAL_ in contract name), any price is
                # valid: positive, zero, or negative. A zero price means both
                # legs offset exactly; a negative price means the near leg
                # trades above the far leg. Both are economically legitimate.
                # For outrights, any non-positive price is a data error —
                # no outright futures instrument trades at zero or below.
                contract = event.get("contract", "")
                if "_CAL_" not in contract:
                    if price == 0:
                        return RejectReason.INVALID_PRICE_ZERO
                    return RejectReason.INVALID_PRICE_NEGATIVE

    # --- Size ---
    if action in Action.SIZE_REQUIRED:
        if size is None or size == 0:
            return RejectReason.INVALID_SIZE_ZERO

    return None  # structural pass


def _validate_semantic(event: dict, state: ValidatorState) -> str | None:
    """
    Book-state coherence checks.
    Returns a RejectReason string if invalid, None if clean.

    Mutates state ONLY when returning None (i.e. on clean events).
    Caller is responsible for not applying state changes on rejected events.
    """
    action   = event["action"]
    order_id = event.get("order_id", 0)
    side     = event["side"]
    price    = event.get("price", 0)
    flags    = event.get("flags", 0)

    # --- CLEAR: reset shadow state, nothing to validate ---
    if action == Action.CLEAR:
        state.reset()
        return None

    # --- Non-book actions: pass through immediately ---
    if action not in Action.BOOK_ACTIONS:
        return None

    # --- F_TOB synthetic messages: skip order-level checks ---
    # F_TOB messages replace an entire book side. They carry order_id=0
    # and may carry UNDEF_PRICE to signal side clearing. No shadow state update.
    if flags & Flags.F_TOB:
        return None

    # --- Deduplication ---
    sequence = event.get("sequence", 0)
    if state.is_duplicate(
        event["ts_event"], sequence, order_id, action,
        event.get("price", 0), event.get("size", 0),
    ):
        return RejectReason.DUPLICATE_EVENT

    # --- ADD ---
    if action == Action.ADD:
        existing = state.get_order(order_id)
        if existing is not None:
            # Duplicate ADD — same order_id already active.
            # In LOOSE mode this will be logged but kept (caller handles mode).
            return RejectReason.DUPLICATE_ADD
        # Clean ADD — register in shadow state
        state.register_add(order_id, side, price)
        return None

    # --- CANCEL ---
    if action == Action.CANCEL:
        existing = state.get_order(order_id)
        if existing is None:
            if state.warmup_mode:
                # Pre-existing resting order — silent skip during warmup
                state.n_warmup_skip += 1
                return None
            # Two known sources of legitimate orphan CANCELs post-warmup:
            #
            # 1. EUREX implied orders (calendar spreads): EUREX EOBI generates
            #    synthetic implied orders from outright prices. These produce
            #    CANCELs without a prior ADD in the MBO stream. Affects all
            #    _CAL_ instruments throughout the session.
            #
            # 2. GTC orders crossing session boundaries (outrights): orders
            #    placed on day N-1 and still resting on day N appear as orphan
            #    CANCELs/MODIFYs because the ADD is in the previous day's file.
            #    The F_SNAPSHOT warmup bootstrap does not cover all GTC orders.
            #    Observed on EUREX FDAX/FESX/FSMI at ~1000 events/day — stable
            #    and consistent, confirming structural GTC overnight activity.
            #
            # Both cases are accepted silently — the reconstruction engine will
            # ignore CANCELs for unknown order_ids without corrupting book state.
            state.n_warmup_skip += 1
            return None
        # Side mismatch on cancel — strong indicator of feed corruption
        if existing.side != side:
            return RejectReason.SIDE_MISMATCH
        # Always remove from shadow state on any CANCEL.
        #
        # Databento CANCEL semantics: the size field = quantity being cancelled,
        # NOT the remaining quantity. A partial cancel (e.g. cancel 3 of 5)
        # has size=3, not size=2. We do not track remaining size in the shadow
        # state — too expensive and not needed for validation purposes.
        #
        # Consequence: we optimistically remove the order on any CANCEL.
        # This means a subsequent ADD with the same order_id (which CME allows
        # after a full cancel — the exchange reuses order_ids) will be accepted
        # correctly instead of triggering a false DUPLICATE_ADD.
        #
        # The reconstruction engine tracks actual remaining size and will handle
        # partial vs full cancels correctly.
        state.remove_order(order_id)
        return None

    # --- MODIFY ---
    if action == Action.MODIFY:
        existing = state.get_order(order_id)
        if existing is None:
            if state.warmup_mode:
                # Pre-existing resting order — treat as ADD in shadow state
                state.register_add(order_id, side, price)
                state.n_warmup_skip += 1
                return None
            # Same rationale as orphan CANCELs — GTC orders from previous
            # session or EUREX implied orders. Accept silently post-warmup.
            # Register in shadow state so subsequent CANCELs on this order
            # are resolved correctly within the same session.
            state.register_add(order_id, side, price)
            state.n_warmup_skip += 1
            return None
        if existing.side != side:
            return RejectReason.SIDE_MISMATCH
        # Update shadow state with new price (price change loses queue priority)
        existing.price = price
        return None

    return None


# ---------------------------------------------------------------------------
# PUBLIC API
# ---------------------------------------------------------------------------

def validate_event(
    event : dict,
    state : ValidatorState,
) -> tuple[bool, str | None]:
    """
    Validate a single normalized MBO event dict against the canonical schema.

    Returns:
        (is_clean, reject_reason)
        is_clean=True  → event passes all checks (structural + semantic)
        is_clean=False → reject_reason contains the RejectReason code

    State mutation:
        Shadow state is updated ONLY when is_clean=True.
        In LOOSE mode the caller may choose to keep the event in the output
        despite is_clean=False — but state is NOT updated for rejected events
        to avoid cascading corruption.

    Note on LOOSE mode:
        This function returns is_clean=False for anomalous events regardless
        of mode. The caller (validate_batch) applies mode logic to decide
        whether to include or drop the event from the clean output.
    """
    state.n_validated += 1

    # Pass 1 — structural
    reason = _validate_structural(event)
    if reason is not None:
        state.n_rejected += 1
        return False, reason

    # Pass 2 — semantic (only for book-relevant actions)
    reason = _validate_semantic(event, state)
    if reason is not None:
        # Special case: warmup silent skips are not counted as rejections
        if reason not in (RejectReason.ORPHAN_CANCEL, RejectReason.ORPHAN_MODIFY):
            state.n_rejected += 1
        return False, reason

    return True, None


def validate_batch(
    events : list[dict],
    state  : ValidatorState,
    mode   : str = ValidationMode.STRICT,
) -> tuple[pa.Table, pa.Table]:
    """
    Validate a batch of normalized MBO event dicts.

    Args:
        events:  list of normalized event dicts (from adapter output)
        state:   ValidatorState for this (contract, session) — caller owns it
        mode:    ValidationMode.STRICT or ValidationMode.LOOSE

    Returns:
        (clean_table, rejected_table)
        clean_table:    pa.Table with NORMALIZED_MBO_SCHEMA
        rejected_table: pa.Table with REJECTED_EVENTS_SCHEMA

    Performance note:
        We build two flat lists of dicts and convert to Arrow at the end.
        pa.Table.from_pylist() with an explicit schema is faster than
        appending row-by-row to a RecordBatch.
        For very large batches (>5M events), consider chunking.
    """
    clean_rows    : list[dict] = []
    rejected_rows : list[dict] = []

    for event in events:
        is_clean, reason = validate_event(event, state)

        if is_clean:
            clean_rows.append(event)
        else:
            # Build rejected row — always written regardless of mode
            rejected_rows.append(_build_rejected_row(event, reason, mode))

            if mode == ValidationMode.LOOSE:
                # In LOOSE mode: keep the event in clean output but flag it.
                # Set F_BAD_TS or a custom flag? We reuse flags field bit 0x04
                # (F_BAD_TS repurposed as F_ANOMALY for non-timestamp issues).
                # The reconstruction engine must tolerate flagged events.
                flagged = dict(event)
                flagged["flags"] = event.get("flags", 0) | 0x04
                clean_rows.append(flagged)

    # Convert to Arrow tables with explicit schemas for type safety
    clean_table = pa.Table.from_pylist(
        clean_rows,
        schema=NORMALIZED_MBO_SCHEMA,
    ) if clean_rows else pa.table({}, schema=NORMALIZED_MBO_SCHEMA)

    rejected_table = pa.Table.from_pylist(
        rejected_rows,
        schema=REJECTED_EVENTS_SCHEMA,
    ) if rejected_rows else pa.table({}, schema=REJECTED_EVENTS_SCHEMA)

    return clean_table, rejected_table


def log_stats(state: ValidatorState, contract: str, date_str: str) -> None:
    """
    Print end-of-day validation summary to stdout.
    Call after validate_batch() completes for a full session.

    Fixed-width columns for readability in batch runs:
        contract   : left-aligned, 20 chars
        date       : 8 chars
        validated  : right-aligned, 12 chars
        clean      : right-aligned, 12 chars + pct
        rejected   : right-aligned,  6 chars
        warmup_skip: right-aligned,  6 chars
    """
    total     = state.n_validated
    rejected  = state.n_rejected
    warmup_sk = state.n_warmup_skip
    clean     = total - rejected
    pct_clean = 100 * clean / total if total > 0 else 0.0

    # Skip instruments with zero activity — they clutter batch output
    if total == 0:
        return

    log.info(
        "  %-22s  %s  validated=%10s  clean=%10s (%6.2f%%)  rejected=%5d  warmup_skip=%5d",
        contract, date_str, f"{total:,}", f"{clean:,}", pct_clean, rejected, warmup_sk,
    )


# ---------------------------------------------------------------------------
# INTERNAL HELPERS
# ---------------------------------------------------------------------------

def _build_rejected_row(
    event  : dict,
    reason : str,
    mode   : str,
) -> dict:
    """
    Build a row for the rejected events audit log.
    Nullable fields use None when missing from the event dict.
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