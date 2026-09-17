"""
validator.py — provider-neutral normalized MBO validator.

Validation is deliberately separate from provider adaptation. The validator
checks structural invariants and maintains a lightweight order shadow for
semantic checks without rebuilding the full LOB.

STRICT:
    anomalous events are rejected and persisted to the audit log.
LOOSE:
    anomalous events are persisted to the audit log and retained with
    NormFlags.N_VALIDATION_ANOMALY set. Provider/control flags are never
    overloaded with Destiny validation state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Final

import pyarrow as pa

from .schema import (
    Action,
    Flags,
    NormFlags,
    Side,
    ValidationMode,
    UNDEF_PRICE,
    NORMALIZED_MBO_SCHEMA,
    REJECTED_EVENTS_SCHEMA,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# REJECTION REASONS
# ---------------------------------------------------------------------------

class RejectReason:
    """Stable audit codes stored in rejected-event Parquet files."""

    INVALID_PRICE_NEGATIVE   : Final[str] = "INVALID_PRICE_NEGATIVE"
    INVALID_PRICE_ZERO       : Final[str] = "INVALID_PRICE_ZERO"
    INVALID_SIZE_ZERO        : Final[str] = "INVALID_SIZE_ZERO"
    INVALID_ORDER_ID_ZERO    : Final[str] = "INVALID_ORDER_ID_ZERO"
    INVALID_ACTION           : Final[str] = "INVALID_ACTION"
    INVALID_SIDE             : Final[str] = "INVALID_SIDE"
    INVALID_SIDE_FOR_ACTION  : Final[str] = "INVALID_SIDE_FOR_ACTION"
    MISSING_PRICE_FOR_ACTION : Final[str] = "MISSING_PRICE_FOR_ACTION"

    ORPHAN_CANCEL            : Final[str] = "ORPHAN_CANCEL"
    ORPHAN_MODIFY            : Final[str] = "ORPHAN_MODIFY"
    DUPLICATE_ADD            : Final[str] = "DUPLICATE_ADD"
    SIDE_MISMATCH            : Final[str] = "SIDE_MISMATCH"
    PRICE_MISMATCH_ON_CANCEL : Final[str] = "PRICE_MISMATCH_ON_CANCEL"

    TS_EVENT_ZERO            : Final[str] = "TS_EVENT_ZERO"
    TS_RECV_ZERO             : Final[str] = "TS_RECV_ZERO"
    DUPLICATE_EVENT          : Final[str] = "DUPLICATE_EVENT"


# ---------------------------------------------------------------------------
# LIGHTWEIGHT SHADOW STATE
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class ShadowOrder:
    """Minimal resting state required for validation."""

    order_id : int
    side     : str
    price    : int
    size     : int


@dataclass
class ValidatorState:
    """Per-contract, per-session validator state."""

    mode        : str  = ValidationMode.STRICT
    warmup_mode : bool = True

    _active_orders: dict[int, ShadowOrder] = field(default_factory=dict)
    _seen_events: set[tuple] = field(default_factory=set)

    n_validated   : int = 0
    n_rejected    : int = 0
    n_warmup_skip : int = 0

    def warmup_end(self) -> None:
        self.warmup_mode = False
        self._seen_events.clear()

    def reset(self) -> None:
        self._active_orders.clear()

    def register_add(
        self,
        order_id: int,
        side: str,
        price: int,
        size: int,
    ) -> None:
        self._active_orders[order_id] = ShadowOrder(order_id, side, price, size)

    def remove_order(self, order_id: int) -> None:
        self._active_orders.pop(order_id, None)

    def get_order(self, order_id: int) -> ShadowOrder | None:
        return self._active_orders.get(order_id)

    def is_duplicate(
        self,
        ts_event: int,
        sequence: int,
        order_id: int,
        action: str,
        price: int,
        size: int,
    ) -> bool:
        """Detect bit-for-bit semantic duplicates without conflating groups."""
        key = (ts_event, sequence, order_id, action, price, size)
        if key in self._seen_events:
            return True
        self._seen_events.add(key)
        return False


# ---------------------------------------------------------------------------
# STRUCTURAL VALIDATION
# ---------------------------------------------------------------------------

def _validate_structural(event: dict) -> str | None:
    action = event.get("action")
    side = event.get("side")
    price = event.get("price")
    size = event.get("size")

    if event.get("ts_event", 0) == 0:
        return RejectReason.TS_EVENT_ZERO
    if event.get("ts_recv", 0) == 0:
        return RejectReason.TS_RECV_ZERO

    valid_actions = {
        Action.ADD,
        Action.CANCEL,
        Action.MODIFY,
        Action.CLEAR,
        Action.TRADE,
        Action.FILL,
        Action.NONE,
    }
    if action not in valid_actions:
        return RejectReason.INVALID_ACTION

    if side not in {Side.BID, Side.ASK, Side.NONE}:
        return RejectReason.INVALID_SIDE

    if action in Action.ORDER_ID_REQUIRED and side == Side.NONE:
        return RejectReason.INVALID_SIDE_FOR_ACTION

    flags = int(event.get("flags", 0))
    if action in Action.ORDER_ID_REQUIRED:
        if event.get("order_id", 0) == 0 and not (flags & int(Flags.F_TOB)):
            return RejectReason.INVALID_ORDER_ID_ZERO

    if action in Action.PRICE_REQUIRED and not (flags & int(Flags.F_TOB)):
        if price is None or price == UNDEF_PRICE:
            return RejectReason.MISSING_PRICE_FOR_ACTION
        if price <= 0 and "_CAL_" not in event.get("contract", ""):
            if price == 0:
                return RejectReason.INVALID_PRICE_ZERO
            return RejectReason.INVALID_PRICE_NEGATIVE

    if action in Action.SIZE_REQUIRED:
        # F_TOB + UNDEF_PRICE is the canonical one-side clear sentinel and may
        # legitimately carry size=0.
        is_tob_clear = bool(flags & int(Flags.F_TOB)) and price == UNDEF_PRICE
        if not is_tob_clear and (size is None or size == 0):
            return RejectReason.INVALID_SIZE_ZERO

    return None


# ---------------------------------------------------------------------------
# SEMANTIC VALIDATION
# ---------------------------------------------------------------------------

def _explicit_adapter_anomaly(event: dict) -> bool:
    return bool(int(event.get("norm_flags", 0)) & int(NormFlags.N_VALIDATION_ANOMALY))


def _validate_semantic(event: dict, state: ValidatorState) -> str | None:
    action = event["action"]
    order_id = int(event.get("order_id", 0))
    side = event["side"]
    price = int(event.get("price", 0))
    size = int(event.get("size", 0))
    flags = int(event.get("flags", 0))

    if action == Action.CLEAR:
        state.reset()
        return None

    if action not in Action.BOOK_ACTIONS:
        return None

    if flags & int(Flags.F_TOB):
        return None

    if state.is_duplicate(
        int(event["ts_event"]),
        int(event.get("sequence", 0)),
        order_id,
        action,
        price,
        size,
    ):
        return RejectReason.DUPLICATE_EVENT

    if action == Action.ADD:
        if state.get_order(order_id) is not None:
            return RejectReason.DUPLICATE_ADD
        state.register_add(order_id, side, price, size)
        return None

    if action == Action.CANCEL:
        existing = state.get_order(order_id)
        if existing is None:
            if state.warmup_mode:
                state.n_warmup_skip += 1
                return None

            # General cross-session/implied-order orphans remain tolerated until
            # R0.3. Provider adapters can explicitly mark an event anomalous when
            # they know a native state reference should have been resolvable.
            if _explicit_adapter_anomaly(event):
                return RejectReason.ORPHAN_CANCEL
            state.n_warmup_skip += 1
            return None

        if existing.side != side:
            return RejectReason.SIDE_MISMATCH

        if price not in (0, existing.price):
            return RejectReason.PRICE_MISMATCH_ON_CANCEL

        # CANCEL.size is quantity removed. Keep residual state for partial
        # cancels instead of deleting the order optimistically.
        if size >= existing.size:
            state.remove_order(order_id)
        else:
            existing.size -= size
        return None

    if action == Action.MODIFY:
        existing = state.get_order(order_id)
        if existing is None:
            if state.warmup_mode:
                state.register_add(order_id, side, price, size)
                state.n_warmup_skip += 1
                return None
            if _explicit_adapter_anomaly(event):
                return RejectReason.ORPHAN_MODIFY
            state.register_add(order_id, side, price, size)
            state.n_warmup_skip += 1
            return None

        if existing.side != side:
            return RejectReason.SIDE_MISMATCH

        # MODIFY.size is the new absolute resting quantity.
        existing.price = price
        existing.size = size
        return None

    return None


# ---------------------------------------------------------------------------
# PUBLIC API
# ---------------------------------------------------------------------------

def validate_event(event: dict, state: ValidatorState) -> tuple[bool, str | None]:
    """Validate one canonical MBO event and update shadow state when clean."""
    state.n_validated += 1

    reason = _validate_structural(event)
    if reason is None:
        reason = _validate_semantic(event, state)

    if reason is not None:
        state.n_rejected += 1
        return False, reason

    return True, None


def mark_validation_anomaly(event: dict) -> dict:
    """Return a copy carrying Destiny validation provenance only."""
    flagged = dict(event)
    flagged["norm_flags"] = int(event.get("norm_flags", 0)) | int(
        NormFlags.N_VALIDATION_ANOMALY
    )
    return flagged


def validate_batch(
    events: list[dict],
    state: ValidatorState,
    mode: str = ValidationMode.STRICT,
) -> tuple[pa.Table, pa.Table]:
    """Validate a small/in-memory batch using the same event path as ingestion."""
    clean_rows: list[dict] = []
    rejected_rows: list[dict] = []

    for event in events:
        is_clean, reason = validate_event(event, state)
        if is_clean:
            clean_rows.append(event)
            continue

        rejected_rows.append(_build_rejected_row(event, reason or "UNKNOWN", mode))
        if mode == ValidationMode.LOOSE:
            clean_rows.append(mark_validation_anomaly(event))

    # from_pylist([], schema=...) is reliable across supported PyArrow versions;
    # pa.table({}, schema=...) is not.
    clean_table = pa.Table.from_pylist(clean_rows, schema=NORMALIZED_MBO_SCHEMA)
    rejected_table = pa.Table.from_pylist(rejected_rows, schema=REJECTED_EVENTS_SCHEMA)
    return clean_table, rejected_table


def log_stats(state: ValidatorState, contract: str, date_str: str) -> None:
    total = state.n_validated
    if total == 0:
        return

    rejected = state.n_rejected
    clean = total - rejected
    pct_clean = 100.0 * clean / total
    log.info(
        "  %-22s  %s  validated=%10s  clean=%10s (%6.2f%%)  "
        "rejected=%5d  warmup_skip=%5d",
        contract,
        date_str,
        f"{total:,}",
        f"{clean:,}",
        pct_clean,
        rejected,
        state.n_warmup_skip,
    )


def _build_rejected_row(event: dict, reason: str, mode: str) -> dict:
    """Build one durable rejected-event audit row."""
    return {
        "ts_event": event.get("ts_event", 0),
        "ts_recv": event.get("ts_recv", 0),
        "venue": event.get("venue", ""),
        "product": event.get("product", ""),
        "contract": event.get("contract", ""),
        "action": event.get("action"),
        "side": event.get("side"),
        "price": event.get("price"),
        "size": event.get("size"),
        "order_id": event.get("order_id"),
        "flags": event.get("flags"),
        "norm_flags": event.get("norm_flags"),
        "sequence": event.get("sequence"),
        "subsequence": event.get("subsequence"),
        "publisher_id": event.get("publisher_id"),
        "instrument_id": event.get("instrument_id"),
        "reject_reason": reason,
        "mode": mode,
    }
