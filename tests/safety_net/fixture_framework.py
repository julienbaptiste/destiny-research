"""Human-readable synthetic fixture runner for the R0 safety net.

The framework deliberately keeps provider input fixtures as JSON and exercises
real production adapter, validator and reconstruction code. Databento records
are represented by a lightweight test record class instead of opaque DBN bytes;
the adapter's translation logic itself is not replaced.

Fixtures marked ``baseline_compatible=false`` describe future normative targets
and are schema-checked but are not asserted against the pre-R0 implementation.
This keeps future contract expectations separate from legacy characterization.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

import ingestion.adapters.databento_adapter as databento_module
from ingestion.adapters.base import ContractInfo, SessionConfig
from ingestion.adapters.databento_adapter import DatabentoAdapter
from ingestion.adapters.hkex_adapter import HKEXAdapter
from ingestion.schema import NORMALIZED_MBO_SCHEMA, ValidationMode
from ingestion.validator import ValidatorState, validate_batch
from reconstruction.build_mbp1 import Market, _apply_book, reconstruct_day


_REQUIRED_TOP_LEVEL = {
    "fixture_version",
    "id",
    "status",
    "baseline_compatible",
    "provider",
    "description",
    "session",
    "source_events",
    "expected",
}


class FixtureMBOMsg:
    """Attribute-based stand-in for Databento MBOMsg used only by tests."""

    def __init__(self, **values: object) -> None:
        self.__dict__.update(values)


def load_fixture_cases(path: Path) -> list[dict[str, Any]]:
    """Load one fixture object or a JSON list of fixture objects."""
    payload = json.loads(path.read_text())
    cases = payload if isinstance(payload, list) else [payload]
    for case in cases:
        validate_fixture_shape(case)
    return cases


def validate_fixture_shape(case: dict[str, Any]) -> None:
    """Fail fast when a fixture is missing structural metadata."""
    missing = sorted(_REQUIRED_TOP_LEVEL - set(case))
    if missing:
        raise ValueError(f"Fixture {case.get('id', '<unknown>')} missing fields: {missing}")

    if case["fixture_version"] != 1:
        raise ValueError(f"Unsupported fixture version: {case['fixture_version']}")
    if case["status"] != "NORMATIVE":
        raise ValueError(f"Normative fixture has invalid status: {case['status']}")
    if case["provider"] not in {"databento", "hkex"}:
        raise ValueError(f"Unsupported fixture provider: {case['provider']}")

    session = case["session"]
    required_session = {"date", "venue", "product", "contract", "instrument_id"}
    missing_session = sorted(required_session - set(session))
    if missing_session:
        raise ValueError(f"Fixture {case['id']} missing session fields: {missing_session}")

    expected = case["expected"]
    if case["baseline_compatible"]:
        required_expected = {
            "adapter_events",
            "validator_stats",
            "rejected_reasons",
            "final_orders",
            "mbp1_rows",
            "reconstruction_stats",
        }
        missing_expected = sorted(required_expected - set(expected))
        if missing_expected:
            raise ValueError(
                f"Baseline-compatible fixture {case['id']} missing expectations: "
                f"{missing_expected}"
            )


def _session_config(case: dict[str, Any]) -> SessionConfig:
    session = case["session"]
    return SessionConfig(
        session_date=date.fromisoformat(session["date"]),
        warmup_enabled=bool(session.get("warmup_enabled", False)),
        validation_mode=session.get("validation_mode", ValidationMode.STRICT),
    )


def _contract_info(case: dict[str, Any]) -> ContractInfo:
    session = case["session"]
    return ContractInfo(
        product=session["product"],
        contract=session["contract"],
        venue=session["venue"],
        instrument_id=int(session["instrument_id"]),
        is_spread=bool(session.get("is_spread", False)),
        tick_size=int(session.get("tick_size", 0)),
        currency=session.get("currency", ""),
    )


def _expand_normalized_row(case: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    """Fill static session fields omitted from human-readable expectations."""
    session = case["session"]
    expanded = dict(row)
    expanded.setdefault("venue", session["venue"])
    expanded.setdefault("product", session["product"])
    expanded.setdefault("contract", session["contract"])
    expanded.setdefault("publisher_id", int(session.get("publisher_id", 0)))
    expanded.setdefault("instrument_id", int(session["instrument_id"]))
    return expanded


def _translate_databento(case: dict[str, Any], monkeypatch: Any) -> list[dict[str, Any]]:
    """Translate fixture records through the production Databento adapter."""
    session = case["session"]
    adapter = DatabentoAdapter()
    adapter._config = _session_config(case)
    adapter._venue = session["venue"]
    info = _contract_info(case)
    adapter._contract_cache[int(session["instrument_id"])] = info

    # DatabentoAdapter has an isinstance guard against db.MBOMsg. Replacing the
    # class symbol only inside the test process lets JSON records exercise the
    # real translation path without manufacturing proprietary DBN binaries.
    monkeypatch.setattr(databento_module.db, "MBOMsg", FixtureMBOMsg)

    translated: list[dict[str, Any]] = []
    for source in case["source_events"]:
        values = dict(source)
        values.setdefault("instrument_id", int(session["instrument_id"]))
        values.setdefault("publisher_id", int(session.get("publisher_id", 0)))
        event = adapter.translate(FixtureMBOMsg(**values))
        if event is not None:
            translated.append(event)
    return translated


def _translate_hkex(case: dict[str, Any]) -> list[dict[str, Any]]:
    """Translate fixture dictionaries through the production HKEX adapter."""
    session = case["session"]
    adapter = HKEXAdapter()
    adapter._config = _session_config(case)
    info = _contract_info(case)
    instrument_id = int(session["instrument_id"])
    adapter._ob_map[instrument_id] = info

    translated: list[dict[str, Any]] = []
    for source in case["source_events"]:
        values = {
            "send_time_ns": int(source.get("send_time_ns", source.get("ts_event", 1))),
            "seq_num": int(source.get("seq_num", source.get("sequence", 0))),
            "msg_index": int(source.get("msg_index", 0)),
            "msg_type": int(source["msg_type"]),
            "orderbook_id": int(source.get("orderbook_id", instrument_id)),
            "symbol": source.get("symbol", session["contract"]),
            "class_code": source.get("class_code", session["product"]),
            "order_id": int(source.get("order_id", 0)),
            "price": int(source.get("price", 0)),
            "quantity": int(source.get("quantity", 0)),
            "side": int(source.get("side", -1)),
            "trade_time_ns": int(source.get("trade_time_ns", 0)),
            "trade_id": int(source.get("trade_id", 0)),
            "deal_type": int(source.get("deal_type", 0)),
            "combo_group_id": int(source.get("combo_group_id", 0)),
            "source": source.get(
                "source", "trade" if int(source["msg_type"]) == 350 else "order"
            ),
        }
        result = adapter.translate(values)
        if result:
            translated.extend(result)
    return translated


def translate_fixture(case: dict[str, Any], monkeypatch: Any) -> list[dict[str, Any]]:
    """Translate all provider records for one fixture."""
    if case["provider"] == "databento":
        return _translate_databento(case, monkeypatch)
    return _translate_hkex(case)


def _validate_events(
    case: dict[str, Any],
    adapter_events: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], ValidatorState]:
    """Validate events in feed order while supporting explicit warmup boundaries."""
    session = case["session"]
    mode = session.get("validation_mode", ValidationMode.STRICT)
    state = ValidatorState(
        mode=mode,
        warmup_mode=bool(session.get("validator_warmup_mode", False)),
    )
    warmup_end_before = session.get("warmup_end_before_event")

    clean_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    for index, event in enumerate(adapter_events):
        if warmup_end_before is not None and index == int(warmup_end_before):
            state.warmup_end()
        clean, rejected = validate_batch([event], state, mode=mode)
        clean_rows.extend(clean.to_pylist())
        rejected_rows.extend(rejected.to_pylist())

    return clean_rows, rejected_rows, state


def _final_order_state(clean_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Apply validated events to the production book state and expose active orders."""
    market = Market()
    for event in clean_rows:
        _apply_book(
            market,
            event["action"],
            event["side"],
            int(event["price"]),
            int(event["size"]),
            int(event["order_id"]),
            int(event["flags"]),
            int(event["instrument_id"]),
            int(event["publisher_id"]),
        )

    orders: list[dict[str, Any]] = []
    for instrument_id, books_by_publisher in market._books.items():
        for publisher_id, book in books_by_publisher.items():
            for order in book.orders_by_id.values():
                orders.append(
                    {
                        "instrument_id": instrument_id,
                        "publisher_id": publisher_id,
                        "order_id": order.order_id,
                        "side": order.side,
                        "price": order.price,
                        "size": order.size,
                    }
                )
    return sorted(
        orders,
        key=lambda row: (
            row["instrument_id"],
            row["publisher_id"],
            row["side"],
            row["price"],
            row["order_id"],
        ),
    )


def _reconstruct(
    case: dict[str, Any],
    clean_rows: list[dict[str, Any]],
    tmp_path: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run the production reconstruction engine on validated synthetic events."""
    mbo_path = tmp_path / f"{case['id']}_mbo.parquet"
    mbp1_path = tmp_path / f"{case['id']}_mbp1.parquet"
    table = pa.Table.from_pylist(clean_rows, schema=NORMALIZED_MBO_SCHEMA)
    pq.write_table(table, mbo_path, compression="zstd")
    stats = reconstruct_day(
        mbo_file=mbo_path,
        out_file=mbp1_path,
        product=case["session"]["product"],
        contract=case["session"]["contract"],
    )
    rows = pq.read_table(mbp1_path).to_pylist()
    return rows, stats


def execute_fixture(
    case: dict[str, Any],
    tmp_path: Path,
    monkeypatch: Any,
) -> dict[str, Any]:
    """Execute one baseline-compatible normative fixture end to end."""
    validate_fixture_shape(case)
    if not case["baseline_compatible"]:
        raise ValueError(f"Fixture {case['id']} is a future target, not baseline-compatible")

    adapter_events = translate_fixture(case, monkeypatch)
    clean_rows, rejected_rows, state = _validate_events(case, adapter_events)
    mbp1_rows, reconstruction_stats = _reconstruct(case, clean_rows, tmp_path)

    return {
        "adapter_events": adapter_events,
        "clean_events": clean_rows,
        "rejected_reasons": [row["reject_reason"] for row in rejected_rows],
        "validator_stats": {
            "n_validated": state.n_validated,
            "n_rejected": state.n_rejected,
            "n_warmup_skip": state.n_warmup_skip,
        },
        "final_orders": _final_order_state(clean_rows),
        "mbp1_rows": mbp1_rows,
        "reconstruction_stats": {
            key: reconstruction_stats[key]
            for key in (
                "n_events",
                "n_rows_emitted",
                "n_orphan_cancel",
                "n_orphan_modify",
            )
        },
    }


def expected_fixture_result(case: dict[str, Any]) -> dict[str, Any]:
    """Expand compact JSON expectations into the exact comparison payload."""
    expected = case["expected"]
    adapter_events = [
        _expand_normalized_row(case, row) for row in expected["adapter_events"]
    ]
    if expected.get("clean_equals_adapter", False):
        clean_events = adapter_events
    else:
        clean_events = [
            _expand_normalized_row(case, row) for row in expected.get("clean_events", [])
        ]

    return {
        "adapter_events": adapter_events,
        "clean_events": clean_events,
        "rejected_reasons": expected["rejected_reasons"],
        "validator_stats": expected["validator_stats"],
        "final_orders": expected["final_orders"],
        "mbp1_rows": expected["mbp1_rows"],
        "reconstruction_stats": expected["reconstruction_stats"],
    }
