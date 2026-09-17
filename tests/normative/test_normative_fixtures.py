"""Execute and audit all canonical R0 normative fixtures."""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pytest

_SAFETY_NET_DIR = Path(__file__).resolve().parents[1] / "safety_net"
sys.path.insert(0, str(_SAFETY_NET_DIR))

from fixture_framework import (  # noqa: E402
    execute_fixture,
    expected_fixture_result,
    load_fixture_cases,
)


_FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "normative"
_FIXTURE_FILES = sorted(_FIXTURE_ROOT.glob("*.json"))
_ALL_CASES = [
    case
    for fixture_file in _FIXTURE_FILES
    for case in load_fixture_cases(fixture_file)
]

_REQUIRED_EXPECTED = {
    "adapter_events",
    "validator_stats",
    "rejected_reasons",
    "final_orders",
    "mbp1_rows",
    "reconstruction_stats",
}
_EXPECTED_MATRIX_IDS = {
    *(f"DB-{index:02d}" for index in range(1, 16)),
    *(f"HK-{index:02d}" for index in range(1, 19)),
}


def _assert_subset(actual: Any, expected: Any, path: str = "root") -> None:
    """Assert that expected recursively constrains actual without extra assumptions."""
    if isinstance(expected, dict):
        assert isinstance(actual, dict), f"{path}: expected dict, got {type(actual).__name__}"
        for key, value in expected.items():
            assert key in actual, f"{path}: missing key {key!r}"
            _assert_subset(actual[key], value, f"{path}.{key}")
        return

    if isinstance(expected, list):
        assert isinstance(actual, list), f"{path}: expected list, got {type(actual).__name__}"
        assert len(actual) == len(expected), (
            f"{path}: length mismatch actual={len(actual)} expected={len(expected)}"
        )
        for index, (actual_item, expected_item) in enumerate(zip(actual, expected)):
            _assert_subset(actual_item, expected_item, f"{path}[{index}]")
        return

    assert actual == expected, f"{path}: actual={actual!r} expected={expected!r}"


def _matches_subset(actual: Any, expected: Any) -> bool:
    """Boolean counterpart used for unordered/ordered-subsequence row matching."""
    try:
        _assert_subset(actual, expected)
        return True
    except AssertionError:
        return False


def _assert_unordered_rows(
    actual: list[dict[str, Any]],
    expected: list[dict[str, Any]],
    path: str,
) -> None:
    """Match expected final-state rows one-to-one without imposing storage order."""
    assert len(actual) == len(expected), (
        f"{path}: length mismatch actual={len(actual)} expected={len(expected)}"
    )
    remaining = list(range(len(actual)))
    for expected_index, expected_row in enumerate(expected):
        for actual_index in remaining:
            if _matches_subset(actual[actual_index], expected_row):
                remaining.remove(actual_index)
                break
        else:
            raise AssertionError(
                f"{path}[{expected_index}]: no matching actual row for {expected_row!r}; "
                f"actual={actual!r}"
            )


def _assert_ordered_subsequence(
    actual: list[dict[str, Any]],
    expected: list[dict[str, Any]],
    path: str,
) -> None:
    """Require expected diagnostic rows to appear in order inside the full stream."""
    cursor = 0
    for expected_index, expected_row in enumerate(expected):
        while cursor < len(actual) and not _matches_subset(actual[cursor], expected_row):
            cursor += 1
        if cursor >= len(actual):
            raise AssertionError(
                f"{path}[{expected_index}]: expected row not found after cursor; "
                f"expected={expected_row!r} actual={actual!r}"
            )
        cursor += 1


def _assert_future_target(
    case: dict[str, Any],
    actual: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    """Validate compact future targets without weakening semantic boundaries."""
    # Adapter output defines the canonical event contract, therefore count and
    # order are exact even when each expected row omits irrelevant boilerplate.
    _assert_subset(actual["adapter_events"], expected["adapter_events"], "adapter_events")

    expected_source = case["expected"]
    if expected_source.get("clean_equals_adapter", False) or "clean_events" in expected_source:
        _assert_subset(actual["clean_events"], expected["clean_events"], "clean_events")

    _assert_subset(
        actual["rejected_reasons"],
        expected["rejected_reasons"],
        "rejected_reasons",
    )
    _assert_subset(actual["validator_stats"], expected["validator_stats"], "validator_stats")

    # A final LOB is a set of resting orders; list serialization order is not
    # part of the contract.
    _assert_unordered_rows(actual["final_orders"], expected["final_orders"], "final_orders")

    # Compact targets often list only the snapshots relevant to the scenario.
    # n_rows_emitted below still checks the total stream cardinality exactly.
    _assert_ordered_subsequence(actual["mbp1_rows"], expected["mbp1_rows"], "mbp1_rows")
    _assert_subset(
        actual["reconstruction_stats"],
        expected["reconstruction_stats"],
        "reconstruction_stats",
    )


def test_normative_matrix_is_complete_and_unique() -> None:
    ids = [case["id"] for case in _ALL_CASES]
    duplicates = sorted(case_id for case_id, count in Counter(ids).items() if count > 1)
    assert duplicates == []
    assert set(ids) == _EXPECTED_MATRIX_IDS


def test_future_targets_are_governed_and_complete() -> None:
    for case in _ALL_CASES:
        missing = sorted(_REQUIRED_EXPECTED - set(case["expected"]))
        assert missing == [], f"{case['id']} missing expected sections: {missing}"
        if not case["baseline_compatible"]:
            decision_ref = case.get("decision_ref", "")
            assert decision_ref.startswith("ADR-"), (
                f"{case['id']} future target requires an ADR decision_ref"
            )


@pytest.mark.parametrize("case", _ALL_CASES, ids=lambda case: case["id"])
def test_normative_fixture(case, tmp_path, monkeypatch):
    """Execute every canonical DB/HK scenario against the migration candidate."""
    actual = execute_fixture(case, tmp_path, monkeypatch)
    expected = expected_fixture_result(case)

    if case["baseline_compatible"]:
        # R0.0 fixtures remain full exact assertions after schema expansion.
        assert actual == expected
    else:
        _assert_future_target(case, actual, expected)
