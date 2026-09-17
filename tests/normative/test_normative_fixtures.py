"""Execute and audit human-readable R0 normative fixtures."""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

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
_CASES = [case for case in _ALL_CASES if case["baseline_compatible"]]

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


def test_normative_matrix_is_complete_and_unique() -> None:
    """Require every approved DB/HK matrix case exactly once."""
    ids = [case["id"] for case in _ALL_CASES]
    duplicates = sorted(case_id for case_id, count in Counter(ids).items() if count > 1)

    assert duplicates == []
    assert set(ids) == _EXPECTED_MATRIX_IDS


def test_future_targets_are_governed_and_complete() -> None:
    """Future semantic targets must be explicit contracts, never placeholders."""
    for case in _ALL_CASES:
        missing = sorted(_REQUIRED_EXPECTED - set(case["expected"]))
        assert missing == [], f"{case['id']} missing expected sections: {missing}"

        if not case["baseline_compatible"]:
            decision_ref = case.get("decision_ref", "")
            assert decision_ref.startswith("ADR-"), (
                f"{case['id']} future target requires an ADR decision_ref"
            )


@pytest.mark.parametrize("case", _CASES, ids=lambda case: case["id"])
def test_normative_fixture(case, tmp_path, monkeypatch):
    """Assert baseline-compatible adapter/validator/LOB/MBP-1 behavior exactly."""
    actual = execute_fixture(case, tmp_path, monkeypatch)
    expected = expected_fixture_result(case)
    assert actual == expected
