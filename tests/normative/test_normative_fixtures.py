"""Execute human-readable R0 normative fixtures against production code paths."""

from __future__ import annotations

import sys
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
_CASES = [
    case
    for fixture_file in _FIXTURE_FILES
    for case in load_fixture_cases(fixture_file)
    if case["baseline_compatible"]
]


@pytest.mark.parametrize("case", _CASES, ids=lambda case: case["id"])
def test_normative_fixture(case, tmp_path, monkeypatch):
    """Assert adapter, validation, final LOB and MBP-1 expectations exactly."""
    actual = execute_fixture(case, tmp_path, monkeypatch)
    expected = expected_fixture_result(case)
    assert actual == expected
