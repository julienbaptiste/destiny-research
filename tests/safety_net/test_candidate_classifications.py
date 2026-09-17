"""Static contract tests for reviewed R0.1/R0.2 migration classifications."""

from __future__ import annotations

import json
from pathlib import Path
import sys

_THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_THIS_DIR))

from classify_candidate_evidence import _validate_registry  # noqa: E402


_REGISTRY = _THIS_DIR / "classifications" / "r0_1_r0_2.json"
_REQUIRED_PRODUCTS = {"ES", "NIY", "FDAX", "FESX", "HSI", "MHI"}


def test_reviewed_classification_registry_is_complete_and_well_formed():
    registry = json.loads(_REGISTRY.read_text())

    _validate_registry(registry)

    classifications = registry["classifications"]
    assert set(classifications) == _REQUIRED_PRODUCTS
    assert registry["reviewed_against_candidate_commit"] == (
        "c731de7f41769a7234778e911a101a4afa4b07c5"
    )


def test_databento_classifications_bind_to_adr_003_only():
    registry = json.loads(_REGISTRY.read_text())

    for product in ("ES", "NIY", "FDAX", "FESX"):
        for kind in ("mbo", "mbp1"):
            classification = registry["classifications"][product][kind]
            assert classification["classification"] == "EXPECTED_CHANGE"
            assert classification["decision_ref"] == "ADR-003"


def test_hkex_classifications_bind_to_adr_003_and_adr_004():
    registry = json.loads(_REGISTRY.read_text())

    for product in ("HSI", "MHI"):
        for kind in ("mbo", "mbp1"):
            classification = registry["classifications"][product][kind]
            assert classification["classification"] == "EXPECTED_CHANGE"
            assert classification["decision_ref"] == "ADR-003/ADR-004"
