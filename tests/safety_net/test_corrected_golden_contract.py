"""Static guards for the R0.1/R0.2 corrected-golden freeze contract."""

from __future__ import annotations

from freeze_corrected_goldens import (
    MBO_CHECKSUM_COLS,
    MBP1_CHECKSUM_COLS,
    _DEFAULT_OUTPUT_ROOT,
    _GOLDEN_NAMESPACE,
    _REPO_ROOT,
    _assert_checksum_contract,
)


def test_corrected_checksum_contract_contains_canonical_ordering_and_provenance():
    _assert_checksum_contract()
    assert {"norm_flags", "sequence", "subsequence"} <= set(MBO_CHECKSUM_COLS)
    assert {"sequence", "subsequence"} <= set(MBP1_CHECKSUM_COLS)


def test_corrected_goldens_live_outside_legacy_golden_directories():
    expected = _REPO_ROOT / "tests" / "regression" / "corrected" / "r0_1_r0_2"
    assert _DEFAULT_OUTPUT_ROOT == expected
    assert _GOLDEN_NAMESPACE == "corrected/r0_1_r0_2"
    assert "/normalization/golden/" not in f"{_DEFAULT_OUTPUT_ROOT}/"
    assert "/reconstruction/golden/" not in f"{_DEFAULT_OUTPUT_ROOT}/"
