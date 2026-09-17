"""Pre-R0 reconstruction characterization for HKEX atomic trade bundles.

These tests intentionally freeze a known-suspicious legacy behavior: the HKEX
adapter currently places F_LAST on both TRADE and synthetic CANCEL, so the
reconstruction engine emits one snapshot before the resting-order decrement and
another after it. This is a regression anchor only, not a normative contract.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from ingestion.adapters.base import ContractInfo  # noqa: E402
from ingestion.adapters.hkex_adapter import HKEXAdapter  # noqa: E402
from ingestion.schema import NORMALIZED_MBO_SCHEMA  # noqa: E402
from reconstruction.build_mbp1 import reconstruct_day  # noqa: E402


_FIXTURE = (
    _REPO_ROOT
    / "tests"
    / "fixtures"
    / "legacy_pre_r0"
    / "hkex_trade_reconstruction.json"
)


def test_pre_r0_hkex_trade_emits_intermediate_pre_decrement_snapshot(tmp_path) -> None:
    """Freeze the current double-F_LAST reconstruction effect explicitly."""
    payload = json.loads(_FIXTURE.read_text())
    contract = payload["contract"]
    expected = payload["expected_pre_r0"]

    adapter = HKEXAdapter()
    adapter._ob_map[contract["orderbook_id"]] = ContractInfo(
        product=contract["product"],
        contract=contract["contract"],
        venue="HKEX",
        instrument_id=contract["orderbook_id"],
        is_spread=False,
        tick_size=contract["tick_size_fp"],
        currency=contract["currency"],
    )

    normalized = []
    for raw_event in payload["raw_events"]:
        result = adapter.translate(raw_event)
        assert result is not None
        normalized.extend(result)

    assert [row["action"] for row in normalized] == expected["normalized_actions"]
    assert [row["flags"] for row in normalized] == expected["normalized_flags"]

    mbo_path = tmp_path / "legacy_hkex_mbo.parquet"
    mbp1_path = tmp_path / "legacy_hkex_mbp1.parquet"
    pq.write_table(
        pa.Table.from_pylist(normalized, schema=NORMALIZED_MBO_SCHEMA),
        mbo_path,
        compression="zstd",
    )

    stats = reconstruct_day(
        mbo_file=mbo_path,
        out_file=mbp1_path,
        product=contract["product"],
        contract=contract["contract"],
    )
    rows = pq.read_table(mbp1_path).to_pylist()

    assert [row["action"] for row in rows] == expected["mbp1_actions"]
    assert [row["bid_sz_00"] for row in rows] == expected["mbp1_bid_sizes"]
    assert [row["bid_px_00"] for row in rows] == expected["mbp1_bid_prices"]
    assert stats["n_rows_emitted"] == expected["n_rows_emitted"]
    assert stats["n_orphan_cancel"] == expected["n_orphan_cancel"]
    assert stats["n_orphan_modify"] == expected["n_orphan_modify"]

    # The middle row is the legacy inconsistency we intend to make visible:
    # TRADE already ends an atomic group, but the resting BID is still size 5.
    assert rows[1]["action"] == "TRADE"
    assert rows[1]["bid_sz_00"] == 5
    assert rows[2]["action"] == "CANCEL"
    assert rows[2]["bid_sz_00"] == 3
