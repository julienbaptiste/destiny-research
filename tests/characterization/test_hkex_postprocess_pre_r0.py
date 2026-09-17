"""Pre-R0 characterization for HKEX Delete+Add -> MODIFY post-processing.

These tests intentionally freeze the legacy adjacency heuristic. They do not
endorse it as canonical semantics. R0.2 targets preserve native CANCEL/ADD
unless a deterministic provider-backed relationship is available.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from ingestion.post_process_hkex_ingestion import process_file  # noqa: E402
from ingestion.schema import NORMALIZED_MBO_SCHEMA  # noqa: E402


def _row(
    *,
    action: str,
    order_id: int,
    sequence: int,
    price: int,
    size: int,
    side: str = "BID",
) -> dict:
    """Build one minimal normalized HKEX row for legacy post-process tests."""
    return {
        "ts_event": 1_760_000_000_000_000_000 + sequence,
        "ts_recv": 1_760_000_000_000_000_100 + sequence,
        "venue": "HKEX",
        "product": "HSI",
        "contract": "HSIZ26",
        "action": action,
        "side": side,
        "price": price,
        "size": size,
        "order_id": order_id,
        "flags": 0x84,
        "sequence": sequence,
        "publisher_id": 0,
        "instrument_id": 1234,
    }


def _run_postprocess(tmp_path: Path, rows: list[dict]) -> tuple[dict, list[dict]]:
    """Write a tiny normalized file, run the real post-processor, and reload it."""
    mbo_path = tmp_path / "input_mbo.parquet"
    processed_path = tmp_path / "processed_mbo.parquet"
    pq.write_table(
        pa.Table.from_pylist(rows, schema=NORMALIZED_MBO_SCHEMA),
        mbo_path,
        compression="zstd",
    )

    stats = process_file(mbo_path, processed_path, overwrite=True)
    output = pq.read_table(mbo_path).to_pylist()
    return stats, output


def test_pre_r0_adjacent_cancel_add_is_rewritten_as_modify(tmp_path) -> None:
    """Freeze the legacy global-adjacency rewrite for a same-order pair."""
    # Use an order ID above 2^53 to also preserve the current uint64-safe
    # lookahead behavior in the pandas shift operation.
    order_id = 9_007_199_254_740_993
    rows = [
        _row(
            action="CANCEL",
            order_id=order_id,
            sequence=10,
            price=27_000_000_000_000,
            size=5,
        ),
        _row(
            action="ADD",
            order_id=order_id,
            sequence=11,
            price=27_010_000_000_000,
            size=7,
        ),
    ]

    stats, output = _run_postprocess(tmp_path, rows)

    assert stats == {
        "n_original": 2,
        "n_processed": 1,
        "n_modify": 1,
        "n_dropped_cancel": 1,
    }
    assert len(output) == 1
    assert output[0]["action"] == "MODIFY"
    assert output[0]["order_id"] == order_id
    assert output[0]["price"] == 27_010_000_000_000
    assert output[0]["size"] == 7
    assert output[0]["sequence"] == 11


def test_pre_r0_interleaved_cancel_add_is_not_rewritten(tmp_path) -> None:
    """Freeze the legacy failure to associate a non-adjacent same-order pair."""
    target_order_id = 42
    rows = [
        _row(
            action="CANCEL",
            order_id=target_order_id,
            sequence=20,
            price=27_000_000_000_000,
            size=5,
        ),
        _row(
            action="ADD",
            order_id=99,
            sequence=21,
            price=26_990_000_000_000,
            size=3,
        ),
        _row(
            action="ADD",
            order_id=target_order_id,
            sequence=22,
            price=27_010_000_000_000,
            size=7,
        ),
    ]

    stats, output = _run_postprocess(tmp_path, rows)

    assert stats == {
        "n_original": 3,
        "n_processed": 3,
        "n_modify": 0,
        "n_dropped_cancel": 0,
    }
    assert [row["action"] for row in output] == ["CANCEL", "ADD", "ADD"]
    assert [row["order_id"] for row in output] == [target_order_id, 99, target_order_id]
    assert all(row["action"] != "MODIFY" for row in output)
