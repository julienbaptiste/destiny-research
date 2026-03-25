"""
tests/validation/validate_mbp1_databento.py — MBP-1 reconstruction validator
against Databento native reference feed.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONTEXT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

This validator is intentionally Databento-specific. The reconstruction engine
(build_mbp1.py) is provider-agnostic, but validating it requires a certified
ground-truth reference. Databento's native MBP-1 feed is that reference for
CME and EUREX products.

Implication: once the engine is validated RELIABLE on Databento products, the
same engine can be trusted for HKEX reconstruction — the state machine logic
is identical. The only remaining risk for HKEX is in the hkex_adapter.py
mapping, not in the reconstruction engine itself.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
METHODOLOGY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Step 1 — Stream the Databento .mbp-1.dbn.zst reference file to a temporary
    Parquet file in /tmp. Uses store.__iter__() + InstrumentMap.resolve()
    (same pattern as databento_adapter.py) to stream records in batches
    without loading the full file into RAM.

Step 2 — DuckDB unique-key join on (ts_recv, sequence).
    ts_recv is the index of Databento MBP-1 records (uint64 ns since epoch).
    Within the same (ts_recv, sequence), multiple events can coexist in burst
    groups. We restrict to pairs that appear exactly once in both datasets to
    get a clean 1-to-1 alignment. Burst events (~12% of records on ES) are
    excluded from match rates but counted for diagnostics.

Step 3 — Compare 6 TOB columns: bid_px_00, ask_px_00, bid_sz_00, ask_sz_00,
    bid_ct_00, ask_ct_00. Prices are float64 in both datasets (real price,
    already divided by 1e9) — exact equality is valid.

Step 4 — Delete the temporary Parquet file. Use --keep-ref to suppress
    deletion (useful for manual inspection or debugging mismatches).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REFERENCE FILE FORMAT (Databento MBP-1 native, confirmed 2026-03)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    Index: ts_recv    uint64 ns since epoch  ← join key (stored as column)
    ts_event          uint64 ns since epoch
    rtype             uint8
    publisher_id      uint16
    instrument_id     uint32
    action            str
    side              str
    depth             uint8
    price             float64               ← real price (already / 1e9)
    size              uint32
    flags             uint8
    ts_in_delta       int32
    sequence          uint32
    bid_px_00         float64
    ask_px_00         float64
    bid_sz_00         uint32
    ask_sz_00         uint32
    bid_ct_00         uint32
    ask_ct_00         uint32
    symbol            str                   ← short Databento notation, e.g. "ESZ5"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VERDICT THRESHOLDS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    RELIABLE   : px >= 99.9%  AND sz >= 98.0%  → batch reconstruction is safe
    ACCEPTABLE : px >= 99.5%  AND sz >= 95.0%  → investigate but not blocking
    NEEDS_WORK : below ACCEPTABLE              → do not batch, fix engine first

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
USAGE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Single date
    python tests/validation/validate_mbp1_databento.py \\
        --product ES --contract ESZ25 --date 20251001

    # Multiple dates (3 ES reference dates)
    python tests/validation/validate_mbp1_databento.py \\
        --product ES --contract ESZ25 --date 20251001 20251010 20251027

    # Keep temporary Parquet for manual inspection
    python tests/validation/validate_mbp1_databento.py \\
        --product ES --contract ESZ25 --date 20251001 --keep-ref

    # Suppress mismatch row display
    python tests/validation/validate_mbp1_databento.py \\
        --product ES --contract ESZ25 --date 20251001 --max-mismatches 0

Exit codes:
    0 — all dates RELIABLE or ACCEPTABLE
    1 — one or more dates NEEDS_WORK
    2 — setup error (missing .dbn.zst, unknown product, etc.)
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from datetime import date
from pathlib import Path

import databento as db
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

# ---------------------------------------------------------------------------
# Repo root + project imports
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from config import DATA_RAW, DATA_RECONSTRUCTED     # noqa: E402
from ingestion.market_config import MARKET_CONFIG   # noqa: E402
from ingestion.schema import reconstructed_path     # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ANSI colors — consistent with check_regression.py
# ---------------------------------------------------------------------------

BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"

# ---------------------------------------------------------------------------
# Verdict thresholds
# ---------------------------------------------------------------------------

# (min_px_pct, min_sz_pct) per verdict level
_THRESHOLDS = {
    "RELIABLE"   : (99.9, 98.0),
    "ACCEPTABLE" : (99.5, 95.0),
}

# ---------------------------------------------------------------------------
# Streaming batch size — controls peak RAM during reference conversion.
# 100K records ≈ 15MB Arrow in memory (MBP-1 records are wider than MBO).
# Consistent with build_mbp1.py READ_BATCH_SIZE.
# ---------------------------------------------------------------------------

_STREAM_BATCH_SIZE = 100_000

# Output row group size for the temporary Parquet — tuned for DuckDB scan.
_ROW_GROUP_SIZE = 500_000

# ---------------------------------------------------------------------------
# Reference Parquet schema — mirrors Databento MBP-1 native types exactly.
# ts_recv is stored as uint64 (nanoseconds since epoch) — same as our output.
# ts_event is also stored as uint64 for consistency.
# ---------------------------------------------------------------------------

_REF_SCHEMA = pa.schema([
    pa.field("ts_recv",       pa.uint64(),  nullable=False),  # join key
    pa.field("ts_event",      pa.uint64(),  nullable=False),
    pa.field("publisher_id",  pa.uint16(),  nullable=False),
    pa.field("instrument_id", pa.uint32(),  nullable=False),
    pa.field("action",        pa.string(),  nullable=False),
    pa.field("side",          pa.string(),  nullable=False),
    pa.field("price",         pa.float64(), nullable=False),
    pa.field("flags",         pa.uint8(),   nullable=False),
    pa.field("sequence",      pa.uint32(),  nullable=False),
    pa.field("bid_px_00",     pa.float64(), nullable=True),
    pa.field("ask_px_00",     pa.float64(), nullable=True),
    pa.field("bid_sz_00",     pa.uint32(),  nullable=True),
    pa.field("ask_sz_00",     pa.uint32(),  nullable=True),
    pa.field("bid_ct_00",     pa.uint32(),  nullable=True),
    pa.field("ask_ct_00",     pa.uint32(),  nullable=True),
    pa.field("symbol",        pa.string(),  nullable=True),
])

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

_DATASET_PREFIX = {
    "CME"  : "glbx-mdp3",
    "EUREX": "xeur-eobi",
}


def _raw_mbp1_path(product: str, date_str: str) -> Path:
    """
    Return the path of the Databento .mbp-1.dbn.zst reference file.

    Stored alongside MBO raw files in DATA_RAW.
    Convention: {dataset_prefix}-{YYYYMMDD}.mbp-1.dbn.zst

    Example:
        data/raw/provider=databento/venue=CME/product=ES/year=2025/month=10/
            glbx-mdp3-20251001.mbp-1.dbn.zst
    """
    cfg      = MARKET_CONFIG[product]
    venue    = cfg["exchange"]
    provider = cfg["provider"]
    prefix   = _DATASET_PREFIX.get(venue, venue.lower())
    year     = date_str[:4]
    month    = date_str[4:6]
    return (
        DATA_RAW
        / f"provider={provider}"
        / f"venue={venue}"
        / f"product={product}"
        / f"year={year}"
        / f"month={month}"
        / f"{prefix}-{date_str}.mbp-1.dbn.zst"
    )


def _our_mbp1_path(product: str, contract: str, date_str: str) -> Path:
    """
    Return the path of our reconstructed MBP-1 Parquet file.

    Mirrors build_mbp1.py _out_path convention exactly.

    Example:
        data/reconstructed/venue=CME/product=ES/contract=ESZ25/
            year=2025/month=10/ESZ25_20251001_mbp1.parquet
    """
    cfg   = MARKET_CONFIG[product]
    venue = cfg["exchange"]
    year  = int(date_str[:4])
    month = int(date_str[4:6])
    return reconstructed_path(
        base_dir = DATA_RECONSTRUCTED,
        venue    = venue,
        product  = product,
        contract = contract,
        year     = year,
        month    = month,
        date_str = date_str,
        schema   = "mbp1",
    )


# ---------------------------------------------------------------------------
# Step 1 — Stream .mbp-1.dbn.zst → temporary Parquet
# ---------------------------------------------------------------------------

def _stream_ref_to_parquet(
    dbn_path     : Path,
    out_path     : Path,
    contract     : str,
    session_date : date,
) -> tuple[int, str | None]:
    """
    Stream a Databento .mbp-1.dbn.zst file to a temporary Parquet file,
    filtering to a single contract.

    Uses store.__iter__() to iterate MBP1Msg records one by one, accumulated
    into batches of _STREAM_BATCH_SIZE before flushing to Parquet. Peak RAM
    is proportional to batch size only (~15MB), not the full file size.

    Symbol resolution follows the same pattern as databento_adapter.py:
    InstrumentMap.resolve(instrument_id, session_date) returns the short
    Databento symbol (e.g. "ESZ5"). We match against our contract name by
    stripping the 2-digit year to a 1-digit year for comparison
    (e.g. "ESZ25" → "ESZ5").

    Args:
        dbn_path:     path to the .mbp-1.dbn.zst source file
        out_path:     path for the output Parquet file (in /tmp)
        contract:     our normalized contract name, e.g. "ESZ25"
        session_date: trading date for InstrumentMap resolution

    Returns:
        (n_rows_written, ref_symbol) where ref_symbol is the Databento short
        symbol matched (e.g. "ESZ5"), or None if no matching instrument found.
    """
    store        = db.DBNStore.from_file(dbn_path)
    imap         = db.common.symbology.InstrumentMap()
    imap.insert_metadata(store.metadata)

    # Build the set of short-form candidate symbols for our contract.
    # ESZ25 → {"ESZ5", "ESZ25"}  (try both — be defensive about SDK versions)
    candidates: set[str] = set()
    if len(contract) >= 3:
        year_2d = contract[-2:]    # "25"
        month_c = contract[-3]     # "Z"
        prefix  = contract[:-3]    # "ES"
        candidates.add(f"{prefix}{month_c}{year_2d[-1]}")  # "ESZ5"
        candidates.add(contract)                            # "ESZ25"

    # Cache: instrument_id → bool (True = matches our contract, False = skip)
    # Populated lazily during iteration — avoids repeated InstrumentMap lookups.
    _iid_match   : dict[int, bool]       = {}
    _iid_symbol  : dict[int, str | None] = {}

    ref_symbol   : str | None = None   # first matched Databento symbol
    n_rows       = 0
    batch        : list[dict] = []

    out_path.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(out_path, _REF_SCHEMA, compression="zstd")

    def _flush(batch: list[dict]) -> None:
        """Write accumulated batch to Parquet and clear the list."""
        if not batch:
            return
        rb = pa.RecordBatch.from_pydict(
            {col: [r[col] for r in batch] for col in _REF_SCHEMA.names},
            schema=_REF_SCHEMA,
        )
        writer.write_batch(rb)

    try:
        for record in store:
            # Guard: MBP-1 files may contain instrument definition records
            # at stream start — skip anything that isn't an MBP1Msg.
            if not isinstance(record, db.MBP1Msg):
                continue

            iid = record.instrument_id

            # Lazy instrument resolution and filtering
            if iid not in _iid_match:
                sym = imap.resolve(iid, session_date)
                _iid_symbol[iid] = sym
                _iid_match[iid]  = (sym in candidates) if sym else False
                if _iid_match[iid] and ref_symbol is None:
                    ref_symbol = sym
                    log.info(
                        "  Matched instrument_id=%d → symbol='%s'",
                        iid, sym,
                    )

            if not _iid_match[iid]:
                continue

            # Accumulate record into batch.
            # ts_recv is the DataFrame index in to_df() — it is accessible
            # as record.ts_recv directly on MBP1Msg (uint64 ns since epoch).
            # bid/ask level 0 are accessed via record.levels[0].
            lvl = record.levels[0]
            batch.append({
                "ts_recv"       : int(record.ts_recv),
                "ts_event"      : int(record.ts_event),
                "publisher_id"  : int(record.publisher_id),
                "instrument_id" : iid,
                "action"        : str(record.action),
                "side"          : str(record.side),
                "price"         : float(record.price) / 1e9,
                "flags"         : int(record.flags),
                "sequence"      : int(record.sequence),
                "bid_px_00"     : float(lvl.bid_px) / 1e9,
                "ask_px_00"     : float(lvl.ask_px) / 1e9,
                "bid_sz_00"     : int(lvl.bid_sz),
                "ask_sz_00"     : int(lvl.ask_sz),
                "bid_ct_00"     : int(lvl.bid_ct),
                "ask_ct_00"     : int(lvl.ask_ct),
                "symbol"        : _iid_symbol[iid],
            })
            n_rows += 1

            # Flush when batch is full — bounds peak RAM to ~15MB per batch
            if len(batch) >= _STREAM_BATCH_SIZE:
                _flush(batch)
                batch.clear()

        # Flush remaining records
        _flush(batch)

    finally:
        writer.close()

    return n_rows, ref_symbol


# ---------------------------------------------------------------------------
# Step 2 — DuckDB validation
# ---------------------------------------------------------------------------

def _run_validation(
    our_str    : str,
    ref_str    : str,
    ref_symbol : str,
    n_ref_rows : int,
) -> dict:
    """
    Run the DuckDB unique-key join validation between our MBP-1 and the
    Databento reference.

    Join key: (ts_recv, sequence) — unique pairs only.
    Compares 6 TOB columns: bid_px_00, ask_px_00, bid_sz_00, ask_sz_00,
    bid_ct_00, ask_ct_00.

    All computation stays inside DuckDB — no Arrow tables materialized
    in Python beyond the scalar stats tuple returned by fetchone().

    Args:
        our_str:    str path to our reconstructed MBP-1 Parquet
        ref_str:    str path to the temporary reference Parquet
        ref_symbol: Databento short symbol, e.g. "ESZ5" (for logging only)
        n_ref_rows: row count of the reference file (pre-computed)

    Returns:
        dict with all match counts, rates, size direction diagnostics,
        and the computed verdict string.
    """
    con = duckdb.connect()

    our_total = con.execute(f"SELECT COUNT(*) FROM '{our_str}'").fetchone()[0]

    # Count burst events excluded from the unique-key join.
    # Bursts are (ts_recv, sequence) pairs with more than one row in either
    # dataset. They cannot be aligned 1-to-1 without sub-sequence ordering
    # that differs between MBO→MBP-1 reconstruction and native MBP-1.
    our_burst = con.execute(f"""
        SELECT COUNT(*) FROM '{our_str}'
        WHERE (ts_recv, sequence) IN (
            SELECT ts_recv, sequence FROM '{our_str}'
            GROUP BY ts_recv, sequence
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    ref_burst = con.execute(f"""
        SELECT COUNT(*) FROM '{ref_str}'
        WHERE (ts_recv, sequence) IN (
            SELECT ts_recv, sequence FROM '{ref_str}'
            GROUP BY ts_recv, sequence
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    # ── Main match stats ──────────────────────────────────────────────────
    #
    # Both datasets use uint64 ts_recv and uint32 sequence — no cast needed.
    # Prices are float64 real values in both — exact equality is valid.
    # bid/ask_ct are cast to INT to avoid UINT32 overflow in ABS(delta).
    #
    stats = con.execute(f"""
        WITH our_unique AS (
            SELECT
                ts_recv,
                sequence,
                bid_px_00,
                ask_px_00,
                bid_sz_00,
                ask_sz_00,
                CAST(bid_ct_00 AS INT) AS bid_ct_00,
                CAST(ask_ct_00 AS INT) AS ask_ct_00
            FROM '{our_str}'
            QUALIFY COUNT(*) OVER (PARTITION BY ts_recv, sequence) = 1
        ),
        ref_unique AS (
            SELECT
                ts_recv,
                sequence,
                bid_px_00,
                ask_px_00,
                bid_sz_00,
                ask_sz_00,
                CAST(bid_ct_00 AS INT) AS bid_ct_00,
                CAST(ask_ct_00 AS INT) AS ask_ct_00
            FROM '{ref_str}'
            QUALIFY COUNT(*) OVER (PARTITION BY ts_recv, sequence) = 1
        ),
        joined AS (
            SELECT
                o.bid_px_00  AS our_bid_px,   r.bid_px_00  AS ref_bid_px,
                o.ask_px_00  AS our_ask_px,   r.ask_px_00  AS ref_ask_px,
                o.bid_sz_00  AS our_bid_sz,   r.bid_sz_00  AS ref_bid_sz,
                o.ask_sz_00  AS our_ask_sz,   r.ask_sz_00  AS ref_ask_sz,
                o.bid_ct_00  AS our_bid_ct,   r.bid_ct_00  AS ref_bid_ct,
                o.ask_ct_00  AS our_ask_ct,   r.ask_ct_00  AS ref_ask_ct
            FROM our_unique o
            INNER JOIN ref_unique r USING (ts_recv, sequence)
        )
        SELECT
            COUNT(*)                                                                  AS n_joined,
            -- Per-column match counts
            SUM(CASE WHEN our_bid_px = ref_bid_px THEN 1 ELSE 0 END)                AS bid_px,
            SUM(CASE WHEN our_ask_px = ref_ask_px THEN 1 ELSE 0 END)                AS ask_px,
            SUM(CASE WHEN our_bid_sz = ref_bid_sz THEN 1 ELSE 0 END)                AS bid_sz,
            SUM(CASE WHEN our_ask_sz = ref_ask_sz THEN 1 ELSE 0 END)                AS ask_sz,
            SUM(CASE WHEN our_bid_ct = ref_bid_ct THEN 1 ELSE 0 END)                AS bid_ct,
            SUM(CASE WHEN our_ask_ct = ref_ask_ct THEN 1 ELSE 0 END)                AS ask_ct,
            -- Full match: all 6 TOB fields correct simultaneously
            SUM(CASE WHEN
                    our_bid_px = ref_bid_px AND our_ask_px = ref_ask_px AND
                    our_bid_sz = ref_bid_sz AND our_ask_sz = ref_ask_sz AND
                    our_bid_ct = ref_bid_ct AND our_ask_ct = ref_ask_ct
                THEN 1 ELSE 0 END)                                                   AS full_match,
            -- Size direction (price-correct subset) — diagnose systematic bias
            SUM(CASE WHEN our_bid_px = ref_bid_px AND our_bid_sz > ref_bid_sz
                THEN 1 ELSE 0 END)                                                   AS bid_sz_over,
            SUM(CASE WHEN our_bid_px = ref_bid_px AND our_bid_sz < ref_bid_sz
                THEN 1 ELSE 0 END)                                                   AS bid_sz_under,
            SUM(CASE WHEN our_ask_px = ref_ask_px AND our_ask_sz > ref_ask_sz
                THEN 1 ELSE 0 END)                                                   AS ask_sz_over,
            SUM(CASE WHEN our_ask_px = ref_ask_px AND our_ask_sz < ref_ask_sz
                THEN 1 ELSE 0 END)                                                   AS ask_sz_under,
            -- Average mismatch magnitude (INT already — no overflow risk)
            ROUND(AVG(CASE WHEN our_bid_px = ref_bid_px AND our_bid_sz != ref_bid_sz
                THEN ABS(our_bid_sz - ref_bid_sz) END), 2)                           AS avg_bid_sz_delta,
            ROUND(AVG(CASE WHEN our_ask_px = ref_ask_px AND our_ask_sz != ref_ask_sz
                THEN ABS(our_ask_sz - ref_ask_sz) END), 2)                           AS avg_ask_sz_delta
        FROM joined
    """).fetchone()

    n_joined     = stats[0]
    n_bid_px     = stats[1]
    n_ask_px     = stats[2]
    n_bid_sz     = stats[3]
    n_ask_sz     = stats[4]
    n_bid_ct     = stats[5]
    n_ask_ct     = stats[6]
    n_full       = stats[7]
    bid_sz_over  = stats[8]
    bid_sz_under = stats[9]
    ask_sz_over  = stats[10]
    ask_sz_under = stats[11]
    avg_bid_d    = stats[12]
    avg_ask_d    = stats[13]

    con.close()

    pct = lambda n: round(100.0 * n / n_joined, 4) if n_joined > 0 else 0.0

    px_rate = min(pct(n_bid_px), pct(n_ask_px))
    sz_rate = min(pct(n_bid_sz), pct(n_ask_sz))

    if px_rate >= _THRESHOLDS["RELIABLE"][0] and sz_rate >= _THRESHOLDS["RELIABLE"][1]:
        verdict = "RELIABLE"
    elif px_rate >= _THRESHOLDS["ACCEPTABLE"][0] and sz_rate >= _THRESHOLDS["ACCEPTABLE"][1]:
        verdict = "ACCEPTABLE"
    else:
        verdict = "NEEDS_WORK"

    return {
        "our_total"    : our_total,
        "ref_total"    : n_ref_rows,
        "our_burst"    : our_burst,
        "ref_burst"    : ref_burst,
        "n_joined"     : n_joined,
        "n_bid_px"     : n_bid_px,
        "n_ask_px"     : n_ask_px,
        "n_bid_sz"     : n_bid_sz,
        "n_ask_sz"     : n_ask_sz,
        "n_bid_ct"     : n_bid_ct,
        "n_ask_ct"     : n_ask_ct,
        "n_full"       : n_full,
        "bid_sz_over"  : bid_sz_over,
        "bid_sz_under" : bid_sz_under,
        "ask_sz_over"  : ask_sz_over,
        "ask_sz_under" : ask_sz_under,
        "avg_bid_d"    : avg_bid_d,
        "avg_ask_d"    : avg_ask_d,
        "px_rate"      : px_rate,
        "sz_rate"      : sz_rate,
        "verdict"      : verdict,
        "pct"          : pct,   # lambda — used by caller for per-column display
    }


# ---------------------------------------------------------------------------
# Step 3 — Mismatch display
# ---------------------------------------------------------------------------

def _print_mismatches(
    our_str       : str,
    ref_str       : str,
    max_mismatches: int,
) -> None:
    """
    Print the first N mismatch rows for manual inspection.

    Skipped if max_mismatches == 0 or if there are no mismatches.
    All computation stays in DuckDB.
    """
    if max_mismatches == 0:
        return

    con = duckdb.connect()
    mismatches = con.execute(f"""
        WITH our_unique AS (
            SELECT
                ts_recv,
                sequence,
                action,
                side,
                price                  AS our_price,
                bid_px_00              AS our_bid_px,
                ask_px_00              AS our_ask_px,
                bid_sz_00              AS our_bid_sz,
                ask_sz_00              AS our_ask_sz,
                CAST(bid_ct_00 AS INT) AS our_bid_ct,
                CAST(ask_ct_00 AS INT) AS our_ask_ct
            FROM '{our_str}'
            QUALIFY COUNT(*) OVER (PARTITION BY ts_recv, sequence) = 1
        ),
        ref_unique AS (
            SELECT
                ts_recv,
                sequence,
                bid_px_00              AS ref_bid_px,
                ask_px_00              AS ref_ask_px,
                bid_sz_00              AS ref_bid_sz,
                ask_sz_00              AS ref_ask_sz,
                CAST(bid_ct_00 AS INT) AS ref_bid_ct,
                CAST(ask_ct_00 AS INT) AS ref_ask_ct
            FROM '{ref_str}'
            QUALIFY COUNT(*) OVER (PARTITION BY ts_recv, sequence) = 1
        )
        SELECT
            o.ts_recv,
            o.sequence,
            o.action,
            o.side,
            o.our_price,
            o.our_bid_px,  r.ref_bid_px,
            o.our_bid_sz,  r.ref_bid_sz,
            o.our_ask_px,  r.ref_ask_px,
            o.our_ask_sz,  r.ref_ask_sz,
            o.our_bid_ct,  r.ref_bid_ct,
            o.our_ask_ct,  r.ref_ask_ct
        FROM our_unique o
        INNER JOIN ref_unique r USING (ts_recv, sequence)
        WHERE NOT (
            o.our_bid_px = r.ref_bid_px AND o.our_ask_px = r.ref_ask_px AND
            o.our_bid_sz = r.ref_bid_sz AND o.our_ask_sz = r.ref_ask_sz AND
            o.our_bid_ct = r.ref_bid_ct AND o.our_ask_ct = r.ref_ask_ct
        )
        ORDER BY o.ts_recv
        LIMIT {max_mismatches}
    """).fetchall()
    con.close()

    if not mismatches:
        print(f"\n  {GREEN}PERFECT MATCH — 0 mismatches on unique-key subset{RESET}")
        return

    print(f"\n  First {len(mismatches)} mismatches "
          f"(ts_recv, seq, action, side, price, "
          f"our_bid/ref_bid, our_bid_sz/ref_bid_sz, "
          f"our_ask/ref_ask, our_ask_sz/ref_ask_sz, "
          f"our_bid_ct/ref_bid_ct, our_ask_ct/ref_ask_ct):")
    for m in mismatches:
        print(f"    {m}")


# ---------------------------------------------------------------------------
# Top-level per-date orchestration
# ---------------------------------------------------------------------------

def validate(
    product       : str,
    contract      : str,
    date_str      : str,
    max_mismatches: int  = 20,
    keep_ref      : bool = False,
) -> dict:
    """
    Full validation pipeline for one product/contract/date.

    Steps:
        1. Check file existence (our MBP-1 + Databento .dbn.zst)
        2. Stream .dbn.zst → temporary Parquet in /tmp (filtered to contract)
        3. DuckDB unique-key join validation
        4. Print report
        5. Delete temporary Parquet (unless keep_ref=True)

    Args:
        product:        product ticker, e.g. "ES"
        contract:       normalized contract name, e.g. "ESZ25"
        date_str:       date in YYYYMMDD format
        max_mismatches: max mismatch rows to display (0 = suppress)
        keep_ref:       if True, do not delete the temporary reference Parquet

    Returns:
        Result dict suitable for multi-date summary display.
        Keys: product, contract, date, ref_symbol, our_total, ref_total,
              our_burst, ref_burst, n_joined, n_full_match, full_rate,
              bid_px_rate, ask_px_rate, bid_sz_rate, ask_sz_rate,
              bid_ct_rate, ask_ct_rate, px_rate, sz_rate, verdict.
    """
    our_path = _our_mbp1_path(product, contract, date_str)
    dbn_path = _raw_mbp1_path(product, date_str)

    # ── File existence ────────────────────────────────────────────────────
    if not our_path.exists():
        raise FileNotFoundError(
            f"Reconstructed MBP-1 not found: {our_path}\n"
            f"  → Run: python reconstruction/build_mbp1.py "
            f"--product {product} --contract {contract} "
            f"--date {date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        )
    if not dbn_path.exists():
        raise FileNotFoundError(
            f"Databento MBP-1 reference not found: {dbn_path}\n"
            f"  → Expected: {dbn_path.name}"
        )

    session_date = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:]))

    # ── Step 1: Stream .dbn.zst → temporary Parquet ──────────────────────
    # Use a named temp file in /tmp — deleted in the finally block unless
    # keep_ref=True. Suffix encodes product+contract+date for easy identification
    # when --keep-ref is used.
    tmp_suffix  = f"_{product}_{contract}_{date_str}_mbp1_ref.parquet"
    tmp_path    = Path(tempfile.mktemp(suffix=tmp_suffix, dir="/tmp"))

    log.info("[STREAM] %s → %s", dbn_path.name, tmp_path.name)
    try:
        n_ref_rows, ref_symbol = _stream_ref_to_parquet(
            dbn_path     = dbn_path,
            out_path     = tmp_path,
            contract     = contract,
            session_date = session_date,
        )
    except Exception:
        # Clean up partial tmp file on streaming failure
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    if ref_symbol is None or n_ref_rows == 0:
        if tmp_path.exists():
            tmp_path.unlink()
        raise ValueError(
            f"No records found for contract '{contract}' in {dbn_path.name}.\n"
            f"  → Check that the .dbn.zst file covers this contract."
        )

    log.info(
        "[STREAM] Done — %d rows for symbol '%s'", n_ref_rows, ref_symbol
    )

    # ── Steps 2 & 3: Validate + print mismatches ─────────────────────────
    our_str = str(our_path)
    ref_str = str(tmp_path)

    try:
        v = _run_validation(our_str, ref_str, ref_symbol, n_ref_rows)
        pct = v.pop("pct")   # lambda — used for display, not stored in result

        # ── Print report ──────────────────────────────────────────────────
        verdict_color = GREEN if v["verdict"] == "RELIABLE" else (
            YELLOW if v["verdict"] == "ACCEPTABLE" else RED
        )

        print(f"\n{'=' * 72}")
        print(f"  {BOLD}MBP-1 VALIDATION — {product} {contract} {date_str}{RESET}")
        print(f"{'=' * 72}")
        print(f"  Reference symbol    : {ref_symbol}")
        print(f"  Our MBP-1 rows      : {v['our_total']:>12,}")
        print(f"  Ref MBP-1 rows      : {v['ref_total']:>12,}")
        print(f"  Our burst excluded  : {v['our_burst']:>12,}"
              f"  ({100*v['our_burst']/v['our_total']:.1f}% of our rows)")
        print(f"  Ref burst excluded  : {v['ref_burst']:>12,}"
              f"  ({100*v['ref_burst']/v['ref_total']:.1f}% of ref rows)")
        print(f"  Unique-key joined   : {v['n_joined']:>12,}"
              f"  ({100*v['n_joined']/v['ref_total']:.1f}% of ref)")
        print(f"{'─' * 72}")
        print(f"  {'Column':<14}  {'Matched':>10}  {'Total':>10}  {'Rate':>10}")
        print(f"  {'─'*14}  {'─'*10}  {'─'*10}  {'─'*10}")
        for col, n in [
            ("bid_px_00", v["n_bid_px"]), ("ask_px_00", v["n_ask_px"]),
            ("bid_sz_00", v["n_bid_sz"]), ("ask_sz_00", v["n_ask_sz"]),
            ("bid_ct_00", v["n_bid_ct"]), ("ask_ct_00", v["n_ask_ct"]),
        ]:
            print(f"  {col:<14}  {n:>10,}  {v['n_joined']:>10,}  {pct(n):>9.4f}%")
        print(f"{'─' * 72}")
        print(f"  {'FULL MATCH':<14}  {v['n_full']:>10,}  {v['n_joined']:>10,}"
              f"  {pct(v['n_full']):>9.4f}%")
        print(f"{'─' * 72}")
        print(f"  Size direction (price-correct subset):")
        print(f"    bid_sz : +{v['bid_sz_over']:,} over / -{v['bid_sz_under']:,} under"
              f"  (avg |delta|: {v['avg_bid_d']})")
        print(f"    ask_sz : +{v['ask_sz_over']:,} over / -{v['ask_sz_under']:,} under"
              f"  (avg |delta|: {v['avg_ask_d']})")
        print(f"{'─' * 72}")
        print(f"  {BOLD}VERDICT: {verdict_color}{v['verdict']}{RESET}{BOLD}"
              f"  (px_min={v['px_rate']:.2f}%  sz_min={v['sz_rate']:.2f}%){RESET}")
        print(f"{'=' * 72}")

        if keep_ref:
            log.info("[KEEP]  Temporary reference Parquet kept at: %s", tmp_path)
        else:
            log.info("[CLEAN] Deleting temporary reference Parquet")

        _print_mismatches(our_str, ref_str, max_mismatches)

    finally:
        # Always clean up the temp file — even if validation throws.
        # Suppressed by --keep-ref to allow manual DuckDB inspection.
        if not keep_ref and tmp_path.exists():
            tmp_path.unlink()
            log.debug("Deleted temporary file: %s", tmp_path)

    # ── Build result dict ─────────────────────────────────────────────────
    return {
        "product"      : product,
        "contract"     : contract,
        "ref_symbol"   : ref_symbol,
        "date"         : date_str,
        "our_total"    : v["our_total"],
        "ref_total"    : v["ref_total"],
        "our_burst"    : v["our_burst"],
        "ref_burst"    : v["ref_burst"],
        "n_joined"     : v["n_joined"],
        "n_full_match" : v["n_full"],
        "full_rate"    : pct(v["n_full"]),
        "bid_px_rate"  : pct(v["n_bid_px"]),
        "ask_px_rate"  : pct(v["n_ask_px"]),
        "bid_sz_rate"  : pct(v["n_bid_sz"]),
        "ask_sz_rate"  : pct(v["n_ask_sz"]),
        "bid_ct_rate"  : pct(v["n_bid_ct"]),
        "ask_ct_rate"  : pct(v["n_ask_ct"]),
        "px_rate"      : v["px_rate"],
        "sz_rate"      : v["sz_rate"],
        "verdict"      : v["verdict"],
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate reconstructed MBP-1 against Databento native reference."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python tests/validation/validate_mbp1_databento.py \\\n"
            "      --product ES --contract ESZ25 --date 20251001\n\n"
            "  python tests/validation/validate_mbp1_databento.py \\\n"
            "      --product ES --contract ESZ25 \\\n"
            "      --date 20251001 20251010 20251027\n"
        ),
    )
    parser.add_argument(
        "--product", required=True,
        help="Product ticker (e.g. ES).",
    )
    parser.add_argument(
        "--contract", required=True,
        help="Normalized contract name (e.g. ESZ25).",
    )
    parser.add_argument(
        "--date", required=True, nargs="+",
        help="One or more dates in YYYYMMDD or YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--max-mismatches", type=int, default=20,
        help=(
            "Max mismatch rows to display per date (default: 20). "
            "Set to 0 to suppress."
        ),
    )
    parser.add_argument(
        "--keep-ref", action="store_true",
        help=(
            "Keep the temporary reference Parquet in /tmp after validation. "
            "Useful for manual DuckDB inspection of mismatches."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    if args.product not in MARKET_CONFIG:
        log.error("Unknown product '%s'. Add it to market_config.py.", args.product)
        return 2

    all_results    : list[dict] = []
    any_needs_work  = False
    any_setup_error = False

    for date_str in args.date:
        # Accept both YYYYMMDD and YYYY-MM-DD
        date_str = date_str.replace("-", "")
        if len(date_str) != 8 or not date_str.isdigit():
            log.error(
                "Invalid date '%s'. Expected YYYYMMDD or YYYY-MM-DD.", date_str
            )
            any_setup_error = True
            continue

        print(f"\n{BOLD}--- {args.product} {args.contract} {date_str} ---{RESET}")
        try:
            result = validate(
                product        = args.product,
                contract       = args.contract,
                date_str       = date_str,
                max_mismatches = args.max_mismatches,
                keep_ref       = args.keep_ref,
            )
            all_results.append(result)
            if result["verdict"] == "NEEDS_WORK":
                any_needs_work = True

        except FileNotFoundError as e:
            log.error("[SETUP ERROR]\n%s", e)
            any_setup_error = True
        except Exception as e:
            log.exception("[ERROR] %s %s %s: %s", args.product, args.contract, date_str, e)
            any_setup_error = True

    # ── Multi-date summary ────────────────────────────────────────────────
    if len(all_results) > 1:
        print(f"\n{'=' * 72}")
        print(f"  {BOLD}SUMMARY — {args.product} {args.contract}{RESET}")
        print(f"{'=' * 72}")
        print(f"  {'Date':<10}  {'px_min':>8}  {'sz_min':>8}  {'full':>8}  verdict")
        print(f"  {'─'*10}  {'─'*8}  {'─'*8}  {'─'*8}  {'─'*10}")
        for r in all_results:
            color = GREEN if r["verdict"] == "RELIABLE" else (
                YELLOW if r["verdict"] == "ACCEPTABLE" else RED
            )
            print(
                f"  {r['date']:<10}  "
                f"{r['px_rate']:>7.2f}%  "
                f"{r['sz_rate']:>7.2f}%  "
                f"{r['full_rate']:>7.2f}%  "
                f"{color}{r['verdict']}{RESET}"
            )
        print(f"{'=' * 72}")

    # ── Exit codes — consistent with check_regression.py ─────────────────
    if any_setup_error:
        return 2
    if any_needs_work:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())