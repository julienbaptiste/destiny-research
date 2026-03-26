"""
config.py — Project-wide path constants and shared configuration.

Single import point for all path references across the codebase:
  - ingestion pipeline (ingest.py, adapters)
  - reconstruction engine
  - regression tests (metrics_mbo.py, generate_golden.py, check_regression.py)
  - feature engineering (Phase 3)

Data lives on external HDD — all paths are absolute.
Import this module wherever a data path is needed rather than hardcoding strings.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Repository root
# ---------------------------------------------------------------------------

# Resolves to the destiny-research repo root regardless of where scripts are
# invoked from. All relative imports within the repo should anchor to REPO_ROOT.
REPO_ROOT = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# Data root — external HDD
# ---------------------------------------------------------------------------

DATA_ROOT          = Path("/media/julien/HDD/data")

# Binary source files — immutable, provider-specific raw feed captures
# data/binaries/provider=<P>/product=<PR>/year=<Y>/month=<MM>/
DATA_BINARIES      = DATA_ROOT / "binaries"

# Raw archives — immutable, provider-specific, compressed
# data/raw/provider=<P>/venue=<V>/product=<PR>/year=<Y>/month=<MM>/
DATA_RAW           = DATA_ROOT / "raw"

# Normalized MBO events — provider-agnostic, event-based, source of truth
# data/normalized/provider=<P>/venue=<V>/product=<PR>/contract=<C>/year=<Y>/month=<MM>/
DATA_NORMALIZED    = DATA_ROOT / "normalized"

# Reconstructed LOB snapshots — provider-agnostic
# data/reconstructed/provider=<P>/venue=<V>/product=<PR>/contract=<C>/year=<Y>/month=<MM>/
DATA_RECONSTRUCTED = DATA_ROOT / "reconstructed"

# Feature signals — recomputable from reconstructed
# data/features/venue=<V>/product=<PR>/contract=<C>/year=<Y>/month=<MM>/
DATA_FEATURES      = DATA_ROOT / "features"

# Instrument map — provider metadata, mapping instrument_id → product/contract
# data/metadata/instrument_map.parquet
DATA_METADATA      = DATA_ROOT / "metadata"
INSTRUMENT_MAP     = DATA_METADATA / "instrument_map.parquet"

# ---------------------------------------------------------------------------
# Regression test paths
# ---------------------------------------------------------------------------

TESTS_ROOT         = REPO_ROOT / "tests"
REGRESSION_ROOT    = TESTS_ROOT / "regression"
GOLDEN_DIR_NORM    = REGRESSION_ROOT / "normalization" / "golden"
GOLDEN_DIR_RECON   = REGRESSION_ROOT / "reconstruction" / "golden"

# ---------------------------------------------------------------------------
# HKEX configuration
# ---------------------------------------------------------------------------

# Core futures — minimum extract set
# Extended universe (options, warrants, exotics) to be added in Phase 3
HKEX_FUTURES: dict[str, str] = {
    "HSI": "Hang Seng Index Future",
    "MHI": "Mini Hang Seng Index Future",
    "HHI": "Hang Seng China Enterprises Index Future",
    "MCH": "Mini Hang Seng China Enterprises Index Future",
}

# OMD-D historical binary channels — Non-SOM feed
# Series Definitions (pass 1 — must run before market data)
HKEX_CHANNELS_SERIES_DEFS = ["MC101", "MC201", "MC301", "MC151"]
# Full Order Book partitions (pass 2 — market data)
HKEX_CHANNELS_ORDER_BOOK  = ["MC221", "MC121", "MC321"]
# Block trades and trade amendments (pass 2 — appended after order book)
HKEX_CHANNELS_BLOCK_TRADES = ["MC167"]

# Binary path helpers — convention: provider=HKEX/product=OMDD/year=Y/month=MM/
# All MC*_All_YYYYMMDD files for a given day live in the same directory.
def hkex_bin_dir(date: str) -> Path:
    """
    Return the directory containing HKEX binary files for a given date.

    Parameters
    ----------
    date : str
        Date in YYYY-MM-DD format, e.g. '2026-02-03'.

    Returns
    -------
    Path
        e.g. /media/julien/HDD/data/binaries/provider=HKEX/product=OMDD/
             year=2026/month=02/
    """
    year, month, _ = date.split("-")
    return (DATA_BINARIES / "provider=HKEX" / "product=OMDD"
            / f"year={year}" / f"month={month}")