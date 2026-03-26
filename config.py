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

# Raw archives — immutable, provider-specific, compressed
# data/raw/provider=<P>/venue=<V>/product=<PR>/year=<Y>/month=<MM>/
DATA_RAW           = DATA_ROOT / "raw"

# Normalized MBO events — provider-agnostic, event-based, source of truth
# data/normalized/provider=<P>/venue=<V>/product=<PR>/contract=<C>/year=<Y>/month=<MM>/
DATA_NORMALIZED    = DATA_ROOT / "normalized"

# Reconstructed LOB snapshots — provider-agnostic, no provider layer
# data/reconstructed/venue=<V>/product=<PR>/contract=<C>/year=<Y>/month=<MM>/
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
# Instruments of interest — keyed by internal product ticker
# Source: HKEX OMD-D spec section 8.2 — List of Instrument Code
HKEX_INSTRUMENTS: dict[str, str] = {
    "HSI": "Hang Seng Index Future",
    "MHI": "Mini Hang Seng Index Future",
    "HHI": "Hang Seng China Enterprises Index Future",
    "MCH": "Mini Hang Seng China Enterprises Index Future",
}

# OMD-D multicast channels — dual channel A/B, channel B is redundant
# MC151/101/201/301 → Series Definitions (loaded at startup)
# MC121 → MHI/MCH FullTick (orders + trades)
# MC221 → HSI/HHI FullTick (orders + trades)
HKEX_CHANNEL_SERIES_DEFS = ["MC151", "MC101", "MC201", "MC301"]
HKEX_CHANNEL_FULLTICK_A  = "MC121"   # MHI / MCH
HKEX_CHANNEL_FULLTICK_B  = "MC221"   # HSI / HHI
# Channel B variants (redundant — ignored during parsing)
HKEX_CHANNEL_FULLTICK_A_REDUNDANT = "MC121B"
HKEX_CHANNEL_FULLTICK_B_REDUNDANT = "MC221B"