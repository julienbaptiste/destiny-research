from pathlib import Path

# ─── Changer cette ligne uniquement si tu déplaces la donnée ───
DATA_ROOT = Path.home() / "repo" / "destiny-research" / "data" / "market_data"

# Chemins dérivés
RAW_HKEX        = DATA_ROOT / "raw" / "HSI"
RAW_DATABENTO   = DATA_ROOT / "raw"
PARQUET_DIR     = DATA_ROOT / "parquet"

# Instruments HKEX qui nous intéressent
# Source : OMD-D spec section 8.2 — List of Instrument Code
INSTRUMENTS_OF_INTEREST = {
    "HSI":  "Hang Seng Index Future",
    "MHI":  "Mini Hang Seng Index Future",
    "HHI":  "Hang Seng China Enterprises Index Future",
    "MCH":  "Mini Hang Seng China Enterprises Index Future",
}

# Channels OMD-D (dual channel A/B — on prend A uniquement, B est redondant)
# MC121 = FullTick channel A (Add/Modify/Delete orders + Trades)
# MC221 = FullTick channel B (identique, on ignore)
FULLTICK_CHANNEL_A = "MC121"
FULLTICK_CHANNEL_B = "MC221"  # ignoré — redondant
