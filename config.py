from pathlib import Path

# ─── Data paths ───
DATA_ROOT           = "media" / "julien" / "HDD" / "data"
DATA_RAW            = "media" / "julien" / "HDD" / "data" / "raw"
DATA_NORMALIZED     = "media" / "julien" / "HDD" / "data" / "normalized"
DATA_RECONSTRUCTED  = "media" / "julien" / "HDD" / "data" / "reconstructed"

# ─── EVERYTHING BELOW THIS LINE MUST BE RECONSIDERED ───

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
