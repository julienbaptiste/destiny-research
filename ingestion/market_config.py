"""
market_config.py — Unified market configuration for all products.

Single source of truth for per-product constants used across the ingestion,
reconstruction, and feature engineering layers:
  - Contract specs: tick size, point value, currency
  - Session windows: electronic session, RTH, maintenance, timezone
  - Reconstruction hints: warmup requirements, crossed-book thresholds
  - DuckDB helpers: timestamp expressions, session WHERE clauses

Provider-agnostic: keyed by internal product ticker (ES, FDAX, ...), not by
Databento dataset or HKEX channel. The ingestion adapters handle the mapping
from provider-specific identifiers to these product keys.

DST handling:
  - CME/ES: US/Eastern (EDT/EST), DST-aware via _US_DST_TRANSITIONS table
  - CME/NIY, CME/NKD: JST (UTC+9), fixed — no DST in Japan
  - EUREX (FDAX, FESX, FSMI): CET (UTC+1), fixed — Eurex publishes times in
    CET regardless of summer/winter. Subtracting 1h is always correct.
  - HKEX (HSI, MHI, HHI, MCH): HKT (UTC+8), fixed — no DST in Hong Kong

Adding a new product:
  1. Add an entry to MARKET_CONFIG with all required keys (see existing entries).
  2. If the product uses a new rth_mode, implement it in rth_utc_bounds().
  3. Update AVAILABLE_PRODUCTS if needed (auto-derived from MARKET_CONFIG).
"""

from __future__ import annotations

from datetime import date
from typing import Optional


# ── Flag bitmasks (Databento MBO — shared across CME and EUREX) ───────────────
# flags column is UTINYINT (uint8) in normalized Parquet.
F_LAST        = 0x80  # last record in a multi-message atomic event group
F_TOB         = 0x40  # top-of-book synthetic message (order_id=0)
F_SNAPSHOT    = 0x20  # recovery/snapshot block at CME session open
F_MBP         = 0x10  # aggregated price level (not present in MBO feed)
F_BAD_TS_RECV = 0x08  # ts_recv unreliable (clock drift or packet reorder)
F_BAD_TS      = 0x04  # ts_event unreliable (exchange clock issue)

# ── Sentinel values (Databento / CME MDP3) ────────────────────────────────────
INT64_MAX  = 9_223_372_036_854_775_807   # sentinel price for market/stop orders
UINT64_MAX = 18_446_744_073_709_551_615  # sentinel for missing order_id

# ── DST transition dates for US/Eastern ───────────────────────────────────────
# Used by ES only. All other products use fixed UTC offsets.
# Spring forward: second Sunday of March. Fall back: first Sunday of November.
# Extend this table when purchasing data for new years.
_US_DST_TRANSITIONS: dict[int, tuple[date, date]] = {
    # year: (spring_forward_date, fall_back_date)
    2024: (date(2024, 3, 10), date(2024, 11, 3)),
    2025: (date(2025, 3, 9),  date(2025, 11, 2)),
    2026: (date(2026, 3, 8),  date(2026, 11, 1)),
}


def _is_edt(d: date) -> bool:
    """Return True if date d falls in EDT (UTC-4), False if EST (UTC-5).

    EDT is active between spring-forward (inclusive) and fall-back (exclusive).

    Args:
        d: calendar date to check.

    Raises:
        ValueError: if the year is not in _US_DST_TRANSITIONS. Add it.
    """
    year = d.year
    if year not in _US_DST_TRANSITIONS:
        raise ValueError(
            f"Year {year} not in _US_DST_TRANSITIONS. "
            "Add the DST transition dates for this year in market_config.py."
        )
    spring, fall = _US_DST_TRANSITIONS[year]
    return spring <= d < fall


# ── Unified product configuration ─────────────────────────────────────────────
# Required keys for all products:
#   tick_size_fp      int   — tick size in Databento fixed-point (price_fp / 1e9 = points)
#   price_floor_fp    int   — minimum plausible outright price in fixed-point (sanity check)
#   point_value       float — contract value per index point (in product currency)
#   currency          str   — ISO 4217 currency code
#   exchange          str   — exchange identifier (CME, EUREX, HKEX)
#   provider          str   — data provider (databento, hkex)
#   local_tz          str   — IANA timezone for display (not used for filtering)
#   rth_mode          str   — 'us_eastern' | 'fixed_utc' | 'fixed_cet' | 'fixed_hkt'
#
# Session keys (UTC strings 'HH:MM:SS'):
#   session_open_utc  str   — electronic session open (UTC)
#   session_close_utc str   — electronic session close (UTC)
#
# Maintenance window (CME only):
#   maintenance_start str   — daily maintenance window start (UTC)
#   maintenance_end   str   — daily maintenance window end (UTC)
#
# RTH keys depend on rth_mode:
#   fixed_utc:    rth_start_utc, rth_end_utc
#   us_eastern:   rth_start_utc_edt, rth_end_utc_edt, rth_start_utc_est, rth_end_utc_est
#   fixed_cet:    rth_start_cet, rth_end_cet (converted to UTC at query-build time)
#   fixed_hkt:    rth_start_hkt, rth_end_hkt (converted to UTC at query-build time)
#
# Optional keys:
#   needs_warmup        bool  — True if reconstruction engine must load prev-day data
#                               to seed book state before RTH (NIY/NKD: Itayose effect)
#   crossed_threshold   int   — max acceptable crossed-book events/day before alert
#                               (NKD: structurally thin, crossed books are normal)
#   description         str   — human-readable label
#   product_url         str   — official exchange contract specs URL

MARKET_CONFIG: dict[str, dict] = {

    # ── CME Globex ────────────────────────────────────────────────────────────

    "ES": {
        # E-mini S&P 500 Future — CME Globex
        # https://www.cmegroup.com/markets/equities/sp/e-mini-sandp500.contractSpecs.html
        "tick_size_fp":         250_000_000,        # 0.25 pt * 1e9
        "price_floor_fp":       500_000_000_000,    # 500 pts * 1e9
        "point_value":          50.0,               # $50 per index point
        "currency":             "USD",
        "exchange":             "CME",
        "provider":             "databento",
        "local_tz":             "America/New_York",
        "description":          "E-mini S&P 500 Future (CME Globex)",
        "product_url":          "https://www.cmegroup.com/markets/equities/sp/e-mini-sandp500.contractSpecs.html",
        # Electronic session: 22:00 UTC (prev day) → 21:00 UTC — overnight session
        "session_open_utc":     "22:00:00",
        "session_close_utc":    "21:00:00",
        # CME daily maintenance window (excluded from full-session queries)
        "maintenance_start":    "21:00:00",
        "maintenance_end":      "22:00:00",
        # RTH: 09:30–16:00 ET, DST-aware
        "rth_mode":             "us_eastern",
        "rth_start_utc_edt":    "13:30:00",         # 09:30 ET in EDT (UTC-4)
        "rth_end_utc_edt":      "20:00:00",         # 16:00 ET in EDT
        "rth_start_utc_est":    "14:30:00",         # 09:30 ET in EST (UTC-5)
        "rth_end_utc_est":      "21:00:00",         # 16:00 ET in EST
        # Reconstruction: no warmup needed, CME F_SNAPSHOT seeds the book at open
        "needs_warmup":         False,
    },

    "NIY": {
        # Nikkei 225 Future (JPY-denominated) — CME Globex
        # https://www.cmegroup.com/markets/equities/international-indices/nikkei-225-yen.contractSpecs.html
        #
        # RTH rationale: OSE Tokyo opens at 09:00 JST (00:00 UTC). The first ~2h
        # of the CME Globex session are dominated by post-auction stabilization
        # from the Itayose opening auction on OSE, which purges/rebuilds the book
        # but is invisible in the CME MBO feed. Crossed books are concentrated in
        # 00:00–02:00 UTC and drop to zero after 02:00 UTC (validated empirically).
        # Starting RTH at 11:00 JST gives 4h15 of clean, liquid data per day.
        #
        # needs_warmup=True: reconstruction engine must load prev-day file from
        # session_open (22:00 UTC) + pre-RTH events (00:00–02:00 UTC) to seed the
        # book state machine before emitting RTH rows.
        "tick_size_fp":         5_000_000_000,      # 5 pts * 1e9
        "price_floor_fp":       5_000_000_000_000,  # 5000 pts * 1e9
        "point_value":          500.0,              # ¥500 per index point
        "currency":             "JPY",
        "exchange":             "CME",
        "provider":             "databento",
        "local_tz":             "Asia/Tokyo",
        "description":          "Nikkei 225 Future JPY (CME Globex)",
        "product_url":          "https://www.cmegroup.com/markets/equities/international-indices/nikkei-225-yen.contractSpecs.html",
        # Electronic session: same as ES
        "session_open_utc":     "22:00:00",
        "session_close_utc":    "21:00:00",
        "maintenance_start":    "21:00:00",
        "maintenance_end":      "22:00:00",
        # RTH: 11:00–15:15 JST = 02:00–06:15 UTC (fixed, JST = UTC+9, no DST)
        "rth_mode":             "fixed_utc",
        "rth_start_utc":        "02:00:00",         # 11:00 JST
        "rth_end_utc":          "06:15:00",         # 15:15 JST
        # Reconstruction: warmup required (see rationale above)
        "needs_warmup":         True,
        "crossed_threshold":    150_000,            # alert if crossed books > 150K/day
    },

    "NKD": {
        # Nikkei 225 Future (USD-denominated) — CME Globex
        # https://www.cmegroup.com/markets/equities/international-indices/nikkei-225-dollar.contractSpecs.html
        #
        # Same underlying and RTH as NIY — different currency denomination.
        # Structurally thin (~500 trades/day). Crossed books are a known market
        # property of this USD-denominated contract, not a reconstruction bug.
        # See CONTEXT.md section 3 for microstructure notes.
        "tick_size_fp":         5_000_000_000,      # 5 pts * 1e9
        "price_floor_fp":       5_000_000_000_000,  # 5000 pts * 1e9
        "point_value":          5.0,                # $5 per index point
        "currency":             "USD",
        "exchange":             "CME",
        "provider":             "databento",
        "local_tz":             "Asia/Tokyo",
        "description":          "Nikkei 225 Future USD (CME Globex)",
        "product_url":          "https://www.cmegroup.com/markets/equities/international-indices/nikkei-225-dollar.contractSpecs.html",
        # Electronic session: same as ES and NIY
        "session_open_utc":     "22:00:00",
        "session_close_utc":    "21:00:00",
        "maintenance_start":    "21:00:00",
        "maintenance_end":      "22:00:00",
        # RTH: 11:00–15:15 JST = 02:00–06:15 UTC (same as NIY)
        "rth_mode":             "fixed_utc",
        "rth_start_utc":        "02:00:00",
        "rth_end_utc":          "06:15:00",
        # Reconstruction: warmup required (same Itayose rationale as NIY)
        "needs_warmup":         True,
        # NKD crossed books are structural — threshold set high to avoid noise alerts
        "crossed_threshold":    10_000,
    },

    # ── EUREX ─────────────────────────────────────────────────────────────────
    # Session times are published by Eurex in CET (UTC+1), fixed year-round.
    # Eurex does NOT adjust for DST — CET-1h = UTC is always correct.
    # rth_mode='fixed_cet': session_where_clause() converts CET→UTC at query-build time.

    "FDAX": {
        # DAX Future — Eurex EOBI
        # https://www.eurex.com/ex-en/markets/idx/dax/DAX-Futures-139902
        "tick_size_fp":         500_000_000,        # 0.5 pt * 1e9
        "price_floor_fp":       1_000_000_000_000,  # 1000 pt * 1e9
        "point_value":          25.0,               # €25 per index point
        "currency":             "EUR",
        "exchange":             "EUREX",
        "provider":             "databento",
        "local_tz":             "Europe/Berlin",
        "description":          "DAX Future (Eurex EOBI)",
        "product_url":          "https://www.eurex.com/ex-en/markets/idx/dax/DAX-Futures-139902",
        # Electronic session (CET, fixed)
        "session_open_utc":     "00:10:00",         # 01:10 CET - 1h
        "session_close_utc":    "21:00:00",         # 22:00 CET - 1h
        # RTH: 08:00–22:00 CET (liquid hours for cross-market research)
        "rth_mode":             "fixed_cet",
        "rth_start_cet":        "08:00:00",
        "rth_end_cet":          "22:00:00",
        # No maintenance window on Eurex (session simply closes at 22:00 CET)
        # Reconstruction: no warmup needed for MBP-1 RTH
        # (GTC orphan CANCELs ~1000/day on front month — tolerated, see CONTEXT.md §14)
        "needs_warmup":         False,
    },

    "FESX": {
        # Euro Stoxx 50 Future — Eurex EOBI
        # https://www.eurex.com/ex-en/markets/idx/stx/euro-stoxx-50-derivatives/products/EURO-STOXX-50-Index-Futures-160088
        "tick_size_fp":         1_000_000_000,      # 1 pt * 1e9
        "price_floor_fp":       1_000_000_000_000,  # 1000 pt * 1e9
        "point_value":          10.0,               # €10 per index point
        "currency":             "EUR",
        "exchange":             "EUREX",
        "provider":             "databento",
        "local_tz":             "Europe/Berlin",
        "description":          "Euro Stoxx 50 Future (Eurex EOBI)",
        "product_url":          "https://www.eurex.com/ex-en/markets/idx/stx/euro-stoxx-50-derivatives/products/EURO-STOXX-50-Index-Futures-160088",
        "session_open_utc":     "00:10:00",         # 01:10 CET - 1h
        "session_close_utc":    "21:00:00",         # 22:00 CET - 1h
        "rth_mode":             "fixed_cet",
        "rth_start_cet":        "08:00:00",
        "rth_end_cet":          "22:00:00",
        "needs_warmup":         False,
    },

    "FSMI": {
        # SMI Future — Eurex EOBI
        # https://www.eurex.com/ex-en/markets/idx/country/six/SMI-Futures-952762
        # Note: FSMI opens later than FDAX/FESX — follows SIX Exchange hours.
        "tick_size_fp":         1_000_000_000,      # 1 pt * 1e9
        "price_floor_fp":       1_000_000_000_000,  # 1000 pt * 1e9
        "point_value":          10.0,               # CHF10 per index point
        "currency":             "CHF",
        "exchange":             "EUREX",
        "provider":             "databento",
        "local_tz":             "Europe/Zurich",
        "description":          "SMI Future (Eurex EOBI)",
        "product_url":          "https://www.eurex.com/ex-en/markets/idx/country/six/SMI-Futures-952762",
        # FSMI opens at 07:50 CET (later than FDAX/FESX at 01:10 CET)
        "session_open_utc":     "06:50:00",         # 07:50 CET - 1h
        "session_close_utc":    "21:00:00",         # 22:00 CET - 1h
        "rth_mode":             "fixed_cet",
        "rth_start_cet":        "08:00:00",
        "rth_end_cet":          "22:00:00",
        "needs_warmup":         False,
    },

    # ── HKEX ──────────────────────────────────────────────────────────────────
    # Timestamp precision: ~10ms effective (HKEX clock), stored as nanoseconds.
    # Timezone: HKT = UTC+8, fixed — no DST in Hong Kong.
    # Price denominator: to be confirmed from Series Definition records in
    # hkex_adapter.py (open question, see CONTEXT.md §11).
    # Session structure: morning (09:15–12:00 HKT) + afternoon (13:00–16:30 HKT)
    # with a lunch break. T+1 session: 17:15–23:59 HKT.
    # RTH here covers the main daytime session only (morning + afternoon).

    "HSI": {
        # Hang Seng Index Future — HKEX
        # OTR ~580:1 (outlier — see CONTEXT.md §3 microstructure notes)
        "tick_size_fp":         1_000_000_000,      # 1 pt * 1e9 — to confirm with hkex_adapter
        "price_floor_fp":       5_000_000_000_000,  # 5000 pt * 1e9
        "point_value":          50.0,               # HKD50 per index point
        "currency":             "HKD",
        "exchange":             "HKEX",
        "provider":             "hkex",
        "local_tz":             "Asia/Hong_Kong",
        "description":          "Hang Seng Index Future (HKEX)",
        "product_url":          "https://www.hkex.com.hk/Products/Listed-Derivatives/Equity-Index/Hang-Seng-Index/Hang-Seng-Index-Futures",
        # Main daytime session (UTC = HKT - 8h)
        "session_open_utc":     "01:15:00",         # 09:15 HKT - 8h
        "session_close_utc":    "08:30:00",         # 16:30 HKT - 8h
        # Lunch break: 04:00–05:00 UTC (12:00–13:00 HKT)
        "lunch_break_start":    "04:00:00",
        "lunch_break_end":      "05:00:00",
        # RTH = full daytime session (morning + afternoon, excluding lunch)
        "rth_mode":             "fixed_utc",
        "rth_start_utc":        "01:15:00",         # 09:15 HKT
        "rth_end_utc":          "08:30:00",         # 16:30 HKT
        "needs_warmup":         False,
    },

    "MHI": {
        # Mini Hang Seng Index Future — HKEX
        # Same underlying as HSI, contract size 1/5 (HKD10/pt vs HKD50/pt)
        # Higher retail participation expected — OTR and cancel_rate likely differ
        "tick_size_fp":         1_000_000_000,      # 1 pt * 1e9 — to confirm
        "price_floor_fp":       5_000_000_000_000,
        "point_value":          10.0,               # HKD10 per index point
        "currency":             "HKD",
        "exchange":             "HKEX",
        "provider":             "hkex",
        "local_tz":             "Asia/Hong_Kong",
        "description":          "Mini Hang Seng Index Future (HKEX)",
        "product_url":          "https://www.hkex.com.hk/Products/Listed-Derivatives/Equity-Index/Hang-Seng-Index/Mini-Hang-Seng-Index-Futures",
        "session_open_utc":     "01:15:00",
        "session_close_utc":    "08:30:00",
        "lunch_break_start":    "04:00:00",
        "lunch_break_end":      "05:00:00",
        "rth_mode":             "fixed_utc",
        "rth_start_utc":        "01:15:00",
        "rth_end_utc":          "08:30:00",
        "needs_warmup":         False,
    },

    "HHI": {
        # H-shares China Enterprises Index Future — HKEX
        # Pure play China exposure via H-shares. Less liquid than HSI.
        # Fragmentation HHI/MCH likely more pronounced than HSI/MHI (see CONTEXT.md §3)
        "tick_size_fp":         1_000_000_000,      # 1 pt * 1e9 — to confirm
        "price_floor_fp":       1_000_000_000_000,  # 1000 pt * 1e9
        "point_value":          50.0,               # HKD50 per index point
        "currency":             "HKD",
        "exchange":             "HKEX",
        "provider":             "hkex",
        "local_tz":             "Asia/Hong_Kong",
        "description":          "H-shares China Enterprises Index Future (HKEX)",
        "product_url":          "https://www.hkex.com.hk/Products/Listed-Derivatives/Equity-Index/Hang-Seng-China-Enterprises-Index/Hang-Seng-China-Enterprises-Index-Futures",
        "session_open_utc":     "01:15:00",
        "session_close_utc":    "08:30:00",
        "lunch_break_start":    "04:00:00",
        "lunch_break_end":      "05:00:00",
        "rth_mode":             "fixed_utc",
        "rth_start_utc":        "01:15:00",
        "rth_end_utc":          "08:30:00",
        "needs_warmup":         False,
    },

    "MCH": {
        # Mini H-shares China Enterprises Index Future — HKEX
        # Same underlying as HHI, contract size 1/5 (HKD10/pt vs HKD50/pt)
        "tick_size_fp":         1_000_000_000,      # 1 pt * 1e9 — to confirm
        "price_floor_fp":       1_000_000_000_000,
        "point_value":          10.0,               # HKD10 per index point
        "currency":             "HKD",
        "exchange":             "HKEX",
        "provider":             "hkex",
        "local_tz":             "Asia/Hong_Kong",
        "description":          "Mini H-shares China Enterprises Index Future (HKEX)",
        "product_url":          "https://www.hkex.com.hk/Products/Listed-Derivatives/Equity-Index/Hang-Seng-China-Enterprises-Index/Mini-Hang-Seng-China-Enterprises-Index-Futures",
        "session_open_utc":     "01:15:00",
        "session_close_utc":    "08:30:00",
        "lunch_break_start":    "04:00:00",
        "lunch_break_end":      "05:00:00",
        "rth_mode":             "fixed_utc",
        "rth_start_utc":        "01:15:00",
        "rth_end_utc":          "08:30:00",
        "needs_warmup":         False,
    },
}

# Convenience list — auto-derived, no need to maintain separately
AVAILABLE_PRODUCTS: list[str] = list(MARKET_CONFIG.keys())

# Grouped by exchange — useful for batch operations
PRODUCTS_CME:   list[str] = [p for p, c in MARKET_CONFIG.items() if c["exchange"] == "CME"]
PRODUCTS_EUREX: list[str] = [p for p, c in MARKET_CONFIG.items() if c["exchange"] == "EUREX"]
PRODUCTS_HKEX:  list[str] = [p for p, c in MARKET_CONFIG.items() if c["exchange"] == "HKEX"]


# ── DST / timezone helpers ─────────────────────────────────────────────────────

def _cet_to_utc(time_str: str) -> str:
    """Convert a CET time string (HH:MM:SS, UTC+1, fixed) to UTC.

    Eurex publishes all session times in CET regardless of DST.
    Subtracting 1h is always correct for Eurex data.

    Args:
        time_str: time in 'HH:MM:SS' format (CET).

    Returns:
        UTC time string in 'HH:MM:SS' format (wraps midnight correctly).

    Examples:
        '01:10:00' -> '00:10:00'
        '08:00:00' -> '07:00:00'
        '00:30:00' -> '23:30:00'  (midnight wrap)
    """
    h, m, s = map(int, time_str.split(":"))
    h_utc = (h - 1) % 24
    return f"{h_utc:02d}:{m:02d}:{s:02d}"


def rth_utc_bounds(d: date, cfg: dict) -> tuple[str, str]:
    """Return RTH start/end as UTC time strings for a given date and product config.

    Dispatches on cfg['rth_mode']:
      - 'us_eastern': DST-aware US/Eastern (ES). d is required for DST lookup.
      - 'fixed_utc':  Fixed UTC bounds (NIY, NKD, HSI, MHI, HHI, MCH). d is ignored.
      - 'fixed_cet':  Fixed CET bounds converted to UTC (FDAX, FESX, FSMI). d is ignored.

    Args:
        d:   target calendar date (used for DST lookup on us_eastern products).
        cfg: product config dict from MARKET_CONFIG.

    Returns:
        (rth_start_utc, rth_end_utc) as 'HH:MM:SS' strings.

    Raises:
        ValueError: unknown rth_mode, or missing DST year for us_eastern.
    """
    mode = cfg["rth_mode"]

    if mode == "us_eastern":
        if _is_edt(d):
            return (cfg["rth_start_utc_edt"], cfg["rth_end_utc_edt"])
        else:
            return (cfg["rth_start_utc_est"], cfg["rth_end_utc_est"])

    elif mode == "fixed_utc":
        return (cfg["rth_start_utc"], cfg["rth_end_utc"])

    elif mode == "fixed_cet":
        return (
            _cet_to_utc(cfg["rth_start_cet"]),
            _cet_to_utc(cfg["rth_end_cet"]),
        )

    else:
        raise ValueError(
            f"Unknown rth_mode '{mode}' for product. "
            "Expected: 'us_eastern', 'fixed_utc', 'fixed_cet'."
        )


# ── DuckDB query helpers ───────────────────────────────────────────────────────

def ts_utc_expr(col: str = "ts_recv") -> str:
    """Return a DuckDB expression casting a raw uint64 nanosecond timestamp to UTC TIMESTAMPTZ.

    Normalized Parquet stores timestamps as uint64 nanoseconds since Unix epoch (UTC).
    Cast chain: UBIGINT → BIGINT (signed, required for DuckDB arithmetic) →
    divide by 1e9 to get seconds → to_timestamp() for UTC TIMESTAMPTZ.

    Args:
        col: timestamp column name. Use 'ts_event' for exchange clock,
             'ts_recv' (default) for reception clock.

    Returns:
        DuckDB SQL expression string, e.g.:
        "to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)"
    """
    return f"to_timestamp(CAST({col} AS BIGINT) / 1e9)"


def ts_local_expr(col: str = "ts_recv", tz: str | None = None, product: str | None = None) -> str:
    """Return a DuckDB expression casting a uint64 nanosecond timestamp to local TIMESTAMPTZ.

    For display and diagnostics only — NOT for session filtering (use
    session_where_clause() instead, which operates on UTC to avoid per-row tz conversion).

    Args:
        col:     timestamp column name (default 'ts_recv').
        tz:      explicit IANA timezone string. Takes priority over product.
        product: product ticker; if tz is None, local_tz is read from MARKET_CONFIG.

    Returns:
        DuckDB SQL expression string applying timezone conversion.

    Raises:
        ValueError: if neither tz nor product is provided.
    """
    if tz is None:
        if product is None:
            raise ValueError("Provide either tz or product to ts_local_expr().")
        tz = MARKET_CONFIG[product]["local_tz"]
    return f"timezone('{tz}', {ts_utc_expr(col)})"


def session_where_clause(
    cfg: dict,
    use_rth: bool,
    target_date: Optional[date] = None,
    ts_col: str = "ts_recv",
) -> str:
    """Return a DuckDB WHERE clause fragment for session time filtering.

    Operates on UTC timestamps — no per-row timezone conversion for performance.
    DST handling is done once at query-build time via rth_utc_bounds().

    Two modes:
      use_rth=True:  Regular Trading Hours only.
                     Bounds are product-aware via rth_utc_bounds().
      use_rth=False: Full electronic session.
                     CME: excludes maintenance window (21:00–22:00 UTC).
                     EUREX: full session window (session_open_utc → session_close_utc).
                     HKEX: main daytime session, excludes lunch break if present.

    EUREX products also apply an ISODOW(1–5) weekday filter (no weekend sessions).
    CME products run 6 days/week (Sun–Fri) — weekday filter is not applied here;
    handle at the date-loop level if needed.

    Args:
        cfg:         product config dict from MARKET_CONFIG.
        use_rth:     True for RTH only, False for full electronic session.
        target_date: required for use_rth=True on 'us_eastern' products (ES).
                     Ignored for fixed-RTH products.
        ts_col:      timestamp column name (default 'ts_recv').

    Returns:
        SQL WHERE fragment string (no leading AND/WHERE keyword).

    Raises:
        ValueError: if use_rth=True on a DST product and target_date is None.
    """
    utc_expr  = ts_utc_expr(ts_col)
    # DuckDB: cast chain TIMESTAMPTZ → TIMESTAMP (strip tz) → TIME
    time_cast = f"CAST(CAST({utc_expr} AS TIMESTAMP) AS TIME)"

    exchange = cfg["exchange"]

    if use_rth:
        if target_date is None and cfg.get("rth_mode") == "us_eastern":
            raise ValueError(
                "target_date is required for RTH filtering on DST-aware products (ES)."
            )
        # dummy date for fixed-mode products — rth_utc_bounds() ignores it
        d = target_date or date(2025, 1, 1)
        rth_start, rth_end = rth_utc_bounds(d, cfg)

        clause = (
            f"{time_cast} >= TIME '{rth_start}' AND "
            f"{time_cast} <  TIME '{rth_end}'"
        )
        # EUREX: add weekday guard (no weekend sessions on Eurex)
        if exchange == "EUREX":
            clause = f"ISODOW({utc_expr}) BETWEEN 1 AND 5 AND {clause}"
        # HKEX: exclude lunch break if defined
        if exchange == "HKEX" and "lunch_break_start" in cfg:
            lb_start = cfg["lunch_break_start"]
            lb_end   = cfg["lunch_break_end"]
            clause += (
                f" AND NOT ({time_cast} >= TIME '{lb_start}' AND "
                f"          {time_cast} <  TIME '{lb_end}')"
            )
        return clause

    else:
        # Full electronic session
        if exchange == "CME":
            # CME: exclude maintenance window only (21:00–22:00 UTC daily)
            maint_start = cfg["maintenance_start"]
            maint_end   = cfg["maintenance_end"]
            return (
                f"NOT ({time_cast} >= TIME '{maint_start}' AND "
                f"     {time_cast} <  TIME '{maint_end}')"
            )
        elif exchange == "EUREX":
            # EUREX: session open → session close, weekdays only
            open_utc  = cfg["session_open_utc"]
            close_utc = cfg["session_close_utc"]
            return (
                f"ISODOW({utc_expr}) BETWEEN 1 AND 5 AND "
                f"{time_cast} >= TIME '{open_utc}' AND "
                f"{time_cast} <  TIME '{close_utc}'"
            )
        elif exchange == "HKEX":
            # HKEX: daytime session, exclude lunch break
            open_utc  = cfg["session_open_utc"]
            close_utc = cfg["session_close_utc"]
            clause = (
                f"{time_cast} >= TIME '{open_utc}' AND "
                f"{time_cast} <  TIME '{close_utc}'"
            )
            if "lunch_break_start" in cfg:
                lb_start = cfg["lunch_break_start"]
                lb_end   = cfg["lunch_break_end"]
                clause += (
                    f" AND NOT ({time_cast} >= TIME '{lb_start}' AND "
                    f"          {time_cast} <  TIME '{lb_end}')"
                )
            return clause
        else:
            raise ValueError(f"Unknown exchange '{exchange}' in product config.")


# ── Diagnostics ───────────────────────────────────────────────────────────────

def print_config(product: str) -> None:
    """Print a human-readable summary of the product configuration."""
    if product not in MARKET_CONFIG:
        raise ValueError(f"Unknown product '{product}'. Available: {AVAILABLE_PRODUCTS}")
    cfg = MARKET_CONFIG[product]
    tick_pts  = cfg["tick_size_fp"]  / 1e9
    floor_pts = cfg["price_floor_fp"] / 1e9

    print(f"{'─' * 55}")
    print(f"Product:      {product}  —  {cfg.get('description', '')}")
    print(f"Exchange:     {cfg['exchange']}  |  Provider: {cfg['provider']}")
    print(f"Currency:     {cfg['currency']}  |  Point value: {cfg['point_value']:.1f} {cfg['currency']}/pt")
    print(f"Tick size:    {tick_pts:.4f} pts  ({cfg['tick_size_fp']} fp)")
    print(f"Price floor:  {floor_pts:.0f} pts")
    print(f"Session UTC:  {cfg['session_open_utc']} → {cfg['session_close_utc']}")

    mode = cfg.get("rth_mode", "")
    if mode == "fixed_utc":
        print(f"RTH (UTC):    {cfg['rth_start_utc']} → {cfg['rth_end_utc']}  (fixed)")
    elif mode == "us_eastern":
        print(f"RTH EDT:      {cfg['rth_start_utc_edt']} → {cfg['rth_end_utc_edt']} UTC")
        print(f"RTH EST:      {cfg['rth_start_utc_est']} → {cfg['rth_end_utc_est']} UTC")
    elif mode == "fixed_cet":
        utc_start = _cet_to_utc(cfg["rth_start_cet"])
        utc_end   = _cet_to_utc(cfg["rth_end_cet"])
        print(f"RTH CET:      {cfg['rth_start_cet']} → {cfg['rth_end_cet']}  (= {utc_start}–{utc_end} UTC)")

    if cfg.get("needs_warmup"):
        print(f"Warmup:       required (load prev-day data before RTH)")
    if "crossed_threshold" in cfg:
        print(f"Crossed thr:  {cfg['crossed_threshold']:,} events/day")
    print(f"Timezone:     {cfg['local_tz']}")
    if "product_url" in cfg:
        print(f"URL:          {cfg['product_url']}")
    print(f"{'─' * 55}")


if __name__ == "__main__":
    # Smoke test: print all configs + verify DST logic for ES
    for prod in AVAILABLE_PRODUCTS:
        print_config(prod)
        print()

    # DST verification for ES
    test_dates = [
        date(2025, 6, 15),    # EDT
        date(2025, 11, 15),   # EST
        date(2025, 10, 1),    # EDT (ES dataset start)
        date(2026, 3, 10),    # day after spring-forward 2026
    ]
    print("ES DST verification:")
    es_cfg = MARKET_CONFIG["ES"]
    for d in test_dates:
        edt     = _is_edt(d)
        start, end = rth_utc_bounds(d, es_cfg)
        label   = "EDT (UTC-4)" if edt else "EST (UTC-5)"
        print(f"  {d}  {label}  RTH UTC: {start} – {end}")

    # Fixed-UTC verification for NIY/NKD
    print("\nNIY/NKD fixed RTH (should be identical regardless of date):")
    for prod in ("NIY", "NKD"):
        start, end = rth_utc_bounds(date(2025, 6, 15), MARKET_CONFIG[prod])
        print(f"  {prod}: {start} – {end} UTC")

    # CET→UTC verification for EUREX
    print("\nEUREX CET→UTC conversion:")
    for prod in PRODUCTS_EUREX:
        cfg = MARKET_CONFIG[prod]
        start, end = rth_utc_bounds(date(2025, 6, 15), cfg)
        print(f"  {prod}: {cfg['rth_start_cet']}–{cfg['rth_end_cet']} CET  =  {start}–{end} UTC")