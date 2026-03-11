# cleaning/eurex_config.py
#
# Shared configuration and DuckDB helpers for all Eurex cleaning scripts.
# Imported by: clean_eurex_orders.py, clean_eurex_trades.py,
#              build_eurex_lob1.py, run_eurex_pipeline.py
#
# Session time convention:
#   All times are CET (UTC+1). Eurex does NOT adjust for DST.
#   Pipeline filters on UTC timestamps (ts_recv) by subtracting 1h from CET.
#   This avoids per-row timezone() calls on 50M+ rows and is DST-safe.
#
# To add a new Eurex product: add one entry to PRODUCT_CONFIG.
# Verify session times at the product's eurex_url before adding.

# ---------------------------------------------------------------------------
# PRODUCT CONFIG
# ---------------------------------------------------------------------------

PRODUCT_CONFIG = {
    "FDAX": {
        "tick_size_fp":      500_000_000,       # 0.5 pt * 1e9
        "price_floor_fp":    1_000_000_000_000, # 1000 pt * 1e9 — median bootstrap floor
        "description":       "DAX Future (Eurex)",
        "eurex_url":         "https://www.eurex.com/ex-en/markets/idx/dax/DAX-Futures-139902",
        # All times CET (UTC+1) — Eurex does NOT adjust for DST
        "session_start_cet": "01:10:00",        # continuous trading open
        "session_end_cet":   "22:00:00",        # closing auction start
        "rth_start_cet":     "08:00:00",        # liquid hours for cross-market research
        "rth_end_cet":       "22:00:00",
    },
    "FESX": {
        "tick_size_fp":      1_000_000_000,     # 1 pt * 1e9
        "price_floor_fp":    1_000_000_000_000,
        "description":       "Euro Stoxx 50 Future (Eurex)",
        "eurex_url":         "https://www.eurex.com/ex-en/markets/idx/stx/euro-stoxx-50-derivatives/products/EURO-STOXX-50-Index-Futures-160088",
        # All times CET (UTC+1) — Eurex does NOT adjust for DST
        "session_start_cet": "01:10:00",
        "session_end_cet":   "22:00:00",
        "rth_start_cet":     "08:00:00",
        "rth_end_cet":       "22:00:00",
    },
    "FSMI": {
        "tick_size_fp":      1_000_000_000,     # 1 pt * 1e9
        "price_floor_fp":    1_000_000_000_000,
        "description":       "SMI Future (Eurex)",
        "eurex_url":         "https://www.eurex.com/ex-en/markets/idx/country/six/SMI-Futures-952762",
        # All times CET (UTC+1) — Eurex does NOT adjust for DST
        # FSMI opens later than FDAX/FESX — SIX Exchange hours
        "session_start_cet": "07:50:00",
        "session_end_cet":   "22:00:00",
        "rth_start_cet":     "08:00:00",
        "rth_end_cet":       "22:00:00",
    },
}

# Convenience list for argparse choices and orchestrator
AVAILABLE_PRODUCTS = list(PRODUCT_CONFIG.keys())

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def cet_to_utc(time_str: str) -> str:
    """
    Convert a CET time string (HH:MM:SS, UTC+1, fixed — no DST) to UTC.
    Eurex publishes all session times in CET regardless of summer/winter time.
    Subtracting 1h is always correct for Eurex data.

    Examples:
      "01:10:00" -> "00:10:00"
      "08:00:00" -> "07:00:00"
      "22:00:00" -> "21:00:00"
      "00:30:00" -> "23:30:00"  (wraps midnight)
    """
    h, m, s = map(int, time_str.split(":"))
    h_utc = (h - 1) % 24
    return f"{h_utc:02d}:{m:02d}:{s:02d}"


def ts_utc_expr() -> str:
    """
    DuckDB expression: convert ts_recv (UBIGINT nanoseconds UTC) to UTC TIMESTAMP.
    Used as the base for all time-based filters.
    Faster than timezone() — no tz conversion, pure arithmetic.

    Pattern: CAST to BIGINT first (UBIGINT arithmetic unsupported in DuckDB),
    divide by 1e9 to get seconds, then to_timestamp() for UTC TIMESTAMP.
    """
    return "to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)"


def ts_local_expr(tz: str = "Europe/Paris") -> str:
    """
    DuckDB expression: convert ts_recv to local TIMESTAMPTZ.
    Used only where local time display is needed (diagnostics, logging).
    NOT used for session filtering — use session_where_clause() instead.
    """
    return f"timezone('{tz}', {ts_utc_expr()})"


def session_where_clause(
    cfg:         dict,
    use_rth:     bool = False,
    date_filter: str  = "",
) -> str:
    """
    Build a DuckDB WHERE clause for Eurex session filtering.
    Filters on UTC timestamps derived from ts_recv — no per-row timezone conversion.

    Args:
        cfg:         product config dict from PRODUCT_CONFIG
        use_rth:     if True, use rth_start/end (liquid hours) instead of full session
        date_filter: optional extra AND clause (e.g. "AND date_col = '2025-05-02'")

    CET->UTC conversion is applied here once at query-build time.
    ISODOW on UTC timestamp is correct: Eurex sessions do not span midnight UTC
    for the relevant session windows (01:10 CET = 00:10 UTC, still same UTC day).

    Note on FDAX/FESX midnight boundary:
      session_start_cet = 01:10 CET = 00:10 UTC — same UTC weekday, no date shift issue.
      ISODOW filter on UTC is therefore safe for all three products.
    """
    start_cet = cfg["rth_start_cet"]   if use_rth else cfg["session_start_cet"]
    end_cet   = cfg["rth_end_cet"]     if use_rth else cfg["session_end_cet"]

    start_utc = cet_to_utc(start_cet)
    end_utc   = cet_to_utc(end_cet)

    ts_utc = ts_utc_expr()

    return f"""
        ISODOW({ts_utc}) BETWEEN 1 AND 5
        AND {ts_utc}::TIME >= '{start_utc}'::TIME
        AND {ts_utc}::TIME <  '{end_utc}'::TIME
        {date_filter}
    """