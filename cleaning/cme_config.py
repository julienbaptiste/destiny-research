"""
cme_config.py — CME Globex product configuration and DuckDB query helpers.

Mirrors the architecture of eurex_config.py:
  - PRODUCT_CONFIG: per-product constants (tick size, price floor, sessions)
  - AVAILABLE_PRODUCTS: list for CLI validation
  - DuckDB helper functions: ts_utc_expr(), ts_local_expr(),
    session_where_clause(), front_month_subquery()

Key CME-specific differences vs Eurex:
  - Sessions are overnight (22:00 UTC prev day → 21:00 UTC same day)
  - Daily maintenance window: 21:00–22:00 UTC
  - RTH timezone: product-dependent
      ES:      US/Eastern (EDT/EST — DST applies)
      NIY/NKD: Asia/Tokyo (JST — no DST, fixed UTC+9)
  - RTH UTC offsets:
      ES EDT:  13:30–20:00 UTC  |  ES EST: 14:30–21:00 UTC
      NIY/NKD: 00:00–06:15 UTC  (fixed, JST = UTC+9, no DST)
  - F_SNAPSHOT (flag & 32) marks recovery block at session open — must exclude
    from live event stream (unlike Eurex EOBI which uses crossed-book warm-up)
  - Calendar spread instruments share the same MBO feed (e.g. ESZ5-ESH6)
    → front-month filter by dominant symbol is mandatory
  - No implied legs at negative prices on outrights — negatives are all spreads
  - flags=0 on ALL trades (T and F) — F_LAST is NOT usable for T/F pairing
    (use ts_event + price grouping in Phase 3 feature engineering instead)
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Optional

# ── Flag bitmasks (Databento / CME MDP3) ──────────────────────────────────────
# flags column is UTINYINT (uint8). Using Python ints for DuckDB SQL embedding.
F_LAST           = 128  # last record in a multi-message event
F_TOB            = 64   # top-of-book aggregate — NOT used by CME MBO feed
F_SNAPSHOT       = 32   # recovery/snapshot block at session open
F_MBP            = 16   # aggregated price level — NOT used by CME MBO feed
F_BAD_TS_RECV    = 8    # ts_recv unreliable (clock drift or packet reorder)
F_MAYBE_BAD_BOOK = 4    # unrecoverable gap in channel

# ── Sentinel values ────────────────────────────────────────────────────────────
INT64_MAX  = 9_223_372_036_854_775_807   # sentinel price for market/stop orders
UINT64_MAX = 18_446_744_073_709_551_615  # sentinel for missing order_id

# ── DST transition dates for US/Eastern (EDT→EST) ─────────────────────────────
# Used by ES only. NIY/NKD use JST (no DST).
# Clocks fall back: first Sunday of November each year
# Clocks spring forward: second Sunday of March each year
# Add new years as needed when extending the dataset.
_US_DST_TRANSITIONS: dict[int, tuple[date, date]] = {
    # year: (spring_forward, fall_back)
    2024: (date(2024, 3, 10), date(2024, 11, 3)),
    2025: (date(2025, 3, 9),  date(2025, 11, 2)),
    2026: (date(2026, 3, 8),  date(2026, 11, 1)),
}


def _is_edt(d: date) -> bool:
    """Return True if date d falls in EDT (UTC-4), False if EST (UTC-5).

    EDT is active between spring-forward and fall-back (exclusive).
    Raises ValueError if the year is not in _US_DST_TRANSITIONS.
    """
    year = d.year
    if year not in _US_DST_TRANSITIONS:
        raise ValueError(
            f"Year {year} not in _US_DST_TRANSITIONS. "
            "Add the DST transition dates for this year in cme_config.py."
        )
    spring, fall = _US_DST_TRANSITIONS[year]
    return spring <= d < fall


def rth_utc_bounds(d: date, cfg: dict | None = None) -> tuple[str, str]:
    """Return RTH start/end as UTC time strings for a given date and product.

    Product-aware: reads rth_mode from cfg to determine the RTH logic.
      - rth_mode='us_eastern' (ES): DST-aware, 09:30–16:00 ET
          EDT (UTC-4): 13:30–20:00 UTC
          EST (UTC-5): 14:30–21:00 UTC
      - rth_mode='fixed' (NIY/NKD): fixed UTC bounds from config
          No DST, reads rth_start_utc / rth_end_utc directly

    Backward compatibility: if cfg is None, assumes ES (US/Eastern DST-aware).
    This preserves the original call signature for existing code.

    Args:
        d:   target date (used for DST lookup on US/Eastern products)
        cfg: product config dict from PRODUCT_CONFIG (optional for ES compat)

    Returns:
        (rth_start_utc, rth_end_utc) as 'HH:MM:SS' strings.
    """
    # Backward compat: no cfg = ES default behaviour
    if cfg is None:
        if _is_edt(d):
            return ("13:30:00", "20:00:00")
        else:
            return ("14:30:00", "21:00:00")

    rth_mode = cfg.get("rth_mode", "us_eastern")

    if rth_mode == "fixed":
        # Fixed UTC bounds — no DST (e.g. NIY/NKD with JST)
        return (cfg["rth_start_utc"], cfg["rth_end_utc"])
    elif rth_mode == "us_eastern":
        # DST-aware US/Eastern (ES)
        if _is_edt(d):
            return (cfg["rth_start_utc_edt"], cfg["rth_end_utc_edt"])
        else:
            return (cfg["rth_start_utc_est"], cfg["rth_end_utc_est"])
    else:
        raise ValueError(f"Unknown rth_mode '{rth_mode}' in product config")


# ── Product configuration ──────────────────────────────────────────────────────
# tick_size_fp:    tick size in Databento fixed-point (price * 1e-9 = points)
# price_floor_fp:  minimum plausible outright price in fixed-point (sanity check)
# point_value:     value per index point in the contract currency
# rth_mode:        'us_eastern' (DST-aware) or 'fixed' (constant UTC bounds)
# session_open_utc / session_close_utc: electronic session bounds in UTC
# maintenance_start / maintenance_end:  daily CME maintenance window (21:00–22:00 UTC)

PRODUCT_CONFIG: dict[str, dict] = {
    "ES": {
        # E-mini S&P 500 Future — CME Globex
        # Source: https://www.cmegroup.com/markets/equities/sp/e-mini-sandp500.html
        # Dataset: GLBX.MDP3 (Databento)
        "tick_size_fp":      250_000_000,        # 0.25 pt * 1e9
        "price_floor_fp":    500_000_000_000,    # 500 pts * 1e9
        "point_value":       50.0,               # $50 per index point
        "currency":          "USD",
        "exchange":          "CME",
        "databento_dataset": "GLBX.MDP3",
        "description":       "E-mini S&P 500 Future (CME Globex)",
        "product_url":       "https://www.cmegroup.com/markets/equities/sp/e-mini-sandp500.html",
        # Electronic session (UTC)
        "session_open_utc":  "22:00:00",   # prev day 22:00 UTC
        "session_close_utc": "21:00:00",   # today 21:00 UTC
        "maintenance_start": "21:00:00",
        "maintenance_end":   "22:00:00",
        # RTH: 09:30–16:00 ET, DST-aware
        "rth_mode":          "us_eastern",
        "rth_start_utc_edt": "13:30:00",   # 09:30 ET in EDT (UTC-4)
        "rth_end_utc_edt":   "20:00:00",   # 16:00 ET in EDT
        "rth_start_utc_est": "14:30:00",   # 09:30 ET in EST (UTC-5)
        "rth_end_utc_est":   "21:00:00",   # 16:00 ET in EST
        # Timezone for display
        "local_tz":          "America/New_York",
    },
    "NIY": {
        # Nikkei 225 Future (JPY-denominated) — CME Globex
        # Source: https://www.cmegroup.com/markets/equities/nikkei/nikkei-225-yen.html
        # Dataset: GLBX.MDP3 (Databento)
        # Same Globex session as ES (18:00–17:00 ET, maintenance 17:00–18:00 ET)
        #
        # RTH for research: 11:00–15:15 JST = 02:00–06:15 UTC (fixed, no DST)
        # OSE Tokyo opens at 09:00 JST (00:00 UTC) but the first ~2h of the CME
        # Globex session are dominated by post-auction stabilization: the Itayose
        # opening auction on OSE purges/rebuilds the book, but this mechanism is
        # not visible in the CME MBO feed. Crossed books are concentrated in
        # 00:00–02:00 UTC and drop to zero after 02:00 UTC (validated empirically).
        # Starting RTH at 11:00 JST gives 4h15 of clean, liquid data per day.
        #
        # needs_warmup=True: the builder loads the prev-day file from session_open
        # (22:00 UTC) + same-day raw events before RTH (00:00–02:00 UTC) to seed
        # the book state machine before emitting RTH rows.
        #
        # JST = UTC+9 (no DST in Japan)
        "tick_size_fp":      5_000_000_000,      # 5 pts * 1e9
        "price_floor_fp":    5_000_000_000_000,  # 5000 pts * 1e9 (Nikkei floor)
        "point_value":       500.0,              # ¥500 per index point
        "currency":          "JPY",
        "exchange":          "CME",
        "databento_dataset": "GLBX.MDP3",
        "description":       "Nikkei 225 Future JPY (CME Globex)",
        "product_url":       "https://www.cmegroup.com/markets/equities/nikkei/nikkei-225-yen.html",
        # Electronic session (UTC) — identical to ES
        "session_open_utc":  "22:00:00",
        "session_close_utc": "21:00:00",
        "maintenance_start": "21:00:00",
        "maintenance_end":   "22:00:00",
        # RTH: 11:00–15:15 JST = 02:00–06:15 UTC
        "rth_mode":          "fixed",
        "rth_start_utc":     "02:00:00",   # 11:00 JST — after post-open stabilization
        "rth_end_utc":       "06:15:00",   # 15:15 JST
        "needs_warmup":      True,
        "crossed_threshold": 150_000,
        # Timezone for display
        "local_tz":          "Asia/Tokyo",
    },
    "NKD": {
        # Nikkei 225 Future (USD-denominated) — CME Globex
        # Source: https://www.cmegroup.com/markets/equities/nikkei/nikkei-225-dollar.html
        # Dataset: GLBX.MDP3 (Databento)
        # Same Globex session and RTH as NIY — same underlying, different currency
        # See NIY comments for RTH rationale (11:00–15:15 JST = 02:00–06:15 UTC)
        "tick_size_fp":      5_000_000_000,      # 5 pts * 1e9
        "price_floor_fp":    5_000_000_000_000,  # 5000 pts * 1e9
        "point_value":       5.0,                # $5 per index point
        "currency":          "USD",
        "exchange":          "CME",
        "databento_dataset": "GLBX.MDP3",
        "description":       "Nikkei 225 Future USD (CME Globex)",
        "product_url":       "https://www.cmegroup.com/markets/equities/nikkei/nikkei-225-dollar.html",
        # Electronic session (UTC) — identical to ES
        "session_open_utc":  "22:00:00",
        "session_close_utc": "21:00:00",
        "maintenance_start": "21:00:00",
        "maintenance_end":   "22:00:00",
        # RTH: 11:00–15:15 JST = 02:00–06:15 UTC
        "rth_mode":          "fixed",
        "rth_start_utc":     "02:00:00",   # 11:00 JST
        "rth_end_utc":       "06:15:00",   # 15:15 JST
        "needs_warmup":      True,
        # NKD is structurally thin (~500 trades/day). The book never fully
        # stabilizes after Tokyo open — crossed books throughout the session
        # are a known market property, not a reconstruction bug.
        "crossed_threshold": 10_000,
        # Timezone for display
        "local_tz":          "Asia/Tokyo",
    },
}

# Products currently supported by the CME pipeline
AVAILABLE_PRODUCTS: list[str] = list(PRODUCT_CONFIG.keys())


# ── DuckDB query helpers ───────────────────────────────────────────────────────

def ts_utc_expr(col: str = "ts_recv") -> str:
    """Return a DuckDB expression casting a raw UBIGINT nanosecond timestamp
    to a TIMESTAMPTZ in UTC.

    CME timestamps are raw uint64 nanoseconds since Unix epoch (UTC).
    The cast chain: UBIGINT → BIGINT (signed) → divide by 1e9 → TIMESTAMPTZ.

    Args:
        col: column name (default 'ts_recv'; use 'ts_event' for exchange clock)

    Returns:
        DuckDB SQL expression string, e.g.:
        "to_timestamp(CAST(ts_recv AS BIGINT) / 1e9)"
    """
    return f"to_timestamp(CAST({col} AS BIGINT) / 1e9)"


def ts_local_expr(col: str = "ts_recv", tz: str = "America/New_York") -> str:
    """Return a DuckDB expression casting a raw UBIGINT nanosecond timestamp
    to a TIMESTAMPTZ in the product's local timezone.

    Args:
        col: column name (default 'ts_recv')
        tz:  IANA timezone string (default 'America/New_York' for ES/CME)

    Returns:
        DuckDB SQL expression string applying timezone conversion.
    """
    utc_expr = ts_utc_expr(col)
    return f"timezone('{tz}', {utc_expr})"


def front_month_subquery(parquet_path: str, date_str: str) -> str:
    """Return a DuckDB scalar subquery that resolves the front-month symbol
    for a given day's parquet file.

    The front month is defined as the symbol with the highest event count
    among non-snapshot, non-sentinel-price rows. A guard assertion is applied
    in the calling query via a CASE expression to catch pathological cases
    where the dominant symbol is a calendar spread (contains '-').

    This mirrors the MODE(symbol) approach used in clean_eurex_orders.py,
    adapted for the CME snapshot flag filter.

    Args:
        parquet_path: path to the orders parquet file for this day
        date_str:     date label for error messages (YYYY-MM-DD)

    Returns:
        SQL scalar subquery string (suitable for embedding in WHERE clause).

    Example:
        WHERE symbol = {front_month_subquery(path, '2025-10-01')}
    """
    return f"""(
        SELECT symbol
        FROM (
            SELECT
                symbol,
                COUNT(*) AS n
            FROM '{parquet_path}'
            WHERE flags & {F_SNAPSHOT} = 0   -- exclude recovery snapshot
              AND price != {INT64_MAX}        -- exclude sentinel market orders
              AND symbol NOT LIKE '%-%%'      -- pre-filter: outright only
            GROUP BY symbol
            ORDER BY n DESC
            LIMIT 1
        )
    )"""


def session_where_clause(
    cfg: dict,
    use_rth: bool,
    target_date: Optional[date] = None,
    ts_col: str = "ts_recv",
) -> str:
    """Return a DuckDB WHERE clause fragment for session filtering.

    Two modes:
      use_rth=True:  Regular Trading Hours only — product-aware via rth_utc_bounds()
      use_rth=False: Full electronic session (22:00 UTC prev → 21:00 UTC today)
                     Excludes maintenance window (21:00–22:00 UTC)

    For use_rth=True, target_date is required for DST-aware products (ES).
    For fixed-RTH products (NIY/NKD), target_date is accepted but not used for DST.

    The filter is applied on UTC timestamps (no per-row timezone conversion)
    for maximum DuckDB performance.

    Args:
        cfg:         product config dict from PRODUCT_CONFIG
        use_rth:     True for RTH only, False for full electronic session
        target_date: calendar date of the trading session (required for RTH on DST products)
        ts_col:      timestamp column name (default 'ts_recv')

    Returns:
        SQL WHERE fragment string (no leading AND/WHERE keyword).

    Raises:
        ValueError: if use_rth=True on a DST product and target_date is None
    """
    utc_expr = ts_utc_expr(ts_col)
    # DuckDB cannot cast TIMESTAMPTZ directly to TIME.
    # Cast chain: UBIGINT → TIMESTAMPTZ (via to_timestamp) → TIMESTAMP (strip TZ) → TIME.
    time_cast = f"CAST(CAST({utc_expr} AS TIMESTAMP) AS TIME)"

    if use_rth:
        # Product-aware RTH bounds
        if target_date is None and cfg.get("rth_mode") == "us_eastern":
            raise ValueError(
                "target_date required for RTH session filtering on DST-aware products"
            )
        # For fixed-RTH products, target_date is ignored internally by rth_utc_bounds
        # but we still pass it for interface consistency
        rth_start, rth_end = rth_utc_bounds(
            target_date or date(2025, 1, 1),  # dummy for fixed-mode products
            cfg,
        )
        return (
            f"{time_cast} >= TIME '{rth_start}' AND "
            f"{time_cast} <  TIME '{rth_end}'"
        )
    else:
        # Full electronic session: exclude only the maintenance window
        maint_start = cfg["maintenance_start"]
        maint_end   = cfg["maintenance_end"]
        return (
            f"NOT ({time_cast} >= TIME '{maint_start}' AND "
            f"     {time_cast} <  TIME '{maint_end}')"
        )


def snapshot_exclude_clause(flags_col: str = "flags") -> str:
    """Return a DuckDB WHERE fragment that excludes F_SNAPSHOT messages.

    CME sends a recovery snapshot block at session open. These messages
    reconstruct the book state but are NOT live market events — they must
    be excluded from the clean event stream. The LOB state machine
    (build_cme_lob1.py) will handle the snapshot separately for book init.

    Args:
        flags_col: flags column name (default 'flags')

    Returns:
        SQL fragment: "flags & 32 = 0"
    """
    return f"{flags_col} & {F_SNAPSHOT} = 0"


def sentinel_exclude_clause(price_col: str = "price") -> str:
    """Return a DuckDB WHERE fragment that excludes sentinel price rows.

    Sentinel = INT64_MAX, used by CME/Databento for market orders and
    stop orders without a limit price. These cannot be placed in the LOB.

    Args:
        price_col: price column name (default 'price')

    Returns:
        SQL fragment: "price != 9223372036854775807"
    """
    return f"{price_col} != {INT64_MAX}"


# ── Convenience: full standard filter for live outright orders ─────────────────

def live_outright_filter(
    cfg: dict,
    parquet_path: str,
    date_str: str,
    use_rth: bool,
    target_date: Optional[date] = None,
    ts_col: str = "ts_recv",
    flags_col: str = "flags",
    price_col: str = "price",
    symbol_col: str = "symbol",
) -> str:
    """Return the full WHERE clause for clean live outright orders.

    Combines all standard filters:
      1. Exclude F_SNAPSHOT recovery block
      2. Exclude sentinel prices (market/stop orders)
      3. Front-month symbol filter (excludes spreads + back months)
      4. Session filter (RTH or full electronic)

    Args:
        cfg:          product config dict from PRODUCT_CONFIG
        parquet_path: path to orders parquet file
        date_str:     date label 'YYYY-MM-DD' for front_month_subquery
        use_rth:      True for RTH, False for full electronic session
        target_date:  required if use_rth=True on DST products
        ts_col:       timestamp column
        flags_col:    flags column
        price_col:    price column
        symbol_col:   symbol column

    Returns:
        Complete WHERE clause string (no leading WHERE keyword).
    """
    parts = [
        snapshot_exclude_clause(flags_col),
        sentinel_exclude_clause(price_col),
        f"{symbol_col} = {front_month_subquery(parquet_path, date_str)}",
        session_where_clause(cfg, use_rth, target_date, ts_col),
    ]
    return "\n  AND ".join(parts)


# ── Utility: print config summary ─────────────────────────────────────────────

def print_config(product: str) -> None:
    """Print a human-readable summary of the product configuration."""
    if product not in PRODUCT_CONFIG:
        raise ValueError(f"Unknown product '{product}'. Available: {AVAILABLE_PRODUCTS}")
    cfg = PRODUCT_CONFIG[product]
    tick_pts = cfg["tick_size_fp"] / 1e9
    floor_pts = cfg["price_floor_fp"] / 1e9

    print(f"{'─' * 50}")
    print(f"Product:          {product}")
    print(f"Exchange:         {cfg['exchange']} ({cfg['databento_dataset']})")
    print(f"Description:      {cfg['description']}")
    print(f"Currency:         {cfg['currency']}")
    print(f"Tick size:        {tick_pts:.4f} pts  ({cfg['tick_size_fp']} fp)")
    print(f"Price floor:      {floor_pts:.0f} pts")
    print(f"Point value:      {cfg['point_value']:.0f} {cfg['currency']}/pt")
    print(f"Session (UTC):    {cfg['session_open_utc']} (prev day) → {cfg['session_close_utc']}")
    print(f"Maintenance:      {cfg['maintenance_start']}–{cfg['maintenance_end']} UTC daily")

    rth_mode = cfg.get("rth_mode", "us_eastern")
    if rth_mode == "fixed":
        print(f"RTH (UTC):        {cfg['rth_start_utc']}–{cfg['rth_end_utc']}  (fixed, no DST)")
    elif rth_mode == "us_eastern":
        print(f"RTH (EDT/UTC-4):  {cfg['rth_start_utc_edt']}–{cfg['rth_end_utc_edt']} UTC")
        print(f"RTH (EST/UTC-5):  {cfg['rth_start_utc_est']}–{cfg['rth_end_utc_est']} UTC")

    print(f"Local TZ:         {cfg['local_tz']}")
    print(f"URL:              {cfg['product_url']}")
    print(f"{'─' * 50}")


if __name__ == "__main__":
    # Print config for all products
    for prod in AVAILABLE_PRODUCTS:
        print_config(prod)
        print()

    # Test DST helper for ES
    test_dates = [
        date(2025, 6, 15),   # EDT
        date(2025, 11, 15),  # EST
        date(2025, 10, 1),   # EDT (our exploration file)
    ]
    print("ES DST test:")
    for d in test_dates:
        edt = _is_edt(d)
        start, end = rth_utc_bounds(d, PRODUCT_CONFIG["ES"])
        tz_label = "EDT (UTC-4)" if edt else "EST (UTC-5)"
        print(f"  {d}  {tz_label}  RTH UTC: {start}–{end}")

    # Test NIY fixed RTH
    print("\nNIY fixed RTH test:")
    for d in test_dates:
        start, end = rth_utc_bounds(d, PRODUCT_CONFIG["NIY"])
        print(f"  {d}  JST (UTC+9)  RTH UTC: {start}–{end}")