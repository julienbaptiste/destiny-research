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
  - RTH timezone: US/Eastern (EDT in summer, EST in winter — DST applies)
  - RTH UTC offsets: EDT = UTC-4 (Mar–Nov), EST = UTC-5 (Nov–Mar)
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
# CME RTH shifts from 13:30–20:00 UTC (EDT) to 14:30–21:00 UTC (EST)
# Clocks fall back: first Sunday of November each year
# Clocks spring forward: second Sunday of March each year
# These are the transition dates relevant to our data windows.
# Add new years as needed when extending the dataset.
_US_DST_TRANSITIONS: dict[int, tuple[date, date]] = {
    # year: (spring_forward, fall_back)
    # spring_forward: EDT starts (UTC offset becomes -4)
    # fall_back: EST starts (UTC offset becomes -5)
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


def rth_utc_bounds(d: date) -> tuple[str, str]:
    """Return RTH start/end as UTC time strings for a given date.

    RTH = 09:30–16:00 ET
      EDT (UTC-4): 13:30–20:00 UTC
      EST (UTC-5): 14:30–21:00 UTC

    Returns:
        (rth_start_utc, rth_end_utc) as 'HH:MM:SS' strings.
    """
    if _is_edt(d):
        return ("13:30:00", "20:00:00")
    else:
        return ("14:30:00", "21:00:00")


# ── Product configuration ──────────────────────────────────────────────────────
# tick_size_fp : tick size in Databento fixed-point (price * 1e-9 = points)
#                ES tick = 0.25 pt → 0.25 * 1e9 = 250_000_000
# price_floor_fp: minimum plausible outright price in fixed-point.
#                 Used as a sanity check — not a hard filter (spreads are
#                 excluded via symbol filter before this would apply).
#                 ES: index has never traded below 500 pts in modern history.
# point_value:  USD value per index point (for P&L / notional calculations)
# session_open_utc / session_close_utc:
#                Electronic session bounds in UTC.
#                CME Globex: open 22:00 UTC (prev day) → close 21:00 UTC (same day)
#                Maintenance window: 21:00–22:00 UTC daily (no data expected)
# rth_start_utc / rth_end_utc:
#                Regular Trading Hours in UTC — DST-dependent.
#                These are the EDT values; call rth_utc_bounds(date) for
#                the correct value per day. Stored here as EDT defaults
#                for use in static contexts (e.g. config display).

PRODUCT_CONFIG: dict[str, dict] = {
    "ES": {
        # E-mini S&P 500 Future — CME Globex
        # Dataset: GLBX.MDP3 (Databento)
        "tick_size_fp":      250_000_000,        # 0.25 pt * 1e9
        "price_floor_fp":    500_000_000_000,    # 500 pts * 1e9 (hard floor for outrights)
        "point_value_usd":   50.0,               # $50 per index point
        "exchange":          "CME",
        "databento_dataset": "GLBX.MDP3",
        "description":       "E-mini S&P 500 Future (CME Globex)",
        "eurex_url":         "https://www.cmegroup.com/markets/equities/sp/e-mini-sandp500.html",
        # Electronic session (UTC) — these are the target day's bounds
        # Open = 22:00 UTC of the *previous* calendar day
        # Close = 21:00 UTC of the *target* calendar day
        # The DBN file labeled YYYYMMDD contains 00:00–23:59 UTC of that day,
        # so it always contains a partial session start (from 00:00 UTC)
        # and a partial session end (up to 21:00 UTC), plus the next session
        # open (22:00–23:59 UTC). Use session_where_clause() to filter correctly.
        "session_open_utc":  "22:00:00",   # prev day 22:00 UTC → today
        "session_close_utc": "21:00:00",   # today 21:00 UTC
        "maintenance_start": "21:00:00",   # daily CME maintenance window start
        "maintenance_end":   "22:00:00",   # daily CME maintenance window end
        # RTH defaults (EDT = UTC-4). Use rth_utc_bounds(date) for DST-correct values.
        "rth_start_utc_edt": "13:30:00",   # 09:30 ET in EDT
        "rth_end_utc_edt":   "20:00:00",   # 16:00 ET in EDT
        "rth_start_utc_est": "14:30:00",   # 09:30 ET in EST
        "rth_end_utc_est":   "21:00:00",   # 16:00 ET in EST
        # Timezone label for local-time display in notebooks/reports
        "local_tz":          "America/New_York",
        "local_tz_abbr_edt": "EDT",
        "local_tz_abbr_est": "EST",
    },
    # ── Nikkei futures (CME Globex) — OSE-listed, same MDP3 feed ──────────────
    # NIY and NKD share the same CME Globex infrastructure as ES.
    # Session hours differ: Nikkei futures follow Tokyo session (OSE),
    # but the CME-listed contracts trade on Globex with different liquidity windows.
    # TODO: validate NIY/NKD session bounds from data before activating.
    # "NIY": { ... },
    # "NKD": { ... },
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
      use_rth=True:  Regular Trading Hours only (09:30–16:00 ET, DST-aware)
      use_rth=False: Full electronic session (22:00 UTC prev → 21:00 UTC today)
                     Excludes maintenance window (21:00–22:00 UTC)

    For use_rth=True, target_date must be provided to determine EDT vs EST.

    The filter is applied on UTC timestamps (no per-row timezone conversion)
    for maximum DuckDB performance.

    Args:
        cfg:         product config dict from PRODUCT_CONFIG
        use_rth:     True for RTH only, False for full electronic session
        target_date: calendar date of the trading session (required for RTH)
        ts_col:      timestamp column name (default 'ts_recv')

    Returns:
        SQL WHERE fragment string (no leading AND/WHERE keyword).

    Raises:
        ValueError: if use_rth=True and target_date is None
        ValueError: if target_date year is not in _US_DST_TRANSITIONS
    """
    utc_expr = ts_utc_expr(ts_col)
    # DuckDB cannot cast TIMESTAMPTZ directly to TIME.
    # Cast chain: UBIGINT → TIMESTAMPTZ (via to_timestamp) → TIMESTAMP (strip TZ) → TIME.
    # The intermediate TIMESTAMP cast strips the timezone info so the time component
    # reflects UTC wall-clock time, which is what we want for UTC-based session bounds.
    time_cast = f"CAST(CAST({utc_expr} AS TIMESTAMP) AS TIME)"

    if use_rth:
        if target_date is None:
            raise ValueError("target_date required for RTH session filtering (DST-aware)")
        rth_start, rth_end = rth_utc_bounds(target_date)
        return (
            f"{time_cast} >= TIME '{rth_start}' AND "
            f"{time_cast} <  TIME '{rth_end}'"
        )
    else:
        # Full electronic session: exclude only the maintenance window
        # Maintenance: 21:00–22:00 UTC daily
        maint_start = cfg["maintenance_start"]
        maint_end   = cfg["maintenance_end"]
        # Keep everything EXCEPT the maintenance window
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
        target_date:  required if use_rth=True
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
    print(f"Tick size:        {tick_pts:.4f} pts  ({cfg['tick_size_fp']} fp)")
    print(f"Price floor:      {floor_pts:.0f} pts")
    print(f"Point value:      ${cfg['point_value_usd']:.0f}/pt")
    print(f"Session (UTC):    {cfg['session_open_utc']} (prev day) → {cfg['session_close_utc']}")
    print(f"Maintenance:      {cfg['maintenance_start']}–{cfg['maintenance_end']} UTC daily")
    print(f"RTH (EDT/UTC-4):  {cfg['rth_start_utc_edt']}–{cfg['rth_end_utc_edt']} UTC")
    print(f"RTH (EST/UTC-5):  {cfg['rth_start_utc_est']}–{cfg['rth_end_utc_est']} UTC")
    print(f"Local TZ:         {cfg['local_tz']}")
    print(f"{'─' * 50}")


if __name__ == "__main__":
    # Quick sanity check
    print_config("ES")

    # Test DST helper
    test_dates = [
        date(2025, 6, 15),   # EDT
        date(2025, 11, 15),  # EST
        date(2025, 10, 1),   # EDT (our exploration file)
    ]
    for d in test_dates:
        edt = _is_edt(d)
        start, end = rth_utc_bounds(d)
        tz_label = "EDT (UTC-4)" if edt else "EST (UTC-5)"
        print(f"{d}  {tz_label}  RTH UTC: {start}–{end}")