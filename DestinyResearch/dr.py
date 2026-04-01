"""
utils/dr.py — DestinyResearch data access layer
================================================
Unified entry point for notebook exploration. Import as:

    import utils.dr as dr

    # Load MBO data for front-month ES on 2025-10-01
    tbl = dr.get_mbo_front("ES", "2025-10-01")

    # Convert to pandas for manipulation
    df = dr.to_df(tbl)

    # Load specific contract MBP-1 with column selection
    tbl = dr.get_mbp1("ESZ25", "2025-10-01", columns=["ts_recv", "bid_px_00", "ask_px_00"])

Design principles
-----------------
- All get_* functions return pyarrow.Table by default (zero-copy, columnar,
  memory-efficient). Call to_df() explicitly when pandas is needed.
- PyArrow read_table() DOES load data into RAM — use the `columns` parameter
  to avoid reading unnecessary columns from disk (Parquet column pruning is
  done at the file level, so unselected columns are never read).
- For single-day queries: pyarrow.parquet.read_table() — simple and fast.
- For multi-day range queries: DuckDB with glob pattern — pushes predicates
  down to Parquet row groups, avoids loading files day-by-day in Python.
- Front-month detection: DuckDB COUNT on action='T' across all contracts for
  a given day — one SQL pass, no full data load.

Dependencies
------------
    pyarrow >= 10.0
    duckdb >= 0.9
    (pandas — optional, only needed for to_df())
"""

from __future__ import annotations

import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Bootstrap: make sure the repo root is on sys.path so that config.py and
# ingestion/market_config.py are importable from anywhere (notebook, CLI...).
# utils/dr.py lives one level below the repo root.
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from config import DATA_NORMALIZED, DATA_RECONSTRUCTED  # noqa: E402
from ingestion.market_config import MARKET_CONFIG, AVAILABLE_PRODUCTS  # noqa: E402


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _date_str(date_input: str | date) -> str:
    """
    Normalise a date to 'YYYYMMDD' string (no dashes).
    Accepts '2025-10-01', '20251001', or datetime.date.
    """
    if isinstance(date_input, date):
        return date_input.strftime("%Y%m%d")
    # Strip dashes — handles both '2025-10-01' and '20251001'
    return date_input.replace("-", "")


def _date_obj(date_input: str | date) -> date:
    """Convert any supported date format to datetime.date."""
    if isinstance(date_input, date):
        return date_input
    s = date_input.replace("-", "")
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def _product_from_config(product: str) -> dict:
    """Return market config dict for a product, with a clear error."""
    if product not in MARKET_CONFIG:
        raise ValueError(
            f"Unknown product '{product}'. "
            f"Available: {AVAILABLE_PRODUCTS}"
        )
    return MARKET_CONFIG[product]


def _norm_dir(product: str) -> Path:
    """
    Return the DATA_NORMALIZED root directory for a product.
    Structure: provider=<P>/venue=<V>/product=<PR>/
    """
    cfg = _product_from_config(product)
    return (
        DATA_NORMALIZED
        / f"provider={cfg['provider']}"
        / f"venue={cfg['exchange']}"
        / f"product={product}"
    )


def _recon_dir(product: str) -> Path:
    """
    Return the DATA_RECONSTRUCTED root directory for a product.
    Structure: provider=<P>/venue=<V>/product=<PR>/
    """
    cfg = _product_from_config(product)
    return (
        DATA_RECONSTRUCTED
        / f"provider={cfg['provider']}"
        / f"venue={cfg['exchange']}"
        / f"product={product}"
    )


def _contract_norm_path(contract: str, date_str: str) -> Path:
    """
    Build the full path to a normalized MBO parquet file for a given contract
    and date. Raises FileNotFoundError if the file does not exist.

    Path pattern:
        DATA_NORMALIZED/provider=.../venue=.../product=.../
            contract=<C>/year=<Y>/month=<MM>/<C>_<YYYYMMDD>_mbo.parquet
    """
    # Infer product from contract ticker prefix (everything before the month code)
    # e.g. "ESZ25" → "ES", "FDAXM25" → "FDAX", "HSIG26" → "HSI"
    product = _product_from_contract(contract)
    ds = _date_str(date_str)
    d  = _date_obj(date_str)
    path = (
        _norm_dir(product)
        / f"contract={contract}"
        / f"year={d.year}"
        / f"month={d.month:02d}"
        / f"{contract}_{ds}_mbo.parquet"
    )
    if not path.exists():
        raise FileNotFoundError(
            f"Normalized MBO file not found: {path}\n"
            f"Tip: run ingestion first, or check available dates with "
            f"get_available_dates('{product}', '{contract}')."
        )
    return path


def _contract_recon_path(contract: str, date_str: str) -> Path:
    """
    Build the full path to a reconstructed MBP-1 parquet file.
    Raises FileNotFoundError if the file does not exist.
    """
    product = _product_from_contract(contract)
    ds = _date_str(date_str)
    d  = _date_obj(date_str)
    path = (
        _recon_dir(product)
        / f"contract={contract}"
        / f"year={d.year}"
        / f"month={d.month:02d}"
        / f"{contract}_{ds}_mbp1.parquet"
    )
    if not path.exists():
        raise FileNotFoundError(
            f"Reconstructed MBP-1 file not found: {path}\n"
            f"Tip: run build_mbp1 first, or check available dates with "
            f"get_available_dates('{product}', '{contract}')."
        )
    return path


def _product_from_contract(contract: str) -> str:
    """
    Infer the product ticker from a contract symbol by matching known products.

    Strategy: try longest-prefix match against AVAILABLE_PRODUCTS.
    Example: "FDAXM25" → "FDAX", "ESZ25" → "ES", "HSIG26" → "HSI"

    Raises ValueError if no product matches.
    """
    # Sort by length descending so "FDAX" matches before "F" hypothetically
    for prod in sorted(AVAILABLE_PRODUCTS, key=len, reverse=True):
        if contract.upper().startswith(prod):
            return prod
    raise ValueError(
        f"Cannot infer product from contract '{contract}'. "
        f"Known products: {AVAILABLE_PRODUCTS}"
    )


# ---------------------------------------------------------------------------
# Discovery functions
# ---------------------------------------------------------------------------

def get_available_products() -> list[str]:
    """
    Return the list of all products registered in market_config.py.

    Example
    -------
    >>> dr.get_available_products()
    ['ES', 'FDAX', 'FESX', 'FSMI', 'HHI', 'HSI', 'MCH', 'MHI', 'NKD', 'NIY']
    """
    return list(AVAILABLE_PRODUCTS)


def get_available_dates(product: str, contract: Optional[str] = None) -> list[str]:
    """
    Return all dates (as 'YYYY-MM-DD' strings) for which normalized MBO data
    exists on disk for the given product (and optionally a specific contract).

    Scans the Hive-partitioned directory tree — no data is read.

    Parameters
    ----------
    product  : Product ticker, e.g. 'ES', 'HSI'.
    contract : Optional contract symbol, e.g. 'ESZ25'. If None, returns dates
               across all contracts (union).

    Example
    -------
    >>> dr.get_available_dates("ES")
    ['2025-10-01', '2025-10-02', ...]
    >>> dr.get_available_dates("ES", "ESZ25")
    ['2025-10-01', ...]
    """
    base = _norm_dir(product)
    if not base.exists():
        return []

    # Build glob pattern depending on whether a contract is specified
    if contract:
        # Restrict to the specific contract partition
        pattern = f"contract={contract}/year=*/month=*/{contract}_*_mbo.parquet"
    else:
        pattern = "contract=*/year=*/month=*/*_mbo.parquet"

    dates: set[str] = set()
    for p in base.glob(pattern):
        # Extract 8-digit date from filename stem, e.g. "ESZ25_20251001_mbo"
        m = re.search(r"(\d{8})", p.stem)
        if m:
            ds = m.group(1)
            # Reformat to 'YYYY-MM-DD' for readability
            dates.add(f"{ds[:4]}-{ds[4:6]}-{ds[6:8]}")

    return sorted(dates)


def get_contracts(product: str, date_str: str) -> list[str]:
    """
    Return all contract symbols that have normalized MBO data for a given
    product and date. Sorted alphabetically.

    No data is read — discovery is based on directory/file existence only.

    Parameters
    ----------
    product  : Product ticker, e.g. 'FDAX'.
    date_str : Date in 'YYYY-MM-DD' or 'YYYYMMDD' format.

    Example
    -------
    >>> dr.get_contracts("FDAX", "2025-05-14")
    ['FDAXM25', 'FDAX_CAL_M25U25']
    """
    base = _norm_dir(product)
    if not base.exists():
        return []

    ds = _date_str(date_str)
    contracts: list[str] = []
    # Each contract is a subdirectory: contract=<symbol>/
    for contract_dir in base.glob("contract=*"):
        contract = contract_dir.name.replace("contract=", "")
        d = _date_obj(date_str)
        candidate = (
            contract_dir
            / f"year={d.year}"
            / f"month={d.month:02d}"
            / f"{contract}_{ds}_mbo.parquet"
        )
        if candidate.exists():
            contracts.append(contract)

    return sorted(contracts)


def get_front_contract(
    product: str,
    date_str: str,
    method: str = "volume",
) -> str:
    """
    Identify the front-month contract for a product on a given date.

    Parameters
    ----------
    product  : Product ticker, e.g. 'ES'.
    date_str : Date in 'YYYY-MM-DD' or 'YYYYMMDD' format.
    method   : 'volume' (default) — contract with most trades (action='T').
               'expiry' — contract with nearest expiry after date_str.
               Falls back to 'expiry' if 'volume' finds no trades.

    Returns
    -------
    Contract symbol string, e.g. 'ESZ25'.

    Raises
    ------
    ValueError if no contracts are found for the product/date.

    Notes on 'volume' method
    ------------------------
    Uses DuckDB to COUNT action='T' rows across all contract parquet files for
    the given date. DuckDB reads Parquet row-group statistics first (min/max
    on the action column) before scanning — this is much faster than loading
    data into Python. Typical runtime: <1s for most products, ~2-3s for HSI.

    Notes on 'expiry' method
    ------------------------
    Parses the contract ticker symbol to extract expiry month/year. No disk
    read required. Less robust for spread contracts (_CAL_) — those are always
    excluded from front-month detection.
    """
    contracts = get_contracts(product, date_str)
    # Exclude calendar spreads — front month is always an outright
    outrights = [c for c in contracts if "_CAL_" not in c]

    if not outrights:
        raise ValueError(
            f"No outright contracts found for product='{product}' "
            f"on date='{date_str}'. Available: {contracts}"
        )

    if len(outrights) == 1:
        # No ambiguity — return directly without any disk access
        return outrights[0]

    if method == "volume":
        return _front_by_volume(product, date_str, outrights)
    elif method == "expiry":
        return _front_by_expiry(outrights, date_str)
    else:
        raise ValueError(f"Unknown method '{method}'. Use 'volume' or 'expiry'.")


def futures_month_codes() -> "pd.DataFrame":
    """
    Return a reference table of CME/HKEX futures month letter codes.

    Standard month codes used across all products in our universe
    (CME, Eurex, HKEX all use the same convention).

    Returns
    -------
    pandas.DataFrame with columns:
        month_num  : int (1–12)
        month_name : str (e.g. 'January')
        code       : str (e.g. 'F')

    Example
    -------
    >>> dr.futures_month_codes()
    >>> # Find the code for March
    >>> dr.futures_month_codes().query("month_name == 'March'")
    >>> # Find which month 'U' corresponds to
    >>> dr.futures_month_codes().query("code == 'U'")
    """
    import pandas as pd
    return pd.DataFrame([
        {"month_num": 1,  "month_name": "January",   "code": "F"},
        {"month_num": 2,  "month_name": "February",  "code": "G"},
        {"month_num": 3,  "month_name": "March",     "code": "H"},
        {"month_num": 4,  "month_name": "April",     "code": "J"},
        {"month_num": 5,  "month_name": "May",       "code": "K"},
        {"month_num": 6,  "month_name": "June",      "code": "M"},
        {"month_num": 7,  "month_name": "July",      "code": "N"},
        {"month_num": 8,  "month_name": "August",    "code": "Q"},
        {"month_num": 9,  "month_name": "September", "code": "U"},
        {"month_num": 10, "month_name": "October",   "code": "V"},
        {"month_num": 11, "month_name": "November",  "code": "X"},
        {"month_num": 12, "month_name": "December",  "code": "Z"},
    ])


def _front_by_volume(product: str, date_str: str, contracts: list[str]) -> str:
    """
    Return the contract with the highest number of trades (action='T') on
    the given date, using DuckDB for efficient Parquet scanning.

    DuckDB query plan
    -----------------
    1. Build a UNION ALL of all contract files for this date.
    2. Filter WHERE action = 'T' (string dictionary column).
    3. GROUP BY a literal contract label, ORDER BY COUNT DESC, LIMIT 1.

    DuckDB uses Parquet column statistics (min/max per row group) to skip
    row groups where action cannot be 'T' — this avoids a full scan when
    trades are sparse. On HSI (~10M rows, 125K trades), this takes ~1-2s.
    """
    d   = _date_obj(date_str)
    ds  = _date_str(date_str)
    cfg = _product_from_config(product)

    # Build one SELECT per contract, UNION ALL them, then aggregate outside
    # We label each file with a literal contract string for GROUP BY.
    parts = []
    for c in contracts:
        path = (
            DATA_NORMALIZED
            / f"provider={cfg['provider']}"
            / f"venue={cfg['exchange']}"
            / f"product={product}"
            / f"contract={c}"
            / f"year={d.year}"
            / f"month={d.month:02d}"
            / f"{c}_{ds}_mbo.parquet"
        )
        
        if path.exists():
            # Use single quotes inside the SQL — escape any apostrophes in path
            path_str = str(path).replace("'", "''")
            # Select only the action column + a literal contract label
            # (avoids reading all other columns from disk)
            parts.append(
                f"SELECT '{c}' AS contract, action "
                f"FROM read_parquet('{path_str}')"
            )

    if not parts:
        # Fallback if no files found
        return _front_by_expiry(contracts, date_str)

    # UNION ALL → filter trades → count per contract → take top 1
    union_sql = " UNION ALL ".join(parts)
    sql = (
        f"SELECT contract, COUNT(*) AS n_trades "
        f"FROM ({union_sql}) "
        f"WHERE action = 'TRADE' "
        f"GROUP BY contract "
        f"ORDER BY n_trades DESC "
        f"LIMIT 1"
    )

    # Use an in-memory DuckDB connection — no state, safe in notebook context
    con = duckdb.connect()
    result = con.execute(sql).fetchone()
    con.close()

    if result is None:
        # No trades found at all — fall back to expiry-based detection
        return _front_by_expiry(contracts, date_str)

    return result[0]  # contract symbol string


# CME month codes → numeric month (same convention as HKEX)
_MONTH_CODE: dict[str, int] = {
    "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
    "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12,
}


def _front_by_expiry(contracts: list[str], date_str: str) -> str:
    """
    Return the contract with the nearest expiry date after date_str.
    Parses the trailing month-code + 2-digit year from the contract symbol.

    Pattern matched: <PRODUCT><MONTH_CODE><2-digit-year>
    Example: "FDAXM25" → expiry month M=June, year=2025 → date(2025, 6, 1)

    Contracts that don't match the pattern are excluded. If nothing matches,
    returns the first contract alphabetically.
    """
    ref = _date_obj(date_str)
    # Pattern: letters (product), one letter (month code), 2 digits (year)
    pat = re.compile(r"^[A-Z]+([FGHJKMNQUVXZ])(\d{2})$")

    scored: list[tuple[date, str]] = []
    for c in contracts:
        m = pat.match(c)
        if not m:
            continue
        month_num = _MONTH_CODE.get(m.group(1))
        if month_num is None:
            continue
        year_2d = int(m.group(2))
        # Disambiguate decade: assume within ±5 years of reference date
        decade_base = (ref.year // 10) * 10
        year_4d = decade_base + year_2d
        if year_4d < ref.year - 1:
            year_4d += 10
        # Use day=1 as proxy — exact expiry day doesn't matter for ordering
        expiry = date(year_4d, month_num, 1)
        if expiry >= date(ref.year, ref.month, ref.day):
            scored.append((expiry, c))

    if not scored:
        return contracts[0]

    # Front month = nearest expiry that is >= current month
    scored.sort(key=lambda x: x[0])
    return scored[0][1]


# ---------------------------------------------------------------------------
# Core data loading — single day
# ---------------------------------------------------------------------------

def get_mbo(
    contract: str,
    date_str: str,
    columns: Optional[list[str]] = None,
) -> pa.Table:
    """
    Load the normalized MBO event stream for a specific contract and date.

    Returns a pyarrow.Table. Data IS loaded into RAM — use `columns` to
    restrict which columns are read from disk (Parquet column pruning).

    Parameters
    ----------
    contract : Contract symbol, e.g. 'ESZ25', 'HSIG26'.
    date_str : Date in 'YYYY-MM-DD' or 'YYYYMMDD' format.
    columns  : Optional list of column names to load. None = all columns.
               Example: ['ts_recv', 'action', 'side', 'price', 'size']

    Returns
    -------
    pyarrow.Table with the full (or column-pruned) MBO schema.

    Notes on memory
    ---------------
    PyArrow stores data in columnar format in RAM. Each column is a
    contiguous buffer — much more cache-friendly than pandas row-wise storage.
    Memory usage ≈ sum(sizeof(dtype) × n_rows) for selected columns only.
    For HSI (~10M rows), loading all columns ≈ 800 MB. With columns=['ts_recv',
    'action', 'price', 'size'], it drops to ~80 MB.

    Example
    -------
    >>> tbl = dr.get_mbo("ESZ25", "2025-10-01")
    >>> tbl.schema
    >>> tbl = dr.get_mbo("HSIG26", "2026-02-03", columns=["ts_recv", "action", "side", "price", "size"])
    """
    path = _contract_norm_path(contract, date_str)
    # read_table performs Parquet column pruning when `columns` is specified:
    # only the selected column chunks are decompressed and loaded into memory.
    return pq.ParquetFile(path).read(columns=columns)


def get_mbo_front(
    product: str,
    date_str: str,
    columns: Optional[list[str]] = None,
    method: str = "volume",
) -> pa.Table:
    """
    Load the MBO event stream for the front-month contract of a product on a
    given date. Front month is auto-detected (see get_front_contract).

    Parameters
    ----------
    product  : Product ticker, e.g. 'ES', 'HSI'.
    date_str : Date in 'YYYY-MM-DD' or 'YYYYMMDD' format.
    columns  : Optional column selection (see get_mbo).
    method   : Front-month detection method: 'volume' (default) or 'expiry'.

    Returns
    -------
    pyarrow.Table — same schema as get_mbo().

    Example
    -------
    >>> tbl = dr.get_mbo_front("ES", "2025-10-01")
    >>> tbl = dr.get_mbo_front("HSI", "2026-02-03", columns=["ts_recv", "action", "price"])
    """
    contract = get_front_contract(product, date_str, method=method)
    print(f"[dr] Front month for {product} on {date_str}: {contract}")
    return get_mbo(contract, date_str, columns=columns)


def get_mbo_rth(
    contract: str,
    date_str: str,
    columns: Optional[list[str]] = None,
    session: str = "default",
) -> pa.Table:
    """
    Load the normalized MBO event stream for a specific contract and date,
    filtered to Regular Trading Hours (RTH) only.
 
    Convenience wrapper around get_mbo() + filter_rth(). This is the standard
    entry point for all Phase 3 feature engineering — always prefer this over
    get_mbo() to avoid contaminating signals with overnight noise.
 
    Parameters
    ----------
    contract : Contract symbol, e.g. 'ESZ25', 'NIYH26', 'HSIG26'.
    date_str : Date in 'YYYY-MM-DD' or 'YYYYMMDD' format.
    columns  : Optional column selection (Parquet column pruning). None = all columns.
               Applied before RTH filtering — only selected columns are read from disk.
    session  : Named session for multi-session products (NIY, NKD).
               'default' → first declared session in market_config.py ('asia' for NIY/NKD).
               'us'      → CME US session (13:30–20:00 UTC, NIY/NKD only).
               Ignored for single-session products (ES, FDAX, HSI, ...).
 
    Returns
    -------
    pyarrow.Table — MBO events within RTH bounds, sorted by ts_recv.
 
    Examples
    --------
    >>> # Standard single-session products
    >>> tbl = dr.get_mbo_rth("ESZ25",   "2025-10-10")
    >>> tbl = dr.get_mbo_rth("HSIG26",  "2026-02-02")   # lunch break excluded
    >>> tbl = dr.get_mbo_rth("FDAXM25", "2025-05-14")
 
    >>> # Multi-session products (NIY, NKD)
    >>> tbl = dr.get_mbo_rth("NIYH26", "2026-02-02")                # OSE session (default)
    >>> tbl = dr.get_mbo_rth("NIYH26", "2026-02-02", session="us")  # CME US session
    """
    product = _product_from_contract(contract)
    table   = get_mbo(contract, date_str, columns=columns)
    return filter_rth(table, product, date_str, session=session)
 

def get_mbo_front_rth(
    product: str,
    date_str: str,
    columns: Optional[list[str]] = None,
    session: str = "default",
    method: str = "volume",
) -> pa.Table:
    """
    Load the MBO event stream for the front-month contract of a product on a
    given date, filtered to Regular Trading Hours (RTH) only.
 
    Combines front-month detection, data loading, and RTH filtering in a single
    call. This is the highest-level entry point for MBO-based feature engineering
    and the recommended default for notebook exploration.
 
    Parameters
    ----------
    product  : Product ticker, e.g. 'ES', 'NIY', 'HSI'.
    date_str : Date in 'YYYY-MM-DD' or 'YYYYMMDD' format.
    columns  : Optional column selection. None = all columns.
    session  : Named session for multi-session products (NIY, NKD).
               'default' → first declared session ('asia' for NIY/NKD).
               'us'      → CME US session.
               Ignored for single-session products.
    method   : Front-month detection method: 'volume' (default) or 'expiry'.
               See get_front_contract() for details.
 
    Returns
    -------
    pyarrow.Table — front-month MBO events within RTH bounds.
 
    Examples
    --------
    >>> # Recommended standard usage in Phase 3 feature engineering
    >>> tbl = dr.get_mbo_front_rth("ES",  "2025-10-10")
    >>> tbl = dr.get_mbo_front_rth("HSI", "2026-02-02")
    >>> tbl = dr.get_mbo_front_rth("NIY", "2026-02-02", session="us")
    """
    contract = get_front_contract(product, date_str, method=method)
    print(f"[dr] Front month for {product} on {date_str}: {contract}")
    table = get_mbo(contract, date_str, columns=columns)
    return filter_rth(table, product, date_str, session=session)
 

def get_mbp1(
    contract: str,
    date_str: str,
    columns: Optional[list[str]] = None,
) -> pa.Table:
    """
    Load the reconstructed MBP-1 (top-of-book) snapshot stream for a specific
    contract and date.

    Parameters
    ----------
    contract : Contract symbol, e.g. 'ESZ25', 'HSIG26'.
    date_str : Date in 'YYYY-MM-DD' or 'YYYYMMDD' format.
    columns  : Optional list of column names to load. None = all columns.
               Example: ['ts_recv', 'bid_px_00', 'ask_px_00', 'bid_sz_00', 'ask_sz_00']

    Returns
    -------
    pyarrow.Table with the MBP-1 schema (one snapshot per F_LAST event).

    Example
    -------
    >>> tbl = dr.get_mbp1("ESZ25", "2025-10-01")
    >>> tbl = dr.get_mbp1("HSIG26", "2026-02-03", columns=["ts_recv", "bid_px_00", "ask_px_00"])
    """
    path = _contract_recon_path(contract, date_str)
    return pq.ParquetFile(path).read(columns=columns)


def get_mbp1_front(
    product: str,
    date_str: str,
    columns: Optional[list[str]] = None,
    method: str = "volume",
) -> pa.Table:
    """
    Load the MBP-1 snapshot stream for the front-month contract.

    Parameters
    ----------
    product  : Product ticker, e.g. 'ES', 'HSI'.
    date_str : Date in 'YYYY-MM-DD' or 'YYYYMMDD' format.
    columns  : Optional column selection (see get_mbp1).
    method   : Front-month detection method: 'volume' (default) or 'expiry'.

    Returns
    -------
    pyarrow.Table — same schema as get_mbp1().

    Example
    -------
    >>> tbl = dr.get_mbp1_front("ES", "2025-10-01")
    >>> tbl = dr.get_mbp1_front("FDAX", "2025-05-14", columns=["ts_recv", "bid_px_00", "ask_px_00"])
    """
    contract = get_front_contract(product, date_str, method=method)
    print(f"[dr] Front month for {product} on {date_str}: {contract}")
    return get_mbp1(contract, date_str, columns=columns)

 
def get_mbp1_rth(
    contract: str,
    date_str: str,
    columns: Optional[list[str]] = None,
    session: str = "default",
) -> pa.Table:
    """
    Load the reconstructed MBP-1 (top-of-book) snapshot stream for a specific
    contract and date, filtered to Regular Trading Hours (RTH) only.
 
    Convenience wrapper around get_mbp1() + filter_rth(). This is the standard
    entry point for LOB-based feature engineering (OFI, spread, depth signals).
 
    Parameters
    ----------
    contract : Contract symbol, e.g. 'ESZ25', 'NIYH26', 'HSIG26'.
    date_str : Date in 'YYYY-MM-DD' or 'YYYYMMDD' format.
    columns  : Optional column selection. None = all columns.
               Typical minimal set: ['ts_recv', 'bid_px_00', 'ask_px_00',
                                     'bid_sz_00', 'ask_sz_00']
    session  : Named session for multi-session products (NIY, NKD).
               'default' → 'asia' (OSE Tokyo session).
               'us'      → CME US session (13:30–20:00 UTC).
               Ignored for single-session products.
 
    Returns
    -------
    pyarrow.Table — MBP-1 snapshots within RTH bounds, sorted by ts_recv.
 
    Examples
    --------
    >>> tbl = dr.get_mbp1_rth("ESZ25", "2025-10-10",
    ...                        columns=["ts_recv", "bid_px_00", "ask_px_00",
    ...                                 "bid_sz_00", "ask_sz_00"])
    >>> tbl = dr.get_mbp1_rth("NIYH26", "2026-02-02", session="us")
    """
    product = _product_from_contract(contract)
    table   = get_mbp1(contract, date_str, columns=columns)
    return filter_rth(table, product, date_str, session=session)
 
 
def get_mbp1_front_rth(
    product: str,
    date_str: str,
    columns: Optional[list[str]] = None,
    session: str = "default",
    method: str = "volume",
) -> pa.Table:
    """
    Load the MBP-1 (top-of-book) snapshot stream for the front-month contract
    of a product on a given date, filtered to Regular Trading Hours (RTH) only.
 
    This is the primary function for LOB-based feature engineering (OFI, spread,
    depth, VPIN) across all Phase 3 research. Combines front-month detection,
    MBP-1 loading, and RTH filtering in a single call.
 
    Parameters
    ----------
    product  : Product ticker, e.g. 'ES', 'FDAX', 'HSI', 'NIY'.
    date_str : Date in 'YYYY-MM-DD' or 'YYYYMMDD' format.
    columns  : Optional column selection. None = all columns.
               Recommended minimal set for OFI:
                   ['ts_recv', 'bid_px_00', 'ask_px_00', 'bid_sz_00', 'ask_sz_00']
               For spread analysis, add: 'action', 'side'
    session  : Named session for multi-session products (NIY, NKD).
               'default' → 'asia' (OSE Tokyo session, default for cross-market).
               'us'      → CME US session (primary liquidity for NIY/NKD).
               Ignored for single-session products.
    method   : Front-month detection method: 'volume' (default) or 'expiry'.
 
    Returns
    -------
    pyarrow.Table — front-month MBP-1 snapshots within RTH bounds.
 
    Examples
    --------
    >>> # Standard OFI input — recommended for all Phase 3 feature notebooks
    >>> cols = ["ts_recv", "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00"]
    >>> tbl = dr.get_mbp1_front_rth("ES",   "2025-10-10", columns=cols)
    >>> tbl = dr.get_mbp1_front_rth("FDAX", "2025-05-14", columns=cols)
    >>> tbl = dr.get_mbp1_front_rth("HSI",  "2026-02-02", columns=cols)
 
    >>> # NIY — OSE session (default, for cross-market comparison with HKEX)
    >>> tbl = dr.get_mbp1_front_rth("NIY", "2026-02-02", columns=cols)
 
    >>> # NIY — US session (for lead-lag analysis vs ES)
    >>> tbl = dr.get_mbp1_front_rth("NIY", "2026-02-02", columns=cols, session="us")
    """
    contract = get_front_contract(product, date_str, method=method)
    print(f"[dr] Front month for {product} on {date_str}: {contract}")
    table = get_mbp1(contract, date_str, columns=columns)
    return filter_rth(table, product, date_str, session=session)


# ---------------------------------------------------------------------------
# Core data loading — multi-day range
# ---------------------------------------------------------------------------

def get_mbo_range(
    contract: str,
    start: str,
    end: str,
    columns: Optional[list[str]] = None,
) -> pa.Table:
    """
    Load MBO data for a contract over a date range [start, end] inclusive.

    Uses DuckDB read_parquet() with a glob pattern for efficient multi-file
    scanning. DuckDB applies Parquet pushdown: it reads row-group statistics
    before decompressing data, skipping row groups that cannot satisfy filters.

    Parameters
    ----------
    contract : Contract symbol, e.g. 'ESZ25'.
    start    : Start date inclusive, 'YYYY-MM-DD' or 'YYYYMMDD'.
    end      : End date inclusive, 'YYYY-MM-DD' or 'YYYYMMDD'.
    columns  : Optional column selection. None = all columns.

    Returns
    -------
    pyarrow.Table — concatenation of all matching days, sorted by ts_recv.

    Notes on DuckDB vs PyArrow for multi-day
    -----------------------------------------
    DuckDB's read_parquet(glob) opens all matching files in parallel (uses
    internal thread pool), reads only requested columns, and returns a single
    result arrow table. Much faster than a Python loop calling read_table()
    per file and then pa.concat_tables(). DuckDB also deduplicates schema
    checking across files automatically.

    The glob pattern covers year=*/month=* to handle month boundaries in
    the date range (e.g. range spanning Oct → Nov).

    Example
    -------
    >>> tbl = dr.get_mbo_range("ESZ25", "2025-10-01", "2025-10-31")
    >>> tbl = dr.get_mbo_range("NIY", "2025-01-01", "2025-12-31",
    ...                         columns=["ts_recv", "action", "price", "size"])
    """
    product  = _product_from_contract(contract)
    cfg      = _product_from_config(product)
    base     = (
        DATA_NORMALIZED
        / f"provider={cfg['provider']}"
        / f"venue={cfg['exchange']}"
        / f"product={product}"
        / f"contract={contract}"
    )
    # Glob covers all year/month partitions — DuckDB will filter by date later
    glob_pattern = str(base / "year=*" / "month=*" / f"{contract}_*_mbo.parquet")

    # Convert date range to 'YYYYMMDD' integers for filtering on the filename-
    # embedded date. We extract the date from ts_recv instead (more reliable).
    start_ns = int(datetime.strptime(_date_str(start), "%Y%m%d").timestamp()) * 10**9
    end_ns   = int(datetime.strptime(_date_str(end),   "%Y%m%d").timestamp()) * 10**9
    # Add one full day to end_ns to make the range inclusive
    end_ns  += 86_400 * 10**9

    col_sql = "*" if columns is None else ", ".join(columns)

    # DuckDB read_parquet with glob:
    # - hive_partitioning=false: our partitioning keys (year=, month=) are in
    #   the path but NOT as actual columns in the parquet schema — disable
    #   hive mode to avoid DuckDB trying to add those as columns.
    # - ts_recv filter: pushes the date range predicate into Parquet row-group
    #   statistics (min/max on ts_recv) — skips entire row groups outside range.
    sql = f"""
        SELECT {col_sql}
        FROM read_parquet('{glob_pattern}', hive_partitioning=false)
        WHERE ts_recv >= {start_ns}
          AND ts_recv <  {end_ns}
        ORDER BY ts_recv
    """
    con = duckdb.connect()
    tbl = con.execute(sql).arrow()  # returns pyarrow.Table directly
    con.close()
    return tbl


def get_mbp1_range(
    contract: str,
    start: str,
    end: str,
    columns: Optional[list[str]] = None,
) -> pa.Table:
    """
    Load MBP-1 data for a contract over a date range [start, end] inclusive.

    Uses DuckDB read_parquet() with glob — same approach as get_mbo_range().
    See get_mbo_range() docstring for implementation notes.

    Parameters
    ----------
    contract : Contract symbol, e.g. 'NIY'.
    start    : Start date inclusive, 'YYYY-MM-DD' or 'YYYYMMDD'.
    end      : End date inclusive, 'YYYY-MM-DD' or 'YYYYMMDD'.
    columns  : Optional column selection. None = all columns.

    Returns
    -------
    pyarrow.Table — concatenation of all matching days, sorted by ts_recv.

    Example
    -------
    >>> tbl = dr.get_mbp1_range("NIYZ25", "2025-01-01", "2025-06-30")
    >>> tbl = dr.get_mbp1_range("NKDZ25", "2025-01-01", "2025-06-30",
    ...                          columns=["ts_recv", "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00"])
    """
    product  = _product_from_contract(contract)
    cfg      = _product_from_config(product)
    base     = (
        DATA_RECONSTRUCTED
        / f"provider={cfg['provider']}"
        / f"venue={cfg['exchange']}"
        / f"product={product}"
        / f"contract={contract}"
    )
    glob_pattern = str(base / "year=*" / "month=*" / f"{contract}_*_mbp1.parquet")

    start_ns = int(datetime.strptime(_date_str(start), "%Y%m%d").timestamp()) * 10**9
    end_ns   = int(datetime.strptime(_date_str(end),   "%Y%m%d").timestamp()) * 10**9
    end_ns  += 86_400 * 10**9

    col_sql = "*" if columns is None else ", ".join(columns)

    sql = f"""
        SELECT {col_sql}
        FROM read_parquet('{glob_pattern}', hive_partitioning=false)
        WHERE ts_recv >= {start_ns}
          AND ts_recv <  {end_ns}
        ORDER BY ts_recv
    """
    con = duckdb.connect()
    tbl = con.execute(sql).arrow()
    con.close()
    return tbl


# ---------------------------------------------------------------------------
# Stats & diagnostics
# ---------------------------------------------------------------------------

def get_contract_stats(
    contract: str,
    date_str: str,
    schema: str = "mbo",
) -> dict:
    """
    Compute descriptive statistics for a contract on a given date.
    Runs entirely via DuckDB — no data loaded into Python RAM.

    Parameters
    ----------
    contract : Contract symbol, e.g. 'FDAXM25'.
    date_str : Date in 'YYYY-MM-DD' or 'YYYYMMDD' format.
    schema   : 'mbo' (default) — uses normalized MBO file.
               'mbp1' — uses reconstructed MBP-1 file.

    Returns
    -------
    dict with keys:
        contract, date, schema, n_rows,
        n_add, n_cancel, n_modify, n_trade, n_fill, n_clear,
        cancel_rate   (n_cancel / n_add),
        fill_rate     (n_trade / n_add),
        otr           (n_add / n_trade),
        price_min, price_max, price_range_ticks,  (in fixed-point int64)
        price_min_pts, price_max_pts,             (in index points)
        tick_size_pts

    Notes
    -----
    DuckDB computes all aggregations in a single pass over the Parquet file.
    Column statistics (min/max) are often available from Parquet metadata
    without reading any row data. COUNT with GROUP BY requires a full scan of
    the action column only (narrow column, fast).

    Example
    -------
    >>> stats = dr.get_contract_stats("FDAXM25", "2025-05-14")
    >>> print(stats["otr"], stats["cancel_rate"])
    """
    if schema == "mbo":
        path = _contract_norm_path(contract, date_str)
    elif schema == "mbp1":
        path = _contract_recon_path(contract, date_str)
    else:
        raise ValueError(f"Unknown schema '{schema}'. Use 'mbo' or 'mbp1'.")

    path_str = str(path).replace("'", "''")
    product  = _product_from_contract(contract)
    cfg      = _product_from_config(product)
    tick_fp  = cfg["tick_size_fp"]  # fixed-point tick size (price * 1e9)

    # Single DuckDB pass: COUNT per action + price min/max
    # FILTER syntax (SQL:2003) avoids multiple table scans per action —
    # DuckDB evaluates all aggregations in one sequential read.
    sql = f"""
        SELECT
            COUNT(*)                                                            AS n_rows,
            COUNT(*) FILTER (WHERE action = 'ADD')                              AS n_add,
            COUNT(*) FILTER (WHERE action = 'CANCEL')                           AS n_cancel,
            COUNT(*) FILTER (WHERE action = 'MODIFY')                           AS n_modify,
            COUNT(*) FILTER (WHERE action = 'TRADE')                            AS n_trade,
            COUNT(*) FILTER (WHERE action = 'FILL')                             AS n_fill,
            COUNT(*) FILTER (WHERE action = 'CLEAR')                            AS n_clear,
            MIN(price) FILTER (WHERE action = 'TRADE')                          AS price_min,
            MAX(price) FILTER (WHERE action = 'TRADE')                          AS price_max
        FROM read_parquet('{path_str}')
    """
    con = duckdb.connect()
    row = con.execute(sql).fetchone()
    con.close()

    (n_rows, n_add, n_cancel, n_modify, n_trade,
     n_fill, n_clear, price_min, price_max) = row

    # Derived metrics — guard against division by zero
    cancel_rate = (n_cancel / n_add)   if n_add   else None
    fill_rate   = (n_trade  / n_add)   if n_add   else None
    otr         = (n_add    / n_trade) if n_trade  else None

    # Convert fixed-point prices to index points (divide by 1e9)
    price_min_pts = (price_min / 1e9) if price_min is not None else None
    price_max_pts = (price_max / 1e9) if price_max is not None else None
    price_range_ticks = (
        int((price_max - price_min) / tick_fp)
        if price_min is not None and price_max is not None
        else None
    )

    ds_fmt = _date_str(date_str)

    return {
        "contract":         contract,
        "date":             f"{ds_fmt[:4]}-{ds_fmt[4:6]}-{ds_fmt[6:8]}",
        "schema":           schema,
        "n_rows":           int(n_rows),
        "n_add":            int(n_add),
        "n_cancel":         int(n_cancel),
        "n_modify":         int(n_modify),
        "n_trade":          int(n_trade),
        "n_fill":           int(n_fill),
        "n_clear":          int(n_clear),
        "cancel_rate":      round(cancel_rate, 4) if cancel_rate is not None else None,
        "fill_rate":        round(fill_rate,   4) if fill_rate   is not None else None,
        "otr":              round(otr,         2) if otr         is not None else None,
        "price_min":        int(price_min) if price_min is not None else None,
        "price_max":        int(price_max) if price_max is not None else None,
        "price_min_pts":    price_min_pts,
        "price_max_pts":    price_max_pts,
        "price_range_ticks": price_range_ticks,
        "tick_size_pts":    tick_fp / 1e9,
    }


def get_product_stats(
    product: str,
    start: str,
    end: str,
    schema: str = "mbo",
    contract: Optional[str] = None,
) -> "pd.DataFrame":
    """
    Compute per-day statistics for a product over a date range.
    Auto-detects front month per day unless `contract` is specified.

    Uses DuckDB in a tight loop per day — each call is a single Parquet scan.
    Returns a pandas DataFrame (stats are small — pandas is fine here).

    Parameters
    ----------
    product  : Product ticker, e.g. 'NIY'.
    start    : Start date inclusive, 'YYYY-MM-DD'.
    end      : End date inclusive, 'YYYY-MM-DD'.
    schema   : 'mbo' or 'mbp1'.
    contract : If provided, use this specific contract for all days.
               If None, auto-detect front month per day.

    Returns
    -------
    pandas.DataFrame, one row per trading day, sorted by date.
    Columns: same as get_contract_stats() dict keys.

    Example
    -------
    >>> df = dr.get_product_stats("NIY", "2025-01-01", "2025-06-30")
    >>> df[["date", "n_trade", "otr", "cancel_rate"]].head(10)
    """
    import pandas as pd

    dates = get_available_dates(product, contract)
    # Filter to requested range
    dates = [d for d in dates if start <= d <= end]

    rows = []
    for d in dates:
        try:
            if contract is None:
                c = get_front_contract(product, d, method="expiry")
                # Verify file exists for this contract
                contracts_day = get_contracts(product, d)
                if c not in contracts_day:
                    # Front-month detection returned a contract not present this day
                    # Fall back to first available outright
                    outrights = [x for x in contracts_day if "_CAL_" not in x]
                    if not outrights:
                        continue
                    c = outrights[0]
            else:
                c = contract

            stats = get_contract_stats(c, d, schema=schema)
            rows.append(stats)
        except (FileNotFoundError, ValueError):
            continue

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def get_cross_market_summary(
    products: list[str],
    date_str: str,
    schema: str = "mbo",
) -> "pd.DataFrame":
    """
    Compute microstructure summary statistics for multiple products on a
    given date. Useful for cross-market comparison tables.

    Parameters
    ----------
    products : List of product tickers, e.g. ['ES', 'FDAX', 'FESX', 'HSI'].
    date_str : Date in 'YYYY-MM-DD' format.
    schema   : 'mbo' (default) or 'mbp1'.

    Returns
    -------
    pandas.DataFrame with columns:
        product, contract, date, n_add, n_cancel, n_trade,
        otr, cancel_rate, fill_rate,
        price_min_pts, price_max_pts, tick_size_pts,
        currency, point_value, exchange

    Example
    -------
    >>> df = dr.get_cross_market_summary(
    ...     ["ES", "FDAX", "FESX", "FSMI", "HSI", "NIY"],
    ...     "2025-10-01"
    ... )
    """
    import pandas as pd

    rows = []
    for prod in products:
        try:
            c = get_front_contract(prod, date_str, method="volume")
            stats = get_contract_stats(c, date_str, schema=schema)
            cfg   = _product_from_config(prod)
            # Enrich with market config metadata
            stats["product"]     = prod
            stats["currency"]    = cfg["currency"]
            stats["point_value"] = cfg["point_value"]
            stats["exchange"]    = cfg["exchange"]
            rows.append(stats)
        except (FileNotFoundError, ValueError) as e:
            print(f"[dr] Warning: skipping {prod} — {e}")
            continue

    if not rows:
        return pd.DataFrame()

    col_order = [
        "product", "contract", "date", "exchange", "currency",
        "n_rows", "n_add", "n_cancel", "n_modify", "n_trade",
        "otr", "cancel_rate", "fill_rate",
        "price_min_pts", "price_max_pts", "tick_size_pts", "point_value",
    ]
    df = pd.DataFrame(rows)
    # Keep only columns that exist (schema='mbp1' won't have action breakdown)
    col_order = [c for c in col_order if c in df.columns]
    return df[col_order].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Product info
# ---------------------------------------------------------------------------

def get_info(product: str) -> dict:
    """
    Return the full market config dict for a product, with a few derived
    fields added for convenience.

    Parameters
    ----------
    product : Product ticker, e.g. 'ES'.

    Returns
    -------
    dict — all keys from MARKET_CONFIG[product], plus:
        'ticker'         : product ticker (redundant but convenient)
        'tick_size_pts'  : tick size in index points (tick_size_fp / 1e9)
        'price_floor_pts': price floor in index points
        'available_dates': list of dates with data on disk (from get_available_dates)
        'contracts'      : list of all contracts available on disk

    Example
    -------
    >>> info = dr.get_info("HSI")
    >>> print(info["point_value"], info["currency"])
    50.0  HKD
    """
    cfg = dict(_product_from_config(product))  # copy to avoid mutating global
    cfg["ticker"]          = product
    cfg["tick_size_pts"]   = cfg["tick_size_fp"] / 1e9
    cfg["price_floor_pts"] = cfg["price_floor_fp"] / 1e9

    # Discover available data on disk
    dates     = get_available_dates(product)
    base      = _norm_dir(product)
    contracts = sorted(
        d.name.replace("contract=", "")
        for d in base.glob("contract=*")
        if d.is_dir()
    ) if base.exists() else []

    cfg["available_dates"] = dates
    cfg["contracts"]       = contracts
    cfg["n_days"]          = len(dates)

    return cfg


def show_info(product: str) -> None:
    """
    Print a formatted summary of a product's market config + available data.
    Wraps get_info() with human-readable output.

    Example
    -------
    >>> dr.show_info("NIY")
    """
    info = get_info(product)
    sep  = "─" * 60
    print(sep)
    print(f"  {product}  —  {info.get('description', '')}")
    print(sep)
    print(f"  Exchange      : {info['exchange']}  |  Provider: {info['provider']}")
    print(f"  Currency      : {info['currency']}  |  Point value: {info['point_value']} {info['currency']}/pt")
    print(f"  Tick size     : {info['tick_size_pts']:.4f} pts")
    print(f"  Price floor   : {info['price_floor_pts']:.0f} pts")
    print(f"  Timezone      : {info['local_tz']}")
    print(f"  Session UTC   : {info['session_open_utc']} → {info['session_close_utc']}")

    rth_mode = info.get("rth_mode", "")
    if rth_mode == "fixed_utc":
        print(f"  RTH (UTC)     : {info['rth_start_utc']} → {info['rth_end_utc']}")
    elif rth_mode == "us_eastern":
        print(f"  RTH EDT (UTC) : {info['rth_start_utc_edt']} → {info['rth_end_utc_edt']}")
        print(f"  RTH EST (UTC) : {info['rth_start_utc_est']} → {info['rth_end_utc_est']}")
    elif rth_mode == "fixed_cet":
        print(f"  RTH (CET)     : {info['rth_start_cet']} → {info['rth_end_cet']}")

    if info.get("lunch_break_start"):
        print(f"  Lunch break   : {info['lunch_break_start']} → {info['lunch_break_end']} UTC")

    print(f"  Contracts     : {info['contracts']}")
    print(f"  Data on disk  : {info['n_days']} days  ({info['available_dates'][0] if info['available_dates'] else 'none'}"
          f" → {info['available_dates'][-1] if info['available_dates'] else 'none'})")
    if "product_url" in info:
        print(f"  URL           : {info['product_url']}")
    print(sep)


# ---------------------------------------------------------------------------
# Notebook utilities
# ---------------------------------------------------------------------------

def to_df(table: pa.Table) -> "pd.DataFrame":
    """
    Convert a pyarrow.Table to a pandas DataFrame.

    This is the explicit conversion step — it copies data from Arrow columnar
    buffers into pandas Series backed by numpy arrays. Use only when you need
    pandas-specific operations (groupby, rolling, etc.).

    Memory note: pandas stores data in row-major-ish layout and boxes some
    dtypes (e.g. nullable integers → object columns). Expect 1.5–3× the RAM
    of the Arrow table for complex schemas.

    Parameters
    ----------
    table : pyarrow.Table returned by any get_* function.

    Returns
    -------
    pandas.DataFrame with the same columns and dtypes.

    Example
    -------
    >>> tbl = dr.get_mbp1("ESZ25", "2025-10-01", columns=["ts_recv", "bid_px_00", "ask_px_00"])
    >>> df = dr.to_df(tbl)
    >>> df.head()
    """
    return table.to_pandas()


def filter_rth(
    table: pa.Table,
    product: str,
    date_str: str,
    session: str = "default",
) -> pa.Table:
    """
    Filter a pyarrow.Table to Regular Trading Hours (RTH) only.
 
    Uses the RTH definition from market_config.py for the given product.
    Handles DST-aware products (ES via us_eastern mode), fixed-UTC products,
    fixed-CET products (EUREX), and multi-session products (NIY, NKD).
 
    Parameters
    ----------
    table    : pyarrow.Table with a 'ts_recv' column (uint64, nanoseconds UTC).
    product  : Product ticker used to look up RTH bounds in market_config.py.
    date_str : The trading date — required for DST resolution on ES (us_eastern).
    session  : Named session for multi-session products (NIY, NKD).
               'default' resolves to the first declared session ('asia').
               Ignored for single-session products (ES, FDAX, HSI, ...).
               Available sessions per product are declared in market_config.py.
 
    Returns
    -------
    pyarrow.Table filtered to RTH rows only.
 
    Notes
    -----
    Filtering is done via DuckDB on the in-memory Arrow table — zero copy
    (DuckDB registers the Arrow table as a view without copying data).
    For HKEX products with a lunch break, both morning and afternoon sessions
    are included (lunch rows are excluded automatically via market_config.py).
 
    Examples
    --------
    >>> tbl = dr.get_mbp1_front("ES", "2025-10-10")
    >>> tbl_rth = dr.filter_rth(tbl, "ES", "2025-10-10")
 
    >>> tbl = dr.get_mbo("NIYH26", "2026-02-02")
    >>> tbl_asia = dr.filter_rth(tbl, "NIY", "2026-02-02")               # OSE session (default)
    >>> tbl_us   = dr.filter_rth(tbl, "NIY", "2026-02-02", session="us") # US session
    """
    from ingestion.market_config import rth_utc_bounds
 
    d   = _date_obj(date_str)
    cfg = _product_from_config(product)
 
    # rth_utc_bounds() dispatches on 'sessions' dict (multi-session products)
    # or on rth_mode (single-session products). DST, CET conversion, and lunch
    # break exclusion are all handled inside market_config.py.
    rth_start_str, rth_end_str = rth_utc_bounds(d, cfg, session=session)
 
    # Convert "HH:MM:SS" UTC strings to nanosecond epoch offsets for this date
    def _hms_to_ns(date_obj: date, hms: str) -> int:
        """Convert a 'HH:MM:SS' UTC time on a given date to nanoseconds epoch."""
        h, m, s = map(int, hms.split(":"))
        from datetime import timezone
        dt = datetime(date_obj.year, date_obj.month, date_obj.day,
                      h, m, s, tzinfo=timezone.utc)
        return int(dt.timestamp()) * 10**9
 
    rth_start_ns = _hms_to_ns(d, rth_start_str)
    rth_end_ns   = _hms_to_ns(d, rth_end_str)
 
    con = duckdb.connect()
    # Register the Arrow table as a DuckDB view — zero-copy, no data movement
    con.register("tbl", table)
 
    if cfg.get("lunch_break_start") and session == "default":
        # HKEX: exclude lunch break — two sub-sessions
        # Only apply lunch break exclusion for the default (daytime) session.
        # Multi-session products (NIY/NKD) do not have a lunch break.
        lunch_start_ns = _hms_to_ns(d, cfg["lunch_break_start"])
        lunch_end_ns   = _hms_to_ns(d, cfg["lunch_break_end"])
        sql = f"""
            SELECT * FROM tbl
            WHERE ts_recv >= {rth_start_ns}
              AND ts_recv <  {rth_end_ns}
              AND NOT (ts_recv >= {lunch_start_ns} AND ts_recv < {lunch_end_ns})
        """
    else:
        sql = f"""
            SELECT * FROM tbl
            WHERE ts_recv >= {rth_start_ns}
              AND ts_recv <  {rth_end_ns}
        """
 
    result = con.execute(sql).arrow()
    con.close()
    return result


def with_datetime(
    table: pa.Table,
    col: str = "ts_recv",
    new_col: Optional[str] = None,
) -> pa.Table:
    """
    Add a human-readable datetime column (UTC) derived from a nanosecond
    epoch timestamp column. Useful for plotting and inspection.

    Parameters
    ----------
    table   : pyarrow.Table with a uint64/int64 nanosecond timestamp column.
    col     : Source column name (default: 'ts_recv').
    new_col : Name of the new datetime column (default: col + '_dt').

    Returns
    -------
    pyarrow.Table with an additional column of type timestamp[ns, UTC].

    Notes
    -----
    Conversion is done via DuckDB epoch_ns() on the registered Arrow table.
    The resulting column is a DuckDB TIMESTAMP WITH TIME ZONE, returned as
    pyarrow timestamp[us, UTC] (microsecond precision — DuckDB's native
    timestamp resolution). Nanosecond sub-microsecond precision is lost in
    the display column, but the original `col` is preserved unchanged.

    Example
    -------
    >>> tbl = dr.get_mbp1_front("HSI", "2026-02-03", columns=["ts_recv", "bid_px_00", "ask_px_00"])
    >>> tbl = dr.with_datetime(tbl)
    >>> df  = dr.to_df(tbl)
    >>> df[["ts_recv_dt", "bid_px_00", "ask_px_00"]].head()
    """
    out_col = new_col or f"{col}_dt"

    con = duckdb.connect()
    con.register("tbl", table)
    # epoch_ns(col) interprets the integer as nanoseconds since Unix epoch,
    # returning a TIMESTAMPTZ. All other columns are preserved via SELECT *.
    sql = f"""
        SELECT *, epoch_ns(CAST({col} AS BIGINT)) AS {out_col}
        FROM tbl
    """
    result = con.execute(sql).arrow()
    con.close()
    return result


def schema(contract: str, date_str: str, kind: str = "mbo") -> pa.Schema:
    """
    Return the pyarrow Schema for a contract's parquet file without loading
    any row data. Useful for inspecting column names and dtypes quickly.

    Parameters
    ----------
    contract : Contract symbol, e.g. 'HSIG26'.
    date_str : Date in 'YYYY-MM-DD' format.
    kind     : 'mbo' (default) or 'mbp1'.

    Returns
    -------
    pyarrow.Schema

    Example
    -------
    >>> dr.schema("ESZ25", "2025-10-01")
    >>> dr.schema("HSIG26", "2026-02-03", kind="mbp1")
    """
    if kind == "mbo":
        path = _contract_norm_path(contract, date_str)
    elif kind == "mbp1":
        path = _contract_recon_path(contract, date_str)
    else:
        raise ValueError(f"Unknown kind '{kind}'. Use 'mbo' or 'mbp1'.")
    # ParquetFile reads only footer metadata — zero row data read
    return pq.ParquetFile(path).schema_arrow


# ---------------------------------------------------------------------------
# Module-level convenience: quick overview on import
# ---------------------------------------------------------------------------

def overview() -> None:
    """
    Print a quick overview of all products and available data on disk.
    Useful as first cell in a notebook.

    Example
    -------
    >>> dr.overview()
    """
    print("=" * 60)
    print("  DestinyResearch — Data Overview")
    print("=" * 60)
    for prod in sorted(AVAILABLE_PRODUCTS):
        dates = get_available_dates(prod)
        if not dates:
            print(f"  {prod:6s}  —  no data on disk")
        else:
            print(f"  {prod:6s}  —  {len(dates):3d} days  "
                  f"[{dates[0]} → {dates[-1]}]")
    print("=" * 60)
    print("\nQuick start:")
    print("  tbl = dr.get_mbo_front('ES', '2025-10-01')")
    print("  tbl = dr.get_mbp1('ESZ25', '2025-10-01', columns=['ts_recv','bid_px_00','ask_px_00'])")
    print("  df  = dr.to_df(tbl)")
    print("  dr.show_info('HSI')")