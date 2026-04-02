"""
rolling_stats.py — Rolling microstructure metrics over daily stats DataFrames.

Input:  small daily DataFrame produced by dr.get_product_stats()
        (~250 rows per product per year, already aggregated by DuckDB)
Output: same DataFrame enriched with rolling columns — stays in-memory,
        no I/O, no tick data loaded here.

Typical usage
-------------
import DestinyResearch as dr
from DestinyResearch.features.descriptive.rolling_stats import add_rolling_metrics, merge_products

stats_niy = dr.get_product_stats("NIY", "2025-01-01", "2025-12-31", schema="mbo")
stats_nkd = dr.get_product_stats("NKD", "2025-01-01", "2025-12-31", schema="mbo")

stats_niy = add_rolling_metrics(stats_niy, windows=[5, 20])
stats_nkd = add_rolling_metrics(stats_nkd, windows=[5, 20])

merged = merge_products({"NIY": stats_niy, "NKD": stats_nkd})
"""

from __future__ import annotations

from typing import Sequence

import pandas as pd


# ---------------------------------------------------------------------------
# Core rolling computation
# ---------------------------------------------------------------------------

# Metrics we know how to roll — extend this list as dr.get_product_stats grows.
_ROLLABLE_METRICS: list[str] = [
    "order_to_trade_ratio",
    "cancel_rate",
    "fill_rate",
    "n_add",
    "n_cancel",
    "n_trades",
]


def add_rolling_metrics(
    df: pd.DataFrame,
    windows: Sequence[int] = (5, 20),
    metrics: Sequence[str] | None = None,
    min_periods: int = 1,
    session: str = "default",
) -> pd.DataFrame:
    """Enrich a daily stats DataFrame with rolling mean columns.

    Parameters
    ----------
    df : pd.DataFrame
        Output of dr.get_product_stats(). Must have a 'date' column (str or
        datetime) and at least one of the rollable metric columns.
    windows : sequence of int
        Rolling window sizes in trading days. Default (5, 20) = 1 week / 1 month.
    metrics : sequence of str, optional
        Which columns to roll. Defaults to all _ROLLABLE_METRICS present in df.
    min_periods : int
        Minimum non-NaN observations required to compute a rolling value.
        Set to 1 so the first few rows are not dropped (useful on short series).
    session : str
        Passed through as metadata — stored in df.attrs for downstream consumers
        (e.g. plot title). Has no effect on computation.

    Returns
    -------
    pd.DataFrame
        Copy of df with additional columns named
        ``{metric}_roll{window}`` for each (metric, window) pair.
        Sorted by date ascending. Index is reset.

    Notes
    -----
    Rolling is computed on calendar-sorted rows with no reindexing to fill
    missing trading days. A gap of several days (holiday, weekend) counts as
    one step in the window, which is standard practice for daily financial
    series. If you need calendar-day rolling, resample first.
    """
    df = df.copy()

    # -- Ensure date column is datetime and sort ---------------------------------
    if "date" not in df.columns:
        raise ValueError("DataFrame must have a 'date' column (output of get_product_stats).")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # -- Resolve which metrics to roll -------------------------------------------
    if metrics is None:
        metrics = [m for m in _ROLLABLE_METRICS if m in df.columns]
    else:
        missing = [m for m in metrics if m not in df.columns]
        if missing:
            raise ValueError(f"Requested metrics not found in DataFrame: {missing}")

    if not metrics:
        raise ValueError(
            f"None of the rollable metrics {_ROLLABLE_METRICS} found in DataFrame columns: "
            f"{list(df.columns)}"
        )

    # -- Compute rolling means ---------------------------------------------------
    for metric in metrics:
        for w in windows:
            col_name = f"{metric}_roll{w}"
            # min_periods=1 keeps early rows; caller can filter if stricter needed
            df[col_name] = (
                df[metric]
                .rolling(window=w, min_periods=min_periods)
                .mean()
            )

    # -- Store metadata in attrs (survives most pandas operations) ---------------
    df.attrs["rolling_windows"] = list(windows)
    df.attrs["rolling_metrics"] = list(metrics)
    df.attrs["session"] = session

    return df

# ---------------------------------------------------------------------------
# Multi-product merge helper
# ---------------------------------------------------------------------------

def merge_products(
    stats_by_product: dict[str, pd.DataFrame],
    on: str = "date",
) -> pd.DataFrame:
    """Merge per-product rolling stats into a single long-format DataFrame.

    Parameters
    ----------
    stats_by_product : dict[str, pd.DataFrame]
        Keys are product tickers (e.g. "NIY", "NKD"), values are DataFrames
        returned by add_rolling_metrics().
    on : str
        Column to align on. Must be present in all DataFrames.

    Returns
    -------
    pd.DataFrame
        Long-format DataFrame with an additional 'product' column.
        Suitable for seaborn FacetGrid or direct matplotlib multi-panel plots.

    Example
    -------
    merged = merge_products({"NIY": stats_niy, "NKD": stats_nkd})
    # columns: date, product, order_to_trade_ratio, order_to_trade_ratio_roll5, ...
    """
    frames = []
    for product, df in stats_by_product.items():
        tmp = df.copy()
        tmp["product"] = product
        frames.append(tmp)

    if not frames:
        raise ValueError("stats_by_product is empty.")

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.sort_values(["product", on]).reset_index(drop=True)

    return merged

# ---------------------------------------------------------------------------
# Convenience: summary statistics on rolling series
# ---------------------------------------------------------------------------

def rolling_summary(df: pd.DataFrame, metric: str, windows: Sequence[int] = (5, 20)) -> pd.DataFrame:
    """Return descriptive stats for raw and rolling series of a given metric.

    Useful for a quick sanity check before plotting: mean, std, min, max
    for the raw daily series and each rolling window.

    Parameters
    ----------
    df : pd.DataFrame
        Output of add_rolling_metrics().
    metric : str
        Base metric name (e.g. 'order_to_trade_ratio').
    windows : sequence of int
        Window sizes — must match what was passed to add_rolling_metrics().

    Returns
    -------
    pd.DataFrame
        One row per series (raw + one per window), columns = [mean, std, min, max].
    """
    cols = [metric] + [f"{metric}_roll{w}" for w in windows]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Columns not found (did you call add_rolling_metrics first?): {missing}")

    stats = df[cols].describe().T[["mean", "std", "min", "max"]]
    stats.index.name = "series"
    return stats