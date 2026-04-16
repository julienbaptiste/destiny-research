"""
post_process_hkex_ingestion.py — HKEX MODIFY reconstitution post-processor.

Transforms CANCEL+ADD pairs into proper MODIFY events in HKEX normalized MBO files.

═══════════════════════════════════════════════════════════════════════════════
CONTEXT
═══════════════════════════════════════════════════════════════════════════════

HKEX never emits ModifyOrder (331) — instead it emits Delete (332) + Add (330)
when a participant requotes. We reconstitute these pairs into MODIFY for
consistent cross-provider feature engineering.

Three types of CANCEL events in HKEX normalized stream:

1. Fill-cancel
   CANCEL that shares its sequence number with a FILL event (matching engine
   decrement after partial/full fill).
   → Keep as-is (genuine book update).

2. Modify-cancel
   CANCEL immediately followed by ADD (same order_id, same side, next row).
   → Drop CANCEL, convert ADD to MODIFY.

3. Pure cancel
   CANCEL not followed by ADD.
   → Keep as-is (genuine cancellation).

═══════════════════════════════════════════════════════════════════════════════
OUTPUT
═══════════════════════════════════════════════════════════════════════════════

The script replaces the original mbo.parquet file in-place:
  - Original is renamed to mbo.parquet.bak
  - Processed file becomes the new mbo.parquet

This allows build_mbp1.py to work transparently with the processed data.

═══════════════════════════════════════════════════════════════════════════════
USAGE
═══════════════════════════════════════════════════════════════════════════════

Single day:
  python -m ingestion.post_process_hkex_ingestion --product HSI --date 2026-02-02

Full month:
  python -m ingestion.post_process_hkex_ingestion --product HSI --month 2026-02

Multiple products:
  python -m ingestion.post_process_hkex_ingestion --product HSI MHI HHI MCH --month 2026-02

Overwrite existing processed files:
  python -m ingestion.post_process_hkex_ingestion --product HSI --month 2026-02 --overwrite
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np

# Repo root for imports
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT))

from config import DATA_NORMALIZED
from ingestion.schema import NORMALIZED_MBO_SCHEMA

# Module-level logger
log = logging.getLogger(__name__)


def process_file(
    mbo_path: Path,
    processed_path: Path,
    overwrite: bool = False,
) -> dict:
    """
    Post-process a single HKEX normalized MBO Parquet file.

    Merges consecutive CANCEL+ADD pairs (same order_id, same side, adjacent rows)
    into MODIFY events, excluding fill-cancels (CANCEL sharing sequence with FILL).

    The file is loaded entirely in memory (typical size ~100 MB → ~500K-1M rows).
    No chunking needed — simplifies boundary handling and improves performance.

    Returns:
        dict: n_original, n_processed, n_modify, n_dropped_cancel
    """
    if not mbo_path.exists():
        log.warning("Input not found: %s", mbo_path)
        return {}

    # Check if already processed by looking for backup file
    backup_path = mbo_path.with_suffix('.parquet.bak')
    if not overwrite and backup_path.exists():
        log.info("SKIP — backup exists (already processed): %s (use --overwrite)", mbo_path.name)
        return {}

    # Check if file has MODIFY actions (already processed but backup was deleted)
    if not overwrite:
        try:
            pf_check = pq.ParquetFile(mbo_path)
            first_batch = next(pf_check.iter_batches(batch_size=50_000))
            df_check = first_batch.to_pandas()
            if 'action' in df_check.columns:
                actions = (
                    df_check['action'].cat.categories
                    if isinstance(df_check['action'].dtype, pd.CategoricalDtype)
                    else df_check['action'].unique()
                )
                if 'MODIFY' in actions:
                    log.info("SKIP — already processed (MODIFY found): %s", mbo_path.name)
                    return {}
        except Exception as e:
            log.warning("Could not check for MODIFY actions: %s", e)

    log.info("POST-PROCESSING %s", mbo_path.name)

    # ───────────────────────────────────────────────────────────────────────
    # Load entire file into memory
    # ───────────────────────────────────────────────────────────────────────
    parquet_file = pq.ParquetFile(mbo_path)
    n_original = parquet_file.metadata.num_rows
    file_schema = parquet_file.schema_arrow
    df = parquet_file.read().to_pandas()

    # Cast categorical to str for manipulation
    if isinstance(df['action'].dtype, pd.CategoricalDtype):
        df = df.copy()
        df['action'] = df['action'].astype(str)
    if isinstance(df['side'].dtype, pd.CategoricalDtype):
        df['side'] = df['side'].astype(str)

    # ───────────────────────────────────────────────────────────────────────
    # Identify fill-cancels: CANCEL sharing sequence with a FILL event
    # ───────────────────────────────────────────────────────────────────────
    fill_sequences = set(df.loc[df['action'] == 'FILL', 'sequence'].tolist())

    # ───────────────────────────────────────────────────────────────────────
    # Lookahead: next row's action, order_id, side, price, size
    # CRITICAL: use fill_value to preserve dtype (uint64 for order_id).
    # shift(-1) without fill_value converts to float64, causing precision
    # loss on large HKEX order_ids (> 2^53) and false positive matches.
    # ───────────────────────────────────────────────────────────────────────
    df['_next_action'] = df['action'].shift(-1, fill_value='')
    df['_next_order_id'] = df['order_id'].shift(-1, fill_value=0)
    df['_next_side'] = df['side'].shift(-1, fill_value='')
    df['_next_price'] = df['price'].shift(-1, fill_value=0)
    df['_next_size'] = df['size'].shift(-1, fill_value=0)

    # ───────────────────────────────────────────────────────────────────────
    # Detect modify-cancel: CANCEL immediately followed by ADD
    # (same order_id, same side), excluding fill-cancels
    # ───────────────────────────────────────────────────────────────────────
    is_modify_cancel = (
        (df['action'] == 'CANCEL') &
        (df['order_id'] > 0) &
        (~df['sequence'].isin(fill_sequences)) &
        (df['_next_action'] == 'ADD') &
        (df['_next_order_id'] == df['order_id']) &
        (df['_next_side'] == df['side'])
    )

    # ───────────────────────────────────────────────────────────────────────
    # Transform ADD → MODIFY: keep ADD row (it has the new price/size),
    # drop the CANCEL row (it was just removing the old order state)
    # ───────────────────────────────────────────────────────────────────────
    # Find ADD rows that follow a modify-cancel
    is_modify_add = (
        (df['action'] == 'ADD') &
        df.index.to_series().shift(1).isin(df.index[is_modify_cancel])
    )
    
    n_modify = int(is_modify_add.sum())
    if n_modify > 0:
        # Transform ADD → MODIFY (keep all fields from ADD intact)
        df.loc[is_modify_add, 'action'] = 'MODIFY'

    # Drop the CANCEL rows that were part of a modify pair
    df = df[~is_modify_cancel].copy()

    # Drop helper columns
    df = df.drop(columns=[c for c in df.columns if c.startswith('_')])

    # ───────────────────────────────────────────────────────────────────────
    # Write output to temporary file, then atomic rename
    # ───────────────────────────────────────────────────────────────────────
    n_processed = len(df)
    n_dropped_cancel = n_original - n_processed

    table = pa.Table.from_pandas(df, schema=file_schema)
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, processed_path, compression='zstd')

    log.info(
        "  ✓ %s → %s events | %s MODIFY reconstituted | %s cancels dropped",
        f"{n_original:,}", f"{n_processed:,}", f"{n_modify:,}", f"{n_dropped_cancel:,}",
    )

    # ───────────────────────────────────────────────────────────────────────
    # Atomic rename: backup original → replace with processed
    # ───────────────────────────────────────────────────────────────────────
    try:
        backup_path = mbo_path.with_suffix('.parquet.bak')
        
        # Remove old backup if exists
        if backup_path.exists():
            backup_path.unlink()
        
        # Rename original → backup
        mbo_path.rename(backup_path)
        
        # Rename processed → original
        processed_path.rename(mbo_path)
        
        log.info("  ✓ Replaced %s (backup: %s)", mbo_path.name, backup_path.name)
        
    except Exception as e:
        log.error("  ✗ Rename failed: %s", e)
        # Restore backup if original is missing
        if backup_path.exists() and not mbo_path.exists():
            backup_path.rename(mbo_path)
            log.info("  ✓ Restored backup")
        # Clean up temp file if it still exists
        if processed_path.exists():
            processed_path.unlink()
        return {}

    return {
        'n_original': n_original,
        'n_processed': n_processed,
        'n_modify': n_modify,
        'n_dropped_cancel': n_dropped_cancel,
    }


def process_product_month(
    product: str,
    year: int,
    month: int,
    overwrite: bool = False,
) -> None:
    """
    Process all normalized MBO files for a product/month.
    
    Discovers all contracts under:
      DATA_NORMALIZED/provider=HKEX/venue=HKEX/product={product}/contract=*/
    
    Then processes each contract's mbo.parquet files for the given year/month.
    """
    normalized_root = DATA_NORMALIZED / "provider=HKEX" / "venue=HKEX" / f"product={product}"
    
    if not normalized_root.exists():
        log.error("Normalized directory not found: %s", normalized_root)
        return
    
    # Discover all contracts for this product
    contract_dirs = sorted([p for p in normalized_root.glob("contract=*") if p.is_dir()])
    
    if not contract_dirs:
        log.warning("No contracts found for product=%s", product)
        return
    
    # Discover all mbo.parquet files for this year/month across all contracts
    mbo_files = []
    for contract_dir in contract_dirs:
        month_dir = contract_dir / f"year={year}" / f"month={month:02d}"
        if month_dir.exists():
            mbo_files.extend(sorted(month_dir.glob("*_mbo.parquet")))
    
    if not mbo_files:
        log.warning("No mbo.parquet files found for product=%s year=%04d month=%02d", 
                    product, year, month)
        return
    
    log.info("POST-PROCESS | product=%s | year=%04d | month=%02d | %d file(s)",
             product, year, month, len(mbo_files))
    
    total_stats = {
        'n_files': 0,
        'n_original': 0,
        'n_processed': 0,
        'n_modify': 0,
        'n_dropped_cancel': 0,
    }
    
    for mbo_path in mbo_files:
        # Create temp path for processed file
        processed_path = mbo_path.parent / mbo_path.name.replace("_mbo.parquet", "_mbo_temp.parquet")
        
        stats = process_file(mbo_path, processed_path, overwrite=overwrite)
        
        if stats:
            total_stats['n_files'] += 1
            total_stats['n_original'] += stats['n_original']
            total_stats['n_processed'] += stats['n_processed']
            total_stats['n_modify'] += stats['n_modify']
            total_stats['n_dropped_cancel'] += stats['n_dropped_cancel']
    
    if total_stats['n_files'] > 0:
        log.info(
            "Done. %d file(s) processed | %s events → %s events | %s MODIFY total",
            total_stats['n_files'],
            f"{total_stats['n_original']:,}",
            f"{total_stats['n_processed']:,}",
            f"{total_stats['n_modify']:,}",
        )
    else:
        log.info("Done. No files processed.")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="post_process_hkex_ingestion.py",
        description="HKEX MODIFY reconstitution — post-process normalized MBO files",
    )
    parser.add_argument(
        "--product", required=True, nargs="+",
        help="Product code(s), e.g. HSI MHI HHI MCH",
    )
    
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--date", nargs="+",
        help="Single or multiple trading dates YYYY-MM-DD, e.g. 2026-02-03 or 2026-02-02 2026-02-03",
    )
    date_group.add_argument(
        "--month",
        help="Full month YYYY-MM, e.g. 2026-02 — processes all available days",
    )
    
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Re-process even if mbo_processed.parquet already exists",
    )
    
    args = parser.parse_args()
    
    # Parse date range
    if args.date:
        # Single or multiple dates mode
        dates = args.date if isinstance(args.date, list) else [args.date]
        
        for date_str in dates:
            year, month, day = date_str.split("-")
            year, month = int(year), int(month)
            date_short = date_str.replace("-", "")
            
            for product in args.product:
                normalized_root = DATA_NORMALIZED / "provider=HKEX" / "venue=HKEX" / f"product={product}"
                
                if not normalized_root.exists():
                    log.warning("Normalized directory not found for product=%s", product)
                    continue
                
                # Discover all contracts
                contract_dirs = sorted([p for p in normalized_root.glob("contract=*") if p.is_dir()])
                
                if not contract_dirs:
                    log.warning("No contracts found for product=%s", product)
                    continue
                
                # Find all mbo.parquet files for this date across all contracts
                mbo_files = []
                for contract_dir in contract_dirs:
                    month_dir = contract_dir / f"year={year}" / f"month={month:02d}"
                    if month_dir.exists():
                        mbo_files.extend(sorted(month_dir.glob(f"*_{date_short}_mbo.parquet")))
                
                if not mbo_files:
                    log.warning("No mbo.parquet files found for product=%s date=%s", product, date_str)
                    continue
                
                log.info("POST-PROCESS | product=%s | date=%s | %d file(s)", product, date_str, len(mbo_files))
                
                for mbo_path in mbo_files:
                    processed_path = mbo_path.parent / mbo_path.name.replace("_mbo.parquet", "_mbo_temp.parquet")
                    process_file(mbo_path, processed_path, overwrite=args.overwrite)
    else:
        # Month mode
        year, month = args.month.split("-")
        year, month = int(year), int(month)
        
        for product in args.product:
            process_product_month(product, year, month, overwrite=args.overwrite)


if __name__ == "__main__":
    from utils.logging_config import setup_logging
    setup_logging()
    main()