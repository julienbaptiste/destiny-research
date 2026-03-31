"""
Schema consistency scanner for normalized MBO parquet files.
Scans all files under DATA_NORMALIZED and reports dictionary index type
inconsistencies per column, without loading any row data.

Run from notebook:
    %run DestinyResearch/tools/scan_schemas.py
Or directly:
    python DestinyResearch/tools/scan_schemas.py
"""

import sys
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

# Bootstrap repo root on path
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from config import DATA_NORMALIZED


def _dict_index_type(field) -> str:
    """
    Return a human-readable type string for a pyarrow field.
    For dictionary-encoded columns, include the index type explicitly
    (int8, int16, int32...) since this is what causes merge conflicts.

    Example:
        dictionary<values=string, indices=int8>  →  "dict(string, int8)"
        uint64                                   →  "uint64"
    """
    import pyarrow as pa
    t = field.type
    if pa.types.is_dictionary(t):
        return f"dict({t.value_type}, {t.index_type})"
    return str(t)


def scan_all_schemas(data_root: Path) -> dict:
    """
    Walk DATA_NORMALIZED and collect the schema of every parquet file.
    Returns a dict:
        column_name → { type_string → [list of file paths] }

    Only reads Parquet footer metadata (zero row data).
    """
    # column → { type_repr → [paths] }
    col_types: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))

    parquet_files = sorted(data_root.rglob("*_mbo.parquet"))
    print(f"Scanning {len(parquet_files)} normalized MBO files...")

    for i, path in enumerate(parquet_files):
        if i % 50 == 0 and i > 0:
            print(f"  ... {i}/{len(parquet_files)}")
        try:
            # read_schema reads only the Parquet footer — no row data loaded
            schema = pq.read_schema(path)
            rel_path = str(path.relative_to(data_root))
            for field in schema:
                type_repr = _dict_index_type(field)
                col_types[field.name][type_repr].append(rel_path)
        except Exception as e:
            print(f"  ERROR reading schema from {path}: {e}")

    return col_types


def report(col_types: dict) -> None:
    """
    Print a concise report of schema inconsistencies.
    Only shows columns where more than one type is observed across files.
    """
    print("\n" + "=" * 70)
    print("  Schema consistency report — DATA_NORMALIZED")
    print("=" * 70)

    inconsistent = {
        col: types
        for col, types in col_types.items()
        if len(types) > 1  # more than one distinct type observed
    }

    if not inconsistent:
        print("\n  ✅ All columns are type-consistent across all files.\n")
        return

    print(f"\n  ⚠️  {len(inconsistent)} column(s) with inconsistent types:\n")

    for col, types in sorted(inconsistent.items()):
        print(f"  Column: '{col}'")
        for type_repr, paths in sorted(types.items(), key=lambda x: len(x[1]), reverse=True):
            print(f"    {type_repr:40s}  → {len(paths):4d} file(s)")
        # Show which files have the minority type (smallest group = likely the outliers)
        minority_type = min(types.items(), key=lambda x: len(x[1]))
        print(f"    Minority type ({minority_type[0]}) found in:")
        for p in minority_type[1][:10]:  # cap at 10 for readability
            print(f"      {p}")
        if len(minority_type[1]) > 10:
            print(f"      ... and {len(minority_type[1]) - 10} more")
        print()

    print("=" * 70)
    print("\n  Summary: columns with consistent types (not shown above):")
    consistent_cols = [col for col in col_types if len(col_types[col]) == 1]
    for col in sorted(consistent_cols):
        type_repr = next(iter(col_types[col]))
        n = sum(len(v) for v in col_types[col].values())
        print(f"    {col:30s}  {type_repr}  ({n} files)")


if __name__ == "__main__":
    col_types = scan_all_schemas(DATA_NORMALIZED)
    report(col_types)