"""
parse_batch.py — Pipeline batch HKEX : parser plusieurs jours en boucle

Usage
-----
# Parser tous les zips d'un dossier
python parse_batch.py --input data/market_data/raw/HSI/2026/02

# Parser une plage de dates spécifique
python parse_batch.py --input data/market_data/raw/HSI/2026 \\
                      --from-date 20260201 --to-date 20260228

# Parser avec un répertoire de sortie custom
python parse_batch.py --input data/raw --output data/processed \\
                      --instruments HSI MHI

# Reprendre après une interruption (skip les jours déjà parsés)
python parse_batch.py --input data/market_data/raw/HSI/2026/02 --resume

Description
-----------
Ce script :
  1. Trouve tous les zips FBD-NSOM_YYYYMMDD.zip dans --input (récursivement)
  2. Les filtre par plage de dates si --from-date / --to-date sont fournis
  3. Skip les jours déjà parsés si --resume est activé
  4. Parse chaque zip avec HKEXParser.parse_daily_zip()
  5. Écrit les Parquet dans --output avec la convention Hive-style
  6. Affiche un résumé final avec statistiques agrégées et erreurs
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from datetime import datetime

# Ajouter le répertoire parent au path pour l'import du package
sys.path.insert(0, str(Path(__file__).parent))

from hkex_parser import HKEXParser

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)


def find_zip_files(input_dir: Path, from_date: str | None,
                   to_date: str | None) -> list[Path]:
    """
    Trouve tous les zips FBD-NSOM_YYYYMMDD.zip dans input_dir (récursivement).
    Filtre par plage de dates si from_date/to_date sont fournis.

    Retourne la liste triée par date (ordre chronologique).
    """
    zips = sorted(input_dir.rglob('FBD-NSOM_*.zip'))

    if not zips:
        print(f"Aucun fichier FBD-NSOM_*.zip trouvé dans {input_dir}")
        return []

    # Filtrage par plage de dates
    if from_date or to_date:
        filtered = []
        for z in zips:
            date_str = z.stem.split('_')[-1]   # 'FBD-NSOM_20260203' → '20260203'
            if from_date and date_str < from_date:
                continue
            if to_date and date_str > to_date:
                continue
            filtered.append(z)
        zips = filtered

    return zips


def is_already_parsed(zip_path: Path, base_dir: Path,
                      instruments: list[str]) -> bool:
    """
    Vérifie si un zip a déjà été parsé en cherchant les Parquet attendus.

    Un jour est considéré comme "déjà parsé" si tous les fichiers
    orders ET trades de tous les instruments demandés existent.

    Utilisé avec --resume pour reprendre après une interruption.
    """
    date_str = zip_path.stem.split('_')[-1]
    for cc in instruments:
        year  = date_str[:4]
        month = date_str[4:6]
        prod_dir = base_dir / f"product={cc}" / f"year={year}" / f"month={month}"
        orders = prod_dir / f"{cc}_{date_str}_orders.parquet"
        trades = prod_dir / f"{cc}_{date_str}_trades.parquet"
        if not orders.exists() or not trades.exists():
            return False
    return True


def format_duration(seconds: float) -> str:
    """Formate une durée en secondes en string lisible."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}min"
    else:
        return f"{seconds/3600:.1f}h"


def main():
    parser_cli = argparse.ArgumentParser(
        description='Parser batch HKEX FBD-NSOM → Parquet Hive-style',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser_cli.add_argument(
        '--input', type=Path, required=True,
        help='Dossier contenant les zips FBD-NSOM_YYYYMMDD.zip (recherche récursive)',
    )
    parser_cli.add_argument(
        '--output', type=Path, default=Path('data/market_data'),
        help='Racine de l\'arborescence Parquet (défaut: data/market_data)',
    )
    parser_cli.add_argument(
        '--instruments', nargs='+', default=['HSI', 'MHI', 'HHI', 'MCH'],
        help='ClassCodes à extraire (défaut: HSI MHI HHI MCH)',
    )
    parser_cli.add_argument(
        '--from-date', type=str, default=None,
        help='Date de début au format YYYYMMDD (inclus)',
    )
    parser_cli.add_argument(
        '--to-date', type=str, default=None,
        help='Date de fin au format YYYYMMDD (inclus)',
    )
    parser_cli.add_argument(
        '--resume', action='store_true',
        help='Skip les jours déjà parsés (détecte par présence des Parquet)',
    )
    parser_cli.add_argument(
        '--futures-only', action='store_true', default=True,
        help='Extraire uniquement les futures (défaut: True)',
    )
    parser_cli.add_argument(
        '--dry-run', action='store_true',
        help='Afficher les zips qui seraient parsés sans les traiter',
    )
    args = parser_cli.parse_args()

    # ── Validation des arguments ───────────────────────────────────────────────
    if not args.input.exists():
        print(f"ERREUR: --input {args.input} n'existe pas")
        sys.exit(1)

    for date_arg in [args.from_date, args.to_date]:
        if date_arg and (len(date_arg) != 8 or not date_arg.isdigit()):
            print(f"ERREUR: format de date invalide '{date_arg}' (attendu: YYYYMMDD)")
            sys.exit(1)

    # ── Découverte des zips ────────────────────────────────────────────────────
    zip_files = find_zip_files(args.input, args.from_date, args.to_date)
    if not zip_files:
        print("Aucun zip à traiter. Fin.")
        sys.exit(0)

    print(f"\n{'═'*60}")
    print(f"HKEX Batch Parser")
    print(f"{'═'*60}")
    print(f"Input     : {args.input}")
    print(f"Output    : {args.output}")
    print(f"Instruments: {args.instruments}")
    print(f"Zips trouvés: {len(zip_files)}")
    if args.from_date or args.to_date:
        print(f"Plage     : {args.from_date or 'début'} → {args.to_date or 'fin'}")
    if args.resume:
        print(f"Mode      : RESUME (skip jours déjà parsés)")

    # ── Dry run ────────────────────────────────────────────────────────────────
    if args.dry_run:
        print(f"\n[DRY RUN] Zips qui seraient parsés :")
        for z in zip_files:
            date_str = z.stem.split('_')[-1]
            already  = is_already_parsed(z, args.output, args.instruments)
            status   = '✓ déjà parsé' if already else '→ à parser'
            print(f"  {date_str}  {z.name}  [{status}]")
        sys.exit(0)

    # ── Parsing ────────────────────────────────────────────────────────────────
    total_start = time.time()
    stats_agg = {
        'processed': 0, 'skipped': 0, 'errors': 0,
        'add_orders': 0, 'mod_orders': 0, 'del_orders': 0, 'trades': 0,
    }
    errors: list[tuple[str, str]] = []   # (date_str, message d'erreur)

    print(f"\n{'─'*60}")

    for i, zip_path in enumerate(zip_files, 1):
        date_str = zip_path.stem.split('_')[-1]
        prefix   = f"[{i:>3}/{len(zip_files)}] {date_str}"

        # Mode resume : skip si déjà parsé
        if args.resume and is_already_parsed(zip_path, args.output, args.instruments):
            print(f"{prefix}  ✓ skip (déjà parsé)")
            stats_agg['skipped'] += 1
            continue

        # Parsing du jour
        day_start = time.time()
        try:
            # Nouvelle instance par jour pour éviter les effets de bord
            # (orderbook_map et stats sont réinitialisés)
            hkex_parser = HKEXParser(
                instruments=args.instruments,
                futures_only=args.futures_only,
            )
            result = hkex_parser.parse_daily_zip(zip_path, base_dir=args.output)
            day_stats = hkex_parser.get_stats()

            # Accumulation des stats
            stats_agg['processed']  += 1
            stats_agg['add_orders'] += day_stats.get('add_orders', 0)
            stats_agg['mod_orders'] += day_stats.get('mod_orders', 0)
            stats_agg['del_orders'] += day_stats.get('del_orders', 0)
            stats_agg['trades']     += day_stats.get('trades', 0)

            duration = time.time() - day_start
            products_str = ' '.join(sorted(result.keys()))
            print(f"{prefix}  ✅  {format_duration(duration)}  [{products_str}]"
                  f"  add={day_stats['add_orders']:,}  trades={day_stats['trades']:,}")

        except KeyboardInterrupt:
            print(f"\n\nInterruption clavier — {stats_agg['processed']} jours traités.")
            _print_summary(stats_agg, errors, time.time() - total_start)
            sys.exit(1)

        except Exception as e:
            stats_agg['errors'] += 1
            error_msg = f"{type(e).__name__}: {e}"
            errors.append((date_str, error_msg))
            print(f"{prefix}  ❌  {error_msg}")
            logger.exception(f"Erreur sur {zip_path}")
            # On continue avec le jour suivant plutôt que de crasher tout le batch

    # ── Résumé final ───────────────────────────────────────────────────────────
    total_duration = time.time() - total_start
    _print_summary(stats_agg, errors, total_duration)


def _print_summary(stats: dict, errors: list[tuple[str, str]],
                   duration: float) -> None:
    """Affiche le résumé final du batch."""
    print(f"\n{'═'*60}")
    print(f"RÉSUMÉ BATCH")
    print(f"{'═'*60}")
    print(f"Durée totale   : {format_duration(duration)}")
    print(f"Jours traités  : {stats['processed']}")
    print(f"Jours skippés  : {stats['skipped']}")
    print(f"Erreurs        : {stats['errors']}")
    print(f"")
    print(f"Messages parsés (total cumulé) :")
    print(f"  Add Orders   : {stats['add_orders']:>12,}")
    print(f"  Mod Orders   : {stats['mod_orders']:>12,}")
    print(f"  Del Orders   : {stats['del_orders']:>12,}")
    print(f"  Trades       : {stats['trades']:>12,}")

    if stats['processed'] > 0 and duration > 0:
        avg = duration / stats['processed']
        print(f"\nDurée moyenne  : {format_duration(avg)}/jour")

    if errors:
        print(f"\nDétail des erreurs :")
        for date_str, msg in errors:
            print(f"  {date_str}: {msg}")

    print(f"{'═'*60}")


if __name__ == '__main__':
    main()
