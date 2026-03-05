"""
HKEX OMD-D Binary Parser — Version 6 (streaming, low RAM, split par produit)

═══════════════════════════════════════════════════════════════════════════════
STRATÉGIE GÉNÉRALE DU PARSER
═══════════════════════════════════════════════════════════════════════════════

Contrainte principale : un fichier journalier MC221 (HSI/HHI) pèse ~5 GB.
Impossible de tout charger en RAM → on utilise deux techniques combinées :

1. mmap (memory-mapped file) :
   Le fichier est mappé dans l'espace d'adressage virtuel du processus.
   On y accède comme un bytes object avec indexation directe (mm[i:j]),
   mais le noyau ne charge en RAM que les pages réellement accédées.
   → Pas de file.read() qui chargerait tout en mémoire.
   → Idéal pour parser séquentiellement un fichier binaire volumineux.

2. Écriture Parquet par batch (BATCH_SIZE = 500k rows) :
   Les rows extraites sont accumulées dans des listes Python.
   Quand la liste atteint BATCH_SIZE, on la vide ("flush") vers le fichier
   Parquet via ParquetWriter.write_table().
   → RAM stable ~100 MB peu importe la taille du fichier.
   → Parquet supporte nativement l'écriture multi-batch (row groups).

Pipeline en 3 passes sur le zip journalier :
  [1] Series Definitions (MC151, MC101, MC201, MC301)
      → construit _orderbook_map : OrderbookID → ClassCode
      → doit être fait AVANT les passes order/trade

  [2] Orders + Trades (MC221, MC121)
      → lit les Add/Modify/Delete Order et Trade
      → résout le ClassCode via _orderbook_map
      → route chaque row vers le writer du bon produit via _WriterPool

  [3] Block Trades (MC167) — optionnel
      → même traitement que [2] mais pour les block trades déclarés hors marché

Canaux et produits extraits :
  MC221 → HSI futures, HHI futures (et leurs options, filtrées par futures_only)
  MC121 → MHI futures, MCH futures
  MC167 → Block trades tous produits

Convention Parquet finale (Hive-style, alignée sur CONTEXT.md) :
  data/market_data/
    product=HSI/year=2026/month=02/
      HSI_20260203_orders.parquet
      HSI_20260203_trades.parquet
    product=MHI/year=2026/month=02/
      MHI_20260203_orders.parquet
      MHI_20260203_trades.parquet
    ...

  Cette structure est reconnue nativement par PyArrow, Pandas et Spark :
    pq.read_table('data/market_data/', filters=[('product','=','HSI')])
    spark.read.parquet('data/market_data/')  # partitionnement auto-détecté

Changements vs V5 :
  - Split par produit à l'écriture via _WriterPool (lazy, un writer par produit)
  - parse_daily_zip() retourne dict[str, tuple[Path, Path]] au lieu de (Path, Path)
    ⚠️  Signature incompatible avec V5 — mettre à jour les scripts appelants.

Performance observée :
  ~45 minutes par jour de données sur une machine locale standard.
  RAM stable ~100 MB.
"""

import mmap
import logging
import tempfile
from pathlib import Path
from typing import Optional
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

from .messages import (
    RECORD_HEADER_SIZE, MESSAGE_HEADER_SIZE,
    MSG_SERIES_DEF_BASE, MSG_SERIES_DEF_EXTENDED,
    MSG_ADD_ORDER, MSG_MODIFY_ORDER, MSG_DELETE_ORDER,
    MSG_TRADE,
    RecordHeader, MessageHeader,
    SeriesDefinitionBase, SeriesDefinitionExtended,
    AddOrder, ModifyOrder, DeleteOrder, Trade,
)

logger = logging.getLogger(__name__)

# Affiche la progression tous les 10 millions de records traités
PROGRESS_INTERVAL = 10_000_000

# Nombre de rows accumulées en RAM avant flush vers Parquet.
# 500k rows × ~200 bytes/row ≈ 100 MB max en RAM.
BATCH_SIZE = 500_000


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMAS PYARROW
# ═══════════════════════════════════════════════════════════════════════════════
# Les schémas sont définis une seule fois au niveau module (pas dans __init__)
# pour deux raisons :
#   1. Performance : évite de recréer les objets à chaque parse
#   2. Cohérence : même schéma garanti pour tous les fichiers Parquet générés
#
# Choix des types :
#   int64 pour les timestamps (nanoseconds → valeurs ~1.7e18, dépasse int32)
#   int32 pour price (ticks entiers HSI ≤ 50000, int32 suffisant, moitié de int64)
#   int32 pour quantity orders (≤ quelques milliers de lots, int32 ok)
#   int64 pour quantity trades (Uint64 dans la spec, peut déborder int32 pour combos)
#   string pour event_type, class_code, side (faible cardinalité, lisible)
#   bool  pour is_printable (flag binaire)

ORDERS_SCHEMA = pa.schema([
    ('timestamp_ns', pa.int64()),    # send_time du PacketHeader (précision ~1ms)
    ('event_type',   pa.string()),   # 'A'=Add, 'M'=Modify, 'D'=Delete
    ('orderbook_id', pa.int64()),    # ID brut du carnet (utile pour debug/join)
    ('order_id',     pa.int64()),    # ID stable de l'ordre (clé de join A→M→D)
    ('class_code',   pa.string()),   # 'HSI', 'MHI', 'HHI', 'MCH'
    ('side',         pa.string()),   # 'B'=Bid, 'A'=Ask
    ('price',        pa.int32()),    # prix en ticks (décimales à appliquer en Phase 2)
    ('quantity',     pa.int32()),    # quantité en lots (0 pour les Delete)
])

TRADES_SCHEMA = pa.schema([
    ('timestamp_ns',  pa.int64()),   # send_time (heure d'arrivée du message)
    ('trade_time_ns', pa.int64()),   # trade_time du matching engine (plus précis)
    ('orderbook_id',  pa.int64()),   # ID brut du carnet
    ('trade_id',      pa.int64()),   # Match ID unique (utile pour déduplication)
    ('class_code',    pa.string()),  # 'HSI', 'MHI', etc.
    ('side',          pa.string()),  # 'B'=Buy aggressor, 'A'=Sell aggressor, '?'=N/A
    ('price',         pa.int32()),   # prix d'exécution en ticks
    ('quantity',      pa.int64()),   # quantité (int64 car Uint64 dans spec, gros combos)
    ('is_printable',  pa.bool_()),   # True = compter dans volume, False = ignorer
])


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS DE CHEMINS — Convention Hive-style
# ═══════════════════════════════════════════════════════════════════════════════

def _product_dir(base_dir: Path, class_code: str, date_str: str) -> Path:
    """
    Construit le répertoire Hive-style pour un produit et une date.

    Ex: base_dir='data/market_data', class_code='HSI', date_str='20260203'
    →   data/market_data/product=HSI/year=2026/month=02/

    Le format Hive (key=value) est la convention standard pour le partitionnement
    dans l'écosystème Parquet/Spark/Arrow. Il permet des requêtes avec predicat
    pushdown : lire uniquement les fichiers du mois voulu sans scanner tout le dossier.
    """
    year  = date_str[:4]   # '20260203' → '2026'
    month = date_str[4:6]  # '20260203' → '02'
    d = base_dir / f"product={class_code}" / f"year={year}" / f"month={month}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _parquet_paths(base_dir: Path, class_code: str,
                   date_str: str) -> tuple[Path, Path]:
    """
    Retourne (orders_path, trades_path) pour un produit et une date.

    Ex: ('data/market_data/product=HSI/year=2026/month=02/HSI_20260203_orders.parquet',
         'data/market_data/product=HSI/year=2026/month=02/HSI_20260203_trades.parquet')
    """
    d = _product_dir(base_dir, class_code, date_str)
    return (
        d / f"{class_code}_{date_str}_orders.parquet",
        d / f"{class_code}_{date_str}_trades.parquet",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# WRITERPOOL — Gestion des N writers par produit
# ═══════════════════════════════════════════════════════════════════════════════

class _WriterPool:
    """
    Gère un ensemble de ParquetWriters, un par (produit, type).

    Problème à résoudre :
      En V5, il y avait 1 writer orders + 1 writer trades pour tous les produits
      mélangés. En V6, on veut 1 fichier par produit → il faut jusqu'à 8 writers
      ouverts simultanément (4 produits × 2 types orders/trades).

    Lazy opening :
      Le writer d'un produit n'est créé qu'à la première row de ce produit.
      Avantage : si MCH n'a aucune activité ce jour-là (jour férié, marché fermé),
      aucun fichier vide MCH_YYYYMMDD_orders.parquet n'est créé sur disque.

    Batch flush :
      Chaque (produit, type) a son propre buffer (liste de dicts).
      Quand un buffer atteint BATCH_SIZE, il est converti en pa.Table et écrit
      dans le Parquet correspondant — même logique qu'en V5 mais par produit.

    Usage :
        pool = _WriterPool(base_dir, date_str)
        pool.append_order('HSI', row_dict)   # route vers HSI orders
        pool.append_trade('HSI', row_dict)   # route vers HSI trades
        pool.append_order('MHI', row_dict)   # route vers MHI orders
        paths = pool.close_all()             # flush final + fermeture
        # paths = {'HSI': (orders_path, trades_path), 'MHI': (...), ...}
    """

    def __init__(self, base_dir: Path, date_str: str):
        self._base_dir = base_dir
        self._date_str = date_str
        # Clé : (class_code, 'orders'|'trades') → writer Parquet ouvert
        self._writers: dict[tuple[str, str], pq.ParquetWriter] = {}
        # Clé : (class_code, 'orders'|'trades') → buffer de rows en attente de flush
        self._buffers: dict[tuple[str, str], list[dict]]       = {}

    def _get_writer(self, class_code: str, kind: str) -> pq.ParquetWriter:
        """
        Retourne le writer existant ou en crée un nouveau (lazy init).

        kind : 'orders' ou 'trades'

        À la première row d'un produit, cette méthode :
          1. Calcule le chemin Parquet via _parquet_paths()
          2. Ouvre un pq.ParquetWriter avec le bon schéma
          3. Initialise le buffer vide pour ce (produit, type)
        """
        key = (class_code, kind)
        if key not in self._writers:
            orders_path, trades_path = _parquet_paths(
                self._base_dir, class_code, self._date_str
            )
            schema = ORDERS_SCHEMA if kind == 'orders' else TRADES_SCHEMA
            path   = orders_path   if kind == 'orders' else trades_path
            # ParquetWriter ouvre le fichier en mode écriture séquentielle.
            # Chaque write_table() ajoute un row group dans le fichier.
            # Le schéma est fixé ici → cohérence garantie sur tous les batches.
            self._writers[key] = pq.ParquetWriter(path, schema)
            self._buffers[key] = []
        return self._writers[key]

    def _append(self, class_code: str, kind: str, row: dict) -> None:
        """
        Ajoute une row au buffer du (produit, type) et flush si BATCH_SIZE atteint.

        Le flush se déclenche après l'ajout (pas avant) : on écrit toujours
        des batches de exactement BATCH_SIZE sauf le dernier batch (flush final
        dans close_all()).
        """
        key = (class_code, kind)
        self._get_writer(class_code, kind)   # init lazy si première row
        self._buffers[key].append(row)
        if len(self._buffers[key]) >= BATCH_SIZE:
            self._flush(class_code, kind)

    def append_order(self, class_code: str, row: dict) -> None:
        """Route une row order vers le buffer du produit correspondant."""
        self._append(class_code, 'orders', row)

    def append_trade(self, class_code: str, row: dict) -> None:
        """Route une row trade vers le buffer du produit correspondant."""
        self._append(class_code, 'trades', row)

    def _flush(self, class_code: str, kind: str) -> None:
        """
        Écrit le buffer courant dans le Parquet et vide la liste.

        pa.Table.from_pylist(buf, schema) convertit la liste de dicts Python
        en Table Arrow columnar. write_table() l'ajoute comme row group.
        Après flush, buf = [] → la RAM est libérée (GC Python).
        """
        key = (class_code, kind)
        buf = self._buffers.get(key, [])
        if not buf:
            return
        schema = ORDERS_SCHEMA if kind == 'orders' else TRADES_SCHEMA
        self._writers[key].write_table(
            pa.Table.from_pylist(buf, schema=schema)
        )
        self._buffers[key] = []

    def flush_all(self) -> None:
        """Vide tous les buffers. Appelé par close_all() avant fermeture des writers."""
        for class_code, kind in list(self._writers.keys()):
            self._flush(class_code, kind)

    def close_all(self) -> dict[str, tuple[Path, Path]]:
        """
        Flush final + fermeture de tous les writers ouverts.

        IMPORTANT : sans close(), le footer Parquet n'est pas écrit et les
        fichiers seraient illisibles. Cette méthode doit toujours être appelée
        en fin de parsing, même en cas d'interruption (idéalement try/finally).

        Retourne dict[class_code → (orders_path, trades_path)].
        Seuls les produits ayant reçu au moins une row sont présents dans le dict
        (lazy opening → pas de fichiers vides pour les produits sans données).
        """
        self.flush_all()
        for writer in self._writers.values():
            writer.close()

        # Reconstruire le dict des chemins depuis les clés effectivement ouvertes
        products = {cc for cc, _ in self._writers}
        return {
            cc: _parquet_paths(self._base_dir, cc, self._date_str)
            for cc in products
        }


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE PRINCIPALE
# ═══════════════════════════════════════════════════════════════════════════════

class HKEXParser:
    """
    Parser pour fichiers zip journaliers HKEX FBD-NSOM.

    Paramètres
    ----------
    instruments : list[str] | None
        ClassCodes à extraire, ex: ['HSI', 'MHI', 'HHI', 'MCH'].
        Si None, extrait tous les instruments présents dans les channels.
        Les ClassCodes non reconnus (OrderbookID absent de _orderbook_map)
        sont silencieusement ignorés.
    futures_only : bool
        Si True (défaut), extrait uniquement les futures (FinancialProduct=3
        dans SeriesDefinitionBase).
        Mettre à False pour inclure options — utile si vol surface analysis.

    Attributs internes
    ------------------
    _orderbook_map : dict[int, str]
        Table de correspondance OrderbookID → ClassCode.
        Construite lors de la passe Series Definitions.
        Ex: {12345: 'HSI', 12346: 'HSI', 67890: 'MHI', ...}
        Plusieurs OrderbookIDs peuvent mapper vers le même ClassCode
        (un ClassCode = une classe de produits, avec plusieurs expirations).

    _stats : dict
        Compteurs pour monitoring et validation.
        Consultables via get_stats() après le parsing.
    """

    def __init__(self, instruments: Optional[list] = None,
                 futures_only: bool = True):
        self.instruments  = set(instruments) if instruments else None
        self.futures_only = futures_only

        # Table de correspondance principale : OrderbookID (int) → ClassCode (str)
        # dict[int, str] est O(1) en lookup — critique pour les fichiers volumineux
        # où on fait des dizaines de millions de lookups
        self._orderbook_map: dict[int, str] = {}

        self._stats = {
            'records':     0,   # total records traités (toutes passes confondues)
            'add_orders':  0,   # messages Add Order (330) retenus
            'mod_orders':  0,   # messages Modify Order (331) retenus
            'del_orders':  0,   # messages Delete Order (332) retenus
            'trades':      0,   # messages Trade (350) retenus
            'series_defs': 0,   # Series Definitions chargées dans _orderbook_map
            'skipped':     0,   # messages ignorés (OrderbookID hors instruments)
        }

    # ── API publique ───────────────────────────────────────────────────────────

    def parse_daily_zip(
        self,
        zip_path: str | Path,
        base_dir: str | Path = 'data/market_data',
    ) -> dict[str, tuple[Path, Path]]:
        """
        Parse un fichier zip journalier FBD-NSOM complet.

        Paramètres
        ----------
        zip_path : str | Path
            Chemin vers le fichier zip, ex: 'FBD-NSOM_20260203.zip'
            Le nom est utilisé pour extraire la date (date_str = stem[-8:]).
        base_dir : str | Path
            Racine de l'arborescence Parquet.
            Défaut : 'data/market_data' (relatif au répertoire courant).
            Les sous-dossiers product=/year=/month=/ sont créés automatiquement.

        Retourne
        --------
        dict[str, tuple[Path, Path]]
            { 'HSI': (orders_path, trades_path),
              'MHI': (orders_path, trades_path), ... }
            Un produit sans données ce jour sera absent du dict.

        Notes
        -----
        Les fichiers binaires extraits du zip transitent par un répertoire
        temporaire (tempfile.TemporaryDirectory) qui est automatiquement
        supprimé à la fin via le context manager 'with'.
        Les Parquet sont écrits directement dans base_dir/product=X/...
        qui survit à la fin du bloc.
        """
        zip_path = Path(zip_path)
        base_dir = Path(base_dir)

        # Extraire la date depuis le nom du fichier : 'FBD-NSOM_20260203.zip' → '20260203'
        # stem = nom sans extension, split('_')[-1] = dernier segment après underscore
        date_str = zip_path.stem.split('_')[-1]
        print(f"\nParsing {zip_path.name}  →  {base_dir}")

        # tempfile.TemporaryDirectory() crée un dossier temporaire unique
        # et le supprime automatiquement à la sortie du bloc 'with'
        # → propre, pas de fichiers intermédiaires laissés sur disque
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # ── Extraction des channels depuis le zip ──────────────────────────
            with zipfile.ZipFile(zip_path) as zf:
                all_names = zf.namelist()

                def extract(prefix: str) -> Optional[Path]:
                    """
                    Extrait le premier fichier dont le nom contient 'prefix'.
                    Retourne None si absent ou si le fichier est trop petit
                    (< 100 bytes → fichier vide ou header seulement).
                    Cette vérification évite de tenter de parser des channels
                    vides qui existent dans le zip mais ne contiennent pas de données.
                    """
                    matches = [n for n in all_names if prefix in n]
                    if not matches:
                        return None
                    zf.extract(matches[0], tmpdir)
                    p = tmpdir / matches[0]
                    return p if p.stat().st_size > 100 else None

                # Channels Series Definitions (mapping OrderbookID → ClassCode)
                mc151 = extract('MC151')   # SeriesDefExtended (304) principalement
                mc101 = extract('MC101')   # SeriesDefBase (303) pour produits dérivés
                mc201 = extract('MC201')   # SeriesDefBase (303) pour autres marchés
                mc301 = extract('MC301')   # SeriesDefBase (303) pour marchés additionnels

                # Channels Market Data
                mc121 = extract('MC121')   # MHI + MCH futures/options
                mc221 = extract('MC221')   # HSI + HHI futures/options
                mc167 = extract('MC167')   # Block Trades (tous produits)

            # ── Passe 1 : Series Definitions ──────────────────────────────────
            # OBLIGATOIRE avant la passe orders/trades.
            # Sans _orderbook_map, impossible de savoir à quel produit correspondent
            # les OrderbookIDs dans les messages order/trade.
            print(f"  [1/3] Series Definitions...")
            total_before = 0
            for ch_file, ch_name in [(mc151,'MC151'), (mc101,'MC101'),
                                     (mc201,'MC201'), (mc301,'MC301')]:
                if ch_file:
                    self._parse_series_defs(ch_file)
                    # Calcul du delta pour afficher combien chaque channel a contribué
                    added = self._stats['series_defs'] - total_before
                    total_before = self._stats['series_defs']
                    print(f"        {ch_name}: +{added:,}")

            # Résumé de la mapping table par produit
            by_product: dict[str, int] = {}
            for cc in self._orderbook_map.values():
                by_product[cc] = by_product.get(cc, 0) + 1
            summary = '  '.join(f"{k}:{v}" for k, v in sorted(by_product.items()))
            print(f"        Total: {self._stats['series_defs']:,} séries  [{summary}]")

            # ── Initialisation du WriterPool ───────────────────────────────────
            # Le pool gère les N writers (un par produit × type) de façon lazy.
            # Avantage vs V5 : pas de fichiers vides pour les produits absents,
            # et chaque produit a son propre fichier Parquet immédiatement utilisable
            # sans filtre supplémentaire à la lecture.
            pool = _WriterPool(base_dir, date_str)

            # ── Passe 2 : Orders + Trades ──────────────────────────────────────
            # MC221 d'abord (HSI/HHI, plus gros), puis MC121 (MHI/MCH)
            print(f"  [2/3] Orders + Trades...")
            for ch_file, ch_name in [(mc221, 'MC221'), (mc121, 'MC121')]:
                if ch_file:
                    sz = ch_file.stat().st_size
                    print(f"        {ch_name} ({sz/1e9:.2f} GB)...")
                    self._parse_orders_trades(ch_file, pool)
                    print(f"        → add_orders: {self._stats['add_orders']:,}"
                          f"  trades: {self._stats['trades']:,}")

            # ── Passe 3 : Block Trades (optionnel) ────────────────────────────
            if mc167:
                sz = mc167.stat().st_size
                print(f"        MC167 block trades ({sz/1e6:.0f} MB)...")
                self._parse_orders_trades(mc167, pool)

            # Fermeture des writers → flush final des buffers + close() de chaque writer.
            # IMPORTANT : sans close(), le footer Parquet n'est pas écrit et les
            # fichiers seraient illisibles. close_all() retourne le dict des chemins.
            result = pool.close_all()

        # Résumé des fichiers générés
        print(f"  ✅ {len(result)} produit(s) extraits:")
        for cc, (op, tp) in sorted(result.items()):
            o_mb = op.stat().st_size / 1e6 if op and op.exists() else 0
            t_mb = tp.stat().st_size / 1e6 if tp and tp.exists() else 0
            print(f"        {cc}: orders={o_mb:.0f}MB  trades={t_mb:.0f}MB")
        print(f"  Stats: {self._stats}")

        return result

    def get_stats(self) -> dict:
        """
        Retourne une copie des statistiques de parsing.
        .copy() pour éviter que l'appelant ne modifie les stats internes.
        Utile pour validation post-parsing et monitoring.
        """
        return self._stats.copy()

    def reset(self) -> None:
        """
        Remet à zéro les stats et la mapping table.

        En production, il est préférable de créer une nouvelle instance par jour
        (le pipeline batch le fait systématiquement) pour éviter tout effet de bord.
        Cette méthode est principalement utile dans les tests unitaires.
        """
        self._orderbook_map.clear()
        for k in self._stats:
            self._stats[k] = 0

    # ═══════════════════════════════════════════════════════════════════════════
    # PASSE SERIES DEFINITIONS
    # ═══════════════════════════════════════════════════════════════════════════

    def _parse_series_defs(self, filepath: Path) -> None:
        """
        Lit un channel de Series Definitions et peuple _orderbook_map.

        Stratégie de robustesse :
          Si un record a une taille invalide (total < RECORD_HEADER_SIZE ou
          dépasse la fin du fichier), on avance d'un byte et on réessaie.
          Cette "byte-level recovery" permet de survivre à des corruptions
          ponctuelles sans crasher tout le parsing.

        Deux types de messages gérés :
          303 (SeriesDefBase)     → source préférée (contient FinancialProduct)
          304 (SeriesDefExtended) → fallback, enregistré sans filtrage futures
        """
        file_size = filepath.stat().st_size

        # mmap.mmap(fileno, length=0, access=READ) :
        #   length=0 → mappe le fichier entier
        #   ACCESS_READ → lecture seule (protection contre écriture accidentelle)
        # Le résultat 'mm' se comporte comme un bytes object mais n'est pas en RAM
        with open(filepath, 'rb') as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                offset = 0

                while offset + RECORD_HEADER_SIZE <= file_size:
                    # Lire le header du record (18 bytes)
                    rec_hdr = RecordHeader.from_bytes(
                        mm[offset: offset + RECORD_HEADER_SIZE]
                    )
                    total = rec_hdr.total_record_size  # RecLen + 2

                    # Validation de la taille du record
                    # Un record trop petit ou qui dépasse la fin du fichier
                    # indique une corruption → skip d'un byte et retry
                    if total < RECORD_HEADER_SIZE or offset + total > file_size:
                        offset += 1
                        continue

                    # Offset du premier message dans ce record (après les 18 bytes header)
                    msg_offset = offset + RECORD_HEADER_SIZE

                    # Itérer sur les msg_count messages du record
                    for _ in range(rec_hdr.msg_count):
                        # Vérification qu'il reste assez de place pour un MessageHeader
                        if msg_offset + MESSAGE_HEADER_SIZE > offset + total:
                            break

                        msg_hdr = MessageHeader.from_bytes(
                            mm[msg_offset: msg_offset + MESSAGE_HEADER_SIZE]
                        )

                        # Un message doit au moins contenir son propre header (4 bytes)
                        if msg_hdr.msg_size < MESSAGE_HEADER_SIZE:
                            break

                        # Fin du message = début + taille totale
                        msg_end = msg_offset + msg_hdr.msg_size
                        if msg_end > offset + total:
                            break

                        # Payload = tout ce qui suit le MessageHeader
                        payload = mm[msg_offset + MESSAGE_HEADER_SIZE: msg_end]
                        mtype   = msg_hdr.msg_type

                        try:
                            if mtype == MSG_SERIES_DEF_BASE:
                                # Message 303 : source principale, contient FinancialProduct
                                s = SeriesDefinitionBase.from_payload(payload)
                                include = s.orderbook_id > 0 and (
                                    not self.futures_only or s.is_future
                                )
                                if include:
                                    self._orderbook_map[s.orderbook_id] = s.class_code
                                    self._stats['series_defs'] += 1

                            elif mtype == MSG_SERIES_DEF_EXTENDED:
                                # Message 304 : fallback, pas de filtrage par type
                                # (FinancialProduct absent) — on enregistre si ID valide
                                s = SeriesDefinitionExtended.from_payload(payload)
                                if s.orderbook_id > 0:
                                    self._orderbook_map[s.orderbook_id] = s.class_code
                                    self._stats['series_defs'] += 1

                        except Exception:
                            # Silencieux : un payload malformé est ignoré
                            # sans interrompre le parsing du reste du fichier
                            pass

                        # Avancer au message suivant dans ce record
                        msg_offset = msg_end

                    # Avancer au record suivant dans le fichier
                    offset += total

    # ═══════════════════════════════════════════════════════════════════════════
    # PASSE ORDERS + TRADES (streaming)
    # ═══════════════════════════════════════════════════════════════════════════

    def _parse_orders_trades(self, filepath: Path, pool: _WriterPool) -> None:
        """
        Parse un channel de market data et route les rows dans le _WriterPool.

        Différences vs V5 (qui prenait deux writers en paramètre) :
          - Reçoit un _WriterPool au lieu de (orders_writer, trades_writer)
          - La row est construite par _make_*_row() qui retourne son class_code
          - Le pool route automatiquement chaque row vers le bon fichier Parquet
            selon le class_code : pool.append_order('HSI', row) → HSI orders
          - Le pool gère les flushes par batch et l'ouverture lazy des writers

        Stratégie de progression :
          Affichage tous les PROGRESS_INTERVAL records (10M).
          Le pourcentage est calculé sur la taille du fichier courant.
        """
        file_size     = filepath.stat().st_size
        last_progress = 0

        with open(filepath, 'rb') as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                offset = 0

                while offset + RECORD_HEADER_SIZE <= file_size:
                    self._stats['records'] += 1

                    # Affichage de progression périodique
                    if self._stats['records'] - last_progress >= PROGRESS_INTERVAL:
                        pct = offset / file_size * 100
                        print(f"        {pct:.1f}%")
                        last_progress = self._stats['records']

                    rec_hdr = RecordHeader.from_bytes(
                        mm[offset: offset + RECORD_HEADER_SIZE]
                    )
                    total = rec_hdr.total_record_size

                    # Même stratégie de robustesse que dans _parse_series_defs
                    if total < RECORD_HEADER_SIZE or offset + total > file_size:
                        offset += 1
                        continue

                    # send_time : timestamp du PacketHeader, partagé par tous
                    # les messages de ce record (précision ~1ms pour HKEX)
                    send_time  = rec_hdr.send_time
                    msg_offset = offset + RECORD_HEADER_SIZE

                    for _ in range(rec_hdr.msg_count):
                        if msg_offset + MESSAGE_HEADER_SIZE > offset + total:
                            break

                        msg_hdr = MessageHeader.from_bytes(
                            mm[msg_offset: msg_offset + MESSAGE_HEADER_SIZE]
                        )
                        if msg_hdr.msg_size < MESSAGE_HEADER_SIZE:
                            break
                        msg_end = msg_offset + msg_hdr.msg_size
                        if msg_end > offset + total:
                            break

                        # Payload = bytes entre fin du header et fin du message
                        payload = mm[msg_offset + MESSAGE_HEADER_SIZE: msg_end]
                        mtype   = msg_hdr.msg_type

                        # Dispatch par type de message.
                        # Les méthodes _make_*_row retournent None si l'instrument
                        # est hors scope → pas d'accumulation de rows inutiles.
                        # Le class_code est dans la row → pool route vers le bon fichier.
                        if mtype == MSG_ADD_ORDER:
                            row = self._make_order_row(payload, send_time, 'A')
                            if row:
                                pool.append_order(row['class_code'], row)
                        elif mtype == MSG_MODIFY_ORDER:
                            row = self._make_order_row(payload, send_time, 'M')
                            if row:
                                pool.append_order(row['class_code'], row)
                        elif mtype == MSG_DELETE_ORDER:
                            row = self._make_delete_row(payload, send_time)
                            if row:
                                pool.append_order(row['class_code'], row)
                        elif mtype == MSG_TRADE:
                            row = self._make_trade_row(payload, send_time)
                            if row:
                                pool.append_trade(row['class_code'], row)
                        # Les autres types (100, 301-305, 320, 335, 364...)
                        # sont silencieusement ignorés

                        msg_offset = msg_end
                    offset += total

    # ═══════════════════════════════════════════════════════════════════════════
    # ROW BUILDERS — Constructeurs de lignes Parquet
    # ═══════════════════════════════════════════════════════════════════════════

    def _should_include(self, orderbook_id: int) -> tuple[bool, str]:
        """
        Détermine si un OrderbookID doit être inclus dans le output.

        Retourne (include: bool, class_code: str).

        Logique :
          1. Cherche le class_code dans _orderbook_map
             → '' si OrderbookID inconnu (Series Def non chargée)
          2. Un class_code vide est toujours exclu (jamais dans self.instruments)
          3. Si self.instruments is None → inclure tout ce qui est mappé
          4. Sinon → inclure seulement si class_code ∈ instruments

        Note : un class_code vide ('') ne sera jamais dans self.instruments
        (si instruments=['HSI','MHI',...]) → les OrderbookIDs inconnus sont
        automatiquement skippés.
        """
        class_code = self._orderbook_map.get(orderbook_id, '')
        if not class_code:
            # OrderbookID absent de la map → Series Def non reçue ou hors scope
            self._stats['skipped'] += 1
            return False, ''
        if self.instruments is None or class_code in self.instruments:
            return True, class_code
        # Incrémenter le compteur pour monitoring
        # (permet de détecter si beaucoup d'instruments sont inconnus)
        self._stats['skipped'] += 1
        return False, class_code

    def _make_order_row(self, payload: bytes, send_time: int,
                        event_type: str) -> Optional[dict]:
        """
        Construit un dict (row Parquet) pour un Add ou Modify Order.

        event_type : 'A' pour Add (330), 'M' pour Modify (331).
        Les deux messages ont le même layout de payload → même parseur,
        seul event_type diffère.

        Retourne None si :
          - L'instrument est hors scope (not include)
          - Une exception de parsing est levée (payload malformé)

        Le try/except silencieux est intentionnel : dans un fichier de
        plusieurs GB avec des millions de messages, quelques messages
        malformés ne doivent pas interrompre le parsing.
        """
        try:
            msg = (AddOrder if event_type == 'A' else ModifyOrder).from_payload(
                payload, send_time
            )
            include, class_code = self._should_include(msg.orderbook_id)
            if not include:
                return None

            # Incrémenter le bon compteur selon le type
            self._stats['add_orders' if event_type == 'A' else 'mod_orders'] += 1

            return {
                'timestamp_ns': send_time,
                'event_type':   event_type,
                'orderbook_id': msg.orderbook_id,
                'order_id':     msg.order_id,
                'class_code':   class_code,
                'side':         msg.side_str,
                'price':        msg.price,
                'quantity':     msg.quantity,
            }
        except Exception:
            return None

    def _make_delete_row(self, payload: bytes, send_time: int) -> Optional[dict]:
        """
        Construit un dict pour un Delete Order (332).

        Price et Quantity sont mis à 0 car absents du message Delete.
        Pour retrouver ces informations, il faut faire un join sur order_id
        avec le Add Order correspondant lors de la reconstruction LOB.

        event_type = 'D'
        """
        try:
            msg = DeleteOrder.from_payload(payload, send_time)
            include, class_code = self._should_include(msg.orderbook_id)
            if not include:
                return None

            self._stats['del_orders'] += 1
            return {
                'timestamp_ns': send_time,
                'event_type':   'D',
                'orderbook_id': msg.orderbook_id,
                'order_id':     msg.order_id,
                'class_code':   class_code,
                'side':         msg.side_str,
                'price':        0,    # absent du message Delete
                'quantity':     0,    # absent du message Delete
            }
        except Exception:
            return None

    def _make_trade_row(self, payload: bytes, send_time: int) -> Optional[dict]:
        """
        Construit un dict pour un Trade (350).

        Points importants :
          - trade_time_ns (champ TradeTime du payload) est préférable à
            timestamp_ns (send_time) pour l'analyse temporelle précise,
            car il vient du matching engine (précision ~10ms) vs ~1ms pour send_time.
            Les deux sont conservés dans le Parquet pour flexibilité.

          - is_printable : filtrer True pour compter le volume réel.
            Les trades non-printable sont des parties de combo ou reported trades
            qui causeraient un double-comptage si inclus dans le volume.

          - side : 'B'=Buy aggressor, 'A'=Sell aggressor.
            Utile pour le calcul du Order Flow Imbalance (OFI) et VPIN.
        """
        try:
            msg = Trade.from_payload(payload, send_time)
            include, class_code = self._should_include(msg.orderbook_id)
            if not include:
                return None

            self._stats['trades'] += 1
            return {
                'timestamp_ns':  send_time,
                'trade_time_ns': msg.trade_time,   # timestamp matching engine
                'orderbook_id':  msg.orderbook_id,
                'trade_id':      msg.trade_id,
                'class_code':    class_code,
                'side':          msg.side_str,
                'price':         msg.price,
                'quantity':      msg.quantity,
                'is_printable':  msg.is_printable,
            }
        except Exception:
            return None