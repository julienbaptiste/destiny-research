"""
Tests unitaires — hkex_parser
Usage : pytest tests/test_hkex_parser.py -v

Organisation :
  TestMessages        — désérialisation binaire (messages.py)
  TestParserInternals — logique _should_include, row builders, _WriterPool
  TestParquetPaths    — convention de nommage Hive-style
  TestPipelineE2E     — pipeline complet sur données synthétiques (sans vrai zip)

Stratégie de test sans données réelles :
  Les tests E2E construisent des fichiers binaires synthétiques au format
  OMD-D exact (RecLen + PacketHeader + MessageHeader + payload), les injectent
  dans un zip temporaire, et vérifient les Parquet générés.
  Cela valide tout le pipeline sans dépendre d'un fichier HKEX réel.
"""

import struct
import tempfile
import zipfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest

# ── Import du package ──────────────────────────────────────────────────────────
# On ajoute le parent au path si nécessaire (exécution depuis la racine du repo)
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from hkex_parser.messages import (
    RecordHeader, MessageHeader,
    SeriesDefinitionBase, SeriesDefinitionExtended,
    AddOrder, ModifyOrder, DeleteOrder, Trade,
    RECORD_HEADER_SIZE, MESSAGE_HEADER_SIZE,
    MSG_SERIES_DEF_BASE, MSG_SERIES_DEF_EXTENDED,
    MSG_ADD_ORDER, MSG_MODIFY_ORDER, MSG_DELETE_ORDER,
    MSG_TRADE,
    SIDE_BID, SIDE_OFFER,
)
from hkex_parser.parser import (
    HKEXParser, _WriterPool, _parquet_paths, _product_dir,
    ORDERS_SCHEMA, TRADES_SCHEMA,
)


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS — Construction de bytes binaires au format OMD-D
# ═══════════════════════════════════════════════════════════════════════════════

def _pack_record_header(msg_count: int, send_time: int,
                        messages_bytes: bytes) -> bytes:
    """
    Construit un record OMD-D complet (RecordHeader + messages).

    RecLen = 16 (PacketHeader) + len(messages_bytes)   [EXCLU les 2 bytes RecLen]
    PktSize = 2 (PktSize field) + 1 + 1 + 4 + 8 = 16  [INCLUS ses 2 bytes]

    Encodage :
      RecLen   : Big Endian (validé empiriquement)
      PktSize  : Big Endian
      MsgCount : Little Endian (1 byte, pas de différence)
      SeqNum   : Little Endian
      SendTime : Little Endian
    """
    rec_len  = 16 + len(messages_bytes)  # PacketHeader(16) + messages
    pkt_size = 16                         # taille du PacketHeader incluant PktSize
    seq_num  = 1

    header = (
        struct.pack('>H', rec_len)    +  # RecLen BE
        struct.pack('>H', pkt_size)   +  # PktSize BE
        struct.pack('<B', msg_count)  +  # MsgCount LE
        b'\x00'                       +  # Filler
        struct.pack('<I', seq_num)    +  # SeqNum LE
        struct.pack('<Q', send_time)     # SendTime LE
    )
    return header + messages_bytes


def _pack_message(msg_type: int, payload: bytes) -> bytes:
    """
    Construit un MessageHeader + payload.
    MsgSize = 4 (header) + len(payload)
    """
    msg_size = MESSAGE_HEADER_SIZE + len(payload)
    return (
        struct.pack('<H', msg_size) +
        struct.pack('<H', msg_type) +
        payload
    )


def _pack_series_def_base(orderbook_id: int, symbol: str,
                          financial_product: int = 3) -> bytes:
    """
    Payload message 303 — Series Definition Base (60 bytes).
    financial_product=3 → Future (valeur par défaut).
    """
    symbol_bytes = symbol.encode('ascii').ljust(32, b' ')[:32]
    payload = (
        struct.pack('<I', orderbook_id)   +  # [0-3]  OrderbookID
        symbol_bytes                       +  # [4-35] Symbol (32 bytes)
        struct.pack('<B', financial_product) +  # [36]   FinancialProduct
        b'\x00' * 23                          # [37-59] reste du payload (padding)
    )
    return payload


def _pack_add_order(orderbook_id: int, order_id: int, price: int,
                    quantity: int, side: int) -> bytes:
    """Payload message 330 — Add Order (28 bytes de payload)."""
    return (
        struct.pack('<I', orderbook_id)  +  # [0-3]   OrderbookID
        struct.pack('<Q', order_id)      +  # [4-11]  OrderID
        struct.pack('<i', price)         +  # [12-15] Price (signé)
        struct.pack('<I', quantity)      +  # [16-19] Quantity
        struct.pack('<B', side)          +  # [20]    Side
        b'\x00' * 7                         # [21-27] reste (LotType, OrderType, Position)
    )


def _pack_delete_order(orderbook_id: int, order_id: int, side: int) -> bytes:
    """Payload message 332 — Delete Order (14 bytes de payload)."""
    return (
        struct.pack('<I', orderbook_id) +  # [0-3]  OrderbookID
        struct.pack('<Q', order_id)     +  # [4-11] OrderID
        struct.pack('<B', side)         +  # [12]   Side
        b'\x00'                            # [13]   Filler
    )


def _pack_trade(orderbook_id: int, order_id: int, price: int,
                trade_id: int, side: int, quantity: int,
                trade_time: int, deal_type: int = 1) -> bytes:
    """Payload message 350 — Trade (52 bytes de payload)."""
    return (
        struct.pack('<I', orderbook_id)  +  # [0-3]   OrderbookID
        struct.pack('<Q', order_id)      +  # [4-11]  OrderID
        struct.pack('<i', price)         +  # [12-15] Price
        struct.pack('<Q', trade_id)      +  # [16-23] TradeID
        struct.pack('<I', 0)             +  # [24-27] ComboGroupID
        struct.pack('<B', side)          +  # [28]    Side
        struct.pack('<B', deal_type)     +  # [29]    DealType
        struct.pack('<H', 0)             +  # [30-31] TradeCondition
        struct.pack('<H', 0)             +  # [32-33] DealInfo
        b'\x00\x00'                      +  # [34-35] Filler
        struct.pack('<Q', quantity)      +  # [36-43] Quantity
        struct.pack('<Q', trade_time)       # [44-51] TradeTime
    )


def _build_series_def_channel(entries: list[tuple[int, str, int]]) -> bytes:
    """
    Construit un channel binaire avec des messages Series Definition Base (303).
    entries : liste de (orderbook_id, symbol, financial_product)
    """
    data = b''
    for ob_id, sym, fp in entries:
        payload = _pack_series_def_base(ob_id, sym, fp)
        msg     = _pack_message(MSG_SERIES_DEF_BASE, payload)
        data   += _pack_record_header(1, 0, msg)
    return data


def _build_market_data_channel(events: list[dict], send_time: int = 1_000_000_000) -> bytes:
    """
    Construit un channel binaire avec des messages order/trade.
    events : liste de dicts avec clé 'type' + champs spécifiques au message.
    """
    data = b''
    for ev in events:
        t = ev['type']
        if t == 'add':
            payload = _pack_add_order(
                ev['orderbook_id'], ev['order_id'], ev['price'],
                ev['quantity'], ev['side']
            )
            msg = _pack_message(MSG_ADD_ORDER, payload)
        elif t == 'delete':
            payload = _pack_delete_order(
                ev['orderbook_id'], ev['order_id'], ev['side']
            )
            msg = _pack_message(MSG_DELETE_ORDER, payload)
        elif t == 'trade':
            payload = _pack_trade(
                ev['orderbook_id'], ev.get('order_id', 0), ev['price'],
                ev['trade_id'], ev['side'], ev['quantity'],
                ev.get('trade_time', send_time + 1000),
                ev.get('deal_type', 1),
            )
            msg = _pack_message(MSG_TRADE, payload)
        else:
            continue
        data += _pack_record_header(1, send_time, msg)
    return data


def _build_zip(tmpdir: Path, date_str: str,
               mc101_data: bytes, mc221_data: bytes,
               mc121_data: bytes = b'') -> Path:
    """
    Crée un zip FBD-NSOM synthétique dans tmpdir.
    Retourne le chemin du zip.
    """
    zip_path = tmpdir / f"FBD-NSOM_{date_str}.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        if mc101_data:
            zf.writestr(f"MC101_{date_str}.bin", mc101_data)
        if mc221_data:
            zf.writestr(f"MC221_{date_str}.bin", mc221_data)
        if mc121_data:
            zf.writestr(f"MC121_{date_str}.bin", mc121_data)
    return zip_path


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS MESSAGES.PY — Désérialisation binaire
# ═══════════════════════════════════════════════════════════════════════════════

class TestMessages:
    """Valide que les dataclasses désérialisent correctement les bytes OMD-D."""

    def test_record_header_sizes(self):
        """RECORD_HEADER_SIZE = 2 (RecLen) + 16 (PacketHeader) = 18."""
        assert RECORD_HEADER_SIZE == 18
        assert MESSAGE_HEADER_SIZE == 4

    def test_record_header_from_bytes(self):
        """RecordHeader.from_bytes décode correctement les champs."""
        send_time = 1_700_000_000_000_000_000  # ~2023 en nanosecondes
        seq_num   = 42
        rec_len   = 100
        pkt_size  = 16

        data = (
            struct.pack('>H', rec_len)   +  # RecLen BE
            struct.pack('>H', pkt_size)  +  # PktSize BE
            struct.pack('<B', 3)         +  # MsgCount
            b'\x00'                      +  # Filler
            struct.pack('<I', seq_num)   +  # SeqNum LE
            struct.pack('<Q', send_time)    # SendTime LE
        )
        hdr = RecordHeader.from_bytes(data)

        assert hdr.rec_len   == rec_len
        assert hdr.pkt_size  == pkt_size
        assert hdr.msg_count == 3
        assert hdr.seq_num   == seq_num
        assert hdr.send_time == send_time

    def test_record_header_properties(self):
        """total_record_size = rec_len + 2 ; messages_start = 18."""
        data = struct.pack('>H', 50) + b'\x00' * 16
        hdr  = RecordHeader.from_bytes(data)
        assert hdr.total_record_size == 52   # 50 + 2
        assert hdr.messages_start    == 18

    def test_message_header_from_bytes(self):
        """MessageHeader décode MsgSize et MsgType en Little Endian."""
        data = struct.pack('<H', 32) + struct.pack('<H', 330)
        hdr  = MessageHeader.from_bytes(data)
        assert hdr.msg_size == 32
        assert hdr.msg_type == 330

    def test_series_def_base_future(self):
        """SeriesDefinitionBase avec FinancialProduct=3 → is_future=True."""
        payload = _pack_series_def_base(12345, 'HSIH6', financial_product=3)
        s = SeriesDefinitionBase.from_payload(payload)

        assert s.orderbook_id      == 12345
        assert s.symbol            == 'HSIH6'
        assert s.class_code        == 'HSI'
        assert s.financial_product == 3
        assert s.is_future         is True

    def test_series_def_base_option(self):
        """SeriesDefinitionBase avec FinancialProduct=1 → is_future=False."""
        payload = _pack_series_def_base(99999, 'HSIC5', financial_product=1)
        s = SeriesDefinitionBase.from_payload(payload)
        assert s.is_future is False

    def test_series_def_base_class_code_extraction(self):
        """ClassCode = 3 premiers caractères du Symbol."""
        for symbol, expected_cc in [('MHIM6', 'MHI'), ('HHIH6', 'HHI'), ('MCHZ5', 'MCH')]:
            payload = _pack_series_def_base(1, symbol)
            s = SeriesDefinitionBase.from_payload(payload)
            assert s.class_code == expected_cc, f"Échec pour {symbol}"

    def test_add_order_deserialization(self):
        """AddOrder décode tous les champs correctement."""
        payload = _pack_add_order(
            orderbook_id=12345, order_id=9876543210,
            price=24000, quantity=10, side=SIDE_BID
        )
        msg = AddOrder.from_payload(payload, send_time=1_000_000_000)

        assert msg.orderbook_id == 12345
        assert msg.order_id     == 9876543210
        assert msg.price        == 24000
        assert msg.quantity     == 10
        assert msg.side         == SIDE_BID
        assert msg.side_str     == 'B'

    def test_add_order_offer_side(self):
        """Side=SIDE_OFFER → side_str='A'."""
        payload = _pack_add_order(1, 1, 100, 5, SIDE_OFFER)
        msg = AddOrder.from_payload(payload, send_time=0)
        assert msg.side_str == 'A'

    def test_add_order_negative_price(self):
        """Price est un Int32 signé — les prix négatifs doivent être acceptés."""
        payload = _pack_add_order(1, 1, -100, 1, SIDE_BID)
        msg = AddOrder.from_payload(payload, send_time=0)
        assert msg.price == -100

    def test_delete_order_no_price(self):
        """DeleteOrder n'a pas de price/quantity dans le payload."""
        payload = _pack_delete_order(12345, 9876543210, SIDE_BID)
        msg = DeleteOrder.from_payload(payload, send_time=500)

        assert msg.orderbook_id == 12345
        assert msg.order_id     == 9876543210
        assert msg.side_str     == 'B'

    def test_trade_printable(self):
        """Trade avec DealType=1 (bit 0 = 1) → is_printable=True."""
        payload = _pack_trade(1, 0, 24000, 777, 2, 5,
                              trade_time=1_000_001_000, deal_type=1)
        msg = Trade.from_payload(payload, send_time=1_000_000_000)

        assert msg.price        == 24000
        assert msg.quantity     == 5
        assert msg.trade_id     == 777
        assert msg.is_printable is True
        assert msg.side_str     == 'B'   # 2=Buy aggressor

    def test_trade_not_printable(self):
        """Trade avec DealType=2 (bit 0 = 0) → is_printable=False."""
        payload = _pack_trade(1, 0, 24000, 778, 3, 5,
                              trade_time=1_000_001_000, deal_type=2)
        msg = Trade.from_payload(payload, send_time=1_000_000_000)
        assert msg.is_printable is False
        assert msg.side_str     == 'A'   # 3=Sell aggressor

    def test_trade_side_unknown(self):
        """Trade avec Side=0 → side_str='?'."""
        payload = _pack_trade(1, 0, 100, 1, 0, 1, 0)
        msg = Trade.from_payload(payload, send_time=0)
        assert msg.side_str == '?'


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS PARSER INTERNALS — Logique interne sans I/O fichier
# ═══════════════════════════════════════════════════════════════════════════════

class TestParserInternals:
    """Tests de la logique _should_include et des row builders."""

    def _make_parser_with_map(self, orderbook_map: dict,
                               instruments=None) -> HKEXParser:
        """Crée un parser avec une _orderbook_map pré-peuplée."""
        p = HKEXParser(instruments=instruments)
        p._orderbook_map = orderbook_map
        return p

    # ── _should_include ────────────────────────────────────────────────────────

    def test_should_include_known_instrument(self):
        """Un OrderbookID mappé vers un instrument dans la liste → inclus."""
        parser = self._make_parser_with_map({1: 'HSI'}, instruments=['HSI'])
        include, cc = parser._should_include(1)
        assert include is True
        assert cc == 'HSI'

    def test_should_include_unknown_orderbook_id(self):
        """Un OrderbookID absent de la map → exclu, skipped incrémenté."""
        parser = self._make_parser_with_map({}, instruments=['HSI'])
        include, cc = parser._should_include(9999)
        assert include is False
        assert cc == ''
        assert parser._stats['skipped'] == 1

    def test_should_include_wrong_instrument(self):
        """Un OrderbookID mappé vers un instrument hors scope → exclu."""
        parser = self._make_parser_with_map({1: 'MHI'}, instruments=['HSI'])
        include, cc = parser._should_include(1)
        assert include is False
        assert parser._stats['skipped'] == 1

    def test_should_include_none_instruments(self):
        """instruments=None → inclure tous les ClassCodes mappés."""
        parser = self._make_parser_with_map({1: 'HSI', 2: 'MHI'}, instruments=None)
        assert parser._should_include(1)[0] is True
        assert parser._should_include(2)[0] is True

    # ── Row builders ──────────────────────────────────────────────────────────

    def test_make_order_row_add(self):
        """_make_order_row avec event_type='A' retourne un dict valide."""
        parser = self._make_parser_with_map({1: 'HSI'}, instruments=['HSI'])
        payload = _pack_add_order(1, 42, 24000, 5, SIDE_BID)
        row = parser._make_order_row(payload, send_time=1000, event_type='A')

        assert row is not None
        assert row['event_type']   == 'A'
        assert row['class_code']   == 'HSI'
        assert row['price']        == 24000
        assert row['quantity']     == 5
        assert row['side']         == 'B'
        assert row['order_id']     == 42
        assert row['timestamp_ns'] == 1000
        assert parser._stats['add_orders'] == 1

    def test_make_order_row_modify(self):
        """_make_order_row avec event_type='M' incrémente mod_orders."""
        parser = self._make_parser_with_map({1: 'HSI'}, instruments=['HSI'])
        payload = _pack_add_order(1, 42, 24100, 3, SIDE_BID)
        row = parser._make_order_row(payload, send_time=2000, event_type='M')

        assert row['event_type'] == 'M'
        assert parser._stats['mod_orders'] == 1
        assert parser._stats['add_orders'] == 0

    def test_make_order_row_excluded(self):
        """Row pour instrument hors scope → None."""
        parser = self._make_parser_with_map({1: 'MHI'}, instruments=['HSI'])
        payload = _pack_add_order(1, 1, 100, 1, SIDE_BID)
        row = parser._make_order_row(payload, send_time=0, event_type='A')
        assert row is None

    def test_make_delete_row(self):
        """_make_delete_row : price=0, quantity=0, event_type='D'."""
        parser = self._make_parser_with_map({1: 'HSI'}, instruments=['HSI'])
        payload = _pack_delete_order(1, 42, SIDE_OFFER)
        row = parser._make_delete_row(payload, send_time=3000)

        assert row['event_type'] == 'D'
        assert row['price']      == 0
        assert row['quantity']   == 0
        assert row['side']       == 'A'
        assert parser._stats['del_orders'] == 1

    def test_make_trade_row(self):
        """_make_trade_row : tous les champs corrects."""
        parser = self._make_parser_with_map({1: 'HSI'}, instruments=['HSI'])
        payload = _pack_trade(1, 0, 24000, 777, 2, 10,
                              trade_time=1_000_001_000, deal_type=1)
        row = parser._make_trade_row(payload, send_time=1_000_000_000)

        assert row['trade_id']       == 777
        assert row['price']          == 24000
        assert row['quantity']       == 10
        assert row['is_printable']   is True
        assert row['trade_time_ns']  == 1_000_001_000
        assert row['timestamp_ns']   == 1_000_000_000
        assert parser._stats['trades'] == 1

    def test_make_trade_row_excluded(self):
        """Trade pour instrument hors scope → None, trades non incrémenté."""
        parser = self._make_parser_with_map({1: 'MHI'}, instruments=['HSI'])
        payload = _pack_trade(1, 0, 100, 1, 2, 1, 0)
        row = parser._make_trade_row(payload, send_time=0)
        assert row is None
        assert parser._stats['trades'] == 0

    # ── reset() ───────────────────────────────────────────────────────────────

    def test_reset(self):
        """reset() remet stats et orderbook_map à zéro."""
        parser = self._make_parser_with_map({1: 'HSI', 2: 'MHI'})
        parser._stats['add_orders'] = 100
        parser._stats['trades']     = 50
        parser.reset()

        assert len(parser._orderbook_map) == 0
        assert all(v == 0 for v in parser._stats.values())


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS CONVENTION PARQUET — Chemins Hive-style
# ═══════════════════════════════════════════════════════════════════════════════

class TestParquetPaths:
    """Valide la convention de nommage et l'arborescence Hive."""

    def test_product_dir_structure(self, tmp_path):
        """_product_dir crée product=X/year=Y/month=Z."""
        d = _product_dir(tmp_path, 'HSI', '20260203')
        assert d == tmp_path / 'product=HSI' / 'year=2026' / 'month=02'
        assert d.exists()

    def test_product_dir_different_months(self, tmp_path):
        """Chaque mois a son propre sous-dossier."""
        d1 = _product_dir(tmp_path, 'HSI', '20260101')
        d2 = _product_dir(tmp_path, 'HSI', '20260201')
        assert d1 != d2
        assert d1 == tmp_path / 'product=HSI' / 'year=2026' / 'month=01'
        assert d2 == tmp_path / 'product=HSI' / 'year=2026' / 'month=02'

    def test_parquet_paths_naming(self, tmp_path):
        """_parquet_paths retourne les bons noms de fichiers."""
        orders_path, trades_path = _parquet_paths(tmp_path, 'HSI', '20260203')
        assert orders_path.name == 'HSI_20260203_orders.parquet'
        assert trades_path.name == 'HSI_20260203_trades.parquet'

    def test_parquet_paths_in_correct_dir(self, tmp_path):
        """Les fichiers sont dans le bon répertoire Hive."""
        orders_path, trades_path = _parquet_paths(tmp_path, 'MHI', '20260315')
        expected_dir = tmp_path / 'product=MHI' / 'year=2026' / 'month=03'
        assert orders_path.parent == expected_dir
        assert trades_path.parent == expected_dir

    def test_parquet_paths_different_products(self, tmp_path):
        """HSI et MHI ont des chemins distincts."""
        hsi_orders, _ = _parquet_paths(tmp_path, 'HSI', '20260203')
        mhi_orders, _ = _parquet_paths(tmp_path, 'MHI', '20260203')
        assert hsi_orders != mhi_orders
        assert 'product=HSI' in str(hsi_orders)
        assert 'product=MHI' in str(mhi_orders)


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS WRITERPOOL — Gestion des writers par produit
# ═══════════════════════════════════════════════════════════════════════════════

class TestWriterPool:
    """Valide l'ouverture lazy, le routing par produit, et le flush."""

    def _make_order_row(self, class_code: str, order_id: int = 1) -> dict:
        return {
            'timestamp_ns': 1000, 'event_type': 'A',
            'orderbook_id': 1, 'order_id': order_id,
            'class_code': class_code, 'side': 'B',
            'price': 24000, 'quantity': 5,
        }

    def _make_trade_row(self, class_code: str, trade_id: int = 1) -> dict:
        return {
            'timestamp_ns': 1000, 'trade_time_ns': 1001,
            'orderbook_id': 1, 'trade_id': trade_id,
            'class_code': class_code, 'side': 'B',
            'price': 24000, 'quantity': 5, 'is_printable': True,
        }

    def test_lazy_opening_no_empty_files(self, tmp_path):
        """Aucun writer créé avant la première row → pas de fichiers vides."""
        pool = _WriterPool(tmp_path, '20260203')
        assert len(pool._writers) == 0
        result = pool.close_all()
        assert len(result) == 0

    def test_writer_created_on_first_row(self, tmp_path):
        """Un writer est créé à la première append_order."""
        pool = _WriterPool(tmp_path, '20260203')
        pool.append_order('HSI', self._make_order_row('HSI'))
        assert ('HSI', 'orders') in pool._writers
        assert ('HSI', 'trades') not in pool._writers
        pool.close_all()

    def test_routing_by_product(self, tmp_path):
        """Les rows HSI et MHI vont dans des fichiers distincts."""
        pool = _WriterPool(tmp_path, '20260203')
        pool.append_order('HSI', self._make_order_row('HSI', order_id=1))
        pool.append_order('HSI', self._make_order_row('HSI', order_id=2))
        pool.append_order('MHI', self._make_order_row('MHI', order_id=3))
        result = pool.close_all()

        assert 'HSI' in result
        assert 'MHI' in result

        hsi_orders = pq.read_table(result['HSI'][0])
        mhi_orders = pq.read_table(result['MHI'][0])

        assert hsi_orders.num_rows == 2
        assert mhi_orders.num_rows == 1
        assert set(hsi_orders['order_id'].to_pylist()) == {1, 2}
        assert set(mhi_orders['order_id'].to_pylist()) == {3}

    def test_orders_and_trades_separate(self, tmp_path):
        """Orders et trades d'un même produit vont dans des fichiers distincts."""
        pool = _WriterPool(tmp_path, '20260203')
        pool.append_order('HSI', self._make_order_row('HSI'))
        pool.append_trade('HSI', self._make_trade_row('HSI'))
        result = pool.close_all()

        orders_path, trades_path = result['HSI']
        orders_tbl = pq.read_table(orders_path)
        trades_tbl = pq.read_table(trades_path)

        assert orders_tbl.num_rows == 1
        assert trades_tbl.num_rows == 1
        # Vérifier que le schema est correct
        assert 'event_type' in orders_tbl.schema.names
        assert 'trade_id'   in trades_tbl.schema.names

    def test_schema_enforced(self, tmp_path):
        """Le schéma Parquet généré correspond à ORDERS_SCHEMA / TRADES_SCHEMA."""
        pool = _WriterPool(tmp_path, '20260203')
        pool.append_order('HSI', self._make_order_row('HSI'))
        pool.append_trade('HSI', self._make_trade_row('HSI'))
        result = pool.close_all()

        orders_path, trades_path = result['HSI']
        assert pq.read_table(orders_path).schema.equals(ORDERS_SCHEMA)
        assert pq.read_table(trades_path).schema.equals(TRADES_SCHEMA)


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS E2E — Pipeline complet sur données synthétiques
# ═══════════════════════════════════════════════════════════════════════════════

class TestPipelineE2E:
    """
    Tests end-to-end : construction d'un zip OMD-D synthétique,
    parsing complet, validation des Parquet générés.
    Ces tests valident le pipeline sans dépendre de données HKEX réelles.
    """

    DATE = '20260203'
    SEND_TIME = 1_738_540_800_000_000_000  # ~2026-02-03 00:00 UTC en ns

    # OrderbookIDs et symbols de référence
    HSI_OB_ID   = 1001
    MHI_OB_ID   = 2001
    OPTION_OB_ID = 3001   # option HSI, doit être filtré par futures_only

    def _make_zip(self, tmpdir: Path, include_mhi: bool = True) -> Path:
        """Construit un zip avec des données HSI + optionnellement MHI."""
        # Channel Series Definitions (MC101)
        series_entries = [
            (self.HSI_OB_ID,   'HSIH6',  3),   # HSI future
            (self.MHI_OB_ID,   'MHIH6',  3),   # MHI future
            (self.OPTION_OB_ID,'HSIH62400C', 1), # HSI call option
        ]
        mc101_data = _build_series_def_channel(series_entries)

        # Channel market data HSI (MC221)
        hsi_events = [
            {'type': 'add',    'orderbook_id': self.HSI_OB_ID, 'order_id': 1,
             'price': 24000, 'quantity': 10, 'side': SIDE_BID},
            {'type': 'add',    'orderbook_id': self.HSI_OB_ID, 'order_id': 2,
             'price': 24010, 'quantity': 5,  'side': SIDE_OFFER},
            {'type': 'trade',  'orderbook_id': self.HSI_OB_ID, 'order_id': 2,
             'price': 24005, 'quantity': 5,  'side': 2, 'trade_id': 101,
             'trade_time': self.SEND_TIME + 1000, 'deal_type': 1},
            {'type': 'delete', 'orderbook_id': self.HSI_OB_ID, 'order_id': 1,
             'side': SIDE_BID},
            # Option — doit être ignorée car orderbook_id non dans la map futures
            {'type': 'add',    'orderbook_id': self.OPTION_OB_ID, 'order_id': 99,
             'price': 500, 'quantity': 1, 'side': SIDE_BID},
        ]
        mc221_data = _build_market_data_channel(hsi_events, self.SEND_TIME)

        # Channel market data MHI (MC121) — optionnel
        mc121_data = b''
        if include_mhi:
            mhi_events = [
                {'type': 'add',   'orderbook_id': self.MHI_OB_ID, 'order_id': 10,
                 'price': 24000, 'quantity': 2, 'side': SIDE_BID},
                {'type': 'trade', 'orderbook_id': self.MHI_OB_ID, 'order_id': 10,
                 'price': 24000, 'quantity': 2, 'side': 2, 'trade_id': 201,
                 'trade_time': self.SEND_TIME + 2000, 'deal_type': 1},
            ]
            mc121_data = _build_market_data_channel(mhi_events, self.SEND_TIME)

        return _build_zip(tmpdir, self.DATE, mc101_data, mc221_data, mc121_data)

    def test_output_structure(self, tmp_path):
        """Le dict retourné contient HSI et MHI avec les bons chemins."""
        zip_path = self._make_zip(tmp_path / 'raw')
        parser   = HKEXParser(instruments=['HSI', 'MHI', 'HHI', 'MCH'])
        result   = parser.parse_daily_zip(zip_path, base_dir=tmp_path / 'out')

        assert 'HSI' in result
        assert 'MHI' in result
        assert 'HHI' not in result   # pas de données HHI ce jour

        for cc, (op, tp) in result.items():
            assert op.exists(), f"orders Parquet manquant pour {cc}"
            assert tp.exists(), f"trades Parquet manquant pour {cc}"

    def test_hive_directory_structure(self, tmp_path):
        """Les fichiers sont dans la bonne arborescence Hive."""
        zip_path  = self._make_zip(tmp_path / 'raw')
        base_dir  = tmp_path / 'out'
        parser    = HKEXParser(instruments=['HSI', 'MHI', 'HHI', 'MCH'])
        result    = parser.parse_daily_zip(zip_path, base_dir=base_dir)

        hsi_orders, _ = result['HSI']
        expected_dir  = base_dir / 'product=HSI' / 'year=2026' / 'month=02'
        assert hsi_orders.parent == expected_dir
        assert hsi_orders.name   == f'HSI_{self.DATE}_orders.parquet'

    def test_hsi_orders_content(self, tmp_path):
        """Vérifie le contenu exact des orders HSI générés."""
        zip_path = self._make_zip(tmp_path / 'raw')
        parser   = HKEXParser(instruments=['HSI', 'MHI', 'HHI', 'MCH'])
        result   = parser.parse_daily_zip(zip_path, base_dir=tmp_path / 'out')

        orders = pq.read_table(result['HSI'][0]).to_pandas()

        # 2 Add + 1 Delete pour HSI (l'option est filtrée)
        assert len(orders) == 3

        adds    = orders[orders['event_type'] == 'A']
        deletes = orders[orders['event_type'] == 'D']
        assert len(adds)    == 2
        assert len(deletes) == 1

        # Vérifier les prix
        assert set(adds['price'].tolist()) == {24000, 24010}

        # Delete : price=0, quantity=0
        assert deletes.iloc[0]['price']    == 0
        assert deletes.iloc[0]['quantity'] == 0

    def test_hsi_trades_content(self, tmp_path):
        """Vérifie le contenu exact des trades HSI générés."""
        zip_path = self._make_zip(tmp_path / 'raw')
        parser   = HKEXParser(instruments=['HSI', 'MHI', 'HHI', 'MCH'])
        result   = parser.parse_daily_zip(zip_path, base_dir=tmp_path / 'out')

        trades = pq.read_table(result['HSI'][1]).to_pandas()

        assert len(trades) == 1
        t = trades.iloc[0]
        assert t['price']         == 24005
        assert t['quantity']      == 5
        assert t['trade_id']      == 101
        assert t['side']          == 'B'
        assert t['is_printable']  is True
        assert t['trade_time_ns'] == self.SEND_TIME + 1000

    def test_options_filtered_by_futures_only(self, tmp_path):
        """futures_only=True : l'option (FinancialProduct=1) est exclue."""
        zip_path = self._make_zip(tmp_path / 'raw')
        parser   = HKEXParser(instruments=None, futures_only=True)
        result   = parser.parse_daily_zip(zip_path, base_dir=tmp_path / 'out')

        # OPTION_OB_ID ne doit pas apparaître dans les données
        for cc, (op, _) in result.items():
            orders = pq.read_table(op).to_pandas()
            option_rows = orders[orders['orderbook_id'] == self.OPTION_OB_ID]
            assert len(option_rows) == 0, f"Option trouvée dans {cc} orders"

    def test_instrument_filter(self, tmp_path):
        """instruments=['HSI'] → MHI absent du résultat."""
        zip_path = self._make_zip(tmp_path / 'raw', include_mhi=True)
        parser   = HKEXParser(instruments=['HSI'])
        result   = parser.parse_daily_zip(zip_path, base_dir=tmp_path / 'out')

        assert 'HSI' in result
        assert 'MHI' not in result

    def test_no_cross_contamination(self, tmp_path):
        """Les orders HSI ne contiennent que class_code='HSI'."""
        zip_path = self._make_zip(tmp_path / 'raw', include_mhi=True)
        parser   = HKEXParser(instruments=['HSI', 'MHI', 'HHI', 'MCH'])
        result   = parser.parse_daily_zip(zip_path, base_dir=tmp_path / 'out')

        for cc, (op, tp) in result.items():
            orders = pq.read_table(op).to_pandas()
            assert (orders['class_code'] == cc).all(), \
                f"Contamination détectée dans {cc}_orders"

    def test_stats_counts(self, tmp_path):
        """Les stats reflètent exactement les messages parsés."""
        zip_path = self._make_zip(tmp_path / 'raw', include_mhi=True)
        parser   = HKEXParser(instruments=['HSI', 'MHI', 'HHI', 'MCH'])
        parser.parse_daily_zip(zip_path, base_dir=tmp_path / 'out')
        stats = parser.get_stats()

        # HSI : 2 Add + 1 Delete | MHI : 1 Add
        assert stats['add_orders'] == 3
        assert stats['del_orders'] == 1
        # HSI : 1 Trade | MHI : 1 Trade
        assert stats['trades']     == 2

    def test_parquet_schema_compliance(self, tmp_path):
        """Les Parquet générés respectent exactement ORDERS_SCHEMA et TRADES_SCHEMA."""
        zip_path = self._make_zip(tmp_path / 'raw')
        parser   = HKEXParser(instruments=['HSI', 'MHI', 'HHI', 'MCH'])
        result   = parser.parse_daily_zip(zip_path, base_dir=tmp_path / 'out')

        for cc, (op, tp) in result.items():
            assert pq.read_table(op).schema.equals(ORDERS_SCHEMA), \
                f"Schema orders incorrect pour {cc}"
            assert pq.read_table(tp).schema.equals(TRADES_SCHEMA), \
                f"Schema trades incorrect pour {cc}"

    def test_get_stats_returns_copy(self):
        """get_stats() retourne une copie indépendante des stats internes."""
        parser = HKEXParser()
        stats  = parser.get_stats()
        stats['trades'] = 9999
        assert parser._stats['trades'] == 0  # l'original n'est pas modifié