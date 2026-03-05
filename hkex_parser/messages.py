"""
HKEX OMD-D Binary Protocol — Message structures
Source : FBD-NSOM Product Specification (2022-01-28)

═══════════════════════════════════════════════════════════════════════════════
ARCHITECTURE GÉNÉRALE DU PROTOCOLE OMD-D
═══════════════════════════════════════════════════════════════════════════════

Les fichiers historiques HKEX sont des captures du feed de marché OMD-D
(Options Market Data - Derivatives). Chaque fichier correspond à un "channel"
(ex: MC221 pour les HSI futures).

Structure physique d'un fichier :

  ┌──────────────────────────────────────────────────────────┐
  │ Record 1                                                 │
  │  ├── RecLen     (2 bytes, Big Endian)                    │
  │  └── PacketHeader (16 bytes)                             │
  │       ├── PktSize   (2 bytes, Big Endian)                │
  │       ├── MsgCount  (1 byte)                             │
  │       ├── Filler    (1 byte)                             │
  │       ├── SeqNum    (4 bytes, Little Endian)             │
  │       └── SendTime  (8 bytes, Little Endian) ← timestamp │
  │  ├── Message 1                                           │
  │  │    ├── MsgHeader  (4 bytes : MsgSize + MsgType)       │
  │  │    └── Payload    (MsgSize - 4 bytes)                 │
  │  └── Message 2 (si MsgCount > 1)                         │
  │       └── ...                                            │
  ├── Record 2                                               │
  └── ...                                                    │
  └──────────────────────────────────────────────────────────┘

Points importants :
  • RecLen est en Big Endian (validé empiriquement — la spec dit LE, mais les
    données réelles sont BE pour ce champ)
  • PktSize est aussi en Big Endian
  • Tout le reste (SeqNum, SendTime, MsgSize, MsgType, payloads) est Little Endian
  • RecLen EXCLUT ses propres 2 bytes → taille totale du record = RecLen + 2
  • PktSize INCLUT ses propres 2 bytes → incohérence volontaire dans la spec

Channels et types de messages associés :
  MC151/101/201/301 → Series Definitions (CommodityDef 301, ClassDef 302,
                       SeriesDefBase 303, SeriesDefExtended 304)
  MC121/221/321     → Market data temps réel (AddOrder 330, Trade 350, etc.)
  MC167             → Block Trades (Trade 350 uniquement)
  MC161             → Status messages (320, 321, 322)

Clé de mapping critique :
  Chaque message order/trade contient un OrderbookID (entier 32 bits).
  Pour savoir à quel produit correspond un OrderbookID, on lit d'abord les
  Series Definitions qui font le lien : OrderbookID → Symbol → ClassCode
  (ex: 1234 → "HSIH6" → "HSI")
"""

import struct
from dataclasses import dataclass
from typing import Optional

# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTES DE TAILLE
# ═══════════════════════════════════════════════════════════════════════════════

RECORD_LENGTH_SIZE  = 2    # Champ RecLen avant chaque record (2 bytes)
PACKET_HEADER_SIZE  = 16   # PktSize(2)+MsgCount(1)+Filler(1)+SeqNum(4)+SendTime(8)
MESSAGE_HEADER_SIZE = 4    # MsgSize(2)+MsgType(2)
RECORD_HEADER_SIZE  = RECORD_LENGTH_SIZE + PACKET_HEADER_SIZE  # = 18 bytes au total

# ═══════════════════════════════════════════════════════════════════════════════
# TYPES DE MESSAGES (MsgType dans MessageHeader)
# ═══════════════════════════════════════════════════════════════════════════════

MSG_SEQUENCE_RESET          = 100   # Reset de séquence (ignoré dans le parsing)
MSG_COMMODITY_DEFINITION    = 301   # Définition d'une commodity (ex: "Equity Index")
MSG_CLASS_DEFINITION        = 302   # Définition d'une classe (ex: "HSI Options")
MSG_SERIES_DEF_BASE         = 303   # Définition de série avec OrderbookID → source principale
MSG_SERIES_DEF_EXTENDED     = 304   # Définition de série étendue → complément ou fallback
MSG_COMBINATION_DEF         = 305   # Stratégies combinées (straddles, spreads) → ignorées
MSG_MARKET_STATUS           = 320   # Status du marché (pré-ouverture, ouverture, clôture)
MSG_ADD_ORDER               = 330   # Ajout d'un ordre dans le carnet
MSG_MODIFY_ORDER            = 331   # Modification de prix ou quantité d'un ordre existant
MSG_DELETE_ORDER            = 332   # Suppression d'un ordre (exécuté ou annulé)
MSG_ORDERBOOK_CLEAR         = 335   # Remise à zéro du carnet (typiquement en fin de journée)
MSG_TRADE                   = 350   # Trade exécuté (avec prix, quantité, sens)
MSG_CALCULATED_OPENING_PRICE = 364  # Prix d'ouverture calculé (COP) → ignoré ici

# ═══════════════════════════════════════════════════════════════════════════════
# SENS (Side) — utilisé dans Add/Modify/Delete Order et Trade
# ═══════════════════════════════════════════════════════════════════════════════

SIDE_BID   = 0   # Acheteur (Bid) — spec section 3.1
SIDE_OFFER = 1   # Vendeur (Ask/Offer)
# Note Trade : les valeurs sont différentes → 2=Buy, 3=Sell (spec section 3.5)


# ═══════════════════════════════════════════════════════════════════════════════
# RECORD + PACKET HEADER
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class RecordHeader:
    """
    Structure des 18 premiers bytes de chaque record.

    Positionnement dans le fichier :
      [offset 0-1]   RecLen    Uint16 **Big Endian**
                               = taille du record EXCLUANT ces 2 bytes
                               ATTENTION : la spec indique LE, mais les données
                               réelles sont BE — validé en vérifiant que
                               rec_len+2 = taille cohérente avec les messages.
      [offset 2-3]   PktSize   Uint16 **Big Endian**
                               = taille du PacketHeader + messages
                               INCLUT ses propres 2 bytes (incohérence vs RecLen)
      [offset 4]     MsgCount  Uint8  Little Endian
                               = nombre de messages dans ce record
      [offset 5]     Filler    1 byte ignoré
      [offset 6-9]   SeqNum    Uint32 Little Endian
                               = numéro de séquence du packet (monotone croissant)
      [offset 10-17] SendTime  Uint64 Little Endian
                               = timestamp d'envoi par HKEX, en nanosecondes UTC
                               ATTENTION : précision effective ~1ms seulement
                               (l'horloge HKEX ne descend pas sous la milliseconde
                               malgré le stockage en nanosecondes)

    Propriétés calculées :
      total_record_size = rec_len + 2  → pour avancer le curseur de lecture
      messages_start    = 18           → offset du premier message dans le record
    """
    rec_len:   int   # taille record sans les 2 bytes RecLen
    pkt_size:  int   # taille packet header inclus
    msg_count: int   # nombre de messages dans ce record (0 à ~10 typiquement)
    seq_num:   int   # numéro de séquence (utile pour détecter des trous)
    send_time: int   # nanosecondes UTC — précision effective ~1ms

    @classmethod
    def from_bytes(cls, data: bytes) -> 'RecordHeader':
        """
        Désérialise 18 bytes en RecordHeader.

        Utilise struct.unpack_from(fmt, buffer, offset) pour lire des valeurs
        binaires à des positions précises :
          '>H' = Big Endian unsigned short (2 bytes)
          '<B' = Little Endian unsigned byte (1 byte) — LE/BE identique pour 1 byte
          '<I' = Little Endian unsigned int (4 bytes)
          '<Q' = Little Endian unsigned long long (8 bytes)
        """
        rec_len   = struct.unpack_from('>H', data, 0)[0]   # offset 0, Big Endian
        pkt_size  = struct.unpack_from('>H', data, 2)[0]   # offset 2, Big Endian
        msg_count = struct.unpack_from('<B', data, 4)[0]   # offset 4
        # data[5] = Filler, ignoré
        seq_num   = struct.unpack_from('<I', data, 6)[0]   # offset 6, Little Endian
        send_time = struct.unpack_from('<Q', data, 10)[0]  # offset 10, Little Endian
        return cls(rec_len, pkt_size, msg_count, seq_num, send_time)

    @property
    def total_record_size(self) -> int:
        """
        Taille totale du record en bytes = RecLen + 2 bytes du champ RecLen lui-même.
        Utilisé pour avancer le curseur de lecture : offset += total_record_size
        """
        return self.rec_len + 2

    @property
    def messages_start(self) -> int:
        """
        Offset du premier message dans le record.
        = 2 (RecLen) + 16 (PacketHeader) = 18 bytes
        Constant, défini ici comme propriété pour la clarté du code.
        """
        return RECORD_HEADER_SIZE   # = 18


# ═══════════════════════════════════════════════════════════════════════════════
# MESSAGE HEADER
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class MessageHeader:
    """
    Header de 4 bytes présent au début de chaque message (après le PacketHeader).

      [offset 0-1] MsgSize  Uint16 Little Endian
                            = taille totale du message INCLUANT ces 4 bytes
                            → payload = MsgSize - 4 bytes
      [offset 2-3] MsgType  Uint16 Little Endian
                            = type du message (voir constantes MSG_* ci-dessus)

    Usage dans le parser :
      msg_end = msg_offset + msg_hdr.msg_size
      payload = data[msg_offset + 4 : msg_end]
    """
    msg_size: int   # taille totale du message header + payload inclus
    msg_type: int   # identifiant du type de message

    @classmethod
    def from_bytes(cls, data: bytes) -> 'MessageHeader':
        return cls(
            struct.unpack_from('<H', data, 0)[0],   # MsgSize, Little Endian
            struct.unpack_from('<H', data, 2)[0],   # MsgType, Little Endian
        )


# ═══════════════════════════════════════════════════════════════════════════════
# MESSAGE 303 — SERIES DEFINITION BASE
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class SeriesDefinitionBase:
    """
    Message 303 — Series Definition Base (spec section 1.3)

    Source : channels MC101, MC201, MC301
    Rôle   : établit le lien OrderbookID ↔ Symbol ↔ ClassCode.
             C'est la source PRINCIPALE de la mapping table car elle contient
             aussi FinancialProduct qui permet de distinguer futures et options.

    Layout du payload (offset depuis début du payload, après les 4 bytes MsgHeader) :
      [0-3]   OrderbookID        Uint32 LE  ← clé unique du carnet d'ordres
                                             Utilisée dans TOUS les messages
                                             Add/Modify/Delete/Trade pour
                                             identifier le produit
      [4-35]  Symbol             String 32  Ex: 'HSIH6   ' (padded avec espaces)
                                             Les 3 premiers chars = ClassCode
                                             H=année (H→2026, M→2026, etc.)
                                             6=chiffre de l'année
      [36]    FinancialProduct   Uint8      1=Option, 3=Future
                                             Clé pour filtrer futures_only=True
      [37-38] NumberOfDecimalsPrice Uint16  Nombre de décimales du prix
                                             (à utiliser en Phase 2 pour prix réels)
      [44-47] StrikePrice        Int32      Pour les options (0 pour futures)
      [48-55] ExpirationDate     String 8   Format YYYYMMDD
    Total : 60 bytes
    """
    orderbook_id:      int   # clé de mapping — référencée dans tous les messages market data
    symbol:            str   # identifiant complet ex: 'HSIH6' (HSI, expiry mars 2026)
    class_code:        str   # 3 premiers chars du symbol ex: 'HSI', 'MHI', 'HHI'
    financial_product: int   # 1=Option, 3=Future

    @classmethod
    def from_payload(cls, payload: bytes) -> 'SeriesDefinitionBase':
        orderbook_id      = struct.unpack_from('<I', payload, 0)[0]
        # Décoder la string ASCII, ignorer les bytes invalides, enlever les espaces
        symbol            = payload[4:36].decode('ascii', errors='replace').strip()
        # ClassCode = 3 premiers caractères du symbol (convention HKEX)
        class_code        = symbol[:3] if symbol else ''
        financial_product = struct.unpack_from('<B', payload, 36)[0]
        return cls(orderbook_id, symbol, class_code, financial_product)

    @property
    def is_future(self) -> bool:
        """True si FinancialProduct == 3 (Future). False pour options, combos, etc."""
        return self.financial_product == 3


# ═══════════════════════════════════════════════════════════════════════════════
# MESSAGE 304 — SERIES DEFINITION EXTENDED
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class SeriesDefinitionExtended:
    """
    Message 304 — Series Definition Extended (spec section 1.4)

    Source : channels MC151 (principalement)
    Rôle   : complément à SeriesDefinitionBase — contient des infos additionnelles
             (CommodityCode, ExpirationDate encodée) mais PAS FinancialProduct.
             Utilisé comme fallback si un OrderbookID n'est pas dans MC101/201/301.

    Layout du payload :
      [0-3]   OrderBookID     Uint32 LE  ← même clé que SeriesDefinitionBase
      [4-35]  Symbol          String 32  même format : 'HSIH6   '
      [36]    Country         Uint8      ex: 344 = Hong Kong
      [37]    Market          Uint8      identifiant du marché
      [38]    InstrumentGroup Uint8      groupe d'instruments
      [43]    Modifier        Uint8
      [44-45] CommodityCode   Uint16 LE  code de la commodity sous-jacente
      [46-47] ExpirationDate  Uint16 LE  date encodée (format non-standard)
    Total : 104 bytes

    Note importante :
      is_future retourne toujours True car le message 304 ne contient pas
      FinancialProduct. Pour filtrer futures uniquement, on préfère
      SeriesDefinitionBase (303). Le 304 est utilisé pour les symbols
      non couverts par le 303.
    """
    orderbook_id:    int   # clé de mapping
    symbol:          str   # ex: 'HSIH6'
    class_code:      str   # 3 premiers chars du symbol
    commodity_code:  int   # code commodity (ex: 33 = Hang Seng Index)
    expiration_date: int   # date encodée LE (à décoder si besoin)

    @classmethod
    def from_payload(cls, payload: bytes) -> 'SeriesDefinitionExtended':
        orderbook_id    = struct.unpack_from('<I', payload, 0)[0]
        symbol          = payload[4:36].decode('ascii', errors='replace').strip()
        class_code      = symbol[:3] if symbol else ''
        commodity_code  = struct.unpack_from('<H', payload, 44)[0]
        expiration_date = struct.unpack_from('<H', payload, 46)[0]
        return cls(orderbook_id, symbol, class_code, commodity_code, expiration_date)

    @property
    def is_future(self) -> bool:
        """
        Approximation : on suppose que tout ce qui vient de MC151 via le message 304
        et qui a un ClassCode valide est un future.
        À affiner si on veut inclure les options (message 304 ne distingue pas).
        """
        return True


# ═══════════════════════════════════════════════════════════════════════════════
# MESSAGE 330 — ADD ORDER
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class AddOrder:
    """
    Message 330 — Add Order (spec section 3.1)

    Signification : un participant de marché vient d'envoyer un nouvel ordre
    dans le carnet d'ordres. Ce message est central pour la reconstruction LOB
    (Limit Order Book).

    Layout du payload (après MsgHeader 4 bytes) :
      [0-3]   OrderbookID       Uint32 LE  → mapper vers ClassCode via _orderbook_map
      [4-11]  OrderID           Uint64 LE  identifiant unique de l'ordre
                                            Persistant : même ID dans Modify/Delete
      [12-15] Price             Int32  LE  prix en ticks (entier signé)
                                            Pour HSI : 1 tick = 1 point d'index
                                            À confirmer : nombre de décimales en Phase 2
      [16-19] Quantity          Uint32 LE  quantité en lots
      [20]    Side              Uint8      0=Bid (acheteur), 1=Offer (vendeur)
      [21]    LotType           Uint8      type de lot (standard, odd lot...)
      [22-23] OrderType         Uint16 LE  type d'ordre (limit, market...)
      [24-27] OrderBookPosition Uint32 LE  rang dans la file à ce niveau de prix
    Total : 32 bytes

    Dans le Parquet, on stocke :
      event_type = 'A' (Add)
      timestamp_ns = send_time du PacketHeader (précision ~1ms)
    """
    send_time:    int   # timestamp du PacketHeader, propagé depuis RecordHeader
    orderbook_id: int   # → clé vers _orderbook_map du parser
    order_id:     int   # identifiant stable de l'ordre (référencé dans Modify/Delete)
    price:        int   # prix raw en ticks entiers (pas de décimales appliquées ici)
    quantity:     int   # quantité en lots
    side:         int   # 0=Bid, 1=Offer

    @classmethod
    def from_payload(cls, payload: bytes, send_time: int) -> 'AddOrder':
        """
        Désérialise le payload en AddOrder.
        send_time est passé en paramètre car il vient du RecordHeader,
        pas du payload lui-même.
        """
        return cls(
            send_time    = send_time,
            orderbook_id = struct.unpack_from('<I', payload, 0)[0],
            order_id     = struct.unpack_from('<Q', payload, 4)[0],    # Uint64 = 8 bytes
            price        = struct.unpack_from('<i', payload, 12)[0],   # Int32 signé ('<i')
            quantity     = struct.unpack_from('<I', payload, 16)[0],
            side         = struct.unpack_from('<B', payload, 20)[0],
        )

    @property
    def side_str(self) -> str:
        """Retourne 'B' (Bid/Buy) ou 'A' (Ask/Offer) — convention standard."""
        return 'B' if self.side == SIDE_BID else 'A'


# ═══════════════════════════════════════════════════════════════════════════════
# MESSAGE 331 — MODIFY ORDER
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class ModifyOrder:
    """
    Message 331 — Modify Order (spec section 3.2)

    Signification : un ordre existant a été modifié (prix et/ou quantité).
    Structure identique à AddOrder (même layout de payload, même taille 32 bytes).

    Pour la reconstruction LOB :
      1. Retrouver l'ordre par OrderID dans le carnet actif
      2. Mettre à jour Price et/ou Quantity
      3. Si Price change → l'ordre perd sa priorité de temps (repositionné)

    Dans le Parquet : event_type = 'M' (Modify)
    """
    send_time:    int
    orderbook_id: int
    order_id:     int
    price:        int   # nouveau prix (peut être identique à l'ancien si seule la qté change)
    quantity:     int   # nouvelle quantité
    side:         int

    @classmethod
    def from_payload(cls, payload: bytes, send_time: int) -> 'ModifyOrder':
        return cls(
            send_time    = send_time,
            orderbook_id = struct.unpack_from('<I', payload, 0)[0],
            order_id     = struct.unpack_from('<Q', payload, 4)[0],
            price        = struct.unpack_from('<i', payload, 12)[0],
            quantity     = struct.unpack_from('<I', payload, 16)[0],
            side         = struct.unpack_from('<B', payload, 20)[0],
        )

    @property
    def side_str(self) -> str:
        return 'B' if self.side == SIDE_BID else 'A'


# ═══════════════════════════════════════════════════════════════════════════════
# MESSAGE 332 — DELETE ORDER
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class DeleteOrder:
    """
    Message 332 — Delete Order (spec section 3.3)

    Signification : un ordre a été retiré du carnet — soit annulé par le participant,
    soit exécuté (dans ce cas un message Trade 350 accompagne ce Delete).

    Layout du payload (plus court que Add/Modify) :
      [0-3]  OrderbookID Uint32 LE
      [4-11] OrderID     Uint64 LE
      [12]   Side        Uint8
      [13]   Filler      1 byte ignoré
    Total : 18 bytes (vs 32 pour Add/Modify)

    Différence clé vs Add/Modify :
      Pas de Prix ni Quantité → on les met à 0 dans le Parquet.
      Pour retrouver les infos, il faut join sur OrderID avec le Add correspondant.

    Dans le Parquet : event_type = 'D' (Delete), price=0, quantity=0
    """
    send_time:    int
    orderbook_id: int
    order_id:     int   # même ID que dans le Add original → permet le join
    side:         int

    @classmethod
    def from_payload(cls, payload: bytes, send_time: int) -> 'DeleteOrder':
        return cls(
            send_time    = send_time,
            orderbook_id = struct.unpack_from('<I', payload, 0)[0],
            order_id     = struct.unpack_from('<Q', payload, 4)[0],
            side         = struct.unpack_from('<B', payload, 12)[0],
        )

    @property
    def side_str(self) -> str:
        return 'B' if self.side == SIDE_BID else 'A'


# ═══════════════════════════════════════════════════════════════════════════════
# MESSAGE 350 — TRADE
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Trade:
    """
    Message 350 — Trade (spec section 3.5)

    Signification : une transaction a été exécutée sur le marché. C'est le message
    le plus important pour l'analyse microstructure (volume, prix, sens agresseur).

    Layout du payload :
      [0-3]   OrderbookID   Uint32 LE
      [4-11]  OrderID       Uint64 LE  ID de l'ordre agresseur (0 si non disponible)
      [12-15] Price         Int32  LE  prix d'exécution (même convention que Add Order)
      [16-23] TradeID       Uint64 LE  Match ID unique du trade
                                        Utile pour déduplication et audit trail
      [24-27] ComboGroupID  Uint32 LE  0 pour trades simples, >0 pour combo
                                        (straddles, spreads...) — à exclure du volume
      [28]    Side          Uint8      0=N/A, 2=Buy (agresseur acheteur), 3=Sell
                                        ATTENTION : valeurs différentes de Add Order !
      [29]    DealType      Uint8      bitmask :
                                        bit 0 (1) = Printable → doit compter dans le volume
                                        bit 1 (2) = Cross Trade (accord bilatéral)
                                        bit 2 (4) = Reported Trade (déclaré hors marché)
      [30-31] TradeCondition Uint16 LE
      [32-33] DealInfo      Uint16 LE
      [34-35] Filler        2 bytes
      [36-43] Quantity      Uint64 LE  quantité échangée (8 bytes, vs 4 pour Add Order)
      [44-51] TradeTime     Uint64 LE  timestamp du matching engine, nanosecondes UTC
                                        MEILLEURE précision que SendTime (~10ms vs ~1ms)
                                        → utiliser TradeTime pour l'analyse tick-by-tick
    Total : 56 bytes

    Dans le Parquet :
      timestamp_ns   = send_time    (timestamp de livraison du message)
      trade_time_ns  = trade_time   (timestamp du matching engine, préférable pour l'analyse)
      is_printable   = (deal_type & 1) == 1  → filtrer pour le volume réel
    """
    send_time:    int   # timestamp PacketHeader (précision ~1ms) — arrivée du message
    orderbook_id: int
    order_id:     int   # ID de l'ordre agresseur (0 si non communiqué par HKEX)
    price:        int   # prix d'exécution en ticks
    trade_id:     int   # identifiant unique du trade (utile pour déduplication)
    side:         int   # 2=Buy (agresseur acheteur), 3=Sell (agresseur vendeur), 0=N/A
    deal_type:    int   # bitmask : bit0=Printable, bit1=Cross, bit2=Reported
    quantity:     int   # quantité en lots (Uint64 — peut être très grande pour combo)
    trade_time:   int   # timestamp matching engine — plus précis que send_time

    @classmethod
    def from_payload(cls, payload: bytes, send_time: int) -> 'Trade':
        return cls(
            send_time    = send_time,
            orderbook_id = struct.unpack_from('<I', payload, 0)[0],
            order_id     = struct.unpack_from('<Q', payload, 4)[0],
            price        = struct.unpack_from('<i', payload, 12)[0],   # Int32 signé
            trade_id     = struct.unpack_from('<Q', payload, 16)[0],
            side         = struct.unpack_from('<B', payload, 28)[0],
            deal_type    = struct.unpack_from('<B', payload, 29)[0],
            quantity     = struct.unpack_from('<Q', payload, 36)[0],   # Uint64 !
            trade_time   = struct.unpack_from('<Q', payload, 44)[0],
        )

    @property
    def is_printable(self) -> bool:
        """
        True si le trade doit être inclus dans le volume réel (bit 0 de DealType = 1).

        Les trades non-printable incluent :
        - Parties d'une stratégie combo (straddle, spread...) → déjà comptées
          dans le trade combo parent
        - Trades reported (déclarés hors marché, prix non représentatif)

        → Toujours filtrer is_printable=True pour les calculs de volume,
          VPIN, et analyse de market impact.
        """
        return bool(self.deal_type & 1)

    @property
    def side_str(self) -> str:
        """
        Retourne 'B' (Buy, agresseur acheteur), 'A' (Ask/Sell, agresseur vendeur)
        ou '?' si côté non déterminé.

        Note : les valeurs ici sont 2/3 et non 0/1 comme dans Add Order.
        C'est une incohérence de la spec OMD-D.
        """
        if self.side == 2:
            return 'B'
        elif self.side == 3:
            return 'A'
        return '?'