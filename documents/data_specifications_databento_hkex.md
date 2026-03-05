# Data Specifications — Databento MBO & HKEX OMD-D

*Destiny Research — internal reference document*
*Last updated: 2026-03-05*

---

## 1. Overview

This document centralises the technical specifications for all market data sources used in Destiny Research. It covers schema definitions, timestamp semantics, price encoding, and source-specific quirks that informed our ingestion pipeline design decisions.

| Product | Exchange | Feed / Spec | Databento Dataset | Timestamp precision |
|---|---|---|---|---|
| ES | CME (Globex) | CME MDP 3.0 | `GLBX.MDP3` | Nanosecond |
| NIY | OSE via CME Globex | CME MDP 3.0 | `GLBX.MDP3` | Nanosecond |
| NKD | CME (Globex) | CME MDP 3.0 | `GLBX.MDP3` | Nanosecond |
| FDAX | Eurex | Eurex EMDI | `XEUR.EOBI` | Microsecond |
| FESX | Eurex | Eurex EMDI | `XEUR.EOBI` | Microsecond |
| FSMI | Eurex | Eurex EMDI | `XEUR.EOBI` | Microsecond |
| HSI | HKEX | OMD-D (proprietary binary) | — (direct HKEX files) | ~1ms effective (stored as ns) |

> **Note on SGX STI:** not yet acquired. SGX provides CSV files with microsecond timestamps — no binary parsing required. To be documented when data is purchased.

---

## 2. Databento MBO Schema

### 2.1 What is MBO (Level 3)?

MBO = *Market By Order*. Every individual order event is captured: add, cancel, modify, trade, fill. This is the most granular data available — full order-level LOB reconstruction is possible. All products in Destiny Research use MBO exclusively.

### 2.2 MBO Record Fields

| Field | Type | Description |
|---|---|---|
| `ts_recv` | `int64` (ns UTC) | Timestamp when Databento's capture server received the message. **Primary sort key.** Guaranteed monotonically non-decreasing within a file. |
| `ts_event` | `int64` (ns UTC) | Exchange-side timestamp (matching engine or gateway). Can be non-monotonic on SOD snapshots and around session boundaries. Used for latency analysis and lead-lag studies. |
| `ts_in_delta` | `int32` (ns) | `ts_recv - ts_exchange_send` — one-way feed latency proxy from exchange to Databento capture point. Useful for feed quality monitoring. |
| `sequence` | `uint32` | Venue sequence number. Monotonic within a channel session. Gaps indicate dropped messages. |
| `order_id` | `uint64` | Unique order identifier assigned by the venue. Stable across Add/Cancel/Modify for the same order. |
| `price` | `int64` | Fixed-point price with 1e-9 precision. **Stored raw — divide by 1e9 at analysis time.** See section 2.4. |
| `size` | `uint32` | Order quantity in lots. |
| `action` | `string(1)` | Order event type. See section 2.3. |
| `side` | `string(1)` | `A` = Ask, `B` = Bid, `N` = None (for trades where side is not specified). |
| `flags` | `uint8` | Bitmask of event flags. See section 2.5. |
| `channel_id` | `uint16` | Venue channel identifier. Semantics differ by exchange — see section 3. |

### 2.3 Action Codes

| Code | Name | Description | Pipeline split |
|---|---|---|---|
| `A` | Add | New order added to the book | → `orders` file |
| `C` | Cancel | Order fully cancelled | → `orders` file |
| `M` | Modify | Order price or size modified | → `orders` file |
| `R` | Clear / Reset | Book cleared (e.g. session open snapshot reset) | → `orders` file |
| `T` | Trade | Trade execution | → `trades` file |
| `F` | Fill | Partial or full fill of a resting order | → `trades` file |

> **Design decision:** actions `A/C/M/R` go to `_orders.parquet`; actions `T/F` go to `_trades.parquet`. This split allows loading only trade data for alpha decay analysis without materialising the full LOB.

### 2.4 Price Encoding

Databento encodes all prices as `int64` fixed-point integers with a precision of `1e-9` (i.e., the stored integer represents the price in units of 1 billionth of the base currency unit).

**Example — ES futures:**
```
stored value : 5_432_250_000_000
actual price : 5_432_250_000_000 / 1_000_000_000 = 5432.25 USD
```

**Why we keep raw int64 in Parquet:**
- Lossless — no floating-point rounding errors
- Cheaper storage (int64 vs float64 same size, but dict/RLE encoding works better on integers)
- Price arithmetic (spread, mid, OFI) can be done in integer space until the last step
- Convert to float64 only when needed: `price_float = df["price"] / 1e9`

> **Reference:** Databento DBN encoding spec — https://docs.databento.com/knowledge-base/new-users/dbn-encoding

### 2.5 Flags Bitmask

| Bit | Constant | Description |
|---|---|---|
| 0x80 | `F_LAST` | Last message in a packet — use to detect packet boundaries |
| 0x40 | `F_TOB` | Top-of-book update |
| 0x20 | `F_SNAPSHOT` | Message is part of a SOD snapshot, not a live event |
| 0x10 | `F_MBP` | MBP-derived event (not raw order-level) |

> **Practical implication:** filter out `flags & 0x20 != 0` when reconstructing the intraday LOB to avoid double-counting SOD snapshot orders as live additions.

> **Reference:** Databento field reference — https://docs.databento.com/api-reference-historical/schemas/mbo

### 2.6 Timestamp Semantics: ts_recv vs ts_event

This is the most important subtlety for correct time-series construction.

```
Exchange matching engine
        │
        │  ts_event set here (exchange clock)
        ↓
Exchange gateway / multicast feed
        │
        │  ts_in_delta = transit time (exchange → Databento capture point)
        ↓
Databento capture server
        │
        │  ts_recv set here (Databento clock, NTP-synced)
        ↓
DBN file on disk
```

**Use ts_recv as sort key** for all pipeline operations. It is guaranteed monotonically non-decreasing within a DBN file. `ts_event` can violate monotonicity around:
- SOD snapshot delivery (exchange replays old timestamps)
- Clock correction events at the exchange
- Messages from different channels merged into one feed

**Use ts_event for:**
- Feed latency measurement: `latency_ns = ts_recv - ts_event`
- Lead-lag analysis between venues (NIY on OSE vs NKD on CME — both carry exchange-native timestamps)
- Cross-market timestamp alignment studies

> **Reference:** Databento timestamp documentation — https://docs.databento.com/knowledge-base/new-users/fields-and-data/timestamps

---

## 3. Exchange-Specific Notes

### 3.1 CME Globex — Products: ES, NIY, NKD

- **Underlying feed spec:** CME MDP 3.0 (Market Data Platform)
- **Databento dataset:** `GLBX.MDP3`
- **Timestamp precision:** nanosecond (CME matching engine clock, GPS-disciplined)
- **channel_id semantics:** CME MDP3 channel number (e.g. channel 318 = ES). Each channel is an independent multicast feed. Sequence gaps should be checked per-channel.
- **NIY specifics:** NIY is the JPY-denominated Nikkei 225 future traded on OSE (Osaka Stock Exchange) but routed through the CME Globex platform. The feed is technically CME MDP3, but the underlying session schedule follows OSE hours (Tokyo timezone). `ts_event` reflects CME Globex timestamps, not OSE native timestamps.
- **NIY/NKD lead-lag note:** both products track the Nikkei 225 but NIY is JPY-denominated and NKD is USD-denominated. Price discovery analysis should use `ts_event` (exchange-native) rather than `ts_recv` to avoid Databento capture-side latency artefacts contaminating the lead-lag signal.

> **Reference:** CME MDP 3.0 Market Data Interface specification — https://www.cmegroup.com/confluence/display/EPICSANDBOX/CME+MDP+3.0+Market+Data

### 3.2 Eurex EOBI — Products: FDAX, FESX, FSMI

- **Underlying feed spec:** Eurex EOBI (Enhanced Order Book Interface)
- **Databento dataset:** `XEUR.EOBI`
- **Timestamp precision:** microsecond (Eurex matching engine clock). Stored as nanoseconds in DBN but the last 3 digits are always `000` — do not assume nanosecond resolution.
- **channel_id semantics:** Eurex partition number. Eurex partitions its order book across multiple feed channels by product group. FDAX, FESX, and FSMI are on different partitions.
- **Price scale:** Eurex uses a different fixed-point scale per product (defined in the instrument definition records). For FDAX the tick value is 0.5 index points = EUR 12.50 per contract. Verify the price denominator against the instrument definition before computing spread or OFI in price terms.
- **Timestamp comparison with CME:** Eurex timestamps are microsecond-precision. When doing cross-market analysis (e.g. ES vs FDAX), align on 1ms buckets at minimum to avoid false lead-lag artefacts from the precision mismatch.

> **Reference:** Eurex Enhanced Order Book Interface (EOBI) specification — https://www.eurex.com/ec-en/support/initiatives/eurex-eobi

### 3.3 HKEX OMD-D — Product: HSI (and MHI, HHI, MCH)

See section 4 for full HKEX specification.

---

## 4. HKEX OMD-D Binary Format

### 4.1 Overview

HKEX distributes historical tick data as proprietary binary files following the **OMD-D** (Orion Market Data — Derivatives) protocol. Unlike Databento, there is no normalisation layer — raw binary parsing is required.

- **File format:** zipped binary, one file per trading day
- **Channels used:**
  - MC151 / MC101 / MC201 / MC301 → Series Definitions (instrument reference data)
  - MC221 → HSI and HHI futures
  - MC121 → MHI and MCH futures

### 4.2 Binary Encoding Rules

This is the most important and counterintuitive part of the format:

| Field type | Byte order |
|---|---|
| Record length (`RecLen`) | **Big-endian** |
| Packet size (`PktSize`) | **Big-endian** |
| Message size (`MsgSize`) | **Little-endian** |
| All other fields | **Little-endian** |

**Packet structure:**
```
[RecLen : 2 bytes BE]
[PacketHeader : 16 bytes]
  ├─ PktSize    : 2 bytes BE
  ├─ MsgCount   : 1 byte
  ├─ ChannelID  : 2 bytes LE
  ├─ SeqNum     : 4 bytes LE
  ├─ SendTime   : 4 bytes LE
  └─ (padding)
[Message 1]
  ├─ MsgSize    : 2 bytes LE
  ├─ MsgType    : 2 bytes LE
  └─ [payload …]
[Message 2 …]
```

### 4.3 Timestamp Semantics

- **Stored as:** `int64` nanoseconds UTC in our Parquet output
- **Effective precision:** ~1 millisecond (HKEX system clock resolution is 10ms, timestamps are rounded)
- **Implication:** do not use HKEX timestamps for sub-millisecond analysis. For cross-market studies involving HSI, align on 10ms or coarser buckets.
- **This must be documented in any cross-market comparison** to avoid attributing false lead-lag to timestamp noise.

### 4.4 Price Encoding

- Prices are stored as raw `int32` integers in the binary feed
- The price denominator (number of decimal places) is defined in the Series Definition records and varies by product
- We store raw `int32` in Parquet — conversion to float happens at analysis time using the scale factor from the instrument definition
- **Action required in Phase 2 (Cleaning):** verify the price denominator for HSI against HKEX's instrument definition records before any price-level computation

### 4.5 Output Convention

```
product=HSI/year=2026/month=02/HSI_20260203_orders.parquet
product=HSI/year=2026/month=02/HSI_20260203_trades.parquet
```

Consistent with Databento Hive-style convention.

> **Reference:** HKEX OMD-D Interface Specification — available from HKEX Co-location & Market Data portal (requires account)

---

## 5. Cross-Source Timestamp Alignment Summary

| Source | Precision | Reliable? | Use as sort key? | Notes |
|---|---|---|---|---|
| Databento `ts_recv` | Nanosecond | ✅ Yes | ✅ Yes | NTP-synced Databento server clock |
| Databento `ts_event` (CME) | Nanosecond | ⚠️ Mostly | ❌ No | Non-monotonic on snapshots |
| Databento `ts_event` (Eurex) | Microsecond | ⚠️ Mostly | ❌ No | Last 3 digits always 000 |
| HKEX `ts_event` | ~10ms effective | ⚠️ Limited | ❌ No | Clock resolution is 10ms |

**Recommendation for cross-market studies:** use `ts_recv` for all intra-source ordering; use `ts_event` for cross-venue latency and lead-lag, but always sanity-check monotonicity and precision before drawing conclusions.

---

## 6. Parquet Output Convention (all sources)

```
data/market_data/product={TICKER}/year={YYYY}/month={MM}/{TICKER}_{YYYYMMDD}_{kind}.parquet
```

Where `kind` is `orders` or `trades`.

**Compression:** Snappy (fast decompression, adequate compression ratio for tick data)
**Parquet version:** 2.6
**Dictionary encoding:** applied to `action` and `side` columns (low cardinality)
**Price storage:** raw integer (int32 for HKEX, int64 for Databento) — never float at rest

---

## 7. References

| Source | Document | URL |
|---|---|---|
| Databento | MBO schema field reference | https://docs.databento.com/api-reference-historical/schemas/mbo |
| Databento | DBN encoding specification | https://docs.databento.com/knowledge-base/new-users/dbn-encoding |
| Databento | Timestamp semantics | https://docs.databento.com/knowledge-base/new-users/fields-and-data/timestamps |
| CME Group | MDP 3.0 Market Data Interface spec | https://www.cmegroup.com/confluence/display/EPICSANDBOX/CME+MDP+3.0+Market+Data |
| Eurex | EOBI specification | https://www.eurex.com/ec-en/support/initiatives/eurex-eobi |
| HKEX | OMD-D Interface Specification | HKEX Co-location & Market Data portal (login required) |
