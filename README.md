# Destiny Research

**Comparative Microstructure Study — Short-Term Alpha Persistence Across Liquidity Regimes**

A systematic research project analyzing order flow dynamics, alpha decay, and microstructure
structure across equity index futures markets in the US, Europe, and Asia. Built from raw
tick data, from binary feed parsing to validated limit order book reconstruction to
feature engineering.

---

## Research Question

> *How does short-term alpha persist across different liquidity regimes in equity index futures,
> and what microstructure features best characterize these regimes?*

The project compares 10 products across three geographic zones and three liquidity tiers,
with a particular focus on Asian markets where institutional-grade tick data pipelines are
rare in the public domain.

---

## Universe

| Ticker | Product | Zone | Exchange | Source | Timestamp |
|--------|---------|------|----------|--------|-----------|
| ES | E-mini S&P 500 Future | US | CME | Databento | Nanosecond |
| FDAX | DAX Future | Europe | EUREX | Databento | Nanosecond |
| FESX | Euro Stoxx 50 Future | Europe | EUREX | Databento | Nanosecond |
| FSMI | SMI Future | Europe | EUREX | Databento | Nanosecond |
| NIY | Nikkei 225 Future (JPY) | Asia | CME | Databento | Nanosecond |
| NKD | Nikkei 225 Future (USD) | Asia | CME | Databento | Nanosecond |
| HSI | Hang Seng Future | Asia | HKEX | HKEX direct | ~10ms |
| MHI | Mini Hang Seng Future | Asia | HKEX | HKEX direct | ~10ms |
| HHI | H-shares China Enterprises Future | Asia | HKEX | HKEX direct | ~10ms |
| MCH | Mini H-shares Future | Asia | HKEX | HKEX direct | ~10ms |

Three liquidity tiers: **high** (ES, FESX), **medium** (FDAX, FSMI, NIY, NKD), **low** (HSI, MHI, HHI, MCH).

---

## Pipeline Overview

```
[Raw data]
  Databento MBO .dbn.zst          HKEX OMD-D binary (.zip)
         │                                  │
         ▼                                  ▼
[Normalization]                    [Raw parser + Normalization]
  DatabentoAdapter                   hkex_raw_parser.py
  (ingestion/adapters/)              HKEXAdapter
         │                           (ingestion/adapters/)
         └──────────────┬────────────────────┘
                        ▼
              [Normalized MBO Parquet]
              provider-agnostic schema
              (data/normalized/)
                        │
                        ▼
              [MBP-1 Reconstruction]
              build_mbp1.py
              LOB state machine
              (data/reconstructed/)
                        │
                        ▼
              [Feature Engineering]
              OFI, VPIN, spread,
              lead-lag, alpha decay
              (data/features/)
```

The normalization layer produces a **provider-agnostic MBO schema** — the reconstruction
engine and all downstream feature code are identical regardless of whether the data comes
from Databento or HKEX. Adding a new exchange requires only a new adapter.

---

## Project Structure

```
destiny-research/
│
├── ingestion/
│   ├── adapters/
│   │   ├── base.py                  # Abstract adapter interface
│   │   ├── databento_adapter.py     # Databento MBO → normalized MBO
│   │   └── hkex_adapter.py          # HKEX OMD-D → normalized MBO
│   ├── schema.py                    # Normalized MBO schema + action/side enums
│   ├── validator.py                 # Per-event validation (STRICT / LOOSE modes)
│   └── ingest.py                    # Ingestion orchestrator CLI
│
├── hkex_parser/
│   ├── hkex_raw_parser.py           # OMD-D binary → raw Parquet (orders + trades)
│   └── messages.py                  # OMD-D message struct definitions
│
├── reconstruction/
│   └── build_mbp1.py                # Normalized MBO → MBP-1 LOB reconstruction
│
├── DestinyResearch/
│   ├── dr.py                        # Unified data access API (installable package)
│   ├── market_config.py             # Per-product specs, RTH bounds, tick sizes
│   └── features/
│       └── otr.py                   # Order-to-Trade Ratio intraday profile
│
├── tests/
│   ├── regression/                  # Golden-file regression tests (normalization + reconstruction)
│   ├── validation/
│   │   └── validate_mbp1_hkex.py   # 5-check microstructure invariant validator (HKEX)
│   ├── test_hkex_parser.py          # Unit tests — OMD-D binary parser
│   └── test_hkex_synthetic_cancel.py # Unit tests — HKEX adapter translate() logic
│
├── notebooks/
│   ├── 01_data_overview/            # Cross-market OTR, cancel rate, fill rate
│   ├── 02_hkex_artifacts/           # HKEX feed artifacts documentation
│   └── 03_rolling_metrics/          # Rolling OTR and cancel rate (NIY/NKD)
│
├── docs/
│   ├── hkex_omd_d_specificities.md  # HKEX OMD-D protocol details and bugs resolved
│   └── hkex_action_normalization_phase3.md  # Planned FILL_CANCEL/MODIFY normalization
│
└── reports/
    └── figures/                     # Exported static plots (PNG)
```

---

## HKEX Pipeline — A Note for Practitioners

Working with raw HKEX OMD-D data at the order-book level is non-trivial. Unlike Databento
(which abstracts away exchange-specific quirks), the HKEX feed requires careful handling
of several protocol-specific behaviors that are either underdocumented or documented
incorrectly in informal resources. This project implements a production-grade pipeline
and documents everything encountered.

### What makes HKEX different

**Protocol specifics:**
- Raw data ships as two separate binary files per day: one for order events
  (AddOrder/DeleteOrder/OrderbookClear) and one for trade events. These must be merged
  into a single chronological stream using the global `seq_num` counter as the sort key —
  timestamp-based merging produces incorrect results at millisecond ties.
- `DeleteOrder (332)` carries `price=0` and `quantity=0` in the raw feed — both fields
  must be resolved from a shadow state tracking resting orders by `(orderbook_id, order_id, side)`.
- `Trade (350)` carries the **passive** resting order's `order_id` (not the aggressor).
  The passive order must be decremented by the traded quantity — HKEX emits no separate
  fill event for this.
- HKEX never emits `ModifyOrder (331)`. Price/size changes are represented as
  Delete+Add pairs, which must be treated carefully in downstream feature engineering.

**Timestamp quirks:**
- The matching engine clock (`trade_time_ns`) has ~10ms precision despite nanosecond
  storage. The dissemination clock (`send_time_ns`) has ~1ms precision. These can
  diverge by up to ~15ms, causing trades to appear to execute outside the quoted spread
  in the reconstructed book when market makers re-quote within the jitter window.
  See `notebooks/02_hkex_artifacts/` for illustrated examples.

**order_id reuse:**
- `order_id` is not globally unique. The same value is reused across different contracts
  (different `orderbook_id`) and — for combination orders — across both sides of the book
  simultaneously. The shadow state and the reconstruction engine both key on
  `(order_id, side)` to handle this correctly.

All of these behaviors are documented in detail in `docs/hkex_omd_d_specificities.md`,
with root cause analysis, fixes applied, and known remaining limitations.

### Validation

Since HKEX does not sell a reference MBP-1 feed (unlike Databento for CME/EUREX),
the reconstruction is validated using microstructure invariants:

1. No crossed spread during continuous trading
2. Trade price within previous bid/ask spread
3. Spread ≥ 1 tick during continuous trading
4. No negative sizes
5. No side dropping to null during RTH except after CLEAR

Front-month contracts (HSIG26, MHIG26, HHIG26, MCHG26) achieve **RELIABLE** status
(0 crossed spreads, 0–0.002% outside spread) across all available trading days.

---

## Research Axes

1. **Order Flow Imbalance (OFI)** — bid/ask pressure asymmetry, alpha decay across horizons
2. **VPIN** — volume-synchronized probability of informed trading
3. **Cross-market lead-lag** — NIY/NKD price discovery; HSI/MHI size fragmentation
4. **Microstructure regimes** — OTR and cancel rate stability over time
5. **Order lifetime and iceberg detection** — cancel patterns, hidden liquidity

---

## Getting Started

### Requirements

- Python 3.11+
- Dependencies: see `requirements.txt`

```bash
git clone https://github.com/julienbaptiste/destiny-research.git
cd destiny-research
uv pip install -r requirements.txt
uv pip install -e .          # installs DestinyResearch package in editable mode
```

### Data

Raw data is not included in this repository. The pipeline supports:
- **Databento** — purchase MBO data via [databento.com](https://databento.com) and place
  `.dbn.zst` files in `data/binaries/provider=databento/`
- **HKEX** — purchase historical data via [data.hkex.com.hk](https://data.hkex.com.hk)
  and place zip archives in `data/binaries/provider=HKEX/product=OMDD/`

### Running the pipeline

```bash
# Parse HKEX binary → raw Parquet
python hkex_parser/hkex_raw_parser.py --month 2026-02 --products HSI MHI HHI MCH

# Normalize (Databento or HKEX) → provider-agnostic MBO Parquet
python -m ingestion.ingest hkex --product HSI MHI HHI MCH --month 2026-02
python -m ingestion.ingest file data/binaries/.../glbx-mdp3-20251001.mbo.dbn.zst

# Reconstruct MBP-1 LOB
python reconstruction/build_mbp1.py --product HSI HHI MHI MCH --all-data
python reconstruction/build_mbp1.py --product ES --all-data

# Validate HKEX reconstruction
python tests/validation/validate_mbp1_hkex.py --product HSI --contract HSIG26 \
    --date 20260202 20260203 20260210

# Run regression tests
python tests/run_all_checks.py --skip-pipeline
```

### Data access API

```python
import DestinyResearch as dr

# Load normalized MBO — RTH filtered
tbl = dr.get_mbo_front_rth("HSI", "2026-02-02")

# Load reconstructed MBP-1 — RTH filtered
tbl = dr.get_mbp1_front_rth("ES", "2025-10-10",
                              columns=["ts_recv", "bid_px_00", "ask_px_00",
                                       "bid_sz_00", "ask_sz_00"])

# Daily microstructure stats (DuckDB, zero RAM)
stats = dr.get_contract_stats("HSIG26", "2026-02-02")
print(stats["order_to_trade_ratio"], stats["cancel_rate"])

# Cross-market summary
df = dr.get_cross_market_summary(["ES", "FDAX", "HSI", "NIY"], "2026-02-02")
```

---

## Status

| Phase | Description | Status |
|-------|-------------|--------|
| 0 — Setup | Environment, Git, project structure | ✅ Done |
| 1 — Data Acquisition | Databento + HKEX pipelines | ✅ Done |
| 2 — Normalization & Reconstruction | Provider-agnostic MBO + MBP-1 LOB | ✅ Done |
| 3 — Feature Engineering | OFI, VPIN, lead-lag, alpha decay | 🔄 In progress |
| 4 — Alpha Decay & Robustness | IC rolling, regime analysis, cross-market | ⬜ Planned |
| 5 — Reporting | Paper draft, figures, GitHub polish | ⬜ Planned |

---

## Author

Julien — Quantitative Researcher