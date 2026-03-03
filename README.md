# Destiny Research

**Comparative Microstructure Study — Short-Term Alpha Persistence Across Liquidity Regimes**

A systematic research project analyzing order flow dynamics, alpha decay, and liquidity structure across equity index futures markets in the US, Europe, and Asia.

---

## Research Question

> *How does short-term alpha persist across different liquidity regimes in equity index futures, and what microstructure features best characterize these regimes?*

---

## Universe

| Ticker   | Product              | Zone   | Source    | Timestamp precision |
|----------|----------------------|--------|-----------|---------------------|
| ES       | E-mini S&P 500       | US     | Databento | Nanosecond          |
| FDAX     | DAX Future           | Europe | Databento | Microsecond         |
| FESX     | Euro Stoxx 50 Future | Europe | Databento | Microsecond         |
| FSMI     | SMI Future           | Europe | Databento | Microsecond         |
| NIY      | Nikkei Future (JPY)  | Asia   | Databento | Nanosecond          |
| NKD      | Nikkei Future (USD)  | Asia   | Databento | Nanosecond          |
| HSI      | Hang Seng Future     | Asia   | HKEX      | 10ms (HKEX clock)   |
| SGX STI  | Straits Times Future | Asia   | SGX       | Microsecond         |

Three liquidity tiers: **high** (ES, FESX), **medium** (FDAX, FSMI, NIY/NKD), **low** (HSI, SGX STI).

---

## Pipeline

```
Raw tick data (Parquet, per product per day)
    → Cleaning & timestamp normalization
    → Feature engineering (OFI, spread, depth, momentum, volatility)
    → Alpha signal construction
    → IC / alpha decay analysis
    → Cross-market regime comparison
```

---

## Repository Structure

```
destiny-research/
├── src/
│   ├── ingestion/      # Parsers: Databento, HKEX (binary), SGX (CSV)
│   ├── cleaning/       # Outlier removal, timestamp alignment, LOB reconstruction
│   ├── features/       # OFI, spread, momentum, volatility clustering
│   ├── alpha/          # IC computation, alpha decay, regime detection
│   └── utils/          # Parquet I/O, plotting helpers
├── notebooks/
│   ├── 01_data_overview/
│   ├── 02_cleaning/
│   ├── 03_feature_engineering/
│   ├── 04_alpha_decay/
│   └── 05_cross_market/
├── reports/
│   ├── figures/        # Exported plots (PNG/SVG)
│   └── paper_draft.md
└── tests/
```

---

## Key Features Computed

- **Order Flow Imbalance (OFI)** — bid/ask pressure asymmetry at multiple LOB levels
- **VPIN** — volume-synchronized probability of informed trading
- **Spread & depth dynamics** — quoted/effective spread, top-of-book depth ratio
- **Momentum signals** — short-term price momentum at various horizons
- **Volatility clustering** — realized volatility regimes, intraday patterns

---

## Research Angles

1. **Alpha decay curves** — IC as a function of forward horizon (1s → 10min) per product
2. **Liquidity gradient** — microstructure differences from ES (most liquid) to SGX STI (least liquid)
3. **Cross-zone comparison** — US open vs European open vs Asian session overlap effects
4. **NIY/NKD pair** — currency arbitrage and differential liquidity on the same underlying

---

## Stack

- **Python 3.11** — analysis, feature engineering, modeling
- **C++** (via pybind11) — performance-critical computations
- **Parquet** — tick data storage (Hive-style partitioning: `product=/year=/month=/`)
- **Apache Spark** — large-scale feature computation
- **JupyterLab** — exploratory analysis and reporting

---

## Data Note

Raw tick data is stored locally and not committed to this repository.  
Data directory structure follows Hive-style partitioning:
```
data/market_data/product=ES/year=2024/month=01/ES_20240115.parquet
```

---

## Status

| Phase | Description                  | Status      |
|-------|------------------------------|-------------|
| 0     | Environment setup            | In progress |
| 1     | Data acquisition             | Pending     |
| 2     | Cleaning & normalization     | Pending     |
| 3     | Feature engineering          | Pending     |
| 4     | Alpha decay & robustness     | Pending     |
| 5     | Reporting & GitHub polish    | Pending     |

---

*Personal research project — conducted during relocation period, 2024–2025.*
