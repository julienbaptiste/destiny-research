# Regression Tests

Golden-file regression tests for all market cleaning pipelines.

## Structure

```
tests/regression/
  shared/
    __init__.py
    metrics.py          ← all shared logic: extraction, checksum, comparison, I/O
  eurex/
    generate_golden.py  ← Eurex golden generator
    check_regression.py ← Eurex regression checker
    golden/             ← committed JSON reference files
      FDAX_2025-05-02_metrics.json
      FDAX_2025-05-12_metrics.json
      FDAX_2025-05-26_metrics.json
      FESX_*.json
      FSMI_*.json
  cme/                  ← (to be added — ES, NIY, NKD)
    generate_golden.py
    check_regression.py
    golden/
  hkex/                 ← (to be added — HSI, MHI, HHI, MCH)
    generate_golden.py
    check_regression.py
    golden/
```

## Principe

Le pipeline est **déterministe** sur des données d'entrée fixes.
On exploite cette propriété : on fixe un golden dataset, on génère les métriques de référence
une fois, on les commite, et on compare après chaque modification du pipeline.

Les régressions silencieuses détectées :
- Changement de row count (nouveau filtre, filtre cassé)
- Changement de valeurs LOB (bug state machine, mauvais tri)
- Changement d'ordre de tri (ORDER BY manquant)
- Dérive des spreads ou du TOB change rate

## Métriques trackées (shared/metrics.py)

| Métrique | Tolérance | Rationale |
|---|---|---|
| `orders_clean_row_count` | exacte | Toute modification du filtre change le count |
| `trades_clean_row_count` | exacte | idem |
| `trades_unpaired_aggressor_count` | exacte | Sensible au T/F pairing logic |
| `lob1_row_count` | exacte | State machine déterministe |
| `tob_change_count` | exacte | TOB detection logic |
| `crossed_book_count` | exacte | Warm-up burn-in count |
| `lob1_sample_checksum_sha256` | exacte | SHA-256 sur 50k events × colonnes critiques |
| `tob_change_rate` | 1e-6 | Float ratio |
| `spread_median/p25/p75/p95` | exacte | Raw int64 ticks |

## Setup initial (Eurex)

```bash
# Depuis la racine du repo
python tests/regression/eurex/generate_golden.py

# Commiter les golden files
git add tests/regression/eurex/golden/
git commit -m "test(regression): add Eurex pipeline golden files"
```

## Vérification après une modification

```bash
# Full check — re-run pipeline + compare
python tests/regression/eurex/check_regression.py

# Quick — compare existing outputs only (no pipeline re-run)
python tests/regression/eurex/check_regression.py --skip-pipeline

# Single product, verbose
python tests/regression/eurex/check_regression.py --product FDAX --verbose
```

## Interpréter un FAIL

```
[FAIL] 2 regression(s) detected:
  ✗ lob1_row_count: golden=2487321  current=2487319  [EXACT MISMATCH]
  ✗ lob1_sample_checksum_sha256: golden=a3f2...  current=b1c9...  [EXACT MISMATCH]
```

**Row count drift** → un filtre a changé. Normal si intentionnel — mettre à jour les goldens.  
**Checksum seul** → même nombre de rows, valeurs différentes → bug dans la state machine LOB
ou ORDER BY manquant quelque part.

## Mise à jour des goldens après un changement intentionnel

```bash
python tests/regression/eurex/generate_golden.py

git add tests/regression/eurex/golden/
git commit -m "test(regression): update Eurex goldens — Rule 6 added (iceberg detection)"
```

## Ajouter un nouveau marché (CME, HKEX, ...)

1. Créer `tests/regression/{market}/`
2. Copier `eurex/generate_golden.py` et `eurex/check_regression.py`
3. Adapter `GOLDEN_CONFIG`, `PIPELINE_SCRIPT`, `PIPELINE_VERSION`
4. `shared/metrics.py` n'est pas à toucher sauf pour ajouter des métriques communes

## Golden dataset Eurex

| Produit | Date | Rationale |
|---|---|---|
| FDAX | 2025-05-02 | Premier jour du mois, warm-up clean |
| FDAX | 2025-05-12 | Lundi, bord de semaine |
| FDAX | 2025-05-26 | Lundi avant Pentecôte (clôture atypique) |
| FESX | 2025-05-02 | OTR ~14:1, comportement différent de FDAX |
| FESX | 2025-05-12 | Lundi |
| FESX | 2025-05-26 | Lundi avant Pentecôte |
| FSMI | 2025-05-02 | Marché suisse, faible volume |
| FSMI | 2025-05-12 | Lundi |