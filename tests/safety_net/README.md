# R0 Safety Net

This directory protects the pre-R0 behavior of the Destiny Research data stack before any semantic changes are made to normalization, reconstruction or feature engineering.

## Core rule

A regression diff is not automatically a bug and a golden mismatch is not automatically acceptable.

Every changed output must be classified as either:

- `EXPECTED CHANGE` — linked to an approved R0 design decision and reviewed deliberately;
- `REGRESSION` — unintended behavior change that blocks the merge.

Never regenerate a golden simply because a test failed.

## Layers

1. Existing fast goldens under `tests/regression/` provide row counts, distributions, bounds and a 50k-row sample checksum.
2. `shared/fingerprint.py` provides a bounded-memory full-file semantic SHA-256 plus a physical file SHA-256.
3. `capture_baseline.py` captures local fingerprints for the selected real-data canaries without committing vendor data.
4. `check_baseline.py` compares current outputs with that captured manifest.
5. Characterization tests freeze legacy behavior explicitly, including behavior that may later be intentionally changed.
6. Normative synthetic fixtures will be added after the legacy safety net is proven reliable.

## Baseline code

Pre-R0 reference commit:

`69ef65ca3c17df63a72e8fef371140b5b7bc0db0`

This SHA is a historical reference, not an assertion that every behavior at that commit is correct.

## First local capture

From the repository root:

```bash
python -m pytest tests/safety_net/test_fingerprint.py -v
python tests/run_all_checks.py --skip-pipeline --verbose
python tests/safety_net/capture_baseline.py --output /tmp/destiny_pre_r0_manifest.json
python tests/safety_net/check_baseline.py /tmp/destiny_pre_r0_manifest.json
```

The capture is read-only. It fingerprints the existing normalized/reconstructed outputs on disk.

## Deep pipeline rerun

Only after the initial manifest has been captured:

```bash
python tests/run_all_checks.py
```

The normalization regression runner now resolves the exact Databento raw file for the selected golden date instead of silently rerunning a whole product tree. HKEX reruns the exact selected date. Existing outputs for those golden days are overwritten, so keep the pre-rerun manifest as the comparison anchor.

## Vendor data

Do not commit Databento/HKEX source data or extracted real-data windows until redistribution rights have been checked explicitly. Synthetic fixtures are preferred for public deterministic tests.
