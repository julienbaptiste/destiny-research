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
5. `rerun_canary.py` reruns one real-data normalization → optional HKEX post-process → reconstruction chain in an isolated temporary workspace and persists diagnostics.
6. Characterization tests and JSON fixtures freeze legacy behavior explicitly, including behavior that may later be intentionally changed.
7. Normative synthetic fixtures will be added only after the legacy safety net is proven reliable.

## Baseline code

Pre-R0 reference commit:

`69ef65ca3c17df63a72e8fef371140b5b7bc0db0`

This SHA is a historical reference, not an assertion that every behavior at that commit is correct.

## First local capture

From the repository root:

```bash
python tests/safety_net/run_preflight.py
python tests/safety_net/capture_baseline.py --output /tmp/destiny_pre_r0_manifest.json
python tests/safety_net/check_baseline.py /tmp/destiny_pre_r0_manifest.json
```

The capture is read-only. It fingerprints the existing normalized/reconstructed outputs on disk.

## Deep pipeline rerun — preferred R0 path

After the initial manifest exists, rerun canaries in an isolated workspace instead of overwriting the main corpus:

```bash
python tests/safety_net/rerun_canary.py --product ES
python tests/safety_net/rerun_canary.py --product FDAX
python tests/safety_net/rerun_canary.py --product HSI
```

Each run writes normalized/reconstructed outputs under `/tmp/destiny_r0_canary/<PRODUCT>_<DATE>/` and produces a `canary_report.json` containing:

- adapter counters;
- validator counts including `n_warmup_skip`;
- rejected-event reason distribution;
- full normalization fingerprints before/after the current HKEX post-processor;
- true reconstruction orphan counters returned by `reconstruct_day()`;
- full reconstructed-output fingerprint.

This is the preferred deep-check path because it does not mutate the existing normalized/reconstructed corpus.

The historical `python tests/run_all_checks.py` deep mode still writes through the production pipeline. For R0 work, use it only deliberately; `--skip-pipeline` remains the safe fast-golden mode.

## Vendor data

Do not commit Databento/HKEX source data or extracted real-data windows until redistribution rights have been checked explicitly. Synthetic fixtures are preferred for public deterministic tests.
