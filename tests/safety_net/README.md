# R0 Safety Net

This directory protects the pre-R0 behavior of the Destiny Research data stack before any semantic changes are made to normalization, reconstruction or feature engineering.

## Core rule

A regression diff is not automatically a bug and a golden mismatch is not automatically acceptable.

Every changed output must be classified as either:

- `EXPECTED_CHANGE` — linked to an approved R0 design decision and reviewed deliberately;
- `UNEXPECTED_REGRESSION` — unintended behavior change that blocks the merge.

Never regenerate a golden simply because a test failed.

## Layers

1. Existing fast goldens under `tests/regression/` provide row counts, distributions, bounds and a 50k-row sample checksum.
2. `shared/fingerprint.py` provides a bounded-memory full-file semantic SHA-256 plus a physical file SHA-256.
3. `capture_baseline.py` captures local fingerprints for the selected real-data references without committing vendor data.
4. `check_baseline.py` compares current outputs with that captured manifest.
5. `rerun_canary.py` reruns one real-data normalization -> optional HKEX post-process -> reconstruction chain in an isolated temporary workspace and persists provenance/diagnostics.
6. Characterization tests and JSON fixtures freeze legacy behavior explicitly, including behavior that may later be intentionally changed.
7. `tests/fixtures/normative/` contains human-readable synthetic contracts that exercise production adapter -> validator -> reconstruction paths.
8. `diff_outputs.py` produces bounded baseline-vs-candidate semantic diffs and requires explicit human classification for changed outputs.

## Baseline code

Pre-R0 reference commit:

`69ef65ca3c17df63a72e8fef371140b5b7bc0db0`

This SHA is a historical reference, not an assertion that every behavior at that commit is correct.

## Code-only preflight

Always activate the project virtual environment first:

```bash
source .venv/bin/activate
python tests/safety_net/run_preflight.py
```

The preflight now fails fast when Python < 3.11 or required Arrow/DuckDB modules are unavailable. It runs fingerprint/canary/differential self-tests, baseline-compatible normative synthetic fixtures, HKEX legacy characterization and the existing on-disk goldens.

## Local full-output baseline

From the repository root:

```bash
mkdir -p ~/.local/share/destiny/baselines

python tests/safety_net/capture_baseline.py \
  --output ~/.local/share/destiny/baselines/pre_r0_manifest.json

python tests/safety_net/check_baseline.py \
  ~/.local/share/destiny/baselines/pre_r0_manifest.json
```

The capture is read-only. It fingerprints the existing normalized/reconstructed outputs on disk.

## Deep pipeline rerun — preferred R0 path

Rerun canaries in an isolated workspace instead of overwriting the main corpus:

```bash
python tests/safety_net/rerun_canary.py --product ES
python tests/safety_net/rerun_canary.py --product FDAX
python tests/safety_net/rerun_canary.py --product HSI
```

Canary report v2 writes normalized/reconstructed outputs under `/tmp/destiny_r0_canary/<PRODUCT>_<DATE>/` and produces a `canary_report.json` containing:

- machine-independent raw-input path(s), file sizes and streaming SHA-256 hashes;
- Python/PyArrow/DuckDB/pandas/Databento runtime versions;
- adapter counters;
- validator counts including `n_warmup_skip`;
- rejected-event reason distribution;
- full normalization fingerprints before/after the current HKEX post-processor;
- true reconstruction orphan counters returned by `reconstruct_day()`;
- full reconstructed-output fingerprint;
- same-date `MATCH`, `MISMATCH` or `NO_REFERENCE` comparison against the existing local MBO/rejected/MBP1 corpus.

This is the preferred deep-check path because it does not mutate the existing normalized/reconstructed corpus.

## Normative synthetic fixtures

Baseline-compatible fixtures can be run directly:

```bash
python -m pytest tests/normative/test_normative_fixtures.py -v
```

The current Databento core set covers ADD, partial/full CANCEL, MODIFY size/price, CLEAR, spread zero/negative prices, duplicate detection and same-sequence atomic boundaries. See `tests/fixtures/normative/README.md` for the exact matrix state.

Legacy characterization and normative expectations are intentionally separate. Do not copy a suspicious legacy behavior into the normative fixture set merely because the current code emits it.

## Baseline-vs-candidate differential

Compare two normalized MBO files:

```bash
python tests/safety_net/diff_outputs.py \
  /path/to/baseline_mbo.parquet \
  /path/to/candidate_mbo.parquet \
  --kind mbo \
  --report /tmp/mbo_diff.json \
  --allow-unclassified
```

Use `--kind mbp1` for reconstructed outputs.

A changed output receives a deterministic `diff_signature` and is `UNCLASSIFIED` by default. Exploratory runs may use `--allow-unclassified`; merge gates must not.

To classify an approved change, create a small JSON file bound to the exact signature:

```json
{
  "diff_signature": "<exact signature from the diff report>",
  "classification": "EXPECTED_CHANGE",
  "decision_ref": "ADR-or-R0-decision-id",
  "note": "Human-readable reason this output change is intended."
}
```

Then rerun with `--classification /path/to/classification.json`. A stale classification for another diff is rejected. `EXPECTED_CHANGE` requires a decision reference; `UNEXPECTED_REGRESSION` returns a blocking exit code.

## Reviewed R0.1/R0.2 classification replay

The six-canary R0.1/R0.2 qualification generated twelve reviewed MBO/MBP1 signatures from candidate commit `c731de7f41769a7234778e911a101a4afa4b07c5`. Their exact human-reviewed classifications are committed in:

`tests/safety_net/classifications/r0_1_r0_2.json`

Do not rerun the expensive ingestion/reconstruction canaries merely to apply these classifications. Reuse the existing `/tmp/destiny_r0_candidate_canary` evidence:

```bash
python tests/safety_net/run_candidate_preflight.py
python tests/safety_net/classify_candidate_evidence.py
```

`classify_candidate_evidence.py` is intentionally strict:

- the existing qualification summary must reference the exact reviewed candidate commit;
- all six canaries must be present;
- every classification must bind the exact current `diff_signature`;
- stale signatures are rejected by `apply_classification()`;
- classified reports are written beside the original unclassified reports rather than overwriting them;
- success requires every reviewed changed output to resolve to `EXPECTED_CHANGE` (or `NO_CHANGE` if a future identical report is deliberately left unclassified).

The replay writes `/tmp/destiny_r0_candidate_canary/candidate_qualification_classified_summary.json`. Corrected goldens may be frozen only after this replay passes locally.

## Historical deep runner

The historical `python tests/run_all_checks.py` deep mode still writes through the production pipeline. For R0 work, use it only deliberately; `--skip-pipeline` remains the safe fast-golden mode.

## Vendor data

Do not commit Databento/HKEX source data or extracted real-data windows until redistribution rights have been checked explicitly. Synthetic fixtures are preferred for public deterministic tests.
