# Corrected R0.1/R0.2 goldens

This namespace contains canonical post-migration regression references for the six real-data canaries qualified under ADR-003/ADR-004.

It is intentionally separate from the historical `tests/regression/normalization/golden/` and `tests/regression/reconstruction/golden/` directories. Those pre-R0 files remain immutable legacy evidence and must not be overwritten simply because the canonical semantics changed.

The corrected set is frozen only from `/tmp/destiny_r0_candidate_canary` after:

1. candidate preflight passes;
2. all six canaries complete;
3. all twelve MBO/MBP1 diffs are reviewed;
4. `classify_candidate_evidence.py` reports `PASS_CLASSIFIED` for ES, NIY, FDAX, FESX, HSI and MHI.

Freeze with:

```bash
python tests/safety_net/freeze_corrected_goldens.py
```

The freezer does **not** rerun ingestion or reconstruction. Before writing anything it re-computes the full bounded-memory semantic and physical fingerprints of every source Parquet and requires an exact match with the reviewed differential evidence. Divergent existing corrected goldens are never overwritten unless `--overwrite` is explicitly supplied after a newly reviewed qualification.

Each golden stores:

- the exact qualification candidate commit;
- the exact reviewed `diff_signature` and ADR decision reference;
- the full semantic/physical source fingerprint;
- a 50k-row fast checksum using the canonical checksum columns;
- MBO validator/rejected diagnostics or MBP1 reconstruction diagnostics from the canary report.

For MBO, the fast checksum contract includes `norm_flags`, `sequence` and `subsequence`. For MBP1 it includes `sequence` and `subsequence`.

Only the six qualified R0.1/R0.2 canaries belong in this namespace. Additional products require their own reviewed real-data qualification before being promoted to corrected canonical goldens.
