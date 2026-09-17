# R0 normative synthetic fixtures

These fixtures are human-reviewable contracts for provider -> normalized MBO -> validation -> reconstruction behavior.

They are deliberately separate from `tests/fixtures/legacy_pre_r0/`:

- **legacy characterization** records what the pre-R0 implementation actually does, including behavior later judged incorrect;
- **normative fixtures** record behavior accepted as correct by the current specification/ADRs.

A legacy failure may be an intentional semantic change. A normative failure is blocking once the fixture applies to the candidate implementation.

## Governance

Every matrix ID must exist exactly once:

- `DB-01` through `DB-15`;
- `HK-01` through `HK-18`.

The normative test suite checks matrix completeness and duplicate IDs.

Every future target with `baseline_compatible=false` must include `decision_ref` pointing to the ADR that authorizes the semantic change. Future targets must contain the same expected-result sections as executable fixtures; placeholders are not accepted.

`baseline_compatible=true` means the expectation is normative **and** already compatible with the frozen pre-R0 implementation, so CI/preflight executes it now.

`baseline_compatible=false` means the expectation describes an approved target that intentionally differs from pre-R0 code. It is structurally audited now and becomes executable when the relevant candidate implementation is under test.

Never set `baseline_compatible=true` merely to make a test pass.

## Fixture fields

Every normative case contains:

- `id`: matrix ID (`DB-01`, `HK-01`, ...);
- provider/session metadata;
- provider-shaped `source_events`;
- exact/target adapter output expectations;
- validator counters and rejection reasons;
- final active order state;
- expected MBP-1 state transitions;
- reconstruction orphan counters.

Future R0.1/R0.2 fixtures may express `norm_flags` by semantic names before physical bit assignments are frozen. The implementation PR must bind those names to the final `uint16` values and activate the target executor.

## Databento matrix

Baseline-compatible and executed against pre-R0 code:

- DB-01 single ADD;
- DB-02 partial CANCEL;
- DB-03 full CANCEL;
- DB-04 MODIFY size decrease;
- DB-05 MODIFY size increase;
- DB-06 MODIFY price;
- DB-08 CLEAR;
- DB-13 calendar spread zero/negative prices;
- DB-14 duplicate event;
- DB-15 same-sequence atomic boundary.

Approved R0.1 targets linked to ADR-003:

- DB-07 TRADE/FILL/CANCEL atomic group;
- DB-09 canonical `F_TOB=0x40` side replacement;
- DB-10 `F_TOB + UNDEF_PRICE` side clear;
- DB-11 `F_SNAPSHOT` warmup with provider `F_BAD_TS_RECV=0x08` preserved;
- DB-12 provider control/bad-timestamp flags preserved rather than remapped.

## HKEX matrix

HKEX pre-R0 behavior remains characterized separately under `tests/characterization/` and `tests/test_hkex_synthetic_cancel.py`.

The complete HK-01...HK-18 target matrix is linked to ADR-004/ADR-003 and freezes the corrected design before production implementation:

- HK-01 AddOrder 330;
- HK-02 DeleteOrder 332 with residual size/price resolution;
- HK-03 partial Trade 350;
- HK-04 full Trade 350;
- HK-05 two partial trades;
- HK-06 passive BID execution side mapping;
- HK-07 passive ASK execution side mapping;
- HK-08 printable `order_id=0` trade retained without book mutation;
- HK-09 Delete after partial fill;
- HK-10 Delete after full fill anomaly / no double decrement;
- HK-11 OrderbookClear 335;
- HK-12 native ModifyOrder 331 absolute quantity;
- HK-13 adjacent Delete+Add remains native CANCEL+ADD;
- HK-14 interleaved Delete+Add remains native CANCEL+ADD;
- HK-15 combo/deal-type printable policy;
- HK-16 `(seq_num,msg_index)` ordering and collision-free `subsequence`;
- HK-17 no intermediate MBP-1 snapshot inside a trade group;
- HK-18 unknown/truncated-session anomaly accounting.

These HKEX targets are intentionally not executed against pre-R0 production code because several of them describe known corrections: canonical flags/provenance, atomic Trade expansion, retained `order_id=0` printable trades, explicit subsequence ordering and removal of adjacency-based Delete+Add relabeling.
