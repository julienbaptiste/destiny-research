# R0 normative synthetic fixtures

These fixtures are human-reviewable contracts for provider -> normalized MBO -> validation -> reconstruction behavior.

They are deliberately separate from `tests/fixtures/legacy_pre_r0/`:

- **legacy characterization** records what the pre-R0 implementation actually does, including behavior later judged incorrect;
- **normative fixtures** record behavior accepted as correct by the current specification/design review.

A legacy failure may be an intentional semantic change. A normative failure is blocking once the fixture applies to the candidate implementation.

## Fixture fields

Every normative case contains:

- `id`: matrix ID (`DB-01`, `HK-01`, ...);
- provider/session metadata;
- provider-shaped `source_events`;
- exact adapter output expectations;
- validator counters and rejection reasons;
- final active order state;
- exact MBP-1 rows;
- reconstruction orphan counters.

`baseline_compatible=true` means the expectation is both normative and already compatible with the frozen pre-R0 implementation, so CI/preflight executes it now.

`baseline_compatible=false` is reserved for approved future semantics that intentionally differ from the legacy baseline. Such fixtures are shape-checked but must not be asserted against pre-R0 production code. They become active when the relevant candidate implementation is tested.

Never set `baseline_compatible=true` merely to make a test pass.

## Current Databento coverage

Implemented and baseline-compatible:

- DB-01 single ADD
- DB-02 partial CANCEL
- DB-03 full CANCEL
- DB-04 MODIFY size decrease
- DB-05 MODIFY size increase
- DB-06 MODIFY price
- DB-08 CLEAR
- DB-13 calendar spread zero/negative prices
- DB-14 duplicate event
- DB-15 same-sequence atomic boundary

Still pending an explicit R0.1 contract decision or additional fixture work:

- DB-07 TRADE/FILL/CANCEL atomic group
- DB-09 F_TOB replace
- DB-10 F_TOB undefined-price side clear
- DB-11 F_SNAPSHOT warmup
- DB-12 provider bad-timestamp/control flags

DB-07/09/10/12 are intentionally not frozen from the legacy implementation because action/side/flag semantics are under R0.1 design review.

## Current HKEX coverage

HKEX pre-R0 behavior is extensively characterized under `tests/characterization/` and `tests/test_hkex_synthetic_cancel.py`.

The HK-01...HK-18 **normative** matrix is not yet activated because exact flags, trade/fill/cancel atomicity, `order_id=0`, combo/deal-type handling and subsequence ordering are R0.1/R0.2 decisions. Those expectations must be approved first; legacy behavior must not be copied into normative fixtures by default.
