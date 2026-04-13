# HKEX — Action Semantic Normalization (Phase 3 Task)

*Written 2026-04-13 — to be picked up at the start of Phase 3 feature engineering*

---

## CONTEXT

The current normalized MBO stream uses `Action.CANCEL` for three semantically
distinct events. Mixing them in OTR/cancel_rate/fill_rate calculations produces
inflated and cross-provider-incomparable metrics. This must be fixed before
computing any microstructure features.

---

## THE THREE TYPES OF CANCEL IN HKEX NORMALIZED STREAM

### Type 1 — Pure cancel (participant removes their order)
- **Source:** DeleteOrder (332) that is NOT followed by an AddOrder (330) with
  the same `order_id`
- **Correct action:** `Action.CANCEL` (keep as-is)
- **Interpretation:** Participant withdrew their order from the book

### Type 2 — Fill-cancel (matching engine decrements after execution)
- **Source:** Synthetic CANCEL emitted by the adapter after Trade (350)
  with `order_id > 0`
- **Correct action:** `Action.FILL_CANCEL` (new action type)
- **Interpretation:** Order was (partially or fully) executed — not a
  participant decision

### Type 3 — Modify-cancel (first half of a Delete+Add requote)
- **Source:** DeleteOrder (332) immediately followed by AddOrder (330)
  with the same `order_id`
- **Correct action:** Drop the CANCEL and change the ADD to `Action.MODIFY`
- **Interpretation:** Participant moved their quote to a new price/size —
  NOT a cancellation. The order immediately reappears.

---

## IMPACT ON METRICS

Current state (all three types counted as `CANCEL`):

```
cancel_rate = n_cancel / n_add
```

- `n_cancel` is inflated by Type 2 (≈ +1 per trade) and Type 3 (≈ hundreds
  per active MM order_id per day)
- `n_add` is inflated by Type 3 (the ADD half of the Del+Add pair)
- OTR is NOT affected (only uses `n_add` and `n_trade`)
- `fill_rate = n_trade / n_add` is deflated by Type 3 (excess ADDs)

Target state after normalization:

```python
# These formulas remain unchanged — the data is correctly labeled
cancel_rate = n_cancel    / n_add     # only pure participant cancels
fill_rate   = n_trade     / n_add     # only genuine order submissions
otr         = n_add       / n_trade   # unchanged, already correct
```

---

## PROPOSED SOLUTION

**Principle:** features code stays unchanged. The adapter normalizes semantics
so that action labels mean the same thing cross-provider. No provider-specific
logic in `dr.py`, `otr.py`, or any feature module.

### Step A — Introduce `Action.FILL_CANCEL` in `schema.py`

```python
class Action:
    ...
    FILL_CANCEL : Final[str] = "FILL_CANCEL"   # fill-induced decrement
```

In `hkex_adapter.py`: change synthetic CANCEL action from `Action.CANCEL`
to `Action.FILL_CANCEL`.

In `build_mbp1.py`: treat `FILL_CANCEL` identically to `CANCEL` in `_apply_book`.

In `validator.py`: add `FILL_CANCEL` to `BOOK_ACTIONS` and `ORDER_ID_REQUIRED`.

In `dr.py` `get_contract_stats()`: add `n_fill_cancel` counter. Keep `n_cancel`
as pure participant cancels only.

### Step B — Reconstitute MODIFY from Delete+Add pairs in `hkex_adapter.py`

When a Delete (332) is followed by an Add (330) with the same `order_id`
(and no intervening event for that `order_id`), emit a single `MODIFY` instead
of `CANCEL + ADD`.

The MODIFY carries the price and size from the Add (the new quote).

**Implementation approach:** one-event lookahead buffer in `iter_events()`.
Buffer a CANCEL event. On the next event for the same `order_id`:
- If it is an ADD → emit MODIFY, clear buffer
- If it is anything else → emit the buffered CANCEL, then process the new event

This requires buffering across batch boundaries in `_iter_raw()`.

---

## OPEN QUESTIONS (to answer empirically before implementing)

### Q1 — Can non-same-order_id events appear between a Delete and its Add?

Observed empirically: yes — other order_ids can appear between the Delete
and the Add of a modify pair. The seq_num gap can be tens or hundreds of
messages.

**Impact on implementation:** the lookahead cannot be "next event globally"
— it must be "next event for this specific order_id". This requires a pending
dict `{order_id: buffered_cancel}` rather than a single slot buffer.

**To verify:** is there a maximum seq_num gap between the Delete and Add of
a modify pair? Or can it be arbitrarily large (e.g. if a price level is
re-entered minutes later)?

### Q2 — Are all Deletes on HKEX modify-cancels, or do genuine pure cancels exist?

If every Delete (332) is always followed by an Add (330) for the same
`order_id`, then all Deletes are modify-cancels and there are no pure
participant cancels on HKEX.

If some Deletes are NOT followed by an Add (genuine cancels), the lookahead
must distinguish the two cases.

**To verify empirically:**
```python
import duckdb
con = duckdb.connect()
con.execute("""
    WITH cancels AS (
        SELECT order_id, sequence,
               LEAD(action) OVER (PARTITION BY order_id ORDER BY sequence) AS next_action
        FROM read_parquet('.../HSIG26_20260202_mbo.parquet')
        WHERE action IN ('CANCEL', 'ADD')
    )
    SELECT
        SUM(CASE WHEN next_action = 'ADD'  THEN 1 ELSE 0 END) AS cancel_followed_by_add,
        SUM(CASE WHEN next_action != 'ADD' OR next_action IS NULL THEN 1 ELSE 0 END)
            AS cancel_not_followed_by_add
    FROM cancels
    WHERE action = 'CANCEL'
      AND order_id > 0
""").df()
```

### Q3 — Databento: how to identify fill-cancels in the stream?

Databento emits explicit `action='C'` (CANCEL) events for fill-induced
decrements. These appear in the same atomic group (same `ts_event`,
`sequence`) as a FILL or TRADE event for the same `order_id`.

Identifying them requires correlating CANCEL events with FILL/TRADE events
by `(order_id, atomic_group)`. Needs investigation notebook before
implementing in `databento_adapter.py`.

**To verify:** does every fill-induced CANCEL on Databento share the exact
same `(ts_event, sequence)` as its corresponding FILL/TRADE? Or is it the
next event in the stream?

---

## DOWNSTREAM IMPACT ON EXISTING METRICS

The OTR values in `CONTEXT.md` section 3 (cross-market summary table) will
change after this fix. Specifically for HKEX products:

- `cancel_rate` will **decrease** significantly (Type 2 and Type 3 removed)
- `fill_rate` will **increase** (Type 3 ADDs removed from `n_add`)
- `otr` will **decrease** (Type 3 ADDs removed from `n_add`)

The current values are marked as provisional in `CONTEXT.md`.
The cross-market comparison table must be recomputed after this fix.

---

## FILES TO MODIFY

| File | Change |
|---|---|
| `ingestion/schema.py` | Add `Action.FILL_CANCEL`; add to `BOOK_ACTIONS`, `ORDER_ID_REQUIRED` |
| `ingestion/adapters/hkex_adapter.py` | Synthetic CANCEL → `FILL_CANCEL`; Del+Add pair → `MODIFY` (lookahead buffer) |
| `ingestion/adapters/databento_adapter.py` | Tag fill-induced CANCELs as `FILL_CANCEL` (requires investigation first) |
| `reconstruction/build_mbp1.py` | Handle `FILL_CANCEL` in `_apply_book` (same as `CANCEL`) |
| `ingestion/validator.py` | Add `FILL_CANCEL` to `BOOK_ACTIONS` and `ORDER_ID_REQUIRED` |
| `DestinyResearch/dr.py` | Add `n_fill_cancel` counter in `get_contract_stats()` |
| `tests/test_hkex_synthetic_cancel.py` | Update tests: synthetic CANCEL action → `FILL_CANCEL` |

**After implementation:** re-normalize and re-reconstruct all HKEX data,
re-run validation, re-run regression tests, recompute cross-market summary.