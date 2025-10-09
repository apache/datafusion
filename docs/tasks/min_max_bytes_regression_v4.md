# MinMaxBytes Regression Follow-Up (v4)

**Date:** October 11, 2025
**Commit Range:** `1eb9d9ac6^..238657084`
**Branch:** `work`

---

## Summary

The attempt to count unique groups only on first encounters in
`update_batch_sequential_dense` still inflates `unique_groups` for dense
workloads that repeatedly read the same values. As a result, the
regression suite shows significant slowdowns (up to +40%) in
mode-selection scenarios despite the original sparse-workload improvements.

### Root Cause

*File:* `datafusion/functions-aggregate/src/min_max/min_max_bytes.rs`

The guard meant to detect first encounters relies on the
`SequentialDenseLocation` scratch vector. However, groups that compare
against their existing materialised min/max and **do not replace it**
remain tagged as `ExistingMinMax`. Subsequent rows for the same group in
that batch still match the `ExistingMinMax` variant, so `unique_groups`
is incremented again. This recreates the original over-counting problem,
poisoning `total_groups_seen` and the density heuristics that drive mode
switching.

---

## Tasks to Fix the Root Cause

1. **Extend the scratch state to record “touched but unchanged” groups.**
   Add a new variant (for example `Visited`) to `SequentialDenseLocation`
   that marks groups once they are counted, even when the existing
   min/max remains best. Update the processing loop to flip the state to
   `Visited` whenever the group has been seen this batch.
2. **Adjust the update loop to ignore the new state.** Ensure the second
   pass that writes back results treats the `Visited` variant the same as
   `ExistingMinMax` (no update required) while still updating entries
   tagged as `Input`.
3. **Add regression tests for duplicate-heavy batches.** Introduce unit
   tests that repeatedly feed identical batches through
   `MinMaxBytesState::update_batch` and assert that `total_groups_seen`
   does not grow after the first batch.
4. **Re-run the criterion benchmarks.** Validate that dense workloads no
   longer regress and that the sparse-workload gains remain intact.

---

## Additional Verification

* Audit other code paths (simple, sparse, dense-inline) to confirm they
  already mark batches on first encounter and do not need similar
  adjustments.
* Consider adding lightweight instrumentation (behind a feature flag) to
  surface per-batch `unique_groups` counts during development, making
  future regressions easier to detect.
