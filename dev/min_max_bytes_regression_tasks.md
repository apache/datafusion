# Min/max bytes regression tasks

## Root cause synopsis
The change that introduced the `Visited` variant for `SequentialDenseLocation` now writes
`Visited` back into the `locations` buffer for every group whose existing min/max already
wins during a sequential dense batch. Because the sequential dense fast-path is used when
`group_indices == [0, 1, ..., N-1]`, this means **every** row performs an additional
write to the scratch vector in the common "no update" case. That extra per-row store (and
branch) turns the formerly read-mostly scan into one that pounds on memory, which shows up
as the large regressions in the criterion suite.

See `update_batch_sequential_dense` for the new `Visited` writes: `SequentialDenseLocation::Visited`
is assigned at `lines ~1014-1033` and skipped in the write-back loop at `lines ~1038-1044`.

## Tasks
1. Rework the sequential dense unique-group tracking so that we avoid writing back to the
   `locations` vector when the existing min/max wins. Options include reverting to the
   previous two-state enum and tracking first encounters via a lightweight bitmap/epoch or
   only marking groups when a duplicate is actually observed. The fix must keep the
   duplicate counting invariant while restoring the read-mostly behavior for dense scans.
2. Add a microbenchmark (or extend an existing one) that exercises the sequential dense
   path with large `total_num_groups` and mostly stable minima/maxima to catch future
   regressions in this hot path.
3. Audit the new duplicate-heavy tests to ensure they reflect reachable scenarios. If the
   sequential dense path can never legally receive duplicate group IDs, replace the tests
   with coverage on the public `update_batch` API; otherwise, document the expectations so
   the optimized fix can be validated against real workloads.
