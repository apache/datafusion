# Tasks to Resolve `MinMaxBytesState` Performance Regression

## Root Cause Summary

The refactor in commit b670e1ce1666dbb43e8779d5e9fde1607a2f600e replaced the per-batch `HashMap` that tracked touched groups with two vectors keyed directly by `group_index` (`scratch_locations` and `scratch_epochs`). Each call to `update_batch` now extends these vectors up to `group_index + 1` for every previously unseen group, and later uses `group_index` as the direct index during the batch update loop.【F:datafusion/functions-aggregate/src/min_max/min_max_bytes.rs†L432-L506】

For workloads with densely populated group ids, the loop repeatedly performs `Vec::resize` to grow these per-group arrays, zero-filling the new slots. This introduces additional branching and memory writes per row, particularly pronounced when batches introduce many sequential group ids. The dense benchmark shows a +28% regression because the refactor effectively reintroduces `O(total_num_groups)` work when the stream contains many new group identifiers in ascending order.【F:datafusion/functions-aggregate/src/min_max/min_max_bytes.rs†L458-L485】

## Remediation Tasks

1. **Restore per-batch sparse bookkeeping for scratch state.**
   * Replace the direct `group_index` lookup (`scratch_locations` / `scratch_epochs`) with a structure that only allocates entries for groups touched in the current batch (e.g., retain a generational index table that maps group ids to the position in `scratch_group_ids`).
   * Ensure that the data structure avoids per-batch zero-filling of a vector sized to `total_num_groups` while still providing O(1) lookup for repeated rows within a batch.【F:datafusion/functions-aggregate/src/min_max/min_max_bytes.rs†L458-L493】

2. **Add regression benchmarks.**
   * Extend `min_max_bytes` benchmarks to cover monotonic group id growth and large dense group scenarios to detect future allocations that scale with `total_num_groups`.
   * Capture baseline results to guard against reintroducing quadratic behavior.【F:datafusion/functions-aggregate/benches/min_max_bytes.rs†L1-L80】

3. **Validate with profiling and tests.**
   * Re-run the affected Criterion benchmarks and inspect allocator / CPU profiles to confirm the fix removes the dense regression while preserving the sparse improvement.
   * Ensure existing unit tests around `MinMaxBytesState` still pass and add coverage that inspects `scratch_group_ids` / scratch metadata after multiple batches.【F:datafusion/functions-aggregate/src/min_max/min_max_bytes.rs†L506-L620】

