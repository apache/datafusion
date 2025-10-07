# Min/Max Bytes Dense Regression Investigation

## Summary of Findings

The regression was introduced by commit `8d046ca32b1884c94f89ee6728a601c0a8848317`, which rewrote
`MinMaxBytesState::update_batch` to reuse scratch buffers instead of allocating a
`locations` vector sized to `total_num_groups` on every batch. While this removed the
quadratic allocation pattern for sparse workloads, it made the dense path substantially
slower.

The new implementation only allows the dense scratch table to handle group ids that are
strictly below `scratch_dense_limit`. That limit starts at zero and is extended lazily via
`scratch_sparse` each time the code encounters a group id that is **not** yet within the
limit. Concretely:

* The first row of the batch enables dense mode and sets `scratch_dense_limit` to `1`.
* Every subsequent new group id is first routed through the sparse path because
  `group_index < scratch_dense_limit` is false. The sparse branch calls
  `expand_dense_limit`, bumps the limit, and restarts the loop so the same row can be
  re-processed through the dense table.
* As a result, dense batches process each new group twice and pay for hash map lookups
  plus additional branching before they can reach the dense entry.

This fallback happens for **every** previously unseen dense group until the end of the
batch when `scratch_dense_limit` is finally updated to the maximum group index that was
observed. The constant-factor overhead explains the 50–75% regressions measured by the
Criterion benchmarks for dense workloads.【F:datafusion/functions-aggregate/src/min_max/min_max_bytes.rs†L524-L724】

## Tasks to Address the Regression

1. **Avoid detouring through the sparse path once dense mode is active.**
   * When `use_dense` is true and `group_index >= scratch_dense_limit`, grow
     `scratch_dense_limit` (and `scratch_dense`) directly instead of entering the sparse
     branch. This keeps dense batches on the hot path and prevents double-processing.
2. **Pre-size the dense limit for purely dense batches.**
   * Track the running maximum group id within the current batch and use it to expand the
     dense limit proactively, so the vector growth happens without the additional
     `continue` loops.
3. **Add a regression benchmark or test.**
   * Extend `min_bytes_dense_first_batch` (or add a unit test) to assert that dense
     batches only hit the dense path once per group, protecting against future slowdowns.

Implementing the above will retain the sparse-path improvements while restoring the dense
throughput that regressed in this commit.
