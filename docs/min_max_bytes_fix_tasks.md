# Tasks to Resolve `MinMaxBytesState` Performance Regression

## Root Cause Analysis

The PR replaced the original simple `locations` vector (which was O(total_num_groups) allocation per batch) with a sophisticated dense/sparse scratch system to avoid quadratic behavior in high-cardinality scenarios. However, this introduced **multiple sources of overhead** that compound to cause the observed 30–73% regressions:

### 1. **Eliminated Fast Path: Original Code Was Already Optimal for Dense Workloads**

The **original implementation** allocated `vec![MinMaxLocation::ExistingMinMax; total_num_groups]` once per batch, then performed a single-pass loop with zero branching or HashMap operations:
```rust
// Original: simple, cache-friendly, minimal branching
let mut locations = vec![MinMaxLocation::ExistingMinMax; total_num_groups];
for (new_val, group_index) in iter.into_iter().zip(group_indices.iter()) {
    // Direct array access, predictable branches
}
```

**Key insight:** For dense workloads where groups are reused across batches, this O(total_num_groups) allocation is:
- **Amortized to negligible cost** because the same groups are visited repeatedly
- **Extremely cache-friendly** with sequential access patterns
- **Zero HashMap overhead** – just direct indexed array access
- **Trivial to allocate** – modern allocators handle zeroed pages efficiently

### 2. **New Code Introduces Compounding Overheads**

The new implementation pays **multiple penalties simultaneously**:

a) **Complex branching and state tracking** (lines 560–730):
   - Nested conditionals to decide dense vs sparse path
   - Epoch checking for every group access
   - Retry loops with `continue` statements when toggling between paths
   - Tracking `first_touch`, `processed_via_dense`, `pending_dense_growth`, `batch_unique_groups`, `batch_max_group_index`

b) **HashMap operations on every "out-of-range" access**:
   - Even when data is perfectly dense, if `group_index >= scratch_dense_limit`, it falls back to HashMap
   - For monotonic group IDs (benchmark: +30.94%), each new batch starts beyond the limit
   - For reused accumulators (benchmark: +73.19%), similar pattern occurs

c) **Mid-batch migration overhead** (lines 861–875):
   - When heuristic finally triggers, must iterate `scratch_group_ids` and copy from HashMap to dense vector
   - This is pure wasted work that the original code never needed

d) **Pointer invalidation concerns**:
   - Uses `*mut ScratchLocation` pointers to avoid HashMap borrow issues
   - Comments warn about realloc invalidation, adding safety complexity

e) **Lost vectorization and compiler optimization opportunities**:
   - Original code had predictable access patterns that compilers could optimize
   - New code has data-dependent branches that inhibit autovectorization and prefetching

### 3. **The Tradeoff Paradox**

The PR **successfully solves** the sparse/high-cardinality problem (only +5.61% regression for sparse groups) by avoiding O(total_num_groups) allocations when `total_num_groups >> active_groups`.

However, it **over-optimized for the wrong case**:
- Dense workloads (the common case in the benchmarks) now pay 30–73% overhead
- The original "quadratic" allocation was actually **not a problem** for dense workloads because:
  - Groups are reused across batches (amortization)
  - Modern OS memory management makes zeroed page allocation cheap
  - The cost was dominated by actual comparison work, not allocation

### 4. **Specific Regression Explanations**

| Benchmark | Regression | Why |
|-----------|-----------|-----|
| dense first batch | +64.78% | Cold start: heuristic delays dense activation; pays migration cost mid-batch |
| dense groups | +72.51% | Same groups repeatedly hit epoch checks + heuristic evaluation overhead |
| dense reused accumulator | +73.19% | **Worst case**: repeatedly processes same groups but overhead accumulates across batches |
| large dense groups | +52.30% | Single large batch: migration cost + dense overhead vs trivial allocation |
| monotonic group ids | +30.94% | Each batch starts beyond `scratch_dense_limit`, triggers HashMap then migration |
| sparse groups | +5.61% | **Only case where new code wins**: avoided large allocation |

## Remediation Strategy

The fix requires **fundamentally rethinking the optimization**, not just tweaking heuristics. The current approach cannot be salvaged without major changes.

### Implementation Update (Hybrid Mode)

DataFusion now ships with a **hybrid min/max accumulator** that routes each
workload through one of two clearly separated paths:

* **Simple mode** restores the original dense `Vec<MinMaxLocation>` loop for the
  common case where group identifiers are compact and frequently reused. The
  accumulator classifies workloads after the first non-null batch and continues
  using the dense path whenever the observed density is at least 10% and the
  total number of groups in the batch stays below `100_000`.
* **Sparse-optimised mode** retains the scratch-table implementation for
  high-cardinality workloads. Once a workload is classified as sparse the
  accumulator avoids dense allocations and keeps using the sparse machinery
  between batches.

The accumulator also tracks its lifetime statistics (batches processed, groups
populated, and the maximum group index). If a workload that started dense later
touches more than `100_000` groups with less than 1% density, the accumulator
switches to the sparse path **between batches**. This design keeps dense
workloads fast, while still protecting genuinely sparse workloads from the
quadratic allocation costs addressed by the original optimisation.

### Recommended Approach: Hybrid Strategy with Early Classification

1. **Restore the original fast path for common dense cases**
   - Detect truly dense workloads upfront (e.g., `max_group_index < THRESHOLD` and `unique_groups/max_group_index > DENSITY_RATIO`)
   - For dense workloads, use the **original simple `locations` vector approach**
   - Only invoke the complex sparse/dense machinery when necessary

2. **Implement adaptive threshold per accumulator lifetime**
   - Track `total_groups_seen` and `total_batches_processed` across the accumulator's lifetime
   - If `total_groups_seen` remains below a reasonable threshold (e.g., 10K groups), use the simple path
   - Only switch to sparse-optimized path when evidence shows true high-cardinality (e.g., > 100K groups)

3. **Accept the allocation for bounded-cardinality cases**
   - The original "problem" (O(total_num_groups) allocation) **is not actually a problem** when:
     - `total_num_groups` is reasonable (< 10K-100K)
     - Groups are reused across batches (common in real queries)
   - Modern allocators and OS virtual memory make zeroed allocations very cheap
   - The actual comparison work dominates over allocation cost

### Specific Tasks

#### Task 1: Add Density Classification at Accumulator Creation/First Batch
- **Goal:** Determine if the workload is dense enough to warrant the simple approach
- **Implementation:**
  - After first batch, calculate density ratio: `unique_groups / max_group_index`
  - If ratio > 0.1 (10% density) and `total_num_groups < 100_000`, mark as "simple mode"
  - Store this decision in a `WorkloadMode` enum (Simple vs SparseOptimized)

#### Task 2: Restore Original Simple Path for Dense Mode
- **Goal:** Eliminate all overhead for the common dense case
- **Implementation:**
  - Add a fast-path branch at the start of `update_batch` that checks `WorkloadMode`
  - If in Simple mode, execute the original logic:
    ```rust
    let mut locations = vec![MinMaxLocation::ExistingMinMax; total_num_groups];
    for (new_val, group_index) in iter.into_iter().zip(group_indices.iter()) { ... }
    ```
  - No epoch tracking, no HashMap, no heuristics, no retry loops

#### Task 3: Keep Sparse Path for High-Cardinality Cases
- **Goal:** Preserve the fix for genuinely sparse workloads
- **Implementation:**
  - When `WorkloadMode::SparseOptimized`, use the current complex logic
  - But simplify it: remove mid-batch toggling and migration
  - Either go fully dense or fully sparse per batch based on upfront heuristic
  - Avoid the expensive "detect and migrate mid-batch" pattern

#### Task 4: Add Mode-Switch Detection for Evolving Workloads
- **Goal:** Handle workloads that start dense but become sparse (rare but possible)
- **Implementation:**
  - Track cumulative `total_groups_seen` across batches
  - If exceeds threshold (e.g., 100K) and density drops below 0.01, switch to SparseOptimized mode
  - Make this switch **between batches only**, never mid-batch

#### Task 5: Update Benchmarks to Validate Both Modes
- **Goal:** Ensure no regressions in either dense or sparse cases
- **Implementation:**
  - Keep existing benchmarks covering dense scenarios (should match baseline after fix)
  - Add high-cardinality sparse benchmark with `total_num_groups = 1_000_000` and `unique_groups = 1000`
  - Verify sparse case doesn't regress to quadratic behavior
  - Verify dense cases return to original performance (0% regression target)

#### Task 6: Document the Tradeoff and Design Decision
- **Goal:** Future maintainers understand why two paths exist
- **Implementation:**
  - Add module-level documentation explaining:
    - Dense workloads (common): use simple O(total_num_groups) allocation per batch
    - Sparse workloads (rare): use complex sparse tracking to avoid quadratic behavior
  - Document the thresholds and why they were chosen
  - Include performance characteristics of each mode

### Success Criteria

- **Dense benchmarks:** 0–5% regression vs original (main branch) performance
- **Sparse workload:** No O(total_num_groups²) quadratic behavior when `total_num_groups >> unique_groups`
- **Code clarity:** Two clear paths instead of one complex adaptive path
- **No mid-batch mode switching:** Simplifies reasoning about pointer validity and state consistency

---

## Key Insights Summary

1. **The original code was NOT broken for dense workloads** – the O(total_num_groups) allocation per batch is trivial when:
   - Groups are reused across batches (amortizes the cost)
   - `total_num_groups` is bounded (< 100K)
   - Modern memory management makes zeroed allocations cheap

2. **The optimization introduced more overhead than it saved** for the common case:
   - Dense workloads: +30% to +73% regression
   - Sparse workloads: +5.6% regression (acceptable)
   - The complex machinery (epochs, HashMaps, migration, heuristics) costs more than simple allocation

3. **Fix requires architectural change, not parameter tuning:**
   - Cannot salvage current approach by tweaking heuristics or growth constants
   - Need **two separate code paths**: simple for dense, complex for sparse
   - Classify workload early and commit to one path per accumulator

4. **Performance optimization principle violated:**
   - "Premature optimization is the root of all evil"
   - The PR optimized for a rare case (truly sparse with millions of groups) at the expense of the common case
   - Better strategy: optimize common case first, handle edge cases separately

5. **The real quadratic problem is narrow:**
   - Only occurs when `total_num_groups` is very large (> 1M) AND `unique_groups << total_num_groups` AND groups are spread across the range
   - This is rare in practice (most aggregations have bounded cardinality)
   - A simple mode-switch based on observed cardinality handles this cleanly
