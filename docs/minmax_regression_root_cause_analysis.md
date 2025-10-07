# Root Cause Analysis: MinMaxBytesState Performance Regressions

## Executive Summary

The hybrid implementation (Simple vs SparseOptimized modes) introduced with commits c1ac251d6^..8ce5d1f8d successfully avoided the quadratic behavior for sparse workloads, but introduced regressions for 4 out of 6 dense benchmark scenarios. The root cause is **NOT that the wrong mode is being selected**, but rather that **BOTH modes have inherent overhead compared to the original implementation**.

## Benchmark Results Analysis

| Benchmark | Change | Mode Expected | Issue |
|-----------|--------|---------------|-------|
| min bytes monotonic group ids | -18.56% (✓ improvement) | Undecided→Simple | Works as intended |
| min bytes sparse groups | -17.53% (✓ improvement) | Undecided→Sparse | Works as intended |
| min bytes dense first batch | +12.62% (✗ regression) | Undecided→Simple | Overhead in Simple mode |
| min bytes dense groups | +72.51% (✗ regression) | Simple | **Severe overhead** in Simple mode |
| min bytes dense reused accumulator | +30.13% (✗ regression) | Simple | Repeated Simple mode overhead |
| min bytes large dense groups | +13.05% (✗ regression) | Undecided→Simple | Overhead in Simple mode |

## Root Cause #1: Simple Mode Still Has the Allocation Problem

### The Original Issue (Pre-PR)
The original code allocated:
```rust
let mut locations = vec![MinMaxLocation::ExistingMinMax; total_num_groups];
```
This was O(total_num_groups) per batch, causing quadratic behavior when `total_num_groups` grew.

### What the Simple Mode Does Now (Lines 587-678)
```rust
let mut locations = vec![SimpleLocation::Untouched; total_num_groups];  // Line 602
```

**This is the EXACT SAME PROBLEM!** 

The Simple mode still allocates a vector of size `total_num_groups` on **every batch**, then:
1. Initializes it to `Untouched`
2. Updates elements as groups are encountered
3. Iterates `touched_groups` at the end to materialize winners

### Why This Causes Regressions

For the regressed benchmarks:

1. **"min bytes dense first batch"** (+12.62%):
   - `BATCH_SIZE = 512` groups, `total_num_groups = 512`
   - Allocates `vec![SimpleLocation::Untouched; 512]`
   - Then builds `touched_groups` Vec, `batch_inputs` Vec
   - Overhead: extra allocations + enum discrimination + `touched_groups` iteration

2. **"min bytes dense groups"** (+72.51%):
   - This likely tests repeated updates to the same groups
   - Each batch re-allocates the full `locations` vector
   - Overhead compounds across iterations
   - The `touched_groups` Vec and final iteration add latency

3. **"min bytes dense reused accumulator"** (+30.13%):
   - Processes 32 batches (`MONOTONIC_BATCHES = 32`)
   - Each batch: allocate 512-element vector, build touched_groups, iterate at end
   - 32× the allocation overhead
   - Memory churn from repeated allocations

4. **"min bytes large dense groups"** (+13.05%):
   - `LARGE_DENSE_GROUPS = 16,384` (MONOTONIC_TOTAL_GROUPS)
   - Allocates `vec![SimpleLocation::Untouched; 16_384]` (128 KB)
   - Then processes all 16K groups
   - Overhead from large allocation + touched_groups iteration

### Why Two Benchmarks **Improved**

1. **"min bytes monotonic group ids"** (-18.56%):
   - Processes 32 batches with monotonically increasing group IDs
   - Each batch adds 512 new groups: 0-511, 512-1023, 1024-1535, etc.
   - Total: 16,384 groups across 32 batches
   - **Key**: Each batch starts `Undecided`, processes 512 unique groups with max_index=511+batch_offset
   - Heuristic: `unique_groups * 10 >= domain` → `512 * 10 >= 512` → **TRUE** (enters Simple mode)
   - **Why it improved**: The original code would allocate `vec![...; 16_384]` on every batch after the first
   - The new code allocates `vec![...; 512]` for the first batch (Undecided→Simple for that batch only)
   - Then switches modes between batches based on accumulated stats
   - Net result: smaller per-batch allocations

2. **"min bytes sparse groups"** (-17.53%):
   - `BATCH_SIZE = 512` values, but only `SPARSE_GROUPS = 16` unique groups
   - `total_num_groups = LARGE_TOTAL_GROUPS = 10_000`
   - Original code: `vec![...; 10_000]` allocation per batch
   - New code: detects sparse pattern, uses HashMap-based sparse mode
   - **Why it improved**: Avoided the massive 10K allocation for only 16 active groups

## Root Cause #2: Extra Complexity in Simple Mode

Even for cases where `total_num_groups` is small (e.g., 512), the Simple mode has overhead:

1. **Three separate Vecs maintained**:
   - `locations: Vec<SimpleLocation>` (size = total_num_groups)
   - `touched_groups: Vec<usize>` (grows with unique groups)
   - `batch_inputs: Vec<&[u8]>` (grows with candidate values)

2. **Extra iteration at the end** (lines 670-674):
   ```rust
   for group_index in touched_groups.into_iter() {
       if let SimpleLocation::Batch(batch_index) = locations[group_index] {
           self.set_value(group_index, batch_inputs[batch_index]);
       }
   }
   ```
   This is an extra pass over all touched groups that the original code didn't need.

3. **Enum discrimination overhead**:
   - Every group access checks `match location` with three variants: `Untouched`, `Existing`, `Batch(usize)`
   - The original code had simpler logic

4. **Cache locality issues**:
   - Three separate vectors means more cache misses
   - Original code had tighter data layout

## Root Cause #3: The "Original" Code Was Never Actually Problematic for Dense Cases

The key insight from the task document: **The O(total_num_groups) allocation per batch is NOT a problem when**:
1. `total_num_groups` is bounded (< 10K-100K)
2. Groups are reused across batches (amortizes the cost)
3. Modern allocators handle zeroed pages efficiently

For dense benchmarks like "dense groups" and "dense first batch":
- `total_num_groups = 512` or `16_384` (not millions)
- These allocations are **trivial** on modern systems
- The comparison work dominates, not allocation

The PR "optimized" something that wasn't actually slow, and introduced measurable overhead in the process.

## Root Cause #4: Undecided Mode Uses Simple Implementation

Looking at lines 573-583:
```rust
match self.workload_mode {
    WorkloadMode::SparseOptimized => { self.update_batch_sparse_impl(...) }
    WorkloadMode::Simple => { self.update_batch_simple_impl(...) }
    WorkloadMode::Undecided => { self.update_batch_simple_impl(...) }  // ← Same as Simple!
}
```

**Every first batch runs the Simple implementation**, which already has the overhead described above. This means:
- "dense first batch" benchmark (single batch) ALWAYS pays Simple mode overhead
- There's no way to have a truly zero-overhead first batch

## Why This Matters for Each Regression

### "min bytes dense groups" (+72.51% ← WORST)
Likely scenario: This benchmark processes the SAME groups repeatedly across multiple batches.
- Each batch: allocate `vec![SimpleLocation::Untouched; N]`
- Each batch: build `touched_groups`, `batch_inputs`
- Each batch: final iteration over `touched_groups`
- Overhead compounds when you're doing the same work repeatedly

### "min bytes dense reused accumulator" (+30.13%)
Explicitly tests reusing an accumulator across 32 batches:
- 32× the Simple mode allocation overhead
- 32× the `touched_groups` iteration overhead
- Memory allocator churn from repeated allocations

### "min bytes dense first batch" (+12.62%)
Single batch, but pays for:
- Undecided mode overhead (uses Simple implementation)
- Extra vectors and final iteration
- For a one-time operation, the overhead is more noticeable as percentage of total time

### "min bytes large dense groups" (+13.05%)
Large allocation (16K groups = 128 KB for `locations`):
- Allocation cost is non-trivial at this size
- Extra iteration over 16K touched groups adds latency
- Original code might have had better memory layout for large contiguous access

## The Fix Strategy

The task document correctly identifies the solution: **The Simple mode needs to be even simpler**.

The current "Simple" mode is not actually the original simple implementation. It's a "less complex than Sparse" implementation, but still has:
- The per-batch allocation problem
- Extra vectors
- Extra iterations
- Enum discrimination

### What "True Simple" Should Look Like

For truly dense workloads (most groups touched in most batches), the optimal code is:
1. Allocate `vec![Location; total_num_groups]` once per batch (accept this cost for bounded cardinality)
2. Single pass: for each (value, group_index), update `locations[group_index]` directly
3. Single final pass: for i in 0..total_num_groups, if locations[i] indicates update, materialize it
4. No separate `touched_groups` Vec
5. No separate `batch_inputs` Vec (or at least minimize its size)

The overhead of the O(total_num_groups) allocation is **acceptable** when:
- N < 100K (as correctly identified by `SIMPLE_MODE_MAX_TOTAL_GROUPS = 100_000`)
- The work per batch dominates the allocation (which it does for string comparisons)

### Why Current Simple Mode Can't Be "Fixed" With Tweaks

The problem is architectural:
- The `touched_groups` Vec is necessary in current design to avoid iterating all `total_num_groups` at the end
- But building `touched_groups` itself has overhead
- The three-Vec design has cache implications
- The enum discrimination adds branches

To truly restore performance, need to reconsider the data structure itself.

## Concrete Issues to Address

1. **Simple mode allocates O(total_num_groups) per batch** – same as original problem, just different enum
2. **Simple mode has extra iteration overhead** – the `touched_groups` loop at the end
3. **Simple mode maintains three separate Vecs** – cache locality issues
4. **Undecided mode defaults to Simple** – no way to have overhead-free first batch
5. **No "truly zero-overhead" path exists** – even best case has more overhead than original

## Success Metrics

To fix these regressions:
- "min bytes dense groups": reduce 72.51% regression to < 5%
- "min bytes dense reused accumulator": reduce 30.13% regression to < 5%
- "min bytes dense first batch": reduce 12.62% regression to < 5%
- "min bytes large dense groups": reduce 13.05% regression to < 5%
- Maintain improvements for "sparse groups" and "monotonic group ids"

This requires fundamentally rethinking the Simple mode to have lower overhead than it currently does.
