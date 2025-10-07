# Tasks to Fix MinMaxBytesState Performance Regressions

## Overview

The root cause analysis (see `minmax_regression_root_cause_analysis.md`) identified that **BOTH the Simple and SparseOptimized modes have inherent overhead** compared to what a truly optimal implementation would be. The Simple mode still allocates `O(total_num_groups)` per batch and maintains three separate vectors with an extra iteration pass, causing 12-72% regressions across dense benchmarks.

## Key Insight

The current "Simple" mode is **not the original simple implementation**. It's a compromise that:
- Still has the O(total_num_groups) allocation problem
- Adds overhead with `touched_groups` Vec and final iteration
- Uses three separate Vecs (cache locality issues)
- Has enum discrimination overhead

**The fix requires reverting Simple mode to a truly minimal implementation** while keeping the SparseOptimized mode for genuinely sparse workloads.

---

## Task 1: Restore True Original Fast Path for Small-N Dense Workloads

**Goal**: Eliminate all overhead for the common case where `total_num_groups` is small and most groups are touched.

**Current Problem**: 
- Simple mode allocates `vec![SimpleLocation::Untouched; total_num_groups]` per batch
- Maintains separate `touched_groups` and `batch_inputs` Vecs
- Has extra final iteration over `touched_groups`

**Required Changes**:
1. Create a **third implementation variant** (not just Simple/Sparse): call it `TrueSimple` or `DenseInline` mode
2. This mode should use the **absolute minimal** data structure:
   - Single inline Vec sized to batch, not total_num_groups
   - OR accept the O(total_num_groups) allocation but eliminate the touched_groups tracking entirely
   - Direct writes during the main loop, no deferred materialization
3. Heuristic: Use `TrueSimple` when:
   - `total_num_groups <= 10_000` AND
   - First batch shows `unique_groups / total_num_groups > 0.5` (very dense)
4. For `TrueSimple` mode: eliminate the `touched_groups` Vec entirely
   - Trade off: iterate all `total_num_groups` at the end, but for N < 10K this is trivial
   - Benefit: no allocation overhead for tracking, better cache locality

**Success Metric**: "dense first batch" regression reduced from +12.62% to < 5%

---

## Task 2: Optimize Simple Mode to Reduce Per-Batch Allocation Overhead

**Goal**: For workloads that can't use `TrueSimple` (N > 10K) but are still somewhat dense, reduce the Simple mode overhead.

**Current Problem**:
- Simple mode allocates full `vec![SimpleLocation::Untouched; total_num_groups]` every batch
- For `total_num_groups = 16_384`, this is 128 KB of initialization overhead per batch

**Required Changes**:
1. **Pre-allocate and reuse the `locations` Vec across batches** for Simple mode:
   - Add `simple_locations: Vec<SimpleLocation>` as a field in `MinMaxBytesState`
   - Initialize it lazily on first Simple mode batch
   - Between batches: don't drop it, keep it allocated
   - At start of each batch: only reset the `touched_groups` entries, not all N entries
2. **Lazy initialization strategy**:
   - Maintain an epoch counter for `simple_locations` (similar to `scratch_epoch`)
   - Each entry tagged with last-written epoch
   - On each batch: increment epoch, entries from old epoch are implicitly "Untouched"
   - Only iterate over `touched_groups` to actually reset, not all N entries
3. **Reduce enum size**:
   - `SimpleLocation::Batch(usize)` requires sizeof(usize) + tag
   - Consider packed representation or smaller index type if batch is bounded

**Success Metric**: "dense reused accumulator" regression reduced from +30.13% to < 10%

---

## Task 3: Eliminate Final Iteration Overhead in Simple Mode

**Goal**: Remove the extra `touched_groups.into_iter()` loop at the end of Simple mode.

**Current Problem** (lines 670-674):
```rust
for group_index in touched_groups.into_iter() {
    if let SimpleLocation::Batch(batch_index) = locations[group_index] {
        self.set_value(group_index, batch_inputs[batch_index]);
    }
}
```
This is an **extra pass** over all touched groups.

**Required Changes**:
1. **Materialize values inline during the main loop** instead of deferring:
   - When a new winner is determined, immediately call `self.set_value()`
   - Remove the `batch_inputs` Vec and `SimpleLocation::Batch(usize)` indirection
   - Trade-off: might call `set_value()` multiple times for the same group if value keeps improving
   - Benefit: eliminates final iteration pass, reduces allocations
2. **Measure the trade-off**:
   - Benchmark: frequent updates to same group (pathological case)
   - vs. Current: one final write per group (optimal)
   - Hypothesis: for string comparisons, the comparison cost >> set_value cost, so multiple writes OK
3. **Alternative approach** (if inline materialization regresses):
   - Keep deferred materialization, but fuse the final loop into the evaluation step
   - When `evaluate()` is called, materialize then
   - For multi-batch scenarios, this might defer work until the end
   - But need careful lifetime management

**Success Metric**: Reduce per-batch overhead by ~5-10% for all Simple mode cases

---

## Task 4: Add Reallocation-Free Path for Repeated Same-Group Updates

**Goal**: Handle the "dense groups" benchmark case (+72.51% regression) which likely processes the same N groups repeatedly across batches.

**Current Problem**:
- Each batch reallocates `locations`, `touched_groups`, `batch_inputs`
- For workloads that hit the same groups every batch, this is pure waste

**Required Changes**:
1. **Detect stable group pattern**:
   - After 2-3 batches, if we see the same set of groups each time, mark as "stable dense"
   - Track: `last_batch_group_set` as a HashSet or bitset
   - Compare current batch's groups to previous batch
   - If match ratio > 95%, enter "stable dense" mode
2. **Stable dense mode optimizations**:
   - Pre-allocate all Vecs to capacity observed in first batch
   - Reuse allocations across batches without resizing
   - Consider: keep a running "current winner" in `min_max` and compare directly, skip intermediate Vecs
3. **Fast path for perfect reuse**:
   - If every batch processes groups [0, 1, 2, ..., N-1] (dense sequential), use the absolute simplest loop:
     - Direct array access, no indirection
     - No HashMaps, no epochs, no touched tracking
     - Just: `for i in 0..N { if cmp(new[i], old[i]) { old[i] = new[i]; } }`

**Success Metric**: "dense groups" regression reduced from +72.51% to < 5%

---

## Task 5: Optimize Large-N Dense Case (N = 16K)

**Goal**: Handle "large dense groups" benchmark (+13.05% regression) which processes 16,384 groups in one batch.

**Current Problem**:
- Allocating `vec![SimpleLocation::Untouched; 16_384]` = 128 KB
- Then building `touched_groups` with 16K entries
- Then iterating `touched_groups` to materialize

**Required Changes**:
1. **Threshold detection**: When `total_num_groups > LARGE_BATCH_THRESHOLD` (e.g., 8192), use different strategy:
   - Don't allocate O(total_num_groups) `locations` Vec at all
   - Instead: sort `group_indices` (or use counting pass) to find unique groups
   - Build compact working set of only touched groups
   - Process only that working set
2. **Alternative: chunk processing**:
   - If `total_num_groups` is large but groups are dense, process in chunks:
   - Divide into segments of 4096 groups each
   - Process each segment with a smaller `locations` Vec
   - Reduces peak allocation size, improves cache behavior
3. **Memory layout optimization**:
   - For large N, consider using a block-allocated data structure instead of Vec of Options
   - Use an arena allocator for string data
   - Reduce per-group overhead from sizeof(Option<Vec<u8>>) to just offset + length

**Success Metric**: "large dense groups" regression reduced from +13.05% to < 5%

---

## Task 6: Clarify and Optimize Undecided Mode First-Batch Behavior

**Goal**: Reduce "dense first batch" overhead (+12.62%) by making the Undecided mode lighter.

**Current Problem**:
- Undecided mode uses `update_batch_simple_impl` by default (line 579)
- This means first batch ALWAYS pays Simple mode overhead
- No way to have a zero-overhead cold start

**Required Changes**:
1. **Separate Undecided implementation**:
   - Create `update_batch_undecided_impl` that is lighter than Simple
   - Use minimal tracking: just collect stats (unique_groups, max_group_index)
   - Defer complex processing until mode is decided
2. **Inline fast path for single-batch workloads**:
   - If this is the ONLY batch the accumulator will ever see (common in unit tests, small queries):
   - Use absolute minimal logic: direct HashMap or direct Vec, no epochs, no mode switching
   - Accept that we might make suboptimal choice, but for one batch it doesn't matter
3. **Heuristic tuning**:
   - Current: `should_use_simple` requires `total_num_groups <= 100_000` AND `unique_groups * 10 >= domain`
   - The 10% density threshold might be too high for first batch
   - Consider: if `total_num_groups < 1000`, always use TrueSimple regardless of density
   - For N < 1000, the O(N) cost is negligible even if only 1 group touched

**Success Metric**: "dense first batch" regression reduced from +12.62% to < 5%

---

## Task 7: Reduce Sparse Mode Overhead for Mixed Workloads

**Goal**: Ensure that workloads which oscillate between dense and sparse don't pay double overhead.

**Current Problem**:
- Mode switching happens between batches based on lifetime stats
- If workload characteristics change, might thrash between modes
- Each mode has initialization cost (entering Simple clears sparse structures, entering Sparse clears dense structures)

**Required Changes**:
1. **Hysteresis in mode switching**:
   - Don't switch modes after every batch
   - Require N consecutive batches showing different pattern before switching
   - Track: `batches_since_mode_switch` counter
   - Only reevaluate switch after 5-10 batches in current mode
2. **Lazy structure initialization**:
   - When entering Sparse mode from Simple, don't immediately clear dense structures
   - Keep them around in case we switch back
   - Only clear when memory pressure detected or after many batches
3. **Mode affinity tracking**:
   - Track: `simple_batch_count` and `sparse_batch_count` over lifetime
   - If accumulator has processed 100 batches, 95 of which were simple:
     - Bias towards Simple mode, raise threshold for switching to Sparse
   - Prevents pathological thrashing on edge cases

**Success Metric**: No regression for workloads that mix dense/sparse batches; maintain improvements for pure sparse

---

## Task 8: Profile and Validate Memory Allocator Behavior

**Goal**: Verify assumptions about allocation cost and identify if allocator is the bottleneck.

**Current Problem**:
- Task assumes O(total_num_groups) allocation per batch is acceptable for small N
- But benchmarks show 12-72% regression, suggesting allocation cost is significant
- Need empirical data on actual allocation overhead

**Required Actions**:
1. **Microbenchmark pure allocation**:
   - Test: `vec![SimpleLocation::Untouched; N]` for N = 512, 1K, 8K, 16K, 64K
   - Measure: time per allocation, variance
   - Compare: reusing pre-allocated Vec with reset vs fresh allocation
2. **Profile with perf/Instruments**:
   - Run regressed benchmarks under profiler
   - Identify: % time in allocation, % time in comparison logic, % time in iteration
   - Determine: is allocation the bottleneck or is it the extra iteration/indirection?
3. **Allocator experiments**:
   - Try: jemalloc vs system allocator
   - Try: pre-allocating a memory pool for all locations Vecs
   - Try: using a bump allocator for per-batch scratch space
4. **Document findings**:
   - Update task list based on profiling data
   - If allocation is NOT the bottleneck, pivot to optimize iteration/indirection instead
   - If allocation IS the bottleneck, focus on reuse strategies (Task 2, Task 4)

**Success Metric**: Identify true bottleneck(s) with empirical data; adjust tasks accordingly

---

## Task 9: Comprehensive Benchmarking Suite Expansion

**Goal**: Add missing benchmark scenarios to prevent future regressions.

**Current Problem**:
- Benchmark suite doesn't cover all patterns (e.g., no explicit "dense groups" benchmark in code, but appears in regression report)
- Need more granular benchmarks to isolate specific optimizations

**Required Benchmarks**:
1. **Stable group reuse**: same N groups across M batches
2. **Growing group set**: add 100 new groups per batch across 100 batches
3. **Mixed density**: alternate between dense (N=500) and sparse (N=100K) batches
4. **Pathological sparse**: 10 groups out of 1M `total_num_groups`
5. **Single batch, varying N**: N = 10, 100, 1K, 10K, 100K for single-batch workload
6. **Memory-bound**: very large strings (1KB each) across 10K groups to test allocation overhead
7. **Cache-bound**: tiny strings (8 bytes) across 1M groups to test iteration overhead

**Success Metric**: All new benchmarks within 10% of theoretical optimal performance

---

## Task 10: Documentation and Simplification

**Goal**: Make the codebase maintainable and understandable for future contributors.

**Current Problem**:
- Three modes (Undecided, Simple, SparseOptimized) with complex switching logic
- Epochs, retry loops, pointer tricks, multiple scratch structures
- Difficult to reason about correctness and performance

**Required Documentation**:
1. **Module-level design doc**:
   - Explain when each mode is used and why
   - Provide Big-O analysis for each mode
   - Include decision tree flowchart for mode selection
2. **Inline comments for heuristics**:
   - Every threshold constant (e.g., `SIMPLE_MODE_MAX_TOTAL_GROUPS = 100_000`) needs justification
   - Document: "Chosen because benchmarks show X% regression above this value"
3. **Simplification opportunities**:
   - Consider: can we remove Undecided mode entirely? Just start in Simple and switch if needed?
   - Consider: can we remove retry loops and pointer tricks in Sparse mode? Use cleaner design even if slightly slower?
   - Principle: "Multiple simple paths beat one complex adaptive path" (from AGENTS.md)
4. **Examples in doc comments**:
   - Show example query patterns for each mode
   - "Simple mode: SELECT MAX(name) FROM users GROUP BY city -- 100 cities, 1M users"
   - "Sparse mode: SELECT MAX(id) FROM events GROUP BY user_id -- 10M users, 1K events"

**Success Metric**: External contributor can understand and modify the code without needing to reverse-engineer the design

---

## Priority Order

Based on regression severity and implementation complexity:

1. **High Priority** (severe regressions, clear fixes):
   - Task 4: Fix "dense groups" +72.51% regression (stable group pattern detection)
   - Task 2: Fix "dense reused accumulator" +30.13% regression (reuse allocations)

2. **Medium Priority** (moderate regressions, require careful tuning):
   - Task 1: Add TrueSimple mode for tiny-N workloads
   - Task 6: Optimize Undecided mode first-batch
   - Task 5: Optimize large-N dense case

3. **Low Priority** (optimizations and polish):
   - Task 3: Eliminate final iteration (needs measurement of trade-offs)
   - Task 7: Reduce mode-switching thrashing
   - Task 8: Profiling to validate assumptions
   - Task 9: Expand benchmark coverage
   - Task 10: Documentation

## Expected Outcomes

After completing high-priority tasks (1, 2, 4, 6):
- "dense groups": +72.51% → < 5% regression
- "dense reused accumulator": +30.13% → < 10% regression
- "dense first batch": +12.62% → < 5% regression
- "large dense groups": +13.05% → < 5% regression
- Maintain improvements: "sparse groups" -17.53%, "monotonic group ids" -18.56%

After completing all tasks:
- All benchmarks within 5% of baseline OR show improvement
- Code maintainability improved with clearer mode separation
- Comprehensive benchmark coverage prevents future regressions
