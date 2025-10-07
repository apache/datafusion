# Tasks to Fix Remaining MinMaxBytesState Regressions (DenseInline Implementation)

## Context

The DenseInline mode (commit 027966107) successfully improved 4 of 6 benchmarks:
- ✓ monotonic group ids: -18.56% → -40.13% (better)
- ✓ sparse groups: -17.53% → -30.57% (better)  
- ✓ dense first batch: +12.62% → +1.90% (nearly fixed)
- ✓ dense reused: +30.13% → +5.47% (much better)

But 2 regressions remain:
- ✗ **dense groups: +72.51% (unchanged - CRITICAL)**
- ✗ large dense groups: +13.05% → +16.32% (worse)

**Root Cause**: The `DENSE_INLINE_MAX_TOTAL_GROUPS = 10,000` threshold is too conservative, forcing workloads with N > 10K into the slower Simple mode even when DenseInline would be optimal.

---

## Task 1: Raise DENSE_INLINE_MAX_TOTAL_GROUPS Threshold (High Priority)

### Problem
- Current threshold: 10,000 groups
- "dense groups" benchmark likely tests N in range 10,001-99,999
- "large dense groups" uses N = 16,384
- Both exceed 10K, fall back to Simple mode with significant overhead

### Why Current Threshold Is Wrong
1. **Memory cost is negligible**: `Vec<u64>` for 100K groups = only 800 KB
2. **Modern hardware can handle it**: Typical L3 cache is 8-32 MB
3. **Simple mode is MORE expensive**: Allocates 3 Vecs totaling 3-5× the memory of DenseInline
4. **Creates performance cliff**: N=9,999 is fast, N=10,001 is slow

### Task
**Increase `DENSE_INLINE_MAX_TOTAL_GROUPS` from 10,000 to 100,000**

**Rationale**:
- Memory cost: 100K × 8 bytes = 800 KB (acceptable for fast path)
- Covers common medium-cardinality use cases (user IDs, SKUs, geo entities)
- Eliminates artificial performance cliff at 10K
- Simple mode is only needed for truly large cardinalities (> 100K)

**Success Criteria**:
- "dense groups" regression: +72.51% → < 5%
- "large dense groups" regression: +16.32% → < 5%
- No regression for benchmarks with N < 10K
- Maintain improvements for sparse workloads

**Implementation Note**:
- Change line 517: `const DENSE_INLINE_MAX_TOTAL_GROUPS: usize = 100_000;`
- No other code changes required
- Test with N = 10K, 50K, 100K, 150K to find optimal threshold

---

## Task 2: Empirically Determine Optimal Threshold (Medium Priority)

### Problem
Current threshold (10K) is arbitrary. Need data-driven decision.

### Task
Create microbenchmarks to measure DenseInline vs Simple mode crossover point:

1. **Benchmark setup**:
   - Vary N from 1K to 1M (1K, 5K, 10K, 25K, 50K, 100K, 250K, 500K, 1M)
   - For each N: process 10 batches of dense data (80-100% density)
   - Measure: DenseInline time vs Simple mode time

2. **Expected findings**:
   - DenseInline scales linearly with N
   - Simple mode has higher constant overhead + linear scaling
   - Crossover likely at N = 250K-500K (not 10K)

3. **Set threshold to 90th percentile of crossover**:
   - If crossover at 500K, set threshold to 450K
   - Provides margin for variance and different hardware

4. **Document the decision**:
   - Add comment explaining why threshold was chosen
   - Include benchmark data in code comments or docs
   - Make threshold configurable for future tuning

**Success Criteria**:
- Threshold backed by empirical data
- < 5% performance difference at threshold boundary
- Documented rationale for future maintainers

---

## Task 3: Optimize Simple Mode for Large-N Dense Workloads (Medium Priority)

### Problem
For N > threshold (whatever it becomes), Simple mode still has significant overhead:
- Three separate Vec allocations (slots, touched_groups, batch_inputs)
- Deferred materialization requires extra iteration
- Poor cache locality

### Task
When N > DENSE_INLINE_MAX but workload is dense (>= 50% density), optimize Simple mode:

1. **Eliminate `batch_inputs` Vec for dense case**:
   - If density >= 50%, materialize values inline like DenseInline does
   - Trade-off: might write same group multiple times
   - Benefit: eliminates one Vec + final iteration

2. **Preallocate `simple_touched_groups` to capacity**:
   - If workload is stable (same groups each batch), reuse exact capacity
   - Track: `last_batch_unique_groups`
   - Pre-allocate to this capacity on subsequent batches

3. **Use arena allocator for large N**:
   - For N > 100K, allocate from a bump allocator
   - Reduces allocator overhead for large scratch structures
   - Reset arena between batches instead of freeing/reallocating

4. **Add "dense-large" fast path**:
   - When N > 100K AND density > 50%:
   - Use simplified algorithm: direct array access with minimal bookkeeping
   - Accept O(N) initialization cost for O(1) access in tight loop

**Success Criteria**:
- Simple mode overhead reduced by 30-50% for N > 100K, density > 50%
- No regression for sparse or small-N workloads
- Code remains maintainable (don't add excessive complexity)

---

## Task 4: Add Comprehensive Benchmark Coverage (Medium Priority)

### Problem
"dense groups" benchmark appears in regression reports but not in source code. Need full coverage.

### Task
Add missing benchmark scenarios to `benches/min_max_bytes.rs`:

1. **Dense groups reused across many batches**:
   ```rust
   fn min_bytes_dense_groups_stable() {
     // Same 50K groups across 100 batches
     // Tests stable group set with high reuse
   }
   ```

2. **Large dense groups at various sizes**:
   ```rust
   fn min_bytes_dense_N() where N in [10K, 25K, 50K, 100K, 250K] {
     // Single batch with N dense groups
     // Tests threshold boundaries
   }
   ```

3. **Growing dense groups**:
   ```rust
   fn min_bytes_growing_dense() {
     // Start with 1K groups, add 1K per batch for 100 batches
     // Final: 100K groups
     // Tests mode transitions
   }
   ```

4. **Mixed density patterns**:
   ```rust
   fn min_bytes_mixed_density() {
     // Alternate: dense batch (N=10K, 90% density)
     //           sparse batch (N=100K, 1% density)
     // Tests mode switching stability
   }
   ```

5. **Threshold boundary tests**:
   ```rust
   fn min_bytes_at_threshold_minus_1() { N = THRESHOLD - 1 }
   fn min_bytes_at_threshold_plus_1() { N = THRESHOLD + 1 }
   // Ensure no performance cliff
   ```

**Success Criteria**:
- All reported benchmark names exist in source code
- Coverage of N from 100 to 1M
- Coverage of density from 1% to 100%
- All benchmarks within 10% of theoretical optimal

---

## Task 5: Add Dynamic Threshold Tuning (Low Priority / Future Enhancement)

### Problem
Static threshold (even if raised to 100K) may not be optimal for all hardware or workloads.

### Task
Implement runtime threshold adjustment:

1. **Calibration on first use**:
   - On accumulator creation, run quick calibration:
   - Measure: time to allocate+init Vec<u64> of size 100K
   - Measure: overhead of Simple mode for 100K groups
   - Set threshold based on ratio

2. **Adaptive threshold per accumulator**:
   - Track: average time per batch in current mode
   - If Simple mode consistently slow for current N, lower threshold
   - If DenseInline consistently fast even at high N, raise threshold

3. **Environment-based defaults**:
   - Detect: available memory, cache size, CPU features
   - On systems with large cache: higher threshold
   - On memory-constrained systems: lower threshold

4. **Make threshold configurable**:
   - Add session config: `datafusion.execution.min_max_dense_inline_threshold`
   - Default: 100,000
   - Users can tune based on their workload characteristics

**Success Criteria**:
- Threshold adapts to actual system performance
- Overhead of calibration < 1ms (negligible)
- Users can override for specific workloads
- Documented in configuration guide

---

## Task 6: Investigate and Fix "large dense groups" Specific Issue (High Priority)

### Problem
"large dense groups" got **worse** (+13.05% → +16.32%) after DenseInline was added, even though other benchmarks improved.

### Analysis Needed
The benchmark processes N=16,384 groups in a single batch. With DenseInline:
- N=16,384 > 10,000 → uses Simple mode
- Simple mode may have gotten slower in the new implementation
- Need to investigate: what changed in Simple mode that made it worse?

### Task
1. **Profile "large dense groups" benchmark**:
   - Compare: original → first version with Simple → current with DenseInline
   - Identify: where is the extra 3% overhead coming from?
   - Likely: epoch checking overhead, or Simple mode initialization cost

2. **Check Simple mode implementation changes**:
   - Compare Simple mode code before and after DenseInline addition
   - Look for: extra checks, changed data structures, new overhead

3. **Possible issues**:
   - Epoch wrapping check overhead (lines 749-754)
   - Slot initialization overhead (lines 747-748)
   - Interaction with DenseInline's data structures (cross-contamination)

4. **Fix specific to this size range**:
   - If N is "just over" threshold (10K-20K), use optimized Simple variant
   - Or: lower DenseInline threshold to 5K, but add "large DenseInline" mode for 5K-100K
   - Or: fix whatever new overhead was introduced

**Success Criteria**:
- "large dense groups": +16.32% → < 5%
- Identify exact source of 3% additional regression
- Fix without breaking other benchmarks

---

## Task 7: Document Workload Mode Selection Logic (Medium Priority)

### Problem
Four modes (Undecided, DenseInline, Simple, SparseOptimized) with complex switching logic. Difficult to reason about.

### Task
Create comprehensive documentation:

1. **Mode selection flowchart**:
   ```
   Start (Undecided)
     ├─ N ≤ 10K & density ≥ 50% → DenseInline
     ├─ N ≤ 100K & density ≥ 10% → Simple
     └─ Otherwise → SparseOptimized
   
   DenseInline
     ├─ density drops < 50% → switch to Simple or Sparse
     └─ N grows > 10K → switch to Simple
   
   Simple
     └─ populated_groups > 100K & density < 1% → switch to Sparse
   
   SparseOptimized
     └─ (never switches back)
   ```

2. **Per-mode characteristics table**:
   | Mode | Optimal For | Memory | Algorithm | Strengths | Weaknesses |
   |------|-------------|--------|-----------|-----------|------------|
   | DenseInline | N≤10K, dense | O(N) epoch | Direct update | Zero-alloc, fast | Limited to small N |
   | Simple | 10K<N≤100K, dense | O(N)×3 vecs | Deferred | Handles larger N | 3× memory, extra iter |
   | SparseOptimized | N>100K, sparse | O(touched) | Hash+dense | Avoids big alloc | Complex, overhead |

3. **Inline comments for every threshold**:
   - `DENSE_INLINE_MAX_TOTAL_GROUPS = 100_000` // "Chosen because..."
   - `DENSE_INLINE_MIN_DENSITY_PERCENT = 50` // "Chosen because..."
   - All constants justified with benchmark data

4. **Module-level doc with examples**:
   ```rust
   /// # Workload Mode Selection Examples
   ///
   /// - Query: `SELECT MAX(name) FROM users GROUP BY city` where 100 cities, 1M users
   ///   → N=100, density=100% → DenseInline mode
   ///
   /// - Query: `SELECT MIN(id) FROM events GROUP BY user_id` where 10M users, 1K events
   ///   → N=10M, density=0.01% → SparseOptimized mode
   ```

**Success Criteria**:
- External contributor can understand mode selection without reading implementation
- Every threshold has documented rationale
- Flowchart matches actual code logic

---

## Task 8: Consider Consolidating Modes (Low Priority / Future Refactoring)

### Problem
Four modes adds complexity. Can we simplify?

### Analysis
Current modes:
- **Undecided**: Temporary, just decides which mode to use
- **DenseInline**: Optimal for small-N dense (N ≤ threshold)
- **Simple**: Compromise for medium-N dense (threshold < N ≤ 100K)
- **SparseOptimized**: Optimal for large-N sparse (N > 100K, low density)

**Question**: Do we really need both DenseInline AND Simple?

### Possible Simplifications

**Option A: Eliminate Simple, extend DenseInline**
- Raise DenseInline threshold to 100K or higher
- Use DenseInline for all dense workloads
- Only have two modes: DenseInline (dense) and SparseOptimized (sparse)
- **Pro**: Simpler, fewer modes
- **Con**: Large epoch arrays (800KB for 100K groups)

**Option B: Eliminate DenseInline, optimize Simple**
- Remove DenseInline entirely
- Fix Simple mode to be as fast as DenseInline for small N
- Use epoch mechanism in Simple mode (it already does)
- **Pro**: One less mode, reuses Simple infrastructure
- **Con**: Loses the specialized fast path

**Option C: Keep current but clarify**
- Accept that four modes are necessary for optimal performance
- Better naming: `DenseSmall`, `DenseLarge`, `Sparse`, `Undecided`
- Clear threshold boundaries: <10K, 10K-100K, >100K
- **Pro**: Performance, clear separation of concerns
- **Con**: Complexity remains

### Task
1. Benchmark Option A vs Option B vs Current
2. Measure: code complexity (lines, branches) vs performance
3. Choose based on: "multiple simple paths > one complex path" principle
4. If keeping all modes, ensure each has clear, non-overlapping purpose

**Success Criteria**:
- Minimum number of modes needed for optimal performance
- Each mode has clear, distinct purpose
- Code maintainability improved

---

## Priority and Execution Order

### Phase 1: Fix Critical Regressions (Do First)
1. **Task 1**: Raise `DENSE_INLINE_MAX_TOTAL_GROUPS` to 100,000
   - **Estimated impact**: Fixes "dense groups" -72.51%, "large dense" -16.32%
   - **Effort**: 1 line change + testing
   - **Risk**: Low (can revert if issues found)

2. **Task 6**: Investigate "large dense groups" specific regression
   - **Estimated impact**: Fix remaining 3% regression
   - **Effort**: 1-2 hours profiling + targeted fix
   - **Risk**: Medium (need to understand root cause)

### Phase 2: Validate and Optimize (Do Next)
3. **Task 2**: Empirically determine optimal threshold
   - **Purpose**: Validate that 100K is correct choice
   - **Effort**: Create microbenchmarks, analyze data
   - **Outcome**: Data-driven threshold decision

4. **Task 4**: Add comprehensive benchmark coverage
   - **Purpose**: Prevent future regressions
   - **Effort**: Add 5-8 new benchmark functions
   - **Outcome**: Full coverage of N and density ranges

### Phase 3: Refinements (Do Later)
5. **Task 3**: Optimize Simple mode for large-N
6. **Task 7**: Document mode selection logic
7. **Task 8**: Consider mode consolidation
8. **Task 5**: Add dynamic threshold tuning (optional)

## Expected Results After Phase 1

| Benchmark | Current | After Task 1 | After Task 6 | Target |
|-----------|---------|--------------|--------------|--------|
| monotonic group ids | -40.13% | -40.13% | -40.13% | Maintain |
| sparse groups | -30.57% | -30.57% | -30.57% | Maintain |
| dense first batch | +1.90% | +1.90% | +1.90% | < 5% ✓ |
| **dense groups** | **+72.51%** | **< 5%** | **< 5%** | < 5% ✓ |
| dense reused | +5.47% | +5.47% | +5.47% | < 10% ✓ |
| **large dense** | **+16.32%** | **< 5%** | **< 5%** | < 5% ✓ |

## Success Criteria Summary

After completing all high-priority tasks:
- ✓ All benchmarks show < 5% regression OR improvement
- ✓ "dense groups" fixed: +72.51% → < 5%
- ✓ "large dense groups" fixed: +16.32% → < 5%
- ✓ Improvements maintained: monotonic -40%, sparse -30%
- ✓ Threshold decision backed by data
- ✓ Comprehensive benchmark coverage
- ✓ Clear documentation of design decisions
