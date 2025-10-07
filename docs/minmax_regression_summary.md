# MinMaxBytesState Performance Regression Analysis - Executive Summary

## Problem Statement

The PR (commits c1ac251d6^..8ce5d1f8d) successfully fixed quadratic behavior for sparse/high-cardinality aggregations but introduced **4 regressions and 2 improvements** in benchmark performance:

### Benchmark Results
- ✓ **min bytes monotonic group ids**: -18.56% (improvement)
- ✓ **min bytes sparse groups**: -17.53% (improvement)  
- ✗ **min bytes dense first batch**: +12.62% (regression)
- ✗ **min bytes dense groups**: +72.51% (regression) ← **SEVERE**
- ✗ **min bytes dense reused accumulator**: +30.13% (regression)
- ✗ **min bytes large dense groups**: +13.05% (regression)

## Root Cause

**The "Simple" mode still has the same O(total_num_groups) allocation problem as the original code**, just with a different enum type. Additionally, it introduces **extra overhead**:

1. **Per-batch allocation remains**: `vec![SimpleLocation::Untouched; total_num_groups]` (line 602)
2. **Three separate Vecs maintained**: `locations`, `touched_groups`, `batch_inputs`
3. **Extra final iteration**: loops over `touched_groups` to materialize values (lines 670-674)
4. **Enum discrimination overhead**: three-variant enum with matching on every access
5. **No allocation reuse**: each batch re-allocates from scratch

### Why Some Benchmarks Improved

The improvements came from **better worst-case behavior**, not better common-case:
- **Sparse groups** (-17.53%): avoided allocating `vec![...; 10_000]` for only 16 active groups
- **Monotonic group ids** (-18.56%): each of 32 batches allocated `vec![...; 512]` instead of `vec![...; 16_384]`

These improvements are real and valuable for their scenarios.

### Why Most Benchmarks Regressed

The regressions came from **added overhead for dense workloads**:
- **Dense groups** (+72.51%): repeatedly processes same groups, pays full overhead every batch
- **Dense reused** (+30.13%): 32 batches × allocation overhead × iteration overhead
- **Dense first** (+12.62%): single batch but pays for all three Vec allocations + extra iteration
- **Large dense** (+13.05%): 16K allocation + 16K iteration overhead

## The Core Issue

The PR violated the principle documented in `AGENTS.md`:

> **Prefer multiple simple code paths over a single complex adaptive path. Optimize for the common case first and keep that path fast and easy to reason about; handle rare or complex edge cases with separate, well-tested branches or fallbacks.**

The current implementation tried to create a "middle ground" Simple mode that:
- ✗ Still has the allocation problem (not truly simple)
- ✗ Adds complexity with three Vecs and extra iteration (not zero-overhead)
- ✗ Can't handle the common dense case efficiently (defeats the purpose)

## The Fix Strategy

### High-Priority Tasks (Address Severe Regressions)

1. **Add TrueSimple mode** for `total_num_groups < 10_000` + dense workloads
   - Eliminate `touched_groups` Vec entirely
   - Accept O(N) final iteration for small N
   - **Target**: "dense first batch" +12.62% → < 5%

2. **Reuse allocations in Simple mode** across batches
   - Pre-allocate `locations` Vec once, reuse with epoch tagging
   - Lazy reset: only touch entries actually used
   - **Target**: "dense reused accumulator" +30.13% → < 10%

3. **Detect stable group patterns** for repeated processing
   - If same groups touched every batch, use optimized path
   - Pre-allocate to exact capacity, no resizing
   - **Target**: "dense groups" +72.51% → < 5%

4. **Optimize large-N dense case** (N > 8192)
   - Don't allocate O(N) vector, use compact working set
   - Or: chunk processing with smaller scratch space
   - **Target**: "large dense groups" +13.05% → < 5%

### Medium-Priority Tasks

5. Eliminate final iteration overhead in Simple mode
6. Optimize Undecided mode first-batch behavior
7. Add hysteresis to prevent mode-switching thrashing
8. Profile allocator behavior to validate assumptions

### Supporting Tasks

9. Expand benchmark suite for comprehensive coverage
10. Documentation and code simplification

## Key Insights

1. **The original "problem" wasn't actually a problem for dense workloads**
   - O(total_num_groups) allocation per batch is trivial when N < 100K
   - Groups reused across batches amortize the cost
   - Modern allocators handle zeroed pages efficiently

2. **The optimization added more overhead than it saved**
   - Dense: +12% to +72% regression (common case)
   - Sparse: -17% improvement (rare case)
   - Net: pessimized the common case to optimize the rare case

3. **The fix requires architectural change, not parameter tuning**
   - Can't salvage current Simple mode by tweaking thresholds
   - Need truly separate paths: minimal for dense, complex for sparse
   - Each path must be optimal for its scenario

4. **Success requires following the stated principle**
   - Multiple simple paths > one complex adaptive path
   - Optimize common case (dense) first
   - Handle edge case (sparse) separately
   - Don't compromise common case for edge case

## Documentation Generated

See detailed analysis and tasks in:
- `docs/minmax_regression_root_cause_analysis.md` - Technical deep-dive
- `docs/minmax_regression_fix_tasks.md` - Concrete actionable tasks
- This file - Executive summary for quick reference

## Next Steps

1. Implement high-priority tasks (1-4) to address severe regressions
2. Validate with benchmark suite (target: < 5% regression for all dense cases)
3. Ensure improvements maintained for sparse cases (-17% and -18%)
4. Complete supporting tasks for maintainability and future-proofing
