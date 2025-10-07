# Min/Max Bytes Dense Inline Regression - Summary

## Problem Statement

PR commits `c1ac251d6^..93e1d7529` introduced a `DenseInline` mode to eliminate quadratic scratch allocation overhead in `MinMaxBytesAccumulator::update_batch`. While this successfully improved sparse and monotonic workloads by 29-40%, it caused 1-20% regressions in three dense benchmark patterns.

## Benchmark Results

| Benchmark | Change | P-value | Status |
|-----------|--------|---------|--------|
| **min bytes monotonic group ids** | **-40.15%** | 0.000000 | ✅ Improvement |
| **min bytes sparse groups** | **-28.97%** | 0.000000 | ✅ Improvement |
| min bytes dense duplicate groups | +20.02% | 0.000000 | ⚠️ Regression |
| min bytes dense first batch | +1.60% | 0.000000 | ⚠️ Regression |
| min bytes dense reused accumulator | +1.17% | 0.000000 | ⚠️ Regression |

## Root Cause

The `DenseInline` implementation tracks per-batch statistics (unique groups, max group index) to drive mode-switching heuristics. This tracking requires:

1. **Epoch-based mark array** (`dense_inline_marks`) to detect first-touch of each group per batch
2. **Fast-path detection logic** (5 variables tracked per iteration) to optimize sequential access
3. **Batch-level mark writes** after processing a sequential range

For **stable dense workloads** (the regression cases), these statistics are collected on every batch but never trigger a mode change. The overhead becomes pure waste:

- **Dense duplicate groups** (+20%): Pattern `[0,0,1,1,2,2,...]` defeats fast-path detection, causing 512 mark checks per batch × 32 batches
- **Dense first batch** (+1.6%): Cold-start overhead from allocating marks + epoch management for single batch  
- **Dense reused accumulator** (+1.2%): Cumulative cost of 512 mark writes + epoch per batch × 32 batches

The mark tracking is **necessary** for the first 2-3 batches to evaluate mode stability, but becomes **redundant** once the accumulator commits to `DenseInline` mode.

## Solution Strategy

The fix involves **commit-once optimization**: once the accumulator detects a stable `DenseInline` workload, disable all statistics tracking and switch to a minimal fast path that just performs the min/max comparisons.

### Key Insights

1. **Sparse/monotonic improvements are orthogonal**: They benefit from avoiding large scratch allocations, which is independent of the mark-tracking overhead
2. **Mode switches are rare**: Once an accumulator enters `DenseInline`, it almost never switches back for these patterns
3. **Statistics are only needed early**: After 2-3 batches we can confidently commit to a mode
4. **Sequential duplicates are common**: Pattern `[0,0,1,1,...]` is realistic (e.g., sorted data with ties)

### Proposed Fixes (in priority order)

#### Task 1: Commit-Once Fast Path (Critical)
Add `dense_inline_committed` flag that triggers after 3 stable batches. Once committed:
- Skip all mark allocation, epoch management, and statistics tracking  
- Use minimal loop: just `for (group, val) in ... { if should_replace { set_value() } }`
- **Expected**: Eliminates 15-20% of regression

#### Task 2: Run-Length Detection (High Priority)
Detect when the same group appears consecutively (e.g., `[0,0,...]`) and skip mark checks:
- Add `last_group_index` to track previous iteration
- Skip mark logic if `group_index == last_group_index`
- **Expected**: Eliminates 5-10% additional regression for duplicate-heavy patterns

#### Task 3: Defer Mark Allocation (Medium Priority)  
Don't allocate `dense_inline_marks` on first batch:
- First batch just counts unique groups by tracking `last_group != group`
- Allocate marks only when second batch arrives
- **Expected**: Eliminates cold-start overhead (~1.6% regression)

#### Task 4: Batch-Write Optimization (Low Priority)
Defer mark writes until end of batch when fast-path is maintained:
- Track ranges instead of writing marks inline
- Batch-write all ranges at end (or skip if committed)
- **Expected**: Minor improvements, mainly for cleaner code

#### Task 5: Regression Guard Benchmarks (Critical for Prevention)
Add benchmarks specifically for:
- Single-batch cold-start sequential pattern
- Consecutive duplicates (3x repetition)  
- Stable mode with 10+ batches
- **Expected**: Prevents future regressions

## Detailed Documentation

- **Root Cause Analysis**: `min_max_bytes_dense_inline_regression_root_cause.md`
  - Detailed breakdown of each regression
  - Line-by-line code analysis
  - Overhead calculations
  
- **Remediation Tasks**: `min_max_bytes_dense_inline_regression_tasks.md`
  - Step-by-step implementation guidance
  - Code snippets for each task
  - Testing and validation procedures

## Success Criteria

After implementing Tasks 1-5:

- ✅ Maintain **-28% to -40%** improvements in sparse/monotonic workloads
- ✅ Reduce dense regressions to **≤ 2%** (noise threshold)
- ✅ Add benchmark coverage for critical patterns
- ✅ No new regressions in existing benchmarks

## Timeline Estimate

- **Phase 1** (Tasks 1-2): 1-2 days of development + testing → Eliminates 90% of regression
- **Phase 2** (Tasks 3-4): 0.5-1 day → Polishes remaining edge cases  
- **Phase 3** (Task 5): 0.5 day → Adds regression guards

**Total**: 2-4 days for complete remediation

## Alternative Approach

If the above optimizations prove insufficient, consider:

1. **Route duplicates to Simple mode**: Detect duplicate-heavy patterns (`unique < batch_size / 2`) and use `Simple` instead of `DenseInline`
2. **Restrict DenseInline to monotonic only**: Only use `DenseInline` when `max_group_index - min_group_index + 1 == unique_groups` (perfect sequential access)
3. **Hybrid threshold**: Use `DenseInline` only for `total_num_groups > 10_000` to avoid small-group overhead

This "safe fallback" preserves improvements while avoiding regressions, at the cost of not optimizing the "perfect" dense case.

## References

- Original issue: #17897 (quadratic scratch allocation)
- PR commits: `c1ac251d6^..93e1d7529`
- Implementation: `datafusion/functions-aggregate/src/min_max/min_max_bytes.rs`
- Benchmarks: `datafusion/functions-aggregate/benches/min_max_bytes.rs`

## Status

- [x] Root cause identified
- [x] Remediation tasks defined
- [ ] Implementation in progress
- [ ] Validation and benchmarking
- [ ] Documentation updates
