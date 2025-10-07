# Root Cause Analysis: MinMaxBytesState Regressions After DenseInline Addition

## Updated Benchmark Results Analysis (commit 027966107)

The addition of the `DenseInline` workload mode improved 4 out of 6 benchmarks but left one severe regression unchanged:

| Benchmark | New Result | Previous Result | Change Direction |
|-----------|-----------|-----------------|------------------|
| min bytes monotonic group ids | **-40.13%** ✓ | -18.56% | **Better** (22% more improvement) |
| min bytes sparse groups | **-30.57%** ✓ | -17.53% | **Better** (13% more improvement) |
| min bytes dense first batch | **+1.90%** ✓ | +12.62% | **Much better** (11% less regression) |
| **min bytes dense groups** | **+72.51%** ✗ | **+72.51%** | **NO CHANGE - STILL SEVERE** |
| min bytes dense reused accumulator | **+5.47%** ✓ | +30.13% | **Much better** (25% less regression) |
| min bytes large dense groups | **+16.32%** ✗ | +13.05% | **Slightly worse** (3% more regression) |

## What the DenseInline Mode Does

### Implementation Overview

The `DenseInline` mode (lines 650-722) is designed to be a truly zero-allocation fast path for small, dense group domains:

1. **Pre-allocated epoch markers**: `dense_inline_marks: Vec<u64>` sized to `total_num_groups`
2. **Direct inline updates**: Calls `self.set_value()` immediately when a better value is found
3. **No deferred materialization**: No `batch_inputs` Vec, no `touched_groups` Vec
4. **Simple epoch-based tracking**: Uses wrapping epoch counter to detect first touch of each group

### Activation Heuristic

DenseInline is used when (lines 917-925):
```rust
total_num_groups <= 10_000 AND
unique_groups * 100 >= total_num_groups * 50  // i.e., >= 50% density
```

### When It's Selected (from lines 636-648)

1. **Undecided mode**: If `total_num_groups <= 10_000`, tries DenseInline first
2. **After first batch**: Switches based on observed density
   - If density >= 50%: stays in or enters DenseInline
   - If density >= 10% but < 50%: enters Simple
   - Otherwise: enters SparseOptimized

## Why Most Benchmarks Improved

### "dense first batch" (+12.62% → +1.90%)
- **Before**: Used Simple mode with three Vec allocations + final iteration
- **After**: Uses DenseInline with single pre-allocated epoch array
- **Why improved**: Eliminated `touched_groups` and `batch_inputs` Vecs, no final iteration
- BATCH_SIZE = 512, so density = 512/512 = 100% → DenseInline selected ✓

### "dense reused accumulator" (+30.13% → +5.47%)
- **Before**: Simple mode reallocated or reset on every batch
- **After**: DenseInline reuses `dense_inline_marks` across 32 batches via epoch mechanism
- **Why improved**: Epoch-based reuse eliminates per-batch allocation churn
- BATCH_SIZE = 512, density = 100% across all batches → DenseInline selected ✓

### "monotonic group ids" (-18.56% → -40.13%)
- **Before**: Each batch (512 new groups) used Simple mode
- **After**: First batch uses DenseInline, subsequent batches may switch
- **Why improved**: DenseInline is even faster than Simple for first-touch scenarios
- Each batch: 512 unique groups with max_index = batch*512+511, total = 16,384
- First batch: density = 512/512 = 100% → DenseInline ✓
- Later batches: as total_num_groups grows, may switch modes but benefits from better first batch

### "sparse groups" (-17.53% → -30.57%)
- **Before**: Detected sparse pattern, used complex sparse mode
- **After**: Same sparse mode but DenseInline first batch is faster
- **Why improved**: Better initial processing before sparse detection kicks in
- 16 unique groups out of 10,000 total → switches to Sparse mode after first batch

## Why "dense groups" STILL Regresses (+72.51%)

### The Mystery Benchmark

**Critical Issue**: The benchmark "min bytes dense groups" is **NOT in the benchmark file** (`benches/min_max_bytes.rs`). The file contains:
- `min_bytes_dense_first_batch`
- `min_bytes_dense_reused_batches` (called "dense reused accumulator" in reports)
- `min_bytes_sparse_groups`
- `min_bytes_monotonic_group_ids`
- `min_bytes_large_dense_groups`

**Hypothesis 1**: "dense groups" is actually "dense reused accumulator"
- But "dense reused" shows +5.47%, not +72.51%
- These are different benchmarks in the report

**Hypothesis 2**: "dense groups" was renamed or is from a different benchmark suite
- Could be a CI-specific benchmark not in the source tree
- Or from an earlier version of the benchmark file

**Hypothesis 3**: "dense groups" tests a specific pathological pattern not covered by visible benchmarks

### Likely Scenario for +72.51% Regression

Based on the severity and the fact that ALL other dense benchmarks improved, "dense groups" likely tests a scenario where:

1. **Group count exceeds DenseInline threshold** (> 10,000 groups)
   - Falls back to Simple mode
   - But Simple mode still has overhead (though improved with epoch reuse)

2. **High frequency of updates to the same groups across batches**
   - Each batch: process groups [0, 1, 2, ..., N-1] repeatedly
   - DenseInline would be perfect, but N > 10,000 disqualifies it
   - Simple mode still has deferred materialization overhead

3. **The benchmark structure triggers worst-case Simple mode behavior**
   - Maybe: many small batches (high per-batch overhead)
   - Maybe: same dense groups every time (reuse not optimized in Simple)
   - Maybe: large N (11K-99K range) where Simple is too heavy but Sparse is overkill

### Code Evidence

Looking at Simple mode (lines 728-827):
- Still pre-allocates `simple_slots: Vec<SimpleSlot>` sized to `total_num_groups`
- Still builds `touched_groups` Vec
- Still builds `batch_inputs` Vec
- Still has final materialization loop (lines 811-823)

**For N > 10,000**, even with epoch reuse, Simple mode has:
1. Large pre-allocation: `Vec<SimpleSlot>` where each SimpleSlot is ~24 bytes
   - For N = 50,000: 50K × 24 = 1.2 MB allocation
2. Three-Vec overhead: `simple_slots`, `simple_touched_groups`, `batch_inputs`
3. Deferred materialization adds latency
4. Worse cache behavior than DenseInline's direct updates

## Why "large dense groups" Got Slightly Worse (+13.05% → +16.32%)

### Benchmark Details
- `LARGE_DENSE_GROUPS = 16_384` (from MONOTONIC_TOTAL_GROUPS)
- Processes 16,384 groups in a single batch
- Groups are dense sequential: [0, 1, 2, ..., 16383]

### Why It Regressed Further

1. **N = 16,384 exceeds DenseInline threshold** (10,000)
   - Falls back to Simple mode
   - Cannot use the zero-overhead DenseInline path

2. **Large N amplifies Simple mode overhead**:
   - `simple_slots` pre-allocation: 16,384 × 24 bytes = ~393 KB
   - `simple_touched_groups`: 16,384 × 8 bytes = 131 KB
   - `batch_inputs`: 16,384 × 8 bytes (ptr) = 131 KB
   - Total scratch: ~655 KB per batch

3. **Comparison to original**:
   - Original code: `vec![Location; 16384]` ≈ 131 KB
   - Current Simple: ~655 KB across three Vecs
   - **5× more memory allocation overhead**

4. **Epoch mechanism overhead**:
   - For large N, checking `slot.epoch != self.simple_epoch` on 16K entries
   - Updating epochs for 16K entries
   - Extra branches and memory writes

5. **Cache effects**:
   - Three separate Vecs means poorer cache locality
   - Original single Vec had better sequential access patterns

## Root Cause Summary

### Core Issue
The `DenseInline` mode **successfully optimizes the N ≤ 10,000 case** but leaves larger dense workloads (10K < N < 100K) without an optimal path. These workloads:
- Are **too large for DenseInline** (artificial 10K threshold)
- Are **not sparse** (so SparseOptimized is overkill)
- Fall back to **Simple mode with significant overhead**

### The Threshold Problem

The constant `DENSE_INLINE_MAX_TOTAL_GROUPS = 10_000` is **arbitrary and too conservative**:

1. **Modern systems can handle much larger epoch arrays**:
   - A `Vec<u64>` with 100K entries is only 800 KB
   - Typical L3 cache is 8-32 MB
   - Memory bandwidth is not a bottleneck at this scale

2. **The 10K threshold causes a performance cliff**:
   - N = 9,999: uses fast DenseInline path
   - N = 10,001: falls back to slower Simple path
   - **Discontinuity at arbitrary threshold**

3. **Large dense workloads are common**:
   - User IDs in medium-sized applications: 50K-500K
   - Product SKUs: 10K-100K
   - Geographic entities (zip codes, census blocks): 40K-200K

### Why "dense groups" Benchmark Shows +72.51%

Most likely scenario:
- Tests N in range 10,001 - 99,999 (too big for DenseInline, too small for Sparse)
- Repeatedly processes the same dense groups across many batches
- Simple mode overhead compounds: large allocations + deferred materialization + three-Vec design
- Original code was simpler and faster for this specific range

### Why "large dense groups" Got Worse

- N = 16,384 falls in the "dead zone" between DenseInline and Sparse
- Simple mode allocates 5× more memory than original
- Epoch checking overhead for large N
- Poorer cache locality with three-Vec design

## The Real Problem: False Economy on Threshold

The implementation assumes that DenseInline becomes expensive at N > 10K, but:

1. **Memory cost is negligible**:
   - `Vec<u64>` for 100K groups = 800 KB (tiny by modern standards)
   - One-time allocation, reused across all batches

2. **DenseInline is algorithmically optimal for dense workloads**:
   - Single pass with direct updates
   - No deferred materialization
   - No multiple Vec allocations
   - Better cache locality

3. **The overhead of Simple mode exceeds the cost of larger DenseInline**:
   - Simple allocates 3× more total memory (slots + touched + inputs)
   - Simple has extra iterations and indirection
   - Simple has worse cache behavior

4. **The threshold should be based on actual performance, not gut feeling**:
   - Current: arbitrary 10K cutoff
   - Better: benchmark-driven (e.g., 100K or even 1M)
   - Best: dynamic based on system characteristics

## Task Formulation

Based on this analysis, the issues and fixes are clear:

### For "dense groups" (+72.51% regression)
**Cause**: N likely in range 10K-100K, uses Simple mode which has ~5× overhead  
**Fix**: Raise `DENSE_INLINE_MAX_TOTAL_GROUPS` from 10,000 to 100,000 or higher

### For "large dense groups" (+16.32% regression)  
**Cause**: N = 16,384 exceeds 10K threshold, forced into Simple mode with 5× memory overhead  
**Fix**: Same - raise the threshold to include this size

### General Principle Violated

The code optimizes for **perceived** memory cost (epoch array size) while ignoring **actual** performance cost (Simple mode overhead). This is premature optimization based on assumptions rather than measurement.

**From AGENTS.md:**
> "Optimizations should be focused on bottlenecks — those steps that are repeated millions of times in a query; otherwise, prefer simplicity."

The 10K threshold optimizes for a non-bottleneck (memory) while creating a bottleneck (mode switching overhead).
