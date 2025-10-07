# Min/Max Bytes Dense Inline Regression - Root Cause Analysis

## Executive Summary

The PR introduced a `DenseInline` mode optimization to eliminate per-batch scratch allocations for small, dense group workloads. While it successfully improved sparse and monotonic workloads (-28.97% and -40.15%), it regressed three dense benchmarks (+1.17% to +20.02%) due to unnecessary overhead from tracking statistics that are only needed for mode-switching heuristics but add no value once the accumulator has committed to `DenseInline` mode.

## Benchmark Results Analysis

### Improvements ✅
1. **min bytes sparse groups**: -28.97% (p < 0.000001)
   - Benefits from avoiding dense scratch allocation for sparse access patterns
   
2. **min bytes monotonic group ids**: -40.15% (p < 0.000001)  
   - Monotonically increasing group IDs trigger fast-path optimization in the new code
   - Eliminates repeated scratch buffer allocations as groups grow

### Regressions ⚠️
1. **min bytes dense duplicate groups**: +20.02% (p < 0.000001)
   - **Pattern**: 512 rows, 256 unique groups (each appears twice: `[0,0,1,1,2,2,...]`)
   - **Impact**: ~20% slowdown over 32 batches
   
2. **min bytes dense first batch**: +1.60% (p < 0.000001)
   - **Pattern**: 512 rows, 512 sequential groups `[0,1,2,...,511]` in single batch
   - **Impact**: Small but statistically significant regression
   
3. **min bytes dense reused accumulator**: +1.17% (p < 0.000001)
   - **Pattern**: 512 sequential groups, same pattern repeated over 32 batches
   - **Impact**: Cumulative overhead across multiple batches

---

## Root Cause: Redundant Mark-Tracking Overhead

### The Problem

The `update_batch_dense_inline_impl` function performs several tracking operations on **every batch** to collect statistics for mode-switching heuristics:

```rust
// Lines 677-778 in min_max_bytes.rs
fn update_batch_dense_inline_impl<'a, F, I>(...) -> Result<BatchStats> {
    // 1. Initialize or resize epoch tracking array
    if self.dense_inline_marks.len() < total_num_groups {
        self.dense_inline_marks.resize(total_num_groups, 0_u64);
    }
    
    // 2. Increment epoch (with wraparound handling)
    self.dense_inline_epoch = self.dense_inline_epoch.wrapping_add(1);
    if self.dense_inline_epoch == 0 {
        for mark in &mut self.dense_inline_marks {
            *mark = 0;  // Full array reset every 2^64 batches
        }
        self.dense_inline_epoch = 1;
    }
    
    // 3. Track statistics with fast-path detection
    let mut unique_groups = 0_usize;
    let mut max_group_index: Option<usize> = None;
    let mut fast_path = true;
    let mut fast_rows = 0_usize;
    let mut fast_start = 0_usize;
    let mut fast_last = 0_usize;
    
    for (group_index, new_val) in group_indices.iter().copied().zip(iter.into_iter()) {
        // ... null check and bounds check ...
        
        if fast_path {
            // Try to detect sequential access pattern
            if fast_rows == 0 {
                fast_start = group_index;
                fast_last = group_index;
            } else if group_index == fast_last + 1 {
                fast_last = group_index;
            } else {
                fast_path = false;
                // Batch-write marks when we fall off fast path
                if fast_rows > 0 {
                    unique_groups = fast_rows;
                    max_group_index = Some(...);
                    let epoch = self.dense_inline_epoch;
                    let marks = &mut self.dense_inline_marks;
                    for idx in fast_start..=fast_last {
                        marks[idx] = epoch;  // ← Redundant writes
                    }
                }
            }
            if fast_path {
                fast_rows = fast_rows.saturating_add(1);
            }
        }
        
        if !fast_path {
            // Individual mark tracking for non-sequential access
            let mark = &mut self.dense_inline_marks[group_index];
            if *mark != self.dense_inline_epoch {
                *mark = self.dense_inline_epoch;  // ← Redundant write
                unique_groups = unique_groups.saturating_add(1);
                max_group_index = Some(...);
            }
        }
        
        // Actual min/max logic (the real work)
        let should_replace = match self.min_max[group_index].as_ref() {
            Some(existing_val) => cmp(new_val, existing_val.as_ref()),
            None => true,
        };
        if should_replace {
            self.set_value(group_index, new_val);
        }
    }
    
    // Return stats for mode-switching heuristics
    Ok(BatchStats { unique_groups, max_group_index })
}
```

### Why This Causes Regressions

#### 1. **Dense Duplicate Groups** (+20.02% regression)
**Benchmark pattern**: `[0,0,1,1,2,2,...,255,255]` repeated 32 times

**Problem**: 
- First occurrence of each group writes to `dense_inline_marks[group_index]`
- Second occurrence **reads and compares** the mark but doesn't update (already marked)
- Fast-path detection fails on the very first duplicate (group 0 appears twice in a row)
- Falls through to slow path with individual mark checks for **all 512 rows**
- This happens **32 times** (once per batch)

**Overhead breakdown**:
```
Per batch (512 rows):
  - 256 mark writes (first touch of each group)
  - 512 mark reads + comparisons (every row checks the mark)
  - 256 unique_groups increments
  - 512 max_group_index Option comparisons
  
× 32 batches = 16,384 mark checks, 8,192 mark writes
```

The mark tracking is **100% waste** because:
- Mode is already committed to `DenseInline` after first batch
- Statistics aren't used (mode doesn't switch back)
- The actual min/max logic is trivial (just string comparisons)

#### 2. **Dense First Batch** (+1.60% regression)  
**Benchmark pattern**: `[0,1,2,...,511]` single batch, fresh accumulator

**Problem**:
- Sequential access pattern **does** trigger fast path
- But still pays for:
  - Initial `dense_inline_marks` allocation (512 × 8 bytes)
  - Epoch increment and wraparound check
  - Fast-path state tracking (5 variables updated per row)
  - Batch mark write loop at the end (`for idx in 0..=511 { marks[idx] = epoch }`)
  
**Why it matters**:
- This is a **cold start** benchmark (new accumulator each iteration)
- Even 1-2% overhead is significant when the base operation is simple
- The old `Simple` path would just stage values in a scratch vector—no epoch, no marks

#### 3. **Dense Reused Accumulator** (+1.17% regression)
**Benchmark pattern**: `[0,1,2,...,511]` repeated 32 times on same accumulator

**Problem**:
- Sequential pattern keeps triggering fast path (good!)
- But **every batch** still performs:
  - Epoch increment + wraparound check
  - Fast-path loop overhead (tracking 5 variables)
  - Batch mark write at end (512 writes per batch)
  - BatchStats construction and mode evaluation
  
**Cumulative effect**:
```
32 batches × (512 mark writes + epoch management + stats tracking)
  = ~16,384 redundant mark writes
  + 32 mode evaluations that do nothing
```

---

## Why the Improvements Worked

### Sparse Groups (-28.97%)
- **Before**: Allocated `locations` vector sized to `LARGE_TOTAL_GROUPS` (10,000) every batch
- **After**: Only tracks 16 unique groups in marks/sparse scratch
- **Benefit**: Avoided 10,000-element vector allocation/zeroing per batch

### Monotonic Group IDs (-40.15%)
- **Before**: Each batch allocated increasingly large scratch vector (batch 1: 512, batch 2: 1024, ..., batch 32: 16,384)
- **After**: Fast-path detection triggers immediately, uses marks sized to current max (grows incrementally)
- **Benefit**: Eliminated quadratic allocation growth

---

## The Core Issue: Mode-Switching Overhead After Commitment

Once an accumulator enters `DenseInline` mode (which happens on the first batch for these benchmarks), the statistics tracking becomes pure overhead:

```rust
fn record_batch_stats(&mut self, stats: BatchStats, total_num_groups: usize) {
    // ...
    match self.workload_mode {
        WorkloadMode::DenseInline => {
            // Check if we should switch to sparse
            if self.should_switch_to_sparse() {  // Always false for these benchmarks
                self.enter_sparse_mode();
                self.workload_mode = WorkloadMode::SparseOptimized;
            } else if let Some(max_group_index) = stats.max_group_index {
                // Check if we should downgrade to simple or sparse
                // This also never triggers for stable dense workloads
            }
        }
        // ...
    }
}
```

For the regressed benchmarks:
- `total_num_groups` is small (256-512)
- Density remains high (50-100%)
- Mode never switches

**Result**: All the mark tracking, epoch management, and stats collection is wasted work.

---

## Summary Table

| Benchmark | Pattern | Fast Path? | Regression | Root Cause |
|-----------|---------|------------|------------|------------|
| **dense duplicate groups** | `[0,0,1,1,...]` × 32 | ❌ Falls off immediately | **+20.02%** | 512 mark checks per batch × 32 batches |
| **dense first batch** | `[0,1,2,...]` once | ✅ Yes | **+1.60%** | Cold-start overhead: allocation + marks + epoch |
| **dense reused accumulator** | `[0,1,2,...]` × 32 | ✅ Yes | **+1.17%** | 512 mark writes + epoch per batch × 32 |
| sparse groups | 16 groups | N/A | **-28.97%** | Avoided 10K-element allocation |
| monotonic group ids | Growing | ✅ Yes | **-40.15%** | Avoided quadratic growth |

---

## Next Steps

See `min_max_bytes_dense_inline_regression_tasks.md` for detailed remediation tasks.
