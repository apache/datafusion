# Fix Tasks for DenseInline First-Batch Regressions

## Overview

Two benchmarks show regressions after the DenseInline sparse optimization:
- **min bytes dense first batch**: +4.11%
- **min bytes large dense groups**: +2.40%

**Root Cause**: DenseInline allocates `dense_inline_marks` vector on first batch to prepare for reuse, but this overhead is wasted when accumulator is used for only a single batch.

**Solution**: Defer mark allocation until there's evidence of accumulator reuse (second batch).

---

## Task 1: Implement Lazy Mark Allocation (High Priority) 🔴

### Problem Statement
Lines 857-859 set `dense_inline_marks_ready = true` after processing first batch, causing `prepare_dense_inline_marks()` to allocate the marks vector even when it will never be used (single-batch scenarios).

### Proposed Fix
Only prepare marks when there's evidence of reuse (processed_batches > 0).

### Implementation

**File**: `datafusion/functions-aggregate/src/min_max/min_max_bytes.rs`

**Location**: Lines 857-859 in `update_batch_dense_inline_impl()`

**Current Code**:
```rust
if processed_any {
    self.dense_inline_marks_ready = true;
}

Ok(BatchStats {
    unique_groups,
    max_group_index,
})
```

**New Code**:
```rust
// Only prepare marks if we've processed at least one batch already.
// This indicates the accumulator is being reused across multiple batches.
// For single-batch scenarios, we avoid the allocation overhead entirely.
if processed_any && self.processed_batches > 0 {
    self.dense_inline_marks_ready = true;
}

Ok(BatchStats {
    unique_groups,
    max_group_index,
})
```

### Rationale

1. **First batch** (`processed_batches == 0`):
   - Fast path handles sequential groups with zero allocation
   - `dense_inline_marks_ready` remains `false`
   - No marks allocated, no overhead

2. **Second batch** (`processed_batches == 1`):
   - Now we know the accumulator is being reused
   - Set `dense_inline_marks_ready = true`
   - Marks will be allocated on second batch (acceptable cost for reuse benefit)

3. **Subsequent batches** (`processed_batches >= 2`):
   - Marks already allocated and ready
   - Epoch-based reuse provides benefit
   - After 3 batches, commits to fast path with no tracking

### Expected Impact

| Benchmark | Before | After | Improvement |
|-----------|--------|-------|-------------|
| min bytes dense first batch | +4.11% | ~0% | +4% gained |
| min bytes large dense groups | +2.40% | ~0% | +2% gained |
| min bytes dense reused accumulator | -10.45% | -10.45% | No change |
| min bytes monotonic group ids | -39.27% | -39.27% | No change |
| min bytes sparse groups | -26.23% | -26.23% | No change |

**No regressions expected**: Multi-batch scenarios pay mark allocation cost on batch 2 instead of batch 1 (negligible difference when amortized).

### Testing

1. Run existing benchmark suite:
   ```bash
   cargo bench --bench min_max_bytes
   ```

2. Verify:
   - "dense first batch" regression eliminated
   - "large dense groups" regression eliminated
   - All existing improvements maintained
   - No new regressions

3. Manual verification:
   ```rust
   // Single batch: should NOT allocate marks
   let mut acc = MinMaxBytesAccumulator::new_min(DataType::Utf8);
   acc.update_batch(&values, &groups, None, 512).unwrap();
   assert!(acc.inner.dense_inline_marks.is_empty());
   
   // Second batch: should NOW allocate marks
   acc.update_batch(&values, &groups, None, 512).unwrap();
   assert_eq!(acc.inner.dense_inline_marks.len(), 512);
   ```

### Success Criteria
- ✅ Both first-batch regression benchmarks show < 1% overhead
- ✅ All multi-batch improvement benchmarks maintain gains
- ✅ Code remains simple and maintainable
- ✅ Test suite passes

---

## Task 2: Add Single-Batch vs Multi-Batch Benchmark Coverage (Medium Priority) 🟡

### Problem Statement
Current benchmarks don't explicitly distinguish single-batch from multi-batch scenarios, making it hard to catch regressions like this in the future.

### Proposed Addition

**File**: `datafusion/functions-aggregate/benches/min_max_bytes.rs`

Add benchmark pairs to explicitly test both scenarios:

```rust
/// Single batch baseline - should have minimal overhead
fn min_bytes_single_batch_small(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..BATCH_SIZE).map(|i| format!("value_{:04}", i)),
    ));
    let group_indices: Vec<usize> = (0..BATCH_SIZE).collect();

    c.bench_function("min bytes single batch 512", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            black_box(
                accumulator
                    .update_batch(
                        std::slice::from_ref(&values),
                        &group_indices,
                        None,
                        BATCH_SIZE,
                    )
                    .expect("update batch"),
            );
        })
    });
}

/// Single batch with large N - tests allocation overhead scaling
fn min_bytes_single_batch_large(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..LARGE_DENSE_GROUPS).map(|i| format!("value_{:04}", i)),
    ));
    let group_indices: Vec<usize> = (0..LARGE_DENSE_GROUPS).collect();

    c.bench_function("min bytes single batch 16k", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            black_box(
                accumulator
                    .update_batch(
                        std::slice::from_ref(&values),
                        &group_indices,
                        None,
                        LARGE_DENSE_GROUPS,
                    )
                    .expect("update batch"),
            );
        })
    });
}

/// Multi-batch with large N - tests mark reuse benefit at scale
fn min_bytes_multi_batch_large(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..LARGE_DENSE_GROUPS).map(|i| format!("value_{:04}", i)),
    ));
    let group_indices: Vec<usize> = (0..LARGE_DENSE_GROUPS).collect();

    c.bench_function("min bytes multi batch 16k", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            for _ in 0..MONOTONIC_BATCHES {
                black_box(
                    accumulator
                        .update_batch(
                            std::slice::from_ref(&values),
                            &group_indices,
                            None,
                            LARGE_DENSE_GROUPS,
                        )
                        .expect("update batch"),
                );
            }
        })
    });
}
```

Add to `criterion_group!` macro at end of file.

### Success Criteria
- ✅ Benchmarks compile and run
- ✅ Single-batch benchmarks show minimal overhead (< 2%)
- ✅ Multi-batch benchmarks show clear per-batch advantage (> 10% faster)

---

## Task 3: Document Performance Characteristics (Low Priority) 🟢

### Problem Statement
The mode selection logic and performance trade-offs are not well-documented for future maintainers.

### Proposed Addition

**File**: `datafusion/functions-aggregate/src/min_max/min_max_bytes.rs`

**Location**: Above `WorkloadMode` enum (around line 35)

Add comprehensive documentation explaining:
- When each mode is used
- Performance characteristics of each
- Why thresholds are set at current values
- The reuse assumption and its implications
- Mode transition flowchart

See `minmax_denseinline_fix_root_cause_analysis.md` for template.

### Success Criteria
- ✅ Documentation is clear and accurate
- ✅ Future maintainers can understand design decisions
- ✅ Threshold modifications are data-driven

---

## Task 4: Optional - Incremental Mark Allocation (Future Enhancement) 🔵

### Problem Statement
Even on second batch, allocating full `total_num_groups` marks can be wasteful if only a small subset of groups are actually touched.

### Proposed Enhancement
Grow marks vector incrementally based on actual usage (2× growth strategy).

**This is OPTIONAL** - Only implement if Task 1 proves insufficient.

### Success Criteria
- ✅ Reduces memory footprint for large-N scenarios
- ✅ No performance regression for small-N scenarios
- ✅ Complexity justified by measurable improvement

---

## Execution Plan

### Priority Order

1. **Task 1** (High Priority) - Implement lazy mark allocation
   - Estimated time: 30 minutes
   - Impact: Eliminates both regressions

2. **Task 2** (Medium Priority) - Add benchmarks
   - Estimated time: 1 hour
   - Impact: Prevents future regressions

3. **Task 3** (Low Priority) - Documentation
   - Estimated time: 30 minutes
   - Impact: Maintainability

4. **Task 4** (Optional) - Incremental allocation
   - Only if measurements show benefit

### Validation Checklist

Before merging fix:
- [ ] All existing benchmarks pass
- [ ] "dense first batch" regression < 1%
- [ ] "large dense groups" regression < 1%
- [ ] Multi-batch improvements maintained
- [ ] Unit tests pass
- [ ] SQLLogicTest suite passes
- [ ] Documentation updated
- [ ] New benchmarks added

---

## Summary

**Root Cause**: Premature mark allocation on first batch

**Fix**: Defer allocation until second batch (one-line change)

**Impact**: Eliminates +4.11% and +2.40% regressions with zero trade-offs

**Complexity**: Minimal - simple conditional check

This fix restores optimal performance for single-batch scenarios while maintaining all multi-batch improvements from the DenseInline optimization.
