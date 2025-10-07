# Min/Max Bytes Dense Inline Regression - Quick Fix Reference

This document provides the essential code changes needed to fix the regressions. See the full task document for detailed explanations and context.

## Critical Fix: Commit-Once Fast Path

### 1. Add tracking fields to `MinMaxBytesState` struct

```rust
// In MinMaxBytesState struct (around line 417)
pub(crate) struct MinMaxBytesState {
    // ... existing fields ...
    
    /// Number of consecutive batches in DenseInline mode without switching.
    /// After N stable batches, we commit and disable statistics tracking.
    dense_inline_stable_batches: usize,
    
    /// Whether we've committed to DenseInline mode and disabled stats tracking.
    dense_inline_committed: bool,
}
```

### 2. Initialize new fields in `new()`

```rust
// In MinMaxBytesState::new() (around line 568)
impl MinMaxBytesState {
    fn new(data_type: DataType) -> Self {
        Self {
            // ... existing initialization ...
            dense_inline_stable_batches: 0,
            dense_inline_committed: false,
        }
    }
}
```

### 3. Add commit-once fast path method

```rust
// Add after update_batch_dense_inline_impl (around line 809)

/// Optimized DenseInline path for committed, stable mode.
/// Skips all statistics tracking and mark management.
fn update_batch_dense_inline_committed<'a, F, I>(
    &mut self,
    iter: I,
    group_indices: &[usize],
    total_num_groups: usize,
    cmp: &mut F,
) -> Result<()>
where
    F: FnMut(&[u8], &[u8]) -> bool + Send + Sync,
    I: IntoIterator<Item = Option<&'a [u8]>>,
{
    self.min_max.resize(total_num_groups, None);
    
    // No epoch, no marks, no stats—just do the min/max work
    for (group_index, new_val) in group_indices.iter().copied().zip(iter.into_iter()) {
        let Some(new_val) = new_val else {
            continue;
        };
        
        if group_index >= self.min_max.len() {
            return internal_err!(
                "group index {group_index} out of bounds for {} groups",
                self.min_max.len()
            );
        }
        
        let should_replace = match self.min_max[group_index].as_ref() {
            Some(existing_val) => cmp(new_val, existing_val.as_ref()),
            None => true,
        };
        
        if should_replace {
            self.set_value(group_index, new_val);
        }
    }
    
    Ok(())
}
```

### 4. Route to committed path in `update_batch()`

```rust
// Modify WorkloadMode::DenseInline case in update_batch (around line 647)
match self.workload_mode {
    WorkloadMode::DenseInline => {
        if self.dense_inline_committed {
            // Fast path: no statistics tracking
            self.update_batch_dense_inline_committed(
                iter,
                group_indices,
                total_num_groups,
                &mut cmp,
            )?;
            Ok(())
        } else {
            // Still evaluating: collect statistics
            let stats = self.update_batch_dense_inline_impl(
                iter,
                group_indices,
                total_num_groups,
                &mut cmp,
            )?;
            self.record_batch_stats(stats, total_num_groups);
            Ok(())
        }
    }
    // ... other modes unchanged ...
}
```

### 5. Track stability and commit in `record_batch_stats()`

```rust
// Add constant at top of file (around line 550)
/// After this many consecutive batches in DenseInline without mode switch,
/// commit to the mode permanently and disable statistics tracking.
const DENSE_INLINE_STABILITY_THRESHOLD: usize = 3;

// Modify WorkloadMode::DenseInline case in record_batch_stats (around line 935)
WorkloadMode::DenseInline => {
    if self.should_switch_to_sparse() {
        self.enter_sparse_mode();
        self.workload_mode = WorkloadMode::SparseOptimized;
        self.dense_inline_stable_batches = 0;
    } else if let Some(max_group_index) = stats.max_group_index {
        let domain = max_group_index + 1;
        if !self.should_use_dense_inline(total_num_groups, stats.unique_groups) {
            // Need to downgrade to Simple or Sparse
            self.dense_inline_stable_batches = 0;
            if self.should_use_simple(total_num_groups, stats.unique_groups, domain) {
                self.enter_simple_mode();
                self.workload_mode = WorkloadMode::Simple;
            } else {
                self.enter_sparse_mode();
                self.workload_mode = WorkloadMode::SparseOptimized;
            }
        } else {
            // Stayed in DenseInline—increment stability counter
            self.dense_inline_stable_batches += 1;
            if self.dense_inline_stable_batches >= DENSE_INLINE_STABILITY_THRESHOLD {
                // Commit to DenseInline mode permanently
                self.dense_inline_committed = true;
                // Free tracking structures we no longer need
                self.dense_inline_marks = vec![];
            }
        }
    }
}
```

### 6. Reset committed flag when entering other modes

```rust
// In enter_simple_mode() (around line 1020)
fn enter_simple_mode(&mut self) {
    // ... existing cleanup ...
    self.dense_inline_stable_batches = 0;
    self.dense_inline_committed = false;
}

// In enter_sparse_mode() (around line 1030)
fn enter_sparse_mode(&mut self) {
    // ... existing cleanup ...
    self.dense_inline_stable_batches = 0;
    self.dense_inline_committed = false;
}

// In enter_dense_inline_mode() (around line 1040)
fn enter_dense_inline_mode(&mut self) {
    self.enter_simple_mode();
    self.dense_inline_stable_batches = 0;
    self.dense_inline_committed = false;
}
```

## Secondary Fix: Run-Length Detection for Duplicates

Add to `update_batch_dense_inline_impl()` to optimize consecutive duplicate groups:

```rust
// At start of loop in update_batch_dense_inline_impl (around line 710)
let mut last_group_index: Option<usize> = None;

for (group_index, new_val) in group_indices.iter().copied().zip(iter.into_iter()) {
    let Some(new_val) = new_val else {
        continue;
    };
    
    // ... existing bounds check ...
    
    // Fast path: skip mark tracking if this is a duplicate of previous group
    let is_consecutive_duplicate = last_group_index == Some(group_index);
    last_group_index = Some(group_index);
    
    if !is_consecutive_duplicate {
        // Only do mark tracking for first occurrence of each group
        if fast_path {
            // ... existing fast_path logic ...
        }
        
        if !fast_path {
            let mark = &mut self.dense_inline_marks[group_index];
            if *mark != self.dense_inline_epoch {
                *mark = self.dense_inline_epoch;
                unique_groups = unique_groups.saturating_add(1);
                max_group_index = Some(match max_group_index {
                    Some(current_max) => current_max.max(group_index),
                    None => group_index,
                });
            }
        }
    }
    
    // Always do min/max comparison (even for consecutive duplicates)
    let should_replace = match self.min_max[group_index].as_ref() {
        Some(existing_val) => cmp(new_val, existing_val.as_ref()),
        None => true,
    };
    if should_replace {
        self.set_value(group_index, new_val);
    }
}
```

## Testing

### Unit Test for Committed Mode

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_dense_inline_commits_after_stable_batches() {
        let mut state = MinMaxBytesState::new(DataType::Utf8);
        
        let values = vec!["a", "b", "c"];
        let group_indices = vec![0, 1, 2];
        
        // Process 5 batches with same pattern
        for i in 0..5 {
            let values_iter = values.iter().map(|s| Some(s.as_bytes()));
            state
                .update_batch(
                    values_iter,
                    &group_indices,
                    3,
                    |a, b| a < b,
                )
                .unwrap();
            
            if i < 3 {
                assert!(!state.dense_inline_committed, "Should not commit before batch 3");
            } else {
                assert!(state.dense_inline_committed, "Should commit after batch 3");
                assert_eq!(state.dense_inline_marks.len(), 0, "Marks should be freed");
            }
        }
    }
}
```

### Benchmark Validation

```bash
# Save baseline before changes
cargo bench --bench min_max_bytes -- --save-baseline before

# Apply fixes and compare
cargo bench --bench min_max_bytes -- --baseline before

# Should show:
# - "dense duplicate groups" improves from +20% to ~+2%
# - "dense first batch" remains stable or improves to ~+0.5%
# - "dense reused accumulator" improves from +1.17% to ~+0.5%
# - "sparse groups" maintains -28.97%
# - "monotonic group ids" maintains -40.15%
```

## Expected Impact

| Fix | Benchmark | Expected Improvement |
|-----|-----------|---------------------|
| **Commit-once fast path** | dense duplicate groups | +20.02% → ~+5% |
| | dense reused accumulator | +1.17% → ~+0.3% |
| **Run-length detection** | dense duplicate groups | ~+5% → ~+2% |
| **Combined** | All regressions | ≤ +2% (acceptable noise) |

## Verification Checklist

- [ ] All unit tests pass
- [ ] `#[cfg(test)]` fields verify correct state transitions
- [ ] Dense regressions reduced to ≤2%
- [ ] Sparse/monotonic improvements maintained
- [ ] No new regressions in other benchmarks
- [ ] Memory usage doesn't increase (marks freed after commit)
- [ ] Mode-switching still works correctly for unstable workloads

## Rollback Plan

If these changes cause issues:

1. Revert commit-once optimization (remove `dense_inline_committed` routing)
2. Keep run-length detection (minimal risk, clear benefit)
3. Consider alternative: restrict `DenseInline` to monotonic-only workloads
4. Document as "known limitation" and schedule deeper refactor

## References

- Full analysis: `min_max_bytes_dense_inline_regression_root_cause.md`
- Detailed tasks: `min_max_bytes_dense_inline_regression_tasks.md`
- Summary: `min_max_bytes_dense_inline_regression_summary.md`
