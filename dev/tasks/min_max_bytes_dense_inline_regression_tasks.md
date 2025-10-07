# Min/Max Bytes Dense Inline Regression - Remediation Tasks

## Overview

This document provides actionable tasks to eliminate the 1-20% regressions in dense workload benchmarks while preserving the 29-40% improvements in sparse/monotonic workloads. The root cause analysis is in `min_max_bytes_dense_inline_regression_root_cause.md`.

---

## Task 1: Add Commit-Once Fast Path for Stable DenseInline Workloads

**Goal**: Stop tracking statistics after the mode has stabilized in `DenseInline`.

**Rationale**: 
- Once `workload_mode == DenseInline`, the mode-switching checks in `record_batch_stats` almost never trigger for stable workloads
- For the regressed benchmarks, statistics are collected on every batch but never cause a mode change
- We can detect stability after N batches and disable tracking permanently

### Implementation

1. **Add stability tracking fields to `MinMaxBytesState`**:
   ```rust
   /// Number of consecutive batches the accumulator has remained in DenseInline mode
   /// without switching. Once this exceeds a threshold, we can disable statistics
   /// tracking to eliminate overhead.
   dense_inline_stable_batches: usize,
   /// Whether statistics tracking has been disabled due to stable mode.
   dense_inline_committed: bool,
   ```

2. **Add fast-path method for committed DenseInline mode**:
   ```rust
   /// Fast path for DenseInline mode when we know the mode won't switch.
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
       
       // No epoch, no marks, no statistics—just do the work
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

3. **Route to committed path in `update_batch`**:
   ```rust
   fn update_batch<'a, F, I>(...) -> Result<()> {
       let mut cmp = cmp;
       match self.workload_mode {
           WorkloadMode::DenseInline => {
               if self.dense_inline_committed {
                   // Fast path: no statistics tracking
                   self.update_batch_dense_inline_committed(
                       iter, group_indices, total_num_groups, &mut cmp
                   )?;
                   Ok(())
               } else {
                   // Still gathering stats for stability check
                   let stats = self.update_batch_dense_inline_impl(
                       iter, group_indices, total_num_groups, &mut cmp
                   )?;
                   self.record_batch_stats(stats, total_num_groups);
                   Ok(())
               }
           }
           // ... other modes ...
       }
   }
   ```

4. **Update `record_batch_stats` to detect stability**:
   ```rust
   const DENSE_INLINE_STABILITY_THRESHOLD: usize = 3;
   
   fn record_batch_stats(&mut self, stats: BatchStats, total_num_groups: usize) {
       // ... existing logic ...
       
       match self.workload_mode {
           WorkloadMode::DenseInline => {
               if self.should_switch_to_sparse() {
                   self.enter_sparse_mode();
                   self.workload_mode = WorkloadMode::SparseOptimized;
                   self.dense_inline_stable_batches = 0;
               } else if let Some(max_group_index) = stats.max_group_index {
                   let domain = max_group_index + 1;
                   if !self.should_use_dense_inline(total_num_groups, stats.unique_groups) {
                       // Downgrade to Simple or Sparse
                       self.dense_inline_stable_batches = 0;
                       // ... existing transition logic ...
                   } else {
                       // Stayed in DenseInline—increment stability counter
                       self.dense_inline_stable_batches += 1;
                       if self.dense_inline_stable_batches >= DENSE_INLINE_STABILITY_THRESHOLD {
                           self.dense_inline_committed = true;
                           // Free unused tracking structures
                           self.dense_inline_marks = vec![];
                       }
                   }
               }
           }
           // ... other modes ...
       }
   }
   ```

### Expected Impact
- **dense duplicate groups**: +20.02% → ~0% (eliminates 16K mark checks)
- **dense reused accumulator**: +1.17% → ~0% (eliminates 16K mark writes after 3 batches)
- **dense first batch**: +1.60% → minimal change (single batch completes before commitment)

### Testing
- Add test case that processes 10 batches and verifies `dense_inline_committed == true` after batch 3
- Verify `dense_inline_marks` is freed after commitment
- Benchmark shows no regression vs. original sparse-optimized code

---

## Task 2: Optimize Dense Duplicate Groups with Run-Length Detection

**Goal**: Eliminate mark-checking overhead when the same group appears multiple times consecutively.

**Rationale**:
- The "dense duplicate groups" benchmark has pattern `[0,0,1,1,2,2,...]`
- Current fast-path detection fails immediately (expects `group_index == fast_last + 1`)
- We can detect consecutive duplicates and skip mark checks for repeated groups

### Implementation

1. **Add run-length fast path before mark check**:
   ```rust
   fn update_batch_dense_inline_impl<'a, F, I>(...) -> Result<BatchStats> {
       // ... existing setup ...
       
       let mut last_group_index: Option<usize> = None;
       
       for (group_index, new_val) in group_indices.iter().copied().zip(iter.into_iter()) {
           let Some(new_val) = new_val else {
               continue;
           };
           
           // Fast path: if this is the same group as the previous row, skip mark tracking
           let is_duplicate = last_group_index == Some(group_index);
           last_group_index = Some(group_index);
           
           if !is_duplicate {
               // ... existing fast_path logic ...
               
               if !fast_path {
                   let mark = &mut self.dense_inline_marks[group_index];
                   if *mark != self.dense_inline_epoch {
                       *mark = self.dense_inline_epoch;
                       unique_groups = unique_groups.saturating_add(1);
                       max_group_index = Some(...);
                   }
               }
           }
           
           // Actual min/max comparison (happens even for duplicates)
           let should_replace = match self.min_max[group_index].as_ref() {
               Some(existing_val) => cmp(new_val, existing_val.as_ref()),
               None => true,
           };
           if should_replace {
               self.set_value(group_index, new_val);
           }
       }
       
       // ... existing finalization ...
   }
   ```

### Expected Impact
- **dense duplicate groups**: +20.02% → ~+5% (eliminates 256 of 512 mark checks per batch)
- Combined with Task 1: ~0% regression

### Testing
- Add dedicated test for duplicate group pattern
- Verify correct min/max results when duplicates have different values
- Benchmark shows improvement on duplicate-heavy workloads

---

## Task 3: Defer Mark Allocation Until Second Batch

**Goal**: Eliminate cold-start overhead for single-batch workloads.

**Rationale**:
- "dense first batch" benchmark runs a single batch on a fresh accumulator each iteration
- Allocating `dense_inline_marks` upfront wastes memory and time
- We can defer allocation until the second batch (when we actually need to distinguish "seen this batch" from "seen previously")

### Implementation

1. **Add lazy initialization flag**:
   ```rust
   /// Whether dense_inline_marks has been allocated. Deferred until we process
   /// a second batch to avoid cold-start overhead.
   dense_inline_marks_initialized: bool,
   ```

2. **Modify `update_batch_dense_inline_impl` to skip marks on first batch**:
   ```rust
   fn update_batch_dense_inline_impl<'a, F, I>(...) -> Result<BatchStats> {
       self.min_max.resize(total_num_groups, None);
       
       let is_first_batch = !self.dense_inline_marks_initialized;
       
       if is_first_batch {
           // First batch: just do the work, don't track marks
           let mut unique_groups = 0;
           let mut max_group_index: Option<usize> = None;
           let mut last_group: Option<usize> = None;
           
           for (group_index, new_val) in group_indices.iter().copied().zip(iter.into_iter()) {
               let Some(new_val) = new_val else {
                   continue;
               };
               
               // Count unique groups without marks
               if last_group != Some(group_index) {
                   unique_groups += 1;
                   max_group_index = Some(match max_group_index {
                       Some(current_max) => current_max.max(group_index),
                       None => group_index,
                   });
                   last_group = Some(group_index);
               }
               
               let should_replace = match self.min_max[group_index].as_ref() {
                   Some(existing_val) => cmp(new_val, existing_val.as_ref()),
                   None => true,
               };
               if should_replace {
                   self.set_value(group_index, new_val);
               }
           }
           
           // Mark as initialized for next batch
           self.dense_inline_marks_initialized = true;
           
           return Ok(BatchStats { unique_groups, max_group_index });
       }
       
       // Second+ batch: use full mark tracking
       if self.dense_inline_marks.len() < total_num_groups {
           self.dense_inline_marks.resize(total_num_groups, 0_u64);
       }
       
       // ... existing mark-based logic ...
   }
   ```

### Expected Impact
- **dense first batch**: +1.60% → ~0% (eliminates mark allocation + epoch + batch write)
- **dense reused accumulator**: Unchanged (already amortized over 32 batches)

### Testing
- Verify `dense_inline_marks` remains empty after single-batch workload
- Verify correct behavior when second batch arrives
- Benchmark shows no regression on single-batch and multi-batch cases

---

## Task 4: Batch-Write Marks Only for Sequential Fast Path

**Goal**: Reduce mark writes when fast path triggers.

**Rationale**:
- Current fast path writes marks for **every index** in the sequential range
- For sequential access, we only need marks if we're going to check them again
- If the mode is stable (Task 1), we don't need marks at all
- If the mode is still evaluating, we can defer mark writes until we fall off fast path or batch ends

### Implementation

1. **Track fast-path range but defer mark writes**:
   ```rust
   fn update_batch_dense_inline_impl<'a, F, I>(...) -> Result<BatchStats> {
       // ... existing setup ...
       
       let mut fast_path_ranges: Vec<(usize, usize)> = vec![];
       
       for (group_index, new_val) in ... {
           if fast_path {
               // ... existing fast path detection ...
               
               // Don't write marks inline—just track the range
           }
           
           if !fast_path {
               // Fell off fast path—batch-write accumulated range if any
               if fast_rows > 0 {
                   fast_path_ranges.push((fast_start, fast_last));
                   fast_rows = 0;
               }
               
               // ... existing individual mark logic ...
           }
           
           // ... min/max logic ...
       }
       
       // At end of batch, write all fast-path ranges at once
       if fast_path && fast_rows > 0 {
           fast_path_ranges.push((fast_start, fast_last));
       }
       
       let epoch = self.dense_inline_epoch;
       for (start, end) in fast_path_ranges {
           for idx in start..=end {
               self.dense_inline_marks[idx] = epoch;
           }
       }
       
       // ... existing return ...
   }
   ```

2. **Alternative: Skip mark writes entirely if not needed**:
   ```rust
   // If we completed the batch on fast path and it's not the first few batches,
   // we can skip mark writes entirely—just return the stats
   if fast_path && self.dense_inline_stable_batches < DENSE_INLINE_STABILITY_THRESHOLD {
       // Write marks for mode evaluation
       // ...
   }
   // else: no mark writes needed
   ```

### Expected Impact
- **dense reused accumulator**: Minor improvement (defers 512 writes until batch end)
- Combined with Task 1: Eliminates mark writes entirely after commitment

### Testing
- Verify correctness when fast path is maintained across entire batch
- Verify correctness when falling off fast path mid-batch
- Benchmark shows no regression

---

## Task 5: Add Regression Guard Benchmarks

**Goal**: Prevent future regressions by adding benchmark coverage for critical patterns.

### Implementation

1. **Add benchmark for single-batch, sequential, cold-start**:
   ```rust
   fn min_bytes_cold_start_sequential(c: &mut Criterion) {
       let values: ArrayRef = Arc::new(StringArray::from_iter_values(
           (0..BATCH_SIZE).map(|i| format!("value_{:04}", i)),
       ));
       let group_indices: Vec<usize> = (0..BATCH_SIZE).collect();
       
       c.bench_function("min bytes cold start sequential", |b| {
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
   ```

2. **Add benchmark for consecutive duplicates**:
   ```rust
   fn min_bytes_consecutive_duplicates(c: &mut Criterion) {
       // Pattern: [0,0,0,1,1,1,2,2,2,...] (3x repetition)
       let unique_groups = BATCH_SIZE / 3;
       let values: ArrayRef = Arc::new(StringArray::from_iter_values(
           (0..BATCH_SIZE).map(|i| format!("value_{:04}", i / 3)),
       ));
       let group_indices: Vec<usize> = (0..unique_groups)
           .flat_map(|i| [i, i, i])
           .collect();
       
       c.bench_function("min bytes consecutive duplicates", |b| {
           b.iter(|| {
               let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
               for _ in 0..MONOTONIC_BATCHES {
                   black_box(
                       accumulator
                           .update_batch(
                               std::slice::from_ref(&values),
                               &group_indices,
                               None,
                               unique_groups,
                           )
                           .expect("update batch"),
                   );
               }
           })
       });
   }
   ```

3. **Add benchmark for stable DenseInline mode**:
   ```rust
   fn min_bytes_stable_dense_inline(c: &mut Criterion) {
       let values: ArrayRef = Arc::new(StringArray::from_iter_values(
           (0..BATCH_SIZE).map(|i| format!("value_{:04}", i)),
       ));
       let group_indices: Vec<usize> = (0..BATCH_SIZE).collect();
       
       c.bench_function("min bytes stable dense inline", |b| {
           b.iter(|| {
               let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
               // Process enough batches to trigger commitment
               for _ in 0..10 {
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
               }
           })
       });
   }
   ```

### Expected Impact
- Catches future regressions in cold-start, duplicate, and stable-mode scenarios
- Provides baseline for validating Task 1-4 improvements

---

## Task 6: Profile and Validate

**Goal**: Confirm the optimizations eliminate regressions and don't introduce new issues.

### Steps

1. **Run benchmarks before changes**:
   ```bash
   cargo bench --bench min_max_bytes -- --save-baseline before
   ```

2. **Implement Task 1** (commit-once fast path):
   ```bash
   cargo bench --bench min_max_bytes -- --baseline before
   # Verify "dense reused accumulator" and "dense duplicate groups" improve
   ```

3. **Implement Task 2** (run-length detection):
   ```bash
   cargo bench --bench min_max_bytes -- --baseline before
   # Verify "dense duplicate groups" improves further
   ```

4. **Implement Task 3** (defer mark allocation):
   ```bash
   cargo bench --bench min_max_bytes -- --baseline before
   # Verify "dense first batch" improves
   ```

5. **Implement Task 4** (batch-write optimization):
   ```bash
   cargo bench --bench min_max_bytes -- --baseline before
   # Verify no regressions, possible small improvements
   ```

6. **Add Task 5 benchmarks and validate**:
   ```bash
   cargo bench --bench min_max_bytes
   # Verify all benchmarks show 0-5% overhead or better
   ```

7. **Profile with `cargo flamegraph`**:
   ```bash
   cargo bench --bench min_max_bytes --profile profiling -- --profile-time 60
   # Examine flamegraph for remaining hotspots
   ```

### Success Criteria
- **dense duplicate groups**: +20.02% → ≤ +2%
- **dense first batch**: +1.60% → ≤ +0.5%
- **dense reused accumulator**: +1.17% → ≤ +0.5%
- **sparse groups**: -28.97% maintained
- **monotonic group ids**: -40.15% maintained
- All new benchmarks show acceptable performance

---

## Implementation Priority

**Phase 1: Critical fixes** (eliminate major regressions)
1. Task 1 (commit-once fast path) — eliminates 15-20% of regression
2. Task 2 (run-length detection) — eliminates another 5-10% for duplicates

**Phase 2: Polish** (eliminate remaining overhead)
3. Task 3 (defer mark allocation) — fixes cold-start overhead
4. Task 4 (batch-write optimization) — minor improvements

**Phase 3: Prevention** (guard against future regressions)
5. Task 5 (regression guard benchmarks)
6. Task 6 (profile and validate)

---

## Alternative: Revert to Simple Mode for These Patterns

If the above optimizations prove complex or insufficient, consider routing these patterns to the `Simple` mode instead of `DenseInline`:

1. **Detect duplicate-heavy workloads**: If `unique_groups * 2 < group_indices.len()` on first batch, use `Simple` mode
2. **Use Simple for small dense workloads**: If `total_num_groups < 1000`, prefer `Simple` over `DenseInline`
3. **Hybrid approach**: Use `DenseInline` only for monotonic or very sparse workloads

This would preserve the 29-40% improvements while avoiding the 1-20% regressions, at the cost of not optimizing the "perfect" dense inline case.

---

## Notes

- All tasks should include comprehensive unit tests verifying correctness
- Use `#[cfg(test)]` fields to verify internal state transitions
- Document mode-switching decisions in code comments
- Update existing task documents with final results

---

## References

- Root cause analysis: `min_max_bytes_dense_inline_regression_root_cause.md`
- Original issue: #17897
- Benchmark code: `datafusion/functions-aggregate/benches/min_max_bytes.rs`
- Implementation: `datafusion/functions-aggregate/src/min_max/min_max_bytes.rs`
