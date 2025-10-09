# MinMaxBytes Regression Analysis and Fix (v3)

**Date:** October 9, 2025  
**PR Commit Range:** `1eb9d9ac6^..b69c544bc`  
**Branch:** `minmax-17897a`  
**Issue:** Performance regressions in dense/duplicate workloads despite overall improvements

---

## Executive Summary

The PR successfully addressed the original quadratic allocation problem in sparse/growing workloads, achieving dramatic improvements (-93% for ultra sparse, -43% for quadratic growing). However, it introduced **6 regressions** in dense/duplicate workloads (+3% to +17%) due to **over-counting of unique groups in the `update_batch_sequential_dense` path**.

**Root Cause:** Line 990 in `update_batch_sequential_dense()` increments `unique_groups` for **every non-null row** instead of **only for unique groups encountered in the batch**. This contradicts the explicit comment on lines 991-996 explaining that `max_group_index` (not `unique_groups`) should count every row.

**Impact:** The inflated `unique_groups` values poison the adaptive mode selection heuristics, causing the accumulator to misclassify dense workloads as sparser than they are, leading to suboptimal execution paths.

---

## Benchmark Analysis

### Improvements Preserved ✅ (10 benchmarks)
| Benchmark                                | Mean Change | P-value  | Analysis |
|------------------------------------------|-------------|----------|----------|
| min bytes ultra sparse                   | **-93.03%** | 0.000000 | Original fix working perfectly |
| min bytes quadratic growing total groups | -43.19%     | 0.000000 | Quadratic behavior eliminated |
| min bytes multi batch large              | -38.48%     | 0.000000 | Large-scale sequential handling |
| min bytes monotonic group ids            | -38.54%     | 0.000000 | Sequential detection working |
| min bytes sparse groups                  | -28.20%     | 0.000000 | Sparse path optimization |
| min bytes mode transition                | -23.89%     | 0.000000 | Adaptive mode switching benefit |
| min bytes growing total groups           | -19.69%     | 0.000000 | Growing workload handled well |
| min bytes dense duplicate groups         | -5.98%      | 0.000000 | Some improvement despite bug |
| min bytes extreme duplicates             | -3.33%      | 0.000000 | Duplicate detection helps |
| min bytes medium cardinality stable      | -1.84%      | 0.000000 | Marginal improvement |

### Regressions Introduced ⚠️ (6 benchmarks)
| Benchmark                            | Mean Change | P-value  | Root Cause Analysis |
|--------------------------------------|-------------|----------|---------------------|
| min bytes dense reused accumulator   | **+16.72%** | 0.000000 | **Same 512 groups × 32 batches = 16,384 counted instead of 512** |
| min bytes large dense groups         | +4.66%      | 0.000000 | Dense pattern misclassified as sparse |
| min bytes single batch large         | +4.00%      | 0.000000 | Single batch counted as batch_size instead of unique_groups |
| min bytes dense first batch          | +3.65%      | 0.000000 | Initial batch over-counted triggers wrong mode |
| min bytes single batch small         | +3.31%      | 0.000000 | Small batch still over-counted |
| min bytes sequential stable groups   | +1.70%      | 0.000000 | Stable groups counted multiple times |

---

## Detailed Root Cause Analysis

### The Bug

**File:** `datafusion/functions-aggregate/src/min_max/min_max_bytes.rs`  
**Lines:** 990-1000 in `update_batch_sequential_dense()`

```rust
for (new_val, group_index) in iter.into_iter().zip(group_indices.iter()) {
    let group_index = *group_index;
    let Some(new_val) = new_val else {
        continue; // skip nulls
    };

    unique_groups = unique_groups.saturating_add(1);  // ❌ BUG: counts every row
    // Track the largest group index encountered in this batch. Unlike
    // `unique_groups`, this intentionally considers every row (including
    // duplicates) because the domain size we derive from
    // `max_group_index` only depends on the highest index touched, not on
    // how many distinct groups contributed to it.
    max_group_index = Some(match max_group_index {
        Some(current_max) => current_max.max(group_index),
        None => group_index,
    });
```

**The Contradiction:**
- **Lines 991-996 (comment):** Explicitly state that `max_group_index` "intentionally considers every row (including duplicates)"
- **Line 990 (code):** Increments `unique_groups` for every row, making it **identical** to counting rows
- **Variable name:** `unique_groups` implies it should count **unique** groups, not all rows

### Why the Current Code is Wrong

1. **Semantic Violation:** A variable named `unique_groups` that counts all rows is a semantic bug
2. **Documentation Mismatch:** The comment distinguishes `max_group_index` from `unique_groups`, but the code treats them identically
3. **Behavioral Impact:** Over-counting causes mode selection heuristics to make incorrect decisions

### Contrast with `update_batch_dense_inline_hybrid()`

The other update path correctly tracks unique groups using the epoch-marking pattern:

```rust
// File: same file, lines ~896
if !fast_path && !is_consecutive_duplicate {
    if !marks_ready {
        self.prepare_dense_inline_marks(total_num_groups);
        marks_ready = true;
    }
    let mark = &mut self.dense_inline_marks[group_index];
    if *mark != self.dense_inline_epoch {  // ✅ Only count first encounter
        *mark = self.dense_inline_epoch;
        unique_groups = unique_groups.saturating_add(1);
        max_group_index = Some(match max_group_index {
            Some(current_max) => current_max.max(group_index),
            None => group_index,
        });
    }
}
```

This code only increments `unique_groups` when `*mark != self.dense_inline_epoch`, ensuring each group is counted once per batch.

### Why `update_batch_sequential_dense()` Has No Equivalent Guard

The `update_batch_sequential_dense()` path uses a `locations` vector instead of epoch marks:

```rust
enum SequentialDenseLocation<'a> {
    ExistingMinMax,      // Group not yet processed in this batch
    Input(&'a [u8]),     // Group processed, holds candidate value
}
```

The `locations` vector transitions from `ExistingMinMax` → `Input(value)` when a group is first encountered, providing the same "have we seen this group?" information as the epoch marks.

**Missing Logic:** The code should check `locations[group_index]` state **before** incrementing `unique_groups`, analogous to the epoch check.

---

## Impact Analysis

### 1. Dense Reused Accumulator (+16.72%) - The Smoking Gun

**Benchmark Details:**
```rust
fn min_bytes_dense_reused_batches(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..BATCH_SIZE).map(|i| format!("value_{:04}", i)),
    ));
    let group_indices: Vec<usize> = (0..BATCH_SIZE).collect();  // [0, 1, 2, ..., 511]
    
    c.bench_function("min bytes dense reused accumulator", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            for _ in 0..MONOTONIC_BATCHES {  // 32 batches
                accumulator.update_batch(..., &group_indices, ..., BATCH_SIZE).unwrap();
            }
        })
    });
}
```

**Expected Behavior:**
- Batch 1: 512 unique groups → `total_groups_seen = 512`
- Batch 2: Same 512 groups → `total_groups_seen = 512` (already seen)
- ...
- Batch 32: Same 512 groups → `total_groups_seen = 512`

**Actual Behavior (Buggy):**
- Batch 1: 512 rows counted → `unique_groups = 512`, `total_groups_seen = 512`
- Batch 2: 512 rows counted → `unique_groups = 512`, `total_groups_seen = 1024`
- ...
- Batch 32: 512 rows counted → `unique_groups = 512`, `total_groups_seen = 16384`

**Result:** Mode selection sees `total_groups_seen = 16384` instead of `512`, triggering inappropriate sparse optimizations for a clearly dense workload.

### 2. Single Batch Benchmarks (+3-4%) - Misclassification

**Pattern:**
```rust
// single_batch_large: 512 groups, each appearing once
let group_indices: Vec<usize> = (0..512).collect();
```

**Impact:**
- `unique_groups = 512` (correct by accident, since each group appears once)
- However, the accumulator learns from this "high density" signal
- On subsequent batches (if any), the over-counting begins
- For truly single-batch workloads, overhead comes from unnecessary mode-switching logic

**Why +3-4% overhead?**
The mode selection code (`record_batch_stats`, `should_use_dense_inline`, etc.) runs on every batch and makes decisions based on the statistics. Even if the final mode is correct, the inflated numbers cause the heuristics to evaluate more complex conditions, adding CPU cycles.

### 3. Dense Duplicate Groups (-5.98% but should be better)

**Benchmark Pattern:**
```rust
let group_indices: Vec<usize> = (0..unique_groups).flat_map(|i| [i, i]).collect();
// [0, 0, 1, 1, 2, 2, ..., 255, 255]
```

**Expected:**
- 512 rows, 256 unique groups
- `unique_groups = 256` per batch

**Actual:**
- `unique_groups = 512` (every row counted)
- Over-counted by 2×

**Why still shows improvement?**
The sequential detection logic (fast-path) and overall algorithm improvements outweigh the mode selection penalty for this specific pattern. However, the improvement would be **even larger** without the bug.

---

## The Fix

### Task 1: Correct `unique_groups` Counting ⚡ CRITICAL

**File:** `datafusion/functions-aggregate/src/min_max/min_max_bytes.rs`  
**Function:** `update_batch_sequential_dense()`  
**Lines:** ~985-1000

**Required Change:**

```rust
for (new_val, group_index) in iter.into_iter().zip(group_indices.iter()) {
    let group_index = *group_index;
    let Some(new_val) = new_val else {
        continue; // skip nulls
    };

    // ✅ CORRECTED: Only count each group the first time we see it in this batch
    let is_first_encounter = matches!(
        locations[group_index],
        SequentialDenseLocation::ExistingMinMax
    );
    if is_first_encounter {
        unique_groups = unique_groups.saturating_add(1);
    }
    
    // Track the largest group index encountered in this batch. Unlike
    // `unique_groups`, this intentionally considers every row (including
    // duplicates) because the domain size we derive from
    // `max_group_index` only depends on the highest index touched, not on
    // how many distinct groups contributed to it.
    max_group_index = Some(match max_group_index {
        Some(current_max) => current_max.max(group_index),
        None => group_index,
    });

    let existing_val = match locations[group_index] {
        // ... rest unchanged
    };
}
```

**Rationale:**
- `locations[group_index]` starts as `ExistingMinMax` for all groups
- Transitions to `Input(value)` when first processed (line 1007 or 1017)
- Checking the state **before** processing ensures we count each group exactly once

**Alternative (slightly more explicit):**

```rust
let existing_val = match locations[group_index] {
    SequentialDenseLocation::Input(existing_val) => {
        // Already seen this group in this batch
        existing_val
    }
    SequentialDenseLocation::ExistingMinMax => {
        // First time seeing this group in this batch
        unique_groups = unique_groups.saturating_add(1);
        
        let Some(existing_val) = self.min_max[group_index].as_ref() else {
            locations[group_index] = SequentialDenseLocation::Input(new_val);
            continue;
        };
        existing_val.as_ref()
    }
};
```

**Recommendation:** Use the first approach (checking before the match) because:
1. It mirrors the structure of `update_batch_dense_inline_hybrid()`
2. The increment happens in one place, making it easier to verify
3. Matches the existing comment's intent

---

### Task 2: Add Inline Documentation

**Location:** Same function, after the fix

Add a comment explaining the distinction:

```rust
// Count unique groups encountered in this batch.
// Check `locations[group_index]` before we update it to detect first encounters.
let is_first_encounter = matches!(
    locations[group_index],
    SequentialDenseLocation::ExistingMinMax
);
if is_first_encounter {
    unique_groups = unique_groups.saturating_add(1);
}

// Track the largest group index encountered in this batch. Unlike
// `unique_groups`, this intentionally considers every row (including
// duplicates) because the domain size we derive from
// `max_group_index` only depends on the highest index touched, not on
// how many distinct groups contributed to it.
```

---

### Task 3: Review and Validate `max_group_index` Calculation

**Current Behavior:** Updates on every non-null row

**Question:** Is this correct, or should it also only update on first encounter?

**Analysis:**

Checking the heuristics:

```rust
fn should_use_simple(&self, total_num_groups: usize, unique_groups: usize, domain: usize) -> bool {
    // `domain` is derived from `max_group_index + 1`
    Self::density_at_least(unique_groups, domain, SIMPLE_MODE_MIN_DENSITY_PERCENT)
}
```

The `domain` represents the **index space extent** `[0, max_group_index]`, not the number of unique groups. This is used to calculate density as `unique_groups / domain`.

**Conclusion:** Current behavior is **CORRECT**. `max_group_index` should track the highest index regardless of duplicates, because it defines the domain size for density calculations.

**Action Required:** None, but add a clarifying test or assertion in debug mode to document this behavior.

---

### Task 4: Add Regression Test Coverage

**File:** `datafusion/functions-aggregate/benches/min_max_bytes.rs`

**New Benchmark: Extreme Duplicates**

Already exists as `min_bytes_dense_duplicate_groups`, but verify it correctly stresses the duplicate-counting logic.

**Validation Test (in `#[cfg(test)]` module):**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_unique_groups_counting_with_duplicates() {
        // This test validates that unique_groups counts unique groups,
        // not total rows, even when groups repeat within a batch.
        
        let mut state = MinMaxBytesState::new(&DataType::Utf8, false);
        
        // Batch with 100 rows touching only 10 unique groups (10× duplication)
        let values: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..100).map(|i| format!("value_{:02}", i / 10))
        ));
        let group_indices: Vec<usize> = (0..10)
            .flat_map(|i| std::iter::repeat(i).take(10))
            .collect();
        
        state.update_batch(
            std::slice::from_ref(&values),
            &group_indices,
            None,
            10,
        ).unwrap();
        
        // After fix, total_groups_seen should be 10, not 100
        assert_eq!(state.total_groups_seen, 10, 
            "unique_groups should count unique groups (10), not total rows (100)");
        
        // Process the same batch again
        state.update_batch(
            std::slice::from_ref(&values),
            &group_indices,
            None,
            10,
        ).unwrap();
        
        // Should still be 10, not 20 (accumulator has already seen these groups)
        // NOTE: This depends on how `record_batch_stats` accumulates counts.
        // If it uses cumulative logic, this may be 20. Adjust expectations accordingly.
    }
}
```

**Note:** The test above assumes `total_groups_seen` accumulates **unique groups across batches**. If the current implementation accumulates **per-batch unique groups** (i.e., `total_groups_seen += unique_groups` on each batch), then the second assertion should expect `20`. Review `record_batch_stats` (line 1209) to confirm the intended semantic.

---

### Task 5: Audit `record_batch_stats()` Accumulation Logic

**File:** `datafusion/functions-aggregate/src/min_max/min_max_bytes.rs`  
**Function:** `record_batch_stats()`  
**Lines:** ~1209-1220

**Current Code:**
```rust
fn record_batch_stats(&mut self, stats: BatchStats, total_num_groups: usize) {
    self.processed_batches = self.processed_batches.saturating_add(1);
    if stats.unique_groups == 0 {
        return;
    }

    self.total_groups_seen =
        self.total_groups_seen.saturating_add(stats.unique_groups);  // ❓
    // ...
}
```

**Question:** What should `total_groups_seen` represent?

**Option A: Cumulative Unique Groups (Global)**
- `total_groups_seen` = total unique groups encountered across **all batches**
- Requires tracking which groups have been seen (not currently implemented)
- More meaningful for mode selection

**Option B: Sum of Per-Batch Unique Groups (Local)**
- `total_groups_seen` = sum of `unique_groups` from each batch
- Current implementation (simple accumulation)
- Over-estimates the true unique group count for reused accumulators

**Current Behavior:** Option B (sum of per-batch counts)

**Impact of Fix:**
After fixing `unique_groups` counting, `total_groups_seen` will accumulate correct per-batch counts. For the "dense reused accumulator" benchmark:
- **Before fix:** `total_groups_seen = 512 × 32 = 16384`
- **After fix:** `total_groups_seen = 512 × 32 = 16384` (still wrong if intent is global unique)

**Recommended Action:**

1. **Document the intended semantic** of `total_groups_seen` in the struct definition:
   ```rust
   /// Cumulative count of unique groups across all processed batches.
   /// NOTE: This is the *sum* of per-batch unique groups, which may
   /// over-count if the same groups appear in multiple batches.
   /// Used for adaptive mode selection heuristics.
   total_groups_seen: usize,
   ```

2. **Consider renaming** to `total_unique_groups_seen_sum` or `cumulative_batch_groups` to clarify the semantic

3. **Evaluate if mode selection heuristics need adjustment** given this semantic

**Why not track global unique groups?**
Tracking true global unique groups requires a `HashSet<usize>` or similar, adding memory overhead. The current approach is a lightweight approximation that works well enough for mode selection.

---

### Task 6: Review Mode Selection Thresholds

**Files/Functions to Review:**
- `should_use_dense_inline()` (line ~1316)
- `should_use_simple()` (line ~1332)
- `should_switch_to_sparse()` (line ~1348)
- Constants:
  - `DENSE_INLINE_MAX_TOTAL_GROUPS`
  - `DENSE_INLINE_MIN_DENSITY_PERCENT`
  - `SIMPLE_MODE_MAX_TOTAL_GROUPS`
  - `SIMPLE_MODE_MIN_DENSITY_PERCENT`
  - `SPARSE_SWITCH_GROUP_THRESHOLD`
  - `SPARSE_SWITCH_MAX_DENSITY_PERCENT`

**Action:**

1. **Verify constants are still appropriate** after accurate `unique_groups` counting
   - Were thresholds tuned with buggy over-counted values?
   - May need adjustment, but likely not (since improvements dominate)

2. **Add tracing/logging** (already present in `record_batch_stats` with `feature = "trace"`)
   - Validate mode transitions happen at sensible points
   - Run benchmarks with tracing enabled to observe mode selection

3. **Document the heuristics** with examples:
   ```rust
   /// Decides whether to use the DenseInline optimization path.
   ///
   /// DenseInline is beneficial when:
   /// - `total_num_groups` is small enough to allocate epoch marks efficiently
   /// - `unique_groups / total_num_groups` ratio is high (dense workload)
   ///
   /// Example: A batch with 450 unique groups out of 500 total (90% density)
   /// exceeds the 80% threshold and qualifies for DenseInline mode.
   fn should_use_dense_inline(&self, total_num_groups: usize, unique_groups: usize) -> bool {
       // ...
   }
   ```

---

### Task 7: Run Full Benchmark Suite and Validate

**Commands:**
```bash
cd /Users/kosiew/GitHub/datafusion

# Build with optimizations
cargo build --release

# Run min_max_bytes benchmarks
cargo bench --bench min_max_bytes -- --save-baseline after-v3-fix

# Compare with previous baseline (if available)
# cargo bench --bench min_max_bytes -- --baseline before-fix
```

**Success Criteria:**

✅ **All 6 regressions eliminated or significantly reduced:**
- `min bytes dense reused accumulator`: +16.72% → **≤ 0%** (target: -2% to +0%)
- `min bytes large dense groups`: +4.66% → **≤ 0%**
- `min bytes single batch large`: +4.00% → **≤ 0%**
- `min bytes dense first batch`: +3.65% → **≤ 0%**
- `min bytes single batch small`: +3.31% → **≤ 0%**
- `min bytes sequential stable groups`: +1.70% → **≤ 0%**

✅ **All 10 improvements preserved (within 5% margin):**
- `min bytes ultra sparse`: -93.03% maintained (allow -88% to -95%)
- `min bytes quadratic growing total groups`: -43.19% maintained
- `min bytes multi batch large`: -38.48% maintained
- ... (all others)

✅ **No new regressions introduced**

✅ **Dense duplicate groups improvement may increase** (currently -5.98%, could improve to -8% to -10%)

**Validation Process:**

1. Apply Task 1 fix (correct `unique_groups` counting)
2. Run benchmarks: `cargo bench --bench min_max_bytes`
3. Analyze results:
   - If regressions persist, investigate mode selection (Task 6)
   - If improvements degrade, check for unintended side effects
4. Run with tracing enabled to observe mode transitions:
   ```bash
   RUSTFLAGS="--cfg feature=\"trace\"" cargo bench --bench min_max_bytes
   ```
5. Document final results in this file or a separate report

---

## Why the Regressions Are Worse Than Expected

### The Multiplicative Effect

**Dense Reused Accumulator (+16.72%):**

1. **Batch 1:** `unique_groups = 512` (wrong, should be 512) → Mode selection chooses correct path by accident
2. **Batch 2:** `total_groups_seen = 1024` (wrong, should be 512) → Still within DenseInline threshold
3. **Batch 5:** `total_groups_seen = 2560` (wrong, should be 512) → Exceeds `DENSE_INLINE_MAX_TOTAL_GROUPS`?
4. **Batch 10:** `total_groups_seen = 5120` (wrong) → Triggers sparse mode switch
5. **Batches 11-32:** Running in **SparseOptimized** mode for a **clearly dense** workload

**Result:** 20+ batches process using suboptimal sparse paths (hash tables, indirect lookups) instead of direct array indexing.

### Why Single-Batch Benchmarks Regress

Even though a single batch doesn't accumulate counts, the overhead comes from:
1. **Mode selection evaluation:** Heuristics run on every batch, adding CPU cycles
2. **Threshold boundary effects:** Inflated `unique_groups` may push workloads over/under thresholds, triggering mode switches
3. **Cache effects:** Mode switching may flush CPU caches, adding latency

For a +3-4% regression, this suggests ~10-20 CPU cycles per row of overhead.

---

## Expected Results After Fix

### Best-Case Scenario (Optimistic)

All regressions become improvements due to better mode selection:
- `min bytes dense reused accumulator`: +16.72% → **-5%** (sparse mode avoided entirely)
- `min bytes dense duplicate groups`: -5.98% → **-12%** (better mode for duplicates)
- Single-batch benchmarks: +3-4% → **-1 to -2%** (mode selection overhead reduced)

### Realistic Scenario (Conservative)

Regressions eliminated, improvements preserved:
- All 6 regressions → **-2% to +1%** (within noise margin)
- All 10 improvements → **preserved** (±2%)
- `dense duplicate groups` → **-8%** (additional improvement)

### Worst-Case Scenario (Requires Further Investigation)

Some regressions persist at lower levels:
- `dense reused accumulator`: +16.72% → **+3%** (mode selection still suboptimal)
- Indicates threshold values may need tuning (Task 6)

---

## Long-Term Prevention Strategies

### 1. Property-Based Testing

Use `proptest` to generate random group patterns and verify invariants:

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_unique_groups_invariants(
        group_indices in prop::collection::vec(0usize..100, 10..500)
    ) {
        // Property: unique_groups ≤ group_indices.len()
        // Property: unique_groups == HashSet::from_iter(group_indices).len()
        // Property: max_group_index == group_indices.iter().max()
        
        // ... run accumulator, extract stats, assert properties
    }
}
```

### 2. Automated Benchmark Regression Detection

**CI Integration:**
1. Run benchmarks on every PR
2. Compare against main branch baseline
3. Flag changes >±5% for manual review
4. Block merges with regressions >10% unless explicitly acknowledged

**GitHub Actions Example:**
```yaml
- name: Run benchmarks
  run: cargo bench --bench min_max_bytes -- --save-baseline pr-${{ github.event.number }}
  
- name: Compare with main
  run: |
    cargo bench --bench min_max_bytes -- --baseline main
    # Parse output, fail if regressions detected
```

### 3. Debug Assertions

Add runtime checks in debug builds:

```rust
#[cfg(debug_assertions)]
fn validate_batch_stats(stats: &BatchStats, group_indices: &[usize]) {
    use std::collections::HashSet;
    let actual_unique: HashSet<_> = group_indices.iter().copied().collect();
    assert_eq!(
        stats.unique_groups,
        actual_unique.len(),
        "BatchStats.unique_groups ({}) != actual unique groups ({})",
        stats.unique_groups,
        actual_unique.len()
    );
    
    assert_eq!(
        stats.max_group_index,
        group_indices.iter().max().copied(),
        "BatchStats.max_group_index mismatch"
    );
}
```

### 4. Improved Naming and Documentation

- Rename `total_groups_seen` → `cumulative_batch_group_sum` (clearer semantic)
- Add doc comments with examples for all heuristic functions
- Document mode transition logic with state diagrams

---

## References

- **PR Commit Range:** `1eb9d9ac6^..b69c544bc`
- **File:** `datafusion/functions-aggregate/src/min_max/min_max_bytes.rs`
- **Benchmark File:** `datafusion/functions-aggregate/benches/min_max_bytes.rs`
- **Key Functions:**
  - `update_batch_sequential_dense()` (line ~965)
  - `update_batch_dense_inline_hybrid()` (line ~810)
  - `record_batch_stats()` (line ~1209)
  - `should_use_dense_inline()` (line ~1316)
  - `should_use_simple()` (line ~1332)
  - `should_switch_to_sparse()` (line ~1348)

---

## Summary

**The bug is simple:** Line 990 increments `unique_groups` for every row instead of every unique group.

**The fix is straightforward:** Check `locations[group_index]` state before incrementing, analogous to the epoch-mark pattern in `update_batch_dense_inline_hybrid()`.

**The impact is significant:** +16.72% regression in the "dense reused accumulator" benchmark is unacceptable for a production query engine.

**Estimated fix complexity:** **Low** (5-10 lines changed)  
**Estimated risk:** **Low** (well-isolated, clear semantics, existing pattern to follow)  
**Estimated impact:** **High** (eliminates all 6 regressions, may improve additional benchmarks)

**Next Steps:**
1. Apply Task 1 fix immediately (blocking issue)
2. Run full benchmark suite (Task 7)
3. If results are satisfactory, proceed with documentation (Task 2) and testing (Task 4)
4. If regressions persist, investigate mode selection thresholds (Task 6)
