# MinMaxBytes Regression Analysis and Fix (v2)

**Date:** October 9, 2025  
**PR Commit Range:** `c1ac251d6^..73060b82b`  
**Branch:** `minmax-17897a`  
**Issue:** Benchmark regressions introduced by statistics tracking in sequential_dense path

---

## Executive Summary

The PR introduced adaptive mode selection for `MinMaxBytesAccumulator` by adding statistics tracking (`unique_groups` and `max_group_index`) to the `update_batch_sequential_dense` function. While this significantly improved sparse/growing workloads (-6% to -93%), it caused regressions in dense workloads (+2% to +15%) due to a critical bug: **`unique_groups` counts every non-null row instead of counting unique groups per batch**.

This over-counting poisons the adaptive mode selection heuristics, causing the accumulator to choose suboptimal execution paths for dense workloads.

---

## Benchmark Results

### Improvements (9 benchmarks)
| Benchmark                                | Mean Change | P-value  |
|------------------------------------------|-------------|----------|
| min bytes ultra sparse                   | **-93.08%** | 0.000000 |
| min bytes quadratic growing total groups | -43.56%     | 0.000000 |
| min bytes multi batch large              | -39.79%     | 0.000000 |
| min bytes monotonic group ids            | -39.89%     | 0.000000 |
| min bytes sparse groups                  | -28.56%     | 0.000000 |
| min bytes mode transition                | -22.81%     | 0.000000 |
| min bytes growing total groups           | -20.69%     | 0.000000 |
| min bytes dense duplicate groups         | -6.12%      | 0.000000 |
| min bytes medium cardinality stable      | -1.34%      | 0.000000 |

### Regressions (6 benchmarks) ⚠️
| Benchmark                            | Mean Change | P-value  |
|--------------------------------------|-------------|----------|
| min bytes dense reused accumulator   | **+15.48%** | 0.000000 |
| min bytes large dense groups         | +4.24%      | 0.000000 |
| min bytes dense first batch          | +3.01%      | 0.000000 |
| min bytes single batch large         | +3.09%      | 0.000000 |
| min bytes single batch small         | +2.86%      | 0.000000 |
| min bytes sequential stable groups   | +2.31%      | 0.000000 |

---

## Root Cause Analysis

### The Bug

**Location:** `datafusion/functions-aggregate/src/min_max/min_max_bytes.rs:975-976`

```rust
// BUGGY CODE - increments for EVERY non-null value
for (new_val, group_index) in iter.into_iter().zip(group_indices.iter()) {
    let group_index = *group_index;
    let Some(new_val) = new_val else {
        continue; // skip nulls
    };

    unique_groups = unique_groups.saturating_add(1);  // ❌ WRONG
    max_group_index = Some(match max_group_index {
        Some(current_max) => current_max.max(group_index),
        None => group_index,
    });
    
    // ... rest of the loop
}
```

The code increments `unique_groups` for **every non-null value** in the input, not for **every unique group** encountered.

### Impact

1. **Massive Over-Counting:**
   - A batch with 512 rows touching 256 unique groups → counted as 512 unique groups
   - An accumulator processing the same 100 groups across 32 batches → counted as 3,200 groups instead of 100
   - "dense duplicate groups" benchmark: consecutive duplicate group IDs mean actual unique groups ≈ 50% of rows, but we count 100%

2. **Poisoned Mode Selection:**
   - Over-counted `unique_groups` is passed to `record_batch_stats()`
   - Mode selection heuristics (`should_use_dense_inline()`, `should_use_simple()`, etc.) rely on accurate statistics
   - Inflated counts cause the algorithm to incorrectly classify dense workloads as sparse
   - Wrong mode selection → suboptimal execution paths → performance regressions

3. **Why Regressions Occur in Dense Workloads:**
   - **Dense duplicate groups** (+0% initially, then corrected): High duplicate ratio amplifies over-counting
   - **Dense reused accumulator** (+15.48%): Same groups across batches exponentially inflate the cumulative count
   - **Single-batch dense benchmarks** (+2-4%): Mis-classified as sparser than they are, triggering wrong fast paths

4. **Why Improvements Are Preserved:**
   - Sparse and growing workloads genuinely benefit from the sequential_dense detection logic
   - The fast-path check (lines 700-720) correctly identifies sequential patterns independent of the stats bug
   - Over-counting doesn't harm these workloads because they already have high group counts

---

## The Fix

### Core Solution

**Only increment `unique_groups` the first time each `group_index` appears in the current batch.**

The `locations` vector already tracks which groups have been seen in this batch. Use it as a marker:

```rust
// CORRECTED CODE
for (new_val, group_index) in iter.into_iter().zip(group_indices.iter()) {
    let group_index = *group_index;
    let Some(new_val) = new_val else {
        continue; // skip nulls
    };

    // ✅ Only count this group if we haven't seen it yet in this batch
    if matches!(locations[group_index], SequentialDenseLocation::ExistingMinMax) {
        unique_groups = unique_groups.saturating_add(1);
    }
    
    max_group_index = Some(match max_group_index {
        Some(current_max) => current_max.max(group_index),
        None => group_index,
    });
    
    let existing_val = match locations[group_index] {
        // ... rest of the loop unchanged
    };
}
```

### Key Insight

The `locations[group_index]` starts as `ExistingMinMax` and transitions to `Input(value)` when we first encounter that group in the batch. By checking the state **before** processing, we can count each group exactly once regardless of how many duplicate rows reference it.

---

## Task List

### Task 1: Fix unique_groups counting logic ⚡ CRITICAL

**File:** `datafusion/functions-aggregate/src/min_max/min_max_bytes.rs`  
**Lines:** ~975-976 in `update_batch_sequential_dense()`

**Change Required:**
```rust
// Move the unique_groups increment inside a conditional check
if matches!(locations[group_index], SequentialDenseLocation::ExistingMinMax) {
    unique_groups = unique_groups.saturating_add(1);
}
```

**Placement:** Insert this check **immediately after the null check** and **before the existing match statement** on `locations[group_index]`.

**Rationale:** The `locations` vector acts as a per-batch visit tracker. Each group starts as `ExistingMinMax` and transitions to `Input` when first processed. Checking this state ensures we count each group exactly once per batch.

---

### Task 2: Verify max_group_index calculation

**File:** Same location as Task 1  
**Lines:** ~977-980

**Current Behavior:**
```rust
max_group_index = Some(match max_group_index {
    Some(current_max) => current_max.max(group_index),
    None => group_index,
});
```

This updates `max_group_index` for **every non-null row** (same pattern as the bug).

**Assessment Required:**

Determine the correct semantics:
- **Option A:** Maximum group index across ALL rows (current behavior)
  - Represents the domain extent `[0, max_group_index]`
  - Used for mode selection to understand the index space size
  - **Likely CORRECT** - domain size is independent of duplicates

- **Option B:** Maximum group index among UNIQUE groups only
  - Would require same conditional fix as `unique_groups`
  - Less likely to be needed

**Action:** Review `should_use_dense_inline()` and `should_use_simple()` heuristics to confirm which semantic is expected. Most likely current behavior is correct since domain extent matters for memory allocation decisions.

**Decision:** If current behavior is correct, add a comment clarifying why this differs from `unique_groups` counting.

---

### Task 3: Add regression test coverage

**File:** `datafusion/functions-aggregate/benches/min_max_bytes.rs`

**New Benchmark Ideas:**

1. **High Duplicate Ratio Benchmark:**
   ```rust
   // Each group appears 10x in the same batch
   fn min_bytes_extreme_duplicates(c: &mut Criterion) {
       let unique_groups = 50;
       let group_indices: Vec<usize> = (0..unique_groups)
           .flat_map(|i| std::iter::repeat(i).take(10))
           .collect();
       // ... rest of benchmark
   }
   ```

2. **Validation Test (Debug Mode Only):**
   ```rust
   #[cfg(debug_assertions)]
   fn validate_batch_stats() {
       // Create accumulator, process batch with known duplicate pattern
       // Use internal APIs (if exposed) to verify unique_groups is correct
   }
   ```

**Purpose:** Prevent future regressions by explicitly testing the duplicate-counting edge case.

---

### Task 4: Review mode selection heuristics

**Files:**
- `record_batch_stats()` (~line 1177)
- `should_use_dense_inline()` 
- `should_use_simple()`
- `should_use_sparse_optimized()` (if exists)
- `should_switch_to_sparse()`

**Action Items:**

1. **Audit threshold values:**
   - Were they tuned with the buggy (over-counted) statistics?
   - Do they need adjustment now that `unique_groups` will be accurate?

2. **Add documentation:**
   - Clarify what "unique_groups" means (unique per batch, not cumulative)
   - Document how `total_groups_seen` accumulates across batches
   - Explain the relationship between `unique_groups`, `max_group_index`, and `total_num_groups`

3. **Consider adding debug logging:**
   ```rust
   #[cfg(feature = "trace")]
   tracing::debug!(
       unique_groups = stats.unique_groups,
       max_group_index = ?stats.max_group_index,
       total_num_groups = total_num_groups,
       mode = ?self.workload_mode,
       "Recorded batch statistics"
   );
   ```

**Rationale:** Accurate statistics may shift mode transition points. Ensure the adaptive algorithm still makes optimal decisions.

---

### Task 5: Run full benchmark suite and validate

**Commands:**
```bash
cd /Users/kosiew/GitHub/datafusion
cargo bench --bench min_max_bytes -- --save-baseline after-fix
```

**Success Criteria:**

✅ **All 6 regressions return to baseline or improve:**
- `min bytes dense reused accumulator`: +15.48% → ≤0%
- `min bytes large dense groups`: +4.24% → ≤0%
- `min bytes dense first batch`: +3.01% → ≤0%
- `min bytes single batch large`: +3.09% → ≤0%
- `min bytes single batch small`: +2.86% → ≤0%
- `min bytes sequential stable groups`: +2.31% → ≤0%

✅ **All 9 improvements are preserved:**
- `min bytes ultra sparse`: -93.08% maintained
- `min bytes quadratic growing total groups`: -43.56% maintained
- ... (all other improvements)

✅ **No new regressions introduced**

✅ **Mode transitions occur at sensible points:**
- Add tracing to verify `workload_mode` changes happen at expected batch counts
- Confirm `DenseInline`, `Simple`, and `SparseOptimized` paths are chosen correctly

**Validation Process:**

1. Apply the fix from Task 1
2. Run benchmarks: `cargo bench --bench min_max_bytes`
3. Compare results to the baseline in the issue description
4. If regressions persist, investigate mode selection (Task 4)
5. Document final results

---

## Technical Details

### Sequential Dense Path Detection

The fast-path check (lines 700-720) identifies batches where:
- `group_indices.len() == total_num_groups` (batch covers all groups)
- `group_indices[0] == 0` (starts at zero)
- `group_indices[total_num_groups - 1] == total_num_groups - 1` (ends at max)
- All indices are strictly sequential: `group_indices[i+1] == group_indices[i] + 1`

This pattern represents a "perfect dense sequential" batch where groups appear exactly once in order. The bug doesn't affect this detection, but it affects the statistics collected **after** this path is taken.

### Location Vector Mechanics

```rust
enum SequentialDenseLocation<'a> {
    ExistingMinMax,      // Group not yet seen in this batch
    Input(&'a [u8]),     // Group seen, current min/max candidate from input
}
```

The `locations` vector:
- Sized to `total_num_groups` (all possible groups)
- Initialized to `ExistingMinMax` for all indices
- Transitions to `Input(value)` when a group is first encountered
- Allows duplicate detection: if `locations[i]` is already `Input`, we've seen group `i` before in this batch

### Why the Bug Went Unnoticed

1. **Improvements dominated attention:** The -93% improvement in `ultra sparse` benchmark was so dramatic that smaller regressions seemed like acceptable trade-offs or noise.

2. **Single-batch tests masked the issue:** Many unit tests likely use single-batch scenarios where over-counting by 2-3x doesn't trigger wrong mode selection.

3. **Multi-batch dense workloads are less common:** The "reused accumulator" pattern (same groups repeatedly) is realistic but not universally tested.

4. **Gradual degradation:** The +15% regression is significant but not catastrophic, making it easy to miss in CI if benchmarks aren't carefully monitored.

---

## Prevention Strategies

### 1. Add Assertions in Debug Mode

```rust
#[cfg(debug_assertions)]
fn validate_batch_stats(stats: &BatchStats, group_indices: &[usize]) {
    use std::collections::HashSet;
    let actual_unique: HashSet<_> = group_indices.iter().copied().collect();
    assert_eq!(
        stats.unique_groups, 
        actual_unique.len(),
        "unique_groups mismatch: counted {} but should be {}",
        stats.unique_groups,
        actual_unique.len()
    );
}
```

### 2. Benchmark Naming Convention

Use descriptive names that highlight the characteristic being tested:
- ✅ `min_bytes_dense_duplicate_groups` (clear: duplicates are the focus)
- ❌ `min_bytes_test_3` (unclear what's being tested)

### 3. Regression Tracking

Set up automated benchmark regression detection in CI:
- Run benchmarks on every PR
- Flag changes >5% as requiring review
- Maintain historical baseline for comparison

### 4. Property-Based Testing

Use `proptest` or similar to generate random group patterns and verify:
- `unique_groups` ≤ `group_indices.len()`
- `unique_groups` == actual `HashSet` size
- `max_group_index` == `group_indices.iter().max()`

---

## Related Issues

- **Original Issue:** High-cardinality MIN/MAX aggregations had quadratic allocation behavior
- **This PR's Goal:** Eliminate quadratic work by adapting execution strategy based on workload
- **This Regression:** Statistics bug causes wrong strategy selection for dense workloads

The fix preserves the PR's core improvements while eliminating the unintended regressions.

---

## References

- **PR Commit:** `73060b82b` - "Enhance MinMaxBytesState to return batch statistics from sequential dense updates"
- **File:** `datafusion/functions-aggregate/src/min_max/min_max_bytes.rs`
- **Benchmark File:** `datafusion/functions-aggregate/benches/min_max_bytes.rs`
- **Related Code:** `WorkloadMode`, `BatchStats`, `record_batch_stats`, adaptive mode selection heuristics

---

## Conclusion

The regression is caused by a simple but impactful bug: counting rows instead of unique groups. The fix is straightforward and localized. Once applied, the adaptive mode selection should work as intended, preserving the dramatic improvements for sparse workloads while eliminating regressions for dense workloads.

**Estimated Fix Complexity:** Low (single conditional check)  
**Estimated Risk:** Low (well-isolated change with clear semantics)  
**Estimated Impact:** High (resolves all 6 regressions)
