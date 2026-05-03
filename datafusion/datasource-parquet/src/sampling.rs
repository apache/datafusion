// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Random sampling primitives for parquet scans.
//!
//! [`ParquetSampling`] holds the sampling configuration carried on
//! [`crate::source::ParquetSource`]. The two `apply_*_sampling`
//! methods mutate a [`ParquetAccessPlan`] in place — they are invoked
//! by the parquet [`crate::opener`] once the file footer is loaded.
//!
//! Selection within a row group is deterministic-but-random per
//! `(file_index, row_group_index, fraction, cluster_size)`: the methods
//! seed an `SmallRng` from a hash of those inputs so re-runs match.
//! The caller supplies a stable `file_index` (for the parquet opener,
//! that is the execution `partition_index`) so sampling is independent
//! of the on-disk path string and is reproducible across environments.

use crate::access_plan::ParquetAccessPlan;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use rand::Rng;

/// Hierarchical sampling config for parquet scans.
///
/// All fractions are in `(0.0, 1.0]`. `None` (or `1.0`) means "no
/// sampling".
///
///   * `row_group_fraction` — within each scanned file, keep this
///     fraction of row groups. Decision made inside the opener after
///     the footer is loaded so we sample by actual row-group index.
///   * `row_fraction` — within each kept row group, keep this fraction
///     of rows by translating to a `RowSelection` of K small contiguous
///     windows spread across the row group. The parquet reader uses
///     the page index to read only the data pages covering the
///     selected rows, so this gives "page-level" IO savings without
///     requiring per-column page alignment. Falls back to scanning
///     whole pages if the page index is missing.
///   * `row_cluster_size` — controls how the per-row-group target is
///     split into contiguous windows. Smaller = more diversity, more
///     page-index lookups; larger = cheaper, fewer regions covered.
///
/// **Why this lives here, not as a one-shot `ParquetAccessPlan`:** the
/// natural entry-point for "I want a sample" is at config time, before
/// any metadata IO has happened. The actual *which row groups* /
/// *which rows* selection still needs to be deferred until the opener
/// has the footer — that's why these fractions get carried through and
/// applied lazily.
///
/// **Why no file-level fraction:** [`crate::source::ParquetSource`]
/// doesn't own the file list — that lives on `FileScanConfig.file_groups`.
/// Callers that want to drop files should rebuild the `FileScanConfig`
/// with a reduced `file_groups`. Adding a file-fraction setter here
/// would have been a no-op and confusing.
///
/// Selection within a row group is deterministic-but-random per
/// `(file_index, row_group_index, fraction, cluster_size)`: we seed
/// an `SmallRng` from a hash of those inputs so re-runs match exactly.
/// The caller-supplied `file_index` is a stable per-file identifier
/// (the parquet opener uses the execution `partition_index`), keeping
/// sampling reproducible across environments without the keying
/// depending on object-store paths.
#[derive(Debug, Clone)]
pub struct ParquetSampling {
    /// Fraction of row groups to keep in each scanned file.
    pub row_group_fraction: Option<f64>,
    /// Fraction of rows to keep within each scanned row group.
    pub row_fraction: Option<f64>,
    /// Maximum size (in rows) of each contiguous window emitted by
    /// row-fraction sampling. The total target rows are split into
    /// `ceil(target / row_cluster_size)` windows distributed across
    /// the row group with a random offset within each stride.
    pub row_cluster_size: usize,
}

impl Default for ParquetSampling {
    fn default() -> Self {
        Self {
            row_group_fraction: None,
            row_fraction: None,
            row_cluster_size: 32_768,
        }
    }
}

impl ParquetSampling {
    /// Mutate `plan` in-place to keep only a random subset of its
    /// currently-scannable row groups, sized to `self.row_group_fraction`
    /// of the total. No-op if `row_group_fraction` is `None`, `>= 1.0`,
    /// or out of range.
    ///
    /// Selection is deterministic given `(file_index, row_group_count,
    /// fraction)`: we seed an `SmallRng` from a hash of those inputs
    /// and use a partial Fisher-Yates shuffle. Same inputs → same
    /// sample on re-runs. Different `file_index` values produce
    /// uncorrelated samples even when row-group counts and fractions
    /// match, so files in the same scan don't all keep the same
    /// indices.
    pub(crate) fn apply_row_group_sampling(
        &self,
        plan: &mut ParquetAccessPlan,
        row_group_count: usize,
        file_index: usize,
    ) {
        use rand::SeedableRng;
        use rand::seq::SliceRandom;

        let Some(fraction) = self.row_group_fraction else {
            return;
        };
        if row_group_count == 0 || !(0.0..1.0).contains(&fraction) {
            return; // no-op for fraction >= 1.0 (keep all) or invalid input
        }
        let target = ((row_group_count as f64) * fraction).ceil().max(1.0) as usize;
        if target >= row_group_count {
            return;
        }

        let seed = derive_seed(
            b"row-group",
            file_index,
            row_group_count,
            fraction,
            self.row_cluster_size,
        );

        let mut indices: Vec<usize> = (0..row_group_count).collect();
        let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
        indices.shuffle(&mut rng);
        let kept: std::collections::HashSet<usize> =
            indices.into_iter().take(target).collect();

        for i in 0..row_group_count {
            if !kept.contains(&i) {
                plan.skip(i);
            }
        }
    }

    /// For each row group still marked `Scan`, replace it with a
    /// `Selection` that covers `self.row_fraction` of the row group's
    /// rows in `ceil(target / self.row_cluster_size)` spread-out
    /// contiguous windows. No-op if `row_fraction` is `None`, `>= 1.0`,
    /// or out of range.
    ///
    /// Selection is deterministic per `(file_index, row_group_index,
    /// fraction, cluster_size)`.
    ///
    /// If the parquet file has page indexes, the reader only reads the
    /// data pages covering the selected rows. If page indexes are
    /// missing, the reader still has to read whole pages and discard
    /// rows; the IO win disappears in that case but correctness is
    /// unaffected.
    pub(crate) fn apply_row_fraction_sampling(
        &self,
        plan: &mut ParquetAccessPlan,
        rg_metadata: &[parquet::file::metadata::RowGroupMetaData],
        file_index: usize,
    ) {
        use rand::SeedableRng;

        let Some(fraction) = self.row_fraction else {
            return;
        };
        if !(0.0..1.0).contains(&fraction) {
            return; // no-op for fraction >= 1.0 (keep all) or invalid input
        }
        let cluster_size = self.row_cluster_size.max(1);

        for (idx, rg) in rg_metadata.iter().enumerate() {
            if !plan.should_scan(idx) {
                continue;
            }
            let total_rows = rg.num_rows() as usize;
            if total_rows == 0 {
                continue;
            }

            let seed =
                derive_seed(b"row-fraction", file_index, idx, fraction, cluster_size);
            let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);

            let target_rows = ((total_rows as f64) * fraction).ceil().max(1.0) as usize;
            let target_rows = target_rows.min(total_rows);

            let Some(selectors) = build_row_window_selectors(
                total_rows,
                target_rows,
                cluster_size,
                &mut rng,
            ) else {
                continue;
            };
            plan.scan_selection(idx, RowSelection::from(selectors));
        }
    }
}

/// Hash the given inputs into a stable `u64` seed.
///
/// Uses Rust's `DefaultHasher`. Within a single binary the output is
/// deterministic; we deliberately don't promise stability across Rust
/// versions because sampling is a probabilistic optimization, not a
/// data-integrity boundary.
fn derive_seed(
    domain: &[u8],
    file_index: usize,
    secondary_index: usize,
    fraction: f64,
    cluster_size: usize,
) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    domain.hash(&mut hasher);
    file_index.hash(&mut hasher);
    secondary_index.hash(&mut hasher);
    fraction.to_bits().hash(&mut hasher);
    cluster_size.hash(&mut hasher);
    hasher.finish()
}

/// Build a `[skip, select, ..., skip]` selector layout that picks
/// `~target_rows` rows out of `total_rows` as several spread-out
/// contiguous windows.
///
/// Strategy:
///
/// 1. Choose `n_windows = ceil(target_rows / cluster_size)`. The
///    `cluster_size` cap keeps individual selected ranges small enough
///    to align with parquet data-page granularity.
/// 2. Choose `window_size = min(ceil(target_rows / n_windows), stride)`
///    where `stride = total_rows / n_windows`. Capping at `stride` is
///    what guarantees windows in adjacent strides cannot overlap even
///    when `target_rows` is close to `total_rows`. The per-window cap
///    means total selected rows can be slightly below `target_rows`
///    near `fraction → 1.0`; that is the price for correctness.
/// 3. For each stride `[w * stride, (w + 1) * stride)`, place a window
///    of size `window_size` at a random offset in `[0, stride -
///    window_size]`. This jitter makes the sample uniform within each
///    stride rather than always-from-the-start.
///
/// Returns `None` if no valid layout is possible (e.g. zero-row group,
/// zero `cluster_size`, or `target_rows == 0`). The caller should
/// treat that as "nothing to select for this row group" and leave its
/// `Scan` entry untouched.
///
/// ## Invariants (verified by `row_window_selection_*` tests)
///
/// * Selectors cover exactly `total_rows` rows in total — sum of
///   skip + select equals the row group size.
/// * No two `select` selectors overlap; rows in the layout are
///   strictly monotonic.
/// * Every selected position is in `0..total_rows`.
/// * Total selected rows is positive and within
///   `[1, n_windows * window_size]`. It can sit slightly above
///   `target_rows` when `target_rows / n_windows` is not an integer
///   (each window rounds up); the cap at `n_windows * window_size`
///   keeps the over-shoot bounded by `n_windows`.
fn build_row_window_selectors(
    total_rows: usize,
    target_rows: usize,
    cluster_size: usize,
    rng: &mut impl Rng,
) -> Option<Vec<RowSelector>> {
    if total_rows == 0 || target_rows == 0 || cluster_size == 0 {
        return None;
    }
    let target_rows = target_rows.min(total_rows);

    let n_windows = target_rows.div_ceil(cluster_size).max(1);
    let stride = total_rows / n_windows;
    if stride == 0 {
        return None;
    }
    // Capping `window_size` at `stride` is what guarantees adjacent
    // strides cannot produce overlapping windows: each window lives
    // entirely inside its own stride.
    let window_size = target_rows.div_ceil(n_windows).min(stride);
    if window_size == 0 {
        return None;
    }

    let mut selectors: Vec<RowSelector> = Vec::with_capacity(2 * n_windows + 1);
    let mut cursor = 0usize;
    for w in 0..n_windows {
        let stride_start = w * stride;
        let max_offset = stride - window_size; // safe: window_size <= stride
        let offset = if max_offset == 0 {
            0
        } else {
            rng.random_range(0..=max_offset)
        };
        let win_start = stride_start + offset;
        let win_end = win_start + window_size; // <= stride_start + stride <= total_rows
        debug_assert!(win_start >= cursor, "windows must be monotonic");
        debug_assert!(win_end <= total_rows, "window must stay in-bounds");
        if win_start > cursor {
            selectors.push(RowSelector::skip(win_start - cursor));
        }
        selectors.push(RowSelector::select(window_size));
        cursor = win_end;
    }
    if cursor < total_rows {
        selectors.push(RowSelector::skip(total_rows - cursor));
    }
    Some(selectors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

    fn count_scanned(plan: &ParquetAccessPlan) -> usize {
        (0..plan.len()).filter(|i| plan.should_scan(*i)).count()
    }

    fn cfg(row_group_fraction: Option<f64>) -> ParquetSampling {
        ParquetSampling {
            row_group_fraction,
            ..Default::default()
        }
    }

    #[test]
    fn row_group_sampling_keeps_target_count() {
        let mut plan = ParquetAccessPlan::new_all(20);
        cfg(Some(0.25)).apply_row_group_sampling(&mut plan, 20, 0);
        assert_eq!(count_scanned(&plan), 5, "ceil(20 * 0.25) = 5");
    }

    #[test]
    fn row_group_sampling_is_deterministic() {
        let mut a = ParquetAccessPlan::new_all(50);
        let mut b = ParquetAccessPlan::new_all(50);
        cfg(Some(0.10)).apply_row_group_sampling(&mut a, 50, 0);
        cfg(Some(0.10)).apply_row_group_sampling(&mut b, 50, 0);
        let kept_a: Vec<usize> = (0..50).filter(|i| a.should_scan(*i)).collect();
        let kept_b: Vec<usize> = (0..50).filter(|i| b.should_scan(*i)).collect();
        assert_eq!(kept_a, kept_b, "same seed inputs -> same selection");
    }

    #[test]
    fn row_group_sampling_differs_per_file_index() {
        let mut a = ParquetAccessPlan::new_all(50);
        let mut b = ParquetAccessPlan::new_all(50);
        cfg(Some(0.10)).apply_row_group_sampling(&mut a, 50, 0);
        cfg(Some(0.10)).apply_row_group_sampling(&mut b, 50, 1);
        let kept_a: Vec<usize> = (0..50).filter(|i| a.should_scan(*i)).collect();
        let kept_b: Vec<usize> = (0..50).filter(|i| b.should_scan(*i)).collect();
        // Hash differs by file_index -> overwhelmingly likely the
        // sampled subsets differ. (Probability of identical 5-of-50
        // pick by chance is ~5e-7.)
        assert_ne!(kept_a, kept_b);
    }

    #[test]
    fn row_group_sampling_no_op_when_fraction_is_one() {
        let mut plan = ParquetAccessPlan::new_all(8);
        cfg(Some(1.0)).apply_row_group_sampling(&mut plan, 8, 0);
        assert_eq!(count_scanned(&plan), 8, "fraction>=1.0 keeps all");
    }

    #[test]
    fn row_group_sampling_target_at_least_one() {
        let mut plan = ParquetAccessPlan::new_all(100);
        cfg(Some(0.001)).apply_row_group_sampling(&mut plan, 100, 0);
        // ceil(100 * 0.001) == 1, so we keep at least one row group.
        assert_eq!(count_scanned(&plan), 1);
    }

    #[test]
    fn row_group_sampling_no_op_when_unset() {
        let mut plan = ParquetAccessPlan::new_all(8);
        cfg(None).apply_row_group_sampling(&mut plan, 8, 0);
        assert_eq!(count_scanned(&plan), 8, "no fraction set -> no-op");
    }

    /// Sum of skip + select selectors equals total_rows; positions
    /// are monotonic and in-bounds; selected count is close to (but
    /// can slightly exceed) target_rows because of ceil rounding when
    /// splitting target across windows. Hand-checked single case to
    /// anchor the fuzz tests.
    #[test]
    fn row_window_selection_basic_layout() {
        let mut rng = SmallRng::seed_from_u64(1);
        // total=100, target=10, cluster=4 → n_windows=3, window_size=4,
        // so total selected = 12 (slightly above target due to ceil).
        let selectors = build_row_window_selectors(100, 10, 4, &mut rng).unwrap();
        let (skip, select) = sum_selectors(&selectors);
        assert_eq!(
            skip + select,
            100,
            "selectors must cover the full row group"
        );
        assert!(select > 0, "must select something");
        assert!(
            select <= 12,
            "selected {select} exceeds n_windows * window_size = 12"
        );
        verify_no_overlap(&selectors, 100);
    }

    #[test]
    fn row_window_selection_returns_none_on_invalid_input() {
        let mut rng = SmallRng::seed_from_u64(0);
        assert!(build_row_window_selectors(0, 5, 4, &mut rng).is_none());
        assert!(build_row_window_selectors(100, 0, 4, &mut rng).is_none());
        assert!(build_row_window_selectors(100, 10, 0, &mut rng).is_none());
    }

    #[test]
    fn row_window_selection_full_target_no_overlap() {
        // target_rows == total_rows is the worst case for overlap: the
        // earlier ceil-based window_size could exceed stride. Verify
        // the cap at stride keeps things disjoint.
        let mut rng = SmallRng::seed_from_u64(0);
        let selectors = build_row_window_selectors(100, 100, 7, &mut rng).unwrap();
        verify_no_overlap(&selectors, 100);
    }

    /// Fuzz: across 5_000 randomized configurations of (total_rows,
    /// target_rows, cluster_size, seed), every layout must satisfy the
    /// invariants documented on `build_row_window_selectors`.
    ///
    /// This is the regression test the reviewer asked for in
    /// https://github.com/apache/datafusion/pull/22000#discussion_r3187392811.
    #[test]
    fn row_window_selection_fuzz_invariants() {
        let mut driver = SmallRng::seed_from_u64(0xD474_F051_0000_0001);
        for _ in 0..5_000 {
            let total_rows = driver.random_range(1usize..=4_096);
            let target_rows = driver.random_range(1usize..=total_rows);
            // Mix small and large cluster sizes — the small ones force
            // many windows and stress the per-stride math.
            let cluster_size = driver.random_range(1usize..=2_048);
            let seed = driver.random::<u64>();
            let mut rng = SmallRng::seed_from_u64(seed);
            let Some(selectors) = build_row_window_selectors(
                total_rows,
                target_rows,
                cluster_size,
                &mut rng,
            ) else {
                continue;
            };
            let (skip, select) = sum_selectors(&selectors);
            assert_eq!(
                skip + select,
                total_rows,
                "selectors must cover the row group exactly: total_rows={total_rows}, target={target_rows}, cluster={cluster_size}, seed={seed}"
            );
            // `select` can sit slightly above `target_rows` because
            // `window_size = ceil(target / n_windows)` rounds up. The
            // bound below is the constructive maximum: each of the
            // `n_windows` windows holds at most `window_size` rows.
            let n_windows = target_rows.div_ceil(cluster_size).max(1);
            let stride = total_rows / n_windows;
            let window_size = target_rows.div_ceil(n_windows).min(stride);
            let max_select = n_windows.saturating_mul(window_size);
            assert!(
                select > 0 && select <= max_select && select <= total_rows,
                "selected count {select} not in (0, min({max_select}, {total_rows})] for total_rows={total_rows}, target={target_rows}, cluster={cluster_size}, seed={seed}"
            );
            verify_no_overlap(&selectors, total_rows);
        }
    }

    /// Fuzz: identical inputs (including the rng seed) must produce
    /// the exact same selectors. Catches accidental reliance on
    /// non-deterministic sources inside the windowing function.
    #[test]
    fn row_window_selection_fuzz_determinism() {
        let mut driver = SmallRng::seed_from_u64(0xD474_F051_0000_0002);
        for _ in 0..1_000 {
            let total_rows = driver.random_range(1usize..=4_096);
            let target_rows = driver.random_range(1usize..=total_rows);
            let cluster_size = driver.random_range(1usize..=2_048);
            let seed = driver.random::<u64>();
            let a = build_row_window_selectors(
                total_rows,
                target_rows,
                cluster_size,
                &mut SmallRng::seed_from_u64(seed),
            );
            let b = build_row_window_selectors(
                total_rows,
                target_rows,
                cluster_size,
                &mut SmallRng::seed_from_u64(seed),
            );
            assert_eq!(format!("{a:?}"), format!("{b:?}"));
        }
    }

    fn sum_selectors(selectors: &[RowSelector]) -> (usize, usize) {
        let mut skip = 0usize;
        let mut select = 0usize;
        for s in selectors {
            if s.skip {
                skip += s.row_count;
            } else {
                select += s.row_count;
            }
        }
        (skip, select)
    }

    /// Walks selectors as a virtual cursor and asserts every row index
    /// stays within `[0, total_rows]`. Because the layout is
    /// `[skip, select, skip, select, ...]` over a single forward
    /// cursor, no-overlap is equivalent to the cursor never going
    /// backwards or past `total_rows` — both are checked here.
    fn verify_no_overlap(selectors: &[RowSelector], total_rows: usize) {
        let mut cursor: usize = 0;
        for s in selectors {
            let next = cursor.checked_add(s.row_count).expect("overflow");
            assert!(
                next <= total_rows,
                "selector runs past total_rows={total_rows}: cursor={cursor}, len={}",
                s.row_count
            );
            cursor = next;
        }
        assert_eq!(cursor, total_rows, "selectors must end at total_rows");
    }
}
