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

use crate::sort::reverse_row_selection;
use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::datatypes::Schema;
use datafusion_common::{Result, assert_eq_or_internal_err};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use log::debug;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};

/// Fraction of adjacent (in sorted-by-min order) row group pairs whose
/// `[min, max]` ranges overlap above which `reorder_by_statistics` will
/// bail out without reordering.
///
/// When stats overlap heavily (e.g. unsorted columns like ClickBench's
/// `EventTime` on `hits_partitioned`), reordering by min cannot enable
/// row-group-level pruning — every "later" RG still has values that
/// could appear in TopK. The reorder cost (CPU sort + lost IO sequential
/// locality + parallel scheduling pessimization across workers all
/// pulling "best" RGs first) then dominates, producing a net regression.
const REORDER_OVERLAP_SKIP_THRESHOLD: f64 = 0.5;

/// A selection of rows and row groups within a ParquetFile to decode.
///
/// A `ParquetAccessPlan` is used to limit the row groups and data pages a `DataSourceExec`
/// will read and decode to improve performance.
///
/// Note that page level pruning based on ArrowPredicate is applied after all of
/// these selections
///
/// # Example
///
/// For example, given a Parquet file with 4 row groups, a `ParquetAccessPlan`
/// can be used to specify skipping row group 0 and 2, scanning a range of rows
/// in row group 1, and scanning all rows in row group 3 as follows:
///
/// ```rust
/// # use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
/// # use datafusion_datasource_parquet::ParquetAccessPlan;
/// // Default to scan all row groups
/// let mut access_plan = ParquetAccessPlan::new_all(4);
/// access_plan.skip(0); // skip row group
/// // Use parquet reader RowSelector to specify scanning rows 100-200 and 350-400
/// // in a row group that has 1000 rows
/// let row_selection = RowSelection::from(vec![
///    RowSelector::skip(100),
///    RowSelector::select(100),
///    RowSelector::skip(150),
///    RowSelector::select(50),
///    RowSelector::skip(600),  // skip last 600 rows
/// ]);
/// access_plan.scan_selection(1, row_selection);
/// access_plan.skip(2); // skip row group 2
/// // row group 3 is scanned by default
/// ```
///
/// The resulting plan would look like:
///
/// ```text
/// ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///
/// │                   │  SKIP
///
/// └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///  Row Group 0
/// ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///  ┌────────────────┐    SCAN ONLY ROWS
/// │└────────────────┘ │  100-200
///  ┌────────────────┐    350-400
/// │└────────────────┘ │
///  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
///  Row Group 1
/// ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///                        SKIP
/// │                   │
///
/// └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///  Row Group 2
/// ┌───────────────────┐
/// │                   │  SCAN ALL ROWS
/// │                   │
/// │                   │
/// └───────────────────┘
///  Row Group 3
/// ```
///
/// For more background, please also see the [Embedding User-Defined Indexes in Apache Parquet Files blog]
///
/// [Embedding User-Defined Indexes in Apache Parquet Files blog]: https://datafusion.apache.org/blog/2025/07/14/user-defined-parquet-indexes
#[derive(Debug, Clone, PartialEq)]
pub struct ParquetAccessPlan {
    /// How to access the i-th row group
    row_groups: Vec<RowGroupAccess>,
    /// Whether all rows in the i-th row group are known to match the predicate.
    ///
    /// This is tracked separately from [`RowGroupAccess`] because it describes
    /// whether row-level filter evaluation can be skipped, not which rows should
    /// be read.
    fully_matched: Vec<bool>,
}

/// Describes how the parquet reader will access a row group
#[derive(Debug, Clone, PartialEq)]
pub enum RowGroupAccess {
    /// Do not read the row group at all
    Skip,
    /// Read all rows from the row group
    Scan,
    /// Scan only the specified rows within the row group
    Selection(RowSelection),
}

/// A consecutive set of row groups that share the same row filter requirement.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RowGroupRun {
    /// True if this run needs row filter evaluation.
    pub(crate) needs_filter: bool,
    /// The access plan for this run.
    pub(crate) access_plan: ParquetAccessPlan,
}

impl RowGroupRun {
    fn new(needs_filter: bool, access_plan: ParquetAccessPlan) -> Self {
        Self {
            needs_filter,
            access_plan,
        }
    }
}

impl RowGroupAccess {
    /// Return true if this row group should be scanned
    pub fn should_scan(&self) -> bool {
        match self {
            RowGroupAccess::Skip => false,
            RowGroupAccess::Scan | RowGroupAccess::Selection(_) => true,
        }
    }
}

impl ParquetAccessPlan {
    /// Create a new `ParquetAccessPlan` that scans all row groups
    pub fn new_all(row_group_count: usize) -> Self {
        Self {
            row_groups: vec![RowGroupAccess::Scan; row_group_count],
            fully_matched: vec![false; row_group_count],
        }
    }

    /// Create a new `ParquetAccessPlan` that scans no row groups
    pub fn new_none(row_group_count: usize) -> Self {
        Self {
            row_groups: vec![RowGroupAccess::Skip; row_group_count],
            fully_matched: vec![false; row_group_count],
        }
    }

    /// Create a new `ParquetAccessPlan` from the specified [`RowGroupAccess`]es
    pub fn new(row_groups: Vec<RowGroupAccess>) -> Self {
        let row_group_count = row_groups.len();
        Self {
            row_groups,
            fully_matched: vec![false; row_group_count],
        }
    }

    /// Set the i-th row group to the specified [`RowGroupAccess`]
    pub fn set(&mut self, idx: usize, access: RowGroupAccess) {
        let should_scan = access.should_scan();
        self.row_groups[idx] = access;
        if !should_scan {
            self.fully_matched[idx] = false;
        }
    }

    /// skips the i-th row group (should not be scanned)
    pub fn skip(&mut self, idx: usize) {
        self.set(idx, RowGroupAccess::Skip);
    }

    /// scan the i-th row group
    pub fn scan(&mut self, idx: usize) {
        self.set(idx, RowGroupAccess::Scan);
    }

    /// Return true if the i-th row group should be scanned
    pub fn should_scan(&self, idx: usize) -> bool {
        self.row_groups[idx].should_scan()
    }

    /// Marks the i-th row group as fully matched.
    ///
    /// Fully matched row groups are still read according to their
    /// [`RowGroupAccess`], but row-level filter evaluation can be skipped.
    pub(crate) fn mark_fully_matched(&mut self, idx: usize) {
        if self.should_scan(idx) {
            self.fully_matched[idx] = true;
        }
    }

    /// Return true if the i-th row group is fully matched and scanned.
    pub(crate) fn is_fully_matched(&self, idx: usize) -> bool {
        self.should_scan(idx) && self.fully_matched[idx]
    }

    /// Returns the fully matched row group flags.
    pub(crate) fn fully_matched(&self) -> &Vec<bool> {
        &self.fully_matched
    }

    /// Return true if any scanned row group is fully matched.
    fn has_fully_matched(&self) -> bool {
        self.row_group_index_iter()
            .any(|idx| self.is_fully_matched(idx))
    }

    /// Set to scan only the [`RowSelection`] in the specified row group.
    ///
    /// Behavior is different depending on the existing access
    /// * [`RowGroupAccess::Skip`]: does nothing
    /// * [`RowGroupAccess::Scan`]: Updates to scan only the rows in the `RowSelection`
    /// * [`RowGroupAccess::Selection`]: Updates to scan only the intersection of the existing selection and the new selection
    pub fn scan_selection(&mut self, idx: usize, selection: RowSelection) {
        self.row_groups[idx] = match &self.row_groups[idx] {
            // already skipping the entire row group
            RowGroupAccess::Skip => RowGroupAccess::Skip,
            RowGroupAccess::Scan => RowGroupAccess::Selection(selection),
            RowGroupAccess::Selection(existing_selection) => {
                RowGroupAccess::Selection(existing_selection.intersection(&selection))
            }
        }
    }

    /// Return an overall `RowSelection`, if needed
    ///
    /// This is used to compute the row selection for the parquet reader. See
    /// [`ArrowReaderBuilder::with_row_selection`] for more details.
    ///
    /// Returns
    /// * `None` if there are no  [`RowGroupAccess::Selection`]
    /// * `Some(selection)` if there are [`RowGroupAccess::Selection`]s
    ///
    /// The returned selection represents which rows to scan across any row
    /// row groups which are not skipped.
    ///
    /// # Notes
    ///
    /// If there are no [`RowGroupAccess::Selection`]s, the overall row
    /// selection is `None` because each row group is either entirely skipped or
    /// scanned, which is covered by [`Self::row_group_indexes`].
    ///
    /// If there are any [`RowGroupAccess::Selection`], an overall row selection
    /// is returned for *all* the rows in the row groups that are not skipped.
    /// Thus it includes a `Select` selection for any [`RowGroupAccess::Scan`].
    ///
    /// # Errors
    ///
    /// Returns an error if any specified row selection does not specify
    /// the same number of rows as in it's corresponding `row_group_metadata`.
    ///
    /// # Example: No Selections
    ///
    /// Given an access plan like this
    ///
    /// ```text
    ///   RowGroupAccess::Scan (scan all row group 0)
    ///   RowGroupAccess::Skip (skip row group 1)
    ///   RowGroupAccess::Scan (scan all row group 2)
    ///   RowGroupAccess::Scan (scan all row group 3)
    /// ```
    ///
    /// The overall row selection would be `None` because there are no
    /// [`RowGroupAccess::Selection`]s. The row group indexes
    /// returned by [`Self::row_group_indexes`] would be `0, 2, 3` .
    ///
    /// # Example: With Selections
    ///
    /// Given an access plan like this:
    ///
    /// ```text
    ///   RowGroupAccess::Scan (scan all row group 0)
    ///   RowGroupAccess::Skip (skip row group 1)
    ///   RowGroupAccess::Select (skip 50, scan 50, skip 900) (scan rows 50-100 in row group 2)
    ///   RowGroupAccess::Scan (scan all row group 3)
    /// ```
    ///
    /// Assuming each row group has 1000 rows, the resulting row selection would
    /// be the rows to scan in row group 0, 2 and 4:
    ///
    /// ```text
    ///  RowSelection::Select(1000) (scan all rows in row group 0)
    ///  RowSelection::Skip(50)     (skip first 50 rows in row group 2)
    ///  RowSelection::Select(50)   (scan rows 50-100 in row group 2)
    ///  RowSelection::Skip(900)    (skip last 900 rows in row group 2)
    ///  RowSelection::Select(1000) (scan all rows in row group 3)
    /// ```
    ///
    /// Note there is no entry for the (entirely) skipped row group 1.
    ///
    /// The row group indexes returned by [`Self::row_group_indexes`] would
    /// still be `0, 2, 3` .
    ///
    /// [`ArrowReaderBuilder::with_row_selection`]: parquet::arrow::arrow_reader::ArrowReaderBuilder::with_row_selection
    pub fn into_overall_row_selection(
        self,
        row_group_meta_data: &[RowGroupMetaData],
    ) -> Result<Option<RowSelection>> {
        assert_eq!(row_group_meta_data.len(), self.row_groups.len());
        // Intuition: entire row groups are filtered out using
        // `row_group_indexes` which come from Skip and Scan. An overall
        // RowSelection is only useful if there is any parts *within* a row group
        // which can be filtered out, that is a `Selection`.
        if !self
            .row_groups
            .iter()
            .any(|rg| matches!(rg, RowGroupAccess::Selection(_)))
        {
            return Ok(None);
        }

        // validate all Selections
        for (idx, (rg, rg_meta)) in self
            .row_groups
            .iter()
            .zip(row_group_meta_data.iter())
            .enumerate()
        {
            let RowGroupAccess::Selection(selection) = rg else {
                continue;
            };
            let rows_in_selection = selection
                .iter()
                .map(|selection| selection.row_count)
                .sum::<usize>();

            let row_group_row_count = rg_meta.num_rows();
            assert_eq_or_internal_err!(
                rows_in_selection as i64,
                row_group_row_count,
                "Invalid ParquetAccessPlan Selection. Row group {idx} has {row_group_row_count} rows \
                    but selection only specifies {rows_in_selection} rows. \
                    Selection: {selection:?}"
            );
        }

        let total_selection: RowSelection = self
            .row_groups
            .into_iter()
            .zip(row_group_meta_data.iter())
            .flat_map(|(rg, rg_meta)| {
                match rg {
                    RowGroupAccess::Skip => vec![],
                    RowGroupAccess::Scan => {
                        // need a row group access to scan the entire row group (need row group counts)
                        vec![RowSelector::select(rg_meta.num_rows() as usize)]
                    }
                    RowGroupAccess::Selection(selection) => {
                        let selection: Vec<RowSelector> = selection.into();
                        selection
                    }
                }
            })
            .collect();

        Ok(Some(total_selection))
    }

    /// Return an iterator over the row group indexes that should be scanned
    pub fn row_group_index_iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.row_groups
            .iter()
            .enumerate()
            .filter_map(|(idx, b)| if b.should_scan() { Some(idx) } else { None })
    }

    /// Return a vec of all row group indexes to scan
    pub fn row_group_indexes(&self) -> Vec<usize> {
        self.row_group_index_iter().collect()
    }

    /// Return the total number of row groups (not the total number or groups to
    /// scan)
    pub fn len(&self) -> usize {
        self.row_groups.len()
    }

    /// Return true if there are no row groups
    pub fn is_empty(&self) -> bool {
        self.row_groups.is_empty()
    }

    /// Get a reference to the inner accesses
    pub fn inner(&self) -> &[RowGroupAccess] {
        &self.row_groups
    }

    /// Covert into the inner row group accesses
    pub fn into_inner(self) -> Vec<RowGroupAccess> {
        self.row_groups
    }

    /// Split this plan into consecutive row group runs that share the same row
    /// filter requirement.
    pub(crate) fn split_runs(self, needs_filter: bool) -> Vec<RowGroupRun> {
        if !needs_filter || !self.has_fully_matched() {
            return vec![RowGroupRun::new(needs_filter, self)];
        }

        let num_row_groups = self.row_groups.len();
        let row_groups = self.row_groups;
        let fully_matched = self.fully_matched;
        let mut runs: Vec<RowGroupRun> = Vec::new();

        for (idx, (access, fully_matched)) in
            row_groups.into_iter().zip(fully_matched).enumerate()
        {
            if !access.should_scan() {
                continue;
            }

            let row_group_needs_filter = !fully_matched;
            if let Some(run) = runs
                .last_mut()
                .filter(|run| run.needs_filter == row_group_needs_filter)
            {
                run.access_plan.set(idx, access);
                if fully_matched {
                    run.access_plan.mark_fully_matched(idx);
                }
            } else {
                let mut run_plan = ParquetAccessPlan::new_none(num_row_groups);
                run_plan.set(idx, access);
                if fully_matched {
                    run_plan.mark_fully_matched(idx);
                }
                runs.push(RowGroupRun::new(row_group_needs_filter, run_plan));
            }
        }

        if runs.is_empty() {
            vec![RowGroupRun::new(
                needs_filter,
                ParquetAccessPlan::new_none(num_row_groups),
            )]
        } else {
            runs
        }
    }

    /// Prepare this plan and resolve to the final `PreparedAccessPlan`
    pub(crate) fn prepare(
        self,
        row_group_meta_data: &[RowGroupMetaData],
    ) -> Result<PreparedAccessPlan> {
        let row_group_indexes = self.row_group_indexes();
        let row_selection = self.into_overall_row_selection(row_group_meta_data)?;

        PreparedAccessPlan::new(row_group_indexes, row_selection)
    }
}

/// Represents a prepared, fully resolved [`ParquetAccessPlan`]
///
/// The [`RowSelection`] represents the result of applying all pruning such as
/// user provided scans, Row Group statistics, DataPage statistics, and Bloom
/// Filters.
///
/// This plan is what is passed to the parquet reader
pub(crate) struct PreparedAccessPlan {
    /// Row group indexes to read
    pub(crate) row_group_indexes: Vec<usize>,
    /// Optional row selection for filtering within row groups
    pub(crate) row_selection: Option<RowSelection>,
}

impl PreparedAccessPlan {
    /// Create a new prepared access plan
    fn new(
        row_group_indexes: Vec<usize>,
        row_selection: Option<RowSelection>,
    ) -> Result<Self> {
        Ok(Self {
            row_group_indexes,
            row_selection,
        })
    }

    /// Reorder row groups by their min statistics for the given sort order.
    ///
    /// This helps TopK queries find optimal values first. Row groups are
    /// always sorted by min values in ASC order — direction (DESC) is
    /// handled separately by `reverse()` which is applied after reorder.
    ///
    /// Gracefully skips reordering when:
    /// - There is a row_selection (too complex to remap)
    /// - 0 or 1 row groups (nothing to reorder)
    /// - Sort expression is not a simple column reference
    /// - Statistics are unavailable
    pub(crate) fn reorder_by_statistics(
        mut self,
        sort_order: &LexOrdering,
        file_metadata: &ParquetMetaData,
        arrow_schema: &Schema,
    ) -> Result<Self> {
        // Skip if row_selection present (too complex to remap)
        if self.row_selection.is_some() {
            debug!("Skipping RG reorder: row_selection present");
            return Ok(self);
        }

        // Nothing to reorder
        if self.row_group_indexes.len() <= 1 {
            return Ok(self);
        }

        let first_sort_expr = sort_order.first();

        // Extract column name from sort expression
        let column: &Column = match first_sort_expr.expr.downcast_ref::<Column>() {
            Some(col) => col,
            None => {
                debug!("Skipping RG reorder: sort expr is not a simple column");
                return Ok(self);
            }
        };

        // Build statistics converter for this column
        let converter = match StatisticsConverter::try_new(
            column.name(),
            arrow_schema,
            file_metadata.file_metadata().schema_descr(),
        ) {
            Ok(c) => c,
            Err(e) => {
                debug!("Skipping RG reorder: cannot create stats converter: {e}");
                return Ok(self);
            }
        };

        // Always sort ASC by min values — direction is handled by reverse
        let rg_metadata: Vec<&RowGroupMetaData> = self
            .row_group_indexes
            .iter()
            .map(|&idx| file_metadata.row_group(idx))
            .collect();

        let stat_mins = match converter.row_group_mins(rg_metadata.iter().copied()) {
            Ok(vals) => vals,
            Err(e) => {
                debug!("Skipping RG reorder: cannot get min values: {e}");
                return Ok(self);
            }
        };
        let stat_maxes = match converter.row_group_maxes(rg_metadata.iter().copied()) {
            Ok(vals) => vals,
            Err(e) => {
                debug!("Skipping RG reorder: cannot get max values: {e}");
                return Ok(self);
            }
        };

        let sort_options = arrow::compute::SortOptions {
            descending: false,
            nulls_first: first_sort_expr.options.nulls_first,
        };
        let sorted_indices =
            match arrow::compute::sort_to_indices(&stat_mins, Some(sort_options), None) {
                Ok(indices) => indices,
                Err(e) => {
                    debug!("Skipping RG reorder: sort failed: {e}");
                    return Ok(self);
                }
            };

        // Bail out when adjacent ranges overlap heavily: reordering by min
        // would not enable row-group-level pruning and the reorder cost
        // (sort CPU + lost IO locality + parallel scheduling pessimization)
        // dominates. See [`REORDER_OVERLAP_SKIP_THRESHOLD`].
        match adjacent_overlap_ratio(&stat_mins, &stat_maxes, &sorted_indices) {
            Some(ratio) if ratio >= REORDER_OVERLAP_SKIP_THRESHOLD => {
                debug!(
                    "Skipping RG reorder: adjacent stats overlap {:.0}% (>= {:.0}% threshold)",
                    ratio * 100.0,
                    REORDER_OVERLAP_SKIP_THRESHOLD * 100.0
                );
                return Ok(self);
            }
            _ => {}
        }

        // Apply the reordering
        let original_indexes = self.row_group_indexes.clone();
        self.row_group_indexes = sorted_indices
            .values()
            .iter()
            .map(|&i| original_indexes[i as usize])
            .collect();

        Ok(self)
    }

    /// Reverse the access plan for reverse scanning
    pub(crate) fn reverse(mut self, file_metadata: &ParquetMetaData) -> Result<Self> {
        // Get the row group indexes before reversing
        let row_groups_to_scan = self.row_group_indexes.clone();

        // Reverse the row group indexes
        self.row_group_indexes = self.row_group_indexes.into_iter().rev().collect();

        // If we have a row selection, reverse it to match the new row group order
        if let Some(row_selection) = self.row_selection {
            self.row_selection = Some(reverse_row_selection(
                &row_selection,
                file_metadata,
                &row_groups_to_scan, // Pass the original (non-reversed) row group indexes
            )?);
        }

        Ok(self)
    }
}

/// Compute the fraction of adjacent (in sorted-by-min order) row group
/// pairs whose `[min, max]` ranges overlap.
///
/// Two ranges overlap when the later RG's min is `<=` the earlier RG's
/// max. Null mins or maxes (RGs without statistics) are treated as
/// overlapping (conservative — discourages reorder when stats are missing).
///
/// Returns `None` if there are fewer than two row groups, or if the
/// arrow comparison fails (in which case the caller should treat the
/// outcome as "do not skip").
fn adjacent_overlap_ratio(
    mins: &ArrayRef,
    maxes: &ArrayRef,
    sorted_indices: &arrow::array::UInt32Array,
) -> Option<f64> {
    let n = sorted_indices.len();
    if n < 2 {
        return None;
    }
    // Reorder mins and maxes into sorted-by-min order so we can compare
    // adjacent pairs directly.
    let mins_sorted = arrow::compute::take(mins.as_ref(), sorted_indices, None).ok()?;
    let maxes_sorted = arrow::compute::take(maxes.as_ref(), sorted_indices, None).ok()?;

    // Compare mins_sorted[1..n] against maxes_sorted[0..n-1]: an overlap
    // exists when the next min is <= the previous max.
    let mins_next: ArrayRef = mins_sorted.slice(1, n - 1);
    let maxes_prev: ArrayRef = maxes_sorted.slice(0, n - 1);
    let cmp =
        arrow::compute::kernels::cmp::lt_eq(&mins_next.as_ref(), &maxes_prev.as_ref())
            .ok()?;
    let overlap_count = overlap_count_with_null_overlap(&cmp, &mins_next, &maxes_prev);
    Some(overlap_count as f64 / (n - 1) as f64)
}

/// Count adjacent overlaps, treating null comparisons (caused by null mins
/// or maxes) as overlaps so that missing statistics do not silently disable
/// the overlap guard.
fn overlap_count_with_null_overlap(
    cmp: &BooleanArray,
    mins_next: &ArrayRef,
    maxes_prev: &ArrayRef,
) -> usize {
    let n = cmp.len();
    let mut overlaps = 0;
    for i in 0..n {
        let either_null = mins_next.is_null(i) || maxes_prev.is_null(i);
        if either_null {
            overlaps += 1;
            continue;
        }
        // cmp.value(i) is meaningful since neither input was null.
        if !cmp.is_null(i) && cmp.value(i) {
            overlaps += 1;
        }
    }
    overlaps
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{Int32Array, UInt32Array};
    use datafusion_common::assert_contains;
    use parquet::basic::LogicalType;
    use parquet::file::metadata::ColumnChunkMetaData;
    use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor};
    use std::sync::{Arc, LazyLock};

    fn ratio_for(mins: Vec<Option<i32>>, maxes: Vec<Option<i32>>) -> Option<f64> {
        let mins: ArrayRef = Arc::new(Int32Array::from(mins));
        let maxes: ArrayRef = Arc::new(Int32Array::from(maxes));
        let sorted_indices = arrow::compute::sort_to_indices(
            &mins,
            Some(arrow::compute::SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
        )
        .unwrap();
        adjacent_overlap_ratio(&mins, &maxes, &sorted_indices)
    }

    #[test]
    fn overlap_ratio_disjoint_sorted() {
        // [0,10] [20,30] [40,50] — already sorted, no overlap
        let r = ratio_for(
            vec![Some(0), Some(20), Some(40)],
            vec![Some(10), Some(30), Some(50)],
        );
        assert_eq!(r, Some(0.0));
    }

    #[test]
    fn overlap_ratio_disjoint_after_reorder() {
        // [40,50] [0,10] [20,30] — fully overlapping in original order, but
        // sorted-by-min order is disjoint; the helper sees the sorted view.
        let r = ratio_for(
            vec![Some(40), Some(0), Some(20)],
            vec![Some(50), Some(10), Some(30)],
        );
        assert_eq!(r, Some(0.0));
    }

    #[test]
    fn overlap_ratio_fully_overlapping() {
        // All RGs cover [0, 100] — every adjacent pair in sorted order overlaps
        let r = ratio_for(
            vec![Some(0), Some(0), Some(0), Some(0)],
            vec![Some(100), Some(100), Some(100), Some(100)],
        );
        assert_eq!(r, Some(1.0));
    }

    #[test]
    fn overlap_ratio_partial() {
        // Sorted-by-min: [0,15] [10,25] [30,40] [35,50]
        //   pair 0: 10 <= 15 -> overlap
        //   pair 1: 30 <= 25 -> no
        //   pair 2: 35 <= 40 -> overlap
        // 2 / 3
        let r = ratio_for(
            vec![Some(0), Some(10), Some(30), Some(35)],
            vec![Some(15), Some(25), Some(40), Some(50)],
        );
        let r = r.unwrap();
        assert!((r - 2.0 / 3.0).abs() < 1e-9, "expected ~0.667, got {r}");
    }

    #[test]
    fn overlap_ratio_null_max_in_prev_is_overlap() {
        // Sorted-by-min order: [0, null] [20, 30]. The first RG's max is
        // unknown, so we cannot prove the pair is disjoint and conservatively
        // count it as an overlap.
        let r = ratio_for(vec![Some(0), Some(20)], vec![None, Some(30)]);
        assert_eq!(r, Some(1.0));
    }

    #[test]
    fn overlap_ratio_null_min_in_next_is_overlap() {
        // Sorted-by-min order: [0, 10] [null, 20]. The second RG's min is
        // unknown, so the comparison is null and conservatively counts as
        // overlap.
        let r = ratio_for(vec![Some(0), None], vec![Some(10), Some(20)]);
        assert_eq!(r, Some(1.0));
    }

    #[test]
    fn overlap_ratio_too_few_rgs() {
        let mins: ArrayRef = Arc::new(Int32Array::from(vec![Some(0)]));
        let maxes: ArrayRef = Arc::new(Int32Array::from(vec![Some(10)]));
        let sorted = UInt32Array::from(vec![0u32]);
        assert!(adjacent_overlap_ratio(&mins, &maxes, &sorted).is_none());
    }

    #[test]
    fn test_only_scans() {
        let access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
        ]);

        let row_group_indexes = access_plan.row_group_indexes();
        let row_selection = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA)
            .unwrap();

        // scan all row groups, no selection
        assert_eq!(row_group_indexes, vec![0, 1, 2, 3]);
        assert_eq!(row_selection, None);
    }

    #[test]
    fn test_only_skips() {
        let access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Skip,
            RowGroupAccess::Skip,
            RowGroupAccess::Skip,
            RowGroupAccess::Skip,
        ]);

        let row_group_indexes = access_plan.row_group_indexes();
        let row_selection = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA)
            .unwrap();

        // skip all row groups, no selection
        assert_eq!(row_group_indexes, vec![] as Vec<usize>);
        assert_eq!(row_selection, None);
    }
    #[test]
    fn test_mixed_1() {
        let access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            RowGroupAccess::Selection(
                // specifies all 20 rows in row group 1
                vec![
                    RowSelector::select(5),
                    RowSelector::skip(7),
                    RowSelector::select(8),
                ]
                .into(),
            ),
            RowGroupAccess::Skip,
            RowGroupAccess::Skip,
        ]);

        let row_group_indexes = access_plan.row_group_indexes();
        let row_selection = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA)
            .unwrap();

        assert_eq!(row_group_indexes, vec![0, 1]);
        assert_eq!(
            row_selection,
            Some(
                vec![
                    // select the entire first row group
                    RowSelector::select(10),
                    // selectors from the second row group
                    RowSelector::select(5),
                    RowSelector::skip(7),
                    RowSelector::select(8)
                ]
                .into()
            )
        );
    }

    #[test]
    fn test_mixed_2() {
        let access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Skip,
            RowGroupAccess::Scan,
            RowGroupAccess::Selection(
                // specify all 30 rows in row group 1
                vec![
                    RowSelector::select(5),
                    RowSelector::skip(7),
                    RowSelector::select(18),
                ]
                .into(),
            ),
            RowGroupAccess::Scan,
        ]);

        let row_group_indexes = access_plan.row_group_indexes();
        let row_selection = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA)
            .unwrap();

        assert_eq!(row_group_indexes, vec![1, 2, 3]);
        assert_eq!(
            row_selection,
            Some(
                vec![
                    // select the entire second row group
                    RowSelector::select(20),
                    // selectors from the third row group
                    RowSelector::select(5),
                    RowSelector::skip(7),
                    RowSelector::select(18),
                    // select the entire fourth row group
                    RowSelector::select(40),
                ]
                .into()
            )
        );
    }

    #[test]
    fn test_invalid_too_few() {
        let access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            // specify only 12 rows in selection, but row group 1 has 20
            RowGroupAccess::Selection(
                vec![RowSelector::select(5), RowSelector::skip(7)].into(),
            ),
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
        ]);

        let row_group_indexes = access_plan.row_group_indexes();
        let err = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA)
            .unwrap_err()
            .to_string();
        assert_eq!(row_group_indexes, vec![0, 1, 2, 3]);
        assert_contains!(
            err,
            "Row group 1 has 20 rows but selection only specifies 12 rows"
        );
    }

    #[test]
    fn test_invalid_too_many() {
        let access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            // specify 22 rows in selection, but row group 1 has only 20
            RowGroupAccess::Selection(
                vec![
                    RowSelector::select(10),
                    RowSelector::skip(2),
                    RowSelector::select(10),
                ]
                .into(),
            ),
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
        ]);

        let row_group_indexes = access_plan.row_group_indexes();
        let err = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA)
            .unwrap_err()
            .to_string();
        assert_eq!(row_group_indexes, vec![0, 1, 2, 3]);
        assert_contains!(
            err,
            "Invalid ParquetAccessPlan Selection. Row group 1 has 20 rows but selection only specifies 22 rows"
        );
    }

    /// [`RowGroupMetaData`] that returns 4 row groups with 10, 20, 30, 40 rows
    /// respectively
    static ROW_GROUP_METADATA: LazyLock<Vec<RowGroupMetaData>> = LazyLock::new(|| {
        let schema_descr = get_test_schema_descr();
        let row_counts = [10, 20, 30, 40];

        row_counts
            .into_iter()
            .map(|num_rows| {
                let column = ColumnChunkMetaData::builder(schema_descr.column(0))
                    .set_num_values(num_rows)
                    .build()
                    .unwrap();

                RowGroupMetaData::builder(schema_descr.clone())
                    .set_num_rows(num_rows)
                    .set_column_metadata(vec![column])
                    .build()
                    .unwrap()
            })
            .collect()
    });

    /// Single column schema with a single column named "a" of type `BYTE_ARRAY`/`String`
    fn get_test_schema_descr() -> SchemaDescPtr {
        use parquet::basic::Type as PhysicalType;
        use parquet::schema::types::Type as SchemaType;
        let field = SchemaType::primitive_type_builder("a", PhysicalType::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::String))
            .build()
            .unwrap();
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(field)])
            .build()
            .unwrap();
        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }
}
