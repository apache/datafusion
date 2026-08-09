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
use arrow::array::BooleanBufferBuilder;
use arrow::datatypes::Schema;
use datafusion_common::{Result, assert_eq_or_internal_err, exec_err};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use log::debug;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};

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

/// A file-level row selection for a parquet scan.
///
/// Attach this type to a [`PartitionedFile`](datafusion_datasource::PartitionedFile)
/// with [`PartitionedFile::with_extension`](datafusion_datasource::PartitionedFile::with_extension)
/// when an external index produces a [`RowSelection`] across the entire parquet
/// file. DataFusion will use parquet metadata to split it into row-group-level
/// access when the file is opened.
#[derive(Debug, Clone, PartialEq)]
pub struct ParquetRowSelection {
    selection: RowSelection,
}

impl ParquetRowSelection {
    /// Create a new file-level parquet row selection.
    pub fn new(selection: RowSelection) -> Self {
        Self { selection }
    }

    /// Return a reference to the underlying [`RowSelection`].
    pub fn selection(&self) -> &RowSelection {
        &self.selection
    }

    /// Convert into the underlying [`RowSelection`].
    pub fn into_inner(self) -> RowSelection {
        self.selection
    }
}

impl From<RowSelection> for ParquetRowSelection {
    fn from(selection: RowSelection) -> Self {
        Self::new(selection)
    }
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

impl RowGroupAccess {
    /// Return true if this row group should be scanned
    pub fn should_scan(&self) -> bool {
        match self {
            RowGroupAccess::Skip => false,
            RowGroupAccess::Scan | RowGroupAccess::Selection(_) => true,
        }
    }
}

/// Single-pass cursor over a file-level [`RowSelection`].
///
/// `take` returns the next selector fragment capped to the requested row count,
/// splitting the current selector when it straddles a row group boundary.
struct OverallRowSelectionCursor {
    selector_iter: std::vec::IntoIter<RowSelector>,
    current: Option<RowSelector>,
}

impl OverallRowSelectionCursor {
    fn new(selection: RowSelection) -> Self {
        let selectors: Vec<RowSelector> = selection.into();
        let mut selector_iter = selectors.into_iter();
        let current = selector_iter.next();
        Self {
            selector_iter,
            current,
        }
    }

    /// Take up to `max_rows` rows from the current selector.
    ///
    /// If the current selector crosses the requested boundary, this returns the
    /// leading fragment and keeps the remaining rows in `self.current` for the
    /// next call.
    #[inline]
    fn take(&mut self, max_rows: usize) -> Option<RowSelector> {
        let sel = self.current?;
        let row_count = sel.row_count.min(max_rows);
        self.current = if row_count < sel.row_count {
            Some(RowSelector {
                row_count: sel.row_count - row_count,
                skip: sel.skip,
            })
        } else {
            self.selector_iter.next()
        };

        Some(RowSelector {
            row_count,
            skip: sel.skip,
        })
    }
}

/// Accumulates the selector fragments that belong to one row group.
struct RowGroupAccessBuilder {
    /// Selector fragments belonging to this row group.
    selectors: Vec<RowSelector>,
    /// Number of selected rows accumulated for this row group.
    selected: usize,
    /// Number of skipped rows accumulated for this row group.
    skipped: usize,
    /// Number of rows still needed to complete this row group.
    remaining: usize,
}

impl RowGroupAccessBuilder {
    fn new(row_group_rows: usize) -> Self {
        Self {
            selectors: Vec::with_capacity(1),
            selected: 0,
            skipped: 0,
            remaining: row_group_rows,
        }
    }

    #[inline]
    fn push(&mut self, selector: RowSelector) {
        self.remaining -= selector.row_count;

        if selector.skip {
            self.skipped += selector.row_count;
        } else {
            self.selected += selector.row_count;
        }

        self.selectors.push(selector);
    }

    fn into_access(self) -> RowGroupAccess {
        if self.selected == 0 {
            RowGroupAccess::Skip
        } else if self.skipped == 0 {
            RowGroupAccess::Scan
        } else {
            RowGroupAccess::Selection(self.selectors.into())
        }
    }
}

/// Return the total number of rows described by `selection`.
fn row_selection_len(selection: &RowSelection) -> usize {
    match selection.as_mask() {
        Some(mask) => mask.len(),
        None => selection.iter().map(|selector| selector.row_count).sum(),
    }
}

/// Convert `selection` to a bitmap-backed [`RowSelection`].
///
/// No-op if the selection is already bitmap-backed.
fn into_mask_backed(selection: RowSelection) -> RowSelection {
    match selection.as_mask() {
        Some(_) => selection,
        None => {
            let total_rows = row_selection_len(&selection);
            let mut mask = BooleanBufferBuilder::new(total_rows);
            for selector in selection.iter() {
                mask.append_n(selector.row_count, !selector.skip);
            }
            RowSelection::from_boolean_buffer(mask.finish())
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

    /// Create a new `ParquetAccessPlan` from a file-level [`RowSelection`].
    ///
    /// The selection is interpreted across all rows in the file, in row group
    /// order, and is split into row-group level access using `row_group_meta_data`.
    /// Fully skipped row groups become [`RowGroupAccess::Skip`], fully selected
    /// row groups become [`RowGroupAccess::Scan`], and partially selected row
    /// groups become [`RowGroupAccess::Selection`].
    ///
    /// # Errors
    ///
    /// Returns an error if the selection does not specify exactly the same
    /// number of rows as the file metadata.
    pub fn try_new_from_overall_row_selection(
        mut selection: RowSelection,
        row_group_meta_data: &[RowGroupMetaData],
    ) -> Result<Self> {
        let selection_rows = row_selection_len(&selection);
        let file_rows = row_group_meta_data
            .iter()
            .map(|rg| rg.num_rows() as usize)
            .sum::<usize>();

        if selection_rows != file_rows {
            return exec_err!(
                "Invalid Parquet RowSelection. File has {file_rows} rows, \
                but selection specifies {selection_rows} rows."
            );
        }

        let row_groups = match selection.as_mask() {
            // `split_off` slices bitmap-backed selections without converting
            // them to selectors. Keep partially selected row groups
            // bitmap-backed so they can reach the parquet reader without an
            // intermediate RLE.
            Some(_) => row_group_meta_data
                .iter()
                .map(|rg_meta| {
                    let rg_rows = rg_meta.num_rows() as usize;
                    let rg_selection = selection.split_off(rg_rows);
                    let selected_rows = rg_selection.row_count();

                    if selected_rows == 0 {
                        RowGroupAccess::Skip
                    } else if selected_rows == rg_rows {
                        RowGroupAccess::Scan
                    } else {
                        RowGroupAccess::Selection(rg_selection)
                    }
                })
                .collect(),
            None => {
                // Keep this as a single pass over the selector stream rather
                // than repeatedly calling `RowSelection::split_off` per row
                // group. The `split_off` version is simpler, but it
                // clones/retains substantially more selector buffer capacity
                // for highly fragmented selections.
                let mut cursor = OverallRowSelectionCursor::new(selection);

                let mut row_groups = Vec::with_capacity(row_group_meta_data.len());
                for rg_meta in row_group_meta_data {
                    let rg_rows = rg_meta.num_rows() as usize;

                    let mut builder = RowGroupAccessBuilder::new(rg_rows);
                    while builder.remaining > 0 {
                        let selector = cursor
                            .take(builder.remaining)
                            .expect("row selection length was validated");
                        builder.push(selector);
                    }

                    row_groups.push(builder.into_access());
                }
                row_groups
            }
        };

        Ok(Self::new(row_groups))
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
                // `RowSelection::intersection` only stays bitmap-backed when
                // both sides are bitmap-backed, so promote the incoming
                // selection (e.g. from page index pruning) to match an
                // existing bitmap: the intersection is then a bitwise AND
                // instead of a selector merge.
                let selection = match existing_selection.as_mask() {
                    Some(_) => into_mask_backed(selection),
                    None => selection,
                };
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
    /// If any [`RowGroupAccess::Selection`] is bitmap-backed
    /// ([`RowSelection::as_mask`] returns `Some`), the overall selection is
    /// bitmap-backed as well and any selector-backed selections are promoted
    /// to bitmaps; otherwise the overall selection is selector-backed.
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
            let rows_in_selection = row_selection_len(selection);

            let row_group_row_count = rg_meta.num_rows();
            assert_eq_or_internal_err!(
                rows_in_selection as i64,
                row_group_row_count,
                "Invalid ParquetAccessPlan Selection. Row group {idx} has {row_group_row_count} rows \
                    but selection only specifies {rows_in_selection} rows. \
                    Selection: {selection:?}"
            );
        }

        // A bitmap-backed group selection signals the plan originated from a
        // bitmap (e.g. a mask-backed `ParquetRowSelection`), even if later
        // pruning such as the page index intersected some groups back to
        // selectors. Promote the selector-backed groups in that case so the
        // reader still receives a bitmap-backed selection.
        let any_selection_mask_backed = self.row_groups.iter().any(|rg| {
            matches!(rg, RowGroupAccess::Selection(selection) if selection.as_mask().is_some())
        });

        let total_selection = if any_selection_mask_backed {
            let total_rows = self
                .row_groups
                .iter()
                .zip(row_group_meta_data.iter())
                .filter(|(rg, _)| rg.should_scan())
                .map(|(_, rg_meta)| rg_meta.num_rows() as usize)
                .sum();
            let mut mask = BooleanBufferBuilder::new(total_rows);

            for (rg, rg_meta) in self.row_groups.into_iter().zip(row_group_meta_data) {
                match rg {
                    // Skipped row groups are not passed to the parquet reader.
                    RowGroupAccess::Skip => {}
                    // Represent scanned row groups as all-set bitmap ranges.
                    RowGroupAccess::Scan => {
                        mask.append_n(rg_meta.num_rows() as usize, true)
                    }
                    RowGroupAccess::Selection(selection) => match selection.as_mask() {
                        Some(buffer) => mask.append_buffer(buffer),
                        // Promote selector-backed groups to bitmap ranges.
                        None => {
                            for selector in selection.iter() {
                                mask.append_n(selector.row_count, !selector.skip);
                            }
                        }
                    },
                }
            }

            RowSelection::from_boolean_buffer(mask.finish())
        } else {
            // Preserve the existing selector path when no row group
            // selection is bitmap-backed.
            self.row_groups
                .into_iter()
                .zip(row_group_meta_data.iter())
                .flat_map(|(rg, rg_meta)| match rg {
                    RowGroupAccess::Skip => vec![],
                    RowGroupAccess::Scan => {
                        vec![RowSelector::select(rg_meta.num_rows() as usize)]
                    }
                    RowGroupAccess::Selection(selection) => selection.into(),
                })
                .collect()
        };

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
    /// lexicographically sorted by per-column min values over the longest
    /// prefix of the sort order made of plain columns present in the file
    /// schema. The leading column is always sorted ASC by min — direction
    /// (DESC) is handled separately by `reverse()` which is applied after
    /// reorder. Subsequent columns sort by their direction *relative* to
    /// the leading column (and their null placement is flipped when the
    /// plan will be reversed), so that the post-`reverse()` order
    /// approximates the requested lexicographic order.
    ///
    /// Secondary sort keys matter when the leading column's min ties
    /// across row groups (e.g. `ORDER BY low_cardinality_col, ts LIMIT k`)
    /// — without them the reorder is a no-op on such files and the TopK
    /// dynamic filter converges only as fast as disk order allows.
    ///
    /// Gracefully skips reordering when:
    /// - There is a row_selection (too complex to remap)
    /// - 0 or 1 row groups (nothing to reorder)
    /// - The leading sort expression is not a simple column reference
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

        let rg_metadata: Vec<&RowGroupMetaData> = self
            .row_group_indexes
            .iter()
            .map(|&idx| file_metadata.row_group(idx))
            .collect();

        let leading_descending = sort_order.first().options.descending;

        // Build one `SortColumn` of per-RG mins for each usable prefix
        // column of the sort order. The walk stops at the first
        // expression that isn't a plain `Column` in the file schema —
        // stats for later columns can't refine the order once an
        // unresolvable key sits between them and the resolved prefix.
        let mut sort_columns: Vec<arrow::compute::SortColumn> = Vec::new();
        for (i, sort_expr) in sort_order.iter().enumerate() {
            let column: &Column = match sort_expr.expr.downcast_ref::<Column>() {
                Some(col) => col,
                None => {
                    if i == 0 {
                        debug!("Skipping RG reorder: sort expr is not a simple column");
                        return Ok(self);
                    }
                    break;
                }
            };

            // Expected graceful skip: the sort column lives outside the
            // file schema (e.g. a partition column whose ordering came
            // through `reversed_satisfies` rather than
            // `column_in_file_schema`). Parquet has no per-RG stats for
            // it. Bail out quietly — no `debug_assert!` because this is
            // a normal pushdown shape.
            if arrow_schema.field_with_name(column.name()).is_err() {
                if i == 0 {
                    debug!(
                        "Skipping RG reorder: column `{}` not in file schema",
                        column.name()
                    );
                    return Ok(self);
                }
                break;
            }

            // From here, any `StatisticsConverter` / stats read / sort
            // failure is unexpected — the column exists in the file
            // schema, so building the converter and pulling typed mins
            // should succeed on any well-formed parquet file. Trip a
            // `debug_assert!` so CI catches regressions, but stay graceful
            // in release so a single odd file can't take down a scan.
            let converter = match StatisticsConverter::try_new(
                column.name(),
                arrow_schema,
                file_metadata.file_metadata().schema_descr(),
            ) {
                Ok(c) => c,
                Err(e) => {
                    debug_assert!(
                        false,
                        "RG reorder: cannot create stats converter for `{}`: {e}",
                        column.name(),
                    );
                    if i == 0 {
                        return Ok(self);
                    }
                    break;
                }
            };

            let stat_mins = match converter.row_group_mins(rg_metadata.iter().copied()) {
                Ok(vals) => vals,
                Err(e) => {
                    debug_assert!(
                        false,
                        "RG reorder: cannot get min values for `{}`: {e}",
                        column.name(),
                    );
                    if i == 0 {
                        return Ok(self);
                    }
                    break;
                }
            };

            // The plan is later `reverse()`d iff the leading column is
            // DESC, which flips both value order and null placement of
            // every column. Sort each column by its direction relative
            // to the leading column (leading itself is therefore always
            // ASC), and pre-flip null placement when the reverse is
            // coming, so the post-reverse order matches the request.
            // Nulls here are row groups with *missing stats*, so their
            // placement is a heuristic, not a correctness matter.
            let sort_options = arrow::compute::SortOptions {
                descending: sort_expr.options.descending != leading_descending,
                nulls_first: sort_expr.options.nulls_first != leading_descending,
            };
            sort_columns.push(arrow::compute::SortColumn {
                values: stat_mins,
                options: Some(sort_options),
            });
        }

        let sorted_indices = match arrow::compute::lexsort_to_indices(&sort_columns, None)
        {
            Ok(indices) => indices,
            Err(e) => {
                debug_assert!(false, "RG reorder: arrow lexsort_to_indices failed: {e}");
                return Ok(self);
            }
        };

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

#[cfg(test)]
mod test {
    use super::*;
    use arrow::buffer::BooleanBuffer;
    use datafusion_common::assert_contains;
    use parquet::basic::LogicalType;
    use parquet::file::metadata::ColumnChunkMetaData;
    use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor};
    use std::sync::{Arc, LazyLock};

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
    fn test_mixed_backings_promote_to_mask() {
        // One bitmap-backed group next to a selector-backed group (e.g. one
        // that page index pruning intersected): the overall selection is
        // promoted to a bitmap so the caller-provided mask backing survives
        // to the reader.
        let access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Selection(RowSelection::from_boolean_buffer(
                BooleanBuffer::from(vec![
                    true, false, false, false, false, false, false, false, false, false,
                ]),
            )),
            RowGroupAccess::Selection(RowSelection::from(vec![
                RowSelector::select(10),
                RowSelector::skip(10),
            ])),
            RowGroupAccess::Skip,
            RowGroupAccess::Skip,
        ]);

        let row_selection = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA)
            .unwrap()
            .unwrap();

        let mut expected = vec![true];
        expected.extend(vec![false; 9]);
        expected.extend(vec![true; 10]);
        expected.extend(vec![false; 10]);
        let expected = BooleanBuffer::from(expected);
        assert_eq!(row_selection.as_mask(), Some(&expected));
    }

    #[test]
    fn test_scan_selection_preserves_mask_backing() {
        let mut access_plan = ParquetAccessPlan::new(vec![RowGroupAccess::Selection(
            RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![
                true, true, false, false, true, true, false, false, true, true,
            ])),
        )]);

        // Intersect with a selector-backed selection, as produced by page
        // index pruning.
        access_plan.scan_selection(
            0,
            RowSelection::from(vec![RowSelector::select(5), RowSelector::skip(5)]),
        );
        // Intersect with an already mask-backed selection to exercise the
        // no-op path in `into_mask_backed`.
        access_plan.scan_selection(
            0,
            RowSelection::from_boolean_buffer(BooleanBuffer::new_set(10)),
        );

        let selection = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA[..1])
            .unwrap()
            .unwrap();
        let expected = BooleanBuffer::from(vec![
            true, true, false, false, true, false, false, false, false, false,
        ]);
        assert_eq!(selection.as_mask(), Some(&expected));
    }

    #[test]
    fn test_scan_selection_preserves_selector_backing() {
        let mut access_plan = ParquetAccessPlan::new(vec![RowGroupAccess::Selection(
            RowSelection::from(vec![RowSelector::select(6), RowSelector::skip(4)]),
        )]);

        access_plan.scan_selection(
            0,
            RowSelection::from(vec![
                RowSelector::skip(2),
                RowSelector::select(5),
                RowSelector::skip(3),
            ]),
        );

        let selection = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA[..1])
            .unwrap()
            .unwrap();
        assert_eq!(selection.as_mask(), None);
        assert_eq!(
            selection,
            RowSelection::from(vec![
                RowSelector::skip(2),
                RowSelector::select(4),
                RowSelector::skip(4),
            ])
        );
    }

    #[test]
    fn test_new_from_overall_row_selection() {
        let row_selection = RowSelection::from(vec![
            RowSelector::select(10),
            RowSelector::skip(25),
            RowSelector::select(10),
            RowSelector::skip(15),
            RowSelector::select(40),
        ]);

        let access_plan = ParquetAccessPlan::try_new_from_overall_row_selection(
            row_selection,
            &ROW_GROUP_METADATA,
        )
        .unwrap();

        assert_eq!(
            access_plan,
            ParquetAccessPlan::new(vec![
                RowGroupAccess::Scan,
                RowGroupAccess::Skip,
                RowGroupAccess::Selection(
                    vec![
                        RowSelector::skip(5),
                        RowSelector::select(10),
                        RowSelector::skip(15),
                    ]
                    .into()
                ),
                RowGroupAccess::Scan,
            ])
        );
    }

    #[test]
    fn test_new_from_overall_row_selection_invalid_row_count() {
        let row_selection = RowSelection::from(vec![RowSelector::select(99)]);

        let err = ParquetAccessPlan::try_new_from_overall_row_selection(
            row_selection,
            &ROW_GROUP_METADATA,
        )
        .unwrap_err()
        .to_string();

        assert_contains!(
            err,
            "Invalid Parquet RowSelection. File has 100 rows, but selection specifies 99 rows"
        );
    }

    #[test]
    fn test_new_from_overall_row_selection_boundary_splits() {
        let row_selection = RowSelection::from(vec![
            RowSelector::skip(5),
            RowSelector::select(10),
            RowSelector::skip(20),
            RowSelector::select(25),
            RowSelector::skip(40),
        ]);

        let access_plan = ParquetAccessPlan::try_new_from_overall_row_selection(
            row_selection,
            &ROW_GROUP_METADATA,
        )
        .unwrap();

        assert_eq!(
            access_plan,
            ParquetAccessPlan::new(vec![
                RowGroupAccess::Selection(
                    vec![RowSelector::skip(5), RowSelector::select(5)].into()
                ),
                RowGroupAccess::Selection(
                    vec![RowSelector::select(5), RowSelector::skip(15)].into()
                ),
                RowGroupAccess::Selection(
                    vec![RowSelector::skip(5), RowSelector::select(25)].into()
                ),
                RowGroupAccess::Skip,
            ])
        );
    }

    #[test]
    fn test_new_from_overall_mask_preserves_bitmap_backing() {
        let partial_mask = vec![
            false, true, false, true, false, true, false, true, false, true, false, true,
            false, true, false, true, false, true, false, true, false, true, false, true,
            false, true, false, true, false, true,
        ];
        let mut file_mask = vec![true; 10];
        file_mask.extend(vec![false; 20]);
        file_mask.extend(&partial_mask);
        file_mask.extend(vec![true; 40]);

        let access_plan = ParquetAccessPlan::try_new_from_overall_row_selection(
            RowSelection::from_boolean_buffer(BooleanBuffer::from(file_mask)),
            &ROW_GROUP_METADATA,
        )
        .unwrap();

        assert_eq!(
            access_plan,
            ParquetAccessPlan::new(vec![
                RowGroupAccess::Scan,
                RowGroupAccess::Skip,
                RowGroupAccess::Selection(RowSelection::from_boolean_buffer(
                    BooleanBuffer::from(partial_mask.clone()),
                )),
                RowGroupAccess::Scan,
            ])
        );

        // The fully skipped row group is omitted from the reader selection;
        // scanned groups are represented as all-set bitmap ranges.
        let overall = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA)
            .unwrap()
            .unwrap();
        let mut expected_overall = vec![true; 10];
        expected_overall.extend(partial_mask);
        expected_overall.extend(vec![true; 40]);
        let expected_overall = BooleanBuffer::from(expected_overall);
        assert_eq!(overall.as_mask(), Some(&expected_overall));
    }

    #[test]
    fn test_new_from_overall_mask_invalid_row_count() {
        let row_selection = RowSelection::from_boolean_buffer(BooleanBuffer::new_set(99));

        let err = ParquetAccessPlan::try_new_from_overall_row_selection(
            row_selection,
            &ROW_GROUP_METADATA,
        )
        .unwrap_err()
        .to_string();

        assert_contains!(
            err,
            "Invalid Parquet RowSelection. File has 100 rows, but selection specifies 99 rows"
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

    // ----------------------------------------------------------------
    // `reorder_by_statistics` tests
    // ----------------------------------------------------------------

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, lit};
    use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
    use parquet::file::metadata::FileMetaData;
    use parquet::file::statistics::Statistics as ParquetStatistics;

    /// Single-column int32 schema named "a".
    fn int_schema_descr() -> SchemaDescPtr {
        use parquet::basic::Type as PhysicalType;
        use parquet::schema::types::Type as SchemaType;
        let field = SchemaType::primitive_type_builder("a", PhysicalType::INT32)
            .build()
            .unwrap();
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(field)])
            .build()
            .unwrap();
        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }

    /// Build a `ParquetMetaData` with one row group per element of
    /// `mins`. Each row group declares int32 statistics with
    /// `min == max == mins[i]` so the reorder key is unambiguous.
    fn parquet_metadata_with_int_mins(mins: &[i32]) -> ParquetMetaData {
        let schema_descr = int_schema_descr();
        let row_groups: Vec<RowGroupMetaData> = mins
            .iter()
            .map(|&m| {
                let stats =
                    ParquetStatistics::int32(Some(m), Some(m), None, Some(0), false);
                let column = ColumnChunkMetaData::builder(schema_descr.column(0))
                    .set_statistics(stats)
                    .set_num_values(100)
                    .build()
                    .unwrap();
                RowGroupMetaData::builder(schema_descr.clone())
                    .set_num_rows(100)
                    .set_column_metadata(vec![column])
                    .build()
                    .unwrap()
            })
            .collect();
        let file_metadata =
            FileMetaData::new(0, 0, None, None, schema_descr.clone(), None);
        ParquetMetaData::new(file_metadata, row_groups)
    }

    fn arrow_schema_a_int() -> Schema {
        Schema::new(vec![Field::new("a", DataType::Int32, true)])
    }

    fn lex_ordering_a_asc() -> LexOrdering {
        LexOrdering::new(vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("a", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }])
        .unwrap()
    }

    /// Happy path: three row groups with mins 50/10/100. After
    /// `reorder_by_statistics` the indexes are ordered ASC by `min`,
    /// i.e. RG 1 (min=10) first, then RG 0 (min=50), then RG 2
    /// (min=100).
    #[test]
    fn reorder_by_statistics_sorts_row_groups_asc_by_min() {
        let metadata = parquet_metadata_with_int_mins(&[50, 10, 100]);
        let plan = PreparedAccessPlan::new(vec![0, 1, 2], None).unwrap();

        let result = plan
            .reorder_by_statistics(
                &lex_ordering_a_asc(),
                &metadata,
                &arrow_schema_a_int(),
            )
            .unwrap();

        assert_eq!(result.row_group_indexes, vec![1, 0, 2]);
    }

    /// A `row_selection` is "too complex to remap" through reorder,
    /// so the function short-circuits and returns the input untouched.
    #[test]
    fn reorder_by_statistics_skips_when_row_selection_present() {
        let metadata = parquet_metadata_with_int_mins(&[50, 10]);
        let selection = RowSelection::from(vec![RowSelector::select(100)]);
        let plan = PreparedAccessPlan::new(vec![0, 1], Some(selection)).unwrap();

        let result = plan
            .reorder_by_statistics(
                &lex_ordering_a_asc(),
                &metadata,
                &arrow_schema_a_int(),
            )
            .unwrap();

        assert_eq!(result.row_group_indexes, vec![0, 1]);
    }

    /// One row group means nothing to reorder.
    #[test]
    fn reorder_by_statistics_skips_when_at_most_one_row_group() {
        let metadata = parquet_metadata_with_int_mins(&[50]);
        let plan = PreparedAccessPlan::new(vec![0], None).unwrap();

        let result = plan
            .reorder_by_statistics(
                &lex_ordering_a_asc(),
                &metadata,
                &arrow_schema_a_int(),
            )
            .unwrap();

        assert_eq!(result.row_group_indexes, vec![0]);
    }

    /// Non-`Column` sort expressions (e.g. `a + 1`,
    /// `date_trunc(...)`) can't drive a stats lookup, so reorder is
    /// skipped. The opener falls back to whatever order it received.
    #[test]
    fn reorder_by_statistics_skips_for_non_column_sort_expr() {
        let metadata = parquet_metadata_with_int_mins(&[50, 10]);
        let plan = PreparedAccessPlan::new(vec![0, 1], None).unwrap();
        let arrow_schema = arrow_schema_a_int();
        let order = LexOrdering::new(vec![PhysicalSortExpr {
            expr: Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Plus,
                lit(1i32),
            )),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }])
        .unwrap();

        let result = plan
            .reorder_by_statistics(&order, &metadata, &arrow_schema)
            .unwrap();

        assert_eq!(result.row_group_indexes, vec![0, 1]);
    }

    /// When the sort column lives outside the file's arrow schema
    /// (e.g. a partition column that reached this method through
    /// `try_pushdown_sort`'s reversed-equivalence branch), reorder is
    /// an expected graceful skip — no `debug_assert!` should fire.
    #[test]
    fn reorder_by_statistics_skips_when_column_not_in_arrow_schema() {
        let metadata = parquet_metadata_with_int_mins(&[50, 10]);
        let plan = PreparedAccessPlan::new(vec![0, 1], None).unwrap();
        // Arrow schema only has "a"; the sort references "b".
        let arrow_schema = arrow_schema_a_int();
        let order = LexOrdering::new(vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("b", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }])
        .unwrap();

        let result = plan
            .reorder_by_statistics(&order, &metadata, &arrow_schema)
            .unwrap();

        assert_eq!(result.row_group_indexes, vec![0, 1]);
    }

    // ----------------------------------------------------------------
    // multi-column `reorder_by_statistics` tests
    // ----------------------------------------------------------------

    /// Two-column int32 schema named "a", "b".
    fn two_col_schema_descr() -> SchemaDescPtr {
        use parquet::basic::Type as PhysicalType;
        use parquet::schema::types::Type as SchemaType;
        let fields = ["a", "b"]
            .iter()
            .map(|name| {
                Arc::new(
                    SchemaType::primitive_type_builder(name, PhysicalType::INT32)
                        .build()
                        .unwrap(),
                )
            })
            .collect();
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .unwrap();
        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }

    /// Build a `ParquetMetaData` with one row group per element of
    /// `mins`: `(min(a), min(b))` per row group, `min == max`.
    fn parquet_metadata_with_two_col_mins(mins: &[(i32, i32)]) -> ParquetMetaData {
        let schema_descr = two_col_schema_descr();
        let row_groups: Vec<RowGroupMetaData> = mins
            .iter()
            .map(|&(a, b)| {
                let columns = [(0, a), (1, b)]
                    .iter()
                    .map(|&(col, m)| {
                        let stats = ParquetStatistics::int32(
                            Some(m),
                            Some(m),
                            None,
                            Some(0),
                            false,
                        );
                        ColumnChunkMetaData::builder(schema_descr.column(col))
                            .set_statistics(stats)
                            .set_num_values(100)
                            .build()
                            .unwrap()
                    })
                    .collect();
                RowGroupMetaData::builder(schema_descr.clone())
                    .set_num_rows(100)
                    .set_column_metadata(columns)
                    .build()
                    .unwrap()
            })
            .collect();
        let file_metadata =
            FileMetaData::new(0, 0, None, None, schema_descr.clone(), None);
        ParquetMetaData::new(file_metadata, row_groups)
    }

    fn arrow_schema_ab_int() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ])
    }

    fn sort_expr(name: &str, index: usize, descending: bool) -> PhysicalSortExpr {
        PhysicalSortExpr {
            expr: Arc::new(Column::new(name, index)),
            options: SortOptions {
                descending,
                nulls_first: true,
            },
        }
    }

    /// `ORDER BY a ASC, b ASC` with the leading key tied everywhere:
    /// the secondary key must break the tie, so RGs order by `min(b)`.
    #[test]
    fn reorder_by_statistics_breaks_leading_ties_with_secondary_column() {
        let metadata =
            parquet_metadata_with_two_col_mins(&[(1, 300), (1, 100), (1, 200)]);
        let plan = PreparedAccessPlan::new(vec![0, 1, 2], None).unwrap();
        let order =
            LexOrdering::new(vec![sort_expr("a", 0, false), sort_expr("b", 1, false)])
                .unwrap();

        let result = plan
            .reorder_by_statistics(&order, &metadata, &arrow_schema_ab_int())
            .unwrap();

        assert_eq!(result.row_group_indexes, vec![1, 2, 0]);
    }

    /// `ORDER BY a ASC, b DESC`: the secondary key's direction is
    /// honored relative to the leading key, so ties on `min(a)` order
    /// by `min(b)` DESC.
    #[test]
    fn reorder_by_statistics_honors_secondary_direction() {
        let metadata =
            parquet_metadata_with_two_col_mins(&[(1, 100), (1, 300), (0, 500)]);
        let plan = PreparedAccessPlan::new(vec![0, 1, 2], None).unwrap();
        let order =
            LexOrdering::new(vec![sort_expr("a", 0, false), sort_expr("b", 1, true)])
                .unwrap();

        let result = plan
            .reorder_by_statistics(&order, &metadata, &arrow_schema_ab_int())
            .unwrap();

        // a=0 first, then the two a=1 groups by b DESC: 300 before 100.
        assert_eq!(result.row_group_indexes, vec![2, 1, 0]);
    }

    /// `ORDER BY a DESC, b DESC` is normalized to ASC lexsort here and
    /// flipped by the later `reverse()`: both keys sort ASC relative to
    /// the leading direction, so reversing yields `(a DESC, b DESC)`.
    #[test]
    fn reorder_by_statistics_normalizes_desc_desc_for_reverse() {
        let metadata =
            parquet_metadata_with_two_col_mins(&[(1, 300), (2, 100), (1, 100)]);
        let plan = PreparedAccessPlan::new(vec![0, 1, 2], None).unwrap();
        let order =
            LexOrdering::new(vec![sort_expr("a", 0, true), sort_expr("b", 1, true)])
                .unwrap();

        let result = plan
            .reorder_by_statistics(&order, &metadata, &arrow_schema_ab_int())
            .unwrap();

        // ASC lexsort of (a, b): (1,100) < (1,300) < (2,100); the later
        // reverse() produces (2,100), (1,300), (1,100) = (a DESC, b DESC).
        assert_eq!(result.row_group_indexes, vec![2, 0, 1]);
    }

    /// A non-`Column` *secondary* expression stops the stats walk but
    /// keeps the leading column's reorder (prefix semantics).
    #[test]
    fn reorder_by_statistics_keeps_leading_prefix_on_non_column_secondary() {
        let metadata =
            parquet_metadata_with_two_col_mins(&[(5, 300), (3, 100), (4, 200)]);
        let plan = PreparedAccessPlan::new(vec![0, 1, 2], None).unwrap();
        let order = LexOrdering::new(vec![
            sort_expr("a", 0, false),
            PhysicalSortExpr {
                expr: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("b", 1)),
                    Operator::Plus,
                    lit(1i32),
                )),
                options: SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            },
        ])
        .unwrap();

        let result = plan
            .reorder_by_statistics(&order, &metadata, &arrow_schema_ab_int())
            .unwrap();

        // Ordered by min(a) ASC only: 3, 4, 5.
        assert_eq!(result.row_group_indexes, vec![1, 2, 0]);
    }
}
