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

use arrow::array::BooleanBufferBuilder;
use arrow::datatypes::Schema;
use datafusion_common::{Result, assert_eq_or_internal_err, exec_err};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use log::debug;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::arrow::push_decoder::RowGroupSelection;
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

    fn remaining_rows(self) -> usize {
        self.current.map_or(0, |s| s.row_count)
            + self.selector_iter.map(|s| s.row_count).sum::<usize>()
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
        selection: RowSelection,
        row_group_meta_data: &[RowGroupMetaData],
    ) -> Result<Self> {
        if let Some(mask) = selection.as_mask() {
            let selection_rows = mask.len();
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

            // Slice the bitmap without materializing selectors. Use row_count()
            // to cache each partial group's count for later preparation.
            let mut offset = 0;
            let row_groups = row_group_meta_data
                .iter()
                .map(|rg| {
                    let row_count = rg.num_rows() as usize;
                    let group_selection =
                        RowSelection::from(mask.slice(offset, row_count));
                    offset += row_count;
                    match group_selection.row_count() {
                        0 => RowGroupAccess::Skip,
                        selected if selected == row_count => RowGroupAccess::Scan,
                        _ => RowGroupAccess::Selection(group_selection),
                    }
                })
                .collect();
            return Ok(Self::new(row_groups));
        }

        // Keep this as a single pass over the selector stream rather than
        // repeatedly calling `RowSelection::split_off` per row group. The
        // `split_off` version is simpler, but it clones/retains substantially
        // more selector buffer capacity for highly fragmented selections.
        let mut cursor = OverallRowSelectionCursor::new(selection);

        let mut selection_rows = 0usize;
        let mut file_rows = 0usize;

        let mut row_groups = Vec::with_capacity(row_group_meta_data.len());
        for rg_meta in row_group_meta_data {
            let rg_rows = rg_meta.num_rows() as usize;
            file_rows += rg_rows;

            let mut builder = RowGroupAccessBuilder::new(rg_rows);
            while builder.remaining > 0 {
                let Some(selector) = cursor.take(builder.remaining) else {
                    break;
                };
                selection_rows += selector.row_count;
                builder.push(selector);
            }

            row_groups.push(builder.into_access());
        }

        selection_rows += cursor.remaining_rows();

        if selection_rows != file_rows {
            return exec_err!(
                "Invalid Parquet RowSelection. File has {file_rows} rows, \
                but selection specifies {selection_rows} rows."
            );
        }

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
                // Parquet preserves bitmap backing only when both operands
                // are masks. Promote selector-backed page pruning to retain
                // an external index's bitmap and use a bitwise intersection.
                // Revisit this conversion once Parquet optimizes mixed-backed
                // intersections: https://github.com/apache/arrow-rs/issues/10423
                let selection = if existing_selection.as_mask().is_some()
                    && selection.as_mask().is_none()
                {
                    let mut mask = BooleanBufferBuilder::new(selection.total_row_count());
                    for selector in selection.iter() {
                        mask.append_n(selector.row_count, !selector.skip);
                    }
                    RowSelection::from(mask.finish())
                } else {
                    selection
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
    /// groups which are not skipped.
    ///
    /// # Deprecated
    ///
    /// DataFusion scans now keep selections local to each row group. For custom
    /// readers, use [`Self::into_inner`] to obtain the [`RowGroupAccess`] entries
    /// and adapt them to the reader's selection API. This compatibility method
    /// retains its existing conversion and length validation behavior.
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
    /// the same number of rows as in its corresponding `row_group_metadata`.
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
    ///   RowGroupAccess::Selection (skip 50, scan 50, skip 900) (scan rows 50-100 in row group 2)
    ///   RowGroupAccess::Scan (scan all row group 3)
    /// ```
    ///
    /// Assuming each row group has 1000 rows, the resulting row selection would
    /// be the rows to scan in row group 0, 2 and 3:
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
    #[deprecated(
        since = "56.0.0",
        note = "Use into_inner() to obtain row-group-local access entries"
    )]
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

    /// Validate selections and prepare row groups for the decoder without
    /// converting local selections into a single file-level selection.
    pub(crate) fn prepare(
        self,
        row_group_meta_data: &[RowGroupMetaData],
    ) -> Result<PreparedAccessPlan> {
        assert_eq!(row_group_meta_data.len(), self.row_groups.len());
        // Keep the scan policies that previously depended on the presence of
        // an overall selection, even when its partial groups are all empty.
        // Local selections no longer require these restrictions to keep row
        // offsets valid; enabling reordering and runtime pruning with selections
        // is left to a follow-up (#24358).
        let has_row_selection = self
            .row_groups
            .iter()
            .any(|access| matches!(access, RowGroupAccess::Selection(_)));
        let mut row_groups = Vec::with_capacity(self.row_groups.len());
        for (index, (access, fully_matched)) in self
            .row_groups
            .into_iter()
            .zip(self.fully_matched)
            .enumerate()
        {
            let selection = match access {
                RowGroupAccess::Skip => continue,
                RowGroupAccess::Scan => {
                    // Preserve strip_empty_row_groups behavior: without an
                    // overall selection it returned the scan list unchanged;
                    // with one it dropped every group selecting zero rows,
                    // including zero-row Scan groups.
                    if has_row_selection && row_group_meta_data[index].num_rows() == 0 {
                        continue;
                    }
                    None
                }
                RowGroupAccess::Selection(selection) => {
                    let rows_in_selection = selection.total_row_count();
                    let row_group_row_count = row_group_meta_data[index].num_rows();
                    assert_eq_or_internal_err!(
                        rows_in_selection as i64,
                        row_group_row_count,
                        "Invalid ParquetAccessPlan Selection. Row group {index} has {row_group_row_count} rows \
                            but selection only specifies {rows_in_selection} rows. \
                            Selection: {selection:?}"
                    );
                    // Intersections can leave an empty selection: drop the
                    // group together with its match status.
                    if selection.row_count() == 0 {
                        continue;
                    }
                    Some(selection)
                }
            };
            row_groups.push(PreparedRowGroup {
                selection: RowGroupSelection::new(index, selection),
                fully_matched,
            });
        }
        Ok(PreparedAccessPlan {
            has_row_selection: has_row_selection && !row_groups.is_empty(),
            row_groups,
        })
    }
}

/// A row group's selection and static match status travel together when the
/// scan order changes.
#[derive(Debug)]
pub(crate) struct PreparedRowGroup {
    pub(crate) selection: RowGroupSelection,
    /// Whether statistics proved every row passes the predicate.
    pub(crate) fully_matched: bool,
}

/// The final scan order and row-group-local selections passed to the decoder.
#[derive(Debug)]
pub(crate) struct PreparedAccessPlan {
    pub(crate) row_groups: Vec<PreparedRowGroup>,
    /// Preserve the existing reordering and dynamic-pruning eligibility policy.
    /// This can remain true after an empty selection's group is removed.
    pub(crate) has_row_selection: bool,
}

impl PreparedAccessPlan {
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
    /// - There are row selections (preserve the existing scan-order policy)
    /// - 0 or 1 row groups (nothing to reorder)
    /// - The leading sort expression is not a simple column reference
    /// - Statistics are unavailable
    pub(crate) fn reorder_by_statistics(
        mut self,
        sort_order: &LexOrdering,
        file_metadata: &ParquetMetaData,
        arrow_schema: &Schema,
    ) -> Result<Self> {
        // Preserve the current policy for scans with page or external selections.
        if self.has_row_selection {
            debug!("Skipping RG reorder: row_selection present");
            return Ok(self);
        }

        // Nothing to reorder
        if self.row_groups.len() <= 1 {
            return Ok(self);
        }

        let rg_metadata: Vec<&RowGroupMetaData> = self
            .row_groups
            .iter()
            .map(|rg| file_metadata.row_group(rg.selection.row_group_index()))
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

        // Move each selection together with its match status, without cloning
        // potentially large selection buffers just to reorder row groups.
        let mut original: Vec<_> = self.row_groups.into_iter().map(Some).collect();
        self.row_groups = sorted_indices
            .values()
            .iter()
            .map(|&i| original[i as usize].take().expect("unique sort index"))
            .collect();
        Ok(self)
    }

    /// Reverse row-group order while preserving local row coordinates.
    pub(crate) fn reverse(mut self) -> Self {
        self.row_groups.reverse();
        self
    }

    #[cfg(test)]
    pub(crate) fn row_group_indexes(&self) -> Vec<usize> {
        self.row_groups
            .iter()
            .map(|rg| rg.selection.row_group_index())
            .collect()
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
    #[expect(deprecated)]
    fn test_deprecated_overall_row_selection() {
        for plan in [
            ParquetAccessPlan::new_all(4),
            ParquetAccessPlan::new_none(4),
        ] {
            assert_eq!(
                plan.into_overall_row_selection(&ROW_GROUP_METADATA)
                    .unwrap(),
                None
            );
        }

        // Skipped groups do not occupy coordinates in the combined selection.
        // Check both input representations retain the same conversion behavior.
        let selectors =
            RowSelection::from(vec![RowSelector::skip(10), RowSelector::select(20)]);
        let bitmap = RowSelection::from(BooleanBuffer::from(
            (0..30).map(|i| i >= 10).collect::<Vec<_>>(),
        ));
        for selection in [selectors, bitmap] {
            let plan = ParquetAccessPlan::new(vec![
                RowGroupAccess::Scan,
                RowGroupAccess::Skip,
                RowGroupAccess::Selection(selection),
                RowGroupAccess::Scan,
            ]);
            assert_eq!(
                plan.into_overall_row_selection(&ROW_GROUP_METADATA)
                    .unwrap(),
                Some(RowSelection::from(vec![
                    RowSelector::select(10),
                    RowSelector::skip(10),
                    RowSelector::select(60),
                ]))
            );
        }

        let plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Selection(vec![RowSelector::skip(10)].into()),
            RowGroupAccess::Skip,
            RowGroupAccess::Skip,
            RowGroupAccess::Skip,
        ]);
        assert_eq!(
            plan.into_overall_row_selection(&ROW_GROUP_METADATA)
                .unwrap(),
            Some(RowSelection::from(vec![RowSelector::skip(10)]))
        );
    }

    #[test]
    #[expect(deprecated)]
    fn test_deprecated_overall_row_selection_validates_length() {
        for rows in [19, 21] {
            let plan = ParquetAccessPlan::new(vec![
                RowGroupAccess::Scan,
                RowGroupAccess::Selection(vec![RowSelector::select(rows)].into()),
                RowGroupAccess::Skip,
                RowGroupAccess::Skip,
            ]);
            let err = plan
                .into_overall_row_selection(&ROW_GROUP_METADATA)
                .unwrap_err()
                .to_string();
            assert_contains!(
                err,
                format!(
                    "Row group 1 has 20 rows but selection only specifies {rows} rows"
                )
            );
        }
    }

    #[test]
    fn test_only_scans() {
        let plan = ParquetAccessPlan::new_all(4)
            .prepare(&ROW_GROUP_METADATA)
            .unwrap();
        assert_eq!(plan.row_group_indexes(), vec![0, 1, 2, 3]);
        assert!(!plan.has_row_selection);
        assert!(
            plan.row_groups
                .iter()
                .all(|rg| rg.selection.selection().is_none())
        );
    }

    #[test]
    fn test_only_skips() {
        let plan = ParquetAccessPlan::new_none(4)
            .prepare(&ROW_GROUP_METADATA)
            .unwrap();
        assert!(plan.row_groups.is_empty());
        assert!(!plan.has_row_selection);
    }

    #[test]
    fn test_mixed_selections() {
        let selection = RowSelection::from(vec![
            RowSelector::select(5),
            RowSelector::skip(7),
            RowSelector::select(18),
        ]);
        let plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Skip,
            RowGroupAccess::Scan,
            RowGroupAccess::Selection(selection.clone()),
            RowGroupAccess::Scan,
        ])
        .prepare(&ROW_GROUP_METADATA)
        .unwrap();
        assert_eq!(plan.row_group_indexes(), vec![1, 2, 3]);
        assert!(plan.has_row_selection);
        assert_eq!(
            plan.row_groups[0].selection,
            RowGroupSelection::new(1, None)
        );
        assert_eq!(
            plan.row_groups[1].selection,
            RowGroupSelection::new(2, Some(selection))
        );
        assert_eq!(
            plan.row_groups[2].selection,
            RowGroupSelection::new(3, None)
        );
    }

    fn scan_plan(indexes: Vec<usize>) -> PreparedAccessPlan {
        PreparedAccessPlan {
            has_row_selection: false,
            row_groups: indexes
                .into_iter()
                .map(|index| PreparedRowGroup {
                    selection: RowGroupSelection::new(index, None),
                    fully_matched: false,
                })
                .collect(),
        }
    }

    #[test]
    fn test_scan_selection_preserves_mask_backing() {
        let mask = BooleanBuffer::from(vec![
            true, true, false, false, true, true, false, false, true, true,
        ]);
        let selectors =
            RowSelection::from(vec![RowSelector::select(5), RowSelector::skip(5)]);
        // Both selector-backed page pruning and bitmap intersections retain
        // the existing mask, including when no rows survive.
        for incoming in [
            selectors,
            RowSelection::from(BooleanBuffer::from(vec![
                true, true, true, true, true, false, false, false, false, false,
            ])),
            RowSelection::from(vec![RowSelector::skip(10)]),
        ] {
            let empty = incoming.row_count() == 0;
            let mut plan = ParquetAccessPlan::new(vec![RowGroupAccess::Selection(
                RowSelection::from(mask.clone()),
            )]);
            plan.scan_selection(0, incoming);
            let RowGroupAccess::Selection(selection) = &plan.inner()[0] else {
                panic!("expected selection");
            };
            let expected = if empty {
                BooleanBuffer::new_unset(10)
            } else {
                BooleanBuffer::from(vec![
                    true, true, false, false, true, false, false, false, false, false,
                ])
            };
            assert_eq!(selection.as_mask(), Some(&expected));
        }
    }

    #[test]
    fn test_scan_selection_preserves_selector_backing() {
        let mut plan = ParquetAccessPlan::new(vec![RowGroupAccess::Selection(
            RowSelection::from(vec![RowSelector::select(6), RowSelector::skip(4)]),
        )]);
        plan.scan_selection(
            0,
            RowSelection::from(vec![
                RowSelector::skip(2),
                RowSelector::select(5),
                RowSelector::skip(3),
            ]),
        );
        let RowGroupAccess::Selection(selection) = &plan.inner()[0] else {
            panic!("expected selection");
        };
        assert!(selection.as_mask().is_none());
        assert_eq!(
            selection,
            &RowSelection::from(vec![
                RowSelector::skip(2),
                RowSelector::select(4),
                RowSelector::skip(4),
            ])
        );
    }

    #[test]
    fn test_new_from_overall_mask_preserves_bitmap_backing() {
        // Include all-selected, all-skipped, and fragmented groups. Start at
        // a non-byte-aligned offset to exercise slicing an existing bitmap.
        let mut bits = vec![false; 3];
        bits.extend(vec![true; 10]);
        bits.extend(vec![false; 20]);
        bits.extend((0..30).map(|i| i % 2 == 1));
        bits.extend(vec![true; 40]);
        let mask = BooleanBuffer::from(bits).slice(3, 100);
        let plan = ParquetAccessPlan::try_new_from_overall_row_selection(
            RowSelection::from(mask.clone()),
            &ROW_GROUP_METADATA,
        )
        .unwrap();
        assert_eq!(plan.inner()[0], RowGroupAccess::Scan);
        assert_eq!(plan.inner()[1], RowGroupAccess::Skip);
        assert_eq!(plan.inner()[3], RowGroupAccess::Scan);
        let RowGroupAccess::Selection(selection) = &plan.inner()[2] else {
            panic!("expected selection");
        };
        assert_eq!(selection.as_mask(), Some(&mask.slice(30, 30)));

        // The local selection must also survive preparation and reversal.
        let prepared = plan.prepare(&ROW_GROUP_METADATA).unwrap().reverse();
        assert_eq!(prepared.row_group_indexes(), vec![3, 2, 0]);
        assert!(prepared.row_groups[0].selection.selection().is_none());
        assert!(prepared.row_groups[2].selection.selection().is_none());
        assert_eq!(
            prepared.row_groups[1]
                .selection
                .selection()
                .unwrap()
                .as_mask(),
            Some(&mask.slice(30, 30))
        );
    }

    #[test]
    fn test_new_from_overall_mask_invalid_row_count() {
        for selection_rows in [99, 101] {
            let err = ParquetAccessPlan::try_new_from_overall_row_selection(
                RowSelection::from(BooleanBuffer::new_set(selection_rows)),
                &ROW_GROUP_METADATA,
            )
            .unwrap_err()
            .to_string();
            assert_contains!(
                err,
                format!(
                    "Invalid Parquet RowSelection. File has 100 rows, \
                     but selection specifies {selection_rows} rows"
                )
            );
        }
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
            .prepare(&ROW_GROUP_METADATA)
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
            .prepare(&ROW_GROUP_METADATA)
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
        let mut plan = scan_plan(vec![0, 1, 2]);
        plan.row_groups[1].fully_matched = true;

        let result = plan
            .reorder_by_statistics(
                &lex_ordering_a_asc(),
                &metadata,
                &arrow_schema_a_int(),
            )
            .unwrap();

        assert_eq!(result.row_group_indexes(), vec![1, 0, 2]);
        assert_eq!(
            result
                .row_groups
                .iter()
                .map(|rg| rg.fully_matched)
                .collect::<Vec<_>>(),
            vec![true, false, false]
        );
    }

    /// Preserve the existing scan order when selections are present.
    #[test]
    fn reorder_by_statistics_skips_when_row_selection_present() {
        let metadata = parquet_metadata_with_int_mins(&[50, 10]);
        let selection = RowSelection::from(vec![RowSelector::select(100)]);
        let mut plan = scan_plan(vec![0, 1]);
        plan.row_groups[0].selection = RowGroupSelection::new(0, Some(selection));
        plan.has_row_selection = true;

        let result = plan
            .reorder_by_statistics(
                &lex_ordering_a_asc(),
                &metadata,
                &arrow_schema_a_int(),
            )
            .unwrap();

        assert_eq!(result.row_group_indexes(), vec![0, 1]);
    }

    /// One row group means nothing to reorder.
    #[test]
    fn reorder_by_statistics_skips_when_at_most_one_row_group() {
        let metadata = parquet_metadata_with_int_mins(&[50]);
        let plan = scan_plan(vec![0]);

        let result = plan
            .reorder_by_statistics(
                &lex_ordering_a_asc(),
                &metadata,
                &arrow_schema_a_int(),
            )
            .unwrap();

        assert_eq!(result.row_group_indexes(), vec![0]);
    }

    /// Non-`Column` sort expressions (e.g. `a + 1`,
    /// `date_trunc(...)`) can't drive a stats lookup, so reorder is
    /// skipped. The opener falls back to whatever order it received.
    #[test]
    fn reorder_by_statistics_skips_for_non_column_sort_expr() {
        let metadata = parquet_metadata_with_int_mins(&[50, 10]);
        let plan = scan_plan(vec![0, 1]);
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

        assert_eq!(result.row_group_indexes(), vec![0, 1]);
    }

    /// When the sort column lives outside the file's arrow schema
    /// (e.g. a partition column that reached this method through
    /// `try_pushdown_sort`'s reversed-equivalence branch), reorder is
    /// an expected graceful skip — no `debug_assert!` should fire.
    #[test]
    fn reorder_by_statistics_skips_when_column_not_in_arrow_schema() {
        let metadata = parquet_metadata_with_int_mins(&[50, 10]);
        let plan = scan_plan(vec![0, 1]);
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

        assert_eq!(result.row_group_indexes(), vec![0, 1]);
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
        let plan = scan_plan(vec![0, 1, 2]);
        let order =
            LexOrdering::new(vec![sort_expr("a", 0, false), sort_expr("b", 1, false)])
                .unwrap();

        let result = plan
            .reorder_by_statistics(&order, &metadata, &arrow_schema_ab_int())
            .unwrap();

        assert_eq!(result.row_group_indexes(), vec![1, 2, 0]);
    }

    /// `ORDER BY a ASC, b DESC`: the secondary key's direction is
    /// honored relative to the leading key, so ties on `min(a)` order
    /// by `min(b)` DESC.
    #[test]
    fn reorder_by_statistics_honors_secondary_direction() {
        let metadata =
            parquet_metadata_with_two_col_mins(&[(1, 100), (1, 300), (0, 500)]);
        let plan = scan_plan(vec![0, 1, 2]);
        let order =
            LexOrdering::new(vec![sort_expr("a", 0, false), sort_expr("b", 1, true)])
                .unwrap();

        let result = plan
            .reorder_by_statistics(&order, &metadata, &arrow_schema_ab_int())
            .unwrap();

        // a=0 first, then the two a=1 groups by b DESC: 300 before 100.
        assert_eq!(result.row_group_indexes(), vec![2, 1, 0]);
    }

    /// `ORDER BY a DESC, b DESC` is normalized to ASC lexsort here and
    /// flipped by the later `reverse()`: both keys sort ASC relative to
    /// the leading direction, so reversing yields `(a DESC, b DESC)`.
    #[test]
    fn reorder_by_statistics_normalizes_desc_desc_for_reverse() {
        let metadata =
            parquet_metadata_with_two_col_mins(&[(1, 300), (2, 100), (1, 100)]);
        let plan = scan_plan(vec![0, 1, 2]);
        let order =
            LexOrdering::new(vec![sort_expr("a", 0, true), sort_expr("b", 1, true)])
                .unwrap();

        let result = plan
            .reorder_by_statistics(&order, &metadata, &arrow_schema_ab_int())
            .unwrap();

        // ASC lexsort of (a, b): (1,100) < (1,300) < (2,100); the later
        // reverse() produces (2,100), (1,300), (1,100) = (a DESC, b DESC).
        assert_eq!(result.row_group_indexes(), vec![2, 0, 1]);
    }

    /// A non-`Column` *secondary* expression stops the stats walk but
    /// keeps the leading column's reorder (prefix semantics).
    #[test]
    fn reorder_by_statistics_keeps_leading_prefix_on_non_column_secondary() {
        let metadata =
            parquet_metadata_with_two_col_mins(&[(5, 300), (3, 100), (4, 200)]);
        let plan = scan_plan(vec![0, 1, 2]);
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
        assert_eq!(result.row_group_indexes(), vec![1, 2, 0]);
    }

    #[test]
    fn test_prepare_drops_empty_selections_and_keeps_match_status() {
        let mut plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            RowGroupAccess::Selection(RowSelection::from(vec![RowSelector::skip(20)])),
            RowGroupAccess::Selection(RowSelection::from(vec![
                RowSelector::skip(10),
                RowSelector::select(20),
            ])),
            RowGroupAccess::Selection(RowSelection::from(vec![RowSelector::skip(40)])),
        ]);
        plan.fully_matched = vec![true, false, false, true];
        let prepared = plan.prepare(&ROW_GROUP_METADATA).unwrap();
        assert_eq!(prepared.row_group_indexes(), vec![0, 2]);
        assert!(prepared.row_groups[0].fully_matched);
        assert!(!prepared.row_groups[1].fully_matched);
        assert_eq!(
            prepared.row_groups[1].selection.selection(),
            Some(&RowSelection::from(vec![
                RowSelector::skip(10),
                RowSelector::select(20)
            ]))
        );
    }

    #[test]
    fn test_prepare_preserves_selection_policy_after_dropping_empty_group() {
        let prepared = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            RowGroupAccess::Selection(RowSelection::from(vec![RowSelector::skip(20)])),
            RowGroupAccess::Skip,
            RowGroupAccess::Skip,
        ])
        .prepare(&ROW_GROUP_METADATA)
        .unwrap();
        assert_eq!(prepared.row_group_indexes(), vec![0]);
        assert!(prepared.row_groups[0].selection.selection().is_none());
        assert!(prepared.has_row_selection);
    }

    #[test]
    fn test_prepare_strips_row_group_emptied_by_intersecting_selections() {
        // The reachable producer: `scan_selection` intersects two disjoint
        // selections within RG 1 to nothing, leaving an empty `Selection` in
        // the plan (its own `rows_selected > 0` guard checks the incoming
        // selection, not the intersection). `prepare` must strip that row
        // group so the prepared plan never names a row group the decoder would
        // silently skip.
        let mut plan = ParquetAccessPlan::new_all(4); // RGs [10, 20, 30, 40]

        // RG 1 (20 rows): select rows 0..10, then intersect with rows 10..20.
        plan.scan_selection(
            1,
            RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(10)]),
        );
        plan.scan_selection(
            1,
            RowSelection::from(vec![RowSelector::skip(10), RowSelector::select(10)]),
        );
        // RG 2 keeps a genuine partial selection.
        plan.scan_selection(
            2,
            RowSelection::from(vec![RowSelector::skip(10), RowSelector::select(20)]),
        );

        let prepared = plan.prepare(&ROW_GROUP_METADATA).expect("prepare");

        // RG 1 (emptied by the intersection) is stripped; RG 0/2/3 remain.
        assert_eq!(prepared.row_group_indexes(), vec![0, 2, 3]);
    }
}
