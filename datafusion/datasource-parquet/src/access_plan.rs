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
use arrow::datatypes::Schema;
use datafusion_common::{Result, assert_eq_or_internal_err};
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

impl ParquetAccessPlan {
    /// Create a new `ParquetAccessPlan` that scans all row groups
    pub fn new_all(row_group_count: usize) -> Self {
        Self {
            row_groups: vec![RowGroupAccess::Scan; row_group_count],
        }
    }

    /// Create a new `ParquetAccessPlan` that scans no row groups
    pub fn new_none(row_group_count: usize) -> Self {
        Self {
            row_groups: vec![RowGroupAccess::Skip; row_group_count],
        }
    }

    /// Create a new `ParquetAccessPlan` from the specified [`RowGroupAccess`]es
    pub fn new(row_groups: Vec<RowGroupAccess>) -> Self {
        Self { row_groups }
    }

    /// Set the i-th row group to the specified [`RowGroupAccess`]
    pub fn set(&mut self, idx: usize, access: RowGroupAccess) {
        self.row_groups[idx] = access;
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

    /// Return a reference to the row group indexes.
    pub(crate) fn row_group_indexes(&self) -> &[usize] {
        &self.row_group_indexes
    }

    /// Keep only the first `count` row groups, dropping the rest.
    /// Used for TopK cumulative pruning after reorder + reverse.
    pub(crate) fn truncate_row_groups(mut self, count: usize) -> Self {
        self.row_group_indexes.truncate(count);
        // Clear row_selection since it's tied to the original RG set
        if self.row_selection.is_some() {
            self.row_selection = None;
        }
        self
    }

    /// Reorder row groups by their min statistics for the given sort order.
    ///
    /// This helps TopK queries find optimal values first. For ASC sort,
    /// row groups with the smallest min values come first. For DESC sort,
    /// row groups with the largest min values come first.
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

        // Get the first sort expression
        // LexOrdering is guaranteed non-empty, so first() returns &PhysicalSortExpr
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

        // Always sort by min values in ASC order to align row groups with
        // the file's declared output ordering. Direction (DESC) is handled
        // separately by ReverseRowGroups which is applied AFTER reorder.
        //
        // This composable design avoids the problem where reorder(DESC)
        // followed by reverse would double-flip the order, and ensures
        // that for already-sorted data, reorder is a no-op and reverse
        // gives the correct DESC order (including placing small tail RGs first).
        let rg_metadata: Vec<&RowGroupMetaData> = self
            .row_group_indexes
            .iter()
            .map(|&idx| file_metadata.row_group(idx))
            .collect();

        let stat_values = match converter.row_group_mins(rg_metadata.iter().copied()) {
            Ok(vals) => vals,
            Err(e) => {
                debug!("Skipping RG reorder: cannot get min values: {e}");
                return Ok(self);
            }
        };

        // Always sort ASC by min values — direction is handled by reverse
        let sort_options = arrow::compute::SortOptions {
            descending: false,
            nulls_first: first_sort_expr.options.nulls_first,
        };
        let sorted_indices =
            match arrow::compute::sort_to_indices(&stat_values, Some(sort_options), None)
            {
                Ok(indices) => indices,
                Err(e) => {
                    debug!("Skipping RG reorder: sort failed: {e}");
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

    // ---- reorder_by_statistics tests ----

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
    use parquet::basic::Type as PhysicalType;
    use parquet::file::metadata::FileMetaData;
    use parquet::file::statistics::Statistics;
    use parquet::schema::types::Type as SchemaType;

    /// Create ParquetMetaData with row groups that have Int32 min/max stats
    fn make_metadata_with_stats(min_max_pairs: &[(i32, i32)]) -> ParquetMetaData {
        let field = SchemaType::primitive_type_builder("id", PhysicalType::INT32)
            .build()
            .unwrap();
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(field)])
            .build()
            .unwrap();
        let schema_descr = Arc::new(SchemaDescriptor::new(Arc::new(schema)));

        let row_groups: Vec<RowGroupMetaData> = min_max_pairs
            .iter()
            .map(|(min, max)| {
                let stats =
                    Statistics::int32(Some(*min), Some(*max), None, Some(100), false);
                let column = ColumnChunkMetaData::builder(schema_descr.column(0))
                    .set_num_values(100)
                    .set_statistics(stats)
                    .build()
                    .unwrap();
                RowGroupMetaData::builder(schema_descr.clone())
                    .set_num_rows(100)
                    .set_column_metadata(vec![column])
                    .build()
                    .unwrap()
            })
            .collect();

        let file_meta = FileMetaData::new(
            1,
            min_max_pairs.len() as i64 * 100,
            None,
            None,
            schema_descr,
            None,
        );
        ParquetMetaData::new(file_meta, row_groups)
    }

    fn make_sort_order_asc() -> LexOrdering {
        LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
            "id", 0,
        )))])
        .unwrap()
    }

    fn make_sort_order_desc() -> LexOrdering {
        use arrow::compute::SortOptions;
        LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("id", 0)),
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        )])
        .unwrap()
    }

    fn make_arrow_schema() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Int32, false)])
    }

    #[test]
    fn test_reorder_by_statistics_asc() {
        // RGs in wrong order: [50-99, 200-299, 1-30]
        let metadata = make_metadata_with_stats(&[(50, 99), (200, 299), (1, 30)]);
        let schema = make_arrow_schema();
        let sort_order = make_sort_order_asc();

        let plan = PreparedAccessPlan::new(vec![0, 1, 2], None).unwrap();
        let plan = plan
            .reorder_by_statistics(&sort_order, &metadata, &schema)
            .unwrap();

        // Should be reordered: RG2(1-30), RG0(50-99), RG1(200-299)
        assert_eq!(plan.row_group_indexes, vec![2, 0, 1]);
    }

    #[test]
    fn test_reorder_by_statistics_desc_sorts_asc() {
        // reorder_by_statistics always sorts by min ASC regardless of sort
        // direction. DESC is handled separately by ReverseRowGroups which
        // is applied after reorder in the optimizer pipeline.
        //
        // RGs: [50-99, 200-299, 1-30]
        let metadata = make_metadata_with_stats(&[(50, 99), (200, 299), (1, 30)]);
        let schema = make_arrow_schema();
        let sort_order = make_sort_order_desc();

        let plan = PreparedAccessPlan::new(vec![0, 1, 2], None).unwrap();
        let plan = plan
            .reorder_by_statistics(&sort_order, &metadata, &schema)
            .unwrap();

        // Always ASC by min: RG2(min=1), RG0(min=50), RG1(min=200)
        // Reverse is applied separately for DESC queries.
        assert_eq!(plan.row_group_indexes, vec![2, 0, 1]);
    }

    #[test]
    fn test_reorder_by_statistics_single_rg() {
        let metadata = make_metadata_with_stats(&[(1, 100)]);
        let schema = make_arrow_schema();
        let sort_order = make_sort_order_asc();

        let plan = PreparedAccessPlan::new(vec![0], None).unwrap();
        let plan = plan
            .reorder_by_statistics(&sort_order, &metadata, &schema)
            .unwrap();

        // Single RG, no reorder
        assert_eq!(plan.row_group_indexes, vec![0]);
    }

    #[test]
    fn test_reorder_by_statistics_with_skipped_rgs() {
        // 4 RGs but only 0, 2, 3 are selected (RG1 was pruned)
        let metadata =
            make_metadata_with_stats(&[(300, 400), (100, 200), (1, 50), (50, 99)]);
        let schema = make_arrow_schema();
        let sort_order = make_sort_order_asc();

        let plan = PreparedAccessPlan::new(vec![0, 2, 3], None).unwrap();
        let plan = plan
            .reorder_by_statistics(&sort_order, &metadata, &schema)
            .unwrap();

        // Reorder selected RGs by min: RG2(1-50), RG3(50-99), RG0(300-400)
        assert_eq!(plan.row_group_indexes, vec![2, 3, 0]);
    }

    #[test]
    fn test_reorder_by_statistics_skips_with_row_selection() {
        let metadata = make_metadata_with_stats(&[(50, 99), (1, 30)]);
        let schema = make_arrow_schema();
        let sort_order = make_sort_order_asc();

        let selection = RowSelection::from(vec![
            RowSelector::select(50),
            RowSelector::skip(50),
            RowSelector::select(100),
        ]);

        let plan = PreparedAccessPlan::new(vec![0, 1], Some(selection)).unwrap();
        let plan = plan
            .reorder_by_statistics(&sort_order, &metadata, &schema)
            .unwrap();

        // Should NOT reorder because row_selection is present
        assert_eq!(plan.row_group_indexes, vec![0, 1]);
    }

    #[test]
    fn test_reorder_by_statistics_already_sorted() {
        // Already in correct ASC order
        let metadata = make_metadata_with_stats(&[(1, 30), (50, 99), (200, 299)]);
        let schema = make_arrow_schema();
        let sort_order = make_sort_order_asc();

        let plan = PreparedAccessPlan::new(vec![0, 1, 2], None).unwrap();
        let plan = plan
            .reorder_by_statistics(&sort_order, &metadata, &schema)
            .unwrap();

        // Already sorted, order preserved
        assert_eq!(plan.row_group_indexes, vec![0, 1, 2]);
    }

    #[test]
    fn test_reorder_by_statistics_skips_non_column_expr() {
        use datafusion_expr::Operator;
        use datafusion_physical_expr::expressions::BinaryExpr;

        let metadata = make_metadata_with_stats(&[(50, 99), (1, 30)]);
        let schema = make_arrow_schema();

        // Sort expression is a binary expression (id + 1), not a simple column
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 0)),
            Operator::Plus,
            Arc::new(datafusion_physical_expr::expressions::Literal::new(
                datafusion_common::ScalarValue::Int32(Some(1)),
            )),
        ));
        let sort_order =
            LexOrdering::new(vec![PhysicalSortExpr::new_default(expr)]).unwrap();

        let plan = PreparedAccessPlan::new(vec![0, 1], None).unwrap();
        let plan = plan
            .reorder_by_statistics(&sort_order, &metadata, &schema)
            .unwrap();

        // Should NOT reorder because sort expr is not a simple column
        assert_eq!(plan.row_group_indexes, vec![0, 1]);
    }

    #[test]
    fn test_reorder_by_statistics_overlapping_rgs_sorts_asc() {
        // Overlapping ranges — reorder always uses min ASC:
        //   RG0: 50-60
        //   RG1: 40-100 (lower min, wider range)
        //   RG2: 20-30 (lowest min)
        //
        // Sorted by min ASC: [RG2(20), RG1(40), RG0(50)]
        // For DESC queries, ReverseRowGroups is applied after to flip order.
        let metadata = make_metadata_with_stats(&[(50, 60), (40, 100), (20, 30)]);
        let schema = make_arrow_schema();
        let sort_order = make_sort_order_desc();

        let plan = PreparedAccessPlan::new(vec![0, 1, 2], None).unwrap();
        let plan = plan
            .reorder_by_statistics(&sort_order, &metadata, &schema)
            .unwrap();

        // Always ASC by min: RG2(min=20), RG1(min=40), RG0(min=50)
        assert_eq!(plan.row_group_indexes, vec![2, 1, 0]);
    }

    #[test]
    fn test_reorder_by_statistics_skips_missing_column() {
        let metadata = make_metadata_with_stats(&[(50, 99), (1, 30)]);
        // Schema has "id" but sort order references "nonexistent"
        let schema = make_arrow_schema();
        let sort_order = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(
            Column::new("nonexistent", 99),
        ))])
        .unwrap();

        let plan = PreparedAccessPlan::new(vec![0, 1], None).unwrap();
        let plan = plan
            .reorder_by_statistics(&sort_order, &metadata, &schema)
            .unwrap();

        // Should NOT reorder because column not found in schema
        assert_eq!(plan.row_group_indexes, vec![0, 1]);
    }
}
