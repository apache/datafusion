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

use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::RowGroupMetaData;

/// A selection of rows and row groups within a ParquetFile to decode.
///
/// A `ParquetAccessPlan` is used to limit the row groups and data pages a `ParquetExec`
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
/// # use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
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
    ) -> Option<RowSelection> {
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
            return None;
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

        Some(total_selection)
    }

    /// Return an iterator over the row group indexes that should be scanned
    pub fn row_group_index_iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.row_groups.iter().enumerate().filter_map(|(idx, b)| {
            if b.should_scan() {
                Some(idx)
            } else {
                None
            }
        })
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
}

#[cfg(test)]
mod test {
    use super::*;
    use parquet::basic::LogicalType;
    use parquet::file::metadata::ColumnChunkMetaData;
    use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor};
    use std::sync::{Arc, OnceLock};

    #[test]
    fn test_only_scans() {
        let access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
        ]);

        let row_group_indexes = access_plan.row_group_indexes();
        let row_selection = access_plan.into_overall_row_selection(row_group_metadata());

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
        let row_selection = access_plan.into_overall_row_selection(row_group_metadata());

        // skip all row groups, no selection
        assert_eq!(row_group_indexes, vec![] as Vec<usize>);
        assert_eq!(row_selection, None);
    }
    #[test]
    fn test_mixed_1() {
        let access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            RowGroupAccess::Selection(
                vec![RowSelector::select(5), RowSelector::skip(7)].into(),
            ),
            RowGroupAccess::Skip,
            RowGroupAccess::Skip,
        ]);

        let row_group_indexes = access_plan.row_group_indexes();
        let row_selection = access_plan.into_overall_row_selection(row_group_metadata());

        assert_eq!(row_group_indexes, vec![0, 1]);
        assert_eq!(
            row_selection,
            Some(
                vec![
                    // select the entire first row group
                    RowSelector::select(10),
                    // selectors from the second row group
                    RowSelector::select(5),
                    RowSelector::skip(7)
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
                vec![RowSelector::select(5), RowSelector::skip(7)].into(),
            ),
            RowGroupAccess::Scan,
        ]);

        let row_group_indexes = access_plan.row_group_indexes();
        let row_selection = access_plan.into_overall_row_selection(row_group_metadata());

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
                    // select the entire fourth row group
                    RowSelector::select(40),
                ]
                .into()
            )
        );
    }

    static ROW_GROUP_METADATA: OnceLock<Vec<RowGroupMetaData>> = OnceLock::new();

    /// [`RowGroupMetaData`] that returns 4 row groups with 10, 20, 30, 40 rows
    /// respectively
    fn row_group_metadata() -> &'static [RowGroupMetaData] {
        ROW_GROUP_METADATA.get_or_init(|| {
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
        })
    }

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
