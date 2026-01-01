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

//! Sort-related utilities for Parquet scanning

use datafusion_common::Result;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::ParquetMetaData;
use std::collections::HashMap;

/// Reverse a row selection to match reversed row group order.
///
/// When scanning row groups in reverse order, we need to adjust the row selection
/// to account for the new ordering. This function:
/// 1. Maps each selection to its corresponding row group
/// 2. Reverses the order of row groups
/// 3. Reconstructs the row selection for the new order
///
/// # Arguments
/// * `row_selection` - Original row selection (only covers row groups that are scanned)
/// * `parquet_metadata` - Metadata containing row group information
/// * `row_groups_to_scan` - Indexes of row groups that will be scanned (in original order)
///
/// # Returns
/// A new `RowSelection` adjusted for reversed row group order
///
/// # Important Notes
/// The input `row_selection` only covers the row groups specified in `row_groups_to_scan`.
/// Row groups that are skipped (not in `row_groups_to_scan`) are not represented in the
/// `row_selection` at all. This function needs `row_groups_to_scan` to correctly map
/// the selection back to the original row groups.
pub fn reverse_row_selection(
    row_selection: &RowSelection,
    parquet_metadata: &ParquetMetaData,
    row_groups_to_scan: &[usize],
) -> Result<RowSelection> {
    let rg_metadata = parquet_metadata.row_groups();

    // Build a mapping of row group index to its row range, but ONLY for
    // the row groups that are actually being scanned.
    //
    // IMPORTANT: The row numbers in this mapping are RELATIVE to the scanned row groups,
    // not absolute positions in the file.
    //
    // Example: If row_groups_to_scan = [0, 2, 3] and each has 100 rows:
    //   RG0: rows 0-99 (relative to scanned data)
    //   RG2: rows 100-199 (relative to scanned data, NOT 200-299 in file!)
    //   RG3: rows 200-299 (relative to scanned data, NOT 300-399 in file!)
    let mut rg_row_ranges: Vec<(usize, usize, usize)> =
        Vec::with_capacity(row_groups_to_scan.len());
    let mut current_row = 0;
    for &rg_idx in row_groups_to_scan {
        let rg = &rg_metadata[rg_idx];
        let num_rows = rg.num_rows() as usize;
        rg_row_ranges.push((rg_idx, current_row, current_row + num_rows));
        current_row += num_rows; // This is relative row number, NOT absolute file position
    }

    // Map selections to row groups
    let mut rg_selections: HashMap<usize, Vec<RowSelector>> = HashMap::new();

    let mut current_file_row = 0;
    for selector in row_selection.iter() {
        let selector_end = current_file_row + selector.row_count;

        // Find which row groups this selector spans
        for (rg_idx, rg_start, rg_end) in rg_row_ranges.iter() {
            if current_file_row < *rg_end && selector_end > *rg_start {
                // This selector overlaps with this row group
                let overlap_start = current_file_row.max(*rg_start);
                let overlap_end = selector_end.min(*rg_end);
                let overlap_count = overlap_end - overlap_start;

                if overlap_count > 0 {
                    let entry = rg_selections.entry(*rg_idx).or_default();
                    if selector.skip {
                        entry.push(RowSelector::skip(overlap_count));
                    } else {
                        entry.push(RowSelector::select(overlap_count));
                    }
                }
            }
        }

        current_file_row = selector_end;
    }

    // Build new selection for reversed row group order
    // Only iterate over the row groups that are being scanned, in reverse order
    let mut reversed_selectors = Vec::new();
    for &rg_idx in row_groups_to_scan.iter().rev() {
        if let Some(selectors) = rg_selections.get(&rg_idx) {
            reversed_selectors.extend(selectors.iter().cloned());
        } else {
            // No specific selection for this row group means select all rows in it
            if let Some((_, start, end)) =
                rg_row_ranges.iter().find(|(idx, _, _)| *idx == rg_idx)
            {
                reversed_selectors.push(RowSelector::select(end - start));
            }
        }
    }

    Ok(RowSelection::from(reversed_selectors))
}

#[cfg(test)]
mod tests {
    use crate::ParquetAccessPlan;
    use crate::RowGroupAccess;
    use crate::opener::PreparedAccessPlan;
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::sync::Arc;

    /// Helper function to create a ParquetMetaData with specified row group sizes
    /// by actually writing a parquet file in memory
    fn create_test_metadata(
        row_group_sizes: Vec<i64>,
    ) -> parquet::file::metadata::ParquetMetaData {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let mut buffer = Vec::new();
        {
            let props = parquet::file::properties::WriterProperties::builder().build();
            let mut writer =
                ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();

            for &size in &row_group_sizes {
                let array = arrow::array::Int32Array::from(vec![1; size as usize]);
                let batch = arrow::record_batch::RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(array)],
                )
                .unwrap();
                writer.write(&batch).unwrap();
                writer.flush().unwrap();
            }
            writer.close().unwrap();
        }

        let bytes = Bytes::from(buffer);
        let reader = SerializedFileReader::new(bytes).unwrap();
        reader.metadata().clone()
    }

    #[test]
    fn test_prepared_access_plan_reverse_simple() {
        // Test: all row groups are scanned, no row selection
        let metadata = create_test_metadata(vec![100, 100, 100]);

        let access_plan = ParquetAccessPlan::new_all(3);
        let rg_metadata = metadata.row_groups();

        let prepared_plan =
            PreparedAccessPlan::from_access_plan(access_plan, rg_metadata)
                .expect("Failed to create PreparedAccessPlan");

        // Verify original plan
        assert_eq!(prepared_plan.row_group_indexes, vec![0, 1, 2]);

        // No row selection originally due to scanning all rows
        assert_eq!(prepared_plan.row_selection, None);

        let reversed_plan = prepared_plan
            .reverse(&metadata)
            .expect("Failed to reverse PreparedAccessPlan");

        // Verify row groups are reversed
        assert_eq!(reversed_plan.row_group_indexes, vec![2, 1, 0]);

        // If no selection originally, after reversal should still select all rows,
        // and the selection should be None
        assert_eq!(reversed_plan.row_selection, None);
    }

    #[test]
    fn test_prepared_access_plan_reverse_with_selection() {
        // Test: simple row selection that spans multiple row groups
        let metadata = create_test_metadata(vec![100, 100, 100]);

        let mut access_plan = ParquetAccessPlan::new_all(3);

        // Select first 50 rows from first row group, skip rest
        access_plan.scan_selection(
            0,
            RowSelection::from(vec![RowSelector::select(50), RowSelector::skip(50)]),
        );

        let rg_metadata = metadata.row_groups();
        let prepared_plan =
            PreparedAccessPlan::from_access_plan(access_plan, rg_metadata)
                .expect("Failed to create PreparedAccessPlan");

        let original_selected: usize = prepared_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        let reversed_plan = prepared_plan
            .reverse(&metadata)
            .expect("Failed to reverse PreparedAccessPlan");

        let reversed_selected: usize = reversed_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(
            original_selected, reversed_selected,
            "Total selected rows should remain the same"
        );
    }

    #[test]
    fn test_prepared_access_plan_reverse_multi_row_group_selection() {
        // Test: row selection spanning multiple row groups
        let metadata = create_test_metadata(vec![100, 100, 100]);

        let mut access_plan = ParquetAccessPlan::new_all(3);

        // Create selection that spans RG0 and RG1
        access_plan.scan_selection(
            0,
            RowSelection::from(vec![RowSelector::skip(50), RowSelector::select(50)]),
        );
        access_plan.scan_selection(
            1,
            RowSelection::from(vec![RowSelector::select(50), RowSelector::skip(50)]),
        );

        let rg_metadata = metadata.row_groups();
        let prepared_plan =
            PreparedAccessPlan::from_access_plan(access_plan, rg_metadata)
                .expect("Failed to create PreparedAccessPlan");

        let original_selected: usize = prepared_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        let reversed_plan = prepared_plan
            .reverse(&metadata)
            .expect("Failed to reverse PreparedAccessPlan");

        let reversed_selected: usize = reversed_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(original_selected, reversed_selected);
    }

    #[test]
    fn test_prepared_access_plan_reverse_empty_selection() {
        // Test: all rows are skipped
        let metadata = create_test_metadata(vec![100, 100, 100]);

        let mut access_plan = ParquetAccessPlan::new_all(3);

        // Skip all rows in all row groups
        for i in 0..3 {
            access_plan
                .scan_selection(i, RowSelection::from(vec![RowSelector::skip(100)]));
        }

        let rg_metadata = metadata.row_groups();
        let prepared_plan =
            PreparedAccessPlan::from_access_plan(access_plan, rg_metadata)
                .expect("Failed to create PreparedAccessPlan");

        let reversed_plan = prepared_plan
            .reverse(&metadata)
            .expect("Failed to reverse PreparedAccessPlan");

        // Should still skip all rows
        let total_selected: usize = reversed_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(total_selected, 0);
    }

    #[test]
    fn test_prepared_access_plan_reverse_different_row_group_sizes() {
        // Test: row groups with different sizes
        let metadata = create_test_metadata(vec![50, 150, 100]);

        let mut access_plan = ParquetAccessPlan::new_all(3);

        // Create complex selection pattern
        access_plan.scan_selection(
            0,
            RowSelection::from(vec![RowSelector::skip(25), RowSelector::select(25)]),
        );
        access_plan.scan_selection(1, RowSelection::from(vec![RowSelector::select(150)]));
        access_plan.scan_selection(
            2,
            RowSelection::from(vec![RowSelector::select(50), RowSelector::skip(50)]),
        );

        let rg_metadata = metadata.row_groups();
        let prepared_plan =
            PreparedAccessPlan::from_access_plan(access_plan, rg_metadata)
                .expect("Failed to create PreparedAccessPlan");

        let original_selected: usize = prepared_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        let reversed_plan = prepared_plan
            .reverse(&metadata)
            .expect("Failed to reverse PreparedAccessPlan");

        let reversed_selected: usize = reversed_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(original_selected, reversed_selected);
    }

    #[test]
    fn test_prepared_access_plan_reverse_single_row_group() {
        // Test: single row group case
        let metadata = create_test_metadata(vec![100]);

        let mut access_plan = ParquetAccessPlan::new_all(1);
        access_plan.scan_selection(
            0,
            RowSelection::from(vec![RowSelector::select(50), RowSelector::skip(50)]),
        );

        let rg_metadata = metadata.row_groups();
        let prepared_plan =
            PreparedAccessPlan::from_access_plan(access_plan, rg_metadata)
                .expect("Failed to create PreparedAccessPlan");

        let original_selected: usize = prepared_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        let reversed_plan = prepared_plan
            .reverse(&metadata)
            .expect("Failed to reverse PreparedAccessPlan");

        // With single row group, row_group_indexes should remain [0]
        assert_eq!(reversed_plan.row_group_indexes, vec![0]);

        let reversed_selected: usize = reversed_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(original_selected, reversed_selected);
        assert_eq!(original_selected, 50);
    }

    #[test]
    fn test_prepared_access_plan_reverse_complex_pattern() {
        // Test: complex pattern with multiple select/skip segments
        let metadata = create_test_metadata(vec![100, 100, 100]);

        let mut access_plan = ParquetAccessPlan::new_all(3);

        // Complex pattern: select some, skip some, select some more
        access_plan.scan_selection(
            0,
            RowSelection::from(vec![
                RowSelector::select(30),
                RowSelector::skip(40),
                RowSelector::select(30),
            ]),
        );
        access_plan.scan_selection(
            1,
            RowSelection::from(vec![RowSelector::skip(50), RowSelector::select(50)]),
        );
        access_plan.scan_selection(2, RowSelection::from(vec![RowSelector::select(100)]));

        let rg_metadata = metadata.row_groups();
        let prepared_plan =
            PreparedAccessPlan::from_access_plan(access_plan, rg_metadata)
                .expect("Failed to create PreparedAccessPlan");

        let original_selected: usize = prepared_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        let reversed_plan = prepared_plan
            .reverse(&metadata)
            .expect("Failed to reverse PreparedAccessPlan");

        let reversed_selected: usize = reversed_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(original_selected, reversed_selected);
        assert_eq!(original_selected, 210); // 30 + 30 + 50 + 100
    }

    #[test]
    fn test_prepared_access_plan_reverse_with_skipped_row_groups() {
        // This is the KEY test case for the bug fix!
        // Test scenario where some row groups are completely skipped (not in scan plan)
        let metadata = create_test_metadata(vec![100, 100, 100, 100]);

        // Scenario: RG0 (scan all), RG1 (completely skipped), RG2 (partial), RG3 (scan all)
        // Only row groups [0, 2, 3] are in the scan plan
        let mut access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan, // RG0
            RowGroupAccess::Skip, // RG1 - NOT in scan plan!
            RowGroupAccess::Scan, // RG2
            RowGroupAccess::Scan, // RG3
        ]);

        // Add row selections for the scanned row groups
        // Note: The RowSelection only covers row groups [0, 2, 3] (300 rows total)
        access_plan.scan_selection(
            0,
            RowSelection::from(vec![RowSelector::select(100)]), // RG0: all 100 rows
        );
        // RG1 is skipped, no selection needed
        access_plan.scan_selection(
            2,
            RowSelection::from(vec![
                RowSelector::select(25), // RG2: first 25 rows
                RowSelector::skip(75),   // RG2: skip last 75 rows
            ]),
        );
        access_plan.scan_selection(
            3,
            RowSelection::from(vec![RowSelector::select(100)]), // RG3: all 100 rows
        );

        let rg_metadata = metadata.row_groups();

        // Step 1: Create PreparedAccessPlan
        let prepared_plan =
            PreparedAccessPlan::from_access_plan(access_plan, rg_metadata)
                .expect("Failed to create PreparedAccessPlan");

        // Verify original plan
        assert_eq!(prepared_plan.row_group_indexes, vec![0, 2, 3]);
        let original_selected: usize = prepared_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();
        assert_eq!(original_selected, 225); // 100 + 25 + 100

        // Step 2: Reverse the plan (this is the production code path)
        let reversed_plan = prepared_plan
            .reverse(&metadata)
            .expect("Failed to reverse PreparedAccessPlan");

        // Verify reversed results
        // Row group order should be reversed: [3, 2, 0]
        assert_eq!(
            reversed_plan.row_group_indexes,
            vec![3, 2, 0],
            "Row groups should be reversed"
        );

        // Verify row selection is also correctly reversed
        let reversed_selected: usize = reversed_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(
            reversed_selected, 225,
            "Total selected rows should remain the same"
        );

        // Verify the reversed selection structure
        // After reversal, the order becomes: RG3, RG2, RG0
        // - RG3: select(100)
        // - RG2: select(25), skip(75)  (note: internal order preserved, not reversed)
        // - RG0: select(100)
        //
        // After RowSelection::from() merges adjacent selectors of the same type:
        // - RG3's select(100) + RG2's select(25) = select(125)
        // - RG2's skip(75) remains as skip(75)
        // - RG0's select(100) remains as select(100)
        let selectors: Vec<_> = reversed_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(selectors.len(), 3);

        // RG3 (100) + RG2 first part (25) merged into select(125)
        assert!(!selectors[0].skip);
        assert_eq!(selectors[0].row_count, 125);

        // RG2: skip last 75 rows
        assert!(selectors[1].skip);
        assert_eq!(selectors[1].row_count, 75);

        // RG0: select all 100 rows
        assert!(!selectors[2].skip);
        assert_eq!(selectors[2].row_count, 100);
    }

    #[test]
    fn test_prepared_access_plan_reverse_alternating_row_groups() {
        // Test with alternating scan/skip pattern
        let metadata = create_test_metadata(vec![100, 100, 100, 100]);

        // Scan RG0 and RG2, skip RG1 and RG3
        let mut access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan, // RG0
            RowGroupAccess::Skip, // RG1
            RowGroupAccess::Scan, // RG2
            RowGroupAccess::Skip, // RG3
        ]);

        access_plan.scan_selection(0, RowSelection::from(vec![RowSelector::select(100)]));
        access_plan.scan_selection(2, RowSelection::from(vec![RowSelector::select(100)]));

        let rg_metadata = metadata.row_groups();
        let prepared_plan =
            PreparedAccessPlan::from_access_plan(access_plan, rg_metadata)
                .expect("Failed to create PreparedAccessPlan");

        let original_selected: usize = prepared_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        // Original: [0, 2]
        assert_eq!(prepared_plan.row_group_indexes, vec![0, 2]);

        let reversed_plan = prepared_plan
            .reverse(&metadata)
            .expect("Failed to reverse PreparedAccessPlan");

        // After reverse: [2, 0]
        assert_eq!(reversed_plan.row_group_indexes, vec![2, 0]);

        let reversed_selected: usize = reversed_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(original_selected, reversed_selected);
        assert_eq!(original_selected, 200);
    }

    #[test]
    fn test_prepared_access_plan_reverse_middle_row_group_only() {
        // Test selecting only the middle row group
        let metadata = create_test_metadata(vec![100, 100, 100]);

        let mut access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Skip, // RG0
            RowGroupAccess::Scan, // RG1
            RowGroupAccess::Skip, // RG2
        ]);

        access_plan.scan_selection(
            1,
            RowSelection::from(vec![RowSelector::select(100)]), // Select all of RG1
        );

        let rg_metadata = metadata.row_groups();
        let prepared_plan =
            PreparedAccessPlan::from_access_plan(access_plan, rg_metadata)
                .expect("Failed to create PreparedAccessPlan");

        let original_selected: usize = prepared_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        // Original: [1]
        assert_eq!(prepared_plan.row_group_indexes, vec![1]);

        let reversed_plan = prepared_plan
            .reverse(&metadata)
            .expect("Failed to reverse PreparedAccessPlan");

        // After reverse: still [1] (only one row group)
        assert_eq!(reversed_plan.row_group_indexes, vec![1]);

        let reversed_selected: usize = reversed_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(original_selected, reversed_selected);
        assert_eq!(original_selected, 100);
    }

    #[test]
    fn test_prepared_access_plan_reverse_with_skipped_row_groups_detailed() {
        // This is the KEY test case for the bug fix!
        // Test scenario where some row groups are completely skipped (not in scan plan)
        // This version includes DETAILED verification of the selector distribution
        let metadata = create_test_metadata(vec![100, 100, 100, 100]);

        // Scenario: RG0 (scan all), RG1 (completely skipped), RG2 (partial), RG3 (scan all)
        // Only row groups [0, 2, 3] are in the scan plan
        let mut access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan, // RG0
            RowGroupAccess::Skip, // RG1 - NOT in scan plan!
            RowGroupAccess::Scan, // RG2
            RowGroupAccess::Scan, // RG3
        ]);

        // Add row selections for the scanned row groups
        access_plan.scan_selection(
            0,
            RowSelection::from(vec![RowSelector::select(100)]), // RG0: all 100 rows
        );
        // RG1 is skipped, no selection needed
        access_plan.scan_selection(
            2,
            RowSelection::from(vec![
                RowSelector::select(25), // RG2: first 25 rows
                RowSelector::skip(75),   // RG2: skip last 75 rows
            ]),
        );
        access_plan.scan_selection(
            3,
            RowSelection::from(vec![RowSelector::select(100)]), // RG3: all 100 rows
        );

        let rg_metadata = metadata.row_groups();

        // Step 1: Create PreparedAccessPlan
        let prepared_plan =
            PreparedAccessPlan::from_access_plan(access_plan, rg_metadata)
                .expect("Failed to create PreparedAccessPlan");

        // Verify original plan in detail
        assert_eq!(prepared_plan.row_group_indexes, vec![0, 2, 3]);

        // Detailed verification of original selection
        let orig_selectors: Vec<_> = prepared_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .collect();

        // Original structure should be:
        // RG0: select(100)
        // RG2: select(25), skip(75)
        // RG3: select(100)
        // After merging by RowSelection::from(): select(125), skip(75), select(100)
        assert_eq!(
            orig_selectors.len(),
            3,
            "Original should have 3 selectors after merging"
        );
        assert!(
            !orig_selectors[0].skip && orig_selectors[0].row_count == 125,
            "Original: First selector should be select(125) from RG0(100) + RG2(25)"
        );
        assert!(
            orig_selectors[1].skip && orig_selectors[1].row_count == 75,
            "Original: Second selector should be skip(75) from RG2"
        );
        assert!(
            !orig_selectors[2].skip && orig_selectors[2].row_count == 100,
            "Original: Third selector should be select(100) from RG3"
        );

        let original_selected: usize = orig_selectors
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();
        assert_eq!(original_selected, 225); // 100 + 25 + 100

        // Step 2: Reverse the plan (this is the production code path)
        let reversed_plan = prepared_plan
            .reverse(&metadata)
            .expect("Failed to reverse PreparedAccessPlan");

        // Verify reversed results
        // Row group order should be reversed: [3, 2, 0]
        assert_eq!(
            reversed_plan.row_group_indexes,
            vec![3, 2, 0],
            "Row groups should be reversed"
        );

        // Detailed verification of reversed selection
        let rev_selectors: Vec<_> = reversed_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .collect();

        // After reversal, the order becomes: RG3, RG2, RG0
        // - RG3: select(100)
        // - RG2: select(25), skip(75)  (note: internal order preserved, not reversed)
        // - RG0: select(100)
        //
        // After RowSelection::from() merges adjacent selectors of the same type:
        // - RG3's select(100) + RG2's select(25) = select(125)
        // - RG2's skip(75) remains as skip(75)
        // - RG0's select(100) remains as select(100)

        assert_eq!(
            rev_selectors.len(),
            3,
            "Reversed should have 3 selectors after merging"
        );

        // First selector: RG3 (100) + RG2 first part (25) merged into select(125)
        assert!(
            !rev_selectors[0].skip && rev_selectors[0].row_count == 125,
            "Reversed: First selector should be select(125) from RG3(100) + RG2(25), got skip={} count={}",
            rev_selectors[0].skip,
            rev_selectors[0].row_count
        );

        // Second selector: RG2 skip last 75 rows
        assert!(
            rev_selectors[1].skip && rev_selectors[1].row_count == 75,
            "Reversed: Second selector should be skip(75) from RG2, got skip={} count={}",
            rev_selectors[1].skip,
            rev_selectors[1].row_count
        );

        // Third selector: RG0 select all 100 rows
        assert!(
            !rev_selectors[2].skip && rev_selectors[2].row_count == 100,
            "Reversed: Third selector should be select(100) from RG0, got skip={} count={}",
            rev_selectors[2].skip,
            rev_selectors[2].row_count
        );

        // Verify row selection is also correctly reversed (total count)
        let reversed_selected: usize = rev_selectors
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(
            reversed_selected, 225,
            "Total selected rows should remain the same"
        );
    }

    #[test]
    fn test_prepared_access_plan_reverse_complex_pattern_detailed() {
        // Test: complex pattern with detailed verification
        let metadata = create_test_metadata(vec![100, 100, 100]);

        let mut access_plan = ParquetAccessPlan::new_all(3);

        // Complex pattern: select some, skip some, select some more
        access_plan.scan_selection(
            0,
            RowSelection::from(vec![
                RowSelector::select(30),
                RowSelector::skip(40),
                RowSelector::select(30),
            ]),
        );
        access_plan.scan_selection(
            1,
            RowSelection::from(vec![RowSelector::skip(50), RowSelector::select(50)]),
        );
        access_plan.scan_selection(2, RowSelection::from(vec![RowSelector::select(100)]));

        let rg_metadata = metadata.row_groups();
        let prepared_plan =
            PreparedAccessPlan::from_access_plan(access_plan, rg_metadata)
                .expect("Failed to create PreparedAccessPlan");

        // Verify original selection structure in detail
        let orig_selectors: Vec<_> = prepared_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .collect();

        // RG0: select(30), skip(40), select(30)
        // RG1: skip(50), select(50)
        // RG2: select(100)
        // Sequential: sel(30), skip(40), sel(30), skip(50), sel(50), sel(100)
        // After merge: sel(30), skip(40), sel(30), skip(50), sel(150)

        let original_selected: usize = orig_selectors
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();
        assert_eq!(original_selected, 210); // 30 + 30 + 50 + 100

        let reversed_plan = prepared_plan
            .reverse(&metadata)
            .expect("Failed to reverse PreparedAccessPlan");

        // Verify reversed selection structure
        let rev_selectors: Vec<_> = reversed_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .collect();

        // After reversal: RG2, RG1, RG0
        // RG2: select(100)
        // RG1: skip(50), select(50)
        // RG0: select(30), skip(40), select(30)
        // Sequential: sel(100), skip(50), sel(50), sel(30), skip(40), sel(30)
        // After merge: sel(100), skip(50), sel(80), skip(40), sel(30)

        let reversed_selected: usize = rev_selectors
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(
            reversed_selected, 210,
            "Total selected rows should remain the same (30 + 30 + 50 + 100)"
        );

        // Verify row group order
        assert_eq!(reversed_plan.row_group_indexes, vec![2, 1, 0]);
    }

    #[test]
    fn test_prepared_access_plan_reverse_alternating_detailed() {
        // Test with alternating scan/skip pattern with detailed verification
        let metadata = create_test_metadata(vec![100, 100, 100, 100]);

        // Scan RG0 and RG2, skip RG1 and RG3
        let mut access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan, // RG0
            RowGroupAccess::Skip, // RG1
            RowGroupAccess::Scan, // RG2
            RowGroupAccess::Skip, // RG3
        ]);

        access_plan.scan_selection(
            0,
            RowSelection::from(vec![RowSelector::select(30), RowSelector::skip(70)]),
        );
        access_plan.scan_selection(
            2,
            RowSelection::from(vec![RowSelector::skip(20), RowSelector::select(80)]),
        );

        let rg_metadata = metadata.row_groups();
        let prepared_plan =
            PreparedAccessPlan::from_access_plan(access_plan, rg_metadata)
                .expect("Failed to create PreparedAccessPlan");

        // Original: [0, 2]
        assert_eq!(prepared_plan.row_group_indexes, vec![0, 2]);

        // Verify original selection
        let orig_selectors: Vec<_> = prepared_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .collect();

        // Original:
        // RG0: select(30), skip(70)
        // RG2: skip(20), select(80)
        // Sequential: sel(30), skip(90), sel(80)
        //   (RG0's skip(70) + RG2's skip(20) = skip(90))

        let original_selected: usize = orig_selectors
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();
        assert_eq!(original_selected, 110); // 30 + 80

        let reversed_plan = prepared_plan
            .reverse(&metadata)
            .expect("Failed to reverse PreparedAccessPlan");

        // After reverse: [2, 0]
        assert_eq!(reversed_plan.row_group_indexes, vec![2, 0]);

        // Verify reversed selection
        let rev_selectors: Vec<_> = reversed_plan
            .row_selection
            .as_ref()
            .unwrap()
            .iter()
            .collect();

        // After reversal: RG2, RG0
        // RG2: skip(20), select(80)
        // RG0: select(30), skip(70)
        // Sequential: skip(20), sel(110), skip(70)
        //   (RG2's select(80) + RG0's select(30) = select(110))

        let reversed_selected: usize = rev_selectors
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(reversed_selected, 110); // Should still be 30 + 80

        // Detailed verification of structure
        assert_eq!(rev_selectors.len(), 3, "Reversed should have 3 selectors");

        assert!(
            rev_selectors[0].skip && rev_selectors[0].row_count == 20,
            "First selector should be skip(20) from RG2"
        );

        assert!(
            !rev_selectors[1].skip && rev_selectors[1].row_count == 110,
            "Second selector should be select(110) from RG2(80) + RG0(30)"
        );

        assert!(
            rev_selectors[2].skip && rev_selectors[2].row_count == 70,
            "Third selector should be skip(70) from RG0"
        );
    }
}
