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
/// * `row_selection` - Original row selection
/// * `parquet_metadata` - Metadata containing row group information
///
/// # Returns
/// A new `RowSelection` adjusted for reversed row group order
pub fn reverse_row_selection(
    row_selection: &RowSelection,
    parquet_metadata: &ParquetMetaData,
) -> Result<RowSelection> {
    let rg_metadata = parquet_metadata.row_groups();

    // Build a mapping of row group index to its row range in the file
    let mut rg_row_ranges: Vec<(usize, usize, usize)> =
        Vec::with_capacity(rg_metadata.len());
    let mut current_row = 0;
    for (rg_idx, rg) in rg_metadata.iter().enumerate() {
        let num_rows = rg.num_rows() as usize;
        rg_row_ranges.push((rg_idx, current_row, current_row + num_rows));
        current_row += num_rows;
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
    let mut reversed_selectors = Vec::new();
    for rg_idx in (0..rg_metadata.len()).rev() {
        if let Some(selectors) = rg_selections.get(&rg_idx) {
            reversed_selectors.extend(selectors.iter().cloned());
        } else {
            // No specific selection for this row group means select all
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
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::sync::Arc;

    /// Helper function to create a ParquetMetaData with specified row group sizes
    /// by actually writing a parquet file in memory
    fn create_test_metadata(row_group_sizes: Vec<i64>) -> ParquetMetaData {
        // Create a simple schema
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        // Create in-memory parquet file with the specified row groups
        let mut buffer = Vec::new();
        {
            let props = parquet::file::properties::WriterProperties::builder()
                .set_max_row_group_size(row_group_sizes[0] as usize)
                .build();

            let mut writer =
                ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();

            for &size in &row_group_sizes {
                // Create a batch with the specified number of rows
                let array = arrow::array::Int32Array::from(vec![1; size as usize]);
                let batch = arrow::record_batch::RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(array)],
                )
                .unwrap();
                writer.write(&batch).unwrap();
            }
            writer.close().unwrap();
        }

        // Read back the metadata
        let bytes = Bytes::from(buffer);
        let reader = SerializedFileReader::new(bytes).unwrap();
        reader.metadata().clone()
    }

    #[test]
    fn test_reverse_simple_selection() {
        // 3 row groups with 100 rows each
        let metadata = create_test_metadata(vec![100, 100, 100]);

        // Select first 50 rows from first row group
        let selection =
            RowSelection::from(vec![RowSelector::select(50), RowSelector::skip(250)]);

        let reversed = reverse_row_selection(&selection, &metadata).unwrap();

        // Verify total selected rows remain the same
        let original_selected: usize = selection
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();
        let reversed_selected: usize = reversed
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(original_selected, reversed_selected);
    }

    #[test]
    fn test_reverse_multi_row_group_selection() {
        let metadata = create_test_metadata(vec![100, 100, 100]);

        // Select rows spanning multiple row groups
        let selection = RowSelection::from(vec![
            RowSelector::skip(50),
            RowSelector::select(100), // Spans RG0 and RG1
            RowSelector::skip(150),
        ]);

        let reversed = reverse_row_selection(&selection, &metadata).unwrap();

        // Verify total selected rows remain the same
        let original_selected: usize = selection
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();
        let reversed_selected: usize = reversed
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(original_selected, reversed_selected);
    }

    #[test]
    fn test_reverse_full_selection() {
        let metadata = create_test_metadata(vec![100, 100, 100]);

        // Select all rows
        let selection = RowSelection::from(vec![RowSelector::select(300)]);

        let reversed = reverse_row_selection(&selection, &metadata).unwrap();

        // Should still select all rows, just in reversed row group order
        let total_selected: usize = reversed
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(total_selected, 300);
    }

    #[test]
    fn test_reverse_empty_selection() {
        let metadata = create_test_metadata(vec![100, 100, 100]);

        // Skip all rows
        let selection = RowSelection::from(vec![RowSelector::skip(300)]);

        let reversed = reverse_row_selection(&selection, &metadata).unwrap();

        // Should still skip all rows
        let total_selected: usize = reversed
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(total_selected, 0);
    }

    #[test]
    fn test_reverse_with_different_row_group_sizes() {
        let metadata = create_test_metadata(vec![50, 150, 100]);

        let selection = RowSelection::from(vec![
            RowSelector::skip(25),
            RowSelector::select(200), // Spans all row groups
            RowSelector::skip(75),
        ]);

        let reversed = reverse_row_selection(&selection, &metadata).unwrap();

        let original_selected: usize = selection
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();
        let reversed_selected: usize = reversed
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(original_selected, reversed_selected);
    }

    #[test]
    fn test_reverse_single_row_group() {
        let metadata = create_test_metadata(vec![100]);

        let selection =
            RowSelection::from(vec![RowSelector::select(50), RowSelector::skip(50)]);

        let reversed = reverse_row_selection(&selection, &metadata).unwrap();

        // With single row group, selection should remain the same
        let original_selected: usize = selection
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();
        let reversed_selected: usize = reversed
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(original_selected, reversed_selected);
    }

    #[test]
    fn test_reverse_complex_pattern() {
        let metadata = create_test_metadata(vec![100, 100, 100]);

        // Complex pattern: select some, skip some, select some more
        let selection = RowSelection::from(vec![
            RowSelector::select(30),
            RowSelector::skip(40),
            RowSelector::select(80),
            RowSelector::skip(50),
            RowSelector::select(100),
        ]);

        let reversed = reverse_row_selection(&selection, &metadata).unwrap();

        let original_selected: usize = selection
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();
        let reversed_selected: usize = reversed
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(original_selected, reversed_selected);
        assert_eq!(original_selected, 210); // 30 + 80 + 100
    }

    #[test]
    fn test_reverse_with_skipped_row_group() {
        // This test covers the "no specific selection" code path (lines 90-95)
        let metadata = create_test_metadata(vec![100, 100, 100]);

        // Select only from first and third row groups, skip middle one entirely
        let selection = RowSelection::from(vec![
            RowSelector::select(50), // First 50 of RG0
            RowSelector::skip(150),  // Rest of RG0 + all of RG1 + half of RG2
            RowSelector::select(50), // Last 50 of RG2
        ]);

        let reversed = reverse_row_selection(&selection, &metadata).unwrap();

        // Verify total selected rows remain the same
        let original_selected: usize = selection
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();
        let reversed_selected: usize = reversed
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(original_selected, reversed_selected);
        assert_eq!(original_selected, 100); // 50 + 50
    }

    #[test]
    fn test_reverse_middle_row_group_only() {
        // Another test to ensure skipped row groups are handled correctly
        let metadata = create_test_metadata(vec![100, 100, 100]);

        // Select only middle row group
        let selection = RowSelection::from(vec![
            RowSelector::skip(100),   // Skip RG0
            RowSelector::select(100), // Select all of RG1
            RowSelector::skip(100),   // Skip RG2
        ]);

        let reversed = reverse_row_selection(&selection, &metadata).unwrap();

        let original_selected: usize = selection
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();
        let reversed_selected: usize = reversed
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(original_selected, reversed_selected);
        assert_eq!(original_selected, 100);
    }

    #[test]
    fn test_reverse_alternating_row_groups() {
        // Test with more complex skipping pattern
        let metadata = create_test_metadata(vec![100, 100, 100, 100]);

        // Select first and third row groups, skip second and fourth
        let selection = RowSelection::from(vec![
            RowSelector::select(100), // RG0
            RowSelector::skip(100),   // RG1
            RowSelector::select(100), // RG2
            RowSelector::skip(100),   // RG3
        ]);

        let reversed = reverse_row_selection(&selection, &metadata).unwrap();

        let original_selected: usize = selection
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();
        let reversed_selected: usize = reversed
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();

        assert_eq!(original_selected, reversed_selected);
        assert_eq!(original_selected, 200);
    }
}
