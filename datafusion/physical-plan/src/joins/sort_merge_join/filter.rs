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

//! Filter handling for Sort-Merge Join
//!
//! This module encapsulates the complexity of join filter evaluation, including:
//! - Immediate filtering for INNER joins
//! - Deferred filtering for outer/semi/anti/mark joins
//! - Metadata tracking for grouping output rows by input row
//! - Correcting filter masks to handle multiple matches per input row

use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, RecordBatch,
    UInt64Array, UInt64Builder,
};
use arrow::compute::{self, concat_batches, filter_record_batch};
use arrow::datatypes::SchemaRef;
use datafusion_common::{JoinSide, JoinType, Result};

use crate::joins::utils::JoinFilter;

/// Metadata for tracking filter results during deferred filtering
///
/// When a join filter is present and we need to ensure each input row produces
/// at least one output (outer joins) or exactly one output (semi joins), we can't
/// filter immediately. Instead, we accumulate all joined rows with metadata,
/// then post-process to determine which rows to output.
#[derive(Debug)]
pub struct FilterMetadata {
    /// Did each output row pass the join filter?
    /// Used to detect if an input row found ANY match
    pub filter_mask: BooleanBuilder,

    /// Which input row (within batch) produced each output row?
    /// Used for grouping output rows by input row
    pub row_indices: UInt64Builder,

    /// Which input batch did each output row come from?
    /// Used to disambiguate row_indices across multiple batches
    pub batch_ids: Vec<usize>,
}

impl FilterMetadata {
    /// Create new empty filter metadata
    pub fn new() -> Self {
        Self {
            filter_mask: BooleanBuilder::new(),
            row_indices: UInt64Builder::new(),
            batch_ids: vec![],
        }
    }

    /// Returns (row_indices, filter_mask, batch_ids_ref) and clears builders
    pub fn finish_metadata(&mut self) -> (UInt64Array, BooleanArray, &[usize]) {
        let row_indices = self.row_indices.finish();
        let filter_mask = self.filter_mask.finish();
        (row_indices, filter_mask, &self.batch_ids)
    }

    /// Add metadata for null-joined rows (no filter applied)
    pub fn append_nulls(&mut self, num_rows: usize) {
        self.filter_mask.append_nulls(num_rows);
        self.row_indices.append_nulls(num_rows);
        self.batch_ids.resize(
            self.batch_ids.len() + num_rows,
            0, // batch_id = 0 for null-joined rows
        );
    }

    /// Add metadata for filtered rows
    pub fn append_filter_metadata(
        &mut self,
        row_indices: &UInt64Array,
        filter_mask: &BooleanArray,
        batch_id: usize,
    ) {
        debug_assert_eq!(
            row_indices.len(),
            filter_mask.len(),
            "row_indices and filter_mask must have same length"
        );

        for i in 0..row_indices.len() {
            if filter_mask.is_null(i) {
                self.filter_mask.append_null();
            } else if filter_mask.value(i) {
                self.filter_mask.append_value(true);
            } else {
                self.filter_mask.append_value(false);
            }

            if row_indices.is_null(i) {
                self.row_indices.append_null();
            } else {
                self.row_indices.append_value(row_indices.value(i));
            }

            self.batch_ids.push(batch_id);
        }
    }

    /// Verify that metadata arrays are aligned (same length)
    pub fn debug_assert_metadata_aligned(&self) {
        if self.filter_mask.len() > 0 {
            debug_assert_eq!(
                self.filter_mask.len(),
                self.row_indices.len(),
                "filter_mask and row_indices must have same length when metadata is used"
            );
            debug_assert_eq!(
                self.filter_mask.len(),
                self.batch_ids.len(),
                "filter_mask and batch_ids must have same length when metadata is used"
            );
        } else {
            debug_assert_eq!(
                self.filter_mask.len(),
                0,
                "filter_mask should be empty when batches is empty"
            );
        }
    }
}

impl Default for FilterMetadata {
    fn default() -> Self {
        Self::new()
    }
}

/// Determines if a join type needs deferred filtering
///
/// Deferred filtering is required when:
/// - A filter exists AND
/// - The join type requires ensuring each input row produces at least one output
///   (or exactly one for semi joins)
pub fn needs_deferred_filtering(
    filter: &Option<JoinFilter>,
    join_type: JoinType,
) -> bool {
    filter.is_some()
        && matches!(
            join_type,
            JoinType::Left
                | JoinType::LeftSemi
                | JoinType::LeftMark
                | JoinType::Right
                | JoinType::RightSemi
                | JoinType::RightMark
                | JoinType::LeftAnti
                | JoinType::RightAnti
                | JoinType::Full
        )
}

/// Gets the arrays which join filters are applied on
///
/// Extracts the columns needed for filter evaluation from left and right batch columns
pub fn get_filter_columns(
    join_filter: &Option<JoinFilter>,
    left_columns: &[ArrayRef],
    right_columns: &[ArrayRef],
) -> Vec<ArrayRef> {
    let mut filter_columns = vec![];

    if let Some(f) = join_filter {
        let left_columns: Vec<ArrayRef> = f
            .column_indices()
            .iter()
            .filter(|col_index| col_index.side == JoinSide::Left)
            .map(|i| Arc::clone(&left_columns[i.index]))
            .collect();
        let right_columns: Vec<ArrayRef> = f
            .column_indices()
            .iter()
            .filter(|col_index| col_index.side == JoinSide::Right)
            .map(|i| Arc::clone(&right_columns[i.index]))
            .collect();

        filter_columns.extend(left_columns);
        filter_columns.extend(right_columns);
    }

    filter_columns
}

/// Determines if current index is the last occurrence of a row
///
/// Used during filter mask correction to detect row boundaries when grouping
/// output rows by input row.
fn last_index_for_row(
    row_index: usize,
    indices: &UInt64Array,
    batch_ids: &[usize],
    indices_len: usize,
) -> bool {
    debug_assert_eq!(
        indices.len(),
        indices_len,
        "indices.len() should match indices_len parameter"
    );
    debug_assert_eq!(
        batch_ids.len(),
        indices_len,
        "batch_ids.len() should match indices_len"
    );
    debug_assert!(
        row_index < indices_len,
        "row_index {row_index} should be < indices_len {indices_len}",
    );

    // If this is the last index overall, it's definitely the last for this row
    if row_index == indices_len - 1 {
        return true;
    }

    // Check if next row has different (batch_id, index) pair
    let current_batch_id = batch_ids[row_index];
    let next_batch_id = batch_ids[row_index + 1];

    if current_batch_id != next_batch_id {
        return true;
    }

    // Same batch_id, check if row index is different
    // Both current and next should be non-null (already joined rows)
    if indices.is_null(row_index) || indices.is_null(row_index + 1) {
        return true;
    }

    indices.value(row_index) != indices.value(row_index + 1)
}

/// Corrects the filter mask for joins with deferred filtering
///
/// When an input row joins with multiple buffered rows, we get multiple output rows.
/// This function groups them by input row and applies join-type-specific logic:
///
/// - **Outer joins**: Keep first matching row, convert rest to nulls, add null-joined for unmatched
/// - **Semi joins**: Keep first matching row, discard rest
/// - **Anti joins**: Keep row only if NO matches passed filter
/// - **Mark joins**: Like semi but first match only
///
/// # Arguments
/// * `join_type` - The type of join being performed
/// * `row_indices` - Which input row produced each output row
/// * `batch_ids` - Which batch each output row came from
/// * `filter_mask` - Whether each output row passed the filter
/// * `expected_size` - Total number of input rows (for adding unmatched)
///
/// # Returns
/// Corrected mask indicating which rows to include in final output:
/// - `true`: Include this row
/// - `false`: Convert to null-joined row (outer joins) or include as unmatched (anti joins)
/// - `null`: Discard this row
pub fn get_corrected_filter_mask(
    join_type: JoinType,
    row_indices: &UInt64Array,
    batch_ids: &[usize],
    filter_mask: &BooleanArray,
    expected_size: usize,
) -> Option<BooleanArray> {
    let row_indices_length = row_indices.len();
    let mut corrected_mask: BooleanBuilder =
        BooleanBuilder::with_capacity(row_indices_length);
    let mut seen_true = false;

    match join_type {
        JoinType::Left | JoinType::Right => {
            // For outer joins: Keep first matching row per input row,
            // convert rest to nulls, add null-joined rows for unmatched
            for i in 0..row_indices_length {
                let last_index =
                    last_index_for_row(i, row_indices, batch_ids, row_indices_length);
                if filter_mask.value(i) {
                    seen_true = true;
                    corrected_mask.append_value(true);
                } else if seen_true || !filter_mask.value(i) && !last_index {
                    corrected_mask.append_null(); // to be ignored and not set to output
                } else {
                    corrected_mask.append_value(false); // to be converted to null joined row
                }

                if last_index {
                    seen_true = false;
                }
            }

            // Generate null joined rows for records which have no matching join key
            corrected_mask.append_n(expected_size - corrected_mask.len(), false);
            Some(corrected_mask.finish())
        }
        JoinType::LeftMark | JoinType::RightMark => {
            // For mark joins: Like outer but only keep first match, mark with boolean
            for i in 0..row_indices_length {
                let last_index =
                    last_index_for_row(i, row_indices, batch_ids, row_indices_length);
                if filter_mask.value(i) && !seen_true {
                    seen_true = true;
                    corrected_mask.append_value(true);
                } else if seen_true || !filter_mask.value(i) && !last_index {
                    corrected_mask.append_null(); // to be ignored and not set to output
                } else {
                    corrected_mask.append_value(false); // to be converted to null joined row
                }

                if last_index {
                    seen_true = false;
                }
            }

            // Generate null joined rows for records which have no matching join key
            corrected_mask.append_n(expected_size - corrected_mask.len(), false);
            Some(corrected_mask.finish())
        }
        JoinType::LeftSemi | JoinType::RightSemi => {
            // For semi joins: Keep only first matching row per input row, discard rest
            for i in 0..row_indices_length {
                let last_index =
                    last_index_for_row(i, row_indices, batch_ids, row_indices_length);
                if filter_mask.value(i) && !seen_true {
                    seen_true = true;
                    corrected_mask.append_value(true);
                } else {
                    corrected_mask.append_null(); // to be ignored and not set to output
                }

                if last_index {
                    seen_true = false;
                }
            }

            Some(corrected_mask.finish())
        }
        JoinType::LeftAnti | JoinType::RightAnti => {
            // For anti joins: Keep row only if NO matches passed the filter
            for i in 0..row_indices_length {
                let last_index =
                    last_index_for_row(i, row_indices, batch_ids, row_indices_length);

                if filter_mask.value(i) {
                    seen_true = true;
                }

                if last_index {
                    if !seen_true {
                        corrected_mask.append_value(true);
                    } else {
                        corrected_mask.append_null();
                    }

                    seen_true = false;
                } else {
                    corrected_mask.append_null();
                }
            }
            // Generate null joined rows for records which have no matching join key,
            // for LeftAnti non-matched considered as true
            corrected_mask.append_n(expected_size - corrected_mask.len(), true);
            Some(corrected_mask.finish())
        }
        JoinType::Full => {
            // For full joins: Similar to outer but handle both sides
            for i in 0..row_indices_length {
                let last_index =
                    last_index_for_row(i, row_indices, batch_ids, row_indices_length);

                if filter_mask.is_null(i) {
                    // null joined
                    corrected_mask.append_value(true);
                } else if filter_mask.value(i) {
                    seen_true = true;
                    corrected_mask.append_value(true);
                } else if seen_true || !filter_mask.value(i) && !last_index {
                    corrected_mask.append_null(); // to be ignored and not set to output
                } else {
                    corrected_mask.append_value(false); // to be converted to null joined row
                }

                if last_index {
                    seen_true = false;
                }
            }
            // Generate null joined rows for records which have no matching join key
            corrected_mask.append_n(expected_size - corrected_mask.len(), false);
            Some(corrected_mask.finish())
        }
        JoinType::Inner => {
            // Inner joins don't need deferred filtering
            None
        }
    }
}

/// Applies corrected filter mask to record batch based on join type
///
/// Different join types require different handling of filtered results:
/// - Outer joins: Add null-joined rows for false mask values
/// - Semi/Anti joins: May need projection to remove right columns
/// - Full joins: Add null-joined rows for both sides
pub fn filter_record_batch_by_join_type(
    record_batch: &RecordBatch,
    corrected_mask: &BooleanArray,
    join_type: JoinType,
    schema: &SchemaRef,
    streamed_schema: &SchemaRef,
    buffered_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let filtered_record_batch = filter_record_batch(record_batch, corrected_mask)?;

    match join_type {
        JoinType::Left | JoinType::LeftMark => {
            // For left joins, add null-joined rows where mask is false
            let null_mask = compute::not(corrected_mask)?;
            let null_joined_batch = filter_record_batch(record_batch, &null_mask)?;

            if null_joined_batch.num_rows() == 0 {
                return Ok(filtered_record_batch);
            }

            // Create null columns for right side
            let null_joined_streamed_batch = create_null_joined_batch(
                &null_joined_batch,
                buffered_schema,
                JoinSide::Left,
                join_type,
                schema,
            )?;

            Ok(concat_batches(
                schema,
                &[filtered_record_batch, null_joined_streamed_batch],
            )?)
        }
        JoinType::LeftSemi
        | JoinType::LeftAnti
        | JoinType::RightSemi
        | JoinType::RightAnti => {
            // For semi/anti joins, project to only include the outer side columns
            // Both Left and Right semi/anti use streamed_schema.len() because:
            // - For Left: columns are [left, right], so we take first streamed_schema.len()
            // - For Right: columns are [right, left], and streamed side is right, so we take first streamed_schema.len()
            let output_column_indices: Vec<usize> =
                (0..streamed_schema.fields().len()).collect();
            Ok(filtered_record_batch.project(&output_column_indices)?)
        }
        JoinType::Right | JoinType::RightMark => {
            // For right joins, add null-joined rows where mask is false
            let null_mask = compute::not(corrected_mask)?;
            let null_joined_batch = filter_record_batch(record_batch, &null_mask)?;

            if null_joined_batch.num_rows() == 0 {
                return Ok(filtered_record_batch);
            }

            // Create null columns for left side (buffered side for RIGHT join)
            let null_joined_buffered_batch = create_null_joined_batch(
                &null_joined_batch,
                buffered_schema, // Pass buffered (left) schema to create nulls for it
                JoinSide::Right,
                join_type,
                schema,
            )?;

            Ok(concat_batches(
                schema,
                &[filtered_record_batch, null_joined_buffered_batch],
            )?)
        }
        JoinType::Full => {
            // For full joins, add null-joined rows for both sides
            let joined_filter_not_matched_mask = compute::not(corrected_mask)?;
            let joined_filter_not_matched_batch =
                filter_record_batch(record_batch, &joined_filter_not_matched_mask)?;

            if joined_filter_not_matched_batch.num_rows() == 0 {
                return Ok(filtered_record_batch);
            }

            // Create null-joined batches for both sides
            let left_null_joined_batch = create_null_joined_batch(
                &joined_filter_not_matched_batch,
                buffered_schema,
                JoinSide::Left,
                join_type,
                schema,
            )?;

            Ok(concat_batches(
                schema,
                &[filtered_record_batch, left_null_joined_batch],
            )?)
        }
        JoinType::Inner => Ok(filtered_record_batch),
    }
}

/// Creates a batch with null columns for the non-joined side
///
/// Note: The input `batch` is assumed to be a fully-joined batch that already contains
/// columns from both sides. We need to extract the data side columns and replace the
/// null side columns with actual nulls.
fn create_null_joined_batch(
    batch: &RecordBatch,
    null_schema: &SchemaRef,
    join_side: JoinSide,
    join_type: JoinType,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();

    // The input batch is a fully-joined batch [left_cols..., right_cols...]
    // We need to extract the appropriate side and replace the other with nulls (or mark column)
    let columns = match (join_side, join_type) {
        (JoinSide::Left, JoinType::LeftMark) => {
            // For LEFT mark: output is [left_cols..., mark_col]
            // Batch is [left_cols..., right_cols...], extract left from beginning
            // Number of left columns = output columns - 1 (mark column)
            let left_col_count = output_schema.fields().len() - 1;
            let mut result: Vec<ArrayRef> = batch.columns()[..left_col_count].to_vec();
            result.push(Arc::new(BooleanArray::from(vec![false; num_rows])) as ArrayRef);
            result
        }
        (JoinSide::Right, JoinType::RightMark) => {
            // For RIGHT mark: output is [right_cols..., mark_col]
            // For RIGHT joins, batch is [right_cols..., left_cols...] (right comes first!)
            // Extract right columns from the beginning
            let right_col_count = output_schema.fields().len() - 1; // -1 for mark column
            let mut result: Vec<ArrayRef> = batch.columns()[..right_col_count].to_vec();
            result.push(Arc::new(BooleanArray::from(vec![false; num_rows])) as ArrayRef);
            result
        }
        (JoinSide::Left, _) => {
            // For LEFT join: output is [left_cols..., right_cols...]
            // Extract left columns, then add null right columns
            let null_columns: Vec<ArrayRef> = null_schema
                .fields()
                .iter()
                .map(|field| arrow::array::new_null_array(field.data_type(), num_rows))
                .collect();
            let left_col_count = output_schema.fields().len() - null_columns.len();
            let mut result: Vec<ArrayRef> = batch.columns()[..left_col_count].to_vec();
            result.extend(null_columns);
            result
        }
        (JoinSide::Right, _) => {
            // For RIGHT join: batch is [left_cols..., right_cols...] (same as schema)
            // We want: [null_left..., actual_right...]
            // Extract left columns from beginning, replace with nulls, keep right columns
            let null_columns: Vec<ArrayRef> = null_schema
                .fields()
                .iter()
                .map(|field| arrow::array::new_null_array(field.data_type(), num_rows))
                .collect();
            let left_col_count = null_columns.len();
            let mut result = null_columns;
            // Extract right columns starting after left columns
            result.extend_from_slice(&batch.columns()[left_col_count..]);
            result
        }
        (JoinSide::None, _) => {
            // This should not happen in normal join operations
            unreachable!(
                "JoinSide::None should not be used in null-joined batch creation"
            )
        }
    };

    // Create the batch - don't validate nullability since outer joins can have
    // null values in columns that were originally non-nullable
    use arrow::array::RecordBatchOptions;
    let mut options = RecordBatchOptions::new();
    options = options.with_row_count(Some(num_rows));
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(output_schema),
        columns,
        &options,
    )?)
}
