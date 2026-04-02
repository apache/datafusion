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
//! - Deferred filtering for outer joins
//! - Metadata tracking for grouping output rows by input row
//! - Correcting filter masks to handle multiple matches per input row

use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, RecordBatch,
    RecordBatchOptions, UInt64Array, UInt64Builder, new_null_array,
};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{self, filter_record_batch};
use arrow::datatypes::SchemaRef;
use datafusion_common::{JoinSide, JoinType, Result};

use crate::joins::utils::JoinFilter;

/// Metadata for tracking filter results during deferred filtering
///
/// When a join filter is present and we need to ensure each input row produces
/// at least one output (outer joins), we can't filter immediately. Instead,
/// we accumulate all joined rows with metadata, then post-process to determine
/// which rows to output.
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

        self.filter_mask.extend(filter_mask);
        self.row_indices.extend(row_indices);
        self.batch_ids
            .resize(self.batch_ids.len() + row_indices.len(), batch_id);
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
pub fn needs_deferred_filtering(
    filter: &Option<JoinFilter>,
    join_type: JoinType,
) -> bool {
    filter.is_some()
        && matches!(join_type, JoinType::Left | JoinType::Right | JoinType::Full)
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
/// - `false`: Convert to null-joined row (outer joins)
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
        JoinType::Left | JoinType::Right | JoinType::Full => {
            // For each input row group: keep first filter-passing row,
            // discard (null) remaining matches, null-join if none passed.
            // Null metadata entries are already-null-joined rows that
            // flow through unchanged to preserve output ordering.
            for i in 0..row_indices_length {
                let last_index =
                    last_index_for_row(i, row_indices, batch_ids, row_indices_length);
                if filter_mask.is_null(i) {
                    corrected_mask.append_value(true);
                } else if filter_mask.value(i) {
                    seen_true = true;
                    corrected_mask.append_value(true);
                } else if seen_true || !filter_mask.value(i) && !last_index {
                    corrected_mask.append_null();
                } else {
                    corrected_mask.append_value(false);
                }

                if last_index {
                    seen_true = false;
                }
            }

            corrected_mask.append_n(expected_size - corrected_mask.len(), false);
            Some(corrected_mask.finish())
        }
        JoinType::LeftMark
        | JoinType::RightMark
        | JoinType::LeftSemi
        | JoinType::RightSemi
        | JoinType::LeftAnti
        | JoinType::RightAnti => {
            unreachable!("Semi/anti/mark joins are handled by BitwiseSortMergeJoinStream")
        }
        JoinType::Inner => None,
    }
}

/// Applies corrected filter mask to record batch based on join type
///
/// The corrected mask has three possible values per row:
/// - `true`: Keep the row as-is (matched and passed filter)
/// - `false`: Convert to null-joined row (all filter matches failed for this input row)
/// - `null`: Discard the row entirely (duplicate match for an already-output input row)
///
/// This function preserves input row ordering by processing each row in place
/// rather than separating matched/unmatched rows.
pub fn filter_record_batch_by_join_type(
    record_batch: &RecordBatch,
    corrected_mask: &BooleanArray,
    join_type: JoinType,
    schema: &SchemaRef,
    buffered_schema: &SchemaRef,
) -> Result<RecordBatch> {
    match join_type {
        JoinType::Left | JoinType::Right | JoinType::Full => {
            if record_batch.num_rows() == 0 {
                return Ok(record_batch.clone());
            }

            // Discard null-masked rows (keep true + false only)
            let keep_mask = compute::is_not_null(corrected_mask)?;
            let kept_batch = filter_record_batch(record_batch, &keep_mask)?;

            if kept_batch.num_rows() == 0 {
                return Ok(kept_batch);
            }

            let kept_corrected = compute::filter(corrected_mask, &keep_mask)?;
            let kept_corrected = kept_corrected
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();

            // All rows passed the filter — no null-joining needed
            if kept_corrected.true_count() == kept_corrected.len() {
                return Ok(kept_batch);
            }

            // For false entries: replace the non-preserved side with nulls.
            // This preserves row ordering unlike filter+concat.
            let (null_side_start, null_side_len) = match join_type {
                JoinType::Left => {
                    // Left join: null out right (buffered) columns
                    let left_cols =
                        schema.fields().len() - buffered_schema.fields().len();
                    (left_cols, buffered_schema.fields().len())
                }
                JoinType::Right => {
                    // Right join: null out left (buffered) columns
                    (0, buffered_schema.fields().len())
                }
                JoinType::Full => {
                    // Full join: null out buffered columns for streamed rows
                    // that matched but failed the filter. Unmatched buffered
                    // rows are null-joined on the streamed side separately
                    // when the buffered batch is drained.
                    let left_cols =
                        schema.fields().len() - buffered_schema.fields().len();
                    (left_cols, buffered_schema.fields().len())
                }
                _ => unreachable!(),
            };

            let num_rows = kept_batch.num_rows();
            let mut columns: Vec<ArrayRef> = kept_batch.columns().to_vec();

            for col in columns.iter_mut().skip(null_side_start).take(null_side_len) {
                let null_array = new_null_array(col.data_type(), num_rows);
                *col = zip(kept_corrected, &*col, &null_array)?;
            }

            let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
            Ok(RecordBatch::try_new_with_options(
                Arc::clone(schema),
                columns,
                &options,
            )?)
        }
        JoinType::LeftSemi
        | JoinType::LeftAnti
        | JoinType::RightSemi
        | JoinType::RightAnti
        | JoinType::LeftMark
        | JoinType::RightMark => unreachable!(
            "Semi/anti/mark joins are handled by SemiAntiMarkSortMergeJoinStream"
        ),
        JoinType::Inner => Ok(filter_record_batch(record_batch, corrected_mask)?),
    }
}
