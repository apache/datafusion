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

use arrow::array::{Array, RecordBatch};
use arrow::compute::concat_batches;
use arrow_schema::SortOptions;
use datafusion_common::Result;
use datafusion_expr::JoinType;

/// Bounds of the non-null keys in an array sorted with `options`.
pub(super) fn non_null_range(values: &dyn Array, options: SortOptions) -> (usize, usize) {
    let null_count = values.logical_null_count();
    if options.nulls_first {
        (null_count, values.len())
    } else {
        (0, values.len() - null_count)
    }
}

/// Keep the unmatched prefix and any trailing NULL keys outside the matching range.
pub(super) fn unmatched_buffered_batch(
    batch: &RecordBatch,
    min_marked: usize,
    non_null_end: usize,
) -> Result<RecordBatch> {
    let prefix = batch.slice(0, min_marked.min(non_null_end));
    if non_null_end == batch.num_rows() {
        return Ok(prefix);
    }
    let nulls = batch.slice(non_null_end, batch.num_rows() - non_null_end);
    Ok(concat_batches(&batch.schema(), [&prefix, &nulls])?)
}

// Returns boolean for whether the join is a right existence join served by
// `RightExistencePWMJStream`, which reads nothing but a single min/max off the buffered side.
//
// `RightMark` belongs here too: deciding its mark column is the same one-key comparison as
// `RightSemi`/`RightAnti`, just kept instead of used to filter, so it needs no more of the
// buffered side than they do.
pub(super) fn is_right_existence_join(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark
    )
}

// Returns boolean to check if the join type needs to record
// buffered side matches for classic joins
pub(super) fn need_produce_result_in_final(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::Full | JoinType::Left)
}
