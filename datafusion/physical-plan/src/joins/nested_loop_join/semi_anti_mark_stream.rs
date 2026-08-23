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

//! Semi, anti, and mark nested loop join stream.

use std::sync::Arc;

use super::materializing_stream::{
    JoinLeftData, NLJState, NestedLoopJoinMetrics, SpillState,
};
use crate::SendableRecordBatchStream;
use crate::joins::utils::{ColumnIndex, JoinFilter, OnceFut};

use arrow::array::BooleanArray;
use arrow::compute::BatchCoalescer;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion_expr::JoinType;

#[expect(dead_code)]
pub(crate) struct SemiAntiMarkNestedLoopJoinStream {
    // ========================================================================
    // PROPERTIES:
    // Operator's properties that remain constant
    //
    // Note: The implementation uses the terms left/build-side table and
    // right/probe-side table interchangeably. Treating the left side as the
    // build side is a convention in DataFusion: the planner always tries to
    // swap the smaller table to the left side.
    // ========================================================================
    /// Output schema
    pub(crate) output_schema: Arc<Schema>,
    /// join filter
    pub(crate) join_filter: Option<JoinFilter>,
    /// type of the join
    pub(crate) join_type: JoinType,
    /// the probe-side(right) table data of the nested loop join
    /// `Option` is used because memory-limited path requires resetting it.
    pub(crate) right_data: Option<SendableRecordBatchStream>,
    /// the build-side table data of the nested loop join
    pub(crate) left_data: OnceFut<JoinLeftData>,
    /// Projection to construct the output schema from the left and right tables.
    /// Example:
    /// - output_schema: ['a', 'c']
    /// - left_schema: ['a', 'b']
    /// - right_schema: ['c']
    ///
    /// The column indices would be [(left, 0), (right, 0)] -- taking the left
    /// 0th column and right 0th column can construct the output schema.
    ///
    /// Note there are other columns ('b' in the example) still kept after
    /// projection pushdown; this is because they might be used to evaluate
    /// the join filter (e.g., `JOIN ON (b+c)>0`).
    pub(crate) column_indices: Vec<ColumnIndex>,
    /// Join execution metrics
    pub(crate) metrics: NestedLoopJoinMetrics,

    /// `batch_size` from configuration
    batch_size: usize,

    /// See comments in [`need_produce_right_in_final`] for more detail
    should_track_unmatched_right: bool,

    // ========================================================================
    // STATE FLAGS/BUFFERS:
    // Fields that hold intermediate data/flags during execution
    // ========================================================================
    /// State Tracking
    state: NLJState,
    /// Output buffer holds the join result to output. It will emit eagerly when
    /// the threshold is reached.
    output_buffer: Box<BatchCoalescer>,
    /// See comments in [`NLJState::Done`] for its purpose
    handled_empty_output: bool,

    // Buffer(left) side
    // -----------------
    /// The current buffered left data to join
    buffered_left_data: Option<Arc<JoinLeftData>>,
    /// Index into the left buffered batch. Used in `ProbeRight` state
    left_probe_idx: usize,
    /// Index into the left buffered batch. Used in `EmitLeftUnmatched` state
    left_emit_idx: usize,
    /// Should we go back to `BufferingLeft` state again after `EmitLeftUnmatched`
    /// state is over.
    left_exhausted: bool,
    /// If we can buffer all left data in one pass (false means memory-limited multi-pass)
    left_buffered_in_one_pass: bool,

    // Probe(right) side
    // -----------------
    /// The current probe batch to process
    current_right_batch: Option<RecordBatch>,
    // For right join, keep track of matched rows in `current_right_batch`
    // Constructed when fetching each new incoming right batch in `FetchingRight` state.
    current_right_batch_matched: Option<BooleanArray>,

    /// Memory-limited spill fallback state. See [`SpillState`] for details.
    spill_state: SpillState,

    /// Whether this stream is the one responsible for emitting unmatched-left
    /// rows for the current left chunk. Set in the [`NLJState::ProbeEnd`] state,
    /// which is entered exactly once per chunk and owns the single
    /// [`JoinLeftData::report_probe_completed`] call: the stream that drives the
    /// shared probe-threads counter to zero (the last to finish probing) becomes
    /// the emitter. Because the decrement happens once in `ProbeEnd` rather than
    /// in the re-enterable `EmitLeftUnmatched` state, the counter can never be
    /// decremented twice, so it cannot reach zero before all partitions finish
    /// probing (which would otherwise let a partition emit spurious NULL-padded
    /// unmatched-left rows early).
    is_unmatched_left_emitter: bool,
}
