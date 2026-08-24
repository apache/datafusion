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

//! Nested loop join stream specifically for semi, anti, and mark joins
//! Instantiated by [`NestedLoopJoinExec`](crate::joins::nested_loop_join::NestedLoopJoinExec)
//! when the join type is `LeftSemi`, `LeftAnti`, `RightSemi`, `RightAnti`,
//! `LeftMark`, or `RightMark`.

use std::sync::Arc;

use super::materializing_stream::{
    JoinLeftData, NLJState, NestedLoopJoinMetrics, SpillState,
};
use crate::SendableRecordBatchStream;
use crate::joins::utils::{ColumnIndex, JoinFilter, OnceFut};
use crate::stream::{ObservedStream, RecordBatchStreamAdapter};

use arrow::compute::BatchCoalescer;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, JoinSide};
use datafusion_execution::{TryEmitter, async_try_stream};
use datafusion_expr::JoinType;

/// States for join processing
#[derive(Debug, Clone, Copy)]
enum SAMNLJState {
    BufferingLeft,
    FetchingRight,
    ProbeRight,
    /// Entered exactly once per left chunk, when the probe (right) side is
    /// exhausted and probing for the current chunk is finished. This state
    /// owns the single [`JoinLeftData::report_probe_completed`] call that
    /// decrements the shared probe-threads counter.
    ProbeEnd,
    EmitLeftResult,
    EmitRightResult,
    /// Emit rows using the global bitmap accumulated across all left chunks.
    /// Only used in memory-limited mode for join types that require
    /// tracking right-side matches in the final output (RIGHT SEMI/ANTI/MARK)
    EmitGlobalRightResult,
    Done,
}

/// Nested loop join stream for Semi/Anti/Mark joins.
///
/// Evaluates the join predicate for every relevant left/right combination but unlike `materializing_stream`,
/// this does not emit `(left, right)` pairs - instead we accumulate a Boolean value for each row
/// on the output side to check for any match
///
/// For left joins:
///     - matches accumulate in the shared left bitmap
///     - every right partition must finish probing
/// For right joins:
///     - matches accumulate for each right batch
///     - result can be emitted once batch has been compared with all buffered left rows (without spill)
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
    output_schema: Arc<Schema>,
    /// join filter
    join_filter: Option<JoinFilter>,
    /// type of the join
    join_type: JoinType,
    /// output side of the join
    join_side: JoinSide,
    /// the probe-side(right) table data of the nested loop join
    /// `Option` is used because memory-limited path requires resetting it.
    right_data: Option<SendableRecordBatchStream>,
    /// the build-side table data of the nested loop join
    left_data: OnceFut<JoinLeftData>,
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
    column_indices: Vec<ColumnIndex>,
    /// Join execution metrics
    metrics: NestedLoopJoinMetrics,

    /// `batch_size` from configuration
    batch_size: usize,

    // ========================================================================
    // STATE FLAGS/BUFFERS:
    // Fields that hold intermediate data/flags during execution
    // ========================================================================
    /// State Tracking
    state: SAMNLJState,
    /// Output buffer holds the join result to output. It will emit eagerly when
    /// the threshold is reached.
    output_buffer: Box<BatchCoalescer>,

    /// Memory-limited spill fallback state. See [`SpillState`] for details.
    spill_state: SpillState,
}

impl SemiAntiMarkNestedLoopJoinStream {
    #[expect(clippy::too_many_arguments)]
    // TODO: fix later
    pub(crate) fn new(
        schema: Arc<Schema>,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        right_data: SendableRecordBatchStream,
        left_data: OnceFut<JoinLeftData>,
        column_indices: Vec<ColumnIndex>,
        metrics: NestedLoopJoinMetrics,
        batch_size: usize,
        spill_state: SpillState,
    ) -> Result<SendableRecordBatchStream> {
        debug_assert!(
            matches!(
                join_type,
                JoinType::LeftSemi
                    | JoinType::RightSemi
                    | JoinType::LeftAnti
                    | JoinType::RightAnti
                    | JoinType::LeftMark
                    | JoinType::RightMark
            ),
            "SemiAntiMarkNestedLoopJoinStream does not handle {join_type:?}"
        );

        let join_side = match join_type {
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
                JoinSide::Left
            }
            _ => JoinSide::Right,
        };

        let state = Self {
            output_schema: Arc::clone(&schema),
            join_filter: filter,
            join_type,
            join_side,
            right_data: Some(right_data),
            column_indices,
            left_data,
            metrics,
            output_buffer: Box::new(BatchCoalescer::new(schema, batch_size)),
            batch_size,
            state: SAMNLJState::BufferingLeft,
            spill_state,
        };

        let stream = async_try_stream(|mut emitter| async move {
            state.start_join_time();
            let result = state.join(&mut emitter).await;
            state.stop_join_time();
            result
        });
        // ObservedStream records the baseline metrics (output rows/batches,
        // end time) exactly as the former hand-written poll_next did.
        Ok(Box::pin(ObservedStream::new(
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
            baseline_metrics,
            None,
        )))
    }

    /// Start (resume) the `join_time` clock.
    fn start_join_time(&mut self) {
        // debug_assert!(self.join_time_start.is_none(), "join_time already running");
        // self.join_time_start = Some(Instant::now());
    }

    /// Stop (pause) the `join_time` clock, accumulating the elapsed span.
    ///
    /// Called around awaits whose duration is not the join's own work: the
    /// child input streams' `next()` and `emitter.emit()` (where the
    /// consumer processes the batch). The join's own spill read-back is NOT
    /// excluded — that time is join work.
    fn stop_join_time(&mut self) {
        // if let Some(start) = self.join_time_start.take() {
        //     self.join_time.add_elapsed(start);
        // }
    }

    /// Main loop - TODO describe further
    async fn join(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        Ok(())
    }
}
