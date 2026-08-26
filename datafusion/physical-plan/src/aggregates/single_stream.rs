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

//! Single-stage hash aggregation stream implementation.
//!
//! See comments in [`SingleHashAggregateStream`] for details.

use std::mem::size_of;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, assert_ne_or_internal_err};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{TaskContext, TryEmitter, async_try_stream};
use futures::stream::StreamExt;

use super::aggregate_hash_table::{
    AggregateHashTable, ClusteredAggregateTableMetrics, SingleMarker,
};
use super::order::GroupCompletionMode;
use super::spill::AggregateSpill;
use super::{AggregateExec, create_schema};
use crate::SendableRecordBatchStream;
use crate::aggregates::AggregateMode;
use crate::metrics::{BaselineMetrics, SpillMetrics};
use crate::stream::{ObservedStream, RecordBatchStreamAdapter};

/// Hash aggregation can run the full logical aggregation in one operator. This
/// stream implements the single stage for grouped hash aggregation.
///
/// This aggregation variant is useful when:
/// - There is only one partition (config `target_partitions` is set to 1)
/// - When input is already partitioned (`t` is backed by Parquet files, that is range/hash
///   partitioned on the group keys), the single aggregation mode is the most efficient
///   approach to use.
///
/// # Example
///
/// SELECT k, AVG(v) FROM t GROUP BY k;
///
/// ## Plan
/// AggregateExec(stage=single)
/// -- DataSourceExec(t)
///
/// ## Single Stage Behavior
/// Input: raw rows
/// Output: final aggregate values for all groups (for example, `AVG(x)`)
///
/// This stream implements the complete aggregation without a partial/final
/// split. It consumes raw input rows and emits final aggregate values.
///
/// # Grouping Sets
///
/// `GROUPING SETS`, `CUBE` and `ROLLUP` are expanded while consuming raw input:
/// every grouping set of an input batch is evaluated and interned into the same
/// hash table, the same way [`super::hash_stream::PartialHashAggregateStream`]
/// does it. When spilling, the expanded keys are sorted and replayed as a plain
/// group by.
///
/// # Spilling
///
/// During aggregation, group keys and states accumulate. If memory usage exceeds
/// the budget, spilling is triggered as follows:
/// 1. After aggregating a new input batch, if the memory reservation exceeds its
///    limit, spill all accumulated groups and states.
///    - Sort all groups by the group keys before spilling.
/// 2. Repeat until the input is exhausted.
/// 3. Perform a sort-preserving merge of all spill files and feed the merged output
///    into an ordered streaming aggregation, which ensures bounded memory usage and
///    evaluates the final result.
///    - [`ClusteredFinalAggregateStream`](super::clustered_final_stream::ClusteredFinalAggregateStream) is reused for the streaming aggregation.
///
/// # Optimization: DISTINCT LIMIT Soft Limit
///
/// When the input has only one partition or the input is already partitioned,
/// unordered distinct queries such as:
///
/// ```sql
/// SELECT DISTINCT x FROM t LIMIT 10;
/// ```
///
/// are optimized into a single-stage aggregate like:
///
/// ```txt
/// LimitExec, limit=10
/// --AggregateExec(Single), group_by=[x], aggr=[], soft_limit=10
/// ---- Scan(t)
/// ```
///
/// After each input batch, the stream checks whether the soft limit has been
/// reached. If so, it emits the accumulated groups and stops reading input.
///
/// This early termination is skipped after spilling has occurred to keep the
/// spill and replay path simple. In that case, the stream consumes the remaining
/// input and merges all spill runs before producing output.
///
/// This operator does not guarantee an exact limit because a single batch can
/// cross the threshold. The downstream limit operator enforces the exact result
/// size.
pub(crate) struct SingleHashAggregateStream {
    /// Covers group keys, accumulators, and spill sorting; replay reuses it
    /// through a sibling reservation.
    reservation: MemoryReservation,
    context: SingleHashAggregateContext,
    stage: ExecutionStage,
}

/// Execution stages described in [`SingleHashAggregateStream::into_stream`].
enum ExecutionStage {
    Aggregating(Aggregating),
    Outputting(Outputting),
    MergingSpills(SendableRecordBatchStream),
}

struct Aggregating {
    /// Input batches containing raw rows, not partial aggregate state.
    input: SendableRecordBatchStream,
    /// The hash table owns the lower-level state for emitting output batches.
    hash_table: AggregateHashTable<SingleMarker>,
    /// `None` if spilling is not supported by the configured `DiskManager`.
    spill_context: Option<Box<AggregateSpill>>,
}

struct Outputting {
    /// Hash table that has switched to output mode; final aggregate values are
    /// emitted from it incrementally.
    hash_table: AggregateHashTable<SingleMarker>,
}

/// Immutable execution context shared by aggregation and output emission.
struct SingleHashAggregateContext {
    /// Output schema: group columns followed by final aggregate value columns.
    schema: SchemaRef,
    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,
    /// See the "Optimization: DISTINCT LIMIT Soft Limit" section in
    /// [`SingleHashAggregateStream`] for details.
    group_values_soft_limit: Option<usize>,
}

impl SingleHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            AggregateMode::Single | AggregateMode::SinglePartitioned
        ));
        debug_assert_eq!(agg.group_completion_mode, GroupCompletionMode::None);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let input_schema = input.schema();
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);
        let spill_metrics = SpillMetrics::new(&agg.metrics, partition);
        let state_schema = Arc::new(create_schema(
            input_schema.as_ref(),
            agg.group_by(),
            agg.aggr_expr(),
            AggregateMode::Partial,
        )?);

        let hash_table = AggregateHashTable::<SingleMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            Arc::clone(&state_schema),
            batch_size,
        )?;

        let can_spill = context.runtime_env().disk_manager.tmp_files_enabled();
        let spill_context = if can_spill {
            Some(Box::new(AggregateSpill::try_new(
                "SingleHashAggregateSpill",
                agg,
                context,
                partition,
                batch_size,
                &GroupCompletionMode::None,
                &state_schema,
                spill_metrics,
            )?))
        } else {
            None
        };

        let reservation =
            MemoryConsumer::new(format!("SingleHashAggregateStream[{partition}]"))
                .with_can_spill(can_spill)
                .register(context.memory_pool());

        // Reserve memory for the initial hash table. that we hold for the lifetime of the stream.
        reservation.try_grow(hash_table.memory_size())?;

        Ok(Self {
            reservation,
            context: SingleHashAggregateContext {
                schema,
                baseline_metrics,
                group_values_soft_limit: agg.limit_options().map(|config| config.limit()),
            },
            stage: ExecutionStage::Aggregating(Aggregating {
                input,
                hash_table,
                spill_context,
            }),
        })
    }

    /// Entry point for the single hash aggregate execution stages.
    ///
    /// # Stage transition graph:
    ///
    /// ```text
    ///                  +----[2]----+                +----[4]----+
    ///                  |           |                |           |
    ///                  v           |                v           |
    ///              +-------------------+         +------------------+
    ///              |                   |         |                  |
    /// (start)-[1]->|    Aggregating    |---[3]-->|    Outputting    |
    ///              |                   |         |                  |
    ///              +-------------------+         +------------------+
    ///                        |                            |
    ///                       [6]                          [5]
    ///                        |                            |
    ///                        v                            v
    ///              +-------------------+         +------------------+
    ///              |                   |         |                  |
    ///         +--->|   MergingSpills   |---[8]-->|      Done        |-[9]->(end)
    ///         |    |                   |         |                  |
    ///         |    +-------------------+         +------------------+
    ///         |              |
    ///         +-----[7]------+
    /// ```
    ///
    /// ## Stages
    ///
    /// - [`Aggregating`]: Aggregate raw input batches into the hash table.
    /// - [`Outputting`]: Emit final aggregate values from the in-memory hash table.
    /// - [`ExecutionStage::MergingSpills`]: If OOM and spilled before, use this
    ///   stage to finish execution.
    ///
    /// ## Transition Edges
    ///
    /// 1. Start.
    /// 2. Aggregate one input batch:
    ///    - If memory fits, continue reading input.
    ///    - If OOM, spill all accumulated groups as one sorted run and continue.
    /// 3. Input was exhausted without spilling, or the soft group limit was
    ///    reached before any spill: switch the hash table to output mode.
    /// 4. Incremental output at `batch_size`.
    /// 5. All final aggregate values were emitted.
    /// 6. Input was exhausted after spilling: spill the last in-memory run and
    ///    build the replay stream over the merged spill runs.
    /// 7. Forward output of the replay stream.
    /// 8. The replay stream was fully consumed.
    /// 9. End.
    pub(crate) fn into_stream(self) -> SendableRecordBatchStream {
        let Self {
            reservation,
            context,
            stage,
        } = self;
        let schema = Arc::clone(&context.schema);
        let metrics = context.baseline_metrics.clone();
        let stream = async_try_stream(|mut emitter| async move {
            let mut stage = Some(stage);
            while let Some(current_stage) = stage {
                stage = match current_stage {
                    ExecutionStage::Aggregating(aggregating) => {
                        aggregating.handle_stage(&context, &reservation).await?
                    }
                    ExecutionStage::Outputting(outputting) => {
                        outputting
                            .handle_stage(&context, &reservation, &mut emitter)
                            .await?
                    }
                    ExecutionStage::MergingSpills(mut stream) => {
                        while let Some(batch) = stream.next().await.transpose()? {
                            emitter.emit(batch).await;
                        }
                        None
                    }
                };
            }
            Ok(())
        });
        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));
        Box::pin(ObservedStream::new(stream, metrics, None))
    }
}

impl Aggregating {
    /// Reserve memory for the current aggregate table.
    fn reservation_size(&self) -> usize {
        let table_size = self.hash_table.memory_size();
        if self.spill_context.is_some() {
            // Count extra space needed for in-memory sorting and spilling. Only
            // count memory for indices, the payload will be materialize incrementally
            // in smaller chunks.
            table_size.saturating_add(
                self.hash_table
                    .building_group_count()
                    .saturating_mul(size_of::<u32>()),
            )
        } else {
            table_size
        }
    }

    fn has_spills(&self) -> bool {
        self.spill_context.as_ref().is_some_and(|s| s.has_spills())
    }

    /// See the "Optimization: DISTINCT LIMIT Soft Limit" section in
    /// [`SingleHashAggregateStream`] for details.
    fn hit_soft_group_limit(&self, context: &SingleHashAggregateContext) -> bool {
        context
            .group_values_soft_limit
            .is_some_and(|limit| limit <= self.hash_table.building_group_count())
    }

    /// Aggregates raw input batches until the input is exhausted or the soft
    /// group limit is reached, then moves to output or spill replay.
    async fn handle_stage(
        mut self,
        context: &SingleHashAggregateContext,
        reservation: &MemoryReservation,
    ) -> Result<Option<ExecutionStage>> {
        debug_assert!(self.hash_table.is_building());
        let elapsed_compute = context.baseline_metrics.elapsed_compute();

        while let Some(batch) = self.input.next().await.transpose()? {
            let _timer = elapsed_compute.timer();
            self.hash_table.aggregate_batch(&batch)?;

            match reservation.try_resize(self.reservation_size()) {
                Ok(()) => {}
                Err(oom @ DataFusionError::ResourcesExhausted(_)) => {
                    let Some(spill_context) = self.spill_context.as_mut() else {
                        return Err(oom.context(
                            "Single hash aggregate cannot spill because temporary files are not enabled in the DiskManager",
                        ));
                    };
                    assert_ne_or_internal_err!(
                        self.hash_table.building_group_count(),
                        0,
                        "Single hash aggregate ran out of memory with no aggregated groups"
                    );
                    spill_context.sort_and_spill(self.hash_table.take_state_batch()?)?;
                    reservation
                        .try_resize(self.hash_table.memory_size())
                        .map_err(|e| {
                            e.context(
                                "Decreasing allocation after spilling should succeed",
                            )
                        })?;
                    continue;
                }
                Err(e) => return Err(e),
            }

            // Soft group limits are usually small and rarely coincide with
            // spilling. Once spilling has occurred, skip this optimization to
            // make the internal logic simpler.
            if self.hit_soft_group_limit(context) && !self.has_spills() {
                break;
            }
        }

        // Release upstream resources before producing output or replaying spills.
        drop(self.input);
        let _timer = elapsed_compute.timer();

        if let Some(mut spill_context) = self.spill_context.filter(|s| s.has_spills()) {
            // Input was exhausted after spilling. Spill the last in-memory run.
            let mut hash_table = self.hash_table;
            hash_table
                .take_state_batch()
                .and_then(|batch| spill_context.sort_and_spill(batch))?;

            // Construct the replay stream: an ordered final aggregate stream
            // over the sort-preserving merge of all spill runs.
            let metrics = ClusteredAggregateTableMetrics::from_hash_table(&hash_table);
            drop(hash_table);
            reservation.try_resize(0)?;
            // The outer ObservedStream counts output; replay only shares compute time.
            let stream = spill_context.into_replay_stream(
                &context.baseline_metrics.intermediate(),
                metrics,
                reservation.new_empty(),
            )?;
            return Ok(Some(ExecutionStage::MergingSpills(stream)));
        }

        // Either all the input fit in memory or hit soft group limit with no spilling
        let mut hash_table = self.hash_table;
        hash_table.start_output()?;
        Ok(Some(ExecutionStage::Outputting(Outputting { hash_table })))
    }
}

impl Outputting {
    /// Emits final aggregate value batches:
    /// Input was exhausted without spilling, or the soft group limit was reached.
    async fn handle_stage(
        self,
        context: &SingleHashAggregateContext,
        reservation: &MemoryReservation,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<Option<ExecutionStage>> {
        let Self { mut hash_table } = self;
        debug_assert!(!hash_table.is_building());
        let elapsed_compute = context.baseline_metrics.elapsed_compute();
        let mut reserved = true;

        loop {
            let timer = elapsed_compute.timer();
            let Some(batch) = hash_table.next_output_batch()? else {
                // Only reachable when the table held no groups at all: a
                // non-empty table always reports its last batch together with
                // the `Done` state, which the `try_resize` below already zeroes.
                reservation.try_resize(0)?;
                return Ok(None);
            };

            debug_assert!(batch.num_rows() > 0);

            // The table hands over its groups as they are materialized and
            // reports a size of 0 once it reaches `Done`, so this releases the
            // reservation before the final batch goes downstream.
            if reserved {
                match reservation.try_resize(hash_table.memory_size()) {
                    Ok(()) => {}
                    Err(DataFusionError::ResourcesExhausted(_)) => {
                        // Already materialized and nothing left to spill: hand it off unreserved.
                        reservation.try_resize(0)?;
                        reserved = false;
                    }
                    Err(e) => return Err(e),
                }
            }

            timer.done();
            emitter.emit(batch).await;
        }
    }
}
