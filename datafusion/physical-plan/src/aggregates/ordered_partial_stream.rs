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

//! Partial aggregate stream for ordered group input.

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{TaskContext, TryEmitter, async_try_stream};
use futures::stream::StreamExt;

use super::AggregateExec;
use super::aggregate_hash_table::{OrderedAggregateTable, PartialMarker};
use crate::aggregates::AggregateMode;
use crate::aggregates::order::GroupOrdering;
use crate::metrics::{BaselineMetrics, MetricBuilder, SpillMetrics};
use crate::stream::{ObservedStream, RecordBatchStreamAdapter};
use crate::{InputOrderMode, SendableRecordBatchStream, metrics};

/// Partial aggregate stream for `InputOrderMode::Sorted` and
/// `InputOrderMode::PartiallySorted`.
///
/// # Example
///
/// SELECT k, AVG(v) FROM t GROUP BY k;
///
/// If the input is ordered by `k`, the aggregate can use ordered partial and
/// final stages:
///
/// ## Plan
/// AggregateExec(stage=final, ordered)
/// -- RepartitionExec(hash(k), preserves_order=true)
/// ---- AggregateExec(stage=partial, ordered)
///
/// ## Partial Stage Behavior
/// Input: raw rows
/// Output: partial states for all groups (for example, `AVG(x)` emits `SUM(x)`
/// and `COUNT(x)`)
///
/// ## Final Stage Behavior
/// Input: partial states
/// Output: results for all groups (for example, `AVG(x)` calculated from the
/// state)
///
/// # Order-based Optimization
///
/// For the aggregation work, the hash aggregation implementation is reused.
///
/// After each input batch, check whether any groups can be emitted eagerly to
/// improve memory efficiency. For example, if the last group key seen is
/// `k = 100`, it is safe to emit all groups with keys less than 100 because the
/// input is ordered. Materialize that entire completed prefix once, then emit
/// slices of it before reading more input. This avoids repeatedly removing small
/// batches of groups and shifting the remaining hash table and accumulator state.
///
/// # Memory Pressure and Spilling
///
/// ## Fully ordered case
///
/// If the input is ordered by every group key, for example:
///
/// - Input order: `a, b`
/// - `GROUP BY`: `a, b`
///
/// Completed groups can be emitted as soon as the next group is observed. Thus,
/// only the current group remains active after completed groups are emitted, and
/// memory usage does not grow with the total number of groups.
///
/// If a memory reservation nevertheless fails, the stream returns the error
/// directly, indicating an unexpected behavior.
///
/// ## Partially ordered case
///
/// If the input is ordered by only a subset of the group keys, for example:
///
/// - Input order: `a`
/// - `GROUP BY`: `a, b`
///
/// If one `a` value contains many distinct `b` values, the table may accumulate
/// enough groups to exceed the memory limit.
///
/// - `OrderedPartialAggregateStream`: On reservation failure, it emits all current
///   intermediate states downstream and resets the table. The final stage can
///   merge repeated `(a, b)` state rows, so no disk spill is required.
/// - `OrderedFinalAggregateStream`: It cannot emit incomplete final results. On
///   reservation failure, it sorts the current intermediate states by the complete
///   group key and spills them as one run. After the input ends, it spills any
///   remaining states, performs a sort-preserving merge of all runs, and feeds the
///   merged input into a fully ordered final aggregate stream.
pub(crate) struct OrderedPartialAggregateStream {
    reservation: MemoryReservation,
    context: OrderedPartialAggregateContext,
    stage: ExecutionStage,
}

/// Execution stages described in [`OrderedPartialAggregateStream::into_stream`].
enum ExecutionStage {
    Aggregating(Aggregating),
    Outputting(Outputting),
}

struct Aggregating {
    input: SendableRecordBatchStream,
    table: OrderedAggregateTable<PartialMarker>,
}

struct Outputting {
    /// Materialized aggregate states. Each iteration emits the first `batch_size`
    /// rows and replaces this batch with the remaining slice.
    batch: RecordBatch,
    /// Aggregation stage to resume after output; `None` after EOF.
    resume: Option<Aggregating>,
}

/// Immutable execution context shared by aggregation and output emission.
struct OrderedPartialAggregateContext {
    schema: SchemaRef,
    batch_size: usize,
    baseline_metrics: BaselineMetrics,
    reduction_factor: metrics::RatioMetrics,
}

impl OrderedPartialAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert_eq!(agg.mode, AggregateMode::Partial);
        debug_assert_ne!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);
        let reduction_factor = MetricBuilder::new(&agg.metrics)
            .with_type(metrics::MetricType::Summary)
            .ratio_metrics("reduction_factor", partition);

        let table = OrderedAggregateTable::<PartialMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;
        let reservation =
            MemoryConsumer::new(format!("OrderedPartialAggregateStream[{partition}]"))
                .with_can_spill(matches!(
                    table.group_ordering(),
                    GroupOrdering::Partial(_)
                ))
                .register(context.memory_pool());

        Ok(Self {
            reservation,
            context: OrderedPartialAggregateContext {
                schema,
                batch_size,
                baseline_metrics,
                reduction_factor,
            },
            stage: ExecutionStage::Aggregating(Aggregating { input, table }),
        })
    }

    /// Entry point for the ordered partial aggregate execution stages.
    ///
    /// See [`OrderedPartialAggregateStream`] for high-level ideas.
    ///
    /// # Stage transition graph:
    ///
    /// ```text
    ///                  +----[2]----+                     +----[5]----+
    ///                  |           |                     |           |
    ///                  v           |                     v           |
    ///              +-------------------+             +-------------------+
    ///              |                   |             |                   |
    /// (start)-[1]->|    Aggregating    |-----[3]---->|     Outputting    |
    ///              |                   |<----[6]-----|                   |
    ///              +-------------------+             +-------------------+
    ///                        | [4]                             | [7]
    ///                        |                                 |
    ///                        +----------------+----------------+
    ///                                         |
    ///                                         v
    ///                                    +---------+
    ///                                    |   Done  |--[8]--> (end)
    ///                                    +---------+
    /// ```
    ///
    /// ## Stages
    ///
    /// - [`Aggregating`]: Aggregates raw input and materializes one batch of partial
    ///   states.
    /// - [`Outputting`]: Emits slices of one materialized batch. If the materialized
    ///   buffers cannot be reserved while slicing, hand off the whole batch, then
    ///   resume aggregation or finish as described below.
    ///
    /// ### Incremental output
    ///
    /// Consider this query with input ordered only by `k1`:
    ///
    /// ```sql
    /// SELECT k1, k2, AVG(v)
    /// FROM table_with_order_k1
    /// GROUP BY k1, k2
    /// ```
    ///
    /// Suppose one `k1` value spans 1M rows with distinct, unordered `k2`
    /// values. Ordering only proves these 1M `(k1, k2)` groups complete when
    /// `k1` changes, so a single early emission can produce far more than
    /// `batch_size` rows.
    ///
    /// Emitting those groups in small batches through [EmitTo::First] would
    /// repeatedly remove a prefix from [`GroupValues`]. Because the group
    /// values are stored contiguously, each removal copies the remaining values
    /// and updates their group indexes.
    ///
    /// To avoid repeating that work, this stream:
    ///
    /// 1. Materializes all completed groups into one large batch.
    /// 2. Emits `batch_size` slices that share the batch's buffers.
    ///
    /// Blocked aggregate state management may simplify this approach:
    /// <https://github.com/apache/datafusion/issues/24704>
    ///
    /// [`GroupValues`]: crate::aggregates::group_values::GroupValues
    /// [EmitTo::First]: datafusion_expr::EmitTo::First
    ///
    ///
    /// ## Transition Edges
    ///
    /// 1. Start.
    /// 2. Aggregate one input batch. If memory fits and no groups are complete,
    ///    continue reading input.
    /// 3. Prepare output:
    ///    - Ordering proves a prefix complete: materialize the entire prefix once,
    ///      retaining the input and active groups to resume aggregation.
    ///    - On memory pressure with partial ordering, materialize all current
    ///      states instead, including incomplete groups, and reset the table.
    ///    - At EOF, materialize all remaining states and prepare to output.
    /// 4. Input was exhausted with no remaining groups, directly end.
    /// 5. Yield one slice without materializing the table again. Keep the shared
    ///    buffers reserved until handing off the last slice.
    /// 6. The batch was fully emitted and retained aggregation can resume.
    /// 7. The output batch was fully emitted.
    /// 8. End.
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
                };
            }
            Ok(())
        });
        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));
        Box::pin(ObservedStream::new(stream, metrics, None))
    }
}

impl Aggregating {
    /// Aggregates raw input and materializes one batch of partial states.
    ///
    /// See [`OrderedPartialAggregateStream::into_stream`] for stage transitions.
    async fn handle_stage(
        mut self,
        context: &OrderedPartialAggregateContext,
        reservation: &MemoryReservation,
    ) -> Result<Option<ExecutionStage>> {
        let elapsed_compute = context.baseline_metrics.elapsed_compute();

        while let Some(batch) = self.input.next().await.transpose()? {
            context.reduction_factor.add_total(batch.num_rows());
            let timer = elapsed_compute.timer();
            self.table.aggregate_batch(&batch)?;

            let output = match reservation.try_resize(self.table.memory_size()) {
                Ok(()) => self.table.take_completed_state_batch()?,
                Err(oom @ DataFusionError::ResourcesExhausted(_)) => {
                    // Partial ordering may have an unbounded active key range.
                    // The final stage can merge incomplete states emitted here.
                    if matches!(self.table.group_ordering(), GroupOrdering::Full(_)) {
                        return Err(oom);
                    }
                    let batches = self.table.take_all_state_batch()?;
                    if batches.is_empty() {
                        return Err(oom);
                    }
                    // ponytail: concat so the output stage can slice one batch
                    Some(concat_batches(&batches[0].schema(), &batches)?)
                }
                Err(e) => return Err(e),
            };
            let Some(batch) = output else {
                continue;
            };

            timer.done();

            // OOM, do early emit next, and go back to the current state to continue
            // aggregating
            return Ok(Some(ExecutionStage::Outputting(Outputting {
                batch,
                resume: Some(self),
            })));
        }

        // Release upstream resources before draining the remaining states.
        drop(self.input);
        self.table.input_done();
        let timer = elapsed_compute.timer();
        let output = self.table.take_completed_state_batch()?;
        drop(self.table);
        timer.done();

        let Some(batch) = output else {
            reservation.try_resize(0)?;
            return Ok(None);
        };
        Ok(Some(ExecutionStage::Outputting(Outputting {
            batch,
            resume: None,
        })))
    }
}

impl Outputting {
    /// Emits slices of one materialized batch without touching the hash table.
    ///
    /// See [`OrderedPartialAggregateStream::into_stream`] for stage transitions
    /// and output memory accounting.
    async fn handle_stage(
        self,
        context: &OrderedPartialAggregateContext,
        reservation: &MemoryReservation,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<Option<ExecutionStage>> {
        let Self { mut batch, resume } = self;
        let elapsed_compute = context.baseline_metrics.elapsed_compute();
        let mut timer = elapsed_compute.timer();
        let (table_memory, next_stage) = match resume {
            Some(aggregating) => (
                aggregating.table.memory_size(),
                Some(ExecutionStage::Aggregating(aggregating)),
            ),
            None => (0, None),
        };
        let batch_memory = batch.get_array_memory_size();
        match reservation.try_resize(table_memory + batch_memory) {
            Ok(()) => {}
            Err(DataFusionError::ResourcesExhausted(_)) => {
                // If we cannot hold the batch while slicing, hand it off whole.
                // Only the retained table needs to remain reserved.
                reservation.try_resize(table_memory)?;
                context.reduction_factor.add_part(batch.num_rows());
                timer.done();
                emitter.emit(batch).await;
                return Ok(next_stage);
            }
            Err(e) => return Err(e),
        }

        while batch.num_rows() > context.batch_size {
            // 1. Emit first `batch_size` rows from `batch`
            // 2. Update `batch`` with the remaining tail
            let output = batch.slice(0, context.batch_size);
            batch =
                batch.slice(context.batch_size, batch.num_rows() - context.batch_size);
            context.reduction_factor.add_part(output.num_rows());
            timer.done();
            emitter.emit(output).await;
            timer = elapsed_compute.timer();
        }

        // The final slice transfers ownership of the buffers to the consumer.
        reservation.try_shrink(batch_memory)?;
        context.reduction_factor.add_part(batch.num_rows());
        timer.done();
        emitter.emit(batch).await;
        Ok(next_stage)
    }
}
