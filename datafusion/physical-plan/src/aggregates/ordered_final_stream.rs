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

//! Final aggregate stream for ordered partial-state input.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{TaskContext, TryEmitter, async_try_stream};
use futures::stream::StreamExt;

use super::AggregateExec;
use super::aggregate_hash_table::{
    FinalMarker, OrderedAggregateTable, OrderedAggregateTableMetrics,
};
use super::spill::AggregateSpill;
use crate::aggregates::AggregateMode;
use crate::metrics::{BaselineMetrics, SpillMetrics};
use crate::stream::{ObservedStream, RecordBatchStreamAdapter};
use crate::{InputOrderMode, SendableRecordBatchStream};

/// Final aggregate stream for `InputOrderMode::Sorted` and
/// `InputOrderMode::PartiallySorted`.
///
/// See comments at [`super::ordered_partial_stream::OrderedPartialAggregateStream`] for details.
///
/// # Spilling
///
/// This section is only for implementation notes, for background, see [`super::ordered_partial_stream::OrderedPartialAggregateStream`]
///
/// For partially sorted input, spilling works as follows:
///
/// - Reserve the table footprint plus one `u32` sort index per buffered group. The
///   extra index array is used in later sorting before spilling.
/// - On memory pressure, materialize all group states into one batch.
/// - Use [`IncrementalSortIterator`](crate::sorts::IncrementalSortIterator) to compute the full-batch index, then
///   materialize and write one sorted `batch_size` slice at a time. The original
///   batch and full index remain live until the run is written.
/// - After input ends, merge the sorted runs and replay them through a fully
///   ordered final aggregate stream.
pub(crate) struct OrderedFinalAggregateStream {
    reservation: MemoryReservation,
    context: OrderedFinalAggregateContext,
    stage: ExecutionStage,
}

/// Execution stages described in [`OrderedFinalAggregateStream::into_stream`].
enum ExecutionStage {
    Aggregating(Aggregating),
    Outputting(Outputting),
    MergingSpills(SendableRecordBatchStream),
}

struct Aggregating {
    input: SendableRecordBatchStream,
    table: OrderedAggregateTable<FinalMarker>,
    /// None when temporary files are disabled or all group keys are ordered.
    spill_context: Option<Box<AggregateSpill>>,
}

struct Outputting {
    /// Materialized final results, emitted in slices of `batch_size` rows.
    batch: RecordBatch,
    /// Aggregation stage to resume after output; `None` after EOF.
    resume: Option<Aggregating>,
}

/// Immutable execution context shared by aggregation and output emission.
struct OrderedFinalAggregateContext {
    schema: SchemaRef,
    batch_size: usize,
    baseline_metrics: BaselineMetrics,
}

impl OrderedFinalAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            AggregateMode::Final | AggregateMode::FinalPartitioned
        ));
        debug_assert_ne!(agg.input_order_mode, InputOrderMode::Linear);

        let input = agg.input.execute(partition, Arc::clone(context))?;
        Self::new_with_input(agg, context, partition, input, &agg.input_order_mode)
    }

    pub(in crate::aggregates) fn new_with_input(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
        input: SendableRecordBatchStream,
        input_order_mode: &InputOrderMode,
    ) -> Result<Self> {
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);
        let metrics = OrderedAggregateTableMetrics::new(agg, partition);
        let spill_metrics = SpillMetrics::new(&agg.metrics, partition);
        let reservation =
            MemoryConsumer::new(format!("OrderedFinalAggregateStream[{partition}]"))
                // HACK: Technically, fully ordered aggregate is a non-spillable
                // consumer, since it uses bounded memory. There is a known race
                // condition bug, and we set it to spillable to let it have larger
                // memory budget to suppress the bug.
                // Bug issue: https://github.com/apache/datafusion/issues/17334
                .with_can_spill(true)
                .register(context.memory_pool());
        Self::new_with_input_and_metrics(
            agg,
            context,
            partition,
            input,
            input_order_mode,
            baseline_metrics,
            metrics,
            Some(spill_metrics),
            reservation,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "keeps replay metric reuse explicit"
    )]
    /// Builds the stream with the reservation of its logical aggregate operator.
    /// Replay callers pass a sibling of the reservation used by the merge input,
    /// keeping both components under one memory-consumer registration.
    pub(in crate::aggregates) fn new_with_input_and_metrics(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
        input: SendableRecordBatchStream,
        input_order_mode: &InputOrderMode,
        baseline_metrics: BaselineMetrics,
        metrics: OrderedAggregateTableMetrics,
        spill_metrics: Option<SpillMetrics>,
        reservation: MemoryReservation,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            AggregateMode::Final | AggregateMode::FinalPartitioned
        ));
        debug_assert_ne!(*input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input_schema = input.schema();
        let batch_size = context.session_config().batch_size();

        let can_spill = matches!(input_order_mode, InputOrderMode::PartiallySorted(_))
            && context.runtime_env().disk_manager.tmp_files_enabled();
        let spill_context = if can_spill {
            let Some(spill_metrics) = spill_metrics else {
                return internal_err!("Spillable ordered final stream requires metrics");
            };
            Some(Box::new(AggregateSpill::try_new(
                "OrderedFinalAggregateSpill",
                agg,
                context,
                partition,
                batch_size,
                input_order_mode,
                &input_schema,
                spill_metrics,
            )?))
        } else {
            None
        };

        let table = OrderedAggregateTable::<FinalMarker>::new_with_input_order(
            agg,
            &input_schema,
            Arc::clone(&schema),
            batch_size,
            input_order_mode,
            metrics,
        )?;
        Ok(Self {
            reservation,
            context: OrderedFinalAggregateContext {
                schema,
                batch_size,
                baseline_metrics,
            },
            stage: ExecutionStage::Aggregating(Aggregating {
                input,
                table,
                spill_context,
            }),
        })
    }

    /// Entry point for the ordered final aggregate execution stages.
    ///
    /// See [`OrderedFinalAggregateStream`] for high-level ideas.
    ///
    /// # Stage transition graph:
    ///
    /// ```text
    ///                  +----[2]----+                +----[5]----+
    ///                  |           |                |           |
    ///                  v           |                v           |
    ///              +-------------------+         +------------------+
    ///              |                   |         |                  |
    /// (start)-[1]->|    Aggregating    |---[3]-->|    Outputting    |
    ///              |                   |<--[6]---|                  |
    ///              +-------------------+         +------------------+
    ///                        |     |                      |
    ///                       [8]    +------[4]-----+      [7]
    ///                        |                    |       |
    ///                        v                    v       v
    ///              +-------------------+         +------------------+
    ///              |                   |         |                  |
    ///         +--->|   MergingSpills   |---[10-->|      Done        |-[11]->(end)
    ///         |    |                   |         |                  |
    ///         |    +-------------------+         +------------------+
    ///         |              |
    ///         +-----[9]------+
    /// ```
    ///
    /// ## Stages
    ///
    /// - [`Aggregating`]: Aggregate input batches.
    /// - [`Outputting`]: Handle materialzing all aggregated input.
    /// - [`ExecutionStage::MergingSpills`]: If OOM and spilled before, use this
    ///   stage to finish execution.
    ///
    /// ### Incremental output
    ///
    /// See the [ordered partial aggregate notes] for details.
    ///
    /// [ordered partial aggregate notes]: super::ordered_partial_stream::OrderedPartialAggregateStream::into_stream
    ///
    /// ## Transition Edges
    ///
    /// 1. Start.
    /// 2. Merge one input batch:
    ///    - If memory fits and no groups are complete, continue reading input.
    ///    - If OOM, spill.
    /// 3. Prepare output:
    ///    - Before any spill, ordering proves a prefix complete: materialize the
    ///      entire prefix once, retaining the input and active groups to resume
    ///      aggregation.
    ///    - At EOF without spills, materialize all remaining results and prepare
    ///      to output.
    /// 4. Input was exhausted, directly end.
    /// 5. Incremental output at `batch_size`
    /// 6. The batch was fully emitted and retained aggregation can resume.
    /// 7. The output batch was fully emitted.
    /// 8. Input was exhausted after spilling.
    /// 9. Incremental output during reading spill and finalizing results.
    /// 10. The merged spill input was fully aggregated and emitted.
    /// 11. End.
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
    /// Reserve the table footprint and, when spillable, one sort index per group.
    fn reservation_size(&self) -> usize {
        let table_size = self.table.memory_size();
        if self.spill_context.is_some() {
            // See `OrderedFinalAggregateStream` for the spill memory estimate.
            table_size
                .saturating_add(self.table.num_groups().saturating_mul(size_of::<u32>()))
        } else {
            table_size
        }
    }

    /// Merges partial states until final results are ready or spill replay begins.
    async fn handle_stage(
        mut self,
        context: &OrderedFinalAggregateContext,
        reservation: &MemoryReservation,
    ) -> Result<Option<ExecutionStage>> {
        let elapsed_compute = context.baseline_metrics.elapsed_compute();

        while let Some(batch) = self.input.next().await.transpose()? {
            let timer = elapsed_compute.timer();
            self.table.aggregate_batch(&batch)?;

            match reservation.try_resize(self.reservation_size()) {
                Ok(()) => {}
                Err(oom @ DataFusionError::ResourcesExhausted(_)) => {
                    let Some(spill_context) = self.spill_context.as_mut() else {
                        return Err(oom);
                    };
                    if self.table.is_empty() {
                        return Err(oom);
                    }
                    spill_context.sort_and_spill(self.table.take_state_batch()?)?;
                    reservation
                        .try_resize(self.table.memory_size())
                        .map_err(|e| {
                            e.context(
                                "Decreasing allocation after spilling should succeed",
                            )
                        })?;
                    continue;
                }
                Err(e) => return Err(e),
            }

            if self.spill_context.as_ref().is_some_and(|s| s.has_spills()) {
                // Spilled groups may recur, so all remaining states must go
                // through replay before any more final results can be emitted.
                continue;
            }
            let Some(batch) = self.table.take_completed_result_batch()? else {
                continue;
            };
            timer.done();
            return Ok(Some(ExecutionStage::Outputting(Outputting {
                batch,
                resume: Some(self),
            })));
        }

        // Release upstream resources before materializing output or replaying spills.
        drop(self.input);
        let timer = elapsed_compute.timer();
        if let Some(mut spill_context) = self.spill_context.filter(|s| s.has_spills()) {
            spill_context.sort_and_spill(self.table.take_state_batch()?)?;
            let metrics = self.table.metrics();
            drop(self.table);
            reservation.try_resize(0)?;
            // The outer ObservedStream counts output; replay only shares compute time.
            let stream = (*spill_context).into_replay_stream(
                &context.baseline_metrics.intermediate(),
                metrics,
                reservation.new_empty(),
            )?;
            timer.done();
            return Ok(Some(ExecutionStage::MergingSpills(stream)));
        }

        self.table.input_done();
        let output = self.table.take_completed_result_batch()?;
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
    /// Emits slices of one materialized batch without touching the aggregate table.
    async fn handle_stage(
        self,
        context: &OrderedFinalAggregateContext,
        reservation: &MemoryReservation,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<Option<ExecutionStage>> {
        let Self { mut batch, resume } = self;
        let elapsed_compute = context.baseline_metrics.elapsed_compute();
        let mut timer = elapsed_compute.timer();
        let (table_memory, next_stage) = match resume {
            Some(aggregating) => (
                aggregating.reservation_size(),
                Some(ExecutionStage::Aggregating(aggregating)),
            ),
            None => (0, None),
        };
        let batch_memory = batch.get_array_memory_size();
        match reservation.try_resize(table_memory + batch_memory) {
            Ok(()) => {}
            Err(DataFusionError::ResourcesExhausted(_)) => {
                // If we cannot hold the batch while slicing, hand it off whole.
                reservation.try_resize(table_memory)?;
                timer.done();
                emitter.emit(batch).await;
                return Ok(next_stage);
            }
            Err(e) => return Err(e),
        }

        while batch.num_rows() > context.batch_size {
            let output = batch.slice(0, context.batch_size);
            batch =
                batch.slice(context.batch_size, batch.num_rows() - context.batch_size);
            timer.done();
            emitter.emit(output).await;
            timer = elapsed_compute.timer();
        }

        // The final slice transfers ownership of the buffers to the consumer.
        reservation.try_shrink(batch_memory)?;
        timer.done();
        emitter.emit(batch).await;
        Ok(next_stage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExecutionPlan;
    use crate::aggregates::PhysicalGroupBy;
    use crate::common::collect;
    use crate::test::TestMemoryExec;
    use arrow::array::{Int64Array, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_functions_aggregate::{min_max::min_udaf, sum::sum_udaf};
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr_common::sort_expr::LexOrdering;
    use futures::FutureExt;
    use futures::channel::mpsc;
    use std::collections::BTreeMap;

    #[derive(Clone, Copy)]
    enum Finish {
        Collect,
        DropDuringMerge,
        DropDuringReplay,
        InputError,
    }

    #[tokio::test]
    async fn spill_replay_with_another_ordered_partition() -> Result<()> {
        for input_batches in [28, 36, 55, 63] {
            run_shared_pool_case(input_batches, 600 * 1024, Finish::Collect).await?;
        }
        // The same input also produces the reference results without spilling.
        run_shared_pool_case(63, 10 * 1024 * 1024, Finish::Collect).await
    }

    #[tokio::test]
    async fn spill_replay_releases_memory_on_drop() -> Result<()> {
        run_shared_pool_case(36, 600 * 1024, Finish::DropDuringMerge).await?;
        run_shared_pool_case(36, 600 * 1024, Finish::DropDuringReplay).await
    }

    #[tokio::test]
    async fn ordered_spill_releases_memory_on_input_error() -> Result<()> {
        run_shared_pool_case(36, 600 * 1024, Finish::InputError).await
    }

    async fn run_shared_pool_case(
        input_batches: i64,
        limit: usize,
        finish: Finish,
    ) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("s", DataType::Utf8View, false),
        ]));
        let groups = PhysicalGroupBy::new_single(vec![
            (col("a", &schema)?, "a".into()),
            (col("b", &schema)?, "b".into()),
        ]);
        let expressions = vec![
            Arc::new(
                AggregateExprBuilder::new(sum_udaf(), vec![col("v", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("sum")
                    .build()?,
            ),
            Arc::new(
                AggregateExprBuilder::new(min_udaf(), vec![col("s", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("min")
                    .build()?,
            ),
        ];
        let empty = TestMemoryExec::try_new_exec(&[vec![]], Arc::clone(&schema), None)?;
        let partial = AggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            expressions.clone(),
            vec![None; 2],
            empty,
            Arc::clone(&schema),
        )?;
        let partial_schema = partial.schema();
        let ordering =
            LexOrdering::new([PhysicalSortExpr::new_default(col("a", &partial_schema)?)])
                .unwrap();
        let input = TestMemoryExec::try_new(
            &[vec![], vec![]],
            Arc::clone(&partial_schema),
            None,
        )?
        .try_with_sort_information(vec![ordering])?;
        let aggregate = AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            groups.as_final(),
            expressions,
            vec![None; 2],
            Arc::new(TestMemoryExec::update_cache(&Arc::new(input))),
            schema,
        )?;
        assert_eq!(
            aggregate.input_order_mode(),
            &InputOrderMode::PartiallySorted(vec![0])
        );

        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(
                    SessionConfig::new().with_batch_size(128).set_bool(
                        "datafusion.execution.enable_migration_aggregate",
                        true,
                    ),
                )
                .with_runtime(
                    RuntimeEnvBuilder::new()
                        .with_max_spill_merge_fan_in(2)
                        .with_memory_pool(Arc::clone(&pool))
                        .build_arc()?,
                ),
        );
        let mut streams = vec![];
        let mut senders = vec![];
        for partition in 0..2 {
            let (sender, receiver) = mpsc::unbounded();
            let input = Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&partial_schema),
                receiver,
            ));
            let stream = OrderedFinalAggregateStream::new_with_input(
                &aggregate,
                &context,
                partition,
                input,
                aggregate.input_order_mode(),
            )?;
            senders.push(sender);
            streams.push(stream.into_stream());
        }
        let mut expected = BTreeMap::new();
        let mut make_batch = |partition: i64, start: i64| {
            for value in start..start + 128 {
                let entry = expected
                    .entry((
                        partition,
                        if partition == 0 && value % 128 == 0 {
                            0
                        } else {
                            value
                        },
                    ))
                    .or_insert_with(|| (0, (value % 2).to_string()));
                entry.0 += value * 2;
            }
            RecordBatch::try_new(
                Arc::clone(&partial_schema),
                vec![
                    Arc::new(Int64Array::from(vec![partition; 128])),
                    Arc::new(Int64Array::from_iter_values((start..start + 128).map(
                        |value| {
                            if partition == 0 && value % 128 == 0 {
                                0
                            } else {
                                value
                            }
                        },
                    ))),
                    Arc::new(Int64Array::from_iter_values(
                        (start..start + 128).map(|v| v * 2),
                    )),
                    Arc::new(StringViewArray::from_iter_values(
                        (start..start + 128).map(|v| if v % 2 == 0 { "0" } else { "1" }),
                    )),
                ],
            )
            .unwrap()
        };
        // Keep partition 1's incomplete ordered run live while partition 0 spills
        // and replays. Channel inputs return Pending after each supplied batch,
        // making this interleaving independent of task scheduling.
        for batch in 0..55 {
            senders[1]
                .unbounded_send(Ok(make_batch(1, batch * 128)))
                .unwrap();
            assert!(streams[1].next().now_or_never().is_none());
        }
        let held = pool.reserved();
        assert!(held > 500 * 1024);
        for batch in 0..input_batches {
            // Repeated keys cross spill runs, so replay must merge their sums.
            senders[0]
                .unbounded_send(Ok(make_batch(0, batch * 128)))
                .unwrap();
            assert!(streams[0].next().now_or_never().is_none());
        }
        if limit == 600 * 1024 {
            assert!(aggregate.metrics().unwrap().spill_count().unwrap() > 0);
        }
        let mut first = streams.remove(0);
        match finish {
            Finish::Collect => {
                senders[0].close_channel();
                let mut output = collect(first).await?;
                assert_eq!(pool.reserved(), held);
                senders[1].close_channel();
                output.extend(collect(streams.remove(0)).await?);
                let mut actual = BTreeMap::new();
                for batch in output {
                    let a = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let b = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let sum = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let min = batch
                        .column(3)
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .unwrap();
                    for row in 0..batch.num_rows() {
                        assert!(
                            actual
                                .insert(
                                    (a.value(row), b.value(row)),
                                    (sum.value(row), min.value(row).to_string())
                                )
                                .is_none()
                        );
                    }
                }
                assert_eq!(actual, expected);
                assert_eq!(
                    aggregate.metrics().unwrap().spill_count().unwrap() > 0,
                    limit == 600 * 1024
                );
            }
            Finish::DropDuringMerge => {
                senders[0].close_channel();
                assert!(first.next().now_or_never().is_none());
                drop(first);
            }
            Finish::DropDuringReplay => {
                senders[0].close_channel();
                first.next().await.unwrap()?;
                assert!(pool.reserved() > held);
                drop(first);
                assert_eq!(pool.reserved(), held);
            }
            Finish::InputError => {
                senders[0]
                    .unbounded_send(datafusion_common::exec_err!(
                        "injected input failure"
                    ))
                    .unwrap();
                let error = first.next().await.unwrap().unwrap_err();
                assert!(error.to_string().contains("injected input failure"));
                assert_eq!(pool.reserved(), held);
                drop(first);
            }
        }
        drop(streams);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }
}
