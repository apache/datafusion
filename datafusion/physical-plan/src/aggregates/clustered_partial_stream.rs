// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Partial aggregate stream for input with group-clustering guarantees.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{TaskContext, TryEmitter, async_try_stream};
use futures::stream::StreamExt;

use super::AggregateExec;
use super::aggregate_hash_table::{ClusteredAggregateTable, PartialMarker};
use crate::aggregates::AggregateMode;
use crate::aggregates::order::{GroupClustering, GroupClusteringMode};
use crate::metrics::{BaselineMetrics, MetricBuilder, SpillMetrics};
use crate::stream::{ObservedStream, RecordBatchStreamAdapter};
use crate::{SendableRecordBatchStream, metrics};

/// Partial aggregate stream for [`GroupClusteringMode::Partial`] and
/// [`GroupClusteringMode::Full`].
///
/// # Example
///
/// SELECT k, AVG(v) FROM t GROUP BY k;
///
/// If the input is ordered by `k`, the aggregate can use clustered partial and
/// final stages:
///
/// ## Plan
/// AggregateExec(stage=final, clustered)
/// -- RepartitionExec(hash(k), preserves_order=true)
/// ---- AggregateExec(stage=partial, clustered)
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
/// # Group Clustering Optimization
///
/// For the aggregation work, the hash aggregation implementation is reused.
///
/// After each input batch, the group-clustering mode determines whether any
/// groups can be emitted eagerly to improve memory efficiency. For example, if
/// the input is ordered by `k` and the last group key seen is `k = 100`, all
/// groups with keys less than 100 are complete. Materialize that entire completed
/// prefix once, then emit slices of it before reading more input. This avoids
/// repeatedly removing small batches of groups and shifting the remaining hash
/// table and accumulator state.
///
/// # Memory Pressure and Spilling
///
/// ## Full group clustering
///
/// Every complete grouping tuple is contiguous. Ordering by every group key is
/// one way to establish this mode, for example:
///
/// - Input order: `a, b`
/// - `GROUP BY`: `a, b`
///
/// Completed groups can be emitted as soon as the next group is observed. Thus,
/// only the current group remains active after completed groups are emitted, and
/// memory usage does not grow with the total number of groups.
///
/// If the reservation after an input batch fails, `ClusteredPartialAggregateStream`
/// still emits the completed groups, and returns the error only when no group is
/// complete. After the emit it releases the capacity the table grew for the batch,
/// such as hash table slots and per-batch scratch buffers, so only the active
/// group stays reserved.
///
/// ## Partial group clustering
///
/// Rows are contiguous for a subset of the group keys. Ordering by that subset
/// is one way to establish this mode, for example:
///
/// - Input order: `a`
/// - `GROUP BY`: `a, b`
///
/// If one `a` value contains many distinct `b` values, the table may accumulate
/// enough groups to exceed the memory limit.
///
/// - `ClusteredPartialAggregateStream`: On reservation failure, it emits all current
///   intermediate states downstream and resets the table. The final stage can
///   merge repeated `(a, b)` state rows, so no disk spill is required.
/// - `ClusteredFinalAggregateStream`: It cannot emit incomplete final results. On
///   reservation failure, it sorts the current intermediate states by the complete
///   group key and spills them as one run. After the input ends, it spills any
///   remaining states, performs a sort-preserving merge of all runs, and feeds the
///   merged input into a fully clustered final aggregate stream.
pub(crate) struct ClusteredPartialAggregateStream {
    reservation: MemoryReservation,
    context: ClusteredPartialAggregateContext,
    stage: ExecutionStage,
}

/// Execution stages described in [`ClusteredPartialAggregateStream::into_stream`].
enum ExecutionStage {
    Aggregating(Aggregating),
    Outputting(Outputting),
}

struct Aggregating {
    input: SendableRecordBatchStream,
    table: ClusteredAggregateTable<PartialMarker>,
}

struct Outputting {
    /// Materialized aggregate states. Each iteration emits the first `batch_size`
    /// rows and replaces this batch with the remaining slice.
    batch: RecordBatch,
    /// Aggregation stage to resume after output; `None` after EOF.
    resume: Option<Aggregating>,
}

/// Immutable execution context shared by aggregation and output emission.
struct ClusteredPartialAggregateContext {
    schema: SchemaRef,
    batch_size: usize,
    baseline_metrics: BaselineMetrics,
    reduction_factor: metrics::RatioMetrics,
}

impl ClusteredPartialAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert_eq!(agg.mode, AggregateMode::Partial);
        debug_assert_ne!(agg.group_clustering_mode, GroupClusteringMode::None);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);
        let reduction_factor = MetricBuilder::new(&agg.metrics)
            .with_type(metrics::MetricType::Summary)
            .ratio_metrics("reduction_factor", partition);

        let table = ClusteredAggregateTable::<PartialMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
        )?;
        let reservation =
            MemoryConsumer::new(format!("ClusteredPartialAggregateStream[{partition}]"))
                .with_can_spill(matches!(
                    table.group_clustering(),
                    GroupClustering::Partial(_)
                ))
                .register(context.memory_pool());

        // Reserve memory for the initial hash table. that we hold for the lifetime of the stream.
        reservation.try_grow(table.memory_size())?;

        Ok(Self {
            reservation,
            context: ClusteredPartialAggregateContext {
                schema,
                batch_size,
                baseline_metrics,
                reduction_factor,
            },
            stage: ExecutionStage::Aggregating(Aggregating { input, table }),
        })
    }

    /// Entry point for the clustered partial aggregate execution stages.
    ///
    /// See [`ClusteredPartialAggregateStream`] for high-level ideas.
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
    ///    - A prefix of groups is complete: materialize the entire prefix once,
    ///      retaining the input and active groups to resume aggregation.
    ///    - On memory pressure with full group clustering, materialize the
    ///      completed prefix and release the capacity the table grew for it.
    ///    - On memory pressure with partial group clustering, materialize all
    ///      current states, including incomplete groups, and reset the table.
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
    /// See [`ClusteredPartialAggregateStream::into_stream`] for stage transitions.
    async fn handle_stage(
        mut self,
        context: &ClusteredPartialAggregateContext,
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
                    if matches!(self.table.group_clustering(), GroupClustering::Full(_)) {
                        // Only the active group remains after the emit. Release the
                        // capacity the table grew for this batch so that reserving
                        // the table can succeed.
                        let batch =
                            self.table.take_completed_state_batch()?.ok_or(oom)?;
                        self.table.shrink_to_fit();
                        Some(batch)
                    } else {
                        // Partial group clustering may have an unbounded active key range.
                        // The final stage can merge incomplete states emitted here.
                        Some(self.table.take_state_batch()?.ok_or(oom)?)
                    }
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
    /// See [`ClusteredPartialAggregateStream::into_stream`] for stage transitions
    /// and output memory accounting.
    async fn handle_stage(
        self,
        context: &ClusteredPartialAggregateContext,
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

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use super::*;
    use crate::aggregates::GroupClusteringMode;
    use crate::aggregates::PhysicalGroupBy;
    use crate::aggregates::clustered_test_util::{
        aggregate_expr_over_sorted_input, aggregate_over_sorted_input,
        assert_completed_groups_emitted_under_limit, assert_output_unchanged_under_limit,
        collect_under_limit, distinct_key_batches, full_batches, sum_over_sorted_input,
    };
    use crate::test::TestMemoryExec;
    use arrow::array::{ArrayRef, Float64Array, Int64Array, ListArray};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use datafusion_common::test_util::batches_to_string;
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryPool, UnboundedMemoryPool,
    };
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_functions_aggregate::any_value::any_value_udaf;
    use datafusion_functions_aggregate::approx_distinct::approx_distinct_udaf;
    use datafusion_functions_aggregate::first_last::first_value_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr_common::sort_expr::LexOrdering;

    /// One batch per range. Every key column and the value column `v` hold the
    /// range values, as the given key types.
    fn batches(
        key_types: &[DataType],
        ranges: &[Range<i64>],
    ) -> Result<Vec<RecordBatch>> {
        let mut fields: Vec<_> = key_types
            .iter()
            .enumerate()
            .map(|(i, data_type)| Field::new(format!("k{i}"), data_type.clone(), false))
            .collect();
        fields.push(Field::new("v", DataType::Int64, false));
        let schema = Arc::new(Schema::new(fields));
        ranges
            .iter()
            .map(|range| {
                let int64: ArrayRef =
                    Arc::new(Int64Array::from_iter_values(range.clone()));
                let mut columns = key_types
                    .iter()
                    .map(|data_type| match data_type {
                        DataType::Int64 => Arc::clone(&int64),
                        DataType::Float64 => Arc::new(Float64Array::from_iter_values(
                            range.clone().map(|v| v as f64),
                        )),
                        DataType::List(_) => {
                            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
                                range.clone().map(|v| Some([Some(v)])),
                            ))
                        }
                        _ => unreachable!("unsupported key type {data_type}"),
                    })
                    .collect::<Vec<_>>();
                columns.push(int64);
                Ok(RecordBatch::try_new(Arc::clone(&schema), columns)?)
            })
            .collect()
    }

    /// Runs a partial `SUM(v) GROUP BY k0, ..` over `batches`, which are sorted
    /// on every key.
    async fn run(
        batches: Vec<RecordBatch>,
        pool: Arc<dyn MemoryPool>,
    ) -> Result<Vec<RecordBatch>> {
        let schema = batches[0].schema();
        let keys = (0..schema.fields().len() - 1)
            .map(|i| Ok((col(&format!("k{i}"), &schema)?, format!("k{i}"))))
            .collect::<Result<Vec<_>>>()?;
        let ordering = LexOrdering::new(
            keys.iter()
                .map(|(expr, _)| PhysicalSortExpr::new_default(Arc::clone(expr))),
        )
        .unwrap();
        let input = TestMemoryExec::try_new(&[batches], Arc::clone(&schema), None)?
            .try_with_sort_information(vec![ordering])?;
        let aggregate = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(keys),
            vec![Arc::new(
                AggregateExprBuilder::new(sum_udaf(), vec![col("v", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("sum")
                    .build()?,
            )],
            vec![None],
            Arc::new(input),
            Arc::clone(&schema),
        )?;
        assert_eq!(aggregate.group_clustering_mode, GroupClusteringMode::Full);
        let context = Arc::new(
            TaskContext::default().with_runtime(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(pool)
                    .build_arc()?,
            ),
        );
        let stream =
            ClusteredPartialAggregateStream::new(&aggregate, &context, 0)?.into_stream();
        crate::common::collect(stream).await
    }

    /// Every row is a distinct group, and each batch shares its first key with
    /// the previous one, so the active group spans the batches. Aggregating a
    /// batch grows the table for about 8192 groups, for example the single-key
    /// hash table (~272 KB) and the `group_indices` scratch (64 KB). After the
    /// completed groups are emitted, the table must release that capacity, or
    /// reserving it fails the limit although the active group needs a few KB.
    /// The output must not change.
    #[tokio::test]
    async fn completed_groups_under_memory_pressure() -> Result<()> {
        let list = DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)));
        for key_types in [
            // `GroupValuesPrimitive`
            vec![DataType::Int64],
            // `GroupValuesClustered`
            vec![DataType::Int64, DataType::Int64],
            // `GroupValuesColumn`
            vec![DataType::Float64, DataType::Float64],
            // `GroupValuesRows`
            vec![DataType::Int64, list],
        ] {
            let input = || batches(&key_types, &[0..8193, 8192..16385, 16384..24576]);
            let expected =
                run(input()?, Arc::new(UnboundedMemoryPool::default())).await?;
            let actual = run(input()?, Arc::new(GreedyMemoryPool::new(100 * 1024)))
                .await
                .map_err(|e| e.context(format!("key types {key_types:?}")))?;
            assert_eq!(batches_to_string(&actual), batches_to_string(&expected));
        }
        Ok(())
    }

    #[tokio::test]
    async fn fully_clustered_completed_groups_under_memory_pressure() -> Result<()> {
        let plan = sum_over_sorted_input(AggregateMode::Partial, &["k"], full_batches())?;
        assert_eq!(plan.group_clustering_mode(), &GroupClusteringMode::Full);
        assert_completed_groups_emitted_under_limit(&plan).await
    }

    /// Like [`completed_groups_under_memory_pressure`], but with accumulators
    /// that keep per-batch scratch. For example, `APPROX_DISTINCT` keeps the
    /// hashes of the last batch (64 KB for 8192 rows), and the
    /// `GroupsAccumulatorAdapter` of `ANY_VALUE` keeps its per-batch indices
    /// (~160 KB). After the completed groups are emitted, the accumulators must
    /// release that scratch too. The limit fits the active group, but not the
    /// scratch.
    #[tokio::test]
    async fn completed_groups_release_accumulator_scratch() -> Result<()> {
        for udaf in [approx_distinct_udaf(), any_value_udaf()] {
            let plan = aggregate_over_sorted_input(
                AggregateMode::Partial,
                &["k"],
                distinct_key_batches(),
                udaf,
            )?;
            assert_eq!(plan.group_clustering_mode(), &GroupClusteringMode::Full);
            assert_output_unchanged_under_limit(&plan, 64 * 1024).await?;
        }
        Ok(())
    }

    /// Like [`completed_groups_release_accumulator_scratch`], but the active
    /// group holds most rows of the batch. The `GroupsAccumulatorAdapter` of
    /// `ANY_VALUE` keeps the row indices it collected for each group (32 KB for
    /// the 8191 rows of the active group), so it must release them too.
    #[tokio::test]
    async fn active_group_releases_adapter_indices() -> Result<()> {
        let keys = std::iter::once(0)
            .chain(std::iter::repeat_n(1, 8191))
            .collect();
        let plan = aggregate_over_sorted_input(
            AggregateMode::Partial,
            &["k"],
            vec![vec![keys]],
            any_value_udaf(),
        )?;
        assert_eq!(plan.group_clustering_mode(), &GroupClusteringMode::Full);
        assert_output_unchanged_under_limit(&plan, 16 * 1024).await
    }

    /// Like [`completed_groups_release_accumulator_scratch`], with the native
    /// `FIRST_VALUE(v ORDER BY v)` accumulator. It keeps a bitmap of the
    /// groups each batch updated, which grows to one bit per group (16 KB for
    /// 131072 groups), so it must release that bitmap too.
    #[tokio::test]
    async fn completed_groups_release_first_value_bitmap() -> Result<()> {
        let plan = aggregate_expr_over_sorted_input(
            AggregateMode::Partial,
            &["k"],
            vec![vec![(0..131072).collect()]],
            first_value_udaf(),
            |builder, schema| {
                Ok(builder
                    .order_by(vec![PhysicalSortExpr::new_default(col("v", schema)?)]))
            },
        )?;
        assert_eq!(plan.group_clustering_mode(), &GroupClusteringMode::Full);
        assert_output_unchanged_under_limit(&plan, 16 * 1024).await
    }

    /// With partial group clustering, the stream emits every state under
    /// memory pressure, so the output is not compared. After the emit, the
    /// accumulators must release their per-batch scratch, or reserving the
    /// empty table fails the limit.
    #[tokio::test]
    async fn emitted_states_release_accumulator_scratch() -> Result<()> {
        let batches = [0..8193, 8192..16385, 16384..24576]
            .into_iter()
            .map(|range| vec![range.clone().collect(), range.collect()])
            .collect();
        let plan = aggregate_over_sorted_input(
            AggregateMode::Partial,
            &["k", "k2"],
            batches,
            approx_distinct_udaf(),
        )?;
        assert!(matches!(
            plan.group_clustering_mode(),
            GroupClusteringMode::Partial(_)
        ));
        collect_under_limit(&plan, 64 * 1024).await?;
        Ok(())
    }
}
