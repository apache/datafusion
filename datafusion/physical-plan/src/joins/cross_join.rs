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

//! Defines the cross join plan for loading the left side of the cross join
//! and producing batches in parallel for the right partitions

use std::future::poll_fn;
use std::sync::Arc;

use super::utils::{
    BuildProbeJoinMetrics, OnceAsync, OnceFut, adjust_right_output_partitioning,
    reorder_output_after_swap,
};
use crate::execution_plan::{EmissionType, boundedness_from_children};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::projection::{
    ProjectionExec, join_allows_pushdown, join_table_borders, new_join_children,
    physical_to_column_exprs,
};
use crate::statistics::{ChildStats, StatisticsArgs};
use crate::stream::{EmptyRecordBatchStream, ObservedStream, RecordBatchStreamAdapter};
use crate::{
    ChildrenPropertiesMode, ColumnStatistics, DisplayAs, DisplayFormatType, Distribution,
    ExecutionPlan, ExecutionPlanProperties, PlanProperties, ReplaceChildrenOptions,
    SendableRecordBatchStream, Statistics, validate_child_count,
};

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::compute::concat_batches;
use arrow::datatypes::{Fields, Schema, SchemaRef};
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    DataFusionError, JoinType, Result, ScalarValue, assert_eq_or_internal_err,
};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{TaskContext, TryEmitter, async_try_stream};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::equivalence::join_equivalence_properties;

use futures::{StreamExt, TryStreamExt};
use num_traits::Zero;

/// Data of the left side that is buffered into memory
#[derive(Debug)]
struct JoinLeftData {
    /// Single RecordBatch with all rows from the left side
    merged_batch: RecordBatch,
    /// Track memory reservation for merged_batch. Relies on drop
    /// semantics to release reservation when JoinLeftData is dropped.
    _reservation: MemoryReservation,
}

#[expect(rustdoc::private_intra_doc_links)]
/// Cross Join Execution Plan
///
/// This operator is used when there are no predicates between two tables and
/// returns the Cartesian product of the two tables.
///
/// Buffers the left input into memory and then streams batches from each
/// partition on the right input combining them with the buffered left input
/// to generate the output.
///
/// # Clone / Shared State
///
/// Note this structure includes a [`OnceAsync`] that is used to coordinate the
/// loading of the left side with the processing in each output stream.
/// Therefore it can not be [`Clone`]
#[derive(Debug)]
pub struct CrossJoinExec {
    /// left (build) side which gets loaded in memory
    pub left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are combined with left side
    pub right: Arc<dyn ExecutionPlan>,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Buffered copy of left (build) side in memory.
    ///
    /// This structure is *shared* across all output streams.
    ///
    /// Each output stream waits on the `OnceAsync` to signal the completion of
    /// the left side loading.
    left_fut: OnceAsync<JoinLeftData>,
    /// Execution plan metrics
    metrics: ExecutionPlanMetricsSet,
    /// Properties such as schema, equivalence properties, ordering, partitioning, etc.
    cache: Arc<PlanProperties>,
}

impl CrossJoinExec {
    /// Create a new [CrossJoinExec].
    pub fn new(left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>) -> Self {
        // left then right
        let (all_columns, metadata) = {
            let left_schema = left.schema();
            let right_schema = right.schema();
            let left_fields = left_schema.fields().iter();
            let right_fields = right_schema.fields().iter();

            let mut metadata = left_schema.metadata().clone();
            metadata.extend(right_schema.metadata().clone());

            (
                left_fields.chain(right_fields).cloned().collect::<Fields>(),
                metadata,
            )
        };

        let schema = Arc::new(Schema::new(all_columns).with_metadata(metadata));
        let cache = Self::compute_properties(&left, &right, Arc::clone(&schema)).unwrap();

        CrossJoinExec {
            left,
            right,
            schema,
            left_fut: Default::default(),
            metrics: ExecutionPlanMetricsSet::default(),
            cache: Arc::new(cache),
        }
    }

    /// left (build) side which gets loaded in memory
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// right side which gets combined with left side
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties
        // TODO: Check equivalence properties of cross join, it may preserve
        //       ordering in some cases.
        let eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &JoinType::Full,
            schema,
            &[false, false],
            None,
            &[],
        )?;

        // Get output partitioning:
        // TODO: Optimize the cross join implementation to generate M * N
        //       partitions.
        let output_partitioning = adjust_right_output_partitioning(
            right.output_partitioning(),
            left.schema().fields.len(),
        )?;

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Final,
            boundedness_from_children([left, right]),
        ))
    }

    /// Returns a new `ExecutionPlan` that computes the same join as this one,
    /// with the left and right inputs swapped using the  specified
    /// `partition_mode`.
    ///
    /// # Notes:
    ///
    /// This function should be called BEFORE inserting any repartitioning
    /// operators on the join's children. Check [`super::HashJoinExec::swap_inputs`]
    /// for more details.
    pub fn swap_inputs(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let new_join =
            CrossJoinExec::new(Arc::clone(&self.right), Arc::clone(&self.left));
        reorder_output_after_swap(
            Arc::new(new_join),
            &self.left.schema(),
            &self.right.schema(),
        )
    }
}

/// Asynchronously collect the result of the left child
async fn load_left_input(
    stream: SendableRecordBatchStream,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
) -> Result<JoinLeftData> {
    let left_schema = stream.schema();

    // Load all batches and count the rows
    let (batches, metrics, reservation) = stream
        .try_fold(
            (Vec::new(), metrics, reservation),
            |(mut batches, metrics, reservation), batch| async {
                let batch_size = batch.get_array_memory_size();
                // Reserve memory for incoming batch
                reservation.try_grow(batch_size)?;
                // Update metrics
                metrics.build_mem_used.add(batch_size);
                metrics.build_input_batches.add(1);
                metrics.build_input_rows.add(batch.num_rows());
                // Push batch to output
                batches.push(batch);
                Ok((batches, metrics, reservation))
            },
        )
        .await?;

    let build_timer = metrics.build_time.timer();
    let merged_batch = concat_batches(&left_schema, &batches)?;
    build_timer.done();

    Ok(JoinLeftData {
        merged_batch,
        _reservation: reservation,
    })
}

impl DisplayAs for CrossJoinExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CrossJoinExec")
            }
            DisplayFormatType::TreeRender => {
                // no extra info to display
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for CrossJoinExec {
    fn name(&self) -> &'static str {
        "CrossJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // CrossJoin has no join conditions or expressions
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        validate_child_count!(self, children);
        match options.children_properties {
            ChildrenPropertiesMode::Keep => {
                let left = children.swap_remove(0);
                let right = children.swap_remove(0);

                Ok(Arc::new(Self {
                    left,
                    right,
                    metrics: ExecutionPlanMetricsSet::new(),
                    left_fut: Default::default(),
                    cache: Arc::clone(&self.cache),
                    schema: Arc::clone(&self.schema),
                }))
            }
            ChildrenPropertiesMode::Recompute => Ok(Arc::new(CrossJoinExec::new(
                Arc::clone(&children[0]),
                Arc::clone(&children[1]),
            ))),
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn with_new_children_and_same_properties(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
        )
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let new_exec = CrossJoinExec {
            left: Arc::clone(&self.left),
            right: Arc::clone(&self.right),
            schema: Arc::clone(&self.schema),
            left_fut: Default::default(), // reset the build side!
            metrics: ExecutionPlanMetricsSet::default(),
            cache: Arc::clone(&self.cache),
        };
        Ok(Arc::new(new_exec))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input_distribution_requirements().into_per_child()
    }

    fn input_distribution_requirements(&self) -> crate::InputDistributionRequirements {
        crate::InputDistributionRequirements::new(vec![
            Distribution::SinglePartition,
            Distribution::UnspecifiedDistribution,
        ])
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq_or_internal_err!(
            self.left.output_partitioning().partition_count(),
            1,
            "Invalid CrossJoinExec, the output partition count of the left child must be 1,\
                 consider using CoalescePartitionsExec or the EnforceDistribution rule"
        );

        let stream = self.right.execute(partition, Arc::clone(&context))?;

        let join_metrics = BuildProbeJoinMetrics::new(partition, &self.metrics);

        // Initialization of operator-level reservation
        let reservation =
            MemoryConsumer::new("CrossJoinExec").register(context.memory_pool());

        let left_fut = self.left_fut.try_once(|| {
            let left_stream = self.left.execute(0, context)?;

            Ok(load_left_input(
                left_stream,
                join_metrics.clone(),
                reservation,
            ))
        })?;

        let mut state = CrossJoinStream {
            schema: Arc::clone(&self.schema),
            left_fut,
            right: stream,
            join_metrics,
            left_data: RecordBatch::new_empty(self.left().schema()),
        };

        let schema = Arc::clone(&self.schema);
        let baseline_metrics = state.join_metrics.baseline.clone();
        let stream =
            async_try_stream(|mut emitter| async move { state.join(&mut emitter).await });

        Ok(Box::pin(ObservedStream::new(
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
            baseline_metrics,
            None,
        )))
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        // Left side is always broadcast, so it always needs overall stats.
        // Right side is partitioned, so it needs per-partition stats.
        vec![ChildStats::At(None), ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let left_stats = input_stats[0].as_ref().clone();
        let right_stats = input_stats[1].as_ref().clone();

        Ok(Arc::new(stats_cartesian_product(left_stats, right_stats)))
    }

    /// Tries to swap the projection with its input [`CrossJoinExec`]. If it can be done,
    /// it returns the new swapped version having the [`CrossJoinExec`] as the top plan.
    /// Otherwise, it returns None.
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Convert projected PhysicalExpr's to columns. If not possible, we cannot proceed.
        let Some(projection_as_columns) = physical_to_column_exprs(projection.expr())
        else {
            return Ok(None);
        };

        let (far_right_left_col_ind, far_left_right_col_ind) = join_table_borders(
            self.left().schema().fields().len(),
            &projection_as_columns,
        );

        if !join_allows_pushdown(
            &projection_as_columns,
            &self.schema(),
            far_right_left_col_ind,
            far_left_right_col_ind,
        ) {
            return Ok(None);
        }

        let (new_left, new_right) = new_join_children(
            &projection_as_columns,
            far_right_left_col_ind,
            far_left_right_col_ind,
            self.left(),
            self.right(),
        )?;

        Ok(Some(Arc::new(CrossJoinExec::new(
            Arc::new(new_left),
            Arc::new(new_right),
        ))))
    }
    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &crate::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalPlanNode>> {
        use datafusion_proto_models::protobuf;

        // Destructure exhaustively (no `..`) so that a newly added field is a
        // compile error here instead of being silently left out of the proto.
        let Self {
            left,
            right,
            // derived from the children's schemas by `new` on decode
            schema: _,
            // runtime build-side state, not part of the plan
            left_fut: _,
            // runtime metrics, not part of the plan
            metrics: _,
            // recomputed by `new` on decode
            cache: _,
        } = self;

        let left = ctx.encode_child(left)?;
        let right = ctx.encode_child(right)?;

        Ok(Some(protobuf::PhysicalPlanNode {
            physical_plan_type: Some(
                protobuf::physical_plan_node::PhysicalPlanType::CrossJoin(Box::new(
                    protobuf::CrossJoinExecNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                    },
                )),
            ),
        }))
    }
}

#[cfg(feature = "proto")]
impl CrossJoinExec {
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalPlanNode,
        ctx: &crate::proto::ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion_proto_models::protobuf;

        let crossjoin = crate::expect_plan_variant!(
            node,
            protobuf::physical_plan_node::PhysicalPlanType::CrossJoin,
            "CrossJoinExec",
        );

        // Destructure exhaustively (no `..`) so that a newly added proto field
        // is a compile error here instead of being silently ignored.
        let protobuf::CrossJoinExecNode { left, right } = &**crossjoin;

        let left = ctx.decode_required_child(left.as_deref(), "CrossJoinExec", "left")?;
        let right =
            ctx.decode_required_child(right.as_deref(), "CrossJoinExec", "right")?;

        Ok(Arc::new(CrossJoinExec::new(left, right)))
    }
}

/// [left/right]_col_count are required in case the column statistics are None
fn stats_cartesian_product(
    left_stats: Statistics,
    right_stats: Statistics,
) -> Statistics {
    let left_row_count = left_stats.num_rows;
    let right_row_count = right_stats.num_rows;

    // Calculate global stats
    let num_rows = left_row_count.multiply(&right_row_count);

    // Each output row includes every left and right column, so the left side is
    // repeated once per right row and the right side once per left row.
    let left_byte_size = left_stats.total_byte_size.multiply(&right_row_count);
    let right_byte_size = right_stats.total_byte_size.multiply(&left_row_count);
    let total_byte_size = left_byte_size.add(&right_byte_size);

    let left_col_stats = left_stats.column_statistics;
    let right_col_stats = right_stats.column_statistics;

    // the null counts must be multiplied by the row counts of the other side (if defined)
    // Min, max and distinct_count on the other hand are invariants.
    let cross_join_stats = left_col_stats
        .into_iter()
        .map(|s| {
            let widened_sum = s.sum_value.cast_to_sum_type();
            ColumnStatistics {
                null_count: s.null_count.multiply(&right_row_count),
                distinct_count: s.distinct_count,
                min_value: s.min_value,
                max_value: s.max_value,
                sum_value: widened_sum
                    .get_value()
                    // Cast the row count into the same type as any existing sum value
                    .and_then(|v| {
                        Precision::<ScalarValue>::from(right_row_count)
                            .cast_to(&v.data_type())
                            .ok()
                    })
                    .map(|row_count| widened_sum.multiply(&row_count))
                    .unwrap_or(Precision::Absent),
                byte_size: Precision::Absent,
            }
        })
        .chain(right_col_stats.into_iter().map(|s| {
            let widened_sum = s.sum_value.cast_to_sum_type();
            ColumnStatistics {
                null_count: s.null_count.multiply(&left_row_count),
                distinct_count: s.distinct_count,
                min_value: s.min_value,
                max_value: s.max_value,
                sum_value: widened_sum
                    .get_value()
                    // Cast the row count into the same type as any existing sum value
                    .and_then(|v| {
                        Precision::<ScalarValue>::from(left_row_count)
                            .cast_to(&v.data_type())
                            .ok()
                    })
                    .map(|row_count| widened_sum.multiply(&row_count))
                    .unwrap_or(Precision::Absent),
                byte_size: Precision::Absent,
            }
        }))
        .collect();

    Statistics {
        num_rows,
        total_byte_size,
        column_statistics: cross_join_stats,
    }
}

/// A stream that issues [RecordBatch]es as they arrive from the right of the join.
struct CrossJoinStream {
    /// Input schema
    schema: Arc<Schema>,
    /// Future for data from left side
    left_fut: OnceFut<JoinLeftData>,
    /// Right side stream
    right: SendableRecordBatchStream,
    /// Join execution metrics
    join_metrics: BuildProbeJoinMetrics,
    /// Left data (copy of the entire buffered left side)
    left_data: RecordBatch,
}

fn build_batch(
    left_index: usize,
    batch: &RecordBatch,
    left_data: &RecordBatch,
    schema: &Schema,
) -> Result<RecordBatch> {
    // Repeat value on the left n times
    let arrays = left_data
        .columns()
        .iter()
        .map(|arr| {
            let scalar = ScalarValue::try_from_array(arr, left_index)?;
            scalar.to_array_of_size(batch.num_rows())
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new_with_options(
        Arc::new(schema.clone()),
        arrays
            .iter()
            .chain(batch.columns().iter())
            .cloned()
            .collect(),
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(Into::into)
}

impl CrossJoinStream {
    // Collect the left (build) side, then continue processing the right side against it until we have no more rows on the right
    async fn join(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        if !self.collect_build_side().await? {
            return Ok(());
        }

        self.process_right_batch(emitter).await?;

        Ok(())
    }

    /// Collects build (left) side of the join into the state. In case of an empty build batch, the execution terminates.
    /// Returns true if build side was loaded and non-empty
    async fn collect_build_side(&mut self) -> Result<bool> {
        let left_data = poll_fn(|cx| {
            self.left_fut
                .get(cx)
                .map(|res| res.map(|data| data.merged_batch.clone()))
        })
        .await?;

        let is_empty = left_data.num_rows().is_zero();
        self.left_data = left_data;
        Ok(!is_empty)
    }

    /// Fetches the probe (right) batch, updates the metrics, and returns the batch
    async fn fetch_probe_batch(&mut self) -> Result<Option<RecordBatch>> {
        let right_data = match self.right.next().await {
            Some(Ok(right_data)) => right_data,
            Some(Err(e)) => return Err(e),
            None => {
                // Release the right (probe) input pipeline's resources.
                let right_schema = self.right.schema();
                self.right = Box::pin(EmptyRecordBatchStream::new(right_schema));
                return Ok(None);
            }
        };
        self.join_metrics.input_batches.add(1);
        self.join_metrics.input_rows.add(right_data.num_rows());

        Ok(Some(right_data))
    }

    /// Joins the left data with the current probe batch, using the emitter to emit the resultant batches
    async fn process_right_batch(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        while let Some(right_batch) = self.fetch_probe_batch().await? {
            for left_index in 0..self.left_data.num_rows() {
                let join_timer = self.join_metrics.join_time.timer();
                let result =
                    build_batch(left_index, &right_batch, &self.left_data, &self.schema)?;
                join_timer.done();

                emitter.emit(result).await;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common;
    use crate::test::{assert_join_metrics, build_table_scan_i32};

    use datafusion_common::{assert_contains, test_util::batches_to_sort_string};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use insta::assert_snapshot;

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>, MetricsSet)> {
        let join = CrossJoinExec::new(left, right);
        let columns_header = columns(&join.schema());

        let stream = join.execute(0, context)?;
        let batches = common::collect(stream).await?;
        let metrics = join.metrics().unwrap();

        Ok((columns_header, batches, metrics))
    }

    #[tokio::test]
    async fn test_stats_cartesian_product() {
        let left_row_count = 11;
        let left_bytes = 23;
        let right_row_count = 7;
        let right_bytes = 27;

        let left = Statistics {
            num_rows: Precision::Exact(left_row_count),
            total_byte_size: Precision::Exact(left_bytes),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(42))),
                    null_count: Precision::Exact(0),
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(3),
                    byte_size: Precision::Absent,
                },
            ],
        };

        let right = Statistics {
            num_rows: Precision::Exact(right_row_count),
            total_byte_size: Precision::Exact(right_bytes),
            column_statistics: vec![ColumnStatistics {
                distinct_count: Precision::Exact(3),
                max_value: Precision::Exact(ScalarValue::Int64(Some(12))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                sum_value: Precision::Exact(ScalarValue::Int64(Some(20))),
                null_count: Precision::Exact(2),
                byte_size: Precision::Absent,
            }],
        };

        let result = stats_cartesian_product(left, right);

        let expected = Statistics {
            num_rows: Precision::Exact(left_row_count * right_row_count),
            total_byte_size: Precision::Exact(
                left_bytes * right_row_count + right_bytes * left_row_count,
            ),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(
                        42 * right_row_count as i64,
                    ))),
                    null_count: Precision::Exact(0),
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(3 * right_row_count),
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(3),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(12))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(
                        20 * left_row_count as i64,
                    ))),
                    null_count: Precision::Exact(2 * left_row_count),
                    byte_size: Precision::Absent,
                },
            ],
        };

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_stats_cartesian_product_with_unknown_size() {
        let left_row_count = 11;

        let left = Statistics {
            num_rows: Precision::Exact(left_row_count),
            total_byte_size: Precision::Exact(23),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(42))),
                    null_count: Precision::Exact(0),
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(3),
                    byte_size: Precision::Absent,
                },
            ],
        };

        let right = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                distinct_count: Precision::Exact(3),
                max_value: Precision::Exact(ScalarValue::Int64(Some(12))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                sum_value: Precision::Exact(ScalarValue::Int64(Some(20))),
                null_count: Precision::Exact(2),
                byte_size: Precision::Absent,
            }],
        };

        let result = stats_cartesian_product(left, right);

        let expected = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    sum_value: Precision::Absent, // we don't know the row count on the right
                    null_count: Precision::Absent, // we don't know the row count on the right
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Absent, // we don't know the row count on the right
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(3),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(12))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(
                        20 * left_row_count as i64,
                    ))),
                    null_count: Precision::Exact(2 * left_row_count),
                    byte_size: Precision::Absent,
                },
            ],
        };

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_stats_cartesian_product_unsigned_sum_widens_to_u64() {
        let left_row_count = 2;
        let right_row_count = 3;

        let left = Statistics {
            num_rows: Precision::Exact(left_row_count),
            total_byte_size: Precision::Exact(10),
            column_statistics: vec![ColumnStatistics {
                distinct_count: Precision::Exact(2),
                max_value: Precision::Exact(ScalarValue::UInt32(Some(10))),
                min_value: Precision::Exact(ScalarValue::UInt32(Some(1))),
                sum_value: Precision::Exact(ScalarValue::UInt32(Some(7))),
                null_count: Precision::Exact(0),
                byte_size: Precision::Absent,
            }],
        };

        let right = Statistics {
            num_rows: Precision::Exact(right_row_count),
            total_byte_size: Precision::Exact(10),
            column_statistics: vec![ColumnStatistics {
                distinct_count: Precision::Exact(3),
                max_value: Precision::Exact(ScalarValue::UInt32(Some(12))),
                min_value: Precision::Exact(ScalarValue::UInt32(Some(0))),
                sum_value: Precision::Exact(ScalarValue::UInt32(Some(11))),
                null_count: Precision::Exact(0),
                byte_size: Precision::Absent,
            }],
        };

        let result = stats_cartesian_product(left, right);

        assert_eq!(
            result.column_statistics[0].sum_value,
            Precision::Exact(ScalarValue::UInt64(Some(21)))
        );
        assert_eq!(
            result.column_statistics[1].sum_value,
            Precision::Exact(ScalarValue::UInt64(Some(22)))
        );
    }

    #[tokio::test]
    async fn test_join() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        let left = build_table_scan_i32(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 6]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table_scan_i32(
            ("a2", &vec![10, 11]),
            ("b2", &vec![12, 13]),
            ("c2", &vec![14, 15]),
        );

        let (columns, batches, metrics) = join_collect(left, right, task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b2 | c2 |
        +----+----+----+----+----+----+
        | 1  | 4  | 7  | 10 | 12 | 14 |
        | 1  | 4  | 7  | 11 | 13 | 15 |
        | 2  | 5  | 8  | 10 | 12 | 14 |
        | 2  | 5  | 8  | 11 | 13 | 15 |
        | 3  | 6  | 9  | 10 | 12 | 14 |
        | 3  | 6  | 9  | 11 | 13 | 15 |
        +----+----+----+----+----+----+
        ");

        assert_join_metrics!(metrics, 6);

        Ok(())
    }

    #[tokio::test]
    async fn test_overallocation() -> Result<()> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(100, 1.0)
            .build_arc()?;
        let task_ctx = TaskContext::default().with_runtime(runtime);
        let task_ctx = Arc::new(task_ctx);

        let left = build_table_scan_i32(
            ("a1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("b1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("c1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
        );
        let right = build_table_scan_i32(
            ("a2", &vec![10, 11]),
            ("b2", &vec![12, 13]),
            ("c2", &vec![14, 15]),
        );

        let err = join_collect(left, right, task_ctx).await.unwrap_err();

        assert_contains!(
            err.to_string(),
            "Resources exhausted: Additional allocation failed for CrossJoinExec with top memory consumers (across reservations) as:\n  CrossJoinExec"
        );

        Ok(())
    }

    /// Returns the column names on the schema
    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }
}
