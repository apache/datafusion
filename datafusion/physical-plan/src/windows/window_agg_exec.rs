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

//! Stream and channel implementations for window function expressions.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[cfg(feature = "proto")]
use super::proto::{decode_physical_window_expr, encode_physical_window_expr};
use super::utils::create_schema;
use crate::execution_plan::{CardinalityEffect, EmissionType};
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::statistics::{ChildStats, StatisticsArgs};
use crate::stream::EmptyRecordBatchStream;
use crate::windows::{
    calc_requirements, get_ordered_partition_by_indices, get_partition_by_sort_exprs,
    window_equivalence_properties,
};
use crate::{
    ChildrenPropertiesMode, ColumnStatistics, DisplayAs, DisplayFormatType, Distribution,
    ExecutionPlan, ExecutionPlanProperties, InputDistributionRequirements, PhysicalExpr,
    PlanProperties, RecordBatchStream, ReplaceChildrenOptions, SendableRecordBatchStream,
    Statistics, WindowExpr, validate_child_count,
};

use arrow::array::ArrayRef;
use arrow::compute::{concat, concat_batches};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::utils::{evaluate_partition_ranges, transpose};
use datafusion_common::{Result, assert_eq_or_internal_err};
use datafusion_execution::TaskContext;
use datafusion_physical_expr_common::sort_expr::{
    OrderingRequirements, PhysicalSortExpr,
};

use futures::{Stream, StreamExt, ready};

/// Window execution plan
#[derive(Debug, Clone)]
pub struct WindowAggExec {
    /// Input plan
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Window function expression
    window_expr: Vec<Arc<dyn WindowExpr>>,
    /// Schema after the window is run
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Partition by indices that defines preset for existing ordering
    // see `get_ordered_partition_by_indices` for more details.
    ordered_partition_by_indices: Vec<usize>,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: Arc<PlanProperties>,
    /// If `can_partition` is false, partition_keys is always empty.
    can_repartition: bool,
}

impl WindowAggExec {
    /// Create a new execution plan for window aggregates
    pub fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        can_repartition: bool,
    ) -> Result<Self> {
        let schema = create_schema(&input.schema(), &window_expr)?;
        let schema = Arc::new(schema);

        let ordered_partition_by_indices =
            get_ordered_partition_by_indices(window_expr[0].partition_by(), &input)?;
        let cache = Self::compute_properties(&schema, &input, &window_expr)?;
        Ok(Self {
            input,
            window_expr,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            ordered_partition_by_indices,
            cache: Arc::new(cache),
            can_repartition,
        })
    }

    /// Window expressions
    pub fn window_expr(&self) -> &[Arc<dyn WindowExpr>] {
        &self.window_expr
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Return the output sort order of partition keys: For example
    /// OVER(PARTITION BY a, ORDER BY b) -> would give sorting of the column a
    // We are sure that partition by columns are always at the beginning of sort_keys
    // Hence returned `PhysicalSortExpr` corresponding to `PARTITION BY` columns can be used safely
    // to calculate partition separation points
    pub fn partition_by_sort_keys(&self) -> Result<Vec<PhysicalSortExpr>> {
        let partition_by = self.window_expr()[0].partition_by();
        get_partition_by_sort_exprs(
            &self.input,
            partition_by,
            &self.ordered_partition_by_indices,
        )
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: &SchemaRef,
        input: &Arc<dyn ExecutionPlan>,
        window_exprs: &[Arc<dyn WindowExpr>],
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties:
        let eq_properties = window_equivalence_properties(schema, input, window_exprs)?;

        // Get output partitioning:
        // Because we can have repartitioning using the partition keys this
        // would be either 1 or more than 1 depending on the presence of repartitioning.
        let output_partitioning = input.output_partitioning().clone();

        // Construct properties cache:
        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            // TODO: Emission type and boundedness information can be enhanced here
            EmissionType::Final,
            input.boundedness(),
        ))
    }

    pub fn partition_keys(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        if !self.can_repartition {
            vec![]
        } else {
            let all_partition_keys = self
                .window_expr()
                .iter()
                .map(|expr| expr.partition_by().to_vec())
                .collect::<Vec<_>>();

            all_partition_keys
                .into_iter()
                .min_by_key(|s| s.len())
                .unwrap_or_else(Vec::new)
        }
    }
}

impl DisplayAs for WindowAggExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "WindowAggExec: ")?;
                let g: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|e| {
                        format!(
                            "{}: {:?}, frame: {:?}",
                            e.name().to_owned(),
                            e.field(),
                            e.get_window_frame()
                        )
                    })
                    .collect();
                write!(f, "wdw=[{}]", g.join(", "))?;
            }
            DisplayFormatType::TreeRender => {
                let g: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|e| e.name().to_owned().to_string())
                    .collect();
                writeln!(f, "select_list={}", g.join(", "))?;
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for WindowAggExec {
    fn name(&self) -> &'static str {
        "WindowAggExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        let expressions = self.window_expr.iter().flat_map(|window_expr| {
            let expressions = window_expr.all_expressions();
            expressions
                .args
                .into_iter()
                .chain(expressions.partition_by_exprs)
                .chain(expressions.order_by_exprs)
        });
        crate::apply_expression_roots(expressions, f)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let partition_bys = self.window_expr()[0].partition_by();
        let order_keys = self.window_expr()[0].order_by();
        if self.ordered_partition_by_indices.len() < partition_bys.len() {
            vec![calc_requirements(partition_bys, order_keys)]
        } else {
            let partition_bys = self
                .ordered_partition_by_indices
                .iter()
                .map(|idx| &partition_bys[*idx]);
            vec![calc_requirements(partition_bys, order_keys)]
        }
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input_distribution_requirements().into_per_child()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        if self.partition_keys().is_empty() {
            InputDistributionRequirements::new(vec![Distribution::SinglePartition])
        } else {
            InputDistributionRequirements::new(vec![Distribution::KeyPartitioned(
                self.partition_keys(),
            )])
        }
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        validate_child_count!(self, children);
        match options.children_properties {
            ChildrenPropertiesMode::Keep => Ok(Arc::new(Self {
                input: children.swap_remove(0),
                metrics: ExecutionPlanMetricsSet::new(),
                ..Self::clone(&*self)
            })),
            ChildrenPropertiesMode::Recompute => Ok(Arc::new(WindowAggExec::try_new(
                self.window_expr.clone(),
                children.swap_remove(0),
                true,
            )?)),
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

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let stream = Box::pin(WindowAggStream::new(
            Arc::clone(&self.schema),
            self.window_expr.clone(),
            input,
            BaselineMetrics::new(&self.metrics, partition),
            self.partition_by_sort_keys()?,
            self.ordered_partition_by_indices.clone(),
        )?);
        Ok(stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let input_stat = input_stats[0].as_ref().clone();
        let win_cols = self.window_expr.len();
        let input_cols = self.input.schema().fields().len();
        // TODO stats: some windowing function will maintain invariants such as min, max...
        let mut column_statistics = Vec::with_capacity(win_cols + input_cols);
        // copy stats of the input to the beginning of the schema.
        column_statistics.extend(input_stat.column_statistics);
        for _ in 0..win_cols {
            column_statistics.push(ColumnStatistics::new_unknown())
        }
        Ok(Arc::new(Statistics {
            num_rows: input_stat.num_rows,
            column_statistics,
            total_byte_size: Precision::Absent,
        }))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &crate::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalPlanNode>> {
        use datafusion_proto_models::protobuf;

        // Exhaustive destructure: adding a field to `WindowAggExec` without
        // deciding how it is serialized is a compile error, not a silent
        // round-trip gap.
        let Self {
            input,
            window_expr,
            // Derived at construction by `create_schema` from the input schema
            // and the window expressions.
            schema: _,
            // Runtime execution state, rebuilt empty on decode.
            metrics: _,
            // Derived at construction by `get_ordered_partition_by_indices`.
            ordered_partition_by_indices: _,
            // Derived at construction by `Self::compute_properties`.
            cache: _,
            // No wire field of its own; it is folded into `partition_keys`
            // below, since `partition_keys()` returns an empty vec when this is
            // false and the decoder recovers it as `!partition_keys.is_empty()`.
            can_repartition: _,
        } = self;

        let input = ctx.encode_child(input)?;
        let window_expr = window_expr
            .iter()
            .map(|expr| encode_physical_window_expr(expr, ctx))
            .collect::<Result<Vec<_>>>()?;
        let partition_keys = self
            .partition_keys()
            .iter()
            .map(|expr| ctx.encode_expr(expr))
            .collect::<Result<Vec<_>>>()?;

        Ok(Some(protobuf::PhysicalPlanNode {
            physical_plan_type: Some(
                protobuf::physical_plan_node::PhysicalPlanType::Window(Box::new(
                    protobuf::WindowAggExecNode {
                        input: Some(Box::new(input)),
                        window_expr,
                        partition_keys,
                        // `None` distinguishes a `WindowAggExec` from a
                        // `BoundedWindowAggExec` on the shared `Window` variant.
                        input_order_mode: None,
                    },
                )),
            ),
        }))
    }
}

#[cfg(feature = "proto")]
impl WindowAggExec {
    /// Reconstruct a window plan from its protobuf representation.
    ///
    /// This returns a [`WindowAggExec`] when `input_order_mode` is absent and a
    /// [`BoundedWindowAggExec`] when it is present.
    ///
    /// [`BoundedWindowAggExec`]: crate::windows::BoundedWindowAggExec
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalPlanNode,
        ctx: &crate::proto::ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use super::BoundedWindowAggExec;
        use crate::InputOrderMode;
        use datafusion_common::utils::usize_from_wire;
        use datafusion_proto_models::protobuf;
        use protobuf::window_agg_exec_node::InputOrderMode as ProtoInputOrderMode;

        let window_agg = crate::expect_plan_variant!(
            node,
            protobuf::physical_plan_node::PhysicalPlanType::Window,
            "WindowAggExec",
        );
        // Exhaustive destructure: a new field on `WindowAggExecNode` is a
        // compile error here rather than a silently ignored wire field.
        let protobuf::WindowAggExecNode {
            input,
            window_expr,
            partition_keys,
            input_order_mode,
        } = window_agg.as_ref();

        let input =
            ctx.decode_required_child(input.as_deref(), "WindowAggExec", "input")?;
        let input_schema = input.schema();
        let window_expr = window_expr
            .iter()
            .map(|expr| decode_physical_window_expr(expr, ctx, input_schema.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        let partition_keys = partition_keys
            .iter()
            .map(|expr| ctx.decode_expr(expr, input_schema.as_ref()))
            .collect::<Result<Vec<_>>>()?;

        if let Some(input_order_mode) = input_order_mode.as_ref() {
            let input_order_mode = match input_order_mode {
                ProtoInputOrderMode::Linear(_) => InputOrderMode::Linear,
                ProtoInputOrderMode::PartiallySorted(
                    protobuf::PartiallySortedInputOrderMode { columns },
                ) => InputOrderMode::PartiallySorted(
                    columns
                        .iter()
                        .map(|column| {
                            usize_from_wire(*column, "WindowAggExec", "columns")
                        })
                        .collect::<Result<Vec<_>>>()?,
                ),
                ProtoInputOrderMode::Sorted(_) => InputOrderMode::Sorted,
            };
            Ok(Arc::new(BoundedWindowAggExec::try_new(
                window_expr,
                input,
                input_order_mode,
                // `can_repartition` has no wire field: the encoder writes an
                // empty `partition_keys` when it is false.
                !partition_keys.is_empty(),
            )?))
        } else {
            Ok(Arc::new(WindowAggExec::try_new(
                window_expr,
                input,
                // See above: `can_repartition` is recovered from `partition_keys`.
                !partition_keys.is_empty(),
            )?))
        }
    }
}

/// Compute the window aggregate columns
fn compute_window_aggregates(
    window_expr: &[Arc<dyn WindowExpr>],
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    window_expr
        .iter()
        .map(|window_expr| window_expr.evaluate(batch))
        .collect()
}

/// stream for window aggregation plan
pub struct WindowAggStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    batches: Vec<RecordBatch>,
    finished: bool,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    partition_by_sort_keys: Vec<PhysicalSortExpr>,
    baseline_metrics: BaselineMetrics,
    ordered_partition_by_indices: Vec<usize>,
}

impl WindowAggStream {
    /// Create a new WindowAggStream
    pub fn new(
        schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
        partition_by_sort_keys: Vec<PhysicalSortExpr>,
        ordered_partition_by_indices: Vec<usize>,
    ) -> Result<Self> {
        // In WindowAggExec all partition by columns should be ordered.
        assert_eq_or_internal_err!(
            window_expr[0].partition_by().len(),
            ordered_partition_by_indices.len(),
            "All partition by columns should have an ordering"
        );
        Ok(Self {
            schema,
            input,
            batches: vec![],
            finished: false,
            window_expr,
            baseline_metrics,
            partition_by_sort_keys,
            ordered_partition_by_indices,
        })
    }

    fn compute_aggregates(&self) -> Result<Option<RecordBatch>> {
        // record compute time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        let batch = concat_batches(&self.input.schema(), &self.batches)?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let partition_by_sort_keys = self
            .ordered_partition_by_indices
            .iter()
            .map(|idx| self.partition_by_sort_keys[*idx].evaluate_to_sort_column(&batch))
            .collect::<Result<Vec<_>>>()?;
        let partition_points =
            evaluate_partition_ranges(batch.num_rows(), &partition_by_sort_keys)?;

        let mut partition_results = vec![];
        // Calculate window cols
        for partition_point in partition_points {
            let length = partition_point.end - partition_point.start;
            partition_results.push(compute_window_aggregates(
                &self.window_expr,
                &batch.slice(partition_point.start, length),
            )?)
        }
        let columns = transpose(partition_results)
            .iter()
            .map(|elems| concat(&elems.iter().map(|x| x.as_ref()).collect::<Vec<_>>()))
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;

        // combine with the original cols
        // note the setup of window aggregates is that they newly calculated window
        // expression results are always appended to the columns
        let mut batch_columns = batch.columns().to_vec();
        // calculate window cols
        batch_columns.extend_from_slice(&columns);
        Ok(Some(RecordBatch::try_new(
            Arc::clone(&self.schema),
            batch_columns,
        )?))
    }
}

impl Stream for WindowAggStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl WindowAggStream {
    #[inline]
    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.finished {
            return Poll::Ready(None);
        }

        loop {
            return Poll::Ready(Some(match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    self.batches.push(batch);
                    continue;
                }
                Some(Err(e)) => Err(e),
                None => {
                    // Release the input pipeline's resources before computing
                    // the final aggregates.
                    let input_schema = self.input.schema();
                    self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
                    let Some(result) = self.compute_aggregates()? else {
                        return Poll::Ready(None);
                    };
                    self.finished = true;
                    // Empty record batches should not be emitted.
                    // They need to be treated as  [`Option<RecordBatch>`]es and handled separately
                    debug_assert!(result.num_rows() > 0);
                    Ok(result)
                }
            }));
        }
    }
}

impl RecordBatchStream for WindowAggStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::TestMemoryExec;
    use crate::windows::create_window_expr;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{
        WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
    };
    use datafusion_functions_aggregate::count::count_udaf;

    #[test]
    fn test_window_agg_cardinality_effect() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&schema), None)?);
        let args = vec![crate::expressions::col("a", &schema)?];
        let window_expr = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(count_udaf()),
            "count(a)".to_string(),
            &args,
            &[],
            &[],
            Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            )),
            Arc::clone(&schema),
            false,
            false,
            None,
        )?;

        let window = WindowAggExec::try_new(vec![window_expr], input, true)?;
        assert!(matches!(
            window.cardinality_effect(),
            CardinalityEffect::Equal
        ));
        Ok(())
    }
}
