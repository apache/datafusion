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

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use itertools::Itertools;

use super::{
    ColumnStatistics, DisplayAs, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use crate::coalesce::LimitedBatchCoalescer;
use crate::coalesce::PushBatchStatus::LimitReached;
use crate::common::can_project;
use crate::execution_plan::CardinalityEffect;
use crate::filter_pushdown::{
    ChildFilterDescription, ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation, PushedDown, PushedDownPredicate,
};
use crate::metrics::{MetricBuilder, MetricType};
use crate::projection::{
    make_with_child, try_embed_projection, update_expr, EmbeddedProjection,
    ProjectionExec, ProjectionExpr,
};
use crate::{
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RatioMetrics},
    DisplayFormatType, ExecutionPlan,
};

use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::config::ConfigOptions;
use datafusion_common::stats::Precision;
use datafusion_common::{
    internal_err, plan_err, project_schema, DataFusionError, Result, ScalarValue,
};
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::equivalence::ProjectionMapping;
use datafusion_physical_expr::expressions::{lit, BinaryExpr, Column};
use datafusion_physical_expr::intervals::utils::check_support;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{
    analyze, conjunction, split_conjunction, AcrossPartitions, AnalysisContext,
    ConstExpr, ExprBoundaries, PhysicalExpr,
};

use datafusion_physical_expr_common::physical_expr::fmt_sql;
use futures::stream::{Stream, StreamExt};
use log::trace;

const FILTER_EXEC_DEFAULT_SELECTIVITY: u8 = 20;
const FILTER_EXEC_DEFAULT_BATCH_SIZE: usize = 8192;

/// FilterExec evaluates a boolean predicate against all input batches to determine which rows to
/// include in its output batches.
#[derive(Debug, Clone)]
pub struct FilterExec {
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Selectivity for statistics. 0 = no rows, 100 = all rows
    default_selectivity: u8,
    /// Properties equivalence properties, partitioning, etc.
    cache: PlanProperties,
    /// The projection indices of the columns in the output schema of join
    projection: Option<Vec<usize>>,
    /// Target batch size for output batches
    batch_size: usize,
    /// Number of rows to fetch
    fetch: Option<usize>,
}

impl FilterExec {
    /// Create a FilterExec on an input
    #[expect(clippy::needless_pass_by_value)]
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        match predicate.data_type(input.schema().as_ref())? {
            DataType::Boolean => {
                let default_selectivity = FILTER_EXEC_DEFAULT_SELECTIVITY;
                let cache = Self::compute_properties(
                    &input,
                    &predicate,
                    default_selectivity,
                    None,
                )?;
                Ok(Self {
                    predicate,
                    input: Arc::clone(&input),
                    metrics: ExecutionPlanMetricsSet::new(),
                    default_selectivity,
                    cache,
                    projection: None,
                    batch_size: FILTER_EXEC_DEFAULT_BATCH_SIZE,
                    fetch: None,
                })
            }
            other => {
                plan_err!("Filter predicate must return BOOLEAN values, got {other:?}")
            }
        }
    }

    pub fn with_default_selectivity(
        mut self,
        default_selectivity: u8,
    ) -> Result<Self, DataFusionError> {
        if default_selectivity > 100 {
            return plan_err!(
                "Default filter selectivity value needs to be less than or equal to 100"
            );
        }
        self.default_selectivity = default_selectivity;
        Ok(self)
    }

    /// Return new instance of [FilterExec] with the given projection.
    pub fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        //  Check if the projection is valid
        can_project(&self.schema(), projection.as_ref())?;

        let projection = match projection {
            Some(projection) => match &self.projection {
                Some(p) => Some(projection.iter().map(|i| p[*i]).collect()),
                None => Some(projection),
            },
            None => None,
        };

        let cache = Self::compute_properties(
            &self.input,
            &self.predicate,
            self.default_selectivity,
            projection.as_ref(),
        )?;
        Ok(Self {
            predicate: Arc::clone(&self.predicate),
            input: Arc::clone(&self.input),
            metrics: self.metrics.clone(),
            default_selectivity: self.default_selectivity,
            cache,
            projection,
            batch_size: self.batch_size,
            fetch: self.fetch,
        })
    }

    pub fn with_batch_size(&self, batch_size: usize) -> Result<Self> {
        Ok(Self {
            predicate: Arc::clone(&self.predicate),
            input: Arc::clone(&self.input),
            metrics: self.metrics.clone(),
            default_selectivity: self.default_selectivity,
            cache: self.cache.clone(),
            projection: self.projection.clone(),
            batch_size,
            fetch: self.fetch,
        })
    }

    /// The expression to filter on. This expression must evaluate to a boolean value.
    pub fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// The default selectivity
    pub fn default_selectivity(&self) -> u8 {
        self.default_selectivity
    }

    /// Projection
    pub fn projection(&self) -> Option<&Vec<usize>> {
        self.projection.as_ref()
    }

    /// Calculates `Statistics` for `FilterExec`, by applying selectivity (either default, or estimated) to input statistics.
    fn statistics_helper(
        schema: &SchemaRef,
        input_stats: Statistics,
        predicate: &Arc<dyn PhysicalExpr>,
        default_selectivity: u8,
    ) -> Result<Statistics> {
        if !check_support(predicate, schema) {
            let selectivity = default_selectivity as f64 / 100.0;
            let mut stats = input_stats.to_inexact();
            stats.num_rows = stats.num_rows.with_estimated_selectivity(selectivity);
            stats.total_byte_size = stats
                .total_byte_size
                .with_estimated_selectivity(selectivity);
            return Ok(stats);
        }

        let num_rows = input_stats.num_rows;
        let total_byte_size = input_stats.total_byte_size;
        let input_analysis_ctx =
            AnalysisContext::try_from_statistics(schema, &input_stats.column_statistics)?;

        let analysis_ctx = analyze(predicate, input_analysis_ctx, schema)?;

        // Estimate (inexact) selectivity of predicate
        let selectivity = analysis_ctx.selectivity.unwrap_or(1.0);
        let num_rows = num_rows.with_estimated_selectivity(selectivity);
        let total_byte_size = total_byte_size.with_estimated_selectivity(selectivity);

        let column_statistics = collect_new_statistics(
            &input_stats.column_statistics,
            analysis_ctx.boundaries,
        );
        Ok(Statistics {
            num_rows,
            total_byte_size,
            column_statistics,
        })
    }

    fn extend_constants(
        input: &Arc<dyn ExecutionPlan>,
        predicate: &Arc<dyn PhysicalExpr>,
    ) -> Vec<ConstExpr> {
        let mut res_constants = Vec::new();
        let input_eqs = input.equivalence_properties();

        let conjunctions = split_conjunction(predicate);
        for conjunction in conjunctions {
            if let Some(binary) = conjunction.as_any().downcast_ref::<BinaryExpr>() {
                if binary.op() == &Operator::Eq {
                    // Filter evaluates to single value for all partitions
                    if input_eqs.is_expr_constant(binary.left()).is_some() {
                        let across = input_eqs
                            .is_expr_constant(binary.right())
                            .unwrap_or_default();
                        res_constants
                            .push(ConstExpr::new(Arc::clone(binary.right()), across));
                    } else if input_eqs.is_expr_constant(binary.right()).is_some() {
                        let across = input_eqs
                            .is_expr_constant(binary.left())
                            .unwrap_or_default();
                        res_constants
                            .push(ConstExpr::new(Arc::clone(binary.left()), across));
                    }
                }
            }
        }
        res_constants
    }
    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        predicate: &Arc<dyn PhysicalExpr>,
        default_selectivity: u8,
        projection: Option<&Vec<usize>>,
    ) -> Result<PlanProperties> {
        // Combine the equal predicates with the input equivalence properties
        // to construct the equivalence properties:
        let schema = input.schema();
        let stats = Self::statistics_helper(
            &schema,
            input.partition_statistics(None)?,
            predicate,
            default_selectivity,
        )?;
        let mut eq_properties = input.equivalence_properties().clone();
        let (equal_pairs, _) = collect_columns_from_predicate_inner(predicate);
        for (lhs, rhs) in equal_pairs {
            eq_properties.add_equal_conditions(Arc::clone(lhs), Arc::clone(rhs))?
        }
        // Add the columns that have only one viable value (singleton) after
        // filtering to constants.
        let constants = collect_columns(predicate)
            .into_iter()
            .filter(|column| stats.column_statistics[column.index()].is_singleton())
            .map(|column| {
                let value = stats.column_statistics[column.index()]
                    .min_value
                    .get_value();
                let expr = Arc::new(column) as _;
                ConstExpr::new(expr, AcrossPartitions::Uniform(value.cloned()))
            });
        // This is for statistics
        eq_properties.add_constants(constants)?;
        // This is for logical constant (for example: a = '1', then a could be marked as a constant)
        // to do: how to deal with multiple situation to represent = (for example c1 between 0 and 0)
        eq_properties.add_constants(Self::extend_constants(input, predicate))?;

        let mut output_partitioning = input.output_partitioning().clone();
        // If contains projection, update the PlanProperties.
        if let Some(projection) = projection {
            let schema = eq_properties.schema();
            let projection_mapping = ProjectionMapping::from_indices(projection, schema)?;
            let out_schema = project_schema(schema, Some(projection))?;
            output_partitioning =
                output_partitioning.project(&projection_mapping, &eq_properties);
            eq_properties = eq_properties.project(&projection_mapping, out_schema);
        }

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            input.pipeline_behavior(),
            input.boundedness(),
        ))
    }
}

impl DisplayAs for FilterExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_projections = if let Some(projection) =
                    self.projection.as_ref()
                {
                    format!(
                        ", projection=[{}]",
                        projection
                            .iter()
                            .map(|index| format!(
                                "{}@{}",
                                self.input.schema().fields().get(*index).unwrap().name(),
                                index
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else {
                    "".to_string()
                };
                let fetch = self
                    .fetch
                    .map_or_else(|| "".to_string(), |f| format!(", fetch={f}"));
                write!(
                    f,
                    "FilterExec: {}{}{}",
                    self.predicate, display_projections, fetch
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "predicate={}", fmt_sql(self.predicate.as_ref()))
            }
        }
    }
}

impl ExecutionPlan for FilterExec {
    fn name(&self) -> &'static str {
        "FilterExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // Tell optimizer this operator doesn't reorder its input
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        FilterExec::try_new(Arc::clone(&self.predicate), children.swap_remove(0))
            .and_then(|e| {
                let selectivity = e.default_selectivity();
                e.with_default_selectivity(selectivity)
            })
            .and_then(|e| e.with_projection(self.projection().cloned()))
            .map(|e| e.with_fetch(self.fetch).unwrap())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start FilterExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        let metrics = FilterExecMetrics::new(&self.metrics, partition);
        Ok(Box::pin(FilterExecStream {
            schema: self.schema(),
            predicate: Arc::clone(&self.predicate),
            input: self.input.execute(partition, context)?,
            metrics,
            projection: self.projection.clone(),
            batch_coalescer: LimitedBatchCoalescer::new(
                self.schema(),
                self.batch_size,
                self.fetch,
            ),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// The output statistics of a filtering operation can be estimated if the
    /// predicate's selectivity value can be determined for the incoming data.
    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        let input_stats = self.input.partition_statistics(partition)?;
        let schema = self.schema();
        let stats = Self::statistics_helper(
            &schema,
            input_stats,
            self.predicate(),
            self.default_selectivity,
        )?;
        Ok(stats.project(self.projection.as_ref()))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }

    /// Tries to swap `projection` with its input (`filter`). If possible, performs
    /// the swap and returns [`FilterExec`] as the top plan. Otherwise, returns `None`.
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // If the projection does not narrow the schema, we should not try to push it down:
        if projection.expr().len() < projection.input().schema().fields().len() {
            // Each column in the predicate expression must exist after the projection.
            if let Some(new_predicate) =
                update_expr(self.predicate(), projection.expr(), false)?
            {
                return FilterExec::try_new(
                    new_predicate,
                    make_with_child(projection, self.input())?,
                )
                .and_then(|e| {
                    let selectivity = self.default_selectivity();
                    e.with_default_selectivity(selectivity)
                })
                .map(|e| Some(Arc::new(e) as _));
            }
        }
        try_embed_projection(projection, self)
    }

    fn gather_filters_for_pushdown(
        &self,
        phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        if !matches!(phase, FilterPushdownPhase::Pre) {
            // For non-pre phase, filters pass through unchanged
            let filter_supports = parent_filters
                .into_iter()
                .map(PushedDownPredicate::supported)
                .collect();

            return Ok(FilterDescription::new().with_child(ChildFilterDescription {
                parent_filters: filter_supports,
                self_filters: vec![],
            }));
        }

        let child = ChildFilterDescription::from_child(&parent_filters, self.input())?
            .with_self_filters(
                split_conjunction(&self.predicate)
                    .into_iter()
                    .cloned()
                    .collect(),
            );

        Ok(FilterDescription::new().with_child(child))
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        if !matches!(phase, FilterPushdownPhase::Pre) {
            return Ok(FilterPushdownPropagation::if_all(child_pushdown_result));
        }
        // We absorb any parent filters that were not handled by our children
        let unsupported_parent_filters =
            child_pushdown_result.parent_filters.iter().filter_map(|f| {
                matches!(f.all(), PushedDown::No).then_some(Arc::clone(&f.filter))
            });
        let unsupported_self_filters = child_pushdown_result
            .self_filters
            .first()
            .expect("we have exactly one child")
            .iter()
            .filter_map(|f| match f.discriminant {
                PushedDown::Yes => None,
                PushedDown::No => Some(&f.predicate),
            })
            .cloned();

        let unhandled_filters = unsupported_parent_filters
            .into_iter()
            .chain(unsupported_self_filters)
            .collect_vec();

        // If we have unhandled filters, we need to create a new FilterExec
        let filter_input = Arc::clone(self.input());
        let new_predicate = conjunction(unhandled_filters);
        let updated_node = if new_predicate.eq(&lit(true)) {
            // FilterExec is no longer needed, but we may need to leave a projection in place
            match self.projection() {
                Some(projection_indices) => {
                    let filter_child_schema = filter_input.schema();
                    let proj_exprs = projection_indices
                        .iter()
                        .map(|p| {
                            let field = filter_child_schema.field(*p).clone();
                            ProjectionExpr {
                                expr: Arc::new(Column::new(field.name(), *p))
                                    as Arc<dyn PhysicalExpr>,
                                alias: field.name().to_string(),
                            }
                        })
                        .collect::<Vec<_>>();
                    Some(Arc::new(ProjectionExec::try_new(proj_exprs, filter_input)?)
                        as Arc<dyn ExecutionPlan>)
                }
                None => {
                    // No projection needed, just return the input
                    Some(filter_input)
                }
            }
        } else if new_predicate.eq(&self.predicate) {
            // The new predicate is the same as our current predicate
            None
        } else {
            // Create a new FilterExec with the new predicate
            let new = FilterExec {
                predicate: Arc::clone(&new_predicate),
                input: Arc::clone(&filter_input),
                metrics: self.metrics.clone(),
                default_selectivity: self.default_selectivity,
                cache: Self::compute_properties(
                    &filter_input,
                    &new_predicate,
                    self.default_selectivity,
                    self.projection.as_ref(),
                )?,
                projection: None,
                batch_size: self.batch_size,
                fetch: self.fetch,
            };
            Some(Arc::new(new) as _)
        };

        Ok(FilterPushdownPropagation {
            filters: vec![PushedDown::Yes; child_pushdown_result.parent_filters.len()],
            updated_node,
        })
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(Self {
            predicate: Arc::clone(&self.predicate),
            input: Arc::clone(&self.input),
            metrics: self.metrics.clone(),
            default_selectivity: self.default_selectivity,
            cache: self.cache.clone(),
            projection: self.projection.clone(),
            batch_size: self.batch_size,
            fetch,
        }))
    }
}

impl EmbeddedProjection for FilterExec {
    fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        self.with_projection(projection)
    }
}

/// This function ensures that all bounds in the `ExprBoundaries` vector are
/// converted to closed bounds. If a lower/upper bound is initially open, it
/// is adjusted by using the next/previous value for its data type to convert
/// it into a closed bound.
fn collect_new_statistics(
    input_column_stats: &[ColumnStatistics],
    analysis_boundaries: Vec<ExprBoundaries>,
) -> Vec<ColumnStatistics> {
    analysis_boundaries
        .into_iter()
        .enumerate()
        .map(
            |(
                idx,
                ExprBoundaries {
                    interval,
                    distinct_count,
                    ..
                },
            )| {
                let Some(interval) = interval else {
                    // If the interval is `None`, we can say that there are no rows:
                    return ColumnStatistics {
                        null_count: Precision::Exact(0),
                        max_value: Precision::Exact(ScalarValue::Null),
                        min_value: Precision::Exact(ScalarValue::Null),
                        sum_value: Precision::Exact(ScalarValue::Null),
                        distinct_count: Precision::Exact(0),
                    };
                };
                let (lower, upper) = interval.into_bounds();
                let (min_value, max_value) = if lower.eq(&upper) {
                    (Precision::Exact(lower), Precision::Exact(upper))
                } else {
                    (Precision::Inexact(lower), Precision::Inexact(upper))
                };
                ColumnStatistics {
                    null_count: input_column_stats[idx].null_count.to_inexact(),
                    max_value,
                    min_value,
                    sum_value: Precision::Absent,
                    distinct_count: distinct_count.to_inexact(),
                }
            },
        )
        .collect()
}

/// The FilterExec streams wraps the input iterator and applies the predicate expression to
/// determine which rows to include in its output batches
struct FilterExecStream {
    /// Output schema after the projection
    schema: SchemaRef,
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input partition to filter.
    input: SendableRecordBatchStream,
    /// Runtime metrics recording
    metrics: FilterExecMetrics,
    /// The projection indices of the columns in the input schema
    projection: Option<Vec<usize>>,
    /// Batch coalescer to combine small batches
    batch_coalescer: LimitedBatchCoalescer,
}

/// The metrics for `FilterExec`
struct FilterExecMetrics {
    // Common metrics for most operators
    baseline_metrics: BaselineMetrics,
    // Selectivity of the filter, calculated as output_rows / input_rows
    selectivity: RatioMetrics,
}

impl FilterExecMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            selectivity: MetricBuilder::new(metrics)
                .with_type(MetricType::SUMMARY)
                .ratio_metrics("selectivity", partition),
        }
    }
}

impl FilterExecStream {
    fn flush_remaining_batches(
        &mut self,
    ) -> Poll<Option<std::result::Result<RecordBatch, DataFusionError>>> {
        // Flush any remaining buffered batch
        match self.batch_coalescer.finish() {
            Ok(()) => {
                Poll::Ready(self.batch_coalescer.next_completed_batch().map(|batch| {
                    self.metrics.selectivity.add_part(batch.num_rows());
                    Ok(batch)
                }))
            }
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

pub fn batch_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
) -> Result<RecordBatch> {
    filter_and_project(batch, predicate, None)
}

fn filter_and_project(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
    projection: Option<&Vec<usize>>,
) -> Result<RecordBatch> {
    predicate
        .evaluate(batch)
        .and_then(|v| v.into_array(batch.num_rows()))
        .and_then(|array| {
            Ok(match (as_boolean_array(&array), projection) {
                // Apply filter array to record batch
                (Ok(filter_array), None) => filter_record_batch(batch, filter_array)?,
                (Ok(filter_array), Some(projection)) => {
                    let projected_batch = batch.project(projection)?;
                    filter_record_batch(&projected_batch, filter_array)?
                }
                (Err(_), _) => {
                    return internal_err!(
                        "Cannot create filter_array from non-boolean predicates"
                    );
                }
            })
        })
}

impl Stream for FilterExecStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll;
        let elapsed_compute = self.metrics.baseline_metrics.elapsed_compute().clone();
        loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let timer = elapsed_compute.timer();
                    let status = self.predicate.as_ref()
                        .evaluate(&batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                        .and_then(|array| {
                            Ok(match self.projection {
                                Some(ref projection) => {
                                    let projected_batch = batch.project(projection)?;
                                    (array, projected_batch)
                                },
                                None => (array, batch)
                            })
                        }).and_then(|(array, batch)| {
                            match as_boolean_array(&array) {
                                Ok(filter_array) => {
                                    self.metrics.selectivity.add_total(batch.num_rows());
                                    // TODO: support push_batch_with_filter in LimitedBatchCoalescer
                                    let batch = filter_record_batch(&batch, filter_array)?;
                                    let state = self.batch_coalescer.push_batch(batch)?;
                                    Ok(state)
                                }
                                Err(_) => {
                                    internal_err!(
                                        "Cannot create filter_array from non-boolean predicates"
                                    )
                                }
                            }
                        })?;
                    timer.done();

                    if let LimitReached = status {
                        poll = self.flush_remaining_batches();
                        break;
                    }

                    if let Some(batch) = self.batch_coalescer.next_completed_batch() {
                        self.metrics.selectivity.add_part(batch.num_rows());
                        poll = Poll::Ready(Some(Ok(batch)));
                        break;
                    }
                    continue;
                }
                None => {
                    // Flush any remaining buffered batch
                    match self.batch_coalescer.finish() {
                        Ok(()) => {
                            poll = self.flush_remaining_batches();
                        }
                        Err(e) => {
                            poll = Poll::Ready(Some(Err(e)));
                        }
                    }
                    break;
                }
                value => {
                    poll = Poll::Ready(value);
                    break;
                }
            }
        }
        self.metrics.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Same number of record batches
        self.input.size_hint()
    }
}
impl RecordBatchStream for FilterExecStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Return the equals Column-Pairs and Non-equals Column-Pairs
#[deprecated(
    since = "51.0.0",
    note = "This function will be internal in the future"
)]
pub fn collect_columns_from_predicate(
    predicate: &'_ Arc<dyn PhysicalExpr>,
) -> EqualAndNonEqual<'_> {
    collect_columns_from_predicate_inner(predicate)
}

fn collect_columns_from_predicate_inner(
    predicate: &'_ Arc<dyn PhysicalExpr>,
) -> EqualAndNonEqual<'_> {
    let mut eq_predicate_columns = Vec::<PhysicalExprPairRef>::new();
    let mut ne_predicate_columns = Vec::<PhysicalExprPairRef>::new();

    let predicates = split_conjunction(predicate);
    predicates.into_iter().for_each(|p| {
        if let Some(binary) = p.as_any().downcast_ref::<BinaryExpr>() {
            match binary.op() {
                Operator::Eq => {
                    eq_predicate_columns.push((binary.left(), binary.right()))
                }
                Operator::NotEq => {
                    ne_predicate_columns.push((binary.left(), binary.right()))
                }
                _ => {}
            }
        }
    });

    (eq_predicate_columns, ne_predicate_columns)
}

/// Pair of `Arc<dyn PhysicalExpr>`s
pub type PhysicalExprPairRef<'a> = (&'a Arc<dyn PhysicalExpr>, &'a Arc<dyn PhysicalExpr>);

/// The equals Column-Pairs and Non-equals Column-Pairs in the Predicates
pub type EqualAndNonEqual<'a> =
    (Vec<PhysicalExprPairRef<'a>>, Vec<PhysicalExprPairRef<'a>>);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::empty::EmptyExec;
    use crate::expressions::*;
    use crate::test;
    use crate::test::exec::StatisticsExec;
    use arrow::datatypes::{Field, Schema, UnionFields, UnionMode};
    use datafusion_common::ScalarValue;

    #[tokio::test]
    async fn collect_columns_predicates() -> Result<()> {
        let schema = test::aggr_test_schema();
        let predicate: Arc<dyn PhysicalExpr> = binary(
            binary(
                binary(col("c2", &schema)?, Operator::GtEq, lit(1u32), &schema)?,
                Operator::And,
                binary(col("c2", &schema)?, Operator::Eq, lit(4u32), &schema)?,
                &schema,
            )?,
            Operator::And,
            binary(
                binary(
                    col("c2", &schema)?,
                    Operator::Eq,
                    col("c9", &schema)?,
                    &schema,
                )?,
                Operator::And,
                binary(
                    col("c1", &schema)?,
                    Operator::NotEq,
                    col("c13", &schema)?,
                    &schema,
                )?,
                &schema,
            )?,
            &schema,
        )?;

        let (equal_pairs, ne_pairs) = collect_columns_from_predicate_inner(&predicate);
        assert_eq!(2, equal_pairs.len());
        assert!(equal_pairs[0].0.eq(&col("c2", &schema)?));
        assert!(equal_pairs[0].1.eq(&lit(4u32)));

        assert!(equal_pairs[1].0.eq(&col("c2", &schema)?));
        assert!(equal_pairs[1].1.eq(&col("c9", &schema)?));

        assert_eq!(1, ne_pairs.len());
        assert!(ne_pairs[0].0.eq(&col("c1", &schema)?));
        assert!(ne_pairs[0].1.eq(&col("c13", &schema)?));

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_basic_expr() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        let bytes_per_row = 4;
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(100 * bytes_per_row),
                column_statistics: vec![ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                    ..Default::default()
                }],
            },
            schema.clone(),
        ));

        // a <= 25
        let predicate: Arc<dyn PhysicalExpr> =
            binary(col("a", &schema)?, Operator::LtEq, lit(25i32), &schema)?;

        // WHERE a <= 25
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);

        let statistics = filter.partition_statistics(None)?;
        assert_eq!(statistics.num_rows, Precision::Inexact(25));
        assert_eq!(
            statistics.total_byte_size,
            Precision::Inexact(25 * bytes_per_row)
        );
        assert_eq!(
            statistics.column_statistics,
            vec![ColumnStatistics {
                min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(25))),
                ..Default::default()
            }]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_column_level_nested() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(100),
                column_statistics: vec![ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                    ..Default::default()
                }],
                total_byte_size: Precision::Absent,
            },
            schema.clone(),
        ));

        // WHERE a <= 25
        let sub_filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("a", &schema)?, Operator::LtEq, lit(25i32), &schema)?,
            input,
        )?);

        // Nested filters (two separate physical plans, instead of AND chain in the expr)
        // WHERE a >= 10
        // WHERE a <= 25
        let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("a", &schema)?, Operator::GtEq, lit(10i32), &schema)?,
            sub_filter,
        )?);

        let statistics = filter.partition_statistics(None)?;
        assert_eq!(statistics.num_rows, Precision::Inexact(16));
        assert_eq!(
            statistics.column_statistics,
            vec![ColumnStatistics {
                min_value: Precision::Inexact(ScalarValue::Int32(Some(10))),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(25))),
                ..Default::default()
            }]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_column_level_nested_multiple() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        //      b: min=1, max=50
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(100),
                column_statistics: vec![
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(50))),
                        ..Default::default()
                    },
                ],
                total_byte_size: Precision::Absent,
            },
            schema.clone(),
        ));

        // WHERE a <= 25
        let a_lte_25: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("a", &schema)?, Operator::LtEq, lit(25i32), &schema)?,
            input,
        )?);

        // WHERE b > 45
        let b_gt_5: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("b", &schema)?, Operator::Gt, lit(45i32), &schema)?,
            a_lte_25,
        )?);

        // WHERE a >= 10
        let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("a", &schema)?, Operator::GtEq, lit(10i32), &schema)?,
            b_gt_5,
        )?);
        let statistics = filter.partition_statistics(None)?;
        // On a uniform distribution, only fifteen rows will satisfy the
        // filter that 'a' proposed (a >= 10 AND a <= 25) (15/100) and only
        // 5 rows will satisfy the filter that 'b' proposed (b > 45) (5/50).
        //
        // Which would result with a selectivity of  '15/100 * 5/50' or 0.015
        // and that means about %1.5 of the all rows (rounded up to 2 rows).
        assert_eq!(statistics.num_rows, Precision::Inexact(2));
        assert_eq!(
            statistics.column_statistics,
            vec![
                ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(10))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(25))),
                    ..Default::default()
                },
                ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(46))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(50))),
                    ..Default::default()
                }
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_when_input_stats_missing() -> Result<()> {
        // Table:
        //      a: min=???, max=??? (missing)
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics::new_unknown(&schema),
            schema.clone(),
        ));

        // a <= 25
        let predicate: Arc<dyn PhysicalExpr> =
            binary(col("a", &schema)?, Operator::LtEq, lit(25i32), &schema)?;

        // WHERE a <= 25
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);

        let statistics = filter.partition_statistics(None)?;
        assert_eq!(statistics.num_rows, Precision::Absent);

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_multiple_columns() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        //      b: min=1, max=3
        //      c: min=1000.0  max=1100.0
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Float32, false),
        ]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(1000),
                total_byte_size: Precision::Inexact(4000),
                column_statistics: vec![
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(3))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Float32(Some(1000.0))),
                        max_value: Precision::Inexact(ScalarValue::Float32(Some(1100.0))),
                        ..Default::default()
                    },
                ],
            },
            schema,
        ));
        // WHERE a<=53 AND (b=3 AND (c<=1075.0 AND a>b))
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::LtEq,
                Arc::new(Literal::new(ScalarValue::Int32(Some(53)))),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("b", 1)),
                    Operator::Eq,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(3)))),
                )),
                Operator::And,
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 2)),
                        Operator::LtEq,
                        Arc::new(Literal::new(ScalarValue::Float32(Some(1075.0)))),
                    )),
                    Operator::And,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a", 0)),
                        Operator::Gt,
                        Arc::new(Column::new("b", 1)),
                    )),
                )),
            )),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let statistics = filter.partition_statistics(None)?;
        // 0.5 (from a) * 0.333333... (from b) * 0.798387... (from c) ≈ 0.1330...
        // num_rows after ceil => 133.0... => 134
        // total_byte_size after ceil => 532.0... => 533
        assert_eq!(statistics.num_rows, Precision::Inexact(134));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(533));
        let exp_col_stats = vec![
            ColumnStatistics {
                min_value: Precision::Inexact(ScalarValue::Int32(Some(4))),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(53))),
                ..Default::default()
            },
            ColumnStatistics {
                min_value: Precision::Inexact(ScalarValue::Int32(Some(3))),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(3))),
                ..Default::default()
            },
            ColumnStatistics {
                min_value: Precision::Inexact(ScalarValue::Float32(Some(1000.0))),
                max_value: Precision::Inexact(ScalarValue::Float32(Some(1075.0))),
                ..Default::default()
            },
        ];
        let _ = exp_col_stats
            .into_iter()
            .zip(statistics.column_statistics)
            .map(|(expected, actual)| {
                if let Some(val) = actual.min_value.get_value() {
                    if val.data_type().is_floating() {
                        // Windows rounds arithmetic operation results differently for floating point numbers.
                        // Therefore, we check if the actual values are in an epsilon range.
                        let actual_min = actual.min_value.get_value().unwrap();
                        let actual_max = actual.max_value.get_value().unwrap();
                        let expected_min = expected.min_value.get_value().unwrap();
                        let expected_max = expected.max_value.get_value().unwrap();
                        let eps = ScalarValue::Float32(Some(1e-6));

                        assert!(actual_min.sub(expected_min).unwrap() < eps);
                        assert!(actual_min.sub(expected_min).unwrap() < eps);

                        assert!(actual_max.sub(expected_max).unwrap() < eps);
                        assert!(actual_max.sub(expected_max).unwrap() < eps);
                    } else {
                        assert_eq!(actual, expected);
                    }
                } else {
                    assert_eq!(actual, expected);
                }
            });

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_full_selective() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        //      b: min=1, max=3
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(1000),
                total_byte_size: Precision::Inexact(4000),
                column_statistics: vec![
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(3))),
                        ..Default::default()
                    },
                ],
            },
            schema,
        ));
        // WHERE a<200 AND 1<=b
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Lt,
                Arc::new(Literal::new(ScalarValue::Int32(Some(200)))),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                Operator::LtEq,
                Arc::new(Column::new("b", 1)),
            )),
        ));
        // Since filter predicate passes all entries, statistics after filter shouldn't change.
        let expected = input.partition_statistics(None)?.column_statistics;
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let statistics = filter.partition_statistics(None)?;

        assert_eq!(statistics.num_rows, Precision::Inexact(1000));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(4000));
        assert_eq!(statistics.column_statistics, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_zero_selective() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        //      b: min=1, max=3
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(1000),
                total_byte_size: Precision::Inexact(4000),
                column_statistics: vec![
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(3))),
                        ..Default::default()
                    },
                ],
            },
            schema,
        ));
        // WHERE a>200 AND 1<=b
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Int32(Some(200)))),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                Operator::LtEq,
                Arc::new(Column::new("b", 1)),
            )),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let statistics = filter.partition_statistics(None)?;

        assert_eq!(statistics.num_rows, Precision::Inexact(0));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(0));
        assert_eq!(
            statistics.column_statistics,
            vec![
                ColumnStatistics {
                    min_value: Precision::Exact(ScalarValue::Null),
                    max_value: Precision::Exact(ScalarValue::Null),
                    sum_value: Precision::Exact(ScalarValue::Null),
                    distinct_count: Precision::Exact(0),
                    null_count: Precision::Exact(0),
                },
                ColumnStatistics {
                    min_value: Precision::Exact(ScalarValue::Null),
                    max_value: Precision::Exact(ScalarValue::Null),
                    sum_value: Precision::Exact(ScalarValue::Null),
                    distinct_count: Precision::Exact(0),
                    null_count: Precision::Exact(0),
                },
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_more_inputs() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(1000),
                total_byte_size: Precision::Inexact(4000),
                column_statistics: vec![
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                        ..Default::default()
                    },
                ],
            },
            schema,
        ));
        // WHERE a<50
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(50)))),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let statistics = filter.partition_statistics(None)?;

        assert_eq!(statistics.num_rows, Precision::Inexact(490));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(1960));
        assert_eq!(
            statistics.column_statistics,
            vec![
                ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(49))),
                    ..Default::default()
                },
                ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                    ..Default::default()
                },
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_input_statistics() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics::new_unknown(&schema),
            schema,
        ));
        // WHERE a <= 10 AND 0 <= a - 5
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::LtEq,
                Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
                Operator::LtEq,
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Minus,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(5)))),
                )),
            )),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let filter_statistics = filter.partition_statistics(None)?;

        let expected_filter_statistics = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Absent,
                min_value: Precision::Inexact(ScalarValue::Int32(Some(5))),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(10))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
            }],
        };

        assert_eq!(filter_statistics, expected_filter_statistics);

        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_with_constant_column() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics::new_unknown(&schema),
            schema,
        ));
        // WHERE a = 10
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let filter_statistics = filter.partition_statistics(None)?;
        // First column is "a", and it is a column with only one value after the filter.
        assert!(filter_statistics.column_statistics[0].is_singleton());

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_filter_selectivity() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics::new_unknown(&schema),
            schema,
        ));
        // WHERE a = 10
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        ));
        let filter = FilterExec::try_new(predicate, input)?;
        assert!(filter.with_default_selectivity(120).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_custom_filter_selectivity() -> Result<()> {
        // Need a decimal to trigger inexact selectivity
        let schema =
            Schema::new(vec![Field::new("a", DataType::Decimal128(2, 3), false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(1000),
                total_byte_size: Precision::Inexact(4000),
                column_statistics: vec![ColumnStatistics {
                    ..Default::default()
                }],
            },
            schema,
        ));
        // WHERE a = 10
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Decimal128(Some(10), 10, 10))),
        ));
        let filter = FilterExec::try_new(predicate, input)?;
        let statistics = filter.partition_statistics(None)?;
        assert_eq!(statistics.num_rows, Precision::Inexact(200));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(800));
        let filter = filter.with_default_selectivity(40)?;
        let statistics = filter.partition_statistics(None)?;
        assert_eq!(statistics.num_rows, Precision::Inexact(400));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(1600));
        Ok(())
    }

    #[test]
    fn test_equivalence_properties_union_type() -> Result<()> {
        let union_type = DataType::Union(
            UnionFields::new(
                vec![0, 1],
                vec![
                    Field::new("f1", DataType::Int32, true),
                    Field::new("f2", DataType::Utf8, true),
                ],
            ),
            UnionMode::Sparse,
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", union_type, true),
        ]));

        let exec = FilterExec::try_new(
            binary(
                binary(col("c1", &schema)?, Operator::GtEq, lit(1i32), &schema)?,
                Operator::And,
                binary(col("c1", &schema)?, Operator::LtEq, lit(4i32), &schema)?,
                &schema,
            )?,
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
        )?;

        exec.partition_statistics(None).unwrap();

        Ok(())
    }
}
