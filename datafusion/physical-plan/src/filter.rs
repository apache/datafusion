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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use datafusion_physical_expr::projection::{ProjectionRef, combine_projections};
use itertools::Itertools;

use super::{
    ColumnStatistics, DisplayAs, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use crate::check_if_same_properties;
use crate::coalesce::{LimitedBatchCoalescer, PushBatchStatus};
use crate::common::can_project;
use crate::execution_plan::CardinalityEffect;
use crate::filter_pushdown::{
    ChildFilterDescription, ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation, PushedDown,
};
use crate::limit::LocalLimitExec;
use crate::metrics::{MetricBuilder, MetricType};
use crate::projection::{
    EmbeddedProjection, ProjectionExec, ProjectionExpr, make_with_child,
    try_embed_projection, update_expr,
};
use crate::{
    DisplayFormatType, ExecutionPlan,
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RatioMetrics},
};

use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::config::ConfigOptions;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    DataFusionError, Result, ScalarValue, internal_err, plan_err, project_schema,
};
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::equivalence::ProjectionMapping;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal, lit};
use datafusion_physical_expr::intervals::utils::check_support;
use datafusion_physical_expr::utils::{collect_columns, reassign_expr_columns};
use datafusion_physical_expr::{
    AcrossPartitions, AnalysisContext, ConstExpr, ExprBoundaries, PhysicalExpr, analyze,
    conjunction, split_conjunction,
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
    cache: Arc<PlanProperties>,
    /// The projection indices of the columns in the output schema of join
    projection: Option<ProjectionRef>,
    /// Target batch size for output batches
    batch_size: usize,
    /// Number of rows to fetch
    fetch: Option<usize>,
}

/// Builder for [`FilterExec`] to set optional parameters
pub struct FilterExecBuilder {
    predicate: Arc<dyn PhysicalExpr>,
    input: Arc<dyn ExecutionPlan>,
    projection: Option<ProjectionRef>,
    default_selectivity: u8,
    batch_size: usize,
    fetch: Option<usize>,
}

impl FilterExecBuilder {
    /// Create a new builder with required parameters (predicate and input)
    pub fn new(predicate: Arc<dyn PhysicalExpr>, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            predicate,
            input,
            projection: None,
            default_selectivity: FILTER_EXEC_DEFAULT_SELECTIVITY,
            batch_size: FILTER_EXEC_DEFAULT_BATCH_SIZE,
            fetch: None,
        }
    }

    /// Set the input execution plan
    pub fn with_input(mut self, input: Arc<dyn ExecutionPlan>) -> Self {
        self.input = input;
        self
    }

    /// Set the predicate expression
    pub fn with_predicate(mut self, predicate: Arc<dyn PhysicalExpr>) -> Self {
        self.predicate = predicate;
        self
    }

    /// Set the projection, composing with any existing projection.
    ///
    /// If a projection is already set, the new projection indices are mapped
    /// through the existing projection. For example, if the current projection
    /// is `[0, 2, 3]` and `apply_projection(Some(vec![0, 2]))` is called, the
    /// resulting projection will be `[0, 3]` (indices 0 and 2 of `[0, 2, 3]`).
    ///
    /// If no projection is currently set, the new projection is used directly.
    /// If `None` is passed, the projection is cleared.
    pub fn apply_projection(self, projection: Option<Vec<usize>>) -> Result<Self> {
        let projection = projection.map(Into::into);
        self.apply_projection_by_ref(projection.as_ref())
    }

    /// The same as [`Self::apply_projection`] but takes projection shared reference.
    pub fn apply_projection_by_ref(
        mut self,
        projection: Option<&ProjectionRef>,
    ) -> Result<Self> {
        // Check if the projection is valid against current output schema
        can_project(&self.input.schema(), projection.map(AsRef::as_ref))?;
        self.projection = combine_projections(projection, self.projection.as_ref())?;
        Ok(self)
    }

    /// Set the default selectivity
    pub fn with_default_selectivity(mut self, default_selectivity: u8) -> Self {
        self.default_selectivity = default_selectivity;
        self
    }

    /// Set the batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the fetch limit
    pub fn with_fetch(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }

    /// Build the FilterExec, computing properties once with all configured parameters
    pub fn build(self) -> Result<FilterExec> {
        // Validate predicate type
        match self.predicate.data_type(self.input.schema().as_ref())? {
            DataType::Boolean => {}
            other => {
                return plan_err!(
                    "Filter predicate must return BOOLEAN values, got {other:?}"
                );
            }
        }

        // Validate selectivity
        if self.default_selectivity > 100 {
            return plan_err!(
                "Default filter selectivity value needs to be less than or equal to 100"
            );
        }

        // Validate projection if provided
        can_project(&self.input.schema(), self.projection.as_deref())?;

        // Compute properties once with all parameters
        let cache = FilterExec::compute_properties(
            &self.input,
            &self.predicate,
            self.default_selectivity,
            self.projection.as_deref(),
        )?;

        Ok(FilterExec {
            predicate: self.predicate,
            input: self.input,
            metrics: ExecutionPlanMetricsSet::new(),
            default_selectivity: self.default_selectivity,
            cache: Arc::new(cache),
            projection: self.projection,
            batch_size: self.batch_size,
            fetch: self.fetch,
        })
    }
}

impl From<&FilterExec> for FilterExecBuilder {
    fn from(exec: &FilterExec) -> Self {
        Self {
            predicate: Arc::clone(&exec.predicate),
            input: Arc::clone(&exec.input),
            projection: exec.projection.clone(),
            default_selectivity: exec.default_selectivity,
            batch_size: exec.batch_size,
            fetch: exec.fetch,
            // We could cache / copy over PlanProperties
            // here but that would require invalidating them in FilterExecBuilder::apply_projection, etc.
            // and currently every call to this method ends up invalidating them anyway.
            // If useful this can be added in the future as a non-breaking change.
        }
    }
}

impl FilterExec {
    /// Create a FilterExec on an input using the builder pattern
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        FilterExecBuilder::new(predicate, input).build()
    }

    /// Get a batch size
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Set the default selectivity
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
    ///
    /// # Deprecated
    /// Use [`FilterExecBuilder::apply_projection`] instead
    #[deprecated(
        since = "52.0.0",
        note = "Use FilterExecBuilder::apply_projection instead"
    )]
    pub fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        let builder = FilterExecBuilder::from(self);
        builder.apply_projection(projection)?.build()
    }

    /// Set the batch size
    pub fn with_batch_size(&self, batch_size: usize) -> Result<Self> {
        Ok(Self {
            predicate: Arc::clone(&self.predicate),
            input: Arc::clone(&self.input),
            metrics: self.metrics.clone(),
            default_selectivity: self.default_selectivity,
            cache: Arc::clone(&self.cache),
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
    pub fn projection(&self) -> &Option<ProjectionRef> {
        &self.projection
    }

    /// Calculates `Statistics` for `FilterExec`, by applying selectivity
    /// (either default, or estimated) to input statistics.
    ///
    /// Equality predicates (`col = literal`) set NDV to `Exact(1)`, or
    /// `Exact(0)` when the predicate is contradictory (e.g. `a = 1 AND a = 2`).
    pub(crate) fn statistics_helper(
        schema: &SchemaRef,
        input_stats: Statistics,
        predicate: &Arc<dyn PhysicalExpr>,
        default_selectivity: u8,
    ) -> Result<Statistics> {
        let (eq_columns, is_infeasible) = collect_equality_columns(predicate);

        let input_num_rows = input_stats.num_rows;
        let input_total_byte_size = input_stats.total_byte_size;

        let (selectivity, num_rows, column_statistics) = if is_infeasible {
            // Contradictory predicate: zero rows, and null/min/max are
            // undefined on an empty column.
            let mut cs = input_stats.to_inexact().column_statistics;
            for col_stat in &mut cs {
                col_stat.distinct_count = Precision::Exact(0);
                col_stat.null_count = Precision::Exact(0);
                col_stat.min_value = Precision::Absent;
                col_stat.max_value = Precision::Absent;
                col_stat.sum_value = Precision::Absent;
                col_stat.byte_size = Precision::Exact(0);
            }
            (0.0, Precision::Exact(0), cs)
        } else if !check_support(predicate, schema) {
            // Interval analysis is not applicable; fall back to the default
            // selectivity but still pin NDV=1 for every `col = literal` column.
            let selectivity = default_selectivity as f64 / 100.0;
            let mut cs = input_stats.to_inexact().column_statistics;
            for &idx in &eq_columns {
                if idx < cs.len() && cs[idx].distinct_count != Precision::Exact(0) {
                    cs[idx].distinct_count = Precision::Exact(1);
                }
            }
            (
                selectivity,
                input_num_rows.with_estimated_selectivity(selectivity),
                cs,
            )
        } else {
            // Interval-analysis path. `collect_new_statistics` already sets
            // distinct_count = Exact(1) when an interval collapses to a single
            // value, so no post-fix is needed here.
            let input_analysis_ctx = AnalysisContext::try_from_statistics(
                schema,
                &input_stats.column_statistics,
            )?;
            let analysis_ctx = analyze(predicate, input_analysis_ctx, schema)?;
            let selectivity = analysis_ctx.selectivity.unwrap_or(1.0);
            let filtered_num_rows =
                input_num_rows.with_estimated_selectivity(selectivity);
            let cs = collect_new_statistics(
                schema,
                &input_stats.column_statistics,
                analysis_ctx.boundaries,
                match &filtered_num_rows {
                    Precision::Absent => None,
                    p => Some(*p),
                },
            );
            (selectivity, filtered_num_rows, cs)
        };

        let total_byte_size =
            input_total_byte_size.with_estimated_selectivity(selectivity);

        Ok(Statistics {
            num_rows,
            total_byte_size,
            column_statistics,
        })
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        predicate: &Arc<dyn PhysicalExpr>,
        default_selectivity: u8,
        projection: Option<&[usize]>,
    ) -> Result<PlanProperties> {
        // Combine the equal predicates with the input equivalence properties
        // to construct the equivalence properties:
        let schema = input.schema();
        let stats = Self::statistics_helper(
            &schema,
            Arc::unwrap_or_clone(input.partition_statistics(None)?),
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
        eq_properties.add_constants(ConstExpr::collect_predicate_constants(
            input.equivalence_properties(),
            predicate,
        ))?;

        let mut output_partitioning = input.output_partitioning().clone();
        // If contains projection, update the PlanProperties.
        if let Some(projection) = projection {
            let schema = eq_properties.schema();
            let projection_mapping = ProjectionMapping::from_indices(projection, schema)?;
            let out_schema = project_schema(schema, Some(&projection))?;
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

    fn with_new_children_and_same_properties(
        &self,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        Self {
            input: children.swap_remove(0),
            metrics: ExecutionPlanMetricsSet::new(),
            ..Self::clone(self)
        }
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
                if let Some(fetch) = self.fetch {
                    writeln!(f, "fetch={fetch}")?;
                }
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
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        f(self.predicate.as_ref())
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // Tell optimizer this operator doesn't reorder its input
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        check_if_same_properties!(self, children);
        let new_input = children.swap_remove(0);
        FilterExecBuilder::from(&*self)
            .with_input(new_input)
            .build()
            .map(|e| Arc::new(e) as _)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start FilterExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );
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
    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        let input_stats =
            Arc::unwrap_or_clone(self.input.partition_statistics(partition)?);
        let stats = Self::statistics_helper(
            &self.input.schema(),
            input_stats,
            self.predicate(),
            self.default_selectivity,
        )?;
        Ok(Arc::new(stats.project(self.projection.as_ref())))
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
                return FilterExecBuilder::from(self)
                    .with_input(make_with_child(projection, self.input())?)
                    .with_predicate(new_predicate)
                    // The original FilterExec projection referenced columns from its old
                    // input. After the swap the new input is the ProjectionExec which
                    // already handles column selection, so clear the projection here.
                    .apply_projection(None)?
                    .build()
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
        if phase != FilterPushdownPhase::Pre {
            let child =
                ChildFilterDescription::from_child(&parent_filters, self.input())?;
            return Ok(FilterDescription::new().with_child(child));
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
        if phase != FilterPushdownPhase::Pre {
            return Ok(FilterPushdownPropagation::if_all(child_pushdown_result));
        }
        // We absorb any parent filters that were not handled by our children
        let mut unsupported_parent_filters: Vec<Arc<dyn PhysicalExpr>> =
            child_pushdown_result
                .parent_filters
                .iter()
                .filter_map(|f| {
                    matches!(f.all(), PushedDown::No).then_some(Arc::clone(&f.filter))
                })
                .collect();

        // If this FilterExec has a projection, the unsupported parent filters
        // are in the output schema (after projection) coordinates. We need to
        // remap them to the input schema coordinates before combining with self filters.
        if self.projection.is_some() {
            let input_schema = self.input().schema();
            unsupported_parent_filters = unsupported_parent_filters
                .into_iter()
                .map(|expr| reassign_expr_columns(expr, &input_schema))
                .collect::<Result<Vec<_>>>()?;
        }

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
            // FilterExec is no longer needed, but we may need to leave a projection in place.
            // If this FilterExec had a fetch limit, propagate it to the child.
            // When the child also has a fetch, use the minimum of both to preserve
            // the tighter constraint.
            let filter_input = if let Some(outer_fetch) = self.fetch {
                let effective_fetch = match filter_input.fetch() {
                    Some(inner_fetch) => outer_fetch.min(inner_fetch),
                    None => outer_fetch,
                };
                match filter_input.with_fetch(Some(effective_fetch)) {
                    Some(node) => node,
                    None => Arc::new(LocalLimitExec::new(filter_input, effective_fetch)),
                }
            } else {
                filter_input
            };
            match self.projection().as_ref() {
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
            // Create a new FilterExec with the new predicate, preserving the projection
            let new = FilterExec {
                predicate: Arc::clone(&new_predicate),
                input: Arc::clone(&filter_input),
                metrics: self.metrics.clone(),
                default_selectivity: self.default_selectivity,
                cache: Arc::new(Self::compute_properties(
                    &filter_input,
                    &new_predicate,
                    self.default_selectivity,
                    self.projection.as_deref(),
                )?),
                projection: self.projection.clone(),
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

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(Self {
            predicate: Arc::clone(&self.predicate),
            input: Arc::clone(&self.input),
            metrics: self.metrics.clone(),
            default_selectivity: self.default_selectivity,
            cache: Arc::clone(&self.cache),
            projection: self.projection.clone(),
            batch_size: self.batch_size,
            fetch,
        }))
    }

    fn with_preserve_order(
        &self,
        preserve_order: bool,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        self.input
            .with_preserve_order(preserve_order)
            .and_then(|new_input| {
                Arc::new(self.clone())
                    .with_new_children(vec![new_input])
                    .ok()
            })
    }
}

impl EmbeddedProjection for FilterExec {
    fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        FilterExecBuilder::from(self)
            .apply_projection(projection)?
            .build()
    }
}

/// Collects column equality information from `col = literal` predicates in a
/// conjunction.
///
/// Returns `(eq_columns, is_infeasible)`:
/// - `eq_columns`: set of column indices constrained to a single literal value.
/// - `is_infeasible`: `true` when the same column is equated to two different
///   non-null literals (e.g. `name = 'alice' AND name = 'bob'`), which is
///   always unsatisfiable.
///
/// Only AND conjunctions are traversed; OR is intentionally skipped
/// since `a = 1 OR a = 2` does not pin NDV to 1.
fn collect_equality_columns(predicate: &Arc<dyn PhysicalExpr>) -> (HashSet<usize>, bool) {
    let mut eq_values: HashMap<usize, ScalarValue> = HashMap::new();
    let mut infeasible = false;

    for expr in split_conjunction(predicate) {
        let Some(binary) = expr.downcast_ref::<BinaryExpr>() else {
            continue;
        };
        if *binary.op() != Operator::Eq {
            continue;
        }
        let left = binary.left();
        let right = binary.right();
        let pair = if let Some(col) = left.downcast_ref::<Column>()
            && let Some(lit) = right.downcast_ref::<Literal>()
            && !lit.value().is_null()
        {
            Some((col.index(), lit.value().clone()))
        } else if let Some(col) = right.downcast_ref::<Column>()
            && let Some(lit) = left.downcast_ref::<Literal>()
            && !lit.value().is_null()
        {
            Some((col.index(), lit.value().clone()))
        } else {
            None
        };

        if let Some((idx, value)) = pair {
            match eq_values.entry(idx) {
                Entry::Occupied(prev) => {
                    if *prev.get() != value {
                        infeasible = true;
                        break;
                    }
                }
                Entry::Vacant(slot) => {
                    slot.insert(value);
                }
            }
        }
    }

    (eq_values.into_keys().collect(), infeasible)
}

/// Converts an interval bound to a [`Precision`] value. NULL bounds (which
/// represent "unbounded" in the interval type) map to [`Precision::Absent`].
fn interval_bound_to_precision(
    bound: ScalarValue,
    is_exact: bool,
) -> Precision<ScalarValue> {
    if bound.is_null() {
        Precision::Absent
    } else if is_exact {
        Precision::Exact(bound)
    } else {
        Precision::Inexact(bound)
    }
}

/// This function ensures that all bounds in the `ExprBoundaries` vector are
/// converted to closed bounds. If a lower/upper bound is initially open, it
/// is adjusted by using the next/previous value for its data type to convert
/// it into a closed bound.
fn collect_new_statistics(
    schema: &SchemaRef,
    input_column_stats: &[ColumnStatistics],
    analysis_boundaries: Vec<ExprBoundaries>,
    filtered_num_rows: Option<Precision<usize>>,
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
                    // If the interval is `None`, we can say that there are no rows.
                    // Use a typed null to preserve the column's data type, so that
                    // downstream interval analysis can still intersect intervals
                    // of the same type.
                    let typed_null = ScalarValue::try_from(schema.field(idx).data_type())
                        .unwrap_or(ScalarValue::Null);
                    return ColumnStatistics {
                        null_count: Precision::Exact(0),
                        max_value: Precision::Exact(typed_null.clone()),
                        min_value: Precision::Exact(typed_null.clone()),
                        sum_value: Precision::Exact(typed_null),
                        distinct_count: Precision::Exact(0),
                        byte_size: Precision::Exact(0),
                    };
                };
                let (lower, upper) = interval.into_bounds();
                let is_single_value =
                    !lower.is_null() && !upper.is_null() && lower == upper;
                let min_value = interval_bound_to_precision(lower, is_single_value);
                let max_value = interval_bound_to_precision(upper, is_single_value);
                // When the interval collapses to a single value (equality
                // predicate), the column has exactly 1 distinct value.
                // Otherwise, cap NDV at the filtered row count.
                let capped_distinct_count = if is_single_value {
                    Precision::Exact(1)
                } else {
                    match filtered_num_rows {
                        Some(rows) => distinct_count.to_inexact().min(&rows),
                        None => distinct_count.to_inexact(),
                    }
                };
                ColumnStatistics {
                    null_count: input_column_stats[idx].null_count.to_inexact(),
                    max_value,
                    min_value,
                    sum_value: Precision::Absent,
                    distinct_count: capped_distinct_count,
                    byte_size: input_column_stats[idx].byte_size,
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
    projection: Option<ProjectionRef>,
    /// Batch coalescer to combine small batches
    batch_coalescer: LimitedBatchCoalescer,
}

/// The metrics for `FilterExec`
struct FilterExecMetrics {
    /// Common metrics for most operators
    baseline_metrics: BaselineMetrics,
    /// Selectivity of the filter, calculated as output_rows / input_rows
    selectivity: RatioMetrics,
    // Remember to update `docs/source/user-guide/metrics.md` when adding new metrics,
    // or modifying metrics comments
}

impl FilterExecMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            selectivity: MetricBuilder::new(metrics)
                .with_type(MetricType::Summary)
                .ratio_metrics("selectivity", partition),
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
        let elapsed_compute = self.metrics.baseline_metrics.elapsed_compute().clone();
        loop {
            // If there is a completed batch ready, return it
            if let Some(batch) = self.batch_coalescer.next_completed_batch() {
                self.metrics.selectivity.add_part(batch.num_rows());
                let poll = Poll::Ready(Some(Ok(batch)));
                return self.metrics.baseline_metrics.record_poll(poll);
            }

            if self.batch_coalescer.is_finished() {
                // If input is done and no batches are ready, return None to signal end of stream.
                return Poll::Ready(None);
            }

            // Attempt to pull the next batch from the input stream.
            match ready!(self.input.poll_next_unpin(cx)) {
                None => {
                    self.batch_coalescer.finish()?;
                    // continue draining the coalescer
                }
                Some(Ok(batch)) => {
                    let timer = elapsed_compute.timer();
                    let status = self.predicate.as_ref()
                        .evaluate(&batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                        .and_then(|array| {
                            Ok(match self.projection.as_ref()  {
                                Some(projection) => {
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

                    match status {
                        PushBatchStatus::Continue => {
                            // Keep pushing more batches
                        }
                        PushBatchStatus::LimitReached => {
                            // limit was reached, so stop early
                            self.batch_coalescer.finish()?;
                            // continue draining the coalescer
                        }
                    }
                }

                // Error case
                other => return Poll::Ready(other),
            }
        }
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
        if let Some(binary) = p.downcast_ref::<BinaryExpr>() {
            // Only extract pairs where at least one side is a Column reference.
            // Pairs like `complex_expr = literal` should not create equivalence
            // classes — the literal could appear in many unrelated expressions
            // (e.g. sort keys), and normalize_expr's deep traversal would
            // replace those occurrences with the complex expression, corrupting
            // sort orderings. Constant propagation for such pairs is handled
            // separately by `extend_constants`.
            let has_direct_column_operand =
                binary.left().downcast_ref::<Column>().is_some()
                    || binary.right().downcast_ref::<Column>().is_some();
            if !has_direct_column_operand {
                return;
            }
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
            .zip(statistics.column_statistics.clone())
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
        let expected = input.partition_statistics(None)?.column_statistics.clone();
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
                    min_value: Precision::Exact(ScalarValue::Int32(None)),
                    max_value: Precision::Exact(ScalarValue::Int32(None)),
                    sum_value: Precision::Exact(ScalarValue::Int32(None)),
                    distinct_count: Precision::Exact(0),
                    null_count: Precision::Exact(0),
                    byte_size: Precision::Exact(0),
                },
                ColumnStatistics {
                    min_value: Precision::Exact(ScalarValue::Int32(None)),
                    max_value: Precision::Exact(ScalarValue::Int32(None)),
                    sum_value: Precision::Exact(ScalarValue::Int32(None)),
                    distinct_count: Precision::Exact(0),
                    null_count: Precision::Exact(0),
                    byte_size: Precision::Exact(0),
                },
            ]
        );

        Ok(())
    }

    /// Regression test: stacking two FilterExecs where the inner filter
    /// proves zero selectivity should not panic with a type mismatch
    /// during interval intersection.
    ///
    /// Previously, when a filter proved no rows could match, the column
    /// statistics used untyped `ScalarValue::Null` (data type `Null`).
    /// If an outer FilterExec then tried to analyze its own predicate
    /// against those statistics, `Interval::intersect` would fail with:
    ///   "Only intervals with the same data type are intersectable, lhs:Null, rhs:Int32"
    #[tokio::test]
    async fn test_nested_filter_with_zero_selectivity_inner() -> Result<()> {
        // Inner table: a: [1, 100], b: [1, 3]
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

        // Inner filter: a > 200 (impossible given a max=100 → zero selectivity)
        let inner_predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(200)))),
        ));
        let inner_filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(inner_predicate, input)?);

        // Outer filter: a = 50
        // Before the fix, this would panic because the inner filter's
        // zero-selectivity statistics produced Null-typed intervals for
        // column `a`, which couldn't intersect with the Int32 literal.
        let outer_predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(50)))),
        ));
        let outer_filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(outer_predicate, inner_filter)?);

        // Should succeed without error
        let statistics = outer_filter.partition_statistics(None)?;
        assert_eq!(statistics.num_rows, Precision::Inexact(0));

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
                byte_size: Precision::Absent,
            }],
        };

        assert_eq!(*filter_statistics, expected_filter_statistics);

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
            UnionFields::try_new(
                vec![0, 1],
                vec![
                    Field::new("f1", DataType::Int32, true),
                    Field::new("f2", DataType::Utf8, true),
                ],
            )
            .unwrap(),
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

    #[tokio::test]
    async fn test_builder_with_projection() -> Result<()> {
        // Create a schema with multiple columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        // Create a filter predicate: a > 10
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        ));

        // Create filter with projection [0, 2] (columns a and c) using builder
        let projection = Some(vec![0, 2]);
        let filter = FilterExecBuilder::new(predicate, input)
            .apply_projection(projection.clone())
            .unwrap()
            .build()?;

        // Verify projection is set correctly
        assert_eq!(filter.projection(), &Some([0, 2].into()));

        // Verify schema contains only projected columns
        let output_schema = filter.schema();
        assert_eq!(output_schema.fields().len(), 2);
        assert_eq!(output_schema.field(0).name(), "a");
        assert_eq!(output_schema.field(1).name(), "c");

        Ok(())
    }

    #[tokio::test]
    async fn test_builder_without_projection() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(5)))),
        ));

        // Create filter without projection using builder
        let filter = FilterExecBuilder::new(predicate, input).build()?;

        // Verify no projection is set
        assert!(filter.projection().is_none());

        // Verify schema contains all columns
        let output_schema = filter.schema();
        assert_eq!(output_schema.fields().len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_builder_invalid_projection() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(5)))),
        ));

        // Try to create filter with invalid projection (index out of bounds) using builder
        let result =
            FilterExecBuilder::new(predicate, input).apply_projection(Some(vec![0, 5])); // 5 is out of bounds

        // Should return an error
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_builder_vs_with_projection() -> Result<()> {
        // This test verifies that the builder with projection produces the same result
        // as try_new().with_projection(), but more efficiently (one compute_properties call)
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int32, false),
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
                        ..Default::default()
                    },
                    ColumnStatistics {
                        ..Default::default()
                    },
                    ColumnStatistics {
                        ..Default::default()
                    },
                ],
            },
            schema,
        ));
        let input: Arc<dyn ExecutionPlan> = input;

        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(50)))),
        ));

        let projection = Some(vec![0, 2]);

        // Method 1: Builder with projection (one call to compute_properties)
        let filter1 = FilterExecBuilder::new(Arc::clone(&predicate), Arc::clone(&input))
            .apply_projection(projection.clone())
            .unwrap()
            .build()?;

        // Method 2: Also using builder for comparison (deprecated try_new().with_projection() removed)
        let filter2 = FilterExecBuilder::new(predicate, input)
            .apply_projection(projection)
            .unwrap()
            .build()?;

        // Both methods should produce equivalent results
        assert_eq!(filter1.schema(), filter2.schema());
        assert_eq!(filter1.projection(), filter2.projection());

        // Verify statistics are the same
        let stats1 = filter1.partition_statistics(None)?;
        let stats2 = filter2.partition_statistics(None)?;
        assert_eq!(stats1.num_rows, stats2.num_rows);
        assert_eq!(stats1.total_byte_size, stats2.total_byte_size);

        Ok(())
    }

    #[tokio::test]
    async fn test_builder_statistics_with_projection() -> Result<()> {
        // Test that statistics are correctly computed when using builder with projection
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(1000),
                total_byte_size: Precision::Inexact(12000),
                column_statistics: vec![
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(10))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(200))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(5))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(50))),
                        ..Default::default()
                    },
                ],
            },
            schema,
        ));

        // Filter: a < 50, Project: [0, 2]
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(50)))),
        ));

        let filter = FilterExecBuilder::new(predicate, input)
            .apply_projection(Some(vec![0, 2]))
            .unwrap()
            .build()?;

        let statistics = filter.partition_statistics(None)?;

        // Verify statistics reflect both filtering and projection
        assert!(matches!(statistics.num_rows, Precision::Inexact(_)));

        // Schema should only have 2 columns after projection
        assert_eq!(filter.schema().fields().len(), 2);

        Ok(())
    }

    #[test]
    fn test_builder_predicate_validation() -> Result<()> {
        // Test that builder validates predicate type correctly
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        // Create a predicate that doesn't return boolean (returns Int32)
        let invalid_predicate = Arc::new(Column::new("a", 0));

        // Should fail because predicate doesn't return boolean
        let result = FilterExecBuilder::new(invalid_predicate, input)
            .apply_projection(Some(vec![0]))
            .unwrap()
            .build();

        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_builder_projection_composition() -> Result<()> {
        // Test that calling apply_projection multiple times composes projections
        // If initial projection is [0, 2, 3] and we call apply_projection([0, 2]),
        // the result should be [0, 3] (indices 0 and 2 of [0, 2, 3])
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int32, false),
        ]));

        let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        // Create a filter predicate: a > 10
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        ));

        // First projection: [0, 2, 3] -> select columns a, c, d
        // Second projection: [0, 2] -> select indices 0 and 2 of [0, 2, 3] -> [0, 3]
        // Final result: columns a and d
        let filter = FilterExecBuilder::new(predicate, input)
            .apply_projection(Some(vec![0, 2, 3]))?
            .apply_projection(Some(vec![0, 2]))?
            .build()?;

        // Verify composed projection is [0, 3]
        assert_eq!(filter.projection(), &Some([0, 3].into()));

        // Verify schema contains only columns a and d
        let output_schema = filter.schema();
        assert_eq!(output_schema.fields().len(), 2);
        assert_eq!(output_schema.field(0).name(), "a");
        assert_eq!(output_schema.field(1).name(), "d");

        Ok(())
    }

    #[tokio::test]
    async fn test_builder_projection_composition_none_clears() -> Result<()> {
        // Test that passing None clears the projection
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        ));

        // Set a projection then clear it with None
        let filter = FilterExecBuilder::new(predicate, input)
            .apply_projection(Some(vec![0]))?
            .apply_projection(None)?
            .build()?;

        // Projection should be cleared
        assert_eq!(filter.projection(), &None);

        // Schema should have all columns
        let output_schema = filter.schema();
        assert_eq!(output_schema.fields().len(), 2);

        Ok(())
    }

    #[test]
    fn test_filter_with_projection_remaps_post_phase_parent_filters() -> Result<()> {
        // Test that FilterExec with a projection must remap parent dynamic
        // filter column indices from its output schema to the input schema
        // before passing them to the child.
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Float64, false),
        ]));
        let input = Arc::new(EmptyExec::new(Arc::clone(&input_schema)));

        // FilterExec: a > 0, projection=[c@2]
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        ));
        let filter = FilterExecBuilder::new(predicate, input)
            .apply_projection(Some(vec![2]))?
            .build()?;

        // Output schema should be [c:Float64]
        let output_schema = filter.schema();
        assert_eq!(output_schema.fields().len(), 1);
        assert_eq!(output_schema.field(0).name(), "c");

        // Simulate a parent dynamic filter referencing output column c@0
        let parent_filter: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c", 0));

        let config = ConfigOptions::new();
        let desc = filter.gather_filters_for_pushdown(
            FilterPushdownPhase::Post,
            vec![parent_filter],
            &config,
        )?;

        // The filter pushed to the child must reference c@2 (input schema),
        // not c@0 (output schema).
        let parent_filters = desc.parent_filters();
        assert_eq!(parent_filters.len(), 1); // one child
        assert_eq!(parent_filters[0].len(), 1); // one filter
        let remapped = &parent_filters[0][0].predicate;
        let display = format!("{remapped}");
        assert_eq!(
            display, "c@2",
            "Post-phase parent filter column index must be remapped \
             from output schema (c@0) to input schema (c@2)"
        );

        Ok(())
    }

    /// Regression test for https://github.com/apache/datafusion/issues/20194
    ///
    /// `collect_columns_from_predicate_inner` should only extract equality
    /// pairs where at least one side is a Column. Pairs like
    /// `complex_expr = literal` must not create equivalence classes because
    /// `normalize_expr`'s deep traversal would replace the literal inside
    /// unrelated expressions (e.g. sort keys) with the complex expression.
    #[test]
    fn test_collect_columns_skips_non_column_pairs() -> Result<()> {
        let schema = test::aggr_test_schema();

        // Simulate: nvl(c2, 0) = 0  →  (c2 IS DISTINCT FROM 0) = 0
        // Neither side is a Column, so this should NOT be extracted.
        let complex_expr: Arc<dyn PhysicalExpr> = binary(
            col("c2", &schema)?,
            Operator::IsDistinctFrom,
            lit(0u32),
            &schema,
        )?;
        let predicate: Arc<dyn PhysicalExpr> =
            binary(complex_expr, Operator::Eq, lit(0u32), &schema)?;

        let (equal_pairs, _) = collect_columns_from_predicate_inner(&predicate);
        assert_eq!(
            0,
            equal_pairs.len(),
            "Should not extract equality pairs where neither side is a Column"
        );

        // But col = literal should still be extracted
        let predicate: Arc<dyn PhysicalExpr> =
            binary(col("c2", &schema)?, Operator::Eq, lit(0u32), &schema)?;
        let (equal_pairs, _) = collect_columns_from_predicate_inner(&predicate);
        assert_eq!(
            1,
            equal_pairs.len(),
            "Should extract equality pairs where one side is a Column"
        );

        Ok(())
    }

    /// Columns with Absent min/max statistics should remain Absent after
    /// FilterExec.
    #[tokio::test]
    async fn test_filter_statistics_absent_columns_stay_absent() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(1000),
                total_byte_size: Precision::Absent,
                column_statistics: vec![
                    ColumnStatistics::default(),
                    ColumnStatistics::default(),
                ],
            },
            schema.clone(),
        ));

        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);

        let statistics = filter.partition_statistics(None)?;
        let col_b_stats = &statistics.column_statistics[1];
        assert_eq!(col_b_stats.min_value, Precision::Absent);
        assert_eq!(col_b_stats.max_value, Precision::Absent);

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_equality_ndv() -> Result<()> {
        #[expect(clippy::type_complexity)]
        let cases: Vec<(
            &str,
            Vec<Field>,
            Vec<ColumnStatistics>,
            Arc<dyn PhysicalExpr>,
            Vec<Precision<usize>>,
        )> = vec![
            (
                "utf8 equality",
                vec![Field::new("name", DataType::Utf8, false)],
                vec![ColumnStatistics {
                    distinct_count: Precision::Inexact(50),
                    ..Default::default()
                }],
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("name", 0)),
                    Operator::Eq,
                    Arc::new(Literal::new(ScalarValue::Utf8(Some("hello".to_string())))),
                )),
                vec![Precision::Exact(1)],
            ),
            (
                "utf8view equality",
                vec![Field::new("name", DataType::Utf8View, false)],
                vec![ColumnStatistics {
                    distinct_count: Precision::Inexact(50),
                    ..Default::default()
                }],
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("name", 0)),
                    Operator::Eq,
                    Arc::new(Literal::new(ScalarValue::Utf8View(Some(
                        "hello".to_string(),
                    )))),
                )),
                vec![Precision::Exact(1)],
            ),
            (
                "largeutf8 equality",
                vec![Field::new("name", DataType::LargeUtf8, false)],
                vec![ColumnStatistics {
                    distinct_count: Precision::Inexact(50),
                    ..Default::default()
                }],
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("name", 0)),
                    Operator::Eq,
                    Arc::new(Literal::new(ScalarValue::LargeUtf8(Some(
                        "hello".to_string(),
                    )))),
                )),
                vec![Precision::Exact(1)],
            ),
            (
                "utf8 reversed (literal = column)",
                vec![Field::new("name", DataType::Utf8, false)],
                vec![ColumnStatistics {
                    distinct_count: Precision::Inexact(50),
                    ..Default::default()
                }],
                Arc::new(BinaryExpr::new(
                    Arc::new(Literal::new(ScalarValue::Utf8(Some("hello".to_string())))),
                    Operator::Eq,
                    Arc::new(Column::new("name", 0)),
                )),
                vec![Precision::Exact(1)],
            ),
            (
                "OR preserves original NDV",
                vec![Field::new("name", DataType::Utf8, false)],
                vec![ColumnStatistics {
                    distinct_count: Precision::Inexact(50),
                    ..Default::default()
                }],
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("name", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Utf8(Some("a".to_string())))),
                    )),
                    Operator::Or,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("name", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Utf8(Some("b".to_string())))),
                    )),
                )),
                vec![Precision::Inexact(50)],
            ),
            (
                "AND with mixed types (Utf8 + Int32)",
                vec![
                    Field::new("name", DataType::Utf8, false),
                    Field::new("age", DataType::Int32, false),
                ],
                vec![
                    ColumnStatistics {
                        distinct_count: Precision::Inexact(50),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        distinct_count: Precision::Inexact(80),
                        ..Default::default()
                    },
                ],
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("name", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Utf8(Some(
                            "hello".to_string(),
                        )))),
                    )),
                    Operator::And,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("age", 1)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
                    )),
                )),
                vec![Precision::Exact(1), Precision::Exact(1)],
            ),
            (
                "numeric equality with min/max bounds (interval analysis path)",
                vec![Field::new("a", DataType::Int32, false)],
                vec![ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                    distinct_count: Precision::Inexact(80),
                    ..Default::default()
                }],
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Eq,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
                )),
                vec![Precision::Exact(1)],
            ),
            (
                "timestamp equality",
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                    false,
                )],
                vec![ColumnStatistics {
                    distinct_count: Precision::Inexact(500),
                    ..Default::default()
                }],
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("ts", 0)),
                    Operator::Eq,
                    Arc::new(Literal::new(ScalarValue::TimestampNanosecond(
                        Some(1_609_459_200_000_000_000),
                        None,
                    ))),
                )),
                vec![Precision::Exact(1)],
            ),
            (
                "contradictory numeric equality (infeasible)",
                vec![Field::new("a", DataType::Int32, false)],
                vec![ColumnStatistics {
                    distinct_count: Precision::Inexact(50),
                    ..Default::default()
                }],
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
                    )),
                    Operator::And,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(99)))),
                    )),
                )),
                vec![Precision::Exact(0)],
            ),
            (
                "utf8 equality with absent input NDV",
                vec![Field::new("name", DataType::Utf8, false)],
                vec![ColumnStatistics {
                    distinct_count: Precision::Absent,
                    ..Default::default()
                }],
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("name", 0)),
                    Operator::Eq,
                    Arc::new(Literal::new(ScalarValue::Utf8(Some("hello".to_string())))),
                )),
                vec![Precision::Exact(1)],
            ),
            (
                "contradictory utf8 equality (infeasible)",
                vec![Field::new("name", DataType::Utf8, false)],
                vec![ColumnStatistics {
                    distinct_count: Precision::Inexact(100),
                    ..Default::default()
                }],
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("name", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Utf8(Some(
                            "alice".to_string(),
                        )))),
                    )),
                    Operator::And,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("name", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Utf8(Some(
                            "bob".to_string(),
                        )))),
                    )),
                )),
                vec![Precision::Exact(0)],
            ),
            (
                "redundant same-value equality combined with another column",
                vec![
                    Field::new("a", DataType::Int32, false),
                    Field::new("b", DataType::Int32, false),
                ],
                vec![
                    ColumnStatistics {
                        distinct_count: Precision::Inexact(80),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        distinct_count: Precision::Inexact(40),
                        ..Default::default()
                    },
                ],
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("a", 0)),
                            Operator::Eq,
                            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                        )),
                        Operator::And,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("a", 0)),
                            Operator::Eq,
                            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                        )),
                    )),
                    Operator::And,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("b", 1)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
                    )),
                )),
                vec![Precision::Exact(1), Precision::Exact(1)],
            ),
        ];

        for (desc, fields, col_stats, predicate, expected_ndvs) in cases {
            let schema = Schema::new(fields);
            let input = Arc::new(StatisticsExec::new(
                Statistics {
                    num_rows: Precision::Inexact(100),
                    total_byte_size: Precision::Inexact(1000),
                    column_statistics: col_stats,
                },
                schema.clone(),
            ));
            let filter: Arc<dyn ExecutionPlan> =
                Arc::new(FilterExec::try_new(predicate, input)?);
            let statistics = filter.partition_statistics(None)?;

            for (i, expected) in expected_ndvs.iter().enumerate() {
                assert_eq!(
                    statistics.column_statistics[i].distinct_count, *expected,
                    "case '{desc}': column {i} NDV mismatch"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_and_equality_ndv() -> Result<()> {
        // a: min=1, max=100, ndv=80
        // b: min=1, max=50, ndv=40
        // c: min=1, max=200, ndv=150
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(1200),
                column_statistics: vec![
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                        distinct_count: Precision::Inexact(80),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(50))),
                        distinct_count: Precision::Inexact(40),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(200))),
                        distinct_count: Precision::Inexact(150),
                        ..Default::default()
                    },
                ],
            },
            schema.clone(),
        ));

        // a = 42 AND b > 10 AND c = 7
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Eq,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
                )),
                Operator::And,
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("b", 1)),
                    Operator::Gt,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
                )),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("c", 2)),
                Operator::Eq,
                Arc::new(Literal::new(ScalarValue::Int32(Some(7)))),
            )),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let statistics = filter.partition_statistics(None)?;
        // a = 42 collapses to single value
        assert_eq!(
            statistics.column_statistics[0].distinct_count,
            Precision::Exact(1)
        );
        // b > 10 narrows to [11, 50] but doesn't collapse to a single value.
        // The combined selectivity of a=42 (1/80) and c=7 (1/150) on 100 rows
        // computes num_rows = 1, so NDV is capped at the row count: min(40, 1) = 1.
        assert_eq!(
            statistics.column_statistics[1].distinct_count,
            Precision::Inexact(1)
        );
        // c = 7 collapses to single value
        assert_eq!(
            statistics.column_statistics[2].distinct_count,
            Precision::Exact(1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_equality_absent_bounds_ndv() -> Result<()> {
        // a: ndv=80, no min/max
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(400),
                column_statistics: vec![ColumnStatistics {
                    distinct_count: Precision::Inexact(80),
                    ..Default::default()
                }],
            },
            schema.clone(),
        ));

        // a = 42: even without known bounds, interval analysis resolves
        // the equality to [42, 42], so NDV is correctly set to Exact(1)
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let statistics = filter.partition_statistics(None)?;
        assert_eq!(
            statistics.column_statistics[0].distinct_count,
            Precision::Exact(1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_equality_int8_ndv() -> Result<()> {
        // a: min=-100, max=100, ndv=50
        let schema = Schema::new(vec![Field::new("a", DataType::Int8, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(100),
                column_statistics: vec![ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int8(Some(-100))),
                    max_value: Precision::Inexact(ScalarValue::Int8(Some(100))),
                    distinct_count: Precision::Inexact(50),
                    ..Default::default()
                }],
            },
            schema.clone(),
        ));

        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int8(Some(42)))),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let statistics = filter.partition_statistics(None)?;
        assert_eq!(
            statistics.column_statistics[0].distinct_count,
            Precision::Exact(1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_equality_int64_ndv() -> Result<()> {
        // a: min=0, max=1_000_000, ndv=100_000
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(100_000),
                total_byte_size: Precision::Inexact(800_000),
                column_statistics: vec![ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int64(Some(0))),
                    max_value: Precision::Inexact(ScalarValue::Int64(Some(1_000_000))),
                    distinct_count: Precision::Inexact(100_000),
                    ..Default::default()
                }],
            },
            schema.clone(),
        ));

        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int64(Some(42)))),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let statistics = filter.partition_statistics(None)?;
        assert_eq!(
            statistics.column_statistics[0].distinct_count,
            Precision::Exact(1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_equality_float32_ndv() -> Result<()> {
        // a: min=0.0, max=100.0, ndv=50
        let schema = Schema::new(vec![Field::new("a", DataType::Float32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(400),
                column_statistics: vec![ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Float32(Some(0.0))),
                    max_value: Precision::Inexact(ScalarValue::Float32(Some(100.0))),
                    distinct_count: Precision::Inexact(50),
                    ..Default::default()
                }],
            },
            schema.clone(),
        ));

        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Float32(Some(42.5)))),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let statistics = filter.partition_statistics(None)?;
        assert_eq!(
            statistics.column_statistics[0].distinct_count,
            Precision::Exact(1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_equality_reversed_ndv() -> Result<()> {
        // a: min=1, max=100, ndv=80
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(400),
                column_statistics: vec![ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                    distinct_count: Precision::Inexact(80),
                    ..Default::default()
                }],
            },
            schema.clone(),
        ));

        // 42 = a (literal on the left)
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
            Operator::Eq,
            Arc::new(Column::new("a", 0)),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let statistics = filter.partition_statistics(None)?;
        assert_eq!(
            statistics.column_statistics[0].distinct_count,
            Precision::Exact(1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_equality_timestamp_ndv() -> Result<()> {
        // ts: min=1_000_000_000, max=2_000_000_000, ndv=500
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        )]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(1000),
                total_byte_size: Precision::Inexact(8000),
                column_statistics: vec![ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::TimestampNanosecond(
                        Some(1_000_000_000),
                        None,
                    )),
                    max_value: Precision::Inexact(ScalarValue::TimestampNanosecond(
                        Some(2_000_000_000),
                        None,
                    )),
                    distinct_count: Precision::Inexact(500),
                    ..Default::default()
                }],
            },
            schema.clone(),
        ));

        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("ts", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::TimestampNanosecond(
                Some(1_500_000_000),
                None,
            ))),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let statistics = filter.partition_statistics(None)?;
        assert_eq!(
            statistics.column_statistics[0].distinct_count,
            Precision::Exact(1)
        );
        Ok(())
    }

    #[test]
    fn test_collect_equality_columns() {
        use std::collections::HashSet;
        // (description, predicate, expected_column_indices, expected_infeasible)
        #[expect(clippy::type_complexity)]
        let cases: Vec<(&str, Arc<dyn PhysicalExpr>, Vec<usize>, bool)> = vec![
            (
                "simple col = literal",
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Eq,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
                )),
                vec![0],
                false,
            ),
            (
                "reversed literal = col",
                Arc::new(BinaryExpr::new(
                    Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
                    Operator::Eq,
                    Arc::new(Column::new("a", 0)),
                )),
                vec![0],
                false,
            ),
            (
                "AND with two equalities",
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
                    )),
                    Operator::And,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("b", 1)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Utf8(Some(
                            "hello".to_string(),
                        )))),
                    )),
                )),
                vec![0, 1],
                false,
            ),
            (
                "OR produces empty set",
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
                    )),
                    Operator::Or,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(99)))),
                    )),
                )),
                vec![],
                false,
            ),
            (
                "greater-than produces empty set",
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Gt,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
                )),
                vec![],
                false,
            ),
            (
                "col = col produces empty set",
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Eq,
                    Arc::new(Column::new("b", 1)),
                )),
                vec![],
                false,
            ),
            (
                "nested AND with three equalities",
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("a", 0)),
                            Operator::Eq,
                            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                        )),
                        Operator::And,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("b", 1)),
                            Operator::Eq,
                            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
                        )),
                    )),
                    Operator::And,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 2)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(3)))),
                    )),
                )),
                vec![0, 1, 2],
                false,
            ),
            (
                "AND with mixed equality and non-equality",
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
                    )),
                    Operator::And,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("b", 1)),
                        Operator::Gt,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
                    )),
                )),
                vec![0],
                false,
            ),
            (
                "col = NULL is excluded",
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Eq,
                    Arc::new(Literal::new(ScalarValue::Int32(None))),
                )),
                vec![],
                false,
            ),
            (
                "NULL = col is excluded",
                Arc::new(BinaryExpr::new(
                    Arc::new(Literal::new(ScalarValue::Utf8(None))),
                    Operator::Eq,
                    Arc::new(Column::new("a", 0)),
                )),
                vec![],
                false,
            ),
            (
                "contradictory: same col, different literals",
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Utf8(Some(
                            "alice".to_string(),
                        )))),
                    )),
                    Operator::And,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Utf8(Some(
                            "bob".to_string(),
                        )))),
                    )),
                )),
                vec![0],
                true,
            ),
            (
                "same col, same literal is not contradictory",
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
                    )),
                    Operator::And,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
                    )),
                )),
                vec![0],
                false,
            ),
        ];

        for (desc, expr, expected_cols, expected_infeasible) in cases {
            let (result, infeasible) = collect_equality_columns(&expr);
            let expected: HashSet<usize> = expected_cols.into_iter().collect();
            if expected_infeasible {
                // When infeasible, the scan is short-circuited, so we only
                // assert the infeasibility flag — the partial column set
                // contents are an implementation detail.
                assert!(infeasible, "case '{desc}': expected infeasible");
            } else {
                assert_eq!(result, expected, "case '{desc}': columns mismatch");
                assert!(!infeasible, "case '{desc}': expected feasible");
            }
        }
    }

    /// Regression test: ProjectionExec on top of a FilterExec that already has
    /// an explicit projection must not panic when `try_swapping_with_projection`
    /// attempts to swap the two nodes.
    ///
    /// Before the fix, `FilterExecBuilder::from(self)` copied the old projection
    /// (e.g. `[0, 1, 2]`) from the FilterExec. After `.with_input` replaced the
    /// input with the narrower ProjectionExec (2 columns), `.build()` tried to
    /// validate the stale `[0, 1, 2]` projection against the 2-column schema and
    /// panicked with "project index 2 out of bounds, max field 2".
    #[test]
    fn test_filter_with_projection_swap_does_not_panic() -> Result<()> {
        use crate::projection::ProjectionExpr;
        use datafusion_physical_expr::expressions::col;

        // Schema: [ts: Int64, tokens: Int64, svc: Utf8]
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("tokens", DataType::Int64, false),
            Field::new("svc", DataType::Utf8, false),
        ]));
        let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        // FilterExec: ts > 0, projection=[ts@0, tokens@1, svc@2] (all 3 cols)
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("ts", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
        ));
        let filter = Arc::new(
            FilterExecBuilder::new(predicate, input)
                .apply_projection(Some(vec![0, 1, 2]))?
                .build()?,
        );

        // ProjectionExec: narrows to [ts, tokens] (drops svc)
        let proj_exprs = vec![
            ProjectionExpr {
                expr: col("ts", &filter.schema())?,
                alias: "ts".to_string(),
            },
            ProjectionExpr {
                expr: col("tokens", &filter.schema())?,
                alias: "tokens".to_string(),
            },
        ];
        let projection = Arc::new(ProjectionExec::try_new(
            proj_exprs,
            Arc::clone(&filter) as _,
        )?);

        // This must not panic
        let result = filter.try_swapping_with_projection(&projection)?;
        assert!(result.is_some(), "swap should succeed");

        let new_plan = result.unwrap();
        // Output schema must still be [ts, tokens]
        let out_schema = new_plan.schema();
        assert_eq!(out_schema.fields().len(), 2);
        assert_eq!(out_schema.field(0).name(), "ts");
        assert_eq!(out_schema.field(1).name(), "tokens");
        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_ndv_capped_at_row_count() -> Result<()> {
        // Table: a: min=1, max=100, distinct_count=80, 100 rows
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(400),
                column_statistics: vec![ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                    distinct_count: Precision::Inexact(80),
                    ..Default::default()
                }],
            },
            schema.clone(),
        ));

        // a <= 10 => ~10 rows out of 100
        let predicate: Arc<dyn PhysicalExpr> =
            binary(col("a", &schema)?, Operator::LtEq, lit(10i32), &schema)?;

        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);

        let statistics = filter.partition_statistics(None)?;
        // Filter estimates ~10 rows (selectivity = 10/100)
        assert_eq!(statistics.num_rows, Precision::Inexact(10));
        // NDV should be capped at the filtered row count (10), not the original 80
        let ndv = &statistics.column_statistics[0].distinct_count;
        assert!(
            ndv.get_value().copied() <= Some(10),
            "Expected NDV <= 10 (filtered row count), got {ndv:?}"
        );
        Ok(())
    }
}
