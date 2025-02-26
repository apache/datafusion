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

//! Defines the projection execution plan. A projection determines which columns or expressions
//! are returned from a query. The SQL statement `SELECT a, b, a+b FROM t1` is an example
//! of a projection on table `t1` where the expressions `a`, `b`, and `a+b` are the
//! projection expressions. `SELECT` without `FROM` will only evaluate expressions.

use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::expressions::{CastExpr, Column, Literal};
use super::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use super::{
    DisplayAs, ExecutionPlanProperties, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use crate::execution_plan::CardinalityEffect;
use crate::joins::utils::{ColumnIndex, JoinFilter};
use crate::{ColumnStatistics, DisplayFormatType, ExecutionPlan, PhysicalExpr};

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{internal_err, JoinSide, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::ProjectionMapping;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::PhysicalExprRef;

use futures::stream::{Stream, StreamExt};
use itertools::Itertools;
use log::trace;

/// Execution plan for a projection
#[derive(Debug, Clone)]
pub struct ProjectionExec {
    /// The projection expressions stored as tuples of (expression, output column name)
    pub(crate) expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// The schema once the projection has been applied to the input
    schema: SchemaRef,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl ProjectionExec {
    /// Create a projection on an input
    pub fn try_new(
        expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        let fields: Result<Vec<Field>> = expr
            .iter()
            .map(|(e, name)| {
                let mut field = Field::new(
                    name,
                    e.data_type(&input_schema)?,
                    e.nullable(&input_schema)?,
                );
                field.set_metadata(
                    get_field_metadata(e, &input_schema).unwrap_or_default(),
                );

                Ok(field)
            })
            .collect();

        let schema = Arc::new(Schema::new_with_metadata(
            fields?,
            input_schema.metadata().clone(),
        ));

        // Construct a map from the input expressions to the output expression of the Projection
        let projection_mapping = ProjectionMapping::try_new(&expr, &input_schema)?;
        let cache =
            Self::compute_properties(&input, &projection_mapping, Arc::clone(&schema))?;
        Ok(Self {
            expr,
            schema,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    /// The projection expressions stored as tuples of (expression, output column name)
    pub fn expr(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.expr
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        projection_mapping: &ProjectionMapping,
        schema: SchemaRef,
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties:
        let mut input_eq_properties = input.equivalence_properties().clone();
        input_eq_properties.substitute_oeq_class(projection_mapping)?;
        let eq_properties = input_eq_properties.project(projection_mapping, schema);

        // Calculate output partitioning, which needs to respect aliases:
        let input_partition = input.output_partitioning();
        let output_partitioning =
            input_partition.project(projection_mapping, &input_eq_properties);

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            input.pipeline_behavior(),
            input.boundedness(),
        ))
    }
}

impl DisplayAs for ProjectionExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let expr: Vec<String> = self
                    .expr
                    .iter()
                    .map(|(e, alias)| {
                        let e = e.to_string();
                        if &e != alias {
                            format!("{e} as {alias}")
                        } else {
                            e
                        }
                    })
                    .collect();

                write!(f, "ProjectionExec: expr=[{}]", expr.join(", "))
            }
        }
    }
}

impl ExecutionPlan for ProjectionExec {
    fn name(&self) -> &'static str {
        "ProjectionExec"
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
        ProjectionExec::try_new(self.expr.clone(), children.swap_remove(0))
            .map(|p| Arc::new(p) as _)
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        let all_simple_exprs = self
            .expr
            .iter()
            .all(|(e, _)| e.as_any().is::<Column>() || e.as_any().is::<Literal>());
        // If expressions are all either column_expr or Literal, then all computations in this projection are reorder or rename,
        // and projection would not benefit from the repartition, benefits_from_input_partitioning will return false.
        vec![!all_simple_exprs]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start ProjectionExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        Ok(Box::pin(ProjectionStream {
            schema: Arc::clone(&self.schema),
            expr: self.expr.iter().map(|x| Arc::clone(&x.0)).collect(),
            input: self.input.execute(partition, context)?,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(stats_projection(
            self.input.statistics()?,
            self.expr.iter().map(|(e, _)| Arc::clone(e)),
            Arc::clone(&self.schema),
        ))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let maybe_unified = try_unifying_projections(projection, self)?;
        if let Some(new_plan) = maybe_unified {
            // To unify 3 or more sequential projections:
            remove_unnecessary_projections(new_plan).data().map(Some)
        } else {
            Ok(Some(Arc::new(projection.clone())))
        }
    }
}

/// If 'e' is a direct column reference, returns the field level
/// metadata for that field, if any. Otherwise returns None
pub(crate) fn get_field_metadata(
    e: &Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Option<HashMap<String, String>> {
    if let Some(cast) = e.as_any().downcast_ref::<CastExpr>() {
        return get_field_metadata(cast.expr(), input_schema);
    }

    // Look up field by index in schema (not NAME as there can be more than one
    // column with the same name)
    e.as_any()
        .downcast_ref::<Column>()
        .map(|column| input_schema.field(column.index()).metadata())
        .cloned()
}

fn stats_projection(
    mut stats: Statistics,
    exprs: impl Iterator<Item = Arc<dyn PhysicalExpr>>,
    schema: SchemaRef,
) -> Statistics {
    let mut primitive_row_size = 0;
    let mut primitive_row_size_possible = true;
    let mut column_statistics = vec![];
    for expr in exprs {
        let col_stats = if let Some(col) = expr.as_any().downcast_ref::<Column>() {
            stats.column_statistics[col.index()].clone()
        } else {
            // TODO stats: estimate more statistics from expressions
            // (expressions should compute their statistics themselves)
            ColumnStatistics::new_unknown()
        };
        column_statistics.push(col_stats);
        if let Ok(data_type) = expr.data_type(&schema) {
            if let Some(value) = data_type.primitive_width() {
                primitive_row_size += value;
                continue;
            }
        }
        primitive_row_size_possible = false;
    }

    if primitive_row_size_possible {
        stats.total_byte_size =
            Precision::Exact(primitive_row_size).multiply(&stats.num_rows);
    }
    stats.column_statistics = column_statistics;
    stats
}

impl ProjectionStream {
    fn batch_project(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        // Records time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();
        let arrays = self
            .expr
            .iter()
            .map(|expr| {
                expr.evaluate(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;

        if arrays.is_empty() {
            let options =
                RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(Arc::clone(&self.schema), arrays, &options)
                .map_err(Into::into)
        } else {
            RecordBatch::try_new(Arc::clone(&self.schema), arrays).map_err(Into::into)
        }
    }
}

/// Projection iterator
struct ProjectionStream {
    schema: SchemaRef,
    expr: Vec<Arc<dyn PhysicalExpr>>,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
}

impl Stream for ProjectionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => Some(self.batch_project(&batch)),
            other => other,
        });

        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for ProjectionStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

pub trait EmbeddedProjection: ExecutionPlan + Sized {
    fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self>;
}

/// Some projection can't be pushed down left input or right input of hash join because filter or on need may need some columns that won't be used in later.
/// By embed those projection to hash join, we can reduce the cost of build_batch_from_indices in hash join (build_batch_from_indices need to can compute::take() for each column) and avoid unnecessary output creation.
pub fn try_embed_projection<Exec: EmbeddedProjection + 'static>(
    projection: &ProjectionExec,
    execution_plan: &Exec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Collect all column indices from the given projection expressions.
    let projection_index = collect_column_indices(projection.expr());

    if projection_index.is_empty() {
        return Ok(None);
    };

    // If the projection indices is the same as the input columns, we don't need to embed the projection to hash join.
    // Check the projection_index is 0..n-1 and the length of projection_index is the same as the length of execution_plan schema fields.
    if projection_index.len() == projection_index.last().unwrap() + 1
        && projection_index.len() == execution_plan.schema().fields().len()
    {
        return Ok(None);
    }

    let new_execution_plan =
        Arc::new(execution_plan.with_projection(Some(projection_index.to_vec()))?);

    // Build projection expressions for update_expr. Zip the projection_index with the new_execution_plan output schema fields.
    let embed_project_exprs = projection_index
        .iter()
        .zip(new_execution_plan.schema().fields())
        .map(|(index, field)| {
            (
                Arc::new(Column::new(field.name(), *index)) as Arc<dyn PhysicalExpr>,
                field.name().to_owned(),
            )
        })
        .collect::<Vec<_>>();

    let mut new_projection_exprs = Vec::with_capacity(projection.expr().len());

    for (expr, alias) in projection.expr() {
        // update column index for projection expression since the input schema has been changed.
        let Some(expr) = update_expr(expr, embed_project_exprs.as_slice(), false)? else {
            return Ok(None);
        };
        new_projection_exprs.push((expr, alias.clone()));
    }
    // Old projection may contain some alias or expression such as `a + 1` and `CAST('true' AS BOOLEAN)`, but our projection_exprs in hash join just contain column, so we need to create the new projection to keep the original projection.
    let new_projection = Arc::new(ProjectionExec::try_new(
        new_projection_exprs,
        Arc::clone(&new_execution_plan) as _,
    )?);
    if is_projection_removable(&new_projection) {
        Ok(Some(new_execution_plan))
    } else {
        Ok(Some(new_projection))
    }
}

/// The on clause of the join, as vector of (left, right) columns.
pub type JoinOn = Vec<(PhysicalExprRef, PhysicalExprRef)>;
/// Reference for JoinOn.
pub type JoinOnRef<'a> = &'a [(PhysicalExprRef, PhysicalExprRef)];

pub struct JoinData {
    pub projected_left_child: ProjectionExec,
    pub projected_right_child: ProjectionExec,
    pub join_filter: Option<JoinFilter>,
    pub join_on: JoinOn,
}

pub fn try_pushdown_through_join(
    projection: &ProjectionExec,
    join_left: &Arc<dyn ExecutionPlan>,
    join_right: &Arc<dyn ExecutionPlan>,
    join_on: JoinOnRef,
    schema: SchemaRef,
    filter: Option<&JoinFilter>,
) -> Result<Option<JoinData>> {
    // Convert projected expressions to columns. We can not proceed if this is not possible.
    let Some(projection_as_columns) = physical_to_column_exprs(projection.expr()) else {
        return Ok(None);
    };

    let (far_right_left_col_ind, far_left_right_col_ind) =
        join_table_borders(join_left.schema().fields().len(), &projection_as_columns);

    if !join_allows_pushdown(
        &projection_as_columns,
        &schema,
        far_right_left_col_ind,
        far_left_right_col_ind,
    ) {
        return Ok(None);
    }

    let new_filter = if let Some(filter) = filter {
        match update_join_filter(
            &projection_as_columns[0..=far_right_left_col_ind as _],
            &projection_as_columns[far_left_right_col_ind as _..],
            filter,
            join_left.schema().fields().len(),
        ) {
            Some(updated_filter) => Some(updated_filter),
            None => return Ok(None),
        }
    } else {
        None
    };

    let Some(new_on) = update_join_on(
        &projection_as_columns[0..=far_right_left_col_ind as _],
        &projection_as_columns[far_left_right_col_ind as _..],
        join_on,
        join_left.schema().fields().len(),
    ) else {
        return Ok(None);
    };

    let (new_left, new_right) = new_join_children(
        &projection_as_columns,
        far_right_left_col_ind,
        far_left_right_col_ind,
        join_left,
        join_right,
    )?;

    Ok(Some(JoinData {
        projected_left_child: new_left,
        projected_right_child: new_right,
        join_filter: new_filter,
        join_on: new_on,
    }))
}

/// This function checks if `plan` is a [`ProjectionExec`], and inspects its
/// input(s) to test whether it can push `plan` under its input(s). This function
/// will operate on the entire tree and may ultimately remove `plan` entirely
/// by leveraging source providers with built-in projection capabilities.
pub fn remove_unnecessary_projections(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let maybe_modified =
        if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
            // If the projection does not cause any change on the input, we can
            // safely remove it:
            if is_projection_removable(projection) {
                return Ok(Transformed::yes(Arc::clone(projection.input())));
            }
            // If it does, check if we can push it under its child(ren):
            projection
                .input()
                .try_swapping_with_projection(projection)?
        } else {
            return Ok(Transformed::no(plan));
        };
    Ok(maybe_modified.map_or(Transformed::no(plan), Transformed::yes))
}

/// Compare the inputs and outputs of the projection. All expressions must be
/// columns without alias, and projection does not change the order of fields.
/// For example, if the input schema is `a, b`, `SELECT a, b` is removable,
/// but `SELECT b, a` and `SELECT a+1, b` and `SELECT a AS c, b` are not.
fn is_projection_removable(projection: &ProjectionExec) -> bool {
    let exprs = projection.expr();
    exprs.iter().enumerate().all(|(idx, (expr, alias))| {
        let Some(col) = expr.as_any().downcast_ref::<Column>() else {
            return false;
        };
        col.name() == alias && col.index() == idx
    }) && exprs.len() == projection.input().schema().fields().len()
}

/// Given the expression set of a projection, checks if the projection causes
/// any renaming or constructs a non-`Column` physical expression.
pub fn all_alias_free_columns(exprs: &[(Arc<dyn PhysicalExpr>, String)]) -> bool {
    exprs.iter().all(|(expr, alias)| {
        expr.as_any()
            .downcast_ref::<Column>()
            .map(|column| column.name() == alias)
            .unwrap_or(false)
    })
}

/// Updates a source provider's projected columns according to the given
/// projection operator's expressions. To use this function safely, one must
/// ensure that all expressions are `Column` expressions without aliases.
pub fn new_projections_for_columns(
    projection: &ProjectionExec,
    source: &[usize],
) -> Vec<usize> {
    projection
        .expr()
        .iter()
        .filter_map(|(expr, _)| {
            expr.as_any()
                .downcast_ref::<Column>()
                .map(|expr| source[expr.index()])
        })
        .collect()
}

/// Creates a new [`ProjectionExec`] instance with the given child plan and
/// projected expressions.
pub fn make_with_child(
    projection: &ProjectionExec,
    child: &Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    ProjectionExec::try_new(projection.expr().to_vec(), Arc::clone(child))
        .map(|e| Arc::new(e) as _)
}

/// Returns `true` if all the expressions in the argument are `Column`s.
pub fn all_columns(exprs: &[(Arc<dyn PhysicalExpr>, String)]) -> bool {
    exprs.iter().all(|(expr, _)| expr.as_any().is::<Column>())
}

/// The function operates in two modes:
///
/// 1) When `sync_with_child` is `true`:
///
///    The function updates the indices of `expr` if the expression resides
///    in the input plan. For instance, given the expressions `a@1 + b@2`
///    and `c@0` with the input schema `c@2, a@0, b@1`, the expressions are
///    updated to `a@0 + b@1` and `c@2`.
///
/// 2) When `sync_with_child` is `false`:
///
///    The function determines how the expression would be updated if a projection
///    was placed before the plan associated with the expression. If the expression
///    cannot be rewritten after the projection, it returns `None`. For example,
///    given the expressions `c@0`, `a@1` and `b@2`, and the [`ProjectionExec`] with
///    an output schema of `a, c_new`, then `c@0` becomes `c_new@1`, `a@1` becomes
///    `a@0`, but `b@2` results in `None` since the projection does not include `b`.
pub fn update_expr(
    expr: &Arc<dyn PhysicalExpr>,
    projected_exprs: &[(Arc<dyn PhysicalExpr>, String)],
    sync_with_child: bool,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    #[derive(Debug, PartialEq)]
    enum RewriteState {
        /// The expression is unchanged.
        Unchanged,
        /// Some part of the expression has been rewritten
        RewrittenValid,
        /// Some part of the expression has been rewritten, but some column
        /// references could not be.
        RewrittenInvalid,
    }

    let mut state = RewriteState::Unchanged;

    let new_expr = Arc::clone(expr)
        .transform_up(|expr: Arc<dyn PhysicalExpr>| {
            if state == RewriteState::RewrittenInvalid {
                return Ok(Transformed::no(expr));
            }

            let Some(column) = expr.as_any().downcast_ref::<Column>() else {
                return Ok(Transformed::no(expr));
            };
            if sync_with_child {
                state = RewriteState::RewrittenValid;
                // Update the index of `column`:
                Ok(Transformed::yes(Arc::clone(
                    &projected_exprs[column.index()].0,
                )))
            } else {
                // default to invalid, in case we can't find the relevant column
                state = RewriteState::RewrittenInvalid;
                // Determine how to update `column` to accommodate `projected_exprs`
                projected_exprs
                    .iter()
                    .enumerate()
                    .find_map(|(index, (projected_expr, alias))| {
                        projected_expr.as_any().downcast_ref::<Column>().and_then(
                            |projected_column| {
                                (column.name().eq(projected_column.name())
                                    && column.index() == projected_column.index())
                                .then(|| {
                                    state = RewriteState::RewrittenValid;
                                    Arc::new(Column::new(alias, index)) as _
                                })
                            },
                        )
                    })
                    .map_or_else(
                        || Ok(Transformed::no(expr)),
                        |c| Ok(Transformed::yes(c)),
                    )
            }
        })
        .data();

    new_expr.map(|e| (state == RewriteState::RewrittenValid).then_some(e))
}

/// Downcasts all the expressions in `exprs` to `Column`s. If any of the given
/// expressions is not a `Column`, returns `None`.
pub fn physical_to_column_exprs(
    exprs: &[(Arc<dyn PhysicalExpr>, String)],
) -> Option<Vec<(Column, String)>> {
    exprs
        .iter()
        .map(|(expr, alias)| {
            expr.as_any()
                .downcast_ref::<Column>()
                .map(|col| (col.clone(), alias.clone()))
        })
        .collect()
}

/// If pushing down the projection over this join's children seems possible,
/// this function constructs the new [`ProjectionExec`]s that will come on top
/// of the original children of the join.
pub fn new_join_children(
    projection_as_columns: &[(Column, String)],
    far_right_left_col_ind: i32,
    far_left_right_col_ind: i32,
    left_child: &Arc<dyn ExecutionPlan>,
    right_child: &Arc<dyn ExecutionPlan>,
) -> Result<(ProjectionExec, ProjectionExec)> {
    let new_left = ProjectionExec::try_new(
        projection_as_columns[0..=far_right_left_col_ind as _]
            .iter()
            .map(|(col, alias)| {
                (
                    Arc::new(Column::new(col.name(), col.index())) as _,
                    alias.clone(),
                )
            })
            .collect_vec(),
        Arc::clone(left_child),
    )?;
    let left_size = left_child.schema().fields().len() as i32;
    let new_right = ProjectionExec::try_new(
        projection_as_columns[far_left_right_col_ind as _..]
            .iter()
            .map(|(col, alias)| {
                (
                    Arc::new(Column::new(
                        col.name(),
                        // Align projected expressions coming from the right
                        // table with the new right child projection:
                        (col.index() as i32 - left_size) as _,
                    )) as _,
                    alias.clone(),
                )
            })
            .collect_vec(),
        Arc::clone(right_child),
    )?;

    Ok((new_left, new_right))
}

/// Checks three conditions for pushing a projection down through a join:
/// - Projection must narrow the join output schema.
/// - Columns coming from left/right tables must be collected at the left/right
///   sides of the output table.
/// - Left or right table is not lost after the projection.
pub fn join_allows_pushdown(
    projection_as_columns: &[(Column, String)],
    join_schema: &SchemaRef,
    far_right_left_col_ind: i32,
    far_left_right_col_ind: i32,
) -> bool {
    // Projection must narrow the join output:
    projection_as_columns.len() < join_schema.fields().len()
    // Are the columns from different tables mixed?
    && (far_right_left_col_ind + 1 == far_left_right_col_ind)
    // Left or right table is not lost after the projection.
    && far_right_left_col_ind >= 0
    && far_left_right_col_ind < projection_as_columns.len() as i32
}

/// Returns the last index before encountering a column coming from the right table when traveling
/// through the projection from left to right, and the last index before encountering a column
/// coming from the left table when traveling through the projection from right to left.
/// If there is no column in the projection coming from the left side, it returns (-1, ...),
/// if there is no column in the projection coming from the right side, it returns (..., projection length).
pub fn join_table_borders(
    left_table_column_count: usize,
    projection_as_columns: &[(Column, String)],
) -> (i32, i32) {
    let far_right_left_col_ind = projection_as_columns
        .iter()
        .enumerate()
        .take_while(|(_, (projection_column, _))| {
            projection_column.index() < left_table_column_count
        })
        .last()
        .map(|(index, _)| index as i32)
        .unwrap_or(-1);

    let far_left_right_col_ind = projection_as_columns
        .iter()
        .enumerate()
        .rev()
        .take_while(|(_, (projection_column, _))| {
            projection_column.index() >= left_table_column_count
        })
        .last()
        .map(|(index, _)| index as i32)
        .unwrap_or(projection_as_columns.len() as i32);

    (far_right_left_col_ind, far_left_right_col_ind)
}

/// Tries to update the equi-join `Column`'s of a join as if the input of
/// the join was replaced by a projection.
pub fn update_join_on(
    proj_left_exprs: &[(Column, String)],
    proj_right_exprs: &[(Column, String)],
    hash_join_on: &[(PhysicalExprRef, PhysicalExprRef)],
    left_field_size: usize,
) -> Option<Vec<(PhysicalExprRef, PhysicalExprRef)>> {
    // TODO: Clippy wants the "map" call removed, but doing so generates
    //       a compilation error. Remove the clippy directive once this
    //       issue is fixed.
    #[allow(clippy::map_identity)]
    let (left_idx, right_idx): (Vec<_>, Vec<_>) = hash_join_on
        .iter()
        .map(|(left, right)| (left, right))
        .unzip();

    let new_left_columns = new_columns_for_join_on(&left_idx, proj_left_exprs, 0);
    let new_right_columns =
        new_columns_for_join_on(&right_idx, proj_right_exprs, left_field_size);

    match (new_left_columns, new_right_columns) {
        (Some(left), Some(right)) => Some(left.into_iter().zip(right).collect()),
        _ => None,
    }
}

/// Tries to update the column indices of a [`JoinFilter`] as if the input of
/// the join was replaced by a projection.
pub fn update_join_filter(
    projection_left_exprs: &[(Column, String)],
    projection_right_exprs: &[(Column, String)],
    join_filter: &JoinFilter,
    left_field_size: usize,
) -> Option<JoinFilter> {
    let mut new_left_indices = new_indices_for_join_filter(
        join_filter,
        JoinSide::Left,
        projection_left_exprs,
        0,
    )
    .into_iter();
    let mut new_right_indices = new_indices_for_join_filter(
        join_filter,
        JoinSide::Right,
        projection_right_exprs,
        left_field_size,
    )
    .into_iter();

    // Check if all columns match:
    (new_right_indices.len() + new_left_indices.len()
        == join_filter.column_indices().len())
    .then(|| {
        JoinFilter::new(
            Arc::clone(join_filter.expression()),
            join_filter
                .column_indices()
                .iter()
                .map(|col_idx| ColumnIndex {
                    index: if col_idx.side == JoinSide::Left {
                        new_left_indices.next().unwrap()
                    } else {
                        new_right_indices.next().unwrap()
                    },
                    side: col_idx.side,
                })
                .collect(),
            Arc::clone(join_filter.schema()),
        )
    })
}

/// Unifies `projection` with its input (which is also a [`ProjectionExec`]).
fn try_unifying_projections(
    projection: &ProjectionExec,
    child: &ProjectionExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let mut projected_exprs = vec![];
    let mut column_ref_map: HashMap<Column, usize> = HashMap::new();

    // Collect the column references usage in the outer projection.
    projection.expr().iter().for_each(|(expr, _)| {
        expr.apply(|expr| {
            Ok({
                if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                    *column_ref_map.entry(column.clone()).or_default() += 1;
                }
                TreeNodeRecursion::Continue
            })
        })
        .unwrap();
    });
    // Merging these projections is not beneficial, e.g
    // If an expression is not trivial and it is referred more than 1, unifies projections will be
    // beneficial as caching mechanism for non-trivial computations.
    // See discussion in: https://github.com/apache/datafusion/issues/8296
    if column_ref_map.iter().any(|(column, count)| {
        *count > 1 && !is_expr_trivial(&Arc::clone(&child.expr()[column.index()].0))
    }) {
        return Ok(None);
    }
    for (expr, alias) in projection.expr() {
        // If there is no match in the input projection, we cannot unify these
        // projections. This case will arise if the projection expression contains
        // a `PhysicalExpr` variant `update_expr` doesn't support.
        let Some(expr) = update_expr(expr, child.expr(), true)? else {
            return Ok(None);
        };
        projected_exprs.push((expr, alias.clone()));
    }
    ProjectionExec::try_new(projected_exprs, Arc::clone(child.input()))
        .map(|e| Some(Arc::new(e) as _))
}

/// Collect all column indices from the given projection expressions.
fn collect_column_indices(exprs: &[(Arc<dyn PhysicalExpr>, String)]) -> Vec<usize> {
    // Collect indices and remove duplicates.
    let mut indices = exprs
        .iter()
        .flat_map(|(expr, _)| collect_columns(expr))
        .map(|x| x.index())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    indices.sort();
    indices
}

/// This function determines and returns a vector of indices representing the
/// positions of columns in `projection_exprs` that are involved in `join_filter`,
/// and correspond to a particular side (`join_side`) of the join operation.
///
/// Notes: Column indices in the projection expressions are based on the join schema,
/// whereas the join filter is based on the join child schema. `column_index_offset`
/// represents the offset between them.
fn new_indices_for_join_filter(
    join_filter: &JoinFilter,
    join_side: JoinSide,
    projection_exprs: &[(Column, String)],
    column_index_offset: usize,
) -> Vec<usize> {
    join_filter
        .column_indices()
        .iter()
        .filter(|col_idx| col_idx.side == join_side)
        .filter_map(|col_idx| {
            projection_exprs
                .iter()
                .position(|(col, _)| col_idx.index + column_index_offset == col.index())
        })
        .collect()
}

/// This function generates a new set of columns to be used in a hash join
/// operation based on a set of equi-join conditions (`hash_join_on`) and a
/// list of projection expressions (`projection_exprs`).
///
/// Notes: Column indices in the projection expressions are based on the join schema,
/// whereas the join on expressions are based on the join child schema. `column_index_offset`
/// represents the offset between them.
fn new_columns_for_join_on(
    hash_join_on: &[&PhysicalExprRef],
    projection_exprs: &[(Column, String)],
    column_index_offset: usize,
) -> Option<Vec<PhysicalExprRef>> {
    let new_columns = hash_join_on
        .iter()
        .filter_map(|on| {
            // Rewrite all columns in `on`
            Arc::clone(*on)
                .transform(|expr| {
                    if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                        // Find the column in the projection expressions
                        let new_column = projection_exprs
                            .iter()
                            .enumerate()
                            .find(|(_, (proj_column, _))| {
                                column.name() == proj_column.name()
                                    && column.index() + column_index_offset
                                        == proj_column.index()
                            })
                            .map(|(index, (_, alias))| Column::new(alias, index));
                        if let Some(new_column) = new_column {
                            Ok(Transformed::yes(Arc::new(new_column)))
                        } else {
                            // If the column is not found in the projection expressions,
                            // it means that the column is not projected. In this case,
                            // we cannot push the projection down.
                            internal_err!(
                                "Column {:?} not found in projection expressions",
                                column
                            )
                        }
                    } else {
                        Ok(Transformed::no(expr))
                    }
                })
                .data()
                .ok()
        })
        .collect::<Vec<_>>();
    (new_columns.len() == hash_join_on.len()).then_some(new_columns)
}

/// Checks if the given expression is trivial.
/// An expression is considered trivial if it is either a `Column` or a `Literal`.
fn is_expr_trivial(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.as_any().downcast_ref::<Column>().is_some()
        || expr.as_any().downcast_ref::<Literal>().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::common::collect;
    use crate::test;

    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;

    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};

    #[test]
    fn test_collect_column_indices() -> Result<()> {
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("b", 7)),
            Operator::Minus,
            Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                Operator::Plus,
                Arc::new(Column::new("a", 1)),
            )),
        ));
        let column_indices = collect_column_indices(&[(expr, "b-(1+a)".to_string())]);
        assert_eq!(column_indices, vec![1, 7]);
        Ok(())
    }

    #[test]
    fn test_join_table_borders() -> Result<()> {
        let projections = vec![
            (Column::new("b", 1), "b".to_owned()),
            (Column::new("c", 2), "c".to_owned()),
            (Column::new("e", 4), "e".to_owned()),
            (Column::new("d", 3), "d".to_owned()),
            (Column::new("c", 2), "c".to_owned()),
            (Column::new("f", 5), "f".to_owned()),
            (Column::new("h", 7), "h".to_owned()),
            (Column::new("g", 6), "g".to_owned()),
        ];
        let left_table_column_count = 5;
        assert_eq!(
            join_table_borders(left_table_column_count, &projections),
            (4, 5)
        );

        let left_table_column_count = 8;
        assert_eq!(
            join_table_borders(left_table_column_count, &projections),
            (7, 8)
        );

        let left_table_column_count = 1;
        assert_eq!(
            join_table_borders(left_table_column_count, &projections),
            (-1, 0)
        );

        let projections = vec![
            (Column::new("a", 0), "a".to_owned()),
            (Column::new("b", 1), "b".to_owned()),
            (Column::new("d", 3), "d".to_owned()),
            (Column::new("g", 6), "g".to_owned()),
            (Column::new("e", 4), "e".to_owned()),
            (Column::new("f", 5), "f".to_owned()),
            (Column::new("e", 4), "e".to_owned()),
            (Column::new("h", 7), "h".to_owned()),
        ];
        let left_table_column_count = 5;
        assert_eq!(
            join_table_borders(left_table_column_count, &projections),
            (2, 7)
        );

        let left_table_column_count = 7;
        assert_eq!(
            join_table_borders(left_table_column_count, &projections),
            (6, 7)
        );

        Ok(())
    }

    #[tokio::test]
    async fn project_no_column() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        let exec = test::scan_partitioned(1);
        let expected = collect(exec.execute(0, Arc::clone(&task_ctx))?)
            .await
            .unwrap();

        let projection = ProjectionExec::try_new(vec![], exec)?;
        let stream = projection.execute(0, Arc::clone(&task_ctx))?;
        let output = collect(stream).await.unwrap();
        assert_eq!(output.len(), expected.len());

        Ok(())
    }

    fn get_stats() -> Statistics {
        Statistics {
            num_rows: Precision::Exact(5),
            total_byte_size: Precision::Exact(23),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(42))),
                    null_count: Precision::Exact(0),
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(3),
                },
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Exact(ScalarValue::Float32(Some(1.1))),
                    min_value: Precision::Exact(ScalarValue::Float32(Some(0.1))),
                    sum_value: Precision::Exact(ScalarValue::Float32(Some(5.5))),
                    null_count: Precision::Absent,
                },
            ],
        }
    }

    fn get_schema() -> Schema {
        let field_0 = Field::new("col0", DataType::Int64, false);
        let field_1 = Field::new("col1", DataType::Utf8, false);
        let field_2 = Field::new("col2", DataType::Float32, false);
        Schema::new(vec![field_0, field_1, field_2])
    }
    #[tokio::test]
    async fn test_stats_projection_columns_only() {
        let source = get_stats();
        let schema = get_schema();

        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("col1", 1)),
            Arc::new(Column::new("col0", 0)),
        ];

        let result = stats_projection(source, exprs.into_iter(), Arc::new(schema));

        let expected = Statistics {
            num_rows: Precision::Exact(5),
            total_byte_size: Precision::Exact(23),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(3),
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(42))),
                    null_count: Precision::Exact(0),
                },
            ],
        };

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_stats_projection_column_with_primitive_width_only() {
        let source = get_stats();
        let schema = get_schema();

        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("col2", 2)),
            Arc::new(Column::new("col0", 0)),
        ];

        let result = stats_projection(source, exprs.into_iter(), Arc::new(schema));

        let expected = Statistics {
            num_rows: Precision::Exact(5),
            total_byte_size: Precision::Exact(60),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Exact(ScalarValue::Float32(Some(1.1))),
                    min_value: Precision::Exact(ScalarValue::Float32(Some(0.1))),
                    sum_value: Precision::Exact(ScalarValue::Float32(Some(5.5))),
                    null_count: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(42))),
                    null_count: Precision::Exact(0),
                },
            ],
        };

        assert_eq!(result, expected);
    }
}
