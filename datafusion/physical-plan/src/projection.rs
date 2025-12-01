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

use super::expressions::{Column, Literal};
use super::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use super::{
    DisplayAs, ExecutionPlanProperties, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use crate::execution_plan::CardinalityEffect;
use crate::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation,
};
use crate::joins::utils::{ColumnIndex, JoinFilter, JoinOn, JoinOnRef};
use crate::{DisplayFormatType, ExecutionPlan, PhysicalExpr};
use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{internal_err, JoinSide, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::ProjectionMapping;
use datafusion_physical_expr::projection::Projector;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr_common::physical_expr::{fmt_sql, PhysicalExprRef};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, LexRequirement};
// Re-exported from datafusion-physical-expr for backwards compatibility
// We recommend updating your imports to use datafusion-physical-expr directly
pub use datafusion_physical_expr::projection::{
    update_expr, ProjectionExpr, ProjectionExprs,
};

use futures::stream::{Stream, StreamExt};
use log::trace;

/// [`ExecutionPlan`] for a projection
///
/// Computes a set of scalar value expressions for each input row, producing one
/// output row for each input row.
#[derive(Debug, Clone)]
pub struct ProjectionExec {
    /// A projector specialized to apply the projection to the input schema from the child node
    /// and produce [`RecordBatch`]es with the output schema of this node.
    projector: Projector,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl ProjectionExec {
    /// Create a projection on an input
    ///
    /// # Example:
    /// Create a `ProjectionExec` to crate `SELECT a, a+b AS sum_ab FROM t1`:
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_schema::{Schema, Field, DataType};
    /// # use datafusion_expr::Operator;
    /// # use datafusion_physical_plan::ExecutionPlan;
    /// # use datafusion_physical_expr::expressions::{col, binary};
    /// # use datafusion_physical_plan::empty::EmptyExec;
    /// # use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
    /// # fn schema() -> Arc<Schema> {
    /// #  Arc::new(Schema::new(vec![
    /// #   Field::new("a", DataType::Int32, false),
    /// #   Field::new("b", DataType::Int32, false),
    /// # ]))
    /// # }
    /// #
    /// # fn input() -> Arc<dyn ExecutionPlan> {
    /// #  Arc::new(EmptyExec::new(schema()))
    /// # }
    /// #
    /// # fn main() {
    /// let schema = schema();
    /// // Create PhysicalExprs
    /// let a = col("a", &schema).unwrap();
    /// let b = col("b", &schema).unwrap();
    /// let a_plus_b = binary(Arc::clone(&a), Operator::Plus, b, &schema).unwrap();
    /// // create ProjectionExec
    /// let proj = ProjectionExec::try_new(
    ///     [
    ///         ProjectionExpr {
    ///             // expr a produces the column named "a"
    ///             expr: a,
    ///             alias: "a".to_string(),
    ///         },
    ///         ProjectionExpr {
    ///             // expr: a + b produces the column named "sum_ab"
    ///             expr: a_plus_b,
    ///             alias: "sum_ab".to_string(),
    ///         },
    ///     ],
    ///     input(),
    /// )
    /// .unwrap();
    /// # }
    /// ```
    pub fn try_new<I, E>(expr: I, input: Arc<dyn ExecutionPlan>) -> Result<Self>
    where
        I: IntoIterator<Item = E>,
        E: Into<ProjectionExpr>,
    {
        let input_schema = input.schema();
        // convert argument to Vec<ProjectionExpr>
        let expr_vec = expr.into_iter().map(Into::into).collect::<Vec<_>>();
        let projection = ProjectionExprs::new(expr_vec);
        let projector = projection.make_projector(&input_schema)?;

        // Construct a map from the input expressions to the output expression of the Projection
        let projection_mapping = projection.projection_mapping(&input_schema)?;
        let cache = Self::compute_properties(
            &input,
            &projection_mapping,
            Arc::clone(projector.output_schema()),
        )?;
        Ok(Self {
            projector,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    /// The projection expressions stored as tuples of (expression, output column name)
    pub fn expr(&self) -> &[ProjectionExpr] {
        self.projector.projection().as_ref()
    }

    /// The projection expressions as a [`ProjectionExprs`].
    pub fn projection_expr(&self) -> &ProjectionExprs {
        self.projector.projection()
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
        let input_eq_properties = input.equivalence_properties();
        let eq_properties = input_eq_properties.project(projection_mapping, schema);
        // Calculate output partitioning, which needs to respect aliases:
        let output_partitioning = input
            .output_partitioning()
            .project(projection_mapping, input_eq_properties);

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
                    .projector
                    .projection()
                    .as_ref()
                    .iter()
                    .map(|proj_expr| {
                        let e = proj_expr.expr.to_string();
                        if e != proj_expr.alias {
                            format!("{e} as {}", proj_expr.alias)
                        } else {
                            e
                        }
                    })
                    .collect();

                write!(f, "ProjectionExec: expr=[{}]", expr.join(", "))
            }
            DisplayFormatType::TreeRender => {
                for (i, proj_expr) in self.expr().iter().enumerate() {
                    let expr_sql = fmt_sql(proj_expr.expr.as_ref());
                    if proj_expr.expr.to_string() == proj_expr.alias {
                        writeln!(f, "expr{i}={expr_sql}")?;
                    } else {
                        writeln!(f, "{}={expr_sql}", proj_expr.alias)?;
                    }
                }

                Ok(())
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

    fn maintains_input_order(&self) -> Vec<bool> {
        // Tell optimizer this operator doesn't reorder its input
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        let all_simple_exprs =
            self.projector
                .projection()
                .as_ref()
                .iter()
                .all(|proj_expr| {
                    proj_expr.expr.as_any().is::<Column>()
                        || proj_expr.expr.as_any().is::<Literal>()
                });
        // If expressions are all either column_expr or Literal, then all computations in this projection are reorder or rename,
        // and projection would not benefit from the repartition, benefits_from_input_partitioning will return false.
        vec![!all_simple_exprs]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        ProjectionExec::try_new(
            self.projector.projection().clone(),
            children.swap_remove(0),
        )
        .map(|p| Arc::new(p) as _)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start ProjectionExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        Ok(Box::pin(ProjectionStream::new(
            self.projector.clone(),
            self.input.execute(partition, context)?,
            BaselineMetrics::new(&self.metrics, partition),
        )?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        let input_stats = self.input.partition_statistics(partition)?;
        self.projector
            .projection()
            .project_statistics(input_stats, &self.input.schema())
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

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        // TODO: In future, we can try to handle inverting aliases here.
        // For the time being, we pass through untransformed filters, so filters on aliases are not handled.
        // https://github.com/apache/datafusion/issues/17246
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }
}

impl ProjectionStream {
    /// Create a new projection stream
    fn new(
        projector: Projector,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Result<Self> {
        Ok(Self {
            projector,
            input,
            baseline_metrics,
        })
    }

    fn batch_project(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        // Records time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();
        self.projector.project_batch(batch)
    }
}

/// Projection iterator
struct ProjectionStream {
    projector: Projector,
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
        Arc::clone(self.projector.output_schema())
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
        .map(|(index, field)| ProjectionExpr {
            expr: Arc::new(Column::new(field.name(), *index)) as Arc<dyn PhysicalExpr>,
            alias: field.name().to_owned(),
        })
        .collect::<Vec<_>>();

    let mut new_projection_exprs = Vec::with_capacity(projection.expr().len());

    for proj_expr in projection.expr() {
        // update column index for projection expression since the input schema has been changed.
        let Some(expr) =
            update_expr(&proj_expr.expr, embed_project_exprs.as_slice(), false)?
        else {
            return Ok(None);
        };
        new_projection_exprs.push(ProjectionExpr {
            expr,
            alias: proj_expr.alias.clone(),
        });
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
    schema: &SchemaRef,
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
        schema,
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
    Ok(maybe_modified.map_or_else(|| Transformed::no(plan), Transformed::yes))
}

/// Compare the inputs and outputs of the projection. All expressions must be
/// columns without alias, and projection does not change the order of fields.
/// For example, if the input schema is `a, b`, `SELECT a, b` is removable,
/// but `SELECT b, a` and `SELECT a+1, b` and `SELECT a AS c, b` are not.
fn is_projection_removable(projection: &ProjectionExec) -> bool {
    let exprs = projection.expr();
    exprs.iter().enumerate().all(|(idx, proj_expr)| {
        let Some(col) = proj_expr.expr.as_any().downcast_ref::<Column>() else {
            return false;
        };
        col.name() == proj_expr.alias && col.index() == idx
    }) && exprs.len() == projection.input().schema().fields().len()
}

/// Given the expression set of a projection, checks if the projection causes
/// any renaming or constructs a non-`Column` physical expression.
pub fn all_alias_free_columns(exprs: &[ProjectionExpr]) -> bool {
    exprs.iter().all(|proj_expr| {
        proj_expr
            .expr
            .as_any()
            .downcast_ref::<Column>()
            .map(|column| column.name() == proj_expr.alias)
            .unwrap_or(false)
    })
}

/// Updates a source provider's projected columns according to the given
/// projection operator's expressions. To use this function safely, one must
/// ensure that all expressions are `Column` expressions without aliases.
pub fn new_projections_for_columns(
    projection: &[ProjectionExpr],
    source: &[usize],
) -> Vec<usize> {
    projection
        .iter()
        .filter_map(|proj_expr| {
            proj_expr
                .expr
                .as_any()
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
pub fn all_columns(exprs: &[ProjectionExpr]) -> bool {
    exprs
        .iter()
        .all(|proj_expr| proj_expr.expr.as_any().is::<Column>())
}

/// Updates the given lexicographic ordering according to given projected
/// expressions using the [`update_expr`] function.
pub fn update_ordering(
    ordering: LexOrdering,
    projected_exprs: &[ProjectionExpr],
) -> Result<Option<LexOrdering>> {
    let mut updated_exprs = vec![];
    for mut sort_expr in ordering.into_iter() {
        let Some(updated_expr) = update_expr(&sort_expr.expr, projected_exprs, false)?
        else {
            return Ok(None);
        };
        sort_expr.expr = updated_expr;
        updated_exprs.push(sort_expr);
    }
    Ok(LexOrdering::new(updated_exprs))
}

/// Updates the given lexicographic requirement according to given projected
/// expressions using the [`update_expr`] function.
pub fn update_ordering_requirement(
    reqs: LexRequirement,
    projected_exprs: &[ProjectionExpr],
) -> Result<Option<LexRequirement>> {
    let mut updated_exprs = vec![];
    for mut sort_expr in reqs.into_iter() {
        let Some(updated_expr) = update_expr(&sort_expr.expr, projected_exprs, false)?
        else {
            return Ok(None);
        };
        sort_expr.expr = updated_expr;
        updated_exprs.push(sort_expr);
    }
    Ok(LexRequirement::new(updated_exprs))
}

/// Downcasts all the expressions in `exprs` to `Column`s. If any of the given
/// expressions is not a `Column`, returns `None`.
pub fn physical_to_column_exprs(
    exprs: &[ProjectionExpr],
) -> Option<Vec<(Column, String)>> {
    exprs
        .iter()
        .map(|proj_expr| {
            proj_expr
                .expr
                .as_any()
                .downcast_ref::<Column>()
                .map(|col| (col.clone(), proj_expr.alias.clone()))
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
            .map(|(col, alias)| ProjectionExpr {
                expr: Arc::new(Column::new(col.name(), col.index())) as _,
                alias: alias.clone(),
            }),
        Arc::clone(left_child),
    )?;
    let left_size = left_child.schema().fields().len() as i32;
    let new_right = ProjectionExec::try_new(
        projection_as_columns[far_left_right_col_ind as _..]
            .iter()
            .map(|(col, alias)| {
                ProjectionExpr {
                    expr: Arc::new(Column::new(
                        col.name(),
                        // Align projected expressions coming from the right
                        // table with the new right child projection:
                        (col.index() as i32 - left_size) as _,
                    )) as _,
                    alias: alias.clone(),
                }
            }),
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
    projection.expr().iter().for_each(|proj_expr| {
        proj_expr
            .expr
            .apply(|expr| {
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
        *count > 1 && !is_expr_trivial(&Arc::clone(&child.expr()[column.index()].expr))
    }) {
        return Ok(None);
    }
    for proj_expr in projection.expr() {
        // If there is no match in the input projection, we cannot unify these
        // projections. This case will arise if the projection expression contains
        // a `PhysicalExpr` variant `update_expr` doesn't support.
        let Some(expr) = update_expr(&proj_expr.expr, child.expr(), true)? else {
            return Ok(None);
        };
        projected_exprs.push(ProjectionExpr {
            expr,
            alias: proj_expr.alias.clone(),
        });
    }
    ProjectionExec::try_new(projected_exprs, Arc::clone(child.input()))
        .map(|e| Some(Arc::new(e) as _))
}

/// Collect all column indices from the given projection expressions.
fn collect_column_indices(exprs: &[ProjectionExpr]) -> Vec<usize> {
    // Collect indices and remove duplicates.
    let mut indices = exprs
        .iter()
        .flat_map(|proj_expr| collect_columns(&proj_expr.expr))
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
    use crate::test::exec::StatisticsExec;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::stats::{ColumnStatistics, Precision, Statistics};
    use datafusion_common::ScalarValue;

    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{col, BinaryExpr, Column, Literal};

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
        let column_indices = collect_column_indices(&[ProjectionExpr {
            expr,
            alias: "b-(1+a)".to_string(),
        }]);
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
        let expected = collect(exec.execute(0, Arc::clone(&task_ctx))?).await?;

        let projection = ProjectionExec::try_new(vec![] as Vec<ProjectionExpr>, exec)?;
        let stream = projection.execute(0, Arc::clone(&task_ctx))?;
        let output = collect(stream).await?;
        assert_eq!(output.len(), expected.len());

        Ok(())
    }

    #[tokio::test]
    async fn project_old_syntax() {
        let exec = test::scan_partitioned(1);
        let schema = exec.schema();
        let expr = col("i", &schema).unwrap();
        ProjectionExec::try_new(
            vec![
                // use From impl of ProjectionExpr to create ProjectionExpr
                // to test old syntax
                (expr, "c".to_string()),
            ],
            exec,
        )
        // expect this to succeed
        .unwrap();
    }

    #[test]
    fn test_projection_statistics_uses_input_schema() {
        let input_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int32, false),
            Field::new("e", DataType::Int32, false),
            Field::new("f", DataType::Int32, false),
        ]);

        let input_statistics = Statistics {
            num_rows: Precision::Exact(10),
            column_statistics: vec![
                ColumnStatistics {
                    min_value: Precision::Exact(ScalarValue::Int32(Some(1))),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(100))),
                    ..Default::default()
                },
                ColumnStatistics {
                    min_value: Precision::Exact(ScalarValue::Int32(Some(5))),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(50))),
                    ..Default::default()
                },
                ColumnStatistics {
                    min_value: Precision::Exact(ScalarValue::Int32(Some(10))),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(40))),
                    ..Default::default()
                },
                ColumnStatistics {
                    min_value: Precision::Exact(ScalarValue::Int32(Some(20))),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(30))),
                    ..Default::default()
                },
                ColumnStatistics {
                    min_value: Precision::Exact(ScalarValue::Int32(Some(21))),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(29))),
                    ..Default::default()
                },
                ColumnStatistics {
                    min_value: Precision::Exact(ScalarValue::Int32(Some(24))),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(26))),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let input = Arc::new(StatisticsExec::new(input_statistics, input_schema));

        // Create projection expressions that reference columns from the input schema and the length
        // of output schema columns < input schema columns and hence if we use the last few columns
        // from the input schema in the expressions here, bounds_check would fail on them if output
        // schema is supplied to the partitions_statistics method.
        let exprs: Vec<ProjectionExpr> = vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("c", 2)) as Arc<dyn PhysicalExpr>,
                alias: "c_renamed".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("e", 4)),
                    Operator::Plus,
                    Arc::new(Column::new("f", 5)),
                )) as Arc<dyn PhysicalExpr>,
                alias: "e_plus_f".to_string(),
            },
        ];

        let projection = ProjectionExec::try_new(exprs, input).unwrap();

        let stats = projection.partition_statistics(None).unwrap();

        assert_eq!(stats.num_rows, Precision::Exact(10));
        assert_eq!(
            stats.column_statistics.len(),
            2,
            "Expected 2 columns in projection statistics"
        );
        assert!(stats.total_byte_size.is_exact().unwrap_or(false));
    }
}
