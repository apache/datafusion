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
use crate::{ColumnStatistics, DisplayFormatType, ExecutionPlan, PhysicalExpr};
use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion_common::config::ConfigOptions;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{internal_datafusion_err, internal_err, JoinSide, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::ProjectionMapping;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr_common::physical_expr::{fmt_sql, PhysicalExprRef};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, LexRequirement};

use futures::stream::{Stream, StreamExt};
use itertools::Itertools;
use log::trace;

/// [`ExecutionPlan`] for a projection
///
/// Computes a set of scalar value expressions for each input row, producing one
/// output row for each input row.
#[derive(Debug, Clone)]
pub struct ProjectionExec {
    /// The projection expressions stored as tuples of (expression, output column name)
    projection: Projection,
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
    /// let proj = ProjectionExec::try_new([
    ///     ProjectionExpr {
    ///       // expr a produces the column named "a"
    ///       expr: a,
    ///       alias: "a".to_string(),
    ///     },
    ///     ProjectionExpr {
    ///       // expr: a + b produces the column named "sum_ab"
    ///       expr: a_plus_b,
    ///       alias: "sum_ab".to_string(),
    ///     }
    ///   ], input()).unwrap();
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
        let projection = Projection::new(expr_vec);

        let schema = Arc::new(projection.project_schema(&input_schema)?);

        // Construct a map from the input expressions to the output expression of the Projection
        let projection_mapping = ProjectionMapping::try_new(
            projection
                .as_ref()
                .iter()
                .map(|p| (Arc::clone(&p.expr), p.alias.clone())),
            &input_schema,
        )?;
        let cache =
            Self::compute_properties(&input, &projection_mapping, Arc::clone(&schema))?;
        Ok(Self {
            projection,
            schema,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    /// The projection expressions stored as tuples of (expression, output column name)
    pub fn expr(&self) -> &[ProjectionExpr] {
        self.projection.as_ref()
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

/// A projection expression as used by [`ProjectionExec`].
///
/// The expression is evaluated and the result is stored in a column
/// with the name specified by `alias`.
///
/// For example, the SQL expression `a + b AS sum_ab` would be represented
/// as a `ProjectionExpr` where `expr` is the expression `a + b`
/// and `alias` is the string `sum_ab`.
#[derive(Debug, Clone)]
pub struct ProjectionExpr {
    /// The expression that will be evaluated.
    pub expr: Arc<dyn PhysicalExpr>,
    /// The name of the output column for use an output schema.
    pub alias: String,
}

impl std::fmt::Display for ProjectionExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.expr.to_string() == self.alias {
            write!(f, "{}", self.alias)
        } else {
            write!(f, "{} AS {}", self.expr, self.alias)
        }
    }
}

impl ProjectionExpr {
    /// Create a new projection expression
    pub fn new(expr: Arc<dyn PhysicalExpr>, alias: String) -> Self {
        Self { expr, alias }
    }

    /// Create a new projection expression from an expression and a schema using the expression's output field name as alias.
    pub fn new_from_expression(
        expr: Arc<dyn PhysicalExpr>,
        schema: &Schema,
    ) -> Result<Self> {
        let field = expr.return_field(schema)?;
        Ok(Self {
            expr,
            alias: field.name().to_string(),
        })
    }
}

impl From<(Arc<dyn PhysicalExpr>, String)> for ProjectionExpr {
    fn from(value: (Arc<dyn PhysicalExpr>, String)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl From<&(Arc<dyn PhysicalExpr>, String)> for ProjectionExpr {
    fn from(value: &(Arc<dyn PhysicalExpr>, String)) -> Self {
        Self::new(Arc::clone(&value.0), value.1.clone())
    }
}

impl From<ProjectionExpr> for (Arc<dyn PhysicalExpr>, String) {
    fn from(value: ProjectionExpr) -> Self {
        (value.expr, value.alias)
    }
}

/// A collection of projection expressions.
///
/// This struct encapsulates multiple `ProjectionExpr` instances,
/// representing a complete projection operation and provides
/// methods to manipulate and analyze the projection as a whole.
#[derive(Debug, Clone)]
pub struct Projection {
    exprs: Vec<ProjectionExpr>,
}

impl From<Vec<ProjectionExpr>> for Projection {
    fn from(value: Vec<ProjectionExpr>) -> Self {
        Self { exprs: value }
    }
}

impl From<&[ProjectionExpr]> for Projection {
    fn from(value: &[ProjectionExpr]) -> Self {
        Self {
            exprs: value.to_vec(),
        }
    }
}

impl AsRef<[ProjectionExpr]> for Projection {
    fn as_ref(&self) -> &[ProjectionExpr] {
        &self.exprs
    }
}

impl Projection {
    pub fn new(exprs: Vec<ProjectionExpr>) -> Self {
        Self { exprs }
    }

    /// Apply another projection on top of this projection, returning the combined projection.
    /// For example, if this projection is `SELECT c@2 AS x, b@1 AS y, a@0 as z` and the other projection is `SELECT x@0 + 1 AS c1, y@1 + z@2 as c2`,
    /// we return a projection equivalent to `SELECT c@2 + 1 AS c1, b@1 + a@0 as c2`.
    pub fn try_merge(&self, other: &Projection) -> Result<Projection> {
        let mut new_exprs = Vec::with_capacity(other.exprs.len());
        for proj_expr in &other.exprs {
            let new_expr = update_expr(&proj_expr.expr, &self.exprs, true)?
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Failed to combine projections: expression {} could not be applied on top of existing projections {}",
                        proj_expr.expr,
                        self.exprs.iter().map(|e| format!("{}", e)).join(", ")
                    )
                })?;
            new_exprs.push(ProjectionExpr {
                expr: new_expr,
                alias: proj_expr.alias.clone(),
            });
        }
        Ok(Projection::new(new_exprs))
    }

    /// Extract the column indices used in this projection.
    /// For example, for a projection `SELECT a AS x, b + 1 AS y`, where `a` is at index 0 and `b` is at index 1,
    /// this function would return `[0, 1]`.
    /// Repeated indices are returned only once, and the order is ascending.
    pub fn column_indices(&self) -> Vec<usize> {
        self.exprs
            .iter()
            .flat_map(|e| collect_columns(&e.expr).into_iter().map(|col| col.index()))
            .sorted_unstable()
            .dedup()
            .collect_vec()
    }

    /// Project a schema according to this projection.
    /// For example, for a projection `SELECT a AS x, b + 1 AS y`, where `a` is at index 0 and `b` is at index 1,
    /// if the input schema is `[a: Int32, b: Int32, c: Int32]`, the output schema would be `[x: Int32, y: Int32]`.
    /// Fields' metadata are preserved from the input schema.
    pub fn project_schema(&self, input_schema: &Schema) -> Result<Schema> {
        let fields: Result<Vec<Field>> = self
            .exprs
            .iter()
            .map(|proj_expr| {
                let metadata = proj_expr
                    .expr
                    .return_field(input_schema)?
                    .metadata()
                    .clone();

                let field = Field::new(
                    &proj_expr.alias,
                    proj_expr.expr.data_type(input_schema)?,
                    proj_expr.expr.nullable(input_schema)?,
                )
                .with_metadata(metadata);

                Ok(field)
            })
            .collect();

        Ok(Schema::new_with_metadata(
            fields?,
            input_schema.metadata().clone(),
        ))
    }

    /// Project statistics according to this projection.
    /// For example, for a projection `SELECT a AS x, b + 1 AS y`, where `a` is at index 0 and `b` is at index 1,
    /// if the input statistics has column statistics for columns `a`, `b`, and `c`, the output statistics would have column statistics for columns `x` and `y`.
    pub fn project_statistics(
        &self,
        mut stats: Statistics,
        input_schema: &Schema,
    ) -> Result<Statistics> {
        let mut primitive_row_size = 0;
        let mut primitive_row_size_possible = true;
        let mut column_statistics = vec![];

        for proj_expr in &self.exprs {
            let expr = &proj_expr.expr;
            let col_stats = if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                stats.column_statistics[col.index()].clone()
            } else {
                // TODO stats: estimate more statistics from expressions
                // (expressions should compute their statistics themselves)
                ColumnStatistics::new_unknown()
            };
            column_statistics.push(col_stats);
            let data_type = expr.data_type(input_schema)?;
            if let Some(value) = data_type.primitive_width() {
                primitive_row_size += value;
                continue;
            }
            primitive_row_size_possible = false;
        }

        if primitive_row_size_possible {
            stats.total_byte_size =
                Precision::Exact(primitive_row_size).multiply(&stats.num_rows);
        }
        stats.column_statistics = column_statistics;
        Ok(stats)
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
                    .projection
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
        let all_simple_exprs = self.projection.as_ref().iter().all(|proj_expr| {
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
            self.projection.as_ref().to_vec(),
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
        Ok(Box::pin(ProjectionStream {
            schema: Arc::clone(&self.schema),
            expr: self
                .projection
                .as_ref()
                .iter()
                .map(|x| Arc::clone(&x.expr))
                .collect(),
            input: self.input.execute(partition, context)?,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        let input_stats = self.input.partition_statistics(partition)?;
        self.projection
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
pub struct ProjectionStream {
    schema: SchemaRef,
    expr: Vec<Arc<dyn PhysicalExpr>>,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
}

impl ProjectionStream {
    /// Create a new projection stream
    pub fn new(
        schema: SchemaRef,
        expr: Vec<Arc<dyn PhysicalExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        Self {
            schema,
            expr,
            input,
            baseline_metrics,
        }
    }
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
    projected_exprs: &[ProjectionExpr],
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
        .transform_up(|expr| {
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
                    &projected_exprs[column.index()].expr,
                )))
            } else {
                // default to invalid, in case we can't find the relevant column
                state = RewriteState::RewrittenInvalid;
                // Determine how to update `column` to accommodate `projected_exprs`
                projected_exprs
                    .iter()
                    .enumerate()
                    .find_map(|(index, proj_expr)| {
                        proj_expr.expr.as_any().downcast_ref::<Column>().and_then(
                            |projected_column| {
                                (column.name().eq(projected_column.name())
                                    && column.index() == projected_column.index())
                                .then(|| {
                                    state = RewriteState::RewrittenValid;
                                    Arc::new(Column::new(&proj_expr.alias, index)) as _
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

        let projection = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("col1", 1)),
                alias: "col1".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col0", 0)),
                alias: "col0".to_string(),
            },
        ]);

        let result = projection.project_statistics(source, &schema).unwrap();

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

        let projection = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("col2", 2)),
                alias: "col2".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col0", 0)),
                alias: "col0".to_string(),
            },
        ]);

        let result = projection.project_statistics(source, &schema).unwrap();

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

    // Tests for Projection struct

    #[test]
    fn test_projection_new() -> Result<()> {
        let exprs = vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("a", 0)),
                alias: "a".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b", 1)),
                alias: "b".to_string(),
            },
        ];
        let projection = Projection::new(exprs.clone());
        assert_eq!(projection.as_ref().len(), 2);
        Ok(())
    }

    #[test]
    fn test_projection_from_vec() -> Result<()> {
        let exprs = vec![ProjectionExpr {
            expr: Arc::new(Column::new("x", 0)),
            alias: "x".to_string(),
        }];
        let projection: Projection = exprs.clone().into();
        assert_eq!(projection.as_ref().len(), 1);
        Ok(())
    }

    #[test]
    fn test_projection_as_ref() -> Result<()> {
        let exprs = vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("col1", 0)),
                alias: "col1".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col2", 1)),
                alias: "col2".to_string(),
            },
        ];
        let projection = Projection::new(exprs);
        let as_ref: &[ProjectionExpr] = projection.as_ref();
        assert_eq!(as_ref.len(), 2);
        Ok(())
    }

    #[test]
    fn test_column_indices_single_column() -> Result<()> {
        let projection = Projection::new(vec![ProjectionExpr {
            expr: Arc::new(Column::new("a", 3)),
            alias: "a".to_string(),
        }]);
        assert_eq!(projection.column_indices(), vec![3]);
        Ok(())
    }

    #[test]
    fn test_column_indices_multiple_columns() -> Result<()> {
        let projection = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("a", 0)),
                alias: "a".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b", 2)),
                alias: "b".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("c", 5)),
                alias: "c".to_string(),
            },
        ]);
        assert_eq!(projection.column_indices(), vec![0, 2, 5]);
        Ok(())
    }

    #[test]
    fn test_column_indices_duplicates() -> Result<()> {
        // Test that duplicate column indices appear only once
        let projection = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("a", 1)),
                alias: "a".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b", 3)),
                alias: "b".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("a2", 1)), // duplicate index
                alias: "a2".to_string(),
            },
        ]);
        assert_eq!(projection.column_indices(), vec![1, 3]);
        Ok(())
    }

    #[test]
    fn test_column_indices_unsorted() -> Result<()> {
        // Test that column indices are sorted in the output
        let projection = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("c", 5)),
                alias: "c".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("a", 1)),
                alias: "a".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b", 3)),
                alias: "b".to_string(),
            },
        ]);
        assert_eq!(projection.column_indices(), vec![1, 3, 5]);
        Ok(())
    }

    #[test]
    fn test_column_indices_complex_expr() -> Result<()> {
        // Test with complex expressions containing multiple columns
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 1)),
            Operator::Plus,
            Arc::new(Column::new("b", 4)),
        ));
        let projection = Projection::new(vec![
            ProjectionExpr {
                expr,
                alias: "sum".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("c", 2)),
                alias: "c".to_string(),
            },
        ]);
        // Should return [1, 2, 4] - all columns used, sorted and deduplicated
        assert_eq!(projection.column_indices(), vec![1, 2, 4]);
        Ok(())
    }

    #[test]
    fn test_column_indices_empty() -> Result<()> {
        let projection = Projection::new(vec![]);
        assert_eq!(projection.column_indices(), Vec::<usize>::new());
        Ok(())
    }

    #[test]
    fn test_merge_simple_columns() -> Result<()> {
        // First projection: SELECT c@2 AS x, b@1 AS y, a@0 AS z
        let base_projection = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("c", 2)),
                alias: "x".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b", 1)),
                alias: "y".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("a", 0)),
                alias: "z".to_string(),
            },
        ]);

        // Second projection: SELECT x@0 AS col1, y@1 AS col2
        let top_projection = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("x", 0)),
                alias: "col1".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("y", 1)),
                alias: "col2".to_string(),
            },
        ]);

        // Merge should produce: SELECT c@2 AS col1, b@1 AS col2
        let merged = base_projection.try_merge(&top_projection)?;
        assert_eq!(merged.as_ref().len(), 2);

        // Check first expression (col1 should reference c@2)
        let col1_expr = merged.as_ref()[0].expr.as_any().downcast_ref::<Column>();
        assert!(col1_expr.is_some());
        let col1 = col1_expr.unwrap();
        assert_eq!(col1.name(), "c");
        assert_eq!(col1.index(), 2);
        assert_eq!(merged.as_ref()[0].alias, "col1");

        // Check second expression (col2 should reference b@1)
        let col2_expr = merged.as_ref()[1].expr.as_any().downcast_ref::<Column>();
        assert!(col2_expr.is_some());
        let col2 = col2_expr.unwrap();
        assert_eq!(col2.name(), "b");
        assert_eq!(col2.index(), 1);
        assert_eq!(merged.as_ref()[1].alias, "col2");

        Ok(())
    }

    #[test]
    fn test_merge_with_expressions() -> Result<()> {
        // First projection: SELECT c@2 AS x, b@1 AS y, a@0 AS z
        let base_projection = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("c", 2)),
                alias: "x".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b", 1)),
                alias: "y".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("a", 0)),
                alias: "z".to_string(),
            },
        ]);

        // Second projection: SELECT x@0 + 1 AS c1, y@1 + z@2 AS c2
        let top_projection = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("x", 0)),
                    Operator::Plus,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                )),
                alias: "c1".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("y", 1)),
                    Operator::Plus,
                    Arc::new(Column::new("z", 2)),
                )),
                alias: "c2".to_string(),
            },
        ]);

        // Merge should produce: SELECT c@2 + 1 AS c1, b@1 + a@0 AS c2
        let merged = base_projection.try_merge(&top_projection)?;
        assert_eq!(merged.as_ref().len(), 2);
        assert_eq!(merged.as_ref()[0].alias, "c1");
        assert_eq!(merged.as_ref()[1].alias, "c2");

        // Check that the expressions are BinaryExpr (not just Column)
        assert!(merged.as_ref()[0].expr.as_any().is::<BinaryExpr>());
        assert!(merged.as_ref()[1].expr.as_any().is::<BinaryExpr>());

        Ok(())
    }

    #[test]
    fn test_merge_docstring_example() -> Result<()> {
        // Example from the docstring:
        // Base projection: SELECT c@2 AS x, b@1 AS y, a@0 AS z
        let base = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("c", 2)),
                alias: "x".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b", 1)),
                alias: "y".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("a", 0)),
                alias: "z".to_string(),
            },
        ]);

        // Top projection: SELECT x@0 + 1 AS c1, y@1 + z@2 AS c2
        let top = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("x", 0)),
                    Operator::Plus,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                )),
                alias: "c1".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("y", 1)),
                    Operator::Plus,
                    Arc::new(Column::new("z", 2)),
                )),
                alias: "c2".to_string(),
            },
        ]);

        // Expected result: SELECT c@2 + 1 AS c1, b@1 + a@0 AS c2
        let result = base.try_merge(&top)?;

        assert_eq!(result.as_ref().len(), 2);
        assert_eq!(result.as_ref()[0].alias, "c1");
        assert_eq!(result.as_ref()[1].alias, "c2");

        Ok(())
    }

    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn test_merge_failure() {
        // Create a base projection with only 2 columns
        let base = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("a", 0)),
                alias: "x".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b", 1)),
                alias: "y".to_string(),
            },
        ]);

        // Try to merge with a projection that references column index 3 (out of bounds)
        let top = Projection::new(vec![ProjectionExpr {
            expr: Arc::new(Column::new("z", 3)), // This will panic - index 3 doesn't exist
            alias: "result".to_string(),
        }]);

        // This should panic because column index 3 is out of bounds
        let _result = base.try_merge(&top);
    }

    #[test]
    fn test_project_schema_simple_columns() -> Result<()> {
        // Input schema: [col0: Int64, col1: Utf8, col2: Float32]
        let input_schema = get_schema();

        // Projection: SELECT col2 AS c, col0 AS a
        let projection = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("col2", 2)),
                alias: "c".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col0", 0)),
                alias: "a".to_string(),
            },
        ]);

        let output_schema = projection.project_schema(&input_schema)?;

        // Should have 2 fields
        assert_eq!(output_schema.fields().len(), 2);

        // First field should be "c" with Float32 type
        assert_eq!(output_schema.field(0).name(), "c");
        assert_eq!(output_schema.field(0).data_type(), &DataType::Float32);

        // Second field should be "a" with Int64 type
        assert_eq!(output_schema.field(1).name(), "a");
        assert_eq!(output_schema.field(1).data_type(), &DataType::Int64);

        Ok(())
    }

    #[test]
    fn test_project_schema_with_expressions() -> Result<()> {
        // Input schema: [col0: Int64, col1: Utf8, col2: Float32]
        let input_schema = get_schema();

        // Projection: SELECT col0 + 1 AS incremented
        let projection = Projection::new(vec![ProjectionExpr {
            expr: Arc::new(BinaryExpr::new(
                Arc::new(Column::new("col0", 0)),
                Operator::Plus,
                Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
            )),
            alias: "incremented".to_string(),
        }]);

        let output_schema = projection.project_schema(&input_schema)?;

        // Should have 1 field
        assert_eq!(output_schema.fields().len(), 1);

        // Field should be "incremented" with Int64 type
        assert_eq!(output_schema.field(0).name(), "incremented");
        assert_eq!(output_schema.field(0).data_type(), &DataType::Int64);

        Ok(())
    }

    #[test]
    fn test_project_schema_preserves_metadata() -> Result<()> {
        // Create schema with metadata
        let mut metadata = HashMap::new();
        metadata.insert("key".to_string(), "value".to_string());
        let field_with_metadata =
            Field::new("col0", DataType::Int64, false).with_metadata(metadata.clone());
        let input_schema = Schema::new(vec![
            field_with_metadata,
            Field::new("col1", DataType::Utf8, false),
        ]);

        // Projection: SELECT col0 AS renamed
        let projection = Projection::new(vec![ProjectionExpr {
            expr: Arc::new(Column::new("col0", 0)),
            alias: "renamed".to_string(),
        }]);

        let output_schema = projection.project_schema(&input_schema)?;

        // Should have 1 field
        assert_eq!(output_schema.fields().len(), 1);

        // Field should be "renamed" with metadata preserved
        assert_eq!(output_schema.field(0).name(), "renamed");
        assert_eq!(output_schema.field(0).metadata(), &metadata);

        Ok(())
    }

    #[test]
    fn test_project_schema_empty() -> Result<()> {
        let input_schema = get_schema();
        let projection = Projection::new(vec![]);

        let output_schema = projection.project_schema(&input_schema)?;

        assert_eq!(output_schema.fields().len(), 0);

        Ok(())
    }

    #[test]
    fn test_project_schema_single_column() -> Result<()> {
        let input_schema = get_schema();

        // Projection: SELECT col1 AS text
        let projection = Projection::new(vec![ProjectionExpr {
            expr: Arc::new(Column::new("col1", 1)),
            alias: "text".to_string(),
        }]);

        let output_schema = projection.project_schema(&input_schema)?;

        assert_eq!(output_schema.fields().len(), 1);
        assert_eq!(output_schema.field(0).name(), "text");
        assert_eq!(output_schema.field(0).data_type(), &DataType::Utf8);

        Ok(())
    }

    #[test]
    fn test_project_statistics_columns_only() -> Result<()> {
        let input_stats = get_stats();
        let input_schema = get_schema();

        // Projection: SELECT col1 AS text, col0 AS num
        let projection = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("col1", 1)),
                alias: "text".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col0", 0)),
                alias: "num".to_string(),
            },
        ]);

        let output_stats = projection.project_statistics(input_stats, &input_schema)?;

        // Row count should be preserved
        assert_eq!(output_stats.num_rows, Precision::Exact(5));

        // Should have 2 column statistics (reordered from input)
        assert_eq!(output_stats.column_statistics.len(), 2);

        // First column (col1 from input)
        assert_eq!(
            output_stats.column_statistics[0].distinct_count,
            Precision::Exact(1)
        );
        assert_eq!(
            output_stats.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::from("x"))
        );

        // Second column (col0 from input)
        assert_eq!(
            output_stats.column_statistics[1].distinct_count,
            Precision::Exact(5)
        );
        assert_eq!(
            output_stats.column_statistics[1].max_value,
            Precision::Exact(ScalarValue::Int64(Some(21)))
        );

        Ok(())
    }

    #[test]
    fn test_project_statistics_with_expressions() -> Result<()> {
        let input_stats = get_stats();
        let input_schema = get_schema();

        // Projection with expression: SELECT col0 + 1 AS incremented, col1 AS text
        let projection = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("col0", 0)),
                    Operator::Plus,
                    Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
                )),
                alias: "incremented".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col1", 1)),
                alias: "text".to_string(),
            },
        ]);

        let output_stats = projection.project_statistics(input_stats, &input_schema)?;

        // Row count should be preserved
        assert_eq!(output_stats.num_rows, Precision::Exact(5));

        // Should have 2 column statistics
        assert_eq!(output_stats.column_statistics.len(), 2);

        // First column (expression) should have unknown statistics
        assert_eq!(
            output_stats.column_statistics[0].distinct_count,
            Precision::Absent
        );
        assert_eq!(
            output_stats.column_statistics[0].max_value,
            Precision::Absent
        );

        // Second column (col1) should preserve statistics
        assert_eq!(
            output_stats.column_statistics[1].distinct_count,
            Precision::Exact(1)
        );

        Ok(())
    }

    #[test]
    fn test_project_statistics_primitive_width_only() -> Result<()> {
        let input_stats = get_stats();
        let input_schema = get_schema();

        // Projection with only primitive width columns: SELECT col2 AS f, col0 AS i
        let projection = Projection::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("col2", 2)),
                alias: "f".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col0", 0)),
                alias: "i".to_string(),
            },
        ]);

        let output_stats = projection.project_statistics(input_stats, &input_schema)?;

        // Row count should be preserved
        assert_eq!(output_stats.num_rows, Precision::Exact(5));

        // Total byte size should be recalculated for primitive types
        // Float32 (4 bytes) + Int64 (8 bytes) = 12 bytes per row, 5 rows = 60 bytes
        assert_eq!(output_stats.total_byte_size, Precision::Exact(60));

        // Should have 2 column statistics
        assert_eq!(output_stats.column_statistics.len(), 2);

        Ok(())
    }

    #[test]
    fn test_project_statistics_empty() -> Result<()> {
        let input_stats = get_stats();
        let input_schema = get_schema();

        let projection = Projection::new(vec![]);

        let output_stats = projection.project_statistics(input_stats, &input_schema)?;

        // Row count should be preserved
        assert_eq!(output_stats.num_rows, Precision::Exact(5));

        // Should have no column statistics
        assert_eq!(output_stats.column_statistics.len(), 0);

        // Total byte size should be 0 for empty projection
        assert_eq!(output_stats.total_byte_size, Precision::Exact(0));

        Ok(())
    }
}
