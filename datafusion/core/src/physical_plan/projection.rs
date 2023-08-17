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

use crate::physical_plan::{
    ColumnStatistics, DisplayFormatType, EquivalenceProperties, ExecutionPlan,
    Partitioning, PhysicalExpr,
};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow_schema::SortOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, VisitRecursion};
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::utils::get_indices_of_matching_sort_exprs_with_order_eq;
use futures::stream::{Stream, StreamExt};
use log::trace;

use super::expressions::{Column, PhysicalSortExpr};
use super::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use super::{DisplayAs, RecordBatchStream, SendableRecordBatchStream, Statistics};

use datafusion_physical_expr::expressions::UnKnownColumn;
use datafusion_physical_expr::{
    normalize_out_expr_with_columns_map, project_equivalence_properties,
    project_ordering_equivalence_properties, ExtendedSortOptions,
    OrderingEquivalenceProperties,
};

/// Execution plan for a projection
#[derive(Debug)]
pub struct ProjectionExec {
    /// The projection expressions stored as tuples of (expression, output column name)
    pub(crate) expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// The schema once the projection has been applied to the input
    schema: SchemaRef,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// The output ordering
    output_ordering: Option<Vec<PhysicalSortExpr>>,
    /// The columns map used to normalize out expressions like Partitioning and PhysicalSortExpr
    /// The key is the column from the input schema and the values are the columns from the output schema
    columns_map: HashMap<Column, Vec<Column>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Expressions' orderings calculated by output ordering and equivalence classes of input plan.
    /// The projected expressions are mapped by their indices to this vector.
    orderings: Vec<Option<PhysicalSortExpr>>,
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

        // construct a map from the input columns to the output columns of the Projection
        let mut columns_map: HashMap<Column, Vec<Column>> = HashMap::new();
        for (expr_idx, (expression, name)) in expr.iter().enumerate() {
            if let Some(column) = expression.as_any().downcast_ref::<Column>() {
                // For some executors, logical and physical plan schema fields
                // are not the same. The information in a `Column` comes from
                // the logical plan schema. Therefore, to produce correct results
                // we use the field in the input schema with the same index. This
                // corresponds to the physical plan `Column`.
                let idx = column.index();
                let matching_input_field = input_schema.field(idx);
                let matching_input_column = Column::new(matching_input_field.name(), idx);
                let entry = columns_map
                    .entry(matching_input_column)
                    .or_insert_with(Vec::new);
                entry.push(Column::new(name, expr_idx));
            };
        }

        let orderings = find_orderings_of_exprs(&expr, &input)?;

        // Output Ordering need to respect the alias
        let child_output_ordering = input.output_ordering();
        let output_ordering = match child_output_ordering {
            Some(sort_exprs) => {
                let normalized_exprs = sort_exprs
                    .iter()
                    .map(|sort_expr| {
                        let expr = normalize_out_expr_with_columns_map(
                            sort_expr.expr.clone(),
                            &columns_map,
                        );
                        PhysicalSortExpr {
                            expr,
                            options: sort_expr.options,
                        }
                    })
                    .collect::<Vec<_>>();
                Some(normalized_exprs)
            }
            None => None,
        };

        let output_ordering =
            validate_output_ordering(output_ordering, &orderings, &expr);

        Ok(Self {
            expr,
            schema,
            input,
            output_ordering,
            columns_map,
            metrics: ExecutionPlanMetricsSet::new(),
            orderings,
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
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children[0])
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        // Output partition need to respect the alias
        let input_partition = self.input.output_partitioning();
        match input_partition {
            Partitioning::Hash(exprs, part) => {
                let normalized_exprs = exprs
                    .into_iter()
                    .map(|expr| {
                        normalize_out_expr_with_columns_map(expr, &self.columns_map)
                    })
                    .collect::<Vec<_>>();

                Partitioning::Hash(normalized_exprs, part)
            }
            _ => input_partition,
        }
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.output_ordering.as_deref()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // tell optimizer this operator doesn't reorder its input
        vec![true]
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        let mut new_properties = EquivalenceProperties::new(self.schema());
        project_equivalence_properties(
            self.input.equivalence_properties(),
            &self.columns_map,
            &mut new_properties,
        );
        new_properties
    }

    fn ordering_equivalence_properties(&self) -> OrderingEquivalenceProperties {
        let mut new_properties = OrderingEquivalenceProperties::new(self.schema());
        if self.output_ordering.is_none() {
            // If there is no output ordering, return an "empty" equivalence set:
            return new_properties;
        }

        let input_oeq = self.input().ordering_equivalence_properties();

        project_ordering_equivalence_properties(
            input_oeq,
            &self.columns_map,
            &mut new_properties,
        );

        if let Some(output_ordering) =
            self.output_ordering.as_ref().and_then(|o| o.get(0))
        {
            for order in self.orderings.iter().flatten() {
                if order.eq(output_ordering) {
                    continue;
                }

                let add_new_oeq =
                    if let Some(first_class) = new_properties.classes().get(0) {
                        !first_class.others().iter().any(|v| &v[0] == order)
                    } else {
                        true
                    };

                if add_new_oeq {
                    new_properties.add_equal_conditions((
                        &vec![output_ordering.clone()],
                        &vec![order.clone()],
                    ));
                }
            }
        }

        new_properties
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ProjectionExec::try_new(
            self.expr.clone(),
            children[0].clone(),
        )?))
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        let all_column_expr = self
            .expr
            .iter()
            .all(|(e, _)| e.as_any().downcast_ref::<Column>().is_some());
        // If expressions are all column_expr, then all computations in this projection are reorder or rename,
        // and projection would not benefit from the repartition, benefits_from_input_partitioning will return false.
        !all_column_expr
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start ProjectionExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        Ok(Box::pin(ProjectionStream {
            schema: self.schema.clone(),
            expr: self.expr.iter().map(|x| x.0.clone()).collect(),
            input: self.input.execute(partition, context)?,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        stats_projection(
            self.input.statistics(),
            self.expr.iter().map(|(e, _)| Arc::clone(e)),
        )
    }
}

fn find_orderings_of_exprs(
    expr: &[(Arc<dyn PhysicalExpr>, String)],
    input: &Arc<dyn ExecutionPlan>,
) -> Result<Vec<Option<PhysicalSortExpr>>> {
    let mut orderings: Vec<Option<PhysicalSortExpr>> = vec![];
    if let Some(input_output_ordering) = input.output_ordering().unwrap_or(&[]).get(0) {
        for (index, (expression, name)) in expr.iter().enumerate() {
            let initial_expr = ExprOrdering::new(expression.clone());
            let transformed = initial_expr.transform_up(&|expr| {
                update_ordering(
                    expr,
                    input_output_ordering,
                    || input.equivalence_properties().clone(),
                    || input.ordering_equivalence_properties().clone(),
                )
            })?;
            if let Some(ExtendedSortOptions::Ordered(sort_options)) = transformed.state {
                orderings.push(Some(PhysicalSortExpr {
                    expr: Arc::new(Column::new(name, index)),
                    options: sort_options,
                }));
            } else {
                orderings.push(None);
            }
        }
    }
    Ok(orderings)
}

// If the output ordering corresponds to an UnKnownColumn, it means that the column
// having the input output ordering is not found in any of the projected expressions.
// In that case, we set the new output ordering to be the column constructed by
// the expression residing at the leftmost side of the expressions that have an ordering.
fn validate_output_ordering(
    output_ordering: Option<Vec<PhysicalSortExpr>>,
    orderings: &[Option<PhysicalSortExpr>],
    expr: &[(Arc<dyn PhysicalExpr>, String)],
) -> Option<Vec<PhysicalSortExpr>> {
    output_ordering.and_then(|ordering| {
        if ordering
            .get(0)?
            .expr
            .clone()
            .as_any()
            .downcast_ref::<UnKnownColumn>()
            .is_some()
        {
            orderings
                .iter()
                .position(|o| o.is_some())
                .map(|index| {
                    [PhysicalSortExpr {
                        expr: Arc::new(Column::new(&expr[index].1, index)),
                        options: orderings[index].clone().unwrap().options,
                    }]
                    .to_vec()
                })
                .or(None)
        } else {
            Some(ordering)
        }
    })
}

/// Each expression in a PhysicalExpr is stated with [`ExprOrdering`] struct.
/// Parent expression's [`ExprOrdering`] is set according to [`ExprOrdering`] of its children.
#[derive(Debug)]
struct ExprOrdering {
    expr: Arc<dyn PhysicalExpr>,
    state: Option<ExtendedSortOptions>,
    children_states: Option<Vec<ExtendedSortOptions>>,
}

impl ExprOrdering {
    fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            expr,
            state: None,
            children_states: None,
        }
    }

    fn children(&self) -> Vec<ExprOrdering> {
        self.expr
            .children()
            .into_iter()
            .map(|e| ExprOrdering::new(e))
            .collect()
    }

    pub fn new_with_children(
        children_states: Vec<ExtendedSortOptions>,
        parent_expr: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self {
            expr: parent_expr,
            state: None,
            children_states: Some(children_states),
        }
    }
}

/// Calculates the a [`PhysicalExpr`] node's [`ExprOrdering`] from its children.
fn update_ordering<
    F: Fn() -> EquivalenceProperties,
    F2: Fn() -> OrderingEquivalenceProperties,
>(
    mut node: ExprOrdering,
    sort_expr: &PhysicalSortExpr,
    equal_properties: F,
    ordering_equal_properties: F2,
) -> Result<Transformed<ExprOrdering>> {
    // if we can directly match a sort expr with the current node, we can set its state and return early.
    // TODO: If there is a PhysicalExpr other than Column at the node (let's say a+b), and there is an
    // ordering equivalence of it (let's say c+d), we actually can find it at this step.
    if sort_expr.expr.eq(&node.expr) {
        node.state = Some(ExtendedSortOptions::Ordered(sort_expr.options));
        return Ok(Transformed::Yes(node));
    }

    // intermediate node calculation:
    if let Some(children_sort_options) = &node.children_states {
        let parent_sort_options = node.expr.get_ordering(children_sort_options);

        node.state = Some(parent_sort_options);

        Ok(Transformed::Yes(node))
    }
    // leaf node: (only Column and Literal do not have a child)
    else {
        // column leaf:
        if let Some(column) = node.expr.as_any().downcast_ref::<Column>() {
            node.state = get_indices_of_matching_sort_exprs_with_order_eq(
                &[sort_expr.clone()],
                &[column.clone()],
                equal_properties,
                ordering_equal_properties,
            )
            .map(|(sort_options, _)| {
                ExtendedSortOptions::Ordered(SortOptions {
                    descending: sort_options[0].descending,
                    nulls_first: sort_options[0].nulls_first,
                })
            });
            return Ok(Transformed::Yes(node));
        }
        // last option, literal leaf:
        node.state = Some(node.expr.get_ordering(&[]));
        Ok(Transformed::Yes(node))
    }
}

impl TreeNode for ExprOrdering {
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        let children = self.children();
        for child in children {
            match op(&child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }

        Ok(VisitRecursion::Continue)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if children.is_empty() {
            Ok(self)
        } else {
            let children_nodes = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<_>>>()?;
            Ok(ExprOrdering::new_with_children(
                children_nodes
                    .iter()
                    .map(|c| c.state.unwrap_or(ExtendedSortOptions::Unordered))
                    .collect(),
                self.expr,
            ))
        }
    }
}

/// If e is a direct column reference, returns the field level
/// metadata for that field, if any. Otherwise returns None
fn get_field_metadata(
    e: &Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Option<HashMap<String, String>> {
    let name = if let Some(column) = e.as_any().downcast_ref::<Column>() {
        column.name()
    } else {
        return None;
    };

    input_schema
        .field_with_name(name)
        .ok()
        .map(|f| f.metadata().clone())
}

fn stats_projection(
    stats: Statistics,
    exprs: impl Iterator<Item = Arc<dyn PhysicalExpr>>,
) -> Statistics {
    let column_statistics = stats.column_statistics.map(|input_col_stats| {
        exprs
            .map(|e| {
                if let Some(col) = e.as_any().downcast_ref::<Column>() {
                    input_col_stats[col.index()].clone()
                } else {
                    // TODO stats: estimate more statistics from expressions
                    // (expressions should compute their statistics themselves)
                    ColumnStatistics::default()
                }
            })
            .collect()
    });

    Statistics {
        is_exact: stats.is_exact,
        num_rows: stats.num_rows,
        column_statistics,
        // TODO stats: knowing the type of the new columns we can guess the output size
        total_byte_size: None,
    }
}

impl ProjectionStream {
    fn batch_project(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        // records time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();
        let arrays = self
            .expr
            .iter()
            .map(|expr| expr.evaluate(batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()?;

        if arrays.is_empty() {
            let options =
                RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(self.schema.clone(), arrays, &options)
                .map_err(Into::into)
        } else {
            RecordBatch::try_new(self.schema.clone(), arrays).map_err(Into::into)
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
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for ProjectionStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::physical_plan::common::collect;
    use crate::physical_plan::expressions::{self, col};
    use crate::test::{self};
    use crate::test_util;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::binary;
    use futures::future;

    // Create a binary expression without coercion. Used here when we do not want to coerce the expressions
    // to valid types. Usage can result in an execution (after plan) error.
    fn binary_simple(
        l: Arc<dyn PhysicalExpr>,
        op: Operator,
        r: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
    ) -> Arc<dyn PhysicalExpr> {
        binary(l, op, r, input_schema).unwrap()
    }

    #[tokio::test]
    async fn project_first_column() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = test_util::aggr_test_schema();

        let partitions = 4;
        let csv = test::scan_partitioned_csv(partitions)?;

        // pick column c1 and name it column c1 in the output schema
        let projection =
            ProjectionExec::try_new(vec![(col("c1", &schema)?, "c1".to_string())], csv)?;

        let col_field = projection.schema.field(0);
        let col_metadata = col_field.metadata();
        let data: &str = &col_metadata["testing"];
        assert_eq!(data, "test");

        let mut partition_count = 0;
        let mut row_count = 0;
        for partition in 0..projection.output_partitioning().partition_count() {
            partition_count += 1;
            let stream = projection.execute(partition, task_ctx.clone())?;

            row_count += stream
                .map(|batch| {
                    let batch = batch.unwrap();
                    assert_eq!(1, batch.num_columns());
                    batch.num_rows()
                })
                .fold(0, |acc, x| future::ready(acc + x))
                .await;
        }
        assert_eq!(partitions, partition_count);
        assert_eq!(100, row_count);

        Ok(())
    }

    #[tokio::test]
    async fn project_input_not_partitioning() -> Result<()> {
        let schema = test_util::aggr_test_schema();

        let partitions = 4;
        let csv = test::scan_partitioned_csv(partitions)?;

        // pick column c1 and name it column c1 in the output schema
        let projection =
            ProjectionExec::try_new(vec![(col("c1", &schema)?, "c1".to_string())], csv)?;
        assert!(!projection.benefits_from_input_partitioning());
        Ok(())
    }

    #[tokio::test]
    async fn project_input_partitioning() -> Result<()> {
        let schema = test_util::aggr_test_schema();

        let partitions = 4;
        let csv = test::scan_partitioned_csv(partitions)?;

        let c1 = col("c2", &schema).unwrap();
        let c2 = col("c9", &schema).unwrap();
        let c1_plus_c2 = binary_simple(c1, Operator::Plus, c2, &schema);

        let projection =
            ProjectionExec::try_new(vec![(c1_plus_c2, "c2 + c9".to_string())], csv)?;

        assert!(projection.benefits_from_input_partitioning());
        Ok(())
    }

    #[tokio::test]
    async fn project_no_column() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        let csv = test::scan_partitioned_csv(1)?;
        let expected = collect(csv.execute(0, task_ctx.clone())?).await.unwrap();

        let projection = ProjectionExec::try_new(vec![], csv)?;
        let stream = projection.execute(0, task_ctx.clone())?;
        let output = collect(stream).await.unwrap();
        assert_eq!(output.len(), expected.len());

        Ok(())
    }

    #[tokio::test]
    async fn test_stats_projection_columns_only() {
        let source = Statistics {
            is_exact: true,
            num_rows: Some(5),
            total_byte_size: Some(23),
            column_statistics: Some(vec![
                ColumnStatistics {
                    distinct_count: Some(5),
                    max_value: Some(ScalarValue::Int64(Some(21))),
                    min_value: Some(ScalarValue::Int64(Some(-4))),
                    null_count: Some(0),
                },
                ColumnStatistics {
                    distinct_count: Some(1),
                    max_value: Some(ScalarValue::Utf8(Some(String::from("x")))),
                    min_value: Some(ScalarValue::Utf8(Some(String::from("a")))),
                    null_count: Some(3),
                },
                ColumnStatistics {
                    distinct_count: None,
                    max_value: Some(ScalarValue::Float32(Some(1.1))),
                    min_value: Some(ScalarValue::Float32(Some(0.1))),
                    null_count: None,
                },
            ]),
        };

        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(expressions::Column::new("col1", 1)),
            Arc::new(expressions::Column::new("col0", 0)),
        ];

        let result = stats_projection(source, exprs.into_iter());

        let expected = Statistics {
            is_exact: true,
            num_rows: Some(5),
            total_byte_size: None,
            column_statistics: Some(vec![
                ColumnStatistics {
                    distinct_count: Some(1),
                    max_value: Some(ScalarValue::Utf8(Some(String::from("x")))),
                    min_value: Some(ScalarValue::Utf8(Some(String::from("a")))),
                    null_count: Some(3),
                },
                ColumnStatistics {
                    distinct_count: Some(5),
                    max_value: Some(ScalarValue::Int64(Some(21))),
                    min_value: Some(ScalarValue::Int64(Some(-4))),
                    null_count: Some(0),
                },
            ]),
        };

        assert_eq!(result, expected);
    }
}
