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

use super::expressions::{Column, PhysicalSortExpr};
use super::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use super::{DisplayAs, RecordBatchStream, SendableRecordBatchStream, Statistics};
use crate::{
    ColumnStatistics, DisplayFormatType, EquivalenceProperties, ExecutionPlan,
    Partitioning, PhysicalExpr,
};

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::{Literal, UnKnownColumn};
use datafusion_physical_expr::{
    normalize_out_expr_with_columns_map, project_equivalence_properties,
    project_ordering_equivalence_properties, OrderingEquivalenceProperties,
};

use datafusion_physical_expr::utils::find_orderings_of_exprs;
use futures::stream::{Stream, StreamExt};
use log::trace;

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
    /// Expressions' normalized orderings (as given by the output ordering API
    /// and normalized with respect to equivalence classes of input plan). The
    /// projected expressions are mapped by their indices to this vector.
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
                let entry = columns_map.entry(matching_input_column).or_default();
                entry.push(Column::new(name, expr_idx));
            };
        }

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

        let orderings = find_orderings_of_exprs(
            &expr,
            input.output_ordering(),
            input.equivalence_properties(),
            input.ordering_equivalence_properties(),
        )?;

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

        if let Some(leading_ordering) = self
            .output_ordering
            .as_ref()
            .map(|output_ordering| &output_ordering[0])
        {
            for order in self.orderings.iter().flatten() {
                if !order.eq(leading_ordering)
                    && !new_properties.satisfies_leading_ordering(order)
                {
                    new_properties.add_equal_conditions((
                        &vec![leading_ordering.clone()],
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
            self.schema.clone(),
        )
    }
}

/// This function takes the current `output_ordering`, the `orderings` based on projected expressions,
/// and the `expr` representing the projected expressions themselves. It aims to ensure that the output
/// ordering is valid and correctly corresponds to the projected columns.
///
/// If the leading expression in the `output_ordering` is an [`UnKnownColumn`], it indicates that the column
/// referenced in the ordering is not found among the projected expressions. In such cases, this function
/// attempts to create a new output ordering by referring to valid columns from the leftmost side of the
/// expressions that have an ordering specified.
fn validate_output_ordering(
    output_ordering: Option<Vec<PhysicalSortExpr>>,
    orderings: &[Option<PhysicalSortExpr>],
    expr: &[(Arc<dyn PhysicalExpr>, String)],
) -> Option<Vec<PhysicalSortExpr>> {
    output_ordering.and_then(|ordering| {
        // If the leading expression is invalid column, change output
        // ordering of the projection so that it refers to valid columns if
        // possible.
        if ordering[0].expr.as_any().is::<UnKnownColumn>() {
            for (idx, order) in orderings.iter().enumerate() {
                if let Some(sort_expr) = order {
                    let (_, col_name) = &expr[idx];
                    return Some(vec![PhysicalSortExpr {
                        expr: Arc::new(Column::new(col_name, idx)),
                        options: sort_expr.options,
                    }]);
                }
            }
            None
        } else {
            Some(ordering)
        }
    })
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
    schema: SchemaRef,
) -> Statistics {
    let inner_exprs = exprs.collect::<Vec<_>>();
    let column_statistics = stats.column_statistics.map(|input_col_stats| {
        inner_exprs
            .clone()
            .into_iter()
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

    let primitive_row_size = inner_exprs
        .into_iter()
        .map(|e| match e.data_type(schema.as_ref()) {
            Ok(data_type) => data_type.primitive_width(),
            Err(_) => None,
        })
        .try_fold(0usize, |init, v| v.map(|value| init + value));

    match (primitive_row_size, stats.num_rows) {
        (Some(row_size), Some(row_count)) => {
            Statistics {
                is_exact: stats.is_exact,
                num_rows: stats.num_rows,
                column_statistics,
                // Use the row_size * row_count as the total byte size
                total_byte_size: Some(row_size * row_count),
            }
        }
        _ => {
            Statistics {
                is_exact: stats.is_exact,
                num_rows: stats.num_rows,
                column_statistics,
                // TODO stats: knowing the type of the new columns we can guess the output size
                // If we can't get the exact statistics for the project
                // Before we get the exact result, we just use the child status
                total_byte_size: stats.total_byte_size,
            }
        }
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
    use crate::common::collect;
    use crate::expressions;
    use crate::test;
    use arrow_schema::DataType;
    use datafusion_common::ScalarValue;

    #[tokio::test]
    async fn project_no_column() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        let exec = test::scan_partitioned(1);
        let expected = collect(exec.execute(0, task_ctx.clone())?).await.unwrap();

        let projection = ProjectionExec::try_new(vec![], exec)?;
        let stream = projection.execute(0, task_ctx.clone())?;
        let output = collect(stream).await.unwrap();
        assert_eq!(output.len(), expected.len());

        Ok(())
    }

    fn get_stats() -> Statistics {
        Statistics {
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
            Arc::new(expressions::Column::new("col1", 1)),
            Arc::new(expressions::Column::new("col0", 0)),
        ];

        let result = stats_projection(source, exprs.into_iter(), Arc::new(schema));

        let expected = Statistics {
            is_exact: true,
            num_rows: Some(5),
            total_byte_size: Some(23),
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

    #[tokio::test]
    async fn test_stats_projection_column_with_primitive_width_only() {
        let source = get_stats();
        let schema = get_schema();

        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(expressions::Column::new("col2", 2)),
            Arc::new(expressions::Column::new("col0", 0)),
        ];

        let result = stats_projection(source, exprs.into_iter(), Arc::new(schema));

        let expected = Statistics {
            is_exact: true,
            num_rows: Some(5),
            total_byte_size: Some(60),
            column_statistics: Some(vec![
                ColumnStatistics {
                    distinct_count: None,
                    max_value: Some(ScalarValue::Float32(Some(1.1))),
                    min_value: Some(ScalarValue::Float32(Some(0.1))),
                    null_count: None,
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
