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

//! Replace inner joins against a single row of literals with a filter.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{JoinSide, JoinType, internal_datafusion_err};
use datafusion_physical_expr::expressions::{Column, LambdaExpr, Literal};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprSimplifier};
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion_physical_plan::joins::{CrossJoinExec, NestedLoopJoinExec};
use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::PhysicalOptimizerRule;

/// Rewrites an inner [`NestedLoopJoinExec`] or a [`CrossJoinExec`] whose input
/// provably produces one row of literals into a [`FilterExec`] over the other
/// input, with the literals substituted into the join filter, under a
/// [`ProjectionExec`] that restores the join's output.
///
/// For example, [`AggregateStatistics`] folds the scalar subquery in
///
/// ```sql
/// SELECT e.* FROM events e CROSS JOIN (SELECT min(ts) AS first_ts FROM live) l
/// WHERE e.ts < l.first_ts
/// ```
///
/// into literals over a [`PlaceholderRowExec`], leaving a join that only filters
/// `events`. A nested-loop join preserves neither input's order, while the filter
/// and projection preserve the other input's ordering and partitioning. This lets
/// later operators, such as an aggregation grouped by a sort key of `events`,
/// use that ordering.
///
/// [`AggregateStatistics`]: crate::aggregate_statistics::AggregateStatistics
#[derive(Debug, Default)]
pub struct EliminateConstantJoin {}

impl EliminateConstantJoin {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for EliminateConstantJoin {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let rewritten = if let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>()
            {
                eliminate_nested_loop_join(join)?
            } else if let Some(join) = plan.downcast_ref::<CrossJoinExec>() {
                eliminate(join.left(), join.right(), None, None, &join.schema())?
            } else {
                None
            };
            Ok(match rewritten {
                Some(rewritten) => Transformed::yes(rewritten),
                None => Transformed::no(plan),
            })
        })
        .data()
    }

    fn name(&self) -> &str {
        "EliminateConstantJoin"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Rewrites `join` if it is an inner join against a constant row and its filter
/// has no lambdas.
fn eliminate_nested_loop_join(
    join: &NestedLoopJoinExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if join.join_type() != &JoinType::Inner {
        return Ok(None);
    }
    if let Some(filter) = join.filter()
        // Lambda variables are numbered after the filter's own columns, not the
        // other input's.
        && filter
            .expression()
            .exists(|expr| Ok(expr.is::<LambdaExpr>()))?
    {
        return Ok(None);
    }
    eliminate(
        join.left(),
        join.right(),
        join.filter(),
        join.projection().as_deref(),
        &join.schema(),
    )
}

/// Rewrites an inner join of `left_input` and `right_input` over its non-constant
/// input, if the other input is a constant row.
fn eliminate(
    left_input: &Arc<dyn ExecutionPlan>,
    right_input: &Arc<dyn ExecutionPlan>,
    filter: Option<&JoinFilter>,
    projection: Option<&[usize]>,
    schema: &SchemaRef,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Each input's columns as expressions over the non-constant input.
    let (left, right, input) = if let Some(row) = constant_row(left_input) {
        (row, columns(right_input), right_input)
    } else if let Some(row) = constant_row(right_input) {
        (columns(left_input), row, left_input)
    } else {
        return Ok(None);
    };

    let mut input = Arc::clone(input);
    if let Some(filter) = filter {
        let predicate = Arc::clone(filter.expression())
            .transform_up(|expr| {
                let Some(column) = expr.downcast_ref::<Column>() else {
                    return Ok(Transformed::no(expr));
                };
                // Filter columns index the filter's own schema, which
                // `column_indices` maps back to the join inputs.
                let ColumnIndex { index, side } =
                    filter.column_indices().get(column.index()).ok_or_else(|| {
                        internal_datafusion_err!("Unmapped join filter column")
                    })?;
                let value = match side {
                    JoinSide::Left => left.get(*index),
                    JoinSide::Right => right.get(*index),
                    JoinSide::None => None,
                }
                .ok_or_else(|| {
                    internal_datafusion_err!("Join filter column out of range")
                })?;
                Ok(Transformed::yes(Arc::clone(value)))
            })
            .data()?;
        input = Arc::new(FilterExec::try_new(predicate, input)?);
    }

    let mut outputs = left.into_iter().chain(right).collect::<Vec<_>>();
    if let Some(projection) = projection {
        outputs = projection
            .iter()
            .map(|&index| outputs.get(index).cloned())
            .collect::<Option<_>>()
            .ok_or_else(|| internal_datafusion_err!("Join projection out of range"))?;
    }
    let exprs = outputs
        .into_iter()
        .zip(schema.fields())
        .map(|(expr, field)| ProjectionExpr {
            expr,
            alias: field.name().clone(),
        });
    // Keep the join's metadata, which may come from the constant input.
    Ok(Some(Arc::new(
        ProjectionExec::try_new_with_schema_metadata(exprs, input, schema)?,
    )))
}

/// Returns the literal values of the one row `plan` produces, if it is a stack of
/// projections over a single-partition [`PlaceholderRowExec`] whose expressions
/// evaluate to literals at planning time.
fn constant_row(plan: &Arc<dyn ExecutionPlan>) -> Option<Vec<Arc<dyn PhysicalExpr>>> {
    let projection = plan.downcast_ref::<ProjectionExec>()?;
    let input = projection.input();
    let input_row = if input.is::<PlaceholderRowExec>() {
        // A placeholder emits one row per partition, and its columns are not
        // literals.
        if input.output_partitioning().partition_count() != 1 {
            return None;
        }
        Vec::new()
    } else {
        constant_row(input)?
    };
    // The logical simplifier already folds constant expressions, but those over a
    // value `AggregateStatistics` folded, such as `min(ts) + 1`, are only constant
    // now. Volatile expressions and ones that fail to evaluate are not folded, and
    // are rejected: the filter would evaluate them for every row instead of once.
    let schema = input.schema();
    let simplifier = PhysicalExprSimplifier::new(&schema);
    projection
        .expr()
        .iter()
        .map(|ProjectionExpr { expr, .. }| {
            let expr = Arc::clone(expr)
                .transform_up(|expr| {
                    let value = expr
                        .downcast_ref::<Column>()
                        .and_then(|column| input_row.get(column.index()));
                    Ok(match value {
                        Some(value) => Transformed::yes(Arc::clone(value)),
                        None => Transformed::no(expr),
                    })
                })
                .data()
                .and_then(|expr| simplifier.simplify(expr))
                .ok()?;
            expr.is::<Literal>().then_some(expr)
        })
        .collect()
}

/// Returns a column reference for each field of `plan`.
fn columns(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn PhysicalExpr>> {
    plan.schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| Arc::new(Column::new(field.name(), index)) as _)
        .collect()
}
