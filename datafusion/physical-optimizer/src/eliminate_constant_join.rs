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

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{JoinSide, JoinType, internal_datafusion_err};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{Column, LambdaExpr, Literal};
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::NestedLoopJoinExec;
use datafusion_physical_plan::joins::utils::ColumnIndex;
use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::PhysicalOptimizerRule;

/// Rewrites an inner [`NestedLoopJoinExec`] whose input provably produces one row
/// of literals into a [`FilterExec`] over the other input, with the literals
/// substituted into the join filter, under a [`ProjectionExec`] that restores the
/// join's output.
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
            let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>() else {
                return Ok(Transformed::no(plan));
            };
            Ok(match eliminate(join)? {
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

/// Rewrites `join` over its non-constant input, if the other input is a constant row.
fn eliminate(join: &NestedLoopJoinExec) -> Result<Option<Arc<dyn ExecutionPlan>>> {
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
    // Each input's columns as expressions over the non-constant input.
    let (left, right, input) = if let Some(row) = constant_row(join.left()) {
        (row, columns(join.right()), join.right())
    } else if let Some(row) = constant_row(join.right()) {
        (columns(join.left()), row, join.left())
    } else {
        return Ok(None);
    };

    let mut input = Arc::clone(input);
    if let Some(filter) = join.filter() {
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
    if let Some(projection) = join.projection() {
        outputs = projection
            .iter()
            .map(|&index| outputs.get(index).cloned())
            .collect::<Option<_>>()
            .ok_or_else(|| internal_datafusion_err!("Join projection out of range"))?;
    }
    let schema = join.schema();
    let exprs = outputs
        .into_iter()
        .zip(schema.fields())
        .map(|(expr, field)| ProjectionExpr {
            expr,
            alias: field.name().clone(),
        });
    // Keep the join's metadata, which may come from the constant input.
    Ok(Some(Arc::new(
        ProjectionExec::try_new_with_schema_metadata(exprs, input, &schema)?,
    )))
}

/// Returns the literal values of the one row `plan` produces, if it is a stack of
/// projections over a single-partition [`PlaceholderRowExec`] that project only
/// literals and columns of the projection below.
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
    projection
        .expr()
        .iter()
        .map(|ProjectionExpr { expr, .. }| {
            if expr.is::<Literal>() {
                return Some(Arc::clone(expr));
            }
            let column = expr.downcast_ref::<Column>()?;
            input_row.get(column.index()).cloned()
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
