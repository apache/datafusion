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

//! Analyzed rule to replace TableScan references
//! such as DataFrames and Views and inlines the LogicalPlan.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use crate::analyzer::AnalyzerRule;

use arrow::datatypes::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{
    internal_datafusion_err, plan_err, Column, DFSchemaRef, Result, ScalarValue,
};
use datafusion_expr::expr::{AggregateFunction, Alias};
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::utils::grouping_set_to_exprlist;
use datafusion_expr::{
    bitwise_and, bitwise_or, bitwise_shift_left, bitwise_shift_right, cast, Aggregate,
    Expr, Projection,
};
use itertools::Itertools;

/// Replaces grouping aggregation function with value derived from internal grouping id
#[derive(Default, Debug)]
pub struct ResolveGroupingFunction;

impl ResolveGroupingFunction {
    pub fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for ResolveGroupingFunction {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(analyze_internal).data()
    }

    fn name(&self) -> &str {
        "resolve_grouping_function"
    }
}

/// Create a map from grouping expr to index in the internal grouping id.
///
/// For more details on how the grouping id bitmap works the documentation for
/// [[Aggregate::INTERNAL_GROUPING_ID]]
fn group_expr_to_bitmap_index(group_expr: &[Expr]) -> Result<HashMap<&Expr, usize>> {
    Ok(grouping_set_to_exprlist(group_expr)?
        .into_iter()
        .rev()
        .enumerate()
        .map(|(idx, v)| (v, idx))
        .collect::<HashMap<_, _>>())
}

fn replace_grouping_exprs(
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
    group_expr: Vec<Expr>,
    aggr_expr: Vec<Expr>,
) -> Result<LogicalPlan> {
    // Create HashMap from Expr to index in the grouping_id bitmap
    let is_grouping_set = matches!(group_expr.as_slice(), [Expr::GroupingSet(_)]);
    let group_expr_to_bitmap_index = group_expr_to_bitmap_index(&group_expr)?;
    let columns = schema.columns();
    let mut new_agg_expr = Vec::new();
    let mut projection_exprs = Vec::new();
    let grouping_id_len = if is_grouping_set { 1 } else { 0 };
    let group_expr_len = columns.len() - aggr_expr.len() - grouping_id_len;
    projection_exprs.extend(
        columns
            .iter()
            .take(group_expr_len)
            .map(|column| Expr::Column(column.clone())),
    );
    for (expr, column) in aggr_expr
        .into_iter()
        .zip(columns.into_iter().skip(group_expr_len + grouping_id_len))
    {
        match expr {
            Expr::AggregateFunction(ref function) if is_grouping_function(&expr) => {
                let grouping_expr = grouping_function_on_id(
                    function,
                    &group_expr_to_bitmap_index,
                    is_grouping_set,
                )?;
                projection_exprs.push(Expr::Alias(Alias::new(
                    grouping_expr,
                    column.relation,
                    column.name,
                )));
            }
            _ => {
                projection_exprs.push(Expr::Column(column));
                new_agg_expr.push(expr);
            }
        }
    }
    // Recreate aggregate without grouping functions
    let new_aggregate =
        LogicalPlan::Aggregate(Aggregate::try_new(input, group_expr, new_agg_expr)?);
    // Create projection with grouping functions calculations
    let projection = LogicalPlan::Projection(Projection::try_new(
        projection_exprs,
        new_aggregate.into(),
    )?);
    Ok(projection)
}

fn analyze_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    // rewrite any subqueries in the plan first
    let transformed_plan =
        plan.map_subqueries(|plan| plan.transform_up(analyze_internal))?;

    let transformed_plan = transformed_plan.transform_data(|plan| match plan {
        LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema,
            ..
        }) if contains_grouping_function(&aggr_expr) => Ok(Transformed::yes(
            replace_grouping_exprs(input, schema, group_expr, aggr_expr)?,
        )),
        _ => Ok(Transformed::no(plan)),
    })?;

    Ok(transformed_plan)
}

fn is_grouping_function(expr: &Expr) -> bool {
    // TODO: Do something better than name here should grouping be a built
    // in expression?
    matches!(expr, Expr::AggregateFunction(AggregateFunction { ref func, .. }) if func.name() == "grouping")
}

fn contains_grouping_function(exprs: &[Expr]) -> bool {
    exprs.iter().any(is_grouping_function)
}

/// Validate that the arguments to the grouping function are in the group by clause.
fn validate_args(
    function: &AggregateFunction,
    group_by_expr: &HashMap<&Expr, usize>,
) -> Result<()> {
    let expr_not_in_group_by = function
        .args
        .iter()
        .find(|expr| !group_by_expr.contains_key(expr));
    if let Some(expr) = expr_not_in_group_by {
        plan_err!(
            "Argument {} to grouping function is not in grouping columns {}",
            expr,
            group_by_expr.keys().map(|e| e.to_string()).join(", ")
        )
    } else {
        Ok(())
    }
}

fn grouping_function_on_id(
    function: &AggregateFunction,
    group_by_expr: &HashMap<&Expr, usize>,
    is_grouping_set: bool,
) -> Result<Expr> {
    validate_args(function, group_by_expr)?;
    let args = &function.args;

    // Postgres allows grouping function for group by without grouping sets, the result is then
    // always 0
    if !is_grouping_set {
        return Ok(Expr::Literal(ScalarValue::from(0i32)));
    }

    let group_by_expr_count = group_by_expr.len();
    let literal = |value: usize| {
        if group_by_expr_count < 8 {
            Expr::Literal(ScalarValue::from(value as u8))
        } else if group_by_expr_count < 16 {
            Expr::Literal(ScalarValue::from(value as u16))
        } else if group_by_expr_count < 32 {
            Expr::Literal(ScalarValue::from(value as u32))
        } else {
            Expr::Literal(ScalarValue::from(value as u64))
        }
    };

    let grouping_id_column = Expr::Column(Column::from(Aggregate::INTERNAL_GROUPING_ID));
    // The grouping call is exactly our internal grouping id
    if args.len() == group_by_expr_count
        && args
            .iter()
            .rev()
            .enumerate()
            .all(|(idx, expr)| group_by_expr.get(expr) == Some(&idx))
    {
        return Ok(cast(grouping_id_column, DataType::Int32));
    }

    args.iter()
        .rev()
        .enumerate()
        .map(|(arg_idx, expr)| {
            group_by_expr.get(expr).map(|group_by_idx| {
                let group_by_bit =
                    bitwise_and(grouping_id_column.clone(), literal(1 << group_by_idx));
                match group_by_idx.cmp(&arg_idx) {
                    Ordering::Less => {
                        bitwise_shift_left(group_by_bit, literal(arg_idx - group_by_idx))
                    }
                    Ordering::Greater => {
                        bitwise_shift_right(group_by_bit, literal(group_by_idx - arg_idx))
                    }
                    Ordering::Equal => group_by_bit,
                }
            })
        })
        .collect::<Option<Vec<_>>>()
        .and_then(|bit_exprs| {
            bit_exprs
                .into_iter()
                .reduce(bitwise_or)
                .map(|expr| cast(expr, DataType::Int32))
        })
        .ok_or_else(|| {
            internal_datafusion_err!("Grouping sets should contains at least one element")
        })
}
