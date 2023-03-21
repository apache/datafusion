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

use crate::analyzer::AnalyzerRule;
use crate::rewrite::TreeNodeRewritable;
use arrow::datatypes::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::expr_rewriter::rewrite_expr;
use datafusion_expr::utils::find_exprs_in_expr;
use datafusion_expr::{
    aggregate_function, bitwise_and, bitwise_shift_right, cast, col, lit, Filter,
    GroupingSet, Sort,
};
use datafusion_expr::{Aggregate, Expr, LogicalPlan};

use datafusion_common::{Column, DataFusionError, Result};

use hashbrown::HashSet;

pub struct ReplaceGroupingFunc;

impl ReplaceGroupingFunc {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

const INTERNAL_GROUPING_COLUMN: &str = "_grouping_id";

impl AnalyzerRule for ReplaceGroupingFunc {
    fn analyze(
        &self,
        plan: &LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<LogicalPlan> {
        plan.clone().transform_up(&|plan| match plan {
            LogicalPlan::Aggregate(Aggregate {
                input,
                aggr_expr,
                group_expr,
                ..
            }) if contains_grouping_funcs_in_exprs(&aggr_expr) => {
                let gid_column = Column {
                    relation: None,
                    name: INTERNAL_GROUPING_COLUMN.to_owned(),
                };
                let distinct_group_by = distinct_group_exprs(&group_expr);
                let new_agg_expr = aggr_expr
                    .into_iter()
                    .map(|expr| {
                        replace_grouping_func(
                            expr,
                            &distinct_group_by,
                            gid_column.clone(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Some(LogicalPlan::Aggregate(Aggregate::try_new(
                    input,
                    group_expr,
                    new_agg_expr,
                )?)))
            }
            LogicalPlan::Filter(Filter { predicate, .. })
                if contains_grouping_funcs(&predicate) =>
            {
                Ok(None)
            }
            LogicalPlan::Sort(Sort { expr, .. })
                if contains_grouping_funcs_in_exprs(&expr) =>
            {
                Ok(None)
            }
            _ => Ok(None),
        })
    }
    fn name(&self) -> &str {
        "replace_grouping_func"
    }
}

pub fn distinct_group_exprs(group_expr: &[Expr]) -> Vec<Expr> {
    let mut dedup_expr = Vec::new();
    let mut dedup_set = HashSet::new();
    group_expr.iter().for_each(|expr| match expr {
        Expr::GroupingSet(grouping_set) => match grouping_set {
            GroupingSet::Rollup(exprs) => exprs.iter().for_each(|e| {
                if !dedup_set.contains(e) {
                    dedup_expr.push(e.clone());
                    dedup_set.insert(e.clone());
                }
            }),
            GroupingSet::Cube(exprs) => exprs.iter().for_each(|e| {
                if !dedup_set.contains(e) {
                    dedup_expr.push(e.clone());
                    dedup_set.insert(e.clone());
                }
            }),
            GroupingSet::GroupingSets(groups) => groups.iter().flatten().for_each(|e| {
                if !dedup_set.contains(e) {
                    dedup_expr.push(e.clone());
                    dedup_set.insert(e.clone());
                }
            }),
        },
        _ => {
            if !dedup_set.contains(expr) {
                dedup_expr.push(expr.clone());
                dedup_set.insert(expr.clone());
            }
        }
    });
    dedup_expr
}

fn contains_grouping_funcs_in_exprs(aggr_expr: &[Expr]) -> bool {
    aggr_expr.iter().any(|expr| {
        !find_exprs_in_expr(expr, &|nested_expr| {
            matches!(
                nested_expr,
                Expr::AggregateFunction(AggregateFunction {
                    fun: aggregate_function::AggregateFunction::Grouping,
                    ..
                }) | Expr::AggregateFunction(AggregateFunction {
                    fun: aggregate_function::AggregateFunction::GroupingID,
                    ..
                })
            )
        })
        .is_empty()
    })
}

fn contains_grouping_funcs(expr: &Expr) -> bool {
    !find_exprs_in_expr(expr, &|nested_expr| {
        matches!(
            nested_expr,
            Expr::AggregateFunction(AggregateFunction {
                fun: aggregate_function::AggregateFunction::Grouping,
                ..
            }) | Expr::AggregateFunction(AggregateFunction {
                fun: aggregate_function::AggregateFunction::GroupingID,
                ..
            })
        )
    })
    .is_empty()
}

fn replace_grouping_func(
    expr: Expr,
    group_by_exprs: &[Expr],
    gid_column: Column,
) -> Result<Expr> {
    rewrite_expr(expr, |expr| {
        let display_name = expr.display_name()?;
        match expr {
            Expr::AggregateFunction(AggregateFunction {
                fun: aggregate_function::AggregateFunction::Grouping,
                args,
                ..
            }) => {
                let grouping_col = &args[0];
                match group_by_exprs.iter().position(|e| e == grouping_col) {
                    Some(idx) => Ok(cast(
                        bitwise_and(
                            bitwise_shift_right(
                                col(gid_column.clone()),
                                lit((group_by_exprs.len() - 1 - idx) as u32),
                            ),
                            lit(1),
                        ),
                        DataType::Binary,
                    ).alias(display_name)),
                    None => Err(DataFusionError::Plan(format!(
                        "Column of GROUPING({:?}) can't be found in GROUP BY columns {:?}",
                        grouping_col, group_by_exprs
                    ))),
                }
            }
            Expr::AggregateFunction(AggregateFunction {
                fun: aggregate_function::AggregateFunction::GroupingID,
                args,
                ..
            }) => {
                if group_by_exprs.is_empty()
                    || (group_by_exprs.len() == args.len()
                        && group_by_exprs.iter().zip(args.iter()).all(|(g, a)| g == a))
                {
                    Ok(col(gid_column.clone()).alias(display_name))
                } else {
                    Err(DataFusionError::Plan(format!(
                        "Columns of GROUPING_ID({:?})  does not match GROUP BY columns {:?}",
                        args, group_by_exprs
                    )))
                }
            }
            _ => Ok(expr),
        }
    })
}
