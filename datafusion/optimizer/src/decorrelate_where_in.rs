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

use crate::utils::split_conjunction;
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_expr::logical_plan::{Filter, JoinType, Subquery};
use datafusion_expr::{combine_filters, Expr, LogicalPlan, LogicalPlanBuilder};
use std::sync::Arc;
use itertools::{Either, Itertools};
use datafusion_common::Column;

#[derive(Default)]
pub struct DecorrelateWhereIn {}

impl DecorrelateWhereIn {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for DecorrelateWhereIn {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> datafusion_common::Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(Filter { predicate, input: filter_input }) => {
                // Apply optimizer rule to current input
                let optimized_input = self.optimize(filter_input, optimizer_config)?;

                let (subqueries, others) = extract_subquery_exprs(predicate);
                let optimized_plan = LogicalPlan::Filter(Filter {
                    predicate: predicate.clone(),
                    input: Arc::new(optimized_input),
                });
                if subqueries.is_empty() {
                    // regular filter, no subquery exists clause here
                    return Ok(optimized_plan);
                }

                // iterate through all exists clauses in predicate, turning each into a join
                let mut cur_input = (**filter_input).clone();
                for subquery in subqueries {
                    let (expr, subquery, negated) = subquery;
                    let res = optimize_where_in(
                        expr,
                        subquery,
                        negated,
                        &cur_input,
                        &others,
                    )?;
                    if let Some(res) = res {
                        cur_input = res
                    }
                }
                Ok(cur_input)
            }
            _ => {
                // Apply the optimization to all inputs of the plan
                utils::optimize_children(self, plan, optimizer_config)
            }
        }
    }

    fn name(&self) -> &str {
        "decorrelate_where_in"
    }
}

fn optimize_where_in(
    in_expr: Expr,
    subquery: Subquery,
    negated: bool,
    filter_input: &LogicalPlan,
    outer_others: &[Expr],
) -> datafusion_common::Result<Option<LogicalPlan>> {
    // where in queries should always project a single expression
    let proj = match &*subquery.subquery {
        LogicalPlan::Projection(it) => it,
        _ => return Ok(None),
    };
    let sub_input = proj.input.clone();
    let proj = match proj.expr.as_slice() {
        [it] => it,
        _ => return Ok(None), // in subquery means only 1 expr
    };
    let outer_col = match proj {
        Expr::Column(it) => Column::from(it.flat_name().as_str()),
        _ => return Ok(None), // only operate on columns for now, not arbitrary expressions
    };

    // Grab column names to join on
    let subqry_col = match in_expr {
        Expr::Column(it) => Column::from(it.flat_name().as_str()),
        _ => return Ok(None), // only operate on columns for now, not arbitrary expressions
    };

    // build right side of join - the thing the subquery was querying
    let subqry_plan = LogicalPlanBuilder::from((*sub_input).clone());
    let subqry_plan = subqry_plan.build()?;

    let join_keys = (vec![subqry_col], vec![outer_col]);

    // join our sub query into the main plan
    let new_plan = LogicalPlanBuilder::from(filter_input.clone());
    let new_plan = if negated {
        new_plan.join(&subqry_plan, JoinType::Anti, join_keys, None)?
    } else {
        new_plan.join(&subqry_plan, JoinType::Semi, join_keys, None)?
    };
    let new_plan = if let Some(expr) = combine_filters(outer_others) {
        new_plan.filter(expr)? // if the main query had additional expressions, restore them
    } else {
        new_plan
    };

    let result = new_plan.build()?;
    Ok(Some(result))
}

/// Finds expressions that have a where in subquery
///
/// # Arguments
///
/// * `predicate` - A conjunction to split and search
///
/// Returns a tuple of tuples ((expressions, subqueries, negated), remaining expressions)
pub fn extract_subquery_exprs(predicate: &Expr) -> (Vec<(Expr, Subquery, bool)>, Vec<Expr>) {
    let mut filters = vec![];
    split_conjunction(predicate, &mut filters);

    let (subqueries, others): (Vec<_>, Vec<_>) =
        filters.iter().partition_map(|f| match f {
            Expr::InSubquery { expr, subquery, negated} => {
                Either::Left(((**expr).clone(), subquery.clone(), *negated))
            }
            _ => Either::Right((*f).clone()),
        });
    (subqueries, others)
}

pub fn extract_subqueries(predicate: &Expr) -> Vec<Subquery> {
    let mut filters = vec![];
    split_conjunction(predicate, &mut filters);

    let subqueries = filters.iter().fold(vec![], |mut acc, expr| {
        match expr {
            Expr::InSubquery {
                expr: _,
                subquery,
                negated: _,
            } => acc.push(subquery.clone()),
            _ => {}
        }
        acc
    });
    return subqueries;
}
