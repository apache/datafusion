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

use crate::utils::{exprs_to_join_cols, find_join_exprs, split_conjunction};
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_expr::logical_plan::{Filter, JoinType, Subquery};
use datafusion_expr::{combine_filters, Expr, LogicalPlan, LogicalPlanBuilder};
use log::warn;
use std::sync::Arc;

/// Optimizer rule for rewriting subquery filters to joins
#[derive(Default)]
pub struct DecorrelateWhereExists {}

impl DecorrelateWhereExists {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }

    /// Finds expressions that have an exists subquery in them
    ///
    /// # Arguments
    ///
    /// * `predicate` - A conjunction to split and search
    ///
    /// Returns a tuple of tuples ((subquery expressions, negated), remaining expressions)
    fn extract_subquery_exprs(
        &self,
        predicate: &Expr,
        optimizer_config: &mut OptimizerConfig,
    ) -> datafusion_common::Result<(Vec<SubqueryInfo>, Vec<Expr>)> {
        let mut filters = vec![];
        split_conjunction(predicate, &mut filters);

        let mut subqueries = vec![];
        let mut others = vec![];
        for it in filters.iter() {
            match it {
                Expr::Exists { subquery, negated } => {
                    let subquery =
                        self.optimize(&*subquery.subquery, optimizer_config)?;
                    let subquery = Arc::new(subquery);
                    let subquery = Subquery { subquery };
                    let subquery = SubqueryInfo::new(subquery.clone(), *negated);
                    subqueries.push(subquery);
                }
                _ => others.push((*it).clone()),
            }
        }

        Ok((subqueries, others))
    }
}

impl OptimizerRule for DecorrelateWhereExists {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> datafusion_common::Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(Filter {
                predicate,
                input: filter_input,
            }) => {
                // Apply optimizer rule to current input
                let optimized_input = self.optimize(filter_input, optimizer_config)?;

                let (subqueries, other_exprs) =
                    self.extract_subquery_exprs(predicate, optimizer_config)?;
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
                    let res = optimize_exists(&subquery, &cur_input, &other_exprs)?;
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
        "decorrelate_where_exists"
    }
}

/// Takes a query like:
///
/// ```select c.id from customers c where exists (select * from orders o where o.c_id = c.id)```
///
/// and optimizes it into:
///
/// ```select c.id from customers c
/// inner join (select o.c_id from orders o group by o.c_id) o on o.c_id = c.c_id```
///
/// # Arguments
///
/// * subqry - The subquery portion of the `where exists` (select * from orders)
/// * negated - True if the subquery is a `where not exists`
/// * filter_input - The non-subquery portion (from customers)
/// * outer_exprs - Any additional parts to the `where` expression (and c.x = y)
fn optimize_exists(
    query_info: &SubqueryInfo,
    outer_input: &LogicalPlan,
    outer_other_exprs: &[Expr],
) -> datafusion_common::Result<Option<LogicalPlan>> {
    // Only operate if there is one input
    let subqry_inputs = query_info.query.subquery.inputs();
    let subqry_input = match subqry_inputs.as_slice() {
        [it] => it,
        _ => {
            warn!("Filter with multiple inputs during where exists!");
            return Ok(None); // where exists is a filter, not a join, so 1 input only
        }
    };

    // Only operate on subqueries that are trying to filter on an expression from an outer query
    let subqry_filter = match subqry_input {
        LogicalPlan::Filter(f) => f,
        _ => return Ok(None), // Not correlated - TODO: also handle this case
    };

    // split into filters
    let mut subqry_filter_exprs = vec![];
    split_conjunction(&subqry_filter.predicate, &mut subqry_filter_exprs);

    // Grab column names to join on
    let (col_exprs, other_subqry_exprs) =
        find_join_exprs(subqry_filter_exprs, subqry_filter.input.schema());
    let (outer_cols, subqry_cols, join_filters) =
        exprs_to_join_cols(&col_exprs, subqry_filter.input.schema(), false)?;
    if subqry_cols.is_empty() || outer_cols.is_empty() {
        return Ok(None); // not correlated
    }

    // build subquery side of join - the thing the subquery was querying
    let subqry_plan = LogicalPlanBuilder::from((*subqry_filter.input).clone());
    let subqry_plan = if let Some(expr) = combine_filters(&other_subqry_exprs) {
        subqry_plan.filter(expr)? // if the subquery had additional expressions, restore them
    } else {
        subqry_plan
    };
    let subqry_plan = subqry_plan.build()?;

    let join_keys = (subqry_cols, outer_cols);

    // join our sub query into the main plan
    let new_plan = LogicalPlanBuilder::from(outer_input.clone());
    let new_plan = if query_info.negated {
        new_plan.join(&subqry_plan, JoinType::Anti, join_keys, join_filters)?
    } else {
        new_plan.join(&subqry_plan, JoinType::Semi, join_keys, join_filters)?
    };
    let new_plan = if let Some(expr) = combine_filters(outer_other_exprs) {
        new_plan.filter(expr)? // if the main query had additional expressions, restore them
    } else {
        new_plan
    };

    let result = new_plan.build()?;
    Ok(Some(result))
}

struct SubqueryInfo {
    query: Subquery,
    negated: bool,
}

impl SubqueryInfo {
    pub fn new(query: Subquery, negated: bool) -> Self {
        Self { query, negated }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_common::{Column, Result};
    use datafusion_expr::{col, exists, logical_plan::LogicalPlanBuilder, Operator};

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = DecorrelateWhereExists::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{}", optimized_plan.display_indent_schema());
        assert_eq!(formatted_plan, expected);
    }

    /// Test for correlated EXISTS subquery filter
    #[test]
    fn exists_subquery_correlated() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
                .filter(col("test.a").eq(col("sq.a")))?
                .project(vec![col("c")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("test")?)
            .filter(exists(sq))?
            .project(vec![col("test.c")])?
            .build()?;

        let input = r#"Projection: #test.c
  Filter: EXISTS (Subquery: Projection: #sq.c
  Filter: #test.a = #sq.a
    TableScan: sq)
    TableScan: test"#;
        assert_eq!(format!("{}", plan.display_indent()), input);

        let expected = r#"Projection: #test.c [c:UInt32]
  Semi Join: #test.a = #sq.a [a:UInt32, b:UInt32, c:UInt32]
    TableScan: test [a:UInt32, b:UInt32, c:UInt32]
    TableScan: sq [a:UInt32, b:UInt32, c:UInt32]"#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }
}
