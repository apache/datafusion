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

//! [`MultiDistinctToCrossJoin`] splits multiple distinct aggregates on
//! different columns into separate aggregates joined by a cross join,
//! improving memory locality by executing each distinct aggregate sequentially.

use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::tree_node::Transformed;
use datafusion_common::{HashSet, Result};
use datafusion_expr::expr::AggregateFunctionParams;
use datafusion_expr::expr_fn::ident;
use datafusion_expr::{
    Expr, LogicalPlanBuilder,
    expr::AggregateFunction,
    logical_plan::{Aggregate, LogicalPlan},
};

/// Optimizer rule that rewrites queries with multiple distinct aggregates on
/// different columns (without GROUP BY) into a cross join of individual
/// aggregates. Each aggregate runs sequentially to completion, improving
/// memory locality since only one hash table is live at a time.
///
/// ```text
/// Before:
///   SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) FROM t
///
/// After:
///   SELECT count(DISTINCT a), count(DISTINCT b)
///   FROM (SELECT COUNT(DISTINCT a) FROM t)
///   CROSS JOIN (SELECT COUNT(DISTINCT b) FROM t)
/// ```
#[derive(Default, Debug)]
pub struct MultiDistinctToCrossJoin {}

impl MultiDistinctToCrossJoin {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Returns true if all aggregate expressions are distinct aggregates on
/// different fields, with no filters or order_by, and no GROUP BY.
fn is_multi_distinct_agg(group_expr: &[Expr], aggr_expr: &[Expr]) -> bool {
    // Must have no GROUP BY and at least 2 aggregates
    if !group_expr.is_empty() || aggr_expr.len() < 2 {
        return false;
    }

    let mut fields_set = HashSet::new();
    for expr in aggr_expr {
        if let Expr::AggregateFunction(AggregateFunction {
            params:
                AggregateFunctionParams {
                    distinct,
                    args,
                    filter,
                    order_by,
                    null_treatment: _,
                },
            ..
        }) = expr
        {
            // Must be distinct, no filter, no order_by, single arg
            if !distinct || filter.is_some() || !order_by.is_empty() || args.len() != 1 {
                return false;
            }
            // Each distinct aggregate must be on a different field
            if !fields_set.insert(&args[0]) {
                return false;
            }
        } else {
            return false;
        }
    }

    true
}

impl OptimizerRule for MultiDistinctToCrossJoin {
    fn name(&self) -> &str {
        "multi_distinct_to_cross_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(Aggregate {
            input,
            aggr_expr,
            schema,
            group_expr,
            ..
        }) = plan
        else {
            return Ok(Transformed::no(plan));
        };

        if !is_multi_distinct_agg(&group_expr, &aggr_expr) {
            return Ok(Transformed::no(LogicalPlan::Aggregate(
                Aggregate::try_new_with_schema(
                    input,
                    group_expr,
                    aggr_expr,
                    schema,
                )?,
            )));
        }

        // Build individual aggregates and cross join them.
        // Use into_iter to avoid cloning each Expr.
        let n = aggr_expr.len();
        let mut builder: Option<LogicalPlanBuilder> = None;
        let mut projection_exprs = Vec::with_capacity(n);

        for (idx, expr) in aggr_expr.into_iter().enumerate() {
            let single_agg = LogicalPlan::Aggregate(Aggregate::try_new(
                Arc::clone(&input),
                vec![],
                vec![expr],
            )?);

            // Reference the single output column by name
            let agg_col = ident(single_agg.schema().field(0).name());

            // Alias to preserve original schema names
            let (qualifier, original_field) = schema.qualified_field(idx);
            projection_exprs.push(
                agg_col.alias_qualified(qualifier.cloned(), original_field.name()),
            );

            builder = Some(match builder {
                None => LogicalPlanBuilder::from(single_agg),
                Some(b) => b.cross_join(single_agg)?,
            });
        }

        let result = builder.unwrap().project(projection_exprs)?.build()?;

        Ok(Transformed::yes(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_optimized_plan_eq_display_indent_snapshot;
    use crate::test::*;
    use datafusion_expr::{col, logical_plan::builder::LogicalPlanBuilder};
    use datafusion_functions_aggregate::expr_fn::count_distinct;
    use datafusion_functions_aggregate::sum::sum_distinct;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rule: Arc<dyn crate::OptimizerRule + Send + Sync> = Arc::new(MultiDistinctToCrossJoin::new());
            assert_optimized_plan_eq_display_indent_snapshot!(
                rule,
                $plan,
                @ $expected,
            )
        }};
    }

    #[test]
    fn multi_distinct_count_two_cols() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                Vec::<Expr>::new(),
                vec![count_distinct(col("a")), count_distinct(col("b"))],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Projection: count(DISTINCT test.a) AS count(DISTINCT test.a), count(DISTINCT test.b) AS count(DISTINCT test.b) [count(DISTINCT test.a):Int64, count(DISTINCT test.b):Int64]
          Cross Join: [count(DISTINCT test.a):Int64, count(DISTINCT test.b):Int64]
            Aggregate: groupBy=[[]], aggr=[[count(DISTINCT test.a)]] [count(DISTINCT test.a):Int64]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            Aggregate: groupBy=[[]], aggr=[[count(DISTINCT test.b)]] [count(DISTINCT test.b):Int64]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn multi_distinct_mixed_agg_types() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                Vec::<Expr>::new(),
                vec![count_distinct(col("a")), sum_distinct(col("b"))],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Projection: count(DISTINCT test.a) AS count(DISTINCT test.a), sum(DISTINCT test.b) AS sum(DISTINCT test.b) [count(DISTINCT test.a):Int64, sum(DISTINCT test.b):UInt64;N]
          Cross Join: [count(DISTINCT test.a):Int64, sum(DISTINCT test.b):UInt64;N]
            Aggregate: groupBy=[[]], aggr=[[count(DISTINCT test.a)]] [count(DISTINCT test.a):Int64]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            Aggregate: groupBy=[[]], aggr=[[sum(DISTINCT test.b)]] [sum(DISTINCT test.b):UInt64;N]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }
}
