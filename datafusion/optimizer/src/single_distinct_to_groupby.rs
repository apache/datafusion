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

//! single distinct to group by optimizer rule

use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::Result;
use datafusion_expr::aggregate_function::{
    self,
    AggregateFunction::{Count, Max, Min, Sum},
};
use datafusion_expr::{
    col,
    expr::AggregateFunction,
    logical_plan::{Aggregate, LogicalPlan},
    Expr,
};

use hashbrown::HashSet;

/// single distinct to group by optimizer rule
///  ```text
///    Before:
///    SELECT a, COUNT(DINSTINCT b), COUNT(c)
///    FROM t
///    GROUP BY a
///
///    After:
///    SELECT a, COUNT(alias1), SUM(alias2)
///    FROM (
///      SELECT a, b as alias1, COUNT(c) as alias2
///      FROM t
///      GROUP BY a, b
///    )
///    GROUP BY a
///  ```
#[derive(Default)]
pub struct SingleDistinctToGroupBy {}

const SINGLE_DISTINCT_ALIAS: &str = "alias1";

impl SingleDistinctToGroupBy {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Check whether all distinct aggregate exprs are distinct on a single field.
fn is_single_distinct_agg(plan: &LogicalPlan) -> Result<bool> {
    match plan {
        LogicalPlan::Aggregate(Aggregate { aggr_expr, .. }) => {
            let mut fields_set = HashSet::new();
            let mut aggregate_count = 0;
            for expr in aggr_expr {
                if let Expr::AggregateFunction(AggregateFunction {
                    fun,
                    distinct,
                    args,
                    ..
                }) = expr
                {
                    aggregate_count += 1;
                    if *distinct {
                        for e in args {
                            fields_set.insert(e.canonical_name());
                        }
                    } else if !matches!(fun, Count | Sum | Min | Max) {
                        return Ok(false);
                    }
                }
            }
            Ok(fields_set.len() == 1 && aggregate_count == aggr_expr.len())
        }
        _ => Ok(false),
    }
}

/// Check if the first expr is [Expr::GroupingSet].
fn contains_grouping_set(expr: &[Expr]) -> bool {
    matches!(expr.first(), Some(Expr::GroupingSet(_)))
}

/// Rewrite the aggregate function to two aggregate functions.
fn aggregate_function_rewrite(
    fun: &aggregate_function::AggregateFunction,
) -> (
    aggregate_function::AggregateFunction,
    aggregate_function::AggregateFunction,
) {
    match fun {
        Count => (Sum, Count),
        Sum => (Sum, Sum),
        Min => (Min, Min),
        Max => (Max, Max),
        _ => panic!(
            "Not supported aggregate function in single distinct optimization rule"
        ),
    }
}

impl OptimizerRule for SingleDistinctToGroupBy {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Aggregate(Aggregate {
                input,
                aggr_expr,
                schema,
                group_expr,
                ..
            }) => {
                if is_single_distinct_agg(plan)? && !contains_grouping_set(group_expr) {
                    let fields = schema.fields();
                    // alias all original group_by exprs
                    let (mut inner_group_exprs, out_group_expr_with_alias): (
                        Vec<Expr>,
                        Vec<(Expr, Option<String>)>,
                    ) = group_expr
                        .iter()
                        .enumerate()
                        .map(|(i, group_expr)| {
                            if let Expr::Column(_) = group_expr {
                                // For Column expressions we can use existing expression as is.
                                (group_expr.clone(), (group_expr.clone(), None))
                            } else {
                                // For complex expression write is as alias, to be able to refer
                                // if from parent operators successfully.
                                // Consider plan below.
                                //
                                // Aggregate: groupBy=[[group_alias_0]], aggr=[[COUNT(alias1)]] [group_alias_0:Int32, COUNT(alias1):Int64;N]\
                                // --Aggregate: groupBy=[[test.a + Int32(1) AS group_alias_0, test.c AS alias1]], aggr=[[]] [group_alias_0:Int32, alias1:UInt32]\
                                // ----TableScan: test [a:UInt32, b:UInt32, c:UInt32]
                                //
                                // First aggregate(from bottom) refers to `test.a` column.
                                // Second aggregate refers to the `group_alias_0` column, Which is a valid field in the first aggregate.
                                // If we were to write plan above as below without alias
                                //
                                // Aggregate: groupBy=[[test.a + Int32(1)]], aggr=[[COUNT(alias1)]] [group_alias_0:Int32, COUNT(alias1):Int64;N]\
                                // --Aggregate: groupBy=[[test.a + Int32(1), test.c AS alias1]], aggr=[[]] [group_alias_0:Int32, alias1:UInt32]\
                                // ----TableScan: test [a:UInt32, b:UInt32, c:UInt32]
                                //
                                // Second aggregate refers to the `test.a + Int32(1)` expression However, its input do not have `test.a` expression in it.
                                let alias_str = format!("group_alias_{i}");
                                let alias_expr = group_expr.clone().alias(&alias_str);
                                (
                                    alias_expr,
                                    (col(alias_str), Some(fields[i].qualified_name())),
                                )
                            }
                        })
                        .unzip();

                    // and they can be referenced by the alias in the outer aggr plan
                    let outer_group_exprs = out_group_expr_with_alias
                        .iter()
                        .map(|(out_group_expr, _)| out_group_expr.clone())
                        .collect::<Vec<_>>();

                    // replace the distinct arg with alias
                    let mut index = 1;
                    let mut group_fields_set = HashSet::new();
                    let mut inner_aggr_exprs = vec![];
                    let outer_aggr_exprs = aggr_expr
                        .iter()
                        .map(|aggr_expr| match aggr_expr {
                            Expr::AggregateFunction(AggregateFunction {
                                fun,
                                args,
                                filter,
                                order_by,
                                distinct,
                                ..
                            }) => {
                                // is_single_distinct_agg ensure args.len=1
                                if *distinct
                                    && group_fields_set.insert(args[0].display_name()?)
                                {
                                    inner_group_exprs.push(
                                        args[0].clone().alias(SINGLE_DISTINCT_ALIAS),
                                    );
                                }

                                let mut expr =
                                    Expr::AggregateFunction(AggregateFunction::new(
                                        fun.clone(),
                                        vec![col(SINGLE_DISTINCT_ALIAS)],
                                        false, // intentional to remove distinct here
                                        filter.clone(),
                                        order_by.clone(),
                                    ))
                                    .alias(aggr_expr.display_name()?);

                                // if the aggregate function is not distinct, we need to rewrite it like two phase aggregation
                                if !(*distinct) {
                                    index += 1;
                                    let alias_str = format!("alias{}", index);

                                    let (out_fun, inner_fun) =
                                        aggregate_function_rewrite(fun);
                                    expr =
                                        Expr::AggregateFunction(AggregateFunction::new(
                                            out_fun,
                                            vec![col(&alias_str)],
                                            false,
                                            filter.clone(),
                                            order_by.clone(),
                                        )).alias(aggr_expr.display_name()?);
                                    let inner_expr =
                                        Expr::AggregateFunction(AggregateFunction::new(
                                            inner_fun,
                                            args.clone(),
                                            false,
                                            filter.clone(),
                                            order_by.clone(),
                                        ))
                                        .alias(&alias_str);
                                    inner_aggr_exprs.push(inner_expr);
                                }

                                Ok(expr)
                            }
                            _ => Ok(aggr_expr.clone()),
                        })
                        .collect::<Result<Vec<_>>>()?;
                    
                    // construct the inner AggrPlan
                    let inner_agg = LogicalPlan::Aggregate(Aggregate::try_new(
                        input.clone(),
                        inner_group_exprs,
                        inner_aggr_exprs,
                    )?);

                    Ok(Some(LogicalPlan::Aggregate(Aggregate::try_new(
                        Arc::new(inner_agg),
                        outer_group_exprs,
                        outer_aggr_exprs,
                    )?)))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "single_distinct_aggregation_to_group_by"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_expr::expr;
    use datafusion_expr::expr::GroupingSet;
    use datafusion_expr::{
        col, count, count_distinct, lit, logical_plan::builder::LogicalPlanBuilder, max,
        AggregateFunction,
    };

    fn assert_optimized_plan_equal(plan: &LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq_display_indent(
            Arc::new(SingleDistinctToGroupBy::new()),
            plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn not_exist_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
            .build()?;

        // Do nothing
        let expected =
            "Aggregate: groupBy=[[]], aggr=[[MAX(test.b)]] [MAX(test.b):UInt32;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn single_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![count_distinct(col("b"))])?
            .build()?;

        // Should work
        let expected = "Aggregate: groupBy=[[]], aggr=[[COUNT(alias1) AS COUNT(DISTINCT test.b)]] [COUNT(DISTINCT test.b):Int64;N]\
                            \n  Aggregate: groupBy=[[test.b AS alias1]], aggr=[[]] [alias1:UInt32]\
                            \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
    }

    // Currently this optimization is disabled for CUBE/ROLLUP/GROUPING SET
    #[test]
    fn single_distinct_and_grouping_set() -> Result<()> {
        let table_scan = test_table_scan()?;

        let grouping_set = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("a")],
            vec![col("b")],
        ]));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![grouping_set], vec![count_distinct(col("c"))])?
            .build()?;

        // Should not be optimized
        let expected = "Aggregate: groupBy=[[GROUPING SETS ((test.a), (test.b))]], aggr=[[COUNT(DISTINCT test.c)]] [a:UInt32, b:UInt32, COUNT(DISTINCT test.c):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
    }

    // Currently this optimization is disabled for CUBE/ROLLUP/GROUPING SET
    #[test]
    fn single_distinct_and_cube() -> Result<()> {
        let table_scan = test_table_scan()?;

        let grouping_set = Expr::GroupingSet(GroupingSet::Cube(vec![col("a"), col("b")]));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![grouping_set], vec![count_distinct(col("c"))])?
            .build()?;

        // Should not be optimized
        let expected = "Aggregate: groupBy=[[CUBE (test.a, test.b)]], aggr=[[COUNT(DISTINCT test.c)]] [a:UInt32, b:UInt32, COUNT(DISTINCT test.c):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
    }

    // Currently this optimization is disabled for CUBE/ROLLUP/GROUPING SET
    #[test]
    fn single_distinct_and_rollup() -> Result<()> {
        let table_scan = test_table_scan()?;

        let grouping_set =
            Expr::GroupingSet(GroupingSet::Rollup(vec![col("a"), col("b")]));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![grouping_set], vec![count_distinct(col("c"))])?
            .build()?;

        // Should not be optimized
        let expected = "Aggregate: groupBy=[[ROLLUP (test.a, test.b)]], aggr=[[COUNT(DISTINCT test.c)]] [a:UInt32, b:UInt32, COUNT(DISTINCT test.c):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn single_distinct_expr() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![count_distinct(lit(2) * col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[COUNT(alias1) AS COUNT(DISTINCT Int32(2) * test.b)]] [COUNT(DISTINCT Int32(2) * test.b):Int64;N]\
                            \n  Aggregate: groupBy=[[Int32(2) * test.b AS alias1]], aggr=[[]] [alias1:Int32]\
                            \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn single_distinct_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![count_distinct(col("b"))])?
            .build()?;

        // Should work
        let expected = "Aggregate: groupBy=[[test.a]], aggr=[[COUNT(alias1) AS COUNT(DISTINCT test.b)]] [a:UInt32, COUNT(DISTINCT test.b):Int64;N]\
                            \n  Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[]] [a:UInt32, alias1:UInt32]\
                            \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn two_distinct_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count_distinct(col("b")), count_distinct(col("c"))],
            )?
            .build()?;

        // Do nothing
        let expected = "Aggregate: groupBy=[[test.a]], aggr=[[COUNT(DISTINCT test.b), COUNT(DISTINCT test.c)]] [a:UInt32, COUNT(DISTINCT test.b):Int64;N, COUNT(DISTINCT test.c):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn one_field_two_distinct_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    count_distinct(col("b")),
                    Expr::AggregateFunction(expr::AggregateFunction::new(
                        AggregateFunction::Max,
                        vec![col("b")],
                        true,
                        None,
                        None,
                    )),
                ],
            )?
            .build()?;
        // Should work
        let expected = "Aggregate: groupBy=[[test.a]], aggr=[[COUNT(alias1) AS COUNT(DISTINCT test.b), MAX(alias1) AS MAX(DISTINCT test.b)]] [a:UInt32, COUNT(DISTINCT test.b):Int64;N, MAX(DISTINCT test.b):UInt32;N]\
                            \n  Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[]] [a:UInt32, alias1:UInt32]\
                            \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn distinct_and_common() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count_distinct(col("b")), count(col("c"))],
            )?
            .build()?;

        // Should work
        let expected = "Aggregate: groupBy=[[test.a]], aggr=[[COUNT(alias1) AS COUNT(DISTINCT test.b), SUM(alias2) AS COUNT(test.c)]] [a:UInt32, COUNT(DISTINCT test.b):Int64;N, COUNT(test.c):Int64;N]\
                            \n  Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[COUNT(test.c) AS alias2]] [a:UInt32, alias1:UInt32, alias2:Int64;N]\
                            \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn group_by_with_expr() -> Result<()> {
        let table_scan = test_table_scan().unwrap();

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a") + lit(1)], vec![count_distinct(col("c"))])?
            .build()?;

        // Should work
        let expected = "Aggregate: groupBy=[[group_alias_0]], aggr=[[COUNT(alias1) AS COUNT(DISTINCT test.c)]] [group_alias_0:Int32, COUNT(DISTINCT test.c):Int64;N]\
                            \n  Aggregate: groupBy=[[test.a + Int32(1) AS group_alias_0, test.c AS alias1]], aggr=[[]] [group_alias_0:Int32, alias1:UInt32]\
                            \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
    }
}
