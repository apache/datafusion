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

use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::{
    col,
    logical_plan::{Aggregate, LogicalPlan, Projection},
    utils::{columnize_expr, from_plan},
    Expr, ExprSchemable,
};
use hashbrown::HashSet;
use std::sync::Arc;

/// single distinct to group by optimizer rule
///  ```text
///    SELECT F1(DISTINCT s),F2(DISTINCT s)
///    ...
///    GROUP BY k
///
///    Into
///
///    SELECT F1(alias1),F2(alias1)
///    FROM (
///      SELECT s as alias1, k ... GROUP BY s, k
///    )
///    GROUP BY k
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

fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Aggregate(Aggregate {
            input,
            aggr_expr,
            schema,
            group_expr,
        }) => {
            if is_single_distinct_agg(plan)? && !contains_grouping_set(group_expr) {
                // alias all original group_by exprs
                let mut group_expr_alias = Vec::with_capacity(group_expr.len());
                let mut inner_group_exprs = group_expr
                    .iter()
                    .enumerate()
                    .map(|(i, group_expr)| {
                        let alias_str = format!("group_alias_{}", i);
                        let alias_expr = group_expr.clone().alias(&alias_str);
                        group_expr_alias.push((alias_str, schema.fields()[i].clone()));
                        alias_expr
                    })
                    .collect::<Vec<_>>();

                // and they can be referenced by the alias in the outer aggr plan
                let outer_group_exprs = group_expr_alias
                    .iter()
                    .map(|(alias, _)| col(alias))
                    .collect::<Vec<_>>();

                // replace the distinct arg with alias
                let mut group_fields_set = HashSet::new();
                let new_aggr_exprs = aggr_expr
                    .iter()
                    .map(|aggr_expr| match aggr_expr {
                        Expr::AggregateFunction {
                            fun, args, filter, ..
                        } => {
                            // is_single_distinct_agg ensure args.len=1
                            if group_fields_set.insert(args[0].name()?) {
                                inner_group_exprs
                                    .push(args[0].clone().alias(SINGLE_DISTINCT_ALIAS));
                            }
                            Ok(Expr::AggregateFunction {
                                fun: fun.clone(),
                                args: vec![col(SINGLE_DISTINCT_ALIAS)],
                                distinct: false, // intentional to remove distinct here
                                filter: filter.clone(),
                            })
                        }
                        _ => Ok(aggr_expr.clone()),
                    })
                    .collect::<Result<Vec<_>>>()?;

                // construct the inner AggrPlan
                let inner_fields = inner_group_exprs
                    .iter()
                    .map(|expr| expr.to_field(input.schema()))
                    .collect::<Result<Vec<_>>>()?;
                let inner_schema = DFSchema::new_with_metadata(
                    inner_fields,
                    input.schema().metadata().clone(),
                )?;
                let grouped_aggr = LogicalPlan::Aggregate(Aggregate::try_new(
                    input.clone(),
                    inner_group_exprs,
                    Vec::new(),
                    Arc::new(inner_schema.clone()),
                )?);
                let inner_agg = optimize_children(&grouped_aggr)?;

                let outer_aggr_schema = Arc::new(DFSchema::new_with_metadata(
                    outer_group_exprs
                        .iter()
                        .chain(new_aggr_exprs.iter())
                        .map(|expr| expr.to_field(&inner_schema))
                        .collect::<Result<Vec<_>>>()?,
                    input.schema().metadata().clone(),
                )?);

                // so the aggregates are displayed in the same way even after the rewrite
                // this optimizer has two kinds of alias:
                // - group_by aggr
                // - aggr expr
                let mut alias_expr: Vec<Expr> = Vec::new();
                for (alias, original_field) in group_expr_alias {
                    alias_expr.push(col(&alias).alias(original_field.name()));
                }
                for (i, expr) in new_aggr_exprs.iter().enumerate() {
                    alias_expr.push(columnize_expr(
                        expr.clone()
                            .alias(schema.clone().fields()[i + group_expr.len()].name()),
                        &outer_aggr_schema,
                    ));
                }

                let outer_aggr = LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::new(inner_agg),
                    outer_group_exprs,
                    new_aggr_exprs,
                    outer_aggr_schema,
                )?);

                Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                    alias_expr,
                    Arc::new(outer_aggr),
                    schema.clone(),
                    None,
                )?))
            } else {
                optimize_children(plan)
            }
        }
        _ => optimize_children(plan),
    }
}

fn optimize_children(plan: &LogicalPlan) -> Result<LogicalPlan> {
    let expr = plan.expressions();
    let inputs = plan.inputs();
    let new_inputs = inputs
        .iter()
        .map(|plan| optimize(plan))
        .collect::<Result<Vec<_>>>()?;
    from_plan(plan, &expr, &new_inputs)
}

/// Check whether all aggregate exprs are distinct on a single field.
fn is_single_distinct_agg(plan: &LogicalPlan) -> Result<bool> {
    match plan {
        LogicalPlan::Aggregate(Aggregate { aggr_expr, .. }) => {
            let mut fields_set = HashSet::new();
            let mut distinct_count = 0;
            for expr in aggr_expr {
                if let Expr::AggregateFunction { distinct, args, .. } = expr {
                    if *distinct {
                        distinct_count += 1;
                    }
                    for e in args {
                        fields_set.insert(e.name()?);
                    }
                }
            }
            let res = distinct_count == aggr_expr.len() && fields_set.len() == 1;
            Ok(res)
        }
        _ => Ok(false),
    }
}

/// Check if the first expr is [Expr::GroupingSet].
fn contains_grouping_set(expr: &[Expr]) -> bool {
    matches!(expr.first(), Some(Expr::GroupingSet(_)))
}

impl OptimizerRule for SingleDistinctToGroupBy {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        optimize(plan)
    }
    fn name(&self) -> &str {
        "single_distinct_aggregation_to_group_by"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_expr::expr::GroupingSet;
    use datafusion_expr::{
        col, count, count_distinct, lit, logical_plan::builder::LogicalPlanBuilder, max,
        AggregateFunction,
    };

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = SingleDistinctToGroupBy::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");

        let formatted_plan = format!("{}", optimized_plan.display_indent_schema());
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn not_exist_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
            .build()?;

        // Do nothing
        let expected =
            "Aggregate: groupBy=[[]], aggr=[[MAX(#test.b)]] [MAX(test.b):UInt32;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn single_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![count_distinct(col("b"))])?
            .build()?;

        // Should work
        let expected = "Projection: #COUNT(alias1) AS COUNT(DISTINCT test.b) [COUNT(DISTINCT test.b):Int64;N]\
                            \n  Aggregate: groupBy=[[]], aggr=[[COUNT(#alias1)]] [COUNT(alias1):Int64;N]\
                            \n    Aggregate: groupBy=[[#test.b AS alias1]], aggr=[[]] [alias1:UInt32]\
                            \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
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
        let expected = "Aggregate: groupBy=[[GROUPING SETS ((#test.a), (#test.b))]], aggr=[[COUNT(DISTINCT #test.c)]] [a:UInt32, b:UInt32, COUNT(DISTINCT test.c):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    // Currently this optimization is disabled for CUBE/ROLLUP/GROUPING SET
    #[test]
    fn single_distinct_and_cube() -> Result<()> {
        let table_scan = test_table_scan()?;

        let grouping_set = Expr::GroupingSet(GroupingSet::Cube(vec![col("a"), col("b")]));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![grouping_set], vec![count_distinct(col("c"))])?
            .build()?;

        println!("{:?}", plan);

        // Should not be optimized
        let expected = "Aggregate: groupBy=[[CUBE (#test.a, #test.b)]], aggr=[[COUNT(DISTINCT #test.c)]] [a:UInt32, b:UInt32, COUNT(DISTINCT test.c):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
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
        let expected = "Aggregate: groupBy=[[ROLLUP (#test.a, #test.b)]], aggr=[[COUNT(DISTINCT #test.c)]] [a:UInt32, b:UInt32, COUNT(DISTINCT test.c):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn single_distinct_expr() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![count_distinct(lit(2) * col("b"))])?
            .build()?;

        let expected = "Projection: #COUNT(alias1) AS COUNT(DISTINCT Int32(2) * test.b) [COUNT(DISTINCT Int32(2) * test.b):Int64;N]\
                            \n  Aggregate: groupBy=[[]], aggr=[[COUNT(#alias1)]] [COUNT(alias1):Int64;N]\
                            \n    Aggregate: groupBy=[[Int32(2) * #test.b AS alias1]], aggr=[[]] [alias1:Int32]\
                            \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn single_distinct_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![count_distinct(col("b"))])?
            .build()?;

        // Should work
        let expected = "Projection: #group_alias_0 AS a, #COUNT(alias1) AS COUNT(DISTINCT test.b) [a:UInt32, COUNT(DISTINCT test.b):Int64;N]\
                            \n  Aggregate: groupBy=[[#group_alias_0]], aggr=[[COUNT(#alias1)]] [group_alias_0:UInt32, COUNT(alias1):Int64;N]\
                            \n    Aggregate: groupBy=[[#test.a AS group_alias_0, #test.b AS alias1]], aggr=[[]] [group_alias_0:UInt32, alias1:UInt32]\
                            \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
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
        let expected = "Aggregate: groupBy=[[#test.a]], aggr=[[COUNT(DISTINCT #test.b), COUNT(DISTINCT #test.c)]] [a:UInt32, COUNT(DISTINCT test.b):Int64;N, COUNT(DISTINCT test.c):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn one_field_two_distinct_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    count_distinct(col("b")),
                    Expr::AggregateFunction {
                        fun: AggregateFunction::Max,
                        distinct: true,
                        args: vec![col("b")],
                        filter: None,
                    },
                ],
            )?
            .build()?;
        // Should work
        let expected = "Projection: #group_alias_0 AS a, #COUNT(alias1) AS COUNT(DISTINCT test.b), #MAX(alias1) AS MAX(DISTINCT test.b) [a:UInt32, COUNT(DISTINCT test.b):Int64;N, MAX(DISTINCT test.b):UInt32;N]\
                            \n  Aggregate: groupBy=[[#group_alias_0]], aggr=[[COUNT(#alias1), MAX(#alias1)]] [group_alias_0:UInt32, COUNT(alias1):Int64;N, MAX(alias1):UInt32;N]\
                            \n    Aggregate: groupBy=[[#test.a AS group_alias_0, #test.b AS alias1]], aggr=[[]] [group_alias_0:UInt32, alias1:UInt32]\
                            \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
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

        // Do nothing
        let expected = "Aggregate: groupBy=[[#test.a]], aggr=[[COUNT(DISTINCT #test.b), COUNT(#test.c)]] [a:UInt32, COUNT(DISTINCT test.b):Int64;N, COUNT(test.c):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn group_by_with_expr() {
        let table_scan = test_table_scan().unwrap();

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a") + lit(1)], vec![count_distinct(col("c"))])
            .unwrap()
            .build()
            .unwrap();

        // Should work
        let expected = "Projection: #group_alias_0 AS test.a + Int32(1), #COUNT(alias1) AS COUNT(DISTINCT test.c) [test.a + Int32(1):Int32, COUNT(DISTINCT test.c):Int64;N]\
                            \n  Aggregate: groupBy=[[#group_alias_0]], aggr=[[COUNT(#alias1)]] [group_alias_0:Int32, COUNT(alias1):Int64;N]\
                            \n    Aggregate: groupBy=[[#test.a + Int32(1) AS group_alias_0, #test.c AS alias1]], aggr=[[]] [group_alias_0:Int32, alias1:UInt32]\
                            \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
    }
}
