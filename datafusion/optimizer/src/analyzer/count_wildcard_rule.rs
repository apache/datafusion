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

use crate::utils::NamePreserver;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::Result;
use datafusion_expr::expr::{
    AggregateFunction, AggregateFunctionDefinition, WindowFunction,
};
use datafusion_expr::utils::COUNT_STAR_EXPANSION;
use datafusion_expr::{
    aggregate_function, lit, Expr, LogicalPlan, WindowFunctionDefinition,
};

/// Rewrite `Count(Expr:Wildcard)` to `Count(Expr:Literal)`.
///
/// Resolves issue: <https://github.com/apache/datafusion/issues/5473>
#[derive(Default)]
pub struct CountWildcardRule {}

impl CountWildcardRule {
    pub fn new() -> Self {
        CountWildcardRule {}
    }
}

impl AnalyzerRule for CountWildcardRule {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_down_with_subqueries(analyze_internal).data()
    }

    fn name(&self) -> &str {
        "count_wildcard_rule"
    }
}

fn is_wildcard(expr: &Expr) -> bool {
    matches!(expr, Expr::Wildcard { qualifier: None })
}

fn is_count_star_aggregate(aggregate_function: &AggregateFunction) -> bool {
    match aggregate_function {
        AggregateFunction {
            func_def: AggregateFunctionDefinition::UDF(udf),
            args,
            ..
        } if udf.name() == "COUNT" && args.len() == 1 && is_wildcard(&args[0]) => true,
        AggregateFunction {
            func_def:
                AggregateFunctionDefinition::BuiltIn(
                    datafusion_expr::aggregate_function::AggregateFunction::Count,
                ),
            args,
            ..
        } if args.len() == 1 && is_wildcard(&args[0]) => true,
        _ => false,
    }
}

fn is_count_star_window_aggregate(window_function: &WindowFunction) -> bool {
    let args = &window_function.args;
    match window_function.fun {
        WindowFunctionDefinition::AggregateFunction(
            aggregate_function::AggregateFunction::Count,
        ) if args.len() == 1 && is_wildcard(&args[0]) => true,
        WindowFunctionDefinition::AggregateUDF(ref udaf)
            if udaf.name() == "COUNT" && args.len() == 1 && is_wildcard(&args[0]) =>
        {
            true
        }
        _ => false,
    }
}

fn analyze_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let name_preserver = NamePreserver::new(&plan);
    plan.map_expressions(|expr| {
        let original_name = name_preserver.save(&expr)?;
        let transformed_expr = expr.transform_up(|expr| match expr {
            Expr::WindowFunction(mut window_function)
                if is_count_star_window_aggregate(&window_function) =>
            {
                window_function.args = vec![lit(COUNT_STAR_EXPANSION)];
                Ok(Transformed::yes(Expr::WindowFunction(window_function)))
            }
            Expr::AggregateFunction(mut aggregate_function)
                if is_count_star_aggregate(&aggregate_function) =>
            {
                aggregate_function.args = vec![lit(COUNT_STAR_EXPANSION)];
                Ok(Transformed::yes(Expr::AggregateFunction(
                    aggregate_function,
                )))
            }
            _ => Ok(Transformed::no(expr)),
        })?;
        transformed_expr.map_data(|data| original_name.restore(data))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use datafusion_expr::expr::Sort;
    use datafusion_expr::{
        col, count, exists, expr, in_subquery, logical_plan::LogicalPlanBuilder, max,
        out_ref_col, scalar_subquery, sum, wildcard, AggregateFunction, WindowFrame,
        WindowFrameBound, WindowFrameUnits,
    };
    use std::sync::Arc;

    fn assert_plan_eq(plan: LogicalPlan, expected: &str) -> Result<()> {
        assert_analyzed_plan_eq_display_indent(
            Arc::new(CountWildcardRule::new()),
            plan,
            expected,
        )
    }

    #[test]
    fn test_count_wildcard_on_sort() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("b")], vec![count(wildcard())])?
            .project(vec![count(wildcard())])?
            .sort(vec![count(wildcard()).sort(true, false)])?
            .build()?;
        let expected = "Sort: COUNT(*) ASC NULLS LAST [COUNT(*):Int64;N]\
        \n  Projection: COUNT(*) [COUNT(*):Int64;N]\
        \n    Aggregate: groupBy=[[test.b]], aggr=[[COUNT(Int64(1)) AS COUNT(*)]] [b:UInt32, COUNT(*):Int64;N]\
        \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(plan, expected)
    }

    #[test]
    fn test_count_wildcard_on_where_in() -> Result<()> {
        let table_scan_t1 = test_table_scan_with_name("t1")?;
        let table_scan_t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(table_scan_t1)
            .filter(in_subquery(
                col("a"),
                Arc::new(
                    LogicalPlanBuilder::from(table_scan_t2)
                        .aggregate(Vec::<Expr>::new(), vec![count(wildcard())])?
                        .project(vec![count(wildcard())])?
                        .build()?,
                ),
            ))?
            .build()?;

        let expected = "Filter: t1.a IN (<subquery>) [a:UInt32, b:UInt32, c:UInt32]\
        \n  Subquery: [COUNT(*):Int64;N]\
        \n    Projection: COUNT(*) [COUNT(*):Int64;N]\
        \n      Aggregate: groupBy=[[]], aggr=[[COUNT(Int64(1)) AS COUNT(*)]] [COUNT(*):Int64;N]\
        \n        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]\
        \n  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(plan, expected)
    }

    #[test]
    fn test_count_wildcard_on_where_exists() -> Result<()> {
        let table_scan_t1 = test_table_scan_with_name("t1")?;
        let table_scan_t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(table_scan_t1)
            .filter(exists(Arc::new(
                LogicalPlanBuilder::from(table_scan_t2)
                    .aggregate(Vec::<Expr>::new(), vec![count(wildcard())])?
                    .project(vec![count(wildcard())])?
                    .build()?,
            )))?
            .build()?;

        let expected = "Filter: EXISTS (<subquery>) [a:UInt32, b:UInt32, c:UInt32]\
        \n  Subquery: [COUNT(*):Int64;N]\
        \n    Projection: COUNT(*) [COUNT(*):Int64;N]\
        \n      Aggregate: groupBy=[[]], aggr=[[COUNT(Int64(1)) AS COUNT(*)]] [COUNT(*):Int64;N]\
        \n        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]\
        \n  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(plan, expected)
    }

    #[test]
    fn test_count_wildcard_on_where_scalar_subquery() -> Result<()> {
        let table_scan_t1 = test_table_scan_with_name("t1")?;
        let table_scan_t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(table_scan_t1)
            .filter(
                scalar_subquery(Arc::new(
                    LogicalPlanBuilder::from(table_scan_t2)
                        .filter(out_ref_col(DataType::UInt32, "t1.a").eq(col("t2.a")))?
                        .aggregate(
                            Vec::<Expr>::new(),
                            vec![count(lit(COUNT_STAR_EXPANSION))],
                        )?
                        .project(vec![count(lit(COUNT_STAR_EXPANSION))])?
                        .build()?,
                ))
                .gt(lit(ScalarValue::UInt8(Some(0)))),
            )?
            .project(vec![col("t1.a"), col("t1.b")])?
            .build()?;

        let expected = "Projection: t1.a, t1.b [a:UInt32, b:UInt32]\
              \n  Filter: (<subquery>) > UInt8(0) [a:UInt32, b:UInt32, c:UInt32]\
              \n    Subquery: [COUNT(Int64(1)):Int64;N]\
              \n      Projection: COUNT(Int64(1)) [COUNT(Int64(1)):Int64;N]\
              \n        Aggregate: groupBy=[[]], aggr=[[COUNT(Int64(1))]] [COUNT(Int64(1)):Int64;N]\
              \n          Filter: outer_ref(t1.a) = t2.a [a:UInt32, b:UInt32, c:UInt32]\
              \n            TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]\
              \n    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(plan, expected)
    }
    #[test]
    fn test_count_wildcard_on_window() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![Expr::WindowFunction(expr::WindowFunction::new(
                WindowFunctionDefinition::AggregateFunction(AggregateFunction::Count),
                vec![wildcard()],
                vec![],
                vec![Expr::Sort(Sort::new(Box::new(col("a")), false, true))],
                WindowFrame::new_bounds(
                    WindowFrameUnits::Range,
                    WindowFrameBound::Preceding(ScalarValue::UInt32(Some(6))),
                    WindowFrameBound::Following(ScalarValue::UInt32(Some(2))),
                ),
                None,
            ))])?
            .project(vec![count(wildcard())])?
            .build()?;

        let expected = "Projection: COUNT(Int64(1)) AS COUNT(*) [COUNT(*):Int64;N]\
        \n  WindowAggr: windowExpr=[[COUNT(Int64(1)) ORDER BY [test.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING AS COUNT(*) ORDER BY [test.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING]] [a:UInt32, b:UInt32, c:UInt32, COUNT(*) ORDER BY [test.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING:Int64;N]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(plan, expected)
    }

    #[test]
    fn test_count_wildcard_on_aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![count(wildcard())])?
            .project(vec![count(wildcard())])?
            .build()?;

        let expected = "Projection: COUNT(*) [COUNT(*):Int64;N]\
        \n  Aggregate: groupBy=[[]], aggr=[[COUNT(Int64(1)) AS COUNT(*)]] [COUNT(*):Int64;N]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(plan, expected)
    }

    #[test]
    fn test_count_wildcard_on_non_count_aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;
        let err = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![sum(wildcard())])
            .unwrap_err()
            .to_string();
        assert!(err.contains("Error during planning: No function matches the given name and argument types 'SUM(Null)'."), "{err}");
        Ok(())
    }

    #[test]
    fn test_count_wildcard_on_nesting() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![max(count(wildcard()))])?
            .project(vec![count(wildcard())])?
            .build()?;

        let expected = "Projection: COUNT(Int64(1)) AS COUNT(*) [COUNT(*):Int64;N]\
        \n  Aggregate: groupBy=[[]], aggr=[[MAX(COUNT(Int64(1))) AS MAX(COUNT(*))]] [MAX(COUNT(*)):Int64;N]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(plan, expected)
    }
}
