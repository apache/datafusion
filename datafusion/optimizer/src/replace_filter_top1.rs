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

//! [`ReplaceFilterTop1`] replaces `DISTINCT ...` with `GROUP BY ...`

use crate::optimizer::{ApplyOrder, ApplyOrder::BottomUp};
use crate::{OptimizerConfig, OptimizerRule};
use std::sync::Arc;

use datafusion_common::ScalarValue;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Column, Result};
use datafusion_expr::{
    Aggregate, BinaryExpr, Expr, Filter, LogicalPlan, Operator, SortExpr,
};
use datafusion_expr::{ExprFunctionExt, LogicalPlanBuilder, lit};

/// Optimizer that replaces logical [[Filter]] with a "topredicate, that has a child  with a logical [[Window]] with a function using `row_number`
/// to an aggregate
///
/// ```text
/// SELECT * FROM (
///     SELECT *,
///     ROW_NUMBER() OVER (PARTITION BY p ORDER BY o DESC) AS rn
///     FROM t
/// ) WHERE rn = 1
/// ```
///
/// Input plan:
/// ```text
/// Filter: rn = 1                    -- or rn <= 1, or rn < 2
///     Projection: ...                 -- optional passthrough
///         WindowAggr: row_number() OVER (PARTITION BY p ORDER BY o DESC) AS rn
/// child
/// ```
///
/// Rewritten plan:
/// Aggregate:
///  group_by=[p]
///  aggr=[first_value(col_i ORDER BY o DESC) for each output col_i]
/// child
///
/// Notes:
/// - the window function must be `row_number`
/// - filter predicate must be "top-1" (rn = 1, <= 1, < 2)
/// - `rn` is not referenced anywhere else in the plan above filtering (no aliasing, no join key, other projections)
/// - window has a `PARITION BY` clause
#[derive(Default, Debug)]
pub struct ReplaceFilterTop1 {}

impl ReplaceFilterTop1 {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ReplaceFilterTop1 {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(Filter {
                ref predicate,
                ref input,
                ..
            }) => {
                let Some((order_by, partition_by, rn_name, input_cols, child)) =
                    has_valid_window_input(input)
                else {
                    return Ok(Transformed::no(plan));
                };

                if !has_valid_predicate(predicate, &rn_name) {
                    return Ok(Transformed::no(plan));
                }

                let is_partition = |c: &Column| {
                    partition_by.iter().any(|e| matches!(e, Expr::Column(p) if p.name == c.name && p.relation == c.relation))
                };

                let first_value =
                    config.function_registry().unwrap().udaf("first_value")?;
                let aggr_expr = input_cols
                    .iter()
                    .filter(|c| !is_partition(c))
                    .map(|c| {
                        first_value
                            .call(vec![Expr::Column(c.clone())])
                            .order_by(order_by.clone())
                            .build()
                            .map(|e| {
                                e.alias_qualified(c.relation.clone(), c.name.clone())
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;

                let aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::clone(child),
                    partition_by.clone(),
                    aggr_expr,
                )?);

                let proj_exprs = input
                    .schema()
                    .iter()
                    .map(|(qualifier, field)| {
                        if qualifier.is_none() && field.name() == &rn_name {
                            lit(1u64).alias(field.name())
                        } else {
                            Expr::Column(Column::new(qualifier.cloned(), field.name()))
                        }
                    })
                    .collect::<Vec<_>>();

                let new_plan = LogicalPlanBuilder::from(aggregate)
                    .project(proj_exprs)?
                    .build()?;
                Ok(Transformed::yes(new_plan))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }

    fn name(&self) -> &str {
        "replace_distinct_aggregate"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(BottomUp)
    }
}

fn has_valid_predicate(predicate: &Expr, rn_name: &str) -> bool {
    let Expr::BinaryExpr(BinaryExpr { left, right, op }) = predicate else {
        return false;
    };

    let (name, op, val) = match (&**left, &**right) {
        (
            Expr::Column(Column { name, .. }),
            Expr::Literal(ScalarValue::UInt64(Some(val)), _),
        ) => (name, *op, *val),
        (
            Expr::Literal(ScalarValue::UInt64(Some(val)), _),
            Expr::Column(Column { name, .. }),
        ) => {
            let Some(op) = op.swap() else { return false };
            (name, op, *val)
        }
        _ => return false,
    };

    name.as_str() == rn_name
        && match op {
            Operator::Lt => val == 2,
            Operator::Eq | Operator::LtEq => val == 1,
            _ => false,
        }
}

fn has_valid_window_input(
    input: &Arc<LogicalPlan>,
) -> Option<(
    &Vec<SortExpr>,
    &Vec<Expr>,
    String,
    Vec<Column>,
    &Arc<LogicalPlan>,
)> {
    let window = match &**input {
        LogicalPlan::Window(w) => w,
        LogicalPlan::Projection(p) => match &*p.input {
            LogicalPlan::Window(w) => w,
            _ => return None,
        },
        _ => return None,
    };

    if window.window_expr.len() != 1 {
        return None;
    }

    let window_expr = window.window_expr.first().expect("lol");
    match window_expr {
        Expr::WindowFunction(e) => {
            if e.params.partition_by.is_empty() {
                return None;
            }

            if e.fun.name() != "row_number" {
                return None;
            }

            return Some((
                &e.params.order_by,
                &e.params.partition_by,
                window_expr.schema_name().to_string(),
                window.input.schema().columns(),
                &window.input,
            ));
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::test::*;

    use std::collections::HashSet;

    use chrono::{DateTime, Utc};
    use datafusion_common::DataFusionError;
    use datafusion_common::alias::AliasGenerator;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::WindowFunctionDefinition;
    use datafusion_expr::col;
    use datafusion_expr::expr::WindowFunction;
    use datafusion_expr::planner::ExprPlanner;
    use datafusion_expr::registry::FunctionRegistry;
    use datafusion_expr::{AggregateUDF, HigherOrderUDF, ScalarUDF, WindowUDF};

    /// A minimal [`FunctionRegistry`] that only knows `first_value` (all the
    /// rewrite needs). `OptimizerContext` provides no registry on its own.
    struct TestRegistry;

    impl FunctionRegistry for TestRegistry {
        fn udfs(&self) -> HashSet<String> {
            HashSet::new()
        }
        fn higher_order_function_names(&self) -> HashSet<String> {
            HashSet::new()
        }
        fn udafs(&self) -> HashSet<String> {
            HashSet::new()
        }
        fn udwfs(&self) -> HashSet<String> {
            HashSet::new()
        }
        fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
            Err(DataFusionError::Plan(format!("udf {name} not found")))
        }
        fn higher_order_function(&self, name: &str) -> Result<Arc<HigherOrderUDF>> {
            Err(DataFusionError::Plan(format!(
                "higher order fn {name} not found"
            )))
        }
        fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
            match name {
                "first_value" => {
                    Ok(datafusion_functions_aggregate::first_last::first_value_udaf())
                }
                _ => Err(DataFusionError::Plan(format!("udaf {name} not found"))),
            }
        }
        fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>> {
            Err(DataFusionError::Plan(format!("udwf {name} not found")))
        }
        fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
            vec![]
        }
    }

    /// Wraps [`OptimizerContext`] to supply the [`TestRegistry`].
    struct TestConfig {
        inner: OptimizerContext,
        registry: TestRegistry,
    }

    impl TestConfig {
        fn new() -> Self {
            Self {
                inner: OptimizerContext::new(),
                registry: TestRegistry,
            }
        }
    }

    impl OptimizerConfig for TestConfig {
        fn query_execution_start_time(&self) -> Option<DateTime<Utc>> {
            self.inner.query_execution_start_time()
        }
        fn alias_generator(&self) -> &Arc<AliasGenerator> {
            self.inner.alias_generator()
        }
        fn options(&self) -> Arc<ConfigOptions> {
            self.inner.options()
        }
        fn function_registry(&self) -> Option<&dyn FunctionRegistry> {
            Some(&self.registry)
        }
    }

    /// Build a `row_number()` window expr with the given PARTITION BY / ORDER BY.
    fn row_number_window(partition_by: Vec<Expr>, order_by: Vec<SortExpr>) -> Expr {
        Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::row_number::row_number_udwf(),
            ),
            vec![],
        ))
        .partition_by(partition_by)
        .order_by(order_by)
        .build()
        .unwrap()
    }

    /// `Filter(<pred over rn>) -> WindowAggr(row_number PARTITION BY .. ORDER BY c DESC) -> TableScan`.
    /// `make_predicate` receives the `rn` column expression.
    fn top1_plan(
        partition_by: Vec<Expr>,
        make_predicate: impl FnOnce(Expr) -> Expr,
    ) -> Result<LogicalPlan> {
        let table_scan = test_table_scan()?;
        let window = row_number_window(partition_by, vec![col("c").sort(false, true)]);
        let window_plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![window])?
            .build()?;
        // The row_number output is the last column of the WindowAggr schema;
        // its name is fully normalized/qualified here (unlike the pre-build expr).
        let rn = Expr::Column(window_plan.schema().columns().last().unwrap().clone());
        LogicalPlanBuilder::from(window_plan)
            .filter(make_predicate(rn))?
            .build()
    }

    fn rewrite(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        ReplaceFilterTop1::new().rewrite(plan, &TestConfig::new())
    }

    // ---------- rewrite fires ----------

    #[test]
    fn rewrite_rn_eq_1() -> Result<()> {
        let plan = top1_plan(vec![col("a")], |rn| rn.eq(lit(1u64)))?;
        let optimized = rewrite(plan)?;
        assert!(optimized.transformed);
        insta::assert_snapshot!(optimized.data, @r"
Projection: test.a, test.b, test.c, UInt64(1) AS row_number() PARTITION BY [test.a] ORDER BY [test.c DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  Aggregate: groupBy=[[test.a]], aggr=[[first_value(test.b) ORDER BY [test.c DESC NULLS FIRST] AS b, first_value(test.c) ORDER BY [test.c DESC NULLS FIRST] AS c]]
    TableScan: test
");
        Ok(())
    }

    #[test]
    fn rewrite_rn_eq_1_literal_on_left() -> Result<()> {
        let plan = top1_plan(vec![col("a")], |rn| lit(1u64).eq(rn))?;
        let optimized = rewrite(plan)?;
        assert!(optimized.transformed);
        insta::assert_snapshot!(optimized.data, @r"
Projection: test.a, test.b, test.c, UInt64(1) AS row_number() PARTITION BY [test.a] ORDER BY [test.c DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  Aggregate: groupBy=[[test.a]], aggr=[[first_value(test.b) ORDER BY [test.c DESC NULLS FIRST] AS b, first_value(test.c) ORDER BY [test.c DESC NULLS FIRST] AS c]]
    TableScan: test
");
        Ok(())
    }

    #[test]
    fn rewrite_rn_lteq_1() -> Result<()> {
        let plan = top1_plan(vec![col("a")], |rn| rn.lt_eq(lit(1u64)))?;
        let optimized = rewrite(plan)?;
        assert!(optimized.transformed);
        insta::assert_snapshot!(optimized.data, @r"
Projection: test.a, test.b, test.c, UInt64(1) AS row_number() PARTITION BY [test.a] ORDER BY [test.c DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  Aggregate: groupBy=[[test.a]], aggr=[[first_value(test.b) ORDER BY [test.c DESC NULLS FIRST] AS b, first_value(test.c) ORDER BY [test.c DESC NULLS FIRST] AS c]]
    TableScan: test
");
        Ok(())
    }

    #[test]
    fn rewrite_rn_lt_2() -> Result<()> {
        let plan = top1_plan(vec![col("a")], |rn| rn.lt(lit(2u64)))?;
        let optimized = rewrite(plan)?;
        assert!(optimized.transformed);
        insta::assert_snapshot!(optimized.data, @r"
Projection: test.a, test.b, test.c, UInt64(1) AS row_number() PARTITION BY [test.a] ORDER BY [test.c DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  Aggregate: groupBy=[[test.a]], aggr=[[first_value(test.b) ORDER BY [test.c DESC NULLS FIRST] AS b, first_value(test.c) ORDER BY [test.c DESC NULLS FIRST] AS c]]
    TableScan: test
");
        Ok(())
    }

    // ---------- rewrite does not fire ----------

    #[test]
    fn no_rewrite_rn_eq_2() -> Result<()> {
        let plan = top1_plan(vec![col("a")], |rn| rn.eq(lit(2u64)))?;
        let optimized = rewrite(plan)?;
        assert!(!optimized.transformed);
        Ok(())
    }

    #[test]
    fn no_rewrite_predicate_on_other_column() -> Result<()> {
        // predicate compares `a`, not the row_number column
        let plan = top1_plan(vec![col("a")], |_rn| col("a").eq(lit(1u64)))?;
        let optimized = rewrite(plan)?;
        assert!(!optimized.transformed);
        Ok(())
    }

    #[test]
    fn no_rewrite_missing_partition_by() -> Result<()> {
        let plan = top1_plan(vec![], |rn| rn.eq(lit(1u64)))?;
        let optimized = rewrite(plan)?;
        assert!(!optimized.transformed);
        Ok(())
    }

    #[test]
    fn no_rewrite_not_row_number() -> Result<()> {
        let table_scan = test_table_scan()?;
        let window = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::rank::rank_udwf(),
            ),
            vec![],
        ))
        .partition_by(vec![col("a")])
        .order_by(vec![col("c").sort(false, true)])
        .build()
        .unwrap();
        let window_plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![window])?
            .build()?;
        let rank_col =
            Expr::Column(window_plan.schema().columns().last().unwrap().clone());
        let plan = LogicalPlanBuilder::from(window_plan)
            .filter(rank_col.eq(lit(1u64)))?
            .build()?;

        let optimized = rewrite(plan)?;
        assert!(!optimized.transformed);
        Ok(())
    }
}
