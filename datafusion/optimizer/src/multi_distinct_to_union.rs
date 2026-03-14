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

//! [`MultiDistinctToUnion`] splits multiple distinct aggregates on
//! different columns into a UNION ALL of individual aggregates, enabling
//! parallel execution via InterleaveExec.

use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::tree_node::Transformed;
use datafusion_common::{HashSet, Result, ScalarValue};
use datafusion_expr::builder::project;
use datafusion_expr::expr::AggregateFunctionParams;
use datafusion_expr::expr_fn::ident;
use datafusion_expr::{
    Expr, col,
    expr::AggregateFunction,
    logical_plan::{Aggregate, LogicalPlan, Union},
};

/// Optimizer rule that rewrites queries with multiple distinct aggregates on
/// different columns (without GROUP BY) into a UNION ALL of individual
/// aggregates with an outer MAX to combine results. This allows each distinct
/// aggregate to execute in parallel via InterleaveExec.
///
/// ```text
/// Before:
///   SELECT COUNT(DISTINCT a), SUM(DISTINCT b) FROM t
///
/// After:
///   SELECT MAX(c0) AS "count(DISTINCT a)", MAX(c1) AS "sum(DISTINCT b)"
///   FROM (
///     SELECT COUNT(DISTINCT a) AS __multi_distinct_c0, NULL AS __multi_distinct_c1 FROM t
///     UNION ALL
///     SELECT NULL AS __multi_distinct_c0, SUM(DISTINCT b) AS __multi_distinct_c1 FROM t
///   )
/// ```
#[derive(Default, Debug)]
pub struct MultiDistinctToUnion {}

impl MultiDistinctToUnion {
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

impl OptimizerRule for MultiDistinctToUnion {
    fn name(&self) -> &str {
        "multi_distinct_to_union"
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
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Aggregate(Aggregate {
                ref input,
                ref aggr_expr,
                ref schema,
                ref group_expr,
                ..
            }) if is_multi_distinct_agg(group_expr, aggr_expr) => {
                // Look up "max" aggregate from the function registry
                let Some(registry) = config.function_registry() else {
                    return Ok(Transformed::no(plan));
                };
                let Ok(max_udaf) = registry.udaf("max") else {
                    return Ok(Transformed::no(plan));
                };

                // Move out of the plan to avoid cloning
                let LogicalPlan::Aggregate(Aggregate {
                    input,
                    aggr_expr,
                    schema,
                    ..
                }) = plan
                else {
                    unreachable!()
                };
                let n = aggr_expr.len();

                // Column aliases used across union branches.
                // These are internal to the rewritten plan (Union -> Aggregate -> Projection)
                // and won't conflict with user-visible column names.
                let col_aliases: Vec<String> =
                    (0..n).map(|i| format!("__multi_distinct_c{i}")).collect();

                // Get the data type for each aggregate output (for typed NULLs)
                let null_values: Vec<ScalarValue> = (0..n)
                    .map(|i| {
                        let field = schema.field(i);
                        ScalarValue::try_from(field.data_type())
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Build one union branch per distinct aggregate:
                //   SELECT NULL AS __multi_distinct_c0, ..., AGG(DISTINCT xi) AS ci, ..., NULL AS cn
                //   FROM t
                let mut union_inputs = Vec::with_capacity(n);
                for (i, expr) in aggr_expr.iter().enumerate() {
                    let single_agg = LogicalPlan::Aggregate(Aggregate::try_new(
                        Arc::clone(&input),
                        vec![],
                        vec![expr.clone()],
                    )?);

                    // Project: the aggregate result in position i, typed NULLs elsewhere
                    let proj_exprs: Vec<Expr> = (0..n)
                        .map(|j| {
                            if j == i {
                                let field = single_agg.schema().field(0);
                                ident(field.name()).alias(&col_aliases[j])
                            } else {
                                Expr::Literal(null_values[j].clone(), None)
                                    .alias(&col_aliases[j])
                            }
                        })
                        .collect();

                    let branch = project(single_agg, proj_exprs)?;
                    union_inputs.push(Arc::new(branch));
                }

                // UNION ALL
                let union_plan = LogicalPlan::Union(Union::try_new(union_inputs)?);

                // Outer aggregate: SELECT MAX(c0), MAX(c1), ...
                // MAX picks the single non-null value from each column
                let outer_aggr_exprs: Vec<Expr> = col_aliases
                    .iter()
                    .map(|alias| {
                        Expr::AggregateFunction(AggregateFunction::new_udf(
                            Arc::clone(&max_udaf),
                            vec![col(alias)],
                            false,
                            None,
                            vec![],
                            None,
                        ))
                    })
                    .collect();

                let outer_agg = LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::new(union_plan),
                    vec![],
                    outer_aggr_exprs.clone(),
                )?);

                // Final projection: alias back to original schema names
                let final_proj: Vec<Expr> = outer_aggr_exprs
                    .iter()
                    .enumerate()
                    .map(|(idx, agg_expr)| {
                        let outer_name = agg_expr.schema_name().to_string();
                        let (qualifier, original_field) = schema.qualified_field(idx);
                        col(outer_name)
                            .alias_qualified(qualifier.cloned(), original_field.name())
                    })
                    .collect();

                let result = project(outer_agg, final_proj)?;
                Ok(Transformed::yes(result))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_expr::registry::{FunctionRegistry, MemoryFunctionRegistry};
    use datafusion_expr::{col, logical_plan::builder::LogicalPlanBuilder};
    use datafusion_functions_aggregate::expr_fn::count_distinct;
    use datafusion_functions_aggregate::min_max::max_udaf;
    use datafusion_functions_aggregate::sum::sum_distinct;

    /// An OptimizerConfig that provides a function registry with max_udaf
    struct TestConfig {
        inner: crate::OptimizerContext,
        registry: MemoryFunctionRegistry,
    }

    impl TestConfig {
        fn new() -> Self {
            let mut registry = MemoryFunctionRegistry::new();
            registry.register_udaf(max_udaf()).unwrap();
            Self {
                inner: crate::OptimizerContext::new(),
                registry,
            }
        }
    }

    impl OptimizerConfig for TestConfig {
        fn query_execution_start_time(&self) -> Option<chrono::DateTime<chrono::Utc>> {
            self.inner.query_execution_start_time()
        }
        fn alias_generator(&self) -> &Arc<datafusion_common::alias::AliasGenerator> {
            self.inner.alias_generator()
        }
        fn options(&self) -> Arc<datafusion_common::config::ConfigOptions> {
            self.inner.options()
        }
        fn function_registry(&self) -> Option<&dyn FunctionRegistry> {
            Some(&self.registry)
        }
    }

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rule: Arc<dyn crate::OptimizerRule + Send + Sync> = Arc::new(MultiDistinctToUnion::new());
            let config = TestConfig::new();
            let optimizer = crate::Optimizer::with_rules(vec![rule]);
            let optimized_plan = optimizer
                .optimize($plan, &config, |_, _| {})
                .expect("failed to optimize plan");
            let formatted_plan = optimized_plan.display_indent_schema();
            insta::assert_snapshot!(formatted_plan, @ $expected);
            Ok::<(), datafusion_common::DataFusionError>(())
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
        Projection: max(__multi_distinct_c0) AS count(DISTINCT test.a), max(__multi_distinct_c1) AS count(DISTINCT test.b) [count(DISTINCT test.a):Int64;N, count(DISTINCT test.b):Int64;N]
          Aggregate: groupBy=[[]], aggr=[[max(__multi_distinct_c0), max(__multi_distinct_c1)]] [max(__multi_distinct_c0):Int64;N, max(__multi_distinct_c1):Int64;N]
            Union [__multi_distinct_c0:Int64;N, __multi_distinct_c1:Int64;N]
              Projection: count(DISTINCT test.a) AS __multi_distinct_c0, Int64(NULL) AS __multi_distinct_c1 [__multi_distinct_c0:Int64, __multi_distinct_c1:Int64;N]
                Aggregate: groupBy=[[]], aggr=[[count(DISTINCT test.a)]] [count(DISTINCT test.a):Int64]
                  TableScan: test [a:UInt32, b:UInt32, c:UInt32]
              Projection: Int64(NULL) AS __multi_distinct_c0, count(DISTINCT test.b) AS __multi_distinct_c1 [__multi_distinct_c0:Int64;N, __multi_distinct_c1:Int64]
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
        Projection: max(__multi_distinct_c0) AS count(DISTINCT test.a), max(__multi_distinct_c1) AS sum(DISTINCT test.b) [count(DISTINCT test.a):Int64;N, sum(DISTINCT test.b):UInt64;N]
          Aggregate: groupBy=[[]], aggr=[[max(__multi_distinct_c0), max(__multi_distinct_c1)]] [max(__multi_distinct_c0):Int64;N, max(__multi_distinct_c1):UInt64;N]
            Union [__multi_distinct_c0:Int64;N, __multi_distinct_c1:UInt64;N]
              Projection: count(DISTINCT test.a) AS __multi_distinct_c0, UInt64(NULL) AS __multi_distinct_c1 [__multi_distinct_c0:Int64, __multi_distinct_c1:UInt64;N]
                Aggregate: groupBy=[[]], aggr=[[count(DISTINCT test.a)]] [count(DISTINCT test.a):Int64]
                  TableScan: test [a:UInt32, b:UInt32, c:UInt32]
              Projection: Int64(NULL) AS __multi_distinct_c0, sum(DISTINCT test.b) AS __multi_distinct_c1 [__multi_distinct_c0:Int64;N, __multi_distinct_c1:UInt64;N]
                Aggregate: groupBy=[[]], aggr=[[sum(DISTINCT test.b)]] [sum(DISTINCT test.b):UInt64;N]
                  TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }
}
