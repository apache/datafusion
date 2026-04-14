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

//! [`DecomposeAggregate`] rewrites `AVG(x)` into `SUM(x) / COUNT(*)`
//! to reduce accumulator overhead and enable sharing the `COUNT(*)`
//! with other aggregates via common subexpression elimination.

use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use arrow::datatypes::DataType;
use datafusion_common::tree_node::Transformed;
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_common::{Column, DataFusionError, Result};
use datafusion_expr::builder::project;
use datafusion_expr::expr::AggregateFunctionParams;
use datafusion_expr::expr_fn::cast;
use datafusion_expr::{
    Expr, ExprSchemable, col,
    expr::AggregateFunction,
    logical_plan::{Aggregate, LogicalPlan},
};

/// Rewrites `AVG(x)` aggregate functions into `SUM(x) / COUNT(*)`.
///
/// ```text
/// Before:
///   Aggregate: groupBy=[[g]], aggr=[[SUM(a), AVG(b), COUNT(*)]]
///
/// After:
///   Projection: g, SUM(a), SUM(b) / CAST(COUNT(*) AS Float64) AS AVG(b), COUNT(*)
///     Aggregate: groupBy=[[g]], aggr=[[SUM(a), SUM(b), COUNT(*)]]
/// ```
///
/// This reduces accumulator overhead (AVG stores sum + count per group
/// internally) and uses `COUNT(*)` which can be shared with an existing
/// `COUNT(*)` in the query via `CommonSubexprEliminate`.
///
/// Only applies to `AVG` with Float64 return type (the common case for
/// integer/float columns after type coercion). Skips DISTINCT, filtered,
/// and ordered AVGs, as well as decimal/duration/interval types.
#[derive(Default, Debug)]
pub struct DecomposeAggregate {}

impl DecomposeAggregate {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Returns true if this is a simple AVG returning Float64.
fn is_eligible_avg(
    expr: &Expr,
    schema: &datafusion_common::DFSchema,
    field_idx: usize,
) -> bool {
    if let Expr::AggregateFunction(AggregateFunction {
        func,
        params:
            AggregateFunctionParams {
                distinct,
                filter,
                order_by,
                ..
            },
    }) = expr
    {
        func.name() == "avg"
            && !distinct
            && filter.is_none()
            && order_by.is_empty()
            && *schema.field(field_idx).data_type() == DataType::Float64
    } else {
        false
    }
}

impl OptimizerRule for DecomposeAggregate {
    fn name(&self) -> &str {
        "decompose_aggregate"
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
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
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

        // Skip GroupingSet aggregates (ROLLUP/CUBE/GROUPING SETS) — the
        // group_expr expands to more schema fields than group_expr.len(),
        // which breaks our index arithmetic.
        if group_expr
            .first()
            .is_some_and(|e| matches!(e, Expr::GroupingSet(_)))
        {
            return Ok(Transformed::no(LogicalPlan::Aggregate(
                Aggregate::try_new_with_schema(input, group_expr, aggr_expr, schema)?,
            )));
        }

        let group_size = group_expr.len();

        // Quick check: any eligible AVGs?
        let has_avg = aggr_expr
            .iter()
            .enumerate()
            .any(|(i, e)| is_eligible_avg(e, &schema, group_size + i));
        if !has_avg {
            return Ok(Transformed::no(LogicalPlan::Aggregate(
                Aggregate::try_new_with_schema(input, group_expr, aggr_expr, schema)?,
            )));
        }

        // We need the function registry to look up sum / count UDAFs.
        let Some(registry) = config.function_registry() else {
            return Ok(Transformed::no(LogicalPlan::Aggregate(
                Aggregate::try_new_with_schema(input, group_expr, aggr_expr, schema)?,
            )));
        };

        let sum_udaf = registry.udaf("sum")?;
        let count_udaf = registry.udaf("count")?;

        let mut new_aggr_exprs = Vec::new();
        let mut alias_idx = 0usize;

        enum AggrMapping {
            AvgRewrite {
                sum_alias: String,
                count_alias: String,
            },
            PassThrough(String),
        }
        let mut mappings: Vec<AggrMapping> = Vec::new();

        // COUNT(*) expression — CSE will deduplicate if one already exists.
        let count_star = Expr::AggregateFunction(AggregateFunction::new_udf(
            Arc::clone(&count_udaf),
            vec![Expr::Literal(COUNT_STAR_EXPANSION, None)],
            false,
            None,
            vec![],
            None,
        ));

        let input_schema = input.schema();

        for (idx, expr) in aggr_expr.into_iter().enumerate() {
            let field_idx = group_size + idx;

            if is_eligible_avg(&expr, &schema, field_idx) {
                // Extract args from the AVG expression.
                let args = match expr {
                    Expr::AggregateFunction(AggregateFunction {
                        params: AggregateFunctionParams { args, .. },
                        ..
                    }) => args,
                    _ => unreachable!(),
                };

                let sum_alias = format!("__decompose_{alias_idx}");
                alias_idx += 1;
                let count_alias = format!("__decompose_{alias_idx}");
                alias_idx += 1;

                // Strip CAST(x AS Float64) added by AVG's type coercion
                // and let SUM apply its own coercion (e.g. Int16 → Int64).
                let sum_args: Vec<Expr> = args
                    .iter()
                    .map(|a| match a {
                        Expr::Cast(c) if *c.field.data_type() == DataType::Float64 => {
                            (*c.expr).clone()
                        }
                        other => other.clone(),
                    })
                    .collect();

                let sum_expr = Expr::AggregateFunction(AggregateFunction::new_udf(
                    Arc::clone(&sum_udaf),
                    sum_args,
                    false,
                    None,
                    vec![],
                    None,
                ))
                .alias(&sum_alias);

                // Use COUNT(*) when the arg is non-nullable (can share with
                // existing COUNT(*) via CSE). Fall back to COUNT(x) for
                // nullable args since AVG ignores NULLs.
                let arg_nullable =
                    args[0].nullable(input_schema.as_ref()).unwrap_or(true);
                let count_expr = if arg_nullable {
                    Expr::AggregateFunction(AggregateFunction::new_udf(
                        Arc::clone(&count_udaf),
                        args,
                        false,
                        None,
                        vec![],
                        None,
                    ))
                } else {
                    count_star.clone()
                }
                .alias(&count_alias);

                new_aggr_exprs.push(sum_expr);
                new_aggr_exprs.push(count_expr);
                mappings.push(AggrMapping::AvgRewrite {
                    sum_alias,
                    count_alias,
                });
            } else {
                let pt_alias = format!("__decompose_{alias_idx}");
                alias_idx += 1;
                new_aggr_exprs.push(expr.alias(&pt_alias));
                mappings.push(AggrMapping::PassThrough(pt_alias));
            }
        }

        // Inner Aggregate with rewritten expressions
        let inner_agg = LogicalPlan::Aggregate(Aggregate::try_new(
            input,
            group_expr.clone(),
            new_aggr_exprs,
        )?);

        // Projection that restores the original schema
        let mut proj_exprs = Vec::new();

        // Group-by columns
        for i in 0..group_size {
            let (qualifier, field) = schema.qualified_field(i);
            let inner_schema = inner_agg.schema();
            let (inner_qual, inner_field) = inner_schema.qualified_field(i);
            let col_ref =
                Expr::Column(Column::new(inner_qual.cloned(), inner_field.name()));
            if qualifier != inner_qual || field.name() != inner_field.name() {
                proj_exprs
                    .push(col_ref.alias_qualified(qualifier.cloned(), field.name()));
            } else {
                proj_exprs.push(col_ref);
            }
        }

        // Aggregate results
        for (mapping_idx, mapping) in mappings.into_iter().enumerate() {
            let orig_idx = group_size + mapping_idx;
            let (qualifier, field) = schema.qualified_field(orig_idx);

            match mapping {
                AggrMapping::AvgRewrite {
                    sum_alias,
                    count_alias,
                } => {
                    let avg_expr = cast(col(&sum_alias), DataType::Float64)
                        / cast(col(&count_alias), DataType::Float64);
                    proj_exprs
                        .push(avg_expr.alias_qualified(qualifier.cloned(), field.name()));
                }
                AggrMapping::PassThrough(alias) => {
                    proj_exprs.push(
                        col(alias).alias_qualified(qualifier.cloned(), field.name()),
                    );
                }
            }
        }

        Ok(Transformed::yes(project(inner_agg, proj_exprs)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::test::*;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
    use datafusion_expr::registry::{FunctionRegistry, MemoryFunctionRegistry};
    use datafusion_expr::{col, lit};
    use datafusion_functions_aggregate::average::{avg, avg_distinct, avg_udaf};
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::expr_fn::{count, sum};
    use datafusion_functions_aggregate::sum::sum_udaf;

    use chrono::Utc;
    use datafusion_common::alias::AliasGenerator;

    /// An OptimizerConfig that includes a function registry with sum/count.
    struct TestConfig {
        inner: OptimizerContext,
        registry: MemoryFunctionRegistry,
    }

    impl TestConfig {
        fn new() -> Self {
            let mut registry = MemoryFunctionRegistry::new();
            registry.register_udaf(sum_udaf()).unwrap();
            registry.register_udaf(count_udaf()).unwrap();
            Self {
                inner: OptimizerContext::new(),
                registry,
            }
        }
    }

    impl OptimizerConfig for TestConfig {
        fn query_execution_start_time(&self) -> Option<chrono::DateTime<Utc>> {
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

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer = $crate::Optimizer::with_rules(vec![
                Arc::new(DecomposeAggregate::new()),
            ]);
            let config = TestConfig::new();
            let optimized_plan = optimizer
                .optimize($plan, &config, |_, _| {})
                .expect("failed to optimize plan");
            let formatted_plan = optimized_plan.display_indent_schema();
            insta::assert_snapshot!(formatted_plan, @ $expected);
            Ok::<(), datafusion_common::DataFusionError>(())
        }};
    }

    #[test]
    fn no_avg() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b")), count(col("c"))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b), count(test.c)]] [a:UInt32, sum(test.b):UInt64;N, count(test.c):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn simple_avg() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![avg(col("b"))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, CAST(__decompose_0 AS Float64) / CAST(__decompose_1 AS Float64) AS avg(test.b) [a:UInt32, avg(test.b):Float64;N]
          Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS __decompose_0, count(Int64(1)) AS __decompose_1]] [a:UInt32, __decompose_0:UInt64;N, __decompose_1:Int64]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn avg_with_other_aggregates() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![sum(col("b")), avg(col("c")), count(col("b"))],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, __decompose_0 AS sum(test.b), CAST(__decompose_1 AS Float64) / CAST(__decompose_2 AS Float64) AS avg(test.c), __decompose_3 AS count(test.b) [a:UInt32, sum(test.b):UInt64;N, avg(test.c):Float64;N, count(test.b):Int64]
          Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS __decompose_0, sum(test.c) AS __decompose_1, count(Int64(1)) AS __decompose_2, count(test.b) AS __decompose_3]] [a:UInt32, __decompose_0:UInt64;N, __decompose_1:UInt64;N, __decompose_2:Int64, __decompose_3:Int64]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn avg_distinct_not_decomposed() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![avg_distinct(col("b"))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.a]], aggr=[[avg(DISTINCT test.b)]] [a:UInt32, avg(DISTINCT test.b):Float64;N]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn avg_with_filter_not_decomposed() -> Result<()> {
        let table_scan = test_table_scan()?;
        use datafusion_expr::ExprFunctionExt;

        let avg_filtered = avg_udaf()
            .call(vec![col("b")])
            .filter(col("a").gt(lit(5)))
            .build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![avg_filtered])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.a]], aggr=[[avg(test.b) FILTER (WHERE test.a > Int32(5))]] [a:UInt32, avg(test.b) FILTER (WHERE test.a > Int32(5)):Float64;N]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn multiple_avgs() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![avg(col("b")), avg(col("c"))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, CAST(__decompose_0 AS Float64) / CAST(__decompose_1 AS Float64) AS avg(test.b), CAST(__decompose_2 AS Float64) / CAST(__decompose_3 AS Float64) AS avg(test.c) [a:UInt32, avg(test.b):Float64;N, avg(test.c):Float64;N]
          Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS __decompose_0, count(Int64(1)) AS __decompose_1, sum(test.c) AS __decompose_2, count(Int64(1)) AS __decompose_3]] [a:UInt32, __decompose_0:UInt64;N, __decompose_1:Int64, __decompose_2:UInt64;N, __decompose_3:Int64]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn no_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![avg(col("b"))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: CAST(__decompose_0 AS Float64) / CAST(__decompose_1 AS Float64) AS avg(test.b) [avg(test.b):Float64;N]
          Aggregate: groupBy=[[]], aggr=[[sum(test.b) AS __decompose_0, count(Int64(1)) AS __decompose_1]] [__decompose_0:UInt64;N, __decompose_1:Int64]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn grouping_set_not_decomposed() -> Result<()> {
        use datafusion_expr::expr::GroupingSet;

        let table_scan = test_table_scan()?;

        let rollup = Expr::GroupingSet(GroupingSet::Rollup(vec![col("a"), col("b")]));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![rollup], vec![avg(col("c"))])?
            .build()?;

        // ROLLUP aggregates should not be decomposed
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[ROLLUP (test.a, test.b)]], aggr=[[avg(test.c)]] [a:UInt32;N, b:UInt32;N, __grouping_id:UInt8, avg(test.c):Float64;N]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }
}
