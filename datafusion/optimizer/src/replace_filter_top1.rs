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

//! [`ReplaceFilterTop1`] rewrites a "top-1 per group" `row_number()` filter
//! into a `first_value` aggregate.

use crate::optimizer::{ApplyOrder, ApplyOrder::BottomUp};
use crate::{OptimizerConfig, OptimizerRule};
use std::sync::Arc;

use datafusion_common::ScalarValue;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Column, Result};
use datafusion_expr::{
    Aggregate, BinaryExpr, Expr, Filter, LogicalPlan, Operator, Projection, SortExpr,
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
///     [Projection: ...]              -- optional passthrough projection
///         WindowAggr: row_number() OVER (PARTITION BY p ORDER BY o DESC) AS rn
/// child
/// ```
///
/// Rewritten plan:
/// ```text
/// Projection: <original Filter output schema; rn -> literal 1>
///     Aggregate:
///      group_by=[p]
///      aggr=[first_value(col_i ORDER BY o DESC) for each non-partition col_i]
///     child
/// ```
///
/// Notes:
/// - the window function must be `row_number`
/// - filter predicate must be "top-1" (rn = 1, <= 1, < 2)
/// - window has a `PARTITION BY` clause
/// - gated behind the `optimizer.enable_row_number_to_aggregate` config option (off by default)
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
        if !config.options().optimizer.enable_row_number_to_aggregate {
            return Ok(Transformed::no(plan));
        }

        let LogicalPlan::Filter(Filter {
            ref predicate,
            ref input,
            ..
        }) = plan
        else {
            return Ok(Transformed::no(plan));
        };

        let Some(WindowTop1 {
            order_by,
            partition_by,
            rn_col,
            input_cols,
            child,
            projection,
        }) = validate_window_input(input)
        else {
            return Ok(Transformed::no(plan));
        };

        // The filter must reference the row_number output as a "top-1"
        // predicate. When a passthrough projection sits between the
        // filter and the window, the filter references the projection's
        // *output* name for that column, resolve accordingly
        let rn_ref_name = match projection {
            None => rn_col.name.clone(),
            Some(p) => match rn_passthrough_name(p, &rn_col) {
                Some(name) => name,
                None => return Ok(Transformed::no(plan)),
            },
        };
        if !has_valid_predicate(predicate, &rn_ref_name) {
            return Ok(Transformed::no(plan));
        }

        let is_partition = |c: &Column| {
            partition_by.iter().any(|e| matches!(e, Expr::Column(p) if p.name == c.name && p.relation == c.relation))
        };

        // Aggregate over the window's input: group by the partition
        // keys and take `first_value(col ORDER BY ...)` for every other
        // input column, aliased back to that column's qualifier+name.
        // This reproduces every input column by name, so any expression
        // defined over the window output (partition keys pass through
        // group-by; the rest via first_value of the top-1 row) can be
        // re-applied unchanged on top of the aggregate.
        let first_value = config.function_registry().unwrap().udaf("first_value")?;
        let aggr_expr = input_cols
            .iter()
            .filter(|c| !is_partition(c))
            .map(|c| {
                first_value
                    .call(vec![Expr::Column(c.clone())])
                    .order_by(order_by.to_vec())
                    .build()
                    .map(|e| e.alias_qualified(c.relation.clone(), c.name.clone()))
            })
            .collect::<Result<Vec<_>>>()?;

        let aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
            Arc::clone(child),
            partition_by.to_vec(),
            aggr_expr,
        )?);

        // Restore the Filter's exact output schema on top of the
        // aggregate. Every surviving row has `rn = 1`, so references to
        // the row_number column fold to the literal 1.
        let proj_exprs = match projection {
            // Direct `Filter -> Window`: rebuild the window's output
            // (input columns + rn) as an identity projection.
            None => input
                .schema()
                .iter()
                .map(|(qualifier, field)| {
                    if qualifier.is_none() && field.name() == &rn_col.name {
                        lit(1u64).alias(field.name())
                    } else {
                        Expr::Column(Column::new(qualifier.cloned(), field.name()))
                    }
                })
                .collect::<Vec<_>>(),
            // Passthrough projection: reuse its expressions verbatim,
            // swapping the sole rn passthrough for `1` (aliased to the
            // projection's output name so the schema is preserved).
            Some(p) => p
                .expr
                .iter()
                .zip(p.schema.iter())
                .map(|(expr, (qualifier, field))| {
                    if matches!(strip_alias(expr), Expr::Column(c) if *c == rn_col) {
                        lit(1u64).alias_qualified(qualifier.cloned(), field.name())
                    } else {
                        expr.clone()
                    }
                })
                .collect::<Vec<_>>(),
        };

        let new_plan = LogicalPlanBuilder::from(aggregate)
            .project(proj_exprs)?
            .build()?;
        Ok(Transformed::yes(new_plan))
    }

    fn name(&self) -> &str {
        "replace_filter_top1"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(BottomUp)
    }
}

/// Validating that the filter predicate is `rn_name` == 1 (or equivalent)
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

/// A `row_number()` window recognized beneath a `Filter`, decomposed into the
/// pieces the rewrite needs. Borrows from the matched [`LogicalPlan`].
struct WindowTop1<'a> {
    /// `ORDER BY` of the window (becomes the `first_value` ordering).
    order_by: &'a [SortExpr],
    /// `PARTITION BY` of the window (becomes the aggregate group-by).
    partition_by: &'a [Expr],
    /// The `row_number()` output column produced by the window.
    rn_col: Column,
    /// Columns produced by the window's input (candidate payload columns).
    input_cols: Vec<Column>,
    /// The window's input, which becomes the aggregate's input.
    child: &'a Arc<LogicalPlan>,
    /// Optional passthrough `Projection` sitting between the `Filter` and the
    /// `Window`. When present, its expressions are re-applied on top of the
    /// rewritten aggregate.
    projection: Option<&'a Projection>,
}

/// Recognize either `Filter -> Window` or `Filter -> Projection -> Window`,
/// where the window is a single `row_number()` with a non-empty `PARTITION BY`.
fn validate_window_input(input: &Arc<LogicalPlan>) -> Option<WindowTop1<'_>> {
    let (projection, window) = match &**input {
        LogicalPlan::Window(w) => (None, w),
        LogicalPlan::Projection(p) => match &*p.input {
            LogicalPlan::Window(w) => (Some(p), w),
            _ => return None,
        },
        _ => return None,
    };

    if window.window_expr.len() != 1 {
        return None;
    }

    let window_expr = window.window_expr.first()?;
    let Expr::WindowFunction(window_function) = window_expr else {
        return None;
    };

    if window_function.params.partition_by.is_empty() {
        return None;
    }

    if window_function.fun.name() != "row_number" {
        return None;
    }

    // The row_number output is the single window expression, appended
    // as the last column of the window's output schema.
    let rn_col = window.schema.columns().last()?.clone();

    Some(WindowTop1 {
        order_by: &window_function.params.order_by,
        partition_by: &window_function.params.partition_by,
        rn_col,
        input_cols: window.input.schema().columns(),
        child: &window.input,
        projection,
    })
}

/// If `projection` is a valid passthrough for the rewrite, return the output
/// name under which it exposes the `row_number()` column (`rn_col`).
///
/// A projection is a valid passthrough when exactly one output column is a
/// (possibly aliased) plain reference to `rn_col` and **no other** output
/// column references `rn_col`. Returns `None` (bail) otherwise, e.g. if the
/// row_number column is dropped, duplicated, or used inside a computed
/// expression that we cannot safely fold to the constant `1`.
fn rn_passthrough_name(projection: &Projection, rn_col: &Column) -> Option<String> {
    let mut rn_output_name = None;
    for (expr, (_, field)) in projection.expr.iter().zip(projection.schema.iter()) {
        let is_passthrough = matches!(strip_alias(expr), Expr::Column(c) if c == rn_col);
        if is_passthrough {
            if rn_output_name.is_some() {
                // rn exposed under more than one output column.
                return None;
            }
            rn_output_name = Some(field.name().clone());
        } else if expr.column_refs().iter().any(|c| *c == rn_col) {
            // rn used inside a non-passthrough (e.g. computed) expression.
            return None;
        }
    }
    rn_output_name
}

/// Strip any (possibly nested) top-level alias, returning the underlying `Expr`.
fn strip_alias(mut expr: &Expr) -> &Expr {
    while let Expr::Alias(alias) = expr {
        expr = alias.expr.as_ref();
    }
    expr
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

    /// Wraps [`OptimizerContext`] to supply the [`TestRegistry`] and to toggle
    /// the `enable_row_number_to_aggregate` gate.
    struct TestConfig {
        inner: OptimizerContext,
        registry: TestRegistry,
        options: Arc<ConfigOptions>,
    }

    impl TestConfig {
        /// Config with the rewrite gate enabled (what most tests exercise).
        fn new() -> Self {
            Self::with_gate(true)
        }

        fn with_gate(enable: bool) -> Self {
            let mut options = ConfigOptions::default();
            options.optimizer.enable_row_number_to_aggregate = enable;
            Self {
                inner: OptimizerContext::new(),
                registry: TestRegistry,
                options: Arc::new(options),
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
            Arc::clone(&self.options)
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

    /// `Filter(<pred>) -> Projection(<proj(rn)>) -> WindowAggr(row_number) -> TableScan`.
    /// `proj` receives the window's `rn` column and returns the projection
    /// expressions; `make_predicate` receives the projection's `rn` *output*
    /// column (the last projection expression).
    fn top1_plan_with_projection(
        partition_by: Vec<Expr>,
        proj: impl FnOnce(Expr) -> Vec<Expr>,
        make_predicate: impl FnOnce(Expr) -> Expr,
    ) -> Result<LogicalPlan> {
        let table_scan = test_table_scan()?;
        let window = row_number_window(partition_by, vec![col("c").sort(false, true)]);
        let window_plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![window])?
            .build()?;
        let rn = Expr::Column(window_plan.schema().columns().last().unwrap().clone());
        let proj_plan = LogicalPlanBuilder::from(window_plan)
            .project(proj(rn))?
            .build()?;
        // The filter references the projection's rn output column (its last
        // output column, fully normalized).
        let rn_out = Expr::Column(proj_plan.schema().columns().last().unwrap().clone());
        LogicalPlanBuilder::from(proj_plan)
            .filter(make_predicate(rn_out))?
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

    #[test]
    fn rewrite_with_passthrough_projection() -> Result<()> {
        // A projection between Filter and Window that reorders the columns and
        // aliases the row_number output to `rn`. The rewrite re-applies the
        // same projection on top of the aggregate, folding rn -> literal 1.
        let plan = top1_plan_with_projection(
            vec![col("a")],
            |rn| vec![col("c"), col("a"), col("b"), rn.alias("rn")],
            |rn_out| rn_out.eq(lit(1u64)),
        )?;
        let optimized = rewrite(plan)?;
        assert!(optimized.transformed);
        insta::assert_snapshot!(optimized.data, @r"
Projection: test.c, test.a, test.b, UInt64(1) AS rn
  Aggregate: groupBy=[[test.a]], aggr=[[first_value(test.b) ORDER BY [test.c DESC NULLS FIRST] AS b, first_value(test.c) ORDER BY [test.c DESC NULLS FIRST] AS c]]
    TableScan: test
");
        Ok(())
    }

    #[test]
    fn no_rewrite_projection_uses_rn_in_computed_expr() -> Result<()> {
        // rn appears inside a computed projection expression, so it can't be
        // safely folded to a constant: the rule must bail.
        let plan = top1_plan_with_projection(
            vec![col("a")],
            |rn| vec![col("a"), col("b"), col("c"), (rn + lit(0u64)).alias("rn")],
            |rn_out| rn_out.eq(lit(1u64)),
        )?;
        let optimized = rewrite(plan)?;
        assert!(!optimized.transformed);
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

    #[test]
    fn no_rewrite_when_gate_disabled() -> Result<()> {
        // Same "top-1" shape that fires above, but with the config gate off
        // (the default) the rule must be a no-op.
        let plan = top1_plan(vec![col("a")], |rn| rn.eq(lit(1u64)))?;
        let optimized =
            ReplaceFilterTop1::new().rewrite(plan, &TestConfig::with_gate(false))?;
        assert!(!optimized.transformed);
        Ok(())
    }
}
