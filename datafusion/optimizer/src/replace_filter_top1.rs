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

use datafusion_common::ScalarValue::{self, UInt64};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Column, Result};
use datafusion_expr::{
    Aggregate, BinaryExpr, Expr, Filter, LogicalPlan, Operator, SortExpr,
};
use datafusion_expr::{ExprFunctionExt, LogicalPlanBuilder, col, lit};

/// Optimizer that replaces logical [[Filter]] with a "top 1" predicate, that has a child  with a logical [[Window]] with a function using `row_number`
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
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::replace_filter_top1::ReplaceFilterTop1;
    use crate::test::*;
    use arrow::datatypes::{Fields, Schema};
    use std::sync::Arc;

    use crate::OptimizerContext;
    use datafusion_common::Result;
    use datafusion_expr::{
        Expr, col, logical_plan::builder::LogicalPlanBuilder, table_scan,
    };
    use datafusion_functions_aggregate::sum::sum;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(ReplaceFilterTop1::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[test]
    fn eliminate_redundant_distinct_simple() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], Vec::<Expr>::new())?
            .project(vec![col("c")])?
            .distinct()?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: test.c
          Aggregate: groupBy=[[test.c]], aggr=[[]]
            TableScan: test
        ")
    }

    #[test]
    fn eliminate_redundant_distinct_pair() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a"), col("b")], Vec::<Expr>::new())?
            .project(vec![col("a"), col("b")])?
            .distinct()?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: test.a, test.b
          Aggregate: groupBy=[[test.a, test.b]], aggr=[[]]
            TableScan: test
        ")
    }

    #[test]
    fn do_not_eliminate_distinct() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .distinct()?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a, test.b]], aggr=[[]]
          Projection: test.a, test.b
            TableScan: test
        ")
    }

    #[test]
    fn do_not_eliminate_distinct_with_aggr() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a"), col("b"), col("c")], vec![sum(col("c"))])?
            .project(vec![col("a"), col("b")])?
            .distinct()?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a, test.b]], aggr=[[]]
          Projection: test.a, test.b
            Aggregate: groupBy=[[test.a, test.b, test.c]], aggr=[[sum(test.c)]]
              TableScan: test
        ")
    }

    #[test]
    fn use_limit_1_when_no_columns() -> Result<()> {
        let plan = table_scan(Some("test"), &Schema::new(Fields::empty()), None)?
            .distinct()?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Limit: skip=0, fetch=1
          TableScan: test
        ")
    }
}
