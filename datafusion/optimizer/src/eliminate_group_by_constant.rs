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

//! [`EliminateGroupByConstant`] removes constant and functionally redundant
//! expressions from `GROUP BY` clause
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use std::collections::HashSet;

use datafusion_common::Result;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::{Aggregate, Expr, LogicalPlan, LogicalPlanBuilder, Volatility};

/// Optimizer rule that removes constant expressions from `GROUP BY` clause
/// and places additional projection on top of aggregation, to preserve
/// original schema
#[derive(Default, Debug)]
pub struct EliminateGroupByConstant {}

impl EliminateGroupByConstant {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateGroupByConstant {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Aggregate(aggregate) => {
                // Collect bare column references in GROUP BY
                let group_by_columns: HashSet<&datafusion_common::Column> = aggregate
                    .group_expr
                    .iter()
                    .filter_map(|expr| match expr {
                        Expr::Column(c) => Some(c),
                        _ => None,
                    })
                    .collect();

                let (redundant, required): (Vec<_>, Vec<_>) = aggregate
                    .group_expr
                    .iter()
                    .partition(|expr| is_redundant_group_expr(expr, &group_by_columns));

                if redundant.is_empty()
                    || (required.is_empty() && aggregate.aggr_expr.is_empty())
                {
                    return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
                }

                let simplified_aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
                    aggregate.input,
                    required.into_iter().cloned().collect(),
                    aggregate.aggr_expr.clone(),
                )?);

                let projection_expr =
                    aggregate.group_expr.into_iter().chain(aggregate.aggr_expr);

                let projection = LogicalPlanBuilder::from(simplified_aggregate)
                    .project(projection_expr)?
                    .build()?;

                Ok(Transformed::yes(projection))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }

    fn name(&self) -> &str {
        "eliminate_group_by_constant"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}

/// Checks if a GROUP BY expression is redundant (can be removed without
/// changing grouping semantics). An expression is redundant if it is a
/// deterministic function of constants and columns already present as bare
/// column references in the GROUP BY.
fn is_redundant_group_expr(
    expr: &Expr,
    group_by_columns: &HashSet<&datafusion_common::Column>,
) -> bool {
    // Bare column references are never redundant - they define the grouping
    if matches!(expr, Expr::Column(_)) {
        return false;
    }
    is_deterministic_of(expr, group_by_columns)
}

/// Returns true if `expr` is a deterministic expression whose only column
/// references are contained in `known_columns`.
fn is_deterministic_of(
    expr: &Expr,
    known_columns: &HashSet<&datafusion_common::Column>,
) -> bool {
    match expr {
        Expr::Alias(e) => is_deterministic_of(&e.expr, known_columns),
        Expr::Column(c) => known_columns.contains(c),
        Expr::Literal(_, _) => true,
        Expr::BinaryExpr(e) => {
            is_deterministic_of(&e.left, known_columns)
                && is_deterministic_of(&e.right, known_columns)
        }
        Expr::ScalarFunction(e) => {
            matches!(
                e.func.signature().volatility,
                Volatility::Immutable | Volatility::Stable
            ) && e
                .args
                .iter()
                .all(|arg| is_deterministic_of(arg, known_columns))
        }
        Expr::Cast(e) => is_deterministic_of(&e.expr, known_columns),
        Expr::TryCast(e) => is_deterministic_of(&e.expr, known_columns),
        Expr::Negative(e) => is_deterministic_of(e, known_columns),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::*;

    use arrow::datatypes::DataType;
    use datafusion_common::Result;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{
        ColumnarValue, LogicalPlanBuilder, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
        Signature, TypeSignature, col, lit,
    };

    use datafusion_functions_aggregate::expr_fn::count;

    use std::sync::Arc;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(EliminateGroupByConstant::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct ScalarUDFMock {
        signature: Signature,
    }

    impl ScalarUDFMock {
        fn new_with_volatility(volatility: Volatility) -> Self {
            Self {
                signature: Signature::new(TypeSignature::Any(1), volatility),
            }
        }
    }

    impl ScalarUDFImpl for ScalarUDFMock {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            "scalar_fn_mock"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }
        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            unimplemented!()
        }
    }

    #[test]
    fn test_eliminate_gby_literal() -> Result<()> {
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![col("a"), lit(1u32)], vec![count(col("c"))])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: test.a, UInt32(1), count(test.c)
          Aggregate: groupBy=[[test.a]], aggr=[[count(test.c)]]
            TableScan: test
        ")
    }

    #[test]
    fn test_eliminate_constant() -> Result<()> {
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![lit("test"), lit(123u32)], vec![count(col("c"))])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r#"
        Projection: Utf8("test"), UInt32(123), count(test.c)
          Aggregate: groupBy=[[]], aggr=[[count(test.c)]]
            TableScan: test
        "#)
    }

    #[test]
    fn test_no_op_no_constants() -> Result<()> {
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![col("a"), col("b")], vec![count(col("c"))])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a, test.b]], aggr=[[count(test.c)]]
          TableScan: test
        ")
    }

    #[test]
    fn test_no_op_only_constant() -> Result<()> {
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![lit(123u32)], Vec::<Expr>::new())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[UInt32(123)]], aggr=[[]]
          TableScan: test
        ")
    }

    #[test]
    fn test_eliminate_constant_with_alias() -> Result<()> {
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(
                vec![lit(123u32).alias("const"), col("a")],
                vec![count(col("c"))],
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: UInt32(123) AS const, test.a, count(test.c)
          Aggregate: groupBy=[[test.a]], aggr=[[count(test.c)]]
            TableScan: test
        ")
    }

    #[test]
    fn test_eliminate_scalar_fn_with_constant_arg() -> Result<()> {
        let udf = ScalarUDF::new_from_impl(ScalarUDFMock::new_with_volatility(
            Volatility::Immutable,
        ));
        let udf_expr =
            Expr::ScalarFunction(ScalarFunction::new_udf(udf.into(), vec![lit(123u32)]));
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![udf_expr, col("a")], vec![count(col("c"))])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: scalar_fn_mock(UInt32(123)), test.a, count(test.c)
          Aggregate: groupBy=[[test.a]], aggr=[[count(test.c)]]
            TableScan: test
        ")
    }

    #[test]
    fn test_eliminate_deterministic_expr_of_group_by_column() -> Result<()> {
        let scan = test_table_scan()?;
        // GROUP BY a, a - 1, a - 2, a - 3  ->  GROUP BY a
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(
                vec![
                    col("a"),
                    col("a") - lit(1u32),
                    col("a") - lit(2u32),
                    col("a") - lit(3u32),
                ],
                vec![count(col("c"))],
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: test.a, test.a - UInt32(1), test.a - UInt32(2), test.a - UInt32(3), count(test.c)
          Aggregate: groupBy=[[test.a]], aggr=[[count(test.c)]]
            TableScan: test
        ")
    }

    #[test]
    fn test_no_eliminate_independent_columns() -> Result<()> {
        // GROUP BY a, b - 1 should NOT eliminate b - 1 (b is not a group by column)
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![col("a"), col("b") - lit(1u32)], vec![count(col("c"))])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a, test.b - UInt32(1)]], aggr=[[count(test.c)]]
          TableScan: test
        ")
    }

    #[test]
    fn test_no_op_volatile_scalar_fn_with_constant_arg() -> Result<()> {
        let udf = ScalarUDF::new_from_impl(ScalarUDFMock::new_with_volatility(
            Volatility::Volatile,
        ));
        let udf_expr =
            Expr::ScalarFunction(ScalarFunction::new_udf(udf.into(), vec![lit(123u32)]));
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![udf_expr, col("a")], vec![count(col("c"))])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[scalar_fn_mock(UInt32(123)), test.a]], aggr=[[count(test.c)]]
          TableScan: test
        ")
    }
}
