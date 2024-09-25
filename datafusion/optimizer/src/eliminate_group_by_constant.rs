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

//! [`EliminateGroupByConstant`] removes constant expressions from `GROUP BY` clause
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::tree_node::Transformed;
use datafusion_common::Result;
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
                let (const_group_expr, nonconst_group_expr): (Vec<_>, Vec<_>) = aggregate
                    .group_expr
                    .iter()
                    .partition(|expr| is_constant_expression(expr));

                // If no constant expressions found (nothing to optimize) or
                // constant expression is the only expression in aggregate,
                // optimization is skipped
                if const_group_expr.is_empty()
                    || (!const_group_expr.is_empty()
                        && nonconst_group_expr.is_empty()
                        && aggregate.aggr_expr.is_empty())
                {
                    return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
                }

                let simplified_aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
                    aggregate.input,
                    nonconst_group_expr.into_iter().cloned().collect(),
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

/// Checks if expression is constant, and can be eliminated from group by.
///
/// Intended to be used only within this rule, helper function, which heavily
/// reiles on `SimplifyExpressions` result.
fn is_constant_expression(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(e) => is_constant_expression(&e.expr),
        Expr::BinaryExpr(e) => {
            is_constant_expression(&e.left) && is_constant_expression(&e.right)
        }
        Expr::Literal(_) => true,
        Expr::ScalarFunction(e) => {
            matches!(
                e.func.signature().volatility,
                Volatility::Immutable | Volatility::Stable
            ) && e.args.iter().all(is_constant_expression)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;

    use arrow::datatypes::DataType;
    use datafusion_common::Result;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{
        col, lit, ColumnarValue, LogicalPlanBuilder, ScalarUDF, ScalarUDFImpl, Signature,
        TypeSignature,
    };

    use datafusion_functions_aggregate::expr_fn::count;

    use std::sync::Arc;

    #[derive(Debug)]
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
        fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
            unimplemented!()
        }
    }

    #[test]
    fn test_eliminate_gby_literal() -> Result<()> {
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![col("a"), lit(1u32)], vec![count(col("c"))])?
            .build()?;

        let expected = "\
            Projection: test.a, UInt32(1), count(test.c)\
            \n  Aggregate: groupBy=[[test.a]], aggr=[[count(test.c)]]\
            \n    TableScan: test\
        ";

        assert_optimized_plan_eq(
            Arc::new(EliminateGroupByConstant::new()),
            plan,
            expected,
        )
    }

    #[test]
    fn test_eliminate_constant() -> Result<()> {
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![lit("test"), lit(123u32)], vec![count(col("c"))])?
            .build()?;

        let expected = "\
            Projection: Utf8(\"test\"), UInt32(123), count(test.c)\
            \n  Aggregate: groupBy=[[]], aggr=[[count(test.c)]]\
            \n    TableScan: test\
        ";

        assert_optimized_plan_eq(
            Arc::new(EliminateGroupByConstant::new()),
            plan,
            expected,
        )
    }

    #[test]
    fn test_no_op_no_constants() -> Result<()> {
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![col("a"), col("b")], vec![count(col("c"))])?
            .build()?;

        let expected = "\
            Aggregate: groupBy=[[test.a, test.b]], aggr=[[count(test.c)]]\
            \n  TableScan: test\
        ";

        assert_optimized_plan_eq(
            Arc::new(EliminateGroupByConstant::new()),
            plan,
            expected,
        )
    }

    #[test]
    fn test_no_op_only_constant() -> Result<()> {
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![lit(123u32)], Vec::<Expr>::new())?
            .build()?;

        let expected = "\
            Aggregate: groupBy=[[UInt32(123)]], aggr=[[]]\
            \n  TableScan: test\
        ";

        assert_optimized_plan_eq(
            Arc::new(EliminateGroupByConstant::new()),
            plan,
            expected,
        )
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

        let expected = "\
            Projection: UInt32(123) AS const, test.a, count(test.c)\
            \n  Aggregate: groupBy=[[test.a]], aggr=[[count(test.c)]]\
            \n    TableScan: test\
        ";

        assert_optimized_plan_eq(
            Arc::new(EliminateGroupByConstant::new()),
            plan,
            expected,
        )
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

        let expected = "\
            Projection: scalar_fn_mock(UInt32(123)), test.a, count(test.c)\
            \n  Aggregate: groupBy=[[test.a]], aggr=[[count(test.c)]]\
            \n    TableScan: test\
        ";

        assert_optimized_plan_eq(
            Arc::new(EliminateGroupByConstant::new()),
            plan,
            expected,
        )
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

        let expected = "\
            Aggregate: groupBy=[[scalar_fn_mock(UInt32(123)), test.a]], aggr=[[count(test.c)]]\
            \n  TableScan: test\
        ";

        assert_optimized_plan_eq(
            Arc::new(EliminateGroupByConstant::new()),
            plan,
            expected,
        )
    }
}
