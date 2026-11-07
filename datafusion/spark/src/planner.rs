// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_common::Result;
use datafusion_expr::Expr;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::planner::{ExprPlanner, PlannerResult};
#[cfg(feature = "sql")]
use datafusion_expr::{
    ExprSchemable, ScalarUDF, planner::RawBinaryExpr, sqlparser::ast::BinaryOperator,
};

#[derive(Default, Debug)]
pub struct SparkFunctionPlanner;

impl ExprPlanner for SparkFunctionPlanner {
    #[cfg(feature = "sql")]
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        schema: &datafusion_common::DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        if expr.op != BinaryOperator::Divide {
            return Ok(PlannerResult::Original(expr));
        }

        let left_type = expr.left.get_type(schema)?;
        let right_type = expr.right.get_type(schema)?;

        // Spark divides a non-decimal pair as DOUBLE; DECIMAL division keeps its
        // precision and scale, which is what the `/` operator already does.
        if !left_type.is_numeric()
            || !right_type.is_numeric()
            || left_type.is_decimal()
            || right_type.is_decimal()
        {
            return Ok(PlannerResult::Original(expr));
        }

        let divide =
            ScalarUDF::new_from_impl(crate::function::math::divide::SparkDivide::new());
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(
                std::sync::Arc::new(divide),
                vec![expr.left, expr.right],
            ),
        )))
    }

    fn plan_extract(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(crate::function::datetime::date_part(), args),
        )))
    }

    fn plan_substring(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(crate::function::string::substring(), args),
        )))
    }
}
