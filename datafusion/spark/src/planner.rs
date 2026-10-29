// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::function::datetime::date_part::{date_part_seconds, is_seconds_part};
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::planner::{ExprPlanner, PlannerResult, RawScalarExpr};

#[derive(Default, Debug)]
pub struct SparkFunctionPlanner;

impl ExprPlanner for SparkFunctionPlanner {
    fn plan_extract(
        &self,
        args: Vec<Expr>,
    ) -> datafusion_common::Result<PlannerResult<Vec<Expr>>> {
        // EXTRACT(SECOND FROM ts) must return DECIMAL(8, 6) like date_part('SECOND', ts).
        if let Some(part) = args.first().and_then(expr_string_literal)
            && is_seconds_part(&part)
        {
            let date_expr = args.into_iter().nth(1).ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "EXTRACT expects a source expression".to_string(),
                )
            })?;
            return Ok(PlannerResult::Planned(Expr::ScalarFunction(
                ScalarFunction::new_udf(date_part_seconds(), vec![date_expr]),
            )));
        }

        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(crate::function::datetime::date_part(), args),
        )))
    }

    fn plan_substring(
        &self,
        args: Vec<Expr>,
    ) -> datafusion_common::Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(crate::function::string::substring(), args),
        )))
    }

    fn plan_scalar(
        &self,
        expr: RawScalarExpr,
        _schema: &datafusion_common::DFSchema,
    ) -> datafusion_common::Result<PlannerResult<RawScalarExpr>> {
        // date_part('SECOND', ts) / datepart(...) → DECIMAL(8, 6) seconds UDF.
        // Done here so the return type is fixed before analysis; a user-written
        // cast on the part literal is invisible to return_field_from_args.
        if expr.func.name() == "date_part"
            && let Some(part) = expr.args.first().and_then(expr_string_literal)
            && is_seconds_part(&part)
        {
            let date_expr = expr.args.into_iter().nth(1).ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "date_part expects a date/timestamp argument".to_string(),
                )
            })?;
            return Ok(PlannerResult::Planned(Expr::ScalarFunction(
                ScalarFunction::new_udf(date_part_seconds(), vec![date_expr]),
            )));
        }

        Ok(PlannerResult::Original(expr))
    }
}

/// String value of a literal, or of a cast/try_cast wrapping a string literal.
fn expr_string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(sv, _) => match sv {
            ScalarValue::Utf8(Some(s))
            | ScalarValue::Utf8View(Some(s))
            | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
            _ => None,
        },
        Expr::Cast(c) => expr_string_literal(c.expr.as_ref()),
        Expr::TryCast(c) => expr_string_literal(c.expr.as_ref()),
        _ => None,
    }
}
