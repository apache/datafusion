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

//! Parses and simplifies an expression to a literal of a given type.
//!
//! This module provides functionality to parse and simplify static expressions
//! used in SQL constructs like `FROM TABLE SAMPLE (10 + 50 * 2)`. If they are required
//! in a planning (not an execution) phase, they need to be reduced to literals of a given type.

use crate::simplify_expressions::ExprSimplifier;
use arrow::datatypes::ArrowPrimitiveType;
use datafusion_common::{
    DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue, plan_datafusion_err,
    plan_err,
};
use datafusion_expr::Expr;
use datafusion_expr::simplify::SimplifyContext;
use std::sync::Arc;

/// Parse and simplifies an expression to a numeric literal,
/// corresponding to an arrow primitive type `T` (for example, Float64Type).
///
/// This function simplifies and coerces the expression, then extracts the underlying
/// native type using `TryFrom<ScalarValue>`.
///
/// # Example
/// ```ignore
/// let value: f64 = parse_literal::<Float64Type>(expr)?;
/// ```
pub fn parse_literal<T>(expr: &Expr) -> Result<T::Native>
where
    T: ArrowPrimitiveType,
    T::Native: TryFrom<ScalarValue, Error = DataFusionError>,
{
    // Empty schema is sufficient because it parses only literal expressions
    let schema = DFSchemaRef::new(DFSchema::empty());

    log::debug!("Parsing expr {:?} to type {}", expr, T::DATA_TYPE);

    let simplifier =
        ExprSimplifier::new(SimplifyContext::default().with_schema(Arc::clone(&schema)));

    // Simplify and coerce expression in case of constant arithmetic operations (e.g., 10 + 5)
    let simplified_expr: Expr = simplifier
        .simplify(expr.clone())
        .map_err(|err| plan_datafusion_err!("Cannot simplify {expr:?}: {err}"))?;
    let coerced_expr: Expr = simplifier.coerce(simplified_expr, schema.as_ref())?;
    log::debug!("Coerced expression: {:?}", &coerced_expr);

    match coerced_expr {
        Expr::Literal(scalar_value, _) => {
            // It is a literal - proceed to the underlying value
            // Cast to the target type if needed
            let casted_scalar = scalar_value.cast_to(&T::DATA_TYPE)?;

            // Extract the native type
            T::Native::try_from(casted_scalar).map_err(|err| {
                plan_datafusion_err!(
                    "Cannot extract {} from scalar value: {err}",
                    std::any::type_name::<T>()
                )
            })
        }
        actual => {
            plan_err!(
                "Cannot extract literal from coerced {actual:?} expression given {expr:?} expression"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Float64Type, Int64Type};
    use datafusion_expr::{BinaryExpr, lit};
    use datafusion_expr_common::operator::Operator;

    #[test]
    fn test_parse_sql_float_literal() {
        let test_cases = vec![
            (Expr::Literal(ScalarValue::Float64(Some(0.0)), None), 0.0),
            (Expr::Literal(ScalarValue::Float64(Some(1.0)), None), 1.0),
            (
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(lit(50.0)),
                    Operator::Minus,
                    Box::new(lit(10.0)),
                )),
                40.0,
            ),
            (
                Expr::Literal(ScalarValue::Utf8(Some("1e2".into())), None),
                100.0,
            ),
            (
                Expr::Literal(ScalarValue::Utf8(Some("2.5e-1".into())), None),
                0.25,
            ),
        ];

        for (expr, expected) in test_cases {
            let result: Result<f64> = parse_literal::<Float64Type>(&expr);

            match result {
                Ok(value) => {
                    assert!(
                        (value - expected).abs() < 1e-10,
                        "For expression '{expr}': expected {expected}, got {value}",
                    );
                }
                Err(e) => panic!("Failed to parse expression '{expr}': {e}"),
            }
        }
    }

    #[test]
    fn test_parse_sql_integer_literal() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit(2)),
            Operator::Plus,
            Box::new(lit(4)),
        ));

        let result: Result<i64> = parse_literal::<Int64Type>(&expr);

        match result {
            Ok(value) => {
                assert_eq!(6, value);
            }
            Err(e) => panic!("Failed to parse expression: {e}"),
        }
    }
}
