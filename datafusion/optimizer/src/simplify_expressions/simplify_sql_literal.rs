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

//! Parses and simplifies a SQL expression to a literal of a given type.
//!
//! This module provides functionality to parse and simplify static SQL expressions
//! used in SQL constructs like `FROM TABLE SAMPLE (10 + 50 * 2)`. If they are required
//! in a planning (not an execution) phase, they need to be reduced to literals of a given type.

use crate::simplify_expressions::ExprSimplifier;
use arrow::datatypes::ArrowPrimitiveType;
use datafusion_common::{
    DFSchemaRef, DataFusionError, Result, ScalarValue, plan_datafusion_err, plan_err,
};
use datafusion_expr::Expr;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::planner::RelationPlannerContext;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::sqlparser::ast;
use std::sync::Arc;

/// Parse and simplifies a SQL expression to a numeric literal,
/// corresponding to an arrow primitive type `T` (for example, Float64Type).
///
/// This function simplifies and coerces the expression, then extracts the underlying
/// native type using `TryFrom<ScalarValue>`.
///
/// # Arguments
/// * `expr` - A logical AST expression
/// * `schema` - Schema reference for expression planning
/// * `context` - `RelationPlannerContext` context
///
/// # Returns
/// A `Result` containing a literal type
///
/// # Example
/// ```ignore
/// let value: f64 = parse_sql_literal::<Float64Type>(&expr, &schema, &mut relPlannerContext)?;
/// ```
pub fn parse_sql_literal<T>(
    expr: &ast::Expr,
    schema: &DFSchemaRef,
    context: &mut dyn RelationPlannerContext,
) -> Result<T::Native>
where
    T: ArrowPrimitiveType,
    <T as ArrowPrimitiveType>::Native: TryFrom<ScalarValue, Error = DataFusionError>,
{
    match context.sql_to_expr(expr.clone(), &Arc::clone(schema)) {
        Ok(logical_expr) => {
            log::debug!("Parsing expr {:?} to type {}", logical_expr, T::DATA_TYPE);

            let execution_props = ExecutionProps::new();
            let simplifier = ExprSimplifier::new(
                SimplifyContext::new(&execution_props).with_schema(Arc::clone(schema)),
            );

            // Simplify and coerce expression in case of constant arithmetic operations (e.g., 10 + 5)
            let simplified_expr: Expr = simplifier
                .simplify(logical_expr.clone())
                .map_err(|err| plan_datafusion_err!("Cannot simplify {expr:?}: {err}"))?;
            let coerced_expr: Expr = simplifier.coerce(simplified_expr, schema)?;
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
        Err(err) => {
            plan_err!("Cannot construct logical expression from {expr:?}: {err}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Float64Type, Int64Type};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{DFSchema, TableReference, not_impl_err};
    use datafusion_expr::planner::ContextProvider;
    use datafusion_expr::sqlparser::parser::Parser;
    use datafusion_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};
    use datafusion_sql::planner::{PlannerContext, SqlToRel};
    use datafusion_sql::relation::SqlToRelRelationContext;
    use datafusion_sql::sqlparser::dialect::GenericDialect;
    use std::sync::Arc;

    // Simple mock context provider for testing
    struct MockContextProvider {
        options: ConfigOptions,
    }

    impl ContextProvider for MockContextProvider {
        fn get_table_source(&self, _: TableReference) -> Result<Arc<dyn TableSource>> {
            not_impl_err!("mock")
        }

        fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
            None
        }

        fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
            None
        }

        fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
            None
        }

        fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
            None
        }

        fn options(&self) -> &ConfigOptions {
            &self.options
        }

        fn udf_names(&self) -> Vec<String> {
            vec![]
        }

        fn udaf_names(&self) -> Vec<String> {
            vec![]
        }

        fn udwf_names(&self) -> Vec<String> {
            vec![]
        }
    }

    #[test]
    fn test_parse_sql_float_literal() {
        let test_cases = vec![
            ("0.0", 0.0),
            ("1.0", 1.0),
            ("0", 0.0),
            ("1", 1.0),
            ("0.5", 0.5),
            ("100.0", 100.0),
            ("0.001", 0.001),
            ("999.999", 999.999),
            ("1.0 + 2.0", 3.0),
            ("10.0 * 0.5", 5.0),
            ("100.0 / 4.0", 25.0),
            ("(80.0 + 2.0*10.0) / 4.0", 25.0),
            ("50.0 - 10.0", 40.0),
            ("1e2", 100.0),
            ("1.5e1", 15.0),
            ("2.5e-1", 0.25),
        ];

        let schema = DFSchemaRef::new(DFSchema::empty());
        let context = MockContextProvider {
            options: ConfigOptions::default(),
        };
        let sql_to_rel = SqlToRel::new(&context);
        let mut planner_context = PlannerContext::new();
        let mut sql_context =
            SqlToRelRelationContext::new(&sql_to_rel, &mut planner_context);
        let dialect = GenericDialect {};

        for (sql_expr, expected) in test_cases {
            let ast_expr = Parser::new(&dialect)
                .try_with_sql(sql_expr)
                .unwrap()
                .parse_expr()
                .unwrap();

            let result: Result<f64> =
                parse_sql_literal::<Float64Type>(&ast_expr, &schema, &mut sql_context);

            match result {
                Ok(value) => {
                    assert!(
                        (value - expected).abs() < 1e-10,
                        "For expression '{sql_expr}': expected {expected}, got {value}",
                    );
                }
                Err(e) => panic!("Failed to parse expression '{sql_expr}': {e}"),
            }
        }
    }

    #[test]
    fn test_parse_sql_integer_literal() {
        let schema = DFSchemaRef::new(DFSchema::empty());
        let context = MockContextProvider {
            options: ConfigOptions::default(),
        };
        let sql_to_rel = SqlToRel::new(&context);
        let mut planner_context = PlannerContext::new();
        let mut sql_context =
            SqlToRelRelationContext::new(&sql_to_rel, &mut planner_context);
        let dialect = GenericDialect {};

        // Integer
        let ast_expr = Parser::new(&dialect)
            .try_with_sql("2 + 4")
            .unwrap()
            .parse_expr()
            .unwrap();

        let result: Result<i64> =
            parse_sql_literal::<Int64Type>(&ast_expr, &schema, &mut sql_context);

        match result {
            Ok(value) => {
                assert_eq!(6, value);
            }
            Err(e) => panic!("Failed to parse expression: {e}"),
        }
    }
}
