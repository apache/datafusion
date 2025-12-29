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

//! Registry of physical expressions that support nested list column pushdown
//! to the Parquet decoder.
//!
//! This module provides a trait-based approach for determining which predicates
//! can be safely evaluated on nested list columns during Parquet decoding.

use std::sync::Arc;

use datafusion_physical_expr::expressions::{IsNotNullExpr, IsNullExpr};
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};

/// Trait for physical expressions that support list column pushdown during
/// Parquet decoding.
///
/// This trait provides a type-safe mechanism for identifying expressions that
/// can be safely pushed down to the Parquet decoder for evaluation on nested
/// list columns.
///
/// # Implementation Notes
///
/// Expression types in external crates cannot directly implement this trait
/// due to Rust's orphan rules. Instead, we use a blanket implementation that
/// delegates to a registration mechanism.
///
/// # Examples
///
/// ```ignore
/// use datafusion_physical_expr::PhysicalExpr;
/// use datafusion_datasource_parquet::SupportsListPushdown;
///
/// let expr: Arc<dyn PhysicalExpr> = ...;
/// if expr.supports_list_pushdown() {
///     // Can safely push down to Parquet decoder
/// }
/// ```
pub trait SupportsListPushdown {
    /// Returns `true` if this expression supports list column pushdown.
    fn supports_list_pushdown(&self) -> bool;
}

/// Blanket implementation for all physical expressions.
///
/// This delegates to specialized predicates that check whether the concrete
/// expression type is registered as supporting list pushdown. This design
/// allows the trait to work with expression types defined in external crates.
impl SupportsListPushdown for dyn PhysicalExpr {
    fn supports_list_pushdown(&self) -> bool {
        is_null_check(self) || is_supported_scalar_function(self)
    }
}

/// Checks if an expression is a NULL or NOT NULL check.
///
/// These checks are universally supported for all column types.
fn is_null_check(expr: &dyn PhysicalExpr) -> bool {
    expr.as_any().downcast_ref::<IsNullExpr>().is_some()
        || expr.as_any().downcast_ref::<IsNotNullExpr>().is_some()
}

/// Checks if an expression is a scalar function registered for list pushdown.
///
/// Returns `true` if the expression is a `ScalarFunctionExpr` whose function
/// is in the registry of supported operations.
fn is_supported_scalar_function(expr: &dyn PhysicalExpr) -> bool {
    expr.as_any()
        .downcast_ref::<ScalarFunctionExpr>()
        .is_some_and(|fun| {
            // Registry of verified array functions
            matches!(fun.name(), "array_has" | "array_has_all" | "array_has_any")
        })
}

/// Checks whether the given physical expression contains a supported nested
/// predicate (for example, `array_has_all`).
///
/// This function recursively traverses the expression tree to determine if
/// any node contains predicates that support list column pushdown to the
/// Parquet decoder.
///
/// # Supported predicates
///
/// - `IS NULL` and `IS NOT NULL` checks on any column type
/// - Array functions: `array_has`, `array_has_all`, `array_has_any`
///
/// # Returns
///
/// `true` if the expression or any of its children contain supported predicates.
pub fn supports_list_predicates(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.supports_list_pushdown()
        || expr
            .children()
            .iter()
            .any(|child| supports_list_predicates(child))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{Expr, col};
    use datafusion_functions_nested::expr_fn::{
        array_has, array_has_all, array_has_any, make_array,
    };
    use datafusion_physical_expr::expressions::{Column, IsNullExpr};
    use datafusion_physical_expr::planner::logical2physical;

    #[test]
    fn test_null_check_detection() {
        let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("test", 0));
        assert!(!is_null_check(col_expr.as_ref()));

        // Test IS NULL expression
        let is_null_expr: Arc<dyn PhysicalExpr> =
            Arc::new(IsNullExpr::new(Arc::new(Column::new("test", 0))));
        assert!(is_null_check(is_null_expr.as_ref()));
        assert!(is_null_expr.supports_list_pushdown());
    }

    #[test]
    fn test_supported_scalar_functions() {
        let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("test", 0));

        // Non-function expressions should return false
        assert!(!is_supported_scalar_function(col_expr.as_ref()));
    }

    /// Creates a test schema with a list column for building array function expressions.
    fn create_test_schema() -> Arc<arrow::datatypes::Schema> {
        let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
        Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "tags",
            DataType::List(item_field),
            true,
        )]))
    }

    /// Helper to verify a physical expression supports pushdown
    fn assert_supports_pushdown(expr: &Arc<dyn PhysicalExpr>, msg: &str) {
        assert!(expr.supports_list_pushdown(), "{msg}");
        assert!(is_supported_scalar_function(expr.as_ref()));
        assert!(supports_list_predicates(expr));
    }

    #[test]
    fn test_array_has_all_supports_pushdown() {
        let schema = create_test_schema();
        let expr = array_has_all(
            col("tags"),
            make_array(vec![Expr::Literal(
                ScalarValue::Utf8(Some("c".to_string())),
                None,
            )]),
        );
        let physical_expr = logical2physical(&expr, &schema);
        assert_supports_pushdown(
            &physical_expr,
            "array_has_all should support list pushdown",
        );
    }

    #[test]
    fn test_array_has_any_supports_pushdown() {
        let schema = create_test_schema();
        let expr = array_has_any(
            col("tags"),
            make_array(vec![
                Expr::Literal(ScalarValue::Utf8(Some("a".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("d".to_string())), None),
            ]),
        );
        let physical_expr = logical2physical(&expr, &schema);
        assert_supports_pushdown(
            &physical_expr,
            "array_has_any should support list pushdown",
        );
    }

    #[test]
    fn test_array_has_supports_pushdown() {
        let schema = create_test_schema();
        let expr = array_has(
            col("tags"),
            Expr::Literal(ScalarValue::Utf8(Some("c".to_string())), None),
        );
        let physical_expr = logical2physical(&expr, &schema);
        assert_supports_pushdown(
            &physical_expr,
            "array_has should support list pushdown",
        );
    }

    #[test]
    fn test_unsupported_function_does_not_support_pushdown() {
        let schema = create_test_schema();

        // Build a non-supported function expression (e.g., using a simple column)
        let expr = col("tags");
        let physical_expr = logical2physical(&expr, &schema);

        // Verify unsupported expressions are correctly identified
        assert!(
            !physical_expr.supports_list_pushdown(),
            "column reference should not support list pushdown"
        );
        assert!(
            !is_supported_scalar_function(physical_expr.as_ref()),
            "column should not be detected as supported scalar function"
        );
        assert!(
            !supports_list_predicates(&physical_expr),
            "supports_list_predicates should return false for column reference"
        );
    }

    #[test]
    fn test_recursive_detection_in_complex_expression() {
        let schema = create_test_schema();

        // Build a complex expression that contains array_has_all
        // For example: array_has_all(tags, ['c']) (simplified test)
        let expr = array_has_all(
            col("tags"),
            make_array(vec![Expr::Literal(
                ScalarValue::Utf8(Some("c".to_string())),
                None,
            )]),
        );

        let physical_expr = logical2physical(&expr, &schema);

        // Test recursive detection
        assert!(
            supports_list_predicates(&physical_expr),
            "supports_list_predicates should recursively find array_has_all"
        );
    }

    /// Tests that demonstrate the physical plan structure for array functions.
    /// These show that the functions are correctly represented as ScalarFunctionExpr
    /// and can be detected for pushdown.
    mod physical_plan_tests {
        use super::*;

        /// Helper to verify physical plan structure for array functions
        fn verify_array_function_physical_plan(
            physical_expr: &Arc<dyn PhysicalExpr>,
            expected_name: &str,
        ) {
            let scalar_fn = physical_expr
                .as_any()
                .downcast_ref::<ScalarFunctionExpr>()
                .expect("Should be ScalarFunctionExpr");

            assert_eq!(
                scalar_fn.name(),
                expected_name,
                "Function name should be {expected_name}"
            );

            // Verify it has 2 arguments: the column and the array/value
            assert_eq!(
                scalar_fn.children().len(),
                2,
                "{expected_name} should have 2 arguments"
            );

            // Verify pushdown detection
            assert!(
                physical_expr.supports_list_pushdown(),
                "{expected_name} physical expr should support pushdown"
            );
        }

        #[test]
        fn test_array_has_all_physical_plan() {
            let schema = create_test_schema();

            // Build array_has_all(tags, ['rust', 'performance'])
            let expr = array_has_all(
                col("tags"),
                make_array(vec![
                    Expr::Literal(ScalarValue::Utf8(Some("rust".to_string())), None),
                    Expr::Literal(
                        ScalarValue::Utf8(Some("performance".to_string())),
                        None,
                    ),
                ]),
            );

            let physical_expr = logical2physical(&expr, &schema);
            verify_array_function_physical_plan(&physical_expr, "array_has_all");
        }

        #[test]
        fn test_array_has_any_physical_plan() {
            let schema = create_test_schema();

            // Build array_has_any(tags, ['python', 'javascript', 'go'])
            let expr = array_has_any(
                col("tags"),
                make_array(vec![
                    Expr::Literal(ScalarValue::Utf8(Some("python".to_string())), None),
                    Expr::Literal(
                        ScalarValue::Utf8(Some("javascript".to_string())),
                        None,
                    ),
                    Expr::Literal(ScalarValue::Utf8(Some("go".to_string())), None),
                ]),
            );

            let physical_expr = logical2physical(&expr, &schema);
            verify_array_function_physical_plan(&physical_expr, "array_has_any");
        }

        #[test]
        fn test_array_has_physical_plan() {
            let schema = create_test_schema();

            // Build array_has(tags, 'rust')
            let expr = array_has(
                col("tags"),
                Expr::Literal(ScalarValue::Utf8(Some("rust".to_string())), None),
            );

            let physical_expr = logical2physical(&expr, &schema);
            verify_array_function_physical_plan(&physical_expr, "array_has");
        }

        #[test]
        fn test_physical_plan_display() {
            let schema = create_test_schema();

            let test_cases = vec![
                (
                    array_has_all(
                        col("tags"),
                        make_array(vec![Expr::Literal(
                            ScalarValue::Utf8(Some("test".to_string())),
                            None,
                        )]),
                    ),
                    "array_has_all",
                ),
                (
                    array_has_any(
                        col("tags"),
                        make_array(vec![Expr::Literal(
                            ScalarValue::Utf8(Some("test".to_string())),
                            None,
                        )]),
                    ),
                    "array_has_any",
                ),
                (
                    array_has(
                        col("tags"),
                        Expr::Literal(ScalarValue::Utf8(Some("test".to_string())), None),
                    ),
                    "array_has",
                ),
            ];

            for (expr, expected_fn_name) in test_cases {
                let physical_expr = logical2physical(&expr, &schema);
                let display = format!("{physical_expr:?}");
                assert!(
                    display.contains(expected_fn_name),
                    "Display should contain function name {expected_fn_name}: {display}"
                );
            }
        }

        #[test]
        fn test_complex_predicate_with_array_functions() {
            let schema = create_test_schema();

            // Build a more complex expression:
            // array_has_all(tags, ['rust']) OR array_has_any(tags, ['python', 'go'])
            use datafusion_expr::Operator;
            let left = array_has_all(
                col("tags"),
                make_array(vec![Expr::Literal(
                    ScalarValue::Utf8(Some("rust".to_string())),
                    None,
                )]),
            );
            let right = array_has_any(
                col("tags"),
                make_array(vec![
                    Expr::Literal(ScalarValue::Utf8(Some("python".to_string())), None),
                    Expr::Literal(ScalarValue::Utf8(Some("go".to_string())), None),
                ]),
            );
            let expr = Expr::BinaryExpr(datafusion_expr::BinaryExpr {
                left: Box::new(left),
                op: Operator::Or,
                right: Box::new(right),
            });

            let physical_expr = logical2physical(&expr, &schema);

            // Verify that supports_list_predicates recursively finds the supported functions
            assert!(
                supports_list_predicates(&physical_expr),
                "Complex predicate with array functions should support pushdown"
            );

            // Verify the top-level is a BinaryExpr
            assert_eq!(
                physical_expr.children().len(),
                2,
                "OR expression should have 2 children"
            );

            // Verify both children are supported
            for child in physical_expr.children() {
                assert!(
                    supports_list_predicates(child),
                    "Each child should be a supported array function"
                );
            }
        }
    }
}
