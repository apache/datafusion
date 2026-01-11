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
    scalar_function_name(expr).is_some_and(|name| {
        // Registry of verified array functions
        matches!(name, "array_has" | "array_has_all" | "array_has_any")
    })
}

fn scalar_function_name(expr: &dyn PhysicalExpr) -> Option<&str> {
    expr.as_any()
        .downcast_ref::<ScalarFunctionExpr>()
        .map(ScalarFunctionExpr::name)
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

    #[test]
    fn test_null_check_detection() {
        use datafusion_physical_expr::expressions::Column;

        let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("test", 0));
        assert!(!is_null_check(col_expr.as_ref()));

        // IsNullExpr and IsNotNullExpr detection requires actual instances
        // which need schema setup - tested in integration tests
    }

    #[test]
    fn test_supported_scalar_functions() {
        use datafusion_physical_expr::expressions::Column;

        let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("test", 0));

        // Non-function expressions should return false
        assert!(!is_supported_scalar_function(col_expr.as_ref()));

        // Testing with actual ScalarFunctionExpr requires function setup
        // and is better suited for integration tests
    }
}
