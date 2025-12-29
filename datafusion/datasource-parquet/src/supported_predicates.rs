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

/// Array functions supported for nested list pushdown.
///
/// These functions have been verified to work correctly when evaluated
/// during Parquet decoding on list columns.
const SUPPORTED_ARRAY_FUNCTIONS: &[&str] =
    &["array_has", "array_has_all", "array_has_any"];

/// Trait for physical expressions that support list column pushdown during
/// Parquet decoding.
///
/// Implement this trait on physical expressions that can be safely evaluated
/// on nested list columns during Parquet decoding. This provides a robust,
/// type-safe mechanism for identifying supported predicates.
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

/// Default implementation for all physical expressions.
///
/// Checks if the expression is a supported type (IS NULL, IS NOT NULL) or
/// a scalar function in the registry of supported array functions.
impl SupportsListPushdown for dyn PhysicalExpr {
    fn supports_list_pushdown(&self) -> bool {
        // NULL checks are universally supported for all column types
        if self.as_any().downcast_ref::<IsNullExpr>().is_some()
            || self.as_any().downcast_ref::<IsNotNullExpr>().is_some()
        {
            return true;
        }

        // Check if this is a supported scalar function
        if let Some(fun) = self.as_any().downcast_ref::<ScalarFunctionExpr>() {
            return is_supported_list_predicate(fun.name());
        }

        false
    }
}

/// Checks whether a function name is supported for nested list pushdown.
///
/// Returns `true` if the function is in the registry of supported predicates.
///
/// # Examples
///
/// ```ignore
/// assert!(is_supported_list_predicate("array_has_all"));
/// assert!(is_supported_list_predicate("array_has"));
/// assert!(!is_supported_list_predicate("array_append"));
/// ```
fn is_supported_list_predicate(name: &str) -> bool {
    SUPPORTED_ARRAY_FUNCTIONS.contains(&name)
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
/// - Array functions listed in [`SUPPORTED_ARRAY_FUNCTIONS`]
///
/// # Returns
///
/// `true` if the expression or any of its children contain supported predicates.
pub fn supports_list_predicates(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if expr.supports_list_pushdown() {
        return true;
    }

    // Recursively check children
    expr.children()
        .iter()
        .any(|child| supports_list_predicates(child))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_supported_list_predicate() {
        // Supported functions
        assert!(is_supported_list_predicate("array_has"));
        assert!(is_supported_list_predicate("array_has_all"));
        assert!(is_supported_list_predicate("array_has_any"));

        // Unsupported functions
        assert!(!is_supported_list_predicate("array_append"));
        assert!(!is_supported_list_predicate("array_length"));
        assert!(!is_supported_list_predicate("some_other_function"));
    }

    #[test]
    fn test_trait_based_detection() {
        use datafusion_physical_expr::expressions::Column;
        use std::sync::Arc;

        // Create a simple column expression (should not support pushdown)
        let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("test", 0));
        assert!(!col_expr.supports_list_pushdown());

        // Note: Testing with actual IsNullExpr and ScalarFunctionExpr
        // would require more complex setup and is better suited for
        // integration tests.
    }
}
