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
//! This module maintains a centralized registry of predicates that can be
//! safely evaluated on nested list columns during Parquet decoding. Adding
//! new supported predicates requires only updating the registry in this module.

use std::sync::Arc;

use datafusion_physical_expr::expressions::{IsNotNullExpr, IsNullExpr};
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};

/// Array functions supported for nested list pushdown.
///
/// These functions have been verified to work correctly when evaluated
/// during Parquet decoding on list columns.
const SUPPORTED_ARRAY_FUNCTIONS: &[&str] =
    &["array_has", "array_has_all", "array_has_any"];

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
pub fn is_supported_list_predicate(name: &str) -> bool {
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
    // NULL checks are universally supported for all column types
    if expr.as_any().downcast_ref::<IsNullExpr>().is_some()
        || expr.as_any().downcast_ref::<IsNotNullExpr>().is_some()
    {
        return true;
    }

    // Check if this is a supported scalar function
    // NOTE: This relies on function names matching exactly. If function names
    // are refactored, this check must be updated. Consider using a trait-based
    // approach (e.g., a marker trait) for more robust detection in the future.
    if let Some(fun) = expr.as_any().downcast_ref::<ScalarFunctionExpr>()
        && is_supported_list_predicate(fun.name())
    {
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
}
