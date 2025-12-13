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

//! Argument resolution logic for named function parameters

use crate::Expr;
use datafusion_common::{Result, plan_err};
use std::collections::HashMap;

/// Resolves function arguments, handling named and positional notation.
///
/// This function validates and reorders arguments to match the function's parameter names
/// when named arguments are used.
///
/// # Rules
/// - All positional arguments must come before named arguments
/// - Named arguments can be in any order after positional arguments
/// - Parameter names follow SQL identifier rules: unquoted names are case-insensitive
///   (normalized to lowercase), quoted names are case-sensitive
/// - No duplicate parameter names allowed
///
/// # Arguments
/// * `param_names` - The function's parameter names in order
/// * `args` - The argument expressions
/// * `arg_names` - Optional parameter name for each argument
///
/// # Returns
/// A vector of expressions in the correct order matching the parameter names
///
/// # Examples
/// ```text
/// Given parameters ["a", "b", "c"]
/// And call: func(10, c => 30, b => 20)
/// Returns: [Expr(10), Expr(20), Expr(30)]
/// ```
pub fn resolve_function_arguments(
    param_names: &[String],
    args: Vec<Expr>,
    arg_names: Vec<Option<String>>,
) -> Result<Vec<Expr>> {
    if args.len() != arg_names.len() {
        return plan_err!(
            "Internal error: args length ({}) != arg_names length ({})",
            args.len(),
            arg_names.len()
        );
    }

    // Check if all arguments are positional (fast path)
    if arg_names.iter().all(|name| name.is_none()) {
        return Ok(args);
    }

    validate_argument_order(&arg_names)?;

    reorder_named_arguments(param_names, args, arg_names)
}

/// Validates that positional arguments come before named arguments
fn validate_argument_order(arg_names: &[Option<String>]) -> Result<()> {
    let mut seen_named = false;
    for (i, arg_name) in arg_names.iter().enumerate() {
        match arg_name {
            Some(_) => seen_named = true,
            None if seen_named => {
                return plan_err!(
                    "Positional argument at position {} follows named argument. \
                     All positional arguments must come before named arguments.",
                    i
                );
            }
            None => {}
        }
    }
    Ok(())
}

/// Reorders arguments based on named parameters to match signature order
fn reorder_named_arguments(
    param_names: &[String],
    args: Vec<Expr>,
    arg_names: Vec<Option<String>>,
) -> Result<Vec<Expr>> {
    // Build HashMap for O(1) parameter name lookups
    let param_index_map: HashMap<&str, usize> = param_names
        .iter()
        .enumerate()
        .map(|(idx, name)| (name.as_str(), idx))
        .collect();

    let positional_count = arg_names.iter().filter(|n| n.is_none()).count();

    let expected_arg_count = param_names.len();

    if positional_count > expected_arg_count {
        return plan_err!(
            "Too many positional arguments: expected at most {}, got {}",
            expected_arg_count,
            positional_count
        );
    }

    let mut result: Vec<Option<Expr>> = vec![None; expected_arg_count];

    for (i, (arg, arg_name)) in args.into_iter().zip(arg_names).enumerate() {
        if let Some(name) = arg_name {
            // Named argument - O(1) lookup in HashMap
            let param_index =
                param_index_map.get(name.as_str()).copied().ok_or_else(|| {
                    datafusion_common::plan_datafusion_err!(
                        "Unknown parameter name '{}'. Valid parameters are: [{}]",
                        name,
                        param_names.join(", ")
                    )
                })?;

            if result[param_index].is_some() {
                return plan_err!("Parameter '{}' specified multiple times", name);
            }

            result[param_index] = Some(arg);
        } else {
            result[i] = Some(arg);
        }
    }

    // Find the highest parameter index that was provided
    let max_provided_index = result.iter().rposition(|p| p.is_some()).unwrap_or(0);

    // Convert None values to NULL expressions for missing optional parameters
    let result_with_nulls: Vec<Expr> = result
        .into_iter()
        .take(max_provided_index + 1)
        .map(|opt_expr| {
            opt_expr.unwrap_or_else(|| {
                Expr::Literal(datafusion_common::ScalarValue::Null, None)
            })
        })
        .collect();

    Ok(result_with_nulls)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lit;

    #[test]
    fn test_all_positional() {
        let param_names = vec!["a".to_string(), "b".to_string()];

        let args = vec![lit(1), lit("hello")];
        let arg_names = vec![None, None];

        let result =
            resolve_function_arguments(&param_names, args.clone(), arg_names).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_all_named() {
        let param_names = vec!["a".to_string(), "b".to_string()];

        let args = vec![lit(1), lit("hello")];
        let arg_names = vec![Some("a".to_string()), Some("b".to_string())];

        let result = resolve_function_arguments(&param_names, args, arg_names).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_named_reordering() {
        let param_names = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        // Call with: func(c => 3.0, a => 1, b => "hello")
        let args = vec![lit(3.0), lit(1), lit("hello")];
        let arg_names = vec![
            Some("c".to_string()),
            Some("a".to_string()),
            Some("b".to_string()),
        ];

        let result = resolve_function_arguments(&param_names, args, arg_names).unwrap();

        // Should be reordered to [a, b, c] = [1, "hello", 3.0]
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], lit(1));
        assert_eq!(result[1], lit("hello"));
        assert_eq!(result[2], lit(3.0));
    }

    #[test]
    fn test_mixed_positional_and_named() {
        let param_names = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        // Call with: func(1, c => 3.0, b => "hello")
        let args = vec![lit(1), lit(3.0), lit("hello")];
        let arg_names = vec![None, Some("c".to_string()), Some("b".to_string())];

        let result = resolve_function_arguments(&param_names, args, arg_names).unwrap();

        // Should be reordered to [a, b, c] = [1, "hello", 3.0]
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], lit(1));
        assert_eq!(result[1], lit("hello"));
        assert_eq!(result[2], lit(3.0));
    }

    #[test]
    fn test_positional_after_named_error() {
        let param_names = vec!["a".to_string(), "b".to_string()];

        // Call with: func(a => 1, "hello") - ERROR
        let args = vec![lit(1), lit("hello")];
        let arg_names = vec![Some("a".to_string()), None];

        let result = resolve_function_arguments(&param_names, args, arg_names);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Positional argument")
        );
    }

    #[test]
    fn test_unknown_parameter_name() {
        let param_names = vec!["a".to_string(), "b".to_string()];

        // Call with: func(x => 1, b => "hello") - ERROR
        let args = vec![lit(1), lit("hello")];
        let arg_names = vec![Some("x".to_string()), Some("b".to_string())];

        let result = resolve_function_arguments(&param_names, args, arg_names);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unknown parameter")
        );
    }

    #[test]
    fn test_duplicate_parameter_name() {
        let param_names = vec!["a".to_string(), "b".to_string()];

        // Call with: func(a => 1, a => 2) - ERROR
        let args = vec![lit(1), lit(2)];
        let arg_names = vec![Some("a".to_string()), Some("a".to_string())];

        let result = resolve_function_arguments(&param_names, args, arg_names);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("specified multiple times")
        );
    }

    #[test]
    fn test_skip_middle_optional_parameter() {
        let param_names = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        // Call with: func(a => 1, c => 3.0) - skipping 'b', should fill with NULL
        let args = vec![lit(1), lit(3.0)];
        let arg_names = vec![Some("a".to_string()), Some("c".to_string())];

        let result = resolve_function_arguments(&param_names, args, arg_names).unwrap();

        assert_result_pattern(&result, &[Some(lit(1)), None, Some(lit(3.0))]);
    }

    #[test]
    fn test_skip_multiple_middle_parameters() {
        let param_names = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ];

        // Call with: func(a => 1, e => 5) - skipping b, c, d
        let args = vec![lit(1), lit(5)];
        let arg_names = vec![Some("a".to_string()), Some("e".to_string())];

        let result = resolve_function_arguments(&param_names, args, arg_names).unwrap();

        assert_result_pattern(&result, &[Some(lit(1)), None, None, None, Some(lit(5))]);
    }

    #[test]
    fn test_skip_first_parameters() {
        let param_names = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        // Call with: func(c => 3.0) - skipping a, b
        let args = vec![lit(3.0)];
        let arg_names = vec![Some("c".to_string())];

        let result = resolve_function_arguments(&param_names, args, arg_names).unwrap();

        assert_result_pattern(&result, &[None, None, Some(lit(3.0))]);
    }

    #[test]
    fn test_mixed_positional_and_skipped_named() {
        let param_names = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];

        // Call with: func(1, 2, d => 4) - positional a, b, skip c, named d
        let args = vec![lit(1), lit(2), lit(4)];
        let arg_names = vec![None, None, Some("d".to_string())];

        let result = resolve_function_arguments(&param_names, args, arg_names).unwrap();

        assert_result_pattern(&result, &[Some(lit(1)), Some(lit(2)), None, Some(lit(4))]);
    }

    #[test]
    fn test_alternating_filled_and_skipped() {
        let param_names = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ];

        // Call with: func(a => 1, c => 3, e => 5) - alternating pattern
        let args = vec![lit(1), lit(3), lit(5)];
        let arg_names = vec![
            Some("a".to_string()),
            Some("c".to_string()),
            Some("e".to_string()),
        ];

        let result = resolve_function_arguments(&param_names, args, arg_names).unwrap();

        assert_result_pattern(
            &result,
            &[Some(lit(1)), None, Some(lit(3)), None, Some(lit(5))],
        );
    }

    #[test]
    fn test_reverse_order_with_skips() {
        let param_names = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];

        // Call with: func(d => 4, b => 2) - reverse order with skips
        let args = vec![lit(4), lit(2)];
        let arg_names = vec![Some("d".to_string()), Some("b".to_string())];

        let result = resolve_function_arguments(&param_names, args, arg_names).unwrap();

        assert_result_pattern(&result, &[None, Some(lit(2)), None, Some(lit(4))]);
    }

    #[test]
    fn test_positional_then_far_named() {
        let param_names = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
            "f".to_string(),
        ];

        // Call with: func(1, 2, f => 6) - two positional, then skip to last
        let args = vec![lit(1), lit(2), lit(6)];
        let arg_names = vec![None, None, Some("f".to_string())];

        let result = resolve_function_arguments(&param_names, args, arg_names).unwrap();

        assert_result_pattern(
            &result,
            &[Some(lit(1)), Some(lit(2)), None, None, None, Some(lit(6))],
        );
    }

    #[test]
    fn test_complex_mixed_positional_named_with_multiple_skips() {
        let param_names = vec![
            "p1".to_string(),
            "p2".to_string(),
            "p3".to_string(),
            "p4".to_string(),
            "p5".to_string(),
            "p6".to_string(),
            "p7".to_string(),
        ];

        // Call with: func(100, 200, p5 => 500, p7 => 700)
        // Positional p1, p2, skip p3, p4, named p5, skip p6, named p7
        let args = vec![lit(100), lit(200), lit(500), lit(700)];
        let arg_names = vec![None, None, Some("p5".to_string()), Some("p7".to_string())];

        let result = resolve_function_arguments(&param_names, args, arg_names).unwrap();

        assert_result_pattern(
            &result,
            &[
                Some(lit(100)),
                Some(lit(200)),
                None,
                None,
                Some(lit(500)),
                None,
                Some(lit(700)),
            ],
        );
    }

    #[test]
    fn test_empty_parameter_names_with_named_args() {
        let param_names: Vec<String> = vec![];

        // Call with no parameters but named args provided - should fail
        let args = vec![lit(1)];
        let arg_names = vec![Some("x".to_string())];

        let result = resolve_function_arguments(&param_names, args, arg_names);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unknown parameter name")
        );
    }

    #[test]
    fn test_sparse_parameters() {
        let param_names = vec![
            "p1".to_string(),
            "p2".to_string(),
            "p3".to_string(),
            "p4".to_string(),
            "p5".to_string(),
            "p6".to_string(),
            "p7".to_string(),
            "p8".to_string(),
            "p9".to_string(),
            "p10".to_string(),
        ];

        // Call with: func(p2 => 2, p5 => 5, p9 => 9) - very sparse
        let args = vec![lit(2), lit(5), lit(9)];
        let arg_names = vec![
            Some("p2".to_string()),
            Some("p5".to_string()),
            Some("p9".to_string()),
        ];

        let result = resolve_function_arguments(&param_names, args, arg_names).unwrap();

        assert_result_pattern(
            &result,
            &[
                None,
                Some(lit(2)),
                None,
                None,
                Some(lit(5)),
                None,
                None,
                None,
                Some(lit(9)),
            ],
        );
    }

    fn is_null(expr: &Expr) -> bool {
        matches!(expr, Expr::Literal(datafusion_common::ScalarValue::Null, _))
    }

    fn assert_result_pattern(result: &[Expr], pattern: &[Option<Expr>]) {
        assert_eq!(
            result.len(),
            pattern.len(),
            "Result length mismatch: expected {}, got {}",
            pattern.len(),
            result.len()
        );
        for (i, (actual, expected)) in result.iter().zip(pattern.iter()).enumerate() {
            match expected {
                Some(expected_expr) => {
                    assert_eq!(
                        actual, expected_expr,
                        "Mismatch at position {}: expected {:?}, got {:?}",
                        i, expected_expr, actual
                    );
                }
                None => {
                    assert!(
                        is_null(actual),
                        "Expected NULL at position {}, got {:?}",
                        i,
                        actual
                    );
                }
            }
        }
    }
}
