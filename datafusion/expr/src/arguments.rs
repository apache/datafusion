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
    // Build HashMap for O(1) parameter name lookups (case-insensitive)
    let param_index_map: HashMap<String, usize> = param_names
        .iter()
        .enumerate()
        .map(|(idx, name)| (name.to_ascii_lowercase(), idx))
        .collect();

    let positional_count = arg_names.iter().filter(|n| n.is_none()).count();

    // Capture args length before consuming the vector
    let args_len = args.len();

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
            // Named argument - O(1) lookup in HashMap (case-insensitive)
            let param_index = param_index_map
                .get(&name.to_ascii_lowercase())
                .copied()
                .ok_or_else(|| {
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

    // Only require parameters up to the number of arguments provided (supports optional parameters)
    let required_count = args_len;
    for i in 0..required_count {
        if result[i].is_none() {
            return plan_err!("Missing required parameter '{}'", param_names[i]);
        }
    }

    // Return only the assigned parameters (handles optional trailing parameters)
    Ok(result.into_iter().take(required_count).flatten().collect())
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
    fn test_case_insensitive_parameter_matching() {
        // Parameter names with mixed case
        let param_names = vec!["StartPos".to_string(), "Length".to_string()];

        // Arguments with different casing should match
        let args = vec![lit(1), lit(10)];
        let arg_names = vec![Some("startpos".to_string()), Some("LENGTH".to_string())];

        let result = resolve_function_arguments(&param_names, args, arg_names).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], lit(1));
        assert_eq!(result[1], lit(10));

        // Test with reordering and different cases
        let args2 = vec![lit(20), lit(5)];
        let arg_names2 = vec![Some("length".to_string()), Some("STARTPOS".to_string())];

        let result2 =
            resolve_function_arguments(&param_names, args2, arg_names2).unwrap();
        assert_eq!(result2.len(), 2);
        assert_eq!(result2[0], lit(5)); // startpos
        assert_eq!(result2[1], lit(20)); // length
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
    fn test_missing_required_parameter() {
        let param_names = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        // Call with: func(a => 1, c => 3.0) - missing 'b'
        let args = vec![lit(1), lit(3.0)];
        let arg_names = vec![Some("a".to_string()), Some("c".to_string())];

        let result = resolve_function_arguments(&param_names, args, arg_names);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Missing required parameter")
        );
    }
}
