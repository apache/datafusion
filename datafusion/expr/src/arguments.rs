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

/// Represents a named function argument with its original case and quote information.
///
/// This struct preserves whether an identifier was quoted in the SQL, which determines
/// whether case-sensitive or case-insensitive matching should be used per SQL standards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArgumentName {
    /// The argument name in its original case as it appeared in the SQL
    pub value: String,
    /// Whether the identifier was quoted (e.g., "STR" vs STR)
    /// - true: quoted identifier, requires case-sensitive matching
    /// - false: unquoted identifier, uses case-insensitive matching
    pub is_quoted: bool,
}

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
    arg_names: Vec<Option<ArgumentName>>,
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
fn validate_argument_order(arg_names: &[Option<ArgumentName>]) -> Result<()> {
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
    arg_names: Vec<Option<ArgumentName>>,
) -> Result<Vec<Expr>> {
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
        if let Some(arg_name) = arg_name {
            // Named argument - find parameter index using linear search
            // Match based on SQL identifier rules:
            // - Quoted identifiers: case-sensitive (exact match)
            // - Unquoted identifiers: case-insensitive match
            let param_index = param_names
                .iter()
                .position(|p| {
                    if arg_name.is_quoted {
                        // Quoted: exact case match
                        p == &arg_name.value
                    } else {
                        // Unquoted: case-insensitive match
                        p.eq_ignore_ascii_case(&arg_name.value)
                    }
                })
                .ok_or_else(|| {
                    datafusion_common::plan_datafusion_err!(
                        "Unknown parameter name '{}'. Valid parameters are: [{}]",
                        arg_name.value,
                        param_names.join(", ")
                    )
                })?;

            if result[param_index].is_some() {
                return plan_err!(
                    "Parameter '{}' specified multiple times",
                    arg_name.value
                );
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
        let arg_names = vec![
            Some(ArgumentName {
                value: "a".to_string(),
                is_quoted: false,
            }),
            Some(ArgumentName {
                value: "b".to_string(),
                is_quoted: false,
            }),
        ];

        let result = resolve_function_arguments(&param_names, args, arg_names).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_case_insensitive_parameter_matching() {
        // Parameter names in function signature (lowercase)
        let param_names = vec!["startpos".to_string(), "length".to_string()];

        // Unquoted arguments with different casing should match case-insensitively
        let args = vec![lit(1), lit(10)];
        let arg_names = vec![
            Some(ArgumentName {
                value: "STARTPOS".to_string(),
                is_quoted: false,
            }),
            Some(ArgumentName {
                value: "LENGTH".to_string(),
                is_quoted: false,
            }),
        ];

        let result = resolve_function_arguments(&param_names, args, arg_names).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], lit(1));
        assert_eq!(result[1], lit(10));

        // Test with reordering and different cases
        let args2 = vec![lit(20), lit(5)];
        let arg_names2 = vec![
            Some(ArgumentName {
                value: "Length".to_string(),
                is_quoted: false,
            }),
            Some(ArgumentName {
                value: "StartPos".to_string(),
                is_quoted: false,
            }),
        ];

        let result2 =
            resolve_function_arguments(&param_names, args2, arg_names2).unwrap();
        assert_eq!(result2.len(), 2);
        assert_eq!(result2[0], lit(5)); // startpos
        assert_eq!(result2[1], lit(20)); // length
    }

    #[test]
    fn test_quoted_parameter_case_sensitive() {
        // Parameter names in function signature (lowercase)
        let param_names = vec!["str".to_string(), "start_pos".to_string()];

        // Quoted identifiers with wrong case should fail
        let args = vec![lit("hello"), lit(1)];
        let arg_names = vec![
            Some(ArgumentName {
                value: "STR".to_string(),
                is_quoted: true,
            }),
            Some(ArgumentName {
                value: "start_pos".to_string(),
                is_quoted: true,
            }),
        ];

        let result = resolve_function_arguments(&param_names, args, arg_names);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unknown parameter")
        );

        // Quoted identifiers with correct case should succeed
        let args2 = vec![lit("hello"), lit(1)];
        let arg_names2 = vec![
            Some(ArgumentName {
                value: "str".to_string(),
                is_quoted: true,
            }),
            Some(ArgumentName {
                value: "start_pos".to_string(),
                is_quoted: true,
            }),
        ];

        let result2 =
            resolve_function_arguments(&param_names, args2, arg_names2).unwrap();
        assert_eq!(result2.len(), 2);
        assert_eq!(result2[0], lit("hello"));
        assert_eq!(result2[1], lit(1));
    }

    #[test]
    fn test_named_reordering() {
        let param_names = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        // Call with: func(c => 3.0, a => 1, b => "hello")
        let args = vec![lit(3.0), lit(1), lit("hello")];
        let arg_names = vec![
            Some(ArgumentName {
                value: "c".to_string(),
                is_quoted: false,
            }),
            Some(ArgumentName {
                value: "a".to_string(),
                is_quoted: false,
            }),
            Some(ArgumentName {
                value: "b".to_string(),
                is_quoted: false,
            }),
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
        let arg_names = vec![
            None,
            Some(ArgumentName {
                value: "c".to_string(),
                is_quoted: false,
            }),
            Some(ArgumentName {
                value: "b".to_string(),
                is_quoted: false,
            }),
        ];

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
        let arg_names = vec![
            Some(ArgumentName {
                value: "a".to_string(),
                is_quoted: false,
            }),
            None,
        ];

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
        let arg_names = vec![
            Some(ArgumentName {
                value: "x".to_string(),
                is_quoted: false,
            }),
            Some(ArgumentName {
                value: "b".to_string(),
                is_quoted: false,
            }),
        ];

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
        let arg_names = vec![
            Some(ArgumentName {
                value: "a".to_string(),
                is_quoted: false,
            }),
            Some(ArgumentName {
                value: "a".to_string(),
                is_quoted: false,
            }),
        ];

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
        let arg_names = vec![
            Some(ArgumentName {
                value: "a".to_string(),
                is_quoted: false,
            }),
            Some(ArgumentName {
                value: "c".to_string(),
                is_quoted: false,
            }),
        ];

        let result = resolve_function_arguments(&param_names, args, arg_names);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Missing required parameter")
        );
    }

    #[test]
    fn test_mixed_case_signature_unquoted_matching() {
        // Test with mixed-case signature parameters (lowercase, camelCase, UPPERCASE)
        // This proves case-insensitive matching works for unquoted identifiers
        let param_names = vec![
            "prefix".to_string(),   // lowercase
            "startPos".to_string(), // camelCase
            "LENGTH".to_string(),   // UPPERCASE
        ];

        // Test 1: All lowercase unquoted arguments should match
        let args1 = vec![lit("a"), lit(1), lit(5)];
        let arg_names1 = vec![
            Some(ArgumentName {
                value: "prefix".to_string(),
                is_quoted: false,
            }),
            Some(ArgumentName {
                value: "startpos".to_string(), // lowercase version of startPos
                is_quoted: false,
            }),
            Some(ArgumentName {
                value: "length".to_string(), // lowercase version of LENGTH
                is_quoted: false,
            }),
        ];

        let result1 =
            resolve_function_arguments(&param_names, args1, arg_names1).unwrap();
        assert_eq!(result1.len(), 3);
        assert_eq!(result1[0], lit("a"));
        assert_eq!(result1[1], lit(1));
        assert_eq!(result1[2], lit(5));

        // Test 2: All uppercase unquoted arguments should match
        let args2 = vec![lit("b"), lit(2), lit(10)];
        let arg_names2 = vec![
            Some(ArgumentName {
                value: "PREFIX".to_string(), // uppercase version of prefix
                is_quoted: false,
            }),
            Some(ArgumentName {
                value: "STARTPOS".to_string(), // uppercase version of startPos
                is_quoted: false,
            }),
            Some(ArgumentName {
                value: "LENGTH".to_string(), // matches UPPERCASE
                is_quoted: false,
            }),
        ];

        let result2 =
            resolve_function_arguments(&param_names, args2, arg_names2).unwrap();
        assert_eq!(result2.len(), 3);
        assert_eq!(result2[0], lit("b"));
        assert_eq!(result2[1], lit(2));
        assert_eq!(result2[2], lit(10));

        // Test 3: Mixed case unquoted arguments should match
        let args3 = vec![lit("c"), lit(3), lit(15)];
        let arg_names3 = vec![
            Some(ArgumentName {
                value: "Prefix".to_string(), // Title case
                is_quoted: false,
            }),
            Some(ArgumentName {
                value: "StartPos".to_string(), // matches camelCase
                is_quoted: false,
            }),
            Some(ArgumentName {
                value: "Length".to_string(), // Title case
                is_quoted: false,
            }),
        ];

        let result3 =
            resolve_function_arguments(&param_names, args3, arg_names3).unwrap();
        assert_eq!(result3.len(), 3);
        assert_eq!(result3[0], lit("c"));
        assert_eq!(result3[1], lit(3));
        assert_eq!(result3[2], lit(15));
    }

    #[test]
    fn test_mixed_case_signature_quoted_matching() {
        // Test that quoted identifiers require exact case match with signature
        let param_names = vec![
            "prefix".to_string(),   // lowercase
            "startPos".to_string(), // camelCase
            "LENGTH".to_string(),   // UPPERCASE
        ];

        // Test 1: Quoted with wrong case should fail for "prefix"
        let args_wrong_prefix = vec![lit("a"), lit(1), lit(5)];
        let arg_names_wrong_prefix = vec![
            Some(ArgumentName {
                value: "PREFIX".to_string(), // Wrong case
                is_quoted: true,
            }),
            Some(ArgumentName {
                value: "startPos".to_string(),
                is_quoted: true,
            }),
            Some(ArgumentName {
                value: "LENGTH".to_string(),
                is_quoted: true,
            }),
        ];

        let result = resolve_function_arguments(
            &param_names,
            args_wrong_prefix,
            arg_names_wrong_prefix,
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unknown parameter")
        );

        // Test 2: Quoted with wrong case should fail for "startPos"
        let args_wrong_startpos = vec![lit("a"), lit(1), lit(5)];
        let arg_names_wrong_startpos = vec![
            Some(ArgumentName {
                value: "prefix".to_string(),
                is_quoted: true,
            }),
            Some(ArgumentName {
                value: "STARTPOS".to_string(), // Wrong case
                is_quoted: true,
            }),
            Some(ArgumentName {
                value: "LENGTH".to_string(),
                is_quoted: true,
            }),
        ];

        let result2 = resolve_function_arguments(
            &param_names,
            args_wrong_startpos,
            arg_names_wrong_startpos,
        );
        assert!(result2.is_err());
        assert!(
            result2
                .unwrap_err()
                .to_string()
                .contains("Unknown parameter")
        );

        // Test 3: Quoted with wrong case should fail for "LENGTH"
        let args_wrong_length = vec![lit("a"), lit(1), lit(5)];
        let arg_names_wrong_length = vec![
            Some(ArgumentName {
                value: "prefix".to_string(),
                is_quoted: true,
            }),
            Some(ArgumentName {
                value: "startPos".to_string(),
                is_quoted: true,
            }),
            Some(ArgumentName {
                value: "length".to_string(), // Wrong case
                is_quoted: true,
            }),
        ];

        let result3 = resolve_function_arguments(
            &param_names,
            args_wrong_length,
            arg_names_wrong_length,
        );
        assert!(result3.is_err());
        assert!(
            result3
                .unwrap_err()
                .to_string()
                .contains("Unknown parameter")
        );

        // Test 4: Quoted with exact case should succeed
        let args_correct = vec![lit("a"), lit(1), lit(5)];
        let arg_names_correct = vec![
            Some(ArgumentName {
                value: "prefix".to_string(), // Exact match
                is_quoted: true,
            }),
            Some(ArgumentName {
                value: "startPos".to_string(), // Exact match
                is_quoted: true,
            }),
            Some(ArgumentName {
                value: "LENGTH".to_string(), // Exact match
                is_quoted: true,
            }),
        ];

        let result4 =
            resolve_function_arguments(&param_names, args_correct, arg_names_correct)
                .unwrap();
        assert_eq!(result4.len(), 3);
        assert_eq!(result4[0], lit("a"));
        assert_eq!(result4[1], lit(1));
        assert_eq!(result4[2], lit(5));
    }
}
