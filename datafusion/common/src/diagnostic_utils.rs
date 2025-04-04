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

use crate::{Diagnostic, Result, ScalarValue, Span, _exec_datafusion_err};
use arrow::datatypes::DataType;

/// Create a diagnostic for argument count mismatch in function calls.
///
/// This utility creates a diagnostic with helpful information when a function
/// is called with the wrong number of arguments.
///
/// # Arguments
/// * `function_name` - Name of the function
/// * `expected_counts` - List of valid argument counts
/// * `actual_count` - Actual number of arguments provided
/// * `span` - Optional source code span for error location
///
/// # Returns
/// A Diagnostic object with error message and notes
pub fn create_arg_count_mismatch_diagnostic(
    function_name: &str,
    expected_counts: &[usize],
    actual_count: usize,
    span: Option<Span>,
) -> Diagnostic {
    let mut diagnostic = Diagnostic::new_error(
        format!(
            "Wrong number of arguments for {} function call",
            function_name
        ),
        span,
    );

    // Format possible argument counts for display
    let expected_args_desc = match expected_counts.len() {
        0 => "no arguments".to_string(),
        1 => {
            let count = expected_counts[0];
            if count == 1 {
                "exactly 1 argument".to_string()
            } else {
                format!("exactly {} arguments", count)
            }
        }
        _ => {
            // Check if these are sequential numbers indicating a range
            let is_sequential = expected_counts.windows(2).all(|w| w[1] == w[0] + 1);

            if is_sequential && expected_counts.len() > 2 {
                // Display as a range
                let min = expected_counts.first().unwrap();
                let max = expected_counts.last().unwrap();

                if *min == 0 {
                    format!("between 0 and {} arguments", max)
                } else {
                    format!("between {} and {} arguments", min, max)
                }
            } else {
                // Create a list of possible counts
                let counts_str = expected_counts
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("one of {} arguments", counts_str)
            }
        }
    };

    diagnostic.add_note(
        format!(
            "Function {} expects {}, but {} {} provided",
            function_name,
            expected_args_desc,
            actual_count,
            if actual_count == 1 { "was" } else { "were" }
        ),
        None,
    );

    diagnostic
}

/// Create a basic diagnostic for a type mismatch in function arguments.
///
/// This utility creates a diagnostic for simple type mismatches without
/// requiring knowledge of the full function signature details.
///
/// # Arguments
/// * `function_name` - Name of the function
/// * `expected_types` - Expected argument types (if known)
/// * `actual_types` - Actual argument types provided
/// * `span` - Optional source code span for error location
///
/// # Returns
/// A Diagnostic object with error message and notes
pub fn create_type_mismatch_diagnostic(
    function_name: &str,
    expected_types: Option<&[DataType]>,
    actual_types: &[DataType],
    span: Option<Span>,
) -> Diagnostic {
    let mut diagnostic = Diagnostic::new_error(
        format!("Type mismatch in {} function call", function_name),
        span,
    );

    // Format the actual types for display
    let types_str = actual_types
        .iter()
        .map(|dt| format!("{}", dt))
        .collect::<Vec<_>>()
        .join(", ");

    diagnostic.add_note(
        format!(
            "Function {} cannot accept argument types ({})",
            function_name, types_str
        ),
        None,
    );

    // If expected types are provided, add them as well
    if let Some(expected) = expected_types {
        if expected.len() == actual_types.len() {
            // Show expected vs actual for each argument
            for (i, (expected, actual)) in
                expected.iter().zip(actual_types.iter()).enumerate()
            {
                if expected != actual {
                    diagnostic.add_note(
                        format!(
                            "Argument {} expected type {} but got {}",
                            i + 1,
                            expected,
                            actual
                        ),
                        None,
                    );
                }
            }
        } else {
            // Just show the expected types
            let expected_str = expected
                .iter()
                .map(|dt| format!("{}", dt))
                .collect::<Vec<_>>()
                .join(", ");

            diagnostic
                .add_note(format!("Expected argument types: ({})", expected_str), None);
        }
    }

    // Add general help suggestion
    diagnostic.add_help(
        "You might need to add explicit type casts to the arguments".to_string(),
        None,
    );

    diagnostic
}
