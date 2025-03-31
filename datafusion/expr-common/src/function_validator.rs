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

//! Utilities for validating function arguments based on signatures.
//! This module provides functions for checking if arguments match function signatures
//! and generating detailed error messages when they don't.

use arrow::datatypes::DataType;
use datafusion_common::{
    diagnostic_utils::{
        create_arg_count_mismatch_diagnostic, create_type_mismatch_diagnostic,
    },
    exec_datafusion_err, plan_datafusion_err, Diagnostic, Result, Span,
};

use crate::columnar_value::ColumnarValue;
use crate::signature::{ArrayFunctionSignature, Signature, TypeSignature};

/// Create a comprehensive diagnostic for function argument mismatches
///
/// This function analyzes a function signature and actual argument types
/// to create detailed diagnostic information for error reporting.
///
/// # Arguments
/// * `function_name` - Name of the function
/// * `signature` - Function signature to validate against
/// * `current_types` - Actual argument types provided
/// * `span` - Optional source code span for error location
///
/// # Returns
/// A Diagnostic object with detailed error information
pub fn create_signature_mismatch_diagnostic(
    function_name: &str,
    signature: &Signature,
    current_types: &[DataType],
    span: Option<Span>,
) -> Diagnostic {
    // Handle zero arguments case first
    if current_types.is_empty() && !signature.type_signature.supports_zero_argument() {
        let mut diagnostic = Diagnostic::new_error(
            format!(
                "Wrong number of arguments for {} function call",
                function_name
            ),
            span,
        );
        diagnostic.add_note(
            format!("Function {} doesn't accept zero arguments", function_name),
            None,
        );
        return diagnostic;
    }

    // Check if argument count mismatch is the issue
    let possible_arg_counts = collect_possible_arg_counts(&signature.type_signature);

    if !possible_arg_counts.contains(&current_types.len()) {
        return create_arg_count_mismatch_diagnostic(
            function_name,
            &possible_arg_counts,
            current_types.len(),
            span,
        );
    }

    // If we got here, the argument count matches at least one signature variant,
    // but there might be type mismatches
    create_signature_type_mismatch_diagnostic(
        function_name,
        signature,
        current_types,
        span,
    )
}

/// Collects all possible argument counts accepted by a signature
fn collect_possible_arg_counts(type_signature: &TypeSignature) -> Vec<usize> {
    let mut counts = Vec::new();
    match type_signature {
        TypeSignature::Nullary => {
            counts.push(0);
        }
        TypeSignature::Numeric(n)
        | TypeSignature::String(n)
        | TypeSignature::Comparable(n)
        | TypeSignature::Any(n) => {
            counts.push(*n);
        }
        TypeSignature::Coercible(c) => {
            counts.push(c.len());
        }
        TypeSignature::Uniform(n, _) => {
            if *n > 0 {
                counts.push(*n);
            }
        }
        TypeSignature::Exact(types) => {
            counts.push(types.len());
        }
        TypeSignature::VariadicAny => {
            // Variadic functions accept 1 or more arguments
            for i in 1..=10 {
                // Arbitrary upper limit for practicality
                counts.push(i);
            }
        }
        TypeSignature::Variadic(_) => {
            // Variadic functions accept 0 or more arguments of the same type
            for i in 0..=10 {
                // Arbitrary upper limit for practicality
                counts.push(i);
            }
        }
        TypeSignature::ArraySignature(ref func_sig) => match func_sig {
            ArrayFunctionSignature::Array { arguments, .. } => {
                // The number of arguments is determined by the arguments vector
                counts.push(arguments.len());
            }
            ArrayFunctionSignature::RecursiveArray | ArrayFunctionSignature::MapArray => {
                // These both take a single argument
                counts.push(1);
            }
        },
        TypeSignature::OneOf(signatures) => {
            for sig in signatures {
                // Recursively collect counts from nested signatures
                let inner_counts = collect_possible_arg_counts(sig);
                counts.extend(inner_counts);
            }

            // Remove duplicates
            counts.sort();
            counts.dedup();
        }
        TypeSignature::UserDefined => {
            // For user-defined functions, we don't know the possible counts
            // so we provide a default behavior
            counts.push(1); // Assume at least one argument as a fallback
        }
    }
    counts
}

/// Creates a detailed diagnostic for type mismatch based on signature
fn create_signature_type_mismatch_diagnostic(
    function_name: &str,
    signature: &Signature,
    current_types: &[DataType],
    span: Option<Span>,
) -> Diagnostic {
    let mut diagnostic = Diagnostic::new_error(
        format!("Type mismatch in {} function call", function_name),
        span,
    );

    // Format the actual types for display
    let types_str = current_types
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

    // Add type-specific suggestions based on signature
    add_type_suggestions(&mut diagnostic, function_name, signature, current_types);

    diagnostic
}

/// Add suggestions about valid argument types based on signature type
fn add_type_suggestions(
    diagnostic: &mut Diagnostic,
    function_name: &str,
    signature: &Signature,
    current_types: &[DataType],
) {
    match &signature.type_signature {
        TypeSignature::Numeric(_) => {
            diagnostic.add_help(
                format!(
                    "Function {} expects numeric arguments (Int32, Int64, Float64, etc.)",
                    function_name
                ),
                None,
            );

            // Check if any of the current types are non-numeric
            for (i, dt) in current_types.iter().enumerate() {
                if !is_numeric_type(dt) && !dt.is_null() {
                    diagnostic.add_note(
                        format!("Argument {} has non-numeric type: {}", i + 1, dt),
                        None,
                    );
                }
            }
        }
        TypeSignature::String(_) => {
            diagnostic.add_help(
                format!(
                    "Function {} expects string arguments (Utf8, LargeUtf8, etc.)",
                    function_name
                ),
                None,
            );

            // Check if any of the current types are non-string
            for (i, dt) in current_types.iter().enumerate() {
                if !is_string_type(dt) && !dt.is_null() {
                    diagnostic.add_note(
                        format!("Argument {} has non-string type: {}", i + 1, dt),
                        None,
                    );
                }
            }
        }
        TypeSignature::Comparable(_) => {
            diagnostic.add_help(
                format!(
                    "Function {} expects comparable arguments of the same type",
                    function_name
                ),
                None,
            );

            // Check for type consistency
            if current_types.len() > 1 {
                let all_same = current_types
                    .windows(2)
                    .all(|w| w[0] == w[1] || w[0].is_null() || w[1].is_null());
                if !all_same {
                    diagnostic
                        .add_note("Arguments have inconsistent types".to_string(), None);
                }
            }
        }
        TypeSignature::Exact(expected_types) => {
            if expected_types.len() == current_types.len() {
                diagnostic.add_help(
                    format!("Function {} expects specific argument types", function_name),
                    None,
                );

                // Compare each argument with expected type
                for (i, (expected, actual)) in
                    expected_types.iter().zip(current_types.iter()).enumerate()
                {
                    if expected != actual && !actual.is_null() {
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
            }
        }
        TypeSignature::Variadic(expected_type) => {
            diagnostic.add_help(
                format!(
                    "Function {} expects arguments of type {}",
                    function_name, expected_type
                ),
                None,
            );

            // Check which arguments don't match
            for (i, actual) in current_types.iter().enumerate() {
                if expected_type != actual && !actual.is_null() {
                    diagnostic.add_note(
                        format!(
                            "Argument {} has type {} but {} expected",
                            i + 1,
                            actual,
                            expected_type
                        ),
                        None,
                    );
                }
            }
        }
        TypeSignature::ArraySignature(_) => {
            diagnostic.add_help(
                format!("Function {} expects array arguments", function_name),
                None,
            );

            // Check if any argument is not an array type
            for (i, dt) in current_types.iter().enumerate() {
                match dt {
                    DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _) => {}
                    _ => {
                        diagnostic.add_note(
                            format!("Argument {} has non-array type: {}", i + 1, dt),
                            None,
                        );
                    }
                }
            }
        }
        TypeSignature::OneOf(signatures) => {
            diagnostic.add_help(
                format!(
                    "Function {} accepts different combinations of argument types",
                    function_name
                ),
                None,
            );

            // Try to provide a few example signatures
            let mut example_count = 0;
            for (idx, sig) in signatures.iter().enumerate() {
                if example_count >= 3 {
                    // Limit to 3 examples
                    break;
                }

                match sig {
                    TypeSignature::Exact(types) if types.len() == current_types.len() => {
                        let types_str = types
                            .iter()
                            .map(|dt| format!("{}", dt))
                            .collect::<Vec<_>>()
                            .join(", ");

                        diagnostic.add_note(
                            format!("Example signature {}: ({})", idx + 1, types_str),
                            None,
                        );
                        example_count += 1;
                    }
                    TypeSignature::Numeric(n) if *n == current_types.len() => {
                        diagnostic.add_note(
                            format!(
                                "Example signature {}: {} numeric arguments",
                                idx + 1,
                                n
                            ),
                            None,
                        );
                        example_count += 1;
                    }
                    TypeSignature::String(n) if *n == current_types.len() => {
                        diagnostic.add_note(
                            format!(
                                "Example signature {}: {} string arguments",
                                idx + 1,
                                n
                            ),
                            None,
                        );
                        example_count += 1;
                    }
                    _ => {}
                }
            }

            if example_count == 0 {
                diagnostic.add_note(
                    "Function has complex signature requirements".to_string(),
                    None,
                );
            }
        }
        _ => {
            // For other types, provide a general suggestion
            diagnostic.add_help(
                format!(
                    "Check the documentation for function {} argument requirements",
                    function_name
                ),
                None,
            );
            diagnostic.add_help(
                "You might need to add explicit type casts to the arguments".to_string(),
                None,
            );
        }
    }
}

/// Helper function to check if a type is numeric
fn is_numeric_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// Helper function to check if a type is string
fn is_string_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Helper function to get data types from ColumnarValues
pub fn get_columnar_value_types(args: &[ColumnarValue]) -> Vec<DataType> {
    args.iter()
        .map(|arg| match arg {
            ColumnarValue::Array(arr) => arr.data_type().clone(),
            ColumnarValue::Scalar(scalar) => scalar.data_type(),
        })
        .collect()
}

/// Implement validate_args method for Signature
impl Signature {
    /// Validate function arguments against this signature
    ///
    /// # Arguments
    /// * `function_name` - Name of the function being called
    /// * `args` - Actual argument types provided to the function
    /// * `span` - Optional code span for error reporting
    ///
    /// # Returns
    /// * `Ok(())` if arguments match the signature
    /// * `Err(DataFusionError)` with detailed diagnostic information if arguments don't match
    ///
    /// # Example
    /// ```
    /// let signature = Signature::exact(vec![DataType::Float64, DataType::Int32], Volatility::Immutable);
    /// signature.validate_args("my_function", &[DataType::Float64, DataType::Int32], None)?;
    /// ```
    pub fn validate_args(
        &self,
        function_name: &str,
        args: &[DataType],
        span: Option<Span>,
    ) -> Result<()> {
        // Try to coerce types according to signature
        match data_types(function_name, args, self) {
            Ok(_) => Ok(()), // Arguments can be coerced to match signature
            Err(_) => {
                // Generate detailed diagnostic information
                let diagnostic =
                    create_signature_mismatch_diagnostic(function_name, self, args, span);
                Err(plan_datafusion_err!(
                    "Invalid arguments for function {}",
                    function_name
                )
                .with_diagnostic(diagnostic))
            }
        }
    }
}

/// Validate function arguments against a signature
///
/// This function is useful for cases where the function signature is known but
/// not enforced by a fixed array size.
///
/// # Arguments
/// * `function_name` - The name of the function
/// * `signature` - The function signature for validation
/// * `arg_types` - Data types of the actual arguments
/// * `span` - Optional span indicating the location of the function call
///
/// # Returns
/// * `Ok(())` if the arguments are valid according to the signature
/// * `Err(DataFusionError)` with detailed diagnostic information if arguments are invalid
///
/// # Example
/// ```rust
/// // Validate arguments against signature
/// validate_function_args(
///     "log",
///     &signature,
///     &[DataType::Float64, DataType::Utf8],
///     Some(function_span)
/// )?;
/// ```
pub fn validate_function_args(
    function_name: &str,
    signature: &Signature,
    arg_types: &[DataType],
    span: Option<Span>,
) -> Result<()> {
    signature.validate_args(function_name, arg_types, span)
}

/// Converts a collection of function arguments into a fixed-size array of length N,
/// producing a detailed error message with diagnostic information in case of mismatch.
///
/// If a signature is provided, it will use signature information to create richer diagnostics
/// that consider both argument count and types.
///
/// # Arguments
/// * `function_name` - The name of the function
/// * `args` - The arguments collection
/// * `function_call_site` - Optional span indicating the location of the function call
/// * `signature` - Function signature for enhanced type checking
/// * `arg_types` - Data types of the actual arguments
///
/// # Example
/// ```rust
/// // With signature and type information for rich diagnostics
/// let [arg1, arg2] = take_function_args_with_signature(
///     "my_function",
///     args,
///     Some(span),
///     &signature,
///     &[DataType::Float64, DataType::Float64]
/// )?;
/// ```
pub fn take_function_args_with_signature<const N: usize, T>(
    function_name: &str,
    args: impl IntoIterator<Item = T>,
    function_call_site: Option<Span>,
    signature: &Signature,
    arg_types: &[DataType],
) -> Result<[T; N]> {
    let args_vec = args.into_iter().collect::<Vec<_>>();

    // Validate arguments using the signature
    if let Err(validation_err) =
        signature.validate_args(function_name, arg_types, function_call_site)
    {
        // If validation fails but we have the exact expected number of arguments,
        // we can still try to convert to fixed array (type mismatch will be caught later)
        if args_vec.len() == N {
            // Continue with conversion but remember validation error
            match args_vec.try_into() {
                Ok(args_array) => return Ok(args_array),
                Err(_) => return Err(validation_err), // Should never happen, but just in case
            }
        } else {
            // Wrong argument count or other validation error
            return Err(validation_err);
        }
    }

    // Try to convert to fixed size array
    args_vec.try_into().map_err(|v: Vec<T>| {
        // This should never happen since we already validated the signature,
        // but we handle it just in case
        let base_error = exec_datafusion_err!(
            "{} function requires {} {}, got {}",
            function_name,
            N,
            if N == 1 { "argument" } else { "arguments" },
            v.len()
        );

        let diagnostic = create_arg_count_mismatch_diagnostic(
            function_name,
            &[N],
            v.len(),
            function_call_site,
        );

        base_error.with_diagnostic(diagnostic)
    })
}
