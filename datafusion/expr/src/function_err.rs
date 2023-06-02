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

//! Function_err module enhances frontend error messages for unresolved functions due to incorrect parameters,
//! by providing the correct function signatures.
//!
//! For example, a query like `select round(3.14, 1.1);` would yield:
//! ```text
//! Error during planning: No function matches 'round(Float64, Float64)'. You might need to add explicit type casts.
//!     Candidate functions:
//!     round(Float64, Int64)
//!     round(Float32, Int64)
//!     round(Float64)
//!     round(Float32)
//! ```

use crate::function::signature;
use crate::{BuiltinScalarFunction, TypeSignature};
use arrow::datatypes::DataType;

impl TypeSignature {
    fn to_string_repr(&self) -> Vec<String> {
        match self {
            TypeSignature::Variadic(types) => {
                vec![format!("{}, ..", join_types(types, "/"))]
            }
            TypeSignature::Uniform(arg_count, valid_types) => {
                vec![std::iter::repeat(join_types(valid_types, "/"))
                    .take(*arg_count)
                    .collect::<Vec<String>>()
                    .join(", ")]
            }
            TypeSignature::Exact(types) => {
                vec![join_types(types, ", ")]
            }
            TypeSignature::Any(arg_count) => {
                vec![std::iter::repeat("Any")
                    .take(*arg_count)
                    .collect::<Vec<&str>>()
                    .join(", ")]
            }
            TypeSignature::VariadicEqual(_) => vec!["T, .., T".to_string()],
            TypeSignature::VariadicAny => vec!["Any, .., Any".to_string()],
            TypeSignature::OneOf(sigs) => {
                sigs.iter().flat_map(|s| s.to_string_repr()).collect()
            }
        }
    }
}

/// Helper function to join types with specified delimiter.
fn join_types<T: std::fmt::Display>(types: &[T], delimiter: &str) -> String {
    types
        .iter()
        .map(|t| t.to_string())
        .collect::<Vec<String>>()
        .join(delimiter)
}

/// Creates a detailed error message for a function with wrong signature.
pub fn generate_signature_error_msg(
    fun: &BuiltinScalarFunction,
    input_expr_types: &[DataType],
) -> String {
    let candidate_signatures = signature(fun)
        .type_signature
        .to_string_repr()
        .iter()
        .map(|args_str| format!("\t{}({})", fun, args_str))
        .collect::<Vec<String>>()
        .join("\n");

    format!(
        "No function matches the given name and argument types '{}({})'. You might need to add explicit type casts.\n\tCandidate functions:\n{}",
        fun, join_types(input_expr_types, ", "), candidate_signatures
    )
}
