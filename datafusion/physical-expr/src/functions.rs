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

//! Deprecated module. Add new feature in scalar_function.rs
//!
//! This module contains built-in functions' enumeration and metadata.
//!
//! Generally, a function has:
//! * a signature
//! * a return type, that is a function of the incoming argument's types
//! * the computation, that must accept each valid signature
//!
//! * Signature: see `Signature`
//! * Return type: a function `(arg_types) -> return_type`. E.g. for sqrt, ([f32]) -> f32, ([f64]) -> f64.
//!
//! This module also supports coercion to improve user experience: if
//! an argument i32 is passed to a function that supports f64, the
//! argument is automatically is coerced to f64.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionImplementation};

pub use crate::scalar_function::create_physical_expr;

#[derive(Debug, Clone, Copy)]
pub enum Hint {
    /// Indicates the argument needs to be padded if it is scalar
    Pad,
    /// Indicates the argument can be converted to an array of length 1
    AcceptsSingular,
}

#[deprecated(since = "36.0.0", note = "Use ColumarValue::values_to_arrays instead")]
pub fn columnar_values_to_array(args: &[ColumnarValue]) -> Result<Vec<ArrayRef>> {
    ColumnarValue::values_to_arrays(args)
}

/// Decorates a function to handle [`ScalarValue`]s by converting them to arrays before calling the function
/// and vice-versa after evaluation.
/// Note that this function makes a scalar function with no arguments or all scalar inputs return a scalar.
/// That's said its output will be same for all input rows in a batch.
#[deprecated(
    since = "36.0.0",
    note = "Implement your function directly in terms of ColumnarValue or use `ScalarUDF` instead"
)]
pub fn make_scalar_function<F>(inner: F) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    make_scalar_function_inner(inner)
}

/// Internal implementation, see comments on `make_scalar_function` for caveats
pub(crate) fn make_scalar_function_inner<F>(inner: F) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    make_scalar_function_with_hints(inner, vec![])
}

/// Just like [`make_scalar_function`], decorates the given function to handle both [`ScalarValue`]s and arrays.
/// Additionally can receive a `hints` vector which can be used to control the output arrays when generating them
/// from [`ScalarValue`]s.
///
/// Each element of the `hints` vector gets mapped to the corresponding argument of the function. The number of hints
/// can be less or greater than the number of arguments (for functions with variable number of arguments). Each unmapped
/// argument will assume the default hint (for padding, it is [`Hint::Pad`]).
pub(crate) fn make_scalar_function_with_hints<F>(
    inner: F,
    hints: Vec<Hint>,
) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    Arc::new(move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .zip(hints.iter().chain(std::iter::repeat(&Hint::Pad)))
            .map(|(arg, hint)| {
                // Decide on the length to expand this scalar to depending
                // on the given hints.
                let expansion_len = match hint {
                    Hint::AcceptsSingular => 1,
                    Hint::Pad => inferred_length,
                };
                arg.clone().into_array(expansion_len)
            })
            .collect::<Result<Vec<_>>>()?;

        let result = (inner)(&args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    })
}
