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

//! Declaration of built-in (scalar) functions.
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

use std::ops::Neg;
use std::sync::Arc;

use arrow::{array::ArrayRef, datatypes::Schema};
use arrow_array::Array;

use datafusion_common::{DFSchema, Result, ScalarValue};
pub use datafusion_expr::FuncMonotonicity;
use datafusion_expr::{
    type_coercion::functions::data_types, ColumnarValue, ScalarFunctionImplementation,
};
use datafusion_expr::{Expr, ScalarFunctionDefinition, ScalarUDF};

use crate::sort_properties::SortProperties;
use crate::{PhysicalExpr, ScalarFunctionExpr};

/// Create a physical (function) expression.
/// This function errors when `args`' can't be coerced to a valid argument type of the function.
pub fn create_physical_expr(
    fun: &ScalarUDF,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    args: &[Expr],
    input_dfschema: &DFSchema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let input_expr_types = input_phy_exprs
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    // verify that input data types is consistent with function's `TypeSignature`
    data_types(&input_expr_types, fun.signature())?;

    // Since we have arg_types, we don't need args and schema.
    let return_type =
        fun.return_type_from_exprs(args, input_dfschema, &input_expr_types)?;

    let fun_def = ScalarFunctionDefinition::UDF(Arc::new(fun.clone()));
    Ok(Arc::new(ScalarFunctionExpr::new(
        fun.name(),
        fun_def,
        input_phy_exprs.to_vec(),
        return_type,
        fun.monotonicity()?,
        fun.signature().type_signature.supports_zero_argument(),
    )))
}

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

/// Determines a [`ScalarFunctionExpr`]'s monotonicity for the given arguments
/// and the function's behavior depending on its arguments.
pub fn out_ordering(
    func: &FuncMonotonicity,
    arg_orderings: &[SortProperties],
) -> SortProperties {
    func.iter().zip(arg_orderings).fold(
        SortProperties::Singleton,
        |prev_sort, (item, arg)| {
            let current_sort = func_order_in_one_dimension(item, arg);

            match (prev_sort, current_sort) {
                (_, SortProperties::Unordered) => SortProperties::Unordered,
                (SortProperties::Singleton, SortProperties::Ordered(_)) => current_sort,
                (SortProperties::Ordered(prev), SortProperties::Ordered(current))
                    if prev.descending != current.descending =>
                {
                    SortProperties::Unordered
                }
                _ => prev_sort,
            }
        },
    )
}

/// This function decides the monotonicity property of a [`ScalarFunctionExpr`] for a single argument (i.e. across a single dimension), given that argument's sort properties.
fn func_order_in_one_dimension(
    func_monotonicity: &Option<bool>,
    arg: &SortProperties,
) -> SortProperties {
    if *arg == SortProperties::Singleton {
        SortProperties::Singleton
    } else {
        match func_monotonicity {
            None => SortProperties::Unordered,
            Some(false) => {
                if let SortProperties::Ordered(_) = arg {
                    arg.neg()
                } else {
                    SortProperties::Unordered
                }
            }
            Some(true) => {
                if let SortProperties::Ordered(_) = arg {
                    *arg
                } else {
                    SortProperties::Unordered
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, ArrayRef, UInt64Array},
        datatypes::{DataType, Field},
    };
    use arrow_schema::DataType::Utf8;

    use datafusion_common::cast::as_uint64_array;
    use datafusion_common::{internal_err, plan_err};
    use datafusion_common::{DataFusionError, Result, ScalarValue};
    use datafusion_expr::type_coercion::functions::data_types;
    use datafusion_expr::{Signature, Volatility};

    use crate::expressions::try_cast;
    use crate::utils::tests::TestScalarUDF;

    use super::*;

    #[test]
    fn test_empty_arguments_error() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let udf = ScalarUDF::new_from_impl(TestScalarUDF {
            signature: Signature::variadic(vec![Utf8], Volatility::Immutable),
        });
        let expr = create_physical_expr_with_type_coercion(
            &udf,
            &[],
            &schema,
            &[],
            &DFSchema::empty(),
        );

        match expr {
            Ok(..) => {
                return plan_err!(
                    "ScalarUDF function {udf:?} does not support empty arguments"
                );
            }
            Err(DataFusionError::Plan(_)) => {
                // Continue the loop
            }
            Err(..) => {
                return internal_err!(
                    "ScalarUDF function {udf:?} didn't got the right error with empty arguments");
            }
        }

        Ok(())
    }

    // Helper function just for testing.
    // Returns `expressions` coerced to types compatible with
    // `signature`, if possible.
    pub fn coerce(
        expressions: &[Arc<dyn PhysicalExpr>],
        schema: &Schema,
        signature: &Signature,
    ) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
        if expressions.is_empty() {
            return Ok(vec![]);
        }

        let current_types = expressions
            .iter()
            .map(|e| e.data_type(schema))
            .collect::<Result<Vec<_>>>()?;

        let new_types = data_types(&current_types, signature)?;

        expressions
            .iter()
            .enumerate()
            .map(|(i, expr)| try_cast(expr.clone(), schema, new_types[i].clone()))
            .collect::<Result<Vec<_>>>()
    }

    // Helper function just for testing.
    // The type coercion will be done in the logical phase, should do the type coercion for the test
    fn create_physical_expr_with_type_coercion(
        fun: &ScalarUDF,
        input_phy_exprs: &[Arc<dyn PhysicalExpr>],
        input_schema: &Schema,
        args: &[Expr],
        input_dfschema: &DFSchema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let type_coerced_phy_exprs =
            coerce(input_phy_exprs, input_schema, fun.signature()).unwrap();
        create_physical_expr(
            fun,
            &type_coerced_phy_exprs,
            input_schema,
            args,
            input_dfschema,
        )
    }

    fn dummy_function(args: &[ArrayRef]) -> Result<ArrayRef> {
        let result: UInt64Array =
            args.iter().map(|array| Some(array.len() as u64)).collect();
        Ok(Arc::new(result) as ArrayRef)
    }

    fn unpack_uint64_array(col: Result<ColumnarValue>) -> Result<Vec<u64>> {
        if let ColumnarValue::Array(array) = col? {
            Ok(as_uint64_array(&array)?.values().to_vec())
        } else {
            internal_err!("Unexpected scalar created by a test function")
        }
    }

    #[test]
    fn test_make_scalar_function() -> Result<()> {
        let adapter_func = make_scalar_function_inner(dummy_function);

        let scalar_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let array_arg = ColumnarValue::Array(
            ScalarValue::Int64(Some(1))
                .to_array_of_size(5)
                .expect("Failed to convert to array of size"),
        );
        let result = unpack_uint64_array(adapter_func(&[array_arg, scalar_arg]))?;
        assert_eq!(result, vec![5, 5]);

        Ok(())
    }

    #[test]
    fn test_make_scalar_function_with_no_hints() -> Result<()> {
        let adapter_func = make_scalar_function_with_hints(dummy_function, vec![]);

        let scalar_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let array_arg = ColumnarValue::Array(
            ScalarValue::Int64(Some(1))
                .to_array_of_size(5)
                .expect("Failed to convert to array of size"),
        );
        let result = unpack_uint64_array(adapter_func(&[array_arg, scalar_arg]))?;
        assert_eq!(result, vec![5, 5]);

        Ok(())
    }

    #[test]
    fn test_make_scalar_function_with_hints() -> Result<()> {
        let adapter_func = make_scalar_function_with_hints(
            dummy_function,
            vec![Hint::Pad, Hint::AcceptsSingular],
        );

        let scalar_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let array_arg = ColumnarValue::Array(
            ScalarValue::Int64(Some(1))
                .to_array_of_size(5)
                .expect("Failed to convert to array of size"),
        );
        let result = unpack_uint64_array(adapter_func(&[array_arg, scalar_arg]))?;
        assert_eq!(result, vec![5, 1]);

        Ok(())
    }

    #[test]
    fn test_make_scalar_function_with_hints_on_arrays() -> Result<()> {
        let array_arg = ColumnarValue::Array(
            ScalarValue::Int64(Some(1))
                .to_array_of_size(5)
                .expect("Failed to convert to array of size"),
        );
        let adapter_func = make_scalar_function_with_hints(
            dummy_function,
            vec![Hint::Pad, Hint::AcceptsSingular],
        );

        let result = unpack_uint64_array(adapter_func(&[array_arg.clone(), array_arg]))?;
        assert_eq!(result, vec![5, 5]);

        Ok(())
    }

    #[test]
    fn test_make_scalar_function_with_mixed_hints() -> Result<()> {
        let adapter_func = make_scalar_function_with_hints(
            dummy_function,
            vec![Hint::Pad, Hint::AcceptsSingular, Hint::Pad],
        );

        let scalar_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let array_arg = ColumnarValue::Array(
            ScalarValue::Int64(Some(1))
                .to_array_of_size(5)
                .expect("Failed to convert to array of size"),
        );
        let result = unpack_uint64_array(adapter_func(&[
            array_arg,
            scalar_arg.clone(),
            scalar_arg,
        ]))?;
        assert_eq!(result, vec![5, 1, 5]);

        Ok(())
    }

    #[test]
    fn test_make_scalar_function_with_more_arguments_than_hints() -> Result<()> {
        let adapter_func = make_scalar_function_with_hints(
            dummy_function,
            vec![Hint::Pad, Hint::AcceptsSingular, Hint::Pad],
        );

        let scalar_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let array_arg = ColumnarValue::Array(
            ScalarValue::Int64(Some(1))
                .to_array_of_size(5)
                .expect("Failed to convert to array of size"),
        );
        let result = unpack_uint64_array(adapter_func(&[
            array_arg.clone(),
            scalar_arg.clone(),
            scalar_arg,
            array_arg,
        ]))?;
        assert_eq!(result, vec![5, 1, 5, 5]);

        Ok(())
    }

    #[test]
    fn test_make_scalar_function_with_hints_than_arguments() -> Result<()> {
        let adapter_func = make_scalar_function_with_hints(
            dummy_function,
            vec![
                Hint::Pad,
                Hint::AcceptsSingular,
                Hint::Pad,
                Hint::Pad,
                Hint::AcceptsSingular,
                Hint::Pad,
            ],
        );

        let scalar_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let array_arg = ColumnarValue::Array(
            ScalarValue::Int64(Some(1))
                .to_array_of_size(5)
                .expect("Failed to convert to array of size"),
        );
        let result = unpack_uint64_array(adapter_func(&[array_arg, scalar_arg]))?;
        assert_eq!(result, vec![5, 1]);

        Ok(())
    }
}
