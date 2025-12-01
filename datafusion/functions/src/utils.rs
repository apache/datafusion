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

use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray};
use arrow::compute::try_binary;
use arrow::datatypes::{DataType, DecimalType};
use arrow::error::ArrowError;
use datafusion_common::{not_impl_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::function::Hint;
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

/// Creates a function to identify the optimal return type of a string function given
/// the type of its first argument.
///
/// If the input type is `LargeUtf8` or `LargeBinary` the return type is
/// `$largeUtf8Type`,
///
/// If the input type is `Utf8` or `Binary` the return type is `$utf8Type`,
///
/// If the input type is `Utf8View` the return type is $utf8Type,
macro_rules! get_optimal_return_type {
    ($FUNC:ident, $largeUtf8Type:expr, $utf8Type:expr) => {
        pub(crate) fn $FUNC(arg_type: &DataType, name: &str) -> Result<DataType> {
            Ok(match arg_type {
                // LargeBinary inputs are automatically coerced to Utf8
                DataType::LargeUtf8 | DataType::LargeBinary => $largeUtf8Type,
                // Binary inputs are automatically coerced to Utf8
                DataType::Utf8 | DataType::Binary => $utf8Type,
                // Utf8View max offset size is u32::MAX, the same as UTF8
                DataType::Utf8View | DataType::BinaryView => $utf8Type,
                DataType::Null => DataType::Null,
                DataType::Dictionary(_, value_type) => match **value_type {
                    DataType::LargeUtf8 | DataType::LargeBinary => $largeUtf8Type,
                    DataType::Utf8 | DataType::Binary => $utf8Type,
                    DataType::Null => DataType::Null,
                    _ => {
                        return datafusion_common::exec_err!(
                            "The {} function can only accept strings, but got {:?}.",
                            name.to_uppercase(),
                            **value_type
                        );
                    }
                },
                data_type => {
                    return datafusion_common::exec_err!(
                        "The {} function can only accept strings, but got {:?}.",
                        name.to_uppercase(),
                        data_type
                    );
                }
            })
        }
    };
}

// `utf8_to_str_type`: returns either a Utf8 or LargeUtf8 based on the input type size.
get_optimal_return_type!(utf8_to_str_type, DataType::LargeUtf8, DataType::Utf8);

// `utf8_to_int_type`: returns either a Int32 or Int64 based on the input type size.
get_optimal_return_type!(utf8_to_int_type, DataType::Int64, DataType::Int32);

/// Creates a scalar function implementation for the given function.
/// * `inner` - the function to be executed
/// * `hints` - hints to be used when expanding scalars to arrays
pub fn make_scalar_function<F>(
    inner: F,
    hints: Vec<Hint>,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue>
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef>,
{
    move |args: &[ColumnarValue]| {
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
                arg.to_array(expansion_len)
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
    }
}

/// Computes a binary math function for input arrays using a specified function.
/// Generic types:
/// - `L`: Left array primitive type
/// - `R`: Right array primitive type
/// - `O`: Output array primitive type
/// - `F`: Functor computing `fun(l: L, r: R) -> Result<OutputType>`
pub fn calculate_binary_math<L, R, O, F>(
    left: &dyn Array,
    right: &ColumnarValue,
    fun: F,
) -> Result<Arc<PrimitiveArray<O>>>
where
    L: ArrowPrimitiveType,
    R: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
    F: Fn(L::Native, R::Native) -> Result<O::Native, ArrowError>,
    R::Native: TryFrom<ScalarValue>,
{
    let left = left.as_primitive::<L>();
    let right = right.cast_to(&R::DATA_TYPE, None)?;
    let result = match right {
        ColumnarValue::Scalar(scalar) => {
            if scalar.is_null() {
                // Null scalar is castable to any numeric, creating a non-null expression.
                // Provide null array explicitly to make result null
                PrimitiveArray::<O>::new_null(1)
            } else {
                let right = R::Native::try_from(scalar.clone()).map_err(|_| {
                    DataFusionError::NotImplemented(format!(
                        "Cannot convert scalar value {} to {}",
                        &scalar,
                        R::DATA_TYPE
                    ))
                })?;
                left.try_unary::<_, O, _>(|lvalue| fun(lvalue, right))?
            }
        }
        ColumnarValue::Array(right) => {
            let right = right.as_primitive::<R>();
            try_binary::<_, _, _, O>(left, right, &fun)?
        }
    };
    Ok(Arc::new(result) as _)
}

/// Computes a binary math function for input arrays using a specified function
/// and apply rescaling to given precision and scale.
/// Generic types:
/// - `L`: Left array decimal type
/// - `R`: Right array primitive type
/// - `O`: Output array decimal type
/// - `F`: Functor computing `fun(l: L, r: R) -> Result<OutputType>`
pub fn calculate_binary_decimal_math<L, R, O, F>(
    left: &dyn Array,
    right: &ColumnarValue,
    fun: F,
    precision: u8,
    scale: i8,
) -> Result<Arc<PrimitiveArray<O>>>
where
    L: DecimalType,
    R: ArrowPrimitiveType,
    O: DecimalType,
    F: Fn(L::Native, R::Native) -> Result<O::Native, ArrowError>,
    R::Native: TryFrom<ScalarValue>,
{
    let result_array = calculate_binary_math::<L, R, O, F>(left, right, fun)?;
    if scale < 0 {
        not_impl_err!("Negative scale is not supported for power for decimal types")
    } else {
        Ok(Arc::new(
            result_array
                .as_ref()
                .clone()
                .with_precision_and_scale(precision, scale)?,
        ))
    }
}

/// Converts Decimal128 components (value and scale) to an unscaled i128
pub fn decimal128_to_i128(value: i128, scale: i8) -> Result<i128, ArrowError> {
    if scale < 0 {
        Err(ArrowError::ComputeError(
            "Negative scale is not supported".into(),
        ))
    } else if scale == 0 {
        Ok(value)
    } else {
        match i128::from(10).checked_pow(scale as u32) {
            Some(divisor) => Ok(value / divisor),
            None => Err(ArrowError::ComputeError(format!(
                "Cannot get a power of {scale}"
            ))),
        }
    }
}

#[cfg(test)]
pub mod test {
    /// $FUNC ScalarUDFImpl to test
    /// $ARGS arguments (vec) to pass to function
    /// $EXPECTED a Result<ColumnarValue>
    /// $EXPECTED_TYPE is the expected value type
    /// $EXPECTED_DATA_TYPE is the expected result type
    /// $ARRAY_TYPE is the column type after function applied
    /// $CONFIG_OPTIONS config options to pass to function
    macro_rules! test_function {
    ($FUNC:expr, $ARGS:expr, $EXPECTED:expr, $EXPECTED_TYPE:ty, $EXPECTED_DATA_TYPE:expr, $ARRAY_TYPE:ident, $CONFIG_OPTIONS:expr) => {
        let expected: Result<Option<$EXPECTED_TYPE>> = $EXPECTED;
        let func = $FUNC;

        let data_array = $ARGS.iter().map(|arg| arg.data_type()).collect::<Vec<_>>();
        let cardinality = $ARGS
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            })
            .unwrap_or(1);

            let scalar_arguments = $ARGS.iter().map(|arg| match arg {
                ColumnarValue::Scalar(scalar) => Some(scalar.clone()),
                ColumnarValue::Array(_) => None,
            }).collect::<Vec<_>>();
            let scalar_arguments_refs = scalar_arguments.iter().map(|arg| arg.as_ref()).collect::<Vec<_>>();

            let nullables = $ARGS.iter().map(|arg| match arg {
                ColumnarValue::Scalar(scalar) => scalar.is_null(),
                ColumnarValue::Array(a) => a.null_count() > 0,
            }).collect::<Vec<_>>();

            let field_array = data_array.into_iter().zip(nullables).enumerate()
                .map(|(idx, (data_type, nullable))| arrow::datatypes::Field::new(format!("field_{idx}"), data_type, nullable))
            .map(std::sync::Arc::new)
            .collect::<Vec<_>>();

        let return_field = func.return_field_from_args(datafusion_expr::ReturnFieldArgs {
            arg_fields: &field_array,
            scalar_arguments: &scalar_arguments_refs,
        });
            let arg_fields = $ARGS.iter()
            .enumerate()
                .map(|(idx, arg)| arrow::datatypes::Field::new(format!("f_{idx}"), arg.data_type(), true).into())
            .collect::<Vec<_>>();

        match expected {
            Ok(expected) => {
                assert_eq!(return_field.is_ok(), true);
                let return_field = return_field.unwrap();
                let return_type = return_field.data_type();
                assert_eq!(return_type, &$EXPECTED_DATA_TYPE);

                    let result = func.invoke_with_args(datafusion_expr::ScalarFunctionArgs{
                    args: $ARGS,
                    arg_fields,
                    number_rows: cardinality,
                    return_field,
                        config_options: $CONFIG_OPTIONS
                });
                    assert_eq!(result.is_ok(), true, "function returned an error: {}", result.unwrap_err());

                    let result = result.unwrap().to_array(cardinality).expect("Failed to convert to array");
                    let result = result.as_any().downcast_ref::<$ARRAY_TYPE>().expect("Failed to convert to type");
                assert_eq!(result.data_type(), &$EXPECTED_DATA_TYPE);

                // value is correct
                match expected {
                    Some(v) => assert_eq!(result.value(0), v),
                    None => assert!(result.is_null(0)),
                };
            }
            Err(expected_error) => {
                if let Ok(return_field) = return_field {
                    // invoke is expected error - cannot use .expect_err() due to Debug not being implemented
                    match func.invoke_with_args(datafusion_expr::ScalarFunctionArgs {
                        args: $ARGS,
                        arg_fields,
                        number_rows: cardinality,
                        return_field,
                        config_options: $CONFIG_OPTIONS,
                    }) {
                        Ok(_) => assert!(false, "expected error"),
                        Err(error) => {
                            assert!(expected_error
                                .strip_backtrace()
                                .starts_with(&error.strip_backtrace()));
                        }
                    }
                } else if let Err(error) = return_field {
                    datafusion_common::assert_contains!(
                        expected_error.strip_backtrace(),
                        error.strip_backtrace()
                    );
                }
            }
        };
    };

        ($FUNC:expr, $ARGS:expr, $EXPECTED:expr, $EXPECTED_TYPE:ty, $EXPECTED_DATA_TYPE:expr, $ARRAY_TYPE:ident) => {
            test_function!(
                $FUNC,
                $ARGS,
                $EXPECTED,
                $EXPECTED_TYPE,
                $EXPECTED_DATA_TYPE,
                $ARRAY_TYPE,
                std::sync::Arc::new(datafusion_common::config::ConfigOptions::default())
            )
        };
    }

    use arrow::datatypes::DataType;
    pub(crate) use test_function;

    use super::*;

    #[test]
    fn string_to_int_type() {
        let v = utf8_to_int_type(&DataType::Utf8, "test").unwrap();
        assert_eq!(v, DataType::Int32);

        let v = utf8_to_int_type(&DataType::Utf8View, "test").unwrap();
        assert_eq!(v, DataType::Int32);

        let v = utf8_to_int_type(&DataType::LargeUtf8, "test").unwrap();
        assert_eq!(v, DataType::Int64);
    }

    #[test]
    fn test_decimal128_to_i128() {
        let cases = [
            (123, 0, Some(123)),
            (1230, 1, Some(123)),
            (123000, 3, Some(123)),
            (1, 0, Some(1)),
            (123, -3, None),
            (123, i8::MAX, None),
            (i128::MAX, 0, Some(i128::MAX)),
            (i128::MAX, 3, Some(i128::MAX / 1000)),
        ];

        for (value, scale, expected) in cases {
            match decimal128_to_i128(value, scale) {
                Ok(actual) => {
                    assert_eq!(
                        actual,
                        expected.expect("Got value but expected none"),
                        "{value} and {scale} vs {expected:?}"
                    );
                }
                Err(_) => assert!(expected.is_none()),
            }
        }
    }
}
