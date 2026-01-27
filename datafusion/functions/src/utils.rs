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

use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, AsArray, PrimitiveArray,
    PrimitiveBuilder,
};
use arrow::compute::{DecimalCast, try_binary};
use arrow::datatypes::{
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
};
use arrow::error::ArrowError;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_datafusion_err};
use datafusion_expr::ColumnarValue;
use datafusion_expr::function::Hint;
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
///
/// Deprecated. use calculate_binary_math_numeric instead
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

/// Helper to extract a native value from a ScalarValue, providing a DataFusionError
fn try_from_scalar<T>(value: ScalarValue) -> Result<T::Native>
where
    T: ArrowPrimitiveType,
    T::Native: TryFrom<ScalarValue>,
{
    // Construct an error string beforehand to avoid extra cloning
    let err_str = format!(
        "Cannot convert scalar value {} of type {} to {}",
        value,
        value.data_type(),
        T::DATA_TYPE
    );
    T::Native::try_from(value).map_err(|_| DataFusionError::Execution(err_str))
}

/// Extract precision and scale from a decimal DataType, or None if not decimal
fn get_decimal_precision_scale(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Decimal32(precision, scale) => Some((*precision, *scale)),
        DataType::Decimal64(precision, scale) => Some((*precision, *scale)),
        DataType::Decimal128(precision, scale) => Some((*precision, *scale)),
        DataType::Decimal256(precision, scale) => Some((*precision, *scale)),
        _ => None,
    }
}

/// Adapter for `arrow::compute::rescale_decimal` to rescale an array of values
fn rescale_array<T>(
    input: &PrimitiveArray<T>,
    output_precision: u8,
    output_scale: i8,
) -> Result<PrimitiveArray<T>>
where
    T: DecimalType,
    T::Native: DecimalCast + ArrowNativeTypeOp,
{
    let input_precision = input.precision();
    let input_scale = input.scale();
    let mut builder = PrimitiveBuilder::<T>::with_capacity(input.len());
    for i in 0..input.len() {
        if input.is_null(i) {
            builder.append_null();
        } else {
            let value = input.value(i);
            // Change scale of one value using arrow casting function
            match arrow::compute::rescale_decimal::<T, T>(
                value,
                input_precision,
                input_scale,
                output_precision,
                output_scale,
            ) {
                Some(rescaled_value) => builder.append_value(rescaled_value),
                None => builder.append_null(),
            }
        }
    }
    let result: PrimitiveArray<T> = builder
        .finish()
        .with_precision_and_scale(output_precision, output_scale)?;
    Ok(result)
}

/// Rescales an array to the given precision and scale if it is a decimal
/// Returns an execution error otherwise
fn rescale_decimal_array(
    input: &dyn Array,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef> {
    match input.data_type() {
        DataType::Decimal128(_, _) => Ok(Arc::new(rescale_array::<Decimal128Type>(
            input.as_primitive::<Decimal128Type>(),
            precision,
            scale,
        )?)),
        DataType::Decimal256(_, _) => Ok(Arc::new(rescale_array::<Decimal256Type>(
            input.as_primitive::<Decimal256Type>(),
            precision,
            scale,
        )?)),
        DataType::Decimal32(_, _) => Ok(Arc::new(rescale_array::<Decimal32Type>(
            input.as_primitive::<Decimal32Type>(),
            precision,
            scale,
        )?)),
        DataType::Decimal64(_, _) => Ok(Arc::new(rescale_array::<Decimal64Type>(
            input.as_primitive::<Decimal64Type>(),
            precision,
            scale,
        )?)),
        _ => Err(exec_datafusion_err!(
            "Failed to rescale value of non-decimal type {}",
            input.data_type()
        )),
    }
}

/// Rescales a scalar value to the given precision and scale if it is a decimal
/// Returns an execution error otherwise
fn rescale_decimal_scalar(
    input: &ScalarValue,
    precision: u8,
    scale: i8,
) -> Result<ScalarValue> {
    match input {
        ScalarValue::Decimal128(_, _, _) => {
            input.cast_to(&DataType::Decimal128(precision, scale))
        }
        ScalarValue::Decimal256(_, _, _) => {
            input.cast_to(&DataType::Decimal256(precision, scale))
        }
        ScalarValue::Decimal32(_, _, _) => {
            input.cast_to(&DataType::Decimal32(precision, scale))
        }
        ScalarValue::Decimal64(_, _, _) => {
            input.cast_to(&DataType::Decimal64(precision, scale))
        }
        _ => Err(exec_datafusion_err!(
            "Failed to rescale value of non-decimal type {}",
            input.data_type()
        )),
    }
}

/// Cast an array to the given output data type using `arrow::compute::cast`
fn cast_array_to<O>(
    input: Arc<PrimitiveArray<O>>,
    out_type: &DataType,
) -> Result<Arc<PrimitiveArray<O>>>
where
    O: ArrowPrimitiveType,
{
    if input.data_type() == out_type {
        // Return as is
        Ok(input)
    } else {
        // cast to output data type, performing rescaling
        let casted_result = arrow::compute::cast(input.as_ref(), out_type)?;
        casted_result
            .as_primitive_opt::<O>()
            .ok_or_else(|| {
                exec_datafusion_err!("Failed to cast array to type {}", O::DATA_TYPE)
            })
            .map(|arr| Arc::new(arr.clone()))
    }
}

// Shorthand type for decimal precision and scale information
pub type DecScale = Option<(u8, i8)>;

/// A helper function for internal use.
/// Computes a binary math function for input arrays using a specified function.
///
/// Handles left `L` and right `R` Arrow types and perform decimal rescaling on them, if needed.
///
/// If a left type is a decimal, then it's scale is passed as a `DecScale` parameter to the functor,
/// otherwise None is passed.
fn calculate_binary_math_impl<L, R, O, F>(
    left: &dyn Array,
    right: &ColumnarValue,
    fun: F,
) -> Result<(Arc<PrimitiveArray<O>>, DecScale)>
where
    L: ArrowPrimitiveType,
    R: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
    F: Fn(L::Native, R::Native, DecScale) -> Result<O::Native, ArrowError>,
    R::Native: TryFrom<ScalarValue>,
{
    log::debug!(
        "calculate_binary_math_impl called with left {left:?} and right {right:?}, types {} x {} -> {}",
        L::DATA_TYPE,
        R::DATA_TYPE,
        O::DATA_TYPE
    );

    let left = left.as_primitive_opt::<L>().ok_or_else(|| {
        exec_datafusion_err!("Failed to cast left to type {}", L::DATA_TYPE)
    })?;
    let right = right.cast_to(&R::DATA_TYPE, None)?;

    let result = match right {
        ColumnarValue::Array(right) => {
            let right = right.as_primitive_opt::<R>().ok_or_else(|| {
                exec_datafusion_err!("Failed to cast right to type {}", R::DATA_TYPE)
            })?;

            // Four possible combinations of decimal and non-decimal inputs
            match (
                get_decimal_precision_scale(left.data_type()),
                get_decimal_precision_scale(right.data_type()),
            ) {
                (
                    Some((left_precision, left_scale)),
                    Some((right_precision, right_scale)),
                ) => {
                    log::debug!(
                        "calculate_binary_math: rescaling {left_precision}, {left_scale} and {right_precision}, {right_scale}"
                    );

                    // Scale both arguments to a common scale (choose the smaller to avoid overflows)
                    if left_scale < right_scale {
                        let right_scaled =
                            rescale_decimal_array(right, left_precision, left_scale)?;
                        let right_scaled =
                            right_scaled.as_primitive_opt::<R>().ok_or_else(|| {
                                exec_datafusion_err!(
                                    "Failed to cast right array to type {}",
                                    R::DATA_TYPE
                                )
                            })?;
                        log::debug!(
                            "calculate_binary_math: rescaled array right {right_scaled:?}"
                        );
                        let interim =
                            try_binary::<_, _, _, O>(left, right_scaled, |l, r| {
                                fun(l, r, Some((left_precision, left_scale)))
                            })?;
                        (Arc::new(interim) as _, Some((left_precision, left_scale)))
                    } else {
                        let left_scaled =
                            rescale_decimal_array(left, right_precision, right_scale)?;
                        let left_scaled =
                            left_scaled.as_primitive_opt::<L>().ok_or_else(|| {
                                exec_datafusion_err!(
                                    "Failed to cast left array to type {}",
                                    L::DATA_TYPE
                                )
                            })?;
                        log::debug!(
                            "calculate_binary_math: rescaled array left {left_scaled:?}"
                        );
                        let interim =
                            try_binary::<_, _, _, O>(left_scaled, right, |l, r| {
                                fun(l, r, Some((right_precision, right_scale)))
                            })?;
                        (Arc::new(interim) as _, Some((right_precision, right_scale)))
                    }
                }
                (Some(left_dec_scale), None) => {
                    let interim = try_binary::<_, _, _, O>(left, right, |l, r| {
                        fun(l, r, Some(left_dec_scale))
                    })?;
                    (Arc::new(interim) as _, Some(left_dec_scale))
                }
                // Two last patterns together, when left is not decimal
                (None, opt_right_precision_and_scale) => {
                    let interim = try_binary::<_, _, _, O>(left, right, |l, r| {
                        fun(l, r, opt_right_precision_and_scale)
                    })?;
                    (Arc::new(interim) as _, opt_right_precision_and_scale)
                }
            }
        }
        ColumnarValue::Scalar(scalar) if scalar.is_null() => {
            // Null scalar is castable to any numeric, creating a non-null expression.
            // Provide null array explicitly to make result null
            let interim = PrimitiveArray::<O>::new_null(left.len());
            (Arc::new(interim) as _, None)
        }
        ColumnarValue::Scalar(right) => {
            // Four possible combinations of decimal and non-decimal inputs
            match (
                get_decimal_precision_scale(left.data_type()),
                get_decimal_precision_scale(&right.data_type()),
            ) {
                (
                    Some((left_precision, left_scale)),
                    Some((right_precision, right_scale)),
                ) => {
                    if left_scale < right_scale {
                        let right_scaled =
                            rescale_decimal_scalar(&right, left_precision, left_scale)?;
                        let right_native = try_from_scalar::<R>(right_scaled)?;
                        log::debug!(
                            "calculate_binary_math: rescaled scalar right {right_native:?}"
                        );
                        let interim = left.try_unary::<_, O, _>(|l| {
                            fun(l, right_native, Some((left_precision, left_scale)))
                        })?;
                        (Arc::new(interim) as _, Some((left_precision, left_scale)))
                    } else {
                        let left_scaled =
                            rescale_decimal_array(left, right_precision, right_scale)?;
                        let left_scaled =
                            left_scaled.as_primitive_opt::<L>().ok_or_else(|| {
                                exec_datafusion_err!(
                                    "Failed to cast left array to type {}",
                                    L::DATA_TYPE
                                )
                            })?;
                        log::debug!(
                            "calculate_binary_math: rescaled array left {left_scaled:?}"
                        );
                        let right_native = try_from_scalar::<R>(right.clone())?;
                        let interim = left_scaled.try_unary::<_, O, _>(|l| {
                            fun(l, right_native, Some((right_precision, right_scale)))
                        })?;
                        (Arc::new(interim) as _, Some((right_precision, right_scale)))
                    }
                }
                (Some((left_precision, left_scale)), None) => {
                    let right_native = try_from_scalar::<R>(right.clone())?;
                    let interim = left.try_unary::<_, O, _>(|l| {
                        fun(l, right_native, Some((left_precision, left_scale)))
                    })?;
                    (Arc::new(interim) as _, Some((left_precision, left_scale)))
                }
                // Two last patterns together, when left is not decimal
                (None, opt_right_precision_and_scale) => {
                    let right_native = try_from_scalar::<R>(right.clone())?;
                    let interim = left.try_unary::<_, O, _>(|l| {
                        fun(l, right_native, opt_right_precision_and_scale)
                    })?;
                    (Arc::new(interim) as _, opt_right_precision_and_scale)
                }
            }
        }
    };
    log::debug!("calculate_binary_math: result {result:?}");
    Ok(result)
}

/// Computes a binary math function for input arrays using a specified function
/// with any left `L` and right `R` Arrow types, and result of a decimal type `O`.
///
/// Functor `F` computes one operation for Arrow types.
/// If a left type is a decimal, then it's scale is passed as a `DecScale` parameter to the functor,
/// otherwise None is passed.
///
pub fn calculate_binary_math_decimal<L, R, O, F>(
    left: &dyn Array,
    right: &ColumnarValue,
    fun: F,
    out_type: &DataType,
) -> Result<Arc<PrimitiveArray<O>>>
where
    L: ArrowPrimitiveType,
    R: ArrowPrimitiveType,
    O: ArrowPrimitiveType + DecimalType,
    F: Fn(L::Native, R::Native, DecScale) -> Result<O::Native, ArrowError>,
    R::Native: TryFrom<ScalarValue>,
{
    let (interim, scale_opt) =
        calculate_binary_math_impl::<L, R, O, F>(left, right, fun)?;

    // Perform rescaling having `interim` as a decimal array
    let result: Arc<PrimitiveArray<O>> =
        if let Some((out_precision, out_scale)) = scale_opt {
            // Apply scale and cast
            let interim = Arc::unwrap_or_clone(interim)
                .with_precision_and_scale(out_precision, out_scale)?;
            cast_array_to(Arc::new(interim), out_type)?
        } else {
            // Just cast
            cast_array_to(interim, out_type)?
        };
    log::debug!("calculate_binary_math_decimal: result {result:?} out_type={out_type:?}");
    Ok(result)
}

/// Computes a binary math function for input arrays using a specified function
/// with any left `L` and right `R` Arrow types, and result of a non-decimal type `O`.
///
/// Functor `F` computes one operation for Arrow types.
///
pub fn calculate_binary_math_numeric<L, R, O, F>(
    left: &dyn Array,
    right: &ColumnarValue,
    fun: F,
    out_type: &DataType,
) -> Result<Arc<PrimitiveArray<O>>>
where
    L: ArrowPrimitiveType,
    R: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
    F: Fn(L::Native, R::Native, DecScale) -> Result<O::Native, ArrowError>,
    R::Native: TryFrom<ScalarValue>,
{
    let (interim, _) = calculate_binary_math_impl::<L, R, O, F>(left, right, fun)?;
    // Ignore provided decimal scale and precision, just cast to output type
    let result = cast_array_to(interim, out_type)?;
    log::debug!("calculate_binary_math_numeric: result {result:?}");
    Ok(result)
}

/// Computes a binary math function for input arrays using a specified function
/// and apply rescaling to given precision and scale.
/// Generic types:
/// - `L`: Left array decimal type
/// - `R`: Right array primitive type
/// - `O`: Output array decimal type
/// - `F`: Functor computing `fun(l: L, r: R) -> Result<OutputType>`
///
/// Deprecated. use calculate_binary_math_decimal instead
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
    Ok(Arc::new(
        result_array
            .as_ref()
            .clone()
            .with_precision_and_scale(precision, scale)?,
    ))
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

pub fn decimal32_to_i32(value: i32, scale: i8) -> Result<i32, ArrowError> {
    if scale < 0 {
        Err(ArrowError::ComputeError(
            "Negative scale is not supported".into(),
        ))
    } else if scale == 0 {
        Ok(value)
    } else {
        match 10_i32.checked_pow(scale as u32) {
            Some(divisor) => Ok(value / divisor),
            None => Err(ArrowError::ComputeError(format!(
                "Cannot get a power of {scale}"
            ))),
        }
    }
}

pub fn decimal64_to_i64(value: i64, scale: i8) -> Result<i64, ArrowError> {
    if scale < 0 {
        Err(ArrowError::ComputeError(
            "Negative scale is not supported".into(),
        ))
    } else if scale == 0 {
        Ok(value)
    } else {
        match i64::from(10).checked_pow(scale as u32) {
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

    use itertools::Either;
    pub(crate) use test_function;

    use super::*;
    use arrow::array::{Decimal128Array, Float64Array, Int64Array, PrimitiveArray};
    use arrow::datatypes::{
        DECIMAL128_MAX_PRECISION, DataType, Decimal128Type, Float64Type, Int64Type,
    };

    #[cfg(test)]
    #[ctor::ctor]
    fn init() {
        // Enable RUST_LOG logging configuration for test
        let _ = env_logger::try_init();
    }
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

    #[test]
    fn test_decimal32_to_i32() {
        let cases: [(i32, i8, Either<i32, String>); _] = [
            (123, 0, Either::Left(123)),
            (1230, 1, Either::Left(123)),
            (123000, 3, Either::Left(123)),
            (1234567, 2, Either::Left(12345)),
            (-1234567, 2, Either::Left(-12345)),
            (1, 0, Either::Left(1)),
            (
                123,
                -3,
                Either::Right("Negative scale is not supported".into()),
            ),
            (
                123,
                i8::MAX,
                Either::Right("Cannot get a power of 127".into()),
            ),
            (999999999, 0, Either::Left(999999999)),
            (999999999, 3, Either::Left(999999)),
        ];

        for (value, scale, expected) in cases {
            match decimal32_to_i32(value, scale) {
                Ok(actual) => {
                    let expected_value =
                        expected.left().expect("Got value but expected none");
                    assert_eq!(
                        actual, expected_value,
                        "{value} and {scale} vs {expected_value:?}"
                    );
                }
                Err(ArrowError::ComputeError(msg)) => {
                    assert_eq!(
                        msg,
                        expected.right().expect("Got error but expected value")
                    );
                }
                Err(_) => {
                    assert!(expected.is_right())
                }
            }
        }
    }

    #[test]
    fn test_decimal64_to_i64() {
        let cases: [(i64, i8, Either<i64, String>); _] = [
            (123, 0, Either::Left(123)),
            (1234567890, 2, Either::Left(12345678)),
            (-1234567890, 2, Either::Left(-12345678)),
            (
                123,
                -3,
                Either::Right("Negative scale is not supported".into()),
            ),
            (
                123,
                i8::MAX,
                Either::Right("Cannot get a power of 127".into()),
            ),
            (
                999999999999999999i64,
                0,
                Either::Left(999999999999999999i64),
            ),
            (
                999999999999999999i64,
                3,
                Either::Left(999999999999999999i64 / 1000),
            ),
            (
                -999999999999999999i64,
                3,
                Either::Left(-999999999999999999i64 / 1000),
            ),
        ];

        for (value, scale, expected) in cases {
            match decimal64_to_i64(value, scale) {
                Ok(actual) => {
                    let expected_value =
                        expected.left().expect("Got value but expected none");
                    assert_eq!(
                        actual, expected_value,
                        "{value} and {scale} vs {expected_value:?}"
                    );
                }
                Err(ArrowError::ComputeError(msg)) => {
                    assert_eq!(
                        msg,
                        expected.right().expect("Got error but expected value")
                    );
                }
                Err(_) => {
                    assert!(expected.is_right())
                }
            }
        }
    }

    // Test constant
    const LARGE: i128 = 2i128.pow(70);

    #[test]
    fn test_calculate_binary_math_array_scalar_int64() {
        let left = Int64Array::from(vec![0, 12, i64::MAX - 42]);
        let right = ColumnarValue::Scalar(ScalarValue::Int64(Some(42)));
        let result = calculate_binary_math::<Int64Type, Int64Type, Int64Type, _>(
            &left,
            &right,
            |x, y| Ok(x + y),
        )
        .expect("calculate");
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 42);
        assert_eq!(result.value(1), 54);
        assert_eq!(result.value(2), i64::MAX);
    }

    #[test]
    fn test_calculate_binary_math_array_array_int64() {
        let left = Int64Array::from(vec![0, 12, i64::MAX - 42]);
        let right = ColumnarValue::Array(Arc::new(Int64Array::from(vec![42, 42, 42])));
        let result = calculate_binary_math::<Int64Type, Int64Type, Int64Type, _>(
            &left,
            &right,
            |x, y| Ok(x + y),
        )
        .expect("calculate");
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 42);
        assert_eq!(result.value(1), 54);
        assert_eq!(result.value(2), i64::MAX);
    }

    #[test]
    fn test_calculate_binary_decimal128_array_decimal_scalar_unscaled() {
        for precision in [10, 20, DECIMAL128_MAX_PRECISION] {
            let left = Decimal128Array::from(vec![0, 12, LARGE])
                .with_precision_and_scale(precision, 0)
                .unwrap();
            let right =
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(42), precision, 0));
            let result =
                calculate_binary_math_decimal::<
                    Decimal128Type,
                    Decimal128Type,
                    Decimal128Type,
                    _,
                >(&left, &right, |x, y, _| Ok(x + y), left.data_type())
                .expect("calculate");
            _check_calculate_binary_result(result, left.data_type().clone());
        }
    }

    #[test]
    fn test_calculate_binary_decimal128_array_decimal_array_unscaled() {
        for precision in [10, 20, DECIMAL128_MAX_PRECISION] {
            let left = Decimal128Array::from(vec![0, 12, LARGE])
                .with_precision_and_scale(precision, 0)
                .unwrap();
            let right = ColumnarValue::Array(Arc::new(
                Decimal128Array::from(vec![Some(42), Some(42), Some(42)])
                    .with_precision_and_scale(precision, 0)
                    .unwrap(),
            ));
            let result =
                calculate_binary_math_decimal::<
                    Decimal128Type,
                    Decimal128Type,
                    Decimal128Type,
                    _,
                >(&left, &right, |x, y, _| Ok(x + y), left.data_type())
                .expect("calculate");
            _check_calculate_binary_result(result, left.data_type().clone());
        }
    }

    #[test]
    fn test_calculate_binary_decimal128_array_decimal_array_same_scale() {
        for precision in [10, 20, DECIMAL128_MAX_PRECISION] {
            // 0, 12, 2**70
            let left = Decimal128Array::from(vec![0, 12000, LARGE * 1000])
                .with_precision_and_scale(precision, 3)
                .unwrap();
            // 42
            let right = ColumnarValue::Array(Arc::new(
                Decimal128Array::from(vec![Some(42000), Some(42000), Some(42000)])
                    .with_precision_and_scale(precision, 3)
                    .unwrap(),
            ));
            let result =
                calculate_binary_math_decimal::<
                    Decimal128Type,
                    Decimal128Type,
                    Decimal128Type,
                    _,
                >(&left, &right, |x, y, _| Ok(x + y), left.data_type())
                .expect("calculate");
            _check_calculate_binary_result(result, left.data_type().clone());
        }
    }

    #[test]
    fn test_calculate_binary_decimal128_array_decimal_array_different_scale() {
        for precision in [10, 20, DECIMAL128_MAX_PRECISION] {
            // 0, 12, 2**70
            let left = Decimal128Array::from(vec![0, 12000, LARGE * 1000])
                .with_precision_and_scale(precision, 3)
                .unwrap();
            // 42
            let right = ColumnarValue::Array(Arc::new(
                Decimal128Array::from(vec![Some(4200000), Some(4200000), Some(4200000)])
                    .with_precision_and_scale(precision, 5)
                    .unwrap(),
            ));
            let result =
                calculate_binary_math_decimal::<
                    Decimal128Type,
                    Decimal128Type,
                    Decimal128Type,
                    _,
                >(&left, &right, |x, y, _| Ok(x + y), left.data_type())
                .expect("calculate");
            _check_calculate_binary_result(result, left.data_type().clone());
        }
    }

    #[test]
    fn test_calculate_binary_decimal128_array_decimal_literal_different_scale() {
        for precision in [10, 20, DECIMAL128_MAX_PRECISION] {
            // 0, 12, 2**70
            let left = Decimal128Array::from(vec![0, 12000, LARGE * 1000])
                .with_precision_and_scale(precision, 3)
                .unwrap();
            // 42
            let right = ColumnarValue::Scalar(ScalarValue::Decimal128(
                Some(4200000),
                precision,
                5,
            ));

            let result = calculate_binary_math_decimal::<
                Decimal128Type,
                Decimal128Type,
                Decimal128Type,
                _,
            >(
                &left,
                &right,
                |x, y, dec_scale| {
                    let op_precision = dec_scale.unwrap().0;
                    let op_scale = dec_scale.unwrap().1;
                    assert_eq!(op_precision, precision);
                    assert_eq!(op_scale, 3);
                    Ok(x + y)
                },
                left.data_type(),
            )
            .expect("calculate");
            _check_calculate_binary_result(result, left.data_type().clone());
        }
    }

    #[test]
    fn test_calculate_binary_decimal128_array_float_array() {
        for precision in [10, 20, DECIMAL128_MAX_PRECISION] {
            // 0, 12, 2**70
            let left = Decimal128Array::from(vec![0, 12000, LARGE * 1000])
                .with_precision_and_scale(precision, 3)
                .unwrap();
            // 42
            let right = ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                42.0, 42.0, 42.0,
            ])));
            let result = calculate_binary_math_decimal::<
                Decimal128Type,
                Float64Type,
                Decimal128Type,
                _,
            >(
                &left,
                &right,
                |x, y, dec_scale| {
                    let scale = dec_scale.unwrap().1;
                    // To produce a resulting value, one should scale right numeric to match decimal scale
                    // 0 + 42
                    // 12000 + 42
                    // 1180591620717411303424000 + 42
                    Ok(x + i128::from(y.round() as i64)
                        * i128::from(10).pow(scale as u32))
                },
                left.data_type(),
            )
            .expect("calculate");
            _check_calculate_binary_result(result, left.data_type().clone());
        }
    }

    #[test]
    fn test_calculate_binary_decimal128_array_float_literal() {
        for precision in [10, 20, DECIMAL128_MAX_PRECISION] {
            // 0, 12, 2**70
            let left = Decimal128Array::from(vec![0, 12000, LARGE * 1000])
                .with_precision_and_scale(precision, 3)
                .unwrap();
            // 42
            let right = ColumnarValue::Scalar(ScalarValue::Float64(Some(42.0)));
            let result = calculate_binary_math_decimal::<
                Decimal128Type,
                Float64Type,
                Decimal128Type,
                _,
            >(
                &left,
                &right,
                |x, y, dec_scale| {
                    let scale = dec_scale.unwrap().1;
                    // To produce a resulting value, one should scale right numeric to match decimal scale
                    // 0 + 42
                    // 12000 + 42
                    // 1180591620717411303424000 + 42
                    Ok(x + i128::from(y.round() as i64)
                        * i128::from(10).pow(scale as u32))
                },
                left.data_type(),
            )
            .expect("calculate");
            _check_calculate_binary_result(result, left.data_type().clone());
        }
    }

    #[test]
    fn test_calculate_binary_decimal128_array_float_array_float_result() {
        for precision in [10, 20, DECIMAL128_MAX_PRECISION] {
            // 0, 12, 2**70
            let left = Decimal128Array::from(vec![0, 12000, LARGE * 1000])
                .with_precision_and_scale(precision, 3)
                .unwrap();
            // 42
            let right = ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                42.0, 42.0, 42.0,
            ])));
            let result = calculate_binary_math_numeric::<
                Decimal128Type,
                Float64Type,
                Float64Type,
                _,
            >(
                &left,
                &right,
                |x, y, dec_scale| {
                    let scale = dec_scale.unwrap().1;
                    Ok((1 + x / 10i128.pow(scale as u32)).ilog2() as f64 + y)
                },
                &DataType::Float64,
            )
            .expect("calculate");
            assert_eq!(*result.data_type(), DataType::Float64);
            assert_eq!(result.len(), 3);
            assert!((result.value(0) - 42.0).abs() < f64::EPSILON);
            assert!((result.value(1) - 3.0 - 42.0).abs() < f64::EPSILON);
            assert!((result.value(2) - 70.0 - 42.0).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_calculate_binary_decimal128_array_float_array_float_literal_result() {
        for precision in [10, 20, DECIMAL128_MAX_PRECISION] {
            // 0, 12, 2**70
            let left = Decimal128Array::from(vec![0, 12000, LARGE * 1000])
                .with_precision_and_scale(precision, 3)
                .unwrap();
            // 42
            let right = ColumnarValue::Scalar(ScalarValue::Float64(Some(42.0)));
            let result = calculate_binary_math_numeric::<
                Decimal128Type,
                Float64Type,
                Float64Type,
                _,
            >(
                &left,
                &right,
                |x, y, dec_scale| {
                    // a random calculation to capture scale usage in the test
                    let scale = dec_scale.unwrap().1;
                    Ok((1 + x / 10i128.pow(scale as u32)).ilog2() as f64 + y)
                },
                &DataType::Float64,
            )
            .expect("calculate");

            assert_eq!(*result.data_type(), DataType::Float64);
            assert_eq!(result.len(), 3);
            assert!((result.value(0) - 42.0).abs() < f64::EPSILON);
            assert!((result.value(1) - 3.0 - 42.0).abs() < f64::EPSILON);
            assert!((result.value(2) - 70.0 - 42.0).abs() < f64::EPSILON);
        }
    }

    // Test helper to verify against a known result
    fn _check_calculate_binary_result(
        result: Arc<PrimitiveArray<Decimal128Type>>,
        expected_type: DataType,
    ) {
        log::debug!(
            "checking result: {:?} of type: {}",
            result,
            result.data_type()
        );
        let (_precision, scale) =
            get_decimal_precision_scale(&expected_type).expect("decimal type");
        assert_eq!(*result.data_type(), expected_type);
        let ten_scaled = i128::from(10).pow(scale as u32);
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 42 * ten_scaled);
        assert_eq!(result.value(1), (42 + 12) * ten_scaled);
        assert_eq!(result.value(2), (i128::from(42) + LARGE) * ten_scaled);
    }

    #[test]
    fn test_rescale_array_down() {
        let input = Decimal128Array::from(vec![0, 1200000, 4200000])
            .with_precision_and_scale(20, 5)
            .unwrap();
        let result = rescale_array(&input, 20, 0);
        assert!(result.is_ok());
        let expected = Decimal128Array::from(vec![0, 12, 42])
            .with_precision_and_scale(20, 0)
            .unwrap();
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_rescale_array_up() {
        let input = Decimal128Array::from(vec![0, 12, 42])
            .with_precision_and_scale(20, 0)
            .unwrap();
        let result = rescale_array(&input, 20, 5);
        assert!(result.is_ok());
        let expected = Decimal128Array::from(vec![0, 1200000, 4200000])
            .with_precision_and_scale(20, 5)
            .unwrap();
        assert_eq!(result.unwrap(), expected);
    }
}
