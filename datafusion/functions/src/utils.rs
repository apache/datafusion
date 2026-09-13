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
    Array, ArrayRef, ArrowPrimitiveType, AsArray, MapArray, PrimitiveArray, Scalar,
    UInt32Array, UInt32Builder, make_comparator,
};
use arrow::buffer::NullBuffer;
use arrow::compute::kernels::cmp::eq;
use arrow::compute::{SortOptions, cast, try_binary};
use arrow::datatypes::{DataType, DecimalType};
use arrow::error::ArrowError;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::ColumnarValue;
use datafusion_expr::function::Hint;
use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;

/// Creates a function to identify the optimal return type of a string function given
/// the type of its first argument.
///
/// If the input type is `LargeUtf8` or `LargeBinary` the return type is
/// `$largeUtf8Type`,
///
/// If the input type is `Utf8` or `Binary` the return type is `$utf8Type`,
///
/// If the input type is `Utf8View`, `BinaryView` or `FixedSizeBinary` the
/// return type is `$utf8Type`,
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
                // FixedSizeBinary sizes are declared as i32
                DataType::FixedSizeBinary(_) => $utf8Type,
                DataType::Null => DataType::Null,
                DataType::Dictionary(_, value_type) => match **value_type {
                    DataType::LargeUtf8 | DataType::LargeBinary => $largeUtf8Type,
                    DataType::Utf8 | DataType::Binary => $utf8Type,
                    DataType::FixedSizeBinary(_) => $utf8Type,
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

/// Transforms the leaf type while preserving supported encoding containers.
///
/// Keep encoded type handling centralized here so additional encodings can be
/// supported without changing each function's return type implementation.
pub(crate) fn transform_leaf_type_preserving_encoding<F>(
    arg_type: &DataType,
    transform: &F,
) -> Result<DataType>
where
    F: Fn(&DataType) -> Result<DataType>,
{
    match arg_type {
        DataType::Dictionary(key_type, value_type) => Ok(DataType::Dictionary(
            key_type.clone(),
            Box::new(transform_leaf_type_preserving_encoding(
                value_type, transform,
            )?),
        )),
        _ => transform(arg_type),
    }
}

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
    calculate_binary_math_cast::<L, R, O, F>(left, right, fun, &R::DATA_TYPE)
}

/// Computes a binary math function for input arrays using a specified function
/// and applies rescaling to given precision and scale.
/// Generic types:
/// - `L`: Left array decimal type
/// - `R`: Right array primitive type
/// - `O`: Output array decimal type
/// - `F`: Functor computing `fun(l: L, r: R) -> Result<OutputType>`
#[deprecated(
    since = "55.0.0",
    note = "Use `calculate_binary_decimal_math_cast` instead"
)]
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
    calculate_binary_decimal_math_cast::<L, R, O, F>(
        left,
        right,
        fun,
        precision,
        scale,
        &R::DATA_TYPE,
    )
}

/// Computes a binary math function for input arrays using a specified function.
///
/// It casts the right operand to `cast_target` instead of the default `R::DATA_TYPE` to preserve
/// the right operand scale.
///
/// # Type Parameters
/// - `L`: Left array primitive type
/// - `R`: Right array primitive type
/// - `O`: Output array primitive type
/// - `F`: Functor computing `fun(l: L, r: R) -> Result<OutputType>`
/// # Arguments
/// - `left`: Left input array
/// - `right`: Right input array or scalar value
/// - `fun`: Function of type `F`
/// - `cast_target`: Data type to cast right operand to before applying function
fn calculate_binary_math_cast<L, R, O, F>(
    left: &dyn Array,
    right: &ColumnarValue,
    fun: F,
    cast_target: &DataType,
) -> Result<Arc<PrimitiveArray<O>>>
where
    L: ArrowPrimitiveType,
    R: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
    F: Fn(L::Native, R::Native) -> Result<O::Native, ArrowError>,
    R::Native: TryFrom<ScalarValue>,
{
    let left = left.as_primitive::<L>();
    let right = right.cast_to(cast_target, None)?;
    let result = match right {
        ColumnarValue::Scalar(scalar) => {
            if scalar.is_null() {
                // Null scalar is castable to any numeric, creating a non-null expression.
                // Provide null array explicitly to make result null
                PrimitiveArray::<O>::new_null(left.len())
            } else {
                let right = R::Native::try_from(scalar.clone()).map_err(|_| {
                    DataFusionError::NotImplemented(format!(
                        "Cannot convert scalar value {scalar} to {cast_target}"
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
/// and applies rescaling to given precision and scale.
///
/// It casts the right operand to `cast_target` instead of the default `R::DATA_TYPE` to preserve
/// the right operand scale.
///
/// # Type Parameters
/// - `L`: Left array decimal type
/// - `R`: Right array primitive type
/// - `O`: Output array decimal type
/// - `F`: Functor computing `fun(l: L, r: R) -> Result<OutputType>`
/// # Arguments
/// - `left`: Left input array
/// - `right`: Right input array or scalar value
/// - `fun`: Function of type `F`
/// - `precision`: Precision to apply to output decimal array
/// - `scale`: Scale to apply to output decimal array
/// - `cast_target`: Data type to cast right operand to before applying function
pub fn calculate_binary_decimal_math_cast<L, R, O, F>(
    left: &dyn Array,
    right: &ColumnarValue,
    fun: F,
    precision: u8,
    scale: i8,
    cast_target: &DataType,
) -> Result<Arc<PrimitiveArray<O>>>
where
    L: DecimalType,
    R: ArrowPrimitiveType,
    O: DecimalType,
    F: Fn(L::Native, R::Native) -> Result<O::Native, ArrowError>,
    R::Native: TryFrom<ScalarValue>,
{
    let result_array =
        calculate_binary_math_cast::<L, R, O, F>(left, right, fun, cast_target)?;
    Ok(Arc::new(
        result_array
            .as_ref()
            .clone()
            .with_precision_and_scale(precision, scale)?,
    ))
}

/// Converts Decimal128 components (value and scale) to an unscaled i128
pub fn decimal128_to_i128(value: i128, scale: i8) -> Result<i128, ArrowError> {
    match scale.cmp(&0) {
        Ordering::Less => Err(ArrowError::ComputeError(
            "Negative scale is not supported".into(),
        )),
        Ordering::Equal => Ok(value),
        Ordering::Greater => match i128::from(10).checked_pow(scale as u32) {
            Some(divisor) => Ok(value / divisor),
            None => Err(ArrowError::ComputeError(format!(
                "Cannot get a power of {scale}"
            ))),
        },
    }
}

pub fn decimal32_to_i32(value: i32, scale: i8) -> Result<i32, ArrowError> {
    match scale.cmp(&0) {
        Ordering::Less => Err(ArrowError::ComputeError(
            "Negative scale is not supported".into(),
        )),
        Ordering::Equal => Ok(value),
        Ordering::Greater => match 10_i32.checked_pow(scale as u32) {
            Some(divisor) => Ok(value / divisor),
            None => Err(ArrowError::ComputeError(format!(
                "Cannot get a power of {scale}"
            ))),
        },
    }
}

pub fn decimal64_to_i64(value: i64, scale: i8) -> Result<i64, ArrowError> {
    match scale.cmp(&0) {
        Ordering::Less => Err(ArrowError::ComputeError(
            "Negative scale is not supported".into(),
        )),
        Ordering::Equal => Ok(value),
        Ordering::Greater => match i64::from(10).checked_pow(scale as u32) {
            Some(divisor) => Ok(value / divisor),
            None => Err(ArrowError::ComputeError(format!(
                "Cannot get a power of {scale}"
            ))),
        },
    }
}

/// Finds, for each row of `map`, the first entry whose key equals that row's
/// lookup key.
///
/// `keys` holds either a single key, which every row is looked up with, or
/// one key per map row. The result has one element per map row: the index of
/// the matching entry into `map.values()`, or null when the row is null, the
/// lookup key is null, or no entry matches. It can be passed directly to
/// [`arrow::compute::take`] on `map.values()`.
///
/// Non-nested keys must have the map's key type, up to dictionary encoding.
/// Nested keys must have the same structure, and may differ in field names
/// and nullability. Keys are compared the way `ORDER BY` compares values:
/// floating point keys use total ordering, so `-0.0` and `0.0` are different
/// keys and NaN matches NaN.
pub fn map_lookup(map: &MapArray, keys: &dyn Array) -> Result<UInt32Array> {
    let map_keys = map.keys();
    let single_key = match keys.len() {
        1 => true,
        len if len == map.len() => false,
        len => {
            return internal_err!(
                "map_lookup expects one lookup key or one per map row ({}), got {len}",
                map.len()
            );
        }
    };
    let key_type = map_keys.data_type();
    // A nested lookup key only has to be nested here; `make_comparator`
    // checks its structure. A non-nested lookup key must have the map's
    // key type, ignoring dictionary encoding.
    let compatible = if key_type.is_nested() {
        keys.data_type().is_nested()
    } else {
        strip_dictionary(key_type).equals_datatype(strip_dictionary(keys.data_type()))
    };
    if !compatible {
        return exec_err!(
            "The key type {} does not match the map key type {}",
            keys.data_type(),
            key_type
        );
    }
    // The comparison kernels need both sides to use the same encoding.
    let cast_keys;
    let keys: &dyn Array = if key_type.is_nested() || keys.data_type() == key_type {
        keys
    } else {
        cast_keys = cast(keys, key_type)?;
        cast_keys.as_ref()
    };

    let offsets = map.value_offsets();
    let (first, last) = (offsets[0] as usize, offsets[map.len()] as usize);
    // No row has any entries, so nothing can match. Map keys are never
    // null, so a null lookup key matches nothing either.
    if first == last || (single_key && keys.logical_null_count() > 0) {
        return Ok(UInt32Array::new_null(map.len()));
    }
    let key_nulls = if single_key {
        None
    } else {
        keys.logical_nulls()
    };
    let mut scanner = RowScanner::new(map, key_nulls.as_ref());

    // Scan with a comparator, which stops at the first match in each row.
    // Count the comparisons over a sample of rows to see whether stopping
    // early pays off.
    let cmp = make_comparator(map_keys.as_ref(), keys, SortOptions::default())?;
    let compare =
        |entry: usize, row: usize| cmp(entry, if single_key { 0 } else { row }).is_eq();
    let sample = map.len().min(SAMPLE_ROWS);
    let mut comparisons = 0;
    let sampled_entries = scanner.scan(0..sample, |entry, row| {
        comparisons += 1;
        compare(entry, row)
    });

    // If the sampled rows compared more than half of their entries, stopping
    // early is not paying off, so the remaining rows are cheaper to compare all
    // at once with the vectorized `eq`. We can only use `eq` when we have a
    // single, non-nested key. The exact break-even point depends on the key
    // type and the hardware; half keeps the cost of a wrong guess to about a
    // third in either direction.
    let rest = sample..map.len();
    if single_key
        && !key_type.is_nested()
        && !rest.is_empty()
        && comparisons * 2 > sampled_entries
    {
        let range_start = offsets[sample] as usize;
        let in_range = map_keys.slice(range_start, last - range_start);
        let matches = eq(&Scalar::new(keys.slice(0, 1)), &in_range)?;
        // Neither side has nulls, so the value bits alone are meaningful.
        let bits = matches.values();
        scanner.scan(rest, |entry, _| bits.value(entry - range_start));
    } else {
        scanner.scan(rest, compare);
    }
    Ok(scanner.finish())
}

/// Number of rows [`map_lookup`] scans with the comparator before deciding
/// whether the rest of the batch is better served by the vectorized `eq`.
const SAMPLE_ROWS: usize = 32;

/// The value type of a dictionary-encoded type, or the type itself.
fn strip_dictionary(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value_type) => value_type,
        other => other,
    }
}

/// Scans map rows for the first entry that satisfies a predicate.
struct RowScanner<'a> {
    offsets: &'a [i32],
    /// Rows to skip: null map rows and rows whose lookup key is null.
    skip: Option<NullBuffer>,
    found: UInt32Builder,
    /// Position within its row of the most recent match.
    hint: usize,
}

impl<'a> RowScanner<'a> {
    fn new(map: &'a MapArray, key_nulls: Option<&NullBuffer>) -> Self {
        Self {
            offsets: map.value_offsets(),
            skip: NullBuffer::union(map.nulls(), key_nulls),
            found: UInt32Builder::with_capacity(map.len()),
            hint: 0,
        }
    }

    /// Scans `rows`, recording the first entry for which `is_match(entry, row)`
    /// holds, or null for a skipped row or a row without a match. Returns the
    /// number of entries in the rows that were scanned.
    fn scan(
        &mut self,
        rows: Range<usize>,
        mut is_match: impl FnMut(usize, usize) -> bool,
    ) -> usize {
        let mut entries = 0;
        for row in rows {
            if self.skip.as_ref().is_some_and(|skip| skip.is_null(row)) {
                self.found.append_null();
                continue;
            }
            let start = self.offsets[row] as usize;
            let end = self.offsets[row + 1] as usize;
            entries += end - start;

            // Rows in a batch usually share the same key order, so try the
            // position where the previous row matched first. When that guess
            // is right, the lookup costs one comparison wherever the key sits.
            let hinted = start + self.hint;
            let found = if hinted < end && is_match(hinted, row) {
                Some(hinted)
            } else {
                (start..end).find(|&entry| entry != hinted && is_match(entry, row))
            };
            if let Some(entry) = found {
                self.hint = entry - start;
            }
            self.found.append_option(found.map(|entry| entry as u32));
        }
        entries
    }

    fn finish(mut self) -> UInt32Array {
        self.found.finish()
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

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Int32Type},
    };
    use itertools::Either;
    pub(crate) use test_function;

    use super::*;

    #[test]
    fn test_calculate_binary_math_scalar_null() {
        let left = Int32Array::from(vec![1, 2]);
        let right = ColumnarValue::Scalar(ScalarValue::Int32(None));
        let result = calculate_binary_math::<Int32Type, Int32Type, Int32Type, _>(
            &left,
            &right,
            |x, y| Ok(x + y),
        )
        .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result.null_count(), 2);
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
}

#[cfg(test)]
mod map_lookup_tests {
    use super::*;
    use arrow::array::{
        DictionaryArray, Int32Array, Int64Array, ListArray, StringArray, StructArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Field, Int32Type};

    /// A map whose rows have the given lengths, drawing keys and values in
    /// order from `keys` and `values`. Rows flagged false in `valid` are null.
    fn make_map(
        keys: ArrayRef,
        values: ArrayRef,
        lengths: &[usize],
        valid: Option<&[bool]>,
    ) -> MapArray {
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", keys.data_type().clone(), false)),
                keys,
            ),
            (
                Arc::new(Field::new("value", values.data_type().clone(), true)),
                values,
            ),
        ]);
        MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::from_lengths(lengths.iter().copied()),
            entries,
            valid.map(|valid| NullBuffer::from(valid.to_vec())),
            false,
        )
    }

    /// Rows: `{1: 10, 2: 20}`, `{}`, `{2: 30}`, and a null row that still
    /// carries the entry `{1: 40}`.
    fn int_map() -> MapArray {
        make_map(
            Arc::new(Int32Array::from(vec![1, 2, 2, 1])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            &[2, 0, 1, 1],
            Some(&[true, true, true, false]),
        )
    }

    #[test]
    fn single_key() -> Result<()> {
        let map = int_map();
        let result = map_lookup(&map, &Int32Array::from(vec![2]))?;
        assert_eq!(
            result,
            UInt32Array::from(vec![Some(1), None, Some(2), None])
        );

        // The null row's entry is never matched.
        let result = map_lookup(&map, &Int32Array::from(vec![1]))?;
        assert_eq!(result, UInt32Array::from(vec![Some(0), None, None, None]));

        let result = map_lookup(&map, &Int32Array::from(vec![9]))?;
        assert_eq!(result, UInt32Array::from(vec![None; 4]));
        Ok(())
    }

    #[test]
    fn single_null_key_matches_nothing() -> Result<()> {
        let map = int_map();
        let result = map_lookup(&map, &Int32Array::from(vec![None]))?;
        assert_eq!(result, UInt32Array::from(vec![None; 4]));
        Ok(())
    }

    #[test]
    fn one_key_per_row() -> Result<()> {
        let map = int_map();
        let keys = Int32Array::from(vec![Some(2), Some(1), None, Some(1)]);
        let result = map_lookup(&map, &keys)?;
        assert_eq!(result, UInt32Array::from(vec![Some(1), None, None, None]));
        Ok(())
    }

    #[test]
    fn nested_single_key() -> Result<()> {
        let keys = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4)]),
        ]);
        let map = make_map(
            Arc::new(keys),
            Arc::new(Int32Array::from(vec![10, 20])),
            &[2],
            None,
        );
        let list_key = |values: Vec<i32>| {
            ListArray::from_iter_primitive::<Int32Type, _, _>([Some(
                values.into_iter().map(Some),
            )])
        };
        let result = map_lookup(&map, &list_key(vec![3, 4]))?;
        assert_eq!(result, UInt32Array::from(vec![Some(1)]));

        let result = map_lookup(&map, &list_key(vec![9, 9]))?;
        assert_eq!(result, UInt32Array::from(vec![None]));
        Ok(())
    }

    #[test]
    fn sliced_map_and_keys() -> Result<()> {
        let map = int_map();
        let keys = Int32Array::from(vec![0, 0, 2, 0]);

        // Rows 1 and 2 of the map with keys 1 and 2 of the key array. Entry
        // indices stay relative to the unsliced entries.
        let result = map_lookup(&map.slice(1, 2), &keys.slice(1, 2))?;
        assert_eq!(result, UInt32Array::from(vec![None, Some(2)]));

        // A single key against a slice whose entries start past offset 0.
        let result = map_lookup(&map.slice(2, 1), &Int32Array::from(vec![2]))?;
        assert_eq!(result, UInt32Array::from(vec![Some(2)]));

        // An empty slice still sees the unsliced entries buffer.
        let result = map_lookup(&map.slice(1, 0), &keys.slice(1, 0))?;
        assert_eq!(result.len(), 0);
        Ok(())
    }

    #[test]
    fn dictionary_keys_match_plain_keys() -> Result<()> {
        // Rows: `{a: 10, b: 20}`, `{a: 30}`.
        let keys: DictionaryArray<Int32Type> = vec!["a", "b", "a"].into_iter().collect();
        let map = make_map(
            Arc::new(keys),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            &[2, 1],
            None,
        );
        let result = map_lookup(&map, &StringArray::from(vec!["b"]))?;
        assert_eq!(result, UInt32Array::from(vec![Some(1), None]));

        let result = map_lookup(&map, &StringArray::from(vec!["b", "a"]))?;
        assert_eq!(result, UInt32Array::from(vec![Some(1), Some(2)]));
        Ok(())
    }

    #[test]
    fn vectorized_scan_after_missing_sample() -> Result<()> {
        // The first 40 rows are `{1: 0, 2: 0, 3: 0}` and the rest are
        // `{9: 0, 1: 0, 2: 0}`, so a lookup of 9 misses throughout the sampled
        // rows and must still be found afterwards.
        let rows = 100;
        let switch = 40;
        let keys: Vec<i32> = (0..rows)
            .flat_map(|row| if row < switch { [1, 2, 3] } else { [9, 1, 2] })
            .collect();
        let map = make_map(
            Arc::new(Int32Array::from(keys)),
            Arc::new(Int32Array::from(vec![0; rows * 3])),
            &vec![3; rows],
            None,
        );
        let result = map_lookup(&map, &Int32Array::from(vec![9]))?;
        let expected: UInt32Array = (0..rows)
            .map(|row| (row >= switch).then_some(row as u32 * 3))
            .collect();
        assert_eq!(result, expected);

        // A key present in every row keeps the comparator scan.
        let result = map_lookup(&map, &Int32Array::from(vec![2]))?;
        let expected: UInt32Array = (0..rows)
            .map(|row| Some(row as u32 * 3 + if row < switch { 1 } else { 2 }))
            .collect();
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn nested_keys_may_differ_in_field_nullability() -> Result<()> {
        // A map read from a schema with a non-null struct field, looked up
        // with a struct literal whose fields are nullable.
        let map_keys = StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        )]);
        let map = make_map(
            Arc::new(map_keys),
            Arc::new(Int32Array::from(vec![10, 20])),
            &[2],
            None,
        );
        let lookup = StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![2])) as ArrayRef,
        )]);
        let result = map_lookup(&map, &lookup)?;
        assert_eq!(result, UInt32Array::from(vec![Some(1)]));
        Ok(())
    }

    #[test]
    fn key_type_mismatch_is_an_error() {
        let map = int_map();
        let err = map_lookup(&map, &Int64Array::from(vec![2])).unwrap_err();
        assert!(
            err.to_string()
                .contains("The key type Int64 does not match the map key type Int32"),
            "{err}"
        );
    }

    #[test]
    fn wrong_key_count_is_an_error() {
        let map = int_map();
        assert!(map_lookup(&map, &Int32Array::from(vec![1, 2])).is_err());
    }
}
