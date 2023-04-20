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

//! This module contains computation kernels that are eventually
//! destined for arrow-rs but are in datafusion until they are ported.

use arrow::compute::{
    add_dyn, add_scalar_dyn, divide_dyn_opt, divide_scalar_dyn, modulus_dyn,
    modulus_scalar_dyn, multiply_dyn, multiply_scalar_dyn, subtract_dyn,
    subtract_scalar_dyn, try_unary,
};
use arrow::datatypes::{Date32Type, Date64Type, Decimal128Type};
use arrow::{array::*, datatypes::ArrowNumericType, downcast_dictionary_array};
use arrow_schema::DataType;
use datafusion_common::cast::{as_date32_array, as_date64_array, as_decimal128_array};
use datafusion_common::scalar::{date32_add, date64_add};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use datafusion_expr::type_coercion::binary::decimal_op_mathematics_type;
use datafusion_expr::Operator;
use std::sync::Arc;

use super::{
    interval_array_op, interval_scalar_interval_op, ts_array_op, ts_interval_array_op,
    ts_scalar_interval_op, ts_scalar_ts_op,
};

// Simple (low performance) kernels until optimized kernels are added to arrow
// See https://github.com/apache/arrow-rs/issues/960

macro_rules! distinct_float {
    ($LEFT:expr, $RIGHT:expr, $LEFT_ISNULL:expr, $RIGHT_ISNULL:expr) => {{
        $LEFT_ISNULL != $RIGHT_ISNULL
            || $LEFT.is_nan() != $RIGHT.is_nan()
            || (!$LEFT.is_nan() && !$RIGHT.is_nan() && $LEFT != $RIGHT)
    }};
}

pub(crate) fn is_distinct_from_bool(
    left: &BooleanArray,
    right: &BooleanArray,
) -> Result<BooleanArray> {
    // Different from `neq_bool` because `null is distinct from null` is false and not null
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| Some(left != right))
        .collect())
}

pub(crate) fn is_not_distinct_from_bool(
    left: &BooleanArray,
    right: &BooleanArray,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| Some(left == right))
        .collect())
}

pub(crate) fn is_distinct_from<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    distinct(
        left,
        right,
        |left_value, right_value, left_isnull, right_isnull| {
            left_isnull != right_isnull || left_value != right_value
        },
    )
}

pub(crate) fn is_not_distinct_from<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    distinct(
        left,
        right,
        |left_value, right_value, left_isnull, right_isnull| {
            !(left_isnull != right_isnull || left_value != right_value)
        },
    )
}

fn distinct<
    T,
    F: FnMut(
        <T as ArrowPrimitiveType>::Native,
        <T as ArrowPrimitiveType>::Native,
        bool,
        bool,
    ) -> bool,
>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    mut op: F,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    let left_values = left.values();
    let right_values = right.values();
    let left_nulls = left.nulls();
    let right_nulls = right.nulls();

    let array_len = left.len().min(right.len());
    let distinct = arrow_buffer::MutableBuffer::collect_bool(array_len, |i| {
        op(
            left_values[i],
            right_values[i],
            left_nulls.map(|x| x.is_null(i)).unwrap_or_default(),
            right_nulls.map(|x| x.is_null(i)).unwrap_or_default(),
        )
    });
    let array_data = ArrayData::builder(arrow_schema::DataType::Boolean)
        .len(array_len)
        .add_buffer(distinct.into());

    Ok(BooleanArray::from(unsafe { array_data.build_unchecked() }))
}

pub(crate) fn is_distinct_from_f32(
    left: &Float32Array,
    right: &Float32Array,
) -> Result<BooleanArray> {
    distinct(
        left,
        right,
        |left_value, right_value, left_isnull, right_isnull| {
            distinct_float!(left_value, right_value, left_isnull, right_isnull)
        },
    )
}

pub(crate) fn is_not_distinct_from_f32(
    left: &Float32Array,
    right: &Float32Array,
) -> Result<BooleanArray> {
    distinct(
        left,
        right,
        |left_value, right_value, left_isnull, right_isnull| {
            !(distinct_float!(left_value, right_value, left_isnull, right_isnull))
        },
    )
}

pub(crate) fn is_distinct_from_f64(
    left: &Float64Array,
    right: &Float64Array,
) -> Result<BooleanArray> {
    distinct(
        left,
        right,
        |left_value, right_value, left_isnull, right_isnull| {
            distinct_float!(left_value, right_value, left_isnull, right_isnull)
        },
    )
}

pub(crate) fn is_not_distinct_from_f64(
    left: &Float64Array,
    right: &Float64Array,
) -> Result<BooleanArray> {
    distinct(
        left,
        right,
        |left_value, right_value, left_isnull, right_isnull| {
            !(distinct_float!(left_value, right_value, left_isnull, right_isnull))
        },
    )
}

pub(crate) fn is_distinct_from_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x != y))
        .collect())
}

pub(crate) fn is_distinct_from_null(
    left: &NullArray,
    _right: &NullArray,
) -> Result<BooleanArray> {
    let length = left.len();
    make_boolean_array(length, false)
}

pub(crate) fn is_not_distinct_from_null(
    left: &NullArray,
    _right: &NullArray,
) -> Result<BooleanArray> {
    let length = left.len();
    make_boolean_array(length, true)
}

fn make_boolean_array(length: usize, value: bool) -> Result<BooleanArray> {
    Ok((0..length).map(|_| Some(value)).collect())
}

pub(crate) fn is_not_distinct_from_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x == y))
        .collect())
}

pub(crate) fn is_distinct_from_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| match (left, right) {
            (None, None) => Some(false),
            (None, Some(_)) | (Some(_), None) => Some(true),
            (Some(left), Some(right)) => Some(left != right),
        })
        .collect())
}

pub(crate) fn is_not_distinct_from_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| match (left, right) {
            (None, None) => Some(true),
            (None, Some(_)) | (Some(_), None) => Some(false),
            (Some(left), Some(right)) => Some(left == right),
        })
        .collect())
}

pub(crate) fn add_dyn_decimal(
    left: &dyn Array,
    right: &dyn Array,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;
    let array = add_dyn(left, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn add_decimal_dyn_scalar(
    left: &dyn Array,
    right: i128,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;

    let array = add_scalar_dyn::<Decimal128Type>(left, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn add_dyn_temporal(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
    match (left.data_type(), right.data_type()) {
        (DataType::Timestamp(..), DataType::Timestamp(..)) => ts_array_op(left, right),
        (DataType::Interval(..), DataType::Interval(..)) => {
            interval_array_op(left, right, 1)
        }
        (DataType::Timestamp(..), DataType::Interval(..)) => {
            ts_interval_array_op(left, 1, right)
        }
        (DataType::Interval(..), DataType::Timestamp(..)) => {
            ts_interval_array_op(right, 1, left)
        }
        _ => {
            // fall back to kernels in arrow-rs
            Ok(add_dyn(left, right)?)
        }
    }
}

pub(crate) fn add_dyn_temporal_scalar(
    left: &ArrayRef,
    right: &ScalarValue,
) -> Result<ColumnarValue> {
    match (left.data_type(), right.get_datatype()) {
        (DataType::Date32, DataType::Interval(..)) => {
            let left = as_date32_array(&left)?;
            let ret = Arc::new(try_unary::<Date32Type, _, Date32Type>(left, |days| {
                Ok(date32_add(days, right, 1)?)
            })?) as ArrayRef;
            Ok(ColumnarValue::Array(ret))
        }
        (DataType::Date64, DataType::Interval(..)) => {
            let left = as_date64_array(&left)?;
            let ret = Arc::new(try_unary::<Date64Type, _, Date64Type>(left, |ms| {
                Ok(date64_add(ms, right, 1)?)
            })?) as ArrayRef;
            Ok(ColumnarValue::Array(ret))
        }
        (DataType::Interval(..), DataType::Interval(..)) => {
            interval_scalar_interval_op(left, 1, right)
        }
        (DataType::Timestamp(..), DataType::Interval(..)) => {
            ts_scalar_interval_op(left, 1, right)
        }
        _ => {
            // fall back to kernels in arrow-rs
            Ok(ColumnarValue::Array(add_dyn(left, &right.to_array())?))
        }
    }
}

pub(crate) fn subtract_decimal_dyn_scalar(
    left: &dyn Array,
    right: i128,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;

    let array = subtract_scalar_dyn::<Decimal128Type>(left, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn subtract_dyn_temporal(
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<ArrayRef> {
    match (left.data_type(), right.data_type()) {
        (DataType::Timestamp(..), DataType::Timestamp(..)) => ts_array_op(left, right),
        (DataType::Interval(..), DataType::Interval(..)) => {
            interval_array_op(left, right, -1)
        }
        (DataType::Timestamp(..), DataType::Interval(..)) => {
            ts_interval_array_op(left, -1, right)
        }
        (DataType::Interval(..), DataType::Timestamp(..)) => {
            ts_interval_array_op(right, -1, left)
        }
        _ => {
            // fall back to kernels in arrow-rs
            Ok(subtract_dyn(left, right)?)
        }
    }
}

pub(crate) fn subtract_dyn_temporal_scalar(
    left: &ArrayRef,
    right: &ScalarValue,
) -> Result<ColumnarValue> {
    match (left.data_type(), right.get_datatype()) {
        (DataType::Date32, DataType::Interval(..)) => {
            let left = as_date32_array(&left)?;
            let ret = Arc::new(try_unary::<Date32Type, _, Date32Type>(left, |days| {
                Ok(date32_add(days, right, -1)?)
            })?) as ArrayRef;
            Ok(ColumnarValue::Array(ret))
        }
        (DataType::Date64, DataType::Interval(..)) => {
            let left = as_date64_array(&left)?;
            let ret = Arc::new(try_unary::<Date64Type, _, Date64Type>(left, |ms| {
                Ok(date64_add(ms, right, -1)?)
            })?) as ArrayRef;
            Ok(ColumnarValue::Array(ret))
        }
        (DataType::Timestamp(..), DataType::Timestamp(..)) => {
            ts_scalar_ts_op(left, right)
        }
        (DataType::Interval(..), DataType::Interval(..)) => {
            interval_scalar_interval_op(left, -1, right)
        }
        (DataType::Timestamp(..), DataType::Interval(..)) => {
            ts_scalar_interval_op(left, -1, right)
        }
        _ => {
            // fall back to kernels in arrow-rs
            Ok(ColumnarValue::Array(subtract_dyn(left, &right.to_array())?))
        }
    }
}

fn get_precision_scale(data_type: &DataType) -> Result<(u8, i8)> {
    match data_type {
        DataType::Decimal128(precision, scale) => Ok((*precision, *scale)),
        DataType::Dictionary(_, value_type) => match value_type.as_ref() {
            DataType::Decimal128(precision, scale) => Ok((*precision, *scale)),
            _ => Err(DataFusionError::Internal(
                "Unexpected data type".to_string(),
            )),
        },
        _ => Err(DataFusionError::Internal(
            "Unexpected data type".to_string(),
        )),
    }
}

fn decimal_array_with_precision_scale(
    array: ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef> {
    let array = array.as_ref();
    let decimal_array = match array.data_type() {
        DataType::Decimal128(_, _) => {
            let array = as_decimal128_array(array)?;
            Arc::new(array.clone().with_precision_and_scale(precision, scale)?)
                as ArrayRef
        }
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array!(
                array => match array.values().data_type() {
                    DataType::Decimal128(_, _) => {
                        let decimal_dict_array = array.downcast_dict::<Decimal128Array>().unwrap();
                        let decimal_array = decimal_dict_array.values().clone();
                        let decimal_array = decimal_array.with_precision_and_scale(precision, scale)?;
                        Arc::new(array.with_values(&decimal_array)) as ArrayRef
                    }
                    t => return Err(DataFusionError::Internal(format!("Unexpected dictionary value type {t}"))),
                },
                t => return Err(DataFusionError::Internal(format!("Unexpected datatype {t}"))),
            )
        }
        _ => {
            return Err(DataFusionError::Internal(
                "Unexpected data type".to_string(),
            ))
        }
    };
    Ok(decimal_array)
}

pub(crate) fn multiply_decimal_dyn_scalar(
    left: &dyn Array,
    right: i128,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;

    let op_type = decimal_op_mathematics_type(
        &Operator::Multiply,
        left.data_type(),
        left.data_type(),
    )
    .unwrap();
    let (_, op_scale) = get_precision_scale(&op_type)?;

    let array = multiply_scalar_dyn::<Decimal128Type>(left, right)?;

    if op_scale > scale {
        let div = 10_i128.pow((op_scale - scale) as u32);
        let array = divide_scalar_dyn::<Decimal128Type>(&array, div)?;
        decimal_array_with_precision_scale(array, precision, scale)
    } else {
        decimal_array_with_precision_scale(array, precision, scale)
    }
}

pub(crate) fn divide_decimal_dyn_scalar(
    left: &dyn Array,
    right: i128,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;

    let mul = 10_i128.pow(scale as u32);
    let array = multiply_scalar_dyn::<Decimal128Type>(left, mul)?;

    let array = divide_scalar_dyn::<Decimal128Type>(&array, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn subtract_dyn_decimal(
    left: &dyn Array,
    right: &dyn Array,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;
    let array = subtract_dyn(left, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn multiply_dyn_decimal(
    left: &dyn Array,
    right: &dyn Array,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;

    let op_type = decimal_op_mathematics_type(
        &Operator::Multiply,
        left.data_type(),
        left.data_type(),
    )
    .unwrap();
    let (_, op_scale) = get_precision_scale(&op_type)?;

    let array = multiply_dyn(left, right)?;
    if op_scale > scale {
        let div = 10_i128.pow((op_scale - scale) as u32);
        let array = divide_scalar_dyn::<Decimal128Type>(&array, div)?;
        decimal_array_with_precision_scale(array, precision, scale)
    } else {
        decimal_array_with_precision_scale(array, precision, scale)
    }
}

pub(crate) fn divide_dyn_opt_decimal(
    left: &dyn Array,
    right: &dyn Array,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;

    let mul = 10_i128.pow(scale as u32);
    let array = multiply_scalar_dyn::<Decimal128Type>(left, mul)?;

    // Restore to original precision and scale (metadata only)
    let (org_precision, org_scale) = get_precision_scale(right.data_type())?;
    let array = decimal_array_with_precision_scale(array, org_precision, org_scale)?;
    let array = divide_dyn_opt(&array, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn modulus_dyn_decimal(
    left: &dyn Array,
    right: &dyn Array,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;
    let array = modulus_dyn(left, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn modulus_decimal_dyn_scalar(
    left: &dyn Array,
    right: i128,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;

    let array = modulus_scalar_dyn::<Decimal128Type>(left, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_decimal_array(
        array: &[Option<i128>],
        precision: u8,
        scale: i8,
    ) -> Decimal128Array {
        let mut decimal_builder = Decimal128Builder::with_capacity(array.len());

        for value in array.iter().copied() {
            decimal_builder.append_option(value)
        }
        decimal_builder
            .finish()
            .with_precision_and_scale(precision, scale)
            .unwrap()
    }

    fn create_int_array(array: &[Option<i32>]) -> Int32Array {
        let mut int_builder = Int32Builder::with_capacity(array.len());

        for value in array.iter().copied() {
            int_builder.append_option(value)
        }
        int_builder.finish()
    }

    #[test]
    fn comparison_decimal_op_test() -> Result<()> {
        let value_i128: i128 = 123;
        let decimal_array = create_decimal_array(
            &[
                Some(value_i128),
                None,
                Some(value_i128 - 1),
                Some(value_i128 + 1),
            ],
            25,
            3,
        );
        let left_decimal_array = decimal_array;
        let right_decimal_array = create_decimal_array(
            &[
                Some(value_i128 - 1),
                Some(value_i128),
                Some(value_i128 + 1),
                Some(value_i128 + 1),
            ],
            25,
            3,
        );

        // is_distinct: left distinct right
        let result = is_distinct_from(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(false)]),
            result
        );
        // is_distinct: left distinct right
        let result = is_not_distinct_from(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), Some(false), Some(false), Some(true)]),
            result
        );
        Ok(())
    }

    #[test]
    fn arithmetic_decimal_op_test() -> Result<()> {
        let value_i128: i128 = 123;
        let left_decimal_array = create_decimal_array(
            &[
                Some(value_i128),
                None,
                Some(value_i128 - 1),
                Some(value_i128 + 1),
            ],
            25,
            3,
        );
        let right_decimal_array = create_decimal_array(
            &[
                Some(value_i128),
                Some(value_i128),
                Some(value_i128),
                Some(value_i128),
            ],
            25,
            3,
        );
        // add
        let result_type = decimal_op_mathematics_type(
            &Operator::Plus,
            left_decimal_array.data_type(),
            right_decimal_array.data_type(),
        )
        .unwrap();
        let result =
            add_dyn_decimal(&left_decimal_array, &right_decimal_array, &result_type)?;
        let result = as_decimal128_array(&result)?;
        let expect =
            create_decimal_array(&[Some(246), None, Some(245), Some(247)], 26, 3);
        assert_eq!(&expect, result);
        let result = add_decimal_dyn_scalar(&left_decimal_array, 10, &result_type)?;
        let result = as_decimal128_array(&result)?;
        let expect =
            create_decimal_array(&[Some(133), None, Some(132), Some(134)], 26, 3);
        assert_eq!(&expect, result);
        // subtract
        let result_type = decimal_op_mathematics_type(
            &Operator::Minus,
            left_decimal_array.data_type(),
            right_decimal_array.data_type(),
        )
        .unwrap();
        let result = subtract_dyn_decimal(
            &left_decimal_array,
            &right_decimal_array,
            &result_type,
        )?;
        let result = as_decimal128_array(&result)?;
        let expect = create_decimal_array(&[Some(0), None, Some(-1), Some(1)], 26, 3);
        assert_eq!(&expect, result);
        let result = subtract_decimal_dyn_scalar(&left_decimal_array, 10, &result_type)?;
        let result = as_decimal128_array(&result)?;
        let expect =
            create_decimal_array(&[Some(113), None, Some(112), Some(114)], 26, 3);
        assert_eq!(&expect, result);
        // multiply
        let result_type = decimal_op_mathematics_type(
            &Operator::Multiply,
            left_decimal_array.data_type(),
            right_decimal_array.data_type(),
        )
        .unwrap();
        let result = multiply_dyn_decimal(
            &left_decimal_array,
            &right_decimal_array,
            &result_type,
        )?;
        let result = as_decimal128_array(&result)?;
        let expect =
            create_decimal_array(&[Some(15129), None, Some(15006), Some(15252)], 38, 6);
        assert_eq!(&expect, result);
        let result = multiply_decimal_dyn_scalar(&left_decimal_array, 10, &result_type)?;
        let result = as_decimal128_array(&result)?;
        let expect =
            create_decimal_array(&[Some(1230), None, Some(1220), Some(1240)], 38, 6);
        assert_eq!(&expect, result);
        // divide
        let result_type = decimal_op_mathematics_type(
            &Operator::Divide,
            left_decimal_array.data_type(),
            right_decimal_array.data_type(),
        )
        .unwrap();
        let left_decimal_array = create_decimal_array(
            &[
                Some(1234567),
                None,
                Some(1234567),
                Some(1234567),
                Some(1234567),
            ],
            25,
            3,
        );
        let right_decimal_array = create_decimal_array(
            &[Some(10), Some(100), Some(55), Some(-123), None],
            25,
            3,
        );
        let result = divide_dyn_opt_decimal(
            &left_decimal_array,
            &right_decimal_array,
            &result_type,
        )?;
        let result = as_decimal128_array(&result)?;
        let expect = create_decimal_array(
            &[
                Some(12345670000000000000000000000000000),
                None,
                Some(2244667272727272727272727272727272),
                Some(-1003713008130081300813008130081300),
                None,
            ],
            38,
            29,
        );
        assert_eq!(&expect, result);
        let result = divide_decimal_dyn_scalar(&left_decimal_array, 10, &result_type)?;
        let result = as_decimal128_array(&result)?;
        let expect = create_decimal_array(
            &[
                Some(12345670000000000000000000000000000),
                None,
                Some(12345670000000000000000000000000000),
                Some(12345670000000000000000000000000000),
                Some(12345670000000000000000000000000000),
            ],
            38,
            29,
        );
        assert_eq!(&expect, result);
        // modulus
        let result_type = decimal_op_mathematics_type(
            &Operator::Modulo,
            left_decimal_array.data_type(),
            right_decimal_array.data_type(),
        )
        .unwrap();
        let result =
            modulus_dyn_decimal(&left_decimal_array, &right_decimal_array, &result_type)?;
        let result = as_decimal128_array(&result)?;
        let expect =
            create_decimal_array(&[Some(7), None, Some(37), Some(16), None], 25, 3);
        assert_eq!(&expect, result);
        let result = modulus_decimal_dyn_scalar(&left_decimal_array, 10, &result_type)?;
        let result = as_decimal128_array(&result)?;
        let expect =
            create_decimal_array(&[Some(7), None, Some(7), Some(7), Some(7)], 25, 3);
        assert_eq!(&expect, result);

        Ok(())
    }

    #[test]
    fn arithmetic_decimal_divide_by_zero() {
        let left_decimal_array = create_decimal_array(&[Some(101)], 10, 1);
        let right_decimal_array = create_decimal_array(&[Some(0)], 1, 1);

        let result_type = decimal_op_mathematics_type(
            &Operator::Divide,
            left_decimal_array.data_type(),
            right_decimal_array.data_type(),
        )
        .unwrap();
        let err =
            divide_decimal_dyn_scalar(&left_decimal_array, 0, &result_type).unwrap_err();
        assert_eq!("Arrow error: Divide by zero error", err.to_string());
        let result_type = decimal_op_mathematics_type(
            &Operator::Modulo,
            left_decimal_array.data_type(),
            right_decimal_array.data_type(),
        )
        .unwrap();
        let err =
            modulus_dyn_decimal(&left_decimal_array, &right_decimal_array, &result_type)
                .unwrap_err();
        assert_eq!("Arrow error: Divide by zero error", err.to_string());
        let err =
            modulus_decimal_dyn_scalar(&left_decimal_array, 0, &result_type).unwrap_err();
        assert_eq!("Arrow error: Divide by zero error", err.to_string());
    }

    #[test]
    fn is_distinct_from_non_nulls() -> Result<()> {
        let left_int_array =
            create_int_array(&[Some(0), Some(1), Some(2), Some(3), Some(4)]);
        let right_int_array =
            create_int_array(&[Some(4), Some(3), Some(2), Some(1), Some(0)]);

        assert_eq!(
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                Some(true),
            ]),
            is_distinct_from(&left_int_array, &right_int_array)?
        );
        assert_eq!(
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(false),
                Some(false),
            ]),
            is_not_distinct_from(&left_int_array, &right_int_array)?
        );
        Ok(())
    }

    #[test]
    fn is_distinct_from_nulls() -> Result<()> {
        let left_int_array =
            create_int_array(&[Some(0), Some(0), None, Some(3), Some(0), Some(0)]);
        let right_int_array =
            create_int_array(&[Some(0), None, None, None, Some(0), None]);

        assert_eq!(
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                Some(true),
            ]),
            is_distinct_from(&left_int_array, &right_int_array)?
        );

        assert_eq!(
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                Some(false),
            ]),
            is_not_distinct_from(&left_int_array, &right_int_array)?
        );
        Ok(())
    }
}
