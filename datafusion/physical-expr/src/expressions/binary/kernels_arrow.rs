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

use arrow::{array::*, datatypes::ArrowNumericType};
use datafusion_common::Result;

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
    T: ArrowPrimitiveType,
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
    T: ArrowPrimitiveType,
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

pub(crate) fn is_distinct_from_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
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

pub(crate) fn is_not_distinct_from_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
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
