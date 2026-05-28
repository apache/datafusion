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

use arrow::array::BooleanArray;
use arrow::array::{Array, ArrayRef, AsArray, Datum, make_comparator};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::kernels::arity::unary;
use arrow::compute::kernels::cmp::{
    distinct, eq, gt, gt_eq, lt, lt_eq, neq, not_distinct,
};
use arrow::compute::{SortOptions, ilike, like, nilike, nlike};
use arrow::datatypes::{
    DataType, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
};
use arrow::downcast_dictionary_array;
use arrow::error::ArrowError;
use datafusion_common::{Result, ScalarValue};
use datafusion_common::{arrow_datafusion_err, assert_or_internal_err, internal_err};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::operator::Operator;
use half::f16;
use std::sync::Arc;

/// Applies a binary [`Datum`] kernel `f` to `lhs` and `rhs`
///
/// This maps arrow-rs' [`Datum`] kernels to DataFusion's [`ColumnarValue`] abstraction
pub fn apply(
    lhs: &ColumnarValue,
    rhs: &ColumnarValue,
    f: impl Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>,
) -> Result<ColumnarValue> {
    match (&lhs, &rhs) {
        (ColumnarValue::Array(left), ColumnarValue::Array(right)) => {
            Ok(ColumnarValue::Array(f(&left.as_ref(), &right.as_ref())?))
        }
        (ColumnarValue::Scalar(left), ColumnarValue::Array(right)) => Ok(
            ColumnarValue::Array(f(&left.to_scalar()?, &right.as_ref())?),
        ),
        (ColumnarValue::Array(left), ColumnarValue::Scalar(right)) => Ok(
            ColumnarValue::Array(f(&left.as_ref(), &right.to_scalar()?)?),
        ),
        (ColumnarValue::Scalar(left), ColumnarValue::Scalar(right)) => {
            let array = f(&left.to_scalar()?, &right.to_scalar()?)?;
            let scalar = ScalarValue::try_from_array(array.as_ref(), 0)?;
            Ok(ColumnarValue::Scalar(scalar))
        }
    }
}

/// Applies a binary [`Datum`] comparison operator `op` to `lhs` and `rhs`
pub fn apply_cmp(
    op: Operator,
    lhs: &ColumnarValue,
    rhs: &ColumnarValue,
) -> Result<ColumnarValue> {
    if lhs.data_type().is_nested() {
        apply_cmp_for_nested(op, lhs, rhs)
    } else {
        let f = match op {
            Operator::Eq => eq,
            Operator::NotEq => neq,
            Operator::Lt => lt,
            Operator::LtEq => lt_eq,
            Operator::Gt => gt,
            Operator::GtEq => gt_eq,
            Operator::IsDistinctFrom => distinct,
            Operator::IsNotDistinctFrom => not_distinct,

            Operator::LikeMatch => like,
            Operator::ILikeMatch => ilike,
            Operator::NotLikeMatch => nlike,
            Operator::NotILikeMatch => nilike,

            _ => {
                return internal_err!("Invalid compare operator: {}", op);
            }
        };

        let lhs = normalize_neg_zero(lhs);
        let rhs = normalize_neg_zero(rhs);
        apply(&lhs, &rhs, |l, r| Ok(Arc::new(f(l, r)?)))
    }
}

/// Replace `-0.0` with `+0.0` on float inputs so that comparison operators
/// follow IEEE 754 default semantics (where `-0.0 == +0.0`).
///
/// arrow-rs' comparison kernels intentionally use totalOrder semantics, which
/// treats `-0.0` as strictly less than `+0.0` and not equal to it
///
/// See [`normalize_neg_zero_array`] / [`normalize_neg_zero_scalar`] for the
/// per-variant behavior, including dictionary- and run-end-encoded arrays.
pub fn normalize_neg_zero(value: &ColumnarValue) -> ColumnarValue {
    match value {
        ColumnarValue::Array(array) => {
            ColumnarValue::Array(normalize_neg_zero_array(array))
        }
        ColumnarValue::Scalar(scalar) => {
            ColumnarValue::Scalar(normalize_neg_zero_scalar(scalar))
        }
    }
}

/// Array variant of [`normalize_neg_zero`]. Returns the input unchanged for
/// arrays that don't contain `-0.0` (no allocation) and for arrays whose
/// (transitive) value type is not floating-point.
///
/// Dictionary- and run-end-encoded arrays are peeled to their inner values
/// and rebuilt with normalized values when needed; the keys/run-ends are
/// preserved.
pub fn normalize_neg_zero_array(array: &ArrayRef) -> ArrayRef {
    if !data_type_contains_float(array.data_type()) {
        return Arc::clone(array);
    }

    match array.data_type() {
        DataType::Float16 => {
            let arr = array.as_primitive::<Float16Type>();
            if arr
                .values()
                .iter()
                .any(|v| v.is_sign_negative() && *v == f16::ZERO)
            {
                Arc::new(unary::<Float16Type, _, Float16Type>(arr, |x| {
                    if x == f16::ZERO { f16::ZERO } else { x }
                }))
            } else {
                Arc::clone(array)
            }
        }
        DataType::Float32 => {
            let arr = array.as_primitive::<Float32Type>();
            if arr
                .values()
                .iter()
                .any(|v| v.is_sign_negative() && *v == 0.0)
            {
                Arc::new(unary::<Float32Type, _, Float32Type>(arr, |x| {
                    if x == 0.0 { 0.0 } else { x }
                }))
            } else {
                Arc::clone(array)
            }
        }
        DataType::Float64 => {
            let arr = array.as_primitive::<Float64Type>();
            if arr
                .values()
                .iter()
                .any(|v| v.is_sign_negative() && *v == 0.0)
            {
                Arc::new(unary::<Float64Type, _, Float64Type>(arr, |x| {
                    if x == 0.0 { 0.0 } else { x }
                }))
            } else {
                Arc::clone(array)
            }
        }
        DataType::Dictionary(_, _) => {
            let dyn_array: &dyn Array = array.as_ref();
            downcast_dictionary_array!(
                dyn_array => {
                    let inner = dyn_array.values();
                    let normalized = normalize_neg_zero_array(inner);
                    if Arc::ptr_eq(&normalized, inner) {
                        Arc::clone(array)
                    } else {
                        Arc::new(dyn_array.with_values(normalized))
                    }
                }
                _ => unreachable!("data_type matched Dictionary"),
            )
        }
        DataType::RunEndEncoded(run_ends_field, _) => match run_ends_field.data_type() {
            DataType::Int16 => normalize_neg_zero_ree::<Int16Type>(array),
            DataType::Int32 => normalize_neg_zero_ree::<Int32Type>(array),
            DataType::Int64 => normalize_neg_zero_ree::<Int64Type>(array),
            _ => Arc::clone(array),
        },
        _ => Arc::clone(array),
    }
}

/// Returns `true` if `dt` is, or transitively wraps, a floating-point type.
/// Used to short-circuit [`normalize_neg_zero_array`] for non-float inputs.
fn data_type_contains_float(dt: &DataType) -> bool {
    match dt {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => true,
        DataType::Dictionary(_, value_type) => data_type_contains_float(value_type),
        DataType::RunEndEncoded(_, values_field) => {
            data_type_contains_float(values_field.data_type())
        }
        _ => false,
    }
}

fn normalize_neg_zero_ree<R: arrow::datatypes::RunEndIndexType>(
    array: &ArrayRef,
) -> ArrayRef {
    let ree = array.as_ref().as_run::<R>();
    let inner = ree.values();
    let normalized = normalize_neg_zero_array(inner);
    if Arc::ptr_eq(&normalized, inner) {
        Arc::clone(array)
    } else {
        Arc::new(ree.with_values(normalized))
    }
}

/// Scalar variant of [`normalize_neg_zero`]. Returns the input unchanged for
/// non-float scalars and for non-zero float values.
pub fn normalize_neg_zero_scalar(scalar: &ScalarValue) -> ScalarValue {
    match scalar {
        ScalarValue::Float16(Some(v)) if *v == f16::ZERO => {
            ScalarValue::Float16(Some(f16::ZERO))
        }
        ScalarValue::Float32(Some(v)) if *v == 0.0 => ScalarValue::Float32(Some(0.0)),
        ScalarValue::Float64(Some(v)) if *v == 0.0 => ScalarValue::Float64(Some(0.0)),
        _ => scalar.clone(),
    }
}

/// Applies a binary [`Datum`] comparison operator `op` to `lhs` and `rhs` for nested type like
/// List, FixedSizeList, LargeList, Struct, Union, Map, or a dictionary of a nested type
pub fn apply_cmp_for_nested(
    op: Operator,
    lhs: &ColumnarValue,
    rhs: &ColumnarValue,
) -> Result<ColumnarValue> {
    let left_data_type = lhs.data_type();
    let right_data_type = rhs.data_type();

    assert_or_internal_err!(
        matches!(
            op,
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::Gt
                | Operator::LtEq
                | Operator::GtEq
                | Operator::IsDistinctFrom
                | Operator::IsNotDistinctFrom
        ) && left_data_type.equals_datatype(&right_data_type),
        "invalid operator or data type mismatch for nested data, op {op} left {left_data_type}, right {right_data_type}",
    );

    apply(lhs, rhs, |l, r| {
        Ok(Arc::new(compare_op_for_nested(op, l, r)?))
    })
}

/// Compare with eq with either nested or non-nested
pub fn compare_with_eq(
    lhs: &dyn Datum,
    rhs: &dyn Datum,
    is_nested: bool,
) -> Result<BooleanArray> {
    if is_nested {
        compare_op_for_nested(Operator::Eq, lhs, rhs)
    } else {
        eq(lhs, rhs).map_err(|e| arrow_datafusion_err!(e))
    }
}

/// Compare on nested type List, Struct, and so on
pub fn compare_op_for_nested(
    op: Operator,
    lhs: &dyn Datum,
    rhs: &dyn Datum,
) -> Result<BooleanArray> {
    let (l, is_l_scalar) = lhs.get();
    let (r, is_r_scalar) = rhs.get();
    let l_len = l.len();
    let r_len = r.len();

    assert_or_internal_err!(l_len == r_len || is_l_scalar || is_r_scalar, "len mismatch");

    let len = match is_l_scalar {
        true => r_len,
        false => l_len,
    };

    // fast path, if compare with one null and operator is not 'distinct', then we can return null array directly
    if !matches!(op, Operator::IsDistinctFrom | Operator::IsNotDistinctFrom)
        && (is_l_scalar && l.null_count() == 1 || is_r_scalar && r.null_count() == 1)
    {
        return Ok(BooleanArray::new_null(len));
    }

    // TODO: make SortOptions configurable
    // we choose the default behaviour from arrow-rs which has null-first that follow spark's behaviour
    let cmp = make_comparator(l, r, SortOptions::default())?;

    let cmp_with_op = |i, j| match op {
        Operator::Eq | Operator::IsNotDistinctFrom => cmp(i, j).is_eq(),
        Operator::Lt => cmp(i, j).is_lt(),
        Operator::Gt => cmp(i, j).is_gt(),
        Operator::LtEq => !cmp(i, j).is_gt(),
        Operator::GtEq => !cmp(i, j).is_lt(),
        Operator::NotEq | Operator::IsDistinctFrom => !cmp(i, j).is_eq(),
        _ => unreachable!("unexpected operator found"),
    };

    let values = match (is_l_scalar, is_r_scalar) {
        (false, false) => BooleanBuffer::collect_bool(len, |i| cmp_with_op(i, i)),
        (true, false) => BooleanBuffer::collect_bool(len, |i| cmp_with_op(0, i)),
        (false, true) => BooleanBuffer::collect_bool(len, |i| cmp_with_op(i, 0)),
        (true, true) => std::iter::once(cmp_with_op(0, 0)).collect(),
    };

    // Distinct understand how to compare with NULL
    // i.e NULL is distinct from NULL -> false
    if matches!(op, Operator::IsDistinctFrom | Operator::IsNotDistinctFrom) {
        Ok(BooleanArray::new(values, None))
    } else {
        // If one of the side is NULL, we return NULL
        // i.e. NULL eq NULL -> NULL
        // For nested comparisons, we need to ensure the null buffer matches the result length
        let nulls = match (is_l_scalar, is_r_scalar) {
            (false, false) | (true, true) => NullBuffer::union(l.nulls(), r.nulls()),
            (true, false) => {
                // When left is null-scalar and right is array, expand left nulls to match result length
                match l.nulls().filter(|nulls| nulls.is_null(0)) {
                    Some(_) => Some(NullBuffer::new_null(len)), // Left scalar is null
                    None => r.nulls().cloned(),                 // Left scalar is non-null
                }
            }
            (false, true) => {
                // When right is null-scalar and left is array, expand right nulls to match result length
                match r.nulls().filter(|nulls| nulls.is_null(0)) {
                    Some(_) => Some(NullBuffer::new_null(len)), // Right scalar is null
                    None => l.nulls().cloned(), // Right scalar is non-null
                }
            }
        };
        Ok(BooleanArray::new(values, nulls))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        DictionaryArray, Float16Array, Float32Array, Float64Array, Int32Array, RunArray,
    };

    #[test]
    fn data_type_contains_float_detects_nested() {
        use arrow::datatypes::Field;

        assert!(data_type_contains_float(&DataType::Float32));
        assert!(!data_type_contains_float(&DataType::Int64));

        let dict_of_float =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Float64));
        assert!(data_type_contains_float(&dict_of_float));

        let dict_of_int =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int64));
        assert!(!data_type_contains_float(&dict_of_int));

        let ree_of_float = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Float64, true)),
        );
        assert!(data_type_contains_float(&ree_of_float));
    }

    fn has_neg_zero_f64(array: &dyn Array) -> bool {
        array
            .as_primitive::<Float64Type>()
            .values()
            .iter()
            .any(|v| v.is_sign_negative() && *v == 0.0)
    }

    #[test]
    fn normalize_float64_rewrites_neg_zero() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![-0.0, 0.0, 1.5, -2.0]));
        let normalized = normalize_neg_zero_array(&array);

        assert!(!Arc::ptr_eq(&normalized, &array));
        assert!(!has_neg_zero_f64(normalized.as_ref()));
        let values = normalized.as_primitive::<Float64Type>().values();
        assert_eq!(values[2], 1.5);
        assert_eq!(values[3], -2.0);
    }

    #[test]
    fn normalize_float64_passthrough_when_no_neg_zero() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![0.0, 1.0, 2.0]));
        let normalized = normalize_neg_zero_array(&array);
        assert!(Arc::ptr_eq(&normalized, &array));
    }

    #[test]
    fn normalize_dict_of_float64_peels_and_rewrites() {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![-0.0, 1.0, 2.0]));
        let keys = Int32Array::from(vec![0, 1, 0, 2]);
        let array: ArrayRef =
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap());

        let normalized = normalize_neg_zero_array(&array);
        assert!(!Arc::ptr_eq(&normalized, &array));

        let dict = normalized
            .as_ref()
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert!(!has_neg_zero_f64(dict.values().as_ref()));
    }

    #[test]
    fn normalize_dict_of_non_float_passes_through() {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let keys = Int32Array::from(vec![0, 1, 2, 0]);
        let array: ArrayRef =
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap());

        let normalized = normalize_neg_zero_array(&array);
        assert!(Arc::ptr_eq(&normalized, &array));
    }

    #[test]
    fn normalize_ree_of_float32_peels_and_rewrites() {
        let run_ends = Int32Array::from(vec![2, 3, 5]);
        let values: ArrayRef = Arc::new(Float32Array::from(vec![-0.0, 1.0, 2.0]));
        let array: ArrayRef =
            Arc::new(RunArray::<Int32Type>::try_new(&run_ends, values.as_ref()).unwrap());

        let normalized = normalize_neg_zero_array(&array);
        assert!(!Arc::ptr_eq(&normalized, &array));

        let ree = normalized.as_ref().as_run::<Int32Type>();
        let inner = ree.values().as_primitive::<Float32Type>();
        assert!(
            !inner
                .values()
                .iter()
                .any(|v| v.is_sign_negative() && *v == 0.0)
        );
    }

    #[test]
    fn normalize_float16_rewrites_neg_zero() {
        let array: ArrayRef = Arc::new(Float16Array::from(vec![
            f16::NEG_ZERO,
            f16::ZERO,
            f16::from_f32(1.5),
        ]));
        let normalized = normalize_neg_zero_array(&array);

        assert!(!Arc::ptr_eq(&normalized, &array));
        let values = normalized.as_primitive::<Float16Type>().values();
        assert!(
            !values
                .iter()
                .any(|v| v.is_sign_negative() && *v == f16::ZERO)
        );
        assert_eq!(values[2], f16::from_f32(1.5));
    }

    #[test]
    fn normalize_nested_dict_recurses() {
        // Dictionary<Int32, Dictionary<Int32, Float64>> — exercises the
        // recursive peel through two layers of dictionary encoding.
        let values: ArrayRef = Arc::new(Float64Array::from(vec![-0.0, 1.0]));
        let inner_keys = Int32Array::from(vec![0, 1, 0]);
        let inner_dict: ArrayRef =
            Arc::new(DictionaryArray::<Int32Type>::try_new(inner_keys, values).unwrap());
        let outer_keys = Int32Array::from(vec![0, 1, 2]);
        let array: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(outer_keys, inner_dict).unwrap(),
        );

        let normalized = normalize_neg_zero_array(&array);
        assert!(!Arc::ptr_eq(&normalized, &array));

        let outer = normalized
            .as_ref()
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let inner = outer
            .values()
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert!(!has_neg_zero_f64(inner.values().as_ref()));
    }

    #[test]
    fn normalize_scalar_float64() {
        assert_eq!(
            normalize_neg_zero_scalar(&ScalarValue::Float64(Some(-0.0))),
            ScalarValue::Float64(Some(0.0))
        );
        assert_eq!(
            normalize_neg_zero_scalar(&ScalarValue::Float64(Some(0.0))),
            ScalarValue::Float64(Some(0.0))
        );
        // Non-zero values are unchanged, including the sign of negative numbers.
        assert_eq!(
            normalize_neg_zero_scalar(&ScalarValue::Float64(Some(-1.5))),
            ScalarValue::Float64(Some(-1.5))
        );
        assert_eq!(
            normalize_neg_zero_scalar(&ScalarValue::Float64(None)),
            ScalarValue::Float64(None)
        );
    }

    #[test]
    fn normalize_scalar_float32_and_float16() {
        assert_eq!(
            normalize_neg_zero_scalar(&ScalarValue::Float32(Some(-0.0))),
            ScalarValue::Float32(Some(0.0))
        );
        assert_eq!(
            normalize_neg_zero_scalar(&ScalarValue::Float16(Some(f16::NEG_ZERO))),
            ScalarValue::Float16(Some(f16::ZERO))
        );
    }

    #[test]
    fn normalize_scalar_non_float_passthrough() {
        let s = ScalarValue::Int32(Some(0));
        assert_eq!(normalize_neg_zero_scalar(&s), s);
        let s = ScalarValue::Utf8(Some("hello".into()));
        assert_eq!(normalize_neg_zero_scalar(&s), s);
    }
}
