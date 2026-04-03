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

use arrow::array::ArrowNativeTypeOp;
use arrow::array::BooleanArray;
use arrow::array::types::*;
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, Datum, PrimitiveArray, make_comparator,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::kernels::cmp::{
    distinct, eq, gt, gt_eq, lt, lt_eq, neq, not_distinct,
};
use arrow::compute::{SortOptions, ilike, like, nilike, nlike};
use arrow::error::ArrowError;
use datafusion_common::{Result, ScalarValue};
use datafusion_common::{arrow_datafusion_err, assert_or_internal_err, internal_err};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::operator::Operator;
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

/// Arithmetic operations that can be applied in-place on primitive arrays.
#[derive(Debug, Copy, Clone)]
pub enum ArithmeticOp {
    Add,
    AddWrapping,
    Sub,
    SubWrapping,
    Mul,
    MulWrapping,
    Div,
    Rem,
}

/// Like [`apply`], but takes ownership of `ColumnarValue` inputs to enable
/// in-place buffer reuse for arithmetic on primitive arrays.
///
/// When the left operand is a primitive array whose underlying buffer has a
/// reference count of 1 (i.e. no other consumers), the arithmetic is performed
/// in-place using [`PrimitiveArray::unary_mut`] or [`PrimitiveArray::try_unary_mut`],
/// avoiding a buffer allocation.  If in-place mutation is not possible (shared
/// buffer, non-primitive type, etc.) this falls back to the standard Arrow
/// compute kernel.
pub fn apply_arithmetic(
    lhs: ColumnarValue,
    rhs: ColumnarValue,
    op: ArithmeticOp,
) -> Result<ColumnarValue> {
    let f = arithmetic_op_to_fn(op);
    match (lhs, rhs) {
        (ColumnarValue::Array(left), ColumnarValue::Array(right)) => {
            // Try in-place on left array, then right array
            match try_apply_inplace_array(left, &right, op) {
                Ok(result) => Ok(ColumnarValue::Array(result)),
                Err(left) => match try_apply_inplace_array_rhs(&left, right, op) {
                    Ok(result) => Ok(ColumnarValue::Array(result)),
                    Err(right) => Ok(ColumnarValue::Array(f(&left, &right)?)),
                },
            }
        }
        (ColumnarValue::Scalar(left), ColumnarValue::Array(right)) => {
            // Try in-place on right array with scalar left (flipped)
            match try_apply_inplace_scalar_rhs(right, &left, op) {
                Ok(result) => Ok(ColumnarValue::Array(result)),
                Err(right) => Ok(ColumnarValue::Array(f(&left.to_scalar()?, &right)?)),
            }
        }
        (ColumnarValue::Array(left), ColumnarValue::Scalar(right)) => {
            // Try in-place on left array with scalar right
            match try_apply_inplace_scalar(left, &right, op) {
                Ok(result) => Ok(ColumnarValue::Array(result)),
                Err(left) => Ok(ColumnarValue::Array(f(&left, &right.to_scalar()?)?)),
            }
        }
        (ColumnarValue::Scalar(left), ColumnarValue::Scalar(right)) => {
            let array = f(&left.to_scalar()?, &right.to_scalar()?)?;
            let scalar = ScalarValue::try_from_array(array.as_ref(), 0)?;
            Ok(ColumnarValue::Scalar(scalar))
        }
    }
}

fn arithmetic_op_to_fn(
    op: ArithmeticOp,
) -> fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError> {
    use arrow::compute::kernels::numeric::*;
    match op {
        ArithmeticOp::Add => add,
        ArithmeticOp::AddWrapping => add_wrapping,
        ArithmeticOp::Sub => sub,
        ArithmeticOp::SubWrapping => sub_wrapping,
        ArithmeticOp::Mul => mul,
        ArithmeticOp::MulWrapping => mul_wrapping,
        ArithmeticOp::Div => div,
        ArithmeticOp::Rem => rem,
    }
}

/// Dispatches an in-place unary (array op scalar) operation across all supported primitive types.
/// `$arr` is the ArrayRef, `$scalar_value` is the &ScalarValue, `$op` is ArithmeticOp,
/// `$fn_name` is the function to call (try_inplace_unary or try_inplace_unary_rhs).
macro_rules! dispatch_inplace_unary {
    ($arr:expr, $scalar_value:expr, $op:expr, $fn_name:ident) => {{
        macro_rules! do_dispatch {
            ($arrow_type:ty, $arr_inner:expr) => {{
                let scalar_val = $scalar_value
                    .to_scalar()
                    .map_err(|_| Arc::clone(&$arr_inner))?;
                let scalar_arr = scalar_val.get().0;
                let rhs_val = scalar_arr
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$arrow_type>>()
                    .ok_or_else(|| Arc::clone(&$arr_inner))?
                    .value(0);
                $fn_name::<$arrow_type>($arr_inner, rhs_val, $op)
            }};
        }
        match $arr.data_type() {
            dt if dt == &<Int8Type as ArrowPrimitiveType>::DATA_TYPE => {
                do_dispatch!(Int8Type, $arr)
            }
            dt if dt == &<Int16Type as ArrowPrimitiveType>::DATA_TYPE => {
                do_dispatch!(Int16Type, $arr)
            }
            dt if dt == &<Int32Type as ArrowPrimitiveType>::DATA_TYPE => {
                do_dispatch!(Int32Type, $arr)
            }
            dt if dt == &<Int64Type as ArrowPrimitiveType>::DATA_TYPE => {
                do_dispatch!(Int64Type, $arr)
            }
            dt if dt == &<UInt8Type as ArrowPrimitiveType>::DATA_TYPE => {
                do_dispatch!(UInt8Type, $arr)
            }
            dt if dt == &<UInt16Type as ArrowPrimitiveType>::DATA_TYPE => {
                do_dispatch!(UInt16Type, $arr)
            }
            dt if dt == &<UInt32Type as ArrowPrimitiveType>::DATA_TYPE => {
                do_dispatch!(UInt32Type, $arr)
            }
            dt if dt == &<UInt64Type as ArrowPrimitiveType>::DATA_TYPE => {
                do_dispatch!(UInt64Type, $arr)
            }
            dt if dt == &<Float16Type as ArrowPrimitiveType>::DATA_TYPE => {
                do_dispatch!(Float16Type, $arr)
            }
            dt if dt == &<Float32Type as ArrowPrimitiveType>::DATA_TYPE => {
                do_dispatch!(Float32Type, $arr)
            }
            dt if dt == &<Float64Type as ArrowPrimitiveType>::DATA_TYPE => {
                do_dispatch!(Float64Type, $arr)
            }
            // Decimal types excluded: result precision/scale differs from input
            _ => Err($arr),
        }
    }};
}

/// Dispatches an in-place binary (array op array) operation across all supported primitive types.
/// `$arr` is the ArrayRef to mutate, `$other` is the other ArrayRef, `$op` is ArithmeticOp,
/// `$fn_name` is the function to call (try_inplace_binary or try_inplace_binary_rhs).
macro_rules! dispatch_inplace_binary {
    ($arr:expr, $other:expr, $op:expr, $fn_name:ident) => {{
        match $arr.data_type() {
            dt if dt == &<Int8Type as ArrowPrimitiveType>::DATA_TYPE => {
                $fn_name::<Int8Type>($arr, $other, $op)
            }
            dt if dt == &<Int16Type as ArrowPrimitiveType>::DATA_TYPE => {
                $fn_name::<Int16Type>($arr, $other, $op)
            }
            dt if dt == &<Int32Type as ArrowPrimitiveType>::DATA_TYPE => {
                $fn_name::<Int32Type>($arr, $other, $op)
            }
            dt if dt == &<Int64Type as ArrowPrimitiveType>::DATA_TYPE => {
                $fn_name::<Int64Type>($arr, $other, $op)
            }
            dt if dt == &<UInt8Type as ArrowPrimitiveType>::DATA_TYPE => {
                $fn_name::<UInt8Type>($arr, $other, $op)
            }
            dt if dt == &<UInt16Type as ArrowPrimitiveType>::DATA_TYPE => {
                $fn_name::<UInt16Type>($arr, $other, $op)
            }
            dt if dt == &<UInt32Type as ArrowPrimitiveType>::DATA_TYPE => {
                $fn_name::<UInt32Type>($arr, $other, $op)
            }
            dt if dt == &<UInt64Type as ArrowPrimitiveType>::DATA_TYPE => {
                $fn_name::<UInt64Type>($arr, $other, $op)
            }
            dt if dt == &<Float16Type as ArrowPrimitiveType>::DATA_TYPE => {
                $fn_name::<Float16Type>($arr, $other, $op)
            }
            dt if dt == &<Float32Type as ArrowPrimitiveType>::DATA_TYPE => {
                $fn_name::<Float32Type>($arr, $other, $op)
            }
            dt if dt == &<Float64Type as ArrowPrimitiveType>::DATA_TYPE => {
                $fn_name::<Float64Type>($arr, $other, $op)
            }
            // Decimal types excluded: result precision/scale differs from input
            _ => Err($arr),
        }
    }};
}

/// Try to apply arithmetic in-place on `left` array with a scalar `right`.
/// Returns `Ok(result)` on success, or `Err(left)` if in-place not possible.
fn try_apply_inplace_scalar(
    left: ArrayRef,
    right: &ScalarValue,
    op: ArithmeticOp,
) -> Result<ArrayRef, ArrayRef> {
    if right.is_null() {
        return Err(left);
    }
    dispatch_inplace_unary!(left, right, op, try_inplace_unary)
}

/// Try to apply arithmetic in-place on `left` array using values from `right` array.
/// Returns `Ok(result)` on success, or `Err(left)` if in-place not possible.
fn try_apply_inplace_array(
    left: ArrayRef,
    right: &ArrayRef,
    op: ArithmeticOp,
) -> Result<ArrayRef, ArrayRef> {
    if left.data_type() != right.data_type() {
        return Err(left);
    }
    dispatch_inplace_binary!(left, right, op, try_inplace_binary)
}

/// Attempt in-place unary (array op scalar) mutation on a PrimitiveArray.
fn try_inplace_unary<T: ArrowPrimitiveType>(
    array: ArrayRef,
    scalar: T::Native,
    op: ArithmeticOp,
) -> Result<ArrayRef, ArrayRef>
where
    T::Native: ArrowNativeTypeOp,
{
    // Clone the PrimitiveArray (cheap — shares the buffer via Arc)
    let primitive = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| Arc::clone(&array))?
        .clone();
    // Drop the ArrayRef so the buffer's refcount can drop to 1
    drop(array);

    // Only attempt in-place for wrapping (infallible) operations.
    // Checked ops (Add, Sub, Mul, Div) can fail mid-way, corrupting the buffer.
    // Rem with zero divisor must also fall back for proper error reporting.
    type BinFn<N> = fn(N, N) -> N;
    let op_fn: Option<BinFn<T::Native>> = match op {
        ArithmeticOp::AddWrapping => Some(ArrowNativeTypeOp::add_wrapping),
        ArithmeticOp::SubWrapping => Some(ArrowNativeTypeOp::sub_wrapping),
        ArithmeticOp::MulWrapping => Some(ArrowNativeTypeOp::mul_wrapping),
        ArithmeticOp::Rem if !scalar.is_zero() => Some(ArrowNativeTypeOp::mod_wrapping),
        _ => None,
    };

    let Some(op_fn) = op_fn else {
        return Err(Arc::new(primitive));
    };

    match primitive.unary_mut(|v| op_fn(v, scalar)) {
        Ok(result) => Ok(Arc::new(result)),
        Err(arr) => Err(Arc::new(arr)),
    }
}

/// Attempt in-place binary (array op array) mutation on a PrimitiveArray.
fn try_inplace_binary<T: ArrowPrimitiveType>(
    left: ArrayRef,
    right: &ArrayRef,
    op: ArithmeticOp,
) -> Result<ArrayRef, ArrayRef>
where
    T::Native: ArrowNativeTypeOp,
{
    let right_primitive = right
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| Arc::clone(&left))?;

    let left_primitive = left
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| Arc::clone(&left))?
        .clone();
    drop(left);

    let mut builder = match left_primitive.into_builder() {
        Ok(b) => b,
        Err(arr) => return Err(Arc::new(arr)),
    };

    // Only attempt in-place for wrapping (infallible) operations.
    type BinFn<N> = fn(N, N) -> N;
    let op_fn: Option<BinFn<T::Native>> = match op {
        ArithmeticOp::AddWrapping => Some(ArrowNativeTypeOp::add_wrapping),
        ArithmeticOp::SubWrapping => Some(ArrowNativeTypeOp::sub_wrapping),
        ArithmeticOp::MulWrapping => Some(ArrowNativeTypeOp::mul_wrapping),
        _ => None,
    };

    let Some(op_fn) = op_fn else {
        return Err(Arc::new(builder.finish()));
    };

    let left_slice = builder.values_slice_mut();
    let right_values = right_primitive.values();

    left_slice
        .iter_mut()
        .zip(right_values.iter())
        .for_each(|(l, r)| *l = op_fn(*l, *r));

    // Merge null buffers from both sides
    let result = builder.finish();
    if right_primitive.nulls().is_some() {
        let merged = NullBuffer::union(result.nulls(), right_primitive.nulls());
        let result = PrimitiveArray::<T>::new(result.values().clone(), merged);
        Ok(Arc::new(result))
    } else {
        Ok(Arc::new(result))
    }
}

/// Try to apply arithmetic in-place on `right` array with a scalar `left`.
/// The operation is `result\[i\] = op(scalar_left, right\[i\])`, stored in `right`'s buffer.
/// Returns `Ok(result)` on success, or `Err(right)` if in-place not possible.
fn try_apply_inplace_scalar_rhs(
    right: ArrayRef,
    left: &ScalarValue,
    op: ArithmeticOp,
) -> Result<ArrayRef, ArrayRef> {
    if left.is_null() {
        return Err(right);
    }
    dispatch_inplace_unary!(right, left, op, try_inplace_unary_rhs)
}

/// Try to apply arithmetic in-place on `right` array using values from `left` array.
/// The operation is `result\[i\] = op(left\[i\], right\[i\])`, stored in `right`'s buffer.
/// Returns `Ok(result)` on success, or `Err(right)` if in-place not possible.
fn try_apply_inplace_array_rhs(
    left: &ArrayRef,
    right: ArrayRef,
    op: ArithmeticOp,
) -> Result<ArrayRef, ArrayRef> {
    if left.data_type() != right.data_type() {
        return Err(right);
    }
    dispatch_inplace_binary!(right, left, op, try_inplace_binary_rhs)
}

/// Attempt in-place mutation on the right PrimitiveArray: result\[i\] = op(scalar, right\[i\]).
fn try_inplace_unary_rhs<T: ArrowPrimitiveType>(
    array: ArrayRef,
    scalar: T::Native,
    op: ArithmeticOp,
) -> Result<ArrayRef, ArrayRef>
where
    T::Native: ArrowNativeTypeOp,
{
    let primitive = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| Arc::clone(&array))?
        .clone();
    drop(array);

    // For right-side mutation: result = op(scalar, element)
    // Commutative ops: same as op(element, scalar)
    // Non-commutative: need reversed argument order
    type BinFn<N> = fn(N, N) -> N;
    let op_fn: Option<BinFn<T::Native>> = match op {
        ArithmeticOp::AddWrapping => Some(ArrowNativeTypeOp::add_wrapping),
        ArithmeticOp::MulWrapping => Some(ArrowNativeTypeOp::mul_wrapping),
        ArithmeticOp::SubWrapping => Some(ArrowNativeTypeOp::sub_wrapping),
        ArithmeticOp::Rem if !scalar.is_zero() => Some(ArrowNativeTypeOp::mod_wrapping),
        _ => None,
    };

    let Some(op_fn) = op_fn else {
        return Err(Arc::new(primitive));
    };

    // Note: op(scalar, v) — scalar is the left operand
    match primitive.unary_mut(|v| op_fn(scalar, v)) {
        Ok(result) => Ok(Arc::new(result)),
        Err(arr) => Err(Arc::new(arr)),
    }
}

/// Attempt in-place mutation on the right PrimitiveArray: result\[i\] = op(left\[i\], right\[i\]).
/// Note: parameter order is (right_owned, left_ref) to match the dispatch_inplace_binary macro.
fn try_inplace_binary_rhs<T: ArrowPrimitiveType>(
    right: ArrayRef,
    left: &ArrayRef,
    op: ArithmeticOp,
) -> Result<ArrayRef, ArrayRef>
where
    T::Native: ArrowNativeTypeOp,
{
    let left_primitive = left
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| Arc::clone(&right))?;

    let right_primitive = right
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| Arc::clone(&right))?
        .clone();
    drop(right);

    let mut builder = match right_primitive.into_builder() {
        Ok(b) => b,
        Err(arr) => return Err(Arc::new(arr)),
    };

    type BinFn<N> = fn(N, N) -> N;
    let op_fn: Option<BinFn<T::Native>> = match op {
        ArithmeticOp::AddWrapping => Some(ArrowNativeTypeOp::add_wrapping),
        ArithmeticOp::SubWrapping => Some(ArrowNativeTypeOp::sub_wrapping),
        ArithmeticOp::MulWrapping => Some(ArrowNativeTypeOp::mul_wrapping),
        _ => None,
    };

    let Some(op_fn) = op_fn else {
        return Err(Arc::new(builder.finish()));
    };

    let right_slice = builder.values_slice_mut();
    let left_values = left_primitive.values();

    // Note: op(left\[i\], right\[i\]) — left is the first operand
    right_slice
        .iter_mut()
        .zip(left_values.iter())
        .for_each(|(r, l)| *r = op_fn(*l, *r));

    // Merge null buffers from both sides
    let result = builder.finish();
    if left_primitive.nulls().is_some() {
        let merged = NullBuffer::union(result.nulls(), left_primitive.nulls());
        let result = PrimitiveArray::<T>::new(result.values().clone(), merged);
        Ok(Arc::new(result))
    } else {
        Ok(Arc::new(result))
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

        apply(lhs, rhs, |l, r| Ok(Arc::new(f(l, r)?)))
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
