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
// UnLt required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::BooleanArray;
use arrow::array::{make_comparator, ArrayRef, Datum};
use arrow::buffer::NullBuffer;
use arrow::compute::SortOptions;
use arrow::error::ArrowError;
use datafusion_common::{internal_err, not_impl_err};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, Operator};
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

/// Applies a binary [`Datum`] comparison kernel `f` to `lhs` and `rhs`
pub fn apply_cmp(
    lhs: &ColumnarValue,
    rhs: &ColumnarValue,
    f: impl Fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
) -> Result<ColumnarValue> {
    apply(lhs, rhs, |l, r| Ok(Arc::new(f(l, r)?)))
}

/// Applies a binary [`Datum`] comparison kernel `f` to `lhs` and `rhs` for nested type like
/// List, FixedSizeList, LargeList, Struct, Union, Map, or a dictionary of a nested type
pub fn apply_cmp_for_nested(
    op: Operator,
    lhs: &ColumnarValue,
    rhs: &ColumnarValue,
) -> Result<ColumnarValue> {
    apply(lhs, rhs, |l, r| {
        Ok(Arc::new(compare_op_for_nested(op, l, r)?))
    })
}

/// Compare on nested type List, Struct, and so on
fn compare_op_for_nested(
    op: Operator,
    lhs: &dyn Datum,
    rhs: &dyn Datum,
) -> Result<BooleanArray> {
    let (l, is_l_scalar) = lhs.get();
    let (r, is_r_scalar) = rhs.get();
    let l_len = l.len();
    let r_len = r.len();
    if l_len != r_len && !is_l_scalar && !is_r_scalar {
        return internal_err!("len mismatch");
    }

    let len = match is_l_scalar {
        true => r_len,
        false => l_len,
    };

    let cmp = make_comparator(l, r, SortOptions::default())?;

    if !matches!(
        op,
        Operator::Eq
            | Operator::Lt
            | Operator::Gt
            | Operator::LtEq
            | Operator::GtEq
            | Operator::NotEq
    ) {
        return not_impl_err!("other operation are not implemented");
    }

    let cmp_with_op = |i, j| match op {
        Operator::Eq => cmp(i, j).is_eq(),
        Operator::Lt => cmp(i, j).is_lt(),
        Operator::Gt => cmp(i, j).is_gt(),
        Operator::LtEq => !cmp(i, j).is_gt(),
        Operator::GtEq => !cmp(i, j).is_lt(),
        Operator::NotEq => !cmp(i, j).is_eq(),
        _ => unreachable!("other operatations should be be handled above"),
    };

    let values = match (is_l_scalar, is_r_scalar) {
        (false, false) => (0..len).map(|i| cmp_with_op(i, i)).collect(),
        (true, false) => (0..len).map(|i| cmp_with_op(0, i)).collect(),
        (false, true) => (0..len).map(|i| cmp_with_op(i, 0)).collect(),
        (true, true) => std::iter::once(cmp_with_op(0, 0)).collect(),
    };

    let nulls = NullBuffer::union(l.nulls(), r.nulls());
    Ok(BooleanArray::new(values, nulls))
}
