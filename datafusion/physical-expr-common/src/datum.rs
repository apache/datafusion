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
use datafusion_common::internal_err;
use datafusion_common::{Result, ScalarValue};
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
    if matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::Gt
            | Operator::LtEq
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    ) {
        apply(lhs, rhs, |l, r| {
            Ok(Arc::new(compare_op_for_nested(op, l, r)?))
        })
    } else {
        internal_err!("invalid operator for nested")
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

    if l_len != r_len && !is_l_scalar && !is_r_scalar {
        return internal_err!("len mismatch");
    }

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
        (false, false) => (0..len).map(|i| cmp_with_op(i, i)).collect(),
        (true, false) => (0..len).map(|i| cmp_with_op(0, i)).collect(),
        (false, true) => (0..len).map(|i| cmp_with_op(i, 0)).collect(),
        (true, true) => std::iter::once(cmp_with_op(0, 0)).collect(),
    };

    // Distinct understand how to compare with NULL
    // i.e NULL is distinct from NULL -> false
    if matches!(op, Operator::IsDistinctFrom | Operator::IsNotDistinctFrom) {
        Ok(BooleanArray::new(values, None))
    } else {
        // If one of the side is NULL, we returns NULL
        // i.e. NULL eq NULL -> NULL
        let nulls = NullBuffer::union(l.nulls(), r.nulls());
        Ok(BooleanArray::new(values, nulls))
    }
}
