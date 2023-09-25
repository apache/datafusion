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

use arrow::array::{ArrayRef, Datum};
use arrow::error::ArrowError;
use arrow_array::BooleanArray;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

/// Applies a binary [`Datum`] kernel `f` to `lhs` and `rhs`
///
/// This maps arrow-rs' [`Datum`] kernels to DataFusion's [`ColumnarValue`] abstraction
pub(crate) fn apply(
    lhs: &ColumnarValue,
    rhs: &ColumnarValue,
    f: impl Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>,
) -> Result<ColumnarValue> {
    match (&lhs, &rhs) {
        (ColumnarValue::Array(left), ColumnarValue::Array(right)) => {
            Ok(ColumnarValue::Array(f(&left.as_ref(), &right.as_ref())?))
        }
        (ColumnarValue::Scalar(left), ColumnarValue::Array(right)) => {
            Ok(ColumnarValue::Array(f(&left.to_scalar(), &right.as_ref())?))
        }
        (ColumnarValue::Array(left), ColumnarValue::Scalar(right)) => {
            Ok(ColumnarValue::Array(f(&left.as_ref(), &right.to_scalar())?))
        }
        (ColumnarValue::Scalar(left), ColumnarValue::Scalar(right)) => {
            let array = f(&left.to_scalar(), &right.to_scalar())?;
            let scalar = ScalarValue::try_from_array(array.as_ref(), 0)?;
            Ok(ColumnarValue::Scalar(scalar))
        }
    }
}

/// Applies a binary [`Datum`] comparison kernel `f` to `lhs` and `rhs`
pub(crate) fn apply_cmp(
    lhs: &ColumnarValue,
    rhs: &ColumnarValue,
    f: impl Fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
) -> Result<ColumnarValue> {
    apply(lhs, rhs, |l, r| Ok(Arc::new(f(l, r)?)))
}
