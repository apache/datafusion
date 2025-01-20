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

use crate::math_funcs::utils::get_precision_scale;
use arrow::{
    array::{ArrayRef, AsArray},
    datatypes::Decimal128Type,
};
use arrow_array::{Array, Decimal128Array};
use arrow_schema::{DataType, DECIMAL128_MAX_PRECISION};
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::DataFusionError;
use num::{BigInt, Signed, ToPrimitive};
use std::sync::Arc;

// Let Decimal(p3, s3) as return type i.e. Decimal(p1, s1) / Decimal(p2, s2) = Decimal(p3, s3).
// Conversely, Decimal(p1, s1) = Decimal(p2, s2) * Decimal(p3, s3). This means that, in order to
// get enough scale that matches with Spark behavior, it requires to widen s1 to s2 + s3 + 1. Since
// both s2 and s3 are 38 at max., s1 is 77 at max. DataFusion division cannot handle such scale >
// Decimal256Type::MAX_SCALE. Therefore, we need to implement this decimal division using BigInt.
pub fn spark_decimal_div(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let left = &args[0];
    let right = &args[1];
    let (p3, s3) = get_precision_scale(data_type);

    let (left, right): (ArrayRef, ArrayRef) = match (left, right) {
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => (Arc::clone(l), Arc::clone(r)),
        (ColumnarValue::Scalar(l), ColumnarValue::Array(r)) => {
            (l.to_array_of_size(r.len())?, Arc::clone(r))
        }
        (ColumnarValue::Array(l), ColumnarValue::Scalar(r)) => {
            (Arc::clone(l), r.to_array_of_size(l.len())?)
        }
        (ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)) => (l.to_array()?, r.to_array()?),
    };
    let left = left.as_primitive::<Decimal128Type>();
    let right = right.as_primitive::<Decimal128Type>();
    let (p1, s1) = get_precision_scale(left.data_type());
    let (p2, s2) = get_precision_scale(right.data_type());

    let l_exp = ((s2 + s3 + 1) as u32).saturating_sub(s1 as u32);
    let r_exp = (s1 as u32).saturating_sub((s2 + s3 + 1) as u32);
    let result: Decimal128Array = if p1 as u32 + l_exp > DECIMAL128_MAX_PRECISION as u32
        || p2 as u32 + r_exp > DECIMAL128_MAX_PRECISION as u32
    {
        let ten = BigInt::from(10);
        let l_mul = ten.pow(l_exp);
        let r_mul = ten.pow(r_exp);
        let five = BigInt::from(5);
        let zero = BigInt::from(0);
        arrow::compute::kernels::arity::binary(left, right, |l, r| {
            let l = BigInt::from(l) * &l_mul;
            let r = BigInt::from(r) * &r_mul;
            let div = if r.eq(&zero) { zero.clone() } else { &l / &r };
            let res = if div.is_negative() {
                div - &five
            } else {
                div + &five
            } / &ten;
            res.to_i128().unwrap_or(i128::MAX)
        })?
    } else {
        let l_mul = 10_i128.pow(l_exp);
        let r_mul = 10_i128.pow(r_exp);
        arrow::compute::kernels::arity::binary(left, right, |l, r| {
            let l = l * l_mul;
            let r = r * r_mul;
            let div = if r == 0 { 0 } else { l / r };
            let res = if div.is_negative() { div - 5 } else { div + 5 } / 10;
            res.to_i128().unwrap_or(i128::MAX)
        })?
    };
    let result = result.with_data_type(DataType::Decimal128(p3, s3));
    Ok(ColumnarValue::Array(Arc::new(result)))
}
