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

use crate::math_funcs::utils::{get_precision_scale, make_decimal_array, make_decimal_scalar};
use arrow::array::{Int16Array, Int32Array, Int64Array, Int8Array};
use arrow_array::{Array, ArrowNativeTypeOp};
use arrow_schema::DataType;
use datafusion::{functions::math::round::round, physical_plan::ColumnarValue};
use datafusion_common::{exec_err, internal_err, DataFusionError, ScalarValue};
use std::{cmp::min, sync::Arc};

macro_rules! integer_round {
    ($X:expr, $DIV:expr, $HALF:expr) => {{
        let rem = $X % $DIV;
        if rem <= -$HALF {
            ($X - rem).sub_wrapping($DIV)
        } else if rem >= $HALF {
            ($X - rem).add_wrapping($DIV)
        } else {
            $X - rem
        }
    }};
}

macro_rules! round_integer_array {
    ($ARRAY:expr, $POINT:expr, $TYPE:ty, $NATIVE:ty) => {{
        let array = $ARRAY.as_any().downcast_ref::<$TYPE>().unwrap();
        let ten: $NATIVE = 10;
        let result: $TYPE = if let Some(div) = ten.checked_pow((-(*$POINT)) as u32) {
            let half = div / 2;
            arrow::compute::kernels::arity::unary(array, |x| integer_round!(x, div, half))
        } else {
            arrow::compute::kernels::arity::unary(array, |_| 0)
        };
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

macro_rules! round_integer_scalar {
    ($SCALAR:expr, $POINT:expr, $TYPE:expr, $NATIVE:ty) => {{
        let ten: $NATIVE = 10;
        if let Some(div) = ten.checked_pow((-(*$POINT)) as u32) {
            let half = div / 2;
            Ok(ColumnarValue::Scalar($TYPE(
                $SCALAR.map(|x| integer_round!(x, div, half)),
            )))
        } else {
            Ok(ColumnarValue::Scalar($TYPE(Some(0))))
        }
    }};
}

/// `round` function that simulates Spark `round` expression
pub fn spark_round(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let value = &args[0];
    let point = &args[1];
    let ColumnarValue::Scalar(ScalarValue::Int64(Some(point))) = point else {
        return internal_err!("Invalid point argument for Round(): {:#?}", point);
    };
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Int64 if *point < 0 => round_integer_array!(array, point, Int64Array, i64),
            DataType::Int32 if *point < 0 => round_integer_array!(array, point, Int32Array, i32),
            DataType::Int16 if *point < 0 => round_integer_array!(array, point, Int16Array, i16),
            DataType::Int8 if *point < 0 => round_integer_array!(array, point, Int8Array, i8),
            DataType::Decimal128(_, scale) if *scale >= 0 => {
                let f = decimal_round_f(scale, point);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_array(array, precision, scale, &f)
            }
            DataType::Float32 | DataType::Float64 => {
                Ok(ColumnarValue::Array(round(&[Arc::clone(array)])?))
            }
            dt => exec_err!("Not supported datatype for ROUND: {dt}"),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Int64(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int64, i64)
            }
            ScalarValue::Int32(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int32, i32)
            }
            ScalarValue::Int16(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int16, i16)
            }
            ScalarValue::Int8(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int8, i8)
            }
            ScalarValue::Decimal128(a, _, scale) if *scale >= 0 => {
                let f = decimal_round_f(scale, point);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_scalar(a, precision, scale, &f)
            }
            ScalarValue::Float32(_) | ScalarValue::Float64(_) => Ok(ColumnarValue::Scalar(
                ScalarValue::try_from_array(&round(&[a.to_array()?])?, 0)?,
            )),
            dt => exec_err!("Not supported datatype for ROUND: {dt}"),
        },
    }
}

// Spark uses BigDecimal. See RoundBase implementation in Spark. Instead, we do the same by
// 1) add the half of divisor, 2) round down by division, 3) adjust precision by multiplication
#[inline]
fn decimal_round_f(scale: &i8, point: &i64) -> Box<dyn Fn(i128) -> i128> {
    if *point < 0 {
        if let Some(div) = 10_i128.checked_pow((-(*point) as u32) + (*scale as u32)) {
            let half = div / 2;
            let mul = 10_i128.pow_wrapping((-(*point)) as u32);
            // i128 can hold 39 digits of a base 10 number, adding half will not cause overflow
            Box::new(move |x: i128| (x + x.signum() * half) / div * mul)
        } else {
            Box::new(move |_: i128| 0)
        }
    } else {
        let div = 10_i128.pow_wrapping((*scale as u32) - min(*scale as u32, *point as u32));
        let half = div / 2;
        Box::new(move |x: i128| (x + x.signum() * half) / div)
    }
}
