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
    array::{AsArray, Decimal128Builder},
    datatypes::{validate_decimal_precision, Int64Type},
};
use arrow_schema::DataType;
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::{internal_err, Result as DataFusionResult, ScalarValue};
use std::sync::Arc;

/// Spark-compatible `MakeDecimal` expression (internal to Spark optimizer)
pub fn spark_make_decimal(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> DataFusionResult<ColumnarValue> {
    let (precision, scale) = get_precision_scale(data_type);
    match &args[0] {
        ColumnarValue::Scalar(v) => match v {
            ScalarValue::Int64(n) => Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                long_to_decimal(n, precision),
                precision,
                scale,
            ))),
            sv => internal_err!("Expected Int64 but found {sv:?}"),
        },
        ColumnarValue::Array(a) => {
            let arr = a.as_primitive::<Int64Type>();
            let mut result = Decimal128Builder::new();
            for v in arr.into_iter() {
                result.append_option(long_to_decimal(&v, precision))
            }
            let result_type = DataType::Decimal128(precision, scale);

            Ok(ColumnarValue::Array(Arc::new(
                result.finish().with_data_type(result_type),
            )))
        }
    }
}

/// Convert the input long to decimal with the given maximum precision. If overflows, returns null
/// instead.
#[inline]
fn long_to_decimal(v: &Option<i64>, precision: u8) -> Option<i128> {
    match v {
        Some(v) if validate_decimal_precision(*v as i128, precision).is_ok() => Some(*v as i128),
        _ => None,
    }
}
