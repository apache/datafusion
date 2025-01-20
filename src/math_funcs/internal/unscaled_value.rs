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

use arrow::{
    array::{AsArray, Int64Builder},
    datatypes::Decimal128Type,
};
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::{internal_err, Result as DataFusionResult, ScalarValue};
use std::sync::Arc;

/// Spark-compatible `UnscaledValue` expression (internal to Spark optimizer)
pub fn spark_unscaled_value(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Scalar(v) => match v {
            ScalarValue::Decimal128(d, _, _) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                d.map(|n| n as i64),
            ))),
            dt => internal_err!("Expected Decimal128 but found {dt:}"),
        },
        ColumnarValue::Array(a) => {
            let arr = a.as_primitive::<Decimal128Type>();
            let mut result = Int64Builder::new();
            for v in arr.into_iter() {
                result.append_option(v.map(|v| v as i64));
            }
            Ok(ColumnarValue::Array(Arc::new(result.finish())))
        }
    }
}
