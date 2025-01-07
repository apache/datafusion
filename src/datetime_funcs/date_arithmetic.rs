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

use arrow::array::{ArrayRef, AsArray};
use arrow::compute::kernels::numeric::{add, sub};
use arrow::datatypes::IntervalDayTime;
use arrow_array::builder::IntervalDayTimeBuilder;
use arrow_array::types::{Int16Type, Int32Type, Int8Type};
use arrow_array::{Array, Datum};
use arrow_schema::{ArrowError, DataType};
use datafusion::physical_expr_common::datum;
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::{DataFusionError, ScalarValue};
use std::sync::Arc;

macro_rules! scalar_date_arithmetic {
    ($start:expr, $days:expr, $op:expr) => {{
        let interval = IntervalDayTime::new(*$days as i32, 0);
        let interval_cv = ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(interval)));
        datum::apply($start, &interval_cv, $op)
    }};
}
macro_rules! array_date_arithmetic {
    ($days:expr, $interval_builder:expr, $intType:ty) => {{
        for day in $days.as_primitive::<$intType>().into_iter() {
            if let Some(non_null_day) = day {
                $interval_builder.append_value(IntervalDayTime::new(non_null_day as i32, 0));
            } else {
                $interval_builder.append_null();
            }
        }
    }};
}

/// Spark-compatible `date_add` and `date_sub` expressions, which assumes days for the second
/// argument, but we cannot directly add that to a Date32. We generate an IntervalDayTime from the
/// second argument and use DataFusion's interface to apply Arrow's operators.
fn spark_date_arithmetic(
    args: &[ColumnarValue],
    op: impl Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>,
) -> Result<ColumnarValue, DataFusionError> {
    let start = &args[0];
    match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Int8(Some(days))) => {
            scalar_date_arithmetic!(start, days, op)
        }
        ColumnarValue::Scalar(ScalarValue::Int16(Some(days))) => {
            scalar_date_arithmetic!(start, days, op)
        }
        ColumnarValue::Scalar(ScalarValue::Int32(Some(days))) => {
            scalar_date_arithmetic!(start, days, op)
        }
        ColumnarValue::Array(days) => {
            let mut interval_builder = IntervalDayTimeBuilder::with_capacity(days.len());
            match days.data_type() {
                DataType::Int8 => {
                    array_date_arithmetic!(days, interval_builder, Int8Type)
                }
                DataType::Int16 => {
                    array_date_arithmetic!(days, interval_builder, Int16Type)
                }
                DataType::Int32 => {
                    array_date_arithmetic!(days, interval_builder, Int32Type)
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported data types {:?} for date arithmetic.",
                        args,
                    )))
                }
            }
            let interval_cv = ColumnarValue::Array(Arc::new(interval_builder.finish()));
            datum::apply(start, &interval_cv, op)
        }
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported data types {:?} for date arithmetic.",
            args,
        ))),
    }
}

pub fn spark_date_add(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    spark_date_arithmetic(args, add)
}

pub fn spark_date_sub(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    spark_date_arithmetic(args, sub)
}
