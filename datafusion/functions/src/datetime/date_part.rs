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

use std::any::Any;
use std::sync::Arc;

use arrow::array::types::ArrowTemporalType;
use arrow::array::{Array, ArrayRef, ArrowNumericType, Float64Array, PrimitiveArray};
use arrow::compute::cast;
use arrow::compute::kernels::temporal;
use arrow::datatypes::DataType::{Date32, Date64, Float64, Timestamp, Utf8};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{DataType, TimeUnit};

use datafusion_common::cast::{
    as_date32_array, as_date64_array, as_timestamp_microsecond_array,
    as_timestamp_millisecond_array, as_timestamp_nanosecond_array,
    as_timestamp_second_array,
};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, Volatility, TIMEZONE_WILDCARD,
};

#[derive(Debug)]
pub(super) struct DatePartFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl DatePartFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Timestamp(Nanosecond, None)]),
                    Exact(vec![
                        Utf8,
                        Timestamp(Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![Utf8, Timestamp(Millisecond, None)]),
                    Exact(vec![
                        Utf8,
                        Timestamp(Millisecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![Utf8, Timestamp(Microsecond, None)]),
                    Exact(vec![
                        Utf8,
                        Timestamp(Microsecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![Utf8, Timestamp(Second, None)]),
                    Exact(vec![
                        Utf8,
                        Timestamp(Second, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![Utf8, Date64]),
                    Exact(vec![Utf8, Date32]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("datepart")],
        }
    }
}

macro_rules! extract_date_part {
    ($ARRAY: expr, $FN:expr) => {
        match $ARRAY.data_type() {
            DataType::Date32 => {
                let array = as_date32_array($ARRAY)?;
                Ok($FN(array)
                    .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
            }
            DataType::Date64 => {
                let array = as_date64_array($ARRAY)?;
                Ok($FN(array)
                    .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
            }
            DataType::Timestamp(time_unit, _) => match time_unit {
                TimeUnit::Second => {
                    let array = as_timestamp_second_array($ARRAY)?;
                    Ok($FN(array)
                        .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
                }
                TimeUnit::Millisecond => {
                    let array = as_timestamp_millisecond_array($ARRAY)?;
                    Ok($FN(array)
                        .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
                }
                TimeUnit::Microsecond => {
                    let array = as_timestamp_microsecond_array($ARRAY)?;
                    Ok($FN(array)
                        .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
                }
                TimeUnit::Nanosecond => {
                    let array = as_timestamp_nanosecond_array($ARRAY)?;
                    Ok($FN(array)
                        .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
                }
            },
            datatype => exec_err!("Extract does not support datatype {:?}", datatype),
        }
    };
}

impl ScalarUDFImpl for DatePartFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "date_part"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("Expected two arguments in DATE_PART");
        }
        let (date_part, array) = (&args[0], &args[1]);

        let date_part =
            if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = date_part {
                v
            } else {
                return exec_err!(
                    "First argument of `DATE_PART` must be non-null scalar Utf8"
                );
            };

        let is_scalar = matches!(array, ColumnarValue::Scalar(_));

        let array = match array {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let arr = match date_part.to_lowercase().as_str() {
            "year" => extract_date_part!(&array, temporal::year),
            "quarter" => extract_date_part!(&array, temporal::quarter),
            "month" => extract_date_part!(&array, temporal::month),
            "week" => extract_date_part!(&array, temporal::week),
            "day" => extract_date_part!(&array, temporal::day),
            "doy" => extract_date_part!(&array, temporal::doy),
            "dow" => extract_date_part!(&array, temporal::num_days_from_sunday),
            "hour" => extract_date_part!(&array, temporal::hour),
            "minute" => extract_date_part!(&array, temporal::minute),
            "second" => extract_date_part!(&array, seconds),
            "millisecond" => extract_date_part!(&array, millis),
            "microsecond" => extract_date_part!(&array, micros),
            "nanosecond" => extract_date_part!(&array, nanos),
            "epoch" => extract_date_part!(&array, epoch),
            _ => exec_err!("Date part '{date_part}' not supported"),
        }?;

        Ok(if is_scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&arr?, 0)?)
        } else {
            ColumnarValue::Array(arr?)
        })
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn to_ticks<T>(array: &PrimitiveArray<T>, frac: i32) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    let zipped = temporal::second(array)?
        .values()
        .iter()
        .zip(temporal::nanosecond(array)?.values().iter())
        .map(|o| (*o.0 as f64 + (*o.1 as f64) / 1_000_000_000.0) * (frac as f64))
        .collect::<Vec<f64>>();

    Ok(Float64Array::from(zipped))
}

fn seconds<T>(array: &PrimitiveArray<T>) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    to_ticks(array, 1)
}

fn millis<T>(array: &PrimitiveArray<T>) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    to_ticks(array, 1_000)
}

fn micros<T>(array: &PrimitiveArray<T>) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    to_ticks(array, 1_000_000)
}

fn nanos<T>(array: &PrimitiveArray<T>) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    to_ticks(array, 1_000_000_000)
}

fn epoch<T>(array: &PrimitiveArray<T>) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    let b = match array.data_type() {
        Timestamp(tu, _) => {
            let scale = match tu {
                Second => 1,
                Millisecond => 1_000,
                Microsecond => 1_000_000,
                Nanosecond => 1_000_000_000,
            } as f64;
            array.unary(|n| {
                let n: i64 = n.into();
                n as f64 / scale
            })
        }
        Date32 => {
            let seconds_in_a_day = 86400_f64;
            array.unary(|n| {
                let n: i64 = n.into();
                n as f64 * seconds_in_a_day
            })
        }
        Date64 => array.unary(|n| {
            let n: i64 = n.into();
            n as f64 / 1_000_f64
        }),
        _ => return exec_err!("Can not convert {:?} to epoch", array.data_type()),
    };
    Ok(b)
}
