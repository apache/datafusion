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

use arrow::array::builder::PrimitiveBuilder;
use arrow::array::cast::AsArray;
use arrow::array::types::{Date32Type, Int32Type};
use arrow::array::PrimitiveArray;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Date32, Int32, Int64, UInt32, UInt64, Utf8, Utf8View};
use chrono::prelude::*;

use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct MakeDateFunc {
    signature: Signature,
}

impl Default for MakeDateFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl MakeDateFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                3,
                vec![Int32, Int64, UInt32, UInt64, Utf8, Utf8View],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MakeDateFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "make_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Date32)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 3 {
            return exec_err!(
                "make_date function requires 3 arguments, got {}",
                args.len()
            );
        }

        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let years = args[0].cast_to(&DataType::Int32, None)?;
        let months = args[1].cast_to(&DataType::Int32, None)?;
        let days = args[2].cast_to(&DataType::Int32, None)?;

        let scalar_value_fn = |col: &ColumnarValue| -> Result<i32> {
            let ColumnarValue::Scalar(s) = col else {
                return exec_err!("Expected scalar value");
            };
            let ScalarValue::Int32(Some(i)) = s else {
                return exec_err!("Unable to parse date from null/empty value");
            };
            Ok(*i)
        };

        let value = if let Some(array_size) = len {
            let to_primitive_array_fn =
                |col: &ColumnarValue| -> PrimitiveArray<Int32Type> {
                    match col {
                        ColumnarValue::Array(a) => {
                            a.as_primitive::<Int32Type>().to_owned()
                        }
                        _ => {
                            let v = scalar_value_fn(col).unwrap();
                            PrimitiveArray::<Int32Type>::from_value(v, array_size)
                        }
                    }
                };

            let years = to_primitive_array_fn(&years);
            let months = to_primitive_array_fn(&months);
            let days = to_primitive_array_fn(&days);

            let mut builder: PrimitiveBuilder<Date32Type> =
                PrimitiveArray::builder(array_size);
            for i in 0..array_size {
                make_date_inner(
                    years.value(i),
                    months.value(i),
                    days.value(i),
                    |days: i32| builder.append_value(days),
                )?;
            }

            let arr = builder.finish();

            ColumnarValue::Array(Arc::new(arr))
        } else {
            // For scalar only columns the operation is faster without using the PrimitiveArray.
            // Also, keep the output as scalar since all inputs are scalar.
            let mut value = 0;
            make_date_inner(
                scalar_value_fn(&years)?,
                scalar_value_fn(&months)?,
                scalar_value_fn(&days)?,
                |days: i32| value = days,
            )?;

            ColumnarValue::Scalar(ScalarValue::Date32(Some(value)))
        };

        Ok(value)
    }
}

/// Converts the year/month/day fields to an `i32` representing the days from
/// the unix epoch and invokes `date_consumer_fn` with the value
fn make_date_inner<F: FnMut(i32)>(
    year: i32,
    month: i32,
    day: i32,
    mut date_consumer_fn: F,
) -> Result<()> {
    let Ok(m) = u32::try_from(month) else {
        return exec_err!("Month value '{month:?}' is out of range");
    };
    let Ok(d) = u32::try_from(day) else {
        return exec_err!("Day value '{day:?}' is out of range");
    };

    if let Some(date) = NaiveDate::from_ymd_opt(year, m, d) {
        // The number of days until the start of the unix epoch in the proleptic Gregorian calendar
        // (with January 1, Year 1 (CE) as day 1). See [Datelike::num_days_from_ce].
        const UNIX_DAYS_FROM_CE: i32 = 719_163;

        // since the epoch for the date32 datatype is the unix epoch
        // we need to subtract the unix epoch from the current date
        // note that this can result in a negative value
        date_consumer_fn(date.num_days_from_ce() - UNIX_DAYS_FROM_CE);
        Ok(())
    } else {
        exec_err!("Unable to parse date from {year}, {month}, {day}")
    }
}

#[cfg(test)]
mod tests {
    use crate::datetime::make_date::MakeDateFunc;
    use arrow::array::{Array, Date32Array, Int32Array, Int64Array, UInt32Array};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};
    use std::sync::Arc;

    #[test]
    fn test_make_date() {
        let res = MakeDateFunc::new()
            .invoke(&[
                ColumnarValue::Scalar(ScalarValue::Int32(Some(2024))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
                ColumnarValue::Scalar(ScalarValue::UInt32(Some(14))),
            ])
            .expect("that make_date parsed values without error");

        if let ColumnarValue::Scalar(ScalarValue::Date32(date)) = res {
            assert_eq!(19736, date.unwrap());
        } else {
            panic!("Expected a scalar value")
        }

        let res = MakeDateFunc::new()
            .invoke(&[
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2024))),
                ColumnarValue::Scalar(ScalarValue::UInt64(Some(1))),
                ColumnarValue::Scalar(ScalarValue::UInt32(Some(14))),
            ])
            .expect("that make_date parsed values without error");

        if let ColumnarValue::Scalar(ScalarValue::Date32(date)) = res {
            assert_eq!(19736, date.unwrap());
        } else {
            panic!("Expected a scalar value")
        }

        let res = MakeDateFunc::new()
            .invoke(&[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024".to_string()))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("1".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("14".to_string()))),
            ])
            .expect("that make_date parsed values without error");

        if let ColumnarValue::Scalar(ScalarValue::Date32(date)) = res {
            assert_eq!(19736, date.unwrap());
        } else {
            panic!("Expected a scalar value")
        }

        let years = Arc::new((2021..2025).map(Some).collect::<Int64Array>());
        let months = Arc::new((1..5).map(Some).collect::<Int32Array>());
        let days = Arc::new((11..15).map(Some).collect::<UInt32Array>());
        let res = MakeDateFunc::new()
            .invoke(&[
                ColumnarValue::Array(years),
                ColumnarValue::Array(months),
                ColumnarValue::Array(days),
            ])
            .expect("that make_date parsed values without error");

        if let ColumnarValue::Array(array) = res {
            assert_eq!(array.len(), 4);
            let mut builder = Date32Array::builder(4);
            builder.append_value(18_638);
            builder.append_value(19_035);
            builder.append_value(19_429);
            builder.append_value(19_827);
            assert_eq!(&builder.finish() as &dyn Array, array.as_ref());
        } else {
            panic!("Expected a columnar array")
        }

        //
        // Fallible test cases
        //

        // invalid number of arguments
        let res = MakeDateFunc::new()
            .invoke(&[ColumnarValue::Scalar(ScalarValue::Int32(Some(1)))]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: make_date function requires 3 arguments, got 1"
        );

        // invalid type
        let res = MakeDateFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Arrow error: Cast error: Casting from Interval(YearMonth) to Int32 not supported"
        );

        // overflow of month
        let res = MakeDateFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2023))),
            ColumnarValue::Scalar(ScalarValue::UInt64(Some(u64::MAX))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(22))),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Arrow error: Cast error: Can't cast value 18446744073709551615 to type Int32"
        );

        // overflow of day
        let res = MakeDateFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2023))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(22))),
            ColumnarValue::Scalar(ScalarValue::UInt32(Some(u32::MAX))),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Arrow error: Cast error: Can't cast value 4294967295 to type Int32"
        );
    }
}
