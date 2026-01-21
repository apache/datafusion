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
use arrow::array::{Array, PrimitiveArray};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Date32;
use chrono::prelude::*;

use datafusion_common::types::{NativeType, logical_int32, logical_string};
use datafusion_common::{Result, ScalarValue, exec_err, utils::take_function_args};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignatureClass,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Make a date from year/month/day component parts.",
    syntax_example = "make_date(year, month, day)",
    sql_example = r#"```sql
> select make_date(2023, 1, 31);
+-------------------------------------------+
| make_date(Int64(2023),Int64(1),Int64(31)) |
+-------------------------------------------+
| 2023-01-31                                |
+-------------------------------------------+
> select make_date('2023', '01', '31');
+-----------------------------------------------+
| make_date(Utf8("2023"),Utf8("01"),Utf8("31")) |
+-----------------------------------------------+
| 2023-01-31                                    |
+-----------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/date_time.rs)
"#,
    argument(
        name = "year",
        description = "Year to use when making the date. Can be a constant, column or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "month",
        description = "Month to use when making the date. Can be a constant, column or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "day",
        description = "Day to use when making the date. Can be a constant, column or function, and any combination of arithmetic operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
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
        let int = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int32()),
            vec![
                TypeSignatureClass::Integer,
                TypeSignatureClass::Native(logical_string()),
            ],
            NativeType::Int32,
        );
        Self {
            signature: Signature::coercible(vec![int; 3], Volatility::Immutable),
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

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let [years, months, days] = take_function_args(self.name(), args.args)?;

        match (years, months, days) {
            (ColumnarValue::Scalar(y), _, _) if y.is_null() => {
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)))
            }
            (_, ColumnarValue::Scalar(m), _) if m.is_null() => {
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)))
            }
            (_, _, ColumnarValue::Scalar(d)) if d.is_null() => {
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Int32(Some(years))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(months))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(days))),
            ) => {
                let mut value = 0;
                make_date_inner(years, months, days, |days: i32| value = days)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(Some(value))))
            }
            (years, months, days) => {
                let len = args.number_rows;
                let years = years.into_array(len)?;
                let months = months.into_array(len)?;
                let days = days.into_array(len)?;

                let years = years.as_primitive::<Int32Type>();
                let months = months.as_primitive::<Int32Type>();
                let days = days.as_primitive::<Int32Type>();

                let mut builder: PrimitiveBuilder<Date32Type> =
                    PrimitiveArray::builder(len);

                for i in 0..len {
                    // match postgresql behaviour which returns null for any null input
                    if years.is_null(i) || months.is_null(i) || days.is_null(i) {
                        builder.append_null();
                    } else {
                        make_date_inner(
                            years.value(i),
                            months.value(i),
                            days.value(i),
                            |days: i32| builder.append_value(days),
                        )?;
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
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
    let m = match month {
        1..=12 => month as u32,
        _ => return exec_err!("Month value '{month:?}' is out of range"),
    };
    let d = match day {
        1..=31 => day as u32,
        _ => return exec_err!("Day value '{day:?}' is out of range"),
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
