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
use arrow::array::types::Int32Type;
use arrow::array::PrimitiveArray;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Int32, Int64, Time64, UInt32, UInt64, Utf8, Utf8View};
use arrow::datatypes::Time64NanosecondType;
use arrow::datatypes::TimeUnit::Nanosecond;
use chrono::{NaiveTime, Timelike};

use datafusion_common::{exec_err, utils::take_function_args, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Make a time from hour/minute/second component parts.",
    syntax_example = "make_time(hour, minute, second)",
    sql_example = r#"```sql
> select make_time(14, 30, 45);
+-----------------------------------+
|| make_time(Int64(14),Int64(30),Int64(45)) |
+-----------------------------------+
|| 14:30:45                          |
+-----------------------------------+
> select make_time('14', '30', '45');
+---------------------------------------+
|| make_time(Utf8("14"),Utf8("30"),Utf8("45")) |
+---------------------------------------+
|| 14:30:45                              |
+---------------------------------------+
```
"#,
    argument(
        name = "hour",
        description = "Hour to use when making the time. Can be a constant, column or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "minute",
        description = "Minute to use when making the time. Can be a constant, column or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "second",
        description = "Second to use when making the time. Can be a constant, column or function, and any combination of arithmetic operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MakeTimeFunc {
    signature: Signature,
}

impl Default for MakeTimeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl MakeTimeFunc {
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

impl ScalarUDFImpl for MakeTimeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "make_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Time64(Nanosecond))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = args.args;
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let [hours, minutes, seconds] = take_function_args(self.name(), args)?;

        if matches!(hours, ColumnarValue::Scalar(ScalarValue::Null))
            || matches!(minutes, ColumnarValue::Scalar(ScalarValue::Null))
            || matches!(seconds, ColumnarValue::Scalar(ScalarValue::Null))
        {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }

        let hours = hours.cast_to(&Int32, None)?;
        let minutes = minutes.cast_to(&Int32, None)?;
        let seconds = seconds.cast_to(&Int32, None)?;

        let get_scalar = |col: &ColumnarValue| -> Result<i32> {
            let ColumnarValue::Scalar(s) = col else {
                return exec_err!("Expected scalar value");
            };
            let ScalarValue::Int32(Some(i)) = s else {
                return exec_err!("Unable to parse time from null/empty value");
            };
            Ok(*i)
        };

        let value = if let Some(array_size) = len {
            let to_array = |col: &ColumnarValue| -> PrimitiveArray<Int32Type> {
                match col {
                    ColumnarValue::Array(a) => a.as_primitive::<Int32Type>().to_owned(),
                    _ => {
                        let v = get_scalar(col).unwrap();
                        PrimitiveArray::<Int32Type>::from_value(v, array_size)
                    }
                }
            };

            let hours = to_array(&hours);
            let minutes = to_array(&minutes);
            let seconds = to_array(&seconds);

            let mut builder: PrimitiveBuilder<Time64NanosecondType> =
                PrimitiveArray::builder(array_size);
            for i in 0..array_size {
                make_time_inner(
                    hours.value(i),
                    minutes.value(i),
                    seconds.value(i),
                    |nanos: i64| builder.append_value(nanos),
                )?;
            }

            let arr = builder.finish();

            ColumnarValue::Array(Arc::new(arr))
        } else {
            let mut result = 0i64;
            make_time_inner(
                get_scalar(&hours)?,
                get_scalar(&minutes)?,
                get_scalar(&seconds)?,
                |nanos: i64| result = nanos,
            )?;

            ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(Some(result)))
        };

        Ok(value)
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn make_time_inner<F: FnMut(i64)>(
    hour: i32,
    minute: i32,
    second: i32,
    mut f: F,
) -> Result<()> {
    let Ok(h) = u32::try_from(hour) else {
        return exec_err!("Hour value '{hour}' is out of range");
    };
    let Ok(m) = u32::try_from(minute) else {
        return exec_err!("Minute value '{minute}' is out of range");
    };
    let Ok(s) = u32::try_from(second) else {
        return exec_err!("Second value '{second}' is out of range");
    };

    if let Some(time) = NaiveTime::from_hms_opt(h, m, s) {
        let nanos = (time.hour() as i64 * 3600
            + time.minute() as i64 * 60
            + time.second() as i64)
            * 1_000_000_000
            + time.nanosecond() as i64;
        f(nanos);
        Ok(())
    } else {
        exec_err!("Unable to parse time from {hour}, {minute}, {second}")
    }
}

#[cfg(test)]
mod tests {
    use crate::datetime::make_time::MakeTimeFunc;
    use arrow::array::{Array, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{DataFusionError, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};
    use std::sync::Arc;

    fn invoke_make_time_with_args(
        args: Vec<ColumnarValue>,
        number_rows: usize,
    ) -> Result<ColumnarValue, DataFusionError> {
        let arg_fields = args
            .iter()
            .map(|arg| Field::new("a", arg.data_type(), true).into())
            .collect::<Vec<_>>();
        let args = datafusion_expr::ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Field::new(
                "f",
                DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond),
                true,
            )
            .into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        MakeTimeFunc::new().invoke_with_args(args)
    }

    #[test]
    fn test_make_time() {
        let res = invoke_make_time_with_args(
            vec![
                ColumnarValue::Scalar(ScalarValue::Int32(Some(14))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(30))),
                ColumnarValue::Scalar(ScalarValue::UInt32(Some(45))),
            ],
            1,
        )
        .expect("that make_time parsed values without error");

        if let ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(time)) = res {
            let expected = 14 * 3600 + 30 * 60 + 45;
            assert_eq!(Some(expected * 1_000_000_000), time);
        } else {
            panic!("Expected a scalar value")
        }

        let hours = Arc::new((14..18).map(Some).collect::<Int64Array>());
        let minutes = Arc::new((30..34).map(Some).collect::<Int32Array>());
        let seconds = Arc::new((45..49).map(Some).collect::<Int32Array>());
        let batch_len = hours.len();
        let res = invoke_make_time_with_args(
            vec![
                ColumnarValue::Array(hours),
                ColumnarValue::Array(minutes),
                ColumnarValue::Array(seconds),
            ],
            batch_len,
        )
        .unwrap();

        if let ColumnarValue::Array(array) = res {
            assert_eq!(array.len(), 4);
        } else {
            panic!("Expected a columnar array")
        }
    }

    #[test]
    fn test_make_time_null_param() {
        let res = invoke_make_time_with_args(
            vec![
                ColumnarValue::Scalar(ScalarValue::Null),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(30))),
                ColumnarValue::Scalar(ScalarValue::UInt32(Some(45))),
            ],
            1,
        )
        .expect("that make_time parsed values without error");

        assert!(matches!(res, ColumnarValue::Scalar(ScalarValue::Null)));
    }
}
