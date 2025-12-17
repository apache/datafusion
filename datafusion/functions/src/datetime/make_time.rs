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
use arrow::array::{Array, PrimitiveArray};
use arrow::datatypes::DataType::Time32;
use arrow::datatypes::{DataType, Time32SecondType, TimeUnit};
use chrono::prelude::*;

use datafusion_common::types::{NativeType, logical_int32, logical_string};
use datafusion_common::{Result, ScalarValue, exec_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Make a time from hour/minute/second component parts.",
    syntax_example = "make_time(hour, minute, second)",
    sql_example = r#"```sql
> select make_time(13, 23, 1);
+-------------------------------------------+
| make_time(Int64(13),Int64(23),Int64(1))   |
+-------------------------------------------+
| 13:23:01                                  |
+-------------------------------------------+
> select make_time('23', '01', '31');
+-----------------------------------------------+
| make_time(Utf8("23"),Utf8("01"),Utf8("31"))   |
+-----------------------------------------------+
| 23:01:31                                      |
+-----------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/date_time.rs)
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
        Ok(Time32(TimeUnit::Second))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let [hours, minutes, seconds] = take_function_args(self.name(), args.args)?;

        match (hours, minutes, seconds) {
            (ColumnarValue::Scalar(h), _, _) if h.is_null() => {
                Ok(ColumnarValue::Scalar(ScalarValue::Time32Second(None)))
            }
            (_, ColumnarValue::Scalar(m), _) if m.is_null() => {
                Ok(ColumnarValue::Scalar(ScalarValue::Time32Second(None)))
            }
            (_, _, ColumnarValue::Scalar(s)) if s.is_null() => {
                Ok(ColumnarValue::Scalar(ScalarValue::Time32Second(None)))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Int32(Some(hours))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(minutes))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(seconds))),
            ) => {
                let mut value = 0;
                make_time_inner(hours, minutes, seconds, |seconds: i32| value = seconds)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Time32Second(Some(
                    value,
                ))))
            }
            (hours, minutes, seconds) => {
                let len = args.number_rows;
                let hours = hours.into_array(len)?;
                let minutes = minutes.into_array(len)?;
                let seconds = seconds.into_array(len)?;

                let hours = hours.as_primitive::<Int32Type>();
                let minutes = minutes.as_primitive::<Int32Type>();
                let seconds = seconds.as_primitive::<Int32Type>();

                let mut builder: PrimitiveBuilder<Time32SecondType> =
                    PrimitiveArray::builder(len);

                for i in 0..len {
                    // match postgresql behaviour which returns null for any null input
                    if hours.is_null(i) || minutes.is_null(i) || seconds.is_null(i) {
                        builder.append_null();
                    } else {
                        make_time_inner(
                            hours.value(i),
                            minutes.value(i),
                            seconds.value(i),
                            |seconds: i32| builder.append_value(seconds),
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

/// Converts the hour/minute/second fields to an `i32` representing the seconds from
/// midnight and invokes `time_consumer_fn` with the value
fn make_time_inner<F: FnMut(i32)>(
    hour: i32,
    minute: i32,
    second: i32,
    mut time_consumer_fn: F,
) -> Result<()> {
    let h = match hour {
        0..=24 => hour as u32,
        _ => return exec_err!("Hour value '{hour:?}' is out of range"),
    };
    let m = match minute {
        0..=60 => minute as u32,
        _ => return exec_err!("Minute value '{minute:?}' is out of range"),
    };
    let s = match second {
        0..=60 => second as u32,
        _ => return exec_err!("Second value '{second:?}' is out of range"),
    };

    if let Some(time) = NaiveTime::from_hms_opt(h, m, s) {
        time_consumer_fn(time.num_seconds_from_midnight() as i32);
        Ok(())
    } else {
        exec_err!("Unable to parse time from {hour}, {minute}, {second}")
    }
}

#[cfg(test)]
mod tests {
    use crate::datetime::make_time::MakeTimeFunc;
    use arrow::array::{Array, Int32Array, Time32SecondArray};
    use arrow::datatypes::TimeUnit::Second;
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::DataFusionError;
    use datafusion_common::config::ConfigOptions;
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
            return_field: Field::new("f", DataType::Time32(Second), true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        MakeTimeFunc::new().invoke_with_args(args)
    }

    #[test]
    fn test_make_time() {
        let hours = Arc::new((4..8).map(Some).collect::<Int32Array>());
        let minutes = Arc::new((1..5).map(Some).collect::<Int32Array>());
        let seconds = Arc::new((11..15).map(Some).collect::<Int32Array>());
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

            let mut builder = Time32SecondArray::builder(4);
            builder.append_value(14_471);
            builder.append_value(18_132);
            builder.append_value(21_793);
            builder.append_value(25_454);
            assert_eq!(&builder.finish() as &dyn Array, array.as_ref());
        } else {
            panic!("Expected a columnar array")
        }
    }
}
