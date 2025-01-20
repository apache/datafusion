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

use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Int64, Timestamp, Utf8};
use arrow::datatypes::TimeUnit::Second;
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnInfo, ReturnTypeArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Converts an integer to RFC3339 timestamp format (`YYYY-MM-DDT00:00:00.000000000Z`). Integers and unsigned integers are interpreted as nanoseconds since the unix epoch (`1970-01-01T00:00:00Z`) return the corresponding timestamp.",
    syntax_example = "from_unixtime(expression[, timezone])",
    sql_example = r#"```sql
> select from_unixtime(1599572549, 'America/New_York');
+-----------------------------------------------------------+
| from_unixtime(Int64(1599572549),Utf8("America/New_York")) |
+-----------------------------------------------------------+
| 2020-09-08T09:42:29-04:00                                 |
+-----------------------------------------------------------+
```"#,
    standard_argument(name = "expression",),
    argument(
        name = "timezone",
        description = "Optional timezone to use when converting the integer to a timestamp. If not provided, the default timezone is UTC."
    )
)]
#[derive(Debug)]
pub struct FromUnixtimeFunc {
    signature: Signature,
}

impl Default for FromUnixtimeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl FromUnixtimeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![Exact(vec![Int64, Utf8]), Exact(vec![Int64])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for FromUnixtimeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "from_unixtime"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> Result<ReturnInfo> {
        // Length check handled in the signature
        debug_assert!(matches!(args.scalar_arguments.len(), 1 | 2));

        if args.scalar_arguments.len() == 1 {
            Ok(ReturnInfo::new_nullable(Timestamp(Second, None)))
        } else {
            args.scalar_arguments[1]
                .and_then(|sv| {
                    sv.try_as_str()
                        .flatten()
                        .filter(|s| !s.is_empty())
                        .map(|tz| {
                            ReturnInfo::new_nullable(Timestamp(
                                Second,
                                Some(Arc::from(tz.to_string())),
                            ))
                        })
                })
                .map_or_else(
                    || {
                        exec_err!(
                            "{} requires its second argument to be a constant string",
                            self.name()
                        )
                    },
                    Ok,
                )
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("call return_type_from_args instead")
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        let len = args.len();
        if len != 1 && len != 2 {
            return exec_err!(
                "from_unixtime function requires 1 or 2 argument, got {}",
                args.len()
            );
        }

        if args[0].data_type() != Int64 {
            return exec_err!(
                "Unsupported data type {:?} for function from_unixtime",
                args[0].data_type()
            );
        }

        match len {
            1 => args[0].cast_to(&Timestamp(Second, None), None),
            2 => match &args[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(tz))) => args[0]
                    .cast_to(&Timestamp(Second, Some(Arc::from(tz.to_string()))), None),
                _ => {
                    exec_err!(
                        "Unsupported data type {:?} for function from_unixtime",
                        args[1].data_type()
                    )
                }
            },
            _ => unreachable!(),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod test {
    use crate::datetime::from_unixtime::FromUnixtimeFunc;
    use datafusion_common::ScalarValue;
    use datafusion_common::ScalarValue::Int64;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    #[test]
    fn test_without_timezone() {
        let args = [ColumnarValue::Scalar(Int64(Some(1729900800)))];

        // TODO use invoke_with_args
        let result = FromUnixtimeFunc::new().invoke_batch(&args, 1).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(Some(sec), None)) => {
                assert_eq!(sec, 1729900800);
            }
            _ => panic!("Expected scalar value"),
        }
    }

    #[test]
    fn test_with_timezone() {
        let args = [
            ColumnarValue::Scalar(Int64(Some(1729900800))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "America/New_York".to_string(),
            ))),
        ];

        // TODO use invoke_with_args
        let result = FromUnixtimeFunc::new().invoke_batch(&args, 2).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(Some(sec), Some(tz))) => {
                assert_eq!(sec, 1729900800);
                assert_eq!(tz.to_string(), "America/New_York");
            }
            _ => panic!("Expected scalar value"),
        }
    }
}
