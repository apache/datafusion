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

use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::types::logical_date;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Coercion, ColumnarValue, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignatureClass, Volatility,
};

/// Returns the number of days since epoch (1970-01-01) for the given date.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#unix_date>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUnixDate {
    signature: Signature,
}

impl Default for SparkUnixDate {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkUnixDate {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Native(
                    logical_date(),
                ))],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkUnixDate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "unix_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields[0].is_nullable();
        Ok(Arc::new(Field::new(self.name(), DataType::Int32, nullable)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("invoke_with_args should not be called on SparkUnixDate")
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [date] = take_function_args(self.name(), args)?;
        Ok(ExprSimplifyResult::Simplified(
            date.cast_to(&DataType::Date32, info.schema())?
                .cast_to(&DataType::Int32, info.schema())?,
        ))
    }
}

/// Returns the number of microseconds since epoch (1970-01-01 00:00:00 UTC) for the given timestamp.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#unix_micros>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUnixMicros {
    signature: Signature,
}

/// Returns the number of milliseconds since epoch (1970-01-01 00:00:00 UTC) for the given timestamp.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#unix_millis>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUnixMillis {
    signature: Signature,
}

/// Returns the number of seconds since epoch (1970-01-01 00:00:00 UTC) for the given timestamp.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#unix_seconds>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUnixSeconds {
    signature: Signature,
}

macro_rules! define_unix_timestamp_udf {
    ($func:ty, $func_name:literal, $time_unit:expr) => {
        impl Default for $func {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $func {
            pub fn new() -> Self {
                Self {
                    signature: Signature::coercible(
                        vec![Coercion::new_exact(TypeSignatureClass::Timestamp)],
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl ScalarUDFImpl for $func {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $func_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                internal_err!("return_field_from_args should be used instead")
            }

            fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
                let nullable = args.arg_fields[0].is_nullable();
                Ok(Arc::new(Field::new(self.name(), DataType::Int64, nullable)))
            }

            fn invoke_with_args(
                &self,
                _args: ScalarFunctionArgs,
            ) -> Result<ColumnarValue> {
                internal_err!(
                    "invoke_with_args should not be called on `{}`",
                    self.name()
                )
            }

            fn simplify(
                &self,
                args: Vec<Expr>,
                info: &SimplifyContext,
            ) -> Result<ExprSimplifyResult> {
                let [ts] = take_function_args(self.name(), args)?;
                Ok(ExprSimplifyResult::Simplified(
                    ts.cast_to(
                        &DataType::Timestamp($time_unit, Some("UTC".into())),
                        info.schema(),
                    )?
                    .cast_to(&DataType::Int64, info.schema())?,
                ))
            }
        }
    };
}

define_unix_timestamp_udf!(SparkUnixMicros, "unix_micros", TimeUnit::Microsecond);
define_unix_timestamp_udf!(SparkUnixMillis, "unix_millis", TimeUnit::Millisecond);
define_unix_timestamp_udf!(SparkUnixSeconds, "unix_seconds", TimeUnit::Second);
