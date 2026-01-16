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
use datafusion_common::types::{NativeType, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Coercion, ColumnarValue, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignatureClass, Volatility, lit,
};
use datafusion_functions::datetime::to_local_time;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#from_utc_timestamp>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFromUtcTimestamp {
    signature: Signature,
}

impl SparkFromUtcTimestamp {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_implicit(
                        TypeSignatureClass::Timestamp,
                        vec![TypeSignatureClass::Native(logical_string())],
                        NativeType::Timestamp(TimeUnit::Microsecond, None),
                    ),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkFromUtcTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "from_utc_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());

        Ok(Arc::new(Field::new(
            self.name(),
            args.arg_fields[0].data_type().clone(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("`from_utc_timestamp` should be simplified away before execution")
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [timestamp, timezone] = take_function_args("from_utc_timestamp", args)?;

        match timezone.as_literal() {
            Some(ScalarValue::Utf8(timezone_opt))
            | Some(ScalarValue::LargeUtf8(timezone_opt))
            | Some(ScalarValue::Utf8View(timezone_opt)) => match timezone_opt {
                Some(timezone) => {
                    let strip_timezone = Expr::ScalarFunction(ScalarFunction::new_udf(
                        to_local_time(),
                        vec![timestamp],
                    ));

                    let cast_to_utc = strip_timezone.cast_to(
                        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                        info.schema(),
                    )?;

                    let cast_to_timezone = cast_to_utc.cast_to(
                        &DataType::Timestamp(
                            TimeUnit::Microsecond,
                            Some(Arc::from(timezone.to_string())),
                        ),
                        info.schema(),
                    )?;

                    Ok(ExprSimplifyResult::Simplified(cast_to_timezone))
                }
                None => {
                    let timestamp_type = info.get_data_type(&timestamp)?;
                    Ok(ExprSimplifyResult::Simplified(lit(
                        ScalarValue::Null.cast_to(&timestamp_type)?
                    )))
                }
            },
            _ => Ok(ExprSimplifyResult::Original(vec![timestamp, timezone])),
        }
    }
}
