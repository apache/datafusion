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
use datafusion_common::types::{NativeType, logical_date, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, internal_err, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Coercion, ColumnarValue, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignatureClass, Volatility,
};

/// Spark trunc supports date inputs only and extra format aliases.
/// Also spark trunc's argument order is (date, format).
/// <https://spark.apache.org/docs/latest/api/sql/index.html#trunc>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTrunc {
    signature: Signature,
}

impl Default for SparkTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTrunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_date()),
                        vec![TypeSignatureClass::Native(logical_string())],
                        NativeType::Date,
                    ),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkTrunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "trunc"
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
        internal_err!("spark trunc should have been simplified to standard date_trunc")
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [dt_expr, fmt_expr] = take_function_args(self.name(), args)?;

        let fmt = match fmt_expr.as_literal() {
            Some(ScalarValue::Utf8(Some(v)))
            | Some(ScalarValue::Utf8View(Some(v)))
            | Some(ScalarValue::LargeUtf8(Some(v))) => v.to_lowercase(),
            _ => {
                return plan_err!(
                    "Second argument of `TRUNC` must be non-null scalar Utf8"
                );
            }
        };

        // Map Spark-specific fmt aliases to datafusion ones
        let fmt = match fmt.as_str() {
            "yy" | "yyyy" => "year",
            "mm" | "mon" => "month",
            "year" | "month" | "day" | "week" | "quarter" => fmt.as_str(),
            _ => {
                return plan_err!(
                    "The format argument of `TRUNC` must be one of: year, yy, yyyy, month, mm, mon, day, week, quarter."
                );
            }
        };
        let return_type = dt_expr.get_type(info.schema())?;

        let fmt_expr = Expr::Literal(ScalarValue::new_utf8(fmt), None);

        // Spark uses Dates so we need to cast to timestamp and back to work with datafusion's date_trunc
        Ok(ExprSimplifyResult::Simplified(
            Expr::ScalarFunction(ScalarFunction::new_udf(
                datafusion_functions::datetime::date_trunc(),
                vec![
                    fmt_expr,
                    dt_expr.cast_to(
                        &DataType::Timestamp(TimeUnit::Nanosecond, None),
                        info.schema(),
                    )?,
                ],
            ))
            .cast_to(&return_type, info.schema())?,
        ))
    }
}
