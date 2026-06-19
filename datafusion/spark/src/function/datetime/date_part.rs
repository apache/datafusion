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

use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::types::logical_date;
use datafusion_common::{
    Result, ScalarValue, internal_err, types::logical_string, utils::take_function_args,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Coercion, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignature, TypeSignatureClass, Volatility,
};

/// Wrapper around datafusion date_part function to handle
/// Spark behavior returning day of the week 1-indexed instead of 0-indexed and different part aliases.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#date_part>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDatePart {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkDatePart {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDatePart {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Timestamp),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_date())),
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("datepart")],
        }
    }
}

impl ScalarUDFImpl for SparkDatePart {
    fn name(&self) -> &str {
        "date_part"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("Use return_field_from_args in this case instead.")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Spark's translation layer stores literals in the `arg_fields` instead of
        // `scalar_arguments`. We extract the literal string and patch `scalar_arguments`
        // so we can safely delegate to DataFusion's native `date_part` implementation.

        let extracted_scalar = match args.scalar_arguments.first() {
            Some(None) => {
                let field_name = args.arg_fields[0].name().as_str();

                let extracted_part = if field_name.contains('"') {
                    field_name.split('"').nth(1).unwrap_or(field_name)
                } else {
                    field_name
                };

                Some(ScalarValue::Utf8(Some(extracted_part.to_string())))
            }
            _ => None,
        };

        // Inject the extracted literal into the 0th position if it was missing
        let patched_scalars: Vec<Option<&ScalarValue>> = args
            .scalar_arguments
            .iter()
            .enumerate()
            .map(|(i, &scalar)| {
                if i == 0 && scalar.is_none() {
                    extracted_scalar.as_ref()
                } else {
                    scalar
                }
            })
            .collect();

        let patched_args = ReturnFieldArgs {
            arg_fields: args.arg_fields,
            scalar_arguments: &patched_scalars,
        };

        datafusion_functions::datetime::date_part().return_field_from_args(patched_args)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("spark date_part should have been simplified to standard date_part")
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [part_expr, date_expr] = take_function_args(self.name(), args)?;

        let part = match part_expr.as_literal() {
            Some(ScalarValue::Utf8(Some(v)))
            | Some(ScalarValue::Utf8View(Some(v)))
            | Some(ScalarValue::LargeUtf8(Some(v))) => v.to_lowercase(),
            _ => {
                return internal_err!(
                    "First argument of `DATE_PART` must be non-null scalar Utf8"
                );
            }
        };

        // Map Spark-specific date part aliases to datafusion ones
        let part = match part.as_str() {
            "yearofweek" | "year_iso" => "isoyear",
            "dayofweek" => "dow",
            "dayofweek_iso" | "dow_iso" => "isodow",
            other => other,
        };

        let part_expr = Expr::Literal(ScalarValue::new_utf8(part), None);

        let date_part_expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            datafusion_functions::datetime::date_part(),
            vec![part_expr, date_expr],
        ));

        match part {
            // Spark's `dayofweek` is 1..=7 (Sun=1) but df's `dow` is 0..=6
            // (Sun=0); shift by +1. df's `isodow` already returns the
            // PG-correct 1..=7 (Mon=1), which matches Spark's
            // `dayofweek_iso`/`dow_iso`, so no shift is needed there.
            "dow" => Ok(ExprSimplifyResult::Simplified(
                date_part_expr + Expr::Literal(ScalarValue::Int32(Some(1)), None),
            )),
            _ => Ok(ExprSimplifyResult::Simplified(date_part_expr)),
        }
    }
}
