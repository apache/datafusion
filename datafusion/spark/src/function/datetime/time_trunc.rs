// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::types::logical_string;
use datafusion_common::{Result, ScalarValue, internal_err, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Coercion, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignatureClass, Volatility,
};

/// Spark time_trunc function only handles time inputs.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#time_trunc>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimeTrunc {
    signature: Signature,
}

impl Default for SparkTimeTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTimeTrunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Time),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkTimeTrunc {
    fn name(&self) -> &str {
        "time_trunc"
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
            args.arg_fields[1].data_type().clone(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!(
            "spark time_trunc should have been simplified to standard date_trunc"
        )
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let fmt_expr = &args[0];

        match fmt_expr.as_literal() {
            Some(ScalarValue::Utf8(Some(_)))
            | Some(ScalarValue::Utf8View(Some(_)))
            | Some(ScalarValue::LargeUtf8(Some(_))) => {}
            _ => {
                return plan_err!(
                    "First argument of `TIME_TRUNC` must be non-null scalar Utf8"
                );
            }
        }

        Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
            ScalarFunction::new_udf(datafusion_functions::datetime::date_trunc(), args),
        )))
    }
}
