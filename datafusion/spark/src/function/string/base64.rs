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
use datafusion_common::arrow::datatypes::{Field, FieldRef};
use datafusion_common::types::{NativeType, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{Coercion, Expr, ReturnFieldArgs, TypeSignatureClass, lit};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::expr_fn::{decode, encode};

/// Apache Spark base64 uses padded base64 encoding.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#base64>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBase64 {
    signature: Signature,
}

impl Default for SparkBase64 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBase64 {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_implicit(
                    TypeSignatureClass::Binary,
                    vec![TypeSignatureClass::Native(logical_string())],
                    NativeType::Binary,
                )],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkBase64 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "base64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_type should not be called for {}", self.name())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [bin] = take_function_args(self.name(), args.arg_fields)?;
        let return_type = match bin.data_type() {
            DataType::LargeBinary => DataType::LargeUtf8,
            _ => DataType::Utf8,
        };
        Ok(Arc::new(Field::new(
            self.name(),
            return_type,
            bin.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!(
            "invoke should not be called on a simplified {} function",
            self.name()
        )
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [bin] = take_function_args(self.name(), args)?;
        Ok(ExprSimplifyResult::Simplified(encode(
            bin,
            lit("base64pad"),
        )))
    }
}

/// <https://spark.apache.org/docs/latest/api/sql/index.html#unbase64>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUnBase64 {
    signature: Signature,
}

impl Default for SparkUnBase64 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkUnBase64 {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_implicit(
                    TypeSignatureClass::Binary,
                    vec![TypeSignatureClass::Native(logical_string())],
                    NativeType::Binary,
                )],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkUnBase64 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "unbase64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_type should not be called for {}", self.name())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [str] = take_function_args(self.name(), args.arg_fields)?;
        let return_type = match str.data_type() {
            DataType::LargeBinary => DataType::LargeBinary,
            _ => DataType::Binary,
        };
        Ok(Arc::new(Field::new(
            self.name(),
            return_type,
            str.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("{} should have been simplified", self.name())
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [bin] = take_function_args(self.name(), args)?;
        Ok(ExprSimplifyResult::Simplified(decode(
            bin,
            lit("base64pad"),
        )))
    }
}
