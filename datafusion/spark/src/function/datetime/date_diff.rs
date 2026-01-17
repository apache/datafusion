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

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::types::{NativeType, logical_date, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Coercion, ColumnarValue, Expr, ExprSchemable, Operator, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignatureClass, Volatility,
    binary_expr,
};

/// <https://spark.apache.org/docs/latest/api/sql/index.html#date_diff>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateDiff {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkDateDiff {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDateDiff {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_date()),
                        vec![
                            TypeSignatureClass::Native(logical_string()),
                            TypeSignatureClass::Timestamp,
                        ],
                        NativeType::Date,
                    ),
                    Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_date()),
                        vec![
                            TypeSignatureClass::Native(logical_string()),
                            TypeSignatureClass::Timestamp,
                        ],
                        NativeType::Date,
                    ),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["datediff".to_string()],
        }
    }
}

impl ScalarUDFImpl for SparkDateDiff {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "date_diff"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Int32, nullable)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!(
            "Apache Spark `date_diff` should have been simplified to standard subtraction"
        )
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [end, start] = take_function_args(self.name(), args)?;
        let end = end.cast_to(&DataType::Date32, info.schema())?;
        let start = start.cast_to(&DataType::Date32, info.schema())?;
        Ok(ExprSimplifyResult::Simplified(
            binary_expr(end, Operator::Minus, start)
                .cast_to(&DataType::Int32, info.schema())?,
        ))
    }
}
