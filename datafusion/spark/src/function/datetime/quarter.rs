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

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::types::{NativeType, logical_date, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Coercion, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_functions::datetime::date_part;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkQuarter {
    signature: Signature,
}

impl Default for SparkQuarter {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkQuarter {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Timestamp,
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Native(logical_date()),
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_date()),
                        vec![TypeSignatureClass::Native(logical_string())],
                        NativeType::Date,
                    )]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkQuarter {
    fn name(&self) -> &str {
        "quarter"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Int32, true)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("spark quarter should have been simplified to standard date_part")
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [date_expr] = take_function_args(self.name(), args)?;
        let part_expr = Expr::Literal(ScalarValue::new_utf8("quarter"), None);

        let date_part_expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            date_part(),
            vec![part_expr, date_expr],
        ));
        Ok(ExprSimplifyResult::Simplified(date_part_expr))
    }
}
