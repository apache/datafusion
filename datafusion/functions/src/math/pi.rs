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

use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Float64;
use datafusion_common::{assert_or_internal_err, Result, ScalarValue};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns an approximate value of π.",
    syntax_example = "pi()"
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PiFunc {
    signature: Signature,
}

impl Default for PiFunc {
    fn default() -> Self {
        PiFunc::new()
    }
}

impl PiFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for PiFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "pi"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        assert_or_internal_err!(
            args.args.is_empty(),
            "{} function does not accept arguments",
            self.name()
        );
        Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(
            std::f64::consts::PI,
        ))))
    }

    fn output_ordering(&self, _input: &[ExprProperties]) -> Result<SortProperties> {
        // This function returns a constant value.
        Ok(SortProperties::Singleton)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
