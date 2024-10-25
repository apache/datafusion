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
use std::sync::OnceLock;

use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Float64;
use datafusion_common::{not_impl_err, Result, ScalarValue};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_MATH;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug)]
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
            signature: Signature::exact(vec![], Volatility::Immutable),
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

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        not_impl_err!("{} function does not accept arguments", self.name())
    }

    fn invoke_no_args(&self, _number_rows: usize) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(
            std::f64::consts::PI,
        ))))
    }

    fn output_ordering(&self, _input: &[ExprProperties]) -> Result<SortProperties> {
        // This function returns a constant value.
        Ok(SortProperties::Singleton)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_pi_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_pi_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_MATH)
            .with_description("Returns an approximate value of π.")
            .with_syntax_example("pi()")
            .build()
            .unwrap()
    })
}
