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
use std::sync::{Arc, OnceLock};

use arrow::array::Float64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Float64;
use rand::{thread_rng, Rng};

use datafusion_common::{not_impl_err, Result};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_MATH;
use datafusion_expr::ColumnarValue;
use datafusion_expr::{Documentation, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct RandomFunc {
    signature: Signature,
}

impl Default for RandomFunc {
    fn default() -> Self {
        RandomFunc::new()
    }
}

impl RandomFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for RandomFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "random"
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

    fn invoke_no_args(&self, num_rows: usize) -> Result<ColumnarValue> {
        let mut rng = thread_rng();
        let mut values = vec![0.0; num_rows];
        // Equivalent to set each element with rng.gen_range(0.0..1.0), but more efficient
        rng.fill(&mut values[..]);
        let array = Float64Array::from(values);

        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_random_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_random_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_MATH)
            .with_description(
                r#"Returns a random float value in the range [0, 1).
The random seed is unique to each row."#,
            )
            .with_syntax_example("random()")
            .build()
            .unwrap()
    })
}
