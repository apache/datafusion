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

use arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

#[derive(Debug)]
pub struct ArrowTypeOfFunc {
    signature: Signature,
}

impl Default for ArrowTypeOfFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrowTypeOfFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrowTypeOfFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "arrow_typeof"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "arrow_typeof function requires 1 arguments, got {}",
                args.len()
            );
        }

        let input_data_type = args[0].data_type();
        Ok(ColumnarValue::Scalar(ScalarValue::from(format!(
            "{input_data_type}"
        ))))
    }
}
