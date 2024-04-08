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

use arrow::array::Float64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Float64;

use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, FuncMonotonicity, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};

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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if !matches!(&args[0], ColumnarValue::Array(_)) {
            return exec_err!("Expect pi function to take no param");
        }
        let array = Float64Array::from_value(std::f64::consts::PI, 1);
        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn monotonicity(&self) -> Result<Option<FuncMonotonicity>> {
        Ok(Some(vec![Some(true)]))
    }
}
