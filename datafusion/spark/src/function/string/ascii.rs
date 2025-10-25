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
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::string::ascii::ascii;
use datafusion_functions::utils::make_scalar_function;
use std::any::Any;

/// Spark compatible version of the [ascii] function. Differs from the [default ascii function]
/// in that it is more permissive of input types, for example casting numeric input to string
/// before executing the function (default version doesn't allow numeric input).
///
/// [ascii]: https://spark.apache.org/docs/latest/api/sql/index.html#ascii
/// [default ascii function]: datafusion_functions::string::ascii::AsciiFunc
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkAscii {
    signature: Signature,
}

impl Default for SparkAscii {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkAscii {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkAscii {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ascii"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(ascii, vec![])(&args.args)
    }

    fn coerce_types(&self, _arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(vec![DataType::Utf8])
    }
}
