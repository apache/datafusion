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

use datafusion_common::{Result, exec_err};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use datafusion_functions_nested::arrays_zip::{
    StructOrdinal, arrays_zip_inner, build_return_type,
};
use datafusion_functions_nested::utils::make_scalar_function;
use std::any::Any;

/// Spark-compatible `arrays_zip` function.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArraysZip {
    signature: Signature,
}

impl Default for SparkArraysZip {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArraysZip {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArraysZip {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "arrays_zip"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return exec_err!("arrays_zip requires at least two arguments");
        }

        build_return_type(arg_types, StructOrdinal::ZeroBased)
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(|arr| arrays_zip_inner(arr, StructOrdinal::ZeroBased))(
            &args.args,
        )
    }
}
