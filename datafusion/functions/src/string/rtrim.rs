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

use arrow::array::{ArrayRef, OffsetSizeTrait};
use std::any::Any;

use arrow::datatypes::DataType;

use datafusion_common::{exec_err, Result};
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ColumnarValue, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};
use datafusion_physical_expr::functions::Hint;

use crate::string::common::*;
use crate::utils::{make_scalar_function, utf8_to_str_type};

/// Returns the longest string  with trailing characters removed. If the characters are not specified, whitespace is removed.
/// rtrim('testxxzx', 'xyz') = 'test'
fn rtrim<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    general_trim::<T>(args, TrimType::Right)
}

#[derive(Debug)]
pub struct RtrimFunc {
    signature: Signature,
}

impl RtrimFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![Exact(vec![Utf8]), Exact(vec![Utf8, Utf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RtrimFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "rtrim"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "rtrim")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(
                rtrim::<i32>,
                vec![Hint::Pad, Hint::AcceptsSingular],
            )(args),
            DataType::LargeUtf8 => make_scalar_function(
                rtrim::<i64>,
                vec![Hint::Pad, Hint::AcceptsSingular],
            )(args),
            other => exec_err!("Unsupported data type {other:?} for function rtrim"),
        }
    }
}
