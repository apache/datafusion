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
use arrow::datatypes::DataType;
use datafusion_common::exec_err;
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

use crate::string::{make_scalar_function, utf8_to_str_type};

use super::{general_trim, TrimType};

/// Returns the longest string  with leading and trailing characters removed. If the characters are not specified, whitespace is removed.
/// btrim('xyxtrimyyx', 'xyz') = 'trim'
pub fn btrim<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    general_trim::<T>(args, TrimType::Both)
}

#[derive(Debug)]
pub(super) struct TrimFunc {
    signature: Signature,
}

impl TrimFunc {
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

impl ScalarUDFImpl for TrimFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "trim"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "trim")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(btrim::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(btrim::<i64>, vec![])(args),
            other => exec_err!("Unsupported data type {other:?} for function trim"),
        }
    }
}
