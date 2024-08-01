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

use arrow::array::{ArrayRef, OffsetSizeTrait};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Boolean;

use datafusion_common::cast::as_generic_string_array;
use datafusion_common::{exec_err, Result};
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ColumnarValue, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};

use crate::utils::make_scalar_function;

#[derive(Debug)]
pub struct EndsWithFunc {
    signature: Signature,
}

impl Default for EndsWithFunc {
    fn default() -> Self {
        EndsWithFunc::new()
    }
}

impl EndsWithFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Utf8]),
                    Exact(vec![Utf8, LargeUtf8]),
                    Exact(vec![LargeUtf8, Utf8]),
                    Exact(vec![LargeUtf8, LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EndsWithFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ends_with"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(ends_with::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(ends_with::<i64>, vec![])(args),
            other => {
                exec_err!("Unsupported data type {other:?} for function ends_with")
            }
        }
    }
}

/// Returns true if string ends with suffix.
/// ends_with('alphabet', 'abet') = 't'
pub fn ends_with<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let left = as_generic_string_array::<T>(&args[0])?;
    let right = as_generic_string_array::<T>(&args[1])?;

    let result = arrow::compute::kernels::comparison::ends_with(left, right)?;

    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, BooleanArray};
    use arrow::datatypes::DataType::Boolean;

    use datafusion_common::Result;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::string::ends_with::EndsWithFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            EndsWithFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from("alph")),
            ],
            Ok(Some(false)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            EndsWithFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from("bet")),
            ],
            Ok(Some(true)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            EndsWithFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::from("alph")),
            ],
            Ok(None),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            EndsWithFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            bool,
            Boolean,
            BooleanArray
        );

        Ok(())
    }
}
