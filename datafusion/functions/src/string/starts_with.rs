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

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use datafusion_common::{internal_err, Result};
use datafusion_expr::ColumnarValue;
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};

use crate::utils::make_scalar_function;

/// Returns true if string starts with prefix.
/// starts_with('alphabet', 'alph') = 't'
pub fn starts_with(args: &[ArrayRef]) -> Result<ArrayRef> {
    let result = arrow::compute::kernels::comparison::starts_with(&args[0], &args[1])?;
    Ok(Arc::new(result) as ArrayRef)
}

#[derive(Debug)]
pub struct StartsWithFunc {
    signature: Signature,
}

impl Default for StartsWithFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl StartsWithFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // Planner attempts coercion to the target type starting with the most preferred candidate.
                    // For example, given input `(Utf8View, Utf8)`, it first tries coercing to `(Utf8View, Utf8View)`.
                    // If that fails, it proceeds to `(Utf8, Utf8)`.
                    Exact(vec![DataType::Utf8View, DataType::Utf8View]),
                    Exact(vec![DataType::Utf8, DataType::Utf8]),
                    Exact(vec![DataType::LargeUtf8, DataType::LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StartsWithFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "starts_with"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8View | DataType::Utf8 | DataType::LargeUtf8 => {
                make_scalar_function(starts_with, vec![])(args)
            }
            _ => internal_err!("Unsupported data types for starts_with. Expected Utf8, LargeUtf8 or Utf8View")?,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::test::test_function;
    use arrow::array::{Array, BooleanArray};
    use arrow::datatypes::DataType::Boolean;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use super::*;

    #[test]
    fn test_functions() -> Result<()> {
        // Generate test cases for starts_with
        let test_cases = vec![
            (Some("alphabet"), Some("alph"), Some(true)),
            (Some("alphabet"), Some("bet"), Some(false)),
            (
                Some("somewhat large string"),
                Some("somewhat large"),
                Some(true),
            ),
            (Some("somewhat large string"), Some("large"), Some(false)),
        ]
        .into_iter()
        .flat_map(|(a, b, c)| {
            let utf_8_args = vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(a.map(|s| s.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(b.map(|s| s.to_string()))),
            ];

            let large_utf_8_args = vec![
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(a.map(|s| s.to_string()))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(b.map(|s| s.to_string()))),
            ];

            let utf_8_view_args = vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(a.map(|s| s.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(b.map(|s| s.to_string()))),
            ];

            vec![(utf_8_args, c), (large_utf_8_args, c), (utf_8_view_args, c)]
        });

        for (args, expected) in test_cases {
            test_function!(
                StartsWithFunc::new(),
                &args,
                Ok(expected),
                bool,
                Boolean,
                BooleanArray
            );
        }

        Ok(())
    }
}
