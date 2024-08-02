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

use arrow::array::{ArrayRef, AsArray, OffsetSizeTrait};
use arrow::datatypes::DataType;

use datafusion_common::{cast::as_generic_string_array, internal_err, Result};
use datafusion_expr::ColumnarValue;
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};

use crate::utils::make_scalar_function;

/// Returns true if string starts with prefix.
/// starts_with('alphabet', 'alph') = TRUE
pub fn starts_with<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let bool_result = match (args[0].data_type(), args[1].data_type()) {
        (DataType::Utf8View, DataType::Utf8View) => {
            let left = args[0].as_string_view();
            let right = args[1].as_string_view();

            let result = arrow::compute::kernels::comparison::starts_with(left, right)?;

            result
        }
        (DataType::Utf8View, DataType::Utf8 | DataType::LargeUtf8) => {
            let left = args[0].as_string_view();
            let right = as_generic_string_array::<T>(args[1].as_ref())?;

            let result = arrow::compute::kernels::comparison::starts_with(left, right)?;

            result
        }
        (DataType::Utf8 | DataType::LargeUtf8, DataType::Utf8View) => {
            let left = as_generic_string_array::<T>(args[0].as_ref())?;
            let right = args[1].as_string_view();

            let result = arrow::compute::kernels::comparison::starts_with(left, right)?;

            result
        }
        (DataType::Utf8 | DataType::LargeUtf8, DataType::Utf8 | DataType::LargeUtf8) => {
            let left = as_generic_string_array::<T>(args[0].as_ref())?;
            let right = as_generic_string_array::<T>(args[1].as_ref())?;

            let result = arrow::compute::kernels::comparison::starts_with(left, right)?;

            result
        }
        _ => internal_err!("Unsupported data types for starts_with")?,
    };

    Ok(Arc::new(bool_result) as ArrayRef)
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
        use DataType::*;

        let string_types = vec![Utf8, LargeUtf8, Utf8View];
        let mut type_signatures = vec![];

        for left in &string_types {
            for right in &string_types {
                type_signatures.push(Exact(vec![left.clone(), right.clone()]));
            }
        }

        Self {
            signature: Signature::one_of(type_signatures, Volatility::Immutable),
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
            DataType::Utf8 => make_scalar_function(starts_with::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(starts_with::<i64>, vec![])(args),
            DataType::Utf8View => make_scalar_function(starts_with::<i32>, vec![])(args),
            _ => internal_err!("Unsupported data types for starts_with")?,
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
