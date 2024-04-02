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

use arrow::array::{ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow::datatypes::DataType;

use datafusion_common::cast::as_generic_string_array;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::utils::{make_scalar_function, utf8_to_str_type};

#[derive(Debug)]
pub struct ReverseFunc {
    signature: Signature,
}

impl ReverseFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![Utf8, LargeUtf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ReverseFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "reverse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "reverse")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(reverse::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(reverse::<i64>, vec![])(args),
            other => {
                exec_err!("Unsupported data type {other:?} for function reverse")
            }
        }
    }
}

/// Reverses the order of the characters in the string.
/// reverse('abcde') = 'edcba'
/// The implementation uses UTF-8 code points as characters
pub fn reverse<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;

    let result = string_array
        .iter()
        .map(|string| string.map(|string: &str| string.chars().rev().collect::<String>()))
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::reverse::ReverseFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            ReverseFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::from("abcde"))],
            Ok(Some("edcba")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ReverseFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::from("loẅks"))],
            Ok(Some("sk̈wol")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ReverseFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::from("loẅks"))],
            Ok(Some("sk̈wol")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ReverseFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            ReverseFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::from("abcde"))],
            internal_err!(
                "function reverse requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
