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

use datafusion_common::cast::{as_generic_string_array, as_int64_array};
use datafusion_common::{exec_err, Result};
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ColumnarValue, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};

use crate::utils::{make_scalar_function, utf8_to_str_type};

#[derive(Debug)]
pub struct RepeatFunc {
    signature: Signature,
}

impl Default for RepeatFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RepeatFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![Exact(vec![Utf8, Int64]), Exact(vec![LargeUtf8, Int64])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RepeatFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "repeat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "repeat")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(repeat::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(repeat::<i64>, vec![])(args),
            other => exec_err!("Unsupported data type {other:?} for function repeat"),
        }
    }
}

/// Repeats string the specified number of times.
/// repeat('Pg', 4) = 'PgPgPgPg'
fn repeat<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let number_array = as_int64_array(&args[1])?;

    let result = string_array
        .iter()
        .zip(number_array.iter())
        .map(|(string, number)| match (string, number) {
            (Some(string), Some(number)) if number >= 0 => {
                Some(string.repeat(number as usize))
            }
            (Some(_), Some(_)) => Some("".to_string()),
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::Result;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::string::repeat::RepeatFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            RepeatFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("Pg")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            Ok(Some("PgPgPgPg")),
            &str,
            Utf8,
            StringArray
        );

        test_function!(
            RepeatFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RepeatFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("Pg")))),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
