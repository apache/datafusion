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

use crate::utils::make_scalar_function;
use arrow::array::Int32Array;
use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct AsciiFunc {
    signature: Signature,
}

impl Default for AsciiFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AsciiFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![Utf8, LargeUtf8, Utf8View],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for AsciiFunc {
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
        use DataType::*;

        Ok(Int32)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(ascii, vec![])(args)
    }
}

fn calculate_ascii<'a, I>(string_array: I) -> Result<ArrayRef>
where
    I: IntoIterator<Item = Option<&'a str>>,
{
    let result = string_array
        .into_iter()
        .map(|string| {
            string.map(|s| {
                let mut chars = s.chars();
                chars.next().map_or(0, |v| v as i32)
            })
        })
        .collect::<Int32Array>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns the numeric code of the first character of the argument.
pub fn ascii(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = args[0].as_string::<i32>();
            calculate_ascii(string_array.iter())
        }
        DataType::LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            calculate_ascii(string_array.iter())
        }
        DataType::Utf8View => {
            let string_array = args[0].as_string_view();
            calculate_ascii(string_array.iter())
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use crate::string::ascii::AsciiFunc;
    use crate::utils::test::test_function;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::DataType::Int32;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    macro_rules! test_ascii {
        ($INPUT:expr, $EXPECTED:expr) => {
            test_function!(
                AsciiFunc::new(),
                &[ColumnarValue::Scalar(ScalarValue::Utf8($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );

            test_function!(
                AsciiFunc::new(),
                &[ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );

            test_function!(
                AsciiFunc::new(),
                &[ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );
        };
    }

    #[test]
    fn test_functions() -> Result<()> {
        test_ascii!(Some(String::from("x")), Ok(Some(120)));
        test_ascii!(Some(String::from("a")), Ok(Some(97)));
        test_ascii!(Some(String::from("")), Ok(Some(0)));
        test_ascii!(None, Ok(None));
        Ok(())
    }
}
