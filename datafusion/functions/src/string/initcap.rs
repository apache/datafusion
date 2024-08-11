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

use arrow::array::{ArrayRef, GenericStringArray, OffsetSizeTrait, StringArray};
use arrow::datatypes::DataType;

use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};

use crate::utils::{make_scalar_function, utf8_to_str_type};

#[derive(Debug)]
pub struct InitcapFunc {
    signature: Signature,
}

impl Default for InitcapFunc {
    fn default() -> Self {
        InitcapFunc::new()
    }
}

impl InitcapFunc {
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

impl ScalarUDFImpl for InitcapFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "initcap"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "initcap")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(initcap::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(initcap::<i64>, vec![])(args),
            DataType::Utf8View => make_scalar_function(initcap_utf8view, vec![])(args),
            other => {
                exec_err!("Unsupported data type {other:?} for function initcap")
            }
        }
    }
}

/// Converts the first letter of each word to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.
/// initcap('hi THOMAS') = 'Hi Thomas'
fn initcap<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;

    // first map is the iterator, second is for the `Option<_>`
    let result = string_array
        .iter()
        .map(initcap_string)
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

fn initcap_utf8view(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_view_array = as_string_view_array(&args[0])?;

    let result = string_view_array
        .iter()
        .map(initcap_string)
        .collect::<StringArray>();

    Ok(Arc::new(result) as ArrayRef)
}

fn initcap_string(string: Option<&str>) -> Option<String> {
    let mut char_vector = Vec::<char>::new();
    string.map(|string: &str| {
        char_vector.clear();
        let mut previous_character_letter_or_number = false;
        for c in string.chars() {
            if previous_character_letter_or_number {
                char_vector.push(c.to_ascii_lowercase());
            } else {
                char_vector.push(c.to_ascii_uppercase());
            }
            previous_character_letter_or_number =
                c.is_ascii_uppercase() || c.is_ascii_lowercase() || c.is_ascii_digit();
        }
        char_vector.iter().collect::<String>()
    })
}

#[cfg(test)]
mod tests {
    use crate::string::initcap::InitcapFunc;
    use crate::utils::test::test_function;
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            InitcapFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::from("hi THOMAS"))],
            Ok(Some("Hi Thomas")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitcapFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::from(""))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitcapFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::from(""))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitcapFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitcapFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                "hi THOMAS".to_string()
            )))],
            Ok(Some("Hi Thomas")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitcapFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                "hi THOMAS wIth M0re ThAN 12 ChaRs".to_string()
            )))],
            Ok(Some("Hi Thomas With M0re Than 12 Chars")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitcapFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                "".to_string()
            )))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitcapFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8View(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
