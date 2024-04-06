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

use crate::string::common::case_conversion;
use crate::utils::utf8_to_str_type;
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

#[derive(Debug)]
pub struct UpperFunc {
    signature: Signature,
}

impl UpperFunc {
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

impl ScalarUDFImpl for UpperFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "upper"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "upper")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        case_conversion(args, |string| string.to_uppercase(), "upper")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, StringArray};
    use std::sync::Arc;

    #[test]
    fn upper() -> Result<()> {
        let string_array = Arc::new(StringArray::from(vec![
            Some("arrow"),
            None,
            Some("datafusion"),
            Some("@_"),
            Some("0123456789"),
            None,
            Some(""),
            Some("\t\n"),
        ])) as ArrayRef;

        let args = vec![ColumnarValue::Array(string_array)];
        let result =
            match case_conversion(&args, |string| string.to_uppercase(), "upper")? {
                ColumnarValue::Array(result) => result,
                _ => unreachable!(),
            };

        let expected = Arc::new(StringArray::from(vec![
            Some("ARROW"),
            None,
            Some("DATAFUSION"),
            Some("@_"),
            Some("0123456789"),
            None,
            Some(""),
            Some("\t\n"),
        ])) as ArrayRef;

        assert_eq!(&expected, &result);

        Ok(())
    }
}
