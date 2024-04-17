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

use crate::utils::{make_scalar_function, utf8_to_int_type};
use arrow::array::{
    ArrayRef, ArrowPrimitiveType, GenericStringArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow::datatypes::{ArrowNativeType, DataType, Int32Type, Int64Type};
use datafusion_common::cast::as_generic_string_array;
use datafusion_common::exec_err;
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct CharacterLengthFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl CharacterLengthFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![Utf8, LargeUtf8],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("length"), String::from("char_length")],
        }
    }
}

impl ScalarUDFImpl for CharacterLengthFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "character_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_int_type(&arg_types[0], "character_length")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(character_length::<Int32Type>, vec![])(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(character_length::<Int64Type>, vec![])(args)
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function character_length")
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Returns number of characters in the string.
/// character_length('josé') = 4
/// The implementation counts UTF-8 code points to count the number of characters
fn character_length<T: ArrowPrimitiveType>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    T::Native: OffsetSizeTrait,
{
    let string_array: &GenericStringArray<T::Native> =
        as_generic_string_array::<T::Native>(&args[0])?;

    let result = string_array
        .iter()
        .map(|string| {
            string.map(|string: &str| {
                T::Native::from_usize(string.chars().count())
                    .expect("should not fail as string.chars will always return integer")
            })
        })
        .collect::<PrimitiveArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use crate::unicode::character_length::CharacterLengthFunc;
    use crate::utils::test::test_function;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::DataType::Int32;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    #[test]
    fn test_functions() -> Result<()> {
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            CharacterLengthFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from("chars")
            )))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            CharacterLengthFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from("josé")
            )))],
            Ok(Some(4)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            CharacterLengthFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from("")
            )))],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            CharacterLengthFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8(None))],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            CharacterLengthFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("josé"))))],
            internal_err!(
                "function character_length requires compilation with feature flag: unicode_expressions."
            ),
            i32,
            Int32,
            Int32Array
        );

        Ok(())
    }
}
