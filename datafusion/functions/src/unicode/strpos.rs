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

use arrow::array::{
    ArrayAccessor, ArrayIter, ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray,
};
use arrow::datatypes::{ArrowNativeType, DataType, Int32Type, Int64Type};

use datafusion_common::{exec_err, Result};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::utils::{make_scalar_function, utf8_to_int_type};

#[derive(Debug)]
pub struct StrposFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for StrposFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl StrposFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Utf8]),
                    Exact(vec![Utf8, LargeUtf8]),
                    Exact(vec![LargeUtf8, Utf8]),
                    Exact(vec![LargeUtf8, LargeUtf8]),
                    Exact(vec![Utf8View, Utf8View]),
                    Exact(vec![Utf8View, Utf8]),
                    Exact(vec![Utf8View, LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("instr"), String::from("position")],
        }
    }
}

impl ScalarUDFImpl for StrposFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "strpos"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_int_type(&arg_types[0], "strpos/instr/position")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(strpos, vec![])(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn strpos(args: &[ArrayRef]) -> Result<ArrayRef> {
    match (args[0].data_type(), args[1].data_type()) {
        (DataType::Utf8, DataType::Utf8) => {
            let string_array = args[0].as_string::<i32>();
            let substring_array = args[1].as_string::<i32>();
            calculate_strpos::<_, _, Int32Type>(string_array, substring_array)
        }
        (DataType::Utf8, DataType::LargeUtf8) => {
            let string_array = args[0].as_string::<i32>();
            let substring_array = args[1].as_string::<i64>();
            calculate_strpos::<_, _, Int32Type>(string_array, substring_array)
        }
        (DataType::LargeUtf8, DataType::Utf8) => {
            let string_array = args[0].as_string::<i64>();
            let substring_array = args[1].as_string::<i32>();
            calculate_strpos::<_, _, Int64Type>(string_array, substring_array)
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let string_array = args[0].as_string::<i64>();
            let substring_array = args[1].as_string::<i64>();
            calculate_strpos::<_, _, Int64Type>(string_array, substring_array)
        }
        (DataType::Utf8View, DataType::Utf8View) => {
            let string_array = args[0].as_string_view();
            let substring_array = args[1].as_string_view();
            calculate_strpos::<_, _, Int32Type>(string_array, substring_array)
        }
        (DataType::Utf8View, DataType::Utf8) => {
            let string_array = args[0].as_string_view();
            let substring_array = args[1].as_string::<i32>();
            calculate_strpos::<_, _, Int32Type>(string_array, substring_array)
        }
        (DataType::Utf8View, DataType::LargeUtf8) => {
            let string_array = args[0].as_string_view();
            let substring_array = args[1].as_string::<i64>();
            calculate_strpos::<_, _, Int32Type>(string_array, substring_array)
        }

        other => {
            exec_err!("Unsupported data type combination {other:?} for function strpos")
        }
    }
}

/// Returns starting index of specified substring within string, or zero if it's not present. (Same as position(substring in string), but note the reversed argument order.)
/// strpos('high', 'ig') = 2
/// The implementation uses UTF-8 code points as characters
fn calculate_strpos<'a, V1, V2, T: ArrowPrimitiveType>(
    string_array: V1,
    substring_array: V2,
) -> Result<ArrayRef>
where
    V1: ArrayAccessor<Item = &'a str>,
    V2: ArrayAccessor<Item = &'a str>,
{
    let string_iter = ArrayIter::new(string_array);
    let substring_iter = ArrayIter::new(substring_array);

    let result = string_iter
        .zip(substring_iter)
        .map(|(string, substring)| match (string, substring) {
            (Some(string), Some(substring)) => {
                // The `find` method returns the byte index of the substring.
                // We count the number of chars up to that byte index.
                T::Native::from_usize(
                    string
                        .find(substring)
                        .map(|x| string[..x].chars().count() + 1)
                        .unwrap_or(0),
                )
            }
            _ => None,
        })
        .collect::<PrimitiveArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::DataType::Int32;

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::strpos::StrposFunc;
    use crate::utils::test::test_function;

    macro_rules! test_strpos {
        ($lhs:literal, $rhs:literal -> $result:literal; $t1:ident $t2:ident $t3:ident $t4:ident $t5:ident) => {
            test_function!(
                StrposFunc::new(),
                &[
                    ColumnarValue::from(ScalarValue::$t1(Some($lhs.to_owned()))),
                    ColumnarValue::from(ScalarValue::$t2(Some($rhs.to_owned()))),
                ],
                Ok(Some($result)),
                $t3,
                $t4,
                $t5
            )
        };
    }

    #[test]
    fn test_strpos_functions() {
        // Utf8 and Utf8 combinations
        test_strpos!("alphabet", "ph" -> 3; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "a" -> 1; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "z" -> 0; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "" -> 1; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("", "a" -> 0; Utf8 Utf8 i32 Int32 Int32Array);
    }
}
