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
    ArrayRef, ArrowPrimitiveType, GenericStringArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow::datatypes::{ArrowNativeType, DataType, Int32Type, Int64Type};

use datafusion_common::cast::as_generic_string_array;
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
        match (args[0].data_type(), args[1].data_type()) {
            (DataType::Utf8, DataType::Utf8) => {
                make_scalar_function(strpos::<Int32Type, Int32Type>, vec![])(args)
            }
            (DataType::Utf8, DataType::LargeUtf8) => {
                make_scalar_function(strpos::<Int32Type, Int64Type>, vec![])(args)
            }
            (DataType::LargeUtf8, DataType::Utf8) => {
                make_scalar_function(strpos::<Int64Type, Int32Type>, vec![])(args)
            }
            (DataType::LargeUtf8, DataType::LargeUtf8) => {
                make_scalar_function(strpos::<Int64Type, Int64Type>, vec![])(args)
            }
            other => exec_err!("Unsupported data type {other:?} for function strpos"),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Returns starting index of specified substring within string, or zero if it's not present. (Same as position(substring in string), but note the reversed argument order.)
/// strpos('high', 'ig') = 2
/// The implementation uses UTF-8 code points as characters
fn strpos<T0: ArrowPrimitiveType, T1: ArrowPrimitiveType>(
    args: &[ArrayRef],
) -> Result<ArrayRef>
where
    T0::Native: OffsetSizeTrait,
    T1::Native: OffsetSizeTrait,
{
    let string_array: &GenericStringArray<T0::Native> =
        as_generic_string_array::<T0::Native>(&args[0])?;

    let substring_array: &GenericStringArray<T1::Native> =
        as_generic_string_array::<T1::Native>(&args[1])?;

    let result = string_array
        .iter()
        .zip(substring_array.iter())
        .map(|(string, substring)| match (string, substring) {
            (Some(string), Some(substring)) => {
                // the find method returns the byte index of the substring
                // Next, we count the number of the chars until that byte
                T0::Native::from_usize(
                    string
                        .find(substring)
                        .map(|x| string[..x].chars().count() + 1)
                        .unwrap_or(0),
                )
            }
            _ => None,
        })
        .collect::<PrimitiveArray<T0>>();

    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::utils::test::test_function;
    use arrow::{
        array::{Array as _, Int32Array, Int64Array},
        datatypes::DataType::{Int32, Int64},
    };
    use datafusion_common::ScalarValue;

    macro_rules! test_strpos {
        ($lhs:literal, $rhs:literal -> $result:literal; $t1:ident $t2:ident $t3:ident $t4:ident $t5:ident) => {
            test_function!(
                StrposFunc::new(),
                &[
                    ColumnarValue::Scalar(ScalarValue::$t1(Some($lhs.to_owned()))),
                    ColumnarValue::Scalar(ScalarValue::$t2(Some($rhs.to_owned()))),
                ],
                Ok(Some($result)),
                $t3,
                $t4,
                $t5
            )
        };
    }

    #[test]
    fn strpos() {
        test_strpos!("foo", "bar" -> 0; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("foobar", "foo" -> 1; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("foobar", "bar" -> 4; Utf8 Utf8 i32 Int32 Int32Array);

        test_strpos!("foo", "bar" -> 0; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);
        test_strpos!("foobar", "foo" -> 1; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);
        test_strpos!("foobar", "bar" -> 4; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);

        test_strpos!("foo", "bar" -> 0; Utf8 LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("foobar", "foo" -> 1; Utf8 LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("foobar", "bar" -> 4; Utf8 LargeUtf8 i32 Int32 Int32Array);

        test_strpos!("foo", "bar" -> 0; LargeUtf8 Utf8 i64 Int64 Int64Array);
        test_strpos!("foobar", "foo" -> 1; LargeUtf8 Utf8 i64 Int64 Int64Array);
        test_strpos!("foobar", "bar" -> 4; LargeUtf8 Utf8 i64 Int64 Int64Array);
    }
}
