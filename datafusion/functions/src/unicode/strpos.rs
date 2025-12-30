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

use crate::utils::{make_scalar_function, utf8_to_int_type};
use arrow::array::{
    ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray, StringArrayType,
};
use arrow::datatypes::{
    ArrowNativeType, DataType, Field, FieldRef, Int32Type, Int64Type,
};
use datafusion_common::types::logical_string;
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignatureClass,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns the starting position of a specified substring in a string. Positions begin at 1. If the substring does not exist in the string, the function returns 0.",
    syntax_example = "strpos(str, substr)",
    alternative_syntax = "position(substr in origstr)",
    sql_example = r#"```sql
> select strpos('datafusion', 'fus');
+----------------------------------------+
| strpos(Utf8("datafusion"),Utf8("fus")) |
+----------------------------------------+
| 5                                      |
+----------------------------------------+ 
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(name = "substr", description = "Substring expression to search for.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
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
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        utf8_to_int_type(args.arg_fields[0].data_type(), "strpos/instr/position").map(
            |data_type| {
                Field::new(
                    self.name(),
                    data_type,
                    args.arg_fields.iter().any(|x| x.is_nullable()),
                )
                .into()
            },
        )
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(strpos, vec![])(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn strpos(args: &[ArrayRef]) -> Result<ArrayRef> {
    match (args[0].data_type(), args[1].data_type()) {
        (DataType::Utf8, DataType::Utf8) => {
            let string_array = args[0].as_string::<i32>();
            let substring_array = args[1].as_string::<i32>();
            calculate_strpos::<_, _, Int32Type>(&string_array, &substring_array)
        }
        (DataType::Utf8, DataType::Utf8View) => {
            let string_array = args[0].as_string::<i32>();
            let substring_array = args[1].as_string_view();
            calculate_strpos::<_, _, Int32Type>(&string_array, &substring_array)
        }
        (DataType::Utf8, DataType::LargeUtf8) => {
            let string_array = args[0].as_string::<i32>();
            let substring_array = args[1].as_string::<i64>();
            calculate_strpos::<_, _, Int32Type>(&string_array, &substring_array)
        }
        (DataType::LargeUtf8, DataType::Utf8) => {
            let string_array = args[0].as_string::<i64>();
            let substring_array = args[1].as_string::<i32>();
            calculate_strpos::<_, _, Int64Type>(&string_array, &substring_array)
        }
        (DataType::LargeUtf8, DataType::Utf8View) => {
            let string_array = args[0].as_string::<i64>();
            let substring_array = args[1].as_string_view();
            calculate_strpos::<_, _, Int64Type>(&string_array, &substring_array)
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let string_array = args[0].as_string::<i64>();
            let substring_array = args[1].as_string::<i64>();
            calculate_strpos::<_, _, Int64Type>(&string_array, &substring_array)
        }
        (DataType::Utf8View, DataType::Utf8View) => {
            let string_array = args[0].as_string_view();
            let substring_array = args[1].as_string_view();
            calculate_strpos::<_, _, Int32Type>(&string_array, &substring_array)
        }
        (DataType::Utf8View, DataType::Utf8) => {
            let string_array = args[0].as_string_view();
            let substring_array = args[1].as_string::<i32>();
            calculate_strpos::<_, _, Int32Type>(&string_array, &substring_array)
        }
        (DataType::Utf8View, DataType::LargeUtf8) => {
            let string_array = args[0].as_string_view();
            let substring_array = args[1].as_string::<i64>();
            calculate_strpos::<_, _, Int32Type>(&string_array, &substring_array)
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
    string_array: &V1,
    substring_array: &V2,
) -> Result<ArrayRef>
where
    V1: StringArrayType<'a, Item = &'a str>,
    V2: StringArrayType<'a, Item = &'a str>,
{
    let ascii_only = substring_array.is_ascii() && string_array.is_ascii();
    let string_iter = string_array.iter();
    let substring_iter = substring_array.iter();

    let result = string_iter
        .zip(substring_iter)
        .map(|(string, substring)| match (string, substring) {
            (Some(string), Some(substring)) => {
                // If only ASCII characters are present, we can use the slide window method to find
                // the sub vector in the main vector. This is faster than string.find() method.
                if ascii_only {
                    // If the substring is empty, the result is 1.
                    if substring.is_empty() {
                        T::Native::from_usize(1)
                    } else {
                        T::Native::from_usize(
                            string
                                .as_bytes()
                                .windows(substring.len())
                                .position(|w| w == substring.as_bytes())
                                .map(|x| x + 1)
                                .unwrap_or(0),
                        )
                    }
                } else {
                    // For non-ASCII, use a single-pass search that tracks both
                    // byte position and character position simultaneously
                    if substring.is_empty() {
                        return T::Native::from_usize(1);
                    }

                    let substring_bytes = substring.as_bytes();
                    let string_bytes = string.as_bytes();

                    if substring_bytes.len() > string_bytes.len() {
                        return T::Native::from_usize(0);
                    }

                    // Single pass: find substring while counting characters
                    let mut char_pos = 0;
                    for (byte_idx, _) in string.char_indices() {
                        char_pos += 1;
                        if byte_idx + substring_bytes.len() <= string_bytes.len()
                            && &string_bytes[byte_idx..byte_idx + substring_bytes.len()]
                                == substring_bytes
                        {
                            return T::Native::from_usize(char_pos);
                        }
                    }

                    T::Native::from_usize(0)
                }
            }
            _ => None,
        })
        .collect::<PrimitiveArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int32Array, Int64Array};
    use arrow::datatypes::DataType::{Int32, Int64};

    use arrow::datatypes::{DataType, Field};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::strpos::StrposFunc;
    use crate::utils::test::test_function;

    macro_rules! test_strpos {
        ($lhs:literal, $rhs:literal -> $result:literal; $t1:ident $t2:ident $t3:ident $t4:ident $t5:ident) => {
            test_function!(
                StrposFunc::new(),
                vec![
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
    fn test_strpos_functions() {
        // Utf8 and Utf8 combinations
        test_strpos!("alphabet", "ph" -> 3; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "a" -> 1; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "z" -> 0; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "" -> 1; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("", "a" -> 0; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("", "" -> 1; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("ДатаФусион数据融合📊🔥", "📊" -> 15; Utf8 Utf8 i32 Int32 Int32Array);

        // LargeUtf8 and LargeUtf8 combinations
        test_strpos!("alphabet", "ph" -> 3; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);
        test_strpos!("alphabet", "a" -> 1; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);
        test_strpos!("alphabet", "z" -> 0; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);
        test_strpos!("alphabet", "" -> 1; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);
        test_strpos!("", "a" -> 0; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);
        test_strpos!("", "" -> 1; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);
        test_strpos!("ДатаФусион数据融合📊🔥", "📊" -> 15; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);

        // Utf8 and LargeUtf8 combinations
        test_strpos!("alphabet", "ph" -> 3; Utf8 LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "a" -> 1; Utf8 LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "z" -> 0; Utf8 LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "" -> 1; Utf8 LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("", "a" -> 0; Utf8 LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("", "" -> 1; Utf8 LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("ДатаФусион数据融合📊🔥", "📊" -> 15; Utf8 LargeUtf8 i32 Int32 Int32Array);

        // LargeUtf8 and Utf8 combinations
        test_strpos!("alphabet", "ph" -> 3; LargeUtf8 Utf8 i64 Int64 Int64Array);
        test_strpos!("alphabet", "a" -> 1; LargeUtf8 Utf8 i64 Int64 Int64Array);
        test_strpos!("alphabet", "z" -> 0; LargeUtf8 Utf8 i64 Int64 Int64Array);
        test_strpos!("alphabet", "" -> 1; LargeUtf8 Utf8 i64 Int64 Int64Array);
        test_strpos!("", "a" -> 0; LargeUtf8 Utf8 i64 Int64 Int64Array);
        test_strpos!("", "" -> 1; LargeUtf8 Utf8 i64 Int64 Int64Array);
        test_strpos!("ДатаФусион数据融合📊🔥", "📊" -> 15; LargeUtf8 Utf8 i64 Int64 Int64Array);

        // Utf8View and Utf8View combinations
        test_strpos!("alphabet", "ph" -> 3; Utf8View Utf8View i32 Int32 Int32Array);
        test_strpos!("alphabet", "a" -> 1; Utf8View Utf8View i32 Int32 Int32Array);
        test_strpos!("alphabet", "z" -> 0; Utf8View Utf8View i32 Int32 Int32Array);
        test_strpos!("alphabet", "" -> 1; Utf8View Utf8View i32 Int32 Int32Array);
        test_strpos!("", "a" -> 0; Utf8View Utf8View i32 Int32 Int32Array);
        test_strpos!("", "" -> 1; Utf8View Utf8View i32 Int32 Int32Array);
        test_strpos!("ДатаФусион数据融合📊🔥", "📊" -> 15; Utf8View Utf8View i32 Int32 Int32Array);

        // Utf8View and Utf8 combinations
        test_strpos!("alphabet", "ph" -> 3; Utf8View Utf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "a" -> 1; Utf8View Utf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "z" -> 0; Utf8View Utf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "" -> 1; Utf8View Utf8 i32 Int32 Int32Array);
        test_strpos!("", "a" -> 0; Utf8View Utf8 i32 Int32 Int32Array);
        test_strpos!("", "" -> 1; Utf8View Utf8 i32 Int32 Int32Array);
        test_strpos!("ДатаФусион数据融合📊🔥", "📊" -> 15; Utf8View Utf8 i32 Int32 Int32Array);

        // Utf8View and LargeUtf8 combinations
        test_strpos!("alphabet", "ph" -> 3; Utf8View LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "a" -> 1; Utf8View LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "z" -> 0; Utf8View LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "" -> 1; Utf8View LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("", "a" -> 0; Utf8View LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("", "" -> 1; Utf8View LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("ДатаФусион数据融合📊🔥", "📊" -> 15; Utf8View LargeUtf8 i32 Int32 Int32Array);
    }

    #[test]
    fn nullable_return_type() {
        fn get_nullable(string_array_nullable: bool, substring_nullable: bool) -> bool {
            let strpos = StrposFunc::new();
            let args = datafusion_expr::ReturnFieldArgs {
                arg_fields: &[
                    Field::new("f1", DataType::Utf8, string_array_nullable).into(),
                    Field::new("f2", DataType::Utf8, substring_nullable).into(),
                ],
                scalar_arguments: &[None::<&ScalarValue>, None::<&ScalarValue>],
            };

            strpos.return_field_from_args(args).unwrap().is_nullable()
        }

        assert!(!get_nullable(false, false));

        // If any of the arguments is nullable, the result is nullable
        assert!(get_nullable(true, false));
        assert!(get_nullable(false, true));
        assert!(get_nullable(true, true));
    }
}
