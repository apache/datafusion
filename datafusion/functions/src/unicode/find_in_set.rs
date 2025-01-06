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
    ArrayAccessor, ArrayIter, ArrayRef, ArrowPrimitiveType, AsArray, OffsetSizeTrait,
    PrimitiveArray,
};
use arrow::datatypes::{ArrowNativeType, DataType, Int32Type, Int64Type};

use crate::utils::{make_scalar_function, utf8_to_int_type};
use datafusion_common::{exec_err, Result};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns a value in the range of 1 to N if the string str is in the string list strlist consisting of N substrings.",
    syntax_example = "find_in_set(str, strlist)",
    sql_example = r#"```sql
> select find_in_set('b', 'a,b,c,d');
+----------------------------------------+
| find_in_set(Utf8("b"),Utf8("a,b,c,d")) |
+----------------------------------------+
| 2                                      |
+----------------------------------------+ 
```"#,
    argument(name = "str", description = "String expression to find in strlist."),
    argument(
        name = "strlist",
        description = "A string list is a string composed of substrings separated by , characters."
    )
)]
#[derive(Debug)]
pub struct FindInSetFunc {
    signature: Signature,
}

impl Default for FindInSetFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl FindInSetFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8View, Utf8View]),
                    Exact(vec![Utf8, Utf8]),
                    Exact(vec![LargeUtf8, LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for FindInSetFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "find_in_set"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_int_type(&arg_types[0], "find_in_set")
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        make_scalar_function(find_in_set, vec![])(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

///Returns a value in the range of 1 to N if the string str is in the string list strlist consisting of N substrings
///A string list is a string composed of substrings separated by , characters.
fn find_in_set(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!(
            "find_in_set was called with {} arguments. It requires 2.",
            args.len()
        );
    }
    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = args[0].as_string::<i32>();
            let str_list_array = args[1].as_string::<i32>();
            find_in_set_general::<Int32Type, _>(string_array, str_list_array)
        }
        DataType::LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            let str_list_array = args[1].as_string::<i64>();
            find_in_set_general::<Int64Type, _>(string_array, str_list_array)
        }
        DataType::Utf8View => {
            let string_array = args[0].as_string_view();
            let str_list_array = args[1].as_string_view();
            find_in_set_general::<Int32Type, _>(string_array, str_list_array)
        }
        other => {
            exec_err!("Unsupported data type {other:?} for function find_in_set")
        }
    }
}

pub fn find_in_set_general<'a, T, V>(
    string_array: V,
    str_list_array: V,
) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: OffsetSizeTrait,
    V: ArrayAccessor<Item = &'a str>,
{
    let string_iter = ArrayIter::new(string_array);
    let str_list_iter = ArrayIter::new(str_list_array);

    let mut builder = PrimitiveArray::<T>::builder(string_iter.len());

    string_iter
        .zip(str_list_iter)
        .for_each(
            |(string_opt, str_list_opt)| match (string_opt, str_list_opt) {
                (Some(string), Some(str_list)) => {
                    let position = str_list
                        .split(',')
                        .position(|s| s == string)
                        .map_or(0, |idx| idx + 1);
                    builder.append_value(T::Native::from_usize(position).unwrap());
                }
                _ => builder.append_null(),
            },
        );

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use crate::unicode::find_in_set::FindInSetFunc;
    use crate::utils::test::test_function;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::DataType::Int32;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b,c")))),
            ],
            Ok(Some(1)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("🔥")))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "a,Д,🔥"
                )))),
            ],
            Ok(Some(3)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("d")))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("a,b,c")))),
            ],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "Apache Software Foundation"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "Github,Apache Software Foundation,DataFusion"
                )))),
            ],
            Ok(Some(2)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b,c")))),
            ],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("")))),
            ],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("a")))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
            ],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("a,b,c")))),
            ],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );

        Ok(())
    }
}
