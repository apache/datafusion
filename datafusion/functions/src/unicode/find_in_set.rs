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
use std::sync::{Arc, OnceLock};

use arrow::array::{
    ArrayAccessor, ArrayIter, ArrayRef, ArrowPrimitiveType, AsArray, OffsetSizeTrait,
    PrimitiveArray,
};
use arrow::datatypes::{ArrowNativeType, DataType, Int32Type, Int64Type};

use crate::utils::{make_scalar_function, utf8_to_int_type};
use datafusion_common::{exec_err, Result};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_STRING;
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};

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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(find_in_set, vec![])(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_find_in_set_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_find_in_set_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_STRING)
            .with_description("Returns a value in the range of 1 to N if the string str is in the string list strlist consisting of N substrings.")
            .with_syntax_example("find_in_set(str, strlist)")
            .with_sql_example(r#"```sql
> select find_in_set('b', 'a,b,c,d');
+----------------------------------------+
| find_in_set(Utf8("b"),Utf8("a,b,c,d")) |
+----------------------------------------+
| 2                                      |
+----------------------------------------+ 
```"#)
            .with_argument("str", "String expression to find in strlist.")
            .with_argument("strlist", "A string list is a string composed of substrings separated by , characters.")
            .build()
            .unwrap()
    })
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

pub fn find_in_set_general<'a, T: ArrowPrimitiveType, V: ArrayAccessor<Item = &'a str>>(
    string_array: V,
    str_list_array: V,
) -> Result<ArrayRef>
where
    T::Native: OffsetSizeTrait,
{
    let string_iter = ArrayIter::new(string_array);
    let str_list_iter = ArrayIter::new(str_list_array);
    let result = string_iter
        .zip(str_list_iter)
        .map(|(string, str_list)| match (string, str_list) {
            (Some(string), Some(str_list)) => {
                let mut res = 0;
                let str_set: Vec<&str> = str_list.split(',').collect();
                for (idx, str) in str_set.iter().enumerate() {
                    if str == &string {
                        res = idx + 1;
                        break;
                    }
                }
                T::Native::from_usize(res)
            }
            _ => None,
        })
        .collect::<PrimitiveArray<T>>();
    Ok(Arc::new(result) as ArrayRef)
}
