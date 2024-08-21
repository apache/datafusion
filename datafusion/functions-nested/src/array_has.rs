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

//! [`ScalarUDFImpl`] definitions for array_has, array_has_all and array_has_any functions.

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, OffsetSizeTrait};
use arrow::datatypes::DataType;
use arrow::row::{RowConverter, SortField};
use datafusion_common::cast::as_generic_list_array;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use itertools::Itertools;

use crate::utils::check_datatypes;

use std::any::Any;
use std::sync::Arc;

// Create static instances of ScalarUDFs for each function
make_udf_expr_and_func!(ArrayHas,
    array_has,
    haystack_array element, // arg names
    "returns true, if the element appears in the first array, otherwise false.", // doc
    array_has_udf // internal function name
);
make_udf_expr_and_func!(ArrayHasAll,
    array_has_all,
    haystack_array needle_array, // arg names
    "returns true if each element of the second array appears in the first array; otherwise, it returns false.", // doc
    array_has_all_udf // internal function name
);
make_udf_expr_and_func!(ArrayHasAny,
    array_has_any,
    haystack_array needle_array, // arg names
    "returns true if at least one element of the second array appears in the first array; otherwise, it returns false.", // doc
    array_has_any_udf // internal function name
);

#[derive(Debug)]
pub struct ArrayHas {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayHas {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayHas {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element(Volatility::Immutable),
            aliases: vec![
                String::from("list_has"),
                String::from("array_contains"),
                String::from("list_contains"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayHas {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_has"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        if args.len() != 2 {
            return exec_err!("array_has needs two arguments");
        }

        let array_type = args[0].data_type();

        match array_type {
            DataType::List(_) => {
                array_has_internal::<i32>(&args[0], &args[1], ComparisonType::Single)
                    .map(ColumnarValue::Array)
            }
            DataType::LargeList(_) => general_array_has_dispatch::<i64>(
                &args[0],
                &args[1],
                ComparisonType::Single,
            )
            .map(ColumnarValue::Array),
            _ => exec_err!("array_has does not support type '{array_type:?}'."),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
pub struct ArrayHasAll {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayHasAll {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayHasAll {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            aliases: vec![String::from("list_has_all")],
        }
    }
}

impl ScalarUDFImpl for ArrayHasAll {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_has_all"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        if args.len() != 2 {
            return exec_err!("array_has_all needs two arguments");
        }

        let array_type = args[0].data_type();

        match array_type {
            DataType::List(_) => {
                general_array_has_dispatch::<i32>(&args[0], &args[1], ComparisonType::All)
                    .map(ColumnarValue::Array)
            }
            DataType::LargeList(_) => {
                general_array_has_dispatch::<i64>(&args[0], &args[1], ComparisonType::All)
                    .map(ColumnarValue::Array)
            }
            _ => exec_err!("array_has_all does not support type '{array_type:?}'."),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
pub struct ArrayHasAny {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayHasAny {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayHasAny {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            aliases: vec![String::from("list_has_any")],
        }
    }
}

impl ScalarUDFImpl for ArrayHasAny {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_has_any"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        if args.len() != 2 {
            return exec_err!("array_has_any needs two arguments");
        }

        let array_type = args[0].data_type();

        match array_type {
            DataType::List(_) => {
                general_array_has_dispatch::<i32>(&args[0], &args[1], ComparisonType::Any)
                    .map(ColumnarValue::Array)
            }
            DataType::LargeList(_) => {
                general_array_has_dispatch::<i64>(&args[0], &args[1], ComparisonType::Any)
                    .map(ColumnarValue::Array)
            }
            _ => exec_err!("array_has_any does not support type '{array_type:?}'."),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Represents the type of comparison for array_has.
#[derive(Debug, PartialEq)]
pub enum ComparisonType {
    // array_has_all
    All,
    // array_has_any
    Any,
    // array_has
    Single,
}

/// Public function for internal benchmark, avoid to use it in production
pub fn general_array_has_dispatch<O: OffsetSizeTrait>(
    haystack: &ArrayRef,
    needle: &ArrayRef,
    comparison_type: ComparisonType,
) -> Result<ArrayRef> {
    let array = if comparison_type == ComparisonType::Single {
        let arr = as_generic_list_array::<O>(haystack)?;
        check_datatypes("array_has", &[arr.values(), needle])?;
        arr
    } else {
        check_datatypes("array_has", &[haystack, needle])?;
        as_generic_list_array::<O>(haystack)?
    };

    let mut boolean_builder = BooleanArray::builder(array.len());

    let converter = RowConverter::new(vec![SortField::new(array.value_type())])?;

    let element = Arc::clone(needle);
    let sub_array = if comparison_type != ComparisonType::Single {
        as_generic_list_array::<O>(needle)?
    } else {
        array
    };

    for (row_idx, (arr, sub_arr)) in array.iter().zip(sub_array.iter()).enumerate() {
        match (arr, sub_arr) {
            (Some(arr), Some(sub_arr)) => {
                let arr_values = converter.convert_columns(&[arr])?;
                let sub_arr_values = if comparison_type != ComparisonType::Single {
                    converter.convert_columns(&[sub_arr])?
                } else {
                    converter.convert_columns(&[Arc::clone(&element)])?
                };

                let mut res = match comparison_type {
                    ComparisonType::All => sub_arr_values
                        .iter()
                        .dedup()
                        .all(|elem| arr_values.iter().dedup().any(|x| x == elem)),
                    ComparisonType::Any => sub_arr_values
                        .iter()
                        .dedup()
                        .any(|elem| arr_values.iter().dedup().any(|x| x == elem)),
                    ComparisonType::Single => arr_values
                        .iter()
                        .dedup()
                        .any(|x| x == sub_arr_values.row(row_idx)),
                };

                if comparison_type == ComparisonType::Any {
                    res |= res;
                }
                boolean_builder.append_value(res);
            }
            // respect null input
            (_, _) => {
                boolean_builder.append_null();
            }
        }
    }
    Ok(Arc::new(boolean_builder.finish()))
}

/// Public function for internal benchmark, avoid to use it in production
pub fn array_has_internal<O: OffsetSizeTrait>(
    haystack: &ArrayRef,
    needle: &ArrayRef,
    _comparison_type: ComparisonType,
) -> Result<ArrayRef> {
    let data_type = needle.data_type();
    if *data_type == DataType::Utf8 {
        return array_has_string_internal(haystack, needle);
    }
    general_array_has::<O>(haystack, needle)
}

fn array_has_string_internal(haystack: &ArrayRef, needle: &ArrayRef) -> Result<ArrayRef> {
    let array = as_generic_list_array::<i32>(haystack)?;
    let mut boolean_builder = BooleanArray::builder(array.len());
    let needle_array = needle.as_string::<i32>();
    for (arr, element) in array.iter().zip(needle_array.iter()) {
        match (arr, element) {
            (Some(arr), Some(element)) => {
                let string_arr = arr.as_string::<i32>();
                let mut res = false;
                for sub_arr in string_arr.iter() {
                    if let Some(sub_arr) = sub_arr {
                        if sub_arr == element {
                            res = true;
                            break;
                        }
                    }
                }
                boolean_builder.append_value(res);
            }
            (_, _) => {
                boolean_builder.append_null();
            }
        }
    }

    Ok(Arc::new(boolean_builder.finish()))
}

fn general_array_has<O: OffsetSizeTrait>(
    haystack: &ArrayRef,
    needle: &ArrayRef,
) -> Result<ArrayRef> {
    let array = as_generic_list_array::<O>(haystack)?;
    let mut boolean_builder = BooleanArray::builder(array.len());
    let converter = RowConverter::new(vec![SortField::new(array.value_type())])?;
    let sub_arr_values = converter.convert_columns(&[Arc::clone(needle)])?;

    for (row_idx, arr) in array.iter().enumerate() {
        if let Some(arr) = arr {
            let arr_values = converter.convert_columns(&[arr])?;
            let res = arr_values
                .iter()
                .dedup()
                .any(|x| x == sub_arr_values.row(row_idx));
            boolean_builder.append_value(res);
        } else {
            boolean_builder.append_null();
        }
    }

    Ok(Arc::new(boolean_builder.finish()))
}

#[cfg(test)]
mod tests {
    use std::vec;

    use arrow_array::StringArray;
    use datafusion_common::utils::array_into_list_array;
    use datafusion_expr::lit;
    use rand::Rng;

    use crate::make_array::make_array;

    use super::*;

    fn generate_random_strings(n: usize, size: usize) -> Vec<String> {
        let mut rng = rand::thread_rng();
        let mut strings = Vec::with_capacity(n);

        // Define the characters to use in the random strings
        let charset: &[u8] =
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        for _ in 0..n {
            // Generate a random string of the specified size or length 4
            let random_string: String = if rng.gen_bool(0.5) {
                (0..4)
                    .map(|_| {
                        let idx = rng.gen_range(0..charset.len());
                        charset[idx] as char
                    })
                    .collect()
            } else {
                (0..size)
                    .map(|_| {
                        let idx = rng.gen_range(0..charset.len());
                        charset[idx] as char
                    })
                    .collect()
            };

            strings.push(random_string);
        }

        strings
    }

    #[test]
    fn test_array_has_internal() {
        let data = generate_random_strings(4, 4);
        let haystack_array = make_array(
            (0..data.len())
                .map(|i| lit(data[i].clone()))
                .collect::<Vec<_>>(),
        );
        let element = lit(data[0].clone());
        println!("haystack_array: {:?}", haystack_array);
        println!("element: {:?}", element);
        let result = array_has_udf().call(vec![haystack_array, element]);
        assert_eq!(result, lit(true));
    }

    #[test]
    fn test_array_has_string_internal() {
        let array =
            Arc::new(StringArray::from(vec!["abcd", "efgh", "ijkl", "mnop"])) as ArrayRef;
        let array = Arc::new(array_into_list_array(array, true)) as ArrayRef;

        let sub_array = Arc::new(StringArray::from(vec!["abcd"])) as ArrayRef;

        let result =
            array_has_internal::<i32>(&array, &sub_array, ComparisonType::Single)
                .unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true])) as ArrayRef;
        assert_eq!(&result, &expected);
    }
}
