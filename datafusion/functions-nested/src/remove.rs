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

//! [`ScalarUDFImpl`] definitions for array_remove, array_remove_n, array_remove_all functions.

use crate::utils;
use crate::utils::make_scalar_function;
use arrow_array::cast::AsArray;
use arrow_array::{
    new_empty_array, Array, ArrayRef, BooleanArray, GenericListArray, OffsetSizeTrait,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use datafusion_common::cast::as_int64_array;
use datafusion_common::{exec_err, Result};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_ARRAY;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::{Arc, OnceLock};

make_udf_expr_and_func!(
    ArrayRemove,
    array_remove,
    array element,
    "removes the first element from the array equal to the given value.",
    array_remove_udf
);

#[derive(Debug)]
pub(super) struct ArrayRemove {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayRemove {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element(Volatility::Immutable),
            aliases: vec!["list_remove".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayRemove {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_remove"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_remove_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_array_remove_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_array_remove_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_ARRAY)
            .with_description(
                "Removes the first element from the array equal to the given value.",
            )
            .with_syntax_example("array_remove(array, element)")
            .with_sql_example(
                r#"```sql
> select array_remove([1, 2, 2, 3, 2, 1, 4], 2);
+----------------------------------------------+
| array_remove(List([1,2,2,3,2,1,4]),Int64(2)) |
+----------------------------------------------+
| [1, 2, 3, 2, 1, 4]                           |
+----------------------------------------------+
```"#,
            )
            .with_argument(
                "array",
                "Array expression. Can be a constant, column, or function, and any combination of array operators.",
            )
            .with_argument(
                "element",
                "Element to be removed from the array.",
            )
            .build()
            .unwrap()
    })
}

make_udf_expr_and_func!(
    ArrayRemoveN,
    array_remove_n,
    array element max,
    "removes the first `max` elements from the array equal to the given value.",
    array_remove_n_udf
);

#[derive(Debug)]
pub(super) struct ArrayRemoveN {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayRemoveN {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
            aliases: vec!["list_remove_n".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayRemoveN {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_remove_n"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_remove_n_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_array_remove_n_doc())
    }
}

fn get_array_remove_n_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_ARRAY)
            .with_description(
                "Removes the first `max` elements from the array equal to the given value.",
            )
            .with_syntax_example("array_remove_n(array, element, max)")
            .with_sql_example(
                r#"```sql
> select array_remove_n([1, 2, 2, 3, 2, 1, 4], 2, 2);
+---------------------------------------------------------+
| array_remove_n(List([1,2,2,3,2,1,4]),Int64(2),Int64(2)) |
+---------------------------------------------------------+
| [1, 3, 2, 1, 4]                                         |
+---------------------------------------------------------+
```"#,
            )
            .with_argument(
                "array",
                "Array expression. Can be a constant, column, or function, and any combination of array operators.",
            )
            .with_argument(
                "element",
                "Element to be removed from the array.",
            )
            .with_argument(
                "max",
                "Number of first occurrences to remove.",
            )
            .build()
            .unwrap()
    })
}

make_udf_expr_and_func!(
    ArrayRemoveAll,
    array_remove_all,
    array element,
    "removes all elements from the array equal to the given value.",
    array_remove_all_udf
);

#[derive(Debug)]
pub(super) struct ArrayRemoveAll {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayRemoveAll {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element(Volatility::Immutable),
            aliases: vec!["list_remove_all".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayRemoveAll {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_remove_all"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_remove_all_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_array_remove_all_doc())
    }
}

fn get_array_remove_all_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_ARRAY)
            .with_description(
                "Removes all elements from the array equal to the given value.",
            )
            .with_syntax_example("array_remove_all(array, element)")
            .with_sql_example(
                r#"```sql
> select array_remove_all([1, 2, 2, 3, 2, 1, 4], 2);
+--------------------------------------------------+
| array_remove_all(List([1,2,2,3,2,1,4]),Int64(2)) |
+--------------------------------------------------+
| [1, 3, 1, 4]                                     |
+--------------------------------------------------+
```"#,
            )
            .with_argument(
                "array",
                "Array expression. Can be a constant, column, or function, and any combination of array operators.",
            )
            .with_argument(
                "element",
                "Element to be removed from the array.",
            )
            .build()
            .unwrap()
    })
}

/// Array_remove SQL function
pub fn array_remove_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_remove expects two arguments");
    }

    let arr_n = vec![1; args[0].len()];
    array_remove_internal(&args[0], &args[1], arr_n)
}

/// Array_remove_n SQL function
pub fn array_remove_n_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!("array_remove_n expects three arguments");
    }

    let arr_n = as_int64_array(&args[2])?.values().to_vec();
    array_remove_internal(&args[0], &args[1], arr_n)
}

/// Array_remove_all SQL function
pub fn array_remove_all_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_remove_all expects two arguments");
    }

    let arr_n = vec![i64::MAX; args[0].len()];
    array_remove_internal(&args[0], &args[1], arr_n)
}

fn array_remove_internal(
    array: &ArrayRef,
    element_array: &ArrayRef,
    arr_n: Vec<i64>,
) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::List(_) => {
            let list_array = array.as_list::<i32>();
            general_remove::<i32>(list_array, element_array, arr_n)
        }
        DataType::LargeList(_) => {
            let list_array = array.as_list::<i64>();
            general_remove::<i64>(list_array, element_array, arr_n)
        }
        array_type => {
            exec_err!("array_remove_all does not support type '{array_type:?}'.")
        }
    }
}

/// For each element of `list_array[i]`, removed up to `arr_n[i]` occurrences
/// of `element_array[i]`.
///
/// The type of each **element** in `list_array` must be the same as the type of
/// `element_array`. This function also handles nested arrays
/// ([`arrow_array::ListArray`] of [`arrow_array::ListArray`]s)
///
/// For example, when called to remove a list array (where each element is a
/// list of int32s, the second argument are int32 arrays, and the
/// third argument is the number of occurrences to remove
///
/// ```text
/// general_remove(
///   [1, 2, 3, 2], 2, 1    ==> [1, 3, 2]   (only the first 2 is removed)
///   [4, 5, 6, 5], 5, 2    ==> [4, 6]  (both 5s are removed)
/// )
/// ```
fn general_remove<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    element_array: &ArrayRef,
    arr_n: Vec<i64>,
) -> Result<ArrayRef> {
    let data_type = list_array.value_type();
    let mut new_values = vec![];
    // Build up the offsets for the final output array
    let mut offsets = Vec::<OffsetSize>::with_capacity(arr_n.len() + 1);
    offsets.push(OffsetSize::zero());

    // n is the number of elements to remove in this row
    for (row_index, (list_array_row, n)) in
        list_array.iter().zip(arr_n.iter()).enumerate()
    {
        match list_array_row {
            Some(list_array_row) => {
                let eq_array = utils::compare_element_to_list(
                    &list_array_row,
                    element_array,
                    row_index,
                    false,
                )?;

                // We need to keep at most first n elements as `false`, which represent the elements to remove.
                let eq_array = if eq_array.false_count() < *n as usize {
                    eq_array
                } else {
                    let mut count = 0;
                    eq_array
                        .iter()
                        .map(|e| {
                            // Keep first n `false` elements, and reverse other elements to `true`.
                            if let Some(false) = e {
                                if count < *n {
                                    count += 1;
                                    e
                                } else {
                                    Some(true)
                                }
                            } else {
                                e
                            }
                        })
                        .collect::<BooleanArray>()
                };

                let filtered_array = arrow::compute::filter(&list_array_row, &eq_array)?;
                offsets.push(
                    offsets[row_index] + OffsetSize::usize_as(filtered_array.len()),
                );
                new_values.push(filtered_array);
            }
            None => {
                // Null element results in a null row (no new offsets)
                offsets.push(offsets[row_index]);
            }
        }
    }

    let values = if new_values.is_empty() {
        new_empty_array(&data_type)
    } else {
        let new_values = new_values.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
        arrow::compute::concat(&new_values)?
    };

    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        Arc::new(Field::new("item", data_type, true)),
        OffsetBuffer::new(offsets.into()),
        values,
        list_array.nulls().cloned(),
    )?))
}
