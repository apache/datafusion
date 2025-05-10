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

//! [`ScalarUDFImpl`] definitions for array_sort function.

use crate::utils::make_scalar_function;
use arrow::array::{new_null_array, Array, ArrayRef, ListArray, NullBufferBuilder};
use arrow::buffer::OffsetBuffer;
use arrow::compute::SortColumn;
use arrow::datatypes::{DataType, Field};
use arrow::{compute, compute::SortOptions};
use datafusion_common::cast::{as_list_array, as_string_array};
use datafusion_common::utils::ListCoercion;
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, Documentation,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArraySort,
    array_sort,
    array desc null_first,
    "returns sorted array.",
    array_sort_udf
);

/// Implementation of `array_sort` function
///
/// `array_sort` sorts the elements of an array
///
/// # Example
///
/// `array_sort([3, 1, 2])` returns `[1, 2, 3]`
#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Sort array.",
    syntax_example = "array_sort(array, desc, nulls_first)",
    sql_example = r#"```sql
> select array_sort([3, 1, 2]);
+-----------------------------+
| array_sort(List([3,1,2]))   |
+-----------------------------+
| [1, 2, 3]                   |
+-----------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "desc",
        description = "Whether to sort in descending order(`ASC` or `DESC`)."
    ),
    argument(
        name = "nulls_first",
        description = "Whether to sort nulls first(`NULLS FIRST` or `NULLS LAST`)."
    )
)]
#[derive(Debug)]
pub struct ArraySort {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArraySort {
    fn default() -> Self {
        Self::new()
    }
}

impl ArraySort {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![ArrayFunctionArgument::Array],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    }),
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Array,
                            ArrayFunctionArgument::String,
                        ],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    }),
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Array,
                            ArrayFunctionArgument::String,
                            ArrayFunctionArgument::String,
                        ],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    }),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["list_sort".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArraySort {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_sort"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Null => Ok(DataType::Null),
            DataType::List(field) => {
                Ok(DataType::new_list(field.data_type().clone(), true))
            }
            arg_type => {
                plan_err!("{} does not support type {arg_type}", self.name())
            }
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_sort_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Array_sort SQL function
pub fn array_sort_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() || args.len() > 3 {
        return exec_err!("array_sort expects one to three arguments");
    }

    if args[0].data_type().is_null() {
        return Ok(Arc::clone(&args[0]));
    }

    let list_array = as_list_array(&args[0])?;
    let row_count = list_array.len();
    if row_count == 0 || list_array.value_type().is_null() {
        return Ok(Arc::clone(&args[0]));
    }

    if args[1..].iter().any(|array| array.is_null(0)) {
        return Ok(new_null_array(args[0].data_type(), args[0].len()));
    }

    let sort_option = match args.len() {
        1 => None,
        2 => {
            let sort = as_string_array(&args[1])?.value(0);
            Some(SortOptions {
                descending: order_desc(sort)?,
                nulls_first: true,
            })
        }
        3 => {
            let sort = as_string_array(&args[1])?.value(0);
            let nulls_first = as_string_array(&args[2])?.value(0);
            Some(SortOptions {
                descending: order_desc(sort)?,
                nulls_first: order_nulls_first(nulls_first)?,
            })
        }
        _ => return exec_err!("array_sort expects 1 to 3 arguments"),
    };

    let mut array_lengths = vec![];
    let mut arrays = vec![];
    let mut valid = NullBufferBuilder::new(row_count);
    for i in 0..row_count {
        if list_array.is_null(i) {
            array_lengths.push(0);
            valid.append_null();
        } else {
            let arr_ref = list_array.value(i);

            // arrow sort kernel does not support Structs, so use
            // lexsort_to_indices instead:
            // https://github.com/apache/arrow-rs/issues/6911#issuecomment-2562928843
            let sorted_array = match arr_ref.data_type() {
                DataType::Struct(_) => {
                    let sort_columns: Vec<SortColumn> = vec![SortColumn {
                        values: Arc::clone(&arr_ref),
                        options: sort_option,
                    }];
                    let indices = compute::lexsort_to_indices(&sort_columns, None)?;
                    compute::take(arr_ref.as_ref(), &indices, None)?
                }
                _ => {
                    let arr_ref = arr_ref.as_ref();
                    compute::sort(arr_ref, sort_option)?
                }
            };
            array_lengths.push(sorted_array.len());
            arrays.push(sorted_array);
            valid.append_non_null();
        }
    }

    // Assume all arrays have the same data type
    let data_type = list_array.value_type();
    let buffer = valid.finish();

    let elements = arrays
        .iter()
        .map(|a| a.as_ref())
        .collect::<Vec<&dyn Array>>();

    let list_arr = if elements.is_empty() {
        ListArray::new_null(Arc::new(Field::new_list_field(data_type, true)), row_count)
    } else {
        ListArray::new(
            Arc::new(Field::new_list_field(data_type, true)),
            OffsetBuffer::from_lengths(array_lengths),
            Arc::new(compute::concat(elements.as_slice())?),
            buffer,
        )
    };
    Ok(Arc::new(list_arr))
}

fn order_desc(modifier: &str) -> Result<bool> {
    match modifier.to_uppercase().as_str() {
        "DESC" => Ok(true),
        "ASC" => Ok(false),
        _ => exec_err!("the second parameter of array_sort expects DESC or ASC"),
    }
}

fn order_nulls_first(modifier: &str) -> Result<bool> {
    match modifier.to_uppercase().as_str() {
        "NULLS FIRST" => Ok(true),
        "NULLS LAST" => Ok(false),
        _ => exec_err!(
            "the third parameter of array_sort expects NULLS FIRST or NULLS LAST"
        ),
    }
}
