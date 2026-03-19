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
use arrow::array::{
    Array, ArrayRef, GenericListArray, OffsetSizeTrait, UInt32Array, UInt64Array,
    new_empty_array, new_null_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::SortColumn;
use arrow::datatypes::{DataType, FieldRef};
use arrow::{compute, compute::SortOptions};
use datafusion_common::cast::{as_large_list_array, as_list_array, as_string_array};
use datafusion_common::utils::ListCoercion;
use datafusion_common::{Result, exec_err};
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
        description = "Whether to sort in ascending (`ASC`) or descending (`DESC`) order. The default is `ASC`."
    ),
    argument(
        name = "nulls_first",
        description = "Whether to sort nulls first (`NULLS FIRST`) or last (`NULLS LAST`). The default is `NULLS FIRST`."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
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
        Ok(arg_types[0].clone())
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

pub fn array_sort_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() || args.len() > 3 {
        return exec_err!("array_sort expects one to three arguments");
    }

    if args[0].is_empty() || args[0].data_type().is_null() {
        return Ok(Arc::clone(&args[0]));
    }

    if args[1..].iter().any(|array| array.is_null(0)) {
        return Ok(new_null_array(args[0].data_type(), args[0].len()));
    }

    let sort_options = match args.len() {
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
        // We guard at the top
        _ => unreachable!(),
    };

    match args[0].data_type() {
        DataType::List(field) | DataType::LargeList(field)
            if field.data_type().is_null() =>
        {
            Ok(Arc::clone(&args[0]))
        }
        DataType::List(field) => {
            let array = as_list_array(&args[0])?;
            array_sort_generic(array, Arc::clone(field), sort_options)
        }
        DataType::LargeList(field) => {
            let array = as_large_list_array(&args[0])?;
            array_sort_generic(array, Arc::clone(field), sort_options)
        }
        // Signature should prevent this arm ever occurring
        _ => exec_err!("array_sort expects list for first argument"),
    }
}

fn array_sort_generic<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    field: FieldRef,
    sort_options: Option<SortOptions>,
) -> Result<ArrayRef> {
    let values = list_array.values();

    if values.data_type().is_primitive() {
        array_sort_direct(list_array, field, sort_options)
    } else {
        array_sort_batch_indices(list_array, field, sort_options)
    }
}

/// Sort each row using `compute::sort()` and concatenate the results.
///
/// This is efficient for primitive element types because Arrow's sort kernel
/// does the sorting in-place.
fn array_sort_direct<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    field: FieldRef,
    sort_options: Option<SortOptions>,
) -> Result<ArrayRef> {
    let row_count = list_array.len();
    let values = list_array.values();

    let mut array_lengths = Vec::with_capacity(row_count);
    let mut sorted_arrays = Vec::with_capacity(row_count);
    for i in 0..row_count {
        if list_array.is_null(i) {
            array_lengths.push(0);
        } else {
            let arr_ref = list_array.value(i);
            let sorted = compute::sort(arr_ref.as_ref(), sort_options)?;
            array_lengths.push(sorted.len());
            sorted_arrays.push(sorted);
        }
    }

    let sorted_values: ArrayRef = if sorted_arrays.is_empty() {
        new_empty_array(values.data_type())
    } else {
        let elements: Vec<&dyn Array> =
            sorted_arrays.iter().map(|a| a.as_ref()).collect();
        Arc::new(compute::concat(&elements)?)
    };

    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        field,
        OffsetBuffer::from_lengths(array_lengths),
        sorted_values,
        list_array.nulls().cloned(),
    )?))
}

/// Sort each row by collecting sort indices, then materialize with a single
/// `take()` at the end.
///
/// This is efficient for non-primitive element types because Arrow's sort
/// kernel would internally call `sort_to_indices()` + `take()` per row anyway.
/// Batching into a single `take()` avoids N per-row allocations and the final
/// `concat()`.
fn array_sort_batch_indices<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    field: FieldRef,
    sort_options: Option<SortOptions>,
) -> Result<ArrayRef> {
    let row_count = list_array.len();
    let values = list_array.values();
    let offsets = list_array.offsets();

    let total_values = offsets[row_count].as_usize() - offsets[0].as_usize();
    let mut indices: Vec<OffsetSize> = Vec::with_capacity(total_values);
    let mut new_offsets = Vec::with_capacity(row_count + 1);
    new_offsets.push(OffsetSize::usize_as(0));

    let is_struct = matches!(values.data_type(), DataType::Struct(_));

    for (row_index, window) in offsets.windows(2).enumerate() {
        let start = window[0];
        let end = window[1];

        if list_array.is_null(row_index) {
            new_offsets.push(new_offsets[row_index]);
            continue;
        }

        let len = (end - start).as_usize();
        if len <= 1 {
            // 0 or 1 elements: already sorted, push identity indices
            indices.extend((start.as_usize()..end.as_usize()).map(OffsetSize::usize_as));
        } else {
            let sliced = values.slice(start.as_usize(), len);

            // Arrow's sort kernel does not support Struct arrays, so use
            // lexsort_to_indices instead:
            // https://github.com/apache/arrow-rs/issues/6911#issuecomment-2562928843
            let sorted_indices = if is_struct {
                let sort_columns = vec![SortColumn {
                    values: sliced,
                    options: sort_options,
                }];
                compute::lexsort_to_indices(&sort_columns, None)?
            } else {
                compute::sort_to_indices(&sliced, sort_options, None)?
            };

            indices.extend(
                sorted_indices
                    .values()
                    .iter()
                    .map(|&idx| start + OffsetSize::usize_as(idx as usize)),
            );
        }

        new_offsets.push(new_offsets[row_index] + (end - start));
    }

    let sorted_values = if indices.is_empty() {
        new_empty_array(values.data_type())
    } else {
        take_by_indices(values, indices)?
    };

    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        field,
        OffsetBuffer::<OffsetSize>::new(new_offsets.into()),
        sorted_values,
        list_array.nulls().cloned(),
    )?))
}

/// Select elements from `values` at the given `indices` using `compute::take`.
/// We consume `indices` in order to avoid an intermediate copy.
fn take_by_indices<OffsetSize: OffsetSizeTrait>(
    values: &ArrayRef,
    indices: Vec<OffsetSize>,
) -> Result<ArrayRef> {
    let len = indices.len();
    let buffer = arrow::buffer::Buffer::from_vec(indices);
    let indices_array: ArrayRef = if OffsetSize::IS_LARGE {
        Arc::new(UInt64Array::new(
            arrow::buffer::ScalarBuffer::new(buffer, 0, len),
            None,
        ))
    } else {
        Arc::new(UInt32Array::new(
            arrow::buffer::ScalarBuffer::new(buffer, 0, len),
            None,
        ))
    };
    Ok(compute::take(values.as_ref(), &indices_array, None)?)
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
