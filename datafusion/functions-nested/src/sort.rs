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
use arrow::array::BooleanBufferBuilder;
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, GenericListArray, OffsetSizeTrait,
    PrimitiveArray, UInt32Array, UInt64Array, new_empty_array, new_null_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{ArrowNativeTypeOp, DataType, FieldRef};
use arrow::row::{RowConverter, SortField};
use arrow::{compute, compute::SortOptions, downcast_primitive_array};
use datafusion_common::cast::{as_large_list_array, as_list_array, as_string_array};
use datafusion_common::utils::ListCoercion;
use datafusion_common::{Result, exec_err, internal_datafusion_err};
use datafusion_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, Documentation,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_sort_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_sort_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() || args.len() > 3 {
        return exec_err!("array_sort expects one to three arguments");
    }

    if args[0].is_empty() || args[0].data_type().is_null() {
        return Ok(Arc::clone(&args[0]));
    }

    if args[1..].iter().any(|array| array.is_null(0)) {
        return Ok(new_null_array(args[0].data_type(), args[0].len()));
    }

    let sort_options = if args.len() >= 2 {
        let order = as_string_array(&args[1])?.value(0);
        let descending = order_desc(order)?;
        let nulls_first = if args.len() >= 3 {
            order_nulls_first(as_string_array(&args[2])?.value(0))?
        } else {
            true
        };
        Some(SortOptions {
            descending,
            nulls_first,
        })
    } else {
        None
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
        array_sort_primitive(list_array, field, sort_options)
    } else {
        array_sort_non_primitive(list_array, field, sort_options)
    }
}

/// Sort each row of a primitive-typed ListArray using a custom in-place sort
/// kernel.
fn array_sort_primitive<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    field: FieldRef,
    sort_options: Option<SortOptions>,
) -> Result<ArrayRef> {
    let values = list_array.values().as_ref();
    downcast_primitive_array! {
        values => sort_primitive_list(values, list_array, field, sort_options),
        _ => exec_err!("array_sort: unsupported primitive type")
    }
}

fn sort_primitive_list<T: ArrowPrimitiveType, OffsetSize: OffsetSizeTrait>(
    prim_values: &PrimitiveArray<T>,
    list_array: &GenericListArray<OffsetSize>,
    field: FieldRef,
    sort_options: Option<SortOptions>,
) -> Result<ArrayRef>
where
    T::Native: ArrowNativeTypeOp,
{
    if prim_values.null_count() > 0 {
        sort_list_with_nulls(prim_values, list_array, field, sort_options)
    } else {
        sort_list_no_nulls(prim_values, list_array, field, sort_options)
    }
}

/// Fast path for primitive values with no element-level nulls. Copies all
/// values into a single `Vec` and sorts each row's slice in-place.
fn sort_list_no_nulls<T: ArrowPrimitiveType, OffsetSize: OffsetSizeTrait>(
    prim_values: &PrimitiveArray<T>,
    list_array: &GenericListArray<OffsetSize>,
    field: FieldRef,
    sort_options: Option<SortOptions>,
) -> Result<ArrayRef>
where
    T::Native: ArrowNativeTypeOp,
{
    let row_count = list_array.len();
    let offsets = list_array.offsets();
    let values_start = offsets[0].as_usize();
    let values_end = offsets[row_count].as_usize();

    let descending = sort_options.is_some_and(|o| o.descending);

    // Copy all values into a mutable buffer
    let mut values: Vec<T::Native> =
        prim_values.values()[values_start..values_end].to_vec();

    for (row_index, window) in offsets.windows(2).enumerate() {
        if list_array.is_null(row_index) {
            continue;
        }
        let start = window[0].as_usize() - values_start;
        let end = window[1].as_usize() - values_start;
        let slice = &mut values[start..end];
        if descending {
            slice.sort_unstable_by(|a, b| b.compare(*a));
        } else {
            slice.sort_unstable_by(|a, b| a.compare(*b));
        }
    }

    let new_offsets = rebase_offsets(offsets);
    let sorted_values = Arc::new(
        PrimitiveArray::<T>::new(values.into(), None)
            .with_data_type(prim_values.data_type().clone()),
    );

    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        field,
        new_offsets,
        sorted_values,
        list_array.nulls().cloned(),
    )?))
}

/// Slow path for primitive values with element-level nulls.
fn sort_list_with_nulls<T: ArrowPrimitiveType, OffsetSize: OffsetSizeTrait>(
    prim_values: &PrimitiveArray<T>,
    list_array: &GenericListArray<OffsetSize>,
    field: FieldRef,
    sort_options: Option<SortOptions>,
) -> Result<ArrayRef>
where
    T::Native: ArrowNativeTypeOp,
{
    let row_count = list_array.len();
    let offsets = list_array.offsets();
    let values_start = offsets[0].as_usize();
    let values_end = offsets[row_count].as_usize();
    let total_values = values_end - values_start;

    let descending = sort_options.is_some_and(|o| o.descending);
    let nulls_first = sort_options.is_none_or(|o| o.nulls_first);

    let mut out_values: Vec<T::Native> = vec![T::Native::default(); total_values];
    let mut validity = BooleanBufferBuilder::new(total_values);

    let src_nulls = prim_values.nulls().ok_or_else(|| {
        internal_datafusion_err!(
            "sort_list_with_nulls called but values have no null buffer"
        )
    })?;
    let src_values = prim_values.values();

    for (row_index, window) in offsets.windows(2).enumerate() {
        let start = window[0].as_usize();
        let end = window[1].as_usize();
        let row_len = end - start;
        let out_start = start - values_start;

        if list_array.is_null(row_index) || row_len == 0 {
            validity.append_n(row_len, false);
            continue;
        }

        let null_count = src_nulls.slice(start, row_len).null_count();
        let valid_count = row_len - null_count;

        // Compact valid values directly into the target region of the output
        // buffer: after nulls (if nulls_first) or at the start (if nulls_last).
        let valid_offset = if nulls_first { null_count } else { 0 };
        let mut write_pos = out_start + valid_offset;
        for i in start..end {
            if src_nulls.is_valid(i) {
                out_values[write_pos] = src_values[i];
                write_pos += 1;
            }
        }

        let valid_slice = &mut out_values
            [out_start + valid_offset..out_start + valid_offset + valid_count];
        if descending {
            valid_slice.sort_unstable_by(|a, b| b.compare(*a));
        } else {
            valid_slice.sort_unstable_by(|a, b| a.compare(*b));
        }

        // Build validity bits
        if nulls_first {
            validity.append_n(null_count, false);
            validity.append_n(valid_count, true);
        } else {
            validity.append_n(valid_count, true);
            validity.append_n(null_count, false);
        }
    }

    let new_offsets = rebase_offsets(offsets);

    let null_buffer = NullBuffer::from(validity.finish());
    let sorted_values = Arc::new(
        PrimitiveArray::<T>::new(out_values.into(), Some(null_buffer))
            .with_data_type(prim_values.data_type().clone()),
    );

    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        field,
        new_offsets,
        sorted_values,
        list_array.nulls().cloned(),
    )?))
}

/// Sort a non-pritive-typed ListArray by converting all rows at once using
/// `RowConverter`, and then sort row indices by comparing encoded bytes (sort
/// direction and null ordering are baked into the encoding), and materialize
/// the result with a single `take()`.
fn array_sort_non_primitive<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    field: FieldRef,
    sort_options: Option<SortOptions>,
) -> Result<ArrayRef> {
    let row_count = list_array.len();
    let values = list_array.values();
    let offsets = list_array.offsets();
    let values_start = offsets[0].as_usize();
    let total_values = offsets[row_count].as_usize() - values_start;

    let converter = RowConverter::new(vec![SortField::new_with_options(
        values.data_type().clone(),
        sort_options.unwrap_or_default(),
    )])?;
    let values_sliced = values.slice(values_start, total_values);
    let rows = converter.convert_columns(&[Arc::clone(&values_sliced)])?;

    let mut indices: Vec<OffsetSize> = Vec::with_capacity(total_values);
    let mut new_offsets = Vec::with_capacity(row_count + 1);
    new_offsets.push(OffsetSize::usize_as(0));

    let mut sort_scratch: Vec<usize> = Vec::new();

    for (row_index, window) in offsets.windows(2).enumerate() {
        let start = window[0];
        let end = window[1];

        if list_array.is_null(row_index) {
            new_offsets.push(new_offsets[row_index]);
            continue;
        }

        let len = (end - start).as_usize();
        let local_start = start.as_usize() - values_start;

        if len <= 1 {
            indices.extend((local_start..local_start + len).map(OffsetSize::usize_as));
        } else {
            sort_scratch.clear();
            sort_scratch.extend(local_start..local_start + len);
            sort_scratch.sort_unstable_by(|&a, &b| rows.row(a).cmp(&rows.row(b)));
            indices.extend(sort_scratch.iter().map(|&i| OffsetSize::usize_as(i)));
        }

        new_offsets.push(new_offsets[row_index] + (end - start));
    }

    let sorted_values = if indices.is_empty() {
        new_empty_array(values.data_type())
    } else {
        take_by_indices(&values_sliced, indices)?
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

/// Rebase offsets so they start at 0. For non-sliced ListArrays (the common
/// case) offsets already start at 0 and we can clone the Arc-backed buffer
/// cheaply instead of allocating a new Vec.
fn rebase_offsets<OffsetSize: OffsetSizeTrait>(
    offsets: &OffsetBuffer<OffsetSize>,
) -> OffsetBuffer<OffsetSize> {
    if offsets[0].as_usize() == 0 {
        offsets.clone()
    } else {
        let rebased: Vec<OffsetSize> = offsets.iter().map(|o| *o - offsets[0]).collect();
        OffsetBuffer::new(rebased.into())
    }
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
