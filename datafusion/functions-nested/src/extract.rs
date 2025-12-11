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

//! [`ScalarUDFImpl`] definitions for array_element, array_slice, array_pop_front, array_pop_back, and array_any_value functions.

use arrow::array::{
    Array, ArrayRef, Capacities, GenericListArray, GenericListViewArray, Int64Array,
    MutableArrayData, NullArray, NullBufferBuilder, OffsetSizeTrait,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::DataType;
use arrow::datatypes::{
    DataType::{FixedSizeList, LargeList, LargeListView, List, ListView, Null},
    Field,
};
use datafusion_common::cast::as_large_list_array;
use datafusion_common::cast::as_list_array;
use datafusion_common::cast::{
    as_int64_array, as_large_list_view_array, as_list_view_array,
};
use datafusion_common::internal_err;
use datafusion_common::utils::ListCoercion;
use datafusion_common::{
    Result, exec_datafusion_err, exec_err, internal_datafusion_err, plan_err,
    utils::take_function_args,
};
use datafusion_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, Expr, TypeSignature,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

use crate::utils::make_scalar_function;

// Create static instances of ScalarUDFs for each function
make_udf_expr_and_func!(
    ArrayElement,
    array_element,
    array element,
    "extracts the element with the index n from the array.",
    array_element_udf
);

create_func!(ArraySlice, array_slice_udf);

make_udf_expr_and_func!(
    ArrayPopFront,
    array_pop_front,
    array,
    "returns the array without the first element.",
    array_pop_front_udf
);

make_udf_expr_and_func!(
    ArrayPopBack,
    array_pop_back,
    array,
    "returns the array without the last element.",
    array_pop_back_udf
);

make_udf_expr_and_func!(
    ArrayAnyValue,
    array_any_value,
    array,
    "returns the first non-null element in the array.",
    array_any_value_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Extracts the element with the index n from the array.",
    syntax_example = "array_element(array, index)",
    sql_example = r#"```sql
> select array_element([1, 2, 3, 4], 3);
+-----------------------------------------+
| array_element(List([1,2,3,4]),Int64(3)) |
+-----------------------------------------+
| 3                                       |
+-----------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "index",
        description = "Index to extract the element from the array."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayElement {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayElement {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayElement {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_index(Volatility::Immutable),
            aliases: vec![
                String::from("array_extract"),
                String::from("list_element"),
                String::from("list_extract"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayElement {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_element"
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        let args_name = args.iter().map(ToString::to_string).collect::<Vec<_>>();
        if args_name.len() != 2 {
            return exec_err!("expect 2 args, got {}", args_name.len());
        }

        Ok(format!("{}[{}]", args_name[0], args_name[1]))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let args_name = args
            .iter()
            .map(|e| e.schema_name().to_string())
            .collect::<Vec<_>>();
        if args_name.len() != 2 {
            return exec_err!("expect 2 args, got {}", args_name.len());
        }

        Ok(format!("{}[{}]", args_name[0], args_name[1]))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            Null => Ok(Null),
            List(field) | LargeList(field) => Ok(field.data_type().clone()),
            arg_type => plan_err!("{} does not support type {arg_type}", self.name()),
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_element_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// array_element SQL function
///
/// There are two arguments for array_element, the first one is the array, the second one is the 1-indexed index.
/// `array_element(array, index)`
///
/// For example:
/// > array_element(\[1, 2, 3], 2) -> 2
fn array_element_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array, indexes] = take_function_args("array_element", args)?;

    match &array.data_type() {
        Null => Ok(Arc::new(NullArray::new(array.len()))),
        List(_) => {
            let array = as_list_array(&array)?;
            let indexes = as_int64_array(&indexes)?;
            general_array_element::<i32>(array, indexes)
        }
        LargeList(_) => {
            let array = as_large_list_array(&array)?;
            let indexes = as_int64_array(&indexes)?;
            general_array_element::<i64>(array, indexes)
        }
        arg_type => {
            exec_err!("array_element does not support type {arg_type}")
        }
    }
}

fn general_array_element<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    indexes: &Int64Array,
) -> Result<ArrayRef>
where
    i64: TryInto<O>,
{
    let values = array.values();
    if values.data_type().is_null() {
        return Ok(Arc::new(NullArray::new(array.len())));
    }

    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());

    // use_nulls: true, we don't construct List for array_element, so we need explicit nulls.
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], true, capacity);

    fn adjusted_array_index<O: OffsetSizeTrait>(index: i64, len: O) -> Result<Option<O>>
    where
        i64: TryInto<O>,
    {
        let index: O = index.try_into().map_err(|_| {
            exec_datafusion_err!("array_element got invalid index: {index}")
        })?;
        // 0 ~ len - 1
        let adjusted_zero_index = if index < O::usize_as(0) {
            index + len
        } else {
            index - O::usize_as(1)
        };

        if O::usize_as(0) <= adjusted_zero_index && adjusted_zero_index < len {
            Ok(Some(adjusted_zero_index))
        } else {
            // Out of bounds
            Ok(None)
        }
    }

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        let start = offset_window[0];
        let end = offset_window[1];
        let len = end - start;

        // array is null
        if len == O::usize_as(0) {
            mutable.extend_nulls(1);
            continue;
        }

        let index = adjusted_array_index::<O>(indexes.value(row_index), len)?;

        if let Some(index) = index {
            let start = start.as_usize() + index.as_usize();
            mutable.extend(0, start, start + 1_usize);
        } else {
            // Index out of bounds
            mutable.extend_nulls(1);
        }
    }

    let data = mutable.freeze();
    Ok(arrow::array::make_array(data))
}

#[doc = "returns a slice of the array."]
pub fn array_slice(array: Expr, begin: Expr, end: Expr, stride: Option<Expr>) -> Expr {
    let args = match stride {
        Some(stride) => vec![array, begin, end, stride],
        None => vec![array, begin, end],
    };
    array_slice_udf().call(args)
}

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns a slice of the array based on 1-indexed start and end positions.",
    syntax_example = "array_slice(array, begin, end)",
    sql_example = r#"```sql
> select array_slice([1, 2, 3, 4, 5, 6, 7, 8], 3, 6);
+--------------------------------------------------------+
| array_slice(List([1,2,3,4,5,6,7,8]),Int64(3),Int64(6)) |
+--------------------------------------------------------+
| [3, 4, 5, 6]                                           |
+--------------------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "begin",
        description = "Index of the first element. If negative, it counts backward from the end of the array."
    ),
    argument(
        name = "end",
        description = "Index of the last element. If negative, it counts backward from the end of the array."
    ),
    argument(
        name = "stride",
        description = "Stride of the array slice. The default is 1."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct ArraySlice {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArraySlice {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Array,
                            ArrayFunctionArgument::Index,
                            ArrayFunctionArgument::Index,
                        ],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    }),
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Array,
                            ArrayFunctionArgument::Index,
                            ArrayFunctionArgument::Index,
                            ArrayFunctionArgument::Index,
                        ],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    }),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("list_slice")],
        }
    }
}

impl ScalarUDFImpl for ArraySlice {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        let args_name = args.iter().map(ToString::to_string).collect::<Vec<_>>();
        if let Some((arr, indexes)) = args_name.split_first() {
            Ok(format!("{arr}[{}]", indexes.join(":")))
        } else {
            exec_err!("no argument")
        }
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let args_name = args
            .iter()
            .map(|e| e.schema_name().to_string())
            .collect::<Vec<_>>();
        if let Some((arr, indexes)) = args_name.split_first() {
            Ok(format!("{arr}[{}]", indexes.join(":")))
        } else {
            exec_err!("no argument")
        }
    }

    fn name(&self) -> &str {
        "array_slice"
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
        make_scalar_function(array_slice_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// array_slice SQL function
///
/// We follow the behavior of array_slice in DuckDB
/// Note that array_slice is 1-indexed. And there are two additional arguments `from` and `to` in array_slice.
///
/// > array_slice(array, from, to)
///
/// Positive index is treated as the index from the start of the array. If the
/// `from` index is smaller than 1, it is treated as 1. If the `to` index is larger than the
/// length of the array, it is treated as the length of the array.
///
/// Negative index is treated as the index from the end of the array. If the index
/// is larger than the length of the array, it is NOT VALID, either in `from` or `to`.
/// The `to` index is exclusive like python slice syntax.
///
/// See test cases in `array.slt` for more details.
fn array_slice_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args_len = args.len();
    if args_len != 3 && args_len != 4 {
        return exec_err!("array_slice needs three or four arguments");
    }

    let stride = if args_len == 4 {
        Some(as_int64_array(&args[3])?)
    } else {
        None
    };

    let from_array = as_int64_array(&args[1])?;
    let to_array = as_int64_array(&args[2])?;

    let array_data_type = args[0].data_type();
    match array_data_type {
        List(_) => {
            let array = as_list_array(&args[0])?;
            general_array_slice::<i32>(array, from_array, to_array, stride)
        }
        LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            general_array_slice::<i64>(array, from_array, to_array, stride)
        }
        ListView(_) => {
            let array = as_list_view_array(&args[0])?;
            general_list_view_array_slice::<i32>(array, from_array, to_array, stride)
        }
        LargeListView(_) => {
            let array = as_large_list_view_array(&args[0])?;
            general_list_view_array_slice::<i64>(array, from_array, to_array, stride)
        }
        _ => exec_err!("array_slice does not support type: {}", array_data_type),
    }
}

fn adjusted_from_index<O: OffsetSizeTrait>(index: i64, len: O) -> Result<Option<O>>
where
    i64: TryInto<O>,
{
    // 0 ~ len - 1
    let adjusted_zero_index = if index < 0 {
        if let Ok(index) = index.try_into() {
            // When index < 0 and -index > length, index is clamped to the beginning of the list.
            // Otherwise, when index < 0, the index is counted from the end of the list.
            //
            // Note, we actually test the contrapositive, index < -length, because negating a
            // negative will panic if the negative is equal to the smallest representable value
            // while negating a positive is always safe.
            if index < (O::zero() - O::one()) * len {
                O::zero()
            } else {
                index + len
            }
        } else {
            return exec_err!("array_slice got invalid index: {}", index);
        }
    } else {
        // array_slice(arr, 1, to) is the same as array_slice(arr, 0, to)
        if let Ok(index) = index.try_into() {
            std::cmp::max(index - O::usize_as(1), O::usize_as(0))
        } else {
            return exec_err!("array_slice got invalid index: {}", index);
        }
    };

    if O::usize_as(0) <= adjusted_zero_index && adjusted_zero_index < len {
        Ok(Some(adjusted_zero_index))
    } else {
        // Out of bounds
        Ok(None)
    }
}

fn adjusted_to_index<O: OffsetSizeTrait>(index: i64, len: O) -> Result<Option<O>>
where
    i64: TryInto<O>,
{
    // 0 ~ len - 1
    let adjusted_zero_index = if index < 0 {
        // array_slice in duckdb with negative to_index is python-like, so index itself is exclusive
        if let Ok(index) = index.try_into() {
            index + len
        } else {
            return exec_err!("array_slice got invalid index: {}", index);
        }
    } else {
        // array_slice(arr, from, len + 1) is the same as array_slice(arr, from, len)
        if let Ok(index) = index.try_into() {
            std::cmp::min(index - O::usize_as(1), len - O::usize_as(1))
        } else {
            return exec_err!("array_slice got invalid index: {}", index);
        }
    };

    if O::usize_as(0) <= adjusted_zero_index && adjusted_zero_index < len {
        Ok(Some(adjusted_zero_index))
    } else {
        // Out of bounds
        Ok(None)
    }
}

/// Internal plan describing how to materialize a single row's slice after
/// the slice bounds/stride have been normalized. Both list layouts consume
/// this to drive their copy logic.
enum SlicePlan<O: OffsetSizeTrait> {
    /// No values should be produced.
    Empty,
    /// A contiguous run starting at `start` (relative to the row) with `len`
    /// elements can be copied in one go.
    Contiguous { start: O, len: O },
    /// Arbitrary positions (already relative to the row) must be copied in
    /// sequence.
    Indices(Vec<O>),
}

/// Produces a [`SlicePlan`] for the given logical slice parameters.
fn compute_slice_plan<O: OffsetSizeTrait>(
    len: O,
    from_raw: i64,
    to_raw: i64,
    stride_raw: Option<i64>,
) -> Result<SlicePlan<O>>
where
    i64: TryInto<O>,
{
    if len == O::usize_as(0) {
        return Ok(SlicePlan::Empty);
    }

    let from_index = adjusted_from_index::<O>(from_raw, len)?;
    let to_index = adjusted_to_index::<O>(to_raw, len)?;

    let (Some(from), Some(to)) = (from_index, to_index) else {
        return Ok(SlicePlan::Empty);
    };

    let stride_value = stride_raw.unwrap_or(1);
    if stride_value == 0 {
        return exec_err!(
            "array_slice got invalid stride: {:?}, it cannot be 0",
            stride_value
        );
    }

    if (from < to && stride_value.is_negative())
        || (from > to && stride_value.is_positive())
    {
        return Ok(SlicePlan::Empty);
    }

    let stride: O = stride_value.try_into().map_err(|_| {
        internal_datafusion_err!("array_slice got invalid stride: {}", stride_value)
    })?;

    if from <= to && stride_value.is_positive() {
        if stride_value == 1 {
            let len = to - from + O::usize_as(1);
            Ok(SlicePlan::Contiguous { start: from, len })
        } else {
            let mut indices = Vec::new();
            let mut index = from;
            while index <= to {
                indices.push(index);
                index += stride;
            }
            Ok(SlicePlan::Indices(indices))
        }
    } else {
        let mut indices = Vec::new();
        let mut index = from;
        while index >= to {
            indices.push(index);
            index += stride;
        }
        Ok(SlicePlan::Indices(indices))
    }
}

fn general_array_slice<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    from_array: &Int64Array,
    to_array: &Int64Array,
    stride: Option<&Int64Array>,
) -> Result<ArrayRef>
where
    i64: TryInto<O>,
{
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());

    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], true, capacity);

    // We have the slice syntax compatible with DuckDB v0.8.1.
    // The rule `adjusted_from_index` and `adjusted_to_index` follows the rule of array_slice in duckdb.

    let mut offsets = vec![O::usize_as(0)];
    let mut null_builder = NullBufferBuilder::new(array.len());

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        let start = offset_window[0];
        let end = offset_window[1];
        let len = end - start;

        // If any input is null, return null.
        if array.is_null(row_index)
            || from_array.is_null(row_index)
            || to_array.is_null(row_index)
            || stride.is_some_and(|s| s.is_null(row_index))
        {
            mutable.extend_nulls(1);
            offsets.push(offsets[row_index] + O::usize_as(1));
            null_builder.append_null();
            continue;
        }
        null_builder.append_non_null();

        // Empty arrays always return an empty array.
        if len == O::usize_as(0) {
            offsets.push(offsets[row_index]);
            continue;
        }

        let slice_plan = compute_slice_plan::<O>(
            len,
            from_array.value(row_index),
            to_array.value(row_index),
            stride.map(|s| s.value(row_index)),
        )?;

        match slice_plan {
            SlicePlan::Empty => offsets.push(offsets[row_index]),
            SlicePlan::Contiguous {
                start: rel_start,
                len: slice_len,
            } => {
                let start_index = (start + rel_start).to_usize().unwrap();
                let end_index = (start + rel_start + slice_len).to_usize().unwrap();
                mutable.extend(0, start_index, end_index);
                offsets.push(offsets[row_index] + slice_len);
            }
            SlicePlan::Indices(indices) => {
                let count = indices.len();
                for rel_index in indices {
                    let absolute_index = (start + rel_index).to_usize().unwrap();
                    mutable.extend(0, absolute_index, absolute_index + 1);
                }
                offsets.push(offsets[row_index] + O::usize_as(count));
            }
        }
    }

    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new_list_field(array.value_type(), true)),
        OffsetBuffer::<O>::new(offsets.into()),
        arrow::array::make_array(data),
        null_builder.finish(),
    )?))
}

fn general_list_view_array_slice<O: OffsetSizeTrait>(
    array: &GenericListViewArray<O>,
    from_array: &Int64Array,
    to_array: &Int64Array,
    stride: Option<&Int64Array>,
) -> Result<ArrayRef>
where
    i64: TryInto<O>,
{
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());
    let field = match array.data_type() {
        ListView(field) | LargeListView(field) => Arc::clone(field),
        other => {
            return internal_err!("array_slice got unexpected data type: {}", other);
        }
    };

    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], true, capacity);

    // We must build `offsets` and `sizes` buffers manually as ListView does not enforce
    // monotonically increasing offsets.
    let mut offsets = Vec::with_capacity(array.len());
    let mut sizes = Vec::with_capacity(array.len());
    let mut current_offset = O::usize_as(0);
    let mut null_builder = NullBufferBuilder::new(array.len());

    for row_index in 0..array.len() {
        // Propagate NULL semantics: any NULL input yields a NULL output slot.
        if array.is_null(row_index)
            || from_array.is_null(row_index)
            || to_array.is_null(row_index)
            || stride.is_some_and(|s| s.is_null(row_index))
        {
            null_builder.append_null();
            offsets.push(current_offset);
            sizes.push(O::usize_as(0));
            continue;
        }
        null_builder.append_non_null();

        let len = array.value_size(row_index);

        // Empty arrays always return an empty array.
        if len == O::usize_as(0) {
            offsets.push(current_offset);
            sizes.push(O::usize_as(0));
            continue;
        }

        let slice_plan = compute_slice_plan::<O>(
            len,
            from_array.value(row_index),
            to_array.value(row_index),
            stride.map(|s| s.value(row_index)),
        )?;

        let start = array.value_offset(row_index);
        match slice_plan {
            SlicePlan::Empty => {
                offsets.push(current_offset);
                sizes.push(O::usize_as(0));
            }
            SlicePlan::Contiguous {
                start: rel_start,
                len: slice_len,
            } => {
                let start_index = (start + rel_start).to_usize().unwrap();
                let end_index = (start + rel_start + slice_len).to_usize().unwrap();
                mutable.extend(0, start_index, end_index);
                offsets.push(current_offset);
                sizes.push(slice_len);
                current_offset += slice_len;
            }
            SlicePlan::Indices(indices) => {
                let count = indices.len();
                for rel_index in indices {
                    let absolute_index = (start + rel_index).to_usize().unwrap();
                    mutable.extend(0, absolute_index, absolute_index + 1);
                }
                let length = O::usize_as(count);
                offsets.push(current_offset);
                sizes.push(length);
                current_offset += length;
            }
        }
    }

    let data = mutable.freeze();

    Ok(Arc::new(GenericListViewArray::<O>::try_new(
        field,
        ScalarBuffer::from(offsets),
        ScalarBuffer::from(sizes),
        arrow::array::make_array(data),
        null_builder.finish(),
    )?))
}

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the array without the first element.",
    syntax_example = "array_pop_front(array)",
    sql_example = r#"```sql
> select array_pop_front([1, 2, 3]);
+-------------------------------+
| array_pop_front(List([1,2,3])) |
+-------------------------------+
| [2, 3]                        |
+-------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct ArrayPopFront {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayPopFront {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec![String::from("list_pop_front")],
        }
    }
}

impl ScalarUDFImpl for ArrayPopFront {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_pop_front"
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
        make_scalar_function(array_pop_front_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// array_pop_front SQL function
fn array_pop_front_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let array_data_type = args[0].data_type();
    match array_data_type {
        List(_) => {
            let array = as_list_array(&args[0])?;
            general_pop_front_list::<i32>(array)
        }
        LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            general_pop_front_list::<i64>(array)
        }
        _ => exec_err!("array_pop_front does not support type: {}", array_data_type),
    }
}

fn general_pop_front_list<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
) -> Result<ArrayRef>
where
    i64: TryInto<O>,
{
    let from_array = Int64Array::from(vec![2; array.len()]);
    let to_array = Int64Array::from(
        array
            .iter()
            .map(|arr| arr.map_or(0, |arr| arr.len() as i64))
            .collect::<Vec<i64>>(),
    );
    general_array_slice::<O>(array, &from_array, &to_array, None)
}

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the array without the last element.",
    syntax_example = "array_pop_back(array)",
    sql_example = r#"```sql
> select array_pop_back([1, 2, 3]);
+-------------------------------+
| array_pop_back(List([1,2,3])) |
+-------------------------------+
| [1, 2]                        |
+-------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct ArrayPopBack {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayPopBack {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec![String::from("list_pop_back")],
        }
    }
}

impl ScalarUDFImpl for ArrayPopBack {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_pop_back"
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
        make_scalar_function(array_pop_back_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// array_pop_back SQL function
fn array_pop_back_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("array_pop_back", args)?;

    match array.data_type() {
        List(_) => {
            let array = as_list_array(&array)?;
            general_pop_back_list::<i32>(array)
        }
        LargeList(_) => {
            let array = as_large_list_array(&array)?;
            general_pop_back_list::<i64>(array)
        }
        _ => exec_err!(
            "array_pop_back does not support type: {}",
            array.data_type()
        ),
    }
}

fn general_pop_back_list<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
) -> Result<ArrayRef>
where
    i64: TryInto<O>,
{
    let from_array = Int64Array::from(vec![1; array.len()]);
    let to_array = Int64Array::from(
        array
            .iter()
            .map(|arr| arr.map_or(0, |arr| arr.len() as i64 - 1))
            .collect::<Vec<i64>>(),
    );
    general_array_slice::<O>(array, &from_array, &to_array, None)
}

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the first non-null element in the array.",
    syntax_example = "array_any_value(array)",
    sql_example = r#"```sql
> select array_any_value([NULL, 1, 2, 3]);
+-------------------------------+
| array_any_value(List([NULL,1,2,3])) |
+-------------------------------------+
| 1                                   |
+-------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct ArrayAnyValue {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayAnyValue {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec![String::from("list_any_value")],
        }
    }
}

impl ScalarUDFImpl for ArrayAnyValue {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_any_value"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            List(field) | LargeList(field) | FixedSizeList(field, _) => {
                Ok(field.data_type().clone())
            }
            _ => plan_err!(
                "array_any_value can only accept List, LargeList or FixedSizeList as the argument"
            ),
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_any_value_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_any_value_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("array_any_value", args)?;

    match &array.data_type() {
        List(_) => {
            let array = as_list_array(&array)?;
            general_array_any_value::<i32>(array)
        }
        LargeList(_) => {
            let array = as_large_list_array(&array)?;
            general_array_any_value::<i64>(array)
        }
        data_type => exec_err!("array_any_value does not support type: {data_type}"),
    }
}

fn general_array_any_value<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
) -> Result<ArrayRef>
where
    i64: TryInto<O>,
{
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(array.len());

    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], true, capacity);

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        let start = offset_window[0];
        let end = offset_window[1];
        let len = end - start;

        // array is null
        if len == O::usize_as(0) {
            mutable.extend_nulls(1);
            continue;
        }

        let row_value = array.value(row_index);
        match row_value.nulls() {
            Some(row_nulls_buffer) => {
                // nulls are present in the array so try to take the first valid element
                if let Some(first_non_null_index) =
                    row_nulls_buffer.valid_indices().next()
                {
                    let index = start.as_usize() + first_non_null_index;
                    mutable.extend(0, index, index + 1)
                } else {
                    // all the elements in the array are null
                    mutable.extend_nulls(1);
                }
            }
            None => {
                // no nulls are present in the array so take the first element
                let index = start.as_usize();
                mutable.extend(0, index, index + 1);
            }
        }
    }

    let data = mutable.freeze();
    Ok(arrow::array::make_array(data))
}

#[cfg(test)]
mod tests {
    use super::{array_element_udf, general_list_view_array_slice};
    use arrow::array::{
        Array, ArrayRef, GenericListViewArray, Int32Array, Int64Array, ListViewArray,
        cast::AsArray,
    };
    use arrow::buffer::ScalarBuffer;
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::{Column, DFSchema, Result};
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{Expr, ExprSchemable};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn list_view_values(array: &GenericListViewArray<i32>) -> Vec<Vec<i32>> {
        (0..array.len())
            .map(|i| {
                let child = array.value(i);
                let values = child.as_any().downcast_ref::<Int32Array>().unwrap();
                values.iter().map(|v| v.unwrap()).collect()
            })
            .collect()
    }

    // Regression test for https://github.com/apache/datafusion/issues/13755
    #[test]
    fn test_array_element_return_type_fixed_size_list() {
        let fixed_size_list_type = DataType::FixedSizeList(
            Field::new("some_arbitrary_test_field", DataType::Int32, false).into(),
            13,
        );
        let array_type = DataType::List(
            Field::new_list_field(fixed_size_list_type.clone(), true).into(),
        );
        let index_type = DataType::Int64;

        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("my_array", array_type.clone(), false),
                Field::new("my_index", index_type.clone(), false),
            ]
            .into(),
            HashMap::default(),
        )
        .unwrap();

        let udf = array_element_udf();

        // ScalarUDFImpl::return_type
        assert_eq!(
            udf.return_type(&[array_type.clone(), index_type.clone()])
                .unwrap(),
            fixed_size_list_type
        );

        // Via ExprSchemable::get_type (e.g. SimplifyInfo)
        let udf_expr = Expr::ScalarFunction(ScalarFunction {
            func: array_element_udf(),
            args: vec![
                Expr::Column(Column::new_unqualified("my_array")),
                Expr::Column(Column::new_unqualified("my_index")),
            ],
        });
        assert_eq!(
            ExprSchemable::get_type(&udf_expr, &schema).unwrap(),
            fixed_size_list_type
        );
    }

    #[test]
    fn test_array_slice_list_view_basic() -> Result<()> {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let offsets = ScalarBuffer::from(vec![0, 3]);
        let sizes = ScalarBuffer::from(vec![3, 2]);
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let array = ListViewArray::new(field, offsets, sizes, values, None);

        let from = Int64Array::from(vec![2, 1]);
        let to = Int64Array::from(vec![3, 2]);

        let result = general_list_view_array_slice::<i32>(
            &array,
            &from,
            &to,
            None::<&Int64Array>,
        )?;
        let result = result.as_ref().as_list_view::<i32>();

        assert_eq!(list_view_values(result), vec![vec![2, 3], vec![4, 5]]);
        Ok(())
    }

    #[test]
    fn test_array_slice_list_view_non_monotonic_offsets() -> Result<()> {
        // First list references the tail of the values buffer, second list references the head.
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let offsets = ScalarBuffer::from(vec![3, 0]);
        let sizes = ScalarBuffer::from(vec![2, 3]);
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let array = ListViewArray::new(field, offsets, sizes, values, None);

        let from = Int64Array::from(vec![1, 1]);
        let to = Int64Array::from(vec![2, 2]);

        let result = general_list_view_array_slice::<i32>(
            &array,
            &from,
            &to,
            None::<&Int64Array>,
        )?;
        let result = result.as_ref().as_list_view::<i32>();

        assert_eq!(list_view_values(result), vec![vec![4, 5], vec![1, 2]]);
        Ok(())
    }

    #[test]
    fn test_array_slice_list_view_negative_stride() -> Result<()> {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let offsets = ScalarBuffer::from(vec![0, 3]);
        let sizes = ScalarBuffer::from(vec![3, 2]);
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let array = ListViewArray::new(field, offsets, sizes, values, None);

        let from = Int64Array::from(vec![3, 2]);
        let to = Int64Array::from(vec![1, 1]);
        let stride = Int64Array::from(vec![-1, -1]);

        let result =
            general_list_view_array_slice::<i32>(&array, &from, &to, Some(&stride))?;
        let result = result.as_ref().as_list_view::<i32>();

        assert_eq!(list_view_values(result), vec![vec![3, 2, 1], vec![5, 4]]);
        Ok(())
    }

    #[test]
    fn test_array_slice_list_view_out_of_order() -> Result<()> {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let offsets = ScalarBuffer::from(vec![3, 1, 0]);
        let sizes = ScalarBuffer::from(vec![2, 2, 1]);
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let array = ListViewArray::new(field, offsets, sizes, values, None);
        assert_eq!(
            list_view_values(&array),
            vec![vec![4, 5], vec![2, 3], vec![1]]
        );

        let from = Int64Array::from(vec![2, 2, 2]);
        let to = Int64Array::from(vec![1, 1, 1]);
        let stride = Int64Array::from(vec![-1, -1, -1]);

        let result =
            general_list_view_array_slice::<i32>(&array, &from, &to, Some(&stride))?;
        let result = result.as_ref().as_list_view::<i32>();

        assert_eq!(
            list_view_values(result),
            vec![vec![5, 4], vec![3, 2], vec![]]
        );
        Ok(())
    }

    #[test]
    fn test_array_slice_list_view_with_nulls() -> Result<()> {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        let offsets = ScalarBuffer::from(vec![0, 2, 5]);
        let sizes = ScalarBuffer::from(vec![2, 3, 0]);
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let array = ListViewArray::new(field, offsets, sizes, values, None);

        let from = Int64Array::from(vec![1, 1, 1]);
        let to = Int64Array::from(vec![2, 2, 1]);

        let result = general_list_view_array_slice::<i32>(&array, &from, &to, None)?;
        let result = result.as_ref().as_list_view::<i32>();

        let actual: Vec<Vec<Option<i32>>> = (0..result.len())
            .map(|i| {
                result
                    .value(i)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect()
            })
            .collect();

        assert_eq!(
            actual,
            vec![vec![Some(1), None], vec![Some(3), Some(4)], Vec::new(),]
        );

        // Test with NULL stride - should return NULL for rows with NULL stride
        let stride_with_null = Int64Array::from(vec![Some(1), None, Some(1)]);
        let result = general_list_view_array_slice::<i32>(
            &array,
            &from,
            &to,
            Some(&stride_with_null),
        )?;
        let result = result.as_ref().as_list_view::<i32>();

        // First row: stride = 1, should return [1, None]
        // Second row: stride = NULL, should return NULL
        // Third row: stride = 1, empty array should return empty
        assert!(!result.is_null(0)); // First row should not be null
        assert!(result.is_null(1)); // Second row should be null (stride is NULL)
        assert!(!result.is_null(2)); // Third row should not be null

        let first_row: Vec<Option<i32>> = result
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(first_row, vec![Some(1), None]);

        Ok(())
    }
}
