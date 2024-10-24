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

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::ArrowNativeTypeOp;
use arrow::array::Capacities;
use arrow::array::GenericListArray;
use arrow::array::Int64Array;
use arrow::array::MutableArrayData;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use arrow_schema::DataType::{FixedSizeList, LargeList, List};
use arrow_schema::Field;
use datafusion_common::cast::as_int64_array;
use datafusion_common::cast::as_large_list_array;
use datafusion_common::cast::as_list_array;
use datafusion_common::{
    exec_err, internal_datafusion_err, plan_err, DataFusionError, Result,
};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_ARRAY;
use datafusion_expr::Expr;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::{Arc, OnceLock};

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

#[derive(Debug)]
pub(super) struct ArrayElement {
    signature: Signature,
    aliases: Vec<String>,
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
            List(field)
            | LargeList(field)
            | FixedSizeList(field, _) => Ok(field.data_type().clone()),
            _ => plan_err!(
                "ArrayElement can only accept List, LargeList or FixedSizeList as the first argument"
            ),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_element_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_array_element_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_array_element_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_ARRAY)
            .with_description(
                "Extracts the element with the index n from the array.",
            )
            .with_syntax_example("array_element(array, index)")
            .with_sql_example(
                r#"```sql
> select array_element([1, 2, 3, 4], 3);
+-----------------------------------------+
| array_element(List([1,2,3,4]),Int64(3)) |
+-----------------------------------------+
| 3                                       |
+-----------------------------------------+
```"#,
            )
            .with_argument(
                "array",
                "Array expression. Can be a constant, column, or function, and any combination of array operators.",
            )
            .with_argument(
                "index",
                "Index to extract the element from the array.",
            )
            .build()
            .unwrap()
    })
}

/// array_element SQL function
///
/// There are two arguments for array_element, the first one is the array, the second one is the 1-indexed index.
/// `array_element(array, index)`
///
/// For example:
/// > array_element(\[1, 2, 3], 2) -> 2
fn array_element_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_element needs two arguments");
    }

    match &args[0].data_type() {
        List(_) => {
            let array = as_list_array(&args[0])?;
            let indexes = as_int64_array(&args[1])?;
            general_array_element::<i32>(array, indexes)
        }
        LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            let indexes = as_int64_array(&args[1])?;
            general_array_element::<i64>(array, indexes)
        }
        _ => exec_err!(
            "array_element does not support type: {:?}",
            args[0].data_type()
        ),
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
            DataFusionError::Execution(format!(
                "array_element got invalid index: {}",
                index
            ))
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

#[derive(Debug)]
pub(super) struct ArraySlice {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArraySlice {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_slice_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_array_slice_doc())
    }
}

fn get_array_slice_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_ARRAY)
            .with_description(
                "Returns a slice of the array based on 1-indexed start and end positions.",
            )
            .with_syntax_example("array_slice(array, begin, end)")
            .with_sql_example(
                r#"```sql
> select array_slice([1, 2, 3, 4, 5, 6, 7, 8], 3, 6);
+--------------------------------------------------------+
| array_slice(List([1,2,3,4,5,6,7,8]),Int64(3),Int64(6)) |
+--------------------------------------------------------+
| [3, 4, 5, 6]                                           |
+--------------------------------------------------------+
```"#,
            )
            .with_argument(
                "array",
                "Array expression. Can be a constant, column, or function, and any combination of array operators.",
            )
            .with_argument(
                "begin",
                "Index of the first element. If negative, it counts backward from the end of the array.",
            )
            .with_argument(
                "end",
                "Index of the last element. If negative, it counts backward from the end of the array.",
            )
            .with_argument(
                "stride",
                "Stride of the array slice. The default is 1.",
            )
            .build()
            .unwrap()
    })
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
            let from_array = as_int64_array(&args[1])?;
            let to_array = as_int64_array(&args[2])?;
            general_array_slice::<i64>(array, from_array, to_array, stride)
        }
        _ => exec_err!("array_slice does not support type: {:?}", array_data_type),
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

    // use_nulls: false, we don't need nulls but empty array for array_slice, so we don't need explicit nulls but adjust offset to indicate nulls.
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], false, capacity);

    // We have the slice syntax compatible with DuckDB v0.8.1.
    // The rule `adjusted_from_index` and `adjusted_to_index` follows the rule of array_slice in duckdb.

    fn adjusted_from_index<O: OffsetSizeTrait>(index: i64, len: O) -> Result<Option<O>>
    where
        i64: TryInto<O>,
    {
        // 0 ~ len - 1
        let adjusted_zero_index = if index < 0 {
            if let Ok(index) = index.try_into() {
                index + len
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

    let mut offsets = vec![O::usize_as(0)];

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        let start = offset_window[0];
        let end = offset_window[1];
        let len = end - start;

        // len 0 indicate array is null, return empty array in this row.
        if len == O::usize_as(0) {
            offsets.push(offsets[row_index]);
            continue;
        }

        // If index is null, we consider it as the minimum / maximum index of the array.
        let from_index = if from_array.is_null(row_index) {
            Some(O::usize_as(0))
        } else {
            adjusted_from_index::<O>(from_array.value(row_index), len)?
        };

        let to_index = if to_array.is_null(row_index) {
            Some(len - O::usize_as(1))
        } else {
            adjusted_to_index::<O>(to_array.value(row_index), len)?
        };

        if let (Some(from), Some(to)) = (from_index, to_index) {
            let stride = stride.map(|s| s.value(row_index));
            // Default stride is 1 if not provided
            let stride = stride.unwrap_or(1);
            if stride.is_zero() {
                return exec_err!(
                    "array_slice got invalid stride: {:?}, it cannot be 0",
                    stride
                );
            } else if (from <= to && stride.is_negative())
                || (from > to && stride.is_positive())
            {
                // return empty array
                offsets.push(offsets[row_index]);
                continue;
            }

            let stride: O = stride.try_into().map_err(|_| {
                internal_datafusion_err!("array_slice got invalid stride: {}", stride)
            })?;

            if from <= to {
                assert!(start + to <= end);
                if stride.eq(&O::one()) {
                    // stride is default to 1
                    mutable.extend(
                        0,
                        (start + from).to_usize().unwrap(),
                        (start + to + O::usize_as(1)).to_usize().unwrap(),
                    );
                    offsets.push(offsets[row_index] + (to - from + O::usize_as(1)));
                    continue;
                }
                let mut index = start + from;
                let mut cnt = 0;
                while index <= start + to {
                    mutable.extend(
                        0,
                        index.to_usize().unwrap(),
                        index.to_usize().unwrap() + 1,
                    );
                    index += stride;
                    cnt += 1;
                }
                offsets.push(offsets[row_index] + O::usize_as(cnt));
            } else {
                let mut index = start + from;
                let mut cnt = 0;
                while index >= start + to {
                    mutable.extend(
                        0,
                        index.to_usize().unwrap(),
                        index.to_usize().unwrap() + 1,
                    );
                    index += stride;
                    cnt += 1;
                }
                // invalid range, return empty array
                offsets.push(offsets[row_index] + O::usize_as(cnt));
            }
        } else {
            // invalid range, return empty array
            offsets.push(offsets[row_index]);
        }
    }

    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new("item", array.value_type(), true)),
        OffsetBuffer::<O>::new(offsets.into()),
        arrow_array::make_array(data),
        None,
    )?))
}

#[derive(Debug)]
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_pop_front_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_array_pop_front_doc())
    }
}

fn get_array_pop_front_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_ARRAY)
            .with_description(
                "Returns the array without the first element.",
            )
            .with_syntax_example("array_pop_front(array)")
            .with_sql_example(
                r#"```sql
> select array_pop_front([1, 2, 3]);
+-------------------------------+
| array_pop_front(List([1,2,3])) |
+-------------------------------+
| [2, 3]                        |
+-------------------------------+
```"#,
            )
            .with_argument(
                "array",
                "Array expression. Can be a constant, column, or function, and any combination of array operators.",
            )
            .build()
            .unwrap()
    })
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
        _ => exec_err!(
            "array_pop_front does not support type: {:?}",
            array_data_type
        ),
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

#[derive(Debug)]
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_pop_back_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_array_pop_back_doc())
    }
}

fn get_array_pop_back_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_ARRAY)
            .with_description(
                "Returns the array without the last element.",
            )
            .with_syntax_example("array_pop_back(array)")
            .with_sql_example(
                r#"```sql
> select array_pop_back([1, 2, 3]);
+-------------------------------+
| array_pop_back(List([1,2,3])) |
+-------------------------------+
| [1, 2]                        |
+-------------------------------+
```"#,
            )
            .with_argument(
                "array",
                "Array expression. Can be a constant, column, or function, and any combination of array operators.",
            )
            .build()
            .unwrap()
    })
}

/// array_pop_back SQL function
fn array_pop_back_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_pop_back needs one argument");
    }

    let array_data_type = args[0].data_type();
    match array_data_type {
        List(_) => {
            let array = as_list_array(&args[0])?;
            general_pop_back_list::<i32>(array)
        }
        LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            general_pop_back_list::<i64>(array)
        }
        _ => exec_err!(
            "array_pop_back does not support type: {:?}",
            array_data_type
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

#[derive(Debug)]
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
            List(field)
            | LargeList(field)
            | FixedSizeList(field, _) => Ok(field.data_type().clone()),
            _ => plan_err!(
                "array_any_value can only accept List, LargeList or FixedSizeList as the argument"
            ),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_any_value_inner)(args)
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_array_any_value_doc())
    }
}

fn get_array_any_value_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_ARRAY)
            .with_description(
                "Returns the first non-null element in the array.",
            )
            .with_syntax_example("array_any_value(array)")
            .with_sql_example(
                r#"```sql
> select array_any_value([NULL, 1, 2, 3]);
+-------------------------------+
| array_any_value(List([NULL,1,2,3])) |
+-------------------------------------+
| 1                                   |
+-------------------------------------+
```"#,
            )
            .with_argument(
                "array",
                "Array expression. Can be a constant, column, or function, and any combination of array operators.",
            )
            .build()
            .unwrap()
    })
}

fn array_any_value_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_any_value expects one argument");
    }

    match &args[0].data_type() {
        List(_) => {
            let array = as_list_array(&args[0])?;
            general_array_any_value::<i32>(array)
        }
        LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            general_array_any_value::<i64>(array)
        }
        data_type => exec_err!("array_any_value does not support type: {:?}", data_type),
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
