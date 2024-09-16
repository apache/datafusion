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

use arrow::array::{Array, ArrayRef, BooleanArray, OffsetSizeTrait};
use arrow::datatypes::DataType;
use arrow::row::{RowConverter, Rows, SortField};
use arrow_array::{Datum, GenericListArray, Scalar};
use arrow_buffer::BooleanBuffer;
use datafusion_common::cast::as_generic_list_array;
use datafusion_common::utils::string_utils::string_array_to_vec;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_physical_expr_common::datum::compare_with_eq;
use itertools::Itertools;

use crate::utils::make_scalar_function;

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
        match &args[1] {
            ColumnarValue::Array(array_needle) => {
                // the needle is already an array, convert the haystack to an array of the same length
                let haystack = args[0].to_owned().into_array(array_needle.len())?;
                let array = array_has_inner_for_array(&haystack, array_needle)?;
                Ok(ColumnarValue::Array(array))
            }
            ColumnarValue::Scalar(scalar_needle) => {
                // Always return null if the second argument is null
                // i.e. array_has(array, null) -> null
                if scalar_needle.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
                }

                // since the needle is a scalar, convert it to an array of size 1
                let haystack = args[0].to_owned().into_array(1)?;
                let needle = scalar_needle.to_array_of_size(1)?;
                let needle = Scalar::new(needle);
                let array = array_has_inner_for_scalar(&haystack, &needle)?;
                if let ColumnarValue::Scalar(_) = &args[0] {
                    // If both inputs are scalar, keeps output as scalar
                    let scalar_value = ScalarValue::try_from_array(&array, 0)?;
                    Ok(ColumnarValue::Scalar(scalar_value))
                } else {
                    Ok(ColumnarValue::Array(array))
                }
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn array_has_inner_for_scalar(
    haystack: &ArrayRef,
    needle: &dyn Datum,
) -> Result<ArrayRef> {
    match haystack.data_type() {
        DataType::List(_) => array_has_dispatch_for_scalar::<i32>(haystack, needle),
        DataType::LargeList(_) => array_has_dispatch_for_scalar::<i64>(haystack, needle),
        _ => exec_err!(
            "array_has does not support type '{:?}'.",
            haystack.data_type()
        ),
    }
}

fn array_has_inner_for_array(haystack: &ArrayRef, needle: &ArrayRef) -> Result<ArrayRef> {
    match haystack.data_type() {
        DataType::List(_) => array_has_dispatch_for_array::<i32>(haystack, needle),
        DataType::LargeList(_) => array_has_dispatch_for_array::<i64>(haystack, needle),
        _ => exec_err!(
            "array_has does not support type '{:?}'.",
            haystack.data_type()
        ),
    }
}

fn array_has_dispatch_for_array<O: OffsetSizeTrait>(
    haystack: &ArrayRef,
    needle: &ArrayRef,
) -> Result<ArrayRef> {
    let haystack = as_generic_list_array::<O>(haystack)?;
    let mut boolean_builder = BooleanArray::builder(haystack.len());

    for (i, arr) in haystack.iter().enumerate() {
        if arr.is_none() || needle.is_null(i) {
            boolean_builder.append_null();
            continue;
        }
        let arr = arr.unwrap();
        let is_nested = arr.data_type().is_nested();
        let needle_row = Scalar::new(needle.slice(i, 1));
        let eq_array = compare_with_eq(&arr, &needle_row, is_nested)?;
        let is_contained = eq_array.true_count() > 0;
        boolean_builder.append_value(is_contained)
    }

    Ok(Arc::new(boolean_builder.finish()))
}

fn array_has_dispatch_for_scalar<O: OffsetSizeTrait>(
    haystack: &ArrayRef,
    needle: &dyn Datum,
) -> Result<ArrayRef> {
    let haystack = as_generic_list_array::<O>(haystack)?;
    let values = haystack.values();
    let is_nested = values.data_type().is_nested();
    let offsets = haystack.value_offsets();
    // If first argument is empty list (second argument is non-null), return false
    // i.e. array_has([], non-null element) -> false
    if values.len() == 0 {
        return Ok(Arc::new(BooleanArray::new(
            BooleanBuffer::new_unset(haystack.len()),
            None,
        )));
    }
    let eq_array = compare_with_eq(values, needle, is_nested)?;
    let mut final_contained = vec![None; haystack.len()];
    for (i, offset) in offsets.windows(2).enumerate() {
        let start = offset[0].to_usize().unwrap();
        let end = offset[1].to_usize().unwrap();
        let length = end - start;
        // For non-nested list, length is 0 for null
        if length == 0 {
            continue;
        }
        let sliced_array = eq_array.slice(start, length);
        // For nested list, check number of nulls
        if sliced_array.null_count() != length {
            final_contained[i] = Some(sliced_array.true_count() > 0);
        }
    }

    Ok(Arc::new(BooleanArray::from(final_contained)))
}

fn array_has_all_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::List(_) => {
            array_has_all_and_any_dispatch::<i32>(&args[0], &args[1], ComparisonType::All)
        }
        DataType::LargeList(_) => {
            array_has_all_and_any_dispatch::<i64>(&args[0], &args[1], ComparisonType::All)
        }
        _ => exec_err!(
            "array_has does not support type '{:?}'.",
            args[0].data_type()
        ),
    }
}

fn array_has_any_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::List(_) => {
            array_has_all_and_any_dispatch::<i32>(&args[0], &args[1], ComparisonType::Any)
        }
        DataType::LargeList(_) => {
            array_has_all_and_any_dispatch::<i64>(&args[0], &args[1], ComparisonType::Any)
        }
        _ => exec_err!(
            "array_has does not support type '{:?}'.",
            args[0].data_type()
        ),
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
        make_scalar_function(array_has_all_inner)(args)
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
        make_scalar_function(array_has_any_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Represents the type of comparison for array_has.
#[derive(Debug, PartialEq, Clone, Copy)]
enum ComparisonType {
    // array_has_all
    All,
    // array_has_any
    Any,
}

fn array_has_all_and_any_dispatch<O: OffsetSizeTrait>(
    haystack: &ArrayRef,
    needle: &ArrayRef,
    comparison_type: ComparisonType,
) -> Result<ArrayRef> {
    let haystack = as_generic_list_array::<O>(haystack)?;
    let needle = as_generic_list_array::<O>(needle)?;
    match needle.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            array_has_all_and_any_string_internal::<O>(haystack, needle, comparison_type)
        }
        _ => general_array_has_for_all_and_any::<O>(haystack, needle, comparison_type),
    }
}

// String comparison for array_has_all and array_has_any
fn array_has_all_and_any_string_internal<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    needle: &GenericListArray<O>,
    comparison_type: ComparisonType,
) -> Result<ArrayRef> {
    let mut boolean_builder = BooleanArray::builder(array.len());
    for (arr, sub_arr) in array.iter().zip(needle.iter()) {
        match (arr, sub_arr) {
            (Some(arr), Some(sub_arr)) => {
                let haystack_array = string_array_to_vec(&arr);
                let needle_array = string_array_to_vec(&sub_arr);
                boolean_builder.append_value(array_has_string_kernel(
                    haystack_array,
                    needle_array,
                    comparison_type,
                ));
            }
            (_, _) => {
                boolean_builder.append_null();
            }
        }
    }

    Ok(Arc::new(boolean_builder.finish()))
}

fn array_has_string_kernel(
    haystack: Vec<Option<&str>>,
    needle: Vec<Option<&str>>,
    comparison_type: ComparisonType,
) -> bool {
    match comparison_type {
        ComparisonType::All => needle
            .iter()
            .dedup()
            .all(|x| haystack.iter().dedup().any(|y| y == x)),
        ComparisonType::Any => needle
            .iter()
            .dedup()
            .any(|x| haystack.iter().dedup().any(|y| y == x)),
    }
}

// General row comparison for array_has_all and array_has_any
fn general_array_has_for_all_and_any<O: OffsetSizeTrait>(
    haystack: &GenericListArray<O>,
    needle: &GenericListArray<O>,
    comparison_type: ComparisonType,
) -> Result<ArrayRef> {
    let mut boolean_builder = BooleanArray::builder(haystack.len());
    let converter = RowConverter::new(vec![SortField::new(haystack.value_type())])?;

    for (arr, sub_arr) in haystack.iter().zip(needle.iter()) {
        if let (Some(arr), Some(sub_arr)) = (arr, sub_arr) {
            let arr_values = converter.convert_columns(&[arr])?;
            let sub_arr_values = converter.convert_columns(&[sub_arr])?;
            boolean_builder.append_value(general_array_has_all_and_any_kernel(
                arr_values,
                sub_arr_values,
                comparison_type,
            ));
        } else {
            boolean_builder.append_null();
        }
    }

    Ok(Arc::new(boolean_builder.finish()))
}

fn general_array_has_all_and_any_kernel(
    haystack_rows: Rows,
    needle_rows: Rows,
    comparison_type: ComparisonType,
) -> bool {
    match comparison_type {
        ComparisonType::All => needle_rows.iter().all(|needle_row| {
            haystack_rows
                .iter()
                .any(|haystack_row| haystack_row == needle_row)
        }),
        ComparisonType::Any => needle_rows.iter().any(|needle_row| {
            haystack_rows
                .iter()
                .any(|haystack_row| haystack_row == needle_row)
        }),
    }
}
