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
use arrow::compute::kernels;
use arrow::datatypes::DataType;
use arrow::row::{RowConverter, Rows, SortField};
use arrow_array::{Datum, GenericListArray, LargeStringArray, Scalar, StringArray, StringViewArray, UInt32Array};
use datafusion_common::cast::as_generic_list_array;
use datafusion_common::utils::string_utils::string_array_to_vec;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, Operator, ScalarUDFImpl, Signature, Volatility};

use datafusion_physical_expr_common::datum::compare_op_for_nested;
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
        // invoke_new(args)
        invoke_general_scalar(args)

        // make_scalar_function(array_has_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

pub fn invoke_eq_kernel(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    make_scalar_function(array_has_inner)(args)
}

pub fn invoke_general_kernel(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    make_scalar_function(array_has_inner_general_kerenl)(args)
}

pub fn invoke_iter(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    make_scalar_function(array_has_inner_simple_iter)(args)
}

pub fn invoke_new(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if let ColumnarValue::Scalar(s) = &args[1] {
        if s.is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
        }
    }

    // first, identify if any of the arguments is an Array. If yes, store its `len`,
    // as any scalar will need to be converted to an array of len `len`.
    let len = args
        .iter()
        .fold(Option::<usize>::None, |acc, arg| match arg {
            ColumnarValue::Scalar(_) => acc,
            ColumnarValue::Array(a) => Some(a.len()),
        });

    let is_scalar = len.is_none();

    let result = match args[1] {
        ColumnarValue::Array(_) => {
            let args = ColumnarValue::values_to_arrays(args)?;
            array_has_inner(&args)                
        }
        ColumnarValue::Scalar(_) => {
            let haystack = args[0].to_owned().into_array(1)?;
            if haystack.len() == 0 {
                return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
            }
            let needle = args[1].to_owned().into_array(1)?;
            let needle = Scalar::new(needle);
            array_has_inner_v2(&haystack, &needle)
        }
    };

    if is_scalar {
        // If all inputs are scalar, keeps output as scalar
        let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
        result.map(ColumnarValue::Scalar)
    } else {
        result.map(ColumnarValue::Array)
    }
}

pub fn invoke_general_scalar(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if let ColumnarValue::Scalar(s) = &args[1] {
        if s.is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
        }
    }

    // first, identify if any of the arguments is an Array. If yes, store its `len`,
    // as any scalar will need to be converted to an array of len `len`.
    let len = args
        .iter()
        .fold(Option::<usize>::None, |acc, arg| match arg {
            ColumnarValue::Scalar(_) => acc,
            ColumnarValue::Array(a) => Some(a.len()),
        });

    let is_scalar = len.is_none();

    let result = match args[1] {
        ColumnarValue::Array(_) => {
            let args = ColumnarValue::values_to_arrays(args)?;
            // array_has_inner(&args)                
            array_has_inner_general_kerenl_scalar(&args)
        }
        ColumnarValue::Scalar(_) => {
            let haystack = args[0].to_owned().into_array(1)?;
            if haystack.len() == 0 {
                return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
            }
            let needle = args[1].to_owned().into_array(1)?;
            let needle = Scalar::new(needle);
            array_has_inner_v2(&haystack, &needle)
        }
    };

    if is_scalar {
        // If all inputs are scalar, keeps output as scalar
        let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
        result.map(ColumnarValue::Scalar)
    } else {
        result.map(ColumnarValue::Array)
    }
}


fn array_has_inner_simple_iter(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::List(_) => array_has_dispatch::<i32>(&args[0], &args[1]),
        DataType::LargeList(_) => array_has_dispatch::<i64>(&args[0], &args[1]),
        _ => exec_err!(
            "array_has does not support type '{:?}'.",
            args[0].data_type()
        ),
    }
}

fn array_has_inner_v2(haystack: &ArrayRef, needle: &dyn Datum) -> Result<ArrayRef> {
    match haystack.data_type() {
        DataType::List(_) => array_has_dispatch_eq_kernel_v2::<i32>(haystack, needle),
        DataType::LargeList(_) => array_has_dispatch_eq_kernel_v2::<i64>(haystack, needle),
        _ => exec_err!(
            "array_has does not support type '{:?}'.",
            haystack.data_type()
        ),
    }
}

fn array_has_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::List(_) => array_has_dispatch_eq_kernel::<i32>(&args[0], &args[1]),
        DataType::LargeList(_) => array_has_dispatch_eq_kernel::<i64>(&args[0], &args[1]),
        _ => exec_err!(
            "array_has does not support type '{:?}'.",
            args[0].data_type()
        ),
    }
}

fn array_has_inner_general_kerenl(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::List(_) => array_has_dispatch_general_kernel::<i32>(&args[0], &args[1]),
        DataType::LargeList(_) => array_has_dispatch_general_kernel::<i64>(&args[0], &args[1]),
        _ => exec_err!(
            "array_has does not support type '{:?}'.",
            args[0].data_type()
        ),
    }
}

fn array_has_inner_general_kerenl_scalar(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::List(_) => array_has_dispatch_general_scalar::<i32>(&args[0], &args[1]),
        DataType::LargeList(_) => array_has_dispatch_general_scalar::<i64>(&args[0], &args[1]),
        _ => exec_err!(
            "array_has does not support type '{:?}'.",
            args[0].data_type()
        ),
    }
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
pub enum ComparisonType {
    // array_has_all
    All,
    // array_has_any
    Any,
}

pub fn array_has_dispatch<O: OffsetSizeTrait>(
    haystack: &ArrayRef,
    needle: &ArrayRef,
) -> Result<ArrayRef> {
    let haystack = as_generic_list_array::<O>(haystack)?;
    match needle.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            array_has_string_internal::<O>(haystack, needle)
        }
        _ => general_array_has::<O>(haystack, needle),
    }
}

pub fn array_has_dispatch_general_kernel<O: OffsetSizeTrait>(
    haystack: &ArrayRef,
    needle: &ArrayRef,
) -> Result<ArrayRef> {
    let haystack = as_generic_list_array::<O>(haystack)?;
    let values = haystack.values();
    let offsets = haystack.value_offsets();

    let mut indices = Vec::with_capacity(haystack.len());
    for i in 0..haystack.len() {
        let len = haystack.value_length(i).to_usize().unwrap();
        indices.extend(std::iter::repeat(i as u32).take(len));
    }

    let indices = Arc::new(UInt32Array::from(indices)) as ArrayRef;
    let elements = kernels::take::take(needle, &indices, None).unwrap();
    let eq_array = compare_op_for_nested(Operator::Eq, values, &elements)?;

    let mut final_contained = vec![None; haystack.len()];
    for (i, offset) in offsets.windows(2).enumerate() {
        let start = offset[0].to_usize().unwrap();
        let end = offset[1].to_usize().unwrap();
        let length = end - start;
        // For non-nested list, length is 0 for null
        if length == 0 {
            continue;
        }
        // For nested lsit, check number of nulls
        let null_count = eq_array.slice(start, length).null_count();
        if null_count == length {
            continue;
        }

        let number_of_true = eq_array.slice(start, length).true_count();
        if number_of_true > 0 {
            final_contained[i] = Some(true);
        } else {
            final_contained[i] = Some(false);
        }
    }

    Ok(Arc::new(BooleanArray::from(final_contained)))
}

pub fn array_has_dispatch_eq_kernel<O: OffsetSizeTrait>(
    haystack: &ArrayRef,
    needle: &ArrayRef,
) -> Result<ArrayRef> {
    let haystack = as_generic_list_array::<O>(haystack)?;
    let mut boolean_builder = BooleanArray::builder(haystack.len());
    for (arr, element) in haystack.iter().zip(string_array_to_vec(needle).into_iter()) {
        match (arr, element) {
            (Some(arr), Some(element)) => {
                // let scalar = ScalarValue::Utf8(Some(element.to_string()));
                let eq_array = match arr.data_type() {
                    DataType::Utf8 => {
                        let scalar = StringArray::new_scalar(element.to_string());
                        compare_op_for_nested(Operator::Eq, &arr, &scalar)?
                    }
                    DataType::LargeUtf8 => {
                        let scalar = LargeStringArray::new_scalar(element.to_string());
                        compare_op_for_nested(Operator::Eq, &arr, &scalar)?
                    }
                    DataType::Utf8View => {
                        let scalar = StringViewArray::new_scalar(element.to_string());
                        compare_op_for_nested(Operator::Eq, &arr, &scalar)?
                    }
                    _ => unreachable!("")
                };
                let is_contained = eq_array.true_count() > 0;
                boolean_builder.append_value(is_contained)
            }
            (_, _) => {
                boolean_builder.append_null();
            }
        }
    }

    Ok(Arc::new(boolean_builder.finish()))
}

pub fn array_has_dispatch_general_scalar<O: OffsetSizeTrait>(
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
        let needle_row = Scalar::new(needle.slice(i, 1));
        let eq_array = compare_op_for_nested(Operator::Eq, &arr, &needle_row)?;
        let is_contained = eq_array.true_count() > 0;
        boolean_builder.append_value(is_contained)
    } 

    // for (arr, element) in haystack.iter().zip(string_array_to_vec(needle).into_iter()) {
    //     match (arr, element) {
    //         (Some(arr), Some(element)) => {
    //             // let scalar = ScalarValue::Utf8(Some(element.to_string()));
    //             let eq_array = match arr.data_type() {
    //                 DataType::Utf8 => {
    //                     let scalar = StringArray::new_scalar(element.to_string());
    //                     compare_op_for_nested(Operator::Eq, &arr, &scalar)?
    //                 }
    //                 DataType::LargeUtf8 => {
    //                     let scalar = LargeStringArray::new_scalar(element.to_string());
    //                     compare_op_for_nested(Operator::Eq, &arr, &scalar)?
    //                 }
    //                 DataType::Utf8View => {
    //                     let scalar = StringViewArray::new_scalar(element.to_string());
    //                     compare_op_for_nested(Operator::Eq, &arr, &scalar)?
    //                 }
    //                 _ => unreachable!("")
    //             };
    //             let is_contained = eq_array.true_count() > 0;
    //             boolean_builder.append_value(is_contained)
    //         }
    //         (_, _) => {
    //             boolean_builder.append_null();
    //         }
    //     }
    // }

    Ok(Arc::new(boolean_builder.finish()))
}

pub fn array_has_dispatch_eq_kernel_v2<O: OffsetSizeTrait>(
    haystack: &ArrayRef,
    needle: &dyn Datum,
) -> Result<ArrayRef> {
    let haystack = as_generic_list_array::<O>(haystack)?;
    let values = haystack.values();
    let offsets = haystack.value_offsets();
    let eq_array = compare_op_for_nested(Operator::Eq, values, needle)?;

    let mut final_contained = vec![None; haystack.len()];
    for (i, offset) in offsets.windows(2).enumerate() {
        let start = offset[0].to_usize().unwrap();
        let end = offset[1].to_usize().unwrap();
        let length = end - start;
        // For non-nested list, length is 0 for null
        if length == 0 {
            continue;
        }
        // For nested lsit, check number of nulls
        let null_count = eq_array.slice(start, length).null_count();
        if null_count == length {
            continue;
        }

        let number_of_true = eq_array.slice(start, length).true_count();
        if number_of_true > 0 {
            final_contained[i] = Some(true);
        } else {
            final_contained[i] = Some(false);
        }
    }

    Ok(Arc::new(BooleanArray::from(final_contained)))
}

// fn create_take_indicies<O: OffsetSizeTrait>(
//     needle: &ArrayRef,
//     offsets: &[O],
// ) -> Result<ArrayRef> {
//     for offset in offsets.iter() {

//     }

//     let indices = UInt32Array::from(vec![2, 1]);

//     let mut builder = PrimitiveArray::<Int64Type>::builder(capacity);
//     for (index, repeat) in offsets.iter().enumerate() {
//         // The length array should not contain nulls, so unwrap is safe
//         let repeat = repeat.unwrap();
//         (0..repeat).for_each(|_| builder.append_value(index as i64));
//     }
//     builder.finish()
// }

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

fn array_has_string_internal<O: OffsetSizeTrait>(
    haystack: &GenericListArray<O>,
    needle: &ArrayRef,
) -> Result<ArrayRef> {
    let mut boolean_builder = BooleanArray::builder(haystack.len());
    for (arr, element) in haystack.iter().zip(string_array_to_vec(needle).into_iter()) {
        match (arr, element) {
            (Some(arr), Some(element)) => {
                boolean_builder.append_value(
                    string_array_to_vec(&arr)
                        .into_iter()
                        .flatten()
                        .any(|x| x == element),
                );
            }
            (_, _) => {
                boolean_builder.append_null();
            }
        }
    }

    Ok(Arc::new(boolean_builder.finish()))
}

pub fn general_array_has<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    needle: &ArrayRef,
) -> Result<ArrayRef> {
    let mut boolean_builder = BooleanArray::builder(array.len());
    let converter = RowConverter::new(vec![SortField::new(array.value_type())])?;
    let sub_arr_values = converter.convert_columns(&[Arc::clone(needle)])?;

    for (row_idx, arr) in array.iter().enumerate() {
        if let Some(arr) = arr {
            let arr_values = converter.convert_columns(&[arr])?;
            boolean_builder.append_value(
                arr_values
                    .iter()
                    .dedup()
                    .any(|x| x == sub_arr_values.row(row_idx)),
            );
        } else {
            boolean_builder.append_null();
        }
    }

    Ok(Arc::new(boolean_builder.finish()))
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
