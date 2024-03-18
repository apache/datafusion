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

//! [`ScalarUDFImpl`] definitions for array_position function.

use arrow_schema::DataType::{LargeList, List, UInt64};
use arrow_schema::{DataType, Field};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::Expr;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

use arrow_array::types::UInt64Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, GenericListArray, ListArray, OffsetSizeTrait, Scalar,
    UInt32Array, UInt64Array,
};
use datafusion_common::cast::{
    as_generic_list_array, as_int64_array, as_large_list_array, as_list_array,
};
use datafusion_common::{exec_err, internal_err};
use itertools::Itertools;

make_udf_function!(
    ArrayPosition,
    array_position,
    array element index,
    "searches for an element in the array, returns first occurrence.",
    array_position_udf
);

#[derive(Debug)]
pub(super) struct ArrayPosition {
    signature: Signature,
    aliases: Vec<String>,
}
impl ArrayPosition {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element_and_optional_index(
                Volatility::Immutable,
            ),
            aliases: vec![
                String::from("array_position"),
                String::from("list_position"),
                String::from("array_indexof"),
                String::from("list_indexof"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayPosition {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> datafusion_common::Result<DataType> {
        Ok(UInt64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        array_position_inner(&args).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Array_position SQL function
pub fn array_position_inner(args: &[ArrayRef]) -> datafusion_common::Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("array_position expects two or three arguments");
    }
    match &args[0].data_type() {
        List(_) => general_position_dispatch::<i32>(args),
        LargeList(_) => general_position_dispatch::<i64>(args),
        array_type => exec_err!("array_position does not support type '{array_type:?}'."),
    }
}
fn general_position_dispatch<O: OffsetSizeTrait>(
    args: &[ArrayRef],
) -> datafusion_common::Result<ArrayRef> {
    let list_array = as_generic_list_array::<O>(&args[0])?;
    let element_array = &args[1];

    crate::utils::check_datatypes(
        "array_position",
        &[list_array.values(), element_array],
    )?;

    let arr_from = if args.len() == 3 {
        as_int64_array(&args[2])?
            .values()
            .to_vec()
            .iter()
            .map(|&x| x - 1)
            .collect::<Vec<_>>()
    } else {
        vec![0; list_array.len()]
    };

    // if `start_from` index is out of bounds, return error
    for (arr, &from) in list_array.iter().zip(arr_from.iter()) {
        if let Some(arr) = arr {
            if from < 0 || from as usize >= arr.len() {
                return internal_err!("start_from index out of bounds");
            }
        } else {
            // We will get null if we got null in the array, so we don't need to check
        }
    }

    generic_position::<O>(list_array, element_array, arr_from)
}

fn generic_position<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    element_array: &ArrayRef,
    arr_from: Vec<i64>, // 0-indexed
) -> datafusion_common::Result<ArrayRef> {
    let mut data = Vec::with_capacity(list_array.len());

    for (row_index, (list_array_row, &from)) in
        list_array.iter().zip(arr_from.iter()).enumerate()
    {
        let from = from as usize;

        if let Some(list_array_row) = list_array_row {
            let eq_array =
                compare_element_to_list(&list_array_row, element_array, row_index, true)?;

            // Collect `true`s in 1-indexed positions
            let index = eq_array
                .iter()
                .skip(from)
                .position(|e| e == Some(true))
                .map(|index| (from + index + 1) as u64);

            data.push(index);
        } else {
            data.push(None);
        }
    }

    Ok(Arc::new(UInt64Array::from(data)))
}

/// Computes a BooleanArray indicating equality or inequality between elements in a list array and a specified element array.
///
/// # Arguments
///
/// * `list_array_row` - A reference to a trait object implementing the Arrow `Array` trait. It represents the list array for which the equality or inequality will be compared.
///
/// * `element_array` - A reference to a trait object implementing the Arrow `Array` trait. It represents the array with which each element in the `list_array_row` will be compared.
///
/// * `row_index` - The index of the row in the `element_array` and `list_array` to use for the comparison.
///
/// * `eq` - A boolean flag. If `true`, the function computes equality; if `false`, it computes inequality.
///
/// # Returns
///
/// Returns a `Result<BooleanArray>` representing the comparison results. The result may contain an error if there are issues with the computation.
///
/// # Example
///
/// ```text
/// compare_element_to_list(
///     [1, 2, 3], [1, 2, 3], 0, true => [true, false, false]
///     [1, 2, 3, 3, 2, 1], [1, 2, 3], 1, true => [false, true, false, false, true, false]
///
///     [[1, 2, 3], [2, 3, 4], [3, 4, 5]], [[1, 2, 3], [2, 3, 4], [3, 4, 5]], 0, true => [true, false, false]
///     [[1, 2, 3], [2, 3, 4], [2, 3, 4]], [[1, 2, 3], [2, 3, 4], [3, 4, 5]], 1, false => [true, false, false]
/// )
/// ```
fn compare_element_to_list(
    list_array_row: &dyn Array,
    element_array: &dyn Array,
    row_index: usize,
    eq: bool,
) -> datafusion_common::Result<BooleanArray> {
    if list_array_row.data_type() != element_array.data_type() {
        return exec_err!(
            "compare_element_to_list received incompatible types: '{:?}' and '{:?}'.",
            list_array_row.data_type(),
            element_array.data_type()
        );
    }

    let indices = UInt32Array::from(vec![row_index as u32]);
    let element_array_row = arrow::compute::take(element_array, &indices, None)?;

    // Compute all positions in list_row_array (that is itself an
    // array) that are equal to `from_array_row`
    let res = match element_array_row.data_type() {
        // arrow_ord::cmp::eq does not support ListArray, so we need to compare it by loop
        DataType::List(_) => {
            // compare each element of the from array
            let element_array_row_inner = as_list_array(&element_array_row)?.value(0);
            let list_array_row_inner = as_list_array(list_array_row)?;

            list_array_row_inner
                .iter()
                // compare element by element the current row of list_array
                .map(|row| {
                    row.map(|row| {
                        if eq {
                            row.eq(&element_array_row_inner)
                        } else {
                            row.ne(&element_array_row_inner)
                        }
                    })
                })
                .collect::<BooleanArray>()
        }
        DataType::LargeList(_) => {
            // compare each element of the from array
            let element_array_row_inner =
                as_large_list_array(&element_array_row)?.value(0);
            let list_array_row_inner = as_large_list_array(list_array_row)?;

            list_array_row_inner
                .iter()
                // compare element by element the current row of list_array
                .map(|row| {
                    row.map(|row| {
                        if eq {
                            row.eq(&element_array_row_inner)
                        } else {
                            row.ne(&element_array_row_inner)
                        }
                    })
                })
                .collect::<BooleanArray>()
        }
        _ => {
            let element_arr = Scalar::new(element_array_row);
            // use not_distinct so we can compare NULL
            if eq {
                arrow::compute::kernels::cmp::not_distinct(&list_array_row, &element_arr)?
            } else {
                arrow::compute::kernels::cmp::distinct(&list_array_row, &element_arr)?
            }
        }
    };

    Ok(res)
}

make_udf_function!(
    ArrayPositions,
    array_positions,
    array element, // arg name
    "searches for an element in the array, returns all occurrences.", // doc
    array_positions_udf // internal function name
);
#[derive(Debug)]
pub(super) struct ArrayPositions {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayPositions {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element(Volatility::Immutable),
            aliases: vec![
                String::from("array_positions"),
                String::from("list_positions"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayPositions {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_positions"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> datafusion_common::Result<DataType> {
        Ok(List(Arc::new(Field::new("item", UInt64, true))))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        array_positions_inner(&args).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Array_positions SQL function
pub fn array_positions_inner(args: &[ArrayRef]) -> datafusion_common::Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_positions expects two arguments");
    }

    let element = &args[1];

    match &args[0].data_type() {
        DataType::List(_) => {
            let arr = as_list_array(&args[0])?;
            crate::utils::check_datatypes("array_positions", &[arr.values(), element])?;
            general_positions::<i32>(arr, element)
        }
        DataType::LargeList(_) => {
            let arr = as_large_list_array(&args[0])?;
            crate::utils::check_datatypes("array_positions", &[arr.values(), element])?;
            general_positions::<i64>(arr, element)
        }
        array_type => {
            exec_err!("array_positions does not support type '{array_type:?}'.")
        }
    }
}

fn general_positions<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    element_array: &ArrayRef,
) -> datafusion_common::Result<ArrayRef> {
    let mut data = Vec::with_capacity(list_array.len());

    for (row_index, list_array_row) in list_array.iter().enumerate() {
        if let Some(list_array_row) = list_array_row {
            let eq_array =
                compare_element_to_list(&list_array_row, element_array, row_index, true)?;

            // Collect `true`s in 1-indexed positions
            let indexes = eq_array
                .iter()
                .positions(|e| e == Some(true))
                .map(|index| Some(index as u64 + 1))
                .collect::<Vec<_>>();

            data.push(Some(indexes));
        } else {
            data.push(None);
        }
    }

    Ok(Arc::new(
        ListArray::from_iter_primitive::<UInt64Type, _, _>(data),
    ))
}
