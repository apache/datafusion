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

//! [`ScalarUDFImpl`] definitions for array_position and array_positions functions.

use arrow::datatypes::DataType;
use arrow::datatypes::{
    DataType::{LargeList, List, UInt64},
    Field,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    types::UInt64Type, Array, ArrayRef, GenericListArray, ListArray, OffsetSizeTrait,
    UInt64Array,
};
use datafusion_common::cast::{
    as_generic_list_array, as_int64_array, as_large_list_array, as_list_array,
};
use datafusion_common::{
    assert_or_internal_err, exec_err, utils::take_function_args, Result,
};
use itertools::Itertools;

use crate::utils::{compare_element_to_list, make_scalar_function};

make_udf_expr_and_func!(
    ArrayPosition,
    array_position,
    array element index,
    "searches for an element in the array, returns first occurrence.",
    array_position_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the position of the first occurrence of the specified element in the array, or NULL if not found.",
    syntax_example = "array_position(array, element)\narray_position(array, element, index)",
    sql_example = r#"```sql
> select array_position([1, 2, 2, 3, 1, 4], 2);
+----------------------------------------------+
| array_position(List([1,2,2,3,1,4]),Int64(2)) |
+----------------------------------------------+
| 2                                            |
+----------------------------------------------+
> select array_position([1, 2, 2, 3, 1, 4], 2, 3);
+----------------------------------------------------+
| array_position(List([1,2,2,3,1,4]),Int64(2), Int64(3)) |
+----------------------------------------------------+
| 3                                                  |
+----------------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "element",
        description = "Element to search for position in the array."
    ),
    argument(
        name = "index",
        description = "Index at which to start searching (1-indexed)."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayPosition {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayPosition {
    fn default() -> Self {
        Self::new()
    }
}
impl ArrayPosition {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element_and_optional_index(
                Volatility::Immutable,
            ),
            aliases: vec![
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(UInt64)
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_position_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_position_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("array_position expects two or three arguments");
    }
    match &args[0].data_type() {
        List(_) => general_position_dispatch::<i32>(args),
        LargeList(_) => general_position_dispatch::<i64>(args),
        array_type => exec_err!("array_position does not support type '{array_type}'."),
    }
}

fn general_position_dispatch<O: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
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
        // If `arr` is `None`: we will get null if we got null in the array, so we don't need to check
        assert_or_internal_err!(
            arr.is_none_or(|arr| from >= 0 && (from as usize) <= arr.len()),
            "start_from index out of bounds"
        );
    }

    generic_position::<O>(list_array, element_array, &arr_from)
}

fn generic_position<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    element_array: &ArrayRef,
    arr_from: &[i64], // 0-indexed
) -> Result<ArrayRef> {
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

make_udf_expr_and_func!(
    ArrayPositions,
    array_positions,
    array element, // arg name
    "searches for an element in the array, returns all occurrences.", // doc
    array_positions_udf // internal function name
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Searches for an element in the array, returns all occurrences.",
    syntax_example = "array_positions(array, element)",
    sql_example = r#"```sql
> select array_positions([1, 2, 2, 3, 1, 4], 2);
+-----------------------------------------------+
| array_positions(List([1,2,2,3,1,4]),Int64(2)) |
+-----------------------------------------------+
| [2, 3]                                        |
+-----------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "element",
        description = "Element to search for position in the array."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct ArrayPositions {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayPositions {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element(Volatility::Immutable),
            aliases: vec![String::from("list_positions")],
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(List(Arc::new(Field::new_list_field(UInt64, true))))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_positions_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_positions_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array, element] = take_function_args("array_positions", args)?;

    match &array.data_type() {
        List(_) => {
            let arr = as_list_array(&array)?;
            crate::utils::check_datatypes("array_positions", &[arr.values(), element])?;
            general_positions::<i32>(arr, element)
        }
        LargeList(_) => {
            let arr = as_large_list_array(&array)?;
            crate::utils::check_datatypes("array_positions", &[arr.values(), element])?;
            general_positions::<i64>(arr, element)
        }
        array_type => {
            exec_err!("array_positions does not support type '{array_type}'.")
        }
    }
}

fn general_positions<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    element_array: &ArrayRef,
) -> Result<ArrayRef> {
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
