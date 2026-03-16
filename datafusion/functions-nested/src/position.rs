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

use arrow::array::Scalar;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use arrow::datatypes::{
    DataType::{LargeList, List, UInt64},
    Field,
};
use datafusion_common::ScalarValue;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, GenericListArray, ListArray, OffsetSizeTrait, UInt64Array,
    types::UInt64Type,
};
use datafusion_common::cast::{
    as_generic_list_array, as_int64_array, as_large_list_array, as_list_array,
};
use datafusion_common::{Result, exec_err, utils::take_function_args};
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
    description = "Returns the position of the first occurrence of the specified element in the array, or NULL if not found. Comparisons are done using `IS DISTINCT FROM` semantics, so NULL is considered to match NULL.",
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
    argument(name = "element", description = "Element to search for in the array."),
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match try_array_position_scalar(&args.args)? {
            Some(result) => Ok(result),
            None => make_scalar_function(array_position_inner)(&args.args),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Attempts the scalar-needle fast path for `array_position`.
fn try_array_position_scalar(args: &[ColumnarValue]) -> Result<Option<ColumnarValue>> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("array_position expects two or three arguments");
    }

    // Fallback to the generic code path if the needle is an array
    let scalar_needle = match &args[1] {
        ColumnarValue::Scalar(s) => s,
        ColumnarValue::Array(_) => return Ok(None),
    };

    // `not_distinct` doesn't support nested types (List, Struct, etc.),
    // so fall back to the generic code path for those.
    if scalar_needle.data_type().is_nested() {
        return Ok(None);
    }

    // Determine batch length from whichever argument is columnar;
    // if all inputs are scalar, batch length is 1.
    let (num_rows, all_inputs_scalar) = match (&args[0], args.get(2)) {
        (ColumnarValue::Array(a), _) => (a.len(), false),
        (_, Some(ColumnarValue::Array(a))) => (a.len(), false),
        _ => (1, true),
    };

    let needle = scalar_needle.to_array_of_size(1)?;
    let haystack = args[0].to_array(num_rows)?;
    let arr_from = resolve_start_from(args.get(2), num_rows)?;

    let result = match haystack.data_type() {
        List(_) => {
            let list = as_list_array(&haystack)?;
            array_position_scalar::<i32>(list, &needle, &arr_from)
        }
        LargeList(_) => {
            let list = as_large_list_array(&haystack)?;
            array_position_scalar::<i64>(list, &needle, &arr_from)
        }
        t => exec_err!("array_position does not support type '{t}'"),
    }?;

    if all_inputs_scalar {
        Ok(Some(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?)))
    } else {
        Ok(Some(ColumnarValue::Array(result)))
    }
}

fn array_position_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("array_position expects two or three arguments");
    }
    match &args[0].data_type() {
        List(_) => general_position_dispatch::<i32>(args),
        LargeList(_) => general_position_dispatch::<i64>(args),
        dt => exec_err!("array_position does not support type '{dt}'"),
    }
}

/// Resolves the optional `start_from` argument into a `Vec<i64>` of
/// 0-indexed starting positions.
fn resolve_start_from(
    third_arg: Option<&ColumnarValue>,
    num_rows: usize,
) -> Result<Vec<i64>> {
    match third_arg {
        None => Ok(vec![0i64; num_rows]),
        Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(v)))) => {
            Ok(vec![v - 1; num_rows])
        }
        Some(ColumnarValue::Scalar(s)) => {
            exec_err!("array_position expected Int64 for start_from, got {s}")
        }
        Some(ColumnarValue::Array(a)) => {
            Ok(as_int64_array(a)?.values().iter().map(|&x| x - 1).collect())
        }
    }
}

/// Fast path for `array_position` when the needle is scalar.
///
/// Performs a single bulk `not_distinct` comparison of the needle against the
/// entire flat values buffer, then walks the result bitmap using offsets to
/// find per-row first-match positions.
fn array_position_scalar<O: OffsetSizeTrait>(
    haystack: &GenericListArray<O>,
    needle: &ArrayRef,
    arr_from: &[i64], // 0-indexed
) -> Result<ArrayRef> {
    crate::utils::check_datatypes("array_position", &[haystack.values(), needle])?;

    if haystack.len() == 0 {
        return Ok(Arc::new(UInt64Array::new_null(0)));
    }

    let needle_datum = Scalar::new(Arc::clone(needle));
    let validity = haystack.nulls();

    // Only convert the visible portion of the values array. For sliced
    // ListArrays, values() returns the full underlying array but only
    // elements between the first and last offset are referenced.
    let offsets = haystack.offsets();
    let first_offset = offsets[0].as_usize();
    let last_offset = offsets[haystack.len()].as_usize();
    let visible_values = haystack
        .values()
        .slice(first_offset, last_offset - first_offset);

    // `not_distinct` treats NULL=NULL as true, matching the semantics of
    // `array_position`.
    let eq_array = arrow_ord::cmp::not_distinct(&visible_values, &needle_datum)?;
    let eq_bits = eq_array.values();

    let mut result: Vec<Option<u64>> = Vec::with_capacity(haystack.len());
    let mut matches = eq_bits.set_indices().peekable();

    // Match positions are relative to visible_values (0-based), so
    // subtract first_offset from each offset when comparing.
    for i in 0..haystack.len() {
        let start = offsets[i].as_usize() - first_offset;
        let end = offsets[i + 1].as_usize() - first_offset;

        if validity.is_some_and(|v| v.is_null(i)) {
            // Null row -> null output; advance past matches in range
            while matches.peek().is_some_and(|&p| p < end) {
                matches.next();
            }
            result.push(None);
            continue;
        }

        let from = arr_from[i];
        let row_len = end - start;
        if !(from >= 0 && (from as usize) <= row_len) {
            return exec_err!("start_from out of bounds: {}", from + 1);
        }
        let search_start = start + from as usize;

        // Advance past matches before search_start
        while matches.peek().is_some_and(|&p| p < search_start) {
            matches.next();
        }

        // First match in [search_start, end)?
        if matches.peek().is_some_and(|&p| p < end) {
            let pos = *matches.peek().unwrap();
            result.push(Some((pos - start + 1) as u64));
            // Advance past remaining matches in this row
            while matches.peek().is_some_and(|&p| p < end) {
                matches.next();
            }
        } else {
            result.push(None);
        }
    }

    debug_assert_eq!(result.len(), haystack.len());
    Ok(Arc::new(UInt64Array::from(result)))
}

fn general_position_dispatch<O: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let haystack = as_generic_list_array::<O>(&args[0])?;
    let needle = &args[1];

    crate::utils::check_datatypes("array_position", &[haystack.values(), needle])?;

    let arr_from = if args.len() == 3 {
        as_int64_array(&args[2])?
            .values()
            .iter()
            .map(|&x| x - 1)
            .collect::<Vec<_>>()
    } else {
        vec![0; haystack.len()]
    };

    for (row, &from) in haystack.iter().zip(arr_from.iter()) {
        if !row.is_none_or(|row| from >= 0 && (from as usize) <= row.len()) {
            return exec_err!("start_from out of bounds: {}", from + 1);
        }
    }

    generic_position::<O>(haystack, needle, &arr_from)
}

fn generic_position<O: OffsetSizeTrait>(
    haystack: &GenericListArray<O>,
    needle: &ArrayRef,
    arr_from: &[i64], // 0-indexed
) -> Result<ArrayRef> {
    let mut data = Vec::with_capacity(haystack.len());

    for (row_index, (row, &from)) in haystack.iter().zip(arr_from.iter()).enumerate() {
        let from = from as usize;

        if let Some(row) = row {
            let eq_array = compare_element_to_list(&row, needle, row_index, true)?;

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
    argument(name = "element", description = "Element to search for in the array.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayPositions {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayPositions {
    fn default() -> Self {
        Self::new()
    }
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match try_array_positions_scalar(&args.args)? {
            Some(result) => Ok(result),
            None => make_scalar_function(array_positions_inner)(&args.args),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Attempts the scalar-needle fast path for `array_positions`.
fn try_array_positions_scalar(args: &[ColumnarValue]) -> Result<Option<ColumnarValue>> {
    let [haystack_arg, needle_arg] = take_function_args("array_positions", args)?;

    let scalar_needle = match needle_arg {
        ColumnarValue::Scalar(s) => s,
        ColumnarValue::Array(_) => return Ok(None),
    };

    // `not_distinct` doesn't support nested types (List, Struct, etc.),
    // so fall back to the per-row path for those.
    if scalar_needle.data_type().is_nested() {
        return Ok(None);
    }

    let (num_rows, all_inputs_scalar) = match haystack_arg {
        ColumnarValue::Array(a) => (a.len(), false),
        ColumnarValue::Scalar(_) => (1, true),
    };

    let needle = scalar_needle.to_array_of_size(1)?;
    let haystack = haystack_arg.to_array(num_rows)?;

    let result = match haystack.data_type() {
        List(_) => {
            let list = as_list_array(&haystack)?;
            array_positions_scalar::<i32>(list, &needle)
        }
        LargeList(_) => {
            let list = as_large_list_array(&haystack)?;
            array_positions_scalar::<i64>(list, &needle)
        }
        t => exec_err!("array_positions does not support type '{t}'"),
    }?;

    if all_inputs_scalar {
        Ok(Some(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?)))
    } else {
        Ok(Some(ColumnarValue::Array(result)))
    }
}

fn array_positions_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [haystack, needle] = take_function_args("array_positions", args)?;

    match &haystack.data_type() {
        List(_) => general_positions::<i32>(as_list_array(&haystack)?, needle),
        LargeList(_) => general_positions::<i64>(as_large_list_array(&haystack)?, needle),
        dt => exec_err!("array_positions does not support type '{dt}'"),
    }
}

fn general_positions<O: OffsetSizeTrait>(
    haystack: &GenericListArray<O>,
    needle: &ArrayRef,
) -> Result<ArrayRef> {
    crate::utils::check_datatypes("array_positions", &[haystack.values(), needle])?;
    let mut data = Vec::with_capacity(haystack.len());

    for (row_index, row) in haystack.iter().enumerate() {
        if let Some(row) = row {
            let eq_array = compare_element_to_list(&row, needle, row_index, true)?;

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

/// Fast path for `array_positions` when the needle is scalar.
///
/// Performs a single bulk `not_distinct` comparison of the needle against the
/// entire flat values buffer, then walks the result bitmap using offsets to
/// collect all per-row match positions.
fn array_positions_scalar<O: OffsetSizeTrait>(
    haystack: &GenericListArray<O>,
    needle: &ArrayRef,
) -> Result<ArrayRef> {
    crate::utils::check_datatypes("array_positions", &[haystack.values(), needle])?;

    let num_rows = haystack.len();
    if num_rows == 0 {
        return Ok(Arc::new(ListArray::try_new(
            Arc::new(Field::new_list_field(UInt64, true)),
            OffsetBuffer::new_zeroed(1),
            Arc::new(UInt64Array::from(Vec::<u64>::new())),
            None,
        )?));
    }

    let needle_datum = Scalar::new(Arc::clone(needle));
    let validity = haystack.nulls();

    // Only convert the visible portion of the values array. For sliced
    // ListArrays, values() returns the full underlying array but only
    // elements between the first and last offset are referenced.
    let offsets = haystack.offsets();
    let first_offset = offsets[0].as_usize();
    let last_offset = offsets[num_rows].as_usize();
    let visible_values = haystack
        .values()
        .slice(first_offset, last_offset - first_offset);

    // `not_distinct` treats NULL=NULL as true, matching the semantics of
    // `array_positions`.
    let eq_array = arrow_ord::cmp::not_distinct(&visible_values, &needle_datum)?;
    let eq_bits = eq_array.values();

    let num_matches = eq_bits.count_set_bits();
    let mut positions: Vec<u64> = Vec::with_capacity(num_matches);
    let mut result_offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
    result_offsets.push(0);
    let mut matches = eq_bits.set_indices().peekable();

    // Match positions are relative to visible_values (0-based), so
    // subtract first_offset from each offset when comparing.
    for i in 0..num_rows {
        let start = offsets[i].as_usize() - first_offset;
        let end = offsets[i + 1].as_usize() - first_offset;

        if validity.is_some_and(|v| v.is_null(i)) {
            // Null row -> null output; advance past matches in range.
            while matches.peek().is_some_and(|&p| p < end) {
                matches.next();
            }
            result_offsets.push(positions.len() as i32);
            continue;
        }

        // Collect all matches in [start, end).
        while let Some(pos) = matches.next_if(|&p| p < end) {
            positions.push((pos - start + 1) as u64);
        }
        result_offsets.push(positions.len() as i32);
    }

    debug_assert_eq!(result_offsets.len(), num_rows + 1);
    Ok(Arc::new(ListArray::try_new(
        Arc::new(Field::new_list_field(UInt64, true)),
        OffsetBuffer::new(result_offsets.into()),
        Arc::new(UInt64Array::from(positions)),
        validity.cloned(),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::AsArray;
    use arrow::datatypes::Int32Type;
    use datafusion_common::config::ConfigOptions;

    #[test]
    fn test_array_position_sliced_list() -> Result<()> {
        // [[10, 20], [30, 40], [50, 60], [70, 80]]  →  slice(1,2)  →  [[30, 40], [50, 60]]
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(10), Some(20)]),
            Some(vec![Some(30), Some(40)]),
            Some(vec![Some(50), Some(60)]),
            Some(vec![Some(70), Some(80)]),
        ]);
        let sliced = list.slice(1, 2);
        let haystack_field =
            Arc::new(Field::new("haystack", sliced.data_type().clone(), true));
        let needle_field = Arc::new(Field::new("needle", DataType::Int32, true));
        let return_field = Arc::new(Field::new("return", UInt64, true));

        // Search for elements that exist only in sliced-away rows:
        // 10 is in the prefix row, 70 is in the suffix row.
        let invoke = |needle: i32| -> Result<ArrayRef> {
            ArrayPosition::new()
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Array(Arc::new(sliced.clone())),
                        ColumnarValue::Scalar(ScalarValue::Int32(Some(needle))),
                    ],
                    arg_fields: vec![
                        Arc::clone(&haystack_field),
                        Arc::clone(&needle_field),
                    ],
                    number_rows: 2,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::new(ConfigOptions::default()),
                })?
                .into_array(2)
        };

        let output = invoke(10)?;
        let output = output.as_primitive::<UInt64Type>();
        assert!(output.is_null(0));
        assert!(output.is_null(1));

        let output = invoke(70)?;
        let output = output.as_primitive::<UInt64Type>();
        assert!(output.is_null(0));
        assert!(output.is_null(1));

        Ok(())
    }

    #[test]
    fn test_array_positions_sliced_list() -> Result<()> {
        // [[10, 20, 30], [30, 40, 30], [50, 60, 30], [70, 80, 30]]
        //   → slice(1,2) → [[30, 40, 30], [50, 60, 30]]
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(10), Some(20), Some(30)]),
            Some(vec![Some(30), Some(40), Some(30)]),
            Some(vec![Some(50), Some(60), Some(30)]),
            Some(vec![Some(70), Some(80), Some(30)]),
        ]);
        let sliced = list.slice(1, 2);
        let haystack_field =
            Arc::new(Field::new("haystack", sliced.data_type().clone(), true));
        let needle_field = Arc::new(Field::new("needle", DataType::Int32, true));
        let return_field = Arc::new(Field::new(
            "return",
            List(Arc::new(Field::new_list_field(UInt64, true))),
            true,
        ));

        let invoke = |needle: i32| -> Result<ArrayRef> {
            ArrayPositions::new()
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Array(Arc::new(sliced.clone())),
                        ColumnarValue::Scalar(ScalarValue::Int32(Some(needle))),
                    ],
                    arg_fields: vec![
                        Arc::clone(&haystack_field),
                        Arc::clone(&needle_field),
                    ],
                    number_rows: 2,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::new(ConfigOptions::default()),
                })?
                .into_array(2)
        };

        // Needle 30: appears at positions 1,3 in row 0 ([30,40,30])
        // and position 3 in row 1 ([50,60,30]).
        let output = invoke(30)?;
        let output = output.as_list::<i32>();
        let row0 = output.value(0);
        let row0 = row0.as_primitive::<UInt64Type>();
        assert_eq!(row0.values().as_ref(), &[1, 3]);
        let row1 = output.value(1);
        let row1 = row1.as_primitive::<UInt64Type>();
        assert_eq!(row1.values().as_ref(), &[3]);

        // Needle 10: only in the sliced-away prefix row → empty lists.
        let output = invoke(10)?;
        let output = output.as_list::<i32>();
        assert!(output.value(0).is_empty());
        assert!(output.value(1).is_empty());

        // Needle 70: only in the sliced-away suffix row → empty lists.
        let output = invoke(70)?;
        let output = output.as_list::<i32>();
        assert!(output.value(0).is_empty());
        assert!(output.value(1).is_empty());

        Ok(())
    }

    #[test]
    fn test_array_positions_sliced_list_with_nulls() -> Result<()> {
        // [[1, 2], null, [3, 1], [4, 5]]  →  slice(1,2)  →  [null, [3, 1]]
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![Some(3), Some(1)]),
            Some(vec![Some(4), Some(5)]),
        ]);
        let sliced = list.slice(1, 2);
        let haystack_field =
            Arc::new(Field::new("haystack", sliced.data_type().clone(), true));
        let needle_field = Arc::new(Field::new("needle", DataType::Int32, true));
        let return_field = Arc::new(Field::new(
            "return",
            List(Arc::new(Field::new_list_field(UInt64, true))),
            true,
        ));

        let output = ArrayPositions::new()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::new(sliced)),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
                ],
                arg_fields: vec![Arc::clone(&haystack_field), Arc::clone(&needle_field)],
                number_rows: 2,
                return_field: Arc::clone(&return_field),
                config_options: Arc::new(ConfigOptions::default()),
            })?
            .into_array(2)?;

        let output = output.as_list::<i32>();
        // Row 0 is null (from the sliced null row).
        assert!(output.is_null(0));
        // Row 1 is [3, 1] → needle 1 found at position 2.
        assert!(!output.is_null(1));
        let row1 = output.value(1);
        let row1 = row1.as_primitive::<UInt64Type>();
        assert_eq!(row1.values().as_ref(), &[2]);

        Ok(())
    }
}
