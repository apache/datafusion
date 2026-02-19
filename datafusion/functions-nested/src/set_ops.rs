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

//! [`ScalarUDFImpl`] definitions for array_union, array_intersect, array_distinct and arrays_zip functions.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, Capacities, GenericListArray, ListArray, MutableArrayData,
    OffsetSizeTrait, StructArray, new_empty_array, new_null_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::DataType::{LargeList, List, Null};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use arrow::row::{RowConverter, SortField};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::utils::ListCoercion;
use datafusion_common::{
    Result, assert_eq_or_internal_err, exec_err, internal_err, utils::take_function_args,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

// Create static instances of ScalarUDFs for each function
make_udf_expr_and_func!(
    ArrayUnion,
    array_union,
    array1 array2,
    "returns an array of the elements in the union of array1 and array2 without duplicates.",
    array_union_udf
);

make_udf_expr_and_func!(
    ArrayIntersect,
    array_intersect,
    first_array second_array,
    "returns an array of the elements in the intersection of array1 and array2.",
    array_intersect_udf
);

make_udf_expr_and_func!(
    ArrayDistinct,
    array_distinct,
    array,
    "returns distinct values from the array after removing duplicates.",
    array_distinct_udf
);

make_udf_expr_and_func!(
    ArraysZip,
    arrays_zip,
    "combines multiple arrays into a single array of structs.",
    arrays_zip_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns an array of elements that are present in both arrays (all elements from both arrays) without duplicates.",
    syntax_example = "array_union(array1, array2)",
    sql_example = r#"```sql
> select array_union([1, 2, 3, 4], [5, 6, 3, 4]);
+----------------------------------------------------+
| array_union([1, 2, 3, 4], [5, 6, 3, 4]);           |
+----------------------------------------------------+
| [1, 2, 3, 4, 5, 6]                                 |
+----------------------------------------------------+
> select array_union([1, 2, 3, 4], [5, 6, 7, 8]);
+----------------------------------------------------+
| array_union([1, 2, 3, 4], [5, 6, 7, 8]);           |
+----------------------------------------------------+
| [1, 2, 3, 4, 5, 6, 7, 8]                           |
+----------------------------------------------------+
```"#,
    argument(
        name = "array1",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "array2",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayUnion {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayUnion {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayUnion {
    pub fn new() -> Self {
        Self {
            signature: Signature::arrays(
                2,
                Some(ListCoercion::FixedSizedListToList),
                Volatility::Immutable,
            ),
            aliases: vec![String::from("list_union")],
        }
    }
}

impl ScalarUDFImpl for ArrayUnion {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_union"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [array1, array2] = take_function_args(self.name(), arg_types)?;
        match (array1, array2) {
            (Null, Null) => Ok(DataType::new_list(Null, true)),
            (Null, dt) | (dt, Null) => Ok(dt.clone()),
            (dt, _) => Ok(dt.clone()),
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_union_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns an array of elements in the intersection of array1 and array2.",
    syntax_example = "array_intersect(array1, array2)",
    sql_example = r#"```sql
> select array_intersect([1, 2, 3, 4], [5, 6, 3, 4]);
+----------------------------------------------------+
| array_intersect([1, 2, 3, 4], [5, 6, 3, 4]);       |
+----------------------------------------------------+
| [3, 4]                                             |
+----------------------------------------------------+
> select array_intersect([1, 2, 3, 4], [5, 6, 7, 8]);
+----------------------------------------------------+
| array_intersect([1, 2, 3, 4], [5, 6, 7, 8]);       |
+----------------------------------------------------+
| []                                                 |
+----------------------------------------------------+
```"#,
    argument(
        name = "array1",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "array2",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayIntersect {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayIntersect {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayIntersect {
    pub fn new() -> Self {
        Self {
            signature: Signature::arrays(
                2,
                Some(ListCoercion::FixedSizedListToList),
                Volatility::Immutable,
            ),
            aliases: vec![String::from("list_intersect")],
        }
    }
}

impl ScalarUDFImpl for ArrayIntersect {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_intersect"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [array1, array2] = take_function_args(self.name(), arg_types)?;
        match (array1, array2) {
            (Null, Null) => Ok(DataType::new_list(Null, true)),
            (Null, dt) | (dt, Null) => Ok(dt.clone()),
            (dt, _) => Ok(dt.clone()),
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_intersect_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns distinct values from the array after removing duplicates.",
    syntax_example = "array_distinct(array)",
    sql_example = r#"```sql
> select array_distinct([1, 3, 2, 3, 1, 2, 4]);
+---------------------------------+
| array_distinct(List([1,2,3,4])) |
+---------------------------------+
| [1, 2, 3, 4]                    |
+---------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayDistinct {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayDistinct {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec!["list_distinct".to_string()],
        }
    }
}

impl Default for ArrayDistinct {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayDistinct {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_distinct"
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
        make_scalar_function(array_distinct_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// array_distinct SQL function
/// example: from list [1, 3, 2, 3, 1, 2, 4] to [1, 2, 3, 4]
fn array_distinct_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("array_distinct", args)?;
    match array.data_type() {
        Null => Ok(Arc::clone(array)),
        List(field) => {
            let array = as_list_array(&array)?;
            general_array_distinct(array, field)
        }
        LargeList(field) => {
            let array = as_large_list_array(&array)?;
            general_array_distinct(array, field)
        }
        arg_type => exec_err!("array_distinct does not support type {arg_type}"),
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
enum SetOp {
    Union,
    Intersect,
}

impl Display for SetOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SetOp::Union => write!(f, "array_union"),
            SetOp::Intersect => write!(f, "array_intersect"),
        }
    }
}

fn generic_set_lists<OffsetSize: OffsetSizeTrait>(
    l: &GenericListArray<OffsetSize>,
    r: &GenericListArray<OffsetSize>,
    field: Arc<Field>,
    set_op: SetOp,
) -> Result<ArrayRef> {
    if l.is_empty() || l.value_type().is_null() {
        let field = Arc::new(Field::new_list_field(r.value_type(), true));
        return general_array_distinct::<OffsetSize>(r, &field);
    } else if r.is_empty() || r.value_type().is_null() {
        let field = Arc::new(Field::new_list_field(l.value_type(), true));
        return general_array_distinct::<OffsetSize>(l, &field);
    }

    assert_eq_or_internal_err!(
        l.value_type(),
        r.value_type(),
        "{set_op:?} is not implemented for '{l:?}' and '{r:?}'"
    );

    // Convert all values to rows in batch for performance.
    let converter = RowConverter::new(vec![SortField::new(l.value_type())])?;
    let rows_l = converter.convert_columns(&[Arc::clone(l.values())])?;
    let rows_r = converter.convert_columns(&[Arc::clone(r.values())])?;

    match set_op {
        SetOp::Union => generic_set_loop::<OffsetSize, true>(
            l, r, &rows_l, &rows_r, field, &converter,
        ),
        SetOp::Intersect => generic_set_loop::<OffsetSize, false>(
            l, r, &rows_l, &rows_r, field, &converter,
        ),
    }
}

/// Inner loop for set operations, parameterized by const generic to
/// avoid branching inside the hot loop.
fn generic_set_loop<OffsetSize: OffsetSizeTrait, const IS_UNION: bool>(
    l: &GenericListArray<OffsetSize>,
    r: &GenericListArray<OffsetSize>,
    rows_l: &arrow::row::Rows,
    rows_r: &arrow::row::Rows,
    field: Arc<Field>,
    converter: &RowConverter,
) -> Result<ArrayRef> {
    let l_offsets = l.value_offsets();
    let r_offsets = r.value_offsets();

    let mut result_offsets = Vec::with_capacity(l.len() + 1);
    result_offsets.push(OffsetSize::usize_as(0));
    let initial_capacity = if IS_UNION {
        // Union can include all elements from both sides
        rows_l.num_rows()
    } else {
        // Intersect result is bounded by the smaller side
        rows_l.num_rows().min(rows_r.num_rows())
    };

    let mut final_rows = Vec::with_capacity(initial_capacity);

    // Reuse hash sets across iterations
    let mut seen = HashSet::new();
    let mut lookup_set = HashSet::new();
    for i in 0..l.len() {
        let last_offset = *result_offsets.last().unwrap();

        if l.is_null(i) || r.is_null(i) {
            result_offsets.push(last_offset);
            continue;
        }

        let l_start = l_offsets[i].as_usize();
        let l_end = l_offsets[i + 1].as_usize();
        let r_start = r_offsets[i].as_usize();
        let r_end = r_offsets[i + 1].as_usize();

        seen.clear();

        if IS_UNION {
            for idx in l_start..l_end {
                let row = rows_l.row(idx);
                if seen.insert(row) {
                    final_rows.push(row);
                }
            }
            for idx in r_start..r_end {
                let row = rows_r.row(idx);
                if seen.insert(row) {
                    final_rows.push(row);
                }
            }
        } else {
            let l_len = l_end - l_start;
            let r_len = r_end - r_start;

            // Select shorter side for lookup, longer side for probing
            let (lookup_rows, lookup_range, probe_rows, probe_range) = if l_len < r_len {
                (rows_l, l_start..l_end, rows_r, r_start..r_end)
            } else {
                (rows_r, r_start..r_end, rows_l, l_start..l_end)
            };
            lookup_set.clear();
            lookup_set.reserve(lookup_range.len());

            // Build lookup table
            for idx in lookup_range {
                lookup_set.insert(lookup_rows.row(idx));
            }

            // Probe and emit distinct intersected rows
            for idx in probe_range {
                let row = probe_rows.row(idx);
                if lookup_set.contains(&row) && seen.insert(row) {
                    final_rows.push(row);
                }
            }
        }
        result_offsets.push(last_offset + OffsetSize::usize_as(seen.len()));
    }

    let final_values = if final_rows.is_empty() {
        new_empty_array(&l.value_type())
    } else {
        let arrays = converter.convert_rows(final_rows)?;
        Arc::clone(&arrays[0])
    };

    let arr = GenericListArray::<OffsetSize>::try_new(
        field,
        OffsetBuffer::new(result_offsets.into()),
        final_values,
        NullBuffer::union(l.nulls(), r.nulls()),
    )?;
    Ok(Arc::new(arr))
}

fn general_set_op(
    array1: &ArrayRef,
    array2: &ArrayRef,
    set_op: SetOp,
) -> Result<ArrayRef> {
    let len = array1.len();
    match (array1.data_type(), array2.data_type()) {
        (Null, Null) => Ok(new_null_array(&DataType::new_list(Null, true), len)),
        (Null, dt @ List(_))
        | (Null, dt @ LargeList(_))
        | (dt @ List(_), Null)
        | (dt @ LargeList(_), Null) => Ok(new_null_array(dt, len)),
        (List(field), List(_)) => {
            let array1 = as_list_array(&array1)?;
            let array2 = as_list_array(&array2)?;
            generic_set_lists::<i32>(array1, array2, Arc::clone(field), set_op)
        }
        (LargeList(field), LargeList(_)) => {
            let array1 = as_large_list_array(&array1)?;
            let array2 = as_large_list_array(&array2)?;
            generic_set_lists::<i64>(array1, array2, Arc::clone(field), set_op)
        }
        (data_type1, data_type2) => {
            internal_err!(
                "{set_op} does not support types '{data_type1:?}' and '{data_type2:?}'"
            )
        }
    }
}

fn array_union_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array1, array2] = take_function_args("array_union", args)?;
    general_set_op(array1, array2, SetOp::Union)
}

fn array_intersect_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array1, array2] = take_function_args("array_intersect", args)?;
    general_set_op(array1, array2, SetOp::Intersect)
}

fn general_array_distinct<OffsetSize: OffsetSizeTrait>(
    array: &GenericListArray<OffsetSize>,
    field: &FieldRef,
) -> Result<ArrayRef> {
    if array.is_empty() {
        return Ok(Arc::new(array.clone()) as ArrayRef);
    }
    let value_offsets = array.value_offsets();
    let dt = array.value_type();
    let mut offsets = Vec::with_capacity(array.len() + 1);
    offsets.push(OffsetSize::usize_as(0));

    // Convert all values to row format in a single batch for performance
    let converter = RowConverter::new(vec![SortField::new(dt.clone())])?;
    let rows = converter.convert_columns(&[Arc::clone(array.values())])?;
    let mut final_rows = Vec::with_capacity(rows.num_rows());
    let mut seen = HashSet::new();
    for i in 0..array.len() {
        let last_offset = *offsets.last().unwrap();

        // Null list entries produce no output; just carry forward the offset.
        if array.is_null(i) {
            offsets.push(last_offset);
            continue;
        }

        let start = value_offsets[i].as_usize();
        let end = value_offsets[i + 1].as_usize();
        seen.clear();
        seen.reserve(end - start);

        // Walk the sub-array and keep only the first occurrence of each value.
        for idx in start..end {
            let row = rows.row(idx);
            if seen.insert(row) {
                final_rows.push(row);
            }
        }
        offsets.push(last_offset + OffsetSize::usize_as(seen.len()));
    }

    // Convert all collected distinct rows back
    let final_values = if final_rows.is_empty() {
        new_empty_array(&dt)
    } else {
        let arrays = converter.convert_rows(final_rows)?;
        Arc::clone(&arrays[0])
    };

    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        Arc::clone(field),
        OffsetBuffer::new(offsets.into()),
        final_values,
        // Keep the list nulls
        array.nulls().cloned(),
    )?))
}

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns an array of structs created by combining the elements of each input array at the same index. If the arrays have different lengths, shorter arrays are padded with NULLs.",
    syntax_example = "arrays_zip(array1, array2[, ..., array_n])",
    sql_example = r#"```sql
> select arrays_zip([1, 2, 3], ['a', 'b', 'c']);
+---------------------------------------------------+
| arrays_zip([1, 2, 3], ['a', 'b', 'c'])             |
+---------------------------------------------------+
| [{c0: 1, c1: a}, {c0: 2, c1: b}, {c0: 3, c1: c}] |
+---------------------------------------------------+
> select arrays_zip([1, 2], [3, 4, 5]);
+---------------------------------------------------+
| arrays_zip([1, 2], [3, 4, 5])                       |
+---------------------------------------------------+
| [{c0: 1, c1: 3}, {c0: 2, c1: 4}, {c0: , c1: 5}]  |
+---------------------------------------------------+
```"#,
    argument(name = "array1", description = "First array expression."),
    argument(name = "array2", description = "Second array expression."),
    argument(name = "array_n", description = "Subsequent array expressions.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArraysZip {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArraysZip {
    fn default() -> Self {
        Self::new()
    }
}

impl ArraysZip {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![String::from("list_zip")],
        }
    }
}

impl ScalarUDFImpl for ArraysZip {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "arrays_zip"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return exec_err!("arrays_zip requires at least two arguments");
        }

        let mut fields = Vec::with_capacity(arg_types.len());
        for (i, arg_type) in arg_types.iter().enumerate() {
            let element_type = match arg_type {
                List(field) | LargeList(field) => field.data_type().clone(),
                Null => Null,
                dt => {
                    return exec_err!("arrays_zip expects array arguments, got {dt}");
                }
            };
            fields.push(Field::new(format!("c{i}"), element_type, true));
        }

        Ok(List(Arc::new(Field::new_list_field(
            DataType::Struct(Fields::from(fields)),
            true,
        ))))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(arrays_zip_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Core implementation for arrays_zip.
///
/// Takes N list arrays and produces a list of structs where each struct
/// has one field per input array. If arrays within a row have different
/// lengths, shorter arrays are padded with NULLs.
fn arrays_zip_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 {
        return exec_err!("arrays_zip requires at least two arguments");
    }

    let num_rows = args[0].len();

    // Extract element type from each list argument
    let mut list_arrays: Vec<Option<&ListArray>> = Vec::with_capacity(args.len());
    let mut element_types: Vec<DataType> = Vec::with_capacity(args.len());

    for (i, arg) in args.iter().enumerate() {
        match arg.data_type() {
            List(field) => {
                let list_arr = as_list_array(arg)?;
                element_types.push(field.data_type().clone());
                list_arrays.push(Some(list_arr));
            }
            Null => {
                element_types.push(Null);
                list_arrays.push(None);
            }
            dt => {
                return exec_err!("arrays_zip argument {i} expected list type, got {dt}");
            }
        }
    }

    // Collect per-column values data for MutableArrayData builders.
    let values_data: Vec<_> = list_arrays
        .iter()
        .map(|la| la.map(|arr| arr.values().to_data()))
        .collect();

    let struct_fields: Fields = element_types
        .iter()
        .enumerate()
        .map(|(i, dt)| Field::new(format!("c{i}"), dt.clone(), true))
        .collect::<Vec<_>>()
        .into();

    // Create a MutableArrayData builder per column. For None (Null-typed)
    // args we only need extend_nulls, so we track them separately.
    let mut builders: Vec<Option<MutableArrayData>> = values_data
        .iter()
        .map(|vd| {
            vd.as_ref().map(|data| {
                MutableArrayData::with_capacities(vec![data], true, Capacities::Array(0))
            })
        })
        .collect();

    let mut offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
    offsets.push(0);
    let mut null_mask: Vec<bool> = Vec::with_capacity(num_rows);
    let mut total_values: usize = 0;

    // Process each row: compute per-array lengths, detect if aligned,
    // then either copy values directly or pad shorter arrays with NULLs.
    for row_idx in 0..num_rows {
        // Compute per-array length for this row.
        let mut max_len: usize = 0;
        let mut all_null = true;

        for la in list_arrays.iter().flatten() {
            if !la.is_null(row_idx) {
                all_null = false;
                let len = (la.value_offsets()[row_idx + 1] - la.value_offsets()[row_idx])
                    as usize;
                max_len = max_len.max(len);
            }
        }

        if all_null {
            null_mask.push(true);
            offsets.push(*offsets.last().unwrap());
            continue;
        }
        null_mask.push(false);

        // Extend each column builder for this row.
        for (col_idx, la) in list_arrays.iter().enumerate() {
            match la {
                Some(arr) if !arr.is_null(row_idx) => {
                    let start = arr.value_offsets()[row_idx] as usize;
                    let end = arr.value_offsets()[row_idx + 1] as usize;
                    let len = end - start;
                    let builder = builders[col_idx].as_mut().unwrap();
                    builder.extend(0, start, end);
                    if len < max_len {
                        builder.extend_nulls(max_len - len);
                    }
                }
                _ => {
                    // Null list entry or None (Null-typed) arg — all nulls.
                    if let Some(builder) = builders[col_idx].as_mut() {
                        builder.extend_nulls(max_len);
                    }
                    // None columns are handled after the loop.
                }
            }
        }

        total_values += max_len;
        let last = *offsets.last().unwrap();
        offsets.push(last + max_len as i32);
    }

    // Assemble struct columns from builders.
    let struct_columns: Vec<ArrayRef> = builders
        .into_iter()
        .zip(element_types.iter())
        .map(|(builder, elem_type)| match builder {
            Some(b) => arrow::array::make_array(b.freeze()),
            None => new_null_array(
                if elem_type.is_null() {
                    &Null
                } else {
                    elem_type
                },
                total_values,
            ),
        })
        .collect();

    let struct_array = StructArray::try_new(struct_fields, struct_columns, None)?;

    let null_buffer = if null_mask.iter().any(|&v| v) {
        Some(NullBuffer::from(
            null_mask.iter().map(|v| !v).collect::<Vec<bool>>(),
        ))
    } else {
        None
    };

    let result = ListArray::try_new(
        Arc::new(Field::new_list_field(
            struct_array.data_type().clone(),
            true,
        )),
        OffsetBuffer::new(offsets.into()),
        Arc::new(struct_array),
        null_buffer,
    )?;

    Ok(Arc::new(result))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int32Array, ListArray},
        buffer::OffsetBuffer,
        datatypes::{DataType, Field},
    };
    use datafusion_common::{DataFusionError, config::ConfigOptions};
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};

    use crate::set_ops::array_distinct_udf;

    #[test]
    fn test_array_distinct_inner_nullability_result_type_match_return_type()
    -> Result<(), DataFusionError> {
        let udf = array_distinct_udf();

        for inner_nullable in [true, false] {
            let inner_field = Field::new_list_field(DataType::Int32, inner_nullable);
            let input_field =
                Field::new_list("input", Arc::new(inner_field.clone()), true);

            // [[1, 1, 2]]
            let input_array = ListArray::new(
                inner_field.into(),
                OffsetBuffer::new(vec![0, 3].into()),
                Arc::new(Int32Array::new(vec![1, 1, 2].into(), None)),
                None,
            );

            let input_array = ColumnarValue::Array(Arc::new(input_array));

            let result = udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![input_array],
                arg_fields: vec![input_field.clone().into()],
                number_rows: 1,
                return_field: input_field.clone().into(),
                config_options: Arc::new(ConfigOptions::default()),
            })?;

            assert_eq!(
                result.data_type(),
                udf.return_type(&[input_field.data_type().clone()])?
            );
        }
        Ok(())
    }
}
