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

//! [`ScalarUDFImpl`] definitions for array_union, array_intersect and array_distinct functions.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, GenericListArray, LargeListArray, ListArray, OffsetSizeTrait,
    new_null_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute;
use arrow::datatypes::DataType::{LargeList, List, Null};
use arrow::datatypes::{DataType, Field, FieldRef};
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
use itertools::Itertools;
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

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns an array of elements that are present in both arrays (all elements from both arrays) with out duplicates.",
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
            (Null, dt) => Ok(dt.clone()),
            (dt, Null) => Ok(dt.clone()),
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
pub(super) struct ArrayIntersect {
    signature: Signature,
    aliases: Vec<String>,
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
            (Null, dt) => Ok(dt.clone()),
            (dt, Null) => Ok(dt.clone()),
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
pub(super) struct ArrayDistinct {
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

    // Check if value type contains nested nulls (e.g., List(Null))
    // RowConverter cannot handle these types, so we use a specialized path
    if contains_nested_null(&l.value_type()) {
        // For nested null types, we cannot use RowConverter's convert_rows
        // For Union: combine arrays and deduplicate using length-based equality
        // For Intersect: find common elements based on length
        return generic_set_lists_for_nested_null::<OffsetSize>(l, r, field, set_op);
    }

    assert_eq_or_internal_err!(
        l.value_type(),
        r.value_type(),
        "{set_op:?} is not implemented for '{l:?}' and '{r:?}'"
    );

    let mut offsets = vec![OffsetSize::usize_as(0)];
    let mut new_arrays = vec![];
    let mut new_null_buf = vec![];
    let converter = RowConverter::new(vec![SortField::new(l.value_type())])?;
    for (first_arr, second_arr) in l.iter().zip(r.iter()) {
        let mut ele_should_be_null = false;

        let l_values = if let Some(first_arr) = first_arr {
            converter.convert_columns(&[first_arr])?
        } else {
            ele_should_be_null = true;
            converter.empty_rows(0, 0)
        };

        let r_values = if let Some(second_arr) = second_arr {
            converter.convert_columns(&[second_arr])?
        } else {
            ele_should_be_null = true;
            converter.empty_rows(0, 0)
        };

        let l_iter = l_values.iter().sorted().dedup();
        let values_set: HashSet<_> = l_iter.clone().collect();
        let mut rows = if set_op == SetOp::Union {
            l_iter.collect()
        } else {
            vec![]
        };

        for r_val in r_values.iter().sorted().dedup() {
            match set_op {
                SetOp::Union => {
                    if !values_set.contains(&r_val) {
                        rows.push(r_val);
                    }
                }
                SetOp::Intersect => {
                    if values_set.contains(&r_val) {
                        rows.push(r_val);
                    }
                }
            }
        }

        let last_offset = match offsets.last() {
            Some(offset) => *offset,
            None => return internal_err!("offsets should not be empty"),
        };

        offsets.push(last_offset + OffsetSize::usize_as(rows.len()));
        let arrays = converter.convert_rows(rows)?;
        let array = match arrays.first() {
            Some(array) => Arc::clone(array),
            None => {
                return internal_err!("{set_op}: failed to get array from rows");
            }
        };

        new_null_buf.push(!ele_should_be_null);
        new_arrays.push(array);
    }

    let offsets = OffsetBuffer::new(offsets.into());
    let new_arrays_ref: Vec<_> = new_arrays.iter().map(|v| v.as_ref()).collect();
    let values = compute::concat(&new_arrays_ref)?;
    let arr = GenericListArray::<OffsetSize>::try_new(
        field,
        offsets,
        values,
        Some(NullBuffer::new(new_null_buf.into())),
    )?;
    Ok(Arc::new(arr))
}

fn general_set_op(
    array1: &ArrayRef,
    array2: &ArrayRef,
    set_op: SetOp,
) -> Result<ArrayRef> {
    fn empty_array(data_type: &DataType, len: usize, large: bool) -> Result<ArrayRef> {
        let field = Arc::new(Field::new_list_field(data_type.clone(), true));
        let values = new_null_array(data_type, len);
        if large {
            Ok(Arc::new(LargeListArray::try_new(
                field,
                OffsetBuffer::new_zeroed(len),
                values,
                None,
            )?))
        } else {
            Ok(Arc::new(ListArray::try_new(
                field,
                OffsetBuffer::new_zeroed(len),
                values,
                None,
            )?))
        }
    }

    match (array1.data_type(), array2.data_type()) {
        (Null, Null) => Ok(Arc::new(ListArray::new_null(
            Arc::new(Field::new_list_field(Null, true)),
            array1.len(),
        ))),
        (Null, List(field)) => {
            if set_op == SetOp::Intersect {
                return empty_array(field.data_type(), array1.len(), false);
            }
            let array = as_list_array(&array2)?;
            general_array_distinct::<i32>(array, field)
        }
        (List(field), Null) => {
            if set_op == SetOp::Intersect {
                return empty_array(field.data_type(), array1.len(), false);
            }
            let array = as_list_array(&array1)?;
            general_array_distinct::<i32>(array, field)
        }
        (Null, LargeList(field)) => {
            if set_op == SetOp::Intersect {
                return empty_array(field.data_type(), array1.len(), true);
            }
            let array = as_large_list_array(&array2)?;
            general_array_distinct::<i64>(array, field)
        }
        (LargeList(field), Null) => {
            if set_op == SetOp::Intersect {
                return empty_array(field.data_type(), array1.len(), true);
            }
            let array = as_large_list_array(&array1)?;
            general_array_distinct::<i64>(array, field)
        }
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

/// Checks if a data type contains Null in a nested structure that would cause
/// RowConverter to fail. This includes List(Null), List(List(Null)), etc.
fn contains_nested_null(dt: &DataType) -> bool {
    match dt {
        Null => true,
        List(field) | LargeList(field) => contains_nested_null(field.data_type()),
        _ => false,
    }
}

/// Specialized set operation for List(Null) or types containing nested nulls.
/// Since RowConverter cannot handle these types and all null values are
/// indistinguishable, we handle union/intersect specially.
fn generic_set_lists_for_nested_null<OffsetSize: OffsetSizeTrait>(
    l: &GenericListArray<OffsetSize>,
    r: &GenericListArray<OffsetSize>,
    field: Arc<Field>,
    set_op: SetOp,
) -> Result<ArrayRef> {
    match set_op {
        SetOp::Union => {
            // For union with List(Null) types:
            // After distinct/dedup, inner lists of nulls become empty lists.
            // Return structure with one empty inner list per row.
            let mut new_offsets = vec![OffsetSize::usize_as(0)];
            let mut new_null_buf = Vec::new();

            for (l_elem, r_elem) in l.iter().zip(r.iter()) {
                let last_offset = new_offsets.last().copied().unwrap();

                if l_elem.is_none() && r_elem.is_none() {
                    new_offsets.push(last_offset);
                    new_null_buf.push(false);
                    continue;
                }

                // Add one empty inner list to the result
                new_offsets.push(last_offset + OffsetSize::usize_as(1));
                new_null_buf.push(true);
            }

            // Create inner values: one empty list for each non-null outer element
            let num_inner_lists = new_null_buf.iter().filter(|&&b| b).count();
            let inner_field = Arc::new(Field::new_list_field(Null, true));
            let inner_offsets: Vec<OffsetSize> = (0..=num_inner_lists)
                .map(|_| OffsetSize::usize_as(0))
                .collect();

            let inner_values = new_null_array(&Null, 0);
            let inner_list = GenericListArray::<OffsetSize>::try_new(
                inner_field,
                OffsetBuffer::new(inner_offsets.into()),
                inner_values,
                None,
            )?;

            Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
                field,
                OffsetBuffer::new(new_offsets.into()),
                Arc::new(inner_list),
                Some(NullBuffer::new(new_null_buf.into())),
            )?))
        }
        SetOp::Intersect => {
            // For intersect with List(Null) types:
            // Since different List(Null) elements (with different lengths or null counts)
            // are not considered equal, intersection returns empty lists.
            let mut new_offsets = vec![OffsetSize::usize_as(0)];
            let mut new_null_buf = Vec::new();

            for _ in 0..l.len() {
                new_offsets.push(new_offsets.last().copied().unwrap());
                new_null_buf.push(true);
            }

            Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
                field,
                OffsetBuffer::new(new_offsets.into()),
                new_null_array(&l.value_type(), 0),
                Some(NullBuffer::new(new_null_buf.into())),
            )?))
        }
    }
}

/// Specialized distinct for List(Null) or types containing nested nulls where
/// RowConverter fails. Since all null values are indistinguishable, we treat
/// elements as equal if they have the same length (for lists) or if they're both null.
fn general_array_distinct_for_nested_null<OffsetSize: OffsetSizeTrait>(
    array: &GenericListArray<OffsetSize>,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let mut offsets = Vec::with_capacity(array.len());
    offsets.push(OffsetSize::usize_as(0));
    let mut new_arrays = Vec::with_capacity(array.len());

    // For each list in the array, deduplicate elements by their length
    for arr in array.iter() {
        let last_offset: OffsetSize = offsets.last().copied().unwrap();
        let Some(arr) = arr else {
            // Null list element - just push same offset
            offsets.push(last_offset);
            continue;
        };

        // For List(Null), the only distinguishing feature is length
        // Track seen lengths to implement distinct
        let list_arr = arr.as_any().downcast_ref::<GenericListArray<OffsetSize>>();
        if let Some(list) = list_arr {
            let mut seen_lengths = HashSet::new();
            let mut seen_null = false;
            let mut indices = Vec::new();

            for i in 0..list.len() {
                if list.is_null(i) {
                    if !seen_null {
                        seen_null = true;
                        indices.push(i);
                    }
                } else {
                    let len = list.value_length(i).as_usize();
                    if seen_lengths.insert(len) {
                        indices.push(i);
                    }
                }
            }

            let distinct_count = indices.len();
            offsets.push(last_offset + OffsetSize::usize_as(distinct_count));

            // Take only the distinct elements
            let index_array = arrow::array::UInt32Array::from(
                indices.into_iter().map(|i| i as u32).collect::<Vec<_>>(),
            );
            let taken = compute::take(list, &index_array, None)?;
            new_arrays.push(taken);
        } else {
            // For non-list nested types, just use first element as representative
            // Since they're all null, they're all equal
            if !arr.is_empty() {
                offsets.push(last_offset + OffsetSize::usize_as(1));
                let first = arr.slice(0, 1);
                new_arrays.push(first);
            } else {
                offsets.push(last_offset);
            }
        }
    }

    if new_arrays.is_empty() {
        return Ok(Arc::new(array.clone()) as ArrayRef);
    }

    let offsets = OffsetBuffer::new(offsets.into());
    let new_arrays_ref = new_arrays.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
    let values = compute::concat(&new_arrays_ref)?;
    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        Arc::clone(field),
        offsets,
        values,
        array.nulls().cloned(),
    )?))
}

fn general_array_distinct<OffsetSize: OffsetSizeTrait>(
    array: &GenericListArray<OffsetSize>,
    field: &FieldRef,
) -> Result<ArrayRef> {
    if array.is_empty() {
        return Ok(Arc::new(array.clone()) as ArrayRef);
    }
    let dt = array.value_type();
    // RowConverter cannot handle List(Null) or similar nested null types.
    // In this case, use a specialized distinct that considers elements equal
    // based on their length (since all null values are indistinguishable).
    if contains_nested_null(&dt) {
        return general_array_distinct_for_nested_null::<OffsetSize>(array, field);
    }
    let mut offsets = Vec::with_capacity(array.len());
    offsets.push(OffsetSize::usize_as(0));
    let mut new_arrays = Vec::with_capacity(array.len());
    let converter = RowConverter::new(vec![SortField::new(dt)])?;
    // distinct for each list in ListArray
    for arr in array.iter() {
        let last_offset: OffsetSize = offsets.last().copied().unwrap();
        let Some(arr) = arr else {
            // Add same offset for null
            offsets.push(last_offset);
            continue;
        };
        let values = converter.convert_columns(&[arr])?;
        // sort elements in list and remove duplicates
        let rows = values.iter().sorted().dedup().collect::<Vec<_>>();
        offsets.push(last_offset + OffsetSize::usize_as(rows.len()));
        let arrays = converter.convert_rows(rows)?;
        let array = match arrays.first() {
            Some(array) => Arc::clone(array),
            None => {
                return internal_err!("array_distinct: failed to get array from rows");
            }
        };
        new_arrays.push(array);
    }
    if new_arrays.is_empty() {
        return Ok(Arc::new(array.clone()) as ArrayRef);
    }
    let offsets = OffsetBuffer::new(offsets.into());
    let new_arrays_ref = new_arrays.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
    let values = compute::concat(&new_arrays_ref)?;
    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        Arc::clone(field),
        offsets,
        values,
        // Keep the list nulls
        array.nulls().cloned(),
    )?))
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
