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

//! [`ScalarUDFImpl`] definitions for array_remove, array_remove_n, array_remove_all functions.

use crate::utils;
use crate::utils::make_scalar_function;
use arrow::array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait, cast::AsArray};
use arrow::array::{BooleanArray, UInt64Array, new_empty_array};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::take;
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::assert_eq_or_internal_err;
use datafusion_common::cast::as_int64_array;
use datafusion_common::utils::ListCoercion;
use datafusion_common::{Result, exec_err, internal_err, utils::take_function_args};
use datafusion_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, Documentation,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_macros::user_doc;
use itertools::Itertools;
use std::ops::Range;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayRemove,
    array_remove,
    array element,
    "removes the first element from the array equal to the given value. NULL elements already in the array are preserved when removing a non-NULL value. If `element` evaluates to NULL, the result is NULL rather than removing NULL entries.",
    array_remove_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Removes the first element from the array equal to the given value. NULL elements already in the array are preserved when removing a non-NULL value. If `element` evaluates to NULL, the result is NULL rather than removing NULL entries.",
    syntax_example = "array_remove(array, element)",
    sql_example = r#"```sql
> select array_remove([1, 2, 2, 3, 2, 1, 4], 2);
+----------------------------------------------+
| array_remove(List([1,2,2,3,2,1,4]),Int64(2)) |
+----------------------------------------------+
| [1, 2, 3, 2, 1, 4]                           |
+----------------------------------------------+

> select array_remove([1, 2, NULL, 2, 4], 2);
+---------------------------------------------------+
| array_remove(List([1,2,NULL,2,4]),Int64(2)) |
+---------------------------------------------------+
| [1, NULL, 2, 4]                              |
+---------------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "element",
        description = "Element to be removed from the array."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayRemove {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayRemove {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayRemove {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element(Volatility::Immutable),
            aliases: vec!["list_remove".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayRemove {
    fn name(&self) -> &str {
        "array_remove"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        Ok(Arc::clone(&args.arg_fields[0]))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_remove_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

make_udf_expr_and_func!(
    ArrayRemoveN,
    array_remove_n,
    array element max,
    "removes the first `max` elements from the array equal to the given value. NULL elements already in the array are preserved when removing a non-NULL value. If `element` evaluates to NULL, the result is NULL rather than removing NULL entries.",
    array_remove_n_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Removes the first `max` elements from the array equal to the given value. NULL elements already in the array are preserved when removing a non-NULL value. If `element` evaluates to NULL, the result is NULL rather than removing NULL entries.",
    syntax_example = "array_remove_n(array, element, max)",
    sql_example = r#"```sql
> select array_remove_n([1, 2, 2, 3, 2, 1, 4], 2, 2);
+---------------------------------------------------------+
| array_remove_n(List([1,2,2,3,2,1,4]),Int64(2),Int64(2)) |
+---------------------------------------------------------+
| [1, 3, 2, 1, 4]                                         |
+---------------------------------------------------------+

> select array_remove_n([1, 2, NULL, 2, 4], 2, 2);
+----------------------------------------------------------+
| array_remove_n(List([1,2,NULL,2,4]),Int64(2),Int64(2)) |
+----------------------------------------------------------+
| [1, NULL, 4]                                            |
+----------------------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "element",
        description = "Element to be removed from the array."
    ),
    argument(name = "max", description = "Number of first occurrences to remove.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayRemoveN {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayRemoveN {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayRemoveN {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                    arguments: vec![
                        ArrayFunctionArgument::Array,
                        ArrayFunctionArgument::Element,
                        ArrayFunctionArgument::Index,
                    ],
                    array_coercion: Some(ListCoercion::FixedSizedListToList),
                }),
                Volatility::Immutable,
            ),
            aliases: vec!["list_remove_n".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayRemoveN {
    fn name(&self) -> &str {
        "array_remove_n"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        Ok(Arc::clone(&args.arg_fields[0]))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_remove_n_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

make_udf_expr_and_func!(
    ArrayRemoveAll,
    array_remove_all,
    array element,
    "removes all elements from the array equal to the given value. NULL elements already in the array are preserved when removing a non-NULL value. If `element` evaluates to NULL, the result is NULL rather than removing NULL entries.",
    array_remove_all_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Removes all elements from the array equal to the given value. NULL elements already in the array are preserved when removing a non-NULL value. If `element` evaluates to NULL, the result is NULL rather than removing NULL entries.",
    syntax_example = "array_remove_all(array, element)",
    sql_example = r#"```sql
> select array_remove_all([1, 2, 2, 3, 2, 1, 4], 2);
+--------------------------------------------------+
| array_remove_all(List([1,2,2,3,2,1,4]),Int64(2)) |
+--------------------------------------------------+
| [1, 3, 1, 4]                                     |
+--------------------------------------------------+

> select array_remove_all([1, 2, NULL, 2, 4], 2);
+-----------------------------------------------------+
| array_remove_all(List([1,2,NULL,2,4]),Int64(2)) |
+-----------------------------------------------------+
| [1, NULL, 4]                                     |
+-----------------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "element",
        description = "Element to be removed from the array."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayRemoveAll {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayRemoveAll {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayRemoveAll {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element(Volatility::Immutable),
            aliases: vec!["list_remove_all".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayRemoveAll {
    fn name(&self) -> &str {
        "array_remove_all"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        Ok(Arc::clone(&args.arg_fields[0]))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_remove_all_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_remove_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [haystack, needle] = take_function_args("array_remove", args)?;

    let copy_row_indices = |matches_array: &BooleanArray,
                            take_indices: &mut Vec<u64>,
                            range: Range<usize>,
                            _row_index: usize| {
        copy_with_n_exclusions(matches_array, take_indices, range, 1)
    };

    match haystack.data_type() {
        DataType::List(field) if field.data_type().is_list() => {
            remove_generic::<i32, true>(haystack, needle, copy_row_indices)
        }
        DataType::LargeList(field) if field.data_type().is_list() => {
            remove_generic::<i64, true>(haystack, needle, copy_row_indices)
        }
        DataType::List(_) => {
            remove_generic::<i32, false>(haystack, needle, copy_row_indices)
        }
        DataType::LargeList(_) => {
            remove_generic::<i64, false>(haystack, needle, copy_row_indices)
        }
        dt => {
            exec_err!("array_remove does not support type '{dt}'.")
        }
    }
}

fn array_remove_n_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [haystack, needle, n_array] = take_function_args("array_remove_n", args)?;

    // TODO: null handling
    let n_array = as_int64_array(n_array)?.values();

    let copy_row_indices = |matches_array: &BooleanArray,
                            take_indices: &mut Vec<u64>,
                            range: Range<usize>,
                            row_index: usize| {
        copy_with_n_exclusions(
            matches_array,
            take_indices,
            range,
            n_array[row_index] as usize,
        )
    };

    match haystack.data_type() {
        DataType::List(field) if field.data_type().is_list() => {
            remove_generic::<i32, true>(haystack, needle, copy_row_indices)
        }
        DataType::LargeList(field) if field.data_type().is_list() => {
            remove_generic::<i64, true>(haystack, needle, copy_row_indices)
        }
        DataType::List(_) => {
            remove_generic::<i32, false>(haystack, needle, copy_row_indices)
        }
        DataType::LargeList(_) => {
            remove_generic::<i64, false>(haystack, needle, copy_row_indices)
        }
        dt => {
            exec_err!("array_remove_n does not support type '{dt}'.")
        }
    }
}

fn array_remove_all_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [haystack, needle] = take_function_args("array_remove_all", args)?;

    let copy_row_indices = |matches_array: &BooleanArray,
                            take_indices: &mut Vec<u64>,
                            range: Range<usize>,
                            _row_index: usize| {
        copy_with_n_exclusions(matches_array, take_indices, range, matches_array.len())
    };

    match haystack.data_type() {
        DataType::List(field) if field.data_type().is_list() => {
            remove_generic::<i32, true>(haystack, needle, copy_row_indices)
        }
        DataType::LargeList(field) if field.data_type().is_list() => {
            remove_generic::<i64, true>(haystack, needle, copy_row_indices)
        }
        DataType::List(_) => {
            remove_generic::<i32, false>(haystack, needle, copy_row_indices)
        }
        DataType::LargeList(_) => {
            remove_generic::<i64, false>(haystack, needle, copy_row_indices)
        }
        dt => {
            exec_err!("array_remove_all does not support type '{dt}'.")
        }
    }
}

/// Given a range of `indices`, copy them into `take_indices` excluding the
/// first `n` values which are marked as `true` in `matches_array`.
fn copy_with_n_exclusions(
    matches_array: &BooleanArray,
    take_indices: &mut Vec<u64>,
    indices: Range<usize>,
    n: usize,
) -> usize {
    let (start, end) = (indices.start, indices.end);
    let before_len = take_indices.len();
    // Represents the start of the range gap between elements to remove;
    // if there are consecutive elements to remove then the range we create
    // to copy would be empty and we copy no elements.
    let mut to_copy_from = start as u64;
    for index in matches_array
        .iter()
        .enumerate()
        .filter_map(|(i, e)| (e == Some(true)).then_some(i))
        .take(n)
    {
        let index = start + index;
        take_indices.extend(to_copy_from..index as u64);
        to_copy_from = index as u64 + 1;
    }
    // Flush any leftover elements after last element we removed
    take_indices.extend(to_copy_from..end as u64);
    take_indices.len() - before_len
}

/// Given a `haystack` list array (which may be nested), for each corresponding
/// `needle[i]` element find any occurrences in the list of `haystack[i]` and
/// remove them according to the logic in `copy_with_removals`.
///
/// `copy_row_indices` is intended to copy indices of elements to preserve in
/// `haystack[i]` for `output[i]`.
fn remove_generic<OffsetSize: OffsetSizeTrait, const IS_NESTED: bool>(
    haystack: &ArrayRef,
    needle: &ArrayRef,
    // (matches_array, take_indices, start..end, row_index) -> no_of_elements_copied
    copy_row_indices: impl Fn(&BooleanArray, &mut Vec<u64>, Range<usize>, usize) -> usize,
) -> Result<ArrayRef> {
    let haystack = haystack.as_list::<OffsetSize>();
    assert_eq_or_internal_err!(
        &haystack.value_type(),
        needle.data_type(),
        "remove_generic must be called with equivalent value types"
    );

    let offsets_buffer = haystack.offsets();
    // Safe unwraps since offset buffer must always have at least one offset
    let max_values_length = offsets_buffer.last().unwrap().as_usize()
        - offsets_buffer.first().unwrap().as_usize();
    let mut take_indices: Vec<u64> = Vec::with_capacity(max_values_length);

    let nulls = NullBuffer::union(haystack.nulls(), needle.nulls());
    let mut offsets = Vec::<OffsetSize>::with_capacity(offsets_buffer.len());
    offsets.push(OffsetSize::zero());

    for (row_index, (start, end)) in offsets_buffer.iter().tuple_windows().enumerate() {
        if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row_index)) {
            offsets.push(offsets[row_index]);
            continue;
        }

        // For list element of haystack, find all occurrences of needle[row_index].
        // true in output is match and must be removed.
        let matches_array = utils::compare_element_to_list_fixed::<IS_NESTED>(
            &haystack.value(row_index),
            needle,
            row_index,
        )?;

        let start = start.to_usize().unwrap();
        let end = end.to_usize().unwrap();

        if matches_array.has_true() {
            // Copy indices into take_indices based on whether we remove one,
            // all or N matching elements
            let len = copy_row_indices(
                &matches_array,
                &mut take_indices,
                start..end,
                row_index,
            );
            offsets.push(offsets[row_index] + OffsetSize::usize_as(len));
        } else {
            // Fast path: no elements to remove, copy entire row
            take_indices.extend(start as u64..end as u64);
            offsets.push(offsets[row_index] + OffsetSize::usize_as(end - start));
        }
    }

    let new_values = if take_indices.is_empty() {
        new_empty_array(&haystack.value_type())
    } else {
        take(haystack.values(), &UInt64Array::from(take_indices), None)?
    };

    let (DataType::List(field) | DataType::LargeList(field)) = haystack.data_type()
    else {
        unreachable!("GenericListArray must have DataType List or LargeList")
    };
    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        Arc::clone(field),
        OffsetBuffer::new(offsets.into()),
        new_values,
        nulls,
    )?))
}

#[cfg(test)]
mod tests {
    use crate::remove::{ArrayRemove, ArrayRemoveAll, ArrayRemoveN};
    use arrow::array::{
        Array, ArrayRef, AsArray, GenericListArray, ListArray, OffsetSizeTrait,
    };
    use arrow::datatypes::{DataType, Field, Int32Type};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use std::ops::Deref;
    use std::sync::Arc;

    #[test]
    fn test_array_remove_nullability() {
        for nullability in [true, false] {
            for item_nullability in [true, false] {
                let input_field = Arc::new(Field::new(
                    "num",
                    DataType::new_list(DataType::Int32, item_nullability),
                    nullability,
                ));
                let args_fields = vec![
                    Arc::clone(&input_field),
                    Arc::new(Field::new("a", DataType::Int32, false)),
                ];
                let scalar_args = vec![None, Some(&ScalarValue::Int32(Some(1)))];

                let result = ArrayRemove::new()
                    .return_field_from_args(ReturnFieldArgs {
                        arg_fields: &args_fields,
                        scalar_arguments: &scalar_args,
                    })
                    .unwrap();

                assert_eq!(result, input_field);
            }
        }
    }

    #[test]
    fn test_array_remove_n_nullability() {
        for nullability in [true, false] {
            for item_nullability in [true, false] {
                let input_field = Arc::new(Field::new(
                    "num",
                    DataType::new_list(DataType::Int32, item_nullability),
                    nullability,
                ));
                let args_fields = vec![
                    Arc::clone(&input_field),
                    Arc::new(Field::new("a", DataType::Int32, false)),
                    Arc::new(Field::new("b", DataType::Int64, false)),
                ];
                let scalar_args = vec![
                    None,
                    Some(&ScalarValue::Int32(Some(1))),
                    Some(&ScalarValue::Int64(Some(1))),
                ];

                let result = ArrayRemoveN::new()
                    .return_field_from_args(ReturnFieldArgs {
                        arg_fields: &args_fields,
                        scalar_arguments: &scalar_args,
                    })
                    .unwrap();

                assert_eq!(result, input_field);
            }
        }
    }

    #[test]
    fn test_array_remove_all_nullability() {
        for nullability in [true, false] {
            for item_nullability in [true, false] {
                let input_field = Arc::new(Field::new(
                    "num",
                    DataType::new_list(DataType::Int32, item_nullability),
                    nullability,
                ));
                let result = ArrayRemoveAll::new()
                    .return_field_from_args(ReturnFieldArgs {
                        arg_fields: &[Arc::clone(&input_field)],
                        scalar_arguments: &[None],
                    })
                    .unwrap();

                assert_eq!(result, input_field);
            }
        }
    }

    fn ensure_field_nullability<O: OffsetSizeTrait>(
        field_nullable: bool,
        list: GenericListArray<O>,
    ) -> GenericListArray<O> {
        let (field, offsets, values, nulls) = list.into_parts();

        if field.is_nullable() == field_nullable {
            return GenericListArray::new(field, offsets, values, nulls);
        }
        if !field_nullable {
            assert_eq!(nulls, None);
        }

        let field = Arc::new(field.deref().clone().with_nullable(field_nullable));

        GenericListArray::new(field, offsets, values, nulls)
    }

    #[test]
    fn test_array_remove_non_nullable() {
        let input_list = Arc::new(ensure_field_nullability(
            false,
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(([1, 2, 2, 3, 2, 1, 4]).iter().copied().map(Some)),
                Some(([42, 2, 55, 63, 2]).iter().copied().map(Some)),
            ]),
        ));
        let expected_list = ensure_field_nullability(
            false,
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(([1, 2, 3, 2, 1, 4]).iter().copied().map(Some)),
                Some(([42, 55, 63, 2]).iter().copied().map(Some)),
            ]),
        );

        let element_to_remove = ScalarValue::Int32(Some(2));

        assert_array_remove(input_list, expected_list, element_to_remove);
    }

    #[test]
    fn test_array_remove_nullable() {
        let input_list = Arc::new(ensure_field_nullability(
            true,
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![
                    Some(1),
                    Some(2),
                    Some(2),
                    Some(3),
                    None,
                    Some(1),
                    Some(4),
                ]),
                Some(vec![Some(42), Some(2), None, Some(63), Some(2)]),
            ]),
        ));
        let expected_list = ensure_field_nullability(
            true,
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), Some(2), Some(3), None, Some(1), Some(4)]),
                Some(vec![Some(42), None, Some(63), Some(2)]),
            ]),
        );

        let element_to_remove = ScalarValue::Int32(Some(2));

        assert_array_remove(input_list, expected_list, element_to_remove);
    }

    fn assert_array_remove(
        input_list: ArrayRef,
        expected_list: GenericListArray<i32>,
        element_to_remove: ScalarValue,
    ) {
        assert_eq!(input_list.data_type(), expected_list.data_type());
        assert_eq!(expected_list.value_type(), element_to_remove.data_type());
        let input_list_len = input_list.len();
        let input_list_data_type = input_list.data_type().clone();

        let udf = ArrayRemove::new();
        let args_fields = vec![
            Arc::new(Field::new("num", input_list.data_type().clone(), false)),
            Arc::new(Field::new(
                "el",
                element_to_remove.data_type(),
                element_to_remove.is_null(),
            )),
        ];
        let scalar_args = vec![None, Some(&element_to_remove)];

        let return_field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &args_fields,
                scalar_arguments: &scalar_args,
            })
            .unwrap();

        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(input_list),
                    ColumnarValue::Scalar(element_to_remove),
                ],
                arg_fields: args_fields,
                number_rows: input_list_len,
                return_field,
                config_options: Arc::new(Default::default()),
            })
            .unwrap();

        assert_eq!(result.data_type(), input_list_data_type);
        match result {
            ColumnarValue::Array(array) => {
                let result_list = array.as_list::<i32>();
                assert_eq!(result_list, &expected_list);
            }
            _ => panic!("Expected ColumnarValue::Array"),
        }
    }

    #[test]
    fn test_array_remove_n_non_nullable() {
        let input_list = Arc::new(ensure_field_nullability(
            false,
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(([1, 2, 2, 3, 2, 1, 4]).iter().copied().map(Some)),
                Some(([42, 2, 55, 63, 2]).iter().copied().map(Some)),
            ]),
        ));
        let expected_list = ensure_field_nullability(
            false,
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(([1, 3, 2, 1, 4]).iter().copied().map(Some)),
                Some(([42, 55, 63]).iter().copied().map(Some)),
            ]),
        );

        let element_to_remove = ScalarValue::Int32(Some(2));

        assert_array_remove_n(input_list, expected_list, element_to_remove, 2);
    }

    #[test]
    fn test_array_remove_n_nullable() {
        let input_list = Arc::new(ensure_field_nullability(
            true,
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![
                    Some(1),
                    Some(2),
                    Some(2),
                    Some(3),
                    None,
                    Some(1),
                    Some(4),
                ]),
                Some(vec![Some(42), Some(2), None, Some(63), Some(2)]),
            ]),
        ));
        let expected_list = ensure_field_nullability(
            true,
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), Some(3), None, Some(1), Some(4)]),
                Some(vec![Some(42), None, Some(63)]),
            ]),
        );

        let element_to_remove = ScalarValue::Int32(Some(2));

        assert_array_remove_n(input_list, expected_list, element_to_remove, 2);
    }

    fn assert_array_remove_n(
        input_list: ArrayRef,
        expected_list: GenericListArray<i32>,
        element_to_remove: ScalarValue,
        n: i64,
    ) {
        assert_eq!(input_list.data_type(), expected_list.data_type());
        assert_eq!(expected_list.value_type(), element_to_remove.data_type());
        let input_list_len = input_list.len();
        let input_list_data_type = input_list.data_type().clone();

        let count_scalar = ScalarValue::Int64(Some(n));

        let udf = ArrayRemoveN::new();
        let args_fields = vec![
            Arc::new(Field::new("num", input_list.data_type().clone(), false)),
            Arc::new(Field::new(
                "el",
                element_to_remove.data_type(),
                element_to_remove.is_null(),
            )),
            Arc::new(Field::new("count", DataType::Int64, false)),
        ];
        let scalar_args = vec![None, Some(&element_to_remove), Some(&count_scalar)];

        let return_field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &args_fields,
                scalar_arguments: &scalar_args,
            })
            .unwrap();

        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(input_list),
                    ColumnarValue::Scalar(element_to_remove),
                    ColumnarValue::Scalar(count_scalar),
                ],
                arg_fields: args_fields,
                number_rows: input_list_len,
                return_field,
                config_options: Arc::new(Default::default()),
            })
            .unwrap();

        assert_eq!(result.data_type(), input_list_data_type);
        match result {
            ColumnarValue::Array(array) => {
                let result_list = array.as_list::<i32>();
                assert_eq!(result_list, &expected_list);
            }
            _ => panic!("Expected ColumnarValue::Array"),
        }
    }

    #[test]
    fn test_array_remove_all_non_nullable() {
        let input_list = Arc::new(ensure_field_nullability(
            false,
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(([1, 2, 2, 3, 2, 1, 4]).iter().copied().map(Some)),
                Some(([42, 2, 55, 63, 2]).iter().copied().map(Some)),
            ]),
        ));
        let expected_list = ensure_field_nullability(
            false,
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(([1, 3, 1, 4]).iter().copied().map(Some)),
                Some(([42, 55, 63]).iter().copied().map(Some)),
            ]),
        );

        let element_to_remove = ScalarValue::Int32(Some(2));

        assert_array_remove_all(input_list, expected_list, element_to_remove);
    }

    #[test]
    fn test_array_remove_all_nullable() {
        let input_list = Arc::new(ensure_field_nullability(
            true,
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![
                    Some(1),
                    Some(2),
                    Some(2),
                    Some(3),
                    None,
                    Some(1),
                    Some(4),
                ]),
                Some(vec![Some(42), Some(2), None, Some(63), Some(2)]),
            ]),
        ));
        let expected_list = ensure_field_nullability(
            true,
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), Some(3), None, Some(1), Some(4)]),
                Some(vec![Some(42), None, Some(63)]),
            ]),
        );

        let element_to_remove = ScalarValue::Int32(Some(2));

        assert_array_remove_all(input_list, expected_list, element_to_remove);
    }

    fn assert_array_remove_all(
        input_list: ArrayRef,
        expected_list: GenericListArray<i32>,
        element_to_remove: ScalarValue,
    ) {
        assert_eq!(input_list.data_type(), expected_list.data_type());
        assert_eq!(expected_list.value_type(), element_to_remove.data_type());
        let input_list_len = input_list.len();
        let input_list_data_type = input_list.data_type().clone();

        let udf = ArrayRemoveAll::new();
        let args_fields = vec![
            Arc::new(Field::new("num", input_list.data_type().clone(), false)),
            Arc::new(Field::new(
                "el",
                element_to_remove.data_type(),
                element_to_remove.is_null(),
            )),
        ];
        let scalar_args = vec![None, Some(&element_to_remove)];

        let return_field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &args_fields,
                scalar_arguments: &scalar_args,
            })
            .unwrap();

        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(input_list),
                    ColumnarValue::Scalar(element_to_remove),
                ],
                arg_fields: args_fields,
                number_rows: input_list_len,
                return_field,
                config_options: Arc::new(Default::default()),
            })
            .unwrap();

        assert_eq!(result.data_type(), input_list_data_type);
        match result {
            ColumnarValue::Array(array) => {
                let result_list = array.as_list::<i32>();
                assert_eq!(result_list, &expected_list);
            }
            _ => panic!("Expected ColumnarValue::Array"),
        }
    }
}
