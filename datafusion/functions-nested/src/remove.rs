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
use arrow::array::{
    Array, ArrayRef, BooleanArray, GenericListArray, OffsetSizeTrait, cast::AsArray,
    new_empty_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::cast::as_int64_array;
use datafusion_common::utils::ListCoercion;
use datafusion_common::{Result, exec_err, internal_err, utils::take_function_args};
use datafusion_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, Documentation,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayRemove,
    array_remove,
    array element,
    "removes the first element from the array equal to the given value.",
    array_remove_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Removes the first element from the array equal to the given value.",
    syntax_example = "array_remove(array, element)",
    sql_example = r#"```sql
> select array_remove([1, 2, 2, 3, 2, 1, 4], 2);
+----------------------------------------------+
| array_remove(List([1,2,2,3,2,1,4]),Int64(2)) |
+----------------------------------------------+
| [1, 2, 3, 2, 1, 4]                           |
+----------------------------------------------+
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
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
    "removes the first `max` elements from the array equal to the given value.",
    array_remove_n_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Removes the first `max` elements from the array equal to the given value.",
    syntax_example = "array_remove_n(array, element, max))",
    sql_example = r#"```sql
> select array_remove_n([1, 2, 2, 3, 2, 1, 4], 2, 2);
+---------------------------------------------------------+
| array_remove_n(List([1,2,2,3,2,1,4]),Int64(2),Int64(2)) |
+---------------------------------------------------------+
| [1, 3, 2, 1, 4]                                         |
+---------------------------------------------------------+
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
pub(super) struct ArrayRemoveN {
    signature: Signature,
    aliases: Vec<String>,
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
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
    "removes all elements from the array equal to the given value.",
    array_remove_all_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Removes all elements from the array equal to the given value.",
    syntax_example = "array_remove_all(array, element)",
    sql_example = r#"```sql
> select array_remove_all([1, 2, 2, 3, 2, 1, 4], 2);
+--------------------------------------------------+
| array_remove_all(List([1,2,2,3,2,1,4]),Int64(2)) |
+--------------------------------------------------+
| [1, 3, 1, 4]                                     |
+--------------------------------------------------+
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
pub(super) struct ArrayRemoveAll {
    signature: Signature,
    aliases: Vec<String>,
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
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
    let [array, element] = take_function_args("array_remove", args)?;

    let arr_n = vec![1; array.len()];
    array_remove_internal(array, element, &arr_n)
}

fn array_remove_n_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array, element, max] = take_function_args("array_remove_n", args)?;

    let arr_n = as_int64_array(max)?.values().to_vec();
    array_remove_internal(array, element, &arr_n)
}

fn array_remove_all_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array, element] = take_function_args("array_remove_all", args)?;

    let arr_n = vec![i64::MAX; array.len()];
    array_remove_internal(array, element, &arr_n)
}

fn array_remove_internal(
    array: &ArrayRef,
    element_array: &ArrayRef,
    arr_n: &[i64],
) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::List(_) => {
            let list_array = array.as_list::<i32>();
            general_remove::<i32>(list_array, element_array, arr_n)
        }
        DataType::LargeList(_) => {
            let list_array = array.as_list::<i64>();
            general_remove::<i64>(list_array, element_array, arr_n)
        }
        array_type => {
            exec_err!("array_remove_all does not support type '{array_type}'.")
        }
    }
}

/// For each element of `list_array[i]`, removed up to `arr_n[i]` occurrences
/// of `element_array[i]`.
///
/// The type of each **element** in `list_array` must be the same as the type of
/// `element_array`. This function also handles nested arrays
/// ([`arrow::array::ListArray`] of [`arrow::array::ListArray`]s)
///
/// For example, when called to remove a list array (where each element is a
/// list of int32s, the second argument are int32 arrays, and the
/// third argument is the number of occurrences to remove
///
/// ```text
/// general_remove(
///   [1, 2, 3, 2], 2, 1    ==> [1, 3, 2]   (only the first 2 is removed)
///   [4, 5, 6, 5], 5, 2    ==> [4, 6]  (both 5s are removed)
/// )
/// ```
fn general_remove<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    element_array: &ArrayRef,
    arr_n: &[i64],
) -> Result<ArrayRef> {
    let list_field = match list_array.data_type() {
        DataType::List(field) | DataType::LargeList(field) => field,
        _ => {
            return exec_err!(
                "Expected List or LargeList data type, got {:?}",
                list_array.data_type()
            );
        }
    };
    let data_type = list_field.data_type();
    let mut new_values = vec![];
    // Build up the offsets for the final output array
    let mut offsets = Vec::<OffsetSize>::with_capacity(arr_n.len() + 1);
    offsets.push(OffsetSize::zero());

    // n is the number of elements to remove in this row
    for (row_index, (list_array_row, n)) in
        list_array.iter().zip(arr_n.iter()).enumerate()
    {
        match list_array_row {
            Some(list_array_row) => {
                let eq_array = utils::compare_element_to_list(
                    &list_array_row,
                    element_array,
                    row_index,
                    false,
                )?;

                // We need to keep at most first n elements as `false`, which represent the elements to remove.
                let eq_array = if eq_array.false_count() < *n as usize {
                    eq_array
                } else {
                    let mut count = 0;
                    eq_array
                        .iter()
                        .map(|e| {
                            // Keep first n `false` elements, and reverse other elements to `true`.
                            if let Some(false) = e {
                                if count < *n {
                                    count += 1;
                                    e
                                } else {
                                    Some(true)
                                }
                            } else {
                                e
                            }
                        })
                        .collect::<BooleanArray>()
                };

                let filtered_array = arrow::compute::filter(&list_array_row, &eq_array)?;
                offsets.push(
                    offsets[row_index] + OffsetSize::usize_as(filtered_array.len()),
                );
                new_values.push(filtered_array);
            }
            None => {
                // Null element results in a null row (no new offsets)
                offsets.push(offsets[row_index]);
            }
        }
    }

    let values = if new_values.is_empty() {
        new_empty_array(data_type)
    } else {
        let new_values = new_values.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
        arrow::compute::concat(&new_values)?
    };

    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        Arc::clone(list_field),
        OffsetBuffer::new(offsets.into()),
        values,
        list_array.nulls().cloned(),
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
