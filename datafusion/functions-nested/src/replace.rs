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

//! [`ScalarUDFImpl`] definitions for array_replace, array_replace_n and array_replace_all functions.

use arrow::array::{
    Array, ArrayRef, AsArray, Capacities, GenericListArray, MutableArrayData,
    NullBufferBuilder, OffsetBufferBuilder, OffsetSizeTrait, Scalar, new_null_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use datafusion_common::cast::as_int64_array;
use datafusion_common::utils::ListCoercion;
use datafusion_common::{Result, ScalarValue, exec_err, utils::take_function_args};
use datafusion_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, Documentation,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_macros::user_doc;

use crate::utils::compare_element_to_list;

use std::sync::Arc;

// Create static instances of ScalarUDFs for each function
make_udf_expr_and_func!(ArrayReplace,
    array_replace,
    array from to,
    "replaces the first occurrence of the specified element with another specified element.",
    array_replace_udf
);
make_udf_expr_and_func!(ArrayReplaceN,
    array_replace_n,
    array from to max,
    "replaces the first `max` occurrences of the specified element with another specified element.",
    array_replace_n_udf
);
make_udf_expr_and_func!(ArrayReplaceAll,
    array_replace_all,
    array from to,
    "replaces all occurrences of the specified element with another specified element.",
    array_replace_all_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Replaces the first occurrence of the specified element with another specified element.",
    syntax_example = "array_replace(array, from, to)",
    sql_example = r#"```sql
> select array_replace([1, 2, 2, 3, 2, 1, 4], 2, 5);
+--------------------------------------------------------+
| array_replace(List([1,2,2,3,2,1,4]),Int64(2),Int64(5)) |
+--------------------------------------------------------+
| [1, 5, 2, 3, 2, 1, 4]                                  |
+--------------------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(name = "from", description = "Initial element."),
    argument(name = "to", description = "Final element.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayReplace {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayReplace {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayReplace {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::ArraySignature(
                    ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Array,
                            ArrayFunctionArgument::Element,
                            ArrayFunctionArgument::Element,
                        ],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    },
                ),
                volatility: Volatility::Immutable,
                parameter_names: None,
            },
            aliases: vec![String::from("list_replace")],
        }
    }
}

impl ScalarUDFImpl for ArrayReplace {
    fn name(&self) -> &str {
        "array_replace"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [list_arg, from_arg, to_arg] = take_function_args(self.name(), &args.args)?;
        let num_rows = args.number_rows;
        let list_array = list_arg.to_array(num_rows)?;
        match (from_arg, to_arg) {
            (ColumnarValue::Scalar(scalar_from), ColumnarValue::Scalar(scalar_to)) => {
                let result = array_replace_with_scalar_args(
                    &list_array,
                    scalar_from,
                    scalar_to,
                    1i64,
                )?;
                Ok(ColumnarValue::Array(result))
            }
            (from_arg, to_arg) => {
                let from_array = from_arg.to_array(num_rows)?;
                let to_array = to_arg.to_array(num_rows)?;
                let result = array_replace_internal(
                    &list_array,
                    &from_array,
                    &to_array,
                    &[Some(1)],
                )?;
                Ok(ColumnarValue::Array(result))
            }
        }
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
    description = "Replaces the first `max` occurrences of the specified element with another specified element.",
    syntax_example = "array_replace_n(array, from, to, max)",
    sql_example = r#"```sql
> select array_replace_n([1, 2, 2, 3, 2, 1, 4], 2, 5, 2);
+-------------------------------------------------------------------+
| array_replace_n(List([1,2,2,3,2,1,4]),Int64(2),Int64(5),Int64(2)) |
+-------------------------------------------------------------------+
| [1, 5, 5, 3, 2, 1, 4]                                             |
+-------------------------------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(name = "from", description = "Initial element."),
    argument(name = "to", description = "Final element."),
    argument(name = "max", description = "Number of first occurrences to replace.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct ArrayReplaceN {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayReplaceN {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::ArraySignature(
                    ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Array,
                            ArrayFunctionArgument::Element,
                            ArrayFunctionArgument::Element,
                            ArrayFunctionArgument::Index,
                        ],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    },
                ),
                volatility: Volatility::Immutable,
                parameter_names: None,
            },
            aliases: vec![String::from("list_replace_n")],
        }
    }
}

impl ScalarUDFImpl for ArrayReplaceN {
    fn name(&self) -> &str {
        "array_replace_n"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [list_arg, from_arg, to_arg, max_arg] =
            take_function_args(self.name(), &args.args)?;
        let num_rows = args.number_rows;
        let list_array = list_arg.to_array(num_rows)?;
        match (from_arg, to_arg, max_arg) {
            (
                ColumnarValue::Scalar(scalar_from),
                ColumnarValue::Scalar(scalar_to),
                ColumnarValue::Scalar(scalar_max),
            ) => {
                let ScalarValue::Int64(Some(n)) = scalar_max else {
                    return Ok(ColumnarValue::Array(new_null_array(
                        list_array.data_type(),
                        num_rows,
                    )));
                };
                let result = array_replace_with_scalar_args(
                    &list_array,
                    scalar_from,
                    scalar_to,
                    *n,
                )?;
                Ok(ColumnarValue::Array(result))
            }
            (from_arg, to_arg, max_arg) => {
                let from_array = from_arg.to_array(num_rows)?;
                let to_array = to_arg.to_array(num_rows)?;
                let max_array = max_arg.to_array(num_rows)?;
                let result = array_replace_n_inner(
                    &list_array,
                    &from_array,
                    &to_array,
                    &max_array,
                )?;
                Ok(ColumnarValue::Array(result))
            }
        }
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
    description = "Replaces all occurrences of the specified element with another specified element.",
    syntax_example = "array_replace_all(array, from, to)",
    sql_example = r#"```sql
> select array_replace_all([1, 2, 2, 3, 2, 1, 4], 2, 5);
+------------------------------------------------------------+
| array_replace_all(List([1,2,2,3,2,1,4]),Int64(2),Int64(5)) |
+------------------------------------------------------------+
| [1, 5, 5, 3, 5, 1, 4]                                      |
+------------------------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(name = "from", description = "Initial element."),
    argument(name = "to", description = "Final element.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct ArrayReplaceAll {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayReplaceAll {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::ArraySignature(
                    ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Array,
                            ArrayFunctionArgument::Element,
                            ArrayFunctionArgument::Element,
                        ],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    },
                ),
                volatility: Volatility::Immutable,
                parameter_names: None,
            },
            aliases: vec![String::from("list_replace_all")],
        }
    }
}

impl ScalarUDFImpl for ArrayReplaceAll {
    fn name(&self) -> &str {
        "array_replace_all"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [list_arg, from_arg, to_arg] = take_function_args(self.name(), &args.args)?;
        let num_rows = args.number_rows;
        let list_array = list_arg.to_array(num_rows)?;
        match (from_arg, to_arg) {
            (ColumnarValue::Scalar(scalar_from), ColumnarValue::Scalar(scalar_to)) => {
                let result = array_replace_with_scalar_args(
                    &list_array,
                    scalar_from,
                    scalar_to,
                    i64::MAX,
                )?;
                Ok(ColumnarValue::Array(result))
            }
            (from_arg, to_arg) => {
                let from_array = from_arg.to_array(num_rows)?;
                let to_array = to_arg.to_array(num_rows)?;
                let result = array_replace_internal(
                    &list_array,
                    &from_array,
                    &to_array,
                    &[Some(i64::MAX)],
                )?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// For each element of `list_array[i]`, replaces up to `arr_n[i]`  occurrences
/// of `from_array[i]`, `to_array[i]`.
///
/// The type of each **element** in `list_array` must be the same as the type of
/// `from_array` and `to_array`. This function also handles nested arrays
/// (\[`ListArray`\] of \[`ListArray`\]s)
///
/// For example, when called to replace a list array (where each element is a
/// list of int32s, the second and third argument are int32 arrays, and the
/// fourth argument is the number of occurrences to replace
///
/// ```text
/// general_replace(
///   [1, 2, 3, 2], 2, 10, 1    ==> [1, 10, 3, 2]   (only the first 2 is replaced)
///   [4, 5, 6, 5], 5, 20, 2    ==> [4, 20, 6, 20]  (both 5s are replaced)
/// )
/// ```
fn general_replace<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    from_array: &ArrayRef,
    to_array: &ArrayRef,
    arr_n: &[Option<i64>],
) -> Result<ArrayRef> {
    // Build up the offsets for the final output array
    let mut offsets: Vec<O> = vec![O::usize_as(0)];
    let values = list_array.values();
    let original_data = values.to_data();
    let to_data = to_array.to_data();
    let capacity = Capacities::Array(original_data.len());

    // First array is the original array, second array is the element to replace with.
    let mut mutable = MutableArrayData::with_capacities(
        vec![&original_data, &to_data],
        false,
        capacity,
    );

    let mut valid = NullBufferBuilder::new(list_array.len());

    for (row_index, offset_window) in list_array.offsets().windows(2).enumerate() {
        if list_array.is_null(row_index) {
            offsets.push(offsets[row_index]);
            valid.append_null();
            continue;
        }

        let n = if arr_n.len() == 1 {
            arr_n[0]
        } else {
            arr_n[row_index]
        };
        let Some(n) = n else {
            offsets.push(offsets[row_index]);
            valid.append_null();
            continue;
        };

        let start = offset_window[0];
        let end = offset_window[1];

        let list_array_row = list_array.value(row_index);

        // Compute all positions in list_row_array (that is itself an
        // array) that are equal to `from_array_row`
        let eq_array =
            compare_element_to_list(&list_array_row, &from_array, row_index, true)?;

        let original_idx = O::usize_as(0);
        let replace_idx = O::usize_as(1);
        let mut counter = 0;

        // All elements are false, no need to replace, just copy original data
        if n <= 0 || !eq_array.has_true() {
            mutable.extend(
                original_idx.to_usize().unwrap(),
                start.to_usize().unwrap(),
                end.to_usize().unwrap(),
            );
            offsets.push(offsets[row_index] + (end - start));
            valid.append_non_null();
            continue;
        }

        let mut pending_retain: Option<O> = None;
        for (i, to_replace) in eq_array.iter().enumerate() {
            let i = O::usize_as(i);
            if to_replace == Some(true) && counter < n {
                // Flush any pending retain run before emitting the replacement.
                if let Some(rs) = pending_retain.take() {
                    mutable.extend(
                        original_idx.to_usize().unwrap(),
                        (start + rs).to_usize().unwrap(),
                        (start + i).to_usize().unwrap(),
                    );
                }
                mutable.extend(replace_idx.to_usize().unwrap(), row_index, row_index + 1);
                counter += 1;
                if counter == n {
                    // copy original data for any matches past n
                    mutable.extend(
                        original_idx.to_usize().unwrap(),
                        (start + i).to_usize().unwrap() + 1,
                        end.to_usize().unwrap(),
                    );
                    break;
                }
            } else if pending_retain.is_none() {
                pending_retain = Some(i);
            }
        }

        // Flush trailing retain run when we exited the loop without ever
        // hitting `counter == n` (i.e. fewer than `n` matches in this row).
        if counter < n
            && let Some(rs) = pending_retain
        {
            mutable.extend(
                original_idx.to_usize().unwrap(),
                (start + rs).to_usize().unwrap(),
                end.to_usize().unwrap(),
            );
        }

        offsets.push(offsets[row_index] + (end - start));
        valid.append_non_null();
    }

    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new_list_field(list_array.value_type(), true)),
        OffsetBuffer::<O>::new(offsets.into()),
        arrow::array::make_array(data),
        valid.finish(),
    )?))
}

/// Replaces up to `max_replacements` occurrences of `needle` with the single
/// element in `to_array` for each row in `list_array`.
///
/// This is a specialized fast path for the all-scalar case that uses a single
/// bulk `not_distinct` comparison over only the visible values range, then
/// iterates match positions via `set_indices` instead of scanning every bit.
fn general_replace_with_scalar<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    needle: &Scalar<ArrayRef>,
    scalar_to: &ScalarValue,
    max_replacements: i64,
) -> Result<ArrayRef> {
    // No replacement needed - return unchanged.
    if max_replacements <= 0 {
        return Ok(Arc::new(list_array.clone()));
    }

    let first_offset = list_array.offsets()[0].to_usize().unwrap();
    let last_offset = list_array.offsets()[list_array.len()].to_usize().unwrap();
    let visible_values = list_array
        .values()
        .slice(first_offset, last_offset - first_offset);

    let to_array = scalar_to.to_array_of_size(1)?;
    let original_data = visible_values.to_data();
    let to_data = to_array.to_data();
    let capacity = Capacities::Array(original_data.len());

    let mut mutable = MutableArrayData::with_capacities(
        vec![&original_data, &to_data],
        false,
        capacity,
    );

    let mut offsets = OffsetBufferBuilder::<O>::new(list_array.len());

    // Single bulk comparison over the visible values only.
    let match_bitmap = arrow_ord::cmp::not_distinct(&visible_values, needle)?;
    let match_bits = match_bitmap.values();

    for (row_index, offset_window) in list_array.offsets().windows(2).enumerate() {
        // Offsets relative to visible_values (subtract first_offset).
        let start = offset_window[0].to_usize().unwrap() - first_offset;
        let end = offset_window[1].to_usize().unwrap() - first_offset;
        let row_len = end - start;

        if list_array.is_null(row_index) {
            offsets.push_length(0);
            continue;
        }

        // Slice the match bits to this row and iterate only over true positions.
        let row_bits = match_bits.slice(start, row_len);
        let mut match_positions = row_bits
            .set_indices()
            .take(max_replacements as usize)
            .peekable();
        if match_positions.peek().is_none() {
            mutable.extend(0, start, end);
            offsets.push_length(row_len);
            continue;
        }

        // Iterate only over the positions that match using set_indices,
        // which is more efficient than scanning every bit because the number
        // of matches is typically much smaller than the total array size.
        let mut prev_end = 0usize;
        for match_pos in match_positions {
            // Retain elements before this match.
            if match_pos > prev_end {
                mutable.extend(0, start + prev_end, start + match_pos);
            }
            // Emit the replacement element.
            mutable.extend(1, 0, 1);
            prev_end = match_pos + 1;
        }

        // Copy remaining elements after the last replacement.
        if prev_end < row_len {
            mutable.extend(0, start + prev_end, end);
        }

        offsets.push_length(row_len);
    }

    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new_list_field(list_array.value_type(), true)),
        offsets.finish(),
        arrow::array::make_array(data),
        list_array.nulls().cloned(),
    )?))
}

/// Fast path for `array_replace` when all arguments are scalars.
///
/// Uses a single bulk `not_distinct` comparison instead of per-row comparisons.
fn array_replace_with_scalar_args(
    list_array: &ArrayRef,
    scalar_from: &ScalarValue,
    scalar_to: &ScalarValue,
    max_replacements: i64,
) -> Result<ArrayRef> {
    // `not_distinct` doesn't support nested types, fall back to the generic array path.
    if scalar_from.data_type().is_nested() {
        let num_rows = list_array.len();
        let from_array = scalar_from.to_array_of_size(num_rows)?;
        let to_array = scalar_to.to_array_of_size(num_rows)?;
        return array_replace_internal(
            list_array,
            &from_array,
            &to_array,
            &vec![Some(max_replacements); num_rows],
        );
    }

    let needle = Scalar::new(scalar_from.to_array_of_size(1)?);
    match list_array.data_type() {
        DataType::List(_) => {
            let list = list_array.as_list::<i32>();
            general_replace_with_scalar::<i32>(list, &needle, scalar_to, max_replacements)
        }
        DataType::LargeList(_) => {
            let list = list_array.as_list::<i64>();
            general_replace_with_scalar::<i64>(list, &needle, scalar_to, max_replacements)
        }
        DataType::Null => Ok(new_null_array(list_array.data_type(), list_array.len())),
        array_type => exec_err!("array_replace does not support type '{array_type}'."),
    }
}

fn array_replace_internal(
    array: &ArrayRef,
    from: &ArrayRef,
    to: &ArrayRef,
    arr_n: &[Option<i64>],
) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::List(_) => {
            let list_array = array.as_list::<i32>();
            general_replace::<i32>(list_array, from, to, arr_n)
        }
        DataType::LargeList(_) => {
            let list_array = array.as_list::<i64>();
            general_replace::<i64>(list_array, from, to, arr_n)
        }
        DataType::Null => Ok(new_null_array(array.data_type(), array.len())),
        array_type => exec_err!("array_replace does not support type '{array_type}'."),
    }
}

fn array_replace_n_inner(
    array: &ArrayRef,
    from: &ArrayRef,
    to: &ArrayRef,
    max: &ArrayRef,
) -> Result<ArrayRef> {
    let arr_n = as_int64_array(max)?.iter().collect::<Vec<_>>();
    array_replace_internal(array, from, to, &arr_n)
}

#[cfg(test)]
mod tests {
    use super::{ArrayReplaceN, array_replace_n_inner};
    use arrow::array::{ArrayRef, AsArray, Int32Array, Int64Array, ListArray};
    use arrow::buffer::{NullBuffer, ScalarBuffer};
    use arrow::datatypes::{DataType, Field, Int32Type};
    use datafusion_common::{Result, ScalarValue, config::ConfigOptions};
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use std::sync::Arc;

    #[test]
    fn test_array_replace_n_null_max_returns_null() -> Result<()> {
        let array: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![Some(4), Some(2)]),
            ]));
        let from: ArrayRef = Arc::new(Int32Array::from(vec![2, 2]));
        let to: ArrayRef = Arc::new(Int32Array::from(vec![9, 9]));
        let max: ArrayRef = Arc::new(Int64Array::new(
            ScalarBuffer::from(vec![1, 1]),
            Some(NullBuffer::from(vec![true, false])),
        ));

        let result = array_replace_n_inner(&array, &from, &to, &max)?;
        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(9), Some(3)]),
            None,
        ]);

        assert_eq!(result.as_list::<i32>(), &expected);

        Ok(())
    }

    #[test]
    fn test_array_replace_n_scalar_null_max_returns_null() -> Result<()> {
        let array: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![Some(4), Some(2)]),
            ]));
        let array_field = Arc::new(Field::new("array", array.data_type().clone(), true));

        let result = ArrayReplaceN::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::clone(&array)),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(2))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(9))),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            arg_fields: vec![
                Arc::clone(&array_field),
                Arc::new(Field::new("from", DataType::Int32, false)),
                Arc::new(Field::new("to", DataType::Int32, false)),
                Arc::new(Field::new("max", DataType::Int64, true)),
            ],
            number_rows: array.len(),
            return_field: Arc::clone(&array_field),
            config_options: Arc::new(ConfigOptions::default()),
        })?;

        let result = result.into_array(array.len())?;
        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Option::<Vec<Option<i32>>>::None,
            Option::<Vec<Option<i32>>>::None,
        ]);

        assert_eq!(result.as_list::<i32>(), &expected);

        Ok(())
    }
}
