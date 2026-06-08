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

//! [`ScalarUDFImpl`] definitions for arrays_zip function.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, Capacities, ListArray, MutableArrayData, NullBufferBuilder,
    StructArray, new_null_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::DataType::{FixedSizeList, LargeList, List, Null};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::cast::{
    as_fixed_size_list_array, as_large_list_array, as_list_array,
};
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

/// Type-erased view of a list column (works for both List and LargeList).
/// Stores the information needed to iterate rows without re-downcasting.
struct ListColumnView {
    /// The flat values array backing this list column.
    values: ArrayRef,
    /// Pre-computed per-row start offsets (length = num_rows + 1).
    offsets: Vec<usize>,
    /// Null bitmap from the input array (None means no nulls).
    nulls: Option<NullBuffer>,
}

impl ListColumnView {
    fn is_null(&self, idx: usize) -> bool {
        self.nulls.as_ref().is_some_and(|n| n.is_null(idx))
    }
}

make_udf_expr_and_func!(
    ArraysZip,
    arrays_zip,
    "combines one or multiple arrays into a single array of structs.",
    arrays_zip_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns an array of structs created by combining the elements of each input array at the same index. If the arrays have different lengths, shorter arrays are padded with NULLs.",
    syntax_example = "arrays_zip(array1[, ..., array_n])",
    sql_example = r#"```sql
> select arrays_zip([1, 2, 3]);
+---------------------------------------------------+
| arrays_zip([1, 2, 3])                             |
+---------------------------------------------------+
| [{1: 1}, {1: 2}, {1: 3}]                          |
+---------------------------------------------------+
> select arrays_zip([1, 2], [3, 4, 5]);
+---------------------------------------------------+
| arrays_zip([1, 2], [3, 4, 5])                     |
+---------------------------------------------------+
| [{1: 1, 2: 3}, {1: 2, 2: 4}, {1: NULL, 2: 5}]     |
+---------------------------------------------------+
```"#,
    argument(name = "array1", description = "First array expression."),
    argument(
        name = "array_n",
        description = "Optional additional array expressions."
    )
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
    fn name(&self) -> &str {
        "arrays_zip"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return exec_err!("arrays_zip requires at least one argument");
        }

        let mut fields = Vec::with_capacity(arg_types.len());
        for (i, arg_type) in arg_types.iter().enumerate() {
            let element_type = match arg_type {
                List(field) | LargeList(field) | FixedSizeList(field, _) => {
                    field.data_type().clone()
                }
                Null => Null,
                dt => {
                    return exec_err!("arrays_zip expects array arguments, got {dt}");
                }
            };
            fields.push(Field::new(arrays_zip_field_name(i), element_type, true));
        }

        Ok(List(Arc::new(Field::new_list_field(
            DataType::Struct(Fields::from(fields)),
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
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
/// Supports List, LargeList, and Null input types.
fn arrays_zip_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() {
        return exec_err!("arrays_zip requires at least one argument");
    }

    let field_names = arrays_zip_field_names(args.len());
    let num_rows = args[0].len();

    if let Some(result) = try_perfect_list_zip(args, &field_names)? {
        return Ok(result);
    }

    // Build a type-erased ListColumnView for each argument.
    // None means the argument is Null-typed (all nulls, no backing data).
    let mut views: Vec<Option<ListColumnView>> = Vec::with_capacity(args.len());
    let mut element_types: Vec<DataType> = Vec::with_capacity(args.len());

    for (i, arg) in args.iter().enumerate() {
        match arg.data_type() {
            List(field) => {
                let arr = as_list_array(arg)?;
                let raw_offsets = arr.value_offsets();
                let offsets: Vec<usize> =
                    raw_offsets.iter().map(|&o| o as usize).collect();
                element_types.push(field.data_type().clone());
                views.push(Some(ListColumnView {
                    values: Arc::clone(arr.values()),
                    offsets,
                    nulls: arr.nulls().cloned(),
                }));
            }
            LargeList(field) => {
                let arr = as_large_list_array(arg)?;
                let raw_offsets = arr.value_offsets();
                let offsets: Vec<usize> =
                    raw_offsets.iter().map(|&o| o as usize).collect();
                element_types.push(field.data_type().clone());
                views.push(Some(ListColumnView {
                    values: Arc::clone(arr.values()),
                    offsets,
                    nulls: arr.nulls().cloned(),
                }));
            }
            FixedSizeList(field, size) => {
                let arr = as_fixed_size_list_array(arg)?;
                let size = *size as usize;
                let offsets: Vec<usize> = (0..=num_rows).map(|row| row * size).collect();
                element_types.push(field.data_type().clone());
                views.push(Some(ListColumnView {
                    values: Arc::clone(arr.values()),
                    offsets,
                    nulls: arr.nulls().cloned(),
                }));
            }
            Null => {
                element_types.push(Null);
                views.push(None);
            }
            dt => {
                return exec_err!("arrays_zip argument {i} expected list type, got {dt}");
            }
        }
    }

    // Collect per-column values data for MutableArrayData builders.
    let values_data: Vec<_> = views
        .iter()
        .map(|v| v.as_ref().map(|view| view.values.to_data()))
        .collect();

    let struct_fields: Fields = element_types
        .iter()
        .zip(field_names.iter())
        .map(|(dt, name)| Field::new(name.clone(), dt.clone(), true))
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
    let mut null_builder = NullBufferBuilder::new(num_rows);
    let mut total_values: usize = 0;

    // Process each row: compute per-array lengths, then copy values
    // and pad shorter arrays with NULLs.
    for row_idx in 0..num_rows {
        let mut max_len: usize = 0;
        let mut all_null = true;

        for view in views.iter().flatten() {
            if !view.is_null(row_idx) {
                all_null = false;
                let len = view.offsets[row_idx + 1] - view.offsets[row_idx];
                max_len = max_len.max(len);
            }
        }

        if all_null {
            null_builder.append_null();
            offsets.push(*offsets.last().unwrap());
            continue;
        }
        null_builder.append_non_null();

        // Extend each column builder for this row.
        for (col_idx, view) in views.iter().enumerate() {
            match view {
                Some(v) if !v.is_null(row_idx) => {
                    let start = v.offsets[row_idx];
                    let end = v.offsets[row_idx + 1];
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

    let null_buffer = null_builder.finish();

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

fn arrays_zip_field_name(index: usize) -> String {
    (index + 1).to_string()
}

fn arrays_zip_field_names(len: usize) -> Vec<String> {
    (0..len).map(arrays_zip_field_name).collect()
}

/// Fast path for regular List inputs whose existing buffers already match the
/// zipped output: all offsets and values lengths match, and null rows cover no
/// values. This lets us reuse offsets and child values instead of rebuilding.
fn try_perfect_list_zip(
    args: &[ArrayRef],
    field_names: &[String],
) -> Result<Option<ArrayRef>> {
    debug_assert_eq!(args.len(), field_names.len());

    let mut list_arrays = Vec::with_capacity(args.len());
    let mut struct_fields = Vec::with_capacity(args.len());

    for (arg, field_name) in args.iter().zip(field_names) {
        let arr = match arg.data_type() {
            List(field) => {
                struct_fields.push(Field::new(
                    field_name.clone(),
                    field.data_type().clone(),
                    true,
                ));
                as_list_array(arg)?
            }
            _ => return Ok(None),
        };

        list_arrays.push(arr);
    }

    let first = list_arrays[0];
    let num_rows = first.len();
    let offsets = first.offsets().clone();
    let values_len = first.values().len();

    // Reusing the child arrays is only valid when every list uses the exact
    // same row boundaries and exposes the same total number of child values.
    for arr in &list_arrays {
        if arr.values().len() != values_len || arr.offsets() != &offsets {
            return Ok(None);
        }
    }

    let nulls = if list_arrays.iter().any(|arr| arr.null_count() != 0) {
        let first_nulls = first.nulls();
        if list_arrays.iter().all(|arr| arr.nulls() == first_nulls) {
            first_nulls.cloned()
        } else {
            // Match the general path: arrays_zip only marks an output row null
            // when every concrete input list is null. Mixed null and non-null
            // empty lists still produce a non-null empty list, but mixed null
            // rows with values must fall back to preserve field-level nulls.
            let mut null_builder = NullBufferBuilder::new(num_rows);
            for row_idx in 0..num_rows {
                let mut all_null = true;

                for arr in &list_arrays {
                    if arr.is_null(row_idx) {
                        if arr.offsets()[row_idx + 1] != arr.offsets()[row_idx] {
                            return Ok(None);
                        }
                    } else {
                        all_null = false;
                    }
                }

                if all_null {
                    null_builder.append_null();
                } else {
                    null_builder.append_non_null();
                }
            }

            null_builder.finish()
        }
    } else {
        None
    };

    let struct_columns = list_arrays
        .iter()
        .map(|arr| Arc::clone(arr.values()))
        .collect::<Vec<_>>();
    let struct_array =
        StructArray::try_new(Fields::from(struct_fields), struct_columns, None)?;
    let result = ListArray::try_new(
        Arc::new(Field::new_list_field(
            struct_array.data_type().clone(),
            true,
        )),
        offsets,
        Arc::new(struct_array),
        nulls,
    )?;

    Ok(Some(Arc::new(result)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::buffer::NullBuffer;

    fn list(values: Vec<i64>, offsets: Vec<i32>) -> Arc<ListArray> {
        list_with_validity(values, offsets, None)
    }

    fn list_with_validity(
        values: Vec<i64>,
        offsets: Vec<i32>,
        valid: Option<Vec<bool>>,
    ) -> Arc<ListArray> {
        Arc::new(
            ListArray::try_new(
                Arc::new(Field::new_list_field(DataType::Int64, true)),
                OffsetBuffer::new(offsets.into()),
                Arc::new(Int64Array::from(values)),
                valid.map(NullBuffer::from),
            )
            .unwrap(),
        )
    }

    #[test]
    fn perfect_zip_reuses_input_values_and_offsets() {
        let left = list(vec![1, 2, 3, 4, 5, 6], vec![0, 2, 3, 6]);
        let right = list(vec![10, 20, 30, 40, 50, 60], vec![0, 2, 3, 6]);

        let result = arrays_zip_inner(&[
            Arc::clone(&left) as ArrayRef,
            Arc::clone(&right) as ArrayRef,
        ])
        .unwrap();
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = result
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        assert!(result.offsets().ptr_eq(left.offsets()));
        assert!(Arc::ptr_eq(values.column(0), left.values()));
        assert!(Arc::ptr_eq(values.column(1), right.values()));
    }

    #[test]
    fn perfect_zip_uses_supplied_field_names() {
        let left = list(vec![1, 2, 3], vec![0, 1, 3]);
        let right = list(vec![10, 20, 30], vec![0, 1, 3]);
        let field_names = vec!["left".to_string(), "right".to_string()];

        let result = try_perfect_list_zip(
            &[
                Arc::clone(&left) as ArrayRef,
                Arc::clone(&right) as ArrayRef,
            ],
            &field_names,
        )
        .unwrap()
        .unwrap();
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = result
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let names = values
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["left", "right"]);
    }

    #[test]
    fn perfect_zip_reuses_zero_length_null_rows() {
        let left = list_with_validity(
            vec![1, 2, 3, 4],
            vec![0, 2, 2, 4],
            Some(vec![true, false, true]),
        );
        let right = list_with_validity(
            vec![10, 20, 30, 40],
            vec![0, 2, 2, 4],
            Some(vec![true, false, true]),
        );

        let result = arrays_zip_inner(&[
            Arc::clone(&left) as ArrayRef,
            Arc::clone(&right) as ArrayRef,
        ])
        .unwrap();
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();

        assert!(result.offsets().ptr_eq(left.offsets()));
        assert!(result.is_null(1));
    }

    #[test]
    fn perfect_zip_preserves_mixed_null_empty_rows() {
        let left =
            list_with_validity(vec![], vec![0, 0, 0, 0], Some(vec![false, true, false]));
        let right =
            list_with_validity(vec![], vec![0, 0, 0, 0], Some(vec![true, false, false]));

        let result = arrays_zip_inner(&[
            Arc::clone(&left) as ArrayRef,
            Arc::clone(&right) as ArrayRef,
        ])
        .unwrap();
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();

        assert!(result.offsets().ptr_eq(left.offsets()));
        assert!(!result.is_null(0));
        assert!(!result.is_null(1));
        assert!(result.is_null(2));
    }

    #[test]
    fn perfect_zip_reuses_null_rows_with_hidden_values() {
        let left =
            list_with_validity(vec![1, 2, 3, 4], vec![0, 2, 4], Some(vec![true, false]));
        let right = list_with_validity(
            vec![10, 20, 30, 40],
            vec![0, 2, 4],
            Some(vec![true, false]),
        );

        let result = arrays_zip_inner(&[
            Arc::clone(&left) as ArrayRef,
            Arc::clone(&right) as ArrayRef,
        ])
        .unwrap();
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();

        assert!(result.offsets().ptr_eq(left.offsets()));
        assert_eq!(result.value_offsets(), &[0, 2, 4]);
        assert!(result.is_null(1));
    }

    #[test]
    fn mixed_null_row_with_hidden_values_uses_general_path() {
        let left =
            list_with_validity(vec![1, 2, 3, 4], vec![0, 2, 4], Some(vec![true, false]));
        let right = list_with_validity(
            vec![10, 20, 30, 40],
            vec![0, 2, 4],
            Some(vec![true, true]),
        );

        let result = arrays_zip_inner(&[
            Arc::clone(&left) as ArrayRef,
            Arc::clone(&right) as ArrayRef,
        ])
        .unwrap();
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = result
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        assert!(!result.offsets().ptr_eq(left.offsets()));
        assert_eq!(result.value_offsets(), &[0, 2, 4]);
        assert!(values.column(0).is_null(2));
        assert!(values.column(0).is_null(3));
        assert!(!values.column(1).is_null(2));
        assert!(!values.column(1).is_null(3));
    }
}
