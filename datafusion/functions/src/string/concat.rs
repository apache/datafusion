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

use arrow::array::{
    as_largestring_array, Array, FixedSizeListArray, LargeListArray, ListArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute;
use arrow::datatypes::DataType;
use datafusion_expr::sort_properties::ExprProperties;
use std::any::Any;
use std::sync::Arc;

use crate::string::concat;
use crate::strings::{
    ColumnarValueRef, LargeStringArrayBuilder, StringArrayBuilder, StringViewArrayBuilder,
};
use datafusion_common::cast::{as_string_array, as_string_view_array};
use datafusion_common::{internal_err, plan_err, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{lit, ColumnarValue, Documentation, Expr, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Concatenates multiple strings or arrays together.",
    syntax_example = "concat(str[, ..., str_n]) or concat(array[, ..., array_n])",
    sql_example = r#"```sql
> select concat('data', 'f', 'us', 'ion');
+-------------------------------------------------------+
| concat(Utf8("data"),Utf8("f"),Utf8("us"),Utf8("ion")) |
+-------------------------------------------------------+
| datafusion                                            |
+-------------------------------------------------------+
> select concat(make_array(1, 2), make_array(3, 4));
+--------------------------------------------------+
| concat(make_array(1, 2), make_array(3, 4))      |
+--------------------------------------------------+
| [1, 2, 3, 4]                                     |
+--------------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String or Array"),
    argument(
        name = "str_n",
        description = "Subsequent string or array expressions to concatenate."
    ),
    related_udf(name = "concat_ws")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConcatFunc {
    signature: Signature,
}

impl Default for ConcatFunc {
    fn default() -> Self {
        ConcatFunc::new()
    }
}

impl ConcatFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    /// Infer the element type from input arguments, even when arrays are null.
    /// This helps maintain type consistency between planning and execution.
    fn infer_element_type_from_args(&self, args: &[ColumnarValue]) -> Result<DataType> {
        // Look for any non-null array to get the element type
        for arg in args {
            match arg {
                ColumnarValue::Array(array) => {
                    if let DataType::List(field)
                    | DataType::LargeList(field)
                    | DataType::FixedSizeList(field, _) = array.data_type()
                    {
                        return Ok(field.data_type().clone());
                    }
                }
                ColumnarValue::Scalar(scalar) => match scalar {
                    ScalarValue::List(list_array) => {
                        if let DataType::List(field) = list_array.data_type() {
                            return Ok(field.data_type().clone());
                        }
                    }
                    ScalarValue::LargeList(list_array) => {
                        if let DataType::LargeList(field) = list_array.data_type() {
                            return Ok(field.data_type().clone());
                        }
                    }
                    ScalarValue::FixedSizeList(list_array) => {
                        if let DataType::FixedSizeList(field, _) = list_array.data_type()
                        {
                            return Ok(field.data_type().clone());
                        }
                    }
                    _ => {}
                },
            }
        }

        // If no array type found, default to Int32 (common for cast operations)
        Ok(DataType::Int32)
    }

    /// Concatenates array arguments into a single array.
    ///
    /// This function handles array concatenation by extracting elements from each input array
    /// and combining them into a single result array. It optimizes for single-row vs multi-row
    /// processing to avoid unnecessary scalar-to-array conversions.
    ///
    /// # Arguments
    /// * `args` - Array of ColumnarValue inputs (Arrays or Scalar arrays)
    /// * `num_rows` - Number of rows to process
    ///
    /// # Returns
    /// A ColumnarValue containing a ListArray with concatenated elements
    fn concat_arrays(
        &self,
        args: &[ColumnarValue],
        num_rows: usize,
    ) -> Result<ColumnarValue> {
        if args.is_empty() {
            return plan_err!("concat requires at least one argument");
        }

        if num_rows == 1 {
            return self.concat_arrays_single_row(args);
        }

        // Multi-row case: process each row individually to avoid scalar-to-array conversion
        self.concat_arrays_multi_row(args, num_rows)
    }

    /// Fast path for single-row array concatenation.
    ///
    /// When processing only a single row, this optimized path avoids the overhead
    /// of row-by-row iteration and directly processes all input arrays to extract
    /// their elements and concatenate them into a single result array.
    ///
    /// This method handles both Array and Scalar array inputs by extracting elements
    /// from each non-null input and combining them using Arrow's compute::concat.
    ///
    /// # Arguments
    /// * `args` - Array of ColumnarValue inputs to concatenate
    ///
    /// # Returns
    /// A ColumnarValue containing a single-element ListArray with all concatenated elements
    fn concat_arrays_single_row(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let mut all_elements = Vec::new();

        for arg in args {
            match arg {
                ColumnarValue::Array(array) => {
                    if !array.is_null(0) {
                        let elements = self.extract_row_elements(array.as_ref(), 0)?;
                        all_elements.extend(elements);
                    }
                }
                ColumnarValue::Scalar(scalar) => {
                    if !scalar.is_null() {
                        // For scalars, create a single-element array directly without conversion
                        match scalar {
                            ScalarValue::List(list_array) => {
                                let elements =
                                    self.extract_row_elements(list_array.as_ref(), 0)?;
                                all_elements.extend(elements);
                            }
                            ScalarValue::LargeList(list_array) => {
                                let elements =
                                    self.extract_row_elements(list_array.as_ref(), 0)?;
                                all_elements.extend(elements);
                            }
                            ScalarValue::FixedSizeList(list_array) => {
                                let elements =
                                    self.extract_row_elements(list_array.as_ref(), 0)?;
                                all_elements.extend(elements);
                            }
                            _ => {
                                return plan_err!(
                                    "Expected array scalar, got {}",
                                    scalar.data_type()
                                )
                            }
                        }
                    }
                }
            }
        }

        if all_elements.is_empty() {
            // Return empty array when all inputs are null
            // Try to infer element type from input arguments
            let element_type = self.infer_element_type_from_args(args)?;
            let field = Arc::new(arrow::datatypes::Field::new_list_field(
                element_type.clone(),
                true,
            ));
            let offsets = OffsetBuffer::from_lengths([0]);
            let empty_values = arrow::array::new_empty_array(&element_type);
            let result = ListArray::new(field, offsets, empty_values, None);
            return Ok(ColumnarValue::Array(Arc::new(result)));
        }

        let element_refs: Vec<&dyn Array> =
            all_elements.iter().map(|a| a.as_ref()).collect();
        let concatenated = compute::concat(&element_refs)?;

        // Build single-element ListArray
        let field = Arc::new(arrow::datatypes::Field::new_list_field(
            concatenated.data_type().clone(),
            true,
        ));
        let offsets = OffsetBuffer::from_lengths([concatenated.len()]);
        let result = ListArray::new(field, offsets, concatenated, None);

        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    /// Extract elements from a specific row of an array, optimized for performance.
    ///
    /// This function handles the extraction of individual elements from different types
    /// of list arrays (List, LargeList, FixedSizeList) at a specific row index.
    /// It returns a vector of single-element arrays for each non-null element found.
    ///
    /// The extraction process:
    /// 1. Checks if the array at the given row is null (returns empty if so)
    /// 2. Gets the list value at the specified row using the appropriate array type
    /// 3. Filters out null elements and creates single-element arrays for each
    /// 4. Returns a vector of arrays ready for concatenation
    ///
    /// # Arguments
    /// * `array` - The input array (must be a List, LargeList, or FixedSizeList)
    /// * `row_idx` - The row index to extract elements from
    ///
    /// # Returns
    /// A vector of single-element arrays containing the non-null elements from the specified row
    fn extract_row_elements(
        &self,
        array: &dyn Array,
        row_idx: usize,
    ) -> Result<Vec<Arc<dyn Array>>> {
        if array.is_null(row_idx) {
            return Ok(Vec::new());
        }

        let list_value = match array.data_type() {
            DataType::List(_) => {
                let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
                list_array.value(row_idx)
            }
            DataType::LargeList(_) => {
                let list_array = array.as_any().downcast_ref::<LargeListArray>().unwrap();
                list_array.value(row_idx)
            }
            DataType::FixedSizeList(_, _) => {
                let list_array =
                    array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                list_array.value(row_idx)
            }
            _ => return plan_err!("Expected array type, got {}", array.data_type()),
        };

        // Extract non-null elements efficiently
        Ok((0..list_value.len())
            .filter(|&i| !list_value.is_null(i))
            .map(|i| list_value.slice(i, 1))
            .collect())
    }

    /// Multi-row array concatenation with efficient batching.
    ///
    /// For multiple rows, this method processes each row individually to avoid
    /// the performance penalty of converting scalar arrays to full arrays for
    /// the entire batch. It iterates through each row, extracts elements from
    /// all input arrays at that row index, concatenates them, and builds the
    /// final result array.
    ///
    /// This approach is more memory-efficient for large batches as it processes
    /// one row at a time rather than materializing all scalar arrays upfront.
    ///
    /// # Arguments
    /// * `args` - Array of ColumnarValue inputs (Arrays or Scalar arrays)
    /// * `num_rows` - Number of rows to process
    ///
    /// # Returns
    /// A ColumnarValue containing a ListArray with concatenated elements for each row
    fn concat_arrays_multi_row(
        &self,
        args: &[ColumnarValue],
        num_rows: usize,
    ) -> Result<ColumnarValue> {
        let mut result_arrays = Vec::with_capacity(num_rows);
        let mut sample_array = None;

        for row_idx in 0..num_rows {
            let mut row_elements = Vec::new();

            // Collect elements from this row across all args
            // Process each input argument for the current row, accumulating elements
            for arg in args {
                match arg {
                    ColumnarValue::Array(array) => {
                        // Keep track of a sample array for result type inference
                        if sample_array.is_none() {
                            sample_array = Some(Arc::clone(array));
                        }
                        // Extract elements from this row if not null
                        if !array.is_null(row_idx) {
                            let elements =
                                self.extract_row_elements(array.as_ref(), row_idx)?;
                            row_elements.extend(elements);
                        }
                    }
                    ColumnarValue::Scalar(scalar) => {
                        // Scalar arrays are repeated for each row - extract from index 0
                        if !scalar.is_null() {
                            match scalar {
                                ScalarValue::List(list_array) => {
                                    if sample_array.is_none() {
                                        sample_array = Some(
                                            Arc::clone(list_array) as Arc<dyn Array>
                                        );
                                    }
                                    let elements = self
                                        .extract_row_elements(list_array.as_ref(), 0)?;
                                    row_elements.extend(elements);
                                }
                                ScalarValue::LargeList(list_array) => {
                                    if sample_array.is_none() {
                                        sample_array = Some(
                                            Arc::clone(list_array) as Arc<dyn Array>
                                        );
                                    }
                                    let elements = self
                                        .extract_row_elements(list_array.as_ref(), 0)?;
                                    row_elements.extend(elements);
                                }
                                ScalarValue::FixedSizeList(list_array) => {
                                    if sample_array.is_none() {
                                        sample_array = Some(
                                            Arc::clone(list_array) as Arc<dyn Array>
                                        );
                                    }
                                    let elements = self
                                        .extract_row_elements(list_array.as_ref(), 0)?;
                                    row_elements.extend(elements);
                                }
                                _ => {
                                    return plan_err!(
                                        "Expected array scalar, got {}",
                                        scalar.data_type()
                                    )
                                }
                            }
                        }
                    }
                }
            }

            // Build concatenated result for this row
            if row_elements.is_empty() {
                // No elements found - record as null/empty for this row
                result_arrays.push(None);
            } else {
                // Concatenate all collected elements using Arrow's efficient concat
                let element_refs: Vec<&dyn Array> =
                    row_elements.iter().map(|a| a.as_ref()).collect();
                let concatenated = compute::concat(&element_refs)?;
                result_arrays.push(Some(concatenated));
            }
        }

        // Build the final result array
        if let Some(sample) = sample_array {
            self.build_list_array_result(result_arrays, &sample)
        } else {
            plan_err!("No sample array found for result construction")
        }
    }

    /// Build a ListArray result from concatenated elements.
    ///
    /// This function constructs the final ListArray from a vector of concatenated
    /// arrays (one per row). It handles the complex process of:
    /// 1. Determining the element type from a sample input array
    /// 2. Building efficient offset arrays to track list boundaries
    /// 3. Concatenating all values into a single flat values array
    /// 4. Constructing the final ListArray with proper metadata
    ///
    /// The function handles null rows (empty concatenations) by using empty
    /// ranges in the offset array, which Arrow interprets as empty lists.
    ///
    /// # Arguments
    /// * `result_arrays` - Vector of concatenated arrays for each row (None for empty rows)
    /// * `sample_array` - Sample input array used to determine element type
    ///
    /// # Returns
    /// A ColumnarValue containing a ListArray with all concatenated results
    fn build_list_array_result(
        &self,
        result_arrays: Vec<Option<Arc<dyn Array>>>,
        sample_array: &dyn Array,
    ) -> Result<ColumnarValue> {
        // Determine element type from sample array
        // Extract the inner element type from the list wrapper to create the result field
        let element_type = match sample_array.data_type() {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => field.data_type().clone(),
            _ => return plan_err!("Expected array type for element type determination"),
        };

        let field = Arc::new(arrow::datatypes::Field::new_list_field(
            element_type.clone(),
            true,
        ));

        // Build values and offsets efficiently
        // Create the flat values array and offset array that defines list boundaries
        let mut values_vec = Vec::new();
        let mut offsets = vec![0i32]; // Start with offset 0
        let mut current_offset = 0i32;

        for result in result_arrays {
            match result {
                Some(array) => {
                    // Add this array's length to the current offset
                    current_offset += array.len() as i32;
                    values_vec.push(array);
                }
                None => {
                    // Empty array for null result - offset doesn't change
                    // This creates an empty list in the final result
                }
            }
            // Record the ending offset for this list
            offsets.push(current_offset);
        }

        // Create the final flat values array containing all concatenated elements
        let values = if values_vec.is_empty() {
            // All rows were empty - create an empty array of the correct type
            arrow::array::new_empty_array(&element_type)
        } else {
            // Concatenate all row results into a single flat array
            let array_refs: Vec<&dyn Array> =
                values_vec.iter().map(|a| a.as_ref()).collect();
            compute::concat(&array_refs)?
        };

        let result = ListArray::new(
            field,
            OffsetBuffer::new(offsets.into()),
            values,
            None, // Let nulls be determined by empty ranges
        );

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

impl ScalarUDFImpl for ConcatFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "concat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        use DataType::*;

        if arg_types.is_empty() {
            return plan_err!("concat requires at least one argument");
        }

        // Check if we have array types - all inputs must be arrays if any are arrays
        let has_arrays = arg_types
            .iter()
            .any(|dt| matches!(dt, List(_) | LargeList(_) | FixedSizeList(_, _)));
        let _has_strings = arg_types
            .iter()
            .any(|dt| matches!(dt, Utf8View | Utf8 | LargeUtf8 | _));

        if has_arrays {
            // If we have arrays, validate that ALL inputs are arrays or NULL
            for dt in arg_types {
                if !matches!(dt, List(_) | LargeList(_) | FixedSizeList(_, _) | Null) {
                    return plan_err!("Cannot mix array and non-array arguments in concat function. Found array type and {}", dt);
                }
            }

            // Coerce arrays to a common element type
            // Find the first non-null, non-empty array type to use as target
            let mut target_element_type = None;
            for dt in arg_types {
                if let List(field) | LargeList(field) | FixedSizeList(field, _) = dt {
                    if !matches!(field.data_type(), Null) {
                        target_element_type = Some(field.data_type().clone());
                        break;
                    }
                }
            }

            // If we found a target type, coerce all List<Null> to that type
            if let Some(target_type) = target_element_type {
                let coerced_types: Vec<DataType> = arg_types
                    .iter()
                    .map(|dt| match dt {
                        List(field) if matches!(field.data_type(), Null) => {
                            List(Arc::new(arrow::datatypes::Field::new(
                                "item",
                                target_type.clone(),
                                true,
                            )))
                        }
                        _ => dt.clone(),
                    })
                    .collect();
                return Ok(coerced_types);
            }

            // No target type found (all are null or empty), return as-is
            return Ok(arg_types.to_vec());
        }

        let coerced_types = arg_types
            .iter()
            .map(|data_type| match data_type {
                Utf8View | Utf8 | LargeUtf8 => data_type.clone(),
                _ => Utf8,
            })
            .collect();
        Ok(coerced_types)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;

        // Check if we have any arrays (ignoring nulls)
        let array_types: Vec<&DataType> = arg_types
            .iter()
            .filter(|dt| matches!(dt, List(_) | LargeList(_) | FixedSizeList(_, _)))
            .collect();

        if !array_types.is_empty() {
            // We have arrays - return list type based on first non-null array
            for data_type in array_types {
                if let List(field) | LargeList(field) | FixedSizeList(field, _) =
                    data_type
                {
                    return Ok(List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        field.data_type().clone(),
                        true,
                    ))));
                }
            }
        }

        // Check if all arguments are null (for cast operations like CAST(NULL AS INT[]))
        if arg_types.iter().all(|dt| matches!(dt, Null)) {
            // When all are null, we need to determine from context or use a default
            // For now, return List<Int32> as a reasonable default
            return Ok(List(Arc::new(arrow::datatypes::Field::new(
                "item", Int32, true,
            ))));
        }

        let mut dt = &Utf8;
        for data_type in arg_types.iter() {
            if data_type == &Utf8View || (data_type == &LargeUtf8 && dt != &Utf8View) {
                dt = data_type;
            }
        }
        Ok(dt.clone())
    }

    /// Concatenates the text representations of all the arguments. NULL arguments are ignored.
    /// concat('abcde', 2, NULL, 22) = 'abcde222'
    ///
    /// Also supports array concatenation: concat([1, 2], [3, 4]) = [1, 2, 3, 4]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        if args.is_empty() {
            return plan_err!("concat requires at least one argument");
        }

        // Fast array detection - if ANY argument is an array type, route to array concatenation logic
        // This allows the function to handle both string concat and array concat seamlessly
        for arg in &args {
            let is_array = match arg {
                ColumnarValue::Array(array) => matches!(
                    array.data_type(),
                    DataType::List(_)
                        | DataType::LargeList(_)
                        | DataType::FixedSizeList(_, _)
                ),
                ColumnarValue::Scalar(scalar) => matches!(
                    scalar.data_type(),
                    DataType::List(_)
                        | DataType::LargeList(_)
                        | DataType::FixedSizeList(_, _)
                ),
            };
            if is_array {
                return self.concat_arrays(&args, number_rows);
            }
        }

        let mut return_datatype = DataType::Utf8;
        args.iter().for_each(|col| {
            if col.data_type() == DataType::Utf8View {
                return_datatype = col.data_type();
            }
            if col.data_type() == DataType::LargeUtf8
                && return_datatype != DataType::Utf8View
            {
                return_datatype = col.data_type();
            }
        });

        let array_len = args
            .iter()
            .filter_map(|x| match x {
                ColumnarValue::Array(array) => Some(array.len()),
                _ => None,
            })
            .next();

        // Scalar
        if array_len.is_none() {
            let mut result = String::new();
            for arg in args {
                let ColumnarValue::Scalar(scalar) = arg else {
                    return internal_err!("concat expected scalar value, got {arg:?}");
                };

                match scalar.try_as_str() {
                    Some(Some(v)) => result.push_str(v),
                    Some(None) => {} // null literal
                    None => {
                        // For non-string types, convert to string representation
                        if scalar.is_null() {
                            // Skip null values
                        } else {
                            result.push_str(&format!("{scalar}"));
                        }
                    }
                }
            }

            return match return_datatype {
                DataType::Utf8View => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(result))))
                }
                DataType::Utf8 => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
                }
                DataType::LargeUtf8 => {
                    Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(result))))
                }
                other => {
                    plan_err!("Concat function does not support datatype of {other}")
                }
            };
        }

        // Array
        let len = array_len.unwrap();
        let mut data_size = 0;
        let mut columns = Vec::with_capacity(args.len());

        for arg in &args {
            match arg {
                ColumnarValue::Scalar(ScalarValue::Utf8(maybe_value))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(maybe_value))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(maybe_value)) => {
                    if let Some(s) = maybe_value {
                        data_size += s.len() * len;
                        columns.push(ColumnarValueRef::Scalar(s.as_bytes()));
                    }
                }
                ColumnarValue::Array(array) => {
                    match array.data_type() {
                        DataType::Utf8 => {
                            let string_array = as_string_array(array)?;

                            data_size += string_array.values().len();
                            let column = if array.is_nullable() {
                                ColumnarValueRef::NullableArray(string_array)
                            } else {
                                ColumnarValueRef::NonNullableArray(string_array)
                            };
                            columns.push(column);
                        }
                        DataType::LargeUtf8 => {
                            let string_array = as_largestring_array(array);

                            data_size += string_array.values().len();
                            let column = if array.is_nullable() {
                                ColumnarValueRef::NullableLargeStringArray(string_array)
                            } else {
                                ColumnarValueRef::NonNullableLargeStringArray(
                                    string_array,
                                )
                            };
                            columns.push(column);
                        }
                        DataType::Utf8View => {
                            let string_array = as_string_view_array(array)?;

                            data_size += string_array.len();
                            let column = if array.is_nullable() {
                                ColumnarValueRef::NullableStringViewArray(string_array)
                            } else {
                                ColumnarValueRef::NonNullableStringViewArray(string_array)
                            };
                            columns.push(column);
                        }
                        other => {
                            return plan_err!("Input was {other} which is not a supported datatype for concat function")
                        }
                    };
                }
                _ => return plan_err!("Unsupported argument type: {}", arg.data_type()),
            }
        }

        match return_datatype {
            DataType::Utf8 => {
                let mut builder = StringArrayBuilder::with_capacity(len, data_size);
                for i in 0..len {
                    columns
                        .iter()
                        .for_each(|column| builder.write::<true>(column, i));
                    builder.append_offset();
                }

                let string_array = builder.finish(None);
                Ok(ColumnarValue::Array(Arc::new(string_array)))
            }
            DataType::Utf8View => {
                let mut builder = StringViewArrayBuilder::with_capacity(len, data_size);
                for i in 0..len {
                    columns
                        .iter()
                        .for_each(|column| builder.write::<true>(column, i));
                    builder.append_offset();
                }

                let string_array = builder.finish();
                Ok(ColumnarValue::Array(Arc::new(string_array)))
            }
            DataType::LargeUtf8 => {
                let mut builder = LargeStringArrayBuilder::with_capacity(len, data_size);
                for i in 0..len {
                    columns
                        .iter()
                        .for_each(|column| builder.write::<true>(column, i));
                    builder.append_offset();
                }

                let string_array = builder.finish(None);
                Ok(ColumnarValue::Array(Arc::new(string_array)))
            }
            _ => plan_err!("Unsupported return datatype: {return_datatype}"),
        }
    }

    /// Simplify the `concat` function by
    /// 1. filtering out all `null` literals
    /// 2. concatenating contiguous literal arguments
    ///
    /// For example:
    /// `concat(col(a), 'hello ', 'world', col(b), null)`
    /// will be optimized to
    /// `concat(col(a), 'hello world', col(b))`
    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        simplify_concat(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn preserves_lex_ordering(&self, _inputs: &[ExprProperties]) -> Result<bool> {
        Ok(true)
    }
}

pub fn simplify_concat(args: Vec<Expr>) -> Result<ExprSimplifyResult> {
    if args.is_empty() {
        return plan_err!("concat requires at least one argument");
    }
    let mut new_args = Vec::with_capacity(args.len());
    let mut contiguous_scalar = "".to_string();

    let return_type = {
        let data_types: Vec<_> = args
            .iter()
            .filter_map(|expr| match expr {
                Expr::Literal(l, _) => Some(l.data_type()),
                _ => None,
            })
            .collect();
        ConcatFunc::new().return_type(&data_types)
    }?;

    for arg in args.iter() {
        match arg {
            Expr::Literal(ScalarValue::Utf8(None), _) => {}
            Expr::Literal(ScalarValue::LargeUtf8(None), _) => {}
            Expr::Literal(ScalarValue::Utf8View(None), _) => {}

            Expr::Literal(ScalarValue::Utf8(Some(v)), _) => {
                contiguous_scalar += v;
            }
            Expr::Literal(ScalarValue::LargeUtf8(Some(v)), _) => {
                contiguous_scalar += v;
            }
            Expr::Literal(ScalarValue::Utf8View(Some(v)), _) => {
                contiguous_scalar += v;
            }

            Expr::Literal(scalar_val, _) => {
                // Skip array literals - they should be handled at runtime
                if matches!(
                    scalar_val.data_type(),
                    DataType::List(_)
                        | DataType::LargeList(_)
                        | DataType::FixedSizeList(_, _)
                ) {
                    if !contiguous_scalar.is_empty() {
                        match return_type {
                            DataType::Utf8 => new_args.push(lit(contiguous_scalar)),
                            DataType::LargeUtf8 => new_args.push(lit(
                                ScalarValue::LargeUtf8(Some(contiguous_scalar)),
                            )),
                            DataType::Utf8View => new_args.push(lit(
                                ScalarValue::Utf8View(Some(contiguous_scalar)),
                            )),
                            _ => return Ok(ExprSimplifyResult::Original(args)),
                        }
                        contiguous_scalar = "".to_string();
                    }
                    new_args.push(arg.clone());
                } else {
                    // Convert non-string, non-array literals to their string representation
                    let string_repr = format!("{scalar_val}");
                    contiguous_scalar += &string_repr;
                }
            }
            arg => {
                if !contiguous_scalar.is_empty() {
                    match return_type {
                        DataType::Utf8 => new_args.push(lit(contiguous_scalar)),
                        DataType::LargeUtf8 => new_args
                            .push(lit(ScalarValue::LargeUtf8(Some(contiguous_scalar)))),
                        DataType::Utf8View => new_args
                            .push(lit(ScalarValue::Utf8View(Some(contiguous_scalar)))),
                        _ => return Ok(ExprSimplifyResult::Original(args)),
                    }
                    contiguous_scalar = "".to_string();
                }
                new_args.push(arg.clone());
            }
        }
    }

    if !contiguous_scalar.is_empty() {
        match return_type {
            DataType::Utf8 => new_args.push(lit(contiguous_scalar)),
            DataType::LargeUtf8 => {
                new_args.push(lit(ScalarValue::LargeUtf8(Some(contiguous_scalar))))
            }
            DataType::Utf8View => {
                new_args.push(lit(ScalarValue::Utf8View(Some(contiguous_scalar))))
            }
            _ => return Ok(ExprSimplifyResult::Original(args)),
        }
    }

    if !args.eq(&new_args) {
        Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
            ScalarFunction {
                func: concat(),
                args: new_args,
            },
        )))
    } else {
        Ok(ExprSimplifyResult::Original(args))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test::test_function;
    use arrow::array::{Array, LargeStringArray, StringViewArray};
    use arrow::array::{ArrayRef, StringArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;
    use DataType::*;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            ConcatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("aa")),
                ColumnarValue::Scalar(ScalarValue::from("bb")),
                ColumnarValue::Scalar(ScalarValue::from("cc")),
            ],
            Ok(Some("aabbcc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("aa")),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::from("cc")),
            ],
            Ok(Some("aacc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(None))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("aa")),
                ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)),
                ColumnarValue::Scalar(ScalarValue::from("cc")),
            ],
            Ok(Some("aacc")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            ConcatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("aa")),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)),
                ColumnarValue::Scalar(ScalarValue::from("cc")),
            ],
            Ok(Some("aacc")),
            &str,
            LargeUtf8,
            LargeStringArray
        );
        test_function!(
            ConcatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some("aa".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("cc".to_string()))),
            ],
            Ok(Some("aacc")),
            &str,
            Utf8View,
            StringViewArray
        );

        Ok(())
    }

    #[test]
    fn concat() -> Result<()> {
        let c0 =
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["foo", "bar", "baz"])));
        let c1 = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));
        let c2 = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("x"),
            None,
            Some("z"),
        ])));
        let c3 = ColumnarValue::Scalar(ScalarValue::Utf8View(Some(",".to_string())));
        let c4 = ColumnarValue::Array(Arc::new(StringViewArray::from(vec![
            Some("a"),
            None,
            Some("b"),
        ])));
        let arg_fields = vec![
            Field::new("a", Utf8, true),
            Field::new("a", Utf8, true),
            Field::new("a", Utf8, true),
            Field::new("a", Utf8View, true),
            Field::new("a", Utf8View, true),
        ]
        .into_iter()
        .map(Arc::new)
        .collect();

        let args = ScalarFunctionArgs {
            args: vec![c0, c1, c2, c3, c4],
            arg_fields,
            number_rows: 3,
            return_field: Field::new("f", Utf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatFunc::new().invoke_with_args(args)?;
        let expected =
            Arc::new(StringViewArray::from(vec!["foo,x,a", "bar,,", "baz,z,b"]))
                as ArrayRef;
        match &result {
            ColumnarValue::Array(array) => {
                assert_eq!(&expected, array);
            }
            _ => panic!(),
        }
        Ok(())
    }

    #[test]
    fn test_concat_with_integers() -> Result<()> {
        use datafusion_common::config::ConfigOptions;

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(123))),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)), // NULL
            ColumnarValue::Scalar(ScalarValue::Int64(Some(456))),
        ];

        let arg_fields = vec![
            Field::new("a", Utf8, true),
            Field::new("b", Int64, true),
            Field::new("c", Utf8, true),
            Field::new("d", Int64, true),
        ]
        .into_iter()
        .map(Arc::new)
        .collect();

        let func_args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", Utf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatFunc::new().invoke_with_args(func_args)?;

        // Expected result should be "abc123456"
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                assert_eq!(s, "abc123456");
            }
            _ => panic!("Expected scalar UTF8 result, got {result:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_concat_arrays_basic() -> Result<()> {
        use arrow::array::{Int32Array, ListArray};
        use datafusion_common::config::ConfigOptions;

        let field = Arc::new(Field::new("item", Int32, true));
        let array1 = ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::from_lengths([3]),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            None,
        );
        let array2 = ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::from_lengths([2]),
            Arc::new(Int32Array::from(vec![4, 5])),
            None,
        );

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(array1)),
                ColumnarValue::Array(Arc::new(array2)),
            ],
            arg_fields: vec![
                Arc::new(Field::new("a", List(Arc::clone(&field)), true)),
                Arc::new(Field::new("b", List(Arc::clone(&field)), true)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("f", List(field), true)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatFunc::new().invoke_with_args(args)?;
        if let ColumnarValue::Array(array) = result {
            let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
            let array_value = list_array.value(0);
            let values = array_value.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(values.values(), &[1, 2, 3, 4, 5]);
        }
        Ok(())
    }

    #[test]
    fn test_concat_arrays_multi_row() -> Result<()> {
        use arrow::array::{Int32Array, ListArray};
        use datafusion_common::config::ConfigOptions;

        let field = Arc::new(Field::new("item", Int32, true));
        let array1 = ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::from_lengths([2, 2]),
            Arc::new(Int32Array::from(vec![1, 2, 10, 20])),
            None,
        );
        let array2 = ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::from_lengths([1, 1]),
            Arc::new(Int32Array::from(vec![3, 30])),
            None,
        );

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(array1)),
                ColumnarValue::Array(Arc::new(array2)),
            ],
            arg_fields: vec![
                Arc::new(Field::new("a", List(Arc::clone(&field)), true)),
                Arc::new(Field::new("b", List(Arc::clone(&field)), true)),
            ],
            number_rows: 2,
            return_field: Arc::new(Field::new("f", List(field), true)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatFunc::new().invoke_with_args(args)?;
        if let ColumnarValue::Array(array) = result {
            let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(list_array.len(), 2);

            let array_value1 = list_array.value(0);
            let row1 = array_value1.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(row1.values(), &[1, 2, 3]);

            let array_value2 = list_array.value(1);
            let row2 = array_value2.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(row2.values(), &[10, 20, 30]);
        }
        Ok(())
    }

    #[test]
    fn test_concat_arrays_with_nulls() -> Result<()> {
        use arrow::array::{Int32Array, ListArray};
        use datafusion_common::config::ConfigOptions;

        let field = Arc::new(Field::new("item", Int32, true));
        let array1 = ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::from_lengths([2]),
            Arc::new(Int32Array::from(vec![1, 2])),
            Some(NullBuffer::new_null(1)),
        );
        let array2 = ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::from_lengths([2]),
            Arc::new(Int32Array::from(vec![3, 4])),
            None,
        );

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(array1)),
                ColumnarValue::Array(Arc::new(array2)),
            ],
            arg_fields: vec![
                Arc::new(Field::new("a", List(Arc::clone(&field)), true)),
                Arc::new(Field::new("b", List(Arc::clone(&field)), true)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("f", List(field), true)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatFunc::new().invoke_with_args(args)?;
        if let ColumnarValue::Array(array) = result {
            let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
            let array_value = list_array.value(0);
            let values = array_value.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(values.values(), &[3, 4]);
        }
        Ok(())
    }
}
