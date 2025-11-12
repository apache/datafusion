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

use arrow::array::{as_largestring_array, Array};
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
+------------------------------------------+
| concat(make_array(1, 2), make_array(3, 4)) |
+------------------------------------------+
| [1, 2, 3, 4]                             |
+------------------------------------------+
```"#,
    standard_argument(name = "str_or_array", prefix = "String or Array"),
    argument(
        name = "str_or_array_n",
        description = "Subsequent string or array expressions to concatenate. Cannot mix strings and arrays."
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

    /// Extract array elements from a single row of arguments for array concatenation
    fn extract_arrays_from_row(
        &self,
        args: &[ColumnarValue],
        row_idx: usize,
    ) -> Result<Vec<Arc<dyn Array>>> {
        let mut inner_arrays = Vec::new();

        for arg in args {
            match arg {
                ColumnarValue::Array(arr) => {
                    if let Some(array) = self.extract_list_value(arr, row_idx)? {
                        inner_arrays.push(array);
                    }
                }
                ColumnarValue::Scalar(scalar) => {
                    if let Some(array) = self.extract_scalar_list_value(scalar)? {
                        inner_arrays.push(array);
                    }
                }
            }
        }

        Ok(inner_arrays)
    }

    /// Extract list value from array at given row index, handling all list types
    fn extract_list_value(
        &self,
        arr: &Arc<dyn Array>,
        row_idx: usize,
    ) -> Result<Option<Arc<dyn Array>>> {
        use arrow::array::*;

        if arr.is_null(row_idx) {
            return Ok(None);
        }

        match arr.data_type() {
            DataType::List(_) => {
                let list_array = arr.as_any().downcast_ref::<ListArray>()
                    .ok_or_else(|| datafusion_common::DataFusionError::Plan("Failed to downcast to ListArray".to_string()))?;
                Ok(Some(list_array.value(row_idx)))
            }
            DataType::LargeList(_) => {
                let list_array = arr.as_any().downcast_ref::<LargeListArray>()
                    .ok_or_else(|| datafusion_common::DataFusionError::Plan("Failed to downcast to LargeListArray".to_string()))?;
                Ok(Some(list_array.value(row_idx)))
            }
            DataType::FixedSizeList(_, _) => {
                let list_array = arr.as_any().downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| datafusion_common::DataFusionError::Plan("Failed to downcast to FixedSizeListArray".to_string()))?;
                Ok(Some(list_array.value(row_idx)))
            }
            _ => plan_err!("Expected list array, got {}", arr.data_type()),
        }
    }

    /// Extract list value from scalar, handling all list scalar types
    fn extract_scalar_list_value(
        &self,
        scalar: &ScalarValue,
    ) -> Result<Option<Arc<dyn Array>>> {
        use arrow::array::*;

        if scalar.is_null() {
            return Ok(None);
        }

        match scalar {
            ScalarValue::List(arr) => {
                let list_array = arr.as_any().downcast_ref::<ListArray>()
                    .ok_or_else(|| datafusion_common::DataFusionError::Plan("Failed to downcast scalar List to ListArray".to_string()))?;
                if !list_array.is_null(0) {
                    Ok(Some(list_array.value(0)))
                } else {
                    Ok(None)
                }
            }
            ScalarValue::LargeList(arr) => {
                let list_array = arr.as_any().downcast_ref::<LargeListArray>()
                    .ok_or_else(|| datafusion_common::DataFusionError::Plan("Failed to downcast scalar LargeList to LargeListArray".to_string()))?;
                if !list_array.is_null(0) {
                    Ok(Some(list_array.value(0)))
                } else {
                    Ok(None)
                }
            }
            ScalarValue::FixedSizeList(arr) => {
                let list_array = arr.as_any().downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| datafusion_common::DataFusionError::Plan("Failed to downcast scalar FixedSizeList to FixedSizeListArray".to_string()))?;
                if !list_array.is_null(0) {
                    Ok(Some(list_array.value(0)))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    /// Get the string type with highest precedence: Utf8View > LargeUtf8 > Utf8
    fn get_string_type_precedence(&self, arg_types: &[DataType]) -> DataType {
        use DataType::*;

        for data_type in arg_types {
            if data_type == &Utf8View {
                return Utf8View;
            }
        }

        for data_type in arg_types {
            if data_type == &LargeUtf8 {
                return LargeUtf8;
            }
        }

        Utf8
    }

    /// Determine element type from first valid array argument
    fn determine_element_type(&self, args: &[ColumnarValue]) -> Result<DataType> {
        for arg in args {
            match arg {
                ColumnarValue::Array(arr) => match arr.data_type() {
                    DataType::List(field)
                    | DataType::LargeList(field)
                    | DataType::FixedSizeList(field, _) => {
                        return Ok(field.data_type().clone());
                    }
                    _ => {}
                },
                ColumnarValue::Scalar(scalar) => match scalar {
                    ScalarValue::List(arr) => {
                        if let DataType::List(field) = arr.data_type() {
                            return Ok(field.data_type().clone());
                        }
                    }
                    ScalarValue::LargeList(arr) => {
                        if let DataType::LargeList(field) = arr.data_type() {
                            return Ok(field.data_type().clone());
                        }
                    }
                    ScalarValue::FixedSizeList(arr) => {
                        if let DataType::FixedSizeList(field, _) = arr.data_type() {
                            return Ok(field.data_type().clone());
                        }
                    }
                    _ => {}
                },
            }
        }
        plan_err!("No valid array arguments found. Expected at least one array argument for array concatenation.")
    }

    /// Concatenate array arguments into a single array using runtime delegation to Arrow's compute::concat
    fn concat_arrays(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        use arrow::array::*;
        use arrow::buffer::OffsetBuffer;

        if args.is_empty() {
            return plan_err!("concat requires at least one argument");
        }

        let array_len = args
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(array) => Some(array.len()),
                _ => None,
            })
            .unwrap_or(1);

        if array_len == 1 {
            // Single row case
            let inner_arrays = self.extract_arrays_from_row(args, 0)?;

            if inner_arrays.is_empty() {
                return plan_err!("No valid arrays to concatenate. All array arguments were null or empty.");
            }

            let array_refs: Vec<&dyn Array> =
                inner_arrays.iter().map(|a| a.as_ref()).collect();
            let concatenated = compute::concat(&array_refs)?;

            let field = Arc::new(arrow::datatypes::Field::new_list_field(
                concatenated.data_type().clone(),
                true,
            ));
            let offsets = OffsetBuffer::from_lengths([concatenated.len()]);
            let result_list = ListArray::new(field, offsets, concatenated, None);

            Ok(ColumnarValue::Array(Arc::new(result_list)))
        } else {
            // Multi-row case
            let mut result_arrays = Vec::with_capacity(array_len);

            for row_idx in 0..array_len {
                let row_inner_arrays = self.extract_arrays_from_row(args, row_idx)?;

                if row_inner_arrays.is_empty() {
                    result_arrays.push(None);
                } else {
                    let array_refs: Vec<&dyn Array> =
                        row_inner_arrays.iter().map(|a| a.as_ref()).collect();
                    let concatenated = compute::concat(&array_refs)?;
                    result_arrays.push(Some(concatenated));
                }
            }

            let element_type = self.determine_element_type(args)?;
            let field = Arc::new(arrow::datatypes::Field::new_list_field(
                element_type.clone(),
                true,
            ));

            // Build final ListArray with proper offsets
            let mut values_vec = Vec::new();
            let mut lengths = Vec::with_capacity(array_len);

            for result in result_arrays {
                match result {
                    Some(array) => {
                        lengths.push(array.len());
                        values_vec.push(array);
                    }
                    None => {
                        lengths.push(0);
                    }
                }
            }

            let values = if values_vec.is_empty() {
                new_empty_array(&element_type)
            } else {
                let array_refs: Vec<&dyn Array> =
                    values_vec.iter().map(|a| a.as_ref()).collect();
                compute::concat(&array_refs)?
            };

            let offsets = OffsetBuffer::from_lengths(lengths);
            let result_list = ListArray::new(field, offsets, values, None);

            Ok(ColumnarValue::Array(Arc::new(result_list)))
        }
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

        let has_arrays = arg_types
            .iter()
            .any(|dt| matches!(dt, List(_) | LargeList(_) | FixedSizeList(_, _)));
        let has_non_arrays = arg_types
            .iter()
            .any(|dt| !matches!(dt, List(_) | LargeList(_) | FixedSizeList(_, _) | Null));

        if has_arrays && has_non_arrays {
            return plan_err!(
                "Cannot mix array and non-array arguments in concat function. \
                Use concat(array1, array2, ...) for arrays or concat(str1, str2, ...) for strings, but not both."
            );
        }

        if has_arrays {
            return Ok(arg_types.to_vec());
        }

        let target_type = self.get_string_type_precedence(arg_types);

        // Only coerce types that need coercion, keep string types as-is
        let coerced_types = arg_types
            .iter()
            .map(|data_type| match data_type {
                Utf8View | Utf8 | LargeUtf8 => data_type.clone(),
                _ => target_type.clone(),
            })
            .collect();
        Ok(coerced_types)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;

        // Check if any argument is an array type
        for data_type in arg_types {
            if let List(field) | LargeList(field) | FixedSizeList(field, _) = data_type {
                return Ok(List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    field.data_type().clone(),
                    true,
                ))));
            }
        }

        // For non-array arguments, return string type based on precedence
        let dt = self.get_string_type_precedence(arg_types);
        Ok(dt)
    }

    /// Concatenates the text representations of all the arguments. NULL arguments are ignored.
    /// concat('abcde', 2, NULL, 22) = 'abcde222'
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use DataType::*;
        let ScalarFunctionArgs { args, .. } = args;

        if args.is_empty() {
            return plan_err!("concat requires at least one argument");
        }

        for arg in &args {
            let is_array = match arg {
                ColumnarValue::Array(array) => matches!(
                    array.data_type(),
                    List(_) | LargeList(_) | FixedSizeList(_, _)
                ),
                ColumnarValue::Scalar(scalar) => matches!(
                    scalar.data_type(),
                    List(_) | LargeList(_) | FixedSizeList(_, _)
                ),
            };
            if is_array {
                return self.concat_arrays(&args);
            }
        }

        let data_types: Vec<DataType> = args.iter().map(|col| col.data_type()).collect();
        let return_datatype = self.get_string_type_precedence(&data_types);

        let array_len = args
            .iter()
            .filter_map(|x| match x {
                ColumnarValue::Array(array) => Some(array.len()),
                _ => None,
            })
            .next();

        // Scalar case
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
                        if scalar.is_null() {
                            // Skip null values
                        } else {
                            result.push_str(&format!("{scalar}"));
                        }
                    }
                }
            }

            return match return_datatype {
                Utf8View => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(result))))
                }
                Utf8 => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result)))),
                LargeUtf8 => {
                    Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(result))))
                }
                other => {
                    plan_err!("Concat function does not support datatype of {other}")
                }
            };
        }

        // Array case
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
                        Utf8 => {
                            let string_array = as_string_array(array)?;

                            data_size += string_array.values().len();
                            let column = if array.is_nullable() {
                                ColumnarValueRef::NullableArray(string_array)
                            } else {
                                ColumnarValueRef::NonNullableArray(string_array)
                            };
                            columns.push(column);
                        }
                        LargeUtf8 => {
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
                        Utf8View => {
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
            Utf8 => {
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
            Utf8View => {
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
            LargeUtf8 => {
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
    use DataType::*;

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

            // filter out `null` args
            // All literals have been converted to Utf8 or LargeUtf8 in type_coercion.
            // Concatenate it with the `contiguous_scalar`.
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
                // Convert non-string, non-array literals to their string representation
                // Skip array literals - they should be handled at runtime
                if matches!(
                    scalar_val.data_type(),
                    List(_) | LargeList(_) | FixedSizeList(_, _)
                ) {
                    if !contiguous_scalar.is_empty() {
                        match return_type {
                            Utf8 => new_args.push(lit(contiguous_scalar)),
                            LargeUtf8 => new_args.push(lit(ScalarValue::LargeUtf8(
                                Some(contiguous_scalar),
                            ))),
                            Utf8View => new_args.push(lit(ScalarValue::Utf8View(Some(
                                contiguous_scalar,
                            )))),
                            _ => return Ok(ExprSimplifyResult::Original(args)),
                        }
                        contiguous_scalar = "".to_string();
                    }
                    new_args.push(arg.clone());
                } else {
                    // Convert non-string, non-array literals to their string representation
                    // Skip NULL values (concat ignores NULLs)
                    if !scalar_val.is_null() {
                        let string_repr = format!("{scalar_val}");
                        contiguous_scalar += &string_repr;
                    }
                }
            }
            // If the arg is not a literal, we should first push the current `contiguous_scalar`
            // to the `new_args` (if it is not empty) and reset it to empty string.
            // Then pushing this arg to the `new_args`.
            arg => {
                if !contiguous_scalar.is_empty() {
                    match return_type {
                        Utf8 => new_args.push(lit(contiguous_scalar)),
                        LargeUtf8 => new_args
                            .push(lit(ScalarValue::LargeUtf8(Some(contiguous_scalar)))),
                        Utf8View => new_args
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
            Utf8 => new_args.push(lit(contiguous_scalar)),
            LargeUtf8 => {
                new_args.push(lit(ScalarValue::LargeUtf8(Some(contiguous_scalar))))
            }
            Utf8View => {
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
    fn test_array_concatenation_comprehensive() -> Result<()> {
        use arrow::array::{Int32Array, ListArray};
        use arrow::datatypes::{Field, Int32Type};
        use datafusion_common::config::ConfigOptions;

        // Test basic array concatenation
        let arr1 = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
        ]));
        let arr2 = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(3), Some(4)]),
        ]));

        let args = vec![
            ColumnarValue::Array(arr1),
            ColumnarValue::Array(arr2),
        ];

        let arg_fields = vec![
            Field::new("a", List(Arc::new(Field::new("item", Int32, true))), true),
            Field::new("b", List(Arc::new(Field::new("item", Int32, true))), true),
        ]
        .into_iter()
        .map(Arc::new)
        .collect();

        let func_args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field: Field::new("result", List(Arc::new(Field::new("item", Int32, true))), true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatFunc::new().invoke_with_args(func_args)?;
        
        match result {
            ColumnarValue::Array(array) => {
                let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
                let concatenated = list_array.value(0);
                let int_array = concatenated.as_any().downcast_ref::<Int32Array>().unwrap();
                
                assert_eq!(int_array.len(), 4);
                assert_eq!(int_array.value(0), 1);
                assert_eq!(int_array.value(1), 2);
                assert_eq!(int_array.value(2), 3);
                assert_eq!(int_array.value(3), 4);
            }
            _ => panic!("Expected array result"),
        }

        Ok(())
    }

    #[test]
    fn test_mixed_type_error() -> Result<()> {
        use arrow::datatypes::Field;
        
        // Test that coerce_types properly rejects mixed array/non-array types
        let func = ConcatFunc::new();
        let arg_types = vec![
            List(Arc::new(Field::new("item", Int32, true))),
            Utf8,
        ];

        let result = func.coerce_types(&arg_types);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Cannot mix array and non-array arguments"));
        assert!(err_msg.contains("Use concat(array1, array2, ...) for arrays"));

        Ok(())
    }
}
