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

use arrow::array::{as_largestring_array, Array, ArrayRef, GenericListArray, NullBufferBuilder};
use arrow::buffer::OffsetBuffer;
use arrow::compute;
use arrow::datatypes::{DataType, Field};
use arrow::array::OffsetSizeTrait;
use datafusion_expr::sort_properties::ExprProperties;
use std::any::Any;
use std::sync::Arc;

use crate::string::concat;
use crate::strings::{
    ColumnarValueRef, LargeStringArrayBuilder, StringArrayBuilder, StringViewArrayBuilder,
};
use datafusion_common::cast::{as_string_array, as_string_view_array, as_generic_list_array};
use datafusion_common::{internal_err, plan_err, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{lit, ColumnarValue, Documentation, Expr, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_macros::user_doc;

/// Array concatenation function for arrays
fn concat_arrays(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() {
        return plan_err!("concat expects at least one argument");
    }

    let mut all_null = true;
    let mut large_list = false;
    
    for arg in args {
        match arg.data_type() {
            DataType::Null => continue,
            DataType::LargeList(_) => large_list = true,
            _ => (),
        }
        if arg.null_count() < arg.len() {
            all_null = false;
        }
    }

    if all_null {
        let return_type = args
            .iter()
            .map(|arg| arg.data_type())
            .find(|d| !d.is_null())
            .unwrap_or(args[0].data_type());

        return Ok(arrow::array::make_array(arrow::array::ArrayData::new_null(
            return_type,
            args[0].len(),
        )));
    }

    if large_list {
        concat_arrays_internal::<i64>(args)
    } else {
        concat_arrays_internal::<i32>(args)
    }
}

fn concat_arrays_internal<O: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let list_arrays = args
        .iter()
        .map(|arg| as_generic_list_array::<O>(arg))
        .collect::<Result<Vec<_>>>()?;
    
    let row_count = list_arrays[0].len();
    let mut array_lengths = vec![];
    let mut arrays = vec![];
    let mut valid = NullBufferBuilder::new(row_count);
    
    for i in 0..row_count {
        let nulls = list_arrays
            .iter()
            .map(|arr| arr.is_null(i))
            .collect::<Vec<_>>();

        let is_null = nulls.iter().all(|&x| x);
        if is_null {
            array_lengths.push(0);
            valid.append_null();
        } else {
            let values = list_arrays
                .iter()
                .map(|arr| arr.value(i))
                .collect::<Vec<_>>();

            let elements = values
                .iter()
                .map(|a| a.as_ref())
                .collect::<Vec<&dyn Array>>();

            let concatenated_array = compute::concat(elements.as_slice())?;
            array_lengths.push(concatenated_array.len());
            arrays.push(concatenated_array);
            valid.append_non_null();
        }
    }
    
    let data_type = list_arrays[0].value_type();
    let elements = arrays
        .iter()
        .map(|a| a.as_ref())
        .collect::<Vec<&dyn Array>>();

    let list_arr = GenericListArray::<O>::new(
        Arc::new(Field::new_list_field(data_type, true)),
        OffsetBuffer::from_lengths(array_lengths),
        Arc::new(compute::concat(elements.as_slice())?),
        valid.finish(),
    );

    Ok(Arc::new(list_arr))
}

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

> select concat([1, 2], [3, 4]);
+---------------------------+
| concat(List([1,2]),List([3,4])) |
+---------------------------+
| [1, 2, 3, 4]              |
+---------------------------+
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        
        // Check if any argument is an array type
        let has_array = arg_types.iter().any(|dt| {
            matches!(dt, List(_) | LargeList(_) | FixedSizeList(_, _))
        });
        
        if has_array {
            // Use array_concat-style return type logic
            use datafusion_common::utils::{base_type, list_ndims};
            use datafusion_expr::binary::type_union_resolution;
            
            let mut max_dims = 0;
            let mut large_list = false;
            let mut element_types = Vec::with_capacity(arg_types.len());
            
            for arg_type in arg_types {
                match arg_type {
                    Null | List(_) | FixedSizeList(..) => (),
                    LargeList(_) => large_list = true,
                    _ => {
                        return plan_err!("concat does not support type {arg_type}")
                    }
                }

                max_dims = max_dims.max(list_ndims(arg_type));
                element_types.push(base_type(arg_type))
            }

            if max_dims == 0 {
                Ok(Null)
            } else {
                // Handle the case where we have empty arrays (Null element types)
                // Filter out Null types and find the first non-null type
                let non_null_types: Vec<_> = element_types.iter().filter(|t| **t != Null).collect();
                
                let unified_element_type = if non_null_types.is_empty() {
                    // All arrays are empty, use Int32 as default
                    Int32
                } else if non_null_types.len() == 1 {
                    // Only one non-null type
                    non_null_types[0].clone()
                } else {
                    // Multiple non-null types, try to unify them
                    if let Some(unified) = type_union_resolution(&non_null_types.into_iter().cloned().collect::<Vec<_>>()) {
                        unified
                    } else {
                        return plan_err!(
                            "Failed to unify argument types of concat: [{}]",
                            arg_types.iter().map(|t| format!("{t}")).collect::<Vec<_>>().join(", ")
                        );
                    }
                };
                
                // Build the return type
                let mut return_type = unified_element_type;
                for _ in 1..max_dims {
                    return_type = DataType::new_list(return_type, true)
                }

                if large_list {
                    Ok(DataType::new_large_list(return_type, true))
                } else {
                    Ok(DataType::new_list(return_type, true))
                }
            }
        } else {
            // Original string concatenation logic
            let mut dt = &Utf8;
            arg_types.iter().for_each(|data_type| {
                if data_type == &Utf8View {
                    dt = data_type;
                }
                if data_type == &LargeUtf8 && dt != &Utf8View {
                    dt = data_type;
                }
            });

            Ok(dt.to_owned())
        }
    }

    /// Concatenates the text representations of all the arguments. NULL arguments are ignored.
    /// concat('abcde', 2, NULL, 22) = 'abcde222'
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        
        // Check if any argument is an array type
        let has_array = args.iter().any(|arg| {
            matches!(
                arg.data_type(),
                DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
            )
        });
        
        if has_array {
            // Determine the number of rows for array operations
            let num_rows = args
                .iter()
                .filter_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    _ => None,
                })
                .next()
                .unwrap_or(1);
            
            // Convert to ArrayRef and delegate to array_concat_inner
            let arrays: Result<Vec<ArrayRef>> = args
                .iter()
                .map(|arg| match arg {
                    ColumnarValue::Array(array) => Ok(Arc::clone(array)),
                    ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows),
                })
                .collect();
            
            let arrays = arrays?;
            let result = concat_arrays(&arrays)?;
            return Ok(ColumnarValue::Array(result));
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
                    None => plan_err!(
                        "Concat function does not support scalar type {}",
                        scalar
                    )?,
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
                        },
                        DataType::LargeUtf8 => {
                            let string_array = as_largestring_array(array);

                            data_size += string_array.values().len();
                            let column = if array.is_nullable() {
                                ColumnarValueRef::NullableLargeStringArray(string_array)
                            } else {
                                ColumnarValueRef::NonNullableLargeStringArray(string_array)
                            };
                            columns.push(column);
                        },
                        DataType::Utf8View => {
                            let string_array = as_string_view_array(array)?;

                            data_size += string_array.len();
                            let column = if array.is_nullable() {
                                ColumnarValueRef::NullableStringViewArray(string_array)
                            } else {
                                ColumnarValueRef::NonNullableStringViewArray(string_array)
                            };
                            columns.push(column);
                        },
                        other => {
                            return plan_err!("Input was {other} which is not a supported datatype for concat function")
                        }
                    };
                }
                _ => unreachable!("concat"),
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
            _ => unreachable!(),
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

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // Check if we have no arguments - this should be an error
        if arg_types.is_empty() {
            return plan_err!("concat requires at least one argument");
        }
        
        // Check if any argument is an array type
        let has_array = arg_types.iter().any(|dt| {
            matches!(dt, DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _))
        });
        
        if has_array {
            // For arrays, use array_concat-style type coercion
            use datafusion_common::utils::base_type;
            use datafusion_expr::binary::type_union_resolution;
            use datafusion_common::utils::list_ndims;
            
            // Find the maximum dimensions and whether we have large lists
            let mut max_dims = 0;
            let mut large_list = false;
            let mut element_types = Vec::with_capacity(arg_types.len());
            
            for arg_type in arg_types {
                match arg_type {
                    DataType::Null | DataType::List(_) | DataType::FixedSizeList(..) => (),
                    DataType::LargeList(_) => large_list = true,
                    _ => {
                        // Non-array type mixed with array type - this is an error
                        return plan_err!("concat cannot mix array and non-array arguments");
                    }
                }
                max_dims = max_dims.max(list_ndims(arg_type));
                element_types.push(base_type(arg_type))
            }
            
            // Handle the case where we have empty arrays (Null element types)
            // Filter out Null types and find the first non-null type
            let non_null_types: Vec<_> = element_types.iter().filter(|t| **t != DataType::Null).collect();
            
            let unified_element_type = if non_null_types.is_empty() {
                // All arrays are empty, use Int32 as default
                DataType::Int32
            } else if non_null_types.len() == 1 {
                // Only one non-null type
                non_null_types[0].clone()
            } else {
                // Multiple non-null types, try to unify them
                if let Some(unified) = type_union_resolution(&non_null_types.into_iter().cloned().collect::<Vec<_>>()) {
                    unified
                } else {
                    return plan_err!(
                        "Failed to unify argument types of concat: [{}]",
                        arg_types.iter().map(|t| format!("{t}")).collect::<Vec<_>>().join(", ")
                    );
                }
            };
            
            // Build the return type
            let mut return_type = unified_element_type;
            for _ in 1..max_dims {
                return_type = DataType::new_list(return_type, true)
            }
            
            let final_type = if large_list {
                DataType::new_large_list(return_type, true)
            } else {
                DataType::new_list(return_type, true)
            };
            
            // Coerce all arguments to this type
            Ok(arg_types.iter().map(|_| final_type.clone()).collect())
        } else {
            // For strings, all arguments should be coerced to string types
            // This is the original string concatenation behavior
            let mut dt = DataType::Utf8;
            arg_types.iter().for_each(|data_type| {
                if data_type == &DataType::Utf8View {
                    dt = data_type.clone();
                }
                if data_type == &DataType::LargeUtf8 && dt != DataType::Utf8View {
                    dt = data_type.clone();
                }
            });
            
            // Coerce all arguments to the determined string type
            Ok(vec![dt; arg_types.len()])
        }
    }
}

pub fn simplify_concat(args: Vec<Expr>) -> Result<ExprSimplifyResult> {
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

    for arg in args.clone() {
        match arg {
            Expr::Literal(ScalarValue::Utf8(None), _) => {}
            Expr::Literal(ScalarValue::LargeUtf8(None), _) => {
            }
            Expr::Literal(ScalarValue::Utf8View(None), _) => { }

            // filter out `null` args
            // All literals have been converted to Utf8 or LargeUtf8 in type_coercion.
            // Concatenate it with the `contiguous_scalar`.
            Expr::Literal(ScalarValue::Utf8(Some(v)), _) => {
                contiguous_scalar += &v;
            }
            Expr::Literal(ScalarValue::LargeUtf8(Some(v)), _) => {
                contiguous_scalar += &v;
            }
            Expr::Literal(ScalarValue::Utf8View(Some(v)), _) => {
                contiguous_scalar += &v;
            }

            Expr::Literal(x, _) => {
                return internal_err!(
                    "The scalar {x} should be casted to string type during the type coercion."
                )
            }
            // If the arg is not a literal, we should first push the current `contiguous_scalar`
            // to the `new_args` (if it is not empty) and reset it to empty string.
            // Then pushing this arg to the `new_args`.
            arg => {
                if !contiguous_scalar.is_empty() {
                    match return_type {
                        DataType::Utf8 => new_args.push(lit(contiguous_scalar)),
                        DataType::LargeUtf8 => new_args.push(lit(ScalarValue::LargeUtf8(Some(contiguous_scalar)))),
                        DataType::Utf8View => new_args.push(lit(ScalarValue::Utf8View(Some(contiguous_scalar)))),
                        _ => unreachable!(),
                    }
                    contiguous_scalar = "".to_string();
                }
                new_args.push(arg);
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
            _ => unreachable!(),
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
    use arrow::array::{ArrayRef, StringArray, Int32Array, ListArray};
    use arrow::datatypes::{Field, Int32Type};
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
    fn test_concat_mixed_types() -> Result<()> {
        // Test that mixed non-array types get coerced to strings
        let concat_func = ConcatFunc::new();
        
        // Test coercion with boolean, int, float, string
        let arg_types = vec![
            Boolean,
            Int32,
            Float64,
            Utf8,
        ];
        
        let coerced = concat_func.coerce_types(&arg_types)?;
        
        // All should be coerced to Utf8
        assert_eq!(coerced, vec![Utf8; 4]);
        
        Ok(())
    }

    #[test]
    fn test_concat_mixed_array_non_array_error() -> Result<()> {
        // Test that mixing arrays and non-arrays produces an error
        let concat_func = ConcatFunc::new();
        
        let arg_types = vec![
            List(Arc::new(Field::new("item", Int32, true))),
            Utf8,
        ];
        
        let result = concat_func.coerce_types(&arg_types);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot mix array and non-array"));
        
        Ok(())
    }

    #[test]
    fn test_concat_return_type_mixed() -> Result<()> {
        // Test return type calculation for mixed non-array types
        let concat_func = ConcatFunc::new();
        
        let arg_types = vec![Boolean, Int32, Float64, Utf8, LargeUtf8];
        let return_type = concat_func.return_type(&arg_types)?;
        
        // Should return LargeUtf8 (highest precedence string type)
        assert_eq!(return_type, LargeUtf8);
        
        Ok(())
    }

    #[test]
    fn test_concat_arrays() -> Result<()> {
        // Test concatenating two int arrays
        let list1 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
        ]);
        let list2 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(3), Some(4)]),
        ]);
        
        let c0 = ColumnarValue::Array(Arc::new(list1));
        let c1 = ColumnarValue::Array(Arc::new(list2));
        
        let arg_fields = vec![
            Field::new("a", List(Arc::new(Field::new("item", Int32, true))), true),
            Field::new("b", List(Arc::new(Field::new("item", Int32, true))), true),
        ]
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();

        let args = ScalarFunctionArgs {
            args: vec![c0, c1],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", List(Arc::new(Field::new("item", Int32, true))), true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatFunc::new().invoke_with_args(args)?;
        match &result {
            ColumnarValue::Array(array) => {
                let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
                let values = list_array.value(0);
                let int_array = values.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(int_array.values(), &[1, 2, 3, 4]);
            }
            _ => panic!("Expected array result"),
        }
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
        .collect::<Vec<_>>();

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
}
