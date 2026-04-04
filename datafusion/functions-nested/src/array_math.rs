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

//! [ScalarUDFImpl] definitions for array_add, array_subtract, and array_scale functions.

use crate::utils::make_scalar_function;
use crate::vector_math::convert_to_f64_array;
use arrow::array::{
    Array, ArrayRef, Float64Array, LargeListArray, ListArray, OffsetSizeTrait,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{
    DataType, Field,
    DataType::{FixedSizeList, LargeList, List, Null},
};
use datafusion_common::cast::{as_float64_array, as_generic_list_array};
use datafusion_common::utils::{ListCoercion, coerced_type_with_base_type_only};
use datafusion_common::{Result, exec_err, plan_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_functions::downcast_arg;
use datafusion_macros::user_doc;
use itertools::Itertools;
use std::sync::Arc;

// ============================================================================
// array_add
// ============================================================================

make_udf_expr_and_func!(
    ArrayAdd,
    array_add,
    array1 array2,
    "returns element-wise addition of two numeric arrays.",
    array_add_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns a new array with element-wise addition of two input arrays of equal length.",
    syntax_example = "array_add(array1, array2)",
    sql_example = r#"```sql
> select array_add([1.0, 2.0], [3.0, 4.0]);
+------------------------------------------+
| array_add(List([1.0,2.0]), List([3.0,4.0])) |
+------------------------------------------+
| [4.0, 6.0]                              |
+------------------------------------------+
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
pub struct ArrayAdd {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayAdd {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayAdd {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["list_add".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayAdd {
    fn name(&self) -> &str {
        "array_add"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            List(_) | FixedSizeList(..) | Null => Ok(List(Arc::new(
                Field::new_list_field(DataType::Float64, true),
            ))),
            LargeList(_) => Ok(LargeList(Arc::new(Field::new_list_field(
                DataType::Float64,
                true,
            )))),
            _ => exec_err!("array_add does not support type {}", arg_types[0]),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [_, _] = take_function_args(self.name(), arg_types)?;
        let coercion = Some(&ListCoercion::FixedSizedListToList);
        let arg_types = arg_types.iter().map(|arg_type| {
            if matches!(arg_type, Null | List(_) | LargeList(_) | FixedSizeList(..)) {
                Ok(coerced_type_with_base_type_only(
                    arg_type,
                    &DataType::Float64,
                    coercion,
                ))
            } else {
                plan_err!("{} does not support type {arg_type}", self.name())
            }
        });

        arg_types.try_collect()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_add_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_add_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array1, array2] = take_function_args("array_add", args)?;
    match (array1.data_type(), array2.data_type()) {
        (List(_), List(_)) => general_array_binary_op::<i32>(args, "array_add", |a, b| a + b),
        (LargeList(_), LargeList(_)) => {
            general_array_binary_op::<i64>(args, "array_add", |a, b| a + b)
        }
        (arg_type1, arg_type2) => {
            exec_err!("array_add does not support types {arg_type1} and {arg_type2}")
        }
    }
}

// ============================================================================
// array_subtract
// ============================================================================

make_udf_expr_and_func!(
    ArraySubtract,
    array_subtract,
    array1 array2,
    "returns element-wise subtraction of two numeric arrays.",
    array_subtract_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns a new array with element-wise subtraction of two input arrays of equal length.",
    syntax_example = "array_subtract(array1, array2)",
    sql_example = r#"```sql
> select array_subtract([5.0, 3.0], [1.0, 2.0]);
+-----------------------------------------------+
| array_subtract(List([5.0,3.0]), List([1.0,2.0])) |
+-----------------------------------------------+
| [4.0, 1.0]                                   |
+-----------------------------------------------+
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
pub struct ArraySubtract {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArraySubtract {
    fn default() -> Self {
        Self::new()
    }
}

impl ArraySubtract {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["list_subtract".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArraySubtract {
    fn name(&self) -> &str {
        "array_subtract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            List(_) | FixedSizeList(..) | Null => Ok(List(Arc::new(
                Field::new_list_field(DataType::Float64, true),
            ))),
            LargeList(_) => Ok(LargeList(Arc::new(Field::new_list_field(
                DataType::Float64,
                true,
            )))),
            _ => exec_err!(
                "array_subtract does not support type {}",
                arg_types[0]
            ),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [_, _] = take_function_args(self.name(), arg_types)?;
        let coercion = Some(&ListCoercion::FixedSizedListToList);
        let arg_types = arg_types.iter().map(|arg_type| {
            if matches!(arg_type, Null | List(_) | LargeList(_) | FixedSizeList(..)) {
                Ok(coerced_type_with_base_type_only(
                    arg_type,
                    &DataType::Float64,
                    coercion,
                ))
            } else {
                plan_err!("{} does not support type {arg_type}", self.name())
            }
        });

        arg_types.try_collect()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_subtract_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_subtract_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array1, array2] = take_function_args("array_subtract", args)?;
    match (array1.data_type(), array2.data_type()) {
        (List(_), List(_)) => {
            general_array_binary_op::<i32>(args, "array_subtract", |a, b| a - b)
        }
        (LargeList(_), LargeList(_)) => {
            general_array_binary_op::<i64>(args, "array_subtract", |a, b| a - b)
        }
        (arg_type1, arg_type2) => {
            exec_err!(
                "array_subtract does not support types {arg_type1} and {arg_type2}"
            )
        }
    }
}

// ============================================================================
// array_scale
// ============================================================================

make_udf_expr_and_func!(
    ArrayScale,
    array_scale,
    array scalar,
    "returns array with each element multiplied by a scalar.",
    array_scale_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns a new array with each element multiplied by the given scalar value.",
    syntax_example = "array_scale(array, scalar)",
    sql_example = r#"```sql
> select array_scale([1.0, 2.0, 3.0], 2.0);
+-------------------------------------------+
| array_scale(List([1.0,2.0,3.0]), 2.0)    |
+-------------------------------------------+
| [2.0, 4.0, 6.0]                          |
+-------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "scalar",
        description = "Float64 scalar value to multiply each element by."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayScale {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayScale {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayScale {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Any(2),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["list_scale".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayScale {
    fn name(&self) -> &str {
        "array_scale"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            List(_) | FixedSizeList(..) | Null => Ok(List(Arc::new(
                Field::new_list_field(DataType::Float64, true),
            ))),
            LargeList(_) => Ok(LargeList(Arc::new(Field::new_list_field(
                DataType::Float64,
                true,
            )))),
            _ => exec_err!("array_scale does not support type {}", arg_types[0]),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [_, _] = take_function_args(self.name(), arg_types)?;

        let coercion = Some(&ListCoercion::FixedSizedListToList);
        let first = &arg_types[0];
        let coerced_first = if matches!(
            first,
            Null | List(_) | LargeList(_) | FixedSizeList(..)
        ) {
            coerced_type_with_base_type_only(first, &DataType::Float64, coercion)
        } else {
            return plan_err!("{} does not support type {first}", self.name());
        };

        // Second argument is scalar Float64
        Ok(vec![coerced_first, DataType::Float64])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_scale_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_scale_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array1, _scalar] = take_function_args("array_scale", args)?;
    match array1.data_type() {
        List(_) => general_array_scale::<i32>(args),
        LargeList(_) => general_array_scale::<i64>(args),
        arg_type => {
            exec_err!("array_scale does not support type {arg_type}")
        }
    }
}

fn general_array_scale<O: OffsetSizeTrait>(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_generic_list_array::<O>(&arrays[0])?;
    let scalar_array = as_float64_array(&arrays[1])?;

    let mut result_values: Vec<Option<f64>> = Vec::new();
    let mut offsets: Vec<O> = vec![O::from_usize(0).unwrap()];
    let mut nulls: Vec<bool> = Vec::new();

    for (i, arr) in list_array.iter().enumerate() {
        let scalar_val = if scalar_array.is_null(i) {
            None
        } else {
            Some(scalar_array.value(i))
        };

        match compute_scale(arr, scalar_val)? {
            Some(scaled) => {
                for j in 0..scaled.len() {
                    if scaled.is_null(j) {
                        result_values.push(None);
                    } else {
                        result_values.push(Some(scaled.value(j)));
                    }
                }
                offsets.push(O::from_usize(result_values.len()).unwrap());
                nulls.push(true);
            }
            None => {
                offsets.push(O::from_usize(result_values.len()).unwrap());
                nulls.push(false);
            }
        }
    }

    let values_array = Arc::new(Float64Array::from(result_values)) as ArrayRef;
    let field = Arc::new(Field::new_list_field(DataType::Float64, true));
    let offset_buffer = OffsetBuffer::new(offsets.into());
    let null_buffer = arrow::buffer::NullBuffer::from(nulls);

    Ok(Arc::new(arrow::array::GenericListArray::<O>::new(
        field,
        offset_buffer,
        values_array,
        Some(null_buffer),
    )) as ArrayRef)
}

fn compute_scale(
    arr: Option<ArrayRef>,
    scalar: Option<f64>,
) -> Result<Option<Float64Array>> {
    let value = match arr {
        Some(arr) => arr,
        None => return Ok(None),
    };
    let scalar = match scalar {
        Some(s) => s,
        None => return Ok(None),
    };

    let mut value = value;

    loop {
        match value.data_type() {
            List(_) => {
                if downcast_arg!(value, ListArray).null_count() > 0 {
                    return Ok(None);
                }
                value = downcast_arg!(value, ListArray).value(0);
            }
            LargeList(_) => {
                if downcast_arg!(value, LargeListArray).null_count() > 0 {
                    return Ok(None);
                }
                value = downcast_arg!(value, LargeListArray).value(0);
            }
            _ => break,
        }
    }

    if value.null_count() != 0 {
        return Ok(None);
    }

    let values = convert_to_f64_array(&value)?;
    let scaled: Float64Array = values
        .iter()
        .map(|v| v.map(|val| val * scalar))
        .collect();

    Ok(Some(scaled))
}

// ============================================================================
// Shared binary operation helper for array_add / array_subtract
// ============================================================================

fn general_array_binary_op<O: OffsetSizeTrait>(
    arrays: &[ArrayRef],
    fn_name: &str,
    op: fn(f64, f64) -> f64,
) -> Result<ArrayRef> {
    let list_array1 = as_generic_list_array::<O>(&arrays[0])?;
    let list_array2 = as_generic_list_array::<O>(&arrays[1])?;

    let mut result_values: Vec<Option<f64>> = Vec::new();
    let mut offsets: Vec<O> = vec![O::from_usize(0).unwrap()];
    let mut nulls: Vec<bool> = Vec::new();

    for (arr1, arr2) in list_array1.iter().zip(list_array2.iter()) {
        match compute_binary_op(arr1, arr2, fn_name, op)? {
            Some(result) => {
                for i in 0..result.len() {
                    if result.is_null(i) {
                        result_values.push(None);
                    } else {
                        result_values.push(Some(result.value(i)));
                    }
                }
                offsets.push(O::from_usize(result_values.len()).unwrap());
                nulls.push(true);
            }
            None => {
                offsets.push(O::from_usize(result_values.len()).unwrap());
                nulls.push(false);
            }
        }
    }

    let values_array = Arc::new(Float64Array::from(result_values)) as ArrayRef;
    let field = Arc::new(Field::new_list_field(DataType::Float64, true));
    let offset_buffer = OffsetBuffer::new(offsets.into());
    let null_buffer = arrow::buffer::NullBuffer::from(nulls);

    Ok(Arc::new(arrow::array::GenericListArray::<O>::new(
        field,
        offset_buffer,
        values_array,
        Some(null_buffer),
    )) as ArrayRef)
}

fn compute_binary_op(
    arr1: Option<ArrayRef>,
    arr2: Option<ArrayRef>,
    _fn_name: &str,
    op: fn(f64, f64) -> f64,
) -> Result<Option<Float64Array>> {
    let value1 = match arr1 {
        Some(arr) => arr,
        None => return Ok(None),
    };
    let value2 = match arr2 {
        Some(arr) => arr,
        None => return Ok(None),
    };

    let mut value1 = value1;
    let mut value2 = value2;

    loop {
        match value1.data_type() {
            List(_) => {
                if downcast_arg!(value1, ListArray).null_count() > 0 {
                    return Ok(None);
                }
                value1 = downcast_arg!(value1, ListArray).value(0);
            }
            LargeList(_) => {
                if downcast_arg!(value1, LargeListArray).null_count() > 0 {
                    return Ok(None);
                }
                value1 = downcast_arg!(value1, LargeListArray).value(0);
            }
            _ => break,
        }

        match value2.data_type() {
            List(_) => {
                if downcast_arg!(value2, ListArray).null_count() > 0 {
                    return Ok(None);
                }
                value2 = downcast_arg!(value2, ListArray).value(0);
            }
            LargeList(_) => {
                if downcast_arg!(value2, LargeListArray).null_count() > 0 {
                    return Ok(None);
                }
                value2 = downcast_arg!(value2, LargeListArray).value(0);
            }
            _ => break,
        }
    }

    if value1.null_count() != 0 || value2.null_count() != 0 {
        return Ok(None);
    }

    let values1 = convert_to_f64_array(&value1)?;
    let values2 = convert_to_f64_array(&value2)?;

    if values1.len() != values2.len() {
        return exec_err!("Both arrays must have the same length");
    }

    let result: Float64Array = values1
        .iter()
        .zip(values2.iter())
        .map(|(v1, v2)| match (v1, v2) {
            (Some(a), Some(b)) => Some(op(a, b)),
            _ => None,
        })
        .collect();

    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    fn make_f64_list_array(values: Vec<Option<Vec<Option<f64>>>>) -> ArrayRef {
        let mut flat: Vec<Option<f64>> = Vec::new();
        let mut offsets: Vec<i32> = vec![0];
        for v in &values {
            match v {
                Some(inner) => {
                    flat.extend(inner);
                    offsets.push(flat.len() as i32);
                }
                None => {
                    offsets.push(flat.len() as i32);
                }
            }
        }
        let values_array = Arc::new(Float64Array::from(flat)) as ArrayRef;
        let field = Arc::new(Field::new_list_field(DataType::Float64, true));
        let offset_buffer = OffsetBuffer::new(offsets.into());
        let null_buffer = arrow::buffer::NullBuffer::from(
            values.iter().map(|v| v.is_some()).collect::<Vec<_>>(),
        );
        Arc::new(ListArray::new(
            field,
            offset_buffer,
            values_array,
            Some(null_buffer),
        ))
    }

    fn make_f64_scalar_array(values: Vec<Option<f64>>) -> ArrayRef {
        Arc::new(Float64Array::from(values)) as ArrayRef
    }

    // === array_add tests ===

    #[test]
    fn test_array_add_basic() {
        let arr1 = make_f64_list_array(vec![Some(vec![Some(1.0), Some(2.0)])]);
        let arr2 = make_f64_list_array(vec![Some(vec![Some(3.0), Some(4.0)])]);
        let result = array_add_inner(&[arr1, arr2]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let inner = list.value(0);
        let values = inner.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((values.value(0) - 4.0).abs() < 1e-10);
        assert!((values.value(1) - 6.0).abs() < 1e-10);
    }

    #[test]
    fn test_array_add_null() {
        let arr1 = make_f64_list_array(vec![None]);
        let arr2 = make_f64_list_array(vec![Some(vec![Some(1.0)])]);
        let result = array_add_inner(&[arr1, arr2]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(list.is_null(0));
    }

    #[test]
    fn test_array_add_mismatched_lengths() {
        let arr1 = make_f64_list_array(vec![Some(vec![Some(1.0), Some(2.0)])]);
        let arr2 = make_f64_list_array(vec![Some(vec![Some(1.0)])]);
        let result = array_add_inner(&[arr1, arr2]);
        assert!(result.is_err());
    }

    #[test]
    fn test_array_add_empty() {
        let arr1 = make_f64_list_array(vec![Some(vec![])]);
        let arr2 = make_f64_list_array(vec![Some(vec![])]);
        let result = array_add_inner(&[arr1, arr2]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(!list.is_null(0));
        assert_eq!(list.value(0).len(), 0);
    }

    // === array_subtract tests ===

    #[test]
    fn test_array_subtract_basic() {
        let arr1 = make_f64_list_array(vec![Some(vec![Some(5.0), Some(3.0)])]);
        let arr2 = make_f64_list_array(vec![Some(vec![Some(1.0), Some(2.0)])]);
        let result = array_subtract_inner(&[arr1, arr2]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let inner = list.value(0);
        let values = inner.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((values.value(0) - 4.0).abs() < 1e-10);
        assert!((values.value(1) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_array_subtract_null() {
        let arr1 = make_f64_list_array(vec![Some(vec![Some(1.0)])]);
        let arr2 = make_f64_list_array(vec![None]);
        let result = array_subtract_inner(&[arr1, arr2]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(list.is_null(0));
    }

    #[test]
    fn test_array_subtract_mismatched_lengths() {
        let arr1 = make_f64_list_array(vec![Some(vec![Some(1.0)])]);
        let arr2 = make_f64_list_array(vec![Some(vec![Some(1.0), Some(2.0)])]);
        let result = array_subtract_inner(&[arr1, arr2]);
        assert!(result.is_err());
    }

    // === array_scale tests ===

    #[test]
    fn test_array_scale_basic() {
        let arr = make_f64_list_array(vec![Some(vec![Some(1.0), Some(2.0), Some(3.0)])]);
        let scalar = make_f64_scalar_array(vec![Some(2.0)]);
        let result = array_scale_inner(&[arr, scalar]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let inner = list.value(0);
        let values = inner.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((values.value(0) - 2.0).abs() < 1e-10);
        assert!((values.value(1) - 4.0).abs() < 1e-10);
        assert!((values.value(2) - 6.0).abs() < 1e-10);
    }

    #[test]
    fn test_array_scale_null_array() {
        let arr = make_f64_list_array(vec![None]);
        let scalar = make_f64_scalar_array(vec![Some(2.0)]);
        let result = array_scale_inner(&[arr, scalar]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(list.is_null(0));
    }

    #[test]
    fn test_array_scale_null_scalar() {
        let arr = make_f64_list_array(vec![Some(vec![Some(1.0), Some(2.0)])]);
        let scalar = make_f64_scalar_array(vec![None]);
        let result = array_scale_inner(&[arr, scalar]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(list.is_null(0));
    }

    #[test]
    fn test_array_scale_zero() {
        let arr = make_f64_list_array(vec![Some(vec![Some(1.0), Some(2.0)])]);
        let scalar = make_f64_scalar_array(vec![Some(0.0)]);
        let result = array_scale_inner(&[arr, scalar]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let inner = list.value(0);
        let values = inner.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((values.value(0) - 0.0).abs() < 1e-10);
        assert!((values.value(1) - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_array_scale_empty() {
        let arr = make_f64_list_array(vec![Some(vec![])]);
        let scalar = make_f64_scalar_array(vec![Some(5.0)]);
        let result = array_scale_inner(&[arr, scalar]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(!list.is_null(0));
        assert_eq!(list.value(0).len(), 0);
    }
}
