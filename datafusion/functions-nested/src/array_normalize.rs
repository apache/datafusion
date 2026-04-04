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

//! [ScalarUDFImpl] definitions for array_normalize function.

use crate::utils::make_scalar_function;
use crate::vector_math::{convert_to_f64_array, magnitude_f64};
use arrow::array::{
    Array, ArrayRef, Float64Array, LargeListArray, ListArray, OffsetSizeTrait,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{
    DataType, Field,
    DataType::{FixedSizeList, LargeList, List, Null},
};
use datafusion_common::cast::as_generic_list_array;
use datafusion_common::utils::{ListCoercion, coerced_type_with_base_type_only};
use datafusion_common::{Result, exec_err, plan_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_functions::downcast_arg;
use datafusion_macros::user_doc;
use itertools::Itertools;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayNormalize,
    array_normalize,
    array,
    "returns the array normalized to unit length (L2 norm).",
    array_normalize_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the input array normalized to unit length (each element divided by the L2 norm). Returns NULL if the array has zero magnitude.",
    syntax_example = "array_normalize(array)",
    sql_example = r#"```sql
> select array_normalize([3.0, 4.0]);
+-----------------------------------+
| array_normalize(List([3.0,4.0]))  |
+-----------------------------------+
| [0.6, 0.8]                        |
+-----------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayNormalize {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayNormalize {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayNormalize {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["list_normalize".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayNormalize {
    fn name(&self) -> &str {
        "array_normalize"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Return same list type but with Float64 elements
        match &arg_types[0] {
            List(_) | FixedSizeList(..) | Null => Ok(List(Arc::new(Field::new_list_field(
                DataType::Float64,
                true,
            )))),
            LargeList(_) => Ok(LargeList(Arc::new(Field::new_list_field(
                DataType::Float64,
                true,
            )))),
            _ => exec_err!(
                "array_normalize does not support type {}",
                arg_types[0]
            ),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [_] = take_function_args(self.name(), arg_types)?;
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
        make_scalar_function(array_normalize_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_normalize_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array1] = take_function_args("array_normalize", args)?;
    match array1.data_type() {
        List(_) => general_array_normalize::<i32>(args),
        LargeList(_) => general_array_normalize::<i64>(args),
        arg_type => {
            exec_err!("array_normalize does not support type {arg_type}")
        }
    }
}

fn general_array_normalize<O: OffsetSizeTrait>(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_generic_list_array::<O>(&arrays[0])?;

    let mut result_values: Vec<Option<f64>> = Vec::new();
    let mut offsets: Vec<O> = vec![O::from_usize(0).unwrap()];
    let mut nulls: Vec<bool> = Vec::new();

    for arr in list_array.iter() {
        match compute_normalize(arr)? {
            Some(normalized) => {
                for i in 0..normalized.len() {
                    if normalized.is_null(i) {
                        result_values.push(None);
                    } else {
                        result_values.push(Some(normalized.value(i)));
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

/// Normalizes an array to unit length by dividing each element by the L2 norm.
fn compute_normalize(arr: Option<ArrayRef>) -> Result<Option<Float64Array>> {
    let value = match arr {
        Some(arr) => arr,
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
    let mag = magnitude_f64(&values);

    if mag == 0.0 {
        return Ok(None);
    }

    let normalized: Float64Array = values
        .iter()
        .map(|v| v.map(|val| val / mag))
        .collect();

    Ok(Some(normalized))
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

    #[test]
    fn test_normalize_basic() {
        let arr = make_f64_list_array(vec![Some(vec![Some(3.0), Some(4.0)])]);
        let result = array_normalize_inner(&[arr]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let inner = list.value(0);
        let values = inner.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((values.value(0) - 0.6).abs() < 1e-10);
        assert!((values.value(1) - 0.8).abs() < 1e-10);
    }

    #[test]
    fn test_normalize_null_array() {
        let arr = make_f64_list_array(vec![None]);
        let result = array_normalize_inner(&[arr]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(list.is_null(0));
    }

    #[test]
    fn test_normalize_zero_magnitude() {
        let arr = make_f64_list_array(vec![Some(vec![Some(0.0), Some(0.0)])]);
        let result = array_normalize_inner(&[arr]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(list.is_null(0));
    }

    #[test]
    fn test_normalize_unit_vector() {
        let arr = make_f64_list_array(vec![Some(vec![Some(1.0), Some(0.0)])]);
        let result = array_normalize_inner(&[arr]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let inner = list.value(0);
        let values = inner.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((values.value(0) - 1.0).abs() < 1e-10);
        assert!((values.value(1) - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_normalize_empty_array() {
        let arr = make_f64_list_array(vec![Some(vec![])]);
        let result = array_normalize_inner(&[arr]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        // Zero magnitude -> null
        assert!(list.is_null(0));
    }
}
