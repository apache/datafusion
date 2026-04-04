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

//! [ScalarUDFImpl] definitions for inner_product function.

use crate::utils::make_scalar_function;
use crate::vector_math::{convert_to_f64_array, dot_product_f64};
use arrow::array::{
    Array, ArrayRef, Float64Array, LargeListArray, ListArray, OffsetSizeTrait,
};
use arrow::datatypes::{
    DataType,
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
    InnerProduct,
    inner_product,
    array1 array2,
    "returns the inner (dot) product of two numeric arrays.",
    inner_product_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the inner product (dot product) of two input arrays of equal length: `sum(a[i] * b[i])`.",
    syntax_example = "inner_product(array1, array2)",
    sql_example = r#"```sql
> select inner_product([1.0, 2.0, 3.0], [4.0, 5.0, 6.0]);
+------------------------------------------------------------+
| inner_product(List([1.0,2.0,3.0]), List([4.0,5.0,6.0]))   |
+------------------------------------------------------------+
| 32.0                                                       |
+------------------------------------------------------------+
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
pub struct InnerProduct {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for InnerProduct {
    fn default() -> Self {
        Self::new()
    }
}

impl InnerProduct {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![
                "list_inner_product".to_string(),
                "dot_product".to_string(),
            ],
        }
    }
}

impl ScalarUDFImpl for InnerProduct {
    fn name(&self) -> &str {
        "inner_product"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
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
        make_scalar_function(inner_product_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn inner_product_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array1, array2] = take_function_args("inner_product", args)?;
    match (array1.data_type(), array2.data_type()) {
        (List(_), List(_)) => general_inner_product::<i32>(args),
        (LargeList(_), LargeList(_)) => general_inner_product::<i64>(args),
        (arg_type1, arg_type2) => {
            exec_err!(
                "inner_product does not support types {arg_type1} and {arg_type2}"
            )
        }
    }
}

fn general_inner_product<O: OffsetSizeTrait>(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array1 = as_generic_list_array::<O>(&arrays[0])?;
    let list_array2 = as_generic_list_array::<O>(&arrays[1])?;

    let result = list_array1
        .iter()
        .zip(list_array2.iter())
        .map(|(arr1, arr2)| compute_inner_product(arr1, arr2))
        .collect::<Result<Float64Array>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

/// Computes the inner product of two arrays: sum(a[i] * b[i])
fn compute_inner_product(
    arr1: Option<ArrayRef>,
    arr2: Option<ArrayRef>,
) -> Result<Option<f64>> {
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

    // Check for NULL values inside the arrays
    if value1.null_count() != 0 || value2.null_count() != 0 {
        return Ok(None);
    }

    let values1 = convert_to_f64_array(&value1)?;
    let values2 = convert_to_f64_array(&value2)?;

    if values1.len() != values2.len() {
        return exec_err!("Both arrays must have the same length");
    }

    Ok(Some(dot_product_f64(&values1, &values2)))
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
    fn test_inner_product_basic() {
        let arr1 =
            make_f64_list_array(vec![Some(vec![Some(1.0), Some(2.0), Some(3.0)])]);
        let arr2 =
            make_f64_list_array(vec![Some(vec![Some(4.0), Some(5.0), Some(6.0)])]);
        let result = inner_product_inner(&[arr1, arr2]).unwrap();
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((result.value(0) - 32.0).abs() < 1e-10);
    }

    #[test]
    fn test_inner_product_null_array() {
        let arr1 = make_f64_list_array(vec![None]);
        let arr2 = make_f64_list_array(vec![Some(vec![Some(1.0), Some(2.0)])]);
        let result = inner_product_inner(&[arr1, arr2]).unwrap();
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(result.is_null(0));
    }

    #[test]
    fn test_inner_product_mismatched_lengths() {
        let arr1 = make_f64_list_array(vec![Some(vec![Some(1.0), Some(2.0)])]);
        let arr2 = make_f64_list_array(vec![Some(vec![Some(1.0)])]);
        let result = inner_product_inner(&[arr1, arr2]);
        assert!(result.is_err());
    }

    #[test]
    fn test_inner_product_empty_arrays() {
        let arr1 = make_f64_list_array(vec![Some(vec![])]);
        let arr2 = make_f64_list_array(vec![Some(vec![])]);
        let result = inner_product_inner(&[arr1, arr2]).unwrap();
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((result.value(0) - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_inner_product_orthogonal() {
        let arr1 = make_f64_list_array(vec![Some(vec![Some(1.0), Some(0.0)])]);
        let arr2 = make_f64_list_array(vec![Some(vec![Some(0.0), Some(1.0)])]);
        let result = inner_product_inner(&[arr1, arr2]).unwrap();
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((result.value(0) - 0.0).abs() < 1e-10);
    }
}
