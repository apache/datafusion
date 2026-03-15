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

//! [ScalarUDFImpl] definitions for array_distance function.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, Float64Array, LargeListArray, ListArray, OffsetSizeTrait,
};
use arrow::datatypes::{
    DataType,
    DataType::{FixedSizeList, LargeList, List, Null},
};
use datafusion_common::cast::{
    as_float32_array, as_float64_array, as_generic_list_array, as_int32_array,
    as_int64_array,
};
use datafusion_common::utils::{ListCoercion, coerced_type_with_base_type_only};
use datafusion_common::{Result, exec_err, plan_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::downcast_arg;
use datafusion_macros::user_doc;
use itertools::Itertools;
use std::any::Any;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayDistance,
    array_distance,
    array,
    "returns the Euclidean distance between two numeric arrays.",
    array_distance_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the Euclidean distance between two input arrays of equal length.",
    syntax_example = "array_distance(array1, array2)",
    sql_example = r#"```sql
> select array_distance([1, 2], [1, 4]);
+------------------------------------+
| array_distance(List([1,2], [1,4])) |
+------------------------------------+
| 2.0                                |
+------------------------------------+
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
pub struct ArrayDistance {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayDistance {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayDistance {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["list_distance".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayDistance {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_distance"
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

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_distance_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_distance_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array1, array2] = take_function_args("array_distance", args)?;
    match (array1.data_type(), array2.data_type()) {
        (List(_), List(_)) => general_array_distance::<i32>(args),
        (LargeList(_), LargeList(_)) => general_array_distance::<i64>(args),
        (arg_type1, arg_type2) => {
            exec_err!("array_distance does not support types {arg_type1} and {arg_type2}")
        }
    }
}

fn general_array_distance<O: OffsetSizeTrait>(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array1 = as_generic_list_array::<O>(&arrays[0])?;
    let list_array2 = as_generic_list_array::<O>(&arrays[1])?;

    let result = list_array1
        .iter()
        .zip(list_array2.iter())
        .map(|(arr1, arr2)| compute_array_distance(arr1, arr2))
        .collect::<Result<Float64Array>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

/// Computes the Euclidean distance between two arrays
fn compute_array_distance(
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

    let sum_squares: f64 = values1
        .iter()
        .zip(values2.iter())
        .map(|(v1, v2)| {
            let diff = v1.unwrap_or(0.0) - v2.unwrap_or(0.0);
            diff * diff
        })
        .sum();

    Ok(Some(sum_squares.sqrt()))
}

/// Converts an array of any numeric type to a Float64Array.
fn convert_to_f64_array(array: &ArrayRef) -> Result<Float64Array> {
    match array.data_type() {
        DataType::Float64 => Ok(as_float64_array(array)?.clone()),
        DataType::Float32 => {
            let array = as_float32_array(array)?;
            let converted: Float64Array =
                array.iter().map(|v| v.map(|v| v as f64)).collect();
            Ok(converted)
        }
        DataType::Int64 => {
            let array = as_int64_array(array)?;
            let converted: Float64Array =
                array.iter().map(|v| v.map(|v| v as f64)).collect();
            Ok(converted)
        }
        DataType::Int32 => {
            let array = as_int32_array(array)?;
            let converted: Float64Array =
                array.iter().map(|v| v.map(|v| v as f64)).collect();
            Ok(converted)
        }
        _ => exec_err!("Unsupported array type for conversion to Float64Array"),
    }
}
