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

//! [ScalarUDFImpl] definitions for dot_product function.

use crate::utils::{convert_to_f64_array, downcast_arg, make_scalar_function};
use arrow_array::{
    Array, ArrayRef, Float64Array, LargeListArray, ListArray, OffsetSizeTrait,
};
use arrow_schema::DataType;
use arrow_schema::DataType::{FixedSizeList, Float64, LargeList, List};
use core::any::type_name;
use datafusion_common::cast::as_generic_list_array;
use datafusion_common::utils::coerced_fixed_size_list_to_list;
use datafusion_common::DataFusionError;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayDotProduct,
    array_dot_product,
    array,
    "returns the dot product between two numeric arrays.",
    array_dot_product_udf
);

#[derive(Debug)]
pub(super) struct ArrayDotProduct {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayDotProduct {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["list_dot_product".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayDotProduct {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_dot_product"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => Ok(Float64),
            _ => exec_err!("The array_dot_product function can only accept List/LargeList/FixedSizeList."),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!("array_dot_product expects exactly two arguments");
        }
        let mut result = Vec::new();
        for arg_type in arg_types {
            match arg_type {
                List(_) | LargeList(_) | FixedSizeList(_, _) => result.push(coerced_fixed_size_list_to_list(arg_type)),
                _ => return exec_err!("The array_dot_product function can only accept List/LargeList/FixedSizeList."),
            }
        }

        Ok(result)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_dot_product_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

pub fn array_dot_product_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_dot_product expects exactly two arguments");
    }

    match (&args[0].data_type(), &args[1].data_type()) {
        (List(_), List(_)) => general_array_dot_product::<i32>(args),
        (LargeList(_), LargeList(_)) => general_array_dot_product::<i64>(args),
        (array_type1, array_type2) => {
            exec_err!("array_dot_product does not support types '{array_type1:?}' and '{array_type2:?}'")
        }
    }
}

fn general_array_dot_product<O: OffsetSizeTrait>(
    arrays: &[ArrayRef],
) -> Result<ArrayRef> {
    let list_array1 = as_generic_list_array::<O>(&arrays[0])?;
    let list_array2 = as_generic_list_array::<O>(&arrays[1])?;

    let result = list_array1
        .iter()
        .zip(list_array2.iter())
        .map(|(arr1, arr2)| compute_array_dot_product(arr1, arr2))
        .collect::<Result<Float64Array>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

/// Computes the dot product between two arrays
fn compute_array_dot_product(
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

    let sum_products: f64 = values1
        .iter()
        .zip(values2.iter())
        .map(|(v1, v2)| v1.unwrap_or(0.0) * v2.unwrap_or(0.0))
        .sum();

    Ok(Some(sum_products))
}
