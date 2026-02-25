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

use arrow::datatypes::{DataType, Field};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions_nested::repeat::ArrayRepeat;
use std::any::Any;
use std::sync::Arc;

use crate::function::null_utils::{
    NullMaskResolution, apply_null_mask, compute_null_mask,
};

/// Spark-compatible `array_repeat` expression. The difference with DataFusion's `array_repeat` is the handling of NULL inputs: in spark if any input is NULL, the result is NULL.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#array_repeat>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayRepeat {
    signature: Signature,
}

impl Default for SparkArrayRepeat {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayRepeat {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArrayRepeat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_repeat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            arg_types[0].clone(),
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_array_repeat(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [first_type, second_type] = take_function_args(self.name(), arg_types)?;

        // Coerce the second argument to Int64/UInt64 if it's a numeric type
        let second = match second_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                DataType::Int64
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                DataType::UInt64
            }
            _ => return exec_err!("count must be an integer type"),
        };

        Ok(vec![first_type.clone(), second])
    }
}

/// This is a Spark-specific wrapper around DataFusion's array_repeat that returns NULL
/// if any argument is NULL (Spark behavior), whereas DataFusion's array_repeat ignores NULLs.
fn spark_array_repeat(args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let ScalarFunctionArgs {
        args: arg_values,
        arg_fields,
        number_rows,
        return_field,
        config_options,
    } = args;
    let return_type = return_field.data_type().clone();

    // Step 1: Check for NULL mask in incoming args
    let null_mask = compute_null_mask(&arg_values, number_rows)?;

    // If any argument is null then return NULL immediately
    if matches!(null_mask, NullMaskResolution::ReturnNull) {
        return Ok(ColumnarValue::Scalar(ScalarValue::try_from(return_type)?));
    }

    // Step 2: Delegate to DataFusion's array_repeat
    let array_repeat_func = ArrayRepeat::new();
    let func_args = ScalarFunctionArgs {
        args: arg_values,
        arg_fields,
        number_rows,
        return_field,
        config_options,
    };
    let result = array_repeat_func.invoke_with_args(func_args)?;

    // Step 3: Apply NULL mask to result
    apply_null_mask(result, null_mask, &return_type)
}
