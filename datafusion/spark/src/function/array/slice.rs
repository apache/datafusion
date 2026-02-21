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

use arrow::array::{Array, ArrayRef, Int64Builder};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::{as_int64_array, as_list_array};
use datafusion_common::utils::ListCoercion;
use datafusion_common::{
    DataFusionError, Result, exec_err, internal_err, utils::take_function_args,
};
use datafusion_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_functions_nested::extract::array_slice_udf;
use std::any::Any;
use std::sync::Arc;

/// Spark slice function implementation
/// Main difference from DataFusion's array_slice is that the third argument is the length of the slice and not the end index.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#slice>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSlice {
    signature: Signature,
}

impl Default for SparkSlice {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSlice {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::ArraySignature(
                    ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Array,
                            ArrayFunctionArgument::Index,
                            ArrayFunctionArgument::Index,
                        ],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    },
                ),
                volatility: Volatility::Immutable,
                parameter_names: None,
            },
        }
    }
}

impl ScalarUDFImpl for SparkSlice {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "slice"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());

        Ok(Arc::new(Field::new(
            "slice",
            args.arg_fields[0].data_type().clone(),
            nullable,
        )))
    }

    fn invoke_with_args(
        &self,
        mut func_args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        if func_args.args[0].data_type() == DataType::Null {
            return Ok::<ColumnarValue, DataFusionError>(func_args.args[0].clone());
        };

        let array_len = func_args
            .args
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(array) => Some(array.len()),
                _ => None,
            })
            .unwrap_or(func_args.number_rows);

        let arrays = func_args
            .args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(array) => Ok(Arc::clone(array)),
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(array_len),
            })
            .collect::<Result<Vec<_>>>()?;

        let (start, end) = calculate_start_end(&arrays)?;

        array_slice_udf().invoke_with_args(ScalarFunctionArgs {
            args: vec![
                func_args.args.swap_remove(0),
                ColumnarValue::Array(start),
                ColumnarValue::Array(end),
            ],
            arg_fields: func_args.arg_fields,
            number_rows: func_args.number_rows,
            return_field: func_args.return_field,
            config_options: func_args.config_options,
        })
    }
}

fn calculate_start_end(args: &[ArrayRef]) -> Result<(ArrayRef, ArrayRef)> {
    let [values, start, length] = take_function_args("slice", args)?;

    let values_len = values.len();

    let start = as_int64_array(&start)?;
    let length = as_int64_array(&length)?;

    let values = as_list_array(values)?;

    let mut adjusted_start = Int64Builder::with_capacity(values_len);
    let mut end = Int64Builder::with_capacity(values_len);

    for row in 0..values_len {
        if values.is_null(row) || start.is_null(row) || length.is_null(row) {
            adjusted_start.append_null();
            end.append_null();
            continue;
        }
        let start = start.value(row);
        let length = length.value(row);
        let value_length = values.value(row).len() as i64;

        if start == 0 {
            return exec_err!("Start index must not be zero");
        }
        if length < 0 {
            return exec_err!("Length must be non-negative, but got {}", length);
        }

        let adjusted_start_value = if start < 0 {
            start + value_length + 1
        } else {
            start
        };

        adjusted_start.append_value(adjusted_start_value);
        end.append_value(adjusted_start_value + (length - 1));
    }

    Ok((Arc::new(adjusted_start.finish()), Arc::new(end.finish())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::NullArray;
    use arrow::datatypes::DataType::List;
    use arrow::datatypes::Field;
    use datafusion_common::ScalarValue;

    #[test]
    fn test_spark_slice_function_when_input_array_is_null() {
        let input_args = vec![
            ColumnarValue::Array(Arc::new(NullArray::new(1))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
        ];

        let args = ScalarFunctionArgs {
            args: input_args.to_owned(),
            arg_fields: vec![Arc::new(Field::new(
                "item",
                List(FieldRef::new(Field::new("", DataType::Int64, true))),
                false,
            ))],
            number_rows: 0,
            return_field: Arc::new(Field::new(
                "item",
                List(FieldRef::new(Field::new_list_field(DataType::Int64, true))),
                false,
            )),
            config_options: Arc::new(Default::default()),
        };
        let slice = SparkSlice::new();
        let result = slice.invoke_with_args(args).unwrap();
        assert!(result.to_array(1).unwrap() == Arc::new(NullArray::new(1)));
    }
}
