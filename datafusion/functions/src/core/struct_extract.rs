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
use arrow::datatypes::DataType;
use datafusion_common::DataFusionError;
use datafusion_common::{exec_err, ScalarValue};
use datafusion_expr::{ScalarUDFImpl, Signature, TypeSignature, Volatility};

#[derive(Debug)]
pub struct StructExtractFunc {
    signature: Signature,
}

impl Default for StructExtractFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl StructExtractFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StructExtractFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "struct_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[arrow::datatypes::DataType],
    ) -> datafusion_common::Result<arrow::datatypes::DataType> {
        exec_err!("should not call return type for struct extract function")
    }

    fn return_type_from_exprs(
        &self,
        args: &[datafusion_expr::Expr],
        _schema: &dyn datafusion_common::ExprSchema,
        arg_types: &[DataType],
    ) -> datafusion_common::Result<DataType> {
        if let DataType::Struct(fields) = &arg_types[0] {
            match &arg_types[1] {
                DataType::Int64 => {
                    if let datafusion_expr::Expr::Literal(ScalarValue::Int64(Some(
                        index,
                    ))) = &args[1]
                    {
                        if *index >= 0 && (*index as usize) < fields.len() {
                            return Ok(fields[*index as usize].data_type().clone());
                        } else {
                            return exec_err!(
                                "Index {} is out of bounds for struct",
                                index
                            );
                        }
                    } else {
                        return exec_err!("Expected an Int64 literal for field index");
                    }
                }
                DataType::Utf8 => {
                    if let datafusion_expr::Expr::Literal(ScalarValue::Utf8(Some(name))) =
                        &args[1]
                    {
                        return fields
                        .iter()
                        .find(|field| field.name().eq(name))
                        .map(|field| field.data_type().clone())
                        .ok_or_else(|| DataFusionError::Execution(format!("Error finding name '{}' in the schema for struct extract function", name)));
                    }
                }
                _ => {
                    return exec_err!(
                        "not supported data type for struct extract function"
                    );
                }
            }
        }
        exec_err!("not supported data type for struct extract function")
    }
    fn invoke(
        &self,
        args: &[datafusion_expr::ColumnarValue],
    ) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
        match &args[0] {
            // Handle the case where the first argument is a StructArray
            datafusion_expr::ColumnarValue::Array(array) => {
                let struct_array = array
                    .as_any()
                    .downcast_ref::<arrow::array::StructArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "Failed to downcast to StructArray, got: {:?}",
                            args[0].data_type()
                        ))
                    })?;

                extract_from_struct_array(struct_array, &args[1])
            }

            // Handle the case where the first argument is ScalarValue::Struct
            datafusion_expr::ColumnarValue::Scalar(ScalarValue::Struct(
                arc_struct_array,
            )) => {
                let struct_array = arc_struct_array.as_ref();
                extract_from_struct_array(struct_array, &args[1])
            }

            _ => Err(DataFusionError::Execution(
                "First argument to struct_extract must be a struct".to_string(),
            )),
        }
    }
}
// Extract data from a StructArray
fn extract_from_struct_array(
    struct_array: &arrow::array::StructArray,
    key: &datafusion_expr::ColumnarValue,
) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
    match key {
        datafusion_expr::ColumnarValue::Scalar(ScalarValue::Int64(Some(index))) => {
            if *index >= 0 && (*index as usize) < struct_array.num_columns() {
                Ok(datafusion_expr::ColumnarValue::Array(
                    struct_array.column(*index as usize).clone(),
                ))
            } else {
                exec_err!("Index {} is out of bounds for struct", index)
            }
        }
        datafusion_expr::ColumnarValue::Scalar(ScalarValue::Utf8(Some(field_name))) => {
            let field_index = struct_array
                .fields()
                .iter()
                .position(|f| f.name() == field_name)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Field '{}' not found in struct",
                        field_name
                    ))
                })?;
            Ok(datafusion_expr::ColumnarValue::Array(
                struct_array.column(field_index).clone(),
            ))
        }
        _ => exec_err!(
            "Second argument must be either an Int64 index or a Utf8 field name"
        ),
    }
}
