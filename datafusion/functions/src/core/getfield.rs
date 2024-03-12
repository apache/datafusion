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
use datafusion_common::{exec_err, ExprSchema, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, Expr};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

#[derive(Debug)]
pub struct GetFieldFunc {
    signature: Signature,
}

impl GetFieldFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}
impl Default for GetFieldFunc {
    fn default() -> Self {
        Self::new()
    }
}

// get_field(struct_array, field_name)
impl ScalarUDFImpl for GetFieldFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "get_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        todo!()
    }

    fn return_type_from_exprs(
        &self,
        args: &[Expr],
        schema: &dyn ExprSchema,
        _arg_types: &[DataType],
    ) -> Result<DataType> {
        if args.len() != 2 {
            return exec_err!(
                "get_field function requires 2 arguments, got {}",
                args.len()
            );
        }

        match &args[0] {
            Expr::Column(name) => {
                let data_type = schema.data_type(name)?;
                match data_type {
                    DataType::Struct(fields) => {
                        let field_name = match &args[1] {
                            Expr::Literal(ScalarValue::Utf8(name)) => {
                                name.as_ref().map(|x| x.as_str())
                            }
                            _ => {
                                return exec_err!(
                                        "get_field function requires the argument field_name to be a string"
                                    );
                            }
                        };
                        let field =
                            fields.iter().find(|f| f.name() == field_name.unwrap());
                        match field {
                            Some(field) => Ok(field.data_type().clone()),
                            None => {
                                exec_err!(
                                        "get_field function can't find the field {} in the struct", field_name.unwrap()
                                    )
                            }
                        }
                    }
                    _ => {
                        exec_err!(
                            "get_field function requires the column to have struct type"
                        )
                    }
                }
            }
            _ => {
                exec_err!(
                    "get_field function requires the first argument to be struct array"
                )
            }
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!(
                "get_field function requires 2 arguments, got {}",
                args.len()
            );
        }

        match &args[0] {
            ColumnarValue::Array(array) => {
                let struct_array = match array
                    .as_any()
                    .downcast_ref::<arrow::array::StructArray>()
                {
                    Some(struct_array) => struct_array,
                    None => {
                        return exec_err!(
                            "get_field function requires the first argument to be struct array"
                        );
                    }
                };
                match &args[1] {
                    ColumnarValue::Scalar(scalar) => {
                        let column_name = match scalar {
                            ScalarValue::Utf8(name) => name.as_ref().map(|x| x.as_str()),
                            _ => {
                                return exec_err!(
                                    "get_field function requires the argument field_name to be a string"
                                );
                            }
                        };
                        Ok(ColumnarValue::Array(
                            struct_array
                                .column_by_name(column_name.unwrap())
                                .unwrap()
                                .clone(),
                        ))
                    }
                    _ => {
                        exec_err!(
                            "get_field function requires the argument field_name to be a string"
                        )
                    }
                }
            }
            _ => {
                exec_err!(
                    "get_field function requires the first argument to be struct array"
                )
            }
        }
    }
}
