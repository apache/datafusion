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

use arrow::array::StructArray;
use arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, Expr, ExprSchemable};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

/// put values in a struct array.
fn named_struct_expr(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return exec_err!("named_struct requires at least one pair of arguments");
    }

    if args.len() % 2 != 0 {
        return exec_err!("named_struct requires an even number of arguments");
    }

    let fields = args
        .chunks_exact(2)
        .map(|chunk| {
            let name = &chunk[0];
            let value = &chunk[1];

            if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))) = name {
                let array_ref = match value {
                    ColumnarValue::Array(array) => array.clone(),
                    ColumnarValue::Scalar(scalar) => scalar.to_array()?.clone(),
                };

                Ok((
                    Arc::new(Field::new(
                        name.clone(),
                        array_ref.data_type().clone(),
                        true,
                    )),
                    array_ref,
                ))
            } else {
                exec_err!("named_struct even arguments must be string literals")
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ColumnarValue::Array(Arc::new(StructArray::from(fields))))
}
#[derive(Debug)]
pub(super) struct NamedStructFunc {
    signature: Signature,
}

impl NamedStructFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NamedStructFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "named_struct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(datafusion_common::DataFusionError::Internal(
            "return_type called instead of return_type_from_exprs".into(),
        ))
    }

    fn return_type_from_exprs(
        &self,
        args: &[datafusion_expr::Expr],
        schema: &dyn datafusion_common::ExprSchema,
        _arg_types: &[DataType],
    ) -> Result<DataType> {
        // do not accept 0 arguments.
        if args.is_empty() {
            return exec_err!("named_struct requires at least one pair of arguments");
        }

        if args.len() % 2 != 0 {
            return exec_err!("named_struct requires an even number of arguments");
        }

        let return_fields = args
            .chunks_exact(2)
            .map(|chunk| {
                let name = &chunk[0];
                let value = &chunk[1];

                if let Expr::Literal(ScalarValue::Utf8(Some(name))) = name {
                    Ok(Field::new(name.clone(), value.get_type(schema)?, true))
                } else {
                    exec_err!("named_struct even arguments must be string literals")
                }
            })
            .collect::<Result<Vec<Field>>>()?;
        Ok(DataType::Struct(Fields::from(return_fields)))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        named_struct_expr(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use datafusion_common::cast::as_struct_array;
    use datafusion_common::ScalarValue;

    #[test]
    fn test_named_struct() {
        // named_struct("first", 1, "second", 2, "third", 3) = {"first": 1, "second": 2, "third": 3}
        let args = [
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("first".into()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("second".into()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("third".into()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
        ];
        let struc = named_struct_expr(&args)
            .expect("failed to initialize function struct")
            .into_array(1)
            .expect("Failed to convert to array");
        let result =
            as_struct_array(&struc).expect("failed to initialize function struct");
        assert_eq!(
            &Int64Array::from(vec![1]),
            result
                .column_by_name("first")
                .unwrap()
                .clone()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        );
        assert_eq!(
            &Int64Array::from(vec![2]),
            result
                .column_by_name("second")
                .unwrap()
                .clone()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        );
        assert_eq!(
            &Int64Array::from(vec![3]),
            result
                .column_by_name("third")
                .unwrap()
                .clone()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        );
    }
}
