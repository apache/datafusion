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

use arrow::array::Array;
use arrow::datatypes::{DataType, FieldRef, UnionFields};
use datafusion_common::cast::as_union_array;
use datafusion_common::{
    exec_datafusion_err, exec_err, internal_err, ExprSchema, Result, ScalarValue,
};
use datafusion_expr::{ColumnarValue, Expr};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct UnionExtractFun {
    signature: Signature,
}

impl Default for UnionExtractFun {
    fn default() -> Self {
        Self::new()
    }
}

impl UnionExtractFun {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UnionExtractFun {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "union_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        // should be using return_type_from_exprs and not calling the default implementation
        internal_err!("union_extract should return type from exprs")
    }

    fn return_type_from_exprs(
        &self,
        args: &[Expr],
        _: &dyn ExprSchema,
        arg_types: &[DataType],
    ) -> Result<DataType> {
        if args.len() != 2 {
            return exec_err!(
                "union_extract expects 2 arguments, got {} instead",
                args.len()
            );
        }

        let DataType::Union(fields, _) = &arg_types[0] else {
            return exec_err!(
                "union_extract first argument must be a union, got {} instead",
                arg_types[0]
            );
        };

        let Expr::Literal(ScalarValue::Utf8(Some(field_name))) = &args[1] else {
            return exec_err!(
                "union_extract second argument must be a non-null string literal, got {} instead",
                arg_types[1]
            );
        };

        let field = find_field(fields, field_name)?.1;

        Ok(field.data_type().clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!(
                "union_extract expects 2 arguments, got {} instead",
                args.len()
            );
        }

        let union = &args[0];

        let target_name = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(target_name))) => Ok(target_name),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => exec_err!("union_extract second argument must be a non-null string literal, got a null instead"),
            _ => exec_err!("union_extract second argument must be a non-null string literal, got {} instead", &args[1].data_type()),
        };

        match union {
            ColumnarValue::Array(array) => {
                let _union_array = as_union_array(&array).map_err(|_| {
                    exec_datafusion_err!(
                        "union_extract first argument must be a union, got {} instead",
                        array.data_type()
                    )
                })?;

                // Ok(arrow::compute::kernels::union_extract::union_extract(
                //     &union_array,
                //     target_name,
                // )?)
                Ok(ColumnarValue::Array(std::sync::Arc::new(
                    arrow::array::Int32Array::from(vec![1, 2]),
                )))
            }
            ColumnarValue::Scalar(ScalarValue::Union(value, fields, _)) => {
                let target_name = target_name?;
                let (target_type_id, target) = find_field(fields, target_name)?;

                let result = match value {
                    Some((type_id, value)) if target_type_id == *type_id => {
                        *value.clone()
                    }
                    _ => ScalarValue::try_from(target.data_type())?,
                };

                Ok(ColumnarValue::Scalar(result))
            }
            other => exec_err!(
                "union_extract first argument must be a union, got {} instead",
                other.data_type()
            ),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "union_extract expects 2 arguments, got {} instead",
                arg_types.len()
            );
        }

        if !matches!(arg_types[0], DataType::Union(_, _)) {
            return exec_err!(
                "union_extract first argument must be a union, got {} instead",
                arg_types[0]
            );
        }

        if !matches!(arg_types[1], DataType::Utf8) {
            return exec_err!(
                "union_extract second argument must be a non-null string literal, got {} instead",
                arg_types[1]
            );
        }

        Ok(arg_types.to_vec())
    }
}

fn find_field<'a>(fields: &'a UnionFields, name: &str) -> Result<(i8, &'a FieldRef)> {
    fields
        .iter()
        .find(|field| field.1.name() == name)
        .ok_or_else(|| exec_datafusion_err!("field {name} not found on union"))
}

#[cfg(test)]
mod tests {

    use arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use super::UnionExtractFun;

    // when it becomes possible to construct union scalars in SQL, this should go to sqllogictests
    #[test]
    fn test_scalar_value() -> Result<()> {
        let fun = UnionExtractFun::new();

        let fields = UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        );

        let result = fun.invoke(&[
            ColumnarValue::Scalar(ScalarValue::Union(
                None,
                fields.clone(),
                UnionMode::Dense,
            )),
            ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
        ])?;

        assert_scalar(result, ScalarValue::Utf8(None));

        let result = fun.invoke(&[
            ColumnarValue::Scalar(ScalarValue::Union(
                Some((3, Box::new(ScalarValue::Int32(Some(42))))),
                fields.clone(),
                UnionMode::Dense,
            )),
            ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
        ])?;

        assert_scalar(result, ScalarValue::Utf8(None));

        let result = fun.invoke(&[
            ColumnarValue::Scalar(ScalarValue::Union(
                Some((1, Box::new(ScalarValue::new_utf8("42")))),
                fields.clone(),
                UnionMode::Dense,
            )),
            ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
        ])?;

        assert_scalar(result, ScalarValue::new_utf8("42"));

        Ok(())
    }

    fn assert_scalar(value: ColumnarValue, expected: ScalarValue) {
        match value {
            ColumnarValue::Array(array) => panic!("expected scalar got {array:?}"),
            ColumnarValue::Scalar(scalar) => assert_eq!(scalar, expected),
        }
    }
}
