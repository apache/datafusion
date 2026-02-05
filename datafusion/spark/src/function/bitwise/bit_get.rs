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

use std::any::Any;
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, Int8Array, Int32Array, PrimitiveArray,
    downcast_integer_array,
};
use arrow::compute::try_binary;
use arrow::datatypes::{ArrowNativeType, DataType, Field, FieldRef, Int8Type, Int32Type};
use datafusion_common::types::{NativeType, logical_int32};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBitGet {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkBitGet {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBitGet {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Integer),
                    Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_int32()),
                        vec![TypeSignatureClass::Integer],
                        NativeType::Int32,
                    ),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["getbit".to_string()],
        }
    }
}

impl ScalarUDFImpl for SparkBitGet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bit_get"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Spark derives nullability for BinaryExpression from its children
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Int8, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_bit_get, vec![])(&args.args)
    }
}

fn spark_bit_get_inner<T: ArrowPrimitiveType>(
    value: &PrimitiveArray<T>,
    pos: &Int32Array,
) -> Result<Int8Array> {
    let bit_length = (size_of::<T::Native>() * 8) as i32;

    let result: PrimitiveArray<Int8Type> = try_binary(value, pos, |value, pos| {
        if pos < 0 || pos >= bit_length {
            return Err(arrow::error::ArrowError::ComputeError(format!(
                "bit_get: position {pos} is out of bounds. Expected pos < {bit_length} and pos >= 0"
            )));
        }
        Ok(((value.to_i64().unwrap() >> pos) & 1) as i8)
    })?;
    Ok(result)
}

fn spark_bit_get(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [value, position] = take_function_args("bit_get", args)?;
    let pos_arg = position.as_primitive::<Int32Type>();
    let ret = downcast_integer_array!(
        value => spark_bit_get_inner(value, pos_arg),
        DataType::Null => Ok(Int8Array::new_null(value.len())),
        d => internal_err!("Unsupported datatype for bit_get: {d}"),
    )?;
    Ok(Arc::new(ret))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    #[test]
    fn test_bit_get_nullability_non_nullable_inputs() {
        let func = SparkBitGet::new();
        let value_field = Arc::new(Field::new("value", DataType::Int32, false));
        let pos_field = Arc::new(Field::new("pos", DataType::Int32, false));

        let out_field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[value_field, pos_field],
                scalar_arguments: &[None, None],
            })
            .unwrap();

        assert_eq!(out_field.data_type(), &DataType::Int8);
        assert!(!out_field.is_nullable());
    }

    #[test]
    fn test_bit_get_nullability_nullable_inputs() {
        let func = SparkBitGet::new();
        let value_field = Arc::new(Field::new("value", DataType::Int32, true));
        let pos_field = Arc::new(Field::new("pos", DataType::Int32, false));

        let out_field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[value_field, pos_field],
                scalar_arguments: &[None, None],
            })
            .unwrap();

        assert_eq!(out_field.data_type(), &DataType::Int8);
        assert!(out_field.is_nullable());
    }
}
