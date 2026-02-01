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
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, FieldRef};
use crc32fast::Hasher;
use datafusion_common::cast::{
    as_binary_array, as_binary_view_array, as_fixed_size_binary_array,
    as_large_binary_array,
};
use datafusion_common::types::{NativeType, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#crc32>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCrc32 {
    signature: Signature,
}

impl Default for SparkCrc32 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCrc32 {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_implicit(
                    TypeSignatureClass::Binary,
                    vec![TypeSignatureClass::Native(logical_string())],
                    NativeType::Binary,
                )],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkCrc32 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "crc32"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_crc32, vec![])(&args.args)
    }
}

fn spark_crc32_digest(value: &[u8]) -> i64 {
    let mut hasher = Hasher::new();
    hasher.update(value);
    hasher.finalize() as i64
}

fn spark_crc32_impl<'a>(input: impl Iterator<Item = Option<&'a [u8]>>) -> ArrayRef {
    let result = input
        .map(|value| value.map(spark_crc32_digest))
        .collect::<Int64Array>();
    Arc::new(result)
}

fn spark_crc32(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = take_function_args("crc32", args)?;

    match input.data_type() {
        DataType::Null => Ok(Arc::new(Int64Array::new_null(input.len()))),
        DataType::Binary => {
            let input = as_binary_array(input)?;
            Ok(spark_crc32_impl(input.iter()))
        }
        DataType::LargeBinary => {
            let input = as_large_binary_array(input)?;
            Ok(spark_crc32_impl(input.iter()))
        }
        DataType::BinaryView => {
            let input = as_binary_view_array(input)?;
            Ok(spark_crc32_impl(input.iter()))
        }
        DataType::FixedSizeBinary(_) => {
            let input = as_fixed_size_binary_array(input)?;
            Ok(spark_crc32_impl(input.iter()))
        }
        dt => {
            internal_err!("Unsupported data type for crc32: {dt}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32_nullability() -> Result<()> {
        let crc32_func = SparkCrc32::new();

        // non-nullable field should produce non-nullable output
        let field_not_null = Arc::new(Field::new("data", DataType::Binary, false));
        let result = crc32_func.return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&field_not_null),
            scalar_arguments: &[None],
        })?;
        assert!(!result.is_nullable());
        assert_eq!(result.data_type(), &DataType::Int64);

        // nullable field should produce nullable output
        let field_nullable = Arc::new(Field::new("data", DataType::Binary, true));
        let result = crc32_func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[field_nullable],
            scalar_arguments: &[None],
        })?;
        assert!(result.is_nullable());
        assert_eq!(result.data_type(), &DataType::Int64);

        Ok(())
    }
}
