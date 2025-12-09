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

use arrow::array::*;
use arrow::compute::kernels::bitwise;
use arrow::datatypes::{
    DataType, Field, FieldRef, Int16Type, Int32Type, Int64Type, Int8Type,
};
use datafusion_common::{plan_err, Result};
use datafusion_expr::{ColumnarValue, TypeSignature, Volatility};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_functions::utils::make_scalar_function;
use std::{any::Any, sync::Arc};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBitwiseNot {
    signature: Signature,
}

impl Default for SparkBitwiseNot {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBitwiseNot {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int8]),
                    TypeSignature::Exact(vec![DataType::Int16]),
                    TypeSignature::Exact(vec![DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkBitwiseNot {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bitwise_not"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        plan_err!("SparkBitwiseNot: return_type() is not used; return_field_from_args() is implemented")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() != 1 {
            return plan_err!("bitwise_not expects exactly 1 argument");
        }

        let input_field = &args.arg_fields[0];

        let out_dt = input_field.data_type().clone();
        let out_nullable = input_field.is_nullable();

        Ok(Arc::new(Field::new("bitwise_not", out_dt, out_nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err!("bitwise_not expects exactly 1 argument");
        }
        make_scalar_function(spark_bitwise_not, vec![])(&args.args)
    }
}

pub fn spark_bitwise_not(args: &[ArrayRef]) -> Result<ArrayRef> {
    let array = args[0].as_ref();
    match array.data_type() {
        DataType::Int8 => {
            let result: Int8Array =
                bitwise::bitwise_not(array.as_primitive::<Int8Type>())?;
            Ok(Arc::new(result))
        }
        DataType::Int16 => {
            let result: Int16Array =
                bitwise::bitwise_not(array.as_primitive::<Int16Type>())?;
            Ok(Arc::new(result))
        }
        DataType::Int32 => {
            let result: Int32Array =
                bitwise::bitwise_not(array.as_primitive::<Int32Type>())?;
            Ok(Arc::new(result))
        }
        DataType::Int64 => {
            let result: Int64Array =
                bitwise::bitwise_not(array.as_primitive::<Int64Type>())?;
            Ok(Arc::new(result))
        }
        _ => {
            plan_err!(
                "bitwise_not function does not support data type: {}",
                array.data_type()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    use datafusion_expr::ReturnFieldArgs;

    #[test]
    fn test_bitwise_not_nullability() {
        let bitwise_not = SparkBitwiseNot::new();

        // --- non-nullable Int32 input ---
        let non_nullable_i32 = Arc::new(Field::new("c", DataType::Int32, false));
        let out_non_null = bitwise_not
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_i32)],
                // single-argument function -> one scalar_argument slot (None)
                scalar_arguments: &[None],
            })
            .unwrap();

        // result should be non-nullable and the same DataType as input
        assert!(!out_non_null.is_nullable());
        assert_eq!(out_non_null.data_type(), &DataType::Int32);

        // --- nullable Int32 input ---
        let nullable_i32 = Arc::new(Field::new("c", DataType::Int32, true));
        let out_nullable = bitwise_not
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&nullable_i32)],
                scalar_arguments: &[None],
            })
            .unwrap();

        // result should be nullable and the same DataType as input
        assert!(out_nullable.is_nullable());
        assert_eq!(out_nullable.data_type(), &DataType::Int32);

        // --- also test another integer type (Int64) for completeness ---
        let non_nullable_i64 = Arc::new(Field::new("c", DataType::Int64, false));
        let out_i64 = bitwise_not
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_i64)],
                scalar_arguments: &[None],
            })
            .unwrap();

        assert!(!out_i64.is_nullable());
        assert_eq!(out_i64.data_type(), &DataType::Int64);

        let nullable_i64 = Arc::new(Field::new("c", DataType::Int64, true));
        let out_i64_null = bitwise_not
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&nullable_i64)],
                scalar_arguments: &[None],
            })
            .unwrap();

        assert!(out_i64_null.is_nullable());
        assert_eq!(out_i64_null.data_type(), &DataType::Int64);
    }
}
