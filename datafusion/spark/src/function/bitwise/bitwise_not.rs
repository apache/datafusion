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

use arrow::compute::kernels::bitwise;
use arrow::datatypes::{Int16Type, Int32Type, Int64Type, Int8Type};
use arrow::{array::*, datatypes::DataType};
use datafusion_common::{plan_err, Result};
use datafusion_expr::{ColumnarValue, TypeSignature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
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
