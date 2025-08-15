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

use arrow::array::{ArrayRef, PrimitiveArray};
use arrow::datatypes::{DataType, Int64Type};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

use std::any::Any;
use std::sync::Arc;
use twox_hash::XxHash64;

use crate::function::hash::utils::SparkHasher;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#xxhash64>
#[derive(Debug)]
pub struct SparkXxHash64 {
    signature: Signature,
    seed: i64,
}

impl Default for SparkXxHash64 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkXxHash64 {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            seed: 42,
        }
    }
}

impl ScalarUDFImpl for SparkXxHash64 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "xxhash64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let func = |arr: &[ArrayRef]| {
            let mut result = vec![self.seed; arr[0].len()];
            XxHash64Hasher::hash_arrays(arr, &mut result)?;
            Ok(Arc::new(PrimitiveArray::<Int64Type>::from(result)) as ArrayRef)
        };
        make_scalar_function(func, vec![])(&args.args)
    }
}

struct XxHash64Hasher;

impl SparkHasher<i64> for XxHash64Hasher {
    fn oneshot(seed: i64, data: &[u8]) -> i64 {
        XxHash64::oneshot(seed as u64, data) as i64
    }
}
