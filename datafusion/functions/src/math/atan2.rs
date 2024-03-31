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

//! Math function: `atan2()`.

use arrow::array::{ArrayRef, Float32Array, Float64Array};
use arrow::datatypes::DataType;
use datafusion_common::DataFusionError;
use datafusion_common::{exec_err, Result};
use datafusion_expr::ColumnarValue;
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

use crate::make_function_inputs2;
use crate::utils::make_scalar_function;

#[derive(Debug)]
pub(super) struct Atan2 {
    signature: Signature,
}

impl Atan2 {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![Exact(vec![Float32, Float32]), Exact(vec![Float64, Float64])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for Atan2 {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "atan2"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use self::DataType::*;
        match &arg_types[0] {
            Float32 => Ok(Float32),
            _ => Ok(Float64),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(atan2, vec![])(args)
    }
}

/// Atan2 SQL function
pub fn atan2(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Float64 => Ok(Arc::new(make_function_inputs2!(
            &args[0],
            &args[1],
            "y",
            "x",
            Float64Array,
            { f64::atan2 }
        )) as ArrayRef),

        DataType::Float32 => Ok(Arc::new(make_function_inputs2!(
            &args[0],
            &args[1],
            "y",
            "x",
            Float32Array,
            { f32::atan2 }
        )) as ArrayRef),

        other => exec_err!("Unsupported data type {other:?} for function atan2"),
    }
}
