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

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Float32, Float64, Int64};
use datafusion_common::{exec_err, Result};
use datafusion_expr::Signature;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Volatility};
use datafusion_functions::utils::make_scalar_function;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#ceil>
/// Difference between spark: There is no second optional argument to control the rounding behaviour.
/// Takes an Int64/Float32/Float64 input and returns the smallest number after rounding up that is
/// not smaller than the input.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCeil {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkCeil {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCeil {
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkCeil {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ceil"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_ceil, vec![])(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

pub fn spark_ceil(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("ceil expects exactly 1 argument, got {}", args.len());
    }

    let array: &dyn Array = args[0].as_ref();
    match args[0].data_type() {
        Float32 => {
            let array = array
                .as_primitive::<arrow::datatypes::Float32Type>()
                .unary::<_, arrow::datatypes::Int64Type>(|value: f32| value.ceil() as i64);
            Ok(Arc::new(array))
        }
        Float64 => {
            let array = array
                .as_primitive::<arrow::datatypes::Float64Type>()
                .unary::<_, arrow::datatypes::Int64Type>(|value: f64| value.ceil() as i64);
            Ok(Arc::new(array))
        }
        Int64 => Ok(Arc::clone(&args[0])),
        _ => {
            exec_err!(
                "ceil expects a numeric argument, got {}",
                args[0].data_type()
            )
        }
    }
}
