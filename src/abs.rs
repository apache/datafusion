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

//! Spark-compatible implementation of abs function

use std::{any::Any, sync::Arc};

use arrow::datatypes::DataType;
use arrow_schema::ArrowError;

use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature};
use datafusion_common::DataFusionError;
use datafusion_functions::math;

use super::{EvalMode, SparkError};

/// Spark-compatible ABS expression
#[derive(Debug)]
pub struct Abs {
    inner_abs_func: Arc<dyn ScalarUDFImpl>,
    eval_mode: EvalMode,
    data_type_name: String,
}

impl Abs {
    pub fn new(eval_mode: EvalMode, data_type_name: String) -> Result<Self, DataFusionError> {
        if let EvalMode::Legacy | EvalMode::Ansi = eval_mode {
            Ok(Self {
                inner_abs_func: math::abs().inner().clone(),
                eval_mode,
                data_type_name,
            })
        } else {
            Err(DataFusionError::Execution(format!(
                "Invalid EvalMode: \"{:?}\"",
                eval_mode
            )))
        }
    }
}

impl ScalarUDFImpl for Abs {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "abs"
    }

    fn signature(&self) -> &Signature {
        self.inner_abs_func.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        self.inner_abs_func.return_type(arg_types)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        match self.inner_abs_func.invoke(args) {
            Err(DataFusionError::ArrowError(ArrowError::ComputeError(msg), _))
                if msg.contains("overflow") =>
            {
                if self.eval_mode == EvalMode::Legacy {
                    Ok(args[0].clone())
                } else {
                    Err(DataFusionError::External(Box::new(
                        SparkError::ArithmeticOverflow(self.data_type_name.clone()),
                    )))
                }
            }
            other => other,
        }
    }
}
