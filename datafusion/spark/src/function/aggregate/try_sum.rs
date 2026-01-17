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

use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::Result;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature};
use datafusion_functions_aggregate::sum::Sum;
use std::any::Any;
use std::fmt::Debug;

/// Thin wrapper over DataFusion native [`Sum`] which is configurable into a try
/// sum mode to return `null` on overflows. We need this thin wrapper to provide
/// the `try_sum` named function for use in Spark.
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct SparkTrySum {
    inner: Sum,
}

impl Default for SparkTrySum {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTrySum {
    pub fn new() -> Self {
        Self {
            inner: Sum::try_sum(),
        }
    }
}

impl AggregateUDFImpl for SparkTrySum {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_sum"
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.accumulator(acc_args)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }
}
