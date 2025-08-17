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

use arrow::array::RecordBatch;
use datafusion_common::{DFSchema, Result};
use datafusion_expr_common::columnar_value::ColumnarValue;

use crate::expr::LambdaFunction;

pub trait LambdaPlanner {
    fn plan_lambda(
        &self,
        lambda: &LambdaFunction,
        df_schema: &DFSchema,
    ) -> Result<Box<dyn PhysicalLambda>>;
}

pub trait PhysicalLambda: Send + Sync {
    fn params(&self) -> &[String];
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue>;
}
