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

/// Trait for planning lambda functions into their physical representation.
/// 
/// This trait is implemented by query planners to convert logical lambda expressions
/// into executable physical lambda functions that can be evaluated at runtime.
pub trait LambdaPlanner {
    /// Plans a logical lambda function into a physical lambda implementation.
    ///
    /// # Arguments
    /// * `lambda` - The logical lambda function to plan
    /// * `df_schema` - The schema context for the lambda function
    ///
    /// # Returns
    /// A boxed physical lambda that can be executed
    fn plan_lambda(
        &self,
        lambda: &LambdaFunction,
        df_schema: &DFSchema,
    ) -> Result<Box<dyn PhysicalLambda>>;
}

/// Trait for physical lambda functions that can be executed on record batches.
/// 
/// Physical lambda functions are the runtime representation of lambda expressions
/// that have been planned and optimized for execution. They can evaluate lambda
/// logic against columnar data in record batches.
pub trait PhysicalLambda: Send + Sync {
    /// Returns the parameter names for this lambda function.
    ///
    /// # Returns
    /// A slice of parameter names that this lambda expects
    fn params(&self) -> &[String];
    
    /// Evaluates the lambda function against a record batch.
    ///
    /// # Arguments
    /// * `batch` - The record batch containing the input data
    ///
    /// # Returns
    /// The result of evaluating the lambda function as a columnar value
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue>;
}
