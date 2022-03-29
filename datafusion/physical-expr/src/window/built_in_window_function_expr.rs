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

use super::partition_evaluator::PartitionEvaluator;
use crate::PhysicalExpr;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use std::any::Any;
use std::sync::Arc;

/// A window expression that is a built-in window function.
///
/// Note that unlike aggregation based window functions, built-in window functions normally ignore
/// window frame spec, with the exception of first_value, last_value, and nth_value.
pub trait BuiltInWindowFunctionExpr: Send + Sync + std::fmt::Debug {
    /// Returns the aggregate expression as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// the field of the final result of this aggregation.
    fn field(&self) -> Result<Field>;

    /// expressions that are passed to the Accumulator.
    /// Single-column aggregations such as `sum` return a single value, others (e.g. `cov`) return many.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Human readable name such as `"MIN(c2)"` or `"RANK()"`. The default
    /// implementation returns placeholder text.
    fn name(&self) -> &str {
        "BuiltInWindowFunctionExpr: default name"
    }

    /// Create built-in window evaluator with a batch
    fn create_evaluator(
        &self,
        batch: &RecordBatch,
    ) -> Result<Box<dyn PartitionEvaluator>>;
}
