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
use arrow::array::ArrayRef;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use std::any::Any;
use std::sync::Arc;

/// A window expression that evaluates a window function.
///
/// Note that unlike aggregation based window functions, window
/// functions normally ignore the window frame spec with the exception
/// of `first_value`, `last_value`, and `nth_value`.
///
pub trait BuiltInWindowFunctionExpr: Send + Sync + std::fmt::Debug {
    /// Returns the aggregate expression as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// The field of the final result of evaluating this window function.
    fn field(&self) -> Result<Field>;

    /// Expressions that are passed to the [`PartitionEvaluator`].
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Human readable name such as `"MIN(c2)"` or `"RANK()"`. The default
    /// implementation returns placeholder text.
    fn name(&self) -> &str {
        "BuiltInWindowFunctionExpr: default name"
    }

    /// Evaluate window function's arguments against the batch and
    /// return an array ref. Typically, the resulting vector is a
    /// single element vector.
    fn evaluate_args(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        self.expressions()
            .iter()
            .map(|e| e.evaluate(batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect()
    }

    /// Create a [`PartitionEvaluator`] to evaluate data on a particular partition.
    fn create_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>>;

    /// Construct Reverse Expression that produces the same result
    /// on a reversed window. Retuns `None` if not possible.
    ///
    /// For example, the reverse of `lead(10)` is `lag(10)`.
    fn reverse_expr(&self) -> Option<Arc<dyn BuiltInWindowFunctionExpr>> {
        None
    }

    fn supports_bounded_execution(&self) -> bool {
        false
    }

    /// If returns true, [`Self::create_evaluator`] must implement
    /// [`PartitionEvaluator::evaluate_inside_range`]
    fn uses_window_frame(&self) -> bool {
        false
    }
}
