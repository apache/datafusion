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

/// Evaluates a window function by instantiating a
/// `[PartitionEvaluator]` for calculating the values.
///
/// Note that unlike aggregation based window functions, window
/// functions such as `rank` ignore the values in the window frame,
/// but others such as `first_value`, `last_value`, and
/// `nth_value` need the value.
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

    /// Evaluate window function's arguments against the input window
    /// batch and return an [`ArrayRef`].
    ///
    /// Typically, the resulting vector is a single element vector.
    fn evaluate_args(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        self.expressions()
            .iter()
            .map(|e| e.evaluate(batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect()
    }

    /// Create a [`PartitionEvaluator`] for evaluating data on a
    /// particular partition.
    fn create_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>>;

    /// Construct a reverse expression that produces the same result on
    /// a reversed window. This information is used by the DataFusion
    /// optimizer to potentially avoid resorting the data if possible.
    ///
    /// Returns `None` if the function can not be reversed or if is
    /// not known to be possible (the default).
    ///
    /// For example, the reverse of `lead(10)` is `lag(10)`.
    fn reverse_expr(&self) -> Option<Arc<dyn BuiltInWindowFunctionExpr>> {
        None
    }

    /// Can the window function be incrementally computed using
    /// bounded memory?
    ///
    /// If this function returns true, [`Self::create_evaluator`] must
    /// implement [`PartitionEvaluator::evaluate_stateful`]
    fn supports_bounded_execution(&self) -> bool {
        false
    }

    /// Does the window function use the values from its window frame?
    ///
    /// If this function returns true, [`Self::create_evaluator`] must
    /// implement [`PartitionEvaluator::evaluate_inside_range`]
    fn uses_window_frame(&self) -> bool {
        false
    }
}
