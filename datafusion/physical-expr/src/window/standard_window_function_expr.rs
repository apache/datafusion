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

use crate::{PhysicalExpr, PhysicalSortExpr};

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_expr::PartitionEvaluator;

use std::any::Any;
use std::sync::Arc;

/// Evaluates a window function by instantiating a
/// `[PartitionEvaluator]` for calculating the function's output in
/// that partition.
///
/// Note that unlike aggregation based window functions, some window
/// functions such as `rank` ignore the values in the window frame,
/// but others such as `first_value`, `last_value`, and
/// `nth_value` need the value.
#[allow(rustdoc::private_intra_doc_links)]
pub trait StandardWindowFunctionExpr: Send + Sync + std::fmt::Debug {
    /// Returns the aggregate expression as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// The field of the final result of evaluating this window function.
    fn field(&self) -> Result<Field>;

    /// Expressions that are passed to the [`PartitionEvaluator`].
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Human readable name such as `"MIN(c2)"` or `"RANK()"`. The default
    /// implementation returns placeholder text.
    fn name(&self) -> &str {
        "StandardWindowFunctionExpr: default name"
    }

    /// Evaluate window function's arguments against the input window
    /// batch and return an [`ArrayRef`].
    ///
    /// Typically, the resulting vector is a single element vector.
    fn evaluate_args(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        self.expressions()
            .iter()
            .map(|e| {
                e.evaluate(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
            })
            .collect()
    }

    /// Create a [`PartitionEvaluator`] for evaluating the function on
    /// a particular partition.
    fn create_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>>;

    /// Construct a new [`StandardWindowFunctionExpr`] that produces
    /// the same result as this function on a window with reverse
    /// order. The return value of this function is used by the
    /// DataFusion optimizer to avoid re-sorting the data when
    /// possible.
    ///
    /// Returns `None` (the default) if no reverse is known (or possible).
    ///
    /// For example, the reverse of `lead(10)` is `lag(10)`.
    fn reverse_expr(&self) -> Option<Arc<dyn StandardWindowFunctionExpr>> {
        None
    }

    /// Returns the ordering introduced by the window function, if applicable.
    /// Most window functions don't introduce an ordering, hence the default
    /// value is `None`. Note that this information is used to update ordering
    /// equivalences.
    fn get_result_ordering(&self, _schema: &SchemaRef) -> Option<PhysicalSortExpr> {
        None
    }
}
