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

//! Physical exec for built-in window function expressions.

use super::BuiltInWindowFunctionExpr;
use super::WindowExpr;
use crate::{expressions::PhysicalSortExpr, PhysicalExpr};
use arrow::array::Array;
use arrow::compute::{concat, SortOptions};
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_expr::WindowFrame;
use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

/// A window expr that takes the form of a built in window function
#[derive(Debug)]
pub struct BuiltInWindowExpr {
    expr: Arc<dyn BuiltInWindowFunctionExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Option<WindowFrame>,
}

impl BuiltInWindowExpr {
    /// create a new built-in window function expression
    pub fn new(
        expr: Arc<dyn BuiltInWindowFunctionExpr>,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order_by: &[PhysicalSortExpr],
        window_frame: Option<WindowFrame>,
    ) -> Self {
        Self {
            expr,
            partition_by: partition_by.to_vec(),
            order_by: order_by.to_vec(),
            window_frame,
        }
    }
}

impl WindowExpr for BuiltInWindowExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.expr.name()
    }

    fn field(&self) -> Result<Field> {
        self.expr.field()
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.expr.expressions()
    }

    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.partition_by
    }

    fn order_by(&self) -> &[PhysicalSortExpr] {
        &self.order_by
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let evaluator = self.expr.create_evaluator(batch)?;
        let num_rows = batch.num_rows();
        let partition_columns = self.partition_columns(batch)?;
        let partition_points =
            self.evaluate_partition_points(num_rows, &partition_columns)?;

        let results = match (evaluator.include_rank(), evaluator.is_window_frame_used()) {
            (_, true) => {
                let columns = self.sort_columns(batch)?;
                let n_partition_column = partition_columns.len();
                let n_column = columns.len();
                let order_by_slice = &columns[n_partition_column..n_column];
                let array_refs: Vec<ArrayRef> =
                    order_by_slice.iter().map(|s| s.values.clone()).collect();
                let sort_options: Vec<SortOptions> =
                    self.order_by.iter().map(|o| o.options).collect();

                let mut ranges = vec![];
                let mut indices_range = vec![];
                let window_frame =
                    if self.window_frame.is_none() && !array_refs.is_empty() {
                        // OVER (ORDER BY a) case implicitly cast to OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                        Some(self.implicit_order_by_window())
                    } else {
                        self.window_frame
                    };
                for partition_range in &partition_points {
                    let num_rows = partition_range.end - partition_range.start;
                    let array_ref_slice = array_refs
                        .iter()
                        .map(|elem| elem.slice(partition_range.start, num_rows))
                        .collect::<Vec<ArrayRef>>();
                    for idx in 0..num_rows {
                        let res = self.calculate_range(
                            &window_frame,
                            &array_ref_slice,
                            &sort_options,
                            num_rows,
                            idx,
                        )?;
                        let res = (
                            partition_range.start + res.0,
                            partition_range.start + res.1,
                        );
                        ranges.push(Range {
                            start: res.0,
                            end: res.1,
                        });
                        indices_range.push(Range {
                            start: idx,
                            end: idx + 1,
                        });
                    }
                }

                ranges
                    .iter()
                    .map(|elem| evaluator.evaluate_inside_range(elem.clone()).unwrap())
                    .collect::<Vec<ArrayRef>>()
            }
            (true, false) => {
                let sort_partition_points =
                    self.evaluate_partition_points(num_rows, &self.sort_columns(batch)?)?;
                evaluator.evaluate_with_rank(partition_points, sort_partition_points)?
            }
            (false, false) => evaluator.evaluate(partition_points)?,
        };
        let results = results.iter().map(|i| i.as_ref()).collect::<Vec<_>>();
        let a = concat(&results).map_err(DataFusionError::ArrowError)?;
        Ok(a)
    }
}
