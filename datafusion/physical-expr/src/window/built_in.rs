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

use super::window_frame_state::WindowFrameContext;
use super::BuiltInWindowFunctionExpr;
use super::WindowExpr;
use crate::window::window_expr::reverse_order_bys;
use crate::{expressions::PhysicalSortExpr, PhysicalExpr};
use arrow::compute::SortOptions;
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::WindowFrame;
use std::any::Any;
use std::sync::Arc;

/// A window expr that takes the form of a built in window function
#[derive(Debug)]
pub struct BuiltInWindowExpr {
    expr: Arc<dyn BuiltInWindowFunctionExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Arc<WindowFrame>,
}

impl BuiltInWindowExpr {
    /// create a new built-in window function expression
    pub fn new(
        expr: Arc<dyn BuiltInWindowFunctionExpr>,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order_by: &[PhysicalSortExpr],
        window_frame: Arc<WindowFrame>,
    ) -> Self {
        Self {
            expr,
            partition_by: partition_by.to_vec(),
            order_by: order_by.to_vec(),
            window_frame,
        }
    }

    /// Get BuiltInWindowFunction expr of BuiltInWindowExpr
    pub fn get_built_in_func_expr(&self) -> &Arc<dyn BuiltInWindowFunctionExpr> {
        &self.expr
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
        let evaluator = self.expr.create_evaluator()?;
        let num_rows = batch.num_rows();
        if evaluator.uses_window_frame() {
            let sort_options: Vec<SortOptions> =
                self.order_by.iter().map(|o| o.options).collect();
            let mut row_wise_results = vec![];

            let length = batch.num_rows();
            let (values, order_bys) = self.get_values_orderbys(batch)?;
            let mut window_frame_ctx = WindowFrameContext::new(&self.window_frame);
            // We iterate on each row to calculate window frame range and and window function result
            for idx in 0..length {
                let range = window_frame_ctx.calculate_range(
                    &order_bys,
                    &sort_options,
                    num_rows,
                    idx,
                )?;
                let value = evaluator.evaluate_inside_range(&values, range)?;
                row_wise_results.push(value);
            }
            ScalarValue::iter_to_array(row_wise_results.into_iter())
        } else if evaluator.include_rank() {
            let columns = self.sort_columns(batch)?;
            let sort_partition_points =
                self.evaluate_partition_points(num_rows, &columns)?;
            evaluator.evaluate_with_rank(num_rows, &sort_partition_points)
        } else {
            let (values, _) = self.get_values_orderbys(batch)?;
            evaluator.evaluate(&values, num_rows)
        }
    }

    fn get_window_frame(&self) -> &Arc<WindowFrame> {
        &self.window_frame
    }

    fn is_window_fn_reversible(&self) -> bool {
        self.expr.as_ref().is_window_fn_reversible()
    }

    fn get_reversed_expr(&self) -> Result<Arc<dyn WindowExpr>> {
        Ok(Arc::new(BuiltInWindowExpr::new(
            self.expr.reverse_expr()?,
            &self.partition_by.clone(),
            &reverse_order_bys(&self.order_by),
            Arc::new(self.window_frame.reverse()),
        )))
    }
}
