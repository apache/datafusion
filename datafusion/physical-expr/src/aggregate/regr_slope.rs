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

//! Defines physical expressions that can evaluated at runtime during query execution

use std::any::Any;
use std::sync::Arc;

use crate::{AggregateExpr, PhysicalExpr};
use arrow::array::Float64Array;
use arrow::{
    array::{ArrayRef, UInt64Array},
    compute::cast,
    datatypes::DataType,
    datatypes::Field,
};
use datafusion_common::{downcast_value, unwrap_or_internal_err, ScalarValue};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Accumulator;

use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;

/// regr_slope aggregate expression
/// Returns the slope of the linear regression line for non-null pairs in aggregate columns
/// Given input column Y and X: regr_slope(Y, X) returns the slope (k in Y = k*X + b) using minimal
/// RSS fitting.
#[derive(Debug)]
pub struct RegrSlope {
    name: String,
    expr_y: Arc<dyn PhysicalExpr>,
    expr_x: Arc<dyn PhysicalExpr>,
}

impl RegrSlope {
    pub fn new(
        expr_y: Arc<dyn PhysicalExpr>,
        expr_x: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        return_type: DataType,
    ) -> Self {
        // the result of regr_slope only support FLOAT64 data type.
        assert!(matches!(return_type, DataType::Float64));
        Self {
            name: name.into(),
            expr_y,
            expr_x,
        }
    }
}

impl AggregateExpr for RegrSlope {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Float64, true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(RegrSlopeAccumulator::try_new()?))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(RegrSlopeAccumulator::try_new()?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(&self.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "mean_x"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "mean_y"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "m2_x"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "algo_const"),
                DataType::Float64,
                true,
            ),
        ])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr_y.clone(), self.expr_x.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for RegrSlope {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.expr_y.eq(&x.expr_y)
                    && self.expr_x.eq(&x.expr_x)
            })
            .unwrap_or(false)
    }
}

// regr_slope(y, x) is calculated using cov_pop(x, y)/var_pop(x)
// Reference of online algorithms for calculationg variance:
// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
#[derive(Debug)]
pub struct RegrSlopeAccumulator {
    count: u64,
    mean_x: f64,
    mean_y: f64,
    m2_x: f64,
    algo_const: f64,
}

impl RegrSlopeAccumulator {
    /// Creates a new `RegrSlopeAccumulator`
    pub fn try_new() -> Result<Self> {
        Ok(Self {
            count: 0_u64,
            mean_x: 0_f64,
            mean_y: 0_f64,
            m2_x: 0_f64,
            algo_const: 0_f64,
        })
    }
}

impl Accumulator for RegrSlopeAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::from(self.mean_x),
            ScalarValue::from(self.mean_y),
            ScalarValue::from(self.m2_x),
            ScalarValue::from(self.algo_const),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // regr_slope(Y, X) calculates k in y = k*x + b
        let values_y = &cast(&values[0], &DataType::Float64)?;
        let values_x = &cast(&values[1], &DataType::Float64)?;

        let mut arr_y = downcast_value!(values_y, Float64Array).iter().flatten();
        let mut arr_x = downcast_value!(values_x, Float64Array).iter().flatten();

        for i in 0..values_y.len() {
            // skip either x or y is NULL
            let value_y = if values_y.is_valid(i) {
                arr_y.next()
            } else {
                None
            };
            let value_x = if values_x.is_valid(i) {
                arr_x.next()
            } else {
                None
            };
            if value_y.is_none() || value_x.is_none() {
                continue;
            }

            // Update states for regr_slope(y,x) [using cov_pop(x,y)/var_pop(x)]
            let value_y = unwrap_or_internal_err!(value_y);
            let value_x = unwrap_or_internal_err!(value_x);

            self.count += 1;
            let delta_x = value_x - self.mean_x;
            let delta_y = value_y - self.mean_y;
            self.mean_x += delta_x / self.count as f64;
            let delta_x_2 = value_x - self.mean_x;
            self.m2_x += delta_x * delta_x_2;
            self.mean_y += delta_y / self.count as f64;
            self.algo_const += delta_x * (value_y - self.mean_y);
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values_y = &cast(&values[0], &DataType::Float64)?;
        let values_x = &cast(&values[1], &DataType::Float64)?;

        let mut arr_y = downcast_value!(values_y, Float64Array).iter().flatten();
        let mut arr_x = downcast_value!(values_x, Float64Array).iter().flatten();

        for i in 0..values_y.len() {
            // skip either x or y is NULL
            let value_y = if values_y.is_valid(i) {
                arr_y.next()
            } else {
                None
            };
            let value_x = if values_x.is_valid(i) {
                arr_x.next()
            } else {
                None
            };
            if value_y.is_none() || value_x.is_none() {
                continue;
            }

            // Update states for regr_slope(y,x) [using cov_pop(x,y)/var_pop(x)]
            let value_y = unwrap_or_internal_err!(value_y);
            let value_x = unwrap_or_internal_err!(value_x);

            if self.count > 1 {
                self.count -= 1;
                let delta_x = value_x - self.mean_x;
                let delta_y = value_y - self.mean_y;
                self.mean_x -= delta_x / self.count as f64;
                let delta_x_2 = value_x - self.mean_x;
                self.m2_x -= delta_x * delta_x_2;
                self.mean_y -= delta_y / self.count as f64;
                self.algo_const -= delta_x * (value_y - self.mean_y);
            } else {
                self.count = 0;
                self.mean_x = 0.0;
                self.m2_x = 0.0;
                self.mean_y = 0.0;
                self.algo_const = 0.0;
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let count_arr = downcast_value!(states[0], UInt64Array);
        let mean_x_arr = downcast_value!(states[1], Float64Array);
        let mean_y_arr = downcast_value!(states[2], Float64Array);
        let m2_x_arr = downcast_value!(states[3], Float64Array);
        let algo_const_arr = downcast_value!(states[4], Float64Array);

        for i in 0..count_arr.len() {
            let count_b = count_arr.value(i);
            if count_b == 0_u64 {
                continue;
            }
            let (count_a, mean_x_a, mean_y_a, m2_x_a, algo_const_a) = (
                self.count,
                self.mean_x,
                self.mean_y,
                self.m2_x,
                self.algo_const,
            );
            let (count_b, mean_x_b, mean_y_b, m2_x_b, algo_const_b) = (
                count_b,
                mean_x_arr.value(i),
                mean_y_arr.value(i),
                m2_x_arr.value(i),
                algo_const_arr.value(i),
            );

            // Assuming two different batches of input have calculated the states:
            // batch A of Y, X -> {count_a, mean_x_a, mean_y_a, m2_x_a, algo_const_a}
            // batch B of Y, X -> {count_b, mean_x_b, mean_y_b, m2_x_b, algo_const_b}
            // The merged states from A and B are {count_ab, mean_x_ab, mean_y_ab, m2_x_ab,
            // algo_const_ab}
            //
            // Reference for the algorithm to merge states:
            // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
            let count_ab = count_a + count_b;
            let (count_a, count_b) = (count_a as f64, count_b as f64);
            let d_x = mean_x_b - mean_x_a;
            let d_y = mean_y_b - mean_y_a;
            let mean_x_ab = mean_x_a + d_x * count_b / count_ab as f64;
            let mean_y_ab = mean_y_a + d_y * count_b / count_ab as f64;
            let m2_x_ab =
                m2_x_a + m2_x_b + d_x * d_x * count_a * count_b / count_ab as f64;
            let algo_const_ab = algo_const_a
                + algo_const_b
                + d_x * d_y * count_a * count_b / count_ab as f64;

            self.count = count_ab;
            self.mean_x = mean_x_ab;
            self.mean_y = mean_y_ab;
            self.m2_x = m2_x_ab;
            self.algo_const = algo_const_ab;
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let cov_pop_x_y = self.algo_const / self.count as f64;
        let var_pop_x = self.m2_x / self.count as f64;

        // Only 0/1 point or slope is infinite
        if self.count <= 1 || var_pop_x == 0.0 {
            Ok(ScalarValue::Float64(None))
        } else {
            Ok(ScalarValue::Float64(Some(cov_pop_x_y / var_pop_x)))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
