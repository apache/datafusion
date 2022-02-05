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

//! Defines physical expressions for APPROX_MEDIAN that can be evaluated MEDIAN at runtime during query execution

use std::any::Any;
use std::sync::Arc;

use crate::error::Result;
use crate::physical_plan::{
    expressions::approx_percentile_cont::ApproxPercentileAccumulator, Accumulator,
    AggregateExpr, PhysicalExpr,
};
use crate::scalar::ScalarValue;
use arrow::{array::ArrayRef, datatypes::DataType, datatypes::Field};

use super::format_state_name;

/// MEDIAN aggregate expression
#[derive(Debug)]
pub struct ApproxMedian {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
    data_type: DataType,
}

pub(crate) fn is_approx_median_support_arg_type(arg_type: &DataType) -> bool {
    matches!(
        arg_type,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
    )
}

impl ApproxMedian {
    /// Create a new APPROX_MEDIAN aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
        }
    }
}

impl AggregateExpr for ApproxMedian {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ApproxMedianAccumulator::try_new(
            self.data_type.clone(),
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                &format_state_name(&self.name, "max_size"),
                DataType::UInt64,
                false,
            ),
            Field::new(
                &format_state_name(&self.name, "sum"),
                DataType::Float64,
                false,
            ),
            Field::new(
                &format_state_name(&self.name, "count"),
                DataType::Float64,
                false,
            ),
            Field::new(
                &format_state_name(&self.name, "max"),
                DataType::Float64,
                false,
            ),
            Field::new(
                &format_state_name(&self.name, "min"),
                DataType::Float64,
                false,
            ),
            Field::new(
                &format_state_name(&self.name, "centroids"),
                DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
                false,
            ),
        ])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// An accumulator to compute the approx_median.
/// It is using approx_percentile_cont under the hood, which is an approximation.
/// We will revist this and may provide an implementation to calculate the exact approx_median in the future.
#[derive(Debug)]
pub struct ApproxMedianAccumulator {
    perc_cont: ApproxPercentileAccumulator,
}

impl ApproxMedianAccumulator {
    /// Creates a new `ApproxMedianAccumulator`
    pub fn try_new(data_type: DataType) -> Result<Self> {
        Ok(Self {
            perc_cont: ApproxPercentileAccumulator::new(0.5_f64, data_type),
        })
    }
}

impl Accumulator for ApproxMedianAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(self.perc_cont.get_digest().to_scalar_state())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.perc_cont.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.perc_cont.merge_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        self.perc_cont.evaluate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::from_slice::FromSlice;
    use crate::physical_plan::expressions::col;
    use crate::{error::Result, generic_test_op};
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};

    #[test]
    fn approx_median_f64_1() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from_slice(&[1_f64, 2_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            ApproxMedian,
            ScalarValue::from(1.5_f64),
            DataType::Float64
        )
    }

    #[test]
    fn approx_median_f64_2() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from_slice(&[1.1_f64, 2_f64, 3_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            ApproxMedian,
            ScalarValue::from(2_f64),
            DataType::Float64
        )
    }

    #[test]
    fn approx_median_f64_3() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from_slice(&[
            1_f64, 2_f64, 3_f64, 4_f64, 5_f64,
        ]));
        generic_test_op!(
            a,
            DataType::Float64,
            ApproxMedian,
            ScalarValue::from(3_f64),
            DataType::Float64
        )
    }

    #[test]
    fn approx_median_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            ApproxMedian,
            ScalarValue::from(3),
            DataType::Int32
        )
    }

    #[test]
    fn approx_median_u32() -> Result<()> {
        let a: ArrayRef = Arc::new(UInt32Array::from_slice(&[
            1_u32, 2_u32, 3_u32, 4_u32, 5_u32,
        ]));
        generic_test_op!(
            a,
            DataType::UInt32,
            ApproxMedian,
            ScalarValue::from(3_u32),
            DataType::UInt32
        )
    }

    #[test]
    fn approx_median_f32() -> Result<()> {
        let a: ArrayRef = Arc::new(Float32Array::from_slice(&[
            1_f32, 2_f32, 3_f32, 4_f32, 5_f32,
        ]));
        generic_test_op!(
            a,
            DataType::Float32,
            ApproxMedian,
            ScalarValue::from(3_f32),
            DataType::Float32
        )
    }

    #[test]
    fn approx_median_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        generic_test_op!(
            a,
            DataType::Int32,
            ApproxMedian,
            ScalarValue::from(3),
            DataType::Int32
        )
    }

    #[test]
    fn approx_median_i32_with_nulls_2() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(7),
            Some(6),
            Some(1),
            None,
            None,
            None,
            None,
            Some(5),
            Some(4),
        ]));
        generic_test_op!(
            a,
            DataType::Int32,
            ApproxMedian,
            ScalarValue::from(2),
            DataType::Int32
        )
    }

    #[test]
    fn approx_median_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None]));
        generic_test_op!(
            a,
            DataType::Int32,
            ApproxMedian,
            ScalarValue::from(0),
            DataType::Int32
        )
    }

    fn aggregate(
        batch: &RecordBatch,
        agg: Arc<dyn AggregateExpr>,
    ) -> Result<ScalarValue> {
        let mut accum = agg.create_accumulator()?;
        let expr = agg.expressions();
        let values = expr
            .iter()
            .map(|e| e.evaluate(batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()?;
        accum.update_batch(&values)?;
        accum.evaluate()
    }
}