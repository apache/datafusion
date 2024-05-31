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

use crate::aggregate::stats::StatsType;
use crate::aggregate::utils::down_cast_any_ref;
use crate::aggregate::variance::VarianceAccumulator;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::{array::ArrayRef, datatypes::DataType, datatypes::Field};
use datafusion_common::ScalarValue;
use datafusion_common::{internal_err, Result};
use datafusion_expr::Accumulator;

/// STDDEV and STDDEV_SAMP (standard deviation) aggregate expression
#[derive(Debug)]
pub struct Stddev {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
}

/// STDDEV_POP population aggregate expression
#[derive(Debug)]
pub struct StddevPop {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
}

impl Stddev {
    /// Create a new STDDEV aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        // the result of stddev just support FLOAT64 and Decimal data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            expr,
        }
    }
}

impl AggregateExpr for Stddev {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Float64, true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(StddevAccumulator::try_new(StatsType::Sample)?))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(StddevAccumulator::try_new(StatsType::Sample)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(&self.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "mean"),
                DataType::Float64,
                true,
            ),
            Field::new(format_state_name(&self.name, "m2"), DataType::Float64, true),
        ])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for Stddev {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.name == x.name && self.expr.eq(&x.expr))
            .unwrap_or(false)
    }
}

impl StddevPop {
    /// Create a new STDDEV aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        // the result of stddev just support FLOAT64 and Decimal data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            expr,
        }
    }
}

impl AggregateExpr for StddevPop {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Float64, true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(StddevAccumulator::try_new(StatsType::Population)?))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(StddevAccumulator::try_new(StatsType::Population)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(&self.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "mean"),
                DataType::Float64,
                true,
            ),
            Field::new(format_state_name(&self.name, "m2"), DataType::Float64, true),
        ])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for StddevPop {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.name == x.name && self.expr.eq(&x.expr))
            .unwrap_or(false)
    }
}

/// An accumulator to compute the average
#[derive(Debug)]
pub struct StddevAccumulator {
    variance: VarianceAccumulator,
}

impl StddevAccumulator {
    /// Creates a new `StddevAccumulator`
    pub fn try_new(s_type: StatsType) -> Result<Self> {
        Ok(Self {
            variance: VarianceAccumulator::try_new(s_type)?,
        })
    }

    pub fn get_m2(&self) -> f64 {
        self.variance.get_m2()
    }
}

impl Accumulator for StddevAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.variance.get_count()),
            ScalarValue::from(self.variance.get_mean()),
            ScalarValue::from(self.variance.get_m2()),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.variance.update_batch(values)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.variance.retract_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.variance.merge_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let variance = self.variance.evaluate()?;
        match variance {
            ScalarValue::Float64(e) => {
                if e.is_none() {
                    Ok(ScalarValue::Float64(None))
                } else {
                    Ok(ScalarValue::Float64(e.map(|f| f.sqrt())))
                }
            }
            _ => internal_err!("Variance should be f64"),
        }
    }

    fn size(&self) -> usize {
        std::mem::align_of_val(self) - std::mem::align_of_val(&self.variance)
            + self.variance.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::utils::get_accum_scalar_values_as_arrays;
    use crate::expressions::col;
    use arrow::{array::*, datatypes::*};

    #[test]
    fn stddev_f64_merge_1() -> Result<()> {
        let a = Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64]));
        let b = Arc::new(Float64Array::from(vec![4_f64, 5_f64]));

        let schema = Schema::new(vec![Field::new("a", DataType::Float64, false)]);

        let batch1 = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;
        let batch2 = RecordBatch::try_new(Arc::new(schema.clone()), vec![b])?;

        let agg1 = Arc::new(StddevPop::new(
            col("a", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let agg2 = Arc::new(StddevPop::new(
            col("a", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let actual = merge(&batch1, &batch2, agg1, agg2)?;
        assert!(actual == ScalarValue::from(std::f64::consts::SQRT_2));

        Ok(())
    }

    #[test]
    fn stddev_f64_merge_2() -> Result<()> {
        let a = Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        let b = Arc::new(Float64Array::from(vec![None]));

        let schema = Schema::new(vec![Field::new("a", DataType::Float64, true)]);

        let batch1 = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;
        let batch2 = RecordBatch::try_new(Arc::new(schema.clone()), vec![b])?;

        let agg1 = Arc::new(StddevPop::new(
            col("a", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let agg2 = Arc::new(StddevPop::new(
            col("a", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let actual = merge(&batch1, &batch2, agg1, agg2)?;
        assert!(actual == ScalarValue::from(std::f64::consts::SQRT_2));

        Ok(())
    }

    fn merge(
        batch1: &RecordBatch,
        batch2: &RecordBatch,
        agg1: Arc<dyn AggregateExpr>,
        agg2: Arc<dyn AggregateExpr>,
    ) -> Result<ScalarValue> {
        let mut accum1 = agg1.create_accumulator()?;
        let mut accum2 = agg2.create_accumulator()?;
        let expr1 = agg1.expressions();
        let expr2 = agg2.expressions();

        let values1 = expr1
            .iter()
            .map(|e| {
                e.evaluate(batch1)
                    .and_then(|v| v.into_array(batch1.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;
        let values2 = expr2
            .iter()
            .map(|e| {
                e.evaluate(batch2)
                    .and_then(|v| v.into_array(batch2.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;
        accum1.update_batch(&values1)?;
        accum2.update_batch(&values2)?;
        let state2 = get_accum_scalar_values_as_arrays(accum2.as_mut())?;
        accum1.merge_batch(&state2)?;
        accum1.evaluate()
    }
}
