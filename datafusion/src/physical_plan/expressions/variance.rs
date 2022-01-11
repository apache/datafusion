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

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use crate::scalar::ScalarValue;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;

use super::{format_state_name, StatsType};

/// VAR and VAR_SAMP aggregate expression
#[derive(Debug)]
pub struct Variance {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
}

/// VAR_POP aggregate expression
#[derive(Debug)]
pub struct VariancePop {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
}

/// function return type of variance
pub(crate) fn variance_return_type(arg_type: &DataType) -> Result<DataType> {
    match arg_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => Ok(DataType::Float64),
        other => Err(DataFusionError::Plan(format!(
            "VARIANCE does not support {:?}",
            other
        ))),
    }
}

pub(crate) fn is_variance_support_arg_type(arg_type: &DataType) -> bool {
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

impl Variance {
    /// Create a new VARIANCE aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        // the result of variance just support FLOAT64 data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            expr,
        }
    }
}

impl AggregateExpr for Variance {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Float64, true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(VarianceAccumulator::try_new(StatsType::Sample)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                &format_state_name(&self.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "mean"),
                DataType::Float64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "m2"),
                DataType::Float64,
                true,
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

impl VariancePop {
    /// Create a new VAR_POP aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        // the result of variance just support FLOAT64 data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            expr,
        }
    }
}

impl AggregateExpr for VariancePop {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Float64, true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(VarianceAccumulator::try_new(
            StatsType::Population,
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                &format_state_name(&self.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "mean"),
                DataType::Float64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "m2"),
                DataType::Float64,
                true,
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

/// An accumulator to compute variance
/// The algrithm used is an online implementation and numerically stable. It is based on this paper:
/// Welford, B. P. (1962). "Note on a method for calculating corrected sums of squares and products".
/// Technometrics. 4 (3): 419–420. doi:10.2307/1266577. JSTOR 1266577.
///
/// The algorithm has been analyzed here:
/// Ling, Robert F. (1974). "Comparison of Several Algorithms for Computing Sample Means and Variances".
/// Journal of the American Statistical Association. 69 (348): 859–866. doi:10.2307/2286154. JSTOR 2286154.

#[derive(Debug)]
pub struct VarianceAccumulator {
    m2: ScalarValue,
    mean: ScalarValue,
    count: u64,
    stats_type: StatsType,
}

impl VarianceAccumulator {
    /// Creates a new `VarianceAccumulator`
    pub fn try_new(s_type: StatsType) -> Result<Self> {
        Ok(Self {
            m2: ScalarValue::from(0 as f64),
            mean: ScalarValue::from(0 as f64),
            count: 0,
            stats_type: s_type,
        })
    }

    pub fn get_count(&self) -> u64 {
        self.count
    }

    pub fn get_mean(&self) -> ScalarValue {
        self.mean.clone()
    }

    pub fn get_m2(&self) -> ScalarValue {
        self.m2.clone()
    }
}

impl Accumulator for VarianceAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            self.mean.clone(),
            self.m2.clone(),
        ])
    }

    fn update(&mut self, values: &[ScalarValue]) -> Result<()> {
        let values = &values[0];
        let is_empty = values.is_null();

        if !is_empty {
            let new_count = self.count + 1;
            let delta1 = ScalarValue::add(values, &self.mean.arithmetic_negate())?;
            let new_mean = ScalarValue::add(
                &ScalarValue::div(&delta1, &ScalarValue::from(new_count as f64))?,
                &self.mean,
            )?;
            let delta2 = ScalarValue::add(values, &new_mean.arithmetic_negate())?;
            let tmp = ScalarValue::mul(&delta1, &delta2)?;

            let new_m2 = ScalarValue::add(&self.m2, &tmp)?;
            self.count += 1;
            self.mean = new_mean;
            self.m2 = new_m2;
        }

        Ok(())
    }

    fn merge(&mut self, states: &[ScalarValue]) -> Result<()> {
        let count = &states[0];
        let mean = &states[1];
        let m2 = &states[2];
        let mut new_count: u64 = self.count;

        // counts are summed
        if let ScalarValue::UInt64(Some(c)) = count {
            if *c == 0_u64 {
                return Ok(());
            }

            if self.count == 0 {
                self.count = *c;
                self.mean = mean.clone();
                self.m2 = m2.clone();
                return Ok(());
            }
            new_count += c
        } else {
            unreachable!()
        };

        let new_mean = ScalarValue::div(
            &ScalarValue::add(&self.mean, mean)?,
            &ScalarValue::from(2_f64),
        )?;
        let delta = ScalarValue::add(&mean.arithmetic_negate(), &self.mean)?;
        let delta_sqrt = ScalarValue::mul(&delta, &delta)?;
        let new_m2 = ScalarValue::add(
            &ScalarValue::add(
                &ScalarValue::mul(
                    &delta_sqrt,
                    &ScalarValue::div(
                        &ScalarValue::mul(&ScalarValue::from(self.count), count)?,
                        &ScalarValue::from(new_count as f64),
                    )?,
                )?,
                &self.m2,
            )?,
            m2,
        )?;

        self.count = new_count;
        self.mean = new_mean;
        self.m2 = new_m2;

        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let count = match self.stats_type {
            StatsType::Population => self.count,
            StatsType::Sample => {
                if self.count > 0 {
                    self.count - 1
                } else {
                    self.count
                }
            }
        };

        if count <= 1 {
            return Err(DataFusionError::Internal(
                "At least two values are needed to calculate variance".to_string(),
            ));
        }

        match self.m2 {
            ScalarValue::Float64(e) => {
                if self.count == 0 {
                    Ok(ScalarValue::Float64(None))
                } else {
                    Ok(ScalarValue::Float64(e.map(|f| f / count as f64)))
                }
            }
            _ => Err(DataFusionError::Internal(
                "M2 should be f64 for variance".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::expressions::col;
    use crate::{error::Result, generic_test_op};
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};

    #[test]
    fn variance_f64_1() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1_f64, 2_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            VariancePop,
            ScalarValue::from(0.25_f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_f64_2() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            VariancePop,
            ScalarValue::from(2_f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_f64_3() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Variance,
            ScalarValue::from(2.5_f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_f64_4() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1.1_f64, 2_f64, 3_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Variance,
            ScalarValue::from(0.9033333333333333_f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            VariancePop,
            ScalarValue::from(2_f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(
            a,
            DataType::UInt32,
            VariancePop,
            ScalarValue::from(2.0f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_f32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(
            a,
            DataType::Float32,
            VariancePop,
            ScalarValue::from(2_f64),
            DataType::Float64
        )
    }

    #[test]
    fn test_variance_return_data_type() -> Result<()> {
        let data_type = DataType::Float64;
        let result_type = variance_return_type(&data_type)?;
        assert_eq!(DataType::Float64, result_type);

        let data_type = DataType::Decimal(36, 10);
        assert!(variance_return_type(&data_type).is_err());
        Ok(())
    }

    #[test]
    fn test_variance_1_input() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1_f64]));
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;

        let agg = Arc::new(Variance::new(
            col("a", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));
        let actual = aggregate(&batch, agg);
        assert!(actual.is_err());

        Ok(())
    }

    #[test]
    fn variance_i32_with_nulls() -> Result<()> {
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
            VariancePop,
            ScalarValue::from(2.1875f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;

        let agg = Arc::new(Variance::new(
            col("a", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));
        let actual = aggregate(&batch, agg);
        assert!(actual.is_err());

        Ok(())
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
