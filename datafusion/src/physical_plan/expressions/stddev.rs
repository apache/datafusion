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
use crate::physical_plan::{
    expressions::variance::VarianceAccumulator, Accumulator, AggregateExpr, PhysicalExpr,
};
use crate::scalar::ScalarValue;
use arrow::{array::ArrayRef, datatypes::DataType, datatypes::Field};

use super::{format_state_name, StatsType};

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

/// function return type of standard deviation
pub(crate) fn stddev_return_type(arg_type: &DataType) -> Result<DataType> {
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
            "STDDEV does not support {:?}",
            other
        ))),
    }
}

pub(crate) fn is_stddev_support_arg_type(arg_type: &DataType) -> bool {
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
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.variance.get_count()),
            ScalarValue::from(self.variance.get_mean()),
            ScalarValue::from(self.variance.get_m2()),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.variance.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.variance.merge_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let variance = self.variance.evaluate()?;
        match variance {
            ScalarValue::Float64(e) => {
                if e == None {
                    Ok(ScalarValue::Float64(None))
                } else {
                    Ok(ScalarValue::Float64(e.map(|f| f.sqrt())))
                }
            }
            _ => Err(DataFusionError::Internal(
                "Variance should be f64".to_string(),
            )),
        }
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
    fn stddev_f64_1() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from_slice(&[1_f64, 2_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            StddevPop,
            ScalarValue::from(0.5_f64),
            DataType::Float64
        )
    }

    #[test]
    fn stddev_f64_2() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from_slice(&[1.1_f64, 2_f64, 3_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            StddevPop,
            ScalarValue::from(0.7760297817881877),
            DataType::Float64
        )
    }

    #[test]
    fn stddev_f64_3() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from_slice(&[
            1_f64, 2_f64, 3_f64, 4_f64, 5_f64,
        ]));
        generic_test_op!(
            a,
            DataType::Float64,
            StddevPop,
            ScalarValue::from(std::f64::consts::SQRT_2),
            DataType::Float64
        )
    }

    #[test]
    fn stddev_f64_4() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from_slice(&[1.1_f64, 2_f64, 3_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Stddev,
            ScalarValue::from(0.9504384952922168),
            DataType::Float64
        )
    }

    #[test]
    fn stddev_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            StddevPop,
            ScalarValue::from(std::f64::consts::SQRT_2),
            DataType::Float64
        )
    }

    #[test]
    fn stddev_u32() -> Result<()> {
        let a: ArrayRef = Arc::new(UInt32Array::from_slice(&[
            1_u32, 2_u32, 3_u32, 4_u32, 5_u32,
        ]));
        generic_test_op!(
            a,
            DataType::UInt32,
            StddevPop,
            ScalarValue::from(std::f64::consts::SQRT_2),
            DataType::Float64
        )
    }

    #[test]
    fn stddev_f32() -> Result<()> {
        let a: ArrayRef = Arc::new(Float32Array::from_slice(&[
            1_f32, 2_f32, 3_f32, 4_f32, 5_f32,
        ]));
        generic_test_op!(
            a,
            DataType::Float32,
            StddevPop,
            ScalarValue::from(std::f64::consts::SQRT_2),
            DataType::Float64
        )
    }

    #[test]
    fn test_stddev_return_data_type() -> Result<()> {
        let data_type = DataType::Float64;
        let result_type = stddev_return_type(&data_type)?;
        assert_eq!(DataType::Float64, result_type);

        let data_type = DataType::Decimal(36, 10);
        assert!(stddev_return_type(&data_type).is_err());
        Ok(())
    }

    #[test]
    fn test_stddev_1_input() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from_slice(&[1_f64]));
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;

        let agg = Arc::new(Stddev::new(
            col("a", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));
        let actual = aggregate(&batch, agg);
        assert!(actual.is_err());

        Ok(())
    }

    #[test]
    fn stddev_i32_with_nulls() -> Result<()> {
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
            StddevPop,
            ScalarValue::from(1.479019945774904),
            DataType::Float64
        )
    }

    #[test]
    fn stddev_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;

        let agg = Arc::new(Stddev::new(
            col("a", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));
        let actual = aggregate(&batch, agg);
        assert!(actual.is_err());

        Ok(())
    }

    #[test]
    fn stddev_f64_merge_1() -> Result<()> {
        let a = Arc::new(Float64Array::from_slice(&[1_f64, 2_f64, 3_f64]));
        let b = Arc::new(Float64Array::from_slice(&[4_f64, 5_f64]));

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
        let a = Arc::new(Float64Array::from_slice(&[
            1_f64, 2_f64, 3_f64, 4_f64, 5_f64,
        ]));
        let b = Arc::new(Float64Array::from(vec![None]));

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
            .map(|e| e.evaluate(batch1))
            .map(|r| r.map(|v| v.into_array(batch1.num_rows())))
            .collect::<Result<Vec<_>>>()?;
        let values2 = expr2
            .iter()
            .map(|e| e.evaluate(batch2))
            .map(|r| r.map(|v| v.into_array(batch2.num_rows())))
            .collect::<Result<Vec<_>>>()?;
        accum1.update_batch(&values1)?;
        accum2.update_batch(&values2)?;
        let state2 = accum2
            .state()?
            .iter()
            .map(|v| vec![v.clone()])
            .map(|x| ScalarValue::iter_to_array(x).unwrap())
            .collect::<Vec<_>>();
        accum1.merge_batch(&state2)?;
        accum1.evaluate()
    }
}
