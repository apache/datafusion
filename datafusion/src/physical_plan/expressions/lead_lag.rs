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

//! Defines physical expression for `lead` and `lag` that can evaluated
//! at runtime during query execution

use crate::error::{DataFusionError, Result};
use crate::physical_plan::window_functions::PartitionEvaluator;
use crate::physical_plan::{window_functions::BuiltInWindowFunctionExpr, PhysicalExpr};
use arrow::array::ArrayRef;
use arrow::compute::kernels::window::shift;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

/// window shift expression
#[derive(Debug)]
pub struct WindowShift {
    name: String,
    data_type: DataType,
    shift_offset: i64,
    expr: Arc<dyn PhysicalExpr>,
}

/// lead() window function
pub fn lead(
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
) -> WindowShift {
    WindowShift {
        name,
        data_type,
        shift_offset: -1,
        expr,
    }
}

/// lag() window function
pub fn lag(
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
) -> WindowShift {
    WindowShift {
        name,
        data_type,
        shift_offset: 1,
        expr,
    }
}

impl BuiltInWindowFunctionExpr for WindowShift {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        let nullable = true;
        Ok(Field::new(&self.name, self.data_type.clone(), nullable))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn create_evaluator(
        &self,
        batch: &RecordBatch,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let values = self
            .expressions()
            .iter()
            .map(|e| e.evaluate(batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::new(WindowShiftEvaluator {
            shift_offset: self.shift_offset,
            values,
        }))
    }
}

pub(crate) struct WindowShiftEvaluator {
    shift_offset: i64,
    values: Vec<ArrayRef>,
}

impl PartitionEvaluator for WindowShiftEvaluator {
    fn evaluate_partition(&self, partition: Range<usize>) -> Result<ArrayRef> {
        let value = &self.values[0];
        let value = value.slice(partition.start, partition.end - partition.start);
        shift(value.as_ref(), self.shift_offset).map_err(DataFusionError::ArrowError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::physical_plan::expressions::Column;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};

    fn test_i32_result(expr: WindowShift, expected: Int32Array) -> Result<()> {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, -2, 3, -4, 5, -6, 7, 8]));
        let values = vec![arr];
        let schema = Schema::new(vec![Field::new("arr", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), values.clone())?;
        let result = expr.create_evaluator(&batch)?.evaluate(vec![0..8])?;
        assert_eq!(1, result.len());
        let result = result[0].as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(expected, *result);
        Ok(())
    }

    #[test]
    fn lead_lag_window_shift() -> Result<()> {
        test_i32_result(
            lead(
                "lead".to_owned(),
                DataType::Float32,
                Arc::new(Column::new("c3", 0)),
            ),
            vec![
                Some(-2),
                Some(3),
                Some(-4),
                Some(5),
                Some(-6),
                Some(7),
                Some(8),
                None,
            ]
            .iter()
            .collect::<Int32Array>(),
        )?;

        test_i32_result(
            lag(
                "lead".to_owned(),
                DataType::Float32,
                Arc::new(Column::new("c3", 0)),
            ),
            vec![
                None,
                Some(1),
                Some(-2),
                Some(3),
                Some(-4),
                Some(5),
                Some(-6),
                Some(7),
            ]
            .iter()
            .collect::<Int32Array>(),
        )?;
        Ok(())
    }
}
