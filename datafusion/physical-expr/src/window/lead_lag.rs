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

use crate::window::partition_evaluator::PartitionEvaluator;
use crate::window::BuiltInWindowFunctionExpr;
use crate::PhysicalExpr;
use arrow::array::ArrayRef;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use std::any::Any;
use std::ops::Neg;
use std::ops::Range;
use std::sync::Arc;

/// window shift expression
#[derive(Debug)]
pub struct WindowShift {
    name: String,
    data_type: DataType,
    shift_offset: i64,
    expr: Arc<dyn PhysicalExpr>,
    default_value: Option<ScalarValue>,
}

/// lead() window function
pub fn lead(
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    shift_offset: Option<i64>,
    default_value: Option<ScalarValue>,
) -> WindowShift {
    WindowShift {
        name,
        data_type,
        shift_offset: shift_offset.map(|v| v.neg()).unwrap_or(-1),
        expr,
        default_value,
    }
}

/// lag() window function
pub fn lag(
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    shift_offset: Option<i64>,
    default_value: Option<ScalarValue>,
) -> WindowShift {
    WindowShift {
        name,
        data_type,
        shift_offset: shift_offset.unwrap_or(1),
        expr,
        default_value,
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
            default_value: self.default_value.clone(),
        }))
    }
}

pub(crate) struct WindowShiftEvaluator {
    shift_offset: i64,
    values: Vec<ArrayRef>,
    default_value: Option<ScalarValue>,
}

fn create_empty_array(
    value: &Option<ScalarValue>,
    data_type: &DataType,
    size: usize,
) -> Result<ArrayRef> {
    use arrow::array::new_null_array;
    let array = value
        .as_ref()
        .map(|scalar| scalar.to_array_of_size(size))
        .unwrap_or_else(|| new_null_array(data_type, size));
    if array.data_type() != data_type {
        cast(&array, data_type).map_err(DataFusionError::ArrowError)
    } else {
        Ok(array)
    }
}

// TODO: change the original arrow::compute::kernels::window::shift impl to support an optional default value
fn shift_with_default_value(
    array: &ArrayRef,
    offset: i64,
    value: &Option<ScalarValue>,
) -> Result<ArrayRef> {
    use arrow::compute::concat;

    let value_len = array.len() as i64;
    if offset == 0 {
        Ok(arrow::array::make_array(array.data_ref().clone()))
    } else if offset == i64::MIN || offset.abs() >= value_len {
        create_empty_array(value, array.data_type(), array.len())
    } else {
        let slice_offset = (-offset).clamp(0, value_len) as usize;
        let length = array.len() - offset.unsigned_abs() as usize;
        let slice = array.slice(slice_offset, length);

        // Generate array with remaining `null` items
        let nulls = offset.unsigned_abs() as usize;
        let default_values = create_empty_array(value, slice.data_type(), nulls)?;
        // Concatenate both arrays, add nulls after if shift > 0 else before
        if offset > 0 {
            concat(&[default_values.as_ref(), slice.as_ref()])
                .map_err(DataFusionError::ArrowError)
        } else {
            concat(&[slice.as_ref(), default_values.as_ref()])
                .map_err(DataFusionError::ArrowError)
        }
    }
}

impl PartitionEvaluator for WindowShiftEvaluator {
    fn evaluate_partition(&self, partition: Range<usize>) -> Result<ArrayRef> {
        let value = &self.values[0];
        let value = value.slice(partition.start, partition.end - partition.start);
        shift_with_default_value(&value, self.shift_offset, &self.default_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Column;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::Result;

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
                None,
                None,
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
                None,
                None,
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

        test_i32_result(
            lag(
                "lead".to_owned(),
                DataType::Int32,
                Arc::new(Column::new("c3", 0)),
                None,
                Some(ScalarValue::Int32(Some(100))),
            ),
            vec![
                Some(100),
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
