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

use crate::window::BuiltInWindowFunctionExpr;
use crate::PhysicalExpr;
use arrow::array::ArrayRef;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field};
use datafusion_common::{arrow_datafusion_err, ScalarValue};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_expr::PartitionEvaluator;
use std::any::Any;
use std::cmp::min;
use std::ops::{Neg, Range};
use std::sync::Arc;

/// window shift expression
#[derive(Debug)]
pub struct WindowShift {
    name: String,
    /// Output data type
    data_type: DataType,
    shift_offset: i64,
    expr: Arc<dyn PhysicalExpr>,
    default_value: Option<ScalarValue>,
}

impl WindowShift {
    /// Get shift_offset of window shift expression
    pub fn get_shift_offset(&self) -> i64 {
        self.shift_offset
    }

    /// Get the default_value for window shift expression.
    pub fn get_default_value(&self) -> Option<ScalarValue> {
        self.default_value.clone()
    }
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

    fn create_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(WindowShiftEvaluator {
            shift_offset: self.shift_offset,
            default_value: self.default_value.clone(),
        }))
    }

    fn reverse_expr(&self) -> Option<Arc<dyn BuiltInWindowFunctionExpr>> {
        Some(Arc::new(Self {
            name: self.name.clone(),
            data_type: self.data_type.clone(),
            shift_offset: -self.shift_offset,
            expr: self.expr.clone(),
            default_value: self.default_value.clone(),
        }))
    }
}

#[derive(Debug)]
pub(crate) struct WindowShiftEvaluator {
    shift_offset: i64,
    default_value: Option<ScalarValue>,
}

fn create_empty_array(
    value: Option<&ScalarValue>,
    data_type: &DataType,
    size: usize,
) -> Result<ArrayRef> {
    use arrow::array::new_null_array;
    let array = value
        .as_ref()
        .map(|scalar| scalar.to_array_of_size(size))
        .transpose()?
        .unwrap_or_else(|| new_null_array(data_type, size));
    if array.data_type() != data_type {
        cast(&array, data_type).map_err(|e| arrow_datafusion_err!(e))
    } else {
        Ok(array)
    }
}

// TODO: change the original arrow::compute::kernels::window::shift impl to support an optional default value
fn shift_with_default_value(
    array: &ArrayRef,
    offset: i64,
    value: Option<&ScalarValue>,
) -> Result<ArrayRef> {
    use arrow::compute::concat;

    let value_len = array.len() as i64;
    if offset == 0 {
        Ok(array.clone())
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
                .map_err(|e| arrow_datafusion_err!(e))
        } else {
            concat(&[slice.as_ref(), default_values.as_ref()])
                .map_err(|e| arrow_datafusion_err!(e))
        }
    }
}

impl PartitionEvaluator for WindowShiftEvaluator {
    fn get_range(&self, idx: usize, n_rows: usize) -> Result<Range<usize>> {
        if self.shift_offset > 0 {
            let offset = self.shift_offset as usize;
            let start = idx.saturating_sub(offset);
            let end = idx + 1;
            Ok(Range { start, end })
        } else {
            let offset = (-self.shift_offset) as usize;
            let end = min(idx + offset, n_rows);
            Ok(Range { start: idx, end })
        }
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        let array = &values[0];
        let dtype = array.data_type();
        // LAG mode
        let idx = if self.shift_offset > 0 {
            range.end as i64 - self.shift_offset - 1
        } else {
            // LEAD mode
            range.start as i64 - self.shift_offset
        };

        if idx < 0 || idx as usize >= array.len() {
            get_default_value(self.default_value.as_ref(), dtype)
        } else {
            ScalarValue::try_from_array(array, idx as usize)
        }
    }

    fn evaluate_all(
        &mut self,
        values: &[ArrayRef],
        _num_rows: usize,
    ) -> Result<ArrayRef> {
        // LEAD, LAG window functions take single column, values will have size 1
        let value = &values[0];
        shift_with_default_value(value, self.shift_offset, self.default_value.as_ref())
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }
}

fn get_default_value(
    default_value: Option<&ScalarValue>,
    dtype: &DataType,
) -> Result<ScalarValue> {
    if let Some(value) = default_value {
        if let ScalarValue::Int64(Some(val)) = value {
            ScalarValue::try_from_string(val.to_string(), dtype)
        } else {
            internal_err!("Expects default value to have Int64 type")
        }
    } else {
        Ok(ScalarValue::try_from(dtype)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Column;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::cast::as_int32_array;
    use datafusion_common::Result;

    fn test_i32_result(expr: WindowShift, expected: Int32Array) -> Result<()> {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, -2, 3, -4, 5, -6, 7, 8]));
        let values = vec![arr];
        let schema = Schema::new(vec![Field::new("arr", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), values.clone())?;
        let values = expr.evaluate_args(&batch)?;
        let result = expr
            .create_evaluator()?
            .evaluate_all(&values, batch.num_rows())?;
        let result = as_int32_array(&result)?;
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
            [
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
            [
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
            [
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
