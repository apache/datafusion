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

//! Defines physical expressions for `first_value`, `last_value`, and `nth_value`
//! that can evaluated at runtime during query execution

use crate::window::partition_evaluator::PartitionEvaluator;
use crate::window::window_expr::{BuiltinWindowState, NthValueState};
use crate::window::{BuiltInWindowFunctionExpr, WindowAggState};
use crate::PhysicalExpr;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field};
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

/// nth_value kind
#[derive(Debug, Copy, Clone)]
pub enum NthValueKind {
    First,
    Last,
    Nth(u32),
}

/// nth_value expression
#[derive(Debug)]
pub struct NthValue {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    kind: NthValueKind,
}

impl NthValue {
    /// Create a new FIRST_VALUE window aggregate function
    pub fn first(
        name: impl Into<String>,
        expr: Arc<dyn PhysicalExpr>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
            kind: NthValueKind::First,
        }
    }

    /// Create a new LAST_VALUE window aggregate function
    pub fn last(
        name: impl Into<String>,
        expr: Arc<dyn PhysicalExpr>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
            kind: NthValueKind::Last,
        }
    }

    /// Create a new NTH_VALUE window aggregate function
    pub fn nth(
        name: impl Into<String>,
        expr: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        n: u32,
    ) -> Result<Self> {
        match n {
            0 => Err(DataFusionError::Execution(
                "nth_value expect n to be > 0".to_owned(),
            )),
            _ => Ok(Self {
                name: name.into(),
                expr,
                data_type,
                kind: NthValueKind::Nth(n),
            }),
        }
    }

    /// Get nth_value kind
    pub fn get_kind(&self) -> NthValueKind {
        self.kind
    }
}

impl BuiltInWindowFunctionExpr for NthValue {
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
        Ok(Box::new(NthValueEvaluator {
            state: NthValueState::default(),
            kind: self.kind,
        }))
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    fn uses_window_frame(&self) -> bool {
        true
    }

    fn reverse_expr(&self) -> Option<Arc<dyn BuiltInWindowFunctionExpr>> {
        let reversed_kind = match self.kind {
            NthValueKind::First => NthValueKind::Last,
            NthValueKind::Last => NthValueKind::First,
            NthValueKind::Nth(_) => return None,
        };
        Some(Arc::new(Self {
            name: self.name.clone(),
            expr: self.expr.clone(),
            data_type: self.data_type.clone(),
            kind: reversed_kind,
        }))
    }
}

/// Value evaluator for nth_value functions
#[derive(Debug)]
pub(crate) struct NthValueEvaluator {
    state: NthValueState,
    kind: NthValueKind,
}

impl PartitionEvaluator for NthValueEvaluator {
    fn state(&self) -> Result<BuiltinWindowState> {
        // If we do not use state we just return Default
        Ok(BuiltinWindowState::NthValue(self.state.clone()))
    }

    fn update_state(
        &mut self,
        state: &WindowAggState,
        _range_columns: &[ArrayRef],
        _sort_partition_points: &[Range<usize>],
    ) -> Result<()> {
        // If we do not use state, update_state does nothing
        self.state.range = state.window_frame_range.clone();
        Ok(())
    }

    fn evaluate_stateful(&mut self, values: &[ArrayRef]) -> Result<ScalarValue> {
        self.evaluate_inside_range(values, self.state.range.clone())
    }

    fn evaluate_inside_range(
        &self,
        values: &[ArrayRef],
        range: Range<usize>,
    ) -> Result<ScalarValue> {
        // FIRST_VALUE, LAST_VALUE, NTH_VALUE window functions take single column, values will have size 1
        let arr = &values[0];
        let n_range = range.end - range.start;
        match self.kind {
            NthValueKind::First => ScalarValue::try_from_array(arr, range.start),
            NthValueKind::Last => ScalarValue::try_from_array(arr, range.end - 1),
            NthValueKind::Nth(n) => {
                // We are certain that n > 0.
                let index = (n as usize) - 1;
                if index >= n_range {
                    ScalarValue::try_from(arr.data_type())
                } else {
                    ScalarValue::try_from_array(arr, range.start + index)
                }
            }
        }
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

    fn test_i32_result(expr: NthValue, expected: Int32Array) -> Result<()> {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, -2, 3, -4, 5, -6, 7, 8]));
        let values = vec![arr];
        let schema = Schema::new(vec![Field::new("arr", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), values.clone())?;
        let mut ranges: Vec<Range<usize>> = vec![];
        for i in 0..8 {
            ranges.push(Range {
                start: 0,
                end: i + 1,
            })
        }
        let evaluator = expr.create_evaluator()?;
        let values = expr.evaluate_args(&batch)?;
        let result = ranges
            .into_iter()
            .map(|range| evaluator.evaluate_inside_range(&values, range))
            .into_iter()
            .collect::<Result<Vec<ScalarValue>>>()?;
        let result = ScalarValue::iter_to_array(result.into_iter())?;
        let result = as_int32_array(&result)?;
        assert_eq!(expected, *result);
        Ok(())
    }

    #[test]
    fn first_value() -> Result<()> {
        let first_value = NthValue::first(
            "first_value".to_owned(),
            Arc::new(Column::new("arr", 0)),
            DataType::Int32,
        );
        test_i32_result(first_value, Int32Array::from_iter_values(vec![1; 8]))?;
        Ok(())
    }

    #[test]
    fn last_value() -> Result<()> {
        let last_value = NthValue::last(
            "last_value".to_owned(),
            Arc::new(Column::new("arr", 0)),
            DataType::Int32,
        );
        test_i32_result(
            last_value,
            Int32Array::from(vec![
                Some(1),
                Some(-2),
                Some(3),
                Some(-4),
                Some(5),
                Some(-6),
                Some(7),
                Some(8),
            ]),
        )?;
        Ok(())
    }

    #[test]
    fn nth_value_1() -> Result<()> {
        let nth_value = NthValue::nth(
            "nth_value".to_owned(),
            Arc::new(Column::new("arr", 0)),
            DataType::Int32,
            1,
        )?;
        test_i32_result(nth_value, Int32Array::from_iter_values(vec![1; 8]))?;
        Ok(())
    }

    #[test]
    fn nth_value_2() -> Result<()> {
        let nth_value = NthValue::nth(
            "nth_value".to_owned(),
            Arc::new(Column::new("arr", 0)),
            DataType::Int32,
            2,
        )?;
        test_i32_result(
            nth_value,
            Int32Array::from(vec![
                None,
                Some(-2),
                Some(-2),
                Some(-2),
                Some(-2),
                Some(-2),
                Some(-2),
                Some(-2),
            ]),
        )?;
        Ok(())
    }
}
