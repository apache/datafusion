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

//! Defines physical expressions for `FIRST_VALUE`, `LAST_VALUE`, and `NTH_VALUE`
//! functions that can be evaluated at run time during query execution.

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use crate::window::window_expr::{NthValueKind, NthValueState};
use crate::window::BuiltInWindowFunctionExpr;
use crate::PhysicalExpr;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field};
use datafusion_common::{exec_err, ScalarValue};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::window_state::WindowAggState;
use datafusion_expr::PartitionEvaluator;

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
            0 => exec_err!("NTH_VALUE expects n to be non-zero"),
            _ => Ok(Self {
                name: name.into(),
                expr,
                data_type,
                kind: NthValueKind::Nth(n as i64),
            }),
        }
    }

    /// Get the NTH_VALUE kind
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
        let state = NthValueState {
            range: Default::default(),
            finalized_result: None,
            kind: self.kind,
        };
        Ok(Box::new(NthValueEvaluator { state }))
    }

    fn reverse_expr(&self) -> Option<Arc<dyn BuiltInWindowFunctionExpr>> {
        let reversed_kind = match self.kind {
            NthValueKind::First => NthValueKind::Last,
            NthValueKind::Last => NthValueKind::First,
            NthValueKind::Nth(idx) => NthValueKind::Nth(-idx),
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
}

impl PartitionEvaluator for NthValueEvaluator {
    /// When the window frame has a fixed beginning (e.g UNBOUNDED PRECEDING),
    /// for some functions such as FIRST_VALUE, LAST_VALUE and NTH_VALUE, we
    /// can memoize the result.  Once result is calculated, it will always stay
    /// same. Hence, we do not need to keep past data as we process the entire
    /// dataset.
    fn memoize(&mut self, state: &mut WindowAggState) -> Result<()> {
        let out = &state.out_col;
        let size = out.len();
        let mut buffer_size = 1;
        // Decide if we arrived at a final result yet:
        let (is_prunable, is_reverse_direction) = match self.state.kind {
            NthValueKind::First => {
                let n_range =
                    state.window_frame_range.end - state.window_frame_range.start;
                (n_range > 0 && size > 0, false)
            }
            NthValueKind::Last => (true, true),
            NthValueKind::Nth(n) => {
                let n_range =
                    state.window_frame_range.end - state.window_frame_range.start;
                #[allow(clippy::comparison_chain)]
                if n > 0 {
                    (n_range >= (n as usize) && size > (n as usize), false)
                } else if n < 0 {
                    let reverse_index = (-n) as usize;
                    buffer_size = reverse_index;
                    // Negative index represents reverse direction.
                    (n_range >= reverse_index, true)
                } else {
                    // The case n = 0 is not valid for the NTH_VALUE function.
                    unreachable!();
                }
            }
        };
        if is_prunable {
            if self.state.finalized_result.is_none() && !is_reverse_direction {
                let result = ScalarValue::try_from_array(out, size - 1)?;
                self.state.finalized_result = Some(result);
            }
            state.window_frame_range.start =
                state.window_frame_range.end.saturating_sub(buffer_size);
        }
        Ok(())
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        if let Some(ref result) = self.state.finalized_result {
            Ok(result.clone())
        } else {
            // FIRST_VALUE, LAST_VALUE, NTH_VALUE window functions take a single column, values will have size 1.
            let arr = &values[0];
            let n_range = range.end - range.start;
            if n_range == 0 {
                // We produce None if the window is empty.
                return ScalarValue::try_from(arr.data_type());
            }
            match self.state.kind {
                NthValueKind::First => ScalarValue::try_from_array(arr, range.start),
                NthValueKind::Last => ScalarValue::try_from_array(arr, range.end - 1),
                NthValueKind::Nth(n) => {
                    #[allow(clippy::comparison_chain)]
                    if n > 0 {
                        // SQL indices are not 0-based.
                        let index = (n as usize) - 1;
                        if index >= n_range {
                            // Outside the range, return NULL:
                            ScalarValue::try_from(arr.data_type())
                        } else {
                            ScalarValue::try_from_array(arr, range.start + index)
                        }
                    } else if n < 0 {
                        let reverse_index = (-n) as usize;
                        if n_range >= reverse_index {
                            ScalarValue::try_from_array(
                                arr,
                                range.start + n_range - reverse_index,
                            )
                        } else {
                            // Outside the range, return NULL:
                            ScalarValue::try_from(arr.data_type())
                        }
                    } else {
                        // The case n = 0 is not valid for the NTH_VALUE function.
                        unreachable!();
                    }
                }
            }
        }
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    fn uses_window_frame(&self) -> bool {
        true
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
        let mut evaluator = expr.create_evaluator()?;
        let values = expr.evaluate_args(&batch)?;
        let result = ranges
            .iter()
            .map(|range| evaluator.evaluate(&values, range))
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
        test_i32_result(first_value, Int32Array::from(vec![1; 8]))?;
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
        test_i32_result(nth_value, Int32Array::from(vec![1; 8]))?;
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
