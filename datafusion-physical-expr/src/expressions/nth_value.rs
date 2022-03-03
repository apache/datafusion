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
use crate::window::BuiltInWindowFunctionExpr;
use crate::PhysicalExpr;
use arrow::array::{new_null_array, ArrayRef};
use arrow::compute::kernels::window::shift;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use std::any::Any;
use std::iter;
use std::ops::Range;
use std::sync::Arc;

/// nth_value kind
#[derive(Debug, Copy, Clone)]
enum NthValueKind {
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
        Ok(Box::new(NthValueEvaluator {
            kind: self.kind,
            values,
        }))
    }
}

/// Value evaluator for nth_value functions
pub(crate) struct NthValueEvaluator {
    kind: NthValueKind,
    values: Vec<ArrayRef>,
}

impl PartitionEvaluator for NthValueEvaluator {
    fn include_rank(&self) -> bool {
        true
    }

    fn evaluate_partition(&self, _partition: Range<usize>) -> Result<ArrayRef> {
        unreachable!("first, last, and nth_value evaluation must be called with evaluate_partition_with_rank")
    }

    fn evaluate_partition_with_rank(
        &self,
        partition: Range<usize>,
        ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        let arr = &self.values[0];
        let num_rows = partition.end - partition.start;
        match self.kind {
            NthValueKind::First => {
                let value = ScalarValue::try_from_array(arr, partition.start)?;
                Ok(value.to_array_of_size(num_rows))
            }
            NthValueKind::Last => {
                // because the default window frame is between unbounded preceding and current
                // row with peer evaluation, hence the last rows expands until the end of the peers
                let values = ranks_in_partition
                    .iter()
                    .map(|range| {
                        let len = range.end - range.start;
                        let value = ScalarValue::try_from_array(arr, range.end - 1)?;
                        Ok(iter::repeat(value).take(len))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .flatten();
                ScalarValue::iter_to_array(values)
            }
            NthValueKind::Nth(n) => {
                let index = (n as usize) - 1;
                if index >= num_rows {
                    Ok(new_null_array(arr.data_type(), num_rows))
                } else {
                    let value =
                        ScalarValue::try_from_array(arr, partition.start + index)?;
                    let arr = value.to_array_of_size(num_rows);
                    // because the default window frame is between unbounded preceding and current
                    // row, hence the shift because for values with indices < index they should be
                    // null. This changes when window frames other than default is implemented
                    shift(arr.as_ref(), index as i64).map_err(DataFusionError::ArrowError)
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
    use datafusion_common::Result;

    fn test_i32_result(expr: NthValue, expected: Int32Array) -> Result<()> {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, -2, 3, -4, 5, -6, 7, 8]));
        let values = vec![arr];
        let schema = Schema::new(vec![Field::new("arr", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), values.clone())?;
        let result = expr
            .create_evaluator(&batch)?
            .evaluate_with_rank(vec![0..8], vec![0..8])?;
        assert_eq!(1, result.len());
        let result = result[0].as_any().downcast_ref::<Int32Array>().unwrap();
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
        test_i32_result(last_value, Int32Array::from_iter_values(vec![8; 8]))?;
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
