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

//! Defines physical expression for `cume_dist` that can evaluated
//! at runtime during query execution

use crate::window::partition_evaluator::PartitionEvaluator;
use crate::window::BuiltInWindowFunctionExpr;
use crate::PhysicalExpr;
use arrow::array::ArrayRef;
use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use std::any::Any;
use std::iter;
use std::ops::Range;
use std::sync::Arc;

/// CumeDist calculates the cume_dist in the window function with order by
#[derive(Debug)]
pub struct CumeDist {
    name: String,
}

/// Create a cume_dist window function
pub fn cume_dist(name: String) -> CumeDist {
    CumeDist { name }
}

impl BuiltInWindowFunctionExpr for CumeDist {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        let nullable = false;
        let data_type = DataType::Float64;
        Ok(Field::new(self.name(), data_type, nullable))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn create_evaluator(
        &self,
        _batch: &RecordBatch,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(CumeDistEvaluator {}))
    }
}

pub(crate) struct CumeDistEvaluator;

impl PartitionEvaluator for CumeDistEvaluator {
    fn include_rank(&self) -> bool {
        true
    }

    fn evaluate_partition(&self, _partition: Range<usize>) -> Result<ArrayRef> {
        unreachable!(
            "cume_dist evaluation must be called with evaluate_partition_with_rank"
        )
    }

    fn evaluate_partition_with_rank(
        &self,
        partition: Range<usize>,
        ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        let scaler = (partition.end - partition.start) as f64;
        let result = Float64Array::from_iter_values(
            ranks_in_partition
                .iter()
                .scan(0_u64, |acc, range| {
                    let len = range.end - range.start;
                    *acc += len as u64;
                    let value: f64 = (*acc as f64) / scaler;
                    let result = iter::repeat(value).take(len);
                    Some(result)
                })
                .flatten(),
        );
        Ok(Arc::new(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{array::*, datatypes::*};

    fn test_i32_result(
        expr: &CumeDist,
        data: Vec<i32>,
        partition: Range<usize>,
        ranks: Vec<Range<usize>>,
        expected: Vec<f64>,
    ) -> Result<()> {
        let arr: ArrayRef = Arc::new(Int32Array::from(data));
        let values = vec![arr];
        let schema = Schema::new(vec![Field::new("arr", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), values.clone())?;
        let result = expr
            .create_evaluator(&batch)?
            .evaluate_with_rank(vec![partition], ranks)?;
        assert_eq!(1, result.len());
        let result = result[0].as_any().downcast_ref::<Float64Array>().unwrap();
        let result = result.values();
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_cume_dist() -> Result<()> {
        let r = cume_dist("arr".into());

        let expected = vec![0.0; 0];
        test_i32_result(&r, vec![], 0..0, vec![], expected)?;

        let expected = vec![1.0; 1];
        test_i32_result(&r, vec![20; 1], 0..1, vec![0..1], expected)?;

        let expected = vec![1.0; 2];
        test_i32_result(&r, vec![20; 2], 0..2, vec![0..2], expected)?;

        let expected = vec![0.5, 0.5, 1.0, 1.0];
        test_i32_result(&r, vec![1, 1, 2, 2], 0..4, vec![0..2, 2..4], expected)?;

        let expected = vec![0.25, 0.5, 0.75, 1.0];
        test_i32_result(
            &r,
            vec![1, 2, 4, 5],
            0..4,
            vec![0..1, 1..2, 2..3, 3..4],
            expected,
        )?;

        Ok(())
    }
}
