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

//! Defines physical expression for `rank`, `dense_rank`, and `percent_rank` that can evaluated
//! at runtime during query execution

use crate::window::partition_evaluator::PartitionEvaluator;
use crate::window::BuiltInWindowFunctionExpr;
use crate::PhysicalExpr;
use arrow::array::ArrayRef;
use arrow::array::{Float64Array, UInt64Array};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use std::any::Any;
use std::iter;
use std::ops::Range;
use std::sync::Arc;

/// Rank calculates the rank in the window function with order by
#[derive(Debug)]
pub struct Rank {
    name: String,
    rank_type: RankType,
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum RankType {
    Basic,
    Dense,
    Percent,
}

/// Create a rank window function
pub fn rank(name: String) -> Rank {
    Rank {
        name,
        rank_type: RankType::Basic,
    }
}

/// Create a dense rank window function
pub fn dense_rank(name: String) -> Rank {
    Rank {
        name,
        rank_type: RankType::Dense,
    }
}

/// Create a percent rank window function
pub fn percent_rank(name: String) -> Rank {
    Rank {
        name,
        rank_type: RankType::Percent,
    }
}

impl BuiltInWindowFunctionExpr for Rank {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        let nullable = false;
        let data_type = match self.rank_type {
            RankType::Basic | RankType::Dense => DataType::UInt64,
            RankType::Percent => DataType::Float64,
        };
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
        Ok(Box::new(RankEvaluator {
            rank_type: self.rank_type,
        }))
    }
}

pub(crate) struct RankEvaluator {
    rank_type: RankType,
}

impl PartitionEvaluator for RankEvaluator {
    fn include_rank(&self) -> bool {
        true
    }

    fn evaluate_partition(&self, _partition: Range<usize>) -> Result<ArrayRef> {
        unreachable!("rank evaluation must be called with evaluate_partition_with_rank")
    }

    fn evaluate_partition_with_rank(
        &self,
        partition: Range<usize>,
        ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        // see https://www.postgresql.org/docs/current/functions-window.html
        let result: ArrayRef = match self.rank_type {
            RankType::Dense => Arc::new(UInt64Array::from_iter_values(
                ranks_in_partition
                    .iter()
                    .zip(1u64..)
                    .flat_map(|(range, rank)| {
                        let len = range.end - range.start;
                        iter::repeat(rank).take(len)
                    }),
            )),
            RankType::Percent => {
                // Returns the relative rank of the current row, that is (rank - 1) / (total partition rows - 1). The value thus ranges from 0 to 1 inclusive.
                let denominator = (partition.end - partition.start) as f64;
                Arc::new(Float64Array::from_iter_values(
                    ranks_in_partition
                        .iter()
                        .scan(0_u64, |acc, range| {
                            let len = range.end - range.start;
                            let value = (*acc as f64) / (denominator - 1.0).max(1.0);
                            let result = iter::repeat(value).take(len);
                            *acc += len as u64;
                            Some(result)
                        })
                        .flatten(),
                ))
            }
            RankType::Basic => Arc::new(UInt64Array::from_iter_values(
                ranks_in_partition
                    .iter()
                    .scan(1_u64, |acc, range| {
                        let len = range.end - range.start;
                        let result = iter::repeat(*acc).take(len);
                        *acc += len as u64;
                        Some(result)
                    })
                    .flatten(),
            )),
        };
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{array::*, datatypes::*};

    fn test_with_rank(expr: &Rank, expected: Vec<u64>) -> Result<()> {
        test_i32_result(
            expr,
            vec![-2, -2, 1, 3, 3, 3, 7, 8],
            vec![0..2, 2..3, 3..6, 6..7, 7..8],
            expected,
        )
    }

    fn test_without_rank(expr: &Rank, expected: Vec<u64>) -> Result<()> {
        test_i32_result(expr, vec![-2, -2, 1, 3, 3, 3, 7, 8], vec![0..8], expected)
    }

    fn test_f64_result(
        expr: &Rank,
        data: Vec<i32>,
        range: Range<usize>,
        ranks: Vec<Range<usize>>,
        expected: Vec<f64>,
    ) -> Result<()> {
        let arr: ArrayRef = Arc::new(Int32Array::from(data));
        let values = vec![arr];
        let schema = Schema::new(vec![Field::new("arr", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), values.clone())?;
        let result = expr
            .create_evaluator(&batch)?
            .evaluate_with_rank(vec![range], ranks)?;
        assert_eq!(1, result.len());
        let result = result[0].as_any().downcast_ref::<Float64Array>().unwrap();
        let result = result.values();
        assert_eq!(expected, result);
        Ok(())
    }

    fn test_i32_result(
        expr: &Rank,
        data: Vec<i32>,
        ranks: Vec<Range<usize>>,
        expected: Vec<u64>,
    ) -> Result<()> {
        let arr: ArrayRef = Arc::new(Int32Array::from(data));
        let values = vec![arr];
        let schema = Schema::new(vec![Field::new("arr", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), values.clone())?;
        let result = expr
            .create_evaluator(&batch)?
            .evaluate_with_rank(vec![0..8], ranks)?;
        assert_eq!(1, result.len());
        let result = result[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        let result = result.values();
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_dense_rank() -> Result<()> {
        let r = dense_rank("arr".into());
        test_without_rank(&r, vec![1; 8])?;
        test_with_rank(&r, vec![1, 1, 2, 3, 3, 3, 4, 5])?;
        Ok(())
    }

    #[test]
    fn test_rank() -> Result<()> {
        let r = rank("arr".into());
        test_without_rank(&r, vec![1; 8])?;
        test_with_rank(&r, vec![1, 1, 3, 4, 4, 4, 7, 8])?;
        Ok(())
    }

    #[test]
    fn test_percent_rank() -> Result<()> {
        let r = percent_rank("arr".into());

        // empty case
        let expected = vec![0.0; 0];
        test_f64_result(&r, vec![0; 0], 0..0, vec![0..0; 0], expected)?;

        // singleton case
        let expected = vec![0.0];
        test_f64_result(&r, vec![13], 0..1, vec![0..1], expected)?;

        // uniform case
        let expected = vec![0.0; 7];
        test_f64_result(&r, vec![4; 7], 0..7, vec![0..7], expected)?;

        // non-trivial case
        let expected = vec![0.0, 0.0, 0.0, 0.5, 0.5, 0.5, 0.5];
        test_f64_result(
            &r,
            vec![1, 1, 1, 2, 2, 2, 2],
            0..7,
            vec![0..3, 3..7],
            expected,
        )?;

        Ok(())
    }
}
