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
use crate::window::window_expr::{BuiltinWindowState, RankState};
use crate::window::{BuiltInWindowFunctionExpr, WindowAggState};
use crate::PhysicalExpr;
use arrow::array::ArrayRef;
use arrow::array::{Float64Array, UInt64Array};
use arrow::datatypes::{DataType, Field};
use datafusion_common::{DataFusionError, Result, ScalarValue};
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

impl Rank {
    /// Get rank_type of the rank in window function with order by
    pub fn get_type(&self) -> RankType {
        self.rank_type
    }
}

#[derive(Debug, Copy, Clone)]
pub enum RankType {
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

    fn supports_bounded_execution(&self) -> bool {
        matches!(self.rank_type, RankType::Basic | RankType::Dense)
    }

    fn create_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(RankEvaluator {
            state: RankState::default(),
            rank_type: self.rank_type,
        }))
    }
}

#[derive(Debug)]
pub(crate) struct RankEvaluator {
    state: RankState,
    rank_type: RankType,
}

impl PartitionEvaluator for RankEvaluator {
    fn get_range(&self, state: &WindowAggState, _n_rows: usize) -> Result<Range<usize>> {
        Ok(Range {
            start: state.last_calculated_index,
            end: state.last_calculated_index + 1,
        })
    }

    fn state(&self) -> Result<BuiltinWindowState> {
        Ok(BuiltinWindowState::Rank(self.state.clone()))
    }

    fn update_state(
        &mut self,
        state: &WindowAggState,
        range_columns: &[ArrayRef],
        sort_partition_points: &[Range<usize>],
    ) -> Result<()> {
        // find range inside `sort_partition_points` containing `state.last_calculated_index`
        let chunk_idx = sort_partition_points
            .iter()
            .position(|elem| {
                elem.start <= state.last_calculated_index
                    && state.last_calculated_index < elem.end
            })
            .ok_or_else(|| DataFusionError::Execution("Expects sort_partition_points to contain state.last_calculated_index".to_string()))?;
        let chunk = &sort_partition_points[chunk_idx];
        let last_rank_data = range_columns
            .iter()
            .map(|c| ScalarValue::try_from_array(c, chunk.end - 1))
            .collect::<Result<Vec<_>>>()?;
        let empty = self.state.last_rank_data.is_empty();
        if empty || self.state.last_rank_data != last_rank_data {
            self.state.last_rank_data = last_rank_data;
            self.state.last_rank_boundary = state.offset_pruned_rows + chunk.start;
            self.state.n_rank = 1 + if empty { chunk_idx } else { self.state.n_rank };
        }
        Ok(())
    }

    /// evaluate window function result inside given range
    fn evaluate_stateful(&mut self, _values: &[ArrayRef]) -> Result<ScalarValue> {
        match self.rank_type {
            RankType::Basic => Ok(ScalarValue::UInt64(Some(
                self.state.last_rank_boundary as u64 + 1,
            ))),
            RankType::Dense => Ok(ScalarValue::UInt64(Some(self.state.n_rank as u64))),
            RankType::Percent => Err(DataFusionError::Execution(
                "Can not execute PERCENT_RANK in a streaming fashion".to_string(),
            )),
        }
    }

    fn include_rank(&self) -> bool {
        true
    }

    fn evaluate_with_rank(
        &self,
        num_rows: usize,
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
                let denominator = num_rows as f64;
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
    use datafusion_common::cast::{as_float64_array, as_uint64_array};

    fn test_with_rank(expr: &Rank, expected: Vec<u64>) -> Result<()> {
        test_i32_result(expr, vec![0..2, 2..3, 3..6, 6..7, 7..8], expected)
    }

    fn test_without_rank(expr: &Rank, expected: Vec<u64>) -> Result<()> {
        test_i32_result(expr, vec![0..8], expected)
    }

    fn test_f64_result(
        expr: &Rank,
        num_rows: usize,
        ranks: Vec<Range<usize>>,
        expected: Vec<f64>,
    ) -> Result<()> {
        let result = expr
            .create_evaluator()?
            .evaluate_with_rank(num_rows, &ranks)?;
        let result = as_float64_array(&result)?;
        let result = result.values();
        assert_eq!(expected, result);
        Ok(())
    }

    fn test_i32_result(
        expr: &Rank,
        ranks: Vec<Range<usize>>,
        expected: Vec<u64>,
    ) -> Result<()> {
        let result = expr.create_evaluator()?.evaluate_with_rank(8, &ranks)?;
        let result = as_uint64_array(&result)?;
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
        test_f64_result(&r, 0, vec![0..0; 0], expected)?;

        // singleton case
        let expected = vec![0.0];
        test_f64_result(&r, 1, vec![0..1], expected)?;

        // uniform case
        let expected = vec![0.0; 7];
        test_f64_result(&r, 7, vec![0..7], expected)?;

        // non-trivial case
        let expected = vec![0.0, 0.0, 0.0, 0.5, 0.5, 0.5, 0.5];
        test_f64_result(&r, 7, vec![0..3, 3..7], expected)?;

        Ok(())
    }
}
