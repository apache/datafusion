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

use crate::expressions::Column;
use crate::window::window_expr::RankState;
use crate::window::BuiltInWindowFunctionExpr;
use crate::{PhysicalExpr, PhysicalSortExpr};

use arrow::array::ArrayRef;
use arrow::array::{Float64Array, UInt64Array};
use arrow::datatypes::{DataType, Field};
use arrow_schema::{SchemaRef, SortOptions};
use datafusion_common::utils::get_row_at_idx;
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::PartitionEvaluator;

use std::any::Any;
use std::iter;
use std::ops::Range;
use std::sync::Arc;

/// Rank calculates the rank in the window function with order by
#[derive(Debug)]
pub struct Rank {
    name: String,
    rank_type: RankType,
    /// Output data type
    data_type: DataType,
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
pub fn rank(name: String, data_type: &DataType) -> Rank {
    Rank {
        name,
        rank_type: RankType::Basic,
        data_type: data_type.clone(),
    }
}

/// Create a dense rank window function
pub fn dense_rank(name: String, data_type: &DataType) -> Rank {
    Rank {
        name,
        rank_type: RankType::Dense,
        data_type: data_type.clone(),
    }
}

/// Create a percent rank window function
pub fn percent_rank(name: String, data_type: &DataType) -> Rank {
    Rank {
        name,
        rank_type: RankType::Percent,
        data_type: data_type.clone(),
    }
}

impl BuiltInWindowFunctionExpr for Rank {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        let nullable = false;
        Ok(Field::new(self.name(), self.data_type.clone(), nullable))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn create_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(RankEvaluator {
            state: RankState::default(),
            rank_type: self.rank_type,
        }))
    }

    fn get_result_ordering(&self, schema: &SchemaRef) -> Option<PhysicalSortExpr> {
        // The built-in RANK window function (in all modes) introduces a new ordering:
        schema.column_with_name(self.name()).map(|(idx, field)| {
            let expr = Arc::new(Column::new(field.name(), idx));
            let options = SortOptions {
                descending: false,
                nulls_first: false,
            }; // ASC, NULLS LAST
            PhysicalSortExpr { expr, options }
        })
    }
}

#[derive(Debug)]
pub(crate) struct RankEvaluator {
    state: RankState,
    rank_type: RankType,
}

impl PartitionEvaluator for RankEvaluator {
    fn is_causal(&self) -> bool {
        matches!(self.rank_type, RankType::Basic | RankType::Dense)
    }

    /// Evaluates the window function inside the given range.
    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        let row_idx = range.start;
        // There is no argument, values are order by column values (where rank is calculated)
        let range_columns = values;
        let last_rank_data = get_row_at_idx(range_columns, row_idx)?;
        let new_rank_encountered =
            if let Some(state_last_rank_data) = &self.state.last_rank_data {
                // if rank data changes, new rank is encountered
                state_last_rank_data != &last_rank_data
            } else {
                // First rank seen
                true
            };
        if new_rank_encountered {
            self.state.last_rank_data = Some(last_rank_data);
            self.state.last_rank_boundary += self.state.current_group_count;
            self.state.current_group_count = 1;
            self.state.n_rank += 1;
        } else {
            // data is still in the same rank
            self.state.current_group_count += 1;
        }
        match self.rank_type {
            RankType::Basic => Ok(ScalarValue::UInt64(Some(
                self.state.last_rank_boundary as u64 + 1,
            ))),
            RankType::Dense => Ok(ScalarValue::UInt64(Some(self.state.n_rank as u64))),
            RankType::Percent => {
                exec_err!("Can not execute PERCENT_RANK in a streaming fashion")
            }
        }
    }

    fn evaluate_all_with_rank(
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

    fn supports_bounded_execution(&self) -> bool {
        matches!(self.rank_type, RankType::Basic | RankType::Dense)
    }

    fn include_rank(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::cast::{as_float64_array, as_uint64_array};

    fn test_with_rank(expr: &Rank, expected: Vec<u64>) -> Result<()> {
        test_i32_result(expr, vec![0..2, 2..3, 3..6, 6..7, 7..8], expected)
    }

    #[allow(clippy::single_range_in_vec_init)]
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
            .evaluate_all_with_rank(num_rows, &ranks)?;
        let result = as_float64_array(&result)?;
        let result = result.values();
        assert_eq!(expected, *result);
        Ok(())
    }

    fn test_i32_result(
        expr: &Rank,
        ranks: Vec<Range<usize>>,
        expected: Vec<u64>,
    ) -> Result<()> {
        let result = expr.create_evaluator()?.evaluate_all_with_rank(8, &ranks)?;
        let result = as_uint64_array(&result)?;
        let result = result.values();
        assert_eq!(expected, *result);
        Ok(())
    }

    #[test]
    fn test_dense_rank() -> Result<()> {
        let r = dense_rank("arr".into(), &DataType::UInt64);
        test_without_rank(&r, vec![1; 8])?;
        test_with_rank(&r, vec![1, 1, 2, 3, 3, 3, 4, 5])?;
        Ok(())
    }

    #[test]
    fn test_rank() -> Result<()> {
        let r = rank("arr".into(), &DataType::UInt64);
        test_without_rank(&r, vec![1; 8])?;
        test_with_rank(&r, vec![1, 1, 3, 4, 4, 4, 7, 8])?;
        Ok(())
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn test_percent_rank() -> Result<()> {
        let r = percent_rank("arr".into(), &DataType::Float64);

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
