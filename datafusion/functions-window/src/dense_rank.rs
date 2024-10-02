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

//! Defines physical expression for `dense_rank` that can evaluated at runtime during query execution

use std::any::Any;
use std::fmt::Debug;
use std::iter;
use std::ops::Range;
use std::sync::Arc;

use crate::rank::RankState;
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::arrow::array::UInt64Array;
use datafusion_common::arrow::compute::SortOptions;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::arrow::datatypes::Field;
use datafusion_common::utils::get_row_at_idx;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::expr::WindowFunction;
use datafusion_expr::{Expr, PartitionEvaluator, Signature, Volatility, WindowUDFImpl};
use datafusion_functions_window_common::field;
use field::WindowUDFFieldArgs;

/// Create a [`WindowFunction`](Expr::WindowFunction) expression for
/// `dense_rank` user-defined window function.
pub fn dense_rank() -> Expr {
    Expr::WindowFunction(WindowFunction::new(dense_rank_udwf(), vec![]))
}

/// Singleton instance of `dense_rank`, ensures the UDWF is only created once.
#[allow(non_upper_case_globals)]
static STATIC_DenseRank: std::sync::OnceLock<std::sync::Arc<datafusion_expr::WindowUDF>> =
    std::sync::OnceLock::new();

/// Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for `dense_rank`
/// user-defined window function.
pub fn dense_rank_udwf() -> std::sync::Arc<datafusion_expr::WindowUDF> {
    STATIC_DenseRank
        .get_or_init(|| {
            std::sync::Arc::new(datafusion_expr::WindowUDF::from(DenseRank::default()))
        })
        .clone()
}

/// dense_rank expression
#[derive(Debug)]
pub struct DenseRank {
    signature: Signature,
}

impl DenseRank {
    /// Create a new `dense_rank` function
    pub fn new() -> Self {
        Self {
            signature: Signature::any(0, Volatility::Immutable),
        }
    }
}

impl Default for DenseRank {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for DenseRank {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "dense_rank"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::<DenseRankEvaluator>::default())
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Field> {
        Ok(Field::new(field_args.name(), DataType::UInt64, false))
    }

    fn sort_options(&self) -> Option<SortOptions> {
        Some(SortOptions {
            descending: false,
            nulls_first: false,
        })
    }
}

/// State for the `dense_rank` built-in window function.
#[derive(Debug, Default)]
struct DenseRankEvaluator {
    state: RankState,
}

impl PartitionEvaluator for DenseRankEvaluator {
    fn is_causal(&self) -> bool {
        // The dense_rank function doesn't need "future" values to emit results:
        true
    }

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

        Ok(ScalarValue::UInt64(Some(self.state.n_rank as u64)))
    }

    fn evaluate_all_with_rank(
        &self,
        _num_rows: usize,
        ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        let result = Arc::new(UInt64Array::from_iter_values(
            ranks_in_partition
                .iter()
                .zip(1u64..)
                .flat_map(|(range, rank)| {
                    let len = range.end - range.start;
                    iter::repeat(rank).take(len)
                }),
        ));

        Ok(result)
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::cast::{as_float64_array, as_uint64_array};

    fn test_with_rank(expr: &DenseRank, expected: Vec<u64>) -> Result<()> {
        test_i32_result(expr, vec![0..2, 2..3, 3..6, 6..7, 7..8], expected)
    }

    #[allow(clippy::single_range_in_vec_init)]
    fn test_without_rank(expr: &DenseRank, expected: Vec<u64>) -> Result<()> {
        test_i32_result(expr, vec![0..8], expected)
    }

    fn test_i32_result(
        expr: &DenseRank,
        ranks: Vec<Range<usize>>,
        expected: Vec<u64>,
    ) -> Result<()> {
        let result = expr
            .partition_evaluator()?
            .evaluate_all_with_rank(8, &ranks)?;
        let result = as_uint64_array(&result)?;
        let result = result.values();
        assert_eq!(expected, *result);
        Ok(())
    }

    #[test]
    fn test_dense_rank() -> Result<()> {
        let r = DenseRank::default();
        test_without_rank(&r, vec![1; 8])?;
        test_with_rank(&r, vec![1, 1, 2, 3, 3, 3, 4, 5])?;
        Ok(())
    }
}
