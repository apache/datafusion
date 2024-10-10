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

//! `rank` window function implementation

use std::any::Any;
use std::fmt::Debug;
use std::iter;
use std::ops::Range;
use std::sync::Arc;

use crate::define_udwf_and_expr;
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::arrow::array::UInt64Array;
use datafusion_common::arrow::compute::SortOptions;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::arrow::datatypes::Field;
use datafusion_common::utils::get_row_at_idx;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{PartitionEvaluator, Signature, Volatility, WindowUDFImpl};
use datafusion_functions_window_common::field;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use field::WindowUDFFieldArgs;

define_udwf_and_expr!(
    Rank,
    rank,
    "Returns rank of the current row with gaps. Same as `row_number` of its first peer"
);

/// rank expression
#[derive(Debug)]
pub struct Rank {
    signature: Signature,
}

impl Rank {
    /// Create a new `rank` function
    pub fn new() -> Self {
        Self {
            signature: Signature::any(0, Volatility::Immutable),
        }
    }
}

impl Default for Rank {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for Rank {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "rank"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::<RankEvaluator>::default())
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

/// State for the RANK(rank) built-in window function.
#[derive(Debug, Clone, Default)]
pub struct RankState {
    /// The last values for rank as these values change, we increase n_rank
    pub last_rank_data: Option<Vec<ScalarValue>>,
    /// The index where last_rank_boundary is started
    pub last_rank_boundary: usize,
    /// Keep the number of entries in current rank
    pub current_group_count: usize,
    /// Rank number kept from the start
    pub n_rank: usize,
}

/// State for the `rank` built-in window function.
#[derive(Debug, Default)]
struct RankEvaluator {
    state: RankState,
}

impl PartitionEvaluator for RankEvaluator {
    fn is_causal(&self) -> bool {
        // The rank function doesn't need "future" values to emit results:
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

        Ok(ScalarValue::UInt64(Some(
            self.state.last_rank_boundary as u64 + 1,
        )))
    }

    fn evaluate_all_with_rank(
        &self,
        _num_rows: usize,
        ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        let result = Arc::new(UInt64Array::from_iter_values(
            ranks_in_partition
                .iter()
                .scan(1_u64, |acc, range| {
                    let len = range.end - range.start;
                    let result = iter::repeat(*acc).take(len);
                    *acc += len as u64;
                    Some(result)
                })
                .flatten(),
        ));

        Ok(result)
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    fn include_rank(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::cast::as_uint64_array;

    fn test_with_rank(expr: &Rank, expected: Vec<u64>) -> Result<()> {
        test_i32_result(expr, vec![0..2, 2..3, 3..6, 6..7, 7..8], expected)
    }

    #[allow(clippy::single_range_in_vec_init)]
    fn test_without_rank(expr: &Rank, expected: Vec<u64>) -> Result<()> {
        test_i32_result(expr, vec![0..8], expected)
    }

    fn test_i32_result(
        expr: &Rank,
        ranks: Vec<Range<usize>>,
        expected: Vec<u64>,
    ) -> Result<()> {
        let args = PartitionEvaluatorArgs::default();
        let result = expr
            .partition_evaluator(args)?
            .evaluate_all_with_rank(8, &ranks)?;
        let result = as_uint64_array(&result)?;
        let result = result.values();
        assert_eq!(expected, *result);
        Ok(())
    }

    #[test]
    fn test_rank() -> Result<()> {
        let r = Rank::default();
        test_without_rank(&r, vec![1; 8])?;
        test_with_rank(&r, vec![1, 1, 3, 4, 4, 4, 7, 8])?;
        Ok(())
    }
}
