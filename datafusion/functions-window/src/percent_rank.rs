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

//! `percent_rank` window function implementation

use std::any::Any;
use std::fmt::Debug;
use std::iter;
use std::ops::Range;
use std::sync::Arc;

use crate::define_udwf_and_expr;
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::arrow::array::Float64Array;
use datafusion_common::arrow::compute::SortOptions;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::arrow::datatypes::Field;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{PartitionEvaluator, Signature, Volatility, WindowUDFImpl};
use datafusion_functions_window_common::field;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use field::WindowUDFFieldArgs;

define_udwf_and_expr!(
    PercentRank,
    percent_rank,
    "Returns the relative rank of the current row: (rank - 1) / (total rows - 1)"
);

/// percent_rank expression
#[derive(Debug)]
pub struct PercentRank {
    signature: Signature,
}

impl PercentRank {
    /// Create a new `percent_rank` function
    pub fn new() -> Self {
        Self {
            signature: Signature::any(0, Volatility::Immutable),
        }
    }
}

impl Default for PercentRank {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for PercentRank {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "percent_rank"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::<PercentRankEvaluator>::default())
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Field> {
        Ok(Field::new(field_args.name(), DataType::Float64, false))
    }

    fn sort_options(&self) -> Option<SortOptions> {
        Some(SortOptions {
            descending: false,
            nulls_first: false,
        })
    }
}

/// State for the `percent_rank` built-in window function.
#[derive(Debug, Default)]
struct PercentRankEvaluator {}

impl PartitionEvaluator for PercentRankEvaluator {
    fn is_causal(&self) -> bool {
        // The percent_rank function doesn't need "future" values to emit results:
        false
    }

    fn evaluate(
        &mut self,
        _values: &[ArrayRef],
        _range: &Range<usize>,
    ) -> Result<ScalarValue> {
        exec_err!("Can not execute PERCENT_RANK in a streaming fashion")
    }

    fn evaluate_all_with_rank(
        &self,
        num_rows: usize,
        ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        let denominator = num_rows as f64;
        let result =
        // Returns the relative rank of the current row, that is (rank - 1) / (total partition rows - 1). The value thus ranges from 0 to 1 inclusive.
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
        ));

        Ok(result)
    }

    fn supports_bounded_execution(&self) -> bool {
        false
    }

    fn include_rank(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::cast::as_float64_array;

    fn test_f64_result(
        expr: &PercentRank,
        num_rows: usize,
        ranks: Vec<Range<usize>>,
        expected: Vec<f64>,
    ) -> Result<()> {
        let args = PartitionEvaluatorArgs::default();
        let result = expr
            .partition_evaluator(args)?
            .evaluate_all_with_rank(num_rows, &ranks)?;
        let result = as_float64_array(&result)?;
        let result = result.values();
        assert_eq!(expected, *result);
        Ok(())
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn test_percent_rank() -> Result<()> {
        let r = PercentRank::default();

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
