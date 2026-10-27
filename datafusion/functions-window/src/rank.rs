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

//! Implementation of `rank`, `dense_rank`, and `percent_rank` window functions,
//! which can be evaluated at runtime during query execution.

use arrow::datatypes::FieldRef;
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::arrow::array::{Float64Array, UInt64Array};
use datafusion_common::arrow::compute::SortOptions;
use datafusion_common::arrow::compute::kernels::sort::{
    LexicographicalComparator, SortColumn,
};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::arrow::datatypes::Field;
use datafusion_common::utils::get_row_at_idx;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_doc::window_doc_sections::DOC_SECTION_RANKING;
use datafusion_expr::{
    Documentation, LimitEffect, PartitionEvaluator, Signature, Volatility, WindowUDFImpl,
};
use datafusion_functions_window_common::field;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use field::WindowUDFFieldArgs;
use std::fmt::Debug;
use std::hash::Hash;
use std::iter;
use std::ops::Range;
use std::sync::{Arc, LazyLock};

define_udwf_and_expr!(
    Rank,
    rank,
    rank_udwf,
    "Returns rank of the current row with gaps. Same as `row_number` of its first peer",
    Rank::basic
);

define_udwf_and_expr!(
    DenseRank,
    dense_rank,
    dense_rank_udwf,
    "Returns rank of the current row without gaps. This function counts peer groups",
    Rank::dense_rank
);

define_udwf_and_expr!(
    PercentRank,
    percent_rank,
    percent_rank_udwf,
    "Returns the relative rank of the current row: (rank - 1) / (total rows - 1)",
    Rank::percent_rank
);

/// Rank calculates the rank in the window function with order by
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Rank {
    name: String,
    signature: Signature,
    rank_type: RankType,
}

impl Rank {
    /// Create a new `rank` function with the specified name and rank type
    pub fn new(name: String, rank_type: RankType) -> Self {
        Self {
            name,
            signature: Signature::nullary(Volatility::Immutable),
            rank_type,
        }
    }

    /// Create a `rank` window function
    pub fn basic() -> Self {
        Rank::new("rank".to_string(), RankType::Basic)
    }

    /// Create a `dense_rank` window function
    pub fn dense_rank() -> Self {
        Rank::new("dense_rank".to_string(), RankType::Dense)
    }

    /// Create a `percent_rank` window function
    pub fn percent_rank() -> Self {
        Rank::new("percent_rank".to_string(), RankType::Percent)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum RankType {
    Basic,
    Dense,
    Percent,
}

static RANK_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_RANKING,
            "Returns the rank of the current row within its partition, allowing \
            gaps between ranks. This function provides a ranking similar to `row_number`, but \
            skips ranks for identical values.",

        "rank()")
        .with_sql_example(r#"
```sql
-- Example usage of the rank window function:
SELECT department,
    salary,
    rank() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
FROM employees;

+-------------+--------+------+
| department  | salary | rank |
+-------------+--------+------+
| Sales       | 70000  | 1    |
| Sales       | 50000  | 2    |
| Sales       | 50000  | 2    |
| Sales       | 30000  | 4    |
| Engineering | 90000  | 1    |
| Engineering | 80000  | 2    |
+-------------+--------+------+
```
"#)
        .build()
});

fn get_rank_doc() -> &'static Documentation {
    &RANK_DOCUMENTATION
}

static DENSE_RANK_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(DOC_SECTION_RANKING, "Returns the rank of the current row without gaps. This function ranks \
            rows in a dense manner, meaning consecutive ranks are assigned even for identical \
            values.", "dense_rank()")
        .with_sql_example(r#"
```sql
-- Example usage of the dense_rank window function:
SELECT department,
    salary,
    dense_rank() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rank
FROM employees;

+-------------+--------+------------+
| department  | salary | dense_rank |
+-------------+--------+------------+
| Sales       | 70000  | 1          |
| Sales       | 50000  | 2          |
| Sales       | 50000  | 2          |
| Sales       | 30000  | 3          |
| Engineering | 90000  | 1          |
| Engineering | 80000  | 2          |
+-------------+--------+------------+
```"#)
        .build()
});

fn get_dense_rank_doc() -> &'static Documentation {
    &DENSE_RANK_DOCUMENTATION
}

static PERCENT_RANK_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(DOC_SECTION_RANKING, "Returns the percentage rank of the current row within its partition. \
            The value ranges from 0 to 1 and is computed as `(rank - 1) / (total_rows - 1)`.", "percent_rank()")
        .with_sql_example(r#"```sql
    -- Example usage of the percent_rank window function:
SELECT employee_id,
    salary,
    percent_rank() OVER (ORDER BY salary) AS percent_rank
FROM employees;

+-------------+--------+---------------+
| employee_id | salary | percent_rank  |
+-------------+--------+---------------+
| 1           | 30000  | 0.00          |
| 2           | 50000  | 0.50          |
| 3           | 70000  | 1.00          |
+-------------+--------+---------------+
```"#)
        .build()
});

fn get_percent_rank_doc() -> &'static Documentation {
    &PERCENT_RANK_DOCUMENTATION
}

impl WindowUDFImpl for Rank {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(RankEvaluator {
            state: RankState::default(),
            rank_type: self.rank_type,
        }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        let return_type = match self.rank_type {
            RankType::Basic | RankType::Dense => DataType::UInt64,
            RankType::Percent => DataType::Float64,
        };

        let nullable = false;
        Ok(Field::new(field_args.name(), return_type, nullable).into())
    }

    fn sort_options(&self) -> Option<SortOptions> {
        Some(SortOptions {
            descending: false,
            nulls_first: false,
        })
    }

    fn documentation(&self) -> Option<&Documentation> {
        match self.rank_type {
            RankType::Basic => Some(get_rank_doc()),
            RankType::Dense => Some(get_dense_rank_doc()),
            RankType::Percent => Some(get_percent_rank_doc()),
        }
    }

    fn limit_effect(&self, _args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect {
        match self.rank_type {
            RankType::Basic => LimitEffect::None,
            RankType::Dense => LimitEffect::None,
            RankType::Percent => LimitEffect::Unknown,
        }
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
#[derive(Debug)]
struct RankEvaluator {
    state: RankState,
    rank_type: RankType,
}

impl PartitionEvaluator for RankEvaluator {
    fn is_causal(&self) -> bool {
        matches!(self.rank_type, RankType::Basic | RankType::Dense)
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
                // Nested ORDER BY keys can encode the same value with different
                // physical layouts; ScalarValue inequality would split peers.
                !rows_logically_equal(state_last_rank_data, &last_rank_data)?
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
        let result: ArrayRef = match self.rank_type {
            RankType::Basic => Arc::new(UInt64Array::from_iter_values(
                ranks_in_partition
                    .iter()
                    .scan(1_u64, |acc, range| {
                        let len = range.end - range.start;
                        let result = iter::repeat_n(*acc, len);
                        *acc += len as u64;
                        Some(result)
                    })
                    .flatten(),
            )),

            RankType::Dense => Arc::new(UInt64Array::from_iter_values(
                ranks_in_partition
                    .iter()
                    .zip(1u64..)
                    .flat_map(|(range, rank)| {
                        let len = range.end - range.start;
                        iter::repeat_n(rank, len)
                    }),
            )),

            RankType::Percent => {
                let denominator = num_rows as f64;

                Arc::new(Float64Array::from_iter_values(
                    ranks_in_partition
                        .iter()
                        .scan(0_u64, |acc, range| {
                            let len = range.end - range.start;
                            let value = (*acc as f64) / (denominator - 1.0).max(1.0);
                            let result = iter::repeat_n(value, len);
                            *acc += len as u64;
                            Some(result)
                        })
                        .flatten(),
                ))
            }
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

/// Compare two ORDER BY rows with Arrow logical equality when any value is
/// nested. Physical [`PartialEq`] can distinguish a null dictionary key from a
/// key that references a null value.
fn rows_logically_equal(left: &[ScalarValue], right: &[ScalarValue]) -> Result<bool> {
    if left.len() != right.len() {
        return Ok(false);
    }
    if left == right {
        return Ok(true);
    }
    if !left
        .iter()
        .chain(right.iter())
        .any(|value| value.data_type().is_nested())
    {
        return Ok(false);
    }
    let columns = left
        .iter()
        .zip(right.iter())
        .map(|(a, b)| ScalarValue::iter_to_array([a.clone(), b.clone()]))
        .collect::<Result<Vec<_>>>()?;
    let sort_columns = columns
        .iter()
        .map(|values| SortColumn {
            values: Arc::clone(values),
            options: Some(SortOptions::default()),
        })
        .collect::<Vec<_>>();
    let comparator = LexicographicalComparator::try_new(&sort_columns)?;
    Ok(comparator.compare(0, 1).is_eq())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::cast::{as_float64_array, as_uint64_array};

    fn test_with_rank(expr: &Rank, expected: Vec<u64>) -> Result<()> {
        test_i32_result(expr, vec![0..2, 2..3, 3..6, 6..7, 7..8], expected)
    }

    #[expect(clippy::single_range_in_vec_init)]
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

    fn test_f64_result(
        expr: &Rank,
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
    fn test_rank() -> Result<()> {
        let r = Rank::basic();
        test_without_rank(&r, vec![1; 8])?;
        test_with_rank(&r, vec![1, 1, 3, 4, 4, 4, 7, 8])?;
        Ok(())
    }

    #[test]
    fn test_dense_rank() -> Result<()> {
        let r = Rank::dense_rank();
        test_without_rank(&r, vec![1; 8])?;
        test_with_rank(&r, vec![1, 1, 2, 3, 3, 3, 4, 5])?;
        Ok(())
    }

    #[test]
    #[expect(clippy::single_range_in_vec_init)]
    fn test_percent_rank() -> Result<()> {
        let r = Rank::percent_rank();

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

    #[test]
    fn nested_dictionary_null_peers_share_rank() -> Result<()> {
        use arrow::array::{DictionaryArray, Int8Array, Int32Array, ListArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::{Field, Int8Type};
        use std::sync::Arc;

        // Both lists contain NULL via different dictionary encodings.
        let children: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![None, Some(0)]),
            Arc::new(Int32Array::from(vec![None])),
        )?);
        let key: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", children.data_type().clone(), true)),
            OffsetBuffer::from_lengths([1, 1]),
            children,
            None,
        ));

        for (expr, expected) in [
            (Rank::basic(), vec![1u64, 1u64]),
            (Rank::dense_rank(), vec![1u64, 1u64]),
        ] {
            let mut evaluator =
                expr.partition_evaluator(PartitionEvaluatorArgs::default())?;
            let mut out = Vec::new();
            for idx in 0..2 {
                let value =
                    evaluator.evaluate(std::slice::from_ref(&key), &(idx..idx + 1))?;
                out.push(match value {
                    ScalarValue::UInt64(Some(v)) => v,
                    other => panic!("unexpected {other:?}"),
                });
            }
            assert_eq!(out, expected, "{}", expr.name());
        }
        Ok(())
    }
}
