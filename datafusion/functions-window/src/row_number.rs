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

//! Defines physical expression for `row_number` that can evaluated at runtime during query execution

use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;

use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::arrow::array::UInt64Array;
use datafusion_common::arrow::compute::SortOptions;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::expr::WindowFunction;
use datafusion_expr::{Expr, PartitionEvaluator, Signature, Volatility, WindowUDFImpl};

/// Create a [`WindowFunction`](Expr::WindowFunction) expression for
/// `row_number` user-defined window function.
pub fn row_number() -> Expr {
    Expr::WindowFunction(WindowFunction::new(row_number_udwf(), vec![]))
}

/// Singleton instance of `row_number`, ensures the UDWF is only created once.
#[allow(non_upper_case_globals)]
static STATIC_RowNumber: std::sync::OnceLock<std::sync::Arc<datafusion_expr::WindowUDF>> =
    std::sync::OnceLock::new();

/// Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for `row_number`
/// user-defined window function.
pub fn row_number_udwf() -> std::sync::Arc<datafusion_expr::WindowUDF> {
    STATIC_RowNumber
        .get_or_init(|| {
            std::sync::Arc::new(datafusion_expr::WindowUDF::from(RowNumber::default()))
        })
        .clone()
}

/// row_number expression
#[derive(Debug)]
struct RowNumber {
    signature: Signature,
}

impl RowNumber {
    /// Create a new `row_number` function
    fn new() -> Self {
        Self {
            signature: Signature::any(0, Volatility::Immutable),
        }
    }
}

impl Default for RowNumber {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for RowNumber {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "row_number"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn partition_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::<NumRowsEvaluator>::default())
    }

    fn nullable(&self) -> bool {
        false
    }

    fn sort_options(&self) -> Option<SortOptions> {
        Some(SortOptions {
            descending: false,
            nulls_first: false,
        })
    }
}

/// State for the `row_number` built-in window function.
#[derive(Debug, Default)]
struct NumRowsEvaluator {
    n_rows: usize,
}

impl PartitionEvaluator for NumRowsEvaluator {
    fn is_causal(&self) -> bool {
        // The row_number function doesn't need "future" values to emit results:
        true
    }

    fn evaluate_all(
        &mut self,
        _values: &[ArrayRef],
        num_rows: usize,
    ) -> Result<ArrayRef> {
        Ok(std::sync::Arc::new(UInt64Array::from_iter_values(
            1..(num_rows as u64) + 1,
        )))
    }

    fn evaluate(
        &mut self,
        _values: &[ArrayRef],
        _range: &Range<usize>,
    ) -> Result<ScalarValue> {
        self.n_rows += 1;
        Ok(ScalarValue::UInt64(Some(self.n_rows as u64)))
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::arrow::array::{Array, BooleanArray};
    use datafusion_common::cast::as_uint64_array;

    use super::*;

    #[test]
    fn row_number_all_null() -> Result<()> {
        let values: ArrayRef = Arc::new(BooleanArray::from(vec![
            None, None, None, None, None, None, None, None,
        ]));
        let num_rows = values.len();

        let actual = RowNumber::default()
            .partition_evaluator()?
            .evaluate_all(&[values], num_rows)?;
        let actual = as_uint64_array(&actual)?;

        assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8], *actual.values());
        Ok(())
    }

    #[test]
    fn row_number_all_values() -> Result<()> {
        let values: ArrayRef = Arc::new(BooleanArray::from(vec![
            true, false, true, false, false, true, false, true,
        ]));
        let num_rows = values.len();

        let actual = RowNumber::default()
            .partition_evaluator()?
            .evaluate_all(&[values], num_rows)?;
        let actual = as_uint64_array(&actual)?;

        assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8], *actual.values());
        Ok(())
    }
}
