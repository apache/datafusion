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

use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::arrow::array::UInt64Array;
use datafusion_common::arrow::compute::SortOptions;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::arrow::datatypes::Field;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::window_doc_sections::DOC_SECTION_RANKING;
use datafusion_expr::{
    Documentation, PartitionEvaluator, Signature, Volatility, WindowUDFImpl,
};
use datafusion_functions_window_common::field;
use field::WindowUDFFieldArgs;
use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::OnceLock;

define_udwf_and_expr!(
    RowNumber,
    row_number,
    "Returns a unique row number for each row in window partition beginning at 1."
);

/// row_number expression
#[derive(Debug)]
pub struct RowNumber {
    signature: Signature,
}

impl RowNumber {
    /// Create a new `row_number` function
    pub fn new() -> Self {
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

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_row_number_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_RANKING)
            .with_description(
                "Number of the current row within its partition, counting from 1.",
            )
            .with_syntax_example("row_number()")
            .build()
            .unwrap()
    })
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

    fn partition_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::<NumRowsEvaluator>::default())
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

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_row_number_doc())
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
