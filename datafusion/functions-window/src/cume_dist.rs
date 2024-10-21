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

//! `cume_dist` window function implementation

use datafusion_common::arrow::array::{ArrayRef, Float64Array};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::arrow::datatypes::Field;
use datafusion_common::Result;
use datafusion_expr::window_doc_sections::DOC_SECTION_RANKING;
use datafusion_expr::{
    Documentation, PartitionEvaluator, Signature, Volatility, WindowUDFImpl,
};
use datafusion_functions_window_common::field;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use field::WindowUDFFieldArgs;
use std::any::Any;
use std::fmt::Debug;
use std::iter;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

define_udwf_and_expr!(
    CumeDist,
    cume_dist,
    "Calculates the cumulative distribution of a value in a group of values."
);

/// CumeDist calculates the cume_dist in the window function with order by
#[derive(Debug)]
pub struct CumeDist {
    signature: Signature,
}

impl CumeDist {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(0, Volatility::Immutable),
        }
    }
}

impl Default for CumeDist {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for CumeDist {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "cume_dist"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::<CumeDistEvaluator>::default())
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Field> {
        Ok(Field::new(field_args.name(), DataType::Float64, false))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_cume_dist_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_cume_dist_doc() -> &'static Documentation {
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

#[derive(Debug, Default)]
pub(crate) struct CumeDistEvaluator;

impl PartitionEvaluator for CumeDistEvaluator {
    /// Computes the cumulative distribution for all rows in the partition
    fn evaluate_all_with_rank(
        &self,
        num_rows: usize,
        ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        if num_rows == 0 {
            return Ok(Arc::new(Float64Array::from(Vec::<f64>::new())));
        }

        let scalar = num_rows as f64;
        let mut cumulative = 0_u64;
        let mut values = Vec::with_capacity(num_rows);

        for range in ranks_in_partition {
            let len = range.end - range.start;
            cumulative += len as u64;
            let value = cumulative as f64 / scalar;
            values.extend(iter::repeat(value).take(len));
        }

        Ok(Arc::new(Float64Array::from(values)))
    }

    fn include_rank(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::arrow::array::{Array, Float64Array};
    use datafusion_common::cast::as_float64_array;

    fn test_cume_dist_result(
        expr: &CumeDist,
        num_rows: usize,
        ranks: Vec<Range<usize>>,
        expected: Vec<f64>,
    ) -> Result<()> {
        let result = expr
            .partition_evaluator(PartitionEvaluatorArgs::default())?
            .evaluate_all_with_rank(num_rows, &ranks)?;
        let result = as_float64_array(&result)?;
        let result = result.values();
        assert_eq!(expected, *result);
        Ok(())
    }

    #[test]
    fn test_cume_dist_empty() -> Result<()> {
        let cume_dist = CumeDist::default();
        let expected: Vec<f64> = vec![];
        test_cume_dist_result(&cume_dist, 0, vec![], expected)?;
        Ok(())
    }

    #[test]
    fn test_cume_dist_single() -> Result<()> {
        let cume_dist = CumeDist::default();
        let expected = vec![1.0];
        test_cume_dist_result(&cume_dist, 1, vec![0..1], expected)?;
        Ok(())
    }

    #[test]
    fn test_cume_dist_two_rows() -> Result<()> {
        let cume_dist = CumeDist::default();
        let expected = vec![0.5, 1.0];
        test_cume_dist_result(&cume_dist, 2, vec![0..1, 1..2], expected)?;
        Ok(())
    }

    #[test]
    fn test_cume_dist_multiple_partitions() -> Result<()> {
        let cume_dist = CumeDist::default();
        let expected = vec![0.25, 0.5, 0.75, 1.0];
        test_cume_dist_result(&cume_dist, 4, vec![0..1, 1..2, 2..3, 3..4], expected)?;
        Ok(())
    }

    #[test]
    fn test_cume_dist_with_ties() -> Result<()> {
        let cume_dist = CumeDist::default();
        let expected = vec![0.5, 0.5, 1.0, 1.0];
        test_cume_dist_result(&cume_dist, 4, vec![0..2, 2..4], expected)?;
        Ok(())
    }
}
