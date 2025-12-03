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

use arrow::datatypes::FieldRef;
use datafusion_common::arrow::array::{ArrayRef, Float64Array};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::arrow::datatypes::Field;
use datafusion_common::Result;
use datafusion_expr::{
    Documentation, LimitEffect, PartitionEvaluator, Signature, Volatility, WindowUDFImpl,
};
use datafusion_functions_window_common::field;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use datafusion_macros::user_doc;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use field::WindowUDFFieldArgs;
use std::any::Any;
use std::fmt::Debug;
use std::iter;
use std::ops::Range;
use std::sync::Arc;

define_udwf_and_expr!(
    CumeDist,
    cume_dist,
    "Calculates the cumulative distribution of a value in a group of values."
);

/// CumeDist calculates the cume_dist in the window function with order by
#[user_doc(
    doc_section(label = "Ranking Functions"),
    description = "Relative rank of the current row: (number of rows preceding or peer with the current row) / (total rows).",
    syntax_example = "cume_dist()",
    sql_example = r#"
```sql
-- Example usage of the cume_dist window function:
SELECT salary,
    cume_dist() OVER (ORDER BY salary) AS cume_dist
FROM employees;

+--------+-----------+
| salary | cume_dist |
+--------+-----------+
| 30000  | 0.33      |
| 50000  | 0.67      |
| 70000  | 1.00      |
+--------+-----------+
```
"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CumeDist {
    signature: Signature,
}

impl CumeDist {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Immutable),
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

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Field::new(field_args.name(), DataType::Float64, false).into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn limit_effect(&self, _args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect {
        LimitEffect::Unknown
    }
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
        let scalar = num_rows as f64;
        let result = Float64Array::from_iter_values(
            ranks_in_partition
                .iter()
                .scan(0_u64, |acc, range| {
                    let len = range.end - range.start;
                    *acc += len as u64;
                    let value: f64 = (*acc as f64) / scalar;
                    let result = iter::repeat_n(value, len);
                    Some(result)
                })
                .flatten(),
        );
        Ok(Arc::new(result))
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
        num_rows: usize,
        ranks: Vec<Range<usize>>,
        expected: Vec<f64>,
    ) -> Result<()> {
        let evaluator = CumeDistEvaluator;
        let result = evaluator.evaluate_all_with_rank(num_rows, &ranks)?;
        let result = as_float64_array(&result)?;
        let result = result.values().to_vec();
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    #[expect(clippy::single_range_in_vec_init)]
    fn test_cume_dist() -> Result<()> {
        test_f64_result(0, vec![], vec![])?;

        test_f64_result(1, vec![0..1], vec![1.0])?;

        test_f64_result(2, vec![0..2], vec![1.0, 1.0])?;

        test_f64_result(4, vec![0..2, 2..4], vec![0.5, 0.5, 1.0, 1.0])?;

        Ok(())
    }
}
