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

//! `ntile` window function implementation

use crate::utils::{get_scalar_value_from_args, get_unsigned_integer};
use arrow::datatypes::FieldRef;
use datafusion_common::arrow::array::{ArrayRef, UInt64Array};
use datafusion_common::arrow::datatypes::{DataType, Field};
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use datafusion_expr::{
    Documentation, LimitEffect, PartitionEvaluator, Signature, Volatility, WindowUDFImpl,
};
use datafusion_functions_window_common::field;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use datafusion_macros::user_doc;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use field::WindowUDFFieldArgs;
use std::fmt::Debug;
use std::sync::Arc;

define_udwf_and_expr!(
    Ntile,
    ntile,
    [arg],
    ntile_udwf,
    "Integer ranging from 1 to the argument value, dividing the partition as equally as possible."
);

#[user_doc(
    doc_section(label = "Ranking Functions"),
    description = "Integer ranging from 1 to the argument value, dividing the partition as equally as possible",
    syntax_example = "ntile(expression)",
    argument(
        name = "expression",
        description = "An integer describing the number groups the partition should be split into"
    ),
    sql_example = r#"
```sql
-- Example usage of the ntile window function:
SELECT employee_id,
    salary,
    ntile(4) OVER (ORDER BY salary DESC) AS quartile
FROM employees;

+-------------+--------+----------+
| employee_id | salary | quartile |
+-------------+--------+----------+
| 1           | 90000  | 1        |
| 2           | 85000  | 1        |
| 3           | 80000  | 2        |
| 4           | 70000  | 2        |
| 5           | 60000  | 3        |
| 6           | 50000  | 3        |
| 7           | 40000  | 4        |
| 8           | 30000  | 4        |
+-------------+--------+----------+
```
"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Ntile {
    signature: Signature,
}

impl Ntile {
    /// Create a new `ntile` function
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    DataType::UInt64,
                    DataType::UInt32,
                    DataType::UInt16,
                    DataType::UInt8,
                    DataType::Int64,
                    DataType::Int32,
                    DataType::Int16,
                    DataType::Int8,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for Ntile {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for Ntile {
    fn name(&self) -> &str {
        "ntile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        // check the n value is provided, guard against NTILE()
        let scalar_n =
            get_scalar_value_from_args(partition_evaluator_args.input_exprs(), 0)?
                .ok_or_else(|| {
                    exec_datafusion_err!("NTILE requires a positive integer")
                })?;

        if scalar_n.is_null() {
            return exec_err!("NTILE requires a positive integer, but finds NULL");
        }

        // Works for both signed and unsigned inputs: ScalarValue::cast_to uses
        // safe=false, so negative signed values fail the cast to UInt64, and
        // routing through UInt64 also accepts values greater than i64::MAX.
        let n = get_unsigned_integer(&scalar_n)
            .map_err(|_| exec_datafusion_err!("NTILE requires a positive integer"))?;

        if n == 0 {
            return exec_err!("NTILE requires a positive integer");
        }

        Ok(Box::new(NtileEvaluator { n }))
    }
    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        let nullable = false;

        Ok(Field::new(field_args.name(), DataType::UInt64, nullable).into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn limit_effect(&self, _args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect {
        LimitEffect::Unknown
    }
}

#[derive(Debug)]
struct NtileEvaluator {
    n: u64,
}

impl PartitionEvaluator for NtileEvaluator {
    fn evaluate_all(
        &mut self,
        _values: &[ArrayRef],
        num_rows: usize,
    ) -> Result<ArrayRef> {
        // SQL NTILE distributes rows "as equally as possible": with `base = num_rows / n`
        // and `remainder = num_rows % n`, the first `remainder` buckets each contain
        // `base + 1` rows and the rest contain `base` rows. The previous formula
        // `i * n / num_rows` does not preserve those bucket sizes (e.g. NTILE(4) over
        // 10 rows yielded sizes 3,2,3,2 instead of 3,3,2,2).
        let num_rows = num_rows as u64;
        let n = self.n;
        let mut vec: Vec<u64> = Vec::with_capacity(num_rows as usize);
        let base = num_rows / n;
        let remainder = num_rows % n;
        let large_bucket_size = base + 1;
        let large_rows = remainder * large_bucket_size;
        for i in 0..num_rows {
            let bucket = if i < large_rows {
                i / large_bucket_size + 1
            } else {
                // base > 0 here: i >= large_rows is only reachable when remainder < n,
                // which forces base >= 1 (otherwise large_rows would equal num_rows).
                remainder + (i - large_rows) / base + 1
            };
            vec.push(bucket);
        }
        Ok(Arc::new(UInt64Array::from(vec)))
    }
}
