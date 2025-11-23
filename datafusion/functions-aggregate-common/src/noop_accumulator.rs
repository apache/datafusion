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

use arrow::array::ArrayRef;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr_common::accumulator::Accumulator;

/// [`Accumulator`] that does no work and always returns a fixed value (default
/// of `NULL` but can be customized).
///
/// Useful for aggregate functions that need to handle an input of [`DataType::Null`]
/// that does no work.
///
/// [`DataType::Null`]: arrow::datatypes::DataType::Null
#[derive(Debug)]
pub struct NoopAccumulator {
    evaluate_value: ScalarValue,
}

impl NoopAccumulator {
    pub fn new(evaluate_value: ScalarValue) -> Self {
        Self { evaluate_value }
    }
}

impl Default for NoopAccumulator {
    fn default() -> Self {
        Self {
            evaluate_value: ScalarValue::Null,
        }
    }
}

impl Accumulator for NoopAccumulator {
    fn update_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.evaluate_value.clone())
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // We ensure we return a state field even if unused otherwise we run into
        // issues with queries like `SELECT agg_fn(NULL) FROM table`
        Ok(vec![ScalarValue::Null])
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        Ok(())
    }
}
