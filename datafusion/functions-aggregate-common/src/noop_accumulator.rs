// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
