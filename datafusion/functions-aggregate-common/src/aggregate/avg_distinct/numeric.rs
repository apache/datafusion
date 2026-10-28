// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::fmt::Debug;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Float64Type};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr_common::accumulator::Accumulator;

use crate::aggregate::sum_distinct::DistinctSumAccumulator;

/// Specialized implementation of `AVG DISTINCT` for Float64 values, leveraging
/// the existing DistinctSumAccumulator implementation.
#[derive(Debug)]
pub struct Float64DistinctAvgAccumulator {
    // We use the DistinctSumAccumulator to handle the set of distinct values
    sum_accumulator: DistinctSumAccumulator<Float64Type>,
}

impl Default for Float64DistinctAvgAccumulator {
    fn default() -> Self {
        Self {
            sum_accumulator: DistinctSumAccumulator::<Float64Type>::new(
                DataType::Float64,
            ),
        }
    }
}

impl Accumulator for Float64DistinctAvgAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.sum_accumulator.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.sum_accumulator.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.sum_accumulator.merge_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Get the sum from the DistinctSumAccumulator
        let sum_result = self.sum_accumulator.evaluate()?;

        // Extract the sum value
        if let ScalarValue::Float64(Some(sum)) = sum_result {
            // Get the count of distinct values
            let count = self.sum_accumulator.distinct_count() as f64;
            // Calculate average
            let avg = sum / count;
            Ok(ScalarValue::Float64(Some(avg)))
        } else {
            // If sum is None, return None (null)
            Ok(ScalarValue::Float64(None))
        }
    }

    fn size(&self) -> usize {
        self.sum_accumulator.size()
    }
}
