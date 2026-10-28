// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Defines the accumulator for `SUM DISTINCT` for primitive numeric types

use std::fmt::Debug;
use std::mem::size_of_val;

use arrow::array::ArrayRef;
use arrow::array::ArrowNativeTypeOp;
use arrow::array::ArrowPrimitiveType;
use arrow::datatypes::ArrowNativeType;
use arrow::datatypes::DataType;

use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr_common::accumulator::Accumulator;

use crate::utils::GenericDistinctBuffer;

/// Accumulator for computing SUM(DISTINCT expr)
#[derive(Debug)]
pub struct DistinctSumAccumulator<T: ArrowPrimitiveType> {
    values: GenericDistinctBuffer<T>,
    data_type: DataType,
}

impl<T: ArrowPrimitiveType> DistinctSumAccumulator<T> {
    pub fn new(data_type: DataType) -> Self {
        Self {
            values: GenericDistinctBuffer::new(data_type.clone()),
            data_type,
        }
    }

    pub fn distinct_count(&self) -> usize {
        self.values.values.len()
    }

    /// Iterates the distinct values collected so far. `AVG(DISTINCT)` re-sums
    /// them in a wider type instead of using [`Self::evaluate`]'s input-typed
    /// sum.
    pub(crate) fn distinct_values(&self) -> impl Iterator<Item = T::Native> + '_ {
        self.values.values.iter().map(|v| v.0)
    }
}

impl<T: ArrowPrimitiveType + Debug> Accumulator for DistinctSumAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.values.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.values.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.values.merge_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.distinct_count() == 0 {
            ScalarValue::new_primitive::<T>(None, &self.data_type)
        } else {
            let mut acc = T::Native::usize_as(0);
            for distinct_value in self.values.values.iter() {
                acc = acc.add_wrapping(distinct_value.0)
            }
            ScalarValue::new_primitive::<T>(Some(acc), &self.data_type)
        }
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.size()
    }
}
