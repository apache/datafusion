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

//! Specialized implementation of `COUNT DISTINCT` for "Native" arrays such as
//! [`Int64Array`] and [`Float64Array`]
//!
//! [`Int64Array`]: arrow::array::Int64Array
//! [`Float64Array`]: arrow::array::Float64Array
use std::fmt::Debug;
use std::mem::size_of_val;

use ahash::RandomState;
use arrow::array::types::ArrowPrimitiveType;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use datafusion_common::{HashSet, ScalarValue};
use datafusion_expr_common::accumulator::Accumulator;

use crate::utils::{DistinctStorage, GenericDistinctBuffer, Hashable};

/// Unified distinct count accumulator for primitive types.
///
/// This accumulator works with any primitive type and uses the `DistinctStorage`
/// trait to select the appropriate hashing strategy:
/// - Native `HashSet<T>` for natively hashable types (integers, decimals, dates, timestamps)
/// - `HashSet<Hashable<T>>` for types requiring special hashing (floats)
#[derive(Debug)]
pub struct PrimitiveDistinctCountAccumulator<T, S>
where
    T: ArrowPrimitiveType + Send,
    S: DistinctStorage<Native = T::Native>,
{
    values: GenericDistinctBuffer<T, S>,
}

impl<T, S> PrimitiveDistinctCountAccumulator<T, S>
where
    T: ArrowPrimitiveType + Send,
    S: DistinctStorage<Native = T::Native>,
{
    pub fn new(data_type: &DataType) -> Self {
        Self {
            values: GenericDistinctBuffer::new(data_type.clone()),
        }
    }
}

impl<T, S> Accumulator for PrimitiveDistinctCountAccumulator<T, S>
where
    T: ArrowPrimitiveType + Send + Sync + Debug,
    S: DistinctStorage<Native = T::Native>,
{
    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        self.values.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        self.values.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        self.values.merge_batch(states)
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.values.values.len() as i64)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.size()
    }
}

// Type alias for float distinct count accumulator (uses Hashable wrapper)
pub type FloatDistinctCountAccumulator<T> =
    PrimitiveDistinctCountAccumulator<T, HashSet<Hashable<<T as ArrowPrimitiveType>::Native>, RandomState>>;
