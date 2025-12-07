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

use arrow::array::ArrayRef;
use arrow::array::types::ArrowPrimitiveType;
use arrow::datatypes::DataType;

use datafusion_common::ScalarValue;
use datafusion_expr_common::accumulator::Accumulator;

use crate::utils::{DistinctKey, GenericDistinctBuffer};

#[derive(Debug)]
pub struct DistinctCountAccumulator<T: ArrowPrimitiveType + DistinctKey> {
    values: GenericDistinctBuffer<T>,
}

impl<T: ArrowPrimitiveType + DistinctKey> DistinctCountAccumulator<T> {
    pub fn new() -> Self {
        Self {
            values: GenericDistinctBuffer::new(T::DATA_TYPE),
        }
    }

    pub fn with_data_type(data_type: &DataType) -> Self {
        Self {
            values: GenericDistinctBuffer::new(data_type.clone()),
        }
    }
}

impl<T: ArrowPrimitiveType + DistinctKey> Default for DistinctCountAccumulator<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ArrowPrimitiveType + DistinctKey + Debug> Accumulator
    for DistinctCountAccumulator<T>
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
