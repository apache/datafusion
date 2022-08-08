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

//! Accumulator module contains the trait definition for aggregation function's accumulators.

use arrow::array::ArrayRef;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use std::fmt::Debug;

/// An accumulator represents a stateful object that lives throughout the evaluation of multiple rows and
/// generically accumulates values.
///
/// An accumulator knows how to:
/// * update its state from inputs via `update_batch`
/// * convert its internal state to a vector of aggregate values
/// * update its state from multiple accumulators' states via `merge_batch`
/// * compute the final value from its internal state via `evaluate`
pub trait Accumulator: Send + Sync + Debug {
    /// Returns the state of the accumulator at the end of the accumulation.
    /// in the case of an average on which we track `sum` and `n`, this function should return a vector
    /// of two values, sum and n.
    fn state(&self) -> Result<Vec<AggregateState>>;

    /// updates the accumulator's state from a vector of arrays.
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()>;

    /// updates the accumulator's state from a vector of states.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()>;

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<ScalarValue>;
}

/// Representation of internal accumulator state. Accumulators can potentially have a mix of
/// scalar and array values. It may be desirable to add custom aggregator states here as well
/// in the future (perhaps `Custom(Box<dyn Any>)`?).
#[derive(Debug)]
pub enum AggregateState {
    /// Simple scalar value. Note that `ScalarValue::List` can be used to pass multiple
    /// values around
    Scalar(ScalarValue),
    /// Arrays can be used instead of `ScalarValue::List` and could potentially have better
    /// performance with large data sets, although this has not been verified. It also allows
    /// for use of arrow kernels with less overhead.
    Array(ArrayRef),
}

impl AggregateState {
    /// Access the aggregate state as a scalar value. An error will occur if the
    /// state is not a scalar value.
    pub fn as_scalar(&self) -> Result<&ScalarValue> {
        match &self {
            Self::Scalar(v) => Ok(v),
            _ => Err(DataFusionError::Internal(
                "AggregateState is not a scalar aggregate".to_string(),
            )),
        }
    }

    /// Access the aggregate state as an array value.
    pub fn to_array(&self) -> ArrayRef {
        match &self {
            Self::Scalar(v) => v.to_array(),
            Self::Array(array) => array.clone(),
        }
    }
}
