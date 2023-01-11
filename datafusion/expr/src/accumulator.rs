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
/// * retract an update to its state from given inputs via `retract_batch`
/// * convert its internal state to a vector of aggregate values
/// * update its state from multiple accumulators' states via `merge_batch`
/// * compute the final value from its internal state via `evaluate`
pub trait Accumulator: Send + Sync + Debug {
    /// Returns the partial intermediate state of the accumulator. This
    /// partial state is serialied as `Arrays` and then combined with
    /// other partial states from different instances of this
    /// accumulator (that ran on different partitions, for
    /// example).
    ///
    /// The state can be and often is a different type than the output
    /// type of the [`Accumulator`].
    ///
    /// See [`Self::merge_batch`] for more details on the merging process.
    ///
    /// Some accumulators can return multiple values for their
    /// intermediate states. For example average, tracks `sum` and
    ///  `n`, and this function should return
    /// a vector of two values, sum and n.
    ///
    /// `ScalarValue::List` can also be used to pass multiple values
    /// if the number of intermediate values is not known at planning
    /// time (e.g. median)
    fn state(&self) -> Result<Vec<ScalarValue>>;

    /// Updates the accumulator's state from a vector of arrays.
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()>;

    /// Retracts an update (caused by the given inputs) to
    /// accumulator's state.
    ///
    /// This is the inverse operation of [`Self::update_batch`] and is used
    /// to incrementally calculate window aggregates where the OVER
    /// clause defines a bounded window.
    fn retract_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
        // TODO add retract for all accumulators
        Err(DataFusionError::Internal(
            "Retract should be implemented for aggregate functions when used with custom window frame queries".to_string()
        ))
    }

    /// Updates the accumulator's state from an `Array` containing one
    /// or more intermediate values.
    ///
    /// The `states` array passed was formed by concatenating the
    /// results of calling `[state]` on zero or more other accumulator
    /// instances.
    ///
    /// `states`  is an array of the same types as returned by [`Self::state`]
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()>;

    /// Returns the final aggregate value based on its current state.
    fn evaluate(&self) -> Result<ScalarValue>;

    /// Allocated size required for this accumulator, in bytes, including `Self`.
    /// Allocated means that for internal containers such as `Vec`, the `capacity` should be used
    /// not the `len`
    fn size(&self) -> usize;
}
