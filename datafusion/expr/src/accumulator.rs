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
use datafusion_common::{internal_err, DataFusionError, Result, ScalarValue};
use std::fmt::Debug;

/// Describes an aggregate functions's state.
///
/// `Accumulator`s are stateful objects that live throughout the
/// evaluation of multiple rows and aggregate multiple values together
/// into a final output aggregate.
///
/// An accumulator knows how to:
/// * update its state from inputs via [`update_batch`]
///
/// * compute the final value from its internal state via [`evaluate`]
///
/// * retract an update to its state from given inputs via
/// [`retract_batch`] (when used as a window aggregate [window
/// function])
///
/// * convert its internal state to a vector of aggregate values via
/// [`state`] and combine the state from multiple accumulators'
/// via [`merge_batch`], as part of efficient multi-phase grouping.
///
/// [`update_batch`]: Self::update_batch
/// [`retract_batch`]: Self::retract_batch
/// [`state`]: Self::state
/// [`evaluate`]: Self::evaluate
/// [`merge_batch`]: Self::merge_batch
/// [window function]: https://en.wikipedia.org/wiki/Window_function_(SQL)
pub trait Accumulator: Send + Sync + Debug {
    /// Updates the accumulator's state from its input.
    ///
    /// `values` contains the arguments to this aggregate function.
    ///
    /// For example, the `SUM` accumulator maintains a running sum,
    /// and `update_batch` adds each of the input values to the
    /// running sum.
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()>;

    /// Returns the final aggregate value.
    ///
    /// For example, the `SUM` accumulator maintains a running sum,
    /// and `evaluate` will produce that running sum as its output.
    fn evaluate(&self) -> Result<ScalarValue>;

    /// Returns the allocated size required for this accumulator, in
    /// bytes, including `Self`.
    ///
    /// This value is used to calculate the memory used during
    /// execution so DataFusion can stay within its allotted limit.
    ///
    /// "Allocated" means that for internal containers such as `Vec`,
    /// the `capacity` should be used not the `len`.
    fn size(&self) -> usize;

    /// Returns the intermediate state of the accumulator.
    ///
    /// Intermediate state is used for "multi-phase" grouping in
    /// DataFusion, where an aggregate is computed in parallel with
    /// multiple `Accumulator` instances, as illustrated below:
    ///
    /// # MultiPhase Grouping
    ///
    /// ```text
    ///                               ▲
    ///                               │                   evaluate() is called to
    ///                               │                   produce the final aggregate
    ///                               │                   value per group
    ///                               │
    ///                  ┌─────────────────────────┐
    ///                  │GroupBy                  │
    ///                  │(AggregateMode::Final)   │      state() is called for each
    ///                  │                         │      group and the resulting
    ///                  └─────────────────────────┘      RecordBatches passed to the
    ///                               ▲
    ///                               │
    ///              ┌────────────────┴───────────────┐
    ///              │                                │
    ///              │                                │
    /// ┌─────────────────────────┐      ┌─────────────────────────┐
    /// │        GroubyBy         │      │        GroubyBy         │
    /// │(AggregateMode::Partial) │      │(AggregateMode::Partial) │
    /// └─────────────────────────┘      └────────────▲────────────┘
    ///              ▲                                │
    ///              │                                │    update_batch() is called for
    ///              │                                │    each input RecordBatch
    ///         .─────────.                      .─────────.
    ///      ,─'           '─.                ,─'           '─.
    ///     ;      Input      :              ;      Input      :
    ///     :   Partition 0   ;              :   Partition 1   ;
    ///      ╲               ╱                ╲               ╱
    ///       '─.         ,─'                  '─.         ,─'
    ///          `───────'                        `───────'
    /// ```
    ///
    /// The partial state is serialied as `Arrays` and then combined
    /// with other partial states from different instances of this
    /// Accumulator (that ran on different partitions, for example).
    ///
    /// The state can be and often is a different type than the output
    /// type of the [`Accumulator`] and needs different merge
    /// operations (for example, the partial state for `COUNT` needs
    /// to be summed together)
    ///
    /// Some accumulators can return multiple values for their
    /// intermediate states. For example average, tracks `sum` and
    ///  `n`, and this function should return
    /// a vector of two values, sum and n.
    ///
    /// Note that [`ScalarValue::List`] can be used to pass multiple
    /// values if the number of intermediate values is not known at
    /// planning time (e.g. for `MEDIAN`)
    fn state(&self) -> Result<Vec<ScalarValue>>;

    /// Updates the accumulator's state from an `Array` containing one
    /// or more intermediate values.
    ///
    /// For some aggregates (such as `SUM`), merge_batch is the same
    /// as `update_batch`, but for some aggregrates (such as `COUNT`)
    /// the operations differ. See [`Self::state`] for more details on how
    /// state is used and merged.
    ///
    /// The `states` array passed was formed by concatenating the
    /// results of calling [`Self::state`] on zero or more other
    /// `Accumulator` instances.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()>;

    /// Retracts (removed) an update (caused by the given inputs) to
    /// accumulator's state.
    ///
    /// This is the inverse operation of [`Self::update_batch`] and is used
    /// to incrementally calculate window aggregates where the `OVER`
    /// clause defines a bounded window.
    ///
    /// # Example
    ///
    /// For example, given the following input partition
    ///
    /// ```text
    ///                     │      current      │
    ///                            window
    ///                     │                   │
    ///                ┌────┬────┬────┬────┬────┬────┬────┬────┬────┐
    ///     Input      │ A  │ B  │ C  │ D  │ E  │ F  │ G  │ H  │ I  │
    ///   partition    └────┴────┴────┴────┼────┴────┴────┴────┼────┘
    ///
    ///                                    │         next      │
    ///                                             window
    /// ```
    ///
    /// First, [`Self::evaluate`] will be called to produce the output
    /// for the current window.
    ///
    /// Then, to advance to the next window:
    ///
    /// First, [`Self::retract_batch`] will be called with the values
    /// that are leaving the window, `[B, C, D]` and then
    /// [`Self::update_batch`] will be called with the values that are
    /// entering the window, `[F, G, H]`.
    fn retract_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
        // TODO add retract for all accumulators
        internal_err!(
            "Retract should be implemented for aggregate functions when used with custom window frame queries"
        )
    }

    /// Does the accumulator support incrementally updating its value
    /// by *removing* values.
    ///
    /// If this function returns true, [`Self::retract_batch`] will be
    /// called for sliding window functions such as queries with an
    /// `OVER (ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)`
    fn supports_retract_batch(&self) -> bool {
        false
    }
}
