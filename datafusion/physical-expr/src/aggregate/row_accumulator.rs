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

//! Accumulator over row format

use arrow::array::ArrayRef;
use datafusion_common::{Result, ScalarValue};
use datafusion_row::accessor::RowAccessor;
use std::fmt::Debug;

/// Row-based accumulator where the internal aggregate state(s) are stored using row format.
///
/// Unlike the [`datafusion_expr::Accumulator`], the [`RowAccumulator`] does not store the state internally.
/// Instead, it knows how to access/update the state stored in a row via the the provided accessor and
/// its state's starting field index in the row.
///
/// For example, we are evaluating `SELECT a, sum(b), avg(c), count(d) from GROUP BY a;`, we would have one row used as
/// aggregation state for each distinct `a` value, the index of the first and the only state of `sum(b)` would be 0,
/// the index of the first state of `avg(c)` would be 1, and the index of the first and only state of `cound(d)` would be 3:
///
/// sum(b) state_index = 0              count(d) state_index = 3
///        |                            |
///        v                            v
///        +--------+----------+--------+----------+
///        | sum(b) | count(c) | sum(c) | count(d) |
///        +--------+----------+--------+----------+
///                 ^
///                 |
///           avg(c) state_index = 1
///
pub trait RowAccumulator: Send + Sync + Debug {
    /// updates the accumulator's state from a vector of arrays.
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()>;

    /// updates the accumulator's state from a vector of states.
    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()>;

    /// returns its value based on its current state.
    fn evaluate(&self, accessor: &RowAccessor) -> Result<ScalarValue>;

    /// State's starting field index in the row.
    fn state_index(&self) -> usize;
}
