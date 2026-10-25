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

//! Inputs for the plans under test.
//!
//! [`MockSourceExec`] is a leaf plan that serves fixed batches and reports
//! properties (statistics, ordering, partitioning, constants, equalities) that
//! are verified against those batches. Checks treat what it reports as the
//! truth. Its
//! [`StreamBehavior`] controls what its streams do after serving their
//! batches: end, stall, fail or never end.
//!
//! [`SourceSpec`] describes a source to generate: its schema, how its rows are
//! split into partitions and batches, the value distribution, and the
//! properties the data must satisfy. It is a plain value, so tests and the
//! [`harness`] can derive many variations of an input from one spec.
//!
//! [`harness`]: crate::physical_plan::harness

mod source;
mod spec;
mod values;

pub(crate) use source::map_mock_leaves;
pub use source::{Equality, MockSourceExec, StatisticsPrecision, StreamBehavior};
pub use spec::{BatchLayout, COPY_SUFFIX, ConstantValues, ROW_ID_COLUMN, SourceSpec};
