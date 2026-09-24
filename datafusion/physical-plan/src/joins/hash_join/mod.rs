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

//! [`HashJoinExec`] Partitioned Hash Join Operator

pub use exec::{HashJoinExec, HashJoinExecBuilder};
/// # Public Only for Internal Use:
/// Exposed for expression serialization tests in `datafusion-proto`. Not part of the
/// supported public API.
/// See the [API health policy] for details.
///
/// [API health policy]: https://datafusion.apache.org/contributor-guide/api-health.html#datafusion-internal-public-apis
#[cfg(any(test, feature = "test_utils"))]
#[doc(hidden)]
pub use partitioned_hash_eval::HashTableLookupExpr;
pub use partitioned_hash_eval::{HashExpr, SeededRandomState};

mod exec;
mod inlist_builder;
mod partitioned_hash_eval;
mod probe_completion;
mod shared_bounds;
mod stream;
