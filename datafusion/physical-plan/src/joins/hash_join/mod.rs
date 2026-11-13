// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`HashJoinExec`]: build a key index, enumerate matching rows, apply join semantics.
//!

pub use exec::{HashJoinExec, HashJoinExecBuilder, PreparedHashJoinBuild};
/// # Public Only for Internal Use:
///
/// This is not a public API and is for internal use only; see [API policy] for details.
///
/// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
#[doc(hidden)]
pub use partitioned_hash_eval::HashTableLookupExpr;
pub use partitioned_hash_eval::{HashExpr, SeededRandomState};

mod batched_join_processor;
mod build;
mod exec;
mod inlist_builder;
mod not_in;
mod partitioned_hash_eval;
pub(crate) mod probe_cursor;
mod regular_join;
mod shared_bounds;
mod stream;
