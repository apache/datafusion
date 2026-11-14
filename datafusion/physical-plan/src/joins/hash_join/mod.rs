// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`HashJoinExec`]: implementation for hash join.
//!
//! # Regular(inner/left/right/full) Hash Join implementation
//!
//! There are three major layers during execution
//!
//! 1. [`RegularHashJoinStream::into_stream`]: the state machine entry point,
//!    driving the build, probe, and final output stages.
//! 2. [`BatchedJoinProcessor`]: probes the raw index, evaluates the residual filter,
//!    and coalesces output. Each lookup returns up to `batch_size` candidate pairs.
//! 3. [`Map`]: the raw build-side index (hash table or integer-key array map).
//!
//! [`RegularHashJoinStream::into_stream`]: regular_join::RegularHashJoinStream::into_stream
//! [`BatchedJoinProcessor`]: batched_join_processor::BatchedJoinProcessor
//! [`Map`]: crate::joins::Map

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
mod regular_join;
mod shared_bounds;
mod stream;
