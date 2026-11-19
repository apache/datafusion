// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`HashJoinExec`]: implementation for hash join.
//!
//! # Regular (inner/left/right/full) Hash Join implementation
//!
//! There are three major layers during execution:
//!
//! 1. [`RegularHashJoinStream::into_stream`]: the state machine entry point,
//!    driving the build, probe, and final output stages.
//! 2. [`BatchedJoinProcessor`]: looks up bounded probe batches, expands duplicate
//!    groups in chunks of at most `batch_size` pairs, applies residual filters and
//!    join semantics, and coalesces output.
//! 3. [`JoinIndexMap`]: the build-side index. It resolves equal keys to individual
//!    build rows or handles to contiguous groups of duplicate rows.
//!
//! [`RegularHashJoinStream::into_stream`]: regular_join::RegularHashJoinStream::into_stream
//! [`BatchedJoinProcessor`]: batched_join_processor::BatchedJoinProcessor
//! [`JoinIndexMap`]: crate::joins::join_index::JoinIndexMap

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
