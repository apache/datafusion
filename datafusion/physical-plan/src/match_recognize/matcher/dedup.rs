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

//! Deduplication arena utilities for `PatternMatcher`.
//!
//! Encapsulates the mutable buffers and generation flags required to
//! deduplicate successor NFA states produced for the **current physical row**.
//! Bundling these fields avoids unwieldy parameter lists and makes the hot
//! path self-documenting.
//!
//! Closely related modules:
//! * `state_set.rs` – algorithms that manipulate `DedupArena`
//! * `row_loop.rs`  – high-level row-scanning driver that allocates one
//!   `DedupArena` per row
//!
//! Performance notes:
//! The arena is reused across rows. Instead of clearing the dense vectors on
//! every iteration we employ a **per-row generation counter** to mark
//! populated entries, dramatically reducing time spent zeroing memory inside
//! tight loops.
//!
//! This file should remain **allocation-free** on the hot path.
use crate::match_recognize::nfa::ActiveNFAState;

/// Helper bundle that owns the mutable state used to deduplicate successor
/// NFA states on a given row.  Grouping these fields into a single struct
/// avoids a long parameter list and makes the intent explicit.
pub(crate) struct DedupArena<'a> {
    pub next_active_vec: &'a mut [Option<ActiveNFAState>],
    pub next_gen_flags: &'a mut [u32],
    pub populated_indices: &'a mut Vec<usize>,
    pub cur_row_gen: u32,
}
