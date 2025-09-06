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

//! Active NFA state-set management for `PatternMatcher`.
//!
//! The matcher represents the current simulation frontier as a **set of active
//! NFA states**.  For every new input row the engine computes the successor
//! states and then applies *deduplication* so that each distinct `state_id`
//! is kept only once – retaining the path with the better greedy/left-most
//! score.
//!
//! Why is this necessary?
//! * Different paths through the automaton may converge on the same state in
//!   the same row.  Evaluating them separately would be redundant and would
//!   balloon the state space exponentially.
//! * By discarding the worse path early we keep the algorithm’s per-row
//!   complexity **linear** in the number of NFA states.
//!
//! This module provides the `insert_or_update_next_state` helper which writes
//! into a dense arena (`DedupArena`).  The arena is:
//! * **Generation-tagged** – avoids clearing vectors every row.
//! * **Index-addressable** – constant-time look-ups by `state_id`.
//!
//! Closely related modules:
//! * `dedup.rs` – owns the arena data structure.
//! * `row_loop.rs` – high-level driver that allocates the arena per row.

use super::candidate_mod::Candidate;
use crate::match_recognize::{
    nfa::{ActiveNFAState, PathNode, PathScore},
    PatternMatcher,
};
use std::sync::Arc;

pub(crate) use super::dedup::DedupArena;

impl PatternMatcher {
    /// Insert a successor `state_id` produced for the current row into the dense arena used to
    /// deduplicate active states. If an entry for the same `state_id` already exists the path with
    /// the higher greedy-left-most score wins.
    #[inline]
    pub(crate) fn insert_or_update_next_state(
        &self,
        next_state_id: usize,
        new_node: Arc<PathNode>,
        new_score: PathScore,
        current_row: usize,
        arena: &mut DedupArena,
    ) {
        if arena.next_gen_flags[next_state_id] == arena.cur_row_gen {
            // Already have an entry for this state ⇒ keep the better path.
            let prev = arena.next_active_vec[next_state_id]
                .as_mut()
                .expect("generation flag implies Some entry");

            let prev_row = prev
                .path
                .as_ref()
                .map(|p| p.step.row)
                .unwrap_or(current_row);

            let alt_idx_new = self.compiled.nfa[next_state_id].alt_branch_idx;
            let alt_idx_prev = self.compiled.nfa[prev.state_id].alt_branch_idx;

            let new_cand = Candidate {
                score: new_score,
                state_id: next_state_id,
                row: current_row,
                alt_idx: alt_idx_new,
            };
            let prev_cand = Candidate {
                score: prev.score,
                state_id: prev.state_id,
                row: prev_row,
                alt_idx: alt_idx_prev,
            };

            if new_cand < prev_cand {
                prev.path = Some(new_node);
                prev.score = new_score;
            }
        } else {
            // First time we see this successor on this row.
            arena.next_gen_flags[next_state_id] = arena.cur_row_gen;
            arena.next_active_vec[next_state_id] = Some(ActiveNFAState {
                state_id: next_state_id,
                path: Some(new_node),
                score: new_score,
            });
            arena.populated_indices.push(next_state_id);
        }
    }
}
