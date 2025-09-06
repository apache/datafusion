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

//! ε‐closure utilities for `PatternMatcher`.
//!
//! Computes the **ε-closure** for any given NFA state – that is, the set of
//! states reachable via ε-transitions **without consuming a row**.  This is a
//! critical building-block used throughout the matcher’s row-scanning loop.
//!
//! Key features:
//! * **Anchor awareness** – When the compiled pattern contains `^` / `$`
//!   predicates the closure must honour them. The `AnchorMode` enum lets the
//!   caller decide whether anchors can match on the current virtual row.
//! * **Generation-based visitation** – We reuse a bitmap (`visited`) across
//!   rows and identify already-visited states via a monotonically increasing
//!   `Generation` counter, thereby avoiding costly `Vec::clear()`s inside hot
//!   loops.
//!
//! This module purposefully keeps the implementation **allocation-free** to
//! guarantee predictable latency on large partitions.

use super::generation::Generation;
use crate::match_recognize::{matcher::AnchorMode, nfa::AnchorPredicate, PatternMatcher};

impl PatternMatcher {
    /// Compute the ε-closure for `state_id` under the supplied `anchor_mode`.
    /// Results are written into `out`, which is cleared at the start.
    pub(crate) fn epsilon_closure(
        &self,
        state_id: usize,
        anchor_mode: AnchorMode,
        out: &mut Vec<usize>,
        visited: &mut [u32],
        epsilon_gen: &mut Generation,
    ) {
        match anchor_mode {
            AnchorMode::Ignore => {
                out.clear();
                out.extend_from_slice(&self.compiled.nfa[state_id].unconditional_closure);
            }
            AnchorMode::Check {
                virt_row,
                total_virt_rows,
            } => {
                let _epsilon_timer_guard =
                    self.metrics.as_ref().map(|m| m.epsilon_eval_time.timer());

                out.clear();
                let generation = epsilon_gen.current();
                let mut stack = vec![state_id];

                while let Some(sid) = stack.pop() {
                    if visited[sid] == generation {
                        continue;
                    }
                    visited[sid] = generation;
                    out.push(sid);

                    for &(dst, pred) in &self.compiled.nfa[sid].epsilon_transitions_pred {
                        let allowed = match pred {
                            None => true,
                            Some(AnchorPredicate::StartOfInput) => virt_row == 0,
                            Some(AnchorPredicate::EndOfInput) => {
                                virt_row + 1 == total_virt_rows
                            }
                        };
                        if allowed {
                            stack.push(dst);
                        }
                    }
                }

                // Advance generation counter and reset bitmap on wrap-around.
                epsilon_gen.advance(visited);
            }
        }
    }
}
