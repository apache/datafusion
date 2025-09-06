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

//! Outer row‐scanning loop that drives the pattern matcher.
//!
//! Algorithm overview (greedy / left-most):
//! 1. For every possible **start row** in the input partition we run `scan_from`.
//! 2. `scan_from` simulates the compiled NFA row-by-row, keeping a set of
//!    *active* NFA states.  When multiple complete matches compete for the same
//!    start row we choose the one with the longest (greedy) path; ties fall back
//!    to the left-most rule as defined by the SQL spec.
//! 3. ε-transitions and anchor predicates (^ / $) are handled via a pre-computed
//!    closure plus a light-weight generation bitmap.
//! 4. All scratch buffers that only live for the duration of a single physical
//!    row are bundled in `RowContext` to avoid long parameter lists and make
//!    the hot path self-documenting.

use super::candidate_mod::Candidate;
use super::dedup::DedupArena;
use super::generation::Generation;
use crate::match_recognize::nfa::{
    ActiveNFAState, BestMatch, PathNode, PathScore, PathStep, Sym,
};
use crate::match_recognize::{
    matcher::AnchorMode, nfa::RowIdx, pattern_exec::MatchAccumulator, PatternMatcher,
};
use arrow::array::{BooleanArray, RecordBatch};
use datafusion_common::Result;
use datafusion_expr::match_recognize::AfterMatchSkip;
use std::sync::Arc;

/// Scratch buffers shared amongst ε-closure operations for the current row.
struct EpsilonScratch<'a> {
    tmp: &'a mut Vec<usize>,
    inner: &'a mut Vec<usize>,
    visited: &'a mut [u32],
    gen: &'a mut Generation,
}

/// Dense arena used to deduplicate successor NFA states on the current row.
struct Arena<'a> {
    vec: &'a mut [Option<ActiveNFAState>],
    flags: &'a mut [u32],
    populated: &'a mut Vec<usize>,
    cur_gen: u32,
}

/// Row-local scratch context grouping all the mutable state that `scan_from`
/// needs while processing **one** physical row.  Passing a single reference to
/// this struct keeps helper method signatures tight and borrow-checker-friendly.
pub(super) struct RowContext<'a> {
    // immutable inputs
    matcher: &'a PatternMatcher,
    current_row: usize,
    total_rows: usize,
    matched_syms: &'a [usize],

    // mutable scratch groups
    epsilon: EpsilonScratch<'a>,
    arena: Arena<'a>,

    // candidate tracking
    best_match: &'a mut Option<BestMatch>,
    best_end_row: &'a mut Option<usize>,
}

impl<'a> RowContext<'a> {
    #[inline]
    pub fn advance_active_states(&mut self, active_states: &[ActiveNFAState]) {
        let _trans_guard = self
            .matcher
            .metrics
            .as_ref()
            .map(|m| m.transition_time.timer());

        let matcher = self.matcher;

        for active in active_states {
            let anchor_mode = matcher.anchor_mode(self.current_row, self.total_rows);
            matcher.epsilon_closure(
                active.state_id,
                anchor_mode,
                self.epsilon.tmp,
                self.epsilon.visited,
                self.epsilon.gen,
            );

            for &state_id in self.epsilon.tmp.iter() {
                for &sym_id in self.matched_syms {
                    let next_states =
                        &matcher.compiled.nfa[state_id].numeric_transitions[sym_id];
                    if next_states.is_empty() {
                        continue;
                    }

                    for &next_state_id in next_states {
                        if let Some(m) = &matcher.metrics {
                            m.nfa_state_transitions.add(1);
                        }
                        let sym_enum = Sym::from_index(sym_id);
                        let new_step = PathStep {
                            row: self.current_row,
                            sym: sym_enum,
                        };
                        let new_node = {
                            let _alloc_guard =
                                matcher.metrics.as_ref().map(|m| m.alloc_time.timer());
                            Arc::new(PathNode {
                                step: new_step,
                                prev: active.path.clone(),
                            })
                        };
                        let new_score = active.score.extend(sym_enum);

                        matcher.epsilon_closure(
                            next_state_id,
                            anchor_mode,
                            self.epsilon.inner,
                            self.epsilon.visited,
                            self.epsilon.gen,
                        );
                        if self
                            .epsilon
                            .inner
                            .iter()
                            .any(|&sid| matcher.compiled.nfa[sid].is_accepting)
                        {
                            matcher.push_candidate(
                                self.best_match,
                                self.best_end_row,
                                Arc::clone(&new_node),
                                new_score,
                                next_state_id,
                                self.current_row,
                            );
                        }

                        matcher.insert_or_update_next_state(
                            next_state_id,
                            new_node,
                            new_score,
                            self.current_row,
                            &mut DedupArena {
                                next_active_vec: self.arena.vec,
                                next_gen_flags: self.arena.flags,
                                populated_indices: self.arena.populated,
                                cur_row_gen: self.arena.cur_gen,
                            },
                        );
                    }
                }
            }
        }
    }

    #[inline]
    pub fn check_empty_accepting_states(&mut self, active_states: &[ActiveNFAState]) {
        let matcher = self.matcher;
        for active in active_states {
            let anchor_mode = matcher.anchor_mode(self.current_row, self.total_rows);
            matcher.epsilon_closure(
                active.state_id,
                anchor_mode,
                self.epsilon.inner,
                self.epsilon.visited,
                self.epsilon.gen,
            );
            if self
                .epsilon
                .inner
                .iter()
                .any(|&sid| matcher.compiled.nfa[sid].is_accepting)
            {
                let candidate_node = if let Some(ref node) = active.path {
                    Arc::clone(node)
                } else {
                    Arc::new(PathNode {
                        step: PathStep {
                            row: self.current_row,
                            sym: Sym::Empty,
                        },
                        prev: None,
                    })
                };
                matcher.push_candidate(
                    self.best_match,
                    self.best_end_row,
                    candidate_node,
                    active.score,
                    active.state_id,
                    self.current_row.saturating_sub(1),
                );
            }
        }
    }
}

impl PatternMatcher {
    /// Calculate the next logical row to start scanning from
    #[inline]
    fn advance_row(current_row: usize, best_end_row: Option<usize>) -> usize {
        best_end_row.map_or(current_row + 1, |last| last.max(current_row) + 1)
    }

    /// Determine which `AnchorMode` applies to a given virtual row.
    #[inline]
    fn anchor_mode(&self, virt_row: usize, total_virt_rows: usize) -> AnchorMode {
        if self.compiled.has_anchor_preds && self.is_anchor_row(virt_row, total_virt_rows)
        {
            AnchorMode::Check {
                virt_row,
                total_virt_rows,
            }
        } else {
            AnchorMode::Ignore
        }
    }

    /// Utility: returns true when the given logical row can satisfy ^ / $ anchors.
    /// Row 0 is the virtual StartOfInput, and the last virtual row (total_rows-1)
    /// allows EndOfInput.
    #[inline]
    fn is_anchor_row(&self, virt_row: usize, total_rows: usize) -> bool {
        virt_row == 0 || virt_row + 1 == total_rows
    }

    /// Try to find the longest match that *starts* at `row_idx`.
    ///
    /// Returns `(best_match, best_end_row)` where `best_match` is the path of
    /// (row, symbol) pairs including virtual ^ / $ rows and
    /// `best_end_row` is the last virtual row that belongs to that match (needed for SKIP logic).
    fn scan_from(
        &self,
        bool_columns: &[&BooleanArray],
        row_idx: usize,
        total_rows: usize,
        num_rows: usize,
    ) -> Result<(Option<Vec<PathStep>>, Option<usize>)> {
        let _nfa_timer_guard = self.metrics.as_ref().map(|m| m.nfa_eval_time.timer());
        let mut best_match: Option<BestMatch> = None;
        let mut best_end_row: Option<usize> = None;
        let mut current_row = row_idx;
        let mut active_states = vec![ActiveNFAState {
            state_id: 0,
            path: None,
            score: PathScore::default(),
        }];

        // Reusable buffers for ε-reachable states to avoid repeated allocations
        let mut reachable_tmp: Vec<usize> = Vec::new();
        let mut reachable_inner: Vec<usize> = Vec::new();
        let mut epsilon_visited: Vec<u32> = vec![0; self.compiled.nfa.len()];
        // Generation counter for the reusable visited bitmap
        let mut epsilon_gen: Generation = Generation::new();

        // Preallocate reusable buffer for symbol evaluations to avoid per-row allocations
        let mut symbol_matches: Vec<bool> = vec![false; self.compiled.id_to_symbol.len()];

        // Dense arena (indexed by `state_id`) for deduplicating successor states.
        let n_states = self.compiled.nfa.len();
        let mut next_active_vec: Vec<Option<ActiveNFAState>> = vec![None; n_states];
        let mut next_gen_flags: Vec<u32> = vec![0; n_states];
        let mut cur_row_gen: u32 = 1;

        // Track which entries we populated on the current row so that we can
        // iterate over them directly instead of scanning the entire dense vector afterwards.
        let mut populated_indices: Vec<usize> = Vec::new();

        while !active_states.is_empty() && current_row < total_rows {
            let _row_guard = self.metrics.as_ref().map(|m| m.row_loop_time.timer());
            if let Some(m) = &self.metrics {
                m.active_states_max.set_max(active_states.len());
            }
            // Bump generation counter instead of clearing the dense arena.
            populated_indices.clear();
            cur_row_gen = cur_row_gen.wrapping_add(1);
            if cur_row_gen == 0 {
                // Wrap-around – clear the flags vec cheaply.
                next_gen_flags.fill(0);
                cur_row_gen = 1;
            }

            // Evaluate symbol columns for the current physical row and
            // collect the IDs that matched.
            let matched_syms = self.get_matched_symbol_ids(
                bool_columns,
                current_row,
                num_rows,
                &mut symbol_matches,
            )?;

            if let Some(m) = &self.metrics {
                m.active_states_max.set_max(active_states.len());
            }

            // Row-local context groups all scratch state, reducing param noise.
            let mut ctx = RowContext {
                matcher: self,
                current_row,
                total_rows,
                matched_syms: &matched_syms,

                epsilon: EpsilonScratch {
                    tmp: &mut reachable_tmp,
                    inner: &mut reachable_inner,
                    visited: &mut epsilon_visited,
                    gen: &mut epsilon_gen,
                },

                arena: Arena {
                    vec: &mut next_active_vec,
                    flags: &mut next_gen_flags,
                    populated: &mut populated_indices,
                    cur_gen: cur_row_gen,
                },

                best_match: &mut best_match,
                best_end_row: &mut best_end_row,
            };

            ctx.advance_active_states(&active_states);
            ctx.check_empty_accepting_states(&active_states);

            // Prepare next iteration.
            active_states = populated_indices
                .iter()
                .map(|&idx| next_active_vec[idx].clone().unwrap())
                .collect();
            current_row += 1;
        }

        // Handle accepting states after the last physical row (ε-transitions only).
        for active in &active_states {
            let anchor_mode = self.anchor_mode(current_row, total_rows);
            self.epsilon_closure(
                active.state_id,
                anchor_mode,
                &mut reachable_inner,
                &mut epsilon_visited,
                &mut epsilon_gen,
            );
            if reachable_inner
                .iter()
                .any(|&sid| self.compiled.nfa[sid].is_accepting)
            {
                if let Some(ref node) = active.path {
                    self.push_candidate(
                        &mut best_match,
                        &mut best_end_row,
                        Arc::clone(node),
                        active.score,
                        active.state_id,
                        current_row.saturating_sub(1),
                    );
                }
            }
        }

        Ok((best_match.map(|bm| bm.path), best_end_row))
    }

    /// Primary entry point – process rows [`start_row`, `end_row`) of `batch` and emit matches via `out`.
    pub(crate) fn process_rows(
        &mut self,
        batch: &RecordBatch,
        start_row: usize,
        end_row: usize,
        out: &mut MatchAccumulator,
    ) -> Result<()> {
        debug_assert!(start_row <= end_row && end_row <= batch.num_rows());

        let _match_timer_guard =
            self.metrics.as_ref().map(|m| m.match_compute_time.timer());

        let partition_len = end_row - start_row;
        let total_rows = partition_len + 1; // +1 for virtual EndOfInput row

        // Slice Boolean columns once up-front and resolve them to `BooleanArray`
        // references.
        let slice_store = self.slice_symbol_arrays(batch, start_row, partition_len);
        let bool_columns = self.bool_columns_from_slices(&slice_store)?;

        let mut row_idx = 0; // relative to slice
        let mut last_matched_row: Option<usize> = None;

        while row_idx < total_rows {
            if last_matched_row.is_some_and(|last| row_idx <= last) {
                row_idx += 1;
                continue;
            }

            let (best_match, best_end_row) =
                self.scan_from(&bool_columns, row_idx, total_rows, partition_len)?;

            if let Some(ref path) = best_match {
                if !path.is_empty() {
                    let match_id = self.next_match_number;
                    self.next_match_number += 1;
                    self.record_match_aux(
                        match_id,
                        path,
                        start_row,
                        batch.num_rows(),
                        out,
                    );
                }
            }

            let (next_row_rel, new_last_match) =
                self.next_scan_row(row_idx, &best_match, best_end_row);
            last_matched_row = new_last_match;
            row_idx = next_row_rel;
        }

        Ok(())
    }

    /// Decide the next starting row for the outer scanning loop according to the SQL `AFTER MATCH SKIP` clause.
    fn next_scan_row(
        &self,
        row_idx: usize,
        best_match: &Option<Vec<PathStep>>,
        best_end_row: Option<usize>,
    ) -> (usize, Option<usize>) {
        let mut next_row = row_idx + 1;
        let mut new_last_matched_row: Option<usize> = None;

        match self.compiled.after_match_skip {
            AfterMatchSkip::PastLastRow => {
                next_row = Self::advance_row(row_idx, best_end_row);
                new_last_matched_row = best_end_row;
            }
            AfterMatchSkip::ToNextRow => {}
            AfterMatchSkip::ToFirst(ref sym) => {
                if let Some(&target_id) = self.compiled.symbol_to_id.get(sym) {
                    if let Some(ref path) = best_match {
                        if let Some(step) =
                            path.iter().find(|ps| ps.sym.to_index() == target_id)
                        {
                            next_row = step.row;
                        } else {
                            next_row = Self::advance_row(row_idx, best_end_row);
                            new_last_matched_row = best_end_row;
                        }
                    }
                }
            }
            AfterMatchSkip::ToLast(ref sym) => {
                if let Some(&target_id) = self.compiled.symbol_to_id.get(sym) {
                    if let Some(ref path) = best_match {
                        if let Some(step) =
                            path.iter().rev().find(|ps| ps.sym.to_index() == target_id)
                        {
                            next_row = step.row;
                        } else {
                            next_row = Self::advance_row(row_idx, best_end_row);
                            new_last_matched_row = best_end_row;
                        }
                    }
                }
            }
        }

        if next_row <= row_idx {
            next_row = row_idx + 1;
        }

        (next_row, new_last_matched_row)
    }

    /// Record a candidate match while applying greedy/left-most tie-break rules.
    #[inline]
    fn push_candidate(
        &self,
        best_match: &mut Option<BestMatch>,
        best_end_row: &mut Option<usize>,
        node: Arc<PathNode>,
        score: PathScore,
        state_id: usize,
        row: RowIdx,
    ) {
        let _cand_guard = self.metrics.as_ref().map(|m| m.candidate_time.timer());

        let alt_idx = self.compiled.nfa[state_id].alt_branch_idx;

        let cand = Candidate {
            score,
            state_id,
            row,
            alt_idx,
        };
        let should_update = best_match.as_ref().is_none_or(|cur| {
            let cur_cand = Candidate {
                score: cur.score,
                state_id: cur.state_id,
                row: cur.row,
                alt_idx: self.compiled.nfa[cur.state_id].alt_branch_idx,
            };
            cand < cur_cand
        });

        if !should_update {
            return;
        }

        let path_vec = crate::match_recognize::nfa::collect_path(&node);
        *best_end_row = Some(path_vec.last().map(|ps| ps.row).unwrap_or(row));
        *best_match = Some(BestMatch {
            path: path_vec,
            score,
            state_id,
            row,
        });
    }

    /// Emit a materialised match into the [`MatchAccumulator`].
    #[inline]
    fn record_match_aux(
        &self,
        match_id: u64,
        path_steps: &[PathStep],
        row_offset: usize,
        total_rows: usize,
        out: &mut MatchAccumulator,
    ) {
        let path_abs: Vec<PathStep> = path_steps
            .iter()
            .map(|ps| PathStep {
                row: ps.row + row_offset,
                ..*ps
            })
            .collect();

        // Propagate any internal error when materializing the match
        if let Err(e) = out.record_match(match_id, &path_abs, total_rows) {
            // Surface the error to the caller
            // Note: this function signature doesn't return Result, so bubble via panic is not desired.
            // Instead, use expect in the caller chain; but here we convert to a debug panic since
            // current call sites don't handle Result. If needed, plumb Result through API.
            panic!("record_match failed: {e}");
        }
    }

    /// Slice all symbol columns of `batch` for the given row range.
    /// Handles metrics timing internally.
    #[inline]
    fn slice_symbol_arrays(
        &self,
        batch: &RecordBatch,
        start_row: usize,
        len: usize,
    ) -> Vec<arrow::array::ArrayRef> {
        let _slice_timer_guard = self.metrics.as_ref().map(|m| m.slice_time.timer());

        self.symbol_columns
            .iter()
            .map(|sc| batch.column(sc.column_idx).slice(start_row, len))
            .collect()
    }

    /// Downcast the sliced `ArrayRef`s to `BooleanArray` references.
    #[inline]
    fn bool_columns_from_slices<'a>(
        &self,
        slice_store: &'a [arrow::array::ArrayRef],
    ) -> Result<Vec<&'a BooleanArray>> {
        let mut bool_columns: Vec<&'a BooleanArray> =
            Vec::with_capacity(slice_store.len());
        for (idx, arr) in slice_store.iter().enumerate() {
            let bool_arr =
                arr.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(format!(
                        "Column {} is not BooleanArray",
                        self.symbol_columns[idx].column_idx
                    ))
                })?;
            bool_columns.push(bool_arr);
        }
        Ok(bool_columns)
    }

    /// Evaluate symbol predicates and return the list of matched symbol IDs
    /// for the current row.
    #[inline]
    fn get_matched_symbol_ids(
        &self,
        bool_columns: &[&BooleanArray],
        row_idx: usize,
        num_rows: usize,
        symbol_matches: &mut [bool],
    ) -> Result<Vec<usize>> {
        self.evaluate_symbols_static(bool_columns, row_idx, num_rows, symbol_matches)?;
        Ok(Self::collect_matched_symbol_ids(symbol_matches))
    }
}
