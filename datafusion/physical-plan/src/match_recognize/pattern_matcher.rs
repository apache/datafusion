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

use arrow::array::{
    Array, BooleanArray, BooleanBuilder, RecordBatch, StringBuilder, UInt64Builder,
};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion_common::{HashMap, HashSet, Result};
use datafusion_expr::match_recognize::{
    AfterMatchSkip, EmptyMatchesMode, Pattern, RepetitionQuantifier, RowsPerMatch, Symbol,
};
use std::collections::VecDeque;

use std::sync::Arc;

/// Type alias for logical row index (includes virtual anchor rows).
type RowIdx = usize;

// Strongly-typed symbol identifier used throughout the matcher.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
enum Sym {
    /// Zero-length placeholder inserted for empty matches
    Empty,
    /// Virtual start-anchor "^"
    AnchorStart,
    /// Virtual end-anchor "$"
    AnchorEnd,
    /// User-defined pattern symbol (0-based ordinal)
    User(u16),
}

impl Sym {
    // convert symbolic enum to dense numeric index (0-based)
    const fn to_index(self) -> usize {
        match self {
            Sym::Empty => 0,
            Sym::AnchorStart => 1,
            Sym::AnchorEnd => 2,
            Sym::User(n) => 3 + n as usize,
        }
    }

    const fn from_index(idx: usize) -> Self {
        match idx {
            0 => Sym::Empty,
            1 => Sym::AnchorStart,
            2 => Sym::AnchorEnd,
            n => Sym::User((n - 3) as u16),
        }
    }

    fn is_special(self) -> bool {
        matches!(self, Sym::Empty | Sym::AnchorStart | Sym::AnchorEnd)
    }
}

impl From<usize> for Sym {
    fn from(value: usize) -> Self {
        Sym::from_index(value)
    }
}

impl From<Sym> for usize {
    fn from(sym: Sym) -> usize {
        sym.to_index()
    }
}

// Metadata for a symbol column. Keeps the numeric symbol id so that the hot
// path (`evaluate_symbols_static`) can avoid a hashmap lookup.
#[derive(Debug, Clone)]
struct SymbolColumn {
    name: String,
    id: usize,
    column_idx: Option<usize>,
}

// Symbol index map (see `Sym::to_index()`):
//   0=Empty, 1=^, 2=$, 3+=user-defined symbols (in declaration order)
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct PathStep {
    row: RowIdx,
    sym: Sym,
    excluded: bool,
}

/// Linked-list node sharing common prefixes (cheap push, no per-step clone).
#[derive(Debug)]
struct PathNode {
    step: PathStep,
    prev: Option<Arc<PathNode>>, // older steps (None for list tail)
}

/// Convert an `Arc`-linked path into a forward `Vec<PathStep>` (allocates once).
fn collect_path(node: &Arc<PathNode>) -> Vec<PathStep> {
    // Collect nodes in reverse order and flip once to obtain forward ordering.
    let mut steps: Vec<PathStep> = std::iter::successors(Some(node), |n| n.prev.as_ref())
        .map(|n| n.step)
        .collect();
    steps.reverse();
    steps
}

/// Cheap `(len, classified)` score used to pick the greedy/left-most match.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
struct PathScore {
    /// Total number of steps in the path
    len: u32,
    /// Number of *classified* (i.e. non-special) symbols
    classified: u32,
}

impl PathScore {
    /// Incremental update when a new symbol is appended.
    #[inline]
    fn extend(mut self, sym: Sym) -> Self {
        self.len += 1;
        if !sym.is_special() {
            self.classified += 1;
        }
        self
    }
}

// Comparator for greedy/left-most tie-breaking in the scan loop.
#[inline]
fn prefer(
    new_score: PathScore,
    new_state_id: usize,
    new_row: RowIdx,
    cur_score: PathScore,
    cur_state_id: usize,
    cur_row: RowIdx,
) -> bool {
    // Higher `PathScore` wins; ties are resolved by the left-most (smaller
    // `state_id`, then smaller `row`).  A single tuple comparison with
    // `Reverse` achieves this without explicit branching.
    use std::cmp::Reverse;
    (Reverse(new_score), new_state_id, new_row)
        < (Reverse(cur_score), cur_state_id, cur_row)
}

/// Represents a state in the pattern matching NFA
#[derive(Debug, Clone)]
struct NFAState {
    /// State identifier
    id: usize,
    /// Whether this is an accepting state (complete match)
    is_accepting: bool,
    /// If this state is reached immediately after consuming a symbol that
    /// was wrapped in Pattern::Exclude, the consumed row should be marked as
    /// excluded in the final output.  For all other states this field is
    /// `false`.
    is_excluded: bool,
    /// Transitions from this state: symbol -> set of next state IDs
    transitions: HashMap<String, HashSet<usize>>,
    /// Epsilon transitions (transitions that don't consume input)
    epsilon_transitions: HashSet<usize>,
    /// Pre-computed ε-closure for this state (includes the state itself).
    /// Stored densely as a sorted vector for cache-friendly iteration and deterministic order
    epsilon_closure: Vec<usize>,
}

/// Active NFA state for tracking path through the NFA
#[derive(Debug, Clone)]
struct ActiveNFAState {
    state_id: usize,
    path: Option<Arc<PathNode>>, // shared persistent path (None for empty)
    score: PathScore,            // cached summary for quick comparisons
}

/// Core pattern matching engine implementing NFA logic
pub struct PatternMatcher {
    /// Shared, immutable compiled representation.
    compiled: Arc<CompiledPattern>,
    /// Symbol names and their (lazily resolved) column indices inside the
    /// current `RecordBatch`. A value of `None` indicates that
    /// `update_column_indices` has not been called yet for the partition.
    symbol_columns: Vec<SymbolColumn>,
    /// Next match number to assign (run-time mutable state)
    next_match_number: u64,
}

impl PatternMatcher {
    /// Create a new `PatternMatcher` instance.
    pub fn new(
        pattern: Pattern,
        symbols: Vec<String>,
        after_match_skip: Option<AfterMatchSkip>,
        rows_per_match: Option<RowsPerMatch>,
    ) -> Result<Self> {
        // Compile once and share via Arc
        let compiled = Arc::new(CompiledPattern::compile(
            pattern,
            symbols.clone(),
            after_match_skip,
            rows_per_match,
        )?);

        // Map symbol names to column indices (resolved lazily per batch)
        let symbol_columns: Vec<SymbolColumn> = symbols
            .into_iter()
            .map(|s| {
                let id = *compiled.symbol_to_id.get(&s).ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(format!(
                        "Unknown symbol '{s}'",
                    ))
                })?;
                Ok(SymbolColumn {
                    name: s,
                    id,
                    column_idx: None,
                })
            })
            .collect::<Result<Vec<_>, datafusion_common::DataFusionError>>()?;

        Ok(Self {
            compiled,
            symbol_columns,
            next_match_number: 1,
        })
    }

    /// Resolve symbol columns in the provided `RecordBatch` schema.
    pub fn update_column_indices(&mut self, batch: &RecordBatch) -> Result<()> {
        let schema = batch.schema();
        for sc in &mut self.symbol_columns {
            let col_name = format!("__mr_symbol_{}", sc.name);
            let index = schema
                .fields()
                .iter()
                .position(|field| field.name() == &col_name)
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(format!(
                        "Symbol column '{}' not found in batch schema",
                        col_name
                    ))
                })?;
            sc.column_idx = Some(index);
        }
        Ok(())
    }

    /// Return a read-only reference to the ε-closure for the given `state_id`.
    fn epsilon_closure(&self, state_id: usize) -> &Vec<usize> {
        &self.compiled.states[state_id].epsilon_closure
    }

    /// Helper: convert a logical row index (`RowIdx`) into a physical row
    /// index inside the [`RecordBatch`]. Returns `Some(idx)` for real data
    /// rows and `None` for the virtual `^` (row 0) and `$` (`num_rows + 1`) anchors.
    #[inline(always)]
    const fn phys(row: RowIdx, num_rows: usize) -> Option<usize> {
        match row {
            0 => None,
            r if r == num_rows + 1 => None,
            r => Some(r - 1),
        }
    }

    /// Calculate the next logical row to start scanning from
    fn advance_row(current_row: usize, best_end_row: Option<usize>) -> usize {
        best_end_row.map_or(current_row + 1, |last| last.max(current_row) + 1)
    }

    /// Try to find the longest match that *starts* at `row_idx`.
    ///
    /// Returns `(best_match, best_end_row)` where `best_match` is the path of
    /// (row, symbol, was_excluded?) pairs including virtual ^ / $ rows and `best_end_row` is
    /// the last virtual row that belongs to that match (needed for SKIP logic).
    fn scan_from(
        &self,
        bool_columns: &[&BooleanArray],
        row_idx: usize,
        total_rows: usize,
        num_rows: usize,
    ) -> Result<(Option<Vec<PathStep>>, Option<usize>)> {
        let mut best_match: Option<BestMatch> = None;
        let mut best_end_row: Option<usize> = None;
        let mut current_row = row_idx;
        let mut active_states = vec![ActiveNFAState {
            state_id: 0,
            path: None,
            score: PathScore::default(),
        }];

        // Preallocate reusable buffer for symbol evaluations to avoid per-row allocations
        let mut symbol_matches: Vec<bool> = vec![false; self.compiled.id_to_symbol.len()];
        // Re-use a single HashMap for deduplication across rows to avoid
        // per-row bucket allocations. Its capacity is fixed to the number of
        // NFA states which is a tight upper bound for concurrently active
        // states.
        let mut next_active_map: HashMap<usize, ActiveNFAState> =
            HashMap::with_capacity(self.compiled.states.len());

        while !active_states.is_empty() && current_row < total_rows {
            // Clear the map instead of reallocating it on every row.
            next_active_map.clear();
            self.evaluate_symbols_static(
                bool_columns,
                current_row,
                num_rows,
                &mut symbol_matches,
            )?;
            // Use O(1) hashmap consolidation to deduplicate successor states
            // instead of the previous sort + linear merge.
            for active in &active_states {
                for &state_id in self.epsilon_closure(active.state_id) {
                    // Process symbol transitions for each symbol that matches on this row
                    for (sym_id, matched) in symbol_matches.iter().enumerate() {
                        if !*matched {
                            continue;
                        }

                        let next_states =
                            &self.compiled.numeric_transitions[state_id][sym_id];
                        if next_states.is_empty() {
                            continue;
                        }

                        for &next_state_id in next_states {
                            // Build new path (cheap small Vec clone)
                            let excl_flag =
                                self.compiled.states[next_state_id].is_excluded;
                            let sym_enum = Sym::from_index(sym_id);
                            let new_step = PathStep {
                                row: current_row,
                                sym: sym_enum,
                                excluded: excl_flag,
                            };
                            let new_node = Arc::new(PathNode {
                                step: new_step,
                                prev: active.path.clone(),
                            });
                            let new_score = active.score.extend(sym_enum);

                            if self
                                .epsilon_closure(next_state_id)
                                .iter()
                                .any(|&sid| self.compiled.states[sid].is_accepting)
                            {
                                self.push_candidate(
                                    &mut best_match,
                                    &mut best_end_row,
                                    new_node.clone(),
                                    new_score,
                                    next_state_id,
                                    current_row,
                                );
                            }

                            // Consolidate duplicates: keep the path that wins
                            // according to greedy-left-most semantics.
                            if let Some(prev) = next_active_map.get_mut(&next_state_id) {
                                // Retrieve the row of the previous path (always the current row)
                                let prev_row = prev
                                    .path
                                    .as_ref()
                                    .map(|p| p.step.row)
                                    .unwrap_or(current_row);

                                let better = prefer(
                                    new_score,
                                    next_state_id,
                                    current_row,
                                    prev.score,
                                    prev.state_id,
                                    prev_row,
                                );

                                if better {
                                    prev.path = Some(new_node.clone());
                                    prev.score = new_score;
                                    // `state_id` is identical, no update required
                                }
                            } else {
                                next_active_map.insert(
                                    next_state_id,
                                    ActiveNFAState {
                                        state_id: next_state_id,
                                        path: Some(new_node.clone()),
                                        score: new_score,
                                    },
                                );
                            }
                        }
                    }
                }
            }

            // Also check for accepting states in current active states (for patterns that can match empty sequences)
            for active in &active_states {
                for &state_id in self.epsilon_closure(active.state_id) {
                    if self.compiled.states[state_id].is_accepting {
                        // Ensure we have at least one step for zero-length matches
                        let candidate_node = if let Some(ref node) = active.path {
                            Arc::clone(node)
                        } else {
                            Arc::new(PathNode {
                                step: PathStep {
                                    row: current_row,
                                    sym: Sym::Empty,
                                    excluded: false,
                                },
                                prev: None,
                            })
                        };
                        self.push_candidate(
                            &mut best_match,
                            &mut best_end_row,
                            candidate_node,
                            active.score,
                            active.state_id,
                            current_row.saturating_sub(1),
                        );
                        break;
                    }
                }
            }

            // Flatten map into vector for next iteration without dropping the
            // allocated buckets so that the map can be reused in the next
            // loop iteration.
            active_states = next_active_map.drain().map(|(_, v)| v).collect();
            current_row += 1;
        }

        // Check remaining active states for accepting states reachable via epsilon transitions
        // This handles cases where we've consumed all input but can still reach accepting states
        for active in &active_states {
            let reachable = self.epsilon_closure(active.state_id);
            if reachable
                .iter()
                .any(|&sid| self.compiled.states[sid].is_accepting)
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

    /// Build the output RecordBatch from all collected matches
    fn build_output(
        &self,
        batch: &RecordBatch,
        all_matches: &[(u64, Vec<PathStep>)],
    ) -> Result<Option<RecordBatch>> {
        let num_rows = batch.num_rows();

        // Collect matched rows (duplicates allowed for overlapping matches)
        let mut matched_rows: Vec<u32> = Vec::new();

        // Use Arrow builders to avoid double allocation and copying.
        let mut classifier_builder = StringBuilder::new();
        let mut match_number_builder = UInt64Builder::new();
        let mut sequence_builder = UInt64Builder::new();
        let mut last_flag_builder = BooleanBuilder::new();
        let mut included_builder = BooleanBuilder::new();

        for (match_id, path) in all_matches {
            // Collect physical row indices in the order they appear in the path
            let physical_rows: Vec<(usize, usize, bool)> = path
                .iter()
                .filter_map(|ps| {
                    Self::phys(ps.row, num_rows)
                        .map(|phys_idx| (phys_idx, ps.sym.to_index(), ps.excluded))
                })
                .collect();

            let match_len = physical_rows.len() as u64;
            if match_len == 0 {
                continue;
            }

            let mut seq_counter: u64 = 0;

            for (i, (physical_idx, sym_id, excl_flag)) in
                physical_rows.into_iter().enumerate()
            {
                if physical_idx >= num_rows {
                    continue;
                }

                let included = !excl_flag;

                matched_rows.push(physical_idx as u32);

                // Metadata columns in the same order
                let class_sym = if sym_id == Sym::Empty.to_index() {
                    "(empty)".to_string()
                } else {
                    // Safe to unwrap: user symbols are always Some(..)
                    self.id_to_symbol[sym_id]
                        .clone()
                        .unwrap_or_else(|| "(unknown)".to_string())
                };
                classifier_builder.append_value(&class_sym);
                match_number_builder.append_value(*match_id);

                if included {
                    seq_counter += 1;
                    sequence_builder.append_value(seq_counter);
                } else {
                    sequence_builder.append_value(0_u64); // 0 indicates excluded / not counted
                }

                // Determine if this is the last row in this match
                let is_last = i == match_len as usize - 1;
                last_flag_builder.append_value(is_last);
                included_builder.append_value(included);
            }
        }

        // WITH UNMATCHED ROWS: augment vectors before building arrays
        if self.compiled.with_unmatched_rows {
            let mut seen = vec![false; num_rows];
            for &idx in &matched_rows {
                if (idx as usize) < num_rows {
                    seen[idx as usize] = true;
                }
            }

            for phys_idx in 0..num_rows {
                if !seen[phys_idx] {
                    matched_rows.push(phys_idx as u32);
                    classifier_builder.append_value("(empty)");
                    match_number_builder.append_value(0_u64);
                    sequence_builder.append_value(0_u64);
                    last_flag_builder.append_value(false);
                    included_builder.append_value(false);
                }
            }
        }

        if matched_rows.is_empty() {
            return Ok(None);
        }

        let output_schema = pattern_schema(batch.schema_ref());
        let matched_indices = arrow::array::UInt32Array::from(matched_rows);

        let mut output_columns = Vec::new();
        // Iterate over schema fields to access column names rather than array objects
        let schema = batch.schema();
        for (idx, _field) in schema.fields().iter().enumerate() {
            let col = batch.column(idx);
            let filtered_col: Arc<dyn Array> =
                arrow::compute::take(col, &matched_indices, None)?;
            output_columns.push(filtered_col);
        }

        let classifier_array = classifier_builder.finish();
        let match_number_array = match_number_builder.finish();
        let sequence_number_array = sequence_builder.finish();
        let last_flag_array = last_flag_builder.finish();
        let included_array = included_builder.finish();
        output_columns.push(Arc::new(classifier_array));
        output_columns.push(Arc::new(match_number_array));
        output_columns.push(Arc::new(sequence_number_array));
        output_columns.push(Arc::new(last_flag_array));
        output_columns.push(Arc::new(included_array));

        let output_batch = RecordBatch::try_new(output_schema, output_columns)?;
        Ok(Some(output_batch))
    }

    /// Populate `out` with symbol matches for `row_idx` (buffer reuse avoids allocs).
    fn evaluate_symbols_static(
        &self,
        bool_columns: &[&BooleanArray],
        row_idx: usize,
        num_rows: usize,
        out: &mut [bool],
    ) -> Result<()> {
        // Ensure the caller provided a correctly sized buffer.
        debug_assert_eq!(out.len(), self.compiled.id_to_symbol.len());

        // Reset all positions to false efficiently.
        out.fill(false);

        // Virtual start-anchor row (index 0)
        if row_idx == 0 {
            out[Sym::AnchorStart.to_index()] = true;
            return Ok(());
        }

        // Virtual end-anchor row (index num_rows + 1)
        if row_idx == num_rows + 1 {
            out[Sym::AnchorEnd.to_index()] = true;
            return Ok(());
        }

        // Real data rows: shift by 1 because of the virtual start row
        let physical_idx = row_idx - 1;

        for (i, sc) in self.symbol_columns.iter().enumerate() {
            let matched = bool_columns[i].value(physical_idx);
            out[sc.id] = matched;
        }

        Ok(())
    }

    /// Process a batch and return match results
    pub fn process_batch(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        // Update column indices based on the actual batch schema
        self.update_column_indices(&batch)?;

        // Build a cache of BooleanArray slices for all symbol columns to
        // avoid dynamic down-casts in the hot loop.
        let mut bool_columns: Vec<&BooleanArray> =
            Vec::with_capacity(self.symbol_columns.len());
        for sc in &self.symbol_columns {
            let idx = sc.column_idx.ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "Symbol column indices not initialised – did update_column_indices run?".into(),
                )
            })?;

            let column = batch.column(idx);
            let boolean_array = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(format!(
                        "Column {} is not a BooleanArray",
                        idx
                    ))
                })?;
            bool_columns.push(boolean_array);
        }

        let mut results = Vec::new();
        let num_rows = batch.num_rows();
        let total_rows = num_rows + 2; // include virtual ^ and $ rows

        let mut row_idx = 0;
        let mut last_matched_row: Option<usize> = None;
        let mut all_matches = Vec::new();

        while row_idx < total_rows {
            // If we're in a region that should be skipped (only relevant for
            // PAST_LAST_ROW) skip ahead one row.
            if last_matched_row.map_or(false, |last| row_idx <= last) {
                row_idx += 1;
                continue;
            }

            // Evaluate pattern starting at the current row
            let (best_match, best_end_row) =
                self.scan_from(&bool_columns, row_idx, total_rows, num_rows)?;

            // Track any successful match that contains at least one physical row
            if let Some(ref path_vec) = best_match {
                if Self::path_has_physical_rows(path_vec, num_rows) {
                    let match_id = self.next_match_number;
                    self.next_match_number += 1;
                    all_matches.push((match_id, path_vec.clone()));
                }
            }

            // Decide the next starting row depending on the AFTER MATCH SKIP
            // clause (default is PAST LAST ROW as per SQL spec).
            let (next_row, new_last_matched_row) =
                self.next_scan_row(row_idx, &best_match, best_end_row);

            // Update the end-of-match skip marker only for PAST_LAST_ROW. For
            // other policies we allow overlaps, so we clear the marker unless
            // explicitly set above.
            last_matched_row = new_last_matched_row;

            row_idx = next_row;
        }

        if let Some(output) = self.build_output(&batch, &all_matches)? {
            results.push(output);
        }
        Ok(results)
    }

    /// Compile pattern into NFA states using a single shared arena
    fn compile_pattern_to_nfa(pattern: &Pattern) -> Result<Vec<NFAState>> {
        // 1. Build the pattern into a shared arena and obtain its (start,end)
        //    indices.  The builder guarantees that the very first state
        //    inserted is the global start state with id 0.
        let mut states: Vec<NFAState> = Vec::new();
        let (_, end_id) = Self::build_pattern(&mut states, pattern)?;

        // 2. Append a single accepting sink state and wire ε from the local
        //    end state into that sink.  The sink inherits `is_excluded` if
        //    ANY predecessor carrying the ε-edge had it set.
        let excl_flag = states[end_id].is_excluded;
        let accept_id = Self::new_state(&mut states, true, excl_flag);
        Self::add_epsilon(&mut states, end_id, accept_id);

        // 3. Prune trivial ε-only alias states to shrink the automaton.
        let mut states = Self::prune_redundant_epsilons(states);

        // 4. Compute ε-closures *once* for every state and store them
        //    directly inside the state to avoid repeated look-ups at
        //    runtime.
        for sid in 0..states.len() {
            let mut reachable_set = HashSet::new();
            let mut stack = vec![sid];
            while let Some(id) = stack.pop() {
                if reachable_set.insert(id) {
                    for &eps in &states[id].epsilon_transitions {
                        stack.push(eps);
                    }
                }
            }
            // Convert to a densely packed, deterministic Vec
            let mut reachable: Vec<usize> = reachable_set.into_iter().collect();
            reachable.sort_unstable();
            states[sid].epsilon_closure = reachable;
        }

        Ok(states)
    }

    // Helper: create a fresh state and return its id.
    fn new_state(
        states: &mut Vec<NFAState>,
        is_accepting: bool,
        is_excluded: bool,
    ) -> usize {
        let id = states.len();
        states.push(NFAState {
            id,
            is_accepting,
            is_excluded,
            transitions: HashMap::new(),
            epsilon_transitions: HashSet::new(),
            epsilon_closure: Vec::new(),
        });
        id
    }

    // Helper: add ε-transition `from → to`, propagating the exclusion flag.
    fn add_epsilon(states: &mut Vec<NFAState>, from: usize, to: usize) {
        states[from].epsilon_transitions.insert(to);
        if states[from].is_excluded {
            states[to].is_excluded = true;
        }
    }

    // Recursive builder that constructs `pat` into `states` and returns its
    // local `(start,end)` indices.
    fn build_pattern(
        states: &mut Vec<NFAState>,
        pat: &Pattern,
    ) -> Result<(usize, usize)> {
        match pat {
            // Single symbol (or anchor)
            Pattern::Symbol(sym) => {
                let start = Self::new_state(states, false, false);
                let end = Self::new_state(states, false, false);

                let name = match sym {
                    Symbol::Named(n) => n.clone(),
                    Symbol::Start => "^".to_string(),
                    Symbol::End => "$".to_string(),
                };
                states[start]
                    .transitions
                    .entry(name)
                    .or_insert_with(HashSet::new)
                    .insert(end);
                Ok((start, end))
            }

            // Exclude wrapper – behaves like Symbol but marks `end`.
            Pattern::Exclude(sym) => {
                let (s, e) = Self::build_pattern(states, &Pattern::Symbol(sym.clone()))?;
                states[e].is_excluded = true;
                Ok((s, e))
            }

            // Concatenation – sequentially link sub-patterns.
            Pattern::Concat(parts) => {
                if parts.is_empty() {
                    return Err(datafusion_common::DataFusionError::Internal(
                        "Concatenation must contain at least one sub-pattern".into(),
                    ));
                }
                let mut iter = parts.iter();
                let (start, mut end) = Self::build_pattern(states, iter.next().unwrap())?;
                for p in iter {
                    let (s, e) = Self::build_pattern(states, p)?;
                    Self::add_epsilon(states, end, s);
                    end = e;
                }
                Ok((start, end))
            }

            // Alternation – classic Thompson construction.
            Pattern::Alternation(alts) => {
                if alts.is_empty() {
                    return Err(datafusion_common::DataFusionError::Internal(
                        "Alternation must contain at least one branch".into(),
                    ));
                }
                let start = Self::new_state(states, false, false);
                let mut ends = Vec::new();
                for sub in alts {
                    let (s, e) = Self::build_pattern(states, sub)?;
                    Self::add_epsilon(states, start, s);
                    ends.push(e);
                }
                let end = Self::new_state(states, false, false);
                for e in ends {
                    Self::add_epsilon(states, e, end);
                }
                Ok((start, end))
            }

            // Quantifiers / repetition.
            Pattern::Repetition(inner, quant) => {
                Self::compile_quantifier(states, inner, quant)
            }

            // Explicit grouping – just delegate to inner pattern.
            Pattern::Group(inner) => Self::build_pattern(states, inner),

            // PERMUTE – reuse existing helper and wrap to single end.
            Pattern::Permute(symbols) => {
                let sub = Self::compile_permute_pattern(symbols)?;
                let offset = states.len();
                let mut cloned = Self::copy_states_with_offset(&sub, offset);

                // Collect and clear original accepting states.
                let mut accepting_ids = Vec::new();
                for st in cloned.iter_mut() {
                    if st.is_accepting {
                        accepting_ids.push(st.id);
                        st.is_accepting = false;
                    }
                }

                let sub_start = offset; // first cloned id
                states.extend(cloned);

                let end = Self::new_state(states, false, false);
                for id in accepting_ids {
                    Self::add_epsilon(states, id, end);
                }
                Ok((sub_start, end))
            }
        }
    }

    // Quantifier builder helper.
    fn compile_quantifier(
        states: &mut Vec<NFAState>,
        inner: &Pattern,
        quant: &RepetitionQuantifier,
    ) -> Result<(usize, usize)> {
        let (min, max, add_loop) = match quant {
            RepetitionQuantifier::OneOrMore => (1usize, None, true),
            RepetitionQuantifier::ZeroOrMore => (0, None, true),
            RepetitionQuantifier::AtMostOne => (0, Some(1usize), false),
            RepetitionQuantifier::Exactly(n) => (*n as usize, Some(*n as usize), false),
            RepetitionQuantifier::AtLeast(n) => (*n as usize, None, true),
            RepetitionQuantifier::AtMost(n) => (0, Some(*n as usize), false),
            RepetitionQuantifier::Range(a, b) => (*a as usize, Some(*b as usize), false),
        };

        // Special-case the degenerate `{0}` pattern
        if min == 0 && max == Some(0) {
            let s = Self::new_state(states, false, false);
            return Ok((s, s));
        }

        // 1. Optional dummy entry node for min==0 so that we always return a
        //    valid start even when the first copy is optional.
        let entry = if min == 0 {
            Some(Self::new_state(states, false, false))
        } else {
            None
        };

        let mut first_start: usize = entry.unwrap_or(usize::MAX);
        let mut last_end: usize = entry.unwrap_or(usize::MAX);
        let mut last_copy_start: usize = usize::MAX; // needed for looping

        // 2. Mandatory copies
        for i in 0..min {
            let (s, e) = Self::build_pattern(states, inner)?;
            if i == 0 {
                first_start = if entry.is_some() { entry.unwrap() } else { s };
            }
            if last_end != usize::MAX {
                Self::add_epsilon(states, last_end, s);
            }
            last_end = e;
            last_copy_start = s;
        }

        // 3. Optional bounded copies
        if let Some(max_bound) = max {
            let mut remaining = max_bound.saturating_sub(min);
            while remaining > 0 {
                let (s, e) = Self::build_pattern(states, inner)?;
                Self::add_epsilon(states, last_end, s);
                last_end = e;
                remaining -= 1;
            }
            // For min == 0 we still need a direct skip path entry→end
            if min == 0 {
                Self::add_epsilon(states, first_start, last_end);
            }
        }

        // 4. Unbounded loop
        if max.is_none() && add_loop {
            if last_copy_start == usize::MAX {
                // We haven't built any real copy yet (ZeroOrMore)
                let (s, e) = Self::build_pattern(states, inner)?;
                Self::add_epsilon(states, first_start, s);
                last_copy_start = s;
                last_end = e;
            }
            Self::add_epsilon(states, last_end, last_copy_start);
            if min == 0 {
                Self::add_epsilon(states, first_start, last_end);
            }
        }

        Ok((first_start, last_end))
    }

    /// Remove alias states (non-accepting, no symbol edges, exactly one ε-edge)
    /// and re-index the remaining states.  This shrinks deeply nested NFAs and
    /// speeds up the inner scan loop without changing the accepted language.
    fn prune_redundant_epsilons(states: Vec<NFAState>) -> Vec<NFAState> {
        // The algorithm walks the state vector three times:
        //   1. mark removable aliases,
        //   2. build a redirect table that skips them, and
        //   3. emit a compacted state list with remapped edges.

        let n = states.len();
        if n <= 1 {
            return states; // nothing to do
        }

        // Pass 1 – mark alias states.
        let mut removable = vec![false; n];
        for st in &states {
            if st.id != 0 // never remove the global start state
                && !st.is_accepting
                && st.transitions.is_empty()
                && st.epsilon_transitions.len() == 1
            {
                removable[st.id] = true;
            }
        }
        if !removable.iter().any(|&b| b) {
            return states; // no aliases – fast path
        }

        // Pass 2 – build redirect table (`redirect[id]`).
        let mut redirect: Vec<usize> = (0..n).collect();
        for id in 0..n {
            if removable[id] {
                // Safe unwrap – removable states have exactly one ε target
                redirect[id] = *states[id].epsilon_transitions.iter().next().unwrap();
            }
        }

        // Follow chains until a non-removable state is reached.
        for id in 0..n {
            let mut dst = redirect[id];
            while removable[dst] {
                dst = redirect[dst];
            }
            redirect[id] = dst;
        }

        // Pass 3a – assign fresh contiguous ids to surviving states.
        let mut new_id_map = vec![usize::MAX; n];
        let mut next_id = 0;
        for old_id in 0..n {
            if !removable[old_id] {
                new_id_map[old_id] = next_id;
                next_id += 1;
            }
        }

        // Pass 3b – propagate `is_excluded` flags via `redirect`.
        let mut propagated_excl = vec![false; n];
        for old_id in 0..n {
            if states[old_id].is_excluded {
                let keeper = redirect[old_id];
                propagated_excl[keeper] = true;
            }
        }

        // Pass 3c – build the final, compact state vector.
        let mut new_states: Vec<NFAState> = Vec::with_capacity(next_id);
        for old_id in 0..n {
            if removable[old_id] {
                continue; // drop alias state
            }

            let mut st = states[old_id].clone();
            let new_id = new_id_map[old_id];
            st.id = new_id;
            st.is_excluded |= propagated_excl[old_id];

            // Remap ε-transitions
            st.epsilon_transitions = st
                .epsilon_transitions
                .iter()
                .map(|&dst| redirect[dst]) // bypass aliases
                .filter(|&dst| !removable[dst])
                .map(|dst| new_id_map[dst])
                .collect::<HashSet<_>>();

            // Remap symbol transitions
            let mut new_trans: HashMap<String, HashSet<usize>> = HashMap::new();
            for (sym, dests) in &st.transitions {
                let mapped: HashSet<usize> = dests
                    .iter()
                    .map(|&dst| redirect[dst])
                    .filter(|&dst| !removable[dst])
                    .map(|dst| new_id_map[dst])
                    .collect();
                if !mapped.is_empty() {
                    new_trans.insert(sym.clone(), mapped);
                }
            }
            st.transitions = new_trans;

            new_states.push(st);
        }

        // Ensure deterministic order (ids 0..len-1 ascending)
        new_states.sort_by_key(|s| s.id);
        new_states
    }

    /// Reset internal counters so the matcher can process a new partition.
    pub fn reset(&mut self) {
        self.next_match_number = 1;
    }

    /// Helper: clone a block of states with an ID offset, adjusting transitions & epsilons
    fn copy_states_with_offset(states: &[NFAState], offset: usize) -> Vec<NFAState> {
        let mut cloned = Vec::with_capacity(states.len());
        for s in states {
            let mut new_state = s.clone();
            new_state.id = s.id + offset;
            new_state.transitions = s
                .transitions
                .iter()
                .map(|(sym, set)| {
                    let new_set: HashSet<usize> =
                        set.iter().map(|t| t + offset).collect();
                    (sym.clone(), new_set)
                })
                .collect();
            new_state.epsilon_transitions = s
                .epsilon_transitions
                .iter()
                .map(|t| t + offset)
                .collect::<HashSet<_>>();
            // ε-closure will be recomputed after cloning
            new_state.epsilon_closure = Vec::new();
            cloned.push(new_state);
        }
        cloned
    }

    /// Compile a PERMUTE(S) pattern where the order of the symbols does not
    /// matter.  The NFA tracks per-symbol usage counts instead of enumerating
    /// all n! permutations, yielding far fewer states for duplicates.
    fn compile_permute_pattern(symbols: &[Symbol]) -> Result<Vec<NFAState>> {
        if symbols.is_empty() {
            return Err(datafusion_common::DataFusionError::Internal(
                "PERMUTE pattern must contain at least one symbol".to_string(),
            ));
        }

        // 1.  Collect unique symbols (in insertion order) and their required
        //     multiplicities.
        let mut unique_syms: Vec<String> = Vec::new();
        let mut counts_total: Vec<usize> = Vec::new();
        let mut index_of: HashMap<String, usize> = HashMap::new();

        for sym in symbols {
            let name = match sym {
                Symbol::Named(s) => s.clone(),
                Symbol::Start => "^".to_string(),
                Symbol::End => "$".to_string(),
            };
            if let Some(idx) = index_of.get(&name) {
                counts_total[*idx] += 1;
            } else {
                let idx = unique_syms.len();
                unique_syms.push(name.clone());
                counts_total.push(1);
                index_of.insert(name, idx);
            }
        }

        // Each state is identified by a vector of usage counters, one per
        // distinct symbol.
        type Key = Vec<usize>;
        let mut key_to_id: HashMap<Key, usize> = HashMap::new();
        let mut states: Vec<NFAState> = Vec::new();
        let mut queue: VecDeque<Key> = VecDeque::new();

        // Start state: no symbol has been consumed yet.
        let start_key: Key = vec![0; unique_syms.len()];
        key_to_id.insert(start_key.clone(), 0);
        states.push(NFAState {
            id: 0,
            is_accepting: false, // will be updated later if n==0
            is_excluded: false,
            transitions: HashMap::new(),
            epsilon_transitions: HashSet::new(),
            epsilon_closure: Vec::new(),
        });
        queue.push_back(start_key);

        while let Some(key) = queue.pop_front() {
            let state_id = *key_to_id.get(&key).unwrap();
            let is_accepting = key == counts_total;
            states[state_id].is_accepting = is_accepting;

            // For every symbol that can still be taken, add a transition.
            for (idx, used) in key.iter().enumerate() {
                if *used < counts_total[idx] {
                    let mut next_key = key.clone();
                    next_key[idx] += 1;

                    let next_id = if let Some(id) = key_to_id.get(&next_key) {
                        *id
                    } else {
                        let id = states.len();
                        key_to_id.insert(next_key.clone(), id);
                        states.push(NFAState {
                            id,
                            is_accepting: false,
                            is_excluded: false,
                            transitions: HashMap::new(),
                            epsilon_transitions: HashSet::new(),
                            epsilon_closure: Vec::new(),
                        });
                        queue.push_back(next_key.clone());
                        id
                    };

                    let sym_name = unique_syms[idx].clone();
                    states[state_id]
                        .transitions
                        .entry(sym_name)
                        .or_insert_with(HashSet::new)
                        .insert(next_id);
                }
            }
        }

        Ok(states)
    }

    /// Decide the next starting row for the outer scanning loop according to
    /// the SQL `AFTER MATCH SKIP` clause and return `(next_row, last_matched_row_marker)`.
    ///
    /// * `row_idx` – the row where the last scan started.
    /// * `best_match` – the best match (if any) found starting at `row_idx`.
    /// * `best_end_row` – the logical row index of the last row belonging to
    ///   `best_match` (including virtual rows).
    fn next_scan_row(
        &self,
        row_idx: usize,
        best_match: &Option<Vec<PathStep>>,
        best_end_row: Option<usize>,
    ) -> (usize, Option<usize>) {
        let skip_policy = self
            .compiled
            .after_match_skip
            .clone()
            .unwrap_or(AfterMatchSkip::PastLastRow);

        // Default when there was no match
        let mut next_row = row_idx + 1;
        let mut new_last_matched_row: Option<usize> = None;

        match skip_policy {
            AfterMatchSkip::PastLastRow => {
                // Standard behaviour – skip past the end of the match (or single row when no match)
                next_row = Self::advance_row(row_idx, best_end_row);
                new_last_matched_row = best_end_row;
            }
            AfterMatchSkip::ToNextRow => {
                next_row = row_idx + 1;
            }
            AfterMatchSkip::ToFirst(ref symbol) => {
                if let Some(&target_id) = self.compiled.symbol_to_id.get(symbol) {
                    if let Some(ref path) = best_match {
                        if let Some(step) =
                            path.iter().find(|ps| ps.sym.to_index() == target_id)
                        {
                            next_row = step.row;
                        } else {
                            // Fallback to PastLastRow semantics if symbol not found
                            next_row = Self::advance_row(row_idx, best_end_row);
                            new_last_matched_row = best_end_row;
                        }
                    }
                }
            }
            AfterMatchSkip::ToLast(ref symbol) => {
                if let Some(&target_id) = self.compiled.symbol_to_id.get(symbol) {
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

        // Ensure forward progress in all cases to avoid infinite loops
        if next_row <= row_idx {
            next_row = row_idx + 1;
        }

        (next_row, new_last_matched_row)
    }

    /// Check if the provided path contains at least one *physical* row that
    /// belongs to the underlying `RecordBatch`. This uses [`Self::phys`] so
    /// the logic for distinguishing virtual from physical rows is kept in a
    // single place.
    #[inline]
    fn path_has_physical_rows(path: &[PathStep], num_rows: usize) -> bool {
        path.iter().any(|ps| Self::phys(ps.row, num_rows).is_some())
    }

    /// Helper that converts a `PathNode` into a materialised `BestMatch` and
    /// feeds it into `maybe_update_best`, avoiding duplicate boiler-plate in
    /// the inner scan loop.
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
        // First decide if this candidate would beat the current best match
        // *without* materialising its full path – this avoids many short-lived
        // allocations for losing candidates.
        let should_update = best_match.as_ref().map_or(true, |cur| {
            prefer(score, state_id, row, cur.score, cur.state_id, cur.row)
        });

        if !should_update {
            return; // current best wins – nothing to do
        }

        // Materialise the path only for the winning candidate.
        let path_vec = collect_path(&node);

        *best_end_row = Some(path_vec.last().map(|ps| ps.row).unwrap_or(row));

        *best_match = Some(BestMatch {
            path: path_vec,
            score,
            state_id,
            row,
        });
    }
}

pub(crate) fn pattern_schema(input_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = input_schema.fields().to_vec();

    // Helper to reduce boilerplate when defining the extra metadata columns
    #[inline]
    fn meta(name: &str, data_type: arrow::datatypes::DataType) -> Arc<Field> {
        Arc::new(Field::new(name, data_type, false))
    }

    // Add match metadata columns
    fields.push(meta("__mr_classifier", arrow::datatypes::DataType::Utf8));
    fields.push(meta(
        "__mr_match_number",
        arrow::datatypes::DataType::UInt64,
    ));
    fields.push(meta(
        "__mr_match_sequence_number",
        arrow::datatypes::DataType::UInt64,
    ));
    fields.push(meta(
        "__mr_is_last_match_row",
        arrow::datatypes::DataType::Boolean,
    ));
    fields.push(meta(
        "__mr_is_included_row",
        arrow::datatypes::DataType::Boolean,
    ));

    Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ))
}

// =====================================================================
// Immutable compile-time representation
// =====================================================================

/// Compiled, thread-safe representation of a MATCH_RECOGNIZE pattern.
#[derive(Debug)]
pub struct CompiledPattern {
    pub(crate) id_to_symbol: Vec<Option<String>>,
    pub(crate) symbol_to_id: HashMap<String, usize>,
    pub(crate) numeric_transitions: Vec<Vec<Vec<usize>>>,
    // Internal NFA states; kept private to avoid exposing private type `NFAState`.
    states: Vec<NFAState>,
    pub(crate) after_match_skip: Option<AfterMatchSkip>,
    pub(crate) with_unmatched_rows: bool,
}

impl CompiledPattern {
    pub fn compile(
        pattern: Pattern,
        symbols: Vec<String>,
        after_match_skip: Option<AfterMatchSkip>,
        rows_per_match: Option<RowsPerMatch>,
    ) -> Result<Self> {
        let mut states = PatternMatcher::compile_pattern_to_nfa(&pattern)?;
        let with_unmatched_rows = matches!(
            rows_per_match,
            Some(RowsPerMatch::AllRows(Some(EmptyMatchesMode::WithUnmatched)))
        );
        if with_unmatched_rows
            && pattern_any(&pattern, |p| matches!(p, Pattern::Exclude(_)))
        {
            return Err(datafusion_common::DataFusionError::Plan(
                "WITH UNMATCHED ROWS is not supported when the pattern contains EXCLUDE clauses".into(),
            ));
        }

        // Symbol mapping (anchors + user symbols) & dense transitions
        // 0,1,2 are reserved for special symbols; user symbols start at index 3
        let mut id_to_symbol: Vec<Option<String>> = vec![None; Sym::User(0).to_index()];

        let mut symbol_to_id: HashMap<String, usize> = HashMap::new();
        // Anchors
        symbol_to_id.insert("^".to_string(), Sym::AnchorStart.to_index());
        symbol_to_id.insert("$".to_string(), Sym::AnchorEnd.to_index());

        let mut next_id = Sym::User(0).to_index();
        for s in &symbols {
            symbol_to_id.insert(s.clone(), next_id);
            id_to_symbol.push(Some(s.clone()));
            next_id += 1;
        }

        let num_symbols = id_to_symbol.len();
        let mut numeric_transitions = vec![vec![Vec::new(); num_symbols]; states.len()];
        for (sid, st) in states.iter().enumerate() {
            for (sym, dests) in &st.transitions {
                if let Some(&sym_id) = symbol_to_id.get(sym) {
                    numeric_transitions[sid][sym_id] = dests.iter().copied().collect();
                }
            }
        }

        // The dense numeric transition matrix is all we need at runtime.
        // Free the (often large) per-state `HashMap` to significantly reduce
        // memory footprint, especially for complex patterns (e.g. large
        // PERMUTE).  This trades a tiny amount of upfront work for a
        // substantial reduction in live heap usage.
        // Keep only the dense transition matrix; drop bulky per-state HashMaps.
        for st in states.iter_mut() {
            st.transitions = HashMap::new();
        }

        // Reclaim unused capacity after stripping transitions
        // Trim vector capacity
        states.shrink_to_fit();

        Ok(Self {
            id_to_symbol,
            symbol_to_id,
            numeric_transitions,
            states,
            after_match_skip,
            with_unmatched_rows,
        })
    }
}

// =====================================================================
// Generic Pattern traversal helpers
// =====================================================================

/// Returns `true` if *any* node in the pattern satisfies `pred`.
fn pattern_any<F>(pat: &Pattern, mut pred: F) -> bool
where
    F: FnMut(&Pattern) -> bool,
{
    // Depth-first walk using a small helper. `matches!` identifies leaf nodes.
    fn walk<'a, F>(node: &'a Pattern, pred: &mut F) -> bool
    where
        F: FnMut(&'a Pattern) -> bool,
    {
        if (pred)(node) {
            return true;
        }

        match node {
            Pattern::Concat(parts) | Pattern::Alternation(parts) => {
                parts.iter().any(|p| walk(p, pred))
            }
            Pattern::Repetition(inner, _) | Pattern::Group(inner) => walk(inner, pred),
            Pattern::Permute(_) | Pattern::Symbol(_) | Pattern::Exclude(_) => false,
        }
    }

    walk(pat, &mut pred)
}

// Provide transparent access to compile-time fields (e.g. `self.id_to_symbol`).
impl std::ops::Deref for PatternMatcher {
    type Target = CompiledPattern;
    fn deref(&self) -> &Self::Target {
        &self.compiled
    }
}

// Best-match envelope used only on the *cold* path when a complete match is
// found.  It carries the materialised path (needed later for output / SKIP
// logic) plus cheap metadata that allows us to compare two matches via
// `prefer` without walking the vector again.
#[derive(Clone)]
struct BestMatch {
    path: Vec<PathStep>,
    score: PathScore,
    state_id: usize,
    row: RowIdx,
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_snapshot;

    // Minimal helper duplicating the snapshot stringification
    fn nfa_str(p: &Pattern) -> String {
        let states = PatternMatcher::prune_redundant_epsilons(
            PatternMatcher::compile_pattern_to_nfa(p).expect("compile failed"),
        );

        let mut lines = Vec::new();
        let mut sorted_states: Vec<&NFAState> = states.iter().collect();
        sorted_states.sort_by_key(|s| s.id);

        for st in sorted_states {
            let mut parts = Vec::new();
            if !st.epsilon_transitions.is_empty() {
                let mut dests: Vec<_> = st.epsilon_transitions.iter().copied().collect();
                dests.sort_unstable();
                parts.push(format!(
                    "ε->{}",
                    dests
                        .iter()
                        .map(|d| d.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                ));
            }
            let mut keys: Vec<_> = st.transitions.keys().collect();
            keys.sort();
            for sym in keys {
                let mut dests: Vec<_> = st.transitions[sym].iter().copied().collect();
                dests.sort_unstable();
                parts.push(format!(
                    "{}->{}",
                    sym,
                    dests
                        .iter()
                        .map(|d| d.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                ));
            }
            let transitions = if parts.is_empty() {
                String::new()
            } else {
                format!(" {}", parts.join(" | "))
            };
            let line = if st.is_accepting {
                format!("{}*:{}", st.id, transitions)
            } else {
                format!("{}:{}", st.id, transitions)
            };
            lines.push(line);
        }
        lines.join("\n")
    }

    #[test]
    fn snapshot_cases() {
        use RepetitionQuantifier::*;
        use Symbol::*;

        let cases: &[(&str, Pattern)] = &[
            // Simple symbol / anchor
            ("single_symbol", Pattern::Symbol(Named("A".into()))),
            ("start_anchor", Pattern::Symbol(Start)),
            // Repetition / concatenation
            (
                "triple_a",
                Pattern::Concat(vec![
                    Pattern::Symbol(Named("A".into())),
                    Pattern::Symbol(Named("A".into())),
                    Pattern::Symbol(Named("A".into())),
                ]),
            ),
            (
                "a_optional",
                Pattern::Repetition(
                    Box::new(Pattern::Symbol(Named("A".into()))),
                    AtMostOne,
                ),
            ),
            (
                "a_at_most_three",
                Pattern::Repetition(
                    Box::new(Pattern::Symbol(Named("A".into()))),
                    AtMost(3),
                ),
            ),
            (
                "concat_ab",
                Pattern::Concat(vec![
                    Pattern::Symbol(Named("A".into())),
                    Pattern::Symbol(Named("B".into())),
                ]),
            ),
            (
                "concat_abc",
                Pattern::Concat(vec![
                    Pattern::Symbol(Named("A".into())),
                    Pattern::Symbol(Named("B".into())),
                    Pattern::Symbol(Named("C".into())),
                ]),
            ),
            // Anchors in concat
            (
                "start_anchor_a",
                Pattern::Concat(vec![
                    Pattern::Symbol(Start),
                    Pattern::Symbol(Named("A".into())),
                ]),
            ),
            (
                "a_end_anchor",
                Pattern::Concat(vec![
                    Pattern::Symbol(Named("A".into())),
                    Pattern::Symbol(End),
                ]),
            ),
            // Alternations
            (
                "alternation_ab",
                Pattern::Alternation(vec![
                    Pattern::Symbol(Named("A".into())),
                    Pattern::Symbol(Named("B".into())),
                ]),
            ),
            (
                "alternation_anchor_a",
                Pattern::Alternation(vec![
                    Pattern::Symbol(Start),
                    Pattern::Symbol(Named("A".into())),
                ]),
            ),
            (
                "alternation_abc",
                Pattern::Alternation(vec![
                    Pattern::Symbol(Named("A".into())),
                    Pattern::Symbol(Named("B".into())),
                    Pattern::Symbol(Named("C".into())),
                ]),
            ),
            (
                "alternation_repetition_with_symbol",
                Pattern::Alternation(vec![
                    Pattern::Repetition(
                        Box::new(Pattern::Symbol(Named("A".into()))),
                        Exactly(2),
                    ),
                    Pattern::Symbol(Named("B".into())),
                ]),
            ),
            // Group patterns
            (
                "group_single_a",
                Pattern::Group(Box::new(Pattern::Symbol(Named("A".into())))),
            ),
            (
                "group_concat_ab",
                Pattern::Group(Box::new(Pattern::Concat(vec![
                    Pattern::Symbol(Named("A".into())),
                    Pattern::Symbol(Named("B".into())),
                ]))),
            ),
            ("group_complex", {
                let alt1 = Pattern::Concat(vec![
                    Pattern::Symbol(Named("A".into())),
                    Pattern::Symbol(Named("B".into())),
                ]);
                let alt2 = Pattern::Repetition(
                    Box::new(Pattern::Symbol(Named("C".into()))),
                    OneOrMore,
                );
                Pattern::Group(Box::new(Pattern::Alternation(vec![alt1, alt2])))
            }),
            // PERMUTE patterns
            (
                "permute_ab",
                Pattern::Permute(vec![Named("A".into()), Named("B".into())]),
            ),
            ("permute_a", Pattern::Permute(vec![Named("A".into())])),
            (
                "permute_abc",
                Pattern::Permute(vec![
                    Named("A".into()),
                    Named("B".into()),
                    Named("C".into()),
                ]),
            ),
            (
                "permute_abcde",
                Pattern::Permute(vec![
                    Named("A".into()),
                    Named("B".into()),
                    Named("C".into()),
                    Named("D".into()),
                    Named("E".into()),
                ]),
            ),
            (
                "permute_aac",
                Pattern::Permute(vec![
                    Named("A".into()),
                    Named("A".into()),
                    Named("C".into()),
                ]),
            ),
            // Exclusion patterns
            ("exclude_single", Pattern::Exclude(Named("A".into()))),
            (
                "include_then_exclude",
                Pattern::Concat(vec![
                    Pattern::Symbol(Named("A".into())),
                    Pattern::Exclude(Named("B".into())),
                ]),
            ),
            (
                "double_exclude_then_include",
                Pattern::Concat(vec![
                    Pattern::Exclude(Named("A".into())),
                    Pattern::Exclude(Named("A".into())),
                    Pattern::Symbol(Named("A".into())),
                ]),
            ),
            (
                "include_then_double_exclude",
                Pattern::Concat(vec![
                    Pattern::Symbol(Named("A".into())),
                    Pattern::Exclude(Named("A".into())),
                    Pattern::Exclude(Named("A".into())),
                ]),
            ),
            (
                "exclude_middle",
                Pattern::Concat(vec![
                    Pattern::Symbol(Named("A".into())),
                    Pattern::Exclude(Named("A".into())),
                    Pattern::Symbol(Named("A".into())),
                ]),
            ),
            (
                "complex_snowflake_example", // ^ S1 S2*? ( {- S3 -} S4 )+ | PERMUTE(S1, S2){1,2} $
                {
                    use RepetitionQuantifier::*;
                    use Symbol::*;

                    // First alternative: ^ S1 S2* ( {- S3 -} S4 )+
                    let alt1 = Pattern::Concat(vec![
                        Pattern::Symbol(Start),
                        Pattern::Symbol(Named("S1".into())),
                        Pattern::Repetition(
                            Box::new(Pattern::Symbol(Named("S2".into()))),
                            ZeroOrMore,
                        ),
                        Pattern::Repetition(
                            Box::new(Pattern::Group(Box::new(Pattern::Concat(vec![
                                Pattern::Exclude(Named("S3".into())),
                                Pattern::Symbol(Named("S4".into())),
                            ])))),
                            OneOrMore,
                        ),
                    ]);

                    // Second alternative: PERMUTE(S1, S2){1,2} $
                    let alt2 = Pattern::Concat(vec![
                        Pattern::Repetition(
                            Box::new(Pattern::Permute(vec![
                                Named("S1".into()),
                                Named("S2".into()),
                            ])),
                            Range(1, 2),
                        ),
                        Pattern::Symbol(End),
                    ]);

                    Pattern::Alternation(vec![alt1, alt2])
                },
            ),
        ];

        insta::with_settings!({
            prepend_module_to_snapshot => false,
        }, {
            for (name, pattern) in cases {
                assert_snapshot!(*name, nfa_str(pattern), pattern.to_string().as_str());
            }
        });
    }
}

#[cfg(test)]
mod runtime_tests {
    use super::*;
    use arrow::array::{ArrayRef, BooleanArray, StringArray, UInt32Array};
    use arrow::datatypes::DataType;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    /// Helper to build a `RecordBatch` containing a mandatory `id` column and
    /// any number of Boolean symbol columns. Symbol columns are created with
    /// the required `__mr_symbol_` prefix so the `PatternMatcher` can resolve
    /// them automatically.
    fn make_batch(id_values: &[u32], symbols: &[(&str, Vec<bool>)]) -> RecordBatch {
        let mut fields = vec![Field::new("id", DataType::UInt32, false)];
        let mut columns: Vec<ArrayRef> =
            vec![Arc::new(UInt32Array::from(id_values.to_vec())) as ArrayRef];

        for (name, values) in symbols {
            let field_name = format!("__mr_symbol_{}", name);
            fields.push(Field::new(&field_name, DataType::Boolean, false));
            columns.push(Arc::new(BooleanArray::from(values.clone())) as ArrayRef);
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).expect("failed to build batch")
    }

    /// Verify that a simple `A` pattern matches all rows where the `A` column
    /// is `true` and populates the metadata columns correctly.
    #[test]
    fn single_symbol_match() {
        use datafusion_expr::match_recognize::Symbol;

        let batch = make_batch(&[1, 2, 3, 4], &[("A", vec![true, false, true, true])]);

        let pattern = Pattern::Symbol(Symbol::Named("A".into()));
        let mut matcher =
            PatternMatcher::new(pattern, vec!["A".into()], None, None).unwrap();

        let outputs = matcher.process_batch(batch.clone()).unwrap();
        assert_eq!(outputs.len(), 1);
        let out = &outputs[0];

        // Meta column indices (appended after the original columns)
        let base = batch.num_columns();
        let classifier = out
            .column(base)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let match_num = out
            .column(base + 1)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let seq_num = out
            .column(base + 2)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let last_flag = out
            .column(base + 3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let included = out
            .column(base + 4)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        // Expect three matches on rows 1, 3 and 4 (1-based physical indices)
        let expected_classifier = vec!["A", "A", "A"];
        for (i, exp) in expected_classifier.iter().enumerate() {
            assert_eq!(classifier.value(i), *exp);
        }
        let expected_match: Vec<u64> = vec![1, 2, 3];
        for (i, exp) in expected_match.iter().enumerate() {
            assert_eq!(match_num.value(i), *exp);
        }
        let expected_seq: Vec<u64> = vec![1, 1, 1];
        for (i, exp) in expected_seq.iter().enumerate() {
            assert_eq!(seq_num.value(i), *exp);
            assert!(last_flag.value(i));
            assert!(included.value(i));
        }
    }

    /// Test EXCLUDE semantics: the `B` row should be part of the match but
    /// flagged as excluded.
    #[test]
    fn exclude_symbol_match() {
        use datafusion_expr::match_recognize::Symbol;

        // Two-row input: first `A`, then `B`
        let batch = make_batch(
            &[1, 2],
            &[("A", vec![true, false]), ("B", vec![false, true])],
        );

        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".into())),
            Pattern::Exclude(Symbol::Named("B".into())),
        ]);

        let mut matcher =
            PatternMatcher::new(pattern, vec!["A".into(), "B".into()], None, None)
                .unwrap();

        let outputs = matcher.process_batch(batch.clone()).unwrap();
        assert_eq!(outputs.len(), 1);
        let out = &outputs[0];
        let base = batch.num_columns();

        let classifier = out
            .column(base)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let match_num = out
            .column(base + 1)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let seq_num = out
            .column(base + 2)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let last_flag = out
            .column(base + 3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let included = out
            .column(base + 4)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        // "A" row (included)
        assert_eq!(classifier.value(0), "A");
        assert_eq!(match_num.value(0), 1);
        assert_eq!(seq_num.value(0), 1);
        assert!(!last_flag.value(0));
        assert!(included.value(0));

        // "B" row (excluded)
        assert_eq!(classifier.value(1), "B");
        assert_eq!(match_num.value(1), 1);
        assert_eq!(seq_num.value(1), 0); // excluded rows get seq = 0
        assert!(last_flag.value(1));
        assert!(!included.value(1));
    }

    /// Verify that `AFTER MATCH SKIP TO NEXT ROW` allows overlapping matches
    /// whereas the default `PAST LAST ROW` does not.
    #[test]
    fn after_match_skip_to_next_row_allows_overlap() {
        use datafusion_expr::match_recognize::{AfterMatchSkip, Symbol};

        // Input AAA (three consecutive A rows)
        let batch = make_batch(&[1, 2, 3], &[("A", vec![true, true, true])]);

        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".into())),
            Pattern::Symbol(Symbol::Named("A".into())),
        ]);

        // Default (PAST LAST ROW) – one match covering rows 1&2 → 2 output rows
        let mut matcher_default =
            PatternMatcher::new(pattern.clone(), vec!["A".into()], None, None).unwrap();
        let out_default = matcher_default.process_batch(batch.clone()).unwrap();
        assert_eq!(out_default[0].num_rows(), 2);

        // TO NEXT ROW – overlapping matches (1&2) and (2&3) → 4 output rows
        let mut matcher_overlap = PatternMatcher::new(
            pattern,
            vec!["A".into()],
            Some(AfterMatchSkip::ToNextRow),
            None,
        )
        .unwrap();
        let out_overlap = matcher_overlap.process_batch(batch.clone()).unwrap();
        assert_eq!(out_overlap[0].num_rows(), 4);
    }

    /// Test `WITH UNMATCHED ROWS` behaviour: every input row should appear in
    /// the output even when no pattern rows are found.
    #[test]
    fn with_unmatched_rows() {
        use datafusion_expr::match_recognize::{EmptyMatchesMode, RowsPerMatch, Symbol};

        let batch = make_batch(&[1, 2, 3], &[("A", vec![false, false, false])]);

        let pattern = Pattern::Symbol(Symbol::Named("A".into()));
        let rows_per_match =
            Some(RowsPerMatch::AllRows(Some(EmptyMatchesMode::WithUnmatched)));

        let mut matcher =
            PatternMatcher::new(pattern, vec!["A".into()], None, rows_per_match).unwrap();

        let outputs = matcher.process_batch(batch.clone()).unwrap();
        let out = &outputs[0];
        assert_eq!(out.num_rows(), batch.num_rows());

        let base = batch.num_columns();
        let match_num = out
            .column(base + 1)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let included = out
            .column(base + 4)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        for i in 0..out.num_rows() {
            assert_eq!(match_num.value(i), 0); // unmatched ⇒ 0
            assert!(!included.value(i)); // unmatched rows are not included
        }
    }

    /// Anchor handling: pattern "^ A+ $" must match the entire partition
    /// exactly once, regardless of length.
    #[test]
    fn anchor_full_match() {
        use datafusion_expr::match_recognize::{RepetitionQuantifier, Symbol};

        // Build a partition of 3 rows where every row is an "A"
        let batch = make_batch(&[1, 2, 3], &[("A", vec![true, true, true])]);

        // ^ A+ $
        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Start),
            Pattern::Repetition(
                Box::new(Pattern::Symbol(Symbol::Named("A".into()))),
                RepetitionQuantifier::OneOrMore,
            ),
            Pattern::Symbol(Symbol::End),
        ]);

        let mut matcher =
            PatternMatcher::new(pattern, vec!["A".into()], None, None).unwrap();
        let outputs = matcher.process_batch(batch.clone()).unwrap();

        assert_eq!(outputs.len(), 1);
        let out = &outputs[0];
        assert_eq!(out.num_rows(), 3); // all rows must be part of the single match

        let base = batch.num_columns();
        let match_num = out
            .column(base + 1)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        for i in 0..out.num_rows() {
            assert_eq!(match_num.value(i), 1); // exactly one match id
        }
    }

    /// PERMUTE(A,B) must accept both AB and BA orderings exactly once.
    #[test]
    fn permute_two_symbols_order_insensitive() {
        use datafusion_expr::match_recognize::Symbol;

        let cases = vec![
            // AB ordering
            (
                "AB",
                make_batch(
                    &[1, 2],
                    &[("A", vec![true, false]), ("B", vec![false, true])],
                ),
            ),
            // BA ordering
            (
                "BA",
                make_batch(
                    &[1, 2],
                    &[("A", vec![false, true]), ("B", vec![true, false])],
                ),
            ),
        ];

        for (label, batch) in cases {
            let pattern = Pattern::Permute(vec![
                Symbol::Named("A".into()),
                Symbol::Named("B".into()),
            ]);

            let mut matcher =
                PatternMatcher::new(pattern, vec!["A".into(), "B".into()], None, None)
                    .unwrap();
            let outputs = matcher.process_batch(batch.clone()).unwrap();
            assert_eq!(outputs.len(), 1, "case {} produced no output", label);
            let out = &outputs[0];
            assert_eq!(out.num_rows(), 2, "case {} wrong row count", label);

            // Validate that match number sequence is consistent (both rows share match id 1)
            let base = batch.num_columns();
            let match_num = out
                .column(base + 1)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            assert_eq!(match_num.value(0), 1, "case {}", label);
            assert_eq!(match_num.value(1), 1, "case {}", label);
        }
    }
}
