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

use arrow::array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion_common::{HashMap, HashSet, Result};
use datafusion_expr::match_recognize::{
    AfterMatchSkip, EmptyMatchesMode, Pattern, RepetitionQuantifier, RowsPerMatch, Symbol
};
use hashbrown::hash_map::Entry;
use itertools::Itertools;
use std::collections::VecDeque;

use std::sync::Arc;

/// A single step in a matched path: (row_idx, symbol name, was_excluded?)
type PathStep = (usize, String, bool);

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
}

/// Active NFA state for tracking path through the NFA
#[derive(Debug, Clone)]
struct ActiveNFAState {
    state_id: usize,
    path: Vec<PathStep>, // row, symbol, excluded?
    start_row: usize,
}

/// Core pattern matching engine implementing NFA logic
pub struct PatternMatcher {
    /// Symbol names and their corresponding column indices in the input batch
    /// The boolean columns are named __mr_symbol_<symbol_name>
    symbol_columns: Vec<(String, usize)>,
    /// NFA states
    states: Vec<NFAState>,
    /// Active NFA states (each with its own path)
    active_states: Vec<ActiveNFAState>,
    /// Next match number to assign
    next_match_number: u64,
    /// After match skip behavior
    after_match_skip: Option<AfterMatchSkip>,
    /// All rows per match with unmatched rows enabled
    with_unmatched_rows: bool,
    /// Track match start positions for each active state
    match_starts: HashMap<usize, usize>,
}

impl PatternMatcher {
    /// Create a new pattern matcher
    pub fn new(
        pattern: Pattern,
        symbols: Vec<String>,
        after_match_skip: Option<AfterMatchSkip>,
        rows_per_match: Option<RowsPerMatch>,
    ) -> Result<Self> {
        // Compile the pattern into an NFA and then prune trivial ε-only states
        let raw_states = Self::compile_pattern_to_nfa(&pattern)?;
        let states = Self::prune_redundant_epsilons(raw_states);

        // Map symbol names to their column indices in the input batch
        // The boolean columns are named __mr_symbol_<symbol_name>
        let symbol_columns: Vec<(String, usize)> = symbols
            .into_iter()
            .map(|s| (s, 0))
            .collect();

        // Disallow WITH UNMATCHED ROWS together with EXCLUDE patterns as per
        // SQL spec (and Snowflake behaviour) because excluded rows conflict
        // with the notion of unmatched rows.
        let with_unmatched_rows = matches!(rows_per_match, Some(RowsPerMatch::AllRows(Some(EmptyMatchesMode::WithUnmatched))));

        if with_unmatched_rows && Self::pattern_has_exclude(&pattern) {
            return Err(datafusion_common::DataFusionError::Plan(
                "WITH UNMATCHED ROWS is not supported when the pattern contains EXCLUDE clauses"
                    .to_string(),
            ));
        }

        Ok(Self {
            symbol_columns,
            states,
            active_states: Vec::new(),
            next_match_number: 1,
            after_match_skip,
            with_unmatched_rows,
            match_starts: HashMap::new(),
        })
    }

    /// Recursively check if a pattern tree contains any `Pattern::Exclude`
    fn pattern_has_exclude(pat: &Pattern) -> bool {
        match pat {
            Pattern::Exclude(_) => true,
            Pattern::Concat(parts) | Pattern::Alternation(parts) => {
                parts.iter().any(Self::pattern_has_exclude)
            }
            Pattern::Repetition(inner, _) | Pattern::Group(inner) => {
                Self::pattern_has_exclude(inner)
            }
            Pattern::Permute(_) | Pattern::Symbol(_) => false,
        }
    }

    /// Update symbol column indices based on the actual batch schema
    pub fn update_column_indices(&mut self, batch: &RecordBatch) -> Result<()> {
        let schema = batch.schema();
        for (symbol, col_idx) in &mut self.symbol_columns {
            let base_symbol = symbol.as_str();
            let col_name = format!("__mr_symbol_{}", base_symbol);
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
            *col_idx = index;
        }
        Ok(())
    }

    /// Expand `state_id` to its ε-closure and return the set of reachable ids.
    fn epsilon_closure(&self, state_id: usize) -> HashSet<usize> {
        let mut reachable = HashSet::new();
        let mut stack = vec![state_id];

        while let Some(sid) = stack.pop() {
            if reachable.insert(sid) {
                for &eps in &self.states[sid].epsilon_transitions {
                    stack.push(eps);
                }
            }
        }

        reachable
    }

    /// Given a set of `ActiveNFAState`s, return a new vector that contains
    /// every ε-reachable state *once*, keeping the longest path per state id.
    fn expand_all_eps(&self, active: &[ActiveNFAState]) -> Vec<ActiveNFAState> {
        let mut all_active_states = Vec::new();

        // Collect all states reachable via epsilon transitions
        for active_state in active {
            let reachable_states = self.epsilon_closure(active_state.state_id);

            // Add all reachable states to all_active_states
            for &state_id in &reachable_states {
                all_active_states.push(ActiveNFAState {
                    state_id,
                    path: active_state.path.clone(),
                    start_row: active_state.start_row,
                });
            }
        }

        all_active_states
    }

    /// Decide whether a path should replace the current `best_match` according
    /// to "greedy left-most" semantics.
    fn maybe_update_best(
        best_match: &mut Option<Vec<PathStep>>,
        best_end_row: &mut Option<usize>,
        candidate_path: Vec<PathStep>,
        total_rows: usize,
    ) {
        let update = match best_match {
            None => true,
            Some(path) => candidate_path.len() > path.len(),
        };

        if update {
            *best_end_row = Some(
                candidate_path
                    .last()
                    .map(|(idx, _, _)| *idx)
                    .unwrap_or(total_rows.saturating_sub(1)),
            );
            *best_match = Some(candidate_path);
        }
    }

    /// Check if a path contains any physical (non-virtual) rows
    fn path_has_physical_rows(path: &[PathStep], num_rows: usize) -> bool {
        path.iter().any(|(idx, _, _)| *idx > 0 && *idx <= num_rows)
    }

    /// Calculate the next row index based on current position and match end
    fn next_row(current_row: usize, best_end_row: Option<usize>) -> usize {
        match best_end_row {
            Some(last_row) => {
                let mut next_idx = last_row + 1;
                if next_idx <= current_row {
                    next_idx = current_row + 1;
                }
                next_idx
            }
            None => current_row + 1,
        }
    }

    /// Try to find the longest match that *starts* at `row_idx`.
    ///
    /// Returns `(best_match, best_end_row)` where `best_match` is the path of
    /// (row, symbol, was_excluded?) pairs including virtual ^ / $ rows and `best_end_row` is
    /// the last virtual row that belongs to that match (needed for SKIP logic).
    fn scan_from(
        &self,
        batch: &RecordBatch,
        row_idx: usize,
        total_rows: usize,
        num_rows: usize,
    ) -> Result<(Option<Vec<PathStep>>, Option<usize>)> {
        let mut best_match: Option<Vec<PathStep>> = None;
        let mut best_end_row: Option<usize> = None;
        let mut current_row = row_idx;
        let mut active_states = vec![ActiveNFAState {
            state_id: 0,
            path: Vec::new(),
            start_row: row_idx,
        }];

        while !active_states.is_empty() && current_row < total_rows {
            // For window functions, we need to use the start_row of the current matching attempt
            // We'll use the start_row of the first active state as the match start
            let match_start_row = active_states
                .first()
                .map(|active| active.start_row)
                .unwrap_or(current_row);
            let symbol_matches = Self::evaluate_symbols_static(
                &self.symbol_columns,
                batch,
                current_row,
                num_rows,
                match_start_row,
            )?;
            let mut next_active_states = Vec::new();

            // Process epsilon transitions first (they don't consume input)
            let all_active_states = self.expand_all_eps(&active_states);

            // Now process symbol transitions from all active states (including epsilon-reached ones)
            for active in &all_active_states {
                let state = &self.states[active.state_id];
                for (symbol, matched) in &symbol_matches {
                    if *matched {
                        if let Some(next_states) = state.transitions.get(symbol) {
                            for &next_state_id in next_states {
                                let mut new_path = active.path.clone();
                                let excl_flag = self.states[next_state_id].is_excluded;
                                new_path.push((current_row, symbol.clone(), excl_flag));

                                // Check if this state or any epsilon-reachable state is accepting
                                let reachable = self.epsilon_closure(next_state_id);
                                if reachable
                                    .iter()
                                    .any(|&sid| self.states[sid].is_accepting)
                                {
                                    // Found a match ending at current_row
                                    Self::maybe_update_best(
                                        &mut best_match,
                                        &mut best_end_row,
                                        new_path.clone(),
                                        total_rows,
                                    );
                                    // continue searching for potentially longer match
                                }

                                next_active_states.push(ActiveNFAState {
                                    state_id: next_state_id,
                                    path: new_path,
                                    start_row: active.start_row,
                                });
                            }
                        }
                    }
                }
            }

            // Also check for accepting states in current active states (for patterns that can match empty sequences)
            for active in &all_active_states {
                if self.states[active.state_id].is_accepting {
                    let mut candidate_path = active.path.clone();
                    if candidate_path.is_empty() {
                        candidate_path.push((active.start_row, "".to_string(), false));
                    }
                    Self::maybe_update_best(
                        &mut best_match,
                        &mut best_end_row,
                        candidate_path,
                        total_rows,
                    );
                    break;
                }
            }

            // Deduplicate by state id keeping the longest path (greedy)
            let mut map: HashMap<usize, ActiveNFAState> =
                HashMap::with_capacity(next_active_states.len());
            for state in next_active_states {
                match map.entry(state.state_id) {
                    Entry::Vacant(v) => {
                        v.insert(state);
                    }
                    Entry::Occupied(mut o) => {
                        if state.path.len() > o.get().path.len() {
                            o.insert(state);
                        }
                    }
                }
            }
            active_states = map.into_values().collect();
            current_row += 1;
        }

        // Check remaining active states for accepting states reachable via epsilon transitions
        // This handles cases where we've consumed all input but can still reach accepting states
        for active in &active_states {
            let reachable = self.epsilon_closure(active.state_id);
            if reachable.iter().any(|&sid| self.states[sid].is_accepting) {
                let candidate_path = active.path.clone();
                Self::maybe_update_best(
                    &mut best_match,
                    &mut best_end_row,
                    candidate_path,
                    total_rows,
                );
            }
        }

        Ok((best_match, best_end_row))
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
        let mut classifiers = Vec::new();
        let mut match_numbers = Vec::new();
        let mut sequence_numbers = Vec::new();
        let mut is_last_flags = Vec::new();
        let mut included_flags = Vec::new();

        for (match_id, path) in all_matches {
            if Self::is_consecutive_path(path) {
                // Collect physical row indices in the order they appear in the path
                let physical_rows: Vec<(usize, &String, bool)> = path
                    .iter()
                    .filter_map(|(row_idx, symbol, excl)| {
                        if *row_idx == 0 || symbol == "^" || symbol == "$" {
                            None
                        } else {
                            Some((*row_idx - 1, symbol, *excl))
                        }
                    })
                    .collect();

                let match_len = physical_rows.len() as u64;
                if match_len == 0 {
                    continue;
                }

                // Remember where rows of this match start in the global vectors
                let base_index = matched_rows.len();

                let mut seq_counter: u64 = 0;

                for (physical_idx, symbol, excl_flag) in physical_rows.into_iter() {
                    if physical_idx >= num_rows {
                        continue;
                    }

                    let included = !excl_flag;

                    matched_rows.push(physical_idx as u32);

                    // Metadata columns in the same order
                    let class_sym = if symbol.is_empty() {
                        "(empty)".to_string()
                    } else {
                        symbol.clone()
                    };
                    classifiers.push(class_sym);
                    match_numbers.push(*match_id);

                    if included {
                        seq_counter += 1;
                        sequence_numbers.push(seq_counter);
                    } else {
                        sequence_numbers.push(0); // 0 indicates excluded / not counted
                    }

                    // Placeholder for last flag; will set after loop
                    is_last_flags.push(false);
                    included_flags.push(included);
                }

                // Mark the last physical row (included or excluded) of this match
                if matched_rows.len() > base_index {
                    let last_idx = matched_rows.len() - 1;
                    is_last_flags[last_idx] = true;
                }
            }
        }

        // ---------------------------------------------------------------------
        // WITH UNMATCHED ROWS: augment vectors before building arrays
        // ---------------------------------------------------------------------
        if self.with_unmatched_rows {
            let mut seen = vec![false; num_rows];
            for &idx in &matched_rows {
                if (idx as usize) < num_rows {
                    seen[idx as usize] = true;
                }
            }

            for phys_idx in 0..num_rows {
                if !seen[phys_idx] {
                    matched_rows.push(phys_idx as u32);
                    classifiers.push("(empty)".to_string());
                    match_numbers.push(0);
                    sequence_numbers.push(0);
                    is_last_flags.push(false);
                    included_flags.push(false);
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

        let classifier_array = arrow::array::StringArray::from(classifiers);
        let match_number_array = arrow::array::UInt64Array::from(match_numbers);
        let sequence_number_array = arrow::array::UInt64Array::from(sequence_numbers);
        let last_flag_array = BooleanArray::from(is_last_flags);
        let included_array = BooleanArray::from(included_flags);
        output_columns.push(Arc::new(classifier_array));
        output_columns.push(Arc::new(match_number_array));
        output_columns.push(Arc::new(sequence_number_array));
        output_columns.push(Arc::new(last_flag_array));
        output_columns.push(Arc::new(included_array));

        let output_batch = RecordBatch::try_new(output_schema, output_columns)?;
        Ok(Some(output_batch))
    }

    /// Evaluate all symbol definitions for a given row
    fn evaluate_symbols_static(
        symbol_columns: &[(String, usize)],
        batch: &RecordBatch,
        row_idx: usize,
        num_rows: usize,
        _match_start_row: usize,
    ) -> Result<Vec<(String, bool)>> {
        let mut results = Vec::new();

        // Virtual start-anchor row (index 0)
        if row_idx == 0 {
            // All user-defined symbols are false on the anchor row
            for (sym, _) in symbol_columns {
                results.push((sym.clone(), false));
            }
            results.push(("^".to_string(), true));
            return Ok(results);
        }

        // Virtual end-anchor row (index num_rows + 1)
        if row_idx == num_rows + 1 {
            for (sym, _) in symbol_columns {
                results.push((sym.clone(), false));
            }
            results.push(("$".to_string(), true));
            return Ok(results);
        }

        // Real data rows: shift by 1 because of the virtual start row
        let physical_idx = row_idx - 1;

        for (symbol, col_idx) in symbol_columns {
            // Read directly from the boolean column
            let column = batch.column(*col_idx);
            let boolean_array = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(format!(
                        "Column {} is not a boolean array",
                        col_idx
                    ))
                })?;

            let matched = boolean_array.value(physical_idx);
            results.push((symbol.clone(), matched));
        }

        // Add start-anchor symbol (false except on row 0, which we already handled)
        results.push(("^".to_string(), false));
        // End-anchor symbol is only true on the virtual last row that was handled above

        Ok(results)
    }

    /// Process a batch and return match results
    pub fn process_batch(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        // Update column indices based on the actual batch schema
        self.update_column_indices(&batch)?;

        let mut results = Vec::new();
        let num_rows = batch.num_rows();
        let total_rows = num_rows + 2; // include virtual ^ and $ rows
        self.active_states.clear();

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

            let (best_match, best_end_row) =
                self.scan_from(&batch, row_idx, total_rows, num_rows)?;

            // Track any successful match that contains at least one physical row
            if let Some(path) = best_match.clone() {
                if Self::path_has_physical_rows(&path, num_rows) {
                    let match_id = self.next_match_number;
                    self.next_match_number += 1;
                    all_matches.push((match_id, path));
                }
            }

            // Decide the next starting row depending on the AFTER MATCH SKIP
            // clause (default is PAST LAST ROW as per SQL spec).
            let skip_policy = self
                .after_match_skip
                .clone()
                .unwrap_or(AfterMatchSkip::PastLastRow);

            let mut next_row = row_idx + 1; // sensible default when no match
            let mut new_last_matched_row: Option<usize> = None;

            match skip_policy {
                AfterMatchSkip::PastLastRow => {
                    // Re-use existing helper for default semantics.
                    next_row = Self::next_row(row_idx, best_end_row);
                    new_last_matched_row = best_end_row;
                }
                AfterMatchSkip::ToNextRow => {
                    // Start with the row immediately after the match start.
                    next_row = row_idx + 1;
                }
                AfterMatchSkip::ToFirst(symbol) => {
                    if let Some(ref path) = best_match {
                        // Find first occurrence of the requested symbol.
                        if let Some(&(sym_idx, _, _)) =
                            path.iter().find(|(_, s, _)| *s == *symbol || s == &symbol)
                        {
                            next_row = sym_idx;
                        } else {
                            // Fallback to past-last-row semantics if symbol absent.
                            next_row = Self::next_row(row_idx, best_end_row);
                            new_last_matched_row = best_end_row;
                        }
                    }
                }
                AfterMatchSkip::ToLast(symbol) => {
                    if let Some(ref path) = best_match {
                        // Find last occurrence (scan from the end).
                        if let Some(&(sym_idx, _, _)) = path
                            .iter()
                            .rev()
                            .find(|(_, s, _)| *s == *symbol || s == &symbol)
                        {
                            next_row = sym_idx;
                        } else {
                            // Fallback to past-last-row semantics if symbol absent.
                            next_row = Self::next_row(row_idx, best_end_row);
                            new_last_matched_row = best_end_row;
                        }
                    }
                }
            }

            // Ensure forward progress to avoid infinite loops.
            if next_row <= row_idx {
                next_row = row_idx + 1;
            }

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

    /// Compile pattern into NFA states
    fn compile_pattern_to_nfa(pattern: &Pattern) -> Result<Vec<NFAState>> {
        let mut states = Vec::new();

        match pattern {
            Pattern::Symbol(symbol) => {
                // Treat named symbols and anchors identically: they all consume one input
                // position (which may be a virtual anchor row for ^ or $).
                let symbol_name = match symbol {
                    Symbol::Named(name) => name.clone(),
                    Symbol::Start => "^".to_string(),
                    Symbol::End => "$".to_string(),
                };

                // Start state (state 0)
                let mut start_state = NFAState {
                    id: 0,
                    is_accepting: false,
                    is_excluded: false,
                    transitions: HashMap::new(),
                    epsilon_transitions: HashSet::new(),
                };
                start_state.transitions.insert(symbol_name.clone(), {
                    let mut set = HashSet::new();
                    set.insert(1);
                    set
                });
                states.push(start_state);

                // Accepting state (state 1)
                states.push(NFAState {
                    id: 1,
                    is_accepting: true,
                    is_excluded: false,
                    transitions: HashMap::new(),
                    epsilon_transitions: HashSet::new(),
                });
            }

            Pattern::Concat(patterns) => {
                // 1. If it is a trivial repetition like (A A A) we already have a specialised path.
                if Self::is_repetition_pattern(patterns) {
                    return Self::compile_repetition_pattern(patterns);
                }

                return Self::compile_concat_patterns(patterns);
            }

            Pattern::Repetition(sub_pattern, quantifier) => {
                // Compile the sub-pattern first
                let sub_states = Self::compile_pattern_to_nfa(sub_pattern)?;
                if sub_states.is_empty() {
                    return Err(datafusion_common::DataFusionError::Internal(
                        "Sub-pattern compilation failed".to_string(),
                    ));
                }

                // Apply quantifier to the sub-pattern
                return Self::apply_quantifier(sub_states, quantifier);
            }

            Pattern::Alternation(patterns) => {
                return Self::compile_alternation_patterns(patterns);
            }

            // Handle grouped patterns by compiling the contained pattern
            Pattern::Group(inner) => {
                return Self::compile_pattern_to_nfa(inner);
            }

            Pattern::Permute(symbols) => {
                // Decide whether to enumerate all permutations directly (fast
                // for very small inputs) or use the counter-based automaton
                // (scales better for larger inputs).

                const ENUM_LIMIT: u128 = 24; // 4! – safe upper bound for brute force

                // Compute the number of *distinct* permutations so we don't
                // blow up on many duplicates.
                let mut multiplicities: HashMap<String, usize> = HashMap::new();
                for s in symbols {
                    let name = match s {
                        Symbol::Named(n) => n.clone(),
                        Symbol::Start => "^".to_string(),
                        Symbol::End => "$".to_string(),
                    };
                    *multiplicities.entry(name).or_insert(0) += 1;
                }

                let n = symbols.len() as u128;
                let mut perm_count: u128 = (1..=n).product::<u128>(); // n!
                for &cnt in multiplicities.values() {
                    let c = cnt as u128;
                    perm_count /= (1..=c).product::<u128>();
                }

                if perm_count <= ENUM_LIMIT {
                    return Self::compile_permute_bruteforce(symbols);
                } else {
                    return Self::compile_permute_pattern(symbols);
                }
            }

            Pattern::Exclude(symbol) => {
                let base_name = match symbol {
                    Symbol::Named(name) => name.clone(),
                    Symbol::Start => "^".to_string(),
                    Symbol::End => "$".to_string(),
                };
                let symbol_name = base_name;

                let mut start_state = NFAState {
                    id: 0,
                    is_accepting: false,
                    is_excluded: false,
                    transitions: HashMap::new(),
                    epsilon_transitions: HashSet::new(),
                };
                start_state.transitions.insert(symbol_name.clone(), {
                    let mut set = HashSet::new();
                    set.insert(1);
                    set
                });
                states.push(start_state);

                states.push(NFAState {
                    id: 1,
                    is_accepting: true,
                    is_excluded: true,
                    transitions: HashMap::new(),
                    epsilon_transitions: HashSet::new(),
                });
            }
        }

        Ok(states)
    }

    /// Remove non-accepting states that have *only* a single ε-transition and
    /// no symbol transitions.  All incoming edges are rewired to bypass the
    /// redundant state and the remaining states are re-indexed so that their
    /// `id` fields stay in ascending, gap-free order.
    ///
    /// This optimisation significantly reduces the size of NFAs generated for
    /// heavily nested patterns (e.g. those produced by PERMUTE) without
    /// affecting their accepted language.
    fn prune_redundant_epsilons(states: Vec<NFAState>) -> Vec<NFAState> {
        // -----------------------------------------------------------------
        // This pass removes *trivial* ε-only states, i.e. non-accepting
        // states that have exactly one outgoing ε-transition and *no* symbol
        // transitions.  Such states are effectively aliases of their single
        // ε target and can be skipped without changing the accepted language.
        //
        // The algorithm proceeds in four simple stages:
        //   1. Mark removable states.
        //   2. For each original state id, determine the first *kept* state
        //      reached by following ε edges (the "redirect" array).
        //   3. Assign new contiguous state ids to the kept states.
        //   4. Build the final state vector, remapping all edges via the
        //      redirect array and carrying over the `is_excluded` flag if ANY
        //      removed predecessor had it set.
        // -----------------------------------------------------------------

        let n = states.len();
        if n <= 1 {
            return states; // nothing to do
        }

        // -----------------------------------------------------------------
        // 1. Identify removable states (never remove the global start state 0)
        // -----------------------------------------------------------------
        let mut is_removable = vec![false; n];
        for st in &states {
            if st.id != 0
                && !st.is_accepting
                && st.transitions.is_empty()
                && st.epsilon_transitions.len() == 1
            {
                is_removable[st.id] = true;
            }
        }
        if !is_removable.iter().any(|b| *b) {
            return states; // nothing qualifies – fast path
        }

        // -----------------------------------------------------------------
        // 2. Build redirect table: first *kept* state reachable from `id`.
        // -----------------------------------------------------------------
        let mut redirect: Vec<usize> = (0..n).collect();
        let find_final = |mut id: usize, is_removable: &Vec<bool>, redirect: &Vec<usize>| {
            while is_removable[id] {
                id = redirect[id];
            }
            id
        };
        for id in 0..n {
            if is_removable[id] {
                // Safe unwrap – removable states have exactly one ε target
                redirect[id] = *states[id].epsilon_transitions.iter().next().unwrap();
            }
        }

        // -----------------------------------------------------------------
        // 3. Assign new contiguous ids to kept states.
        // -----------------------------------------------------------------
        let mut new_id_map = vec![usize::MAX; n];
        let mut next_id = 0;
        for old_id in 0..n {
            if !is_removable[old_id] {
                new_id_map[old_id] = next_id;
                next_id += 1;
            }
        }

        // -----------------------------------------------------------------
        // 4. Rebuild kept states with remapped edges & propagated exclusion.
        // -----------------------------------------------------------------
        let mut propagated_excluded = vec![false; next_id];
        // First, determine which kept states must inherit `is_excluded` from
        // any removed ancestors.
        for old_id in 0..n {
            if states[old_id].is_excluded {
                let final_id = find_final(old_id, &is_removable, &redirect);
                if !is_removable[final_id] {
                    propagated_excluded[new_id_map[final_id]] = true;
                }
            }
        }

        let mut new_states = Vec::with_capacity(next_id);
        for old_id in 0..n {
            if is_removable[old_id] {
                continue; // state is dropped
            }

            let mut st = states[old_id].clone();
            let new_id = new_id_map[old_id];
            st.id = new_id;
            st.is_excluded |= propagated_excluded[new_id];

            // Remap ε-transitions
            st.epsilon_transitions = st
                .epsilon_transitions
                .iter()
                .map(|&dst| find_final(dst, &is_removable, &redirect))
                .filter(|&dst| !is_removable[dst])
                .map(|dst| new_id_map[dst])
                .collect();

            // Remap symbol transitions
            let mut new_transitions: HashMap<String, HashSet<usize>> = HashMap::new();
            for (sym, dests) in &st.transitions {
                let mapped: HashSet<usize> = dests
                    .iter()
                    .map(|&dst| find_final(dst, &is_removable, &redirect))
                    .filter(|&dst| !is_removable[dst])
                    .map(|dst| new_id_map[dst])
                    .collect();
                if !mapped.is_empty() {
                    new_transitions.insert(sym.clone(), mapped);
                }
            }
            st.transitions = new_transitions;

            new_states.push(st);
        }

        new_states
    }

    /// Check if a concatenation pattern is actually a repetition pattern like (A A A)
    fn is_repetition_pattern(patterns: &[Pattern]) -> bool {
        if patterns.len() < 2 {
            return false;
        }

        // Check if all patterns are the same symbol
        if let Some(first_pattern) = patterns.first() {
            if let Pattern::Symbol(first_symbol) = first_pattern {
                for pattern in patterns.iter().skip(1) {
                    if let Pattern::Symbol(symbol) = pattern {
                        if !Self::symbols_equal(first_symbol, symbol) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                return true;
            }
        }
        false
    }

    /// Compare two symbols for equality
    fn symbols_equal(a: &Symbol, b: &Symbol) -> bool {
        match (a, b) {
            (Symbol::Named(name_a), Symbol::Named(name_b)) => name_a == name_b,
            (Symbol::Start, Symbol::Start) => true,
            (Symbol::End, Symbol::End) => true,
            _ => false,
        }
    }

    /// Compile a repetition pattern like (A A A) into NFA states
    fn compile_repetition_pattern(patterns: &[Pattern]) -> Result<Vec<NFAState>> {
        let mut states = Vec::new();

        // Get the symbol name from the first pattern
        let symbol_name = if let Pattern::Symbol(symbol) = &patterns[0] {
            match symbol {
                Symbol::Named(name) => name.clone(),
                Symbol::Start => "^".to_string(),
                Symbol::End => "$".to_string(),
            }
        } else {
            return Err(datafusion_common::DataFusionError::Internal(
                "Repetition pattern must contain symbols".to_string(),
            ));
        };

        let repetition_count = patterns.len();
        let mut symbol_sequence = Vec::new();

        // Create start state (state 0)
        states.push(NFAState {
            id: 0,
            is_accepting: false,
            is_excluded: false,
            transitions: HashMap::new(),
            epsilon_transitions: HashSet::new(),
        });

        // Create intermediate states for each repetition
        for i in 0..repetition_count {
            symbol_sequence.push(symbol_name.clone());

            let current_state_id = i + 1;
            let is_accepting = i == repetition_count - 1;

            // Add transition from previous state to current state
            let prev_state = &mut states[current_state_id - 1];
            prev_state.transitions.insert(symbol_name.clone(), {
                let mut set = HashSet::new();
                set.insert(current_state_id);
                set
            });

            // Create state
            states.push(NFAState {
                id: current_state_id,
                is_accepting,
                is_excluded: false,
                transitions: HashMap::new(),
                epsilon_transitions: HashSet::new(),
            });
        }

        Ok(states)
    }

    /// Reset the matcher for a new partition
    pub fn reset(&mut self) {
        self.active_states.clear();
        self.match_starts.clear();
        self.next_match_number = 1;
    }

    /// Check if a path contains consecutive row indices
    fn is_consecutive_path(path: &[PathStep]) -> bool {
        if path.len() < 2 {
            return true;
        }
        for i in 0..path.len() - 1 {
            let cur = path[i].0;
            let next = path[i + 1].0;
            if !(next == cur || next == cur + 1) {
                return false;
            }
        }
        true
    }

    /// Helper: clone a block of states with an ID offset, adjusting transitions & epsilons
    fn clone_with_offset(states: &[NFAState], offset: usize) -> Vec<NFAState> {
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
            cloned.push(new_state);
        }
        cloned
    }

    /// Apply quantifier to a set of NFA states
    fn apply_quantifier(
        mut sub_states: Vec<NFAState>,
        quantifier: &RepetitionQuantifier,
    ) -> Result<Vec<NFAState>> {
        if sub_states.is_empty() {
            return Err(datafusion_common::DataFusionError::Internal(
                "Cannot apply quantifier to empty state set".to_string(),
            ));
        }
        let sub_len = sub_states.len();
        match quantifier {
            RepetitionQuantifier::OneOrMore => {
                // last state accepting and ε‑loop back to start
                let last_id = sub_len - 1;
                sub_states[last_id].is_accepting = true;
                sub_states[last_id].epsilon_transitions.insert(0);
                Ok(sub_states)
            }
            RepetitionQuantifier::ZeroOrMore => {
                // Accept empty (start state accepting) and allow repeating the
                // sub-pattern by looping from its last state back to the
                // start.  The additional ε-edge from the start state to the
                // last state that was previously present created a bidirectional
                // ε-cycle (0 <-> last) which in turn spawned an exponential
                // number of active paths during evaluation.  Removing that
                // edge still preserves the formal semantics (0 occurrences are
                // accepted because state 0 itself is accepting) while greatly
                // improving performance and preventing hangs.

                sub_states[0].is_accepting = true;
                let last_id = sub_len - 1;
                sub_states[last_id].is_accepting = true;
                sub_states[last_id].epsilon_transitions.insert(0);
                Ok(sub_states)
            }
            RepetitionQuantifier::AtMostOne => {
                // Accept zero (start state is accepting) or one occurrence via
                // the explicit symbol transition. The ε-edge from the start
                // directly to the end is not required for correctness and, as
                // with the ZeroOrMore case, leads to unnecessary ε-cycles that
                // blow up the search space.  Removing it keeps the accepted
                // language unchanged while eliminating pathological behaviour.

                let last_id = sub_len - 1;
                sub_states[0].is_accepting = true;
                sub_states[last_id].is_accepting = true;
                Ok(sub_states)
            }
            RepetitionQuantifier::Exactly(n) => {
                if *n == 0 {
                    // match empty sequence
                    return Ok(vec![NFAState {
                        id: 0,
                        is_accepting: true,
                        is_excluded: false,
                        transitions: HashMap::new(),
                        epsilon_transitions: HashSet::new(),
                    }]);
                }
                let mut result_states: Vec<NFAState> = Vec::new();
                for i in 0..*n {
                    let offset = result_states.len();
                    let mut cloned = Self::clone_with_offset(&sub_states, offset);
                    // Only final copy's end state is accepting
                    if i < *n - 1 {
                        let end_id = offset + sub_len - 1;
                        cloned[end_id - offset].is_accepting = false;
                    }
                    // Link with ε from previous copy's end to current copy's start
                    if i > 0 {
                        let prev_end_id = offset - 1; // last state's id before this insertion
                        result_states[prev_end_id]
                            .epsilon_transitions
                            .insert(offset);
                    }
                    result_states.extend(cloned);
                }
                Ok(result_states)
            }
            RepetitionQuantifier::AtLeast(n) => {
                if *n == 0 {
                    return Self::apply_quantifier(
                        sub_states,
                        &RepetitionQuantifier::ZeroOrMore,
                    );
                }
                // Build exactly n copies first
                let mut result_states = Self::apply_quantifier(
                    sub_states.clone(),
                    &RepetitionQuantifier::Exactly(*n),
                )?;
                let start_last = result_states.len() - sub_len; // start of last mandatory copy
                let last_state_id = result_states.len() - 1;
                // ε‑loop from last state back to start of that last copy
                result_states[last_state_id]
                    .epsilon_transitions
                    .insert(start_last);
                result_states[last_state_id].is_accepting = true;
                Ok(result_states)
            }
            RepetitionQuantifier::AtMost(n) => {
                let max = *n as usize;
                if max == 0 {
                    // empty match only
                    return Ok(vec![NFAState {
                        id: 0,
                        is_accepting: true,
                        is_excluded: false,
                        transitions: HashMap::new(),
                        epsilon_transitions: HashSet::new(),
                    }]);
                }
                let mut states: Vec<NFAState> = Vec::new();
                for i in 0..max {
                    let offset = states.len();
                    let mut clone = Self::clone_with_offset(&sub_states, offset);
                    // none of these internal ends are accepting yet
                    clone[sub_len - 1].is_accepting = false;
                    // link previous copy ε
                    if i > 0 {
                        let prev_end_id = offset - 1;
                        states[prev_end_id].epsilon_transitions.insert(offset);
                    }
                    states.extend(clone);
                }
                // Add a single accepting sink
                let sink_id = states.len();
                states.push(NFAState {
                    id: sink_id,
                    is_accepting: true,
                    is_excluded: false,
                    transitions: HashMap::new(),
                    epsilon_transitions: HashSet::new(),
                });
                // ε from start to sink (zero repetitions)
                states[0].epsilon_transitions.insert(sink_id);
                // ε from every repetition end to sink
                for i in 0..max {
                    let end_id = (i + 1) * sub_len - 1;
                    states[end_id].epsilon_transitions.insert(sink_id);
                }
                Ok(states)
            }
            RepetitionQuantifier::Range(min, max) => {
                if min == max {
                    return Self::apply_quantifier(
                        sub_states,
                        &RepetitionQuantifier::Exactly(*min),
                    );
                }
                if *min == 0 {
                    return Self::apply_quantifier(
                        sub_states,
                        &RepetitionQuantifier::AtMost(*max),
                    );
                }
                // Build mandatory part (Exactly(min))
                let mut states = Self::apply_quantifier(
                    sub_states.clone(),
                    &RepetitionQuantifier::Exactly(*min),
                )?;
                let _mandatory_len = states.len();
                let optional_copies = (*max - *min) as usize;
                let sub_len = sub_states.len();
                // Build optional copies serially
                for _i in 0..optional_copies {
                    let offset = states.len();
                    let clone = Self::clone_with_offset(&sub_states, offset);
                    // ε from previous end into this copy's start (making it optional)
                    let prev_end_id = offset - 1;
                    states[prev_end_id].epsilon_transitions.insert(offset);
                    states.extend(clone);
                }
                // Add accepting sink
                let sink_id = states.len();
                states.push(NFAState {
                    id: sink_id,
                    is_accepting: true,
                    is_excluded: false,
                    transitions: HashMap::new(),
                    epsilon_transitions: HashSet::new(),
                });
                // ε from every repetition end past mandatory to sink
                let total_copies = (*max) as usize;
                for i in (*min as usize)..=total_copies {
                    let end_id = i * sub_len - 1;
                    states[end_id].epsilon_transitions.insert(sink_id);
                }
                Ok(states)
            }
        }
    }

    /// Compile a concatenation of arbitrary sub-patterns (A+ B C{2} …)
    /// into a single NFA by linking the individual NFAs with ε-transitions.
    fn compile_concat_patterns(patterns: &[Pattern]) -> Result<Vec<NFAState>> {
        if patterns.is_empty() {
            return Err(datafusion_common::DataFusionError::Internal(
                "Concatenation pattern must contain at least one sub-pattern".to_string(),
            ));
        }

        let mut states: Vec<NFAState> = Vec::new();
        // Offset of the start state of the *current* (last appended) sub-NFA
        let mut prev_accepting_ids: Vec<usize> = Vec::new();

        for (_idx, sub_pattern) in patterns.iter().enumerate() {
            // Recursively compile each sub-pattern
            let sub_states = Self::compile_pattern_to_nfa(sub_pattern)?;

            // Record the accepting state IDs *before* we offset them so we can link later
            let local_accepting: Vec<usize> = sub_states
                .iter()
                .filter(|s| s.is_accepting)
                .map(|s| s.id)
                .collect();

            let offset = states.len();
            // Re-id the states so they don't clash with what we already have
            let cloned = Self::clone_with_offset(&sub_states, offset);

            // Index of the start state of this sub-NFA after offset
            let sub_start_id = offset; // Because clone_with_offset preserves order and first state has id 0

            // If this is not the first component, link all accepting states of the
            // previous component to the start of this one via ε-edges and make
            // them non-accepting (only the overall end may stay accepting).
            if !states.is_empty() {
                for acc_id in &prev_accepting_ids {
                    if let Some(state) = states.get_mut(*acc_id) {
                        state.is_accepting = false;
                        state.epsilon_transitions.insert(sub_start_id);
                    }
                }
            }

            prev_accepting_ids = local_accepting
                .iter()
                .map(|id| id + offset)
                .collect::<Vec<_>>();

            states.extend(cloned);
        }

        // Ensure at least one accepting state exists (those of the last sub-pattern)
        if prev_accepting_ids.is_empty() {
            return Err(datafusion_common::DataFusionError::Internal(
                "Concatenation produced no accepting state".to_string(),
            ));
        }

        Ok(states)
    }

    /// Compile an alternation pattern (A | B | C) into NFA states
    ///
    /// The construction follows the standard Thompson NFA for alternation:
    /// a fresh start state (0) with ε-edges to the start state of each
    /// alternative sub-NFA.  The accepting states of the sub-NFAs are kept as
    /// accepting so that a match is recognised when *any* branch accepts.
    fn compile_alternation_patterns(patterns: &[Pattern]) -> Result<Vec<NFAState>> {
        if patterns.is_empty() {
            return Err(datafusion_common::DataFusionError::Internal(
                "Alternation pattern must contain at least one sub-pattern".to_string(),
            ));
        }

        // Start with a fresh start state (id 0)
        let mut states: Vec<NFAState> = Vec::new();
        states.push(NFAState {
            id: 0,
            is_accepting: false,
            is_excluded: false,
            transitions: HashMap::new(),
            epsilon_transitions: HashSet::new(),
        });

        // Current offset for newly cloned sub-NFA insertion
        let mut next_offset: usize = 1;

        for sub_pattern in patterns {
            // Compile sub-pattern recursively
            let sub_states = Self::compile_pattern_to_nfa(sub_pattern)?;
            let sub_len = sub_states.len();

            // Clone with the current offset so state ids don't clash
            let cloned = Self::clone_with_offset(&sub_states, next_offset);

            // Record ε-edge from the global start (id 0) to the start of this branch
            states[0].epsilon_transitions.insert(next_offset);

            // Append the cloned states
            states.extend(cloned);

            // Advance offset for next branch
            next_offset += sub_len;
        }

        Ok(states)
    }

    /// Brute-force compilation of PERMUTE by enumerating every distinct
    /// ordering (used only for very small inputs).
    fn compile_permute_bruteforce(symbols: &[Symbol]) -> Result<Vec<NFAState>> {
        if symbols.is_empty() {
            return Err(datafusion_common::DataFusionError::Internal(
                "PERMUTE pattern must contain at least one symbol".to_string(),
            ));
        }

        // Single item – just a symbol.
        if symbols.len() == 1 {
            return Self::compile_pattern_to_nfa(&Pattern::Symbol(symbols[0].clone()));
        }

        // Generate all distinct permutations using itertools::Itertools.
        let mut alternatives: Vec<Pattern> = Vec::new();
        let mut seen: HashSet<Vec<String>> = HashSet::new();

        for perm in symbols.iter().permutations(symbols.len()) {
            let key: Vec<String> = perm
                .iter()
                .map(|sym| match *sym {
                    Symbol::Named(ref s) => s.clone(),
                    Symbol::Start => "^".to_string(),
                    Symbol::End => "$".to_string(),
                })
                .collect();

            if !seen.insert(key) {
                continue; // duplicate ordering (due to duplicates in input)
            }

            let seq: Vec<Pattern> = perm
                .into_iter()
                .map(|sym| Pattern::Symbol((*sym).clone()))
                .collect();

            alternatives.push(if seq.len() == 1 {
                seq[0].clone()
            } else {
                Pattern::Concat(seq)
            });
        }

        let alt_pattern = Pattern::Alternation(alternatives);
        Self::compile_pattern_to_nfa(&alt_pattern)
    }

    /// Compile a PERMUTE pattern into an NFA without enumerating all n! orderings.
    ///
    /// Given a multiset of symbols S = {s_1, …, s_n}, the language of
    /// `PERMUTE(S)` consists of all strings that contain each symbol exactly as
    /// many times as it appears in S, in any order.  We build an NFA whose
    /// states encode the *usage count* of every distinct symbol encountered so
    /// far.  From each state we add a transition for every symbol that still
    /// has remaining quota, incrementing that symbol's counter.  The start
    /// state therefore has counters `[0, 0, …]` and the single accepting state
    /// has `[count_1, count_2, …]`.  The number of states is bounded by
    /// `∏(count_i + 1)` which is exponential in the number of *distinct*
    /// symbols, but *much* smaller than `n!` and handles duplicates naturally.
    fn compile_permute_pattern(symbols: &[Symbol]) -> Result<Vec<NFAState>> {
        if symbols.is_empty() {
            return Err(datafusion_common::DataFusionError::Internal(
                "PERMUTE pattern must contain at least one symbol".to_string(),
            ));
        }

        // ------------------------------------------------------------------
        // 1.  Collect unique symbols (in insertion order) and their required
        //     multiplicities.
        // ------------------------------------------------------------------
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
}

pub(crate) fn pattern_schema(input_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = input_schema.fields().to_vec();

    // Add match metadata columns
    fields.push(Arc::new(Field::new(
        "__mr_classifier",
        arrow::datatypes::DataType::Utf8,
        false,
    )));
    fields.push(Arc::new(Field::new(
        "__mr_match_number",
        arrow::datatypes::DataType::UInt64,
        false,
    )));
    fields.push(Arc::new(Field::new(
        "__mr_match_sequence_number",
        arrow::datatypes::DataType::UInt64,
        false,
    )));
    fields.push(Arc::new(Field::new(
        "__mr_is_last_match_row",
        arrow::datatypes::DataType::Boolean,
        false,
    )));
    fields.push(Arc::new(Field::new(
        "__mr_is_included_row",
        arrow::datatypes::DataType::Boolean,
        false,
    )));

    Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::BooleanArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_expr::match_recognize::{Pattern, RepetitionQuantifier, Symbol};
    use insta::assert_snapshot;
    use std::sync::Arc;

    // =====================================================================
    // Test Helper Functions
    // =====================================================================

    /// Produce a deterministic string representation of an NFA suitable for human
    /// inspection and unit-test assertions. Format:
    ///
    /// ```text
    /// 0: ε->1 | A->2
    /// 1*:             (accepting state, no outgoing edges)
    /// ```
    ///
    /// • States appear in ascending ID order.
    /// • `*` marks accepting states.
    /// • Each transition is listed as `symbol->id1,id2` with destination ids in
    ///   ascending order. The epsilon symbol is rendered as `ε`.
    fn pattern_to_nfa(pattern: Pattern) -> String {
        let states = PatternMatcher::prune_redundant_epsilons(
            PatternMatcher::compile_pattern_to_nfa(&pattern).expect("compile failed"),
        );

        let mut lines = Vec::new();
        // Ensure deterministic output
        let mut sorted_states: Vec<&NFAState> = states.iter().collect();
        sorted_states.sort_by_key(|s| s.id);

        for st in sorted_states {
            let mut parts = Vec::new();
            // ε-transitions first
            if !st.epsilon_transitions.is_empty() {
                let mut dests: Vec<_> = st.epsilon_transitions.iter().copied().collect();
                dests.sort_unstable();
                let list = dests
                    .iter()
                    .map(|d| {
                        if states[*d].is_excluded {
                            format!("{}!", d) // '!' marks excluded rows
                        } else {
                            d.to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                parts.push(format!("ε->{}", list));
            }
            // symbol transitions in lexicographic order
            let mut keys: Vec<_> = st.transitions.keys().collect();
            keys.sort();
            for sym in keys {
                let mut dests: Vec<_> = st.transitions[sym].iter().copied().collect();
                dests.sort_unstable();
                let list = dests
                    .iter()
                    .map(|d| {
                        if states[*d].is_excluded {
                            format!("{}!", d) // '!' marks excluded rows
                        } else {
                            d.to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                parts.push(format!("{}->{}", sym, list));
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

    fn nfa(pattern: Pattern) -> Vec<NFAState> {
        let states =
            PatternMatcher::compile_pattern_to_nfa(&pattern).expect("compile failed");
        PatternMatcher::prune_redundant_epsilons(states)
    }

    /// Create an NFA for a single symbol 'A' pattern
    fn nfa_for_symbol_a() -> Vec<NFAState> {
        nfa(Pattern::Symbol(Symbol::Named("A".to_string())))
    }

    // =====================================================================
    // Path Analysis Tests
    // =====================================================================

    /// Test consecutive path validation for various scenarios
    #[test]
    fn test_consecutive_path_validation() {
        // Empty path should be valid
        assert!(PatternMatcher::is_consecutive_path(&[]));

        // Single element path should be valid
        assert!(PatternMatcher::is_consecutive_path(&[(0, "A".into(), false)]));

        // Sequential row indices should be valid
        assert!(PatternMatcher::is_consecutive_path(&[
            (1, "A".into(), false),
            (2, "B".into(), false)
        ]));

        // Duplicate row indices should be valid (considered consecutive)
        assert!(PatternMatcher::is_consecutive_path(&[
            (1, "A".into(), false),
            (1, "B".into(), false)
        ]));

        // Non-consecutive indices (gap > 1) should be invalid
        assert!(!PatternMatcher::is_consecutive_path(&[
            (1, "A".into(), false),
            (3, "B".into(), false)
        ]));
    }

    // =====================================================================
    // Symbol Comparison Tests
    // =====================================================================

    /// Test symbol equality comparison for different symbol types
    #[test]
    fn test_symbol_equality_comparison() {
        let named_a = Symbol::Named("A".to_string());
        let named_a2 = Symbol::Named("A".to_string());
        let named_b = Symbol::Named("B".to_string());
        let start = Symbol::Start;
        let end = Symbol::End;

        // Same named symbols should be equal
        assert!(PatternMatcher::symbols_equal(&named_a, &named_a2));

        // Different named symbols should not be equal
        assert!(!PatternMatcher::symbols_equal(&named_a, &named_b));

        // Same anchor symbols should be equal
        assert!(PatternMatcher::symbols_equal(&start, &Symbol::Start));

        // Different anchor symbols should not be equal
        assert!(!PatternMatcher::symbols_equal(&start, &end));

        // Named symbols and anchors should not be equal
        assert!(!PatternMatcher::symbols_equal(&named_a, &start));
    }

    // =====================================================================
    // Pattern Recognition Tests
    // =====================================================================

    /// Test detection of repetition patterns in concatenations
    #[test]
    fn test_repetition_pattern_detection() {
        // Empty pattern should not be recognized as repetition
        assert!(!PatternMatcher::is_repetition_pattern(&[]));

        // Single element should not be recognized as repetition
        let single = vec![Pattern::Symbol(Symbol::Named("A".to_string()))];
        assert!(!PatternMatcher::is_repetition_pattern(&single));

        // Two identical symbols should be recognized as repetition
        let two_same = vec![
            Pattern::Symbol(Symbol::Named("A".to_string())),
            Pattern::Symbol(Symbol::Named("A".to_string())),
        ];
        assert!(PatternMatcher::is_repetition_pattern(&two_same));

        // Mixed symbols should not be recognized as repetition
        let mixed = vec![
            Pattern::Symbol(Symbol::Named("A".to_string())),
            Pattern::Symbol(Symbol::Named("B".to_string())),
        ];
        assert!(!PatternMatcher::is_repetition_pattern(&mixed));

        // Non-symbol elements should not be recognized as repetition
        let non_symbol = vec![
            Pattern::Symbol(Symbol::Named("A".to_string())),
            Pattern::Concat(vec![]),
        ];
        assert!(!PatternMatcher::is_repetition_pattern(&non_symbol));
    }

    // =====================================================================
    // NFA State Manipulation Tests
    // =====================================================================

    /// Test cloning NFA states with ID offset
    #[test]
    fn test_nfa_state_cloning_with_offset() {
        let original = nfa_for_symbol_a();
        let offset = 10;
        let cloned = PatternMatcher::clone_with_offset(&original, offset);

        // Verify length is preserved
        assert_eq!(original.len(), cloned.len());

        // Verify all state IDs and transitions are properly offset
        for (orig, clone) in original.iter().zip(cloned.iter()) {
            assert_eq!(orig.id + offset, clone.id);

            // Verify symbol transitions are offset
            for (sym, set) in &orig.transitions {
                let cloned_set = clone.transitions.get(sym).unwrap();
                let expected: HashSet<usize> = set.iter().map(|t| t + offset).collect();
                assert_eq!(expected, *cloned_set);
            }

            // Verify epsilon transitions are offset
            let expected_eps: HashSet<usize> = orig
                .epsilon_transitions
                .iter()
                .map(|t| t + offset)
                .collect::<HashSet<_>>();
            assert_eq!(expected_eps, clone.epsilon_transitions);
        }
    }

    // =====================================================================
    // Quantifier Application Tests
    // =====================================================================

    /// Test OneOrMore quantifier creates proper loop structure
    #[test]
    fn test_quantifier_one_or_more() {
        let states = PatternMatcher::apply_quantifier(
            nfa_for_symbol_a(),
            &RepetitionQuantifier::OneOrMore,
        )
        .unwrap();
        let last = states.last().unwrap();

        // Last state should be accepting
        assert!(last.is_accepting);
        // Last state should have epsilon transition back to start
        assert!(last.epsilon_transitions.contains(&0));
    }

    /// Test ZeroOrMore quantifier allows empty matches
    #[test]
    fn test_quantifier_zero_or_more() {
        let states = PatternMatcher::apply_quantifier(
            nfa_for_symbol_a(),
            &RepetitionQuantifier::ZeroOrMore,
        )
        .unwrap();

        // Start state should be accepting (empty match)
        assert!(states[0].is_accepting);
        // Last state should be accepting
        let last = states.last().unwrap();
        assert!(last.is_accepting);
        // Last state should have epsilon transition back to start
        assert!(last.epsilon_transitions.contains(&0));
    }

    /// Test AtMostOne quantifier allows zero or one occurrence
    #[test]
    fn test_quantifier_at_most_one() {
        let states = PatternMatcher::apply_quantifier(
            nfa_for_symbol_a(),
            &RepetitionQuantifier::AtMostOne,
        )
        .unwrap();

        // Start state should be accepting (zero occurrences)
        assert!(states[0].is_accepting);
        // Last state should be accepting (one occurrence)
        assert!(states.last().unwrap().is_accepting);
    }

    /// Test Exactly(0) quantifier creates single accepting state
    #[test]
    fn test_quantifier_exactly_zero() {
        let states = PatternMatcher::apply_quantifier(
            nfa_for_symbol_a(),
            &RepetitionQuantifier::Exactly(0),
        )
        .unwrap();

        // Should have exactly one state
        assert_eq!(states.len(), 1);
        // That state should be accepting
        assert!(states[0].is_accepting);
    }

    /// Test Exactly(n) quantifier creates proper chain
    #[test]
    fn test_quantifier_exactly_three() {
        let states = PatternMatcher::apply_quantifier(
            nfa_for_symbol_a(),
            &RepetitionQuantifier::Exactly(3),
        )
        .unwrap();

        // Should have 6 states (3 copies of 2-state pattern)
        assert_eq!(states.len(), 6);
        // Only final state should be accepting
        assert!(states.last().unwrap().is_accepting);
        assert!(!states[1].is_accepting);
        assert!(!states[3].is_accepting);
    }

    /// Test AtLeast(n) quantifier creates proper repeating structure
    #[test]
    fn test_quantifier_at_least_two() {
        let states = PatternMatcher::apply_quantifier(
            nfa_for_symbol_a(),
            &RepetitionQuantifier::AtLeast(2),
        )
        .unwrap();

        // Final state should be accepting
        assert!(states.last().unwrap().is_accepting);
        // Should have epsilon loop from last to start of last mandatory copy
        let sub_len = 2; // Each symbol pattern has 2 states
        let start_last = states.len() - sub_len;
        assert!(states
            .last()
            .unwrap()
            .epsilon_transitions
            .contains(&start_last));
    }

    /// Test AtMost(n) quantifier creates sink state structure
    #[test]
    fn test_quantifier_at_most_two() {
        let states = PatternMatcher::apply_quantifier(
            nfa_for_symbol_a(),
            &RepetitionQuantifier::AtMost(2),
        )
        .unwrap();

        let sink_id = states.len() - 1;
        // Sink state should be accepting
        assert!(states[sink_id].is_accepting);
        // Should have epsilon transition from start to sink (zero repetitions)
        assert!(states[0].epsilon_transitions.contains(&sink_id));
    }

    /// Test Range quantifier creates proper bounded structure
    #[test]
    fn test_quantifier_range() {
        let states = PatternMatcher::apply_quantifier(
            nfa_for_symbol_a(),
            &RepetitionQuantifier::Range(1, 2),
        )
        .unwrap();

        // Should have accepting sink state at the end
        assert!(states.last().unwrap().is_accepting);
    }

    // =====================================================================
    // Pattern Compilation Tests
    // =====================================================================

    /// Test compilation of simple repetition patterns
    #[test]
    fn test_compile_repetition_pattern() {
        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".to_string())),
            Pattern::Symbol(Symbol::Named("A".to_string())),
            Pattern::Symbol(Symbol::Named("A".to_string())),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1
        1: A->2
        2: A->3
        3*:
        ");
    }

    // =====================================================================
    // Alternation Pattern Tests
    // =====================================================================

    /// Test basic alternation of two symbols (A | B)
    #[test]
    fn test_alternation_two_symbols() {
        let pattern = Pattern::Alternation(vec![
            Pattern::Symbol(Symbol::Named("A".to_string())),
            Pattern::Symbol(Symbol::Named("B".to_string())),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: ε->1,3
        1: A->2
        2*:
        3: B->4
        4*:
        ");
    }

    /// Test alternation with empty list should error
    #[test]
    fn test_alternation_empty_error() {
        let pattern = Pattern::Alternation(vec![]);
        let err = PatternMatcher::compile_pattern_to_nfa(&pattern)
            .expect_err("should error on empty alternation");
        assert!(format!("{err}").contains("at least one sub-pattern"));
    }

    /// Test alternation of start anchor and a symbol (^ | A)
    #[test]
    fn test_alternation_anchor_and_symbol() {
        let pattern = Pattern::Alternation(vec![
            Pattern::Symbol(Symbol::Start),
            Pattern::Symbol(Symbol::Named("A".into())),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: ε->1,3
        1: ^->2
        2*:
        3: A->4
        4*:
        ");
    }

    /// Test alternation of three symbols (A | B | C)
    #[test]
    fn test_alternation_three_symbols() {
        let pattern: Pattern = Pattern::Alternation(vec![
            Pattern::Symbol(Symbol::Named("A".into())),
            Pattern::Symbol(Symbol::Named("B".into())),
            Pattern::Symbol(Symbol::Named("C".into())),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: ε->1,3,5
        1: A->2
        2*:
        3: B->4
        4*:
        5: C->6
        6*:
        ");
    }

    /// Test alternation mixing a repetition and single symbol ((A A) | B)
    #[test]
    fn test_alternation_repetition_with_symbol() {
        let rep = Pattern::Repetition(
            Box::new(Pattern::Symbol(Symbol::Named("A".into()))),
            RepetitionQuantifier::Exactly(2),
        );

        let pattern =
            Pattern::Alternation(vec![rep, Pattern::Symbol(Symbol::Named("B".into()))]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: ε->1,4
        1: A->2
        2: A->3
        3*:
        4: B->5
        5*:
        ");
    }

    // =====================================================================
    // Concatenation Pattern Tests
    // =====================================================================

    /// Test basic two-symbol concatenation
    #[test]
    fn test_concat_two_symbols() {
        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".to_string())),
            Pattern::Symbol(Symbol::Named("B".to_string())),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1
        1: B->2
        2*:
        ");
    }

    /// Test concatenation with empty input should fail
    #[test]
    fn test_concat_empty_patterns_error() {
        let err = PatternMatcher::compile_concat_patterns(&[])
            .expect_err("should error on empty input");
        assert!(format!("{err}").contains("at least one sub-pattern"));
    }

    /// Test concatenation of single symbol
    #[test]
    fn test_concat_single_symbol() {
        let pattern =
            Pattern::Concat(vec![Pattern::Symbol(Symbol::Named("A".to_string()))]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1
        1*:
        ");
    }

    /// Test concatenation of start anchor with symbol
    #[test]
    fn test_concat_start_anchor_with_symbol() {
        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Start),
            Pattern::Symbol(Symbol::Named("A".to_string())),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: ^->1
        1: A->2
        2*:
        ");
    }

    /// Test concatenation of symbol with end anchor
    #[test]
    fn test_concat_symbol_with_end_anchor() {
        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".to_string())),
            Pattern::Symbol(Symbol::End),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1
        1: $->2
        2*:
        ");
    }

    /// Test concatenation of three symbols
    #[test]
    fn test_concat_three_symbols() {
        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".to_string())),
            Pattern::Symbol(Symbol::Named("B".to_string())),
            Pattern::Symbol(Symbol::Named("C".to_string())),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1
        1: B->2
        2: C->3
        3*:
        ");
    }

    /// Test concatenation with repetition pattern
    #[test]
    fn test_concat_repetition_with_symbol() {
        let pattern = Pattern::Concat(vec![
            Pattern::Repetition(
                Box::new(Pattern::Symbol(Symbol::Named("A".into()))),
                RepetitionQuantifier::Exactly(2),
            ),
            Pattern::Symbol(Symbol::Named("B".into())),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1
        1: A->2
        2: B->3
        3*:
        ");
    }

    /// Test concatenation with nested concatenation
    #[test]
    fn test_concat_nested_concatenation() {
        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".into())),
            Pattern::Concat(vec![
                Pattern::Symbol(Symbol::Named("B".into())),
                Pattern::Symbol(Symbol::Named("C".into())),
            ]),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1
        1: B->2
        2: C->3
        3*:
        ");
    }

    // =====================================================================
    // NFA String Representation Tests
    // =====================================================================

    /// Test string representation of single symbol NFA
    #[test]
    fn test_nfa_string_single_symbol() {
        let pattern = Pattern::Symbol(Symbol::Named("A".into()));

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1
        1*:
        ");
    }

    /// Test string representation of start anchor NFA
    #[test]
    fn test_nfa_string_start_anchor() {
        let pattern = Pattern::Symbol(Symbol::Start);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: ^->1
        1*:
        ");
    }

    /// Ensure that pattern `(A?)` (zero or one A) compiles to an NFA where
    /// the start state is accepting and has a single `A` transition to a
    /// second, also-accepting, state.
    #[test]
    fn test_nfa_pattern_a_optional() {
        let pattern = Pattern::Repetition(
            Box::new(Pattern::Symbol(Symbol::Named("A".into()))),
            RepetitionQuantifier::AtMostOne,
        );

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0*: A->1
        1*:
        ");
    }

    /// Ensure that pattern `(A{,3})` (up to three A's) compiles to the
    /// expected chain of three copies of `A` plus ε-transitions into a
    /// single accepting sink.
    #[test]
    fn test_nfa_pattern_a_at_most_three() {
        let pattern = Pattern::Repetition(
            Box::new(Pattern::Symbol(Symbol::Named("A".into()))),
            RepetitionQuantifier::AtMost(3),
        );

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: ε->5 | A->1
        1: ε->2,5
        2: A->3
        3: ε->4,5
        4: A->5
        5*:
        ");
    }

    // =====================================================================
    // Group Pattern Tests
    // =====================================================================

    /// Test compilation of a grouped single symbol pattern `(A)`
    #[test]
    fn test_compile_group_single_symbol() {
        let pattern =
            Pattern::Group(Box::new(Pattern::Symbol(Symbol::Named("A".into()))));
        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1
        1*:
        ");
    }

    /// Test compilation of a grouped concatenation pattern `(A B)`
    #[test]
    fn test_compile_group_concat() {
        let inner = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".into())),
            Pattern::Symbol(Symbol::Named("B".into())),
        ]);
        let pattern = Pattern::Group(Box::new(inner));

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1
        1: B->2
        2*:
        ");
    }

    /// Test compilation of a complex grouped pattern combining alternation,
    /// concatenation and repetition: `(A B | C+)`
    #[test]
    fn test_compile_group_complex_pattern() {
        let alt1 = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".into())),
            Pattern::Symbol(Symbol::Named("B".into())),
        ]);

        let alt2 = Pattern::Repetition(
            Box::new(Pattern::Symbol(Symbol::Named("C".into()))),
            RepetitionQuantifier::OneOrMore,
        );
        let pattern = Pattern::Group(Box::new(Pattern::Alternation(vec![alt1, alt2])));

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: ε->1,4
        1: A->2
        2: B->3
        3*:
        4: C->5
        5*: ε->4
        ");
    }

    /// Test compilation of a deeply nested group pattern. Placeholder NFA
    /// string is used and the test is marked `ignore` until the exact
    /// structure is validated.
    #[test]
    fn test_compile_nested_groups() {
        let inner_alt = Pattern::Alternation(vec![
            Pattern::Symbol(Symbol::Named("B".into())),
            Pattern::Repetition(
                Box::new(Pattern::Symbol(Symbol::Named("C".into()))),
                RepetitionQuantifier::OneOrMore,
            ),
        ]);

        let nested = Pattern::Group(Box::new(Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".into())),
            Pattern::Group(Box::new(inner_alt)),
        ])));

        let pattern = Pattern::Group(Box::new(Pattern::Concat(vec![
            nested,
            Pattern::Symbol(Symbol::Named("D".into())),
        ])));

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1
        1: ε->2,3
        2: B->5
        3: C->4
        4: ε->3,5
        5: D->6
        6*:
        ");
    }

    // =====================================================================
    // Permute Pattern Tests
    // =====================================================================

    /// Test compilation of a PERMUTE pattern with two symbols (A, B)
    #[test]
    fn test_compile_permute_two_symbols() {
        let pattern =
            Pattern::Permute(vec![Symbol::Named("A".into()), Symbol::Named("B".into())]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: ε->1,4
        1: A->2
        2: B->3
        3*:
        4: B->5
        5: A->6
        6*:
        ");
    }

    /// Test compilation of a PERMUTE pattern with a single symbol (degenerate case)
    #[test]
    fn test_compile_permute_single_symbol() {
        let pattern = Pattern::Permute(vec![Symbol::Named("A".into())]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1
        1*:
        ");
    }

    /// Test compilation of a PERMUTE pattern with three symbols (A, B, C)
    #[test]
    fn test_compile_permute_with_three_symbols() {
        let pattern = Pattern::Permute(vec![
            Symbol::Named("A".into()),
            Symbol::Named("B".into()),
            Symbol::Named("C".into()),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: ε->1,5,9,13,17,21
        1: A->2
        2: B->3
        3: C->4
        4*:
        5: A->6
        6: C->7
        7: B->8
        8*:
        9: B->10
        10: A->11
        11: C->12
        12*:
        13: B->14
        14: C->15
        15: A->16
        16*:
        17: C->18
        18: A->19
        19: B->20
        20*:
        21: C->22
        22: B->23
        23: A->24
        24*:
        ");
    }

    /// Test compilation of a PERMUTE pattern with five symbols (A, B, C, D, E) to trigger counter-based automaton.
    /// This should create an automaton with 31 states (2 ^ 5 - 1) instead of 120 states (5!).
    #[test]
    fn test_compile_permute_with_five_symbols() {
        let pattern = Pattern::Permute(vec![
            Symbol::Named("A".into()),
            Symbol::Named("B".into()),
            Symbol::Named("C".into()),
            Symbol::Named("D".into()),
            Symbol::Named("E".into()),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
    0: A->1 | B->2 | C->3 | D->4 | E->5
    1: B->6 | C->7 | D->8 | E->9
    2: A->6 | C->10 | D->11 | E->12
    3: A->7 | B->10 | D->13 | E->14
    4: A->8 | B->11 | C->13 | E->15
    5: A->9 | B->12 | C->14 | D->15
    6: C->16 | D->17 | E->18
    7: B->16 | D->19 | E->20
    8: B->17 | C->19 | E->21
    9: B->18 | C->20 | D->21
    10: A->16 | D->22 | E->23
    11: A->17 | C->22 | E->24
    12: A->18 | C->23 | D->24
    13: A->19 | B->22 | E->25
    14: A->20 | B->23 | D->25
    15: A->21 | B->24 | C->25
    16: D->26 | E->27
    17: C->26 | E->28
    18: C->27 | D->28
    19: B->26 | E->29
    20: B->27 | D->29
    21: B->28 | C->29
    22: A->26 | E->30
    23: A->27 | D->30
    24: A->28 | C->30
    25: A->29 | B->30
    26: E->31
    27: D->31
    28: C->31
    29: B->31
    30: A->31
    31*:
    ");
    }

    /// Test compilation of a PERMUTE pattern with duplicate symbols (A, A, B)
    #[test]
    fn test_compile_permute_with_duplicates() {
        let pattern = Pattern::Permute(vec![
            Symbol::Named("A".into()),
            Symbol::Named("A".into()),
            Symbol::Named("C".into()),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: ε->1,5,9
        1: A->2
        2: A->3
        3: C->4
        4*:
        5: A->6
        6: C->7
        7: A->8
        8*:
        9: C->10
        10: A->11
        11: A->12
        12*:
        ");
    }

    // =====================================================================
    // Exclusion Pattern Tests
    // =====================================================================

    /// Excluding a single symbol should generate an NFA that uses the
    /// internal exclusion variant name.
    #[test]
    fn test_exclude_single_symbol() {
        let pattern = Pattern::Exclude(Symbol::Named("A".into()));

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1!
        1*:
        ");
    }

    /// Inclusion followed by exclusion of a different symbol.
    #[test]
    fn test_include_then_exclude() {
        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".into())),
            Pattern::Exclude(Symbol::Named("B".into())),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1
        1: B->2!
        2*:
        ");
    }

    /// Two exclusions followed by an inclusion of the same symbol.
    #[test]
    fn test_triple_with_start_exclusions() {
        let pattern = Pattern::Concat(vec![
            Pattern::Exclude(Symbol::Named("A".into())),
            Pattern::Exclude(Symbol::Named("A".into())),
            Pattern::Symbol(Symbol::Named("A".into())),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1!
        1: A->2!
        2: A->3
        3*:
        ");
    }

    /// Inclusion followed by two exclusions of the same symbol.
    #[test]
    fn test_triple_with_end_exclusions() {
        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".into())),
            Pattern::Exclude(Symbol::Named("A".into())),
            Pattern::Exclude(Symbol::Named("A".into())),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1
        1: A->2!
        2: A->3!
        3*:
        ");
    }

    /// Exclusion in the middle of two inclusions.
    #[test]
    fn test_triple_with_middle_exclusion() {
        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".into())),
            Pattern::Exclude(Symbol::Named("A".into())),
            Pattern::Symbol(Symbol::Named("A".into())),
        ]);

        assert_snapshot!(pattern_to_nfa(pattern), @r"
        0: A->1
        1: A->2!
        2: A->3
        3*:
        ");
    }

    // =====================================================================
    // Helper Function Unit Tests
    // =====================================================================
    #[cfg(test)]
    mod helper_tests {
        use super::*;

        fn simple_matcher() -> PatternMatcher {
            // A single-symbol pattern "A"
            let pattern = Pattern::Symbol(Symbol::Named("A".into()));

            let symbols = vec!["A".to_string()];

            PatternMatcher::new(pattern, symbols, None, None).unwrap()
        }

        fn boolean_batch(values: &[bool]) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new("flag", DataType::Boolean, false),
                Field::new("__mr_symbol_A", DataType::Boolean, true),
            ]));
            let flag_array = BooleanArray::from(values.to_vec());
            let symbol_array = BooleanArray::from(values.to_vec()); // Use same values for simplicity
            RecordBatch::try_new(
                schema,
                vec![Arc::new(flag_array), Arc::new(symbol_array)],
            )
            .unwrap()
        }

        #[test]
        fn test_epsilon_closure() {
            let matcher = simple_matcher();
            // NFA for single symbol has states 0 and 1, no epsilons → closure should be itself
            let cls0 = matcher.epsilon_closure(0);
            assert_eq!(cls0, [0].into_iter().collect());
            let cls1 = matcher.epsilon_closure(1);
            assert_eq!(cls1, [1].into_iter().collect());
        }

        #[test]
        fn test_expand_all_eps() {
            let matcher = simple_matcher();
            let active = vec![ActiveNFAState {
                state_id: 0,
                path: Vec::new(),
                start_row: 0,
            }];
            let expanded = matcher.expand_all_eps(&active);
            // Should still contain exactly the original state (no epsilons)
            assert_eq!(expanded.len(), 1);
            assert_eq!(expanded[0].state_id, 0);
        }

        #[test]
        fn test_maybe_update_best() {
            let mut best: Option<Vec<(usize, String, bool)>> = None;
            let mut end: Option<usize> = None;
            PatternMatcher::maybe_update_best(
                &mut best,
                &mut end,
                vec![(1, "A".into(), false)],
                10,
            );
            assert!(best.is_some());
            let longer = vec![(1, "A".into(), false), (2, "A".into(), false)];
            PatternMatcher::maybe_update_best(&mut best, &mut end, longer.clone(), 10);
            assert_eq!(best.unwrap().len(), 2);
        }

        #[test]
        fn test_path_has_physical_rows() {
            assert!(PatternMatcher::path_has_physical_rows(
                &[(1, "A".into(), false)],
                3
            ));
            assert!(!PatternMatcher::path_has_physical_rows(
                &[(0, "^".into(), false)],
                3
            ));
        }

        #[test]
        fn test_next_row() {
            // When best_end_row is Some and ahead of current
            assert_eq!(PatternMatcher::next_row(5, Some(6)), 7);
            // When best_end_row == current (zero-length match) ensure progress
            assert_eq!(PatternMatcher::next_row(5, Some(5)), 6);
            // When no match
            assert_eq!(PatternMatcher::next_row(5, None), 6);
        }

        #[test]
        fn test_scan_from_and_build_output() {
            let mut matcher = simple_matcher();
            // batch: T F T F
            let batch = boolean_batch(&[true, false, true, false]);

            // Update column indices based on the batch schema
            matcher.update_column_indices(&batch).unwrap();

            let (best, end) = matcher.scan_from(&batch, 1, 6, 4).unwrap();
            // Starting at first physical row (row_idx=1) we expect single match on that row
            assert!(best.is_some());
            assert_eq!(end, Some(1));

            // Manually add match to build_output
            matcher.next_match_number = 2; // first match id used will be 1
            let output = matcher.build_output(&batch, &[(1, best.unwrap())]).unwrap();
            assert!(output.is_some());
            let out = output.unwrap();
            // Should contain exactly 1 row (only first TRUE)
            assert_eq!(out.num_rows(), 1);
            // Metadata columns present (classifier/match_no/seq_no/last_row_flag/is_included_row) + input columns
            assert_eq!(out.num_columns(), 7);
            let classifier = out
                .column(2)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            assert_eq!(classifier.value(0), "A");
            let match_no = out
                .column(3)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            assert_eq!(match_no.value(0), 1);
            let seq_no = out
                .column(4)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            assert_eq!(seq_no.value(0), 1);
        }

        #[test]
        fn test_evaluate_symbols_static() {
            // TRUE, FALSE rows
            let batch = boolean_batch(&[true, false]);
            let symbol_columns = vec![("A".to_string(), 1)]; // Column index 1 is __mr_symbol_A
                                                             // Row 0 virtual start → A should be false, ^ true
            let row0 =
                PatternMatcher::evaluate_symbols_static(&symbol_columns, &batch, 0, 2, 0)
                    .unwrap();
            let map0: HashMap<_, _> = row0.into_iter().collect();
            assert!(!map0["A"] && map0["^"]);
            // Row 1 physical TRUE → A true
            let row1 =
                PatternMatcher::evaluate_symbols_static(&symbol_columns, &batch, 1, 2, 0)
                    .unwrap();
            let map1: HashMap<_, _> = row1.into_iter().collect();
            assert!(map1["A"] && !map1["^"]);
        }
    }
}
