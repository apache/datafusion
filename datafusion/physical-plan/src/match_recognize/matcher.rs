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

//! Streaming pattern matcher operating on Arrow `RecordBatch`es.

use arrow::array::{
    Array, BooleanArray, BooleanBuilder, RecordBatch, StringBuilder, UInt64Builder,
};
use arrow::compute;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion_common::Result;
use datafusion_expr::match_recognize::AfterMatchSkip;
use std::ops::Deref;
use std::sync::Arc;

use crate::match_recognize::compile::CompiledPattern;
use crate::match_recognize::nfa::{
    ActiveNFAState, AnchorPredicate, BestMatch, PathNode, PathScore, PathStep, RowIdx,
    Sym,
};

/// Metadata for a symbol column. Keeps the numeric symbol id so that the hot
/// path (`evaluate_symbols_static`) can avoid a hashmap lookup.
#[derive(Debug, Clone)]
struct SymbolColumn {
    id: usize,
    // Resolved (static) column index inside the input RecordBatch schema
    column_idx: usize,
}

/// Core pattern matching engine implementing NFA logic
pub struct PatternMatcher {
    /// Shared, immutable compiled representation.
    compiled: Arc<CompiledPattern>,
    /// Symbol metadata, including the resolved (static) column index. The
    /// mapping is computed once during construction as the schema of every
    /// RecordBatch produced by a physical plan is immutable.
    symbol_columns: Vec<SymbolColumn>,
    /// Next match number to assign (run-time mutable state)
    next_match_number: u64,
}

impl PatternMatcher {
    /// Create a new `PatternMatcher` instance.
    pub fn new(compiled: Arc<CompiledPattern>, schema: SchemaRef) -> Result<Self> {
        // Build symbol column metadata based on the compiled pattern's symbol mapping
        let symbol_columns: Vec<SymbolColumn> = compiled
            .symbols_iter()
            .map(|(id, name)| {
                let col_name = format!("__mr_symbol_{}", name);
                let column_idx = schema.index_of(&col_name)?;
                Ok(SymbolColumn { id, column_idx })
            })
            .collect::<Result<Vec<_>, datafusion_common::DataFusionError>>()?;

        Ok(Self {
            compiled,
            symbol_columns,
            next_match_number: 1,
        })
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

        // Reusable buffers for ε-reachable states to avoid repeated allocations
        let mut reachable_tmp: Vec<usize> = Vec::new();
        let mut reachable_inner: Vec<usize> = Vec::new();
        let mut epsilon_visited: Vec<u32> = vec![0; self.compiled.nfa.len()];
        // Generation counter for the reusable visited bitmap
        let mut gen_counter: u32 = 1;

        // Preallocate reusable buffer for symbol evaluations to avoid per-row allocations
        let mut symbol_matches: Vec<bool> = vec![false; self.compiled.id_to_symbol.len()];

        // Dense arena (indexed by `state_id`) for deduplicating successor states.
        // Using `Vec<Option<_>>` avoids hashing entirely as the ID space is dense.
        let mut next_active_vec: Vec<Option<ActiveNFAState>> =
            vec![None; self.compiled.nfa.len()];
        // Track which entries we populated on the current row so that we can
        // iterate over them directly instead of scanning the entire dense
        // vector afterwards.
        let mut populated_indices: Vec<usize> = Vec::new();

        while !active_states.is_empty() && current_row < total_rows {
            // Reset all entries instead of reallocating on every row.
            next_active_vec.fill(None);
            populated_indices.clear();
            self.evaluate_symbols_static(
                bool_columns,
                current_row,
                num_rows,
                &mut symbol_matches,
            )?;
            // Use O(1) direct indexing to deduplicate successor states
            for active in &active_states {
                self.epsilon_reachable(
                    active.state_id,
                    current_row,
                    num_rows,
                    &mut reachable_tmp,
                    &mut epsilon_visited,
                    gen_counter,
                );
                gen_counter = gen_counter.wrapping_add(1);
                if gen_counter == 0 {
                    epsilon_visited.fill(0);
                    gen_counter = 1;
                }
                for &state_id in &reachable_tmp {
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
                            let excl_flag = self.compiled.nfa[next_state_id].is_excluded;
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

                            // Iterate over states reachable via ε-transitions that satisfy anchor predicates
                            self.epsilon_reachable(
                                next_state_id,
                                current_row,
                                num_rows,
                                &mut reachable_inner,
                                &mut epsilon_visited,
                                gen_counter,
                            );
                            gen_counter = gen_counter.wrapping_add(1);
                            if gen_counter == 0 {
                                epsilon_visited.fill(0);
                                gen_counter = 1;
                            }
                            if reachable_inner
                                .iter()
                                .any(|&sid| self.compiled.nfa[sid].is_accepting)
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
                            if let Some(prev) = next_active_vec[next_state_id].as_mut() {
                                // Inline `prev_row` directly in the comparison tuple
                                let better = (
                                    std::cmp::Reverse(new_score),
                                    next_state_id,
                                    current_row,
                                ) < (
                                    std::cmp::Reverse(prev.score),
                                    prev.state_id,
                                    prev.path
                                        .as_ref()
                                        .map(|p| p.step.row)
                                        .unwrap_or(current_row),
                                );

                                if better {
                                    prev.path = Some(new_node.clone());
                                    prev.score = new_score;
                                    // `state_id` is identical, no update required
                                }
                            } else {
                                next_active_vec[next_state_id] = Some(ActiveNFAState {
                                    state_id: next_state_id,
                                    path: Some(new_node.clone()),
                                    score: new_score,
                                });
                                // Remember that we touched this index so we can
                                // efficiently move active states over after the
                                // row is processed.
                                populated_indices.push(next_state_id);
                            }
                        }
                    }
                }
            }

            // Also check for accepting states in current active states (for patterns that can match empty sequences)
            for active in &active_states {
                self.epsilon_reachable(
                    active.state_id,
                    current_row,
                    num_rows,
                    &mut reachable_inner,
                    &mut epsilon_visited,
                    gen_counter,
                );
                gen_counter = gen_counter.wrapping_add(1);
                if gen_counter == 0 {
                    epsilon_visited.fill(0);
                    gen_counter = 1;
                }
                if reachable_inner
                    .iter()
                    .any(|&sid| self.compiled.nfa[sid].is_accepting)
                {
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
                    // Only need one accepting state per active paths
                    continue;
                }
            }

            // Collect populated entries into the vector for the next iteration.
            active_states = populated_indices
                .iter()
                .map(|&idx| {
                    // SAFETY: we only stored indices for which we inserted a
                    // `Some` value above, so `unwrap` cannot panic here.
                    next_active_vec[idx].clone().unwrap()
                })
                .collect();
            current_row += 1;
        }

        // Check remaining active states for accepting states reachable via epsilon transitions
        // This handles cases where we've consumed all input but can still reach accepting states
        for active in &active_states {
            self.epsilon_reachable(
                active.state_id,
                current_row,
                num_rows,
                &mut reachable_inner,
                &mut epsilon_visited,
                gen_counter,
            );
            gen_counter = gen_counter.wrapping_add(1);
            if gen_counter == 0 {
                epsilon_visited.fill(0);
                gen_counter = 1;
            }
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
                .filter(|ps| ps.row < num_rows)
                .map(|ps| (ps.row, ps.sym.to_index(), ps.excluded))
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
                    self.compiled.id_to_symbol[sym_id]
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
                compute::take(col, &matched_indices, None)?;
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

        // Directly use the provided `row_idx` as it now refers to the physical row.
        if row_idx >= num_rows {
            // Virtual position after the last row – no symbols match.
            return Ok(());
        }

        let physical_idx = row_idx;

        for (sc, bool_col) in self.symbol_columns.iter().zip(bool_columns.iter()) {
            let matched = bool_col.value(physical_idx);
            out[sc.id] = matched;
        }

        Ok(())
    }

    /// Process a batch and return match results
    pub fn process_batch(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        // Build a cache of BooleanArray slices for all symbol columns to
        // avoid dynamic down-casts in the hot loop.
        let mut bool_columns: Vec<&BooleanArray> =
            Vec::with_capacity(self.symbol_columns.len());
        for sc in &self.symbol_columns {
            let idx = sc.column_idx;

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
        // Include one extra virtual position *after* the last row so that
        // end-anchor ($) predicates are evaluated at the correct offset.
        let total_rows = num_rows + 1;

        let mut row_idx = 0;
        let mut last_matched_row: Option<usize> = None;
        let mut all_matches: Vec<(u64, Vec<PathStep>)> = Vec::new();

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
                if !path_vec.is_empty() {
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

    /// Reset internal counters so the matcher can process a new partition.
    pub fn reset(&mut self) {
        self.next_match_number = 1;
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
        let candidate_key = (std::cmp::Reverse(score), state_id, row);
        let should_update = best_match.as_ref().map_or(true, |cur| {
            let current_key = (std::cmp::Reverse(cur.score), cur.state_id, cur.row);
            candidate_key < current_key
        });

        if !should_update {
            return; // current best wins – nothing to do
        }

        // Materialise the path only for the winning candidate.
        let path_vec = crate::match_recognize::nfa::collect_path(&node);

        *best_end_row = Some(path_vec.last().map(|ps| ps.row).unwrap_or(row));

        *best_match = Some(BestMatch {
            path: path_vec,
            score,
            state_id,
            row,
        });
    }

    /// Explore ε-transitions that satisfy their optional `AnchorPredicate` and collect
    /// reachable state IDs into `out`. The buffer is cleared at the beginning.
    fn epsilon_reachable(
        &self,
        start_state: usize,
        current_row: usize,
        num_rows: usize,
        out: &mut Vec<usize>,
        visited: &mut [u32],
        generation: u32,
    ) {
        out.clear();
        let mut stack = vec![start_state];

        while let Some(sid) = stack.pop() {
            if visited[sid] == generation {
                continue;
            }
            visited[sid] = generation;
            out.push(sid);

            for &(dst, pred) in &self.compiled.nfa[sid].epsilon_transitions_pred {
                let allowed = match pred {
                    None => true,
                    Some(AnchorPredicate::StartOfInput) => current_row == 0,
                    Some(AnchorPredicate::EndOfInput) => current_row == num_rows,
                };
                if allowed {
                    stack.push(dst);
                }
            }
        }
    }
}

// Provide transparent access to compile-time fields (e.g. `self.id_to_symbol`).
impl Deref for PatternMatcher {
    type Target = CompiledPattern;
    fn deref(&self) -> &Self::Target {
        &self.compiled
    }
}

/// Fixed metadata columns appended by `pattern_schema`
const MATCH_METADATA_COLUMNS: &[(&str, arrow::datatypes::DataType)] = &[
    ("__mr_classifier", arrow::datatypes::DataType::Utf8),
    ("__mr_match_number", arrow::datatypes::DataType::UInt64),
    (
        "__mr_match_sequence_number",
        arrow::datatypes::DataType::UInt64,
    ),
    (
        "__mr_is_last_match_row",
        arrow::datatypes::DataType::Boolean,
    ),
    ("__mr_is_included_row", arrow::datatypes::DataType::Boolean),
];

pub(crate) fn pattern_schema(input_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = input_schema.fields().to_vec();

    // Append fixed match metadata columns
    for (name, data_type) in MATCH_METADATA_COLUMNS {
        fields.push(Arc::new(Field::new(*name, data_type.clone(), false)));
    }

    Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::match_recognize::compile::CompiledPattern;
    use arrow::array::{ArrayRef, BooleanArray, StringArray, UInt32Array};
    use arrow::datatypes::DataType;
    use arrow_schema::{Field, Schema};
    use datafusion_expr::match_recognize::{EmptyMatchesMode, RowsPerMatch, Symbol};
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
        use datafusion_expr::match_recognize::Pattern;

        let batch = make_batch(&[1, 2, 3, 4], &[("A", vec![true, false, true, true])]);

        let pattern = Pattern::Symbol(Symbol::Named("A".into()));
        let compiled = Arc::new(
            CompiledPattern::compile(pattern, vec!["A".into()], None, None).unwrap(),
        );
        let mut matcher = PatternMatcher::new(compiled, batch.schema().clone()).unwrap();

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
        use datafusion_expr::match_recognize::Pattern;

        // Two-row input: first `A`, then `B`
        let batch = make_batch(
            &[1, 2],
            &[("A", vec![true, false]), ("B", vec![false, true])],
        );

        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".into())),
            Pattern::Exclude(Symbol::Named("B".into())),
        ]);

        let compiled = Arc::new(
            CompiledPattern::compile(pattern, vec!["A".into(), "B".into()], None, None)
                .unwrap(),
        );
        let mut matcher = PatternMatcher::new(compiled, batch.schema().clone()).unwrap();

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
        use datafusion_expr::match_recognize::{AfterMatchSkip, Pattern};

        // Input AAA (three consecutive A rows)
        let batch = make_batch(&[1, 2, 3], &[("A", vec![true, true, true])]);

        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".into())),
            Pattern::Symbol(Symbol::Named("A".into())),
        ]);

        // Default (PAST LAST ROW) – one match covering rows 1&2 → 2 output rows
        let compiled_default = Arc::new(
            CompiledPattern::compile(pattern.clone(), vec!["A".into()], None, None)
                .unwrap(),
        );
        let mut matcher_default =
            PatternMatcher::new(compiled_default, batch.schema().clone()).unwrap();
        let out_default = matcher_default.process_batch(batch.clone()).unwrap();
        assert_eq!(out_default[0].num_rows(), 2);

        // TO NEXT ROW – overlapping matches (1&2) and (2&3) → 4 output rows
        let compiled_overlap = Arc::new(
            CompiledPattern::compile(
                pattern,
                vec!["A".into()],
                Some(AfterMatchSkip::ToNextRow),
                None,
            )
            .unwrap(),
        );
        let mut matcher_overlap =
            PatternMatcher::new(compiled_overlap, batch.schema().clone()).unwrap();
        let out_overlap = matcher_overlap.process_batch(batch.clone()).unwrap();
        assert_eq!(out_overlap[0].num_rows(), 4);
    }

    /// Test `WITH UNMATCHED ROWS` behaviour: every input row should appear in
    /// the output even when no pattern rows are found.
    #[test]
    fn with_unmatched_rows() {
        use datafusion_expr::match_recognize::Pattern;

        let batch = make_batch(&[1, 2, 3], &[("A", vec![false, false, false])]);

        let pattern = Pattern::Symbol(Symbol::Named("A".into()));
        let rows_per_match =
            Some(RowsPerMatch::AllRows(Some(EmptyMatchesMode::WithUnmatched)));

        let compiled = Arc::new(
            CompiledPattern::compile(pattern, vec!["A".into()], None, rows_per_match)
                .unwrap(),
        );
        let mut matcher = PatternMatcher::new(compiled, batch.schema().clone()).unwrap();

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
        use datafusion_expr::match_recognize::{Pattern, RepetitionQuantifier};

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

        let compiled = Arc::new(
            CompiledPattern::compile(pattern, vec!["A".into()], None, None).unwrap(),
        );
        let mut matcher = PatternMatcher::new(compiled, batch.schema().clone()).unwrap();
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
        use datafusion_expr::match_recognize::Pattern;

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

            let compiled = Arc::new(
                CompiledPattern::compile(
                    pattern,
                    vec!["A".into(), "B".into()],
                    None,
                    None,
                )
                .unwrap(),
            );
            let mut matcher =
                PatternMatcher::new(compiled, batch.schema().clone()).unwrap();
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
