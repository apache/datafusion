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

use crate::metrics::{Count, Gauge, Time};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion_common::Result;
use std::ops::Deref;
use std::sync::Arc;

use crate::match_recognize::compile::CompiledPattern;
use crate::match_recognize::nfa::RowIdx;

// Add module declarations and imports for the split helpers
mod dedup;
mod symbols;

pub(crate) mod candidate_mod;
pub(crate) mod epsilon;
pub(crate) mod generation;
pub(crate) mod row_loop;
pub(crate) mod state_set;

/// Mode that controls how ε-closures should treat ^ / $ anchor predicates.
#[derive(Copy, Clone, Debug)]
pub(crate) enum AnchorMode {
    /// Ignore anchor predicates completely (fast path).
    Ignore,
    /// Evaluate anchors on the given virtual row (0-based) within a partition
    /// of `total_virt_rows` rows (including the virtual End-Of-Input row).
    Check {
        virt_row: RowIdx,
        total_virt_rows: usize,
    },
}

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
    /// Optional metrics hooks to update runtime counters/gauges
    metrics: Option<MatcherMetrics>,
}

#[derive(Clone, Debug)]
struct MatcherMetrics {
    define_pred_evals: Count,
    nfa_state_transitions: Count,
    active_states_max: Gauge,
    match_compute_time: Time,
    symbol_eval_time: Time,
    epsilon_eval_time: Time,
    nfa_eval_time: Time,
    slice_time: Time,
    row_loop_time: Time,
    transition_time: Time,
    candidate_time: Time,
    alloc_time: Time,
}

// Candidate now lives in `candidate_mod`.

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
            metrics: None,
        })
    }

    /// Create a new PatternMatcher wired up with runtime metrics.
    pub(crate) fn new_with_metrics(
        compiled: Arc<CompiledPattern>,
        schema: SchemaRef,
        pm: &crate::match_recognize::pattern_exec::PatternMetrics,
    ) -> Result<Self> {
        let mut matcher = Self::new(compiled, schema)?;
        matcher.metrics = Some(MatcherMetrics {
            define_pred_evals: pm.define_pred_evals.clone(),
            nfa_state_transitions: pm.nfa_state_transitions.clone(),
            active_states_max: pm.active_states_max.clone(),
            match_compute_time: pm.match_compute_time.clone(),
            symbol_eval_time: pm.symbol_eval_time.clone(),
            epsilon_eval_time: pm.epsilon_eval_time.clone(),
            nfa_eval_time: pm.nfa_eval_time.clone(),
            slice_time: pm.slice_time.clone(),
            row_loop_time: pm.row_loop_time.clone(),
            transition_time: pm.transition_time.clone(),
            candidate_time: pm.candidate_time.clone(),
            alloc_time: pm.alloc_time.clone(),
        });
        Ok(matcher)
    }

    /// Returns a clone of the shared compiled pattern `Arc`.
    #[inline]
    pub fn compiled_arc(&self) -> Arc<CompiledPattern> {
        self.compiled.clone()
    }

    // Core helper methods live in the `row_loop`, `epsilon`, and `state_set` sub-modules.

    /// Reset internal state so the matcher can process a new logical partition.
    #[inline]
    pub fn finish_partition(&mut self) {
        self.reset();
    }

    /// Reset per-partition counters (exposed for `finish_partition`).
    #[inline]
    pub fn reset(&mut self) {
        self.next_match_number = 1;
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
    use crate::match_recognize::pattern_exec::MatchAccumulator;
    use arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray, UInt32Array};
    use arrow::datatypes::DataType;
    use arrow_schema::{Field, Schema};
    use datafusion_expr::match_recognize::{EmptyMatchesMode, RowsPerMatch, Symbol};
    use std::sync::Arc;

    /// Helper that executes the matcher over the entire `batch` and returns the
    /// materialised output `RecordBatch` using the new streaming API.
    fn run_matcher_to_batch(
        matcher: &mut PatternMatcher,
        batch: &RecordBatch,
    ) -> RecordBatch {
        let mut acc = MatchAccumulator::new(
            batch.schema_ref().clone(),
            matcher.compiled_arc(),
            batch.num_rows() + 1,
        );
        matcher
            .process_rows(batch, 0, batch.num_rows(), &mut acc)
            .unwrap();
        acc.flush(batch)
            .unwrap()
            .expect("matcher produced no output")
    }

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

        let out = run_matcher_to_batch(&mut matcher, &batch);

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

        let out = run_matcher_to_batch(&mut matcher, &batch);
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
        let out_default = run_matcher_to_batch(&mut matcher_default, &batch);
        assert_eq!(out_default.num_rows(), 2);

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
        let out_overlap = run_matcher_to_batch(&mut matcher_overlap, &batch);
        assert_eq!(out_overlap.num_rows(), 4);
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

        let out = run_matcher_to_batch(&mut matcher, &batch);
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
        let out = run_matcher_to_batch(&mut matcher, &batch);

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
            let out_batch = run_matcher_to_batch(&mut matcher, &batch);
            assert_eq!(out_batch.num_rows(), 2, "case {} wrong row count", label);
            let out = &out_batch;

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
