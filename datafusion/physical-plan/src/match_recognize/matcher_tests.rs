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

use crate::match_recognize::compile::CompiledPattern;
use crate::match_recognize::pattern_exec::MatchAccumulator;
use crate::match_recognize::PatternMatcher;
use arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion_expr::match_recognize::columns::MrMetadataColumn;
use datafusion_expr::match_recognize::{
    AfterMatchSkip, EmptyMatchesMode, RowsPerMatch, Symbol,
};
use std::sync::Arc;

/// Helper that executes the matcher over the entire `batch` and returns the
/// materialised output `RecordBatch` using the streaming API.
fn run_matcher_to_batch(
    matcher: &mut PatternMatcher,
    batch: &RecordBatch,
) -> RecordBatch {
    // Select all metadata columns in their canonical order for tests
    let all_meta_cols_vec: Vec<MrMetadataColumn> = MrMetadataColumn::all().to_vec();
    let classifier_symbols: Vec<String> = matcher
        .compiled_arc()
        .symbols_iter()
        .map(|(_, name)| name.to_string())
        .collect();
    let passthrough_input_indices: Vec<usize> = (0..batch.num_columns()).collect();

    let spec = datafusion_expr::match_recognize::MatchRecognizeOutputSpec::new(
        passthrough_input_indices.clone(),
        all_meta_cols_vec.clone(),
        classifier_symbols.clone(),
    );

    let mut acc = MatchAccumulator::try_new(
        // Output schema = passthrough input + all meta columns + classifier bitsets
        spec.build_arrow_schema(batch.schema_ref()),
        matcher.compiled_arc(),
        batch.num_rows() + 1,
        spec,
    )
    .unwrap();
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
        let field_name = format!("__mr_symbol_{}", name.to_ascii_lowercase());
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
        CompiledPattern::compile(
            pattern,
            vec!["A".into()],
            AfterMatchSkip::PastLastRow,
            RowsPerMatch::OneRow,
        )
        .unwrap(),
    );
    let mut matcher = PatternMatcher::new(compiled, &batch.schema()).unwrap();

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

    // Expect three matches on rows 1, 3 and 4 (1-based physical indices)
    let expected_classifier = ["A", "A", "A"];
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
    }
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
        CompiledPattern::compile(
            pattern.clone(),
            vec!["A".into()],
            AfterMatchSkip::PastLastRow,
            RowsPerMatch::OneRow,
        )
        .unwrap(),
    );
    let mut matcher_default =
        PatternMatcher::new(compiled_default, &batch.schema()).unwrap();
    let out_default = run_matcher_to_batch(&mut matcher_default, &batch);
    assert_eq!(out_default.num_rows(), 2);

    // TO NEXT ROW – overlapping matches (1&2) and (2&3) → 4 output rows
    let compiled_overlap = Arc::new(
        CompiledPattern::compile(
            pattern,
            vec!["A".into()],
            AfterMatchSkip::ToNextRow,
            RowsPerMatch::OneRow,
        )
        .unwrap(),
    );
    let mut matcher_overlap =
        PatternMatcher::new(compiled_overlap, &batch.schema()).unwrap();
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
    let rows_per_match = RowsPerMatch::AllRows(EmptyMatchesMode::WithUnmatched);

    let compiled = Arc::new(
        CompiledPattern::compile(
            pattern,
            vec!["A".into()],
            AfterMatchSkip::PastLastRow,
            rows_per_match,
        )
        .unwrap(),
    );
    let mut matcher = PatternMatcher::new(compiled, &batch.schema()).unwrap();

    let out = run_matcher_to_batch(&mut matcher, &batch);
    assert_eq!(out.num_rows(), batch.num_rows());

    let base = batch.num_columns();
    let match_num = out
        .column(base + 1)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();

    for i in 0..out.num_rows() {
        assert_eq!(match_num.value(i), 0); // unmatched ⇒ 0
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
        CompiledPattern::compile(
            pattern,
            vec!["A".into()],
            AfterMatchSkip::PastLastRow,
            RowsPerMatch::OneRow,
        )
        .unwrap(),
    );
    let mut matcher = PatternMatcher::new(compiled, &batch.schema()).unwrap();
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
        let pattern =
            Pattern::Permute(vec![Symbol::Named("A".into()), Symbol::Named("B".into())]);

        let compiled = Arc::new(
            CompiledPattern::compile(
                pattern,
                vec!["A".into(), "B".into()],
                AfterMatchSkip::PastLastRow,
                RowsPerMatch::OneRow,
            )
            .unwrap(),
        );
        let mut matcher = PatternMatcher::new(compiled, &batch.schema()).unwrap();
        let out_batch = run_matcher_to_batch(&mut matcher, &batch);
        assert_eq!(out_batch.num_rows(), 2, "case {label} wrong row count");
        let out = &out_batch;

        // Validate that match number sequence is consistent (both rows share match id 1)
        let base = batch.num_columns();
        let match_num = out
            .column(base + 1)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(match_num.value(0), 1, "case {label}");
        assert_eq!(match_num.value(1), 1, "case {label}");
    }
}

/// Left-most precedence: with ambiguous A|B the engine must pick A.
#[test]
fn alternation_leftmost_single_row() {
    use datafusion_expr::match_recognize::Pattern;

    // Row 1: both A and B are true – left-most branch (A) must win
    // Row 2: only B is true – match B branch
    // Row 3: only A is true – match A branch
    let batch = make_batch(
        &[1, 2, 3],
        &[
            ("A", vec![true, false, true]),
            ("B", vec![true, true, false]),
        ],
    );

    let pattern = Pattern::Alternation(vec![
        Pattern::Symbol(Symbol::Named("A".into())),
        Pattern::Symbol(Symbol::Named("B".into())),
    ]);

    let compiled = Arc::new(
        CompiledPattern::compile(
            pattern,
            vec!["A".into(), "B".into()],
            AfterMatchSkip::PastLastRow,
            RowsPerMatch::OneRow,
        )
        .unwrap(),
    );
    let mut matcher = PatternMatcher::new(compiled, &batch.schema()).unwrap();

    let out = run_matcher_to_batch(&mut matcher, &batch);

    // Expect three output rows corresponding to the three input rows that matched
    assert_eq!(out.num_rows(), 3);

    let base = batch.num_columns();
    let classifier = out
        .column(base)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(classifier.value(0), "A"); // both A&B true ⇒ pick A
    assert_eq!(classifier.value(1), "B"); // only B true ⇒ B
    assert_eq!(classifier.value(2), "A"); // only A true ⇒ A
}

/// Left-most precedence beats greedy length: (A | AA) on input AA must choose the single A first.
#[test]
fn alternation_leftmost_beats_greedy() {
    use datafusion_expr::match_recognize::Pattern;

    // Two rows, both satisfy A
    let batch = make_batch(&[1, 2], &[("A", vec![true, true])]);

    let pattern = Pattern::Alternation(vec![
        // Left branch: single A
        Pattern::Symbol(Symbol::Named("A".into())),
        // Right branch: two consecutive As
        Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".into())),
            Pattern::Symbol(Symbol::Named("A".into())),
        ]),
    ]);

    let compiled = Arc::new(
        CompiledPattern::compile(
            pattern,
            vec!["A".into()],
            AfterMatchSkip::PastLastRow,
            RowsPerMatch::OneRow,
        )
        .unwrap(),
    );
    let mut matcher = PatternMatcher::new(compiled, &batch.schema()).unwrap();

    let out = run_matcher_to_batch(&mut matcher, &batch);

    // Expect two single-row matches (rows classified as A) with distinct match numbers.
    assert_eq!(out.num_rows(), 2);

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

    // Both rows come from single-A branch ⇒ classifier "A"
    assert_eq!(classifier.value(0), "A");
    assert_eq!(classifier.value(1), "A");

    // And they must belong to *different* logical matches (ids 1 and 2) –
    // if the greedy AA branch were chosen both rows would share id 1.
    assert_eq!(match_num.value(0), 1);
    assert_eq!(match_num.value(1), 2);
}

/// PERMUTE precedence – lexicographically earlier ordering wins when ambiguous (A,B).
#[test]
fn permute_precedence_ab_vs_ba() {
    use datafusion_expr::match_recognize::Pattern;

    // Two rows where both A and B evaluate to true, allowing AB or BA matches.
    let batch = make_batch(&[1, 2], &[("A", vec![true, true]), ("B", vec![true, true])]);

    // Case 1: PERMUTE(A,B) – expect ordering A then B
    let pattern_ab =
        Pattern::Permute(vec![Symbol::Named("A".into()), Symbol::Named("B".into())]);

    let compiled_ab = Arc::new(
        CompiledPattern::compile(
            pattern_ab,
            vec!["A".into(), "B".into()],
            AfterMatchSkip::PastLastRow,
            RowsPerMatch::OneRow,
        )
        .unwrap(),
    );
    let mut matcher_ab = PatternMatcher::new(compiled_ab, &batch.schema()).unwrap();
    let out_ab = run_matcher_to_batch(&mut matcher_ab, &batch);

    assert_eq!(out_ab.num_rows(), 2);
    let base = batch.num_columns();
    let classifier = out_ab
        .column(base)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(classifier.value(0), "A");
    assert_eq!(classifier.value(1), "B");

    // Case 2: PERMUTE(B,A) – expect ordering B then A
    let pattern_ba =
        Pattern::Permute(vec![Symbol::Named("B".into()), Symbol::Named("A".into())]);

    let compiled_ba = Arc::new(
        CompiledPattern::compile(
            pattern_ba,
            vec!["B".into(), "A".into()],
            AfterMatchSkip::PastLastRow,
            RowsPerMatch::OneRow,
        )
        .unwrap(),
    );
    let mut matcher_ba = PatternMatcher::new(compiled_ba, &batch.schema()).unwrap();
    let out_ba = run_matcher_to_batch(&mut matcher_ba, &batch);

    assert_eq!(out_ba.num_rows(), 2);
    let classifier_ba = out_ba
        .column(base)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(classifier_ba.value(0), "B");
    assert_eq!(classifier_ba.value(1), "A");
}
