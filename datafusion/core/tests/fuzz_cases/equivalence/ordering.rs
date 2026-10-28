// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::fuzz_cases::equivalence::utils::{
    NULL_PCTS, TestScalarUDF, assert_random_ordering_satisfy_is_sound,
    contains_conservative_ordering_op, create_random_schema, create_test_params,
    create_test_schema_2, generate_table_for_eq_properties, generate_table_for_orderings,
    is_table_same_after_sort,
};
use arrow::array::{ArrayRef, Float64Array, RecordBatch, new_null_array};
use arrow::compute::{SortOptions, sort};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{Operator, ScalarUDF};
use datafusion_physical_expr::equivalence::{
    convert_to_orderings, convert_to_sort_exprs,
};
use datafusion_physical_expr::expressions::{BinaryExpr, col, lit};
use datafusion_physical_expr::{EquivalenceProperties, ScalarFunctionExpr};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

#[test]
fn test_ordering_satisfy_with_equivalence_random() -> Result<()> {
    const N_RANDOM_SCHEMA: usize = 5;
    const N_ELEMENTS: usize = 125;
    const N_DISTINCT: usize = 5;
    const SORT_OPTIONS: SortOptions = SortOptions {
        descending: false,
        nulls_first: false,
    };

    for (seed, &null_pct) in (0..N_RANDOM_SCHEMA).cartesian_product(NULL_PCTS) {
        // Create a random schema with random properties
        let (test_schema, eq_properties) = create_random_schema(seed as u64, null_pct)?;
        // Generate a data that satisfies properties given
        let table_data_with_properties = generate_table_for_eq_properties(
            &eq_properties,
            N_ELEMENTS,
            N_DISTINCT,
            null_pct,
        )?;
        let col_exprs = [
            col("a", &test_schema)?,
            col("b", &test_schema)?,
            col("c", &test_schema)?,
            col("d", &test_schema)?,
            col("e", &test_schema)?,
            col("f", &test_schema)?,
        ];

        let mut options_rng = StdRng::seed_from_u64(seed as u64);
        for n_req in 1..=col_exprs.len() {
            for exprs in col_exprs.iter().combinations(n_req) {
                assert_random_ordering_satisfy_is_sound(
                    &eq_properties,
                    &exprs,
                    &table_data_with_properties,
                    &mut options_rng,
                    &format!("seed: {seed}, null_pct: {null_pct}"),
                )?;
                let sort_exprs = exprs
                    .into_iter()
                    .map(|expr| PhysicalSortExpr::new(Arc::clone(expr), SORT_OPTIONS));
                let Some(ordering) = LexOrdering::new(sort_exprs) else {
                    unreachable!("Test should always produce non-degenerate orderings");
                };
                let expected = is_table_same_after_sort(
                    ordering.clone(),
                    &table_data_with_properties,
                )?;
                let err_msg = format!(
                    "Error in test case seed: {seed}, null_pct: {null_pct}, requirement:{ordering:?}, expected: {expected:?}, eq_properties {eq_properties}"
                );
                // Check whether ordering_satisfy API result and
                // experimental result matches.
                assert_eq!(
                    eq_properties.ordering_satisfy(ordering)?,
                    expected,
                    "{err_msg}"
                );
            }
        }
    }

    Ok(())
}

const N_EXHAUSTIVE_EXPRS: usize = 8;
const SAMPLED_COMBINATION_PROBABILITY: f64 = 0.03;

/// Selects all combinations of the first `N_EXHAUSTIVE_EXPRS` expressions and
/// all combinations of at most two expressions, and samples the others.
fn is_combination_selected(indices: &[usize], rng: &mut StdRng) -> bool {
    let only_exhaustive_exprs =
        indices.last().is_none_or(|&last| last < N_EXHAUSTIVE_EXPRS);
    only_exhaustive_exprs
        || indices.len() <= 2
        || rng.random_bool(SAMPLED_COMBINATION_PROBABILITY)
}

#[test]
fn test_ordering_satisfy_with_equivalence_complex_random() -> Result<()> {
    const N_RANDOM_SCHEMA: usize = 100;
    const N_ELEMENTS: usize = 125;
    const N_DISTINCT: usize = 5;
    const SORT_OPTIONS: SortOptions = SortOptions {
        descending: false,
        nulls_first: false,
    };

    for (seed, &null_pct) in (0..N_RANDOM_SCHEMA).cartesian_product(NULL_PCTS) {
        // Create a random schema with random properties
        let (test_schema, eq_properties) = create_random_schema(seed as u64, null_pct)?;
        // Generate a data that satisfies properties given
        let table_data_with_properties = generate_table_for_eq_properties(
            &eq_properties,
            N_ELEMENTS,
            N_DISTINCT,
            null_pct,
        )?;

        let test_fun = Arc::new(ScalarUDF::new_from_impl(TestScalarUDF::new()));
        let col_a = col("a", &test_schema)?;
        let floor_a = Arc::new(ScalarFunctionExpr::try_new(
            Arc::clone(&test_fun),
            vec![col_a],
            &test_schema,
            Arc::new(ConfigOptions::default()),
        )?);
        let a_plus_b = Arc::new(BinaryExpr::new(
            col("a", &test_schema)?,
            Operator::Plus,
            col("b", &test_schema)?,
        )) as Arc<dyn PhysicalExpr>;
        let binary = |lhs: Arc<dyn PhysicalExpr>, op, rhs: Arc<dyn PhysicalExpr>| {
            Arc::new(BinaryExpr::new(lhs, op, rhs)) as Arc<dyn PhysicalExpr>
        };
        // Ordered when `a` and `b` lead orderings in opposite directions.
        let a_gt_b = binary(
            col("a", &test_schema)?,
            Operator::Gt,
            col("b", &test_schema)?,
        );
        // `e` is constant, so each operand has the ordering of its column.
        // `a` and `b` lead separate orderings whose NULLs sit in different
        // rows, which is where Kleene `AND`/`OR` can break an ordering.
        let a_gt_e = binary(
            col("a", &test_schema)?,
            Operator::Gt,
            col("e", &test_schema)?,
        );
        let b_gt_e = binary(
            col("b", &test_schema)?,
            Operator::Gt,
            col("e", &test_schema)?,
        );
        let a_gt_e_and_b_gt_e =
            binary(Arc::clone(&a_gt_e), Operator::And, Arc::clone(&b_gt_e));
        let a_gt_e_or_b_gt_e = binary(a_gt_e, Operator::Or, b_gt_e);
        let exprs = [
            col("a", &test_schema)?,
            col("b", &test_schema)?,
            col("c", &test_schema)?,
            col("d", &test_schema)?,
            col("e", &test_schema)?,
            col("f", &test_schema)?,
            floor_a,
            a_plus_b,
            a_gt_b,
            a_gt_e_and_b_gt_e,
            a_gt_e_or_b_gt_e,
        ];

        let mut options_rng = StdRng::seed_from_u64(seed as u64);
        let mut selection_rng = StdRng::seed_from_u64(!(seed as u64));
        for n_req in 1..=exprs.len() {
            for indices in (0..exprs.len()).combinations(n_req) {
                if !is_combination_selected(&indices, &mut selection_rng) {
                    continue;
                }
                let exprs = indices.iter().map(|&i| &exprs[i]).collect::<Vec<_>>();
                assert_random_ordering_satisfy_is_sound(
                    &eq_properties,
                    &exprs,
                    &table_data_with_properties,
                    &mut options_rng,
                    &format!("seed: {seed}, null_pct: {null_pct}"),
                )?;
                let sort_exprs = exprs
                    .into_iter()
                    .map(|expr| PhysicalSortExpr::new(Arc::clone(expr), SORT_OPTIONS));
                let Some(ordering) = LexOrdering::new(sort_exprs) else {
                    unreachable!("Test should always produce non-degenerate orderings");
                };
                let expected = is_table_same_after_sort(
                    ordering.clone(),
                    &table_data_with_properties,
                )?;
                let err_msg = format!(
                    "Error in test case seed: {seed}, null_pct: {null_pct}, requirement:{ordering:?}, expected: {expected:?}, eq_properties: {eq_properties}",
                );
                // A rejection turns inconclusive only from the first key with
                // a conservative ordering rule onwards (see
                // `contains_conservative_ordering_op`), since such a rule can
                // make an ordering underivable even when the sample happens
                // to be sorted. A table sorted by the full ordering is sorted
                // by every prefix of it, so a rejected prefix without such
                // keys still proves the rejection is genuine.
                let conclusive_prefix = LexOrdering::new(
                    ordering
                        .iter()
                        .take_while(|sort_expr| {
                            !contains_conservative_ordering_op(&sort_expr.expr)
                        })
                        .cloned(),
                );
                if eq_properties.ordering_satisfy(ordering)? {
                    assert!(expected, "{err_msg}");
                } else if let Some(prefix) = conclusive_prefix
                    && !eq_properties.ordering_satisfy(prefix)?
                {
                    assert!(!expected, "{err_msg}");
                }
            }
        }
    }

    Ok(())
}

const ALL_SORT_OPTIONS: [SortOptions; 4] = [
    SortOptions {
        descending: false,
        nulls_first: false,
    },
    SortOptions {
        descending: false,
        nulls_first: true,
    },
    SortOptions {
        descending: true,
        nulls_first: false,
    },
    SortOptions {
        descending: true,
        nulls_first: true,
    },
];

/// Returns `0.0..=5.0` and `n_nulls` NULLs, sorted with `options`.
fn sorted_f64_array(options: SortOptions, n_nulls: usize) -> Result<ArrayRef> {
    let values = (0..6)
        .map(|v| Some(v as f64))
        .chain(std::iter::repeat_n(None, n_nulls))
        .collect::<Float64Array>();
    Ok(sort(&values, Some(options))?)
}

/// Returns whether `eq_properties` claims the ordering of `exprs` with
/// `options`, and asserts that `batch` has every claimed ordering.
fn assert_claim_is_sound(
    eq_properties: &EquivalenceProperties,
    exprs: &[&Arc<dyn PhysicalExpr>],
    options: SortOptions,
    batch: &RecordBatch,
) -> Result<bool> {
    let sort_exprs = exprs
        .iter()
        .map(|expr| PhysicalSortExpr::new(Arc::clone(expr), options));
    let Some(ordering) = LexOrdering::new(sort_exprs) else {
        unreachable!("Test should always produce non-degenerate orderings");
    };
    let claimed = eq_properties.ordering_satisfy(ordering.clone())?;
    if claimed {
        assert!(
            is_table_same_after_sort(ordering.clone(), batch)?,
            "requirement: {ordering:?}, eq_properties: {eq_properties}"
        );
    }
    Ok(claimed)
}

#[test]
fn test_ordering_satisfy_all_null_ordered_column() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float64, true),
        Field::new("b", DataType::Float64, true),
    ]));
    let col_a = &col("a", &schema)?;
    let col_b = &col("b", &schema)?;
    for options in ALL_SORT_OPTIONS {
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
        eq_properties.add_ordering([
            PhysicalSortExpr::new(Arc::clone(col_a), options),
            PhysicalSortExpr::new(Arc::clone(col_b), options),
        ]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                new_null_array(&DataType::Float64, 6),
                sorted_f64_array(options, 0)?,
            ],
        )?;
        for required in ALL_SORT_OPTIONS {
            for exprs in [
                vec![col_a],
                vec![col_b],
                vec![col_a, col_b],
                vec![col_b, col_a],
            ] {
                assert_claim_is_sound(&eq_properties, &exprs, required, &batch)?;
            }
        }
        for exprs in [vec![col_a], vec![col_a, col_b]] {
            assert!(
                assert_claim_is_sound(&eq_properties, &exprs, options, &batch)?,
                "requirement: {exprs:?} {options:?}, eq_properties: {eq_properties}"
            );
        }
    }
    Ok(())
}

#[test]
fn test_ordering_satisfy_null_constant_operand() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, true)]));
    let col_a = col("a", &schema)?;
    // `x` has the ordering of `a`.
    let x =
        Arc::new(BinaryExpr::new(col_a, Operator::Gt, lit(2.0))) as Arc<dyn PhysicalExpr>;
    let null = lit(ScalarValue::Boolean(None));
    let x_and_null = Arc::new(BinaryExpr::new(
        Arc::clone(&x),
        Operator::And,
        Arc::clone(&null),
    )) as Arc<dyn PhysicalExpr>;
    let x_or_null = Arc::new(BinaryExpr::new(Arc::clone(&x), Operator::Or, null))
        as Arc<dyn PhysicalExpr>;
    for options in ALL_SORT_OPTIONS {
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
        eq_properties.add_ordering([PhysicalSortExpr::new(col("a", &schema)?, options)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![sorted_f64_array(options, 2)?],
        )?;
        assert!(
            assert_claim_is_sound(&eq_properties, &[&x], options, &batch)?,
            "requirement: {x} {options:?}, eq_properties: {eq_properties}"
        );
        // A NULL operand moves rows to the NULL position, so `AND` and `OR`
        // keep the ordering only for some NULL positions.
        let mut n_claimed = 0;
        for expr in [&x_and_null, &x_or_null] {
            for required in ALL_SORT_OPTIONS {
                if assert_claim_is_sound(&eq_properties, &[expr], required, &batch)? {
                    n_claimed += 1;
                }
            }
        }
        assert!(n_claimed > 0, "no claim with {options:?}");
    }
    Ok(())
}

#[test]
fn test_ordering_satisfy_with_equivalence() -> Result<()> {
    // Schema satisfies following orderings:
    // [a ASC], [d ASC, b ASC], [e DESC, f ASC, g ASC]
    // and
    // Column [a=c] (e.g they are aliases).
    let (test_schema, eq_properties) = create_test_params()?;
    let col_a = &col("a", &test_schema)?;
    let col_b = &col("b", &test_schema)?;
    let col_c = &col("c", &test_schema)?;
    let col_d = &col("d", &test_schema)?;
    let col_e = &col("e", &test_schema)?;
    let col_f = &col("f", &test_schema)?;
    let col_g = &col("g", &test_schema)?;

    let option_asc = SortOptions {
        descending: false,
        nulls_first: false,
    };

    let option_desc = SortOptions {
        descending: true,
        nulls_first: true,
    };
    let table_data_with_properties =
        generate_table_for_eq_properties(&eq_properties, 625, 5, 0.0)?;

    // First element in the tuple stores vector of requirement, second element is the expected return value for ordering_satisfy function
    let requirements = vec![
        // `a ASC NULLS LAST`, expects `ordering_satisfy` to be `true`, since existing ordering `a ASC NULLS LAST, b ASC NULLS LAST` satisfies it
        (vec![(col_a, option_asc)], true),
        (vec![(col_a, option_desc)], false),
        // Test whether equivalence works as expected
        (vec![(col_c, option_asc)], true),
        (vec![(col_c, option_desc)], false),
        // Test whether ordering equivalence works as expected
        (vec![(col_d, option_asc)], true),
        (vec![(col_d, option_asc), (col_b, option_asc)], true),
        (vec![(col_d, option_desc), (col_b, option_asc)], false),
        (
            vec![
                (col_e, option_desc),
                (col_f, option_asc),
                (col_g, option_asc),
            ],
            true,
        ),
        (vec![(col_e, option_desc), (col_f, option_asc)], true),
        (vec![(col_e, option_asc), (col_f, option_asc)], false),
        (vec![(col_e, option_desc), (col_b, option_asc)], false),
        (vec![(col_e, option_asc), (col_b, option_asc)], false),
        (
            vec![
                (col_d, option_asc),
                (col_b, option_asc),
                (col_d, option_asc),
                (col_b, option_asc),
            ],
            true,
        ),
        (
            vec![
                (col_d, option_asc),
                (col_b, option_asc),
                (col_e, option_desc),
                (col_f, option_asc),
            ],
            true,
        ),
        (
            vec![
                (col_d, option_asc),
                (col_b, option_asc),
                (col_e, option_desc),
                (col_b, option_asc),
            ],
            true,
        ),
        (
            vec![
                (col_d, option_asc),
                (col_b, option_asc),
                (col_d, option_desc),
                (col_b, option_asc),
            ],
            true,
        ),
        (
            vec![
                (col_d, option_asc),
                (col_b, option_asc),
                (col_e, option_asc),
                (col_f, option_asc),
            ],
            false,
        ),
        (
            vec![
                (col_d, option_asc),
                (col_b, option_asc),
                (col_e, option_asc),
                (col_b, option_asc),
            ],
            false,
        ),
        (vec![(col_d, option_asc), (col_e, option_desc)], true),
        (
            vec![
                (col_d, option_asc),
                (col_c, option_asc),
                (col_b, option_asc),
            ],
            true,
        ),
        (
            vec![
                (col_d, option_asc),
                (col_e, option_desc),
                (col_f, option_asc),
                (col_b, option_asc),
            ],
            true,
        ),
        (
            vec![
                (col_d, option_asc),
                (col_e, option_desc),
                (col_c, option_asc),
                (col_b, option_asc),
            ],
            true,
        ),
        (
            vec![
                (col_d, option_asc),
                (col_e, option_desc),
                (col_b, option_asc),
                (col_f, option_asc),
            ],
            true,
        ),
    ];

    for (cols, expected) in requirements {
        let err_msg = format!("Error in test case: {cols:?}");
        let sort_exprs = convert_to_sort_exprs(&cols);
        let Some(ordering) = LexOrdering::new(sort_exprs) else {
            unreachable!("Test should always produce non-degenerate orderings");
        };

        // Check expected result with experimental result.
        assert_eq!(
            is_table_same_after_sort(ordering.clone(), &table_data_with_properties)?,
            expected
        );
        assert_eq!(
            eq_properties.ordering_satisfy(ordering)?,
            expected,
            "{err_msg}"
        );
    }

    Ok(())
}

// This test checks given a table is ordered with `[a ASC, b ASC, c ASC, d ASC]` and `[a ASC, c ASC, b ASC, d ASC]`
// whether the table is also ordered with `[a ASC, b ASC, d ASC]` and `[a ASC, c ASC, d ASC]`
// Since these orderings cannot be deduced, these orderings shouldn't be satisfied by the table generated.
// For background see discussion: https://github.com/apache/datafusion/issues/12700#issuecomment-2411134296
#[test]
fn test_ordering_satisfy_on_data() -> Result<()> {
    let schema = create_test_schema_2()?;
    let col_a = &col("a", &schema)?;
    let col_b = &col("b", &schema)?;
    let col_c = &col("c", &schema)?;
    let col_d = &col("d", &schema)?;

    let option_asc = SortOptions {
        descending: false,
        nulls_first: false,
    };

    let orderings = vec![
        // [a ASC, b ASC, c ASC, d ASC]
        vec![
            (col_a, option_asc),
            (col_b, option_asc),
            (col_c, option_asc),
            (col_d, option_asc),
        ],
        // [a ASC, c ASC, b ASC, d ASC]
        vec![
            (col_a, option_asc),
            (col_c, option_asc),
            (col_b, option_asc),
            (col_d, option_asc),
        ],
    ];
    let orderings = convert_to_orderings(&orderings);

    let batch = generate_table_for_orderings(orderings, schema, 1000, 10, 0.0)?;

    // [a ASC, c ASC, d ASC] cannot be deduced
    let ordering = vec![
        (col_a, option_asc),
        (col_c, option_asc),
        (col_d, option_asc),
    ];
    let ordering = convert_to_orderings(&[ordering])[0].clone();
    assert!(!is_table_same_after_sort(ordering, &batch)?);

    // [a ASC, b ASC, d ASC] cannot be deduced
    let ordering = vec![
        (col_a, option_asc),
        (col_b, option_asc),
        (col_d, option_asc),
    ];
    let ordering = convert_to_orderings(&[ordering])[0].clone();
    assert!(!is_table_same_after_sort(ordering, &batch)?);

    // [a ASC, b ASC] can be deduced
    let ordering = vec![(col_a, option_asc), (col_b, option_asc)];
    let ordering = convert_to_orderings(&[ordering])[0].clone();
    assert!(is_table_same_after_sort(ordering, &batch)?);

    Ok(())
}
