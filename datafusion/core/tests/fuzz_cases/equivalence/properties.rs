// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use crate::fuzz_cases::equivalence::utils::{
    NULL_PCTS, TestScalarUDF, create_random_schema,
    create_random_table_with_casts_and_windows, generate_table_for_eq_properties,
    is_table_same_after_sort,
};

use datafusion_common::Result;
use datafusion_expr::{Operator, ScalarUDF};
use datafusion_physical_expr::expressions::{BinaryExpr, col};
use datafusion_physical_expr::{LexOrdering, ScalarFunctionExpr};
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

use datafusion_common::config::ConfigOptions;
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

#[test]
fn test_find_longest_permutation_random() -> Result<()> {
    const N_RANDOM_SCHEMA: usize = 100;
    const N_ELEMENTS: usize = 125;
    const N_DISTINCT: usize = 5;

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
        )?) as _;

        let a_plus_b = Arc::new(BinaryExpr::new(
            col("a", &test_schema)?,
            Operator::Plus,
            col("b", &test_schema)?,
        )) as _;
        let exprs = [
            col("a", &test_schema)?,
            col("b", &test_schema)?,
            col("c", &test_schema)?,
            col("d", &test_schema)?,
            col("e", &test_schema)?,
            col("f", &test_schema)?,
            floor_a,
            a_plus_b,
        ];

        for n_req in 0..=exprs.len() {
            for exprs in exprs.iter().combinations(n_req) {
                let exprs = exprs.into_iter().cloned().collect::<Vec<_>>();
                let (ordering, indices) =
                    eq_properties.find_longest_permutation(&exprs)?;
                // Make sure that find_longest_permutation return values are consistent
                let ordering2 = indices
                    .iter()
                    .zip(ordering.iter())
                    .map(|(&idx, sort_expr)| {
                        PhysicalSortExpr::new(Arc::clone(&exprs[idx]), sort_expr.options)
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    ordering, ordering2,
                    "indices and lexicographical ordering do not match"
                );

                let err_msg = format!(
                    "Error in test case seed: {seed}, null_pct: {null_pct}, ordering:{ordering:?}, eq_properties: {eq_properties}"
                );
                assert_eq!(ordering.len(), indices.len(), "{err_msg}");
                // Since ordered section satisfies schema, we expect
                // that result will be same after sort (e.g sort was unnecessary).
                let Some(ordering) = LexOrdering::new(ordering) else {
                    continue;
                };
                assert!(
                    is_table_same_after_sort(ordering, &table_data_with_properties)?,
                    "{}",
                    err_msg
                );
            }
        }
    }

    Ok(())
}

/// Checks `find_longest_permutation` on tables with cast equalities and the
/// orderings of whole-partition windows (see
/// `create_random_table_with_casts_and_windows`): the ordering it finds must
/// hold on the rows, and the sets of expressions that can be ordered in full
/// must be found in full.
#[test]
fn test_find_longest_permutation_with_casts_and_windows_random() -> Result<()> {
    const N_RANDOM_TABLES: u64 = 100;
    const N_ELEMENTS: usize = 125;
    const N_DISTINCT: usize = 5;
    const MAX_EXPRS: usize = 4;
    const SAMPLED_SET_PROBABILITY: f64 = 0.3;

    for (seed, &null_pct) in (0..N_RANDOM_TABLES).cartesian_product(NULL_PCTS) {
        let table = create_random_table_with_casts_and_windows(
            seed, null_pct, N_ELEMENTS, N_DISTINCT,
        )?;
        let eq_properties = &table.eq_properties;
        let context =
            format!("seed: {seed}, null_pct: {null_pct}, eq_properties: {eq_properties}");
        let mut rng = StdRng::seed_from_u64(seed);
        for mut exprs in table.provable_permutations.clone() {
            exprs.shuffle(&mut rng);
            let (_, indices) = eq_properties.find_longest_permutation(&exprs)?;
            assert_eq!(
                indices.len(),
                exprs.len(),
                "{context}, not found in full: [{}]",
                exprs.iter().join(", ")
            );
        }

        let candidates = table.keys.iter().map(|(expr, _)| expr).collect::<Vec<_>>();
        for n_exprs in 1..=MAX_EXPRS {
            for indices in (0..candidates.len()).combinations(n_exprs) {
                if n_exprs > 2 && !rng.random_bool(SAMPLED_SET_PROBABILITY) {
                    continue;
                }
                let exprs = indices
                    .iter()
                    .map(|&idx| Arc::clone(candidates[idx]))
                    .collect::<Vec<_>>();
                let (ordering, found) = eq_properties.find_longest_permutation(&exprs)?;
                for (sort_expr, &idx) in ordering.iter().zip(&found) {
                    assert!(sort_expr.expr.eq(&exprs[idx]), "{context}");
                }
                if let Some(ordering) = LexOrdering::new(ordering) {
                    assert!(
                        is_table_same_after_sort(ordering.clone(), &table.batch)?,
                        "{context}, found: {ordering}"
                    );
                }
            }
        }
    }

    Ok(())
}
