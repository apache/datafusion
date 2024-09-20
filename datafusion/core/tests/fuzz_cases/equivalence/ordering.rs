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

use crate::fuzz_cases::equivalence::utils::{
    convert_to_orderings, create_random_schema, create_test_params, create_test_schema_2,
    generate_table_for_eq_properties, generate_table_for_orderings,
    is_table_same_after_sort, TestScalarUDF,
};
use arrow_schema::SortOptions;
use datafusion_common::{DFSchema, Result};
use datafusion_expr::{Operator, ScalarUDF};
use datafusion_physical_expr::expressions::{col, BinaryExpr};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use itertools::Itertools;
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

    for seed in 0..N_RANDOM_SCHEMA {
        // Create a random schema with random properties
        let (test_schema, eq_properties) = create_random_schema(seed as u64)?;
        // Generate a data that satisfies properties given
        let table_data_with_properties =
            generate_table_for_eq_properties(&eq_properties, N_ELEMENTS, N_DISTINCT)?;
        let col_exprs = [
            col("a", &test_schema)?,
            col("b", &test_schema)?,
            col("c", &test_schema)?,
            col("d", &test_schema)?,
            col("e", &test_schema)?,
            col("f", &test_schema)?,
        ];

        for n_req in 0..=col_exprs.len() {
            for exprs in col_exprs.iter().combinations(n_req) {
                let requirement = exprs
                    .into_iter()
                    .map(|expr| PhysicalSortExpr {
                        expr: Arc::clone(expr),
                        options: SORT_OPTIONS,
                    })
                    .collect::<Vec<_>>();
                let expected = is_table_same_after_sort(
                    requirement.clone(),
                    table_data_with_properties.clone(),
                )?;
                let err_msg = format!(
                    "Error in test case requirement:{:?}, expected: {:?}, eq_properties.oeq_class: {:?}, eq_properties.eq_group: {:?}, eq_properties.constants: {:?}",
                    requirement, expected, eq_properties.oeq_class, eq_properties.eq_group, eq_properties.constants
                );
                // Check whether ordering_satisfy API result and
                // experimental result matches.
                assert_eq!(
                    eq_properties.ordering_satisfy(&requirement),
                    expected,
                    "{}",
                    err_msg
                );
            }
        }
    }

    Ok(())
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

    for seed in 0..N_RANDOM_SCHEMA {
        // Create a random schema with random properties
        let (test_schema, eq_properties) = create_random_schema(seed as u64)?;
        // Generate a data that satisfies properties given
        let table_data_with_properties =
            generate_table_for_eq_properties(&eq_properties, N_ELEMENTS, N_DISTINCT)?;

        let test_fun = ScalarUDF::new_from_impl(TestScalarUDF::new());
        let floor_a = datafusion_physical_expr::udf::create_physical_expr(
            &test_fun,
            &[col("a", &test_schema)?],
            &test_schema,
            &[],
            &DFSchema::empty(),
        )?;
        let a_plus_b = Arc::new(BinaryExpr::new(
            col("a", &test_schema)?,
            Operator::Plus,
            col("b", &test_schema)?,
        )) as Arc<dyn PhysicalExpr>;
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
                let requirement = exprs
                    .into_iter()
                    .map(|expr| PhysicalSortExpr {
                        expr: Arc::clone(expr),
                        options: SORT_OPTIONS,
                    })
                    .collect::<Vec<_>>();
                let expected = is_table_same_after_sort(
                    requirement.clone(),
                    table_data_with_properties.clone(),
                )?;
                let err_msg = format!(
                    "Error in test case requirement:{:?}, expected: {:?}, eq_properties.oeq_class: {:?}, eq_properties.eq_group: {:?}, eq_properties.constants: {:?}",
                    requirement, expected, eq_properties.oeq_class, eq_properties.eq_group, eq_properties.constants
                );
                // Check whether ordering_satisfy API result and
                // experimental result matches.

                assert_eq!(
                    eq_properties.ordering_satisfy(&requirement),
                    (expected | false),
                    "{}",
                    err_msg
                );
            }
        }
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
        generate_table_for_eq_properties(&eq_properties, 625, 5)?;

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
        let err_msg = format!("Error in test case:{cols:?}");
        let required = cols
            .into_iter()
            .map(|(expr, options)| PhysicalSortExpr {
                expr: Arc::clone(expr),
                options,
            })
            .collect::<Vec<_>>();

        // Check expected result with experimental result.
        assert_eq!(
            is_table_same_after_sort(
                required.clone(),
                table_data_with_properties.clone()
            )?,
            expected
        );
        assert_eq!(
            eq_properties.ordering_satisfy(&required),
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

    let batch = generate_table_for_orderings(orderings, schema, 1000, 10)?;

    // [a ASC, c ASC, d ASC] cannot be deduced
    let ordering = vec![
        (col_a, option_asc),
        (col_c, option_asc),
        (col_d, option_asc),
    ];
    let ordering = convert_to_orderings(&[ordering])[0].clone();
    assert!(!is_table_same_after_sort(ordering, batch.clone())?);

    // [a ASC, b ASC, d ASC] cannot be deduced
    let ordering = vec![
        (col_a, option_asc),
        (col_b, option_asc),
        (col_d, option_asc),
    ];
    let ordering = convert_to_orderings(&[ordering])[0].clone();
    assert!(!is_table_same_after_sort(ordering, batch.clone())?);

    // [a ASC, b ASC] can be deduced
    let ordering = vec![(col_a, option_asc), (col_b, option_asc)];
    let ordering = convert_to_orderings(&[ordering])[0].clone();
    assert!(is_table_same_after_sort(ordering, batch.clone())?);

    Ok(())
}
