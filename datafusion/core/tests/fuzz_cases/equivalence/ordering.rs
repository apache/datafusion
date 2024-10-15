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
    create_random_schema, generate_table_for_eq_properties, is_table_same_after_sort,
    TestScalarUDF,
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
