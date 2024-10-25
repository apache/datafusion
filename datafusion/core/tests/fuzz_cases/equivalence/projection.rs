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
    apply_projection, create_random_schema, generate_table_for_eq_properties,
    is_table_same_after_sort, TestScalarUDF,
};
use arrow_schema::SortOptions;
use datafusion_common::{DFSchema, Result};
use datafusion_expr::{Operator, ScalarUDF};
use datafusion_physical_expr::equivalence::ProjectionMapping;
use datafusion_physical_expr::expressions::{col, BinaryExpr};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use itertools::Itertools;
use std::sync::Arc;

#[test]
fn project_orderings_random() -> Result<()> {
    const N_RANDOM_SCHEMA: usize = 20;
    const N_ELEMENTS: usize = 125;
    const N_DISTINCT: usize = 5;

    for seed in 0..N_RANDOM_SCHEMA {
        // Create a random schema with random properties
        let (test_schema, eq_properties) = create_random_schema(seed as u64)?;
        // Generate a data that satisfies properties given
        let table_data_with_properties =
            generate_table_for_eq_properties(&eq_properties, N_ELEMENTS, N_DISTINCT)?;
        // Floor(a)
        let test_fun = ScalarUDF::new_from_impl(TestScalarUDF::new());
        let floor_a = datafusion_physical_expr::udf::create_physical_expr(
            &test_fun,
            &[col("a", &test_schema)?],
            &test_schema,
            &[],
            &DFSchema::empty(),
        )?;
        // a + b
        let a_plus_b = Arc::new(BinaryExpr::new(
            col("a", &test_schema)?,
            Operator::Plus,
            col("b", &test_schema)?,
        )) as Arc<dyn PhysicalExpr>;
        let proj_exprs = vec![
            (col("a", &test_schema)?, "a_new"),
            (col("b", &test_schema)?, "b_new"),
            (col("c", &test_schema)?, "c_new"),
            (col("d", &test_schema)?, "d_new"),
            (col("e", &test_schema)?, "e_new"),
            (col("f", &test_schema)?, "f_new"),
            (floor_a, "floor(a)"),
            (a_plus_b, "a+b"),
        ];

        for n_req in 0..=proj_exprs.len() {
            for proj_exprs in proj_exprs.iter().combinations(n_req) {
                let proj_exprs = proj_exprs
                    .into_iter()
                    .map(|(expr, name)| (Arc::clone(expr), name.to_string()))
                    .collect::<Vec<_>>();
                let (projected_batch, projected_eq) = apply_projection(
                    proj_exprs.clone(),
                    &table_data_with_properties,
                    &eq_properties,
                )?;

                // Make sure each ordering after projection is valid.
                for ordering in projected_eq.oeq_class().iter() {
                    let err_msg = format!(
                        "Error in test case ordering:{:?}, eq_properties.oeq_class: {:?}, eq_properties.eq_group: {:?}, eq_properties.constants: {:?}, proj_exprs: {:?}",
                        ordering, eq_properties.oeq_class, eq_properties.eq_group, eq_properties.constants, proj_exprs
                    );
                    // Since ordered section satisfies schema, we expect
                    // that result will be same after sort (e.g sort was unnecessary).
                    assert!(
                        is_table_same_after_sort(
                            ordering.clone(),
                            projected_batch.clone(),
                        )?,
                        "{}",
                        err_msg
                    );
                }
            }
        }
    }

    Ok(())
}

#[test]
fn ordering_satisfy_after_projection_random() -> Result<()> {
    const N_RANDOM_SCHEMA: usize = 20;
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
        // Floor(a)
        let test_fun = ScalarUDF::new_from_impl(TestScalarUDF::new());
        let floor_a = datafusion_physical_expr::udf::create_physical_expr(
            &test_fun,
            &[col("a", &test_schema)?],
            &test_schema,
            &[],
            &DFSchema::empty(),
        )?;
        // a + b
        let a_plus_b = Arc::new(BinaryExpr::new(
            col("a", &test_schema)?,
            Operator::Plus,
            col("b", &test_schema)?,
        )) as Arc<dyn PhysicalExpr>;
        let proj_exprs = vec![
            (col("a", &test_schema)?, "a_new"),
            (col("b", &test_schema)?, "b_new"),
            (col("c", &test_schema)?, "c_new"),
            (col("d", &test_schema)?, "d_new"),
            (col("e", &test_schema)?, "e_new"),
            (col("f", &test_schema)?, "f_new"),
            (floor_a, "floor(a)"),
            (a_plus_b, "a+b"),
        ];

        for n_req in 0..=proj_exprs.len() {
            for proj_exprs in proj_exprs.iter().combinations(n_req) {
                let proj_exprs = proj_exprs
                    .into_iter()
                    .map(|(expr, name)| (Arc::clone(expr), name.to_string()))
                    .collect::<Vec<_>>();
                let (projected_batch, projected_eq) = apply_projection(
                    proj_exprs.clone(),
                    &table_data_with_properties,
                    &eq_properties,
                )?;

                let projection_mapping =
                    ProjectionMapping::try_new(&proj_exprs, &test_schema)?;

                let projected_exprs = projection_mapping
                    .iter()
                    .map(|(_source, target)| Arc::clone(target))
                    .collect::<Vec<_>>();

                for n_req in 0..=projected_exprs.len() {
                    for exprs in projected_exprs.iter().combinations(n_req) {
                        let requirement = exprs
                            .into_iter()
                            .map(|expr| PhysicalSortExpr {
                                expr: Arc::clone(expr),
                                options: SORT_OPTIONS,
                            })
                            .collect::<Vec<_>>();
                        let expected = is_table_same_after_sort(
                            requirement.clone(),
                            projected_batch.clone(),
                        )?;
                        let err_msg = format!(
                            "Error in test case requirement:{:?}, expected: {:?}, eq_properties.oeq_class: {:?}, eq_properties.eq_group: {:?}, eq_properties.constants: {:?}, projected_eq.oeq_class: {:?}, projected_eq.eq_group: {:?}, projected_eq.constants: {:?}, projection_mapping: {:?}",
                            requirement, expected, eq_properties.oeq_class, eq_properties.eq_group, eq_properties.constants, projected_eq.oeq_class, projected_eq.eq_group, projected_eq.constants, projection_mapping
                        );
                        // Check whether ordering_satisfy API result and
                        // experimental result matches.
                        assert_eq!(
                            projected_eq.ordering_satisfy(&requirement),
                            expected,
                            "{}",
                            err_msg
                        );
                    }
                }
            }
        }
    }

    Ok(())
}
