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

use std::ops::Deref;
use std::sync::Arc;

use crate::expressions::Column;
use crate::PhysicalExpr;

use arrow::datatypes::SchemaRef;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{internal_err, Result};

use indexmap::IndexMap;

/// Stores target expressions, along with their indices, that associate with a
/// source expression in a projection mapping.
#[derive(Clone, Debug, Default)]
pub struct ProjectionTargets {
    /// A non-empty vector of pairs of target expressions and their indices.
    /// Consider using a special non-empty collection type in the future (e.g.
    /// if Rust provides one in the standard library).
    exprs_indices: Vec<(Arc<dyn PhysicalExpr>, usize)>,
}

impl ProjectionTargets {
    /// Returns the first target expression and its index.
    pub fn first(&self) -> &(Arc<dyn PhysicalExpr>, usize) {
        // Since the vector is non-empty, we can safely unwrap:
        self.exprs_indices.first().unwrap()
    }

    /// Adds a target expression and its index to the list of targets.
    pub fn push(&mut self, target: (Arc<dyn PhysicalExpr>, usize)) {
        self.exprs_indices.push(target);
    }
}

impl Deref for ProjectionTargets {
    type Target = [(Arc<dyn PhysicalExpr>, usize)];

    fn deref(&self) -> &Self::Target {
        &self.exprs_indices
    }
}

impl From<Vec<(Arc<dyn PhysicalExpr>, usize)>> for ProjectionTargets {
    fn from(exprs_indices: Vec<(Arc<dyn PhysicalExpr>, usize)>) -> Self {
        Self { exprs_indices }
    }
}

/// Stores the mapping between source expressions and target expressions for a
/// projection.
#[derive(Clone, Debug)]
pub struct ProjectionMapping {
    /// Mapping between source expressions and target expressions.
    /// Vector indices correspond to the indices after projection.
    map: IndexMap<Arc<dyn PhysicalExpr>, ProjectionTargets>,
}

impl ProjectionMapping {
    /// Constructs the mapping between a projection's input and output
    /// expressions.
    ///
    /// For example, given the input projection expressions (`a + b`, `c + d`)
    /// and an output schema with two columns `"c + d"` and `"a + b"`, the
    /// projection mapping would be:
    ///
    /// ```text
    ///  [0]: (c + d, [(col("c + d"), 0)])
    ///  [1]: (a + b, [(col("a + b"), 1)])
    /// ```
    ///
    /// where `col("c + d")` means the column named `"c + d"`.
    pub fn try_new(
        expr: impl IntoIterator<Item = (Arc<dyn PhysicalExpr>, String)>,
        input_schema: &SchemaRef,
    ) -> Result<Self> {
        // Construct a map from the input expressions to the output expression of the projection:
        let mut map = IndexMap::<_, ProjectionTargets>::new();
        for (expr_idx, (expr, name)) in expr.into_iter().enumerate() {
            let target_expr = Arc::new(Column::new(&name, expr_idx)) as _;
            let source_expr = expr.transform_down(|e| match e.as_any().downcast_ref::<Column>() {
                Some(col) => {
                    // Sometimes, an expression and its name in the input_schema
                    // doesn't match. This can cause problems, so we make sure
                    // that the expression name matches with the name in `input_schema`.
                    // Conceptually, `source_expr` and `expression` should be the same.
                    let idx = col.index();
                    let matching_field = input_schema.field(idx);
                    let matching_name = matching_field.name();
                    if col.name() != matching_name {
                        return internal_err!(
                            "Input field name {} does not match with the projection expression {}",
                            matching_name,
                            col.name()
                        );
                    }
                    let matching_column = Column::new(matching_name, idx);
                    Ok(Transformed::yes(Arc::new(matching_column)))
                }
                None => Ok(Transformed::no(e)),
            })
            .data()?;
            map.entry(source_expr)
                .or_default()
                .push((target_expr, expr_idx));
        }
        Ok(Self { map })
    }

    /// Constructs a subset mapping using the provided indices.
    ///
    /// This is used when the output is a subset of the input without any
    /// other transformations. The indices are for columns in the schema.
    pub fn from_indices(indices: &[usize], schema: &SchemaRef) -> Result<Self> {
        let projection_exprs = indices.iter().map(|index| {
            let field = schema.field(*index);
            let column = Arc::new(Column::new(field.name(), *index));
            (column as _, field.name().clone())
        });
        ProjectionMapping::try_new(projection_exprs, schema)
    }
}

impl Deref for ProjectionMapping {
    type Target = IndexMap<Arc<dyn PhysicalExpr>, ProjectionTargets>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl FromIterator<(Arc<dyn PhysicalExpr>, ProjectionTargets)> for ProjectionMapping {
    fn from_iter<T: IntoIterator<Item = (Arc<dyn PhysicalExpr>, ProjectionTargets)>>(
        iter: T,
    ) -> Self {
        Self {
            map: IndexMap::from_iter(iter),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::equivalence::tests::output_schema;
    use crate::equivalence::{convert_to_orderings, EquivalenceProperties};
    use crate::expressions::{col, BinaryExpr};
    use crate::utils::tests::TestScalarUDF;
    use crate::{PhysicalExprRef, ScalarFunctionExpr};

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_expr::{Operator, ScalarUDF};

    #[test]
    fn project_orderings() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Int32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_d = &col("d", &schema)?;
        let col_e = &col("e", &schema)?;
        let col_ts = &col("ts", &schema)?;
        let a_plus_b = Arc::new(BinaryExpr::new(
            Arc::clone(col_a),
            Operator::Plus,
            Arc::clone(col_b),
        )) as Arc<dyn PhysicalExpr>;
        let b_plus_d = Arc::new(BinaryExpr::new(
            Arc::clone(col_b),
            Operator::Plus,
            Arc::clone(col_d),
        )) as Arc<dyn PhysicalExpr>;
        let b_plus_e = Arc::new(BinaryExpr::new(
            Arc::clone(col_b),
            Operator::Plus,
            Arc::clone(col_e),
        )) as Arc<dyn PhysicalExpr>;
        let c_plus_d = Arc::new(BinaryExpr::new(
            Arc::clone(col_c),
            Operator::Plus,
            Arc::clone(col_d),
        )) as Arc<dyn PhysicalExpr>;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };

        let test_cases = vec![
            // ---------- TEST CASE 1 ------------
            (
                // orderings
                vec![
                    // [b ASC]
                    vec![(col_b, option_asc)],
                ],
                // projection exprs
                vec![(col_b, "b_new".to_string()), (col_a, "a_new".to_string())],
                // expected
                vec![
                    // [b_new ASC]
                    vec![("b_new", option_asc)],
                ],
            ),
            // ---------- TEST CASE 2 ------------
            (
                // orderings
                vec![
                    // empty ordering
                ],
                // projection exprs
                vec![(col_c, "c_new".to_string()), (col_b, "b_new".to_string())],
                // expected
                vec![
                    // no ordering at the output
                ],
            ),
            // ---------- TEST CASE 3 ------------
            (
                // orderings
                vec![
                    // [ts ASC]
                    vec![(col_ts, option_asc)],
                ],
                // projection exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_ts, "ts_new".to_string()),
                ],
                // expected
                vec![
                    // [ts_new ASC]
                    vec![("ts_new", option_asc)],
                ],
            ),
            // ---------- TEST CASE 4 ------------
            (
                // orderings
                vec![
                    // [a ASC, ts ASC]
                    vec![(col_a, option_asc), (col_ts, option_asc)],
                    // [b ASC, ts ASC]
                    vec![(col_b, option_asc), (col_ts, option_asc)],
                ],
                // projection exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_ts, "ts_new".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, ts_new ASC]
                    vec![("a_new", option_asc), ("ts_new", option_asc)],
                    // [b_new ASC, ts_new ASC]
                    vec![("b_new", option_asc), ("ts_new", option_asc)],
                ],
            ),
            // ---------- TEST CASE 5 ------------
            (
                // orderings
                vec![
                    // [a + b ASC]
                    vec![(&a_plus_b, option_asc)],
                ],
                // projection exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (&a_plus_b, "a+b".to_string()),
                ],
                // expected
                vec![
                    // [a + b ASC]
                    vec![("a+b", option_asc)],
                ],
            ),
            // ---------- TEST CASE 6 ------------
            (
                // orderings
                vec![
                    // [a + b ASC, c ASC]
                    vec![(&a_plus_b, option_asc), (col_c, option_asc)],
                ],
                // projection exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_c, "c_new".to_string()),
                    (&a_plus_b, "a+b".to_string()),
                ],
                // expected
                vec![
                    // [a + b ASC, c_new ASC]
                    vec![("a+b", option_asc), ("c_new", option_asc)],
                ],
            ),
            // ------- TEST CASE 7 ----------
            (
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [a ASC, d ASC]
                    vec![(col_a, option_asc), (col_d, option_asc)],
                ],
                // b as b_new, a as a_new, d as d_new b+d
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_d, "d_new".to_string()),
                    (&b_plus_d, "b+d".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, b_new ASC]
                    vec![("a_new", option_asc), ("b_new", option_asc)],
                    // [a_new ASC, d_new ASC]
                    vec![("a_new", option_asc), ("d_new", option_asc)],
                    // [a_new ASC, b+d ASC]
                    vec![("a_new", option_asc), ("b+d", option_asc)],
                ],
            ),
            // ------- TEST CASE 8 ----------
            (
                // orderings
                vec![
                    // [b+d ASC]
                    vec![(&b_plus_d, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_d, "d_new".to_string()),
                    (&b_plus_d, "b+d".to_string()),
                ],
                // expected
                vec![
                    // [b+d ASC]
                    vec![("b+d", option_asc)],
                ],
            ),
            // ------- TEST CASE 9 ----------
            (
                // orderings
                vec![
                    // [a ASC, d ASC, b ASC]
                    vec![
                        (col_a, option_asc),
                        (col_d, option_asc),
                        (col_b, option_asc),
                    ],
                    // [c ASC]
                    vec![(col_c, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_d, "d_new".to_string()),
                    (col_c, "c_new".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, d_new ASC, b_new ASC]
                    vec![
                        ("a_new", option_asc),
                        ("d_new", option_asc),
                        ("b_new", option_asc),
                    ],
                    // [c_new ASC],
                    vec![("c_new", option_asc)],
                ],
            ),
            // ------- TEST CASE 10 ----------
            (
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                    // [a ASC, d ASC]
                    vec![(col_a, option_asc), (col_d, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_c, "c_new".to_string()),
                    (&c_plus_d, "c+d".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, b_new ASC, c_new ASC]
                    vec![
                        ("a_new", option_asc),
                        ("b_new", option_asc),
                        ("c_new", option_asc),
                    ],
                    // [a_new ASC, b_new ASC, c+d ASC]
                    vec![
                        ("a_new", option_asc),
                        ("b_new", option_asc),
                        ("c+d", option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 11 ----------
            (
                // orderings
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [a ASC, d ASC]
                    vec![(col_a, option_asc), (col_d, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (&b_plus_d, "b+d".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, b_new ASC]
                    vec![("a_new", option_asc), ("b_new", option_asc)],
                    // [a_new ASC, b + d ASC]
                    vec![("a_new", option_asc), ("b+d", option_asc)],
                ],
            ),
            // ------- TEST CASE 12 ----------
            (
                // orderings
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                ],
                // proj exprs
                vec![(col_c, "c_new".to_string()), (col_a, "a_new".to_string())],
                // expected
                vec![
                    // [a_new ASC]
                    vec![("a_new", option_asc)],
                ],
            ),
            // ------- TEST CASE 13 ----------
            (
                // orderings
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                    // [a ASC, a + b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (&a_plus_b, option_asc),
                        (col_c, option_asc),
                    ],
                ],
                // proj exprs
                vec![
                    (col_c, "c_new".to_string()),
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (&a_plus_b, "a+b".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, b_new ASC, c_new ASC]
                    vec![
                        ("a_new", option_asc),
                        ("b_new", option_asc),
                        ("c_new", option_asc),
                    ],
                    // [a_new ASC, a+b ASC, c_new ASC]
                    vec![
                        ("a_new", option_asc),
                        ("a+b", option_asc),
                        ("c_new", option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 14 ----------
            (
                // orderings
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [c ASC, b ASC]
                    vec![(col_c, option_asc), (col_b, option_asc)],
                    // [d ASC, e ASC]
                    vec![(col_d, option_asc), (col_e, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_c, "c_new".to_string()),
                    (col_d, "d_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (&b_plus_e, "b+e".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, d_new ASC, b+e ASC]
                    vec![
                        ("a_new", option_asc),
                        ("d_new", option_asc),
                        ("b+e", option_asc),
                    ],
                    // [d_new ASC, a_new ASC, b+e ASC]
                    vec![
                        ("d_new", option_asc),
                        ("a_new", option_asc),
                        ("b+e", option_asc),
                    ],
                    // [c_new ASC, d_new ASC, b+e ASC]
                    vec![
                        ("c_new", option_asc),
                        ("d_new", option_asc),
                        ("b+e", option_asc),
                    ],
                    // [d_new ASC, c_new ASC, b+e ASC]
                    vec![
                        ("d_new", option_asc),
                        ("c_new", option_asc),
                        ("b+e", option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 15 ----------
            (
                // orderings
                vec![
                    // [a ASC, c ASC, b ASC]
                    vec![
                        (col_a, option_asc),
                        (col_c, option_asc),
                        (col_b, option_asc),
                    ],
                ],
                // proj exprs
                vec![
                    (col_c, "c_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (&a_plus_b, "a+b".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, d_new ASC, b+e ASC]
                    vec![
                        ("a_new", option_asc),
                        ("c_new", option_asc),
                        ("a+b", option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 16 ----------
            (
                // orderings
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [c ASC, b DESC]
                    vec![(col_c, option_asc), (col_b, option_desc)],
                    // [e ASC]
                    vec![(col_e, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_c, "c_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_b, "b_new".to_string()),
                    (&b_plus_e, "b+e".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, b_new ASC]
                    vec![("a_new", option_asc), ("b_new", option_asc)],
                    // [a_new ASC, b_new ASC]
                    vec![("a_new", option_asc), ("b+e", option_asc)],
                    // [c_new ASC, b_new DESC]
                    vec![("c_new", option_asc), ("b_new", option_desc)],
                ],
            ),
        ];

        for (idx, (orderings, proj_exprs, expected)) in test_cases.into_iter().enumerate()
        {
            let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

            let orderings = convert_to_orderings(&orderings);
            eq_properties.add_orderings(orderings);

            let proj_exprs = proj_exprs
                .into_iter()
                .map(|(expr, name)| (Arc::clone(expr), name));
            let projection_mapping = ProjectionMapping::try_new(proj_exprs, &schema)?;
            let output_schema = output_schema(&projection_mapping, &schema)?;

            let expected = expected
                .into_iter()
                .map(|ordering| {
                    ordering
                        .into_iter()
                        .map(|(name, options)| {
                            (col(name, &output_schema).unwrap(), options)
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let expected = convert_to_orderings(&expected);

            let projected_eq = eq_properties.project(&projection_mapping, output_schema);
            let orderings = projected_eq.oeq_class();

            let err_msg = format!(
                "test_idx: {idx:?}, actual: {orderings:?}, expected: {expected:?}, projection_mapping: {projection_mapping:?}"
            );

            assert_eq!(orderings.len(), expected.len(), "{err_msg}");
            for expected_ordering in &expected {
                assert!(orderings.contains(expected_ordering), "{}", err_msg)
            }
        }

        Ok(())
    }

    #[test]
    fn project_orderings2() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_ts = &col("ts", &schema)?;
        let a_plus_b = Arc::new(BinaryExpr::new(
            Arc::clone(col_a),
            Operator::Plus,
            Arc::clone(col_b),
        )) as Arc<dyn PhysicalExpr>;

        let test_fun = Arc::new(ScalarUDF::new_from_impl(TestScalarUDF::new()));

        let round_c = Arc::new(ScalarFunctionExpr::try_new(
            test_fun,
            vec![Arc::clone(col_c)],
            &schema,
        )?) as PhysicalExprRef;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let proj_exprs = vec![
            (col_b, "b_new".to_string()),
            (col_a, "a_new".to_string()),
            (col_c, "c_new".to_string()),
            (&round_c, "round_c_res".to_string()),
        ];
        let proj_exprs = proj_exprs
            .into_iter()
            .map(|(expr, name)| (Arc::clone(expr), name));
        let projection_mapping = ProjectionMapping::try_new(proj_exprs, &schema)?;
        let output_schema = output_schema(&projection_mapping, &schema)?;

        let col_a_new = &col("a_new", &output_schema)?;
        let col_b_new = &col("b_new", &output_schema)?;
        let col_c_new = &col("c_new", &output_schema)?;
        let col_round_c_res = &col("round_c_res", &output_schema)?;
        let a_new_plus_b_new = Arc::new(BinaryExpr::new(
            Arc::clone(col_a_new),
            Operator::Plus,
            Arc::clone(col_b_new),
        )) as Arc<dyn PhysicalExpr>;

        let test_cases = vec![
            // ---------- TEST CASE 1 ------------
            (
                // orderings
                vec![
                    // [a ASC]
                    vec![(col_a, option_asc)],
                ],
                // expected
                vec![
                    // [b_new ASC]
                    vec![(col_a_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 2 ------------
            (
                // orderings
                vec![
                    // [a+b ASC]
                    vec![(&a_plus_b, option_asc)],
                ],
                // expected
                vec![
                    // [b_new ASC]
                    vec![(&a_new_plus_b_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 3 ------------
            (
                // orderings
                vec![
                    // [a ASC, ts ASC]
                    vec![(col_a, option_asc), (col_ts, option_asc)],
                ],
                // expected
                vec![
                    // [a_new ASC, date_bin_res ASC]
                    vec![(col_a_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 4 ------------
            (
                // orderings
                vec![
                    // [a ASC, ts ASC, b ASC]
                    vec![
                        (col_a, option_asc),
                        (col_ts, option_asc),
                        (col_b, option_asc),
                    ],
                ],
                // expected
                vec![
                    // [a_new ASC, date_bin_res ASC]
                    vec![(col_a_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 5 ------------
            (
                // orderings
                vec![
                    // [a ASC, c ASC]
                    vec![(col_a, option_asc), (col_c, option_asc)],
                ],
                // expected
                vec![
                    // [a_new ASC, round_c_res ASC, c_new ASC]
                    vec![(col_a_new, option_asc), (col_round_c_res, option_asc)],
                    // [a_new ASC, c_new ASC]
                    vec![(col_a_new, option_asc), (col_c_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 6 ------------
            (
                // orderings
                vec![
                    // [c ASC, b ASC]
                    vec![(col_c, option_asc), (col_b, option_asc)],
                ],
                // expected
                vec![
                    // [round_c_res ASC]
                    vec![(col_round_c_res, option_asc)],
                    // [c_new ASC, b_new ASC]
                    vec![(col_c_new, option_asc), (col_b_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 7 ------------
            (
                // orderings
                vec![
                    // [a+b ASC, c ASC]
                    vec![(&a_plus_b, option_asc), (col_c, option_asc)],
                ],
                // expected
                vec![
                    // [a+b ASC, round(c) ASC, c_new ASC]
                    vec![
                        (&a_new_plus_b_new, option_asc),
                        (col_round_c_res, option_asc),
                    ],
                    // [a+b ASC, c_new ASC]
                    vec![(&a_new_plus_b_new, option_asc), (col_c_new, option_asc)],
                ],
            ),
        ];

        for (idx, (orderings, expected)) in test_cases.iter().enumerate() {
            let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

            let orderings = convert_to_orderings(orderings);
            eq_properties.add_orderings(orderings);

            let expected = convert_to_orderings(expected);

            let projected_eq =
                eq_properties.project(&projection_mapping, Arc::clone(&output_schema));
            let orderings = projected_eq.oeq_class();

            let err_msg = format!(
                "test idx: {idx:?}, actual: {orderings:?}, expected: {expected:?}, projection_mapping: {projection_mapping:?}"
            );

            assert_eq!(orderings.len(), expected.len(), "{err_msg}");
            for expected_ordering in &expected {
                assert!(orderings.contains(expected_ordering), "{}", err_msg)
            }
        }
        Ok(())
    }

    #[test]
    fn project_orderings3() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Int32, true),
            Field::new("f", DataType::Int32, true),
        ]));
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_d = &col("d", &schema)?;
        let col_e = &col("e", &schema)?;
        let col_f = &col("f", &schema)?;
        let a_plus_b = Arc::new(BinaryExpr::new(
            Arc::clone(col_a),
            Operator::Plus,
            Arc::clone(col_b),
        )) as Arc<dyn PhysicalExpr>;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let proj_exprs = vec![
            (col_c, "c_new".to_string()),
            (col_d, "d_new".to_string()),
            (&a_plus_b, "a+b".to_string()),
        ];
        let proj_exprs = proj_exprs
            .into_iter()
            .map(|(expr, name)| (Arc::clone(expr), name));
        let projection_mapping = ProjectionMapping::try_new(proj_exprs, &schema)?;
        let output_schema = output_schema(&projection_mapping, &schema)?;

        let col_a_plus_b_new = &col("a+b", &output_schema)?;
        let col_c_new = &col("c_new", &output_schema)?;
        let col_d_new = &col("d_new", &output_schema)?;

        let test_cases = vec![
            // ---------- TEST CASE 1 ------------
            (
                // orderings
                vec![
                    // [d ASC, b ASC]
                    vec![(col_d, option_asc), (col_b, option_asc)],
                    // [c ASC, a ASC]
                    vec![(col_c, option_asc), (col_a, option_asc)],
                ],
                // equal conditions
                vec![],
                // expected
                vec![
                    // [d_new ASC, c_new ASC, a+b ASC]
                    vec![
                        (col_d_new, option_asc),
                        (col_c_new, option_asc),
                        (col_a_plus_b_new, option_asc),
                    ],
                    // [c_new ASC, d_new ASC, a+b ASC]
                    vec![
                        (col_c_new, option_asc),
                        (col_d_new, option_asc),
                        (col_a_plus_b_new, option_asc),
                    ],
                ],
            ),
            // ---------- TEST CASE 2 ------------
            (
                // orderings
                vec![
                    // [d ASC, b ASC]
                    vec![(col_d, option_asc), (col_b, option_asc)],
                    // [c ASC, e ASC], Please note that a=e
                    vec![(col_c, option_asc), (col_e, option_asc)],
                ],
                // equal conditions
                vec![(col_e, col_a)],
                // expected
                vec![
                    // [d_new ASC, c_new ASC, a+b ASC]
                    vec![
                        (col_d_new, option_asc),
                        (col_c_new, option_asc),
                        (col_a_plus_b_new, option_asc),
                    ],
                    // [c_new ASC, d_new ASC, a+b ASC]
                    vec![
                        (col_c_new, option_asc),
                        (col_d_new, option_asc),
                        (col_a_plus_b_new, option_asc),
                    ],
                ],
            ),
            // ---------- TEST CASE 3 ------------
            (
                // orderings
                vec![
                    // [d ASC, b ASC]
                    vec![(col_d, option_asc), (col_b, option_asc)],
                    // [c ASC, e ASC], Please note that a=f
                    vec![(col_c, option_asc), (col_e, option_asc)],
                ],
                // equal conditions
                vec![(col_a, col_f)],
                // expected
                vec![
                    // [d_new ASC]
                    vec![(col_d_new, option_asc)],
                    // [c_new ASC]
                    vec![(col_c_new, option_asc)],
                ],
            ),
        ];
        for (orderings, equal_columns, expected) in test_cases {
            let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
            for (lhs, rhs) in equal_columns {
                eq_properties.add_equal_conditions(Arc::clone(lhs), Arc::clone(rhs))?;
            }

            let orderings = convert_to_orderings(&orderings);
            eq_properties.add_orderings(orderings);

            let expected = convert_to_orderings(&expected);

            let projected_eq =
                eq_properties.project(&projection_mapping, Arc::clone(&output_schema));
            let orderings = projected_eq.oeq_class();

            let err_msg = format!(
                "actual: {orderings:?}, expected: {expected:?}, projection_mapping: {projection_mapping:?}"
            );

            assert_eq!(orderings.len(), expected.len(), "{err_msg}");
            for expected_ordering in &expected {
                assert!(orderings.contains(expected_ordering), "{}", err_msg)
            }
        }

        Ok(())
    }
}
