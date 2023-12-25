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

mod class;
mod ordering;
mod projection;
mod properties;
use crate::expressions::Column;
use crate::{LexRequirement, PhysicalExpr, PhysicalSortRequirement};
use arrow::datatypes::SchemaRef;
pub use class::{EquivalenceClass, EquivalenceGroup};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{JoinSide, JoinType};
pub use ordering::OrderingEquivalenceClass;
pub use projection::ProjectionMapping;
pub use properties::EquivalenceProperties;
use std::sync::Arc;

/// This function constructs a duplicate-free `LexOrderingReq` by filtering out
/// duplicate entries that have same physical expression inside. For example,
/// `vec![a Some(ASC), a Some(DESC)]` collapses to `vec![a Some(ASC)]`.
pub fn collapse_lex_req(input: LexRequirement) -> LexRequirement {
    let mut output = Vec::<PhysicalSortRequirement>::new();
    for item in input {
        if !output.iter().any(|req| req.expr.eq(&item.expr)) {
            output.push(item);
        }
    }
    output
}

/// Adds the `offset` value to `Column` indices inside `expr`. This function is
/// generally used during the update of the right table schema in join operations.
pub fn add_offset_to_expr(
    expr: Arc<dyn PhysicalExpr>,
    offset: usize,
) -> Arc<dyn PhysicalExpr> {
    expr.transform_down(&|e| match e.as_any().downcast_ref::<Column>() {
        Some(col) => Ok(Transformed::Yes(Arc::new(Column::new(
            col.name(),
            offset + col.index(),
        )))),
        None => Ok(Transformed::No(e)),
    })
    .unwrap()
    // Note that we can safely unwrap here since our transform always returns
    // an `Ok` value.
}

/// Calculate ordering equivalence properties for the given join operation.
pub fn join_equivalence_properties(
    left: EquivalenceProperties,
    right: EquivalenceProperties,
    join_type: &JoinType,
    join_schema: SchemaRef,
    maintains_input_order: &[bool],
    probe_side: Option<JoinSide>,
    on: &[(Column, Column)],
) -> EquivalenceProperties {
    let left_size = left.schema.fields.len();
    let mut result = EquivalenceProperties::new(join_schema);
    result.add_equivalence_group(left.eq_group().join(
        right.eq_group(),
        join_type,
        left_size,
        on,
    ));

    let left_oeq_class = left.oeq_class;
    let mut right_oeq_class = right.oeq_class;
    match maintains_input_order {
        [true, false] => {
            // In this special case, right side ordering can be prefixed with
            // the left side ordering.
            if let (Some(JoinSide::Left), JoinType::Inner) = (probe_side, join_type) {
                updated_right_ordering_equivalence_class(
                    &mut right_oeq_class,
                    join_type,
                    left_size,
                );

                // Right side ordering equivalence properties should be prepended
                // with those of the left side while constructing output ordering
                // equivalence properties since stream side is the left side.
                //
                // For example, if the right side ordering equivalences contain
                // `b ASC`, and the left side ordering equivalences contain `a ASC`,
                // then we should add `a ASC, b ASC` to the ordering equivalences
                // of the join output.
                let out_oeq_class = left_oeq_class.join_suffix(&right_oeq_class);
                result.add_ordering_equivalence_class(out_oeq_class);
            } else {
                result.add_ordering_equivalence_class(left_oeq_class);
            }
        }
        [false, true] => {
            updated_right_ordering_equivalence_class(
                &mut right_oeq_class,
                join_type,
                left_size,
            );
            // In this special case, left side ordering can be prefixed with
            // the right side ordering.
            if let (Some(JoinSide::Right), JoinType::Inner) = (probe_side, join_type) {
                // Left side ordering equivalence properties should be prepended
                // with those of the right side while constructing output ordering
                // equivalence properties since stream side is the right side.
                //
                // For example, if the left side ordering equivalences contain
                // `a ASC`, and the right side ordering equivalences contain `b ASC`,
                // then we should add `b ASC, a ASC` to the ordering equivalences
                // of the join output.
                let out_oeq_class = right_oeq_class.join_suffix(&left_oeq_class);
                result.add_ordering_equivalence_class(out_oeq_class);
            } else {
                result.add_ordering_equivalence_class(right_oeq_class);
            }
        }
        [false, false] => {}
        [true, true] => unreachable!("Cannot maintain ordering of both sides"),
        _ => unreachable!("Join operators can not have more than two children"),
    }
    result
}

/// In the context of a join, update the right side `OrderingEquivalenceClass`
/// so that they point to valid indices in the join output schema.
///
/// To do so, we increment column indices by the size of the left table when
/// join schema consists of a combination of the left and right schemas. This
/// is the case for `Inner`, `Left`, `Full` and `Right` joins. For other cases,
/// indices do not change.
fn updated_right_ordering_equivalence_class(
    right_oeq_class: &mut OrderingEquivalenceClass,
    join_type: &JoinType,
    left_size: usize,
) {
    if matches!(
        join_type,
        JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right
    ) {
        right_oeq_class.add_offset(left_size);
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Not;
    use std::sync::Arc;

    use super::*;
    use crate::execution_props::ExecutionProps;
    use crate::expressions::{col, lit, BinaryExpr, Column, Literal};
    use crate::functions::create_physical_expr;

    use arrow::compute::{lexsort_to_indices, SortColumn};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{ArrayRef, Float64Array, RecordBatch, UInt32Array};
    use arrow_schema::{Fields, SortOptions, TimeUnit};
    use datafusion_common::{plan_datafusion_err, DataFusionError, Result, ScalarValue};
    use datafusion_expr::{BuiltinScalarFunction, Operator};

    use itertools::{izip, Itertools};
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};

    fn output_schema(
        mapping: &ProjectionMapping,
        input_schema: &Arc<Schema>,
    ) -> Result<SchemaRef> {
        // Calculate output schema
        let fields: Result<Vec<Field>> = mapping
            .iter()
            .map(|(source, target)| {
                let name = target
                    .as_any()
                    .downcast_ref::<Column>()
                    .ok_or_else(|| plan_datafusion_err!("Expects to have column"))?
                    .name();
                let field = Field::new(
                    name,
                    source.data_type(input_schema)?,
                    source.nullable(input_schema)?,
                );

                Ok(field)
            })
            .collect();

        let output_schema = Arc::new(Schema::new_with_metadata(
            fields?,
            input_schema.metadata().clone(),
        ));

        Ok(output_schema)
    }

    // Generate a schema which consists of 8 columns (a, b, c, d, e, f, g, h)
    fn create_test_schema() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, true);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, true);
        let e = Field::new("e", DataType::Int32, true);
        let f = Field::new("f", DataType::Int32, true);
        let g = Field::new("g", DataType::Int32, true);
        let h = Field::new("h", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e, f, g, h]));

        Ok(schema)
    }

    /// Construct a schema with following properties
    /// Schema satisfies following orderings:
    /// [a ASC], [d ASC, b ASC], [e DESC, f ASC, g ASC]
    /// and
    /// Column [a=c] (e.g they are aliases).
    fn create_test_params() -> Result<(SchemaRef, EquivalenceProperties)> {
        let test_schema = create_test_schema()?;
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;
        let col_e = &col("e", &test_schema)?;
        let col_f = &col("f", &test_schema)?;
        let col_g = &col("g", &test_schema)?;
        let mut eq_properties = EquivalenceProperties::new(test_schema.clone());
        eq_properties.add_equal_conditions(col_a, col_c);

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        let orderings = vec![
            // [a ASC]
            vec![(col_a, option_asc)],
            // [d ASC, b ASC]
            vec![(col_d, option_asc), (col_b, option_asc)],
            // [e DESC, f ASC, g ASC]
            vec![
                (col_e, option_desc),
                (col_f, option_asc),
                (col_g, option_asc),
            ],
        ];
        let orderings = convert_to_orderings(&orderings);
        eq_properties.add_new_orderings(orderings);
        Ok((test_schema, eq_properties))
    }

    // Generate a schema which consists of 6 columns (a, b, c, d, e, f)
    fn create_test_schema_2() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Float64, true);
        let b = Field::new("b", DataType::Float64, true);
        let c = Field::new("c", DataType::Float64, true);
        let d = Field::new("d", DataType::Float64, true);
        let e = Field::new("e", DataType::Float64, true);
        let f = Field::new("f", DataType::Float64, true);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e, f]));

        Ok(schema)
    }

    /// Construct a schema with random ordering
    /// among column a, b, c, d
    /// where
    /// Column [a=f] (e.g they are aliases).
    /// Column e is constant.
    fn create_random_schema(seed: u64) -> Result<(SchemaRef, EquivalenceProperties)> {
        let test_schema = create_test_schema_2()?;
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;
        let col_e = &col("e", &test_schema)?;
        let col_f = &col("f", &test_schema)?;
        let col_exprs = [col_a, col_b, col_c, col_d, col_e, col_f];

        let mut eq_properties = EquivalenceProperties::new(test_schema.clone());
        // Define a and f are aliases
        eq_properties.add_equal_conditions(col_a, col_f);
        // Column e has constant value.
        eq_properties = eq_properties.add_constants([col_e.clone()]);

        // Randomly order columns for sorting
        let mut rng = StdRng::seed_from_u64(seed);
        let mut remaining_exprs = col_exprs[0..4].to_vec(); // only a, b, c, d are sorted

        let options_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        while !remaining_exprs.is_empty() {
            let n_sort_expr = rng.gen_range(0..remaining_exprs.len() + 1);
            remaining_exprs.shuffle(&mut rng);

            let ordering = remaining_exprs
                .drain(0..n_sort_expr)
                .map(|expr| PhysicalSortExpr {
                    expr: expr.clone(),
                    options: options_asc,
                })
                .collect();

            eq_properties.add_new_orderings([ordering]);
        }

        Ok((test_schema, eq_properties))
    }

    // Convert each tuple to PhysicalSortRequirement
    fn convert_to_sort_reqs(
        in_data: &[(&Arc<dyn PhysicalExpr>, Option<SortOptions>)],
    ) -> Vec<PhysicalSortRequirement> {
        in_data
            .iter()
            .map(|(expr, options)| {
                PhysicalSortRequirement::new((*expr).clone(), *options)
            })
            .collect()
    }

    // Convert each tuple to PhysicalSortExpr
    fn convert_to_sort_exprs(
        in_data: &[(&Arc<dyn PhysicalExpr>, SortOptions)],
    ) -> Vec<PhysicalSortExpr> {
        in_data
            .iter()
            .map(|(expr, options)| PhysicalSortExpr {
                expr: (*expr).clone(),
                options: *options,
            })
            .collect()
    }

    // Convert each inner tuple to PhysicalSortExpr
    fn convert_to_orderings(
        orderings: &[Vec<(&Arc<dyn PhysicalExpr>, SortOptions)>],
    ) -> Vec<Vec<PhysicalSortExpr>> {
        orderings
            .iter()
            .map(|sort_exprs| convert_to_sort_exprs(sort_exprs))
            .collect()
    }

    // Convert each tuple to PhysicalSortExpr
    fn convert_to_sort_exprs_owned(
        in_data: &[(Arc<dyn PhysicalExpr>, SortOptions)],
    ) -> Vec<PhysicalSortExpr> {
        in_data
            .iter()
            .map(|(expr, options)| PhysicalSortExpr {
                expr: (*expr).clone(),
                options: *options,
            })
            .collect()
    }

    // Convert each inner tuple to PhysicalSortExpr
    fn convert_to_orderings_owned(
        orderings: &[Vec<(Arc<dyn PhysicalExpr>, SortOptions)>],
    ) -> Vec<Vec<PhysicalSortExpr>> {
        orderings
            .iter()
            .map(|sort_exprs| convert_to_sort_exprs_owned(sort_exprs))
            .collect()
    }

    // Apply projection to the input_data, return projected equivalence properties and record batch
    fn apply_projection(
        proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)>,
        input_data: &RecordBatch,
        input_eq_properties: &EquivalenceProperties,
    ) -> Result<(RecordBatch, EquivalenceProperties)> {
        let input_schema = input_data.schema();
        let projection_mapping = ProjectionMapping::try_new(&proj_exprs, &input_schema)?;

        let output_schema = output_schema(&projection_mapping, &input_schema)?;
        let num_rows = input_data.num_rows();
        // Apply projection to the input record batch.
        let projected_values = projection_mapping
            .iter()
            .map(|(source, _target)| source.evaluate(input_data)?.into_array(num_rows))
            .collect::<Result<Vec<_>>>()?;
        let projected_batch = if projected_values.is_empty() {
            RecordBatch::new_empty(output_schema.clone())
        } else {
            RecordBatch::try_new(output_schema.clone(), projected_values)?
        };

        let projected_eq =
            input_eq_properties.project(&projection_mapping, output_schema);
        Ok((projected_batch, projected_eq))
    }

    #[test]
    fn add_equal_conditions_test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Int64, true),
        ]));

        let mut eq_properties = EquivalenceProperties::new(schema);
        let col_a_expr = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let col_b_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;
        let col_c_expr = Arc::new(Column::new("c", 2)) as Arc<dyn PhysicalExpr>;
        let col_x_expr = Arc::new(Column::new("x", 3)) as Arc<dyn PhysicalExpr>;
        let col_y_expr = Arc::new(Column::new("y", 4)) as Arc<dyn PhysicalExpr>;

        // a and b are aliases
        eq_properties.add_equal_conditions(&col_a_expr, &col_b_expr);
        assert_eq!(eq_properties.eq_group().len(), 1);

        // This new entry is redundant, size shouldn't increase
        eq_properties.add_equal_conditions(&col_b_expr, &col_a_expr);
        assert_eq!(eq_properties.eq_group().len(), 1);
        let eq_groups = &eq_properties.eq_group().classes[0];
        assert_eq!(eq_groups.len(), 2);
        assert!(eq_groups.contains(&col_a_expr));
        assert!(eq_groups.contains(&col_b_expr));

        // b and c are aliases. Exising equivalence class should expand,
        // however there shouldn't be any new equivalence class
        eq_properties.add_equal_conditions(&col_b_expr, &col_c_expr);
        assert_eq!(eq_properties.eq_group().len(), 1);
        let eq_groups = &eq_properties.eq_group().classes[0];
        assert_eq!(eq_groups.len(), 3);
        assert!(eq_groups.contains(&col_a_expr));
        assert!(eq_groups.contains(&col_b_expr));
        assert!(eq_groups.contains(&col_c_expr));

        // This is a new set of equality. Hence equivalent class count should be 2.
        eq_properties.add_equal_conditions(&col_x_expr, &col_y_expr);
        assert_eq!(eq_properties.eq_group().len(), 2);

        // This equality bridges distinct equality sets.
        // Hence equivalent class count should decrease from 2 to 1.
        eq_properties.add_equal_conditions(&col_x_expr, &col_a_expr);
        assert_eq!(eq_properties.eq_group().len(), 1);
        let eq_groups = &eq_properties.eq_group().classes[0];
        assert_eq!(eq_groups.len(), 5);
        assert!(eq_groups.contains(&col_a_expr));
        assert!(eq_groups.contains(&col_b_expr));
        assert!(eq_groups.contains(&col_c_expr));
        assert!(eq_groups.contains(&col_x_expr));
        assert!(eq_groups.contains(&col_y_expr));

        Ok(())
    }

    #[test]
    fn project_equivalence_properties_test() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));

        let input_properties = EquivalenceProperties::new(input_schema.clone());
        let col_a = col("a", &input_schema)?;

        // a as a1, a as a2, a as a3, a as a3
        let proj_exprs = vec![
            (col_a.clone(), "a1".to_string()),
            (col_a.clone(), "a2".to_string()),
            (col_a.clone(), "a3".to_string()),
            (col_a.clone(), "a4".to_string()),
        ];
        let projection_mapping = ProjectionMapping::try_new(&proj_exprs, &input_schema)?;

        let out_schema = output_schema(&projection_mapping, &input_schema)?;
        // a as a1, a as a2, a as a3, a as a3
        let proj_exprs = vec![
            (col_a.clone(), "a1".to_string()),
            (col_a.clone(), "a2".to_string()),
            (col_a.clone(), "a3".to_string()),
            (col_a.clone(), "a4".to_string()),
        ];
        let projection_mapping = ProjectionMapping::try_new(&proj_exprs, &input_schema)?;

        // a as a1, a as a2, a as a3, a as a3
        let col_a1 = &col("a1", &out_schema)?;
        let col_a2 = &col("a2", &out_schema)?;
        let col_a3 = &col("a3", &out_schema)?;
        let col_a4 = &col("a4", &out_schema)?;
        let out_properties = input_properties.project(&projection_mapping, out_schema);

        // At the output a1=a2=a3=a4
        assert_eq!(out_properties.eq_group().len(), 1);
        let eq_class = &out_properties.eq_group().classes[0];
        assert_eq!(eq_class.len(), 4);
        assert!(eq_class.contains(col_a1));
        assert!(eq_class.contains(col_a2));
        assert!(eq_class.contains(col_a3));
        assert!(eq_class.contains(col_a4));

        Ok(())
    }

    #[test]
    fn test_ordering_satisfy() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let crude = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("a", 0)),
            options: SortOptions::default(),
        }];
        let finer = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
        ];
        // finer ordering satisfies, crude ordering should return true
        let mut eq_properties_finer = EquivalenceProperties::new(input_schema.clone());
        eq_properties_finer.oeq_class.push(finer.clone());
        assert!(eq_properties_finer.ordering_satisfy(&crude));

        // Crude ordering doesn't satisfy finer ordering. should return false
        let mut eq_properties_crude = EquivalenceProperties::new(input_schema.clone());
        eq_properties_crude.oeq_class.push(crude.clone());
        assert!(!eq_properties_crude.ordering_satisfy(&finer));
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
                    expr: expr.clone(),
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

    #[test]
    fn test_ordering_satisfy_with_equivalence2() -> Result<()> {
        let test_schema = create_test_schema()?;
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;
        let col_e = &col("e", &test_schema)?;
        let col_f = &col("f", &test_schema)?;
        let floor_a = &create_physical_expr(
            &BuiltinScalarFunction::Floor,
            &[col("a", &test_schema)?],
            &test_schema,
            &ExecutionProps::default(),
        )?;
        let floor_f = &create_physical_expr(
            &BuiltinScalarFunction::Floor,
            &[col("f", &test_schema)?],
            &test_schema,
            &ExecutionProps::default(),
        )?;
        let exp_a = &create_physical_expr(
            &BuiltinScalarFunction::Exp,
            &[col("a", &test_schema)?],
            &test_schema,
            &ExecutionProps::default(),
        )?;
        let a_plus_b = Arc::new(BinaryExpr::new(
            col_a.clone(),
            Operator::Plus,
            col_b.clone(),
        )) as Arc<dyn PhysicalExpr>;
        let options = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let test_cases = vec![
            // ------------ TEST CASE 1 ------------
            (
                // orderings
                vec![
                    // [a ASC, d ASC, b ASC]
                    vec![(col_a, options), (col_d, options), (col_b, options)],
                    // [c ASC]
                    vec![(col_c, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [a ASC, b ASC], requirement is not satisfied.
                vec![(col_a, options), (col_b, options)],
                // expected: requirement is not satisfied.
                false,
            ),
            // ------------ TEST CASE 2 ------------
            (
                // orderings
                vec![
                    // [a ASC, c ASC, b ASC]
                    vec![(col_a, options), (col_c, options), (col_b, options)],
                    // [d ASC]
                    vec![(col_d, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [floor(a) ASC],
                vec![(floor_a, options)],
                // expected: requirement is satisfied.
                true,
            ),
            // ------------ TEST CASE 2.1 ------------
            (
                // orderings
                vec![
                    // [a ASC, c ASC, b ASC]
                    vec![(col_a, options), (col_c, options), (col_b, options)],
                    // [d ASC]
                    vec![(col_d, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [floor(f) ASC], (Please note that a=f)
                vec![(floor_f, options)],
                // expected: requirement is satisfied.
                true,
            ),
            // ------------ TEST CASE 3 ------------
            (
                // orderings
                vec![
                    // [a ASC, c ASC, b ASC]
                    vec![(col_a, options), (col_c, options), (col_b, options)],
                    // [d ASC]
                    vec![(col_d, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [a ASC, c ASC, a+b ASC],
                vec![(col_a, options), (col_c, options), (&a_plus_b, options)],
                // expected: requirement is satisfied.
                true,
            ),
            // ------------ TEST CASE 4 ------------
            (
                // orderings
                vec![
                    // [a ASC, b ASC, c ASC, d ASC]
                    vec![
                        (col_a, options),
                        (col_b, options),
                        (col_c, options),
                        (col_d, options),
                    ],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [floor(a) ASC, a+b ASC],
                vec![(floor_a, options), (&a_plus_b, options)],
                // expected: requirement is satisfied.
                false,
            ),
            // ------------ TEST CASE 5 ------------
            (
                // orderings
                vec![
                    // [a ASC, b ASC, c ASC, d ASC]
                    vec![
                        (col_a, options),
                        (col_b, options),
                        (col_c, options),
                        (col_d, options),
                    ],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [exp(a) ASC, a+b ASC],
                vec![(exp_a, options), (&a_plus_b, options)],
                // expected: requirement is not satisfied.
                // TODO: If we know that exp function is 1-to-1 function.
                //  we could have deduced that above requirement is satisfied.
                false,
            ),
            // ------------ TEST CASE 6 ------------
            (
                // orderings
                vec![
                    // [a ASC, d ASC, b ASC]
                    vec![(col_a, options), (col_d, options), (col_b, options)],
                    // [c ASC]
                    vec![(col_c, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [a ASC, d ASC, floor(a) ASC],
                vec![(col_a, options), (col_d, options), (floor_a, options)],
                // expected: requirement is satisfied.
                true,
            ),
            // ------------ TEST CASE 7 ------------
            (
                // orderings
                vec![
                    // [a ASC, c ASC, b ASC]
                    vec![(col_a, options), (col_c, options), (col_b, options)],
                    // [d ASC]
                    vec![(col_d, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [a ASC, floor(a) ASC, a + b ASC],
                vec![(col_a, options), (floor_a, options), (&a_plus_b, options)],
                // expected: requirement is not satisfied.
                false,
            ),
            // ------------ TEST CASE 8 ------------
            (
                // orderings
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![(col_a, options), (col_b, options), (col_c, options)],
                    // [d ASC]
                    vec![(col_d, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [a ASC, c ASC, floor(a) ASC, a + b ASC],
                vec![
                    (col_a, options),
                    (col_c, options),
                    (&floor_a, options),
                    (&a_plus_b, options),
                ],
                // expected: requirement is not satisfied.
                false,
            ),
            // ------------ TEST CASE 9 ------------
            (
                // orderings
                vec![
                    // [a ASC, b ASC, c ASC, d ASC]
                    vec![
                        (col_a, options),
                        (col_b, options),
                        (col_c, options),
                        (col_d, options),
                    ],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [a ASC, b ASC, c ASC, floor(a) ASC],
                vec![
                    (col_a, options),
                    (col_b, options),
                    (&col_c, options),
                    (&floor_a, options),
                ],
                // expected: requirement is satisfied.
                true,
            ),
            // ------------ TEST CASE 10 ------------
            (
                // orderings
                vec![
                    // [d ASC, b ASC]
                    vec![(col_d, options), (col_b, options)],
                    // [c ASC, a ASC]
                    vec![(col_c, options), (col_a, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [c ASC, d ASC, a + b ASC],
                vec![(col_c, options), (col_d, options), (&a_plus_b, options)],
                // expected: requirement is satisfied.
                true,
            ),
        ];

        for (orderings, eq_group, constants, reqs, expected) in test_cases {
            let err_msg =
                format!("error in test orderings: {orderings:?}, eq_group: {eq_group:?}, constants: {constants:?}, reqs: {reqs:?}, expected: {expected:?}");
            let mut eq_properties = EquivalenceProperties::new(test_schema.clone());
            let orderings = convert_to_orderings(&orderings);
            eq_properties.add_new_orderings(orderings);
            let eq_group = eq_group
                .into_iter()
                .map(|eq_class| {
                    let eq_classes = eq_class.into_iter().cloned().collect::<Vec<_>>();
                    EquivalenceClass::new(eq_classes)
                })
                .collect::<Vec<_>>();
            let eq_group = EquivalenceGroup::new(eq_group);
            eq_properties.add_equivalence_group(eq_group);

            let constants = constants.into_iter().cloned();
            eq_properties = eq_properties.add_constants(constants);

            let reqs = convert_to_sort_exprs(&reqs);
            assert_eq!(
                eq_properties.ordering_satisfy(&reqs),
                expected,
                "{}",
                err_msg
            );
        }

        Ok(())
    }

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
            let col_exprs = vec![
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
                            expr: expr.clone(),
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

            let floor_a = create_physical_expr(
                &BuiltinScalarFunction::Floor,
                &[col("a", &test_schema)?],
                &test_schema,
                &ExecutionProps::default(),
            )?;
            let a_plus_b = Arc::new(BinaryExpr::new(
                col("a", &test_schema)?,
                Operator::Plus,
                col("b", &test_schema)?,
            )) as Arc<dyn PhysicalExpr>;
            let exprs = vec![
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
                            expr: expr.clone(),
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
    fn test_ordering_satisfy_different_lengths() -> Result<()> {
        let test_schema = create_test_schema()?;
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;
        let col_e = &col("e", &test_schema)?;
        let col_f = &col("f", &test_schema)?;
        let options = SortOptions {
            descending: false,
            nulls_first: false,
        };
        // a=c (e.g they are aliases).
        let mut eq_properties = EquivalenceProperties::new(test_schema);
        eq_properties.add_equal_conditions(col_a, col_c);

        let orderings = vec![
            vec![(col_a, options)],
            vec![(col_e, options)],
            vec![(col_d, options), (col_f, options)],
        ];
        let orderings = convert_to_orderings(&orderings);

        // Column [a ASC], [e ASC], [d ASC, f ASC] are all valid orderings for the schema.
        eq_properties.add_new_orderings(orderings);

        // First entry in the tuple is required ordering, second entry is the expected flag
        // that indicates whether this required ordering is satisfied.
        // ([a ASC], true) indicate a ASC requirement is already satisfied by existing orderings.
        let test_cases = vec![
            // [c ASC, a ASC, e ASC], expected represents this requirement is satisfied
            (
                vec![(col_c, options), (col_a, options), (col_e, options)],
                true,
            ),
            (vec![(col_c, options), (col_b, options)], false),
            (vec![(col_c, options), (col_d, options)], true),
            (
                vec![(col_d, options), (col_f, options), (col_b, options)],
                false,
            ),
            (vec![(col_d, options), (col_f, options)], true),
        ];

        for (reqs, expected) in test_cases {
            let err_msg =
                format!("error in test reqs: {:?}, expected: {:?}", reqs, expected,);
            let reqs = convert_to_sort_exprs(&reqs);
            assert_eq!(
                eq_properties.ordering_satisfy(&reqs),
                expected,
                "{}",
                err_msg
            );
        }

        Ok(())
    }

    #[test]
    fn test_bridge_groups() -> Result<()> {
        // First entry in the tuple is argument, second entry is the bridged result
        let test_cases = vec![
            // ------- TEST CASE 1 -----------//
            (
                vec![vec![1, 2, 3], vec![2, 4, 5], vec![11, 12, 9], vec![7, 6, 5]],
                // Expected is compared with set equality. Order of the specific results may change.
                vec![vec![1, 2, 3, 4, 5, 6, 7], vec![9, 11, 12]],
            ),
            // ------- TEST CASE 2 -----------//
            (
                vec![vec![1, 2, 3], vec![3, 4, 5], vec![9, 8, 7], vec![7, 6, 5]],
                // Expected
                vec![vec![1, 2, 3, 4, 5, 6, 7, 8, 9]],
            ),
        ];
        for (entries, expected) in test_cases {
            let entries = entries
                .into_iter()
                .map(|entry| entry.into_iter().map(lit).collect::<Vec<_>>())
                .map(EquivalenceClass::new)
                .collect::<Vec<_>>();
            let expected = expected
                .into_iter()
                .map(|entry| entry.into_iter().map(lit).collect::<Vec<_>>())
                .map(EquivalenceClass::new)
                .collect::<Vec<_>>();
            let mut eq_groups = EquivalenceGroup::new(entries.clone());
            eq_groups.bridge_classes();
            let eq_groups = eq_groups.classes;
            let err_msg = format!(
                "error in test entries: {:?}, expected: {:?}, actual:{:?}",
                entries, expected, eq_groups
            );
            assert_eq!(eq_groups.len(), expected.len(), "{}", err_msg);
            for idx in 0..eq_groups.len() {
                assert_eq!(&eq_groups[idx], &expected[idx], "{}", err_msg);
            }
        }
        Ok(())
    }

    #[test]
    fn test_remove_redundant_entries_eq_group() -> Result<()> {
        let entries = vec![
            EquivalenceClass::new(vec![lit(1), lit(1), lit(2)]),
            // This group is meaningless should be removed
            EquivalenceClass::new(vec![lit(3), lit(3)]),
            EquivalenceClass::new(vec![lit(4), lit(5), lit(6)]),
        ];
        // Given equivalences classes are not in succinct form.
        // Expected form is the most plain representation that is functionally same.
        let expected = vec![
            EquivalenceClass::new(vec![lit(1), lit(2)]),
            EquivalenceClass::new(vec![lit(4), lit(5), lit(6)]),
        ];
        let mut eq_groups = EquivalenceGroup::new(entries);
        eq_groups.remove_redundant_entries();

        let eq_groups = eq_groups.classes;
        assert_eq!(eq_groups.len(), expected.len());
        assert_eq!(eq_groups.len(), 2);

        assert_eq!(eq_groups[0], expected[0]);
        assert_eq!(eq_groups[1], expected[1]);
        Ok(())
    }

    #[test]
    fn test_remove_redundant_entries_oeq_class() -> Result<()> {
        let schema = create_test_schema()?;
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_d = &col("d", &schema)?;
        let col_e = &col("e", &schema)?;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };

        // First entry in the tuple is the given orderings for the table
        // Second entry is the simplest version of the given orderings that is functionally equivalent.
        let test_cases = vec![
            // ------- TEST CASE 1 ---------
            (
                // ORDERINGS GIVEN
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                ],
            ),
            // ------- TEST CASE 2 ---------
            (
                // ORDERINGS GIVEN
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 3 ---------
            (
                // ORDERINGS GIVEN
                vec![
                    // [a ASC, b DESC]
                    vec![(col_a, option_asc), (col_b, option_desc)],
                    // [a ASC]
                    vec![(col_a, option_asc)],
                    // [a ASC, c ASC]
                    vec![(col_a, option_asc), (col_c, option_asc)],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [a ASC, b DESC]
                    vec![(col_a, option_asc), (col_b, option_desc)],
                    // [a ASC, c ASC]
                    vec![(col_a, option_asc), (col_c, option_asc)],
                ],
            ),
            // ------- TEST CASE 4 ---------
            (
                // ORDERINGS GIVEN
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                    // [a ASC]
                    vec![(col_a, option_asc)],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 5 ---------
            // Empty ordering
            (
                vec![vec![]],
                // No ordering in the state (empty ordering is ignored).
                vec![],
            ),
            // ------- TEST CASE 6 ---------
            (
                // ORDERINGS GIVEN
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [b ASC]
                    vec![(col_b, option_asc)],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [a ASC]
                    vec![(col_a, option_asc)],
                    // [b ASC]
                    vec![(col_b, option_asc)],
                ],
            ),
            // ------- TEST CASE 7 ---------
            // b, a
            // c, a
            // d, b, c
            (
                // ORDERINGS GIVEN
                vec![
                    // [b ASC, a ASC]
                    vec![(col_b, option_asc), (col_a, option_asc)],
                    // [c ASC, a ASC]
                    vec![(col_c, option_asc), (col_a, option_asc)],
                    // [d ASC, b ASC, c ASC]
                    vec![
                        (col_d, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [b ASC, a ASC]
                    vec![(col_b, option_asc), (col_a, option_asc)],
                    // [c ASC, a ASC]
                    vec![(col_c, option_asc), (col_a, option_asc)],
                    // [d ASC]
                    vec![(col_d, option_asc)],
                ],
            ),
            // ------- TEST CASE 8 ---------
            // b, e
            // c, a
            // d, b, e, c, a
            (
                // ORDERINGS GIVEN
                vec![
                    // [b ASC, e ASC]
                    vec![(col_b, option_asc), (col_e, option_asc)],
                    // [c ASC, a ASC]
                    vec![(col_c, option_asc), (col_a, option_asc)],
                    // [d ASC, b ASC, e ASC, c ASC, a ASC]
                    vec![
                        (col_d, option_asc),
                        (col_b, option_asc),
                        (col_e, option_asc),
                        (col_c, option_asc),
                        (col_a, option_asc),
                    ],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [b ASC, e ASC]
                    vec![(col_b, option_asc), (col_e, option_asc)],
                    // [c ASC, a ASC]
                    vec![(col_c, option_asc), (col_a, option_asc)],
                    // [d ASC]
                    vec![(col_d, option_asc)],
                ],
            ),
            // ------- TEST CASE 9 ---------
            // b
            // a, b, c
            // d, a, b
            (
                // ORDERINGS GIVEN
                vec![
                    // [b ASC]
                    vec![(col_b, option_asc)],
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                    // [d ASC, a ASC, b ASC]
                    vec![
                        (col_d, option_asc),
                        (col_a, option_asc),
                        (col_b, option_asc),
                    ],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [b ASC]
                    vec![(col_b, option_asc)],
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                    // [d ASC]
                    vec![(col_d, option_asc)],
                ],
            ),
        ];
        for (orderings, expected) in test_cases {
            let orderings = convert_to_orderings(&orderings);
            let expected = convert_to_orderings(&expected);
            let actual = OrderingEquivalenceClass::new(orderings.clone());
            let actual = actual.orderings;
            let err_msg = format!(
                "orderings: {:?}, expected: {:?}, actual :{:?}",
                orderings, expected, actual
            );
            assert_eq!(actual.len(), expected.len(), "{}", err_msg);
            for elem in actual {
                assert!(expected.contains(&elem), "{}", err_msg);
            }
        }

        Ok(())
    }

    #[test]
    fn test_get_updated_right_ordering_equivalence_properties() -> Result<()> {
        let join_type = JoinType::Inner;
        // Join right child schema
        let child_fields: Fields = ["x", "y", "z", "w"]
            .into_iter()
            .map(|name| Field::new(name, DataType::Int32, true))
            .collect();
        let child_schema = Schema::new(child_fields);
        let col_x = &col("x", &child_schema)?;
        let col_y = &col("y", &child_schema)?;
        let col_z = &col("z", &child_schema)?;
        let col_w = &col("w", &child_schema)?;
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        // [x ASC, y ASC], [z ASC, w ASC]
        let orderings = vec![
            vec![(col_x, option_asc), (col_y, option_asc)],
            vec![(col_z, option_asc), (col_w, option_asc)],
        ];
        let orderings = convert_to_orderings(&orderings);
        // Right child ordering equivalences
        let mut right_oeq_class = OrderingEquivalenceClass::new(orderings);

        let left_columns_len = 4;

        let fields: Fields = ["a", "b", "c", "d", "x", "y", "z", "w"]
            .into_iter()
            .map(|name| Field::new(name, DataType::Int32, true))
            .collect();

        // Join Schema
        let schema = Schema::new(fields);
        let col_a = &col("a", &schema)?;
        let col_d = &col("d", &schema)?;
        let col_x = &col("x", &schema)?;
        let col_y = &col("y", &schema)?;
        let col_z = &col("z", &schema)?;
        let col_w = &col("w", &schema)?;

        let mut join_eq_properties = EquivalenceProperties::new(Arc::new(schema));
        // a=x and d=w
        join_eq_properties.add_equal_conditions(col_a, col_x);
        join_eq_properties.add_equal_conditions(col_d, col_w);

        updated_right_ordering_equivalence_class(
            &mut right_oeq_class,
            &join_type,
            left_columns_len,
        );
        join_eq_properties.add_ordering_equivalence_class(right_oeq_class);
        let result = join_eq_properties.oeq_class().clone();

        // [x ASC, y ASC], [z ASC, w ASC]
        let orderings = vec![
            vec![(col_x, option_asc), (col_y, option_asc)],
            vec![(col_z, option_asc), (col_w, option_asc)],
        ];
        let orderings = convert_to_orderings(&orderings);
        let expected = OrderingEquivalenceClass::new(orderings);

        assert_eq!(result, expected);

        Ok(())
    }

    /// Checks if the table (RecordBatch) remains unchanged when sorted according to the provided `required_ordering`.
    ///
    /// The function works by adding a unique column of ascending integers to the original table. This column ensures
    /// that rows that are otherwise indistinguishable (e.g., if they have the same values in all other columns) can
    /// still be differentiated. When sorting the extended table, the unique column acts as a tie-breaker to produce
    /// deterministic sorting results.
    ///
    /// If the table remains the same after sorting with the added unique column, it indicates that the table was
    /// already sorted according to `required_ordering` to begin with.
    fn is_table_same_after_sort(
        mut required_ordering: Vec<PhysicalSortExpr>,
        batch: RecordBatch,
    ) -> Result<bool> {
        // Clone the original schema and columns
        let original_schema = batch.schema();
        let mut columns = batch.columns().to_vec();

        // Create a new unique column
        let n_row = batch.num_rows();
        let vals: Vec<usize> = (0..n_row).collect::<Vec<_>>();
        let vals: Vec<f64> = vals.into_iter().map(|val| val as f64).collect();
        let unique_col = Arc::new(Float64Array::from_iter_values(vals)) as ArrayRef;
        columns.push(unique_col.clone());

        // Create a new schema with the added unique column
        let unique_col_name = "unique";
        let unique_field =
            Arc::new(Field::new(unique_col_name, DataType::Float64, false));
        let fields: Vec<_> = original_schema
            .fields()
            .iter()
            .cloned()
            .chain(std::iter::once(unique_field))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Create a new batch with the added column
        let new_batch = RecordBatch::try_new(schema.clone(), columns)?;

        // Add the unique column to the required ordering to ensure deterministic results
        required_ordering.push(PhysicalSortExpr {
            expr: Arc::new(Column::new(unique_col_name, original_schema.fields().len())),
            options: Default::default(),
        });

        // Convert the required ordering to a list of SortColumn
        let sort_columns = required_ordering
            .iter()
            .map(|order_expr| {
                let expr_result = order_expr.expr.evaluate(&new_batch)?;
                let values = expr_result.into_array(new_batch.num_rows())?;
                Ok(SortColumn {
                    values,
                    options: Some(order_expr.options),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Check if the indices after sorting match the initial ordering
        let sorted_indices = lexsort_to_indices(&sort_columns, None)?;
        let original_indices = UInt32Array::from_iter_values(0..n_row as u32);

        Ok(sorted_indices == original_indices)
    }

    // If we already generated a random result for one of the
    // expressions in the equivalence classes. For other expressions in the same
    // equivalence class use same result. This util gets already calculated result, when available.
    fn get_representative_arr(
        eq_group: &EquivalenceClass,
        existing_vec: &[Option<ArrayRef>],
        schema: SchemaRef,
    ) -> Option<ArrayRef> {
        for expr in eq_group.iter() {
            let col = expr.as_any().downcast_ref::<Column>().unwrap();
            let (idx, _field) = schema.column_with_name(col.name()).unwrap();
            if let Some(res) = &existing_vec[idx] {
                return Some(res.clone());
            }
        }
        None
    }

    // Generate a table that satisfies the given equivalence properties; i.e.
    // equivalences, ordering equivalences, and constants.
    fn generate_table_for_eq_properties(
        eq_properties: &EquivalenceProperties,
        n_elem: usize,
        n_distinct: usize,
    ) -> Result<RecordBatch> {
        let mut rng = StdRng::seed_from_u64(23);

        let schema = eq_properties.schema();
        let mut schema_vec = vec![None; schema.fields.len()];

        // Utility closure to generate random array
        let mut generate_random_array = |num_elems: usize, max_val: usize| -> ArrayRef {
            let values: Vec<f64> = (0..num_elems)
                .map(|_| rng.gen_range(0..max_val) as f64 / 2.0)
                .collect();
            Arc::new(Float64Array::from_iter_values(values))
        };

        // Fill constant columns
        for constant in &eq_properties.constants {
            let col = constant.as_any().downcast_ref::<Column>().unwrap();
            let (idx, _field) = schema.column_with_name(col.name()).unwrap();
            let arr = Arc::new(Float64Array::from_iter_values(vec![0 as f64; n_elem]))
                as ArrayRef;
            schema_vec[idx] = Some(arr);
        }

        // Fill columns based on ordering equivalences
        for ordering in eq_properties.oeq_class.iter() {
            let (sort_columns, indices): (Vec<_>, Vec<_>) = ordering
                .iter()
                .map(|PhysicalSortExpr { expr, options }| {
                    let col = expr.as_any().downcast_ref::<Column>().unwrap();
                    let (idx, _field) = schema.column_with_name(col.name()).unwrap();
                    let arr = generate_random_array(n_elem, n_distinct);
                    (
                        SortColumn {
                            values: arr,
                            options: Some(*options),
                        },
                        idx,
                    )
                })
                .unzip();

            let sort_arrs = arrow::compute::lexsort(&sort_columns, None)?;
            for (idx, arr) in izip!(indices, sort_arrs) {
                schema_vec[idx] = Some(arr);
            }
        }

        // Fill columns based on equivalence groups
        for eq_group in eq_properties.eq_group.iter() {
            let representative_array =
                get_representative_arr(eq_group, &schema_vec, schema.clone())
                    .unwrap_or_else(|| generate_random_array(n_elem, n_distinct));

            for expr in eq_group.iter() {
                let col = expr.as_any().downcast_ref::<Column>().unwrap();
                let (idx, _field) = schema.column_with_name(col.name()).unwrap();
                schema_vec[idx] = Some(representative_array.clone());
            }
        }

        let res: Vec<_> = schema_vec
            .into_iter()
            .zip(schema.fields.iter())
            .map(|(elem, field)| {
                (
                    field.name(),
                    // Generate random values for columns that do not occur in any of the groups (equivalence, ordering equivalence, constants)
                    elem.unwrap_or_else(|| generate_random_array(n_elem, n_distinct)),
                )
            })
            .collect();

        Ok(RecordBatch::try_from_iter(res)?)
    }

    #[test]
    fn test_schema_normalize_expr_with_equivalence() -> Result<()> {
        let col_a = &Column::new("a", 0);
        let col_b = &Column::new("b", 1);
        let col_c = &Column::new("c", 2);
        // Assume that column a and c are aliases.
        let (_test_schema, eq_properties) = create_test_params()?;

        let col_a_expr = Arc::new(col_a.clone()) as Arc<dyn PhysicalExpr>;
        let col_b_expr = Arc::new(col_b.clone()) as Arc<dyn PhysicalExpr>;
        let col_c_expr = Arc::new(col_c.clone()) as Arc<dyn PhysicalExpr>;
        // Test cases for equivalence normalization,
        // First entry in the tuple is argument, second entry is expected result after normalization.
        let expressions = vec![
            // Normalized version of the column a and c should go to a
            // (by convention all the expressions inside equivalence class are mapped to the first entry
            // in this case a is the first entry in the equivalence class.)
            (&col_a_expr, &col_a_expr),
            (&col_c_expr, &col_a_expr),
            // Cannot normalize column b
            (&col_b_expr, &col_b_expr),
        ];
        let eq_group = eq_properties.eq_group();
        for (expr, expected_eq) in expressions {
            assert!(
                expected_eq.eq(&eq_group.normalize_expr(expr.clone())),
                "error in test: expr: {expr:?}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_schema_normalize_sort_requirement_with_equivalence() -> Result<()> {
        let option1 = SortOptions {
            descending: false,
            nulls_first: false,
        };
        // Assume that column a and c are aliases.
        let (test_schema, eq_properties) = create_test_params()?;
        let col_a = &col("a", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;

        // Test cases for equivalence normalization
        // First entry in the tuple is PhysicalSortRequirement, second entry in the tuple is
        // expected PhysicalSortRequirement after normalization.
        let test_cases = vec![
            (vec![(col_a, Some(option1))], vec![(col_a, Some(option1))]),
            // In the normalized version column c should be replace with column a
            (vec![(col_c, Some(option1))], vec![(col_a, Some(option1))]),
            (vec![(col_c, None)], vec![(col_a, None)]),
            (vec![(col_d, Some(option1))], vec![(col_d, Some(option1))]),
        ];
        for (reqs, expected) in test_cases.into_iter() {
            let reqs = convert_to_sort_reqs(&reqs);
            let expected = convert_to_sort_reqs(&expected);

            let normalized = eq_properties.normalize_sort_requirements(&reqs);
            assert!(
                expected.eq(&normalized),
                "error in test: reqs: {reqs:?}, expected: {expected:?}, normalized: {normalized:?}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_normalize_sort_reqs() -> Result<()> {
        // Schema satisfies following properties
        // a=c
        // and following orderings are valid
        // [a ASC], [d ASC, b ASC], [e DESC, f ASC, g ASC]
        let (test_schema, eq_properties) = create_test_params()?;
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;
        let col_e = &col("e", &test_schema)?;
        let col_f = &col("f", &test_schema)?;
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        // First element in the tuple stores vector of requirement, second element is the expected return value for ordering_satisfy function
        let requirements = vec![
            (
                vec![(col_a, Some(option_asc))],
                vec![(col_a, Some(option_asc))],
            ),
            (
                vec![(col_a, Some(option_desc))],
                vec![(col_a, Some(option_desc))],
            ),
            (vec![(col_a, None)], vec![(col_a, None)]),
            // Test whether equivalence works as expected
            (
                vec![(col_c, Some(option_asc))],
                vec![(col_a, Some(option_asc))],
            ),
            (vec![(col_c, None)], vec![(col_a, None)]),
            // Test whether ordering equivalence works as expected
            (
                vec![(col_d, Some(option_asc)), (col_b, Some(option_asc))],
                vec![(col_d, Some(option_asc)), (col_b, Some(option_asc))],
            ),
            (
                vec![(col_d, None), (col_b, None)],
                vec![(col_d, None), (col_b, None)],
            ),
            (
                vec![(col_e, Some(option_desc)), (col_f, Some(option_asc))],
                vec![(col_e, Some(option_desc)), (col_f, Some(option_asc))],
            ),
            // We should be able to normalize in compatible requirements also (not exactly equal)
            (
                vec![(col_e, Some(option_desc)), (col_f, None)],
                vec![(col_e, Some(option_desc)), (col_f, None)],
            ),
            (
                vec![(col_e, None), (col_f, None)],
                vec![(col_e, None), (col_f, None)],
            ),
        ];

        for (reqs, expected_normalized) in requirements.into_iter() {
            let req = convert_to_sort_reqs(&reqs);
            let expected_normalized = convert_to_sort_reqs(&expected_normalized);

            assert_eq!(
                eq_properties.normalize_sort_requirements(&req),
                expected_normalized
            );
        }

        Ok(())
    }

    #[test]
    fn test_get_finer() -> Result<()> {
        let schema = create_test_schema()?;
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let eq_properties = EquivalenceProperties::new(schema);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        // First entry, and second entry are the physical sort requirement that are argument for get_finer_requirement.
        // Third entry is the expected result.
        let tests_cases = vec![
            // Get finer requirement between [a Some(ASC)] and [a None, b Some(ASC)]
            // result should be [a Some(ASC), b Some(ASC)]
            (
                vec![(col_a, Some(option_asc))],
                vec![(col_a, None), (col_b, Some(option_asc))],
                Some(vec![(col_a, Some(option_asc)), (col_b, Some(option_asc))]),
            ),
            // Get finer requirement between [a Some(ASC), b Some(ASC), c Some(ASC)] and [a Some(ASC), b Some(ASC)]
            // result should be [a Some(ASC), b Some(ASC), c Some(ASC)]
            (
                vec![
                    (col_a, Some(option_asc)),
                    (col_b, Some(option_asc)),
                    (col_c, Some(option_asc)),
                ],
                vec![(col_a, Some(option_asc)), (col_b, Some(option_asc))],
                Some(vec![
                    (col_a, Some(option_asc)),
                    (col_b, Some(option_asc)),
                    (col_c, Some(option_asc)),
                ]),
            ),
            // Get finer requirement between [a Some(ASC), b Some(ASC)] and [a Some(ASC), b Some(DESC)]
            // result should be None
            (
                vec![(col_a, Some(option_asc)), (col_b, Some(option_asc))],
                vec![(col_a, Some(option_asc)), (col_b, Some(option_desc))],
                None,
            ),
        ];
        for (lhs, rhs, expected) in tests_cases {
            let lhs = convert_to_sort_reqs(&lhs);
            let rhs = convert_to_sort_reqs(&rhs);
            let expected = expected.map(|expected| convert_to_sort_reqs(&expected));
            let finer = eq_properties.get_finer_requirement(&lhs, &rhs);
            assert_eq!(finer, expected)
        }

        Ok(())
    }

    #[test]
    fn test_get_meet_ordering() -> Result<()> {
        let schema = create_test_schema()?;
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let eq_properties = EquivalenceProperties::new(schema);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        let tests_cases = vec![
            // Get meet ordering between [a ASC] and [a ASC, b ASC]
            // result should be [a ASC]
            (
                vec![(col_a, option_asc)],
                vec![(col_a, option_asc), (col_b, option_asc)],
                Some(vec![(col_a, option_asc)]),
            ),
            // Get meet ordering between [a ASC] and [a DESC]
            // result should be None.
            (vec![(col_a, option_asc)], vec![(col_a, option_desc)], None),
            // Get meet ordering between [a ASC, b ASC] and [a ASC, b DESC]
            // result should be [a ASC].
            (
                vec![(col_a, option_asc), (col_b, option_asc)],
                vec![(col_a, option_asc), (col_b, option_desc)],
                Some(vec![(col_a, option_asc)]),
            ),
        ];
        for (lhs, rhs, expected) in tests_cases {
            let lhs = convert_to_sort_exprs(&lhs);
            let rhs = convert_to_sort_exprs(&rhs);
            let expected = expected.map(|expected| convert_to_sort_exprs(&expected));
            let finer = eq_properties.get_meet_ordering(&lhs, &rhs);
            assert_eq!(finer, expected)
        }

        Ok(())
    }

    #[test]
    fn test_find_longest_permutation() -> Result<()> {
        // Schema satisfies following orderings:
        // [a ASC], [d ASC, b ASC], [e DESC, f ASC, g ASC]
        // and
        // Column [a=c] (e.g they are aliases).
        // At below we add [d ASC, h DESC] also, for test purposes
        let (test_schema, mut eq_properties) = create_test_params()?;
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;
        let col_e = &col("e", &test_schema)?;
        let col_h = &col("h", &test_schema)?;
        // a + d
        let a_plus_d = Arc::new(BinaryExpr::new(
            col_a.clone(),
            Operator::Plus,
            col_d.clone(),
        )) as Arc<dyn PhysicalExpr>;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        // [d ASC, h ASC] also satisfies schema.
        eq_properties.add_new_orderings([vec![
            PhysicalSortExpr {
                expr: col_d.clone(),
                options: option_asc,
            },
            PhysicalSortExpr {
                expr: col_h.clone(),
                options: option_desc,
            },
        ]]);
        let test_cases = vec![
            // TEST CASE 1
            (vec![col_a], vec![(col_a, option_asc)]),
            // TEST CASE 2
            (vec![col_c], vec![(col_c, option_asc)]),
            // TEST CASE 3
            (
                vec![col_d, col_e, col_b],
                vec![
                    (col_d, option_asc),
                    (col_e, option_desc),
                    (col_b, option_asc),
                ],
            ),
            // TEST CASE 4
            (vec![col_b], vec![]),
            // TEST CASE 5
            (vec![col_d], vec![(col_d, option_asc)]),
            // TEST CASE 5
            (vec![&a_plus_d], vec![(&a_plus_d, option_asc)]),
            // TEST CASE 6
            (
                vec![col_b, col_d],
                vec![(col_d, option_asc), (col_b, option_asc)],
            ),
            // TEST CASE 6
            (
                vec![col_c, col_e],
                vec![(col_c, option_asc), (col_e, option_desc)],
            ),
        ];
        for (exprs, expected) in test_cases {
            let exprs = exprs.into_iter().cloned().collect::<Vec<_>>();
            let expected = convert_to_sort_exprs(&expected);
            let (actual, _) = eq_properties.find_longest_permutation(&exprs);
            assert_eq!(actual, expected);
        }

        Ok(())
    }

    #[test]
    fn test_find_longest_permutation_random() -> Result<()> {
        const N_RANDOM_SCHEMA: usize = 100;
        const N_ELEMENTS: usize = 125;
        const N_DISTINCT: usize = 5;

        for seed in 0..N_RANDOM_SCHEMA {
            // Create a random schema with random properties
            let (test_schema, eq_properties) = create_random_schema(seed as u64)?;
            // Generate a data that satisfies properties given
            let table_data_with_properties =
                generate_table_for_eq_properties(&eq_properties, N_ELEMENTS, N_DISTINCT)?;

            let floor_a = create_physical_expr(
                &BuiltinScalarFunction::Floor,
                &[col("a", &test_schema)?],
                &test_schema,
                &ExecutionProps::default(),
            )?;
            let a_plus_b = Arc::new(BinaryExpr::new(
                col("a", &test_schema)?,
                Operator::Plus,
                col("b", &test_schema)?,
            )) as Arc<dyn PhysicalExpr>;
            let exprs = vec![
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
                        eq_properties.find_longest_permutation(&exprs);
                    // Make sure that find_longest_permutation return values are consistent
                    let ordering2 = indices
                        .iter()
                        .zip(ordering.iter())
                        .map(|(&idx, sort_expr)| PhysicalSortExpr {
                            expr: exprs[idx].clone(),
                            options: sort_expr.options,
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(
                        ordering, ordering2,
                        "indices and lexicographical ordering do not match"
                    );

                    let err_msg = format!(
                        "Error in test case ordering:{:?}, eq_properties.oeq_class: {:?}, eq_properties.eq_group: {:?}, eq_properties.constants: {:?}",
                        ordering, eq_properties.oeq_class, eq_properties.eq_group, eq_properties.constants
                    );
                    assert_eq!(ordering.len(), indices.len(), "{}", err_msg);
                    // Since ordered section satisfies schema, we expect
                    // that result will be same after sort (e.g sort was unnecessary).
                    assert!(
                        is_table_same_after_sort(
                            ordering.clone(),
                            table_data_with_properties.clone(),
                        )?,
                        "{}",
                        err_msg
                    );
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_update_ordering() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
        ]);

        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema.clone()));
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_d = &col("d", &schema)?;
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        // b=a (e.g they are aliases)
        eq_properties.add_equal_conditions(col_b, col_a);
        // [b ASC], [d ASC]
        eq_properties.add_new_orderings(vec![
            vec![PhysicalSortExpr {
                expr: col_b.clone(),
                options: option_asc,
            }],
            vec![PhysicalSortExpr {
                expr: col_d.clone(),
                options: option_asc,
            }],
        ]);

        let test_cases = vec![
            // d + b
            (
                Arc::new(BinaryExpr::new(
                    col_d.clone(),
                    Operator::Plus,
                    col_b.clone(),
                )) as Arc<dyn PhysicalExpr>,
                SortProperties::Ordered(option_asc),
            ),
            // b
            (col_b.clone(), SortProperties::Ordered(option_asc)),
            // a
            (col_a.clone(), SortProperties::Ordered(option_asc)),
            // a + c
            (
                Arc::new(BinaryExpr::new(
                    col_a.clone(),
                    Operator::Plus,
                    col_c.clone(),
                )),
                SortProperties::Unordered,
            ),
        ];
        for (expr, expected) in test_cases {
            let leading_orderings = eq_properties
                .oeq_class()
                .iter()
                .flat_map(|ordering| ordering.first().cloned())
                .collect::<Vec<_>>();
            let expr_ordering = eq_properties.get_expr_ordering(expr.clone());
            let err_msg = format!(
                "expr:{:?}, expected: {:?}, actual: {:?}, leading_orderings: {leading_orderings:?}",
                expr, expected, expr_ordering.state
            );
            assert_eq!(expr_ordering.state, expected, "{}", err_msg);
        }

        Ok(())
    }

    #[test]
    fn test_contains_any() {
        let lit_true = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
            as Arc<dyn PhysicalExpr>;
        let lit_false = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))
            as Arc<dyn PhysicalExpr>;
        let lit2 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))) as Arc<dyn PhysicalExpr>;
        let lit1 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
        let col_b_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;

        let cls1 = EquivalenceClass::new(vec![lit_true.clone(), lit_false.clone()]);
        let cls2 = EquivalenceClass::new(vec![lit_true.clone(), col_b_expr.clone()]);
        let cls3 = EquivalenceClass::new(vec![lit2.clone(), lit1.clone()]);

        // lit_true is common
        assert!(cls1.contains_any(&cls2));
        // there is no common entry
        assert!(!cls1.contains_any(&cls3));
        assert!(!cls2.contains_any(&cls3));
    }

    #[test]
    fn test_get_indices_of_matching_sort_exprs_with_order_eq() -> Result<()> {
        let sort_options = SortOptions::default();
        let sort_options_not = SortOptions::default().not();

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let required_columns = [col_b.clone(), col_a.clone()];
        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema));
        eq_properties.add_new_orderings([vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: sort_options_not,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: sort_options,
            },
        ]]);
        let (result, idxs) = eq_properties.find_longest_permutation(&required_columns);
        assert_eq!(idxs, vec![0, 1]);
        assert_eq!(
            result,
            vec![
                PhysicalSortExpr {
                    expr: col_b.clone(),
                    options: sort_options_not
                },
                PhysicalSortExpr {
                    expr: col_a.clone(),
                    options: sort_options
                }
            ]
        );

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let required_columns = [col_b.clone(), col_a.clone()];
        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema));
        eq_properties.add_new_orderings([
            vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options: sort_options,
            }],
            vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("b", 1)),
                    options: sort_options_not,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("a", 0)),
                    options: sort_options,
                },
            ],
        ]);
        let (result, idxs) = eq_properties.find_longest_permutation(&required_columns);
        assert_eq!(idxs, vec![0, 1]);
        assert_eq!(
            result,
            vec![
                PhysicalSortExpr {
                    expr: col_b.clone(),
                    options: sort_options_not
                },
                PhysicalSortExpr {
                    expr: col_a.clone(),
                    options: sort_options
                }
            ]
        );

        let required_columns = [
            Arc::new(Column::new("b", 1)) as _,
            Arc::new(Column::new("a", 0)) as _,
        ];
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);
        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema));

        // not satisfied orders
        eq_properties.add_new_orderings([vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: sort_options_not,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options: sort_options,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: sort_options,
            },
        ]]);
        let (_, idxs) = eq_properties.find_longest_permutation(&required_columns);
        assert_eq!(idxs, vec![0]);

        Ok(())
    }

    #[test]
    fn test_normalize_ordering_equivalence_classes() -> Result<()> {
        let sort_options = SortOptions::default();

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);
        let col_a_expr = col("a", &schema)?;
        let col_b_expr = col("b", &schema)?;
        let col_c_expr = col("c", &schema)?;
        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema.clone()));

        eq_properties.add_equal_conditions(&col_a_expr, &col_c_expr);
        let others = vec![
            vec![PhysicalSortExpr {
                expr: col_b_expr.clone(),
                options: sort_options,
            }],
            vec![PhysicalSortExpr {
                expr: col_c_expr.clone(),
                options: sort_options,
            }],
        ];
        eq_properties.add_new_orderings(others);

        let mut expected_eqs = EquivalenceProperties::new(Arc::new(schema));
        expected_eqs.add_new_orderings([
            vec![PhysicalSortExpr {
                expr: col_b_expr.clone(),
                options: sort_options,
            }],
            vec![PhysicalSortExpr {
                expr: col_c_expr.clone(),
                options: sort_options,
            }],
        ]);

        let oeq_class = eq_properties.oeq_class().clone();
        let expected = expected_eqs.oeq_class();
        assert!(oeq_class.eq(expected));

        Ok(())
    }

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
        let interval = Arc::new(Literal::new(ScalarValue::IntervalDayTime(Some(2))))
            as Arc<dyn PhysicalExpr>;
        let date_bin_func = &create_physical_expr(
            &BuiltinScalarFunction::DateBin,
            &[interval, col_ts.clone()],
            &schema,
            &ExecutionProps::default(),
        )?;
        let a_plus_b = Arc::new(BinaryExpr::new(
            col_a.clone(),
            Operator::Plus,
            col_b.clone(),
        )) as Arc<dyn PhysicalExpr>;
        let b_plus_d = Arc::new(BinaryExpr::new(
            col_b.clone(),
            Operator::Plus,
            col_d.clone(),
        )) as Arc<dyn PhysicalExpr>;
        let b_plus_e = Arc::new(BinaryExpr::new(
            col_b.clone(),
            Operator::Plus,
            col_e.clone(),
        )) as Arc<dyn PhysicalExpr>;
        let c_plus_d = Arc::new(BinaryExpr::new(
            col_c.clone(),
            Operator::Plus,
            col_d.clone(),
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
                    (date_bin_func, "date_bin_res".to_string()),
                ],
                // expected
                vec![
                    // [date_bin_res ASC]
                    vec![("date_bin_res", option_asc)],
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
                    (date_bin_func, "date_bin_res".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, ts_new ASC]
                    vec![("a_new", option_asc), ("ts_new", option_asc)],
                    // [a_new ASC, date_bin_res ASC]
                    vec![("a_new", option_asc), ("date_bin_res", option_asc)],
                    // [b_new ASC, ts_new ASC]
                    vec![("b_new", option_asc), ("ts_new", option_asc)],
                    // [b_new ASC, date_bin_res ASC]
                    vec![("b_new", option_asc), ("date_bin_res", option_asc)],
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
                    vec![(&a_plus_b, option_asc), (&col_c, option_asc)],
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
                        (&col_b, option_asc),
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
            let mut eq_properties = EquivalenceProperties::new(schema.clone());

            let orderings = convert_to_orderings(&orderings);
            eq_properties.add_new_orderings(orderings);

            let proj_exprs = proj_exprs
                .into_iter()
                .map(|(expr, name)| (expr.clone(), name))
                .collect::<Vec<_>>();
            let projection_mapping = ProjectionMapping::try_new(&proj_exprs, &schema)?;
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
            let expected = convert_to_orderings_owned(&expected);

            let projected_eq = eq_properties.project(&projection_mapping, output_schema);
            let orderings = projected_eq.oeq_class();

            let err_msg = format!(
                "test_idx: {:?}, actual: {:?}, expected: {:?}, projection_mapping: {:?}",
                idx, orderings.orderings, expected, projection_mapping
            );

            assert_eq!(orderings.len(), expected.len(), "{}", err_msg);
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
            col_a.clone(),
            Operator::Plus,
            col_b.clone(),
        )) as Arc<dyn PhysicalExpr>;
        let interval = Arc::new(Literal::new(ScalarValue::IntervalDayTime(Some(2))))
            as Arc<dyn PhysicalExpr>;
        let date_bin_ts = &create_physical_expr(
            &BuiltinScalarFunction::DateBin,
            &[interval, col_ts.clone()],
            &schema,
            &ExecutionProps::default(),
        )?;

        let round_c = &create_physical_expr(
            &BuiltinScalarFunction::Round,
            &[col_c.clone()],
            &schema,
            &ExecutionProps::default(),
        )?;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let proj_exprs = vec![
            (col_b, "b_new".to_string()),
            (col_a, "a_new".to_string()),
            (col_c, "c_new".to_string()),
            (date_bin_ts, "date_bin_res".to_string()),
            (round_c, "round_c_res".to_string()),
        ];
        let proj_exprs = proj_exprs
            .into_iter()
            .map(|(expr, name)| (expr.clone(), name))
            .collect::<Vec<_>>();
        let projection_mapping = ProjectionMapping::try_new(&proj_exprs, &schema)?;
        let output_schema = output_schema(&projection_mapping, &schema)?;

        let col_a_new = &col("a_new", &output_schema)?;
        let col_b_new = &col("b_new", &output_schema)?;
        let col_c_new = &col("c_new", &output_schema)?;
        let col_date_bin_res = &col("date_bin_res", &output_schema)?;
        let col_round_c_res = &col("round_c_res", &output_schema)?;
        let a_new_plus_b_new = Arc::new(BinaryExpr::new(
            col_a_new.clone(),
            Operator::Plus,
            col_b_new.clone(),
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
                    vec![(col_a_new, option_asc), (col_date_bin_res, option_asc)],
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
                    // Please note that result is not [a_new ASC, date_bin_res ASC, b_new ASC]
                    // because, datebin_res may not be 1-1 function. Hence without introducing ts
                    // dependency we cannot guarantee any ordering after date_bin_res column.
                    vec![(col_a_new, option_asc), (col_date_bin_res, option_asc)],
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
                        (&col_round_c_res, option_asc),
                    ],
                    // [a+b ASC, c_new ASC]
                    vec![(&a_new_plus_b_new, option_asc), (col_c_new, option_asc)],
                ],
            ),
        ];

        for (idx, (orderings, expected)) in test_cases.iter().enumerate() {
            let mut eq_properties = EquivalenceProperties::new(schema.clone());

            let orderings = convert_to_orderings(orderings);
            eq_properties.add_new_orderings(orderings);

            let expected = convert_to_orderings(expected);

            let projected_eq =
                eq_properties.project(&projection_mapping, output_schema.clone());
            let orderings = projected_eq.oeq_class();

            let err_msg = format!(
                "test idx: {:?}, actual: {:?}, expected: {:?}, projection_mapping: {:?}",
                idx, orderings.orderings, expected, projection_mapping
            );

            assert_eq!(orderings.len(), expected.len(), "{}", err_msg);
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
            col_a.clone(),
            Operator::Plus,
            col_b.clone(),
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
            .map(|(expr, name)| (expr.clone(), name))
            .collect::<Vec<_>>();
        let projection_mapping = ProjectionMapping::try_new(&proj_exprs, &schema)?;
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
            let mut eq_properties = EquivalenceProperties::new(schema.clone());
            for (lhs, rhs) in equal_columns {
                eq_properties.add_equal_conditions(lhs, rhs);
            }

            let orderings = convert_to_orderings(&orderings);
            eq_properties.add_new_orderings(orderings);

            let expected = convert_to_orderings(&expected);

            let projected_eq =
                eq_properties.project(&projection_mapping, output_schema.clone());
            let orderings = projected_eq.oeq_class();

            let err_msg = format!(
                "actual: {:?}, expected: {:?}, projection_mapping: {:?}",
                orderings.orderings, expected, projection_mapping
            );

            assert_eq!(orderings.len(), expected.len(), "{}", err_msg);
            for expected_ordering in &expected {
                assert!(orderings.contains(expected_ordering), "{}", err_msg)
            }
        }

        Ok(())
    }

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
            let floor_a = create_physical_expr(
                &BuiltinScalarFunction::Floor,
                &[col("a", &test_schema)?],
                &test_schema,
                &ExecutionProps::default(),
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
                        .map(|(expr, name)| (expr.clone(), name.to_string()))
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
            let floor_a = create_physical_expr(
                &BuiltinScalarFunction::Floor,
                &[col("a", &test_schema)?],
                &test_schema,
                &ExecutionProps::default(),
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
                        .map(|(expr, name)| (expr.clone(), name.to_string()))
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
                        .map(|(_source, target)| target.clone())
                        .collect::<Vec<_>>();

                    for n_req in 0..=projected_exprs.len() {
                        for exprs in projected_exprs.iter().combinations(n_req) {
                            let requirement = exprs
                                .into_iter()
                                .map(|expr| PhysicalSortExpr {
                                    expr: expr.clone(),
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

    #[test]
    fn test_expr_consists_of_constants() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;
        let col_d = col("d", &schema)?;
        let b_plus_d = Arc::new(BinaryExpr::new(
            col_b.clone(),
            Operator::Plus,
            col_d.clone(),
        )) as Arc<dyn PhysicalExpr>;

        let constants = vec![col_a.clone(), col_b.clone()];
        let expr = b_plus_d.clone();
        assert!(!is_constant_recurse(&constants, &expr));

        let constants = vec![col_a.clone(), col_b.clone(), col_d.clone()];
        let expr = b_plus_d.clone();
        assert!(is_constant_recurse(&constants, &expr));
        Ok(())
    }

    #[test]
    fn test_join_equivalence_properties() -> Result<()> {
        let schema = create_test_schema()?;
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let offset = schema.fields.len();
        let col_a2 = &add_offset_to_expr(col_a.clone(), offset);
        let col_b2 = &add_offset_to_expr(col_b.clone(), offset);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let test_cases = vec![
            // ------- TEST CASE 1 --------
            // [a ASC], [b ASC]
            (
                // [a ASC], [b ASC]
                vec![vec![(col_a, option_asc)], vec![(col_b, option_asc)]],
                // [a ASC], [b ASC]
                vec![vec![(col_a, option_asc)], vec![(col_b, option_asc)]],
                // expected [a ASC, a2 ASC], [a ASC, b2 ASC], [b ASC, a2 ASC], [b ASC, b2 ASC]
                vec![
                    vec![(col_a, option_asc), (col_a2, option_asc)],
                    vec![(col_a, option_asc), (col_b2, option_asc)],
                    vec![(col_b, option_asc), (col_a2, option_asc)],
                    vec![(col_b, option_asc), (col_b2, option_asc)],
                ],
            ),
            // ------- TEST CASE 2 --------
            // [a ASC], [b ASC]
            (
                // [a ASC], [b ASC], [c ASC]
                vec![
                    vec![(col_a, option_asc)],
                    vec![(col_b, option_asc)],
                    vec![(col_c, option_asc)],
                ],
                // [a ASC], [b ASC]
                vec![vec![(col_a, option_asc)], vec![(col_b, option_asc)]],
                // expected [a ASC, a2 ASC], [a ASC, b2 ASC], [b ASC, a2 ASC], [b ASC, b2 ASC], [c ASC, a2 ASC], [c ASC, b2 ASC]
                vec![
                    vec![(col_a, option_asc), (col_a2, option_asc)],
                    vec![(col_a, option_asc), (col_b2, option_asc)],
                    vec![(col_b, option_asc), (col_a2, option_asc)],
                    vec![(col_b, option_asc), (col_b2, option_asc)],
                    vec![(col_c, option_asc), (col_a2, option_asc)],
                    vec![(col_c, option_asc), (col_b2, option_asc)],
                ],
            ),
        ];
        for (left_orderings, right_orderings, expected) in test_cases {
            let mut left_eq_properties = EquivalenceProperties::new(schema.clone());
            let mut right_eq_properties = EquivalenceProperties::new(schema.clone());
            let left_orderings = convert_to_orderings(&left_orderings);
            let right_orderings = convert_to_orderings(&right_orderings);
            let expected = convert_to_orderings(&expected);
            left_eq_properties.add_new_orderings(left_orderings);
            right_eq_properties.add_new_orderings(right_orderings);
            let join_eq = join_equivalence_properties(
                left_eq_properties,
                right_eq_properties,
                &JoinType::Inner,
                Arc::new(Schema::empty()),
                &[true, false],
                Some(JoinSide::Left),
                &[],
            );
            let orderings = &join_eq.oeq_class.orderings;
            let err_msg = format!("expected: {:?}, actual:{:?}", expected, orderings);
            assert_eq!(
                join_eq.oeq_class.orderings.len(),
                expected.len(),
                "{}",
                err_msg
            );
            for ordering in orderings {
                assert!(
                    expected.contains(ordering),
                    "{}, ordering: {:?}",
                    err_msg,
                    ordering
                );
            }
        }
        Ok(())
    }
}
