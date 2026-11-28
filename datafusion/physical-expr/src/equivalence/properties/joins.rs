// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::EquivalenceProperties;
use crate::{PhysicalExprRef, equivalence::OrderingEquivalenceClass};

use arrow::datatypes::SchemaRef;
use datafusion_common::{JoinSide, JoinType, Result};

/// Calculate ordering equivalence properties for the given join operation.
pub fn join_equivalence_properties(
    left: EquivalenceProperties,
    right: EquivalenceProperties,
    join_type: &JoinType,
    join_schema: SchemaRef,
    maintains_input_order: &[bool],
    _probe_side: Option<JoinSide>,
    on: &[(PhysicalExprRef, PhysicalExprRef)],
) -> Result<EquivalenceProperties> {
    let left_size = left.schema.fields.len();
    let functional_dependencies = left.functional_dependencies.join(
        &right.functional_dependencies,
        join_type,
        left_size,
    );
    let mut result = EquivalenceProperties::new(join_schema);
    result.functional_dependencies = functional_dependencies;
    result.add_equivalence_group(left.eq_group().join(
        right.eq_group(),
        join_type,
        left_size,
        on,
    )?)?;

    let EquivalenceProperties {
        oeq_class: left_oeq_class,
        ..
    } = left;
    let EquivalenceProperties {
        oeq_class: mut right_oeq_class,
        ..
    } = right;
    match maintains_input_order {
        // Only the maintained side's ordering is guaranteed. With duplicate
        // probe keys, the other side's matches repeat for each probe row, so
        // its ordering cannot be appended as a suffix.
        [true, false] => result.add_orderings(left_oeq_class),
        [false, true] => {
            updated_right_ordering_equivalence_class(
                &mut right_oeq_class,
                join_type,
                left_size,
            )?;
            result.add_orderings(right_oeq_class);
        }
        [false, false] => {}
        [true, true] => unreachable!("Cannot maintain ordering of both sides"),
        _ => unreachable!("Join operators can not have more than two children"),
    }
    Ok(result)
}

/// In the context of a join, update the right side `OrderingEquivalenceClass`
/// so that they point to valid indices in the join output schema.
///
/// To do so, we increment column indices by the size of the left table when
/// join schema consists of a combination of the left and right schemas. This
/// is the case for `Inner`, `Left`, `Full` and `Right` joins. For other cases,
/// indices do not change.
pub fn updated_right_ordering_equivalence_class(
    right_oeq_class: &mut OrderingEquivalenceClass,
    join_type: &JoinType,
    left_size: usize,
) -> Result<()> {
    if matches!(
        join_type,
        JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right
    ) {
        right_oeq_class.add_offset(left_size as _)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::PhysicalSortExpr;
    use crate::equivalence::tests::create_test_schema;
    use crate::equivalence::{ProjectionMapping, convert_to_orderings};
    use crate::expressions::col;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use datafusion_common::{Constraint, Constraints};

    #[test]
    fn test_join_equivalence_properties() -> Result<()> {
        let schema = create_test_schema()?;
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
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
                // Only the left input's orderings are preserved.
                vec![vec![(col_a, option_asc)], vec![(col_b, option_asc)]],
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
                // Only the left input's orderings are preserved.
                vec![
                    vec![(col_a, option_asc)],
                    vec![(col_b, option_asc)],
                    vec![(col_c, option_asc)],
                ],
            ),
        ];
        for (left_orderings, right_orderings, expected) in test_cases {
            let mut left_eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
            let mut right_eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
            let left_orderings = convert_to_orderings(&left_orderings);
            let right_orderings = convert_to_orderings(&right_orderings);
            let expected = convert_to_orderings(&expected);
            left_eq_properties.add_orderings(left_orderings);
            right_eq_properties.add_orderings(right_orderings);
            let join_eq = join_equivalence_properties(
                left_eq_properties,
                right_eq_properties,
                &JoinType::Inner,
                Arc::new(Schema::empty()),
                &[true, false],
                Some(JoinSide::Left),
                &[],
            )?;
            let err_msg =
                format!("expected: {:?}, actual:{:?}", expected, join_eq.oeq_class);
            assert_eq!(join_eq.oeq_class.len(), expected.len(), "{err_msg}");
            for ordering in join_eq.oeq_class {
                assert!(
                    expected.contains(&ordering),
                    "{err_msg}, ordering: {ordering:?}"
                );
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
        let mut right_oeq_class = OrderingEquivalenceClass::from(orderings);

        let left_columns_len = 4;

        let fields: Fields = ["a", "b", "c", "d", "x", "y", "z", "w"]
            .into_iter()
            .map(|name| Field::new(name, DataType::Int32, true))
            .collect();

        // Join Schema
        let schema = Schema::new(fields);
        let col_a = col("a", &schema)?;
        let col_d = col("d", &schema)?;
        let col_x = col("x", &schema)?;
        let col_y = col("y", &schema)?;
        let col_z = col("z", &schema)?;
        let col_w = col("w", &schema)?;

        let mut join_eq_properties = EquivalenceProperties::new(Arc::new(schema));
        // a=x and d=w
        join_eq_properties.add_equal_conditions(col_a, Arc::clone(&col_x))?;
        join_eq_properties.add_equal_conditions(col_d, Arc::clone(&col_w))?;

        updated_right_ordering_equivalence_class(
            &mut right_oeq_class,
            &join_type,
            left_columns_len,
        )?;
        join_eq_properties.add_orderings(right_oeq_class);
        let result = join_eq_properties.oeq_class().clone();

        // [x ASC, y ASC], [z ASC, w ASC]
        let orderings = vec![
            vec![(col_x, option_asc), (col_y, option_asc)],
            vec![(col_z, option_asc), (col_w, option_asc)],
        ];
        let orderings = convert_to_orderings(&orderings);
        let expected = OrderingEquivalenceClass::from(orderings);

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn unique_build_key_determines_join_payload_after_projection() -> Result<()> {
        let build_schema = Arc::new(Schema::new(vec![
            Field::new("build_k", DataType::Int32, false),
            Field::new("build_v", DataType::Int32, true),
        ]));
        let probe_schema = Arc::new(Schema::new(vec![Field::new(
            "probe_k",
            DataType::Int32,
            true,
        )]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("build_k", DataType::Int32, false),
            Field::new("build_v", DataType::Int32, true),
            Field::new("probe_k", DataType::Int32, true),
        ]));

        let build_k = col("build_k", &build_schema)?;
        let probe_k = col("probe_k", &probe_schema)?;
        let mut build_properties = EquivalenceProperties::new(Arc::clone(&build_schema));
        build_properties.set_constraints(Constraints::new_unverified(vec![
            Constraint::PrimaryKey(vec![0]),
        ]));
        let mut probe_properties = EquivalenceProperties::new(Arc::clone(&probe_schema));
        probe_properties
            .add_ordering([PhysicalSortExpr::new_default(Arc::clone(&probe_k))]);

        let join_properties = join_equivalence_properties(
            build_properties,
            probe_properties,
            &JoinType::Inner,
            Arc::clone(&output_schema),
            &[false, true],
            Some(JoinSide::Right),
            &[(build_k, probe_k)],
        )?;

        let output_probe_k = col("probe_k", &output_schema)?;
        let output_build_v = col("build_v", &output_schema)?;
        let required = [
            PhysicalSortExpr::new_default(output_probe_k),
            PhysicalSortExpr::new_default(output_build_v),
        ];
        assert!(join_properties.ordering_satisfy(required.clone())?);

        let projection_mapping =
            ProjectionMapping::from_indices(&[2, 1], &output_schema)?;
        let projected_schema = Arc::new(Schema::new(vec![
            Field::new("probe_k", DataType::Int32, true),
            Field::new("build_v", DataType::Int32, true),
        ]));
        let projected_properties =
            join_properties.project(&projection_mapping, Arc::clone(&projected_schema));
        let projected_required = [
            PhysicalSortExpr::new_default(col("probe_k", &projected_schema)?),
            PhysicalSortExpr::new_default(col("build_v", &projected_schema)?),
        ];
        assert!(projected_properties.ordering_satisfy(projected_required)?);

        Ok(())
    }
}
