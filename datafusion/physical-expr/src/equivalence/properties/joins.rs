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

use std::sync::Arc;

use super::EquivalenceProperties;
use crate::expressions::Column;
use crate::{PhysicalExprRef, equivalence::OrderingEquivalenceClass};

use arrow::datatypes::SchemaRef;
use datafusion_common::{Constraint, JoinSide, JoinType, Result};

/// Calculate ordering equivalence properties for the given join operation.
#[expect(clippy::too_many_arguments)]
pub fn join_equivalence_properties(
    left: EquivalenceProperties,
    right: EquivalenceProperties,
    join_type: &JoinType,
    join_schema: SchemaRef,
    maintains_input_order: &[bool],
    probe_side: Option<JoinSide>,
    on: &[(PhysicalExprRef, PhysicalExprRef)],
    has_filter: bool,
) -> Result<EquivalenceProperties> {
    let left_size = left.schema.fields.len();
    let mut result = EquivalenceProperties::new(join_schema);
    result.add_equivalence_group(left.eq_group().join(
        right.eq_group(),
        join_type,
        left_size,
        on,
    )?)?;

    // Without the uniqueness proof, only the maintained side's ordering is
    // guaranteed: duplicate probe keys can restart the other side's matches.
    match maintains_input_order {
        [true, false] => {
            if probe_side == Some(JoinSide::Left)
                && (*join_type == JoinType::Inner
                    || (*join_type == JoinType::Left && !has_filter))
            {
                result.add_orderings(unique_build_join_orderings(
                    &left,
                    &right,
                    on,
                    JoinSide::Left,
                    *join_type != JoinType::Inner,
                )?);
            }
            result.add_orderings(left.oeq_class);
        }
        [false, true] => {
            if probe_side == Some(JoinSide::Right)
                && (*join_type == JoinType::Inner
                    || (*join_type == JoinType::Right && !has_filter))
            {
                result.add_orderings(unique_build_join_orderings(
                    &right,
                    &left,
                    on,
                    JoinSide::Right,
                    *join_type != JoinType::Inner,
                )?);
            }
            let mut right_oeq_class = right.oeq_class;
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

/// Append build orderings only when equal probe ordering values identify at most
/// one build row. The suffix is then constant within each probe ordering group,
/// even if probe rows repeat. For an outer join preserving the probe side, all
/// join keys must be fixed within the group, and the caller must rule out filters,
/// so the group cannot mix matched build rows with NULL-extended rows.
fn unique_build_join_orderings(
    probe: &EquivalenceProperties,
    build: &EquivalenceProperties,
    on: &[(PhysicalExprRef, PhysicalExprRef)],
    probe_side: JoinSide,
    preserves_unmatched_probe: bool,
) -> Result<OrderingEquivalenceClass> {
    if build.constraints().is_empty() || build.oeq_class().is_empty() {
        return Ok(OrderingEquivalenceClass::default());
    }
    let on = on
        .iter()
        .map(|(left, right)| {
            let (probe_key, build_key) = match probe_side {
                JoinSide::Left => (left, right),
                JoinSide::Right => (right, left),
                JoinSide::None => unreachable!(),
            };
            (
                probe.eq_group().normalize_expr(Arc::clone(probe_key)),
                build.eq_group().normalize_expr(Arc::clone(build_key)),
            )
        })
        .collect::<Vec<_>>();
    let mut valid_orderings = Vec::new();
    for ordering in probe.oeq_class().iter() {
        let probe_exprs = ordering
            .iter()
            .map(|sort| probe.eq_group().normalize_expr(Arc::clone(&sort.expr)))
            .collect::<Vec<_>>();

        // Outer joins must have the same match status throughout the group.
        if preserves_unmatched_probe
            && !on
                .iter()
                .all(|(probe_key, _)| probe_exprs.contains(probe_key))
        {
            continue;
        }
        if !ordering_covers_unique_build_key(build, &on, &probe_exprs) {
            continue;
        }
        valid_orderings.push(ordering.clone());
    }
    let mut probe_orderings = OrderingEquivalenceClass::new(valid_orderings);
    if probe_orderings.is_empty() {
        return Ok(probe_orderings);
    }
    let mut build_orderings = build.oeq_class().clone();
    match probe_side {
        JoinSide::Left => build_orderings.add_offset(probe.schema.fields().len() as _)?,
        JoinSide::Right => probe_orderings.add_offset(build.schema.fields().len() as _)?,
        JoinSide::None => unreachable!(),
    }
    Ok(probe_orderings.join_suffix(&build_orderings))
}

/// Check whether the probe ordering determines a unique build key.
/// Join keys and probe ordering expressions must already be normalized.
fn ordering_covers_unique_build_key(
    build: &EquivalenceProperties,
    on: &[(PhysicalExprRef, PhysicalExprRef)],
    probe_exprs: &[PhysicalExprRef],
) -> bool {
    build.constraints().iter().any(|constraint| {
        let (Constraint::PrimaryKey(indices) | Constraint::Unique(indices)) = constraint;
        !indices.is_empty()
            && indices.iter().all(|&index| {
                let Some(field) = build.schema.fields().get(index) else {
                    return false;
                };
                // UNIQUE can contain repeated NULLs. Without null-equality
                // information, only non-null UNIQUE columns prove uniqueness.
                if matches!(constraint, Constraint::Unique(_)) && field.is_nullable() {
                    return false;
                }
                let column: PhysicalExprRef = Arc::new(Column::new(field.name(), index));
                let column = build.eq_group().normalize_expr(column);
                on.iter().any(|(probe_key, build_key)| {
                    build_key.eq(&column) && probe_exprs.contains(probe_key)
                })
            })
    })
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
    use crate::equivalence::convert_to_orderings;
    use crate::equivalence::tests::create_test_schema;
    use crate::expressions::{Column, col};
    use crate::{LexOrdering, PhysicalSortExpr};
    use datafusion_common::{Constraint, Constraints};

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Fields, Schema};

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
                false,
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
    fn test_unique_build_join_checks_each_ordering() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = col("a", &schema)?;
        let b = col("b", &schema)?;
        let ordering =
            |expr| LexOrdering::new([PhysicalSortExpr::new_default(expr)]).unwrap();
        let probe = EquivalenceProperties::new_with_orderings(
            Arc::clone(&schema),
            [ordering(Arc::clone(&a)), ordering(Arc::clone(&b))],
        );
        let build = EquivalenceProperties::new_with_orderings(
            Arc::clone(&schema),
            [ordering(Arc::clone(&b))],
        )
        .with_constraints(Constraints::new_unverified(vec![
            Constraint::PrimaryKey(vec![0]),
        ]));
        let output_schema = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .chain(schema.fields())
                .cloned()
                .collect::<Vec<_>>(),
        ));
        let join_properties = |join_type, on: &[(PhysicalExprRef, PhysicalExprRef)]| {
            join_equivalence_properties(
                probe.clone(),
                build.clone(),
                &join_type,
                Arc::clone(&output_schema),
                &[true, false],
                Some(JoinSide::Left),
                on,
                false,
            )
        };
        let required = |prefix| {
            LexOrdering::new([
                PhysicalSortExpr::new_default(prefix),
                PhysicalSortExpr::new_default(Arc::new(Column::new("b", 3))),
            ])
            .unwrap()
        };
        let mut on = vec![(Arc::clone(&a), Arc::clone(&a))];
        for join_type in [JoinType::Inner, JoinType::Left] {
            let output = join_properties(join_type, &on)?;
            assert!(output.ordering_satisfy(required(Arc::clone(&a)))?);
            assert!(!output.ordering_satisfy(required(Arc::clone(&b)))?);
        }

        // An additional varying join key can mix matched and NULL-extended rows
        // in an outer join. In an inner join, it only suppresses rows.
        on.push((Arc::clone(&b), Arc::clone(&b)));
        let inner = join_properties(JoinType::Inner, &on)?;
        let outer = join_properties(JoinType::Left, &on)?;
        assert!(inner.ordering_satisfy(required(Arc::clone(&a)))?);
        assert!(!outer.ordering_satisfy(required(a))?);
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
}
