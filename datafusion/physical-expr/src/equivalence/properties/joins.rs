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
use crate::{
    ConstExpr, LexOrdering, PhysicalExprRef, equivalence::OrderingEquivalenceClass,
};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion_common::{Constraint, JoinSide, JoinType, NullEquality, Result};
use datafusion_physical_expr_common::physical_expr::is_volatile;

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
    null_equality: NullEquality,
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
                && matches!(join_type, JoinType::Inner | JoinType::Left)
            {
                result.add_orderings(join_orderings_with_suffix(
                    &left,
                    &right,
                    on,
                    JoinSide::Left,
                    *join_type != JoinType::Inner,
                    has_filter,
                    null_equality,
                )?);
            }
            result.add_orderings(left.oeq_class);
        }
        [false, true] => {
            if probe_side == Some(JoinSide::Right)
                && matches!(join_type, JoinType::Inner | JoinType::Right)
            {
                result.add_orderings(join_orderings_with_suffix(
                    &right,
                    &left,
                    on,
                    JoinSide::Right,
                    *join_type != JoinType::Inner,
                    has_filter,
                    null_equality,
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

/// Append build orderings to each probe ordering that permits a build suffix.
fn join_orderings_with_suffix(
    probe: &EquivalenceProperties,
    build: &EquivalenceProperties,
    on: &[(PhysicalExprRef, PhysicalExprRef)],
    probe_side: JoinSide,
    preserves_unmatched_probe: bool,
    has_filter: bool,
    null_equality: NullEquality,
) -> Result<OrderingEquivalenceClass> {
    if (probe.constraints().is_empty() && build.constraints().is_empty())
        || build.oeq_class().is_empty()
    {
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
            (Arc::clone(probe_key), Arc::clone(build_key))
        })
        .collect::<Vec<_>>();
    let mut build_orderings = build.oeq_class().clone();
    if probe_side == JoinSide::Left {
        build_orderings.add_offset(probe.schema.fields().len() as _)?;
    }
    let mut result = OrderingEquivalenceClass::default();
    let candidates =
        probe_ordering_candidates(probe, build, &on, preserves_unmatched_probe)?;
    for ordering in candidates {
        if !can_append_build_ordering(
            &ordering,
            probe,
            build,
            &on,
            preserves_unmatched_probe,
            has_filter,
            null_equality,
        ) {
            continue;
        }
        // Append before removing redundant prefixes: [a] and [a, b] may both
        // be valid, but [a, suffix] is not implied by [a, b, suffix].
        let mut prefix = OrderingEquivalenceClass::new([ordering]);
        if probe_side == JoinSide::Right {
            prefix.add_offset(build.schema.fields().len() as _)?;
        }
        result.extend(prefix.join_suffix(&build_orderings));
    }
    Ok(result)
}

/// Collect probe ordering prefixes to check before appending build orderings.
/// Keep the original orderings, their combined ordering, and combinations selected
/// by each unique constraint. The caller must prove the suffix for each candidate.
fn probe_ordering_candidates(
    probe: &EquivalenceProperties,
    build: &EquivalenceProperties,
    on: &[(PhysicalExprRef, PhysicalExprRef)],
    preserves_unmatched_probe: bool,
) -> Result<Vec<LexOrdering>> {
    let mut orderings = probe.oeq_class().iter().cloned().collect::<Vec<_>>();
    if orderings.len() > 1 {
        orderings.extend(probe.oeq_class().output_ordering());
    }

    // For a unique probe row, look for an ordering of its constrained columns.
    for constraint in probe.constraints().iter() {
        let keys = constraint_columns(constraint, &probe.schema);
        let (ordering, _) = probe.find_longest_permutation(&keys)?;
        orderings.extend(LexOrdering::new(ordering));
    }

    // For a unique build match, map the constrained columns to probe join keys.
    // This can select [a, b] from [extra], [a], [b] without the unrelated extra.
    for constraint in build.constraints().iter() {
        let mut keys = vec![];
        for column in constraint_columns(constraint, &build.schema) {
            let column = build.eq_group().normalize_expr(column);
            for (probe_key, build_key) in on {
                let build_key = build.eq_group().normalize_expr(Arc::clone(build_key));
                if build_key.eq(&column) {
                    keys.push(Arc::clone(probe_key));
                }
            }
        }
        // Outer joins also need the additional keys to have a fixed match status.
        if preserves_unmatched_probe {
            keys.extend(on.iter().map(|(key, _)| Arc::clone(key)));
        }
        let (ordering, _) = probe.find_longest_permutation(&keys)?;
        orderings.extend(LexOrdering::new(ordering));
    }
    Ok(orderings)
}

fn constraint_columns(constraint: &Constraint, schema: &Schema) -> Vec<PhysicalExprRef> {
    let (Constraint::PrimaryKey(indices) | Constraint::Unique(indices)) = constraint;
    indices
        .iter()
        .filter_map(|&index| {
            let field = schema.fields().get(index)?;
            Some(Arc::new(Column::new(field.name(), index)) as PhysicalExprRef)
        })
        .collect()
}

/// A build suffix is ordered within each group of equal probe ordering values if:
///
/// - the group contains at most one probe row, or only unmatched NULL keys; or
/// - the group can match at most one build row, so the suffix is constant.
///
/// The latter also requires a constant match status for outer joins.
fn can_append_build_ordering(
    ordering: &LexOrdering,
    probe: &EquivalenceProperties,
    build: &EquivalenceProperties,
    on: &[(PhysicalExprRef, PhysicalExprRef)],
    preserves_unmatched_probe: bool,
    has_filter: bool,
    null_equality: NullEquality,
) -> bool {
    // Keep the assumption of equal ordering values local to this proof.
    let mut group = probe.eq_group().clone();
    for sort in ordering {
        let expr = probe.eq_group().normalize_expr(Arc::clone(&sort.expr));
        group.add_constant(ConstExpr::from(expr));
    }
    let probe_key_is_fixed = |key: &PhysicalExprRef| {
        // Normalization can hide a volatile function behind an equivalent
        // column. Check the original expression before using equivalences.
        if is_volatile(key) {
            return false;
        }
        let key = probe.eq_group().normalize_expr(Arc::clone(key));
        !is_volatile(&key) && group.is_expr_constant(&key).is_some()
    };

    if ordering_covers_unique_probe_key(
        probe,
        &build.schema,
        on,
        probe_key_is_fixed,
        null_equality,
    ) {
        return true;
    }
    // Repeated probe rows in an outer join must have the same match status.
    if preserves_unmatched_probe
        && (has_filter || !on.iter().all(|(key, _)| probe_key_is_fixed(key)))
    {
        return false;
    }
    ordering_covers_unique_build_key(
        &probe.schema,
        build,
        on,
        probe_key_is_fixed,
        null_equality,
    )
}

/// A nullable UNIQUE key permits duplicate NULLs. It is sufficient only when
/// those NULLs cannot match: their group then emits no rows or only NULL-extended
/// build columns, while a non-NULL key identifies at most one probe row.
fn ordering_covers_unique_probe_key(
    probe: &EquivalenceProperties,
    build_schema: &Schema,
    on: &[(PhysicalExprRef, PhysicalExprRef)],
    probe_key_is_fixed: impl Fn(&PhysicalExprRef) -> bool,
    null_equality: NullEquality,
) -> bool {
    probe.constraints().iter().any(|constraint| {
        let (Constraint::PrimaryKey(indices) | Constraint::Unique(indices)) = constraint;
        !indices.is_empty()
            && indices.iter().all(|&index| {
                let Some(field) = probe.schema.fields().get(index) else {
                    return false;
                };
                let column: PhysicalExprRef = Arc::new(Column::new(field.name(), index));
                if !probe_key_is_fixed(&column) {
                    return false;
                }
                if !matches!(constraint, Constraint::Unique(_)) || !field.is_nullable() {
                    return true;
                }
                let column = probe.eq_group().normalize_expr(column);
                on.iter().any(|(probe_key, build_key)| {
                    !is_volatile(probe_key)
                        && probe
                            .eq_group()
                            .normalize_expr(Arc::clone(probe_key))
                            .eq(&column)
                        && (null_equality == NullEquality::NullEqualsNothing
                            || !build_key.nullable(build_schema).unwrap_or(true))
                })
            })
    })
}

/// Check whether the probe ordering determines a unique build key.
/// Join keys retain their original volatility and nullability.
fn ordering_covers_unique_build_key(
    probe_schema: &Schema,
    build: &EquivalenceProperties,
    on: &[(PhysicalExprRef, PhysicalExprRef)],
    probe_key_is_fixed: impl Fn(&PhysicalExprRef) -> bool,
    null_equality: NullEquality,
) -> bool {
    build.constraints().iter().any(|constraint| {
        let (Constraint::PrimaryKey(indices) | Constraint::Unique(indices)) = constraint;
        !indices.is_empty()
            && indices.iter().all(|&index| {
                let Some(field) = build.schema.fields().get(index) else {
                    return false;
                };
                // Repeated NULLs in a UNIQUE key are safe only if they cannot
                // match the probe key.
                let requires_non_null_probe = matches!(constraint, Constraint::Unique(_))
                    && field.is_nullable()
                    && null_equality == NullEquality::NullEqualsNull;
                let column: PhysicalExprRef = Arc::new(Column::new(field.name(), index));
                let column = build.eq_group().normalize_expr(column);
                on.iter().any(|(probe_key, build_key)| {
                    // Re-evaluating a volatile key need not preserve uniqueness,
                    // even if a filter equated it with the constrained column.
                    if is_volatile(build_key) {
                        return false;
                    }
                    let build_key =
                        build.eq_group().normalize_expr(Arc::clone(build_key));
                    build_key.eq(&column)
                        && probe_key_is_fixed(probe_key)
                        && (!requires_non_null_probe
                            || !probe_key.nullable(probe_schema).unwrap_or(true))
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
    use crate::expressions::{BinaryExpr, Column, col, lit};
    use crate::{LexOrdering, PhysicalSortExpr};
    use datafusion_common::{Constraint, Constraints};
    use datafusion_expr::Operator;

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
                NullEquality::NullEqualsNothing,
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
                NullEquality::NullEqualsNothing,
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
        assert!(!outer.ordering_satisfy(required(Arc::clone(&a)))?);

        let plus_one = |expr| -> PhysicalExprRef {
            Arc::new(BinaryExpr::new(expr, Operator::Plus, lit(1_i32)))
        };
        let a_plus_one = plus_one(Arc::clone(&a));
        let b_plus_one = plus_one(Arc::clone(&b));
        // Record the ordering before adding equivalence, so the proof must
        // normalize both the ordering expression and the join key consistently.
        let mut probe = EquivalenceProperties::new_with_orderings(
            Arc::clone(&schema),
            [ordering(Arc::clone(&b_plus_one))],
        );
        probe.add_equal_conditions(Arc::clone(&a), b)?;
        for join_type in [JoinType::Inner, JoinType::Left] {
            let output = join_equivalence_properties(
                probe.clone(),
                build.clone(),
                &join_type,
                Arc::clone(&output_schema),
                &[true, false],
                Some(JoinSide::Left),
                &[(Arc::clone(&a_plus_one), Arc::clone(&a))],
                false,
                NullEquality::NullEqualsNothing,
            )?;
            assert!(output.ordering_satisfy(required(Arc::clone(&b_plus_one)))?);
        }
        Ok(())
    }

    #[test]
    fn test_join_suffix_with_unique_probe() -> Result<()> {
        use Constraint::{PrimaryKey, Unique};
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let sort = |name| PhysicalSortExpr::new_default(col(name, &schema).unwrap());
        let build = EquivalenceProperties::new_with_orderings(
            Arc::clone(&schema),
            [vec![sort("v")]],
        );
        let output_schema = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .chain(schema.fields())
                .cloned()
                .collect::<Vec<_>>(),
        ));
        let ordinary = NullEquality::NullEqualsNothing;
        let null_safe = NullEquality::NullEqualsNull;
        for (constraint, prefix, on, null_equality, expected) in [
            (PrimaryKey(vec![0]), vec!["a"], ("a", "a"), null_safe, true),
            (Unique(vec![0]), vec!["a"], ("a", "a"), null_safe, true),
            (Unique(vec![1]), vec!["b"], ("b", "b"), null_safe, false),
            (
                PrimaryKey(vec![0, 1]),
                vec!["a"],
                ("a", "a"),
                null_safe,
                false,
            ),
            // Duplicate NULLs are safe only when the join cannot match them.
            (Unique(vec![1]), vec!["b"], ("b", "b"), ordinary, true),
            (Unique(vec![1]), vec!["b"], ("a", "a"), ordinary, false),
            (Unique(vec![1]), vec!["b"], ("b", "a"), null_safe, true),
            // The join excludes NULLs in b, but not in the other UNIQUE column.
            (
                Unique(vec![1, 2]),
                vec!["b", "v"],
                ("b", "b"),
                ordinary,
                false,
            ),
        ] {
            let probe = EquivalenceProperties::new_with_orderings(
                Arc::clone(&schema),
                [prefix.iter().map(|name| sort(name))],
            )
            .with_constraints(Constraints::new_unverified(vec![constraint.clone()]));
            // A single probe row can either emit its ordered matches or one
            // NULL-extended row, even when an additional ON filter is present.
            for join_type in [JoinType::Inner, JoinType::Left] {
                let output = join_equivalence_properties(
                    probe.clone(),
                    build.clone(),
                    &join_type,
                    Arc::clone(&output_schema),
                    &[true, false],
                    Some(JoinSide::Left),
                    &[(col(on.0, &schema)?, col(on.1, &schema)?)],
                    true,
                    null_equality,
                )?;
                assert_eq!(
                    output.ordering_satisfy(
                        prefix.iter().map(|name| sort(name)).chain([
                            PhysicalSortExpr::new_default(Arc::new(Column::new("v", 5))),
                        ])
                    )?,
                    expected,
                    "{constraint:?}, {join_type:?}, {on:?}, {null_equality:?}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_join_suffix_with_combined_probe_orderings() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
            Field::new("extra", DataType::Int32, false),
        ]));
        let sort = |name| PhysicalSortExpr::new_default(col(name, &schema).unwrap());
        let probe = EquivalenceProperties::new_with_orderings(
            Arc::clone(&schema),
            [vec![sort("extra")], vec![sort("a")], vec![sort("b")]],
        );
        let build = EquivalenceProperties::new_with_orderings(
            Arc::clone(&schema),
            [vec![sort("v")]],
        )
        .with_constraints(Constraints::new_unverified(vec![
            Constraint::PrimaryKey(vec![0, 1]),
        ]));
        let output_schema = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .chain(schema.fields())
                .cloned()
                .collect::<Vec<_>>(),
        ));
        let output = join_equivalence_properties(
            probe,
            build,
            &JoinType::Left,
            output_schema,
            &[true, false],
            Some(JoinSide::Left),
            &[
                (col("a", &schema)?, col("a", &schema)?),
                (col("b", &schema)?, col("b", &schema)?),
            ],
            false,
            NullEquality::NullEqualsNothing,
        )?;
        let suffix = PhysicalSortExpr::new_default(Arc::new(Column::new("v", 6)));
        for (first, second) in [("a", "b"), ("b", "a")] {
            assert!(output.ordering_satisfy([
                sort(first),
                sort(second),
                suffix.clone()
            ])?);
            assert!(!output.ordering_satisfy([sort(first), suffix.clone()])?);
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
}
