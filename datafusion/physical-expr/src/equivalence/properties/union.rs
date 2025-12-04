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

use std::iter::Peekable;
use std::sync::Arc;

use super::EquivalenceProperties;
use crate::equivalence::class::AcrossPartitions;
use crate::{ConstExpr, PhysicalSortExpr};

use arrow::datatypes::SchemaRef;
use datafusion_common::{Result, internal_err};
use datafusion_physical_expr_common::sort_expr::LexOrdering;

/// Computes the union (in the sense of `UnionExec`) `EquivalenceProperties`
/// of `lhs` and `rhs` according to the schema of `lhs`.
///
/// Rules: The `UnionExec` does not interleave its inputs, instead it passes
/// each input partition from the children as its own output.
///
/// Since the output equivalence properties are properties that are true for
/// *all* output partitions, that is the same as being true for all *input*
/// partitions.
fn calculate_union_binary(
    lhs: EquivalenceProperties,
    mut rhs: EquivalenceProperties,
) -> Result<EquivalenceProperties> {
    // Harmonize the schema of the rhs with the schema of the lhs (which is the accumulator schema):
    if !rhs.schema.eq(&lhs.schema) {
        rhs = rhs.with_new_schema(Arc::clone(&lhs.schema))?;
    }

    // First, calculate valid constants for the union. An expression is constant
    // at the output of the union if it is constant in both sides with matching values.
    let rhs_constants = rhs.constants();
    let constants = lhs
        .constants()
        .into_iter()
        .filter_map(|lhs_const| {
            // Find matching constant expression in RHS
            rhs_constants
                .iter()
                .find(|rhs_const| rhs_const.expr.eq(&lhs_const.expr))
                .map(|rhs_const| {
                    let mut const_expr = lhs_const.clone();
                    // If both sides have matching constant values, preserve it.
                    // Otherwise, set fall back to heterogeneous values.
                    if lhs_const.across_partitions != rhs_const.across_partitions {
                        const_expr.across_partitions = AcrossPartitions::Heterogeneous;
                    }
                    const_expr
                })
        })
        .collect::<Vec<_>>();

    // Next, calculate valid orderings for the union by searching for prefixes
    // in both sides.
    let mut orderings = UnionEquivalentOrderingBuilder::new();
    orderings.add_satisfied_orderings(&lhs, &rhs)?;
    orderings.add_satisfied_orderings(&rhs, &lhs)?;
    let orderings = orderings.build();

    let mut eq_properties = EquivalenceProperties::new(lhs.schema);
    eq_properties.add_constants(constants)?;
    eq_properties.add_orderings(orderings);
    Ok(eq_properties)
}

/// Calculates the union (in the sense of `UnionExec`) `EquivalenceProperties`
/// of the given `EquivalenceProperties` in `eqps` according to the given
/// output `schema` (which need not be the same with those of `lhs` and `rhs`
/// as details such as nullability may be different).
pub fn calculate_union(
    eqps: Vec<EquivalenceProperties>,
    schema: SchemaRef,
) -> Result<EquivalenceProperties> {
    // TODO: In some cases, we should be able to preserve some equivalence
    //       classes. Add support for such cases.
    let mut iter = eqps.into_iter();
    let Some(mut acc) = iter.next() else {
        return internal_err!(
            "Cannot calculate EquivalenceProperties for a union with no inputs"
        );
    };

    // Harmonize the schema of the init with the schema of the union:
    if !acc.schema.eq(&schema) {
        acc = acc.with_new_schema(schema)?;
    }
    // Fold in the rest of the EquivalenceProperties:
    for props in iter {
        acc = calculate_union_binary(acc, props)?;
    }
    Ok(acc)
}

#[derive(Debug)]
enum AddedOrdering {
    /// The ordering was added to the in progress result
    Yes,
    /// The ordering was not added
    No(LexOrdering),
}

/// Builds valid output orderings of a `UnionExec`
#[derive(Debug)]
struct UnionEquivalentOrderingBuilder {
    orderings: Vec<LexOrdering>,
}

impl UnionEquivalentOrderingBuilder {
    fn new() -> Self {
        Self { orderings: vec![] }
    }

    /// Add all orderings from `source` that satisfy `properties`,
    /// potentially augmented with the constants in `source`.
    ///
    /// Note: Any column that is known to be constant can be inserted into the
    /// ordering without changing its meaning.
    ///
    /// For example:
    /// * Orderings in `source` contains `[a ASC, c ASC]` and constants contains
    ///   `b`,
    /// * `properties` has the ordering `[a ASC, b ASC]`.
    ///
    /// Then this will add `[a ASC, b ASC]` to the `orderings` list (as `a` was
    /// in the sort order and `b` was a constant).
    fn add_satisfied_orderings(
        &mut self,
        source: &EquivalenceProperties,
        properties: &EquivalenceProperties,
    ) -> Result<()> {
        let constants = source.constants();
        let properties_constants = properties.constants();
        for mut ordering in source.oeq_cache.normal_cls.clone() {
            // Progressively shorten the ordering to search for a satisfied prefix:
            loop {
                ordering = match self.try_add_ordering(
                    ordering,
                    &constants,
                    properties,
                    &properties_constants,
                )? {
                    AddedOrdering::Yes => break,
                    AddedOrdering::No(ordering) => {
                        let mut sort_exprs: Vec<_> = ordering.into();
                        sort_exprs.pop();
                        if let Some(ordering) = LexOrdering::new(sort_exprs) {
                            ordering
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Adds `ordering`, potentially augmented with `constants`, if it satisfies
    /// the given `properties`.
    ///
    /// # Returns
    ///
    /// An [`AddedOrdering::Yes`] instance if the ordering was added (either
    /// directly or augmented), or was empty. An [`AddedOrdering::No`] instance
    /// otherwise.
    fn try_add_ordering(
        &mut self,
        ordering: LexOrdering,
        constants: &[ConstExpr],
        properties: &EquivalenceProperties,
        properties_constants: &[ConstExpr],
    ) -> Result<AddedOrdering> {
        if properties.ordering_satisfy(ordering.clone())? {
            // If the ordering satisfies the target properties, no need to
            // augment it with constants.
            self.orderings.push(ordering);
            Ok(AddedOrdering::Yes)
        } else if self.try_find_augmented_ordering(
            &ordering,
            constants,
            properties,
            properties_constants,
        ) {
            // Augmented with constants to match the properties.
            Ok(AddedOrdering::Yes)
        } else {
            Ok(AddedOrdering::No(ordering))
        }
    }

    /// Attempts to add `constants` to `ordering` to satisfy the properties.
    /// Returns `true` if augmentation took place, `false` otherwise.
    fn try_find_augmented_ordering(
        &mut self,
        ordering: &LexOrdering,
        constants: &[ConstExpr],
        properties: &EquivalenceProperties,
        properties_constants: &[ConstExpr],
    ) -> bool {
        let mut result = false;
        // Can only augment if there are constants.
        if !constants.is_empty() {
            // For each equivalent ordering in properties, try and augment
            // `ordering` with the constants to match `existing_ordering`:
            for existing_ordering in properties.oeq_class.iter() {
                if let Some(augmented_ordering) = Self::augment_ordering(
                    ordering,
                    constants,
                    existing_ordering,
                    properties_constants,
                ) {
                    self.orderings.push(augmented_ordering);
                    result = true;
                }
            }
        }
        result
    }

    /// Attempts to augment the ordering with constants to match `existing_ordering`.
    /// Returns `Some(ordering)` if an augmented ordering was found, `None` otherwise.
    fn augment_ordering(
        ordering: &LexOrdering,
        constants: &[ConstExpr],
        existing_ordering: &LexOrdering,
        existing_constants: &[ConstExpr],
    ) -> Option<LexOrdering> {
        let mut augmented_ordering = vec![];
        let mut sort_exprs = ordering.iter().peekable();
        let mut existing_sort_exprs = existing_ordering.iter().peekable();

        // Walk in parallel down the two orderings, trying to match them up:
        while sort_exprs.peek().is_some() || existing_sort_exprs.peek().is_some() {
            // If the next expressions are equal, add the next match. Otherwise,
            // try and match with a constant.
            if let Some(expr) =
                advance_if_match(&mut sort_exprs, &mut existing_sort_exprs)
            {
                augmented_ordering.push(expr);
            } else if let Some(expr) =
                advance_if_matches_constant(&mut sort_exprs, existing_constants)
            {
                augmented_ordering.push(expr);
            } else if let Some(expr) =
                advance_if_matches_constant(&mut existing_sort_exprs, constants)
            {
                augmented_ordering.push(expr);
            } else {
                // no match, can't continue the ordering, return what we have
                break;
            }
        }

        LexOrdering::new(augmented_ordering)
    }

    fn build(self) -> Vec<LexOrdering> {
        self.orderings
    }
}

/// Advances two iterators in parallel if the next expressions are equal.
/// Otherwise, the iterators are left unchanged and returns `None`.
fn advance_if_match<'a>(
    iter1: &mut Peekable<impl Iterator<Item = &'a PhysicalSortExpr>>,
    iter2: &mut Peekable<impl Iterator<Item = &'a PhysicalSortExpr>>,
) -> Option<PhysicalSortExpr> {
    let (expr1, expr2) = (iter1.peek()?, iter2.peek()?);
    if expr1.eq(expr2) {
        iter1.next();
        iter2.next().cloned()
    } else {
        None
    }
}

/// Advances the iterator with a constant if the next expression matches one of
/// the constants. Otherwise, the iterator is left unchanged and returns `None`.
fn advance_if_matches_constant<'a>(
    iter: &mut Peekable<impl Iterator<Item = &'a PhysicalSortExpr>>,
    constants: &[ConstExpr],
) -> Option<PhysicalSortExpr> {
    let expr = iter.peek()?;
    let const_expr = constants.iter().find(|c| expr.expr.eq(&c.expr))?;
    let found_expr = PhysicalSortExpr::new(Arc::clone(&const_expr.expr), expr.options);
    iter.next();
    Some(found_expr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PhysicalExpr;
    use crate::equivalence::tests::{create_test_schema, parse_sort_expr};
    use crate::expressions::col;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;

    use itertools::Itertools;

    /// Checks whether `expr` is among in the `const_exprs`.
    fn const_exprs_contains(
        const_exprs: &[ConstExpr],
        expr: &Arc<dyn PhysicalExpr>,
    ) -> bool {
        const_exprs
            .iter()
            .any(|const_expr| const_expr.expr.eq(expr))
    }

    #[test]
    fn test_union_equivalence_properties_multi_children_1() -> Result<()> {
        let schema = create_test_schema().unwrap();
        let schema2 = append_fields(&schema, "1");
        let schema3 = append_fields(&schema, "2");
        UnionEquivalenceTest::new(&schema)
            // Children 1
            .with_child_sort(vec![vec!["a", "b", "c"]], &schema)?
            // Children 2
            .with_child_sort(vec![vec!["a1", "b1", "c1"]], &schema2)?
            // Children 3
            .with_child_sort(vec![vec!["a2", "b2"]], &schema3)?
            .with_expected_sort(vec![vec!["a", "b"]])?
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_multi_children_2() -> Result<()> {
        let schema = create_test_schema().unwrap();
        let schema2 = append_fields(&schema, "1");
        let schema3 = append_fields(&schema, "2");
        UnionEquivalenceTest::new(&schema)
            // Children 1
            .with_child_sort(vec![vec!["a", "b", "c"]], &schema)?
            // Children 2
            .with_child_sort(vec![vec!["a1", "b1", "c1"]], &schema2)?
            // Children 3
            .with_child_sort(vec![vec!["a2", "b2", "c2"]], &schema3)?
            .with_expected_sort(vec![vec!["a", "b", "c"]])?
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_multi_children_3() -> Result<()> {
        let schema = create_test_schema().unwrap();
        let schema2 = append_fields(&schema, "1");
        let schema3 = append_fields(&schema, "2");
        UnionEquivalenceTest::new(&schema)
            // Children 1
            .with_child_sort(vec![vec!["a", "b"]], &schema)?
            // Children 2
            .with_child_sort(vec![vec!["a1", "b1", "c1"]], &schema2)?
            // Children 3
            .with_child_sort(vec![vec!["a2", "b2", "c2"]], &schema3)?
            .with_expected_sort(vec![vec!["a", "b"]])?
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_multi_children_4() -> Result<()> {
        let schema = create_test_schema().unwrap();
        let schema2 = append_fields(&schema, "1");
        let schema3 = append_fields(&schema, "2");
        UnionEquivalenceTest::new(&schema)
            // Children 1
            .with_child_sort(vec![vec!["a", "b"]], &schema)?
            // Children 2
            .with_child_sort(vec![vec!["a1", "b1"]], &schema2)?
            // Children 3
            .with_child_sort(vec![vec!["b2", "c2"]], &schema3)?
            .with_expected_sort(vec![])?
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_multi_children_5() -> Result<()> {
        let schema = create_test_schema().unwrap();
        let schema2 = append_fields(&schema, "1");
        UnionEquivalenceTest::new(&schema)
            // Children 1
            .with_child_sort(vec![vec!["a", "b"], vec!["c"]], &schema)?
            // Children 2
            .with_child_sort(vec![vec!["a1", "b1"], vec!["c1"]], &schema2)?
            .with_expected_sort(vec![vec!["a", "b"], vec!["c"]])?
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_common_constants() -> Result<()> {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child: [a ASC], const [b, c]
                vec![vec!["a"]],
                vec!["b", "c"],
                &schema,
            )?
            .with_child_sort_and_const_exprs(
                // Second child: [b ASC], const [a, c]
                vec![vec!["b"]],
                vec!["a", "c"],
                &schema,
            )?
            .with_expected_sort_and_const_exprs(
                // Union expected orderings: [[a ASC], [b ASC]], const [c]
                vec![vec!["a"], vec!["b"]],
                vec!["c"],
            )?
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_prefix() -> Result<()> {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child: [a ASC], const []
                vec![vec!["a"]],
                vec![],
                &schema,
            )?
            .with_child_sort_and_const_exprs(
                // Second child: [a ASC, b ASC], const []
                vec![vec!["a", "b"]],
                vec![],
                &schema,
            )?
            .with_expected_sort_and_const_exprs(
                // Union orderings: [a ASC], const []
                vec![vec!["a"]],
                vec![],
            )?
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_asc_desc_mismatch() -> Result<()> {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child: [a ASC], const []
                vec![vec!["a"]],
                vec![],
                &schema,
            )?
            .with_child_sort_and_const_exprs(
                // Second child orderings: [a DESC], const []
                vec![vec!["a DESC"]],
                vec![],
                &schema,
            )?
            .with_expected_sort_and_const_exprs(
                // Union doesn't have any ordering or constant
                vec![],
                vec![],
            )?
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_different_schemas() -> Result<()> {
        let schema = create_test_schema().unwrap();
        let schema2 = append_fields(&schema, "1");
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child orderings: [a ASC], const []
                vec![vec!["a"]],
                vec![],
                &schema,
            )?
            .with_child_sort_and_const_exprs(
                // Second child orderings: [a1 ASC, b1 ASC], const []
                vec![vec!["a1", "b1"]],
                vec![],
                &schema2,
            )?
            .with_expected_sort_and_const_exprs(
                // Union orderings: [a ASC]
                //
                // Note that a, and a1 are at the same index for their
                // corresponding schemas.
                vec![vec!["a"]],
                vec![],
            )?
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_fill_gaps() -> Result<()> {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child orderings: [a ASC, c ASC], const [b]
                vec![vec!["a", "c"]],
                vec!["b"],
                &schema,
            )?
            .with_child_sort_and_const_exprs(
                // Second child orderings: [b ASC, c ASC], const [a]
                vec![vec!["b", "c"]],
                vec!["a"],
                &schema,
            )?
            .with_expected_sort_and_const_exprs(
                // Union orderings: [
                //   [a ASC, b ASC, c ASC],
                //   [b ASC, a ASC, c ASC]
                // ], const []
                vec![vec!["a", "b", "c"], vec!["b", "a", "c"]],
                vec![],
            )?
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_no_fill_gaps() -> Result<()> {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child orderings: [a ASC, c ASC], const [d] // some other constant
                vec![vec!["a", "c"]],
                vec!["d"],
                &schema,
            )?
            .with_child_sort_and_const_exprs(
                // Second child orderings: [b ASC, c ASC], const [a]
                vec![vec!["b", "c"]],
                vec!["a"],
                &schema,
            )?
            .with_expected_sort_and_const_exprs(
                // Union orderings: [[a]] (only a is constant)
                vec![vec!["a"]],
                vec![],
            )?
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_fill_some_gaps() -> Result<()> {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child orderings: [c ASC], const [a, b] // some other constant
                vec![vec!["c"]],
                vec!["a", "b"],
                &schema,
            )?
            .with_child_sort_and_const_exprs(
                // Second child orderings: [a DESC, b], const []
                vec![vec!["a DESC", "b"]],
                vec![],
                &schema,
            )?
            .with_expected_sort_and_const_exprs(
                // Union orderings: [[a, b]] (can fill in the a/b with constants)
                vec![vec!["a DESC", "b"]],
                vec![],
            )?
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_fill_gaps_non_symmetric() -> Result<()>
    {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child orderings: [a ASC, c ASC], const [b]
                vec![vec!["a", "c"]],
                vec!["b"],
                &schema,
            )?
            .with_child_sort_and_const_exprs(
                // Second child orderings: [b ASC, c ASC], const [a]
                vec![vec!["b DESC", "c"]],
                vec!["a"],
                &schema,
            )?
            .with_expected_sort_and_const_exprs(
                // Union orderings: [
                //   [a ASC, b ASC, c ASC],
                //   [b ASC, a ASC, c ASC]
                // ], const []
                vec![vec!["a", "b DESC", "c"], vec!["b DESC", "a", "c"]],
                vec![],
            )?
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_gap_fill_symmetric() -> Result<()> {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child: [a ASC, b ASC, d ASC], const [c]
                vec![vec!["a", "b", "d"]],
                vec!["c"],
                &schema,
            )?
            .with_child_sort_and_const_exprs(
                // Second child: [a ASC, c ASC, d ASC], const [b]
                vec![vec!["a", "c", "d"]],
                vec!["b"],
                &schema,
            )?
            .with_expected_sort_and_const_exprs(
                // Union orderings:
                // [a, b, c, d]
                // [a, c, b, d]
                vec![vec!["a", "c", "b", "d"], vec!["a", "b", "c", "d"]],
                vec![],
            )?
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_gap_fill_and_common() -> Result<()> {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child: [a DESC, d ASC], const [b, c]
                vec![vec!["a DESC", "d"]],
                vec!["b", "c"],
                &schema,
            )?
            .with_child_sort_and_const_exprs(
                // Second child: [a DESC, c ASC, d ASC], const [b]
                vec![vec!["a DESC", "c", "d"]],
                vec!["b"],
                &schema,
            )?
            .with_expected_sort_and_const_exprs(
                // Union orderings:
                // [a DESC, c, d]  [b]
                vec![vec!["a DESC", "c", "d"]],
                vec!["b"],
            )?
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_middle_desc() -> Result<()> {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // NB `b DESC` in the first child
                //
                // First child: [a ASC, b DESC, d ASC], const [c]
                vec![vec!["a", "b DESC", "d"]],
                vec!["c"],
                &schema,
            )?
            .with_child_sort_and_const_exprs(
                // Second child: [a ASC, c ASC, d ASC], const [b]
                vec![vec!["a", "c", "d"]],
                vec!["b"],
                &schema,
            )?
            .with_expected_sort_and_const_exprs(
                // Union orderings:
                // [a, b, d] (c constant)
                // [a, c, d] (b constant)
                vec![vec!["a", "c", "b DESC", "d"], vec!["a", "b DESC", "c", "d"]],
                vec![],
            )?
            .run()
    }

    // TODO tests with multiple constants

    #[derive(Debug)]
    struct UnionEquivalenceTest {
        /// The schema of the output of the Union
        output_schema: SchemaRef,
        /// The equivalence properties of each child to the union
        child_properties: Vec<EquivalenceProperties>,
        /// The expected output properties of the union. Must be set before
        /// running `build`
        expected_properties: Option<EquivalenceProperties>,
    }

    impl UnionEquivalenceTest {
        fn new(output_schema: &SchemaRef) -> Self {
            Self {
                output_schema: Arc::clone(output_schema),
                child_properties: vec![],
                expected_properties: None,
            }
        }

        /// Add a union input with the specified orderings
        ///
        /// See [`Self::make_props`] for the format of the strings in `orderings`
        fn with_child_sort(
            mut self,
            orderings: Vec<Vec<&str>>,
            schema: &SchemaRef,
        ) -> Result<Self> {
            let properties = self.make_props(orderings, vec![], schema)?;
            self.child_properties.push(properties);
            Ok(self)
        }

        /// Add a union input with the specified orderings and constant
        /// equivalences
        ///
        /// See [`Self::make_props`] for the format of the strings in
        /// `orderings` and `constants`
        fn with_child_sort_and_const_exprs(
            mut self,
            orderings: Vec<Vec<&str>>,
            constants: Vec<&str>,
            schema: &SchemaRef,
        ) -> Result<Self> {
            let properties = self.make_props(orderings, constants, schema)?;
            self.child_properties.push(properties);
            Ok(self)
        }

        /// Set the expected output sort order for the union of the children
        ///
        /// See [`Self::make_props`] for the format of the strings in `orderings`
        fn with_expected_sort(mut self, orderings: Vec<Vec<&str>>) -> Result<Self> {
            let properties = self.make_props(orderings, vec![], &self.output_schema)?;
            self.expected_properties = Some(properties);
            Ok(self)
        }

        /// Set the expected output sort order and constant expressions for the
        /// union of the children
        ///
        /// See [`Self::make_props`] for the format of the strings in
        /// `orderings` and `constants`.
        fn with_expected_sort_and_const_exprs(
            mut self,
            orderings: Vec<Vec<&str>>,
            constants: Vec<&str>,
        ) -> Result<Self> {
            let properties =
                self.make_props(orderings, constants, &self.output_schema)?;
            self.expected_properties = Some(properties);
            Ok(self)
        }

        /// compute the union's output equivalence properties from the child
        /// properties, and compare them to the expected properties
        fn run(self) -> Result<()> {
            let Self {
                output_schema,
                child_properties,
                expected_properties,
            } = self;

            let expected_properties =
                expected_properties.expect("expected_properties not set");

            // try all permutations of the children
            // as the code treats lhs and rhs differently
            for child_properties in child_properties
                .iter()
                .cloned()
                .permutations(child_properties.len())
            {
                println!("--- permutation ---");
                for c in &child_properties {
                    println!("{c}");
                }
                let actual_properties =
                    calculate_union(child_properties, Arc::clone(&output_schema))
                        .expect("failed to calculate union equivalence properties");
                Self::assert_eq_properties_same(
                    &actual_properties,
                    &expected_properties,
                    format!(
                        "expected: {expected_properties:?}\nactual:  {actual_properties:?}"
                    ),
                );
            }
            Ok(())
        }

        fn assert_eq_properties_same(
            lhs: &EquivalenceProperties,
            rhs: &EquivalenceProperties,
            err_msg: String,
        ) {
            // Check whether constants are same
            let lhs_constants = lhs.constants();
            let rhs_constants = rhs.constants();
            for rhs_constant in &rhs_constants {
                assert!(
                    const_exprs_contains(&lhs_constants, &rhs_constant.expr),
                    "{err_msg}\nlhs: {lhs}\nrhs: {rhs}"
                );
            }
            assert_eq!(
                lhs_constants.len(),
                rhs_constants.len(),
                "{err_msg}\nlhs: {lhs}\nrhs: {rhs}"
            );

            // Check whether orderings are same.
            let lhs_orderings = lhs.oeq_class();
            let rhs_orderings = rhs.oeq_class();
            for rhs_ordering in rhs_orderings.iter() {
                assert!(
                    lhs_orderings.contains(rhs_ordering),
                    "{err_msg}\nlhs: {lhs}\nrhs: {rhs}"
                );
            }
            assert_eq!(
                lhs_orderings.len(),
                rhs_orderings.len(),
                "{err_msg}\nlhs: {lhs}\nrhs: {rhs}"
            );
        }

        /// Make equivalence properties for the specified columns named in orderings and constants
        ///
        /// orderings: strings formatted like `"a"` or `"a DESC"`. See [`parse_sort_expr`]
        /// constants: strings formatted like `"a"`.
        fn make_props(
            &self,
            orderings: Vec<Vec<&str>>,
            constants: Vec<&str>,
            schema: &SchemaRef,
        ) -> Result<EquivalenceProperties> {
            let orderings = orderings.iter().map(|ordering| {
                ordering.iter().map(|name| parse_sort_expr(name, schema))
            });

            let constants = constants
                .iter()
                .map(|col_name| ConstExpr::from(col(col_name, schema).unwrap()));

            let mut props =
                EquivalenceProperties::new_with_orderings(Arc::clone(schema), orderings);
            props.add_constants(constants)?;
            Ok(props)
        }
    }

    #[test]
    fn test_union_constant_value_preservation() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));

        let col_a = col("a", &schema)?;
        let literal_10 = ScalarValue::Int32(Some(10));

        // Create first input with a=10
        let const_expr1 = ConstExpr::new(
            Arc::clone(&col_a),
            AcrossPartitions::Uniform(Some(literal_10.clone())),
        );
        let mut input1 = EquivalenceProperties::new(Arc::clone(&schema));
        input1.add_constants(vec![const_expr1])?;

        // Create second input with a=10
        let const_expr2 = ConstExpr::new(
            Arc::clone(&col_a),
            AcrossPartitions::Uniform(Some(literal_10.clone())),
        );
        let mut input2 = EquivalenceProperties::new(Arc::clone(&schema));
        input2.add_constants(vec![const_expr2])?;

        // Calculate union properties
        let union_props = calculate_union(vec![input1, input2], schema)?;

        // Verify column 'a' remains constant with value 10
        let const_a = &union_props.constants()[0];
        assert!(const_a.expr.eq(&col_a));
        assert_eq!(
            const_a.across_partitions,
            AcrossPartitions::Uniform(Some(literal_10))
        );

        Ok(())
    }

    /// Return a new schema with the same types, but new field names
    ///
    /// The new field names are the old field names with `text` appended.
    ///
    /// For example, the schema "a", "b", "c" becomes "a1", "b1", "c1"
    /// if `text` is "1".
    fn append_fields(schema: &SchemaRef, text: &str) -> SchemaRef {
        Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .map(|field| {
                    Field::new(
                        // Annotate name with `text`:
                        format!("{}{}", field.name(), text),
                        field.data_type().clone(),
                        field.is_nullable(),
                    )
                })
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn test_constants_share_values() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("const_1", DataType::Utf8, false),
            Field::new("const_2", DataType::Utf8, false),
        ]));

        let col_const_1 = col("const_1", &schema)?;
        let col_const_2 = col("const_2", &schema)?;

        let literal_foo = ScalarValue::Utf8(Some("foo".to_owned()));
        let literal_bar = ScalarValue::Utf8(Some("bar".to_owned()));

        let const_expr_1_foo = ConstExpr::new(
            Arc::clone(&col_const_1),
            AcrossPartitions::Uniform(Some(literal_foo.clone())),
        );
        let const_expr_2_foo = ConstExpr::new(
            Arc::clone(&col_const_2),
            AcrossPartitions::Uniform(Some(literal_foo.clone())),
        );
        let const_expr_2_bar = ConstExpr::new(
            Arc::clone(&col_const_2),
            AcrossPartitions::Uniform(Some(literal_bar.clone())),
        );

        let mut input1 = EquivalenceProperties::new(Arc::clone(&schema));
        let mut input2 = EquivalenceProperties::new(Arc::clone(&schema));

        // | Input | Const_1 | Const_2 |
        // | ----- | ------- | ------- |
        // |     1 | foo     | foo     |
        // |     2 | foo     | bar     |
        input1.add_constants(vec![const_expr_1_foo.clone(), const_expr_2_foo.clone()])?;
        input2.add_constants(vec![const_expr_1_foo.clone(), const_expr_2_bar.clone()])?;

        // Calculate union properties
        let union_props = calculate_union(vec![input1, input2], schema)?;

        // This should result in:
        //   const_1 = Uniform("foo")
        //   const_2 = Heterogeneous
        assert_eq!(union_props.constants().len(), 2);
        let union_const_1 = &union_props.constants()[0];
        assert!(union_const_1.expr.eq(&col_const_1));
        assert_eq!(
            union_const_1.across_partitions,
            AcrossPartitions::Uniform(Some(literal_foo)),
        );
        let union_const_2 = &union_props.constants()[1];
        assert!(union_const_2.expr.eq(&col_const_2));
        assert_eq!(
            union_const_2.across_partitions,
            AcrossPartitions::Heterogeneous,
        );

        Ok(())
    }
}
