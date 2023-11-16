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

use arrow_schema::{SchemaRef, SortOptions};
use indexmap::map::Entry;
use indexmap::IndexMap;
use std::collections::HashSet;
use std::sync::Arc;

use crate::equivalence::{
    collapse_lex_req, EquivalenceGroup, OrderingEquivalenceClass, ProjectionMapping,
};
use crate::expressions::Column;
use crate::sort_properties::{ExprOrdering, SortProperties};
use crate::{
    physical_exprs_contains, LexOrdering, LexOrderingRef, LexRequirement,
    LexRequirementRef, PhysicalExpr, PhysicalSortExpr, PhysicalSortRequirement,
};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::Result;

/// A `EquivalenceProperties` object stores useful information related to a schema.
/// Currently, it keeps track of:
/// - Equivalent expressions, e.g expressions that have same value.
/// - Valid sort expressions (orderings) for the schema.
/// - Constants expressions (e.g expressions that are known to have constant values).
///
/// Consider table below:
///
/// ```text
/// ┌-------┐
/// | a | b |
/// |---|---|
/// | 1 | 9 |
/// | 2 | 8 |
/// | 3 | 7 |
/// | 5 | 5 |
/// └---┴---┘
/// ```
///
/// where both `a ASC` and `b DESC` can describe the table ordering. With
/// `EquivalenceProperties`, we can keep track of these different valid sort
/// expressions and treat `a ASC` and `b DESC` on an equal footing.
///
/// Similarly, consider the table below:
///
/// ```text
/// ┌-------┐
/// | a | b |
/// |---|---|
/// | 1 | 1 |
/// | 2 | 2 |
/// | 3 | 3 |
/// | 5 | 5 |
/// └---┴---┘
/// ```
///
/// where columns `a` and `b` always have the same value. We keep track of such
/// equivalences inside this object. With this information, we can optimize
/// things like partitioning. For example, if the partition requirement is
/// `Hash(a)` and output partitioning is `Hash(b)`, then we can deduce that
/// the existing partitioning satisfies the requirement.
#[derive(Debug, Clone)]
pub struct EquivalenceProperties {
    /// Collection of equivalence classes that store expressions with the same
    /// value.
    eq_group: EquivalenceGroup,
    /// Equivalent sort expressions for this table.
    pub(crate) oeq_class: OrderingEquivalenceClass,
    /// Expressions whose values are constant throughout the table.
    /// TODO: We do not need to track constants separately, they can be tracked
    ///       inside `eq_groups` as `Literal` expressions.
    constants: Vec<Arc<dyn PhysicalExpr>>,
    /// Schema associated with this object.
    schema: SchemaRef,
}

impl EquivalenceProperties {
    /// Creates an empty `EquivalenceProperties` object.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            eq_group: EquivalenceGroup::empty(),
            oeq_class: OrderingEquivalenceClass::empty(),
            constants: vec![],
            schema,
        }
    }

    /// Creates a new `EquivalenceProperties` object with the given orderings.
    pub fn new_with_orderings(schema: SchemaRef, orderings: &[LexOrdering]) -> Self {
        Self {
            eq_group: EquivalenceGroup::empty(),
            oeq_class: OrderingEquivalenceClass::new(orderings.to_vec()),
            constants: vec![],
            schema,
        }
    }

    /// Returns the associated schema.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns a reference to the ordering equivalence class within.
    pub fn oeq_class(&self) -> &OrderingEquivalenceClass {
        &self.oeq_class
    }

    /// consume self, and returns the ordering equivalence class within.
    pub fn into_oeq_class(self) -> OrderingEquivalenceClass {
        self.oeq_class
    }

    /// Returns a reference to the equivalence group within.
    pub fn eq_group(&self) -> &EquivalenceGroup {
        &self.eq_group
    }

    /// Returns a reference to the constant expressions
    pub fn constants(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.constants
    }

    /// Returns the normalized version of the ordering equivalence class within.
    /// Normalization removes constants and duplicates as well as standardizing
    /// expressions according to the equivalence group within.
    pub fn normalized_oeq_class(&self) -> OrderingEquivalenceClass {
        OrderingEquivalenceClass::new(
            self.oeq_class
                .iter()
                .map(|ordering| self.normalize_sort_exprs(ordering))
                .collect(),
        )
    }

    /// Extends this `EquivalenceProperties` with the `other` object.
    pub fn extend(mut self, other: Self) -> Self {
        self.eq_group.extend(other.eq_group);
        self.oeq_class.extend(other.oeq_class);
        self.add_constants(other.constants)
    }

    /// Clears (empties) the ordering equivalence class within this object.
    /// Call this method when existing orderings are invalidated.
    pub fn clear_orderings(&mut self) {
        self.oeq_class.clear();
    }

    /// Extends this `EquivalenceProperties` by adding the orderings inside the
    /// ordering equivalence class `other`.
    pub fn add_ordering_equivalence_class(&mut self, other: OrderingEquivalenceClass) {
        self.oeq_class.extend(other);
    }

    /// Adds new orderings into the existing ordering equivalence class.
    pub fn add_new_orderings(
        &mut self,
        orderings: impl IntoIterator<Item = LexOrdering>,
    ) {
        self.oeq_class.add_new_orderings(orderings);
    }

    /// Incorporates the given equivalence group to into the existing
    /// equivalence group within.
    pub fn add_equivalence_group(&mut self, other_eq_group: EquivalenceGroup) {
        self.eq_group.extend(other_eq_group);
    }

    /// Adds a new equality condition into the existing equivalence group.
    /// If the given equality defines a new equivalence class, adds this new
    /// equivalence class to the equivalence group.
    pub fn add_equal_conditions(
        &mut self,
        left: &Arc<dyn PhysicalExpr>,
        right: &Arc<dyn PhysicalExpr>,
    ) {
        self.eq_group.add_equal_conditions(left, right);
    }

    /// Track/register physical expressions with constant values.
    pub fn add_constants(
        mut self,
        constants: impl IntoIterator<Item = Arc<dyn PhysicalExpr>>,
    ) -> Self {
        for expr in self.eq_group.normalize_exprs(constants) {
            if !physical_exprs_contains(&self.constants, &expr) {
                self.constants.push(expr);
            }
        }
        self
    }

    /// Updates the ordering equivalence group within assuming that the table
    /// is re-sorted according to the argument `sort_exprs`. Note that constants
    /// and equivalence classes are unchanged as they are unaffected by a re-sort.
    pub fn with_reorder(mut self, sort_exprs: Vec<PhysicalSortExpr>) -> Self {
        // TODO: In some cases, existing ordering equivalences may still be valid add this analysis.
        self.oeq_class = OrderingEquivalenceClass::new(vec![sort_exprs]);
        self
    }

    /// Normalizes the given sort expressions (i.e. `sort_exprs`) using the
    /// equivalence group and the ordering equivalence class within.
    ///
    /// Assume that `self.eq_group` states column `a` and `b` are aliases.
    /// Also assume that `self.oeq_class` states orderings `d ASC` and `a ASC, c ASC`
    /// are equivalent (in the sense that both describe the ordering of the table).
    /// If the `sort_exprs` argument were `vec![b ASC, c ASC, a ASC]`, then this
    /// function would return `vec![a ASC, c ASC]`. Internally, it would first
    /// normalize to `vec![a ASC, c ASC, a ASC]` and end up with the final result
    /// after deduplication.
    fn normalize_sort_exprs(&self, sort_exprs: LexOrderingRef) -> LexOrdering {
        // Convert sort expressions to sort requirements:
        let sort_reqs = PhysicalSortRequirement::from_sort_exprs(sort_exprs.iter());
        // Normalize the requirements:
        let normalized_sort_reqs = self.normalize_sort_requirements(&sort_reqs);
        // Convert sort requirements back to sort expressions:
        PhysicalSortRequirement::to_sort_exprs(normalized_sort_reqs)
    }

    /// Normalizes the given sort requirements (i.e. `sort_reqs`) using the
    /// equivalence group and the ordering equivalence class within. It works by:
    /// - Removing expressions that have a constant value from the given requirement.
    /// - Replacing sections that belong to some equivalence class in the equivalence
    ///   group with the first entry in the matching equivalence class.
    ///
    /// Assume that `self.eq_group` states column `a` and `b` are aliases.
    /// Also assume that `self.oeq_class` states orderings `d ASC` and `a ASC, c ASC`
    /// are equivalent (in the sense that both describe the ordering of the table).
    /// If the `sort_reqs` argument were `vec![b ASC, c ASC, a ASC]`, then this
    /// function would return `vec![a ASC, c ASC]`. Internally, it would first
    /// normalize to `vec![a ASC, c ASC, a ASC]` and end up with the final result
    /// after deduplication.
    pub fn normalize_sort_requirements(
        &self,
        sort_reqs: LexRequirementRef,
    ) -> LexRequirement {
        let normalized_sort_reqs = self.eq_group.normalize_sort_requirements(sort_reqs);
        let constants_normalized = self.eq_group.normalize_exprs(self.constants.clone());
        // Prune redundant sections in the requirement:
        collapse_lex_req(
            normalized_sort_reqs
                .iter()
                .filter(|&order| {
                    !physical_exprs_contains(&constants_normalized, &order.expr)
                })
                .cloned()
                .collect(),
        )
    }

    /// Checks whether the given ordering is satisfied by any of the existing
    /// orderings.
    pub fn ordering_satisfy(&self, given: LexOrderingRef) -> bool {
        // Convert the given sort expressions to sort requirements:
        let sort_requirements = PhysicalSortRequirement::from_sort_exprs(given.iter());
        self.ordering_satisfy_requirement(&sort_requirements)
    }

    /// Checks whether the given sort requirements are satisfied by any of the
    /// existing orderings.
    pub fn ordering_satisfy_requirement(&self, reqs: LexRequirementRef) -> bool {
        // First, standardize the given requirement:
        let normalized_reqs = self.normalize_sort_requirements(reqs);
        if normalized_reqs.is_empty() {
            // Requirements are tautologically satisfied if empty.
            return true;
        }
        let mut indices = HashSet::new();
        for ordering in self.normalized_oeq_class().iter() {
            let match_indices = ordering
                .iter()
                .map(|sort_expr| {
                    normalized_reqs
                        .iter()
                        .position(|sort_req| sort_expr.satisfy(sort_req, &self.schema))
                })
                .collect::<Vec<_>>();
            // Find the largest contiguous increasing sequence starting from the first index:
            if let Some(&Some(first)) = match_indices.first() {
                indices.insert(first);
                let mut iter = match_indices.windows(2);
                while let Some([Some(current), Some(next)]) = iter.next() {
                    if next > current {
                        indices.insert(*next);
                    } else {
                        break;
                    }
                }
            }
        }
        indices.len() == normalized_reqs.len()
    }

    /// Checks whether the `given`` sort requirements are equal or more specific
    /// than the `reference` sort requirements.
    pub fn requirements_compatible(
        &self,
        given: LexRequirementRef,
        reference: LexRequirementRef,
    ) -> bool {
        let normalized_given = self.normalize_sort_requirements(given);
        let normalized_reference = self.normalize_sort_requirements(reference);

        (normalized_reference.len() <= normalized_given.len())
            && normalized_reference
                .into_iter()
                .zip(normalized_given)
                .all(|(reference, given)| given.compatible(&reference))
    }

    /// Returns the finer ordering among the orderings `lhs` and `rhs`, breaking
    /// any ties by choosing `lhs`.
    ///
    /// The finer ordering is the ordering that satisfies both of the orderings.
    /// If the orderings are incomparable, returns `None`.
    ///
    /// For example, the finer ordering among `[a ASC]` and `[a ASC, b ASC]` is
    /// the latter.
    pub fn get_finer_ordering(
        &self,
        lhs: LexOrderingRef,
        rhs: LexOrderingRef,
    ) -> Option<LexOrdering> {
        // Convert the given sort expressions to sort requirements:
        let lhs = PhysicalSortRequirement::from_sort_exprs(lhs);
        let rhs = PhysicalSortRequirement::from_sort_exprs(rhs);
        let finer = self.get_finer_requirement(&lhs, &rhs);
        // Convert the chosen sort requirements back to sort expressions:
        finer.map(PhysicalSortRequirement::to_sort_exprs)
    }

    /// Returns the finer ordering among the requirements `lhs` and `rhs`,
    /// breaking any ties by choosing `lhs`.
    ///
    /// The finer requirements are the ones that satisfy both of the given
    /// requirements. If the requirements are incomparable, returns `None`.
    ///
    /// For example, the finer requirements among `[a ASC]` and `[a ASC, b ASC]`
    /// is the latter.
    pub fn get_finer_requirement(
        &self,
        req1: LexRequirementRef,
        req2: LexRequirementRef,
    ) -> Option<LexRequirement> {
        let mut lhs = self.normalize_sort_requirements(req1);
        let mut rhs = self.normalize_sort_requirements(req2);
        lhs.iter_mut()
            .zip(rhs.iter_mut())
            .all(|(lhs, rhs)| {
                lhs.expr.eq(&rhs.expr)
                    && match (lhs.options, rhs.options) {
                        (Some(lhs_opt), Some(rhs_opt)) => lhs_opt == rhs_opt,
                        (Some(options), None) => {
                            rhs.options = Some(options);
                            true
                        }
                        (None, Some(options)) => {
                            lhs.options = Some(options);
                            true
                        }
                        (None, None) => true,
                    }
            })
            .then_some(if lhs.len() >= rhs.len() { lhs } else { rhs })
    }

    /// Calculates the "meet" of the given orderings (`lhs` and `rhs`).
    /// The meet of a set of orderings is the finest ordering that is satisfied
    /// by all the orderings in that set. For details, see:
    ///
    /// <https://en.wikipedia.org/wiki/Join_and_meet>
    ///
    /// If there is no ordering that satisfies both `lhs` and `rhs`, returns
    /// `None`. As an example, the meet of orderings `[a ASC]` and `[a ASC, b ASC]`
    /// is `[a ASC]`.
    pub fn get_meet_ordering(
        &self,
        lhs: LexOrderingRef,
        rhs: LexOrderingRef,
    ) -> Option<LexOrdering> {
        let lhs = self.normalize_sort_exprs(lhs);
        let rhs = self.normalize_sort_exprs(rhs);
        let mut meet = vec![];
        for (lhs, rhs) in lhs.into_iter().zip(rhs.into_iter()) {
            if lhs.eq(&rhs) {
                meet.push(lhs);
            } else {
                break;
            }
        }
        (!meet.is_empty()).then_some(meet)
    }

    /// Projects argument `expr` according to `projection_mapping`, taking
    /// equivalences into account.
    ///
    /// For example, assume that columns `a` and `c` are always equal, and that
    /// `projection_mapping` encodes following mapping:
    ///
    /// ```text
    /// a -> a1
    /// b -> b1
    /// ```
    ///
    /// Then, this function projects `a + b` to `Some(a1 + b1)`, `c + b` to
    /// `Some(a1 + b1)` and `d` to `None`, meaning that it  cannot be projected.
    pub fn project_expr(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        projection_mapping: &ProjectionMapping,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        self.eq_group.project_expr(projection_mapping, expr)
    }

    /// Projects the equivalences within according to `projection_mapping`
    /// and `output_schema`.
    pub fn project(
        &self,
        projection_mapping: &ProjectionMapping,
        output_schema: SchemaRef,
    ) -> Self {
        let mut projected_orderings = self
            .oeq_class
            .iter()
            .filter_map(|order| self.eq_group.project_ordering(projection_mapping, order))
            .collect::<Vec<_>>();
        for (source, target) in projection_mapping.iter() {
            let expr_ordering = ExprOrdering::new(source.clone())
                .transform_up(&|expr| update_ordering(expr, self))
                .unwrap();
            if let SortProperties::Ordered(options) = expr_ordering.state {
                // Push new ordering to the state.
                projected_orderings.push(vec![PhysicalSortExpr {
                    expr: target.clone(),
                    options,
                }]);
            }
        }
        Self {
            eq_group: self.eq_group.project(projection_mapping),
            oeq_class: OrderingEquivalenceClass::new(projected_orderings),
            constants: vec![],
            schema: output_schema,
        }
    }

    /// Returns the longest (potentially partial) permutation satisfying the
    /// existing ordering. For example, if we have the equivalent orderings
    /// `[a ASC, b ASC]` and `[c DESC]`, with `exprs` containing `[c, b, a, d]`,
    /// then this function returns `([a ASC, b ASC, c DESC], [2, 1, 0])`.
    /// This means that the specification `[a ASC, b ASC, c DESC]` is satisfied
    /// by the existing ordering, and `[a, b, c]` resides at indices: `2, 1, 0`
    /// inside the argument `exprs` (respectively). For the mathematical
    /// definition of "partial permutation", see:
    ///
    /// <https://en.wikipedia.org/wiki/Permutation#k-permutations_of_n>
    pub fn find_longest_permutation(
        &self,
        exprs: &[Arc<dyn PhysicalExpr>],
    ) -> (LexOrdering, Vec<usize>) {
        let normalized_exprs = self.eq_group.normalize_exprs(exprs.to_vec());
        // Use a map to associate expression indices with sort options:
        let mut ordered_exprs = IndexMap::<usize, SortOptions>::new();
        for ordering in self.normalized_oeq_class().iter() {
            for sort_expr in ordering {
                if let Some(idx) = normalized_exprs
                    .iter()
                    .position(|expr| sort_expr.expr.eq(expr))
                {
                    if let Entry::Vacant(e) = ordered_exprs.entry(idx) {
                        e.insert(sort_expr.options);
                    }
                } else {
                    // We only consider expressions that correspond to a prefix
                    // of one of the equivalent orderings we have.
                    break;
                }
            }
        }
        // Construct the lexicographical ordering according to the permutation:
        ordered_exprs
            .into_iter()
            .map(|(idx, options)| {
                (
                    PhysicalSortExpr {
                        expr: exprs[idx].clone(),
                        options,
                    },
                    idx,
                )
            })
            .unzip()
    }
}

/// Calculates the [`SortProperties`] of a given [`ExprOrdering`] node.
/// The node can either be a leaf node, or an intermediate node:
/// - If it is a leaf node, we directly find the order of the node by looking
/// at the given sort expression and equivalence properties if it is a `Column`
/// leaf, or we mark it as unordered. In the case of a `Literal` leaf, we mark
/// it as singleton so that it can cooperate with all ordered columns.
/// - If it is an intermediate node, the children states matter. Each `PhysicalExpr`
/// and operator has its own rules on how to propagate the children orderings.
/// However, before we engage in recursion, we check whether this intermediate
/// node directly matches with the sort expression. If there is a match, the
/// sort expression emerges at that node immediately, discarding the recursive
/// result coming from its children.
fn update_ordering(
    mut node: ExprOrdering,
    eq_properties: &EquivalenceProperties,
) -> Result<Transformed<ExprOrdering>> {
    if !node.expr.children().is_empty() {
        // We have an intermediate (non-leaf) node, account for its children:
        node.state = node.expr.get_ordering(&node.children_states);
        Ok(Transformed::Yes(node))
    } else if node.expr.as_any().is::<Column>() {
        // We have a Column, which is one of the two possible leaf node types:
        let eq_group = &eq_properties.eq_group;
        let normalized_expr = eq_group.normalize_expr(node.expr.clone());
        let oeq_class = &eq_properties.oeq_class;
        if let Some(options) = oeq_class.get_options(&normalized_expr) {
            node.state = SortProperties::Ordered(options);
            Ok(Transformed::Yes(node))
        } else {
            Ok(Transformed::No(node))
        }
    } else {
        // We have a Literal, which is the other possible leaf node type:
        node.state = node.expr.get_ordering(&[]);
        Ok(Transformed::Yes(node))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::expressions::{col, BinaryExpr};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_expr::Operator;

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
            let expr_ordering = ExprOrdering::new(expr.clone());
            let expr_ordering = expr_ordering
                .transform_up(&|expr| update_ordering(expr, &eq_properties))?;
            let err_msg = format!(
                "expr:{:?}, expected: {:?}, actual: {:?}",
                expr, expected, expr_ordering.state
            );
            assert_eq!(expr_ordering.state, expected, "{}", err_msg);
        }

        Ok(())
    }
}
