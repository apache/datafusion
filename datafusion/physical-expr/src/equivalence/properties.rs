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

use crate::expressions::Column;
use arrow_schema::SchemaRef;
use datafusion_common::{JoinSide, JoinType};
use indexmap::IndexSet;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::equivalence::{
    collapse_lex_req, EquivalenceGroup, OrderingEquivalenceClass, ProjectionMapping,
};

use crate::expressions::Literal;
use crate::sort_properties::{ExprOrdering, SortProperties};
use crate::{
    physical_exprs_contains, LexOrdering, LexOrderingRef, LexRequirement,
    LexRequirementRef, PhysicalExpr, PhysicalSortExpr, PhysicalSortRequirement,
};
use datafusion_common::tree_node::{Transformed, TreeNode};

use super::ordering::collapse_lex_ordering;

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
    oeq_class: OrderingEquivalenceClass,
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
    fn normalize_sort_requirements(
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
        let mut eq_properties = self.clone();
        // First, standardize the given requirement:
        let normalized_reqs = eq_properties.normalize_sort_requirements(reqs);
        for normalized_req in normalized_reqs {
            // Check whether given ordering is satisfied
            if !eq_properties.ordering_satisfy_single(&normalized_req) {
                return false;
            }
            // Treat satisfied keys as constants in subsequent iterations. We
            // can do this because the "next" key only matters in a lexicographical
            // ordering when the keys to its left have the same values.
            //
            // Note that these expressions are not properly "constants". This is just
            // an implementation strategy confined to this function.
            //
            // For example, assume that the requirement is `[a ASC, (b + c) ASC]`,
            // and existing equivalent orderings are `[a ASC, b ASC]` and `[c ASC]`.
            // From the analysis above, we know that `[a ASC]` is satisfied. Then,
            // we add column `a` as constant to the algorithm state. This enables us
            // to deduce that `(b + c) ASC` is satisfied, given `a` is constant.
            eq_properties =
                eq_properties.add_constants(std::iter::once(normalized_req.expr));
        }
        true
    }

    /// Determines whether the ordering specified by the given sort requirement
    /// is satisfied based on the orderings within, equivalence classes, and
    /// constant expressions.
    ///
    /// # Arguments
    ///
    /// - `req`: A reference to a `PhysicalSortRequirement` for which the ordering
    ///   satisfaction check will be done.
    ///
    /// # Returns
    ///
    /// Returns `true` if the specified ordering is satisfied, `false` otherwise.
    fn ordering_satisfy_single(&self, req: &PhysicalSortRequirement) -> bool {
        let expr_ordering = self.get_expr_ordering(req.expr.clone());
        let ExprOrdering { expr, state, .. } = expr_ordering;
        match state {
            SortProperties::Ordered(options) => {
                let sort_expr = PhysicalSortExpr { expr, options };
                sort_expr.satisfy(req, self.schema())
            }
            // Singleton expressions satisfies any ordering.
            SortProperties::Singleton => true,
            SortProperties::Unordered => false,
        }
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

    /// Constructs a dependency map based on existing orderings referred to in
    /// the projection.
    ///
    /// This function analyzes the orderings in the normalized order-equivalence
    /// class and builds a dependency map. The dependency map captures relationships
    /// between expressions within the orderings, helping to identify dependencies
    /// and construct valid projected orderings during projection operations.
    ///
    /// # Parameters
    ///
    /// - `mapping`: A reference to the `ProjectionMapping` that defines the
    ///   relationship between source and target expressions.
    ///
    /// # Returns
    ///
    /// A [`DependencyMap`] representing the dependency map, where each
    /// [`DependencyNode`] contains dependencies for the key [`PhysicalSortExpr`].
    ///
    /// # Example
    ///
    /// Assume we have two equivalent orderings: `[a ASC, b ASC]` and `[a ASC, c ASC]`,
    /// and the projection mapping is `[a -> a_new, b -> b_new, b + c -> b + c]`.
    /// Then, the dependency map will be:
    ///
    /// ```text
    /// a ASC: Node {Some(a_new ASC), HashSet{}}
    /// b ASC: Node {Some(b_new ASC), HashSet{a ASC}}
    /// c ASC: Node {None, HashSet{a ASC}}
    /// ```
    fn construct_dependency_map(&self, mapping: &ProjectionMapping) -> DependencyMap {
        let mut dependency_map = HashMap::new();
        for ordering in self.normalized_oeq_class().iter() {
            for (idx, sort_expr) in ordering.iter().enumerate() {
                let target_sort_expr =
                    self.project_expr(&sort_expr.expr, mapping).map(|expr| {
                        PhysicalSortExpr {
                            expr,
                            options: sort_expr.options,
                        }
                    });
                let is_projected = target_sort_expr.is_some();
                if is_projected
                    || mapping
                        .iter()
                        .any(|(source, _)| expr_refers(source, &sort_expr.expr))
                {
                    // Previous ordering is a dependency. Note that there is no,
                    // dependency for a leading ordering (i.e. the first sort
                    // expression).
                    let dependency = idx.checked_sub(1).map(|a| &ordering[a]);
                    // Add sort expressions that can be projected or referred to
                    // by any of the projection expressions to the dependency map:
                    dependency_map
                        .entry(sort_expr.clone())
                        .or_insert_with(|| DependencyNode {
                            target_sort_expr: target_sort_expr.clone(),
                            dependencies: HashSet::new(),
                        })
                        .insert_dependency(dependency);
                }
                if !is_projected {
                    // If we can not project, stop constructing the dependency
                    // map as remaining dependencies will be invalid after projection.
                    break;
                }
            }
        }
        dependency_map
    }

    /// Returns a new `ProjectionMapping` where source expressions are normalized.
    ///
    /// This normalization ensures that source expressions are transformed into a
    /// consistent representation. This is beneficial for algorithms that rely on
    /// exact equalities, as it allows for more precise and reliable comparisons.
    ///
    /// # Parameters
    ///
    /// - `mapping`: A reference to the original `ProjectionMapping` to be normalized.
    ///
    /// # Returns
    ///
    /// A new `ProjectionMapping` with normalized source expressions.
    fn normalized_mapping(&self, mapping: &ProjectionMapping) -> ProjectionMapping {
        // Construct the mapping where source expressions are normalized. In this way
        // In the algorithms below we can work on exact equalities
        ProjectionMapping {
            map: mapping
                .iter()
                .map(|(source, target)| {
                    let normalized_source = self.eq_group.normalize_expr(source.clone());
                    (normalized_source, target.clone())
                })
                .collect(),
        }
    }

    /// Computes projected orderings based on a given projection mapping.
    ///
    /// This function takes a `ProjectionMapping` and computes the possible
    /// orderings for the projected expressions. It considers dependencies
    /// between expressions and generates valid orderings according to the
    /// specified sort properties.
    ///
    /// # Parameters
    ///
    /// - `mapping`: A reference to the `ProjectionMapping` that defines the
    ///   relationship between source and target expressions.
    ///
    /// # Returns
    ///
    /// A vector of `LexOrdering` containing all valid orderings after projection.
    fn projected_orderings(&self, mapping: &ProjectionMapping) -> Vec<LexOrdering> {
        let mapping = self.normalized_mapping(mapping);

        // Get dependency map for existing orderings:
        let dependency_map = self.construct_dependency_map(&mapping);

        let orderings = mapping.iter().flat_map(|(source, target)| {
            referred_dependencies(&dependency_map, source)
                .into_iter()
                .filter_map(|relevant_deps| {
                    if let SortProperties::Ordered(options) =
                        get_expr_ordering(source, &relevant_deps)
                    {
                        Some((options, relevant_deps))
                    } else {
                        // Do not consider unordered cases
                        None
                    }
                })
                .flat_map(|(options, relevant_deps)| {
                    let sort_expr = PhysicalSortExpr {
                        expr: target.clone(),
                        options,
                    };
                    // Generate dependent orderings (i.e. prefixes for `sort_expr`):
                    let mut dependency_orderings =
                        generate_dependency_orderings(&relevant_deps, &dependency_map);
                    // Append `sort_expr` to the dependent orderings:
                    for ordering in dependency_orderings.iter_mut() {
                        ordering.push(sort_expr.clone());
                    }
                    dependency_orderings
                })
        });

        // Add valid projected orderings. For example, if existing ordering is
        // `a + b` and projection is `[a -> a_new, b -> b_new]`, we need to
        // preserve `a_new + b_new` as ordered. Please note that `a_new` and
        // `b_new` themselves need not be ordered. Such dependencies cannot be
        // deduced via the pass above.
        let projected_orderings = dependency_map.iter().flat_map(|(sort_expr, node)| {
            let mut prefixes = construct_prefix_orderings(sort_expr, &dependency_map);
            if prefixes.is_empty() {
                // If prefix is empty, there is no dependency. Insert
                // empty ordering:
                prefixes = vec![vec![]];
            }
            // Append current ordering on top its dependencies:
            for ordering in prefixes.iter_mut() {
                if let Some(target) = &node.target_sort_expr {
                    ordering.push(target.clone())
                }
            }
            prefixes
        });

        // Simplify each ordering by removing redundant sections:
        orderings
            .chain(projected_orderings)
            .map(collapse_lex_ordering)
            .collect()
    }

    /// Projects constants based on the provided `ProjectionMapping`.
    ///
    /// This function takes a `ProjectionMapping` and identifies/projects
    /// constants based on the existing constants and the mapping. It ensures
    /// that constants are appropriately propagated through the projection.
    ///
    /// # Arguments
    ///
    /// - `mapping`: A reference to a `ProjectionMapping` representing the
    ///   mapping of source expressions to target expressions in the projection.
    ///
    /// # Returns
    ///
    /// Returns a `Vec<Arc<dyn PhysicalExpr>>` containing the projected constants.
    fn projected_constants(
        &self,
        mapping: &ProjectionMapping,
    ) -> Vec<Arc<dyn PhysicalExpr>> {
        // First, project existing constants. For example, assume that `a + b`
        // is known to be constant. If the projection were `a as a_new`, `b as b_new`,
        // then we would project constant `a + b` as `a_new + b_new`.
        let mut projected_constants = self
            .constants
            .iter()
            .flat_map(|expr| self.eq_group.project_expr(mapping, expr))
            .collect::<Vec<_>>();
        // Add projection expressions that are known to be constant:
        for (source, target) in mapping.iter() {
            if self.is_expr_constant(source)
                && !physical_exprs_contains(&projected_constants, target)
            {
                projected_constants.push(target.clone());
            }
        }
        projected_constants
    }

    /// Projects the equivalences within according to `projection_mapping`
    /// and `output_schema`.
    pub fn project(
        &self,
        projection_mapping: &ProjectionMapping,
        output_schema: SchemaRef,
    ) -> Self {
        let projected_constants = self.projected_constants(projection_mapping);
        let projected_eq_group = self.eq_group.project(projection_mapping);
        let projected_orderings = self.projected_orderings(projection_mapping);
        Self {
            eq_group: projected_eq_group,
            oeq_class: OrderingEquivalenceClass::new(projected_orderings),
            constants: projected_constants,
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
        let mut eq_properties = self.clone();
        let mut result = vec![];
        // The algorithm is as follows:
        // - Iterate over all the expressions and insert ordered expressions
        //   into the result.
        // - Treat inserted expressions as constants (i.e. add them as constants
        //   to the state).
        // - Continue the above procedure until no expression is inserted; i.e.
        //   the algorithm reaches a fixed point.
        // This algorithm should reach a fixed point in at most `exprs.len()`
        // iterations.
        let mut search_indices = (0..exprs.len()).collect::<IndexSet<_>>();
        for _idx in 0..exprs.len() {
            // Get ordered expressions with their indices.
            let ordered_exprs = search_indices
                .iter()
                .flat_map(|&idx| {
                    let ExprOrdering { expr, state, .. } =
                        eq_properties.get_expr_ordering(exprs[idx].clone());
                    if let SortProperties::Ordered(options) = state {
                        Some((PhysicalSortExpr { expr, options }, idx))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            // We reached a fixed point, exit.
            if ordered_exprs.is_empty() {
                break;
            }
            // Remove indices that have an ordering from `search_indices`, and
            // treat ordered expressions as constants in subsequent iterations.
            // We can do this because the "next" key only matters in a lexicographical
            // ordering when the keys to its left have the same values.
            //
            // Note that these expressions are not properly "constants". This is just
            // an implementation strategy confined to this function.
            for (PhysicalSortExpr { expr, .. }, idx) in &ordered_exprs {
                eq_properties =
                    eq_properties.add_constants(std::iter::once(expr.clone()));
                search_indices.remove(idx);
            }
            // Add new ordered section to the state.
            result.extend(ordered_exprs);
        }
        result.into_iter().unzip()
    }

    /// This function determines whether the provided expression is constant
    /// based on the known constants.
    ///
    /// # Arguments
    ///
    /// - `expr`: A reference to a `Arc<dyn PhysicalExpr>` representing the
    ///   expression to be checked.
    ///
    /// # Returns
    ///
    /// Returns `true` if the expression is constant according to equivalence
    /// group, `false` otherwise.
    fn is_expr_constant(&self, expr: &Arc<dyn PhysicalExpr>) -> bool {
        // As an example, assume that we know columns `a` and `b` are constant.
        // Then, `a`, `b` and `a + b` will all return `true` whereas `c` will
        // return `false`.
        let normalized_constants = self.eq_group.normalize_exprs(self.constants.to_vec());
        let normalized_expr = self.eq_group.normalize_expr(expr.clone());
        is_constant_recurse(&normalized_constants, &normalized_expr)
    }

    /// Retrieves the ordering information for a given physical expression.
    ///
    /// This function constructs an `ExprOrdering` object for the provided
    /// expression, which encapsulates information about the expression's
    /// ordering, including its [`SortProperties`].
    ///
    /// # Arguments
    ///
    /// - `expr`: An `Arc<dyn PhysicalExpr>` representing the physical expression
    ///   for which ordering information is sought.
    ///
    /// # Returns
    ///
    /// Returns an `ExprOrdering` object containing the ordering information for
    /// the given expression.
    pub fn get_expr_ordering(&self, expr: Arc<dyn PhysicalExpr>) -> ExprOrdering {
        ExprOrdering::new(expr.clone())
            .transform_up(&|expr| Ok(update_ordering(expr, self)))
            // Guaranteed to always return `Ok`.
            .unwrap()
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
) -> Transformed<ExprOrdering> {
    // We have a Column, which is one of the two possible leaf node types:
    let normalized_expr = eq_properties.eq_group.normalize_expr(node.expr.clone());
    if eq_properties.is_expr_constant(&normalized_expr) {
        node.state = SortProperties::Singleton;
    } else if let Some(options) = eq_properties
        .normalized_oeq_class()
        .get_options(&normalized_expr)
    {
        node.state = SortProperties::Ordered(options);
    } else if !node.expr.children().is_empty() {
        // We have an intermediate (non-leaf) node, account for its children:
        node.state = node.expr.get_ordering(&node.children_state());
    } else if node.expr.as_any().is::<Literal>() {
        // We have a Literal, which is the other possible leaf node type:
        node.state = node.expr.get_ordering(&[]);
    } else {
        return Transformed::No(node);
    }
    Transformed::Yes(node)
}

/// This function determines whether the provided expression is constant
/// based on the known constants.
///
/// # Arguments
///
/// - `constants`: A `&[Arc<dyn PhysicalExpr>]` containing expressions known to
///   be a constant.
/// - `expr`: A reference to a `Arc<dyn PhysicalExpr>` representing the expression
///   to check.
///
/// # Returns
///
/// Returns `true` if the expression is constant according to equivalence
/// group, `false` otherwise.
fn is_constant_recurse(
    constants: &[Arc<dyn PhysicalExpr>],
    expr: &Arc<dyn PhysicalExpr>,
) -> bool {
    if physical_exprs_contains(constants, expr) {
        return true;
    }
    let children = expr.children();
    !children.is_empty() && children.iter().all(|c| is_constant_recurse(constants, c))
}

/// This function examines whether a referring expression directly refers to a
/// given referred expression or if any of its children in the expression tree
/// refer to the specified expression.
///
/// # Parameters
///
/// - `referring_expr`: A reference to the referring expression (`Arc<dyn PhysicalExpr>`).
/// - `referred_expr`: A reference to the referred expression (`Arc<dyn PhysicalExpr>`)
///
/// # Returns
///
/// A boolean value indicating whether `referring_expr` refers (needs it to evaluate its result)
/// `referred_expr` or not.
fn expr_refers(
    referring_expr: &Arc<dyn PhysicalExpr>,
    referred_expr: &Arc<dyn PhysicalExpr>,
) -> bool {
    referring_expr.eq(referred_expr)
        || referring_expr
            .children()
            .iter()
            .any(|child| expr_refers(child, referred_expr))
}

/// This function analyzes the dependency map to collect referred dependencies for
/// a given source expression.
///
/// # Parameters
///
/// - `dependency_map`: A reference to the `DependencyMap` where each
///   `PhysicalSortExpr` is associated with a `DependencyNode`.
/// - `source`: A reference to the source expression (`Arc<dyn PhysicalExpr>`)
///   for which relevant dependencies need to be identified.
///
/// # Returns
///
/// A `Vec<Dependencies>` containing the dependencies for the given source
/// expression. These dependencies are expressions that are referred to by
/// the source expression based on the provided dependency map.
fn referred_dependencies(
    dependency_map: &DependencyMap,
    source: &Arc<dyn PhysicalExpr>,
) -> Vec<Dependencies> {
    // Associate `PhysicalExpr`s with `PhysicalSortExpr`s that contain them:
    let mut expr_to_sort_exprs = HashMap::<ExprWrapper, Dependencies>::new();
    for sort_expr in dependency_map
        .keys()
        .filter(|sort_expr| expr_refers(source, &sort_expr.expr))
    {
        let key = ExprWrapper(sort_expr.expr.clone());
        expr_to_sort_exprs
            .entry(key)
            .or_default()
            .insert(sort_expr.clone());
    }

    // Generate all valid dependencies for the source. For example, if the source
    // is `a + b` and the map is `[a -> (a ASC, a DESC), b -> (b ASC)]`, we get
    // `vec![HashSet(a ASC, b ASC), HashSet(a DESC, b ASC)]`.
    expr_to_sort_exprs
        .values()
        .multi_cartesian_product()
        .map(|referred_deps| referred_deps.into_iter().cloned().collect())
        .collect()
}

/// This function retrieves the dependencies of the given relevant sort expression
/// from the given dependency map. It then constructs prefix orderings by recursively
/// analyzing the dependencies and include them in the orderings.
///
/// # Parameters
///
/// - `relevant_sort_expr`: A reference to the relevant sort expression
///   (`PhysicalSortExpr`) for which prefix orderings are to be constructed.
/// - `dependency_map`: A reference to the `DependencyMap` containing dependencies.
///
/// # Returns
///
/// A vector of prefix orderings (`Vec<LexOrdering>`) based on the given relevant
/// sort expression and its dependencies.
fn construct_prefix_orderings(
    relevant_sort_expr: &PhysicalSortExpr,
    dependency_map: &DependencyMap,
) -> Vec<LexOrdering> {
    dependency_map[relevant_sort_expr]
        .dependencies
        .iter()
        .flat_map(|dep| construct_orderings(dep, dependency_map))
        .collect()
}

/// Given a set of relevant dependencies (`relevant_deps`) and a map of dependencies
/// (`dependency_map`), this function generates all possible prefix orderings
/// based on the given dependencies.
///
/// # Parameters
///
/// * `dependencies` - A reference to the dependencies.
/// * `dependency_map` - A reference to the map of dependencies for expressions.
///
/// # Returns
///
/// A vector of lexical orderings (`Vec<LexOrdering>`) representing all valid orderings
/// based on the given dependencies.
fn generate_dependency_orderings(
    dependencies: &Dependencies,
    dependency_map: &DependencyMap,
) -> Vec<LexOrdering> {
    // Construct all the valid prefix orderings for each expression appearing
    // in the projection:
    let relevant_prefixes = dependencies
        .iter()
        .flat_map(|dep| {
            let prefixes = construct_prefix_orderings(dep, dependency_map);
            (!prefixes.is_empty()).then_some(prefixes)
        })
        .collect::<Vec<_>>();

    // No dependency, dependent is a leading ordering.
    if relevant_prefixes.is_empty() {
        // Return an empty ordering:
        return vec![vec![]];
    }

    // Generate all possible orderings where dependencies are satisfied for the
    // current projection expression. For example, if expression is `a + b ASC`,
    // and the dependency for `a ASC` is `[c ASC]`, the dependency for `b ASC`
    // is `[d DESC]`, then we generate `[c ASC, d DESC, a + b ASC]` and
    // `[d DESC, c ASC, a + b ASC]`.
    relevant_prefixes
        .into_iter()
        .multi_cartesian_product()
        .flat_map(|prefix_orderings| {
            prefix_orderings
                .iter()
                .permutations(prefix_orderings.len())
                .map(|prefixes| prefixes.into_iter().flatten().cloned().collect())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// This function examines the given expression and the sort expressions it
/// refers to determine the ordering properties of the expression.
///
/// # Parameters
///
/// - `expr`: A reference to the source expression (`Arc<dyn PhysicalExpr>`) for
///   which ordering properties need to be determined.
/// - `dependencies`: A reference to `Dependencies`, containing sort expressions
///   referred to by `expr`.
///
/// # Returns
///
/// A `SortProperties` indicating the ordering information of the given expression.
fn get_expr_ordering(
    expr: &Arc<dyn PhysicalExpr>,
    dependencies: &Dependencies,
) -> SortProperties {
    if let Some(column_order) = dependencies.iter().find(|&order| expr.eq(&order.expr)) {
        // If exact match is found, return its ordering.
        SortProperties::Ordered(column_order.options)
    } else {
        // Find orderings of its children
        let child_states = expr
            .children()
            .iter()
            .map(|child| get_expr_ordering(child, dependencies))
            .collect::<Vec<_>>();
        // Calculate expression ordering using ordering of its children.
        expr.get_ordering(&child_states)
    }
}

/// Represents a node in the dependency map used to construct projected orderings.
///
/// A `DependencyNode` contains information about a particular sort expression,
/// including its target sort expression and a set of dependencies on other sort
/// expressions.
///
/// # Fields
///
/// - `target_sort_expr`: An optional `PhysicalSortExpr` representing the target
///   sort expression associated with the node. It is `None` if the sort expression
///   cannot be projected.
/// - `dependencies`: A [`Dependencies`] containing dependencies on other sort
///   expressions that are referred to by the target sort expression.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DependencyNode {
    target_sort_expr: Option<PhysicalSortExpr>,
    dependencies: Dependencies,
}

impl DependencyNode {
    // Insert dependency to the state (if exists).
    fn insert_dependency(&mut self, dependency: Option<&PhysicalSortExpr>) {
        if let Some(dep) = dependency {
            self.dependencies.insert(dep.clone());
        }
    }
}

type DependencyMap = HashMap<PhysicalSortExpr, DependencyNode>;
type Dependencies = HashSet<PhysicalSortExpr>;

/// This function recursively analyzes the dependencies of the given sort
/// expression within the given dependency map to construct lexicographical
/// orderings that include the sort expression and its dependencies.
///
/// # Parameters
///
/// - `referred_sort_expr`: A reference to the sort expression (`PhysicalSortExpr`)
///   for which lexicographical orderings satisfying its dependencies are to be
///   constructed.
/// - `dependency_map`: A reference to the `DependencyMap` that contains
///   dependencies for different `PhysicalSortExpr`s.
///
/// # Returns
///
/// A vector of lexicographical orderings (`Vec<LexOrdering>`) based on the given
/// sort expression and its dependencies.
fn construct_orderings(
    referred_sort_expr: &PhysicalSortExpr,
    dependency_map: &DependencyMap,
) -> Vec<LexOrdering> {
    // We are sure that `referred_sort_expr` is inside `dependency_map`.
    let node = &dependency_map[referred_sort_expr];
    // Since we work on intermediate nodes, we are sure `val.target_sort_expr`
    // exists.
    let target_sort_expr = node.target_sort_expr.clone().unwrap();
    if node.dependencies.is_empty() {
        vec![vec![target_sort_expr]]
    } else {
        node.dependencies
            .iter()
            .flat_map(|dep| {
                let mut orderings = construct_orderings(dep, dependency_map);
                for ordering in orderings.iter_mut() {
                    ordering.push(target_sort_expr.clone())
                }
                orderings
            })
            .collect()
    }
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

/// Wrapper struct for `Arc<dyn PhysicalExpr>` to use them as keys in a hash map.
#[derive(Debug, Clone)]
struct ExprWrapper(Arc<dyn PhysicalExpr>);

impl PartialEq<Self> for ExprWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for ExprWrapper {}

impl Hash for ExprWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

#[cfg(test)]
mod tests {}
