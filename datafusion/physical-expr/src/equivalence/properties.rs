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

use std::fmt;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

use super::ordering::collapse_lex_ordering;
use crate::equivalence::class::const_exprs_contains;
use crate::equivalence::{
    collapse_lex_req, EquivalenceClass, EquivalenceGroup, OrderingEquivalenceClass,
    ProjectionMapping,
};
use crate::expressions::{with_new_schema, CastExpr, Column, Literal};
use crate::{
    physical_exprs_contains, ConstExpr, LexOrdering, LexOrderingRef, LexRequirement,
    LexRequirementRef, PhysicalExpr, PhysicalExprRef, PhysicalSortExpr,
    PhysicalSortRequirement,
};

use arrow_schema::{SchemaRef, SortOptions};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{internal_err, plan_err, JoinSide, JoinType, Result};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_physical_expr_common::utils::ExprPropertiesNode;

use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;

/// A `EquivalenceProperties` object stores information known about the output
/// of a plan node, that can be used to optimize the plan.
///
/// Currently, it keeps track of:
/// - Sort expressions (orderings)
/// - Equivalent expressions: expressions that are known to have same value.
/// - Constants expressions: expressions that are known to contain a single
///   constant value.
///
/// # Example equivalent sort expressions
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
/// In this case, both `a ASC` and `b DESC` can describe the table ordering.
/// `EquivalenceProperties`, tracks these different valid sort expressions and
/// treat `a ASC` and `b DESC` on an equal footing. For example if the query
/// specifies the output sorted by EITHER `a ASC` or `b DESC`, the sort can be
/// avoided.
///
/// # Example equivalent expressions
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
/// In this case,  columns `a` and `b` always have the same value, which can of
/// such equivalences inside this object. With this information, Datafusion can
/// optimize operations such as. For example, if the partition requirement is
/// `Hash(a)` and output partitioning is `Hash(b)`, then DataFusion avoids
/// repartitioning the data as the existing partitioning satisfies the
/// requirement.
///
/// # Code Example
/// ```
/// # use std::sync::Arc;
/// # use arrow_schema::{Schema, Field, DataType, SchemaRef};
/// # use datafusion_physical_expr::{ConstExpr, EquivalenceProperties};
/// # use datafusion_physical_expr::expressions::col;
/// use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
/// # let schema: SchemaRef = Arc::new(Schema::new(vec![
/// #   Field::new("a", DataType::Int32, false),
/// #   Field::new("b", DataType::Int32, false),
/// #   Field::new("c", DataType::Int32, false),
/// # ]));
/// # let col_a = col("a", &schema).unwrap();
/// # let col_b = col("b", &schema).unwrap();
/// # let col_c = col("c", &schema).unwrap();
/// // This object represents data that is sorted by a ASC, c DESC
/// // with a single constant value of b
/// let mut eq_properties = EquivalenceProperties::new(schema)
///   .with_constants(vec![ConstExpr::from(col_b)]);
/// eq_properties.add_new_ordering(vec![
///   PhysicalSortExpr::new_default(col_a).asc(),
///   PhysicalSortExpr::new_default(col_c).desc(),
/// ]);
///
/// assert_eq!(eq_properties.to_string(), "order: [[a@0 ASC,c@2 DESC]], const: [b@1]")
/// ```
#[derive(Debug, Clone)]
pub struct EquivalenceProperties {
    /// Collection of equivalence classes that store expressions with the same
    /// value.
    pub eq_group: EquivalenceGroup,
    /// Equivalent sort expressions for this table.
    pub oeq_class: OrderingEquivalenceClass,
    /// Expressions whose values are constant throughout the table.
    /// TODO: We do not need to track constants separately, they can be tracked
    ///       inside `eq_groups` as `Literal` expressions.
    pub constants: Vec<ConstExpr>,
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
    pub fn constants(&self) -> &[ConstExpr] {
        &self.constants
    }

    /// Returns the output ordering of the properties.
    pub fn output_ordering(&self) -> Option<LexOrdering> {
        let constants = self.constants();
        let mut output_ordering = self.oeq_class().output_ordering().unwrap_or_default();
        // Prune out constant expressions
        output_ordering
            .retain(|sort_expr| !const_exprs_contains(constants, &sort_expr.expr));
        (!output_ordering.is_empty()).then_some(output_ordering)
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
        self.with_constants(other.constants)
    }

    /// Clears (empties) the ordering equivalence class within this object.
    /// Call this method when existing orderings are invalidated.
    pub fn clear_orderings(&mut self) {
        self.oeq_class.clear();
    }

    /// Removes constant expressions that may change across partitions.
    /// This method should be used when data from different partitions are merged.
    pub fn clear_per_partition_constants(&mut self) {
        self.constants.retain(|item| item.across_partitions());
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

    /// Adds a single ordering to the existing ordering equivalence class.
    pub fn add_new_ordering(&mut self, ordering: LexOrdering) {
        self.add_new_orderings([ordering]);
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
    ) -> Result<()> {
        // Discover new constants in light of new the equality:
        if self.is_expr_constant(left) {
            // Left expression is constant, add right as constant
            if !const_exprs_contains(&self.constants, right) {
                self.constants
                    .push(ConstExpr::from(right).with_across_partitions(true));
            }
        } else if self.is_expr_constant(right) {
            // Right expression is constant, add left as constant
            if !const_exprs_contains(&self.constants, left) {
                self.constants
                    .push(ConstExpr::from(left).with_across_partitions(true));
            }
        }

        // Add equal expressions to the state
        self.eq_group.add_equal_conditions(left, right);

        // Discover any new orderings
        self.discover_new_orderings(left)?;
        Ok(())
    }

    /// Track/register physical expressions with constant values.
    #[deprecated(since = "43.0.0", note = "Use [`with_constants`] instead")]
    pub fn add_constants(self, constants: impl IntoIterator<Item = ConstExpr>) -> Self {
        self.with_constants(constants)
    }

    /// Remove the specified constant
    pub fn remove_constant(mut self, c: &ConstExpr) -> Self {
        self.constants.retain(|existing| existing != c);
        self
    }

    /// Track/register physical expressions with constant values.
    pub fn with_constants(
        mut self,
        constants: impl IntoIterator<Item = ConstExpr>,
    ) -> Self {
        let (const_exprs, across_partition_flags): (
            Vec<Arc<dyn PhysicalExpr>>,
            Vec<bool>,
        ) = constants
            .into_iter()
            .map(|const_expr| {
                let across_partitions = const_expr.across_partitions();
                let expr = const_expr.owned_expr();
                (expr, across_partitions)
            })
            .unzip();
        for (expr, across_partitions) in self
            .eq_group
            .normalize_exprs(const_exprs)
            .into_iter()
            .zip(across_partition_flags)
        {
            if !const_exprs_contains(&self.constants, &expr) {
                let const_expr =
                    ConstExpr::from(expr).with_across_partitions(across_partitions);
                self.constants.push(const_expr);
            }
        }

        for ordering in self.normalized_oeq_class().iter() {
            if let Err(e) = self.discover_new_orderings(&ordering[0].expr) {
                log::debug!("error discovering new orderings: {e}");
            }
        }

        self
    }

    // Discover new valid orderings in light of a new equality.
    // Accepts a single argument (`expr`) which is used to determine
    // which orderings should be updated.
    // When constants or equivalence classes are changed, there may be new orderings
    // that can be discovered with the new equivalence properties.
    // For a discussion, see: https://github.com/apache/datafusion/issues/9812
    fn discover_new_orderings(&mut self, expr: &Arc<dyn PhysicalExpr>) -> Result<()> {
        let normalized_expr = self.eq_group().normalize_expr(Arc::clone(expr));
        let eq_class = self
            .eq_group
            .classes
            .iter()
            .find_map(|class| {
                class
                    .contains(&normalized_expr)
                    .then(|| class.clone().into_vec())
            })
            .unwrap_or_else(|| vec![Arc::clone(&normalized_expr)]);

        let mut new_orderings: Vec<LexOrdering> = vec![];
        for (ordering, next_expr) in self
            .normalized_oeq_class()
            .iter()
            .filter(|ordering| ordering[0].expr.eq(&normalized_expr))
            // First expression after leading ordering
            .filter_map(|ordering| Some(ordering).zip(ordering.get(1)))
        {
            let leading_ordering = ordering[0].options;
            // Currently, we only handle expressions with a single child.
            // TODO: It should be possible to handle expressions orderings like
            //       f(a, b, c), a, b, c if f is monotonic in all arguments.
            for equivalent_expr in &eq_class {
                let children = equivalent_expr.children();
                if children.len() == 1
                    && children[0].eq(&next_expr.expr)
                    && SortProperties::Ordered(leading_ordering)
                        == equivalent_expr
                            .get_properties(&[ExprProperties {
                                sort_properties: SortProperties::Ordered(
                                    leading_ordering,
                                ),
                                range: Interval::make_unbounded(
                                    &equivalent_expr.data_type(&self.schema)?,
                                )?,
                            }])?
                            .sort_properties
                {
                    // Assume existing ordering is [a ASC, b ASC]
                    // When equality a = f(b) is given, If we know that given ordering `[b ASC]`, ordering `[f(b) ASC]` is valid,
                    // then we can deduce that ordering `[b ASC]` is also valid.
                    // Hence, ordering `[b ASC]` can be added to the state as valid ordering.
                    // (e.g. existing ordering where leading ordering is removed)
                    new_orderings.push(ordering[1..].to_vec());
                    break;
                }
            }
        }

        self.oeq_class.add_new_orderings(new_orderings);
        Ok(())
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
        let mut constant_exprs = vec![];
        constant_exprs.extend(
            self.constants
                .iter()
                .map(|const_expr| Arc::clone(const_expr.expr())),
        );
        let constants_normalized = self.eq_group.normalize_exprs(constant_exprs);
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
            eq_properties = eq_properties
                .with_constants(std::iter::once(ConstExpr::from(normalized_req.expr)));
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
        let ExprProperties {
            sort_properties, ..
        } = self.get_expr_properties(Arc::clone(&req.expr));
        match sort_properties {
            SortProperties::Ordered(options) => {
                let sort_expr = PhysicalSortExpr {
                    expr: Arc::clone(&req.expr),
                    options,
                };
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
        lhs.inner
            .iter_mut()
            .zip(rhs.inner.iter_mut())
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

    /// we substitute the ordering according to input expression type, this is a simplified version
    /// In this case, we just substitute when the expression satisfy the following condition:
    /// I. just have one column and is a CAST expression
    /// TODO: Add one-to-ones analysis for monotonic ScalarFunctions.
    /// TODO: we could precompute all the scenario that is computable, for example: atan(x + 1000) should also be substituted if
    ///  x is DESC or ASC
    /// After substitution, we may generate more than 1 `LexOrdering`. As an example,
    /// `[a ASC, b ASC]` will turn into `[a ASC, b ASC], [CAST(a) ASC, b ASC]` when projection expressions `a, b, CAST(a)` is applied.
    pub fn substitute_ordering_component(
        &self,
        mapping: &ProjectionMapping,
        sort_expr: &[PhysicalSortExpr],
    ) -> Result<Vec<Vec<PhysicalSortExpr>>> {
        let new_orderings = sort_expr
            .iter()
            .map(|sort_expr| {
                let referring_exprs: Vec<_> = mapping
                    .iter()
                    .map(|(source, _target)| source)
                    .filter(|source| expr_refers(source, &sort_expr.expr))
                    .cloned()
                    .collect();
                let mut res = vec![sort_expr.clone()];
                // TODO: Add one-to-ones analysis for ScalarFunctions.
                for r_expr in referring_exprs {
                    // we check whether this expression is substitutable or not
                    if let Some(cast_expr) = r_expr.as_any().downcast_ref::<CastExpr>() {
                        // we need to know whether the Cast Expr matches or not
                        let expr_type = sort_expr.expr.data_type(&self.schema)?;
                        if cast_expr.expr.eq(&sort_expr.expr)
                            && cast_expr.is_bigger_cast(expr_type)
                        {
                            res.push(PhysicalSortExpr {
                                expr: Arc::clone(&r_expr),
                                options: sort_expr.options,
                            });
                        }
                    }
                }
                Ok(res)
            })
            .collect::<Result<Vec<_>>>()?;
        // Generate all valid orderings, given substituted expressions.
        let res = new_orderings
            .into_iter()
            .multi_cartesian_product()
            .collect::<Vec<_>>();
        Ok(res)
    }

    /// In projection, supposed we have a input function 'A DESC B DESC' and the output shares the same expression
    /// with A and B, we could surely use the ordering of the original ordering, However, if the A has been changed,
    /// for example, A-> Cast(A, Int64) or any other form, it is invalid if we continue using the original ordering
    /// Since it would cause bug in dependency constructions, we should substitute the input order in order to get correct
    /// dependency map, happen in issue 8838: <https://github.com/apache/datafusion/issues/8838>
    pub fn substitute_oeq_class(&mut self, mapping: &ProjectionMapping) -> Result<()> {
        let orderings = &self.oeq_class.orderings;
        let new_order = orderings
            .iter()
            .map(|order| self.substitute_ordering_component(mapping, order))
            .collect::<Result<Vec<_>>>()?;
        let new_order = new_order.into_iter().flatten().collect();
        self.oeq_class = OrderingEquivalenceClass::new(new_order);
        Ok(())
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
        let mut dependency_map = DependencyMap::new();
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
                    dependency_map.insert(
                        sort_expr,
                        target_sort_expr.as_ref(),
                        dependency,
                    );
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
                    let normalized_source =
                        self.eq_group.normalize_expr(Arc::clone(source));
                    (normalized_source, Arc::clone(target))
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
                    if let Ok(SortProperties::Ordered(options)) =
                        get_expr_properties(source, &relevant_deps, &self.schema)
                            .map(|prop| prop.sort_properties)
                    {
                        Some((options, relevant_deps))
                    } else {
                        // Do not consider unordered cases
                        None
                    }
                })
                .flat_map(|(options, relevant_deps)| {
                    let sort_expr = PhysicalSortExpr {
                        expr: Arc::clone(target),
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
    fn projected_constants(&self, mapping: &ProjectionMapping) -> Vec<ConstExpr> {
        // First, project existing constants. For example, assume that `a + b`
        // is known to be constant. If the projection were `a as a_new`, `b as b_new`,
        // then we would project constant `a + b` as `a_new + b_new`.
        let mut projected_constants = self
            .constants
            .iter()
            .flat_map(|const_expr| {
                const_expr.map(|expr| self.eq_group.project_expr(mapping, expr))
            })
            .collect::<Vec<_>>();
        // Add projection expressions that are known to be constant:
        for (source, target) in mapping.iter() {
            if self.is_expr_constant(source)
                && !const_exprs_contains(&projected_constants, target)
            {
                // Expression evaluates to single value
                projected_constants
                    .push(ConstExpr::from(target).with_across_partitions(true));
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
                    let ExprProperties {
                        sort_properties, ..
                    } = eq_properties.get_expr_properties(Arc::clone(&exprs[idx]));
                    match sort_properties {
                        SortProperties::Ordered(options) => Some((
                            PhysicalSortExpr {
                                expr: Arc::clone(&exprs[idx]),
                                options,
                            },
                            idx,
                        )),
                        SortProperties::Singleton => {
                            // Assign default ordering to constant expressions
                            let options = SortOptions::default();
                            Some((
                                PhysicalSortExpr {
                                    expr: Arc::clone(&exprs[idx]),
                                    options,
                                },
                                idx,
                            ))
                        }
                        SortProperties::Unordered => None,
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
                    eq_properties.with_constants(std::iter::once(ConstExpr::from(expr)));
                search_indices.shift_remove(idx);
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
    pub fn is_expr_constant(&self, expr: &Arc<dyn PhysicalExpr>) -> bool {
        // As an example, assume that we know columns `a` and `b` are constant.
        // Then, `a`, `b` and `a + b` will all return `true` whereas `c` will
        // return `false`.
        let const_exprs = self
            .constants
            .iter()
            .map(|const_expr| Arc::clone(const_expr.expr()));
        let normalized_constants = self.eq_group.normalize_exprs(const_exprs);
        let normalized_expr = self.eq_group.normalize_expr(Arc::clone(expr));
        is_constant_recurse(&normalized_constants, &normalized_expr)
    }

    /// Retrieves the properties for a given physical expression.
    ///
    /// This function constructs an [`ExprProperties`] object for the given
    /// expression, which encapsulates information about the expression's
    /// properties, including its [`SortProperties`] and [`Interval`].
    ///
    /// # Parameters
    ///
    /// - `expr`: An `Arc<dyn PhysicalExpr>` representing the physical expression
    ///   for which ordering information is sought.
    ///
    /// # Returns
    ///
    /// Returns an [`ExprProperties`] object containing the ordering and range
    /// information for the given expression.
    pub fn get_expr_properties(&self, expr: Arc<dyn PhysicalExpr>) -> ExprProperties {
        ExprPropertiesNode::new_unknown(expr)
            .transform_up(|expr| update_properties(expr, self))
            .data()
            .map(|node| node.data)
            .unwrap_or(ExprProperties::new_unknown())
    }

    /// Transforms this `EquivalenceProperties` into a new `EquivalenceProperties`
    /// by mapping columns in the original schema to columns in the new schema
    /// by index.
    pub fn with_new_schema(self, schema: SchemaRef) -> Result<Self> {
        // The new schema and the original schema is aligned when they have the
        // same number of columns, and fields at the same index have the same
        // type in both schemas.
        let schemas_aligned = (self.schema.fields.len() == schema.fields.len())
            && self
                .schema
                .fields
                .iter()
                .zip(schema.fields.iter())
                .all(|(lhs, rhs)| lhs.data_type().eq(rhs.data_type()));
        if !schemas_aligned {
            // Rewriting equivalence properties in terms of new schema is not
            // safe when schemas are not aligned:
            return plan_err!(
                "Cannot rewrite old_schema:{:?} with new schema: {:?}",
                self.schema,
                schema
            );
        }
        // Rewrite constants according to new schema:
        let new_constants = self
            .constants
            .into_iter()
            .map(|const_expr| {
                let across_partitions = const_expr.across_partitions();
                let new_const_expr = with_new_schema(const_expr.owned_expr(), &schema)?;
                Ok(ConstExpr::new(new_const_expr)
                    .with_across_partitions(across_partitions))
            })
            .collect::<Result<Vec<_>>>()?;

        // Rewrite orderings according to new schema:
        let mut new_orderings = vec![];
        for ordering in self.oeq_class.orderings {
            let new_ordering = ordering
                .into_iter()
                .map(|mut sort_expr| {
                    sort_expr.expr = with_new_schema(sort_expr.expr, &schema)?;
                    Ok(sort_expr)
                })
                .collect::<Result<_>>()?;
            new_orderings.push(new_ordering);
        }

        // Rewrite equivalence classes according to the new schema:
        let mut eq_classes = vec![];
        for eq_class in self.eq_group.classes {
            let new_eq_exprs = eq_class
                .into_vec()
                .into_iter()
                .map(|expr| with_new_schema(expr, &schema))
                .collect::<Result<_>>()?;
            eq_classes.push(EquivalenceClass::new(new_eq_exprs));
        }

        // Construct the resulting equivalence properties:
        let mut result = EquivalenceProperties::new(schema);
        result.constants = new_constants;
        result.add_new_orderings(new_orderings);
        result.add_equivalence_group(EquivalenceGroup::new(eq_classes));

        Ok(result)
    }
}

/// More readable display version of the `EquivalenceProperties`.
///
/// Format:
/// ```text
/// order: [[a ASC, b ASC], [a ASC, c ASC]], eq: [[a = b], [a = c]], const: [a = 1]
/// ```
impl Display for EquivalenceProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.eq_group.is_empty()
            && self.oeq_class.is_empty()
            && self.constants.is_empty()
        {
            return write!(f, "No properties");
        }
        if !self.oeq_class.is_empty() {
            write!(f, "order: {}", self.oeq_class)?;
        }
        if !self.eq_group.is_empty() {
            write!(f, ", eq: {}", self.eq_group)?;
        }
        if !self.constants.is_empty() {
            write!(f, ", const: [{}]", ConstExpr::format_list(&self.constants))?;
        }
        Ok(())
    }
}

/// Calculates the properties of a given [`ExprPropertiesNode`].
///
/// Order information can be retrieved as:
/// - If it is a leaf node, we directly find the order of the node by looking
///   at the given sort expression and equivalence properties if it is a `Column`
///   leaf, or we mark it as unordered. In the case of a `Literal` leaf, we mark
///   it as singleton so that it can cooperate with all ordered columns.
/// - If it is an intermediate node, the children states matter. Each `PhysicalExpr`
///   and operator has its own rules on how to propagate the children orderings.
///   However, before we engage in recursion, we check whether this intermediate
///   node directly matches with the sort expression. If there is a match, the
///   sort expression emerges at that node immediately, discarding the recursive
///   result coming from its children.
///
/// Range information is calculated as:
/// - If it is a `Literal` node, we set the range as a point value. If it is a
///   `Column` node, we set the datatype of the range, but cannot give an interval
///   for the range, yet.
/// - If it is an intermediate node, the children states matter. Each `PhysicalExpr`
///   and operator has its own rules on how to propagate the children range.
fn update_properties(
    mut node: ExprPropertiesNode,
    eq_properties: &EquivalenceProperties,
) -> Result<Transformed<ExprPropertiesNode>> {
    // First, try to gather the information from the children:
    if !node.expr.children().is_empty() {
        // We have an intermediate (non-leaf) node, account for its children:
        let children_props = node.children.iter().map(|c| c.data.clone()).collect_vec();
        node.data = node.expr.get_properties(&children_props)?;
    } else if node.expr.as_any().is::<Literal>() {
        // We have a Literal, which is one of the two possible leaf node types:
        node.data = node.expr.get_properties(&[])?;
    } else if node.expr.as_any().is::<Column>() {
        // We have a Column, which is the other possible leaf node type:
        node.data.range =
            Interval::make_unbounded(&node.expr.data_type(eq_properties.schema())?)?
    }
    // Now, check what we know about orderings:
    let normalized_expr = eq_properties
        .eq_group
        .normalize_expr(Arc::clone(&node.expr));
    if eq_properties.is_expr_constant(&normalized_expr) {
        node.data.sort_properties = SortProperties::Singleton;
    } else if let Some(options) = eq_properties
        .normalized_oeq_class()
        .get_options(&normalized_expr)
    {
        node.data.sort_properties = SortProperties::Ordered(options);
    }
    Ok(Transformed::yes(node))
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
    if physical_exprs_contains(constants, expr) || expr.as_any().is::<Literal>() {
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
    let mut expr_to_sort_exprs = IndexMap::<ExprWrapper, Dependencies>::new();
    for sort_expr in dependency_map
        .sort_exprs()
        .filter(|sort_expr| expr_refers(source, &sort_expr.expr))
    {
        let key = ExprWrapper(Arc::clone(&sort_expr.expr));
        expr_to_sort_exprs
            .entry(key)
            .or_default()
            .insert(sort_expr.clone());
    }

    // Generate all valid dependencies for the source. For example, if the source
    // is `a + b` and the map is `[a -> (a ASC, a DESC), b -> (b ASC)]`, we get
    // `vec![HashSet(a ASC, b ASC), HashSet(a DESC, b ASC)]`.
    let dependencies = expr_to_sort_exprs
        .into_values()
        .map(Dependencies::into_inner)
        .collect::<Vec<_>>();
    dependencies
        .iter()
        .multi_cartesian_product()
        .map(|referred_deps| {
            Dependencies::new_from_iter(referred_deps.into_iter().cloned())
        })
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
    let mut dep_enumerator = DependencyEnumerator::new();
    dependency_map
        .get(relevant_sort_expr)
        .expect("no relevant sort expr found")
        .dependencies
        .iter()
        .flat_map(|dep| dep_enumerator.construct_orderings(dep, dependency_map))
        .collect()
}

/// Generates all possible orderings where dependencies are satisfied for the
/// current projection expression.
///
/// # Examaple
///  If `dependences` is `a + b ASC` and the dependency map holds dependencies
///  * `a ASC` --> `[c ASC]`
///  * `b ASC` --> `[d DESC]`,
///
/// This function generates these two sort orders
/// * `[c ASC, d DESC, a + b ASC]`
/// * `[d DESC, c ASC, a + b ASC]`
///
/// # Parameters
///
/// * `dependencies` - Set of relevant expressions.
/// * `dependency_map` - Map of dependencies for expressions that may appear in `dependencies`
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

/// This function examines the given expression and its properties to determine
/// the ordering properties of the expression. The range knowledge is not utilized
/// yet in the scope of this function.
///
/// # Parameters
///
/// - `expr`: A reference to the source expression (`Arc<dyn PhysicalExpr>`) for
///   which ordering properties need to be determined.
/// - `dependencies`: A reference to `Dependencies`, containing sort expressions
///   referred to by `expr`.
/// - `schema``: A reference to the schema which the `expr` columns refer.
///
/// # Returns
///
/// A `SortProperties` indicating the ordering information of the given expression.
fn get_expr_properties(
    expr: &Arc<dyn PhysicalExpr>,
    dependencies: &Dependencies,
    schema: &SchemaRef,
) -> Result<ExprProperties> {
    if let Some(column_order) = dependencies.iter().find(|&order| expr.eq(&order.expr)) {
        // If exact match is found, return its ordering.
        Ok(ExprProperties {
            sort_properties: SortProperties::Ordered(column_order.options),
            range: Interval::make_unbounded(&expr.data_type(schema)?)?,
        })
    } else if expr.as_any().downcast_ref::<Column>().is_some() {
        Ok(ExprProperties {
            sort_properties: SortProperties::Unordered,
            range: Interval::make_unbounded(&expr.data_type(schema)?)?,
        })
    } else if let Some(literal) = expr.as_any().downcast_ref::<Literal>() {
        Ok(ExprProperties {
            sort_properties: SortProperties::Singleton,
            range: Interval::try_new(literal.value().clone(), literal.value().clone())?,
        })
    } else {
        // Find orderings of its children
        let child_states = expr
            .children()
            .iter()
            .map(|child| get_expr_properties(child, dependencies, schema))
            .collect::<Result<Vec<_>>>()?;
        // Calculate expression ordering using ordering of its children.
        expr.get_properties(&child_states)
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
    /// Insert dependency to the state (if exists).
    fn insert_dependency(&mut self, dependency: Option<&PhysicalSortExpr>) {
        if let Some(dep) = dependency {
            self.dependencies.insert(dep.clone());
        }
    }
}

impl Display for DependencyNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(target) = &self.target_sort_expr {
            write!(f, "(target: {}, ", target)?;
        } else {
            write!(f, "(")?;
        }
        write!(f, "dependencies: [{}])", self.dependencies)
    }
}

/// Maps an expression --> DependencyNode
///
/// # Debugging / deplaying `DependencyMap`
///
/// This structure implements `Display` to assist debugging. For example:
///
/// ```text
/// DependencyMap: {
///   a@0 ASC --> (target: a@0 ASC, dependencies: [[]])
///   b@1 ASC --> (target: b@1 ASC, dependencies: [[a@0 ASC, c@2 ASC]])
///   c@2 ASC --> (target: c@2 ASC, dependencies: [[b@1 ASC, a@0 ASC]])
///   d@3 ASC --> (target: d@3 ASC, dependencies: [[c@2 ASC, b@1 ASC]])
/// }
/// ```
///
/// # Note on IndexMap Rationale
///
/// Using `IndexMap` (which preserves insert order) to ensure consistent results
/// across different executions for the same query. We could have used
/// `HashSet`, `HashMap` in place of them without any loss of functionality.
///
/// As an example, if existing orderings are
/// 1. `[a ASC, b ASC]`
/// 2. `[c ASC]` for
///
/// Then both the following output orderings are valid
/// 1. `[a ASC, b ASC, c ASC]`
/// 2. `[c ASC, a ASC, b ASC]`
///
/// (this are both valid as they are concatenated versions of the alternative
/// orderings). When using `HashSet`, `HashMap` it is not guaranteed to generate
/// consistent result, among the possible 2 results in the example above.
#[derive(Debug)]
struct DependencyMap {
    inner: IndexMap<PhysicalSortExpr, DependencyNode>,
}

impl DependencyMap {
    fn new() -> Self {
        Self {
            inner: IndexMap::new(),
        }
    }

    /// Insert a new dependency `sort_expr` --> `dependency` into the map.
    ///
    /// If `target_sort_expr` is none, a new entry is created with empty dependencies.
    fn insert(
        &mut self,
        sort_expr: &PhysicalSortExpr,
        target_sort_expr: Option<&PhysicalSortExpr>,
        dependency: Option<&PhysicalSortExpr>,
    ) {
        self.inner
            .entry(sort_expr.clone())
            .or_insert_with(|| DependencyNode {
                target_sort_expr: target_sort_expr.cloned(),
                dependencies: Dependencies::new(),
            })
            .insert_dependency(dependency)
    }

    /// Iterator over (sort_expr, DependencyNode) pairs
    fn iter(&self) -> impl Iterator<Item = (&PhysicalSortExpr, &DependencyNode)> {
        self.inner.iter()
    }

    /// iterator over all sort exprs
    fn sort_exprs(&self) -> impl Iterator<Item = &PhysicalSortExpr> {
        self.inner.keys()
    }

    /// Return the dependency node for the given sort expression, if any
    fn get(&self, sort_expr: &PhysicalSortExpr) -> Option<&DependencyNode> {
        self.inner.get(sort_expr)
    }
}

impl Display for DependencyMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "DependencyMap: {{")?;
        for (sort_expr, node) in self.inner.iter() {
            writeln!(f, "  {sort_expr} --> {node}")?;
        }
        writeln!(f, "}}")
    }
}

/// A list of sort expressions that can be calculated from a known set of
/// dependencies.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Dependencies {
    inner: IndexSet<PhysicalSortExpr>,
}

impl Display for Dependencies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        let mut iter = self.inner.iter();
        if let Some(dep) = iter.next() {
            write!(f, "{}", dep)?;
        }
        for dep in iter {
            write!(f, ", {}", dep)?;
        }
        write!(f, "]")
    }
}

impl Dependencies {
    /// Create a new empty `Dependencies` instance.
    fn new() -> Self {
        Self {
            inner: IndexSet::new(),
        }
    }

    /// Create a new `Dependencies` from an iterator of `PhysicalSortExpr`.
    fn new_from_iter(iter: impl IntoIterator<Item = PhysicalSortExpr>) -> Self {
        Self {
            inner: iter.into_iter().collect(),
        }
    }

    /// Insert a new dependency into the set.
    fn insert(&mut self, sort_expr: PhysicalSortExpr) {
        self.inner.insert(sort_expr);
    }

    /// Iterator over  dependencies in the set
    fn iter(&self) -> impl Iterator<Item = &PhysicalSortExpr> + Clone {
        self.inner.iter()
    }

    /// Return the inner set of dependencies
    fn into_inner(self) -> IndexSet<PhysicalSortExpr> {
        self.inner
    }

    /// Returns true if there are no dependencies
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// Contains a mapping of all dependencies we have processed for each sort expr
struct DependencyEnumerator<'a> {
    /// Maps `expr` --> `[exprs]` that have previously been processed
    seen: IndexMap<&'a PhysicalSortExpr, IndexSet<&'a PhysicalSortExpr>>,
}

impl<'a> DependencyEnumerator<'a> {
    fn new() -> Self {
        Self {
            seen: IndexMap::new(),
        }
    }

    /// Insert a new dependency,
    ///
    /// returns false if the dependency was already in the map
    /// returns true if the dependency was newly inserted
    fn insert(
        &mut self,
        target: &'a PhysicalSortExpr,
        dep: &'a PhysicalSortExpr,
    ) -> bool {
        self.seen.entry(target).or_default().insert(dep)
    }

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
        &mut self,
        referred_sort_expr: &'a PhysicalSortExpr,
        dependency_map: &'a DependencyMap,
    ) -> Vec<LexOrdering> {
        let node = dependency_map
            .get(referred_sort_expr)
            .expect("`referred_sort_expr` should be inside `dependency_map`");
        // Since we work on intermediate nodes, we are sure `val.target_sort_expr`
        // exists.
        let target_sort_expr = node.target_sort_expr.as_ref().unwrap();
        // An empty dependency means the referred_sort_expr represents a global ordering.
        // Return its projected version, which is the target_expression.
        if node.dependencies.is_empty() {
            return vec![vec![target_sort_expr.clone()]];
        };

        node.dependencies
            .iter()
            .flat_map(|dep| {
                let mut orderings = if self.insert(target_sort_expr, dep) {
                    self.construct_orderings(dep, dependency_map)
                } else {
                    vec![]
                };

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
    on: &[(PhysicalExprRef, PhysicalExprRef)],
) -> EquivalenceProperties {
    let left_size = left.schema.fields.len();
    let mut result = EquivalenceProperties::new(join_schema);
    result.add_equivalence_group(left.eq_group().join(
        right.eq_group(),
        join_type,
        left_size,
        on,
    ));

    let EquivalenceProperties {
        constants: left_constants,
        oeq_class: left_oeq_class,
        ..
    } = left;
    let EquivalenceProperties {
        constants: right_constants,
        oeq_class: mut right_oeq_class,
        ..
    } = right;
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
    match join_type {
        JoinType::LeftAnti | JoinType::LeftSemi => {
            result = result.with_constants(left_constants);
        }
        JoinType::RightAnti | JoinType::RightSemi => {
            result = result.with_constants(right_constants);
        }
        _ => {}
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

/// Calculates the union (in the sense of `UnionExec`) `EquivalenceProperties`
/// of  `lhs` and `rhs` according to the schema of `lhs`.
///
/// Rules: The UnionExec does not interleave its inputs: instead it passes each
/// input partition from the children as its own output.
///
/// Since the output equivalence properties are properties that are true for
/// *all* output partitions, that is the same as being true for all *input*
/// partitions
fn calculate_union_binary(
    mut lhs: EquivalenceProperties,
    mut rhs: EquivalenceProperties,
) -> Result<EquivalenceProperties> {
    // Harmonize the schema of the rhs with the schema of the lhs (which is the accumulator schema):
    if !rhs.schema.eq(&lhs.schema) {
        rhs = rhs.with_new_schema(Arc::clone(&lhs.schema))?;
    }

    // First, calculate valid constants for the union. An expression is constant
    // at the output of the union if it is constant in both sides.
    let constants: Vec<_> = lhs
        .constants()
        .iter()
        .filter(|const_expr| const_exprs_contains(rhs.constants(), const_expr.expr()))
        .map(|const_expr| {
            // TODO: When both sides have a constant column, and the actual
            // constant value is the same, then the output properties could
            // reflect the constant is valid across all partitions. However we
            // don't track the actual value that the ConstExpr takes on, so we
            // can't determine that yet
            ConstExpr::new(Arc::clone(const_expr.expr())).with_across_partitions(false)
        })
        .collect();

    // remove any constants that are shared in both outputs (avoid double counting them)
    for c in &constants {
        lhs = lhs.remove_constant(c);
        rhs = rhs.remove_constant(c);
    }

    // Next, calculate valid orderings for the union by searching for prefixes
    // in both sides.
    let mut orderings = UnionEquivalentOrderingBuilder::new();
    orderings.add_satisfied_orderings(
        lhs.normalized_oeq_class().orderings,
        lhs.constants(),
        &rhs,
    );
    orderings.add_satisfied_orderings(
        rhs.normalized_oeq_class().orderings,
        rhs.constants(),
        &lhs,
    );
    let orderings = orderings.build();

    let mut eq_properties =
        EquivalenceProperties::new(lhs.schema).with_constants(constants);

    eq_properties.add_new_orderings(orderings);
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

    /// Add all orderings from `orderings` that satisfy `properties`,
    /// potentially augmented with`constants`.
    ///
    /// Note: any column that is known to be constant can be inserted into the
    /// ordering without changing its meaning
    ///
    /// For example:
    /// * `orderings` contains `[a ASC, c ASC]` and `constants` contains `b`
    /// * `properties` has required ordering `[a ASC, b ASC]`
    ///
    /// Then this will add `[a ASC, b ASC]` to the `orderings` list (as `a` was
    /// in the sort order and `b` was a constant).
    fn add_satisfied_orderings(
        &mut self,
        orderings: impl IntoIterator<Item = LexOrdering>,
        constants: &[ConstExpr],
        properties: &EquivalenceProperties,
    ) {
        for mut ordering in orderings.into_iter() {
            // Progressively shorten the ordering to search for a satisfied prefix:
            loop {
                match self.try_add_ordering(ordering, constants, properties) {
                    AddedOrdering::Yes => break,
                    AddedOrdering::No(o) => {
                        ordering = o;
                        ordering.pop();
                    }
                }
            }
        }
    }

    /// Adds `ordering`, potentially augmented with constants, if it satisfies
    /// the target `properties` properties.
    ///
    /// Returns
    ///
    /// * [`AddedOrdering::Yes`] if the ordering was added (either directly or
    ///   augmented), or was empty.
    ///
    /// * [`AddedOrdering::No`] if the ordering was not added
    fn try_add_ordering(
        &mut self,
        ordering: LexOrdering,
        constants: &[ConstExpr],
        properties: &EquivalenceProperties,
    ) -> AddedOrdering {
        if ordering.is_empty() {
            AddedOrdering::Yes
        } else if constants.is_empty() && properties.ordering_satisfy(&ordering) {
            // If the ordering satisfies the target properties, no need to
            // augment it with constants.
            self.orderings.push(ordering);
            AddedOrdering::Yes
        } else {
            // Did not satisfy target properties, try and augment with constants
            //  to match the properties
            if self.try_find_augmented_ordering(&ordering, constants, properties) {
                AddedOrdering::Yes
            } else {
                AddedOrdering::No(ordering)
            }
        }
    }

    /// Attempts to add `constants` to `ordering` to satisfy the properties.
    ///
    /// returns true if any orderings were added, false otherwise
    fn try_find_augmented_ordering(
        &mut self,
        ordering: &LexOrdering,
        constants: &[ConstExpr],
        properties: &EquivalenceProperties,
    ) -> bool {
        // can't augment if there is nothing to augment with
        if constants.is_empty() {
            return false;
        }
        let start_num_orderings = self.orderings.len();

        // for each equivalent ordering in properties, try and augment
        // `ordering` it with the constants to match
        for existing_ordering in &properties.oeq_class.orderings {
            if let Some(augmented_ordering) = self.augment_ordering(
                ordering,
                constants,
                existing_ordering,
                &properties.constants,
            ) {
                if !augmented_ordering.is_empty() {
                    assert!(properties.ordering_satisfy(&augmented_ordering));
                    self.orderings.push(augmented_ordering);
                }
            }
        }

        self.orderings.len() > start_num_orderings
    }

    /// Attempts to augment the ordering with constants to match the
    /// `existing_ordering`
    ///
    /// Returns Some(ordering) if an augmented ordering was found, None otherwise
    fn augment_ordering(
        &mut self,
        ordering: &LexOrdering,
        constants: &[ConstExpr],
        existing_ordering: &LexOrdering,
        existing_constants: &[ConstExpr],
    ) -> Option<LexOrdering> {
        let mut augmented_ordering = vec![];
        let mut sort_expr_iter = ordering.iter().peekable();
        let mut existing_sort_expr_iter = existing_ordering.iter().peekable();

        // walk in parallel down the two orderings, trying to match them up
        while sort_expr_iter.peek().is_some() || existing_sort_expr_iter.peek().is_some()
        {
            // If the next expressions are equal, add the next match
            // otherwise try and match with a constant
            if let Some(expr) =
                advance_if_match(&mut sort_expr_iter, &mut existing_sort_expr_iter)
            {
                augmented_ordering.push(expr);
            } else if let Some(expr) =
                advance_if_matches_constant(&mut sort_expr_iter, existing_constants)
            {
                augmented_ordering.push(expr);
            } else if let Some(expr) =
                advance_if_matches_constant(&mut existing_sort_expr_iter, constants)
            {
                augmented_ordering.push(expr);
            } else {
                // no match, can't continue the ordering, return what we have
                break;
            }
        }

        Some(augmented_ordering)
    }

    fn build(self) -> Vec<LexOrdering> {
        self.orderings
    }
}

/// Advances two iterators in parallel
///
/// If the next expressions are equal, the iterators are advanced and returns
/// the matched expression .
///
/// Otherwise, the iterators are left unchanged and return `None`
fn advance_if_match(
    iter1: &mut Peekable<Iter<PhysicalSortExpr>>,
    iter2: &mut Peekable<Iter<PhysicalSortExpr>>,
) -> Option<PhysicalSortExpr> {
    if matches!((iter1.peek(), iter2.peek()), (Some(expr1), Some(expr2)) if expr1.eq(expr2))
    {
        iter1.next().unwrap();
        iter2.next().cloned()
    } else {
        None
    }
}

/// Advances the iterator with a constant
///
/// If the next expression  matches one of the constants, advances the iterator
/// returning the matched expression
///
/// Otherwise, the iterator is left unchanged and returns `None`
fn advance_if_matches_constant(
    iter: &mut Peekable<Iter<PhysicalSortExpr>>,
    constants: &[ConstExpr],
) -> Option<PhysicalSortExpr> {
    let expr = iter.peek()?;
    let const_expr = constants.iter().find(|c| c.eq_expr(expr))?;
    let found_expr = PhysicalSortExpr::new(Arc::clone(const_expr.expr()), expr.options);
    iter.next();
    Some(found_expr)
}

#[cfg(test)]
mod tests {
    use std::ops::Not;

    use super::*;
    use crate::equivalence::add_offset_to_expr;
    use crate::equivalence::tests::{
        convert_to_orderings, convert_to_sort_exprs, convert_to_sort_reqs,
        create_test_params, create_test_schema, output_schema,
    };
    use crate::expressions::{col, BinaryExpr, Column};

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_schema::{Fields, TimeUnit};
    use datafusion_expr::Operator;

    #[test]
    fn project_equivalence_properties_test() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));

        let input_properties = EquivalenceProperties::new(Arc::clone(&input_schema));
        let col_a = col("a", &input_schema)?;

        // a as a1, a as a2, a as a3, a as a3
        let proj_exprs = vec![
            (Arc::clone(&col_a), "a1".to_string()),
            (Arc::clone(&col_a), "a2".to_string()),
            (Arc::clone(&col_a), "a3".to_string()),
            (Arc::clone(&col_a), "a4".to_string()),
        ];
        let projection_mapping = ProjectionMapping::try_new(&proj_exprs, &input_schema)?;

        let out_schema = output_schema(&projection_mapping, &input_schema)?;
        // a as a1, a as a2, a as a3, a as a3
        let proj_exprs = vec![
            (Arc::clone(&col_a), "a1".to_string()),
            (Arc::clone(&col_a), "a2".to_string()),
            (Arc::clone(&col_a), "a3".to_string()),
            (Arc::clone(&col_a), "a4".to_string()),
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
    fn project_equivalence_properties_test_multi() -> Result<()> {
        // test multiple input orderings with equivalence properties
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("d", DataType::Int64, true),
        ]));

        let mut input_properties = EquivalenceProperties::new(Arc::clone(&input_schema));
        // add equivalent ordering [a, b, c, d]
        input_properties.add_new_ordering(vec![
            parse_sort_expr("a", &input_schema),
            parse_sort_expr("b", &input_schema),
            parse_sort_expr("c", &input_schema),
            parse_sort_expr("d", &input_schema),
        ]);

        // add equivalent ordering [a, c, b, d]
        input_properties.add_new_ordering(vec![
            parse_sort_expr("a", &input_schema),
            parse_sort_expr("c", &input_schema),
            parse_sort_expr("b", &input_schema), // NB b and c are swapped
            parse_sort_expr("d", &input_schema),
        ]);

        // simply project all the columns in order
        let proj_exprs = vec![
            (col("a", &input_schema)?, "a".to_string()),
            (col("b", &input_schema)?, "b".to_string()),
            (col("c", &input_schema)?, "c".to_string()),
            (col("d", &input_schema)?, "d".to_string()),
        ];
        let projection_mapping = ProjectionMapping::try_new(&proj_exprs, &input_schema)?;
        let out_properties = input_properties.project(&projection_mapping, input_schema);

        assert_eq!(
            out_properties.to_string(),
            "order: [[a@0 ASC,c@2 ASC,b@1 ASC,d@3 ASC], [a@0 ASC,b@1 ASC,c@2 ASC,d@3 ASC]]"
        );

        Ok(())
    }

    #[test]
    fn test_join_equivalence_properties() -> Result<()> {
        let schema = create_test_schema()?;
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let offset = schema.fields.len();
        let col_a2 = &add_offset_to_expr(Arc::clone(col_a), offset);
        let col_b2 = &add_offset_to_expr(Arc::clone(col_b), offset);
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
            let mut left_eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
            let mut right_eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
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
            Arc::clone(&col_b),
            Operator::Plus,
            Arc::clone(&col_d),
        )) as Arc<dyn PhysicalExpr>;

        let constants = vec![Arc::clone(&col_a), Arc::clone(&col_b)];
        let expr = Arc::clone(&b_plus_d);
        assert!(!is_constant_recurse(&constants, &expr));

        let constants = vec![Arc::clone(&col_a), Arc::clone(&col_b), Arc::clone(&col_d)];
        let expr = Arc::clone(&b_plus_d);
        assert!(is_constant_recurse(&constants, &expr));
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
        join_eq_properties.add_equal_conditions(col_a, col_x)?;
        join_eq_properties.add_equal_conditions(col_d, col_w)?;

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

        eq_properties.add_equal_conditions(&col_a_expr, &col_c_expr)?;
        let others = vec![
            vec![PhysicalSortExpr {
                expr: Arc::clone(&col_b_expr),
                options: sort_options,
            }],
            vec![PhysicalSortExpr {
                expr: Arc::clone(&col_c_expr),
                options: sort_options,
            }],
        ];
        eq_properties.add_new_orderings(others);

        let mut expected_eqs = EquivalenceProperties::new(Arc::new(schema));
        expected_eqs.add_new_orderings([
            vec![PhysicalSortExpr {
                expr: Arc::clone(&col_b_expr),
                options: sort_options,
            }],
            vec![PhysicalSortExpr {
                expr: Arc::clone(&col_c_expr),
                options: sort_options,
            }],
        ]);

        let oeq_class = eq_properties.oeq_class().clone();
        let expected = expected_eqs.oeq_class();
        assert!(oeq_class.eq(expected));

        Ok(())
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
        let required_columns = [Arc::clone(col_b), Arc::clone(col_a)];
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
                    expr: Arc::clone(col_b),
                    options: sort_options_not
                },
                PhysicalSortExpr {
                    expr: Arc::clone(col_a),
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
        let required_columns = [Arc::clone(col_b), Arc::clone(col_a)];
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
                    expr: Arc::clone(col_b),
                    options: sort_options_not
                },
                PhysicalSortExpr {
                    expr: Arc::clone(col_a),
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
    fn test_update_properties() -> Result<()> {
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
        eq_properties.add_equal_conditions(col_b, col_a)?;
        // [b ASC], [d ASC]
        eq_properties.add_new_orderings(vec![
            vec![PhysicalSortExpr {
                expr: Arc::clone(col_b),
                options: option_asc,
            }],
            vec![PhysicalSortExpr {
                expr: Arc::clone(col_d),
                options: option_asc,
            }],
        ]);

        let test_cases = vec![
            // d + b
            (
                Arc::new(BinaryExpr::new(
                    Arc::clone(col_d),
                    Operator::Plus,
                    Arc::clone(col_b),
                )) as Arc<dyn PhysicalExpr>,
                SortProperties::Ordered(option_asc),
            ),
            // b
            (Arc::clone(col_b), SortProperties::Ordered(option_asc)),
            // a
            (Arc::clone(col_a), SortProperties::Ordered(option_asc)),
            // a + c
            (
                Arc::new(BinaryExpr::new(
                    Arc::clone(col_a),
                    Operator::Plus,
                    Arc::clone(col_c),
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
            let expr_props = eq_properties.get_expr_properties(Arc::clone(&expr));
            let err_msg = format!(
                "expr:{:?}, expected: {:?}, actual: {:?}, leading_orderings: {leading_orderings:?}",
                expr, expected, expr_props.sort_properties
            );
            assert_eq!(expr_props.sort_properties, expected, "{}", err_msg);
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
        let col_f = &col("f", &test_schema)?;
        let col_h = &col("h", &test_schema)?;
        // a + d
        let a_plus_d = Arc::new(BinaryExpr::new(
            Arc::clone(col_a),
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
        // [d ASC, h DESC] also satisfies schema.
        eq_properties.add_new_orderings([vec![
            PhysicalSortExpr {
                expr: Arc::clone(col_d),
                options: option_asc,
            },
            PhysicalSortExpr {
                expr: Arc::clone(col_h),
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
            // TEST CASE 7
            (
                vec![col_d, col_h, col_e, col_f, col_b],
                vec![
                    (col_d, option_asc),
                    (col_e, option_desc),
                    (col_h, option_desc),
                    (col_f, option_asc),
                    (col_b, option_asc),
                ],
            ),
            // TEST CASE 8
            (
                vec![col_e, col_d, col_h, col_f, col_b],
                vec![
                    (col_e, option_desc),
                    (col_d, option_asc),
                    (col_h, option_desc),
                    (col_f, option_asc),
                    (col_b, option_asc),
                ],
            ),
            // TEST CASE 9
            (
                vec![col_e, col_d, col_b, col_h, col_f],
                vec![
                    (col_e, option_desc),
                    (col_d, option_asc),
                    (col_b, option_asc),
                    (col_h, option_desc),
                    (col_f, option_asc),
                ],
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
    fn test_find_longest_permutation2() -> Result<()> {
        // Schema satisfies following orderings:
        // [a ASC], [d ASC, b ASC], [e DESC, f ASC, g ASC]
        // and
        // Column [a=c] (e.g they are aliases).
        // At below we add [d ASC, h DESC] also, for test purposes
        let (test_schema, mut eq_properties) = create_test_params()?;
        let col_h = &col("h", &test_schema)?;

        // Add column h as constant
        eq_properties = eq_properties.with_constants(vec![ConstExpr::from(col_h)]);

        let test_cases = vec![
            // TEST CASE 1
            // ordering of the constants are treated as default ordering.
            // This is the convention currently used.
            (vec![col_h], vec![(col_h, SortOptions::default())]),
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
    fn test_eliminate_redundant_monotonic_sorts() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Date32, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let base_properties = EquivalenceProperties::new(Arc::clone(&schema))
            .with_reorder(
                ["a", "b", "c"]
                    .into_iter()
                    .map(|c| {
                        col(c, schema.as_ref()).map(|expr| PhysicalSortExpr {
                            expr,
                            options: SortOptions {
                                descending: false,
                                nulls_first: true,
                            },
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            );

        struct TestCase {
            name: &'static str,
            constants: Vec<Arc<dyn PhysicalExpr>>,
            equal_conditions: Vec<[Arc<dyn PhysicalExpr>; 2]>,
            sort_columns: &'static [&'static str],
            should_satisfy_ordering: bool,
        }

        let col_a = col("a", schema.as_ref())?;
        let col_b = col("b", schema.as_ref())?;
        let col_c = col("c", schema.as_ref())?;
        let cast_c = Arc::new(CastExpr::new(col_c, DataType::Date32, None));

        let cases = vec![
            TestCase {
                name: "(a, b, c) -> (c)",
                // b is constant, so it should be removed from the sort order
                constants: vec![Arc::clone(&col_b)],
                equal_conditions: vec![[
                    Arc::clone(&cast_c) as Arc<dyn PhysicalExpr>,
                    Arc::clone(&col_a),
                ]],
                sort_columns: &["c"],
                should_satisfy_ordering: true,
            },
            // Same test with above test, where equality order is swapped.
            // Algorithm shouldn't depend on this order.
            TestCase {
                name: "(a, b, c) -> (c)",
                // b is constant, so it should be removed from the sort order
                constants: vec![col_b],
                equal_conditions: vec![[
                    Arc::clone(&col_a),
                    Arc::clone(&cast_c) as Arc<dyn PhysicalExpr>,
                ]],
                sort_columns: &["c"],
                should_satisfy_ordering: true,
            },
            TestCase {
                name: "not ordered because (b) is not constant",
                // b is not constant anymore
                constants: vec![],
                // a and c are still compatible, but this is irrelevant since the original ordering is (a, b, c)
                equal_conditions: vec![[
                    Arc::clone(&cast_c) as Arc<dyn PhysicalExpr>,
                    Arc::clone(&col_a),
                ]],
                sort_columns: &["c"],
                should_satisfy_ordering: false,
            },
        ];

        for case in cases {
            // Construct the equivalence properties in different orders
            // to exercise different code paths
            // (The resulting properties _should_ be the same)
            for properties in [
                // Equal conditions before constants
                {
                    let mut properties = base_properties.clone();
                    for [left, right] in &case.equal_conditions {
                        properties.add_equal_conditions(left, right)?
                    }
                    properties.with_constants(
                        case.constants.iter().cloned().map(ConstExpr::from),
                    )
                },
                // Constants before equal conditions
                {
                    let mut properties = base_properties.clone().with_constants(
                        case.constants.iter().cloned().map(ConstExpr::from),
                    );
                    for [left, right] in &case.equal_conditions {
                        properties.add_equal_conditions(left, right)?
                    }
                    properties
                },
            ] {
                let sort = case
                    .sort_columns
                    .iter()
                    .map(|&name| {
                        col(name, &schema).map(|col| PhysicalSortExpr {
                            expr: col,
                            options: SortOptions::default(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                assert_eq!(
                    properties.ordering_satisfy(&sort),
                    case.should_satisfy_ordering,
                    "failed test '{}'",
                    case.name
                );
            }
        }

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
    fn test_union_equivalence_properties_multi_children_1() {
        let schema = create_test_schema().unwrap();
        let schema2 = append_fields(&schema, "1");
        let schema3 = append_fields(&schema, "2");
        UnionEquivalenceTest::new(&schema)
            // Children 1
            .with_child_sort(vec![vec!["a", "b", "c"]], &schema)
            // Children 2
            .with_child_sort(vec![vec!["a1", "b1", "c1"]], &schema2)
            // Children 3
            .with_child_sort(vec![vec!["a2", "b2"]], &schema3)
            .with_expected_sort(vec![vec!["a", "b"]])
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_multi_children_2() {
        let schema = create_test_schema().unwrap();
        let schema2 = append_fields(&schema, "1");
        let schema3 = append_fields(&schema, "2");
        UnionEquivalenceTest::new(&schema)
            // Children 1
            .with_child_sort(vec![vec!["a", "b", "c"]], &schema)
            // Children 2
            .with_child_sort(vec![vec!["a1", "b1", "c1"]], &schema2)
            // Children 3
            .with_child_sort(vec![vec!["a2", "b2", "c2"]], &schema3)
            .with_expected_sort(vec![vec!["a", "b", "c"]])
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_multi_children_3() {
        let schema = create_test_schema().unwrap();
        let schema2 = append_fields(&schema, "1");
        let schema3 = append_fields(&schema, "2");
        UnionEquivalenceTest::new(&schema)
            // Children 1
            .with_child_sort(vec![vec!["a", "b"]], &schema)
            // Children 2
            .with_child_sort(vec![vec!["a1", "b1", "c1"]], &schema2)
            // Children 3
            .with_child_sort(vec![vec!["a2", "b2", "c2"]], &schema3)
            .with_expected_sort(vec![vec!["a", "b"]])
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_multi_children_4() {
        let schema = create_test_schema().unwrap();
        let schema2 = append_fields(&schema, "1");
        let schema3 = append_fields(&schema, "2");
        UnionEquivalenceTest::new(&schema)
            // Children 1
            .with_child_sort(vec![vec!["a", "b"]], &schema)
            // Children 2
            .with_child_sort(vec![vec!["a1", "b1"]], &schema2)
            // Children 3
            .with_child_sort(vec![vec!["b2", "c2"]], &schema3)
            .with_expected_sort(vec![])
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_multi_children_5() {
        let schema = create_test_schema().unwrap();
        let schema2 = append_fields(&schema, "1");
        UnionEquivalenceTest::new(&schema)
            // Children 1
            .with_child_sort(vec![vec!["a", "b"], vec!["c"]], &schema)
            // Children 2
            .with_child_sort(vec![vec!["a1", "b1"], vec!["c1"]], &schema2)
            .with_expected_sort(vec![vec!["a", "b"], vec!["c"]])
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_common_constants() {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child: [a ASC], const [b, c]
                vec![vec!["a"]],
                vec!["b", "c"],
                &schema,
            )
            .with_child_sort_and_const_exprs(
                // Second child: [b ASC], const [a, c]
                vec![vec!["b"]],
                vec!["a", "c"],
                &schema,
            )
            .with_expected_sort_and_const_exprs(
                // Union expected orderings: [[a ASC], [b ASC]], const [c]
                vec![vec!["a"], vec!["b"]],
                vec!["c"],
            )
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_prefix() {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child: [a ASC], const []
                vec![vec!["a"]],
                vec![],
                &schema,
            )
            .with_child_sort_and_const_exprs(
                // Second child: [a ASC, b ASC], const []
                vec![vec!["a", "b"]],
                vec![],
                &schema,
            )
            .with_expected_sort_and_const_exprs(
                // Union orderings: [a ASC], const []
                vec![vec!["a"]],
                vec![],
            )
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_asc_desc_mismatch() {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child: [a ASC], const []
                vec![vec!["a"]],
                vec![],
                &schema,
            )
            .with_child_sort_and_const_exprs(
                // Second child orderings: [a DESC], const []
                vec![vec!["a DESC"]],
                vec![],
                &schema,
            )
            .with_expected_sort_and_const_exprs(
                // Union doesn't have any ordering or constant
                vec![],
                vec![],
            )
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_different_schemas() {
        let schema = create_test_schema().unwrap();
        let schema2 = append_fields(&schema, "1");
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child orderings: [a ASC], const []
                vec![vec!["a"]],
                vec![],
                &schema,
            )
            .with_child_sort_and_const_exprs(
                // Second child orderings: [a1 ASC, b1 ASC], const []
                vec![vec!["a1", "b1"]],
                vec![],
                &schema2,
            )
            .with_expected_sort_and_const_exprs(
                // Union orderings: [a ASC]
                //
                // Note that a, and a1 are at the same index for their
                // corresponding schemas.
                vec![vec!["a"]],
                vec![],
            )
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_fill_gaps() {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child orderings: [a ASC, c ASC], const [b]
                vec![vec!["a", "c"]],
                vec!["b"],
                &schema,
            )
            .with_child_sort_and_const_exprs(
                // Second child orderings: [b ASC, c ASC], const [a]
                vec![vec!["b", "c"]],
                vec!["a"],
                &schema,
            )
            .with_expected_sort_and_const_exprs(
                // Union orderings: [
                //   [a ASC, b ASC, c ASC],
                //   [b ASC, a ASC, c ASC]
                // ], const []
                vec![vec!["a", "b", "c"], vec!["b", "a", "c"]],
                vec![],
            )
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_no_fill_gaps() {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child orderings: [a ASC, c ASC], const [d] // some other constant
                vec![vec!["a", "c"]],
                vec!["d"],
                &schema,
            )
            .with_child_sort_and_const_exprs(
                // Second child orderings: [b ASC, c ASC], const [a]
                vec![vec!["b", "c"]],
                vec!["a"],
                &schema,
            )
            .with_expected_sort_and_const_exprs(
                // Union orderings: [[a]] (only a is constant)
                vec![vec!["a"]],
                vec![],
            )
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_fill_some_gaps() {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child orderings: [c ASC], const [a, b] // some other constant
                vec![vec!["c"]],
                vec!["a", "b"],
                &schema,
            )
            .with_child_sort_and_const_exprs(
                // Second child orderings: [a DESC, b], const []
                vec![vec!["a DESC", "b"]],
                vec![],
                &schema,
            )
            .with_expected_sort_and_const_exprs(
                // Union orderings: [[a, b]] (can fill in the a/b with constants)
                vec![vec!["a DESC", "b"]],
                vec![],
            )
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_fill_gaps_non_symmetric() {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child orderings: [a ASC, c ASC], const [b]
                vec![vec!["a", "c"]],
                vec!["b"],
                &schema,
            )
            .with_child_sort_and_const_exprs(
                // Second child orderings: [b ASC, c ASC], const [a]
                vec![vec!["b DESC", "c"]],
                vec!["a"],
                &schema,
            )
            .with_expected_sort_and_const_exprs(
                // Union orderings: [
                //   [a ASC, b ASC, c ASC],
                //   [b ASC, a ASC, c ASC]
                // ], const []
                vec![vec!["a", "b DESC", "c"], vec!["b DESC", "a", "c"]],
                vec![],
            )
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_gap_fill_symmetric() {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child: [a ASC, b ASC, d ASC], const [c]
                vec![vec!["a", "b", "d"]],
                vec!["c"],
                &schema,
            )
            .with_child_sort_and_const_exprs(
                // Second child: [a ASC, c ASC, d ASC], const [b]
                vec![vec!["a", "c", "d"]],
                vec!["b"],
                &schema,
            )
            .with_expected_sort_and_const_exprs(
                // Union orderings:
                // [a, b, c, d]
                // [a, c, b, d]
                vec![vec!["a", "c", "b", "d"], vec!["a", "b", "c", "d"]],
                vec![],
            )
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_gap_fill_and_common() {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // First child: [a DESC, d ASC], const [b, c]
                vec![vec!["a DESC", "d"]],
                vec!["b", "c"],
                &schema,
            )
            .with_child_sort_and_const_exprs(
                // Second child: [a DESC, c ASC, d ASC], const [b]
                vec![vec!["a DESC", "c", "d"]],
                vec!["b"],
                &schema,
            )
            .with_expected_sort_and_const_exprs(
                // Union orderings:
                // [a DESC, c, d]  [b]
                vec![vec!["a DESC", "c", "d"]],
                vec!["b"],
            )
            .run()
    }

    #[test]
    fn test_union_equivalence_properties_constants_middle_desc() {
        let schema = create_test_schema().unwrap();
        UnionEquivalenceTest::new(&schema)
            .with_child_sort_and_const_exprs(
                // NB `b DESC` in the first child
                //
                // First child: [a ASC, b DESC, d ASC], const [c]
                vec![vec!["a", "b DESC", "d"]],
                vec!["c"],
                &schema,
            )
            .with_child_sort_and_const_exprs(
                // Second child: [a ASC, c ASC, d ASC], const [b]
                vec![vec!["a", "c", "d"]],
                vec!["b"],
                &schema,
            )
            .with_expected_sort_and_const_exprs(
                // Union orderings:
                // [a, b, d] (c constant)
                // [a, c, d] (b constant)
                vec![vec!["a", "c", "b DESC", "d"], vec!["a", "b DESC", "c", "d"]],
                vec![],
            )
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
        ) -> Self {
            let properties = self.make_props(orderings, vec![], schema);
            self.child_properties.push(properties);
            self
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
        ) -> Self {
            let properties = self.make_props(orderings, constants, schema);
            self.child_properties.push(properties);
            self
        }

        /// Set the expected output sort order for the union of the children
        ///
        /// See [`Self::make_props`] for the format of the strings in `orderings`
        fn with_expected_sort(mut self, orderings: Vec<Vec<&str>>) -> Self {
            let properties = self.make_props(orderings, vec![], &self.output_schema);
            self.expected_properties = Some(properties);
            self
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
        ) -> Self {
            let properties = self.make_props(orderings, constants, &self.output_schema);
            self.expected_properties = Some(properties);
            self
        }

        /// compute the union's output equivalence properties from the child
        /// properties, and compare them to the expected properties
        fn run(self) {
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
                assert_eq_properties_same(
                    &actual_properties,
                    &expected_properties,
                    format!(
                        "expected: {expected_properties:?}\nactual:  {actual_properties:?}"
                    ),
                );
            }
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
        ) -> EquivalenceProperties {
            let orderings = orderings
                .iter()
                .map(|ordering| {
                    ordering
                        .iter()
                        .map(|name| parse_sort_expr(name, schema))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            let constants = constants
                .iter()
                .map(|col_name| ConstExpr::new(col(col_name, schema).unwrap()))
                .collect::<Vec<_>>();

            EquivalenceProperties::new_with_orderings(Arc::clone(schema), &orderings)
                .with_constants(constants)
        }
    }

    fn assert_eq_properties_same(
        lhs: &EquivalenceProperties,
        rhs: &EquivalenceProperties,
        err_msg: String,
    ) {
        // Check whether constants are same
        let lhs_constants = lhs.constants();
        let rhs_constants = rhs.constants();
        for rhs_constant in rhs_constants {
            assert!(
                const_exprs_contains(lhs_constants, rhs_constant.expr()),
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
        let rhs_orderings = &rhs.oeq_class.orderings;
        for rhs_ordering in rhs_orderings {
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

    /// Converts a string to a physical sort expression
    ///
    /// # Example
    /// * `"a"` -> (`"a"`, `SortOptions::default()`)
    /// * `"a ASC"` -> (`"a"`, `SortOptions { descending: false, nulls_first: false }`)
    fn parse_sort_expr(name: &str, schema: &SchemaRef) -> PhysicalSortExpr {
        let mut parts = name.split_whitespace();
        let name = parts.next().expect("empty sort expression");
        let mut sort_expr = PhysicalSortExpr::new(
            col(name, schema).expect("invalid column name"),
            SortOptions::default(),
        );

        if let Some(options) = parts.next() {
            sort_expr = match options {
                "ASC" => sort_expr.asc(),
                "DESC" => sort_expr.desc(),
                _ => panic!(
                    "unknown sort options. Expected 'ASC' or 'DESC', got {}",
                    options
                ),
            }
        }

        assert!(
            parts.next().is_none(),
            "unexpected tokens in column name. Expected 'name' / 'name ASC' / 'name DESC' but got  '{name}'"
        );

        sort_expr
    }
}
