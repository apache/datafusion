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

mod dependency; // Submodule containing DependencyMap and Dependencies
mod joins; // Submodule containing join_equivalence_properties
mod union; // Submodule containing calculate_union

use dependency::{
    construct_prefix_orderings, generate_dependency_orderings, referred_dependencies,
    Dependencies, DependencyMap,
};
pub use joins::*;
pub use union::*;

use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::{fmt, mem};

use crate::equivalence::class::{const_exprs_contains, AcrossPartitions};
use crate::equivalence::{
    EquivalenceClass, EquivalenceGroup, OrderingEquivalenceClass, ProjectionMapping,
};
use crate::expressions::{with_new_schema, CastExpr, Column, Literal};
use crate::{
    physical_exprs_contains, ConstExpr, LexOrdering, LexRequirement, PhysicalExpr,
    PhysicalSortExpr, PhysicalSortRequirement,
};

use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{plan_err, Constraint, Constraints, HashMap, Result};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_physical_expr_common::utils::ExprPropertiesNode;

use indexmap::IndexSet;
use itertools::Itertools;

/// `EquivalenceProperties` stores information about the output
/// of a plan node, that can be used to optimize the plan.
///
/// Currently, it keeps track of:
/// - Sort expressions (orderings)
/// - Equivalent expressions: expressions that are known to have same value.
/// - Constants expressions: expressions that are known to contain a single
///   constant value.
///
/// Please see the [Using Ordering for Better Plans] blog for more details.
///
/// [Using Ordering for Better Plans]: https://datafusion.apache.org/blog/2025/03/11/ordering-analysis/
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
/// # use arrow::datatypes::{Schema, Field, DataType, SchemaRef};
/// # use datafusion_physical_expr::{ConstExpr, EquivalenceProperties};
/// # use datafusion_physical_expr::expressions::col;
/// use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
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
/// eq_properties.add_new_ordering(LexOrdering::new(vec![
///   PhysicalSortExpr::new_default(col_a).asc(),
///   PhysicalSortExpr::new_default(col_c).desc(),
/// ]));
///
/// assert_eq!(eq_properties.to_string(), "order: [[a@0 ASC, c@2 DESC]], const: [b@1(heterogeneous)]")
/// ```
#[derive(Debug, Clone)]
pub struct EquivalenceProperties {
    /// Distinct equivalence classes (exprs known to have the same expressions)
    eq_group: EquivalenceGroup,
    /// Equivalent sort expressions
    oeq_class: OrderingEquivalenceClass,
    /// Expressions whose values are constant
    ///
    /// TODO: We do not need to track constants separately, they can be tracked
    ///       inside `eq_group` as `Literal` expressions.
    constants: Vec<ConstExpr>,
    /// Table constraints
    constraints: Constraints,
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
            constraints: Constraints::empty(),
            schema,
        }
    }

    /// Adds constraints to the properties.
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = constraints;
        self
    }

    /// Creates a new `EquivalenceProperties` object with the given orderings.
    pub fn new_with_orderings(schema: SchemaRef, orderings: &[LexOrdering]) -> Self {
        Self {
            eq_group: EquivalenceGroup::empty(),
            oeq_class: OrderingEquivalenceClass::new(orderings.to_vec()),
            constants: vec![],
            constraints: Constraints::empty(),
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

    /// Return the inner OrderingEquivalenceClass, consuming self
    pub fn into_oeq_class(self) -> OrderingEquivalenceClass {
        self.oeq_class
    }

    /// Returns a reference to the equivalence group within.
    pub fn eq_group(&self) -> &EquivalenceGroup {
        &self.eq_group
    }

    /// Returns a reference to the constant expressions
    pub fn constants(&self) -> &[ConstExpr] {
        &self.constants
    }

    pub fn constraints(&self) -> &Constraints {
        &self.constraints
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
        self.constants.retain(|item| {
            matches!(item.across_partitions(), AcrossPartitions::Uniform(_))
        })
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
                let const_expr = ConstExpr::from(right)
                    .with_across_partitions(self.get_expr_constant_value(left));
                self.constants.push(const_expr);
            }
        } else if self.is_expr_constant(right) {
            // Right expression is constant, add left as constant
            if !const_exprs_contains(&self.constants, left) {
                let const_expr = ConstExpr::from(left)
                    .with_across_partitions(self.get_expr_constant_value(right));
                self.constants.push(const_expr);
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
        let normalized_constants = constants
            .into_iter()
            .filter_map(|c| {
                let across_partitions = c.across_partitions();
                let expr = c.owned_expr();
                let normalized_expr = self.eq_group.normalize_expr(expr);

                if const_exprs_contains(&self.constants, &normalized_expr) {
                    return None;
                }

                let const_expr = ConstExpr::from(normalized_expr)
                    .with_across_partitions(across_partitions);

                Some(const_expr)
            })
            .collect::<Vec<_>>();

        // Add all new normalized constants
        self.constants.extend(normalized_constants);

        // Discover any new orderings based on the constants
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
            .iter()
            .find_map(|class| {
                class
                    .contains(&normalized_expr)
                    .then(|| class.clone().into_vec())
            })
            .unwrap_or_else(|| vec![Arc::clone(&normalized_expr)]);

        let mut new_orderings: Vec<LexOrdering> = vec![];
        for ordering in self.normalized_oeq_class().iter() {
            if !ordering[0].expr.eq(&normalized_expr) {
                continue;
            }

            let leading_ordering_options = ordering[0].options;

            for equivalent_expr in &eq_class {
                let children = equivalent_expr.children();
                if children.is_empty() {
                    continue;
                }

                // Check if all children match the next expressions in the ordering
                let mut all_children_match = true;
                let mut child_properties = vec![];

                // Build properties for each child based on the next expressions
                for (i, child) in children.iter().enumerate() {
                    if let Some(next) = ordering.get(i + 1) {
                        if !child.as_ref().eq(next.expr.as_ref()) {
                            all_children_match = false;
                            break;
                        }
                        child_properties.push(ExprProperties {
                            sort_properties: SortProperties::Ordered(next.options),
                            range: Interval::make_unbounded(
                                &child.data_type(&self.schema)?,
                            )?,
                            preserves_lex_ordering: true,
                        });
                    } else {
                        all_children_match = false;
                        break;
                    }
                }

                if all_children_match {
                    // Check if the expression is monotonic in all arguments
                    if let Ok(expr_properties) =
                        equivalent_expr.get_properties(&child_properties)
                    {
                        if expr_properties.preserves_lex_ordering
                            && SortProperties::Ordered(leading_ordering_options)
                                == expr_properties.sort_properties
                        {
                            // Assume existing ordering is [c ASC, a ASC, b ASC]
                            // When equality c = f(a,b) is given, if we know that given ordering `[a ASC, b ASC]`,
                            // ordering `[f(a,b) ASC]` is valid, then we can deduce that ordering `[a ASC, b ASC]` is also valid.
                            // Hence, ordering `[a ASC, b ASC]` can be added to the state as a valid ordering.
                            // (e.g. existing ordering where leading ordering is removed)
                            new_orderings.push(LexOrdering::new(ordering[1..].to_vec()));
                            break;
                        }
                    }
                }
            }
        }

        self.oeq_class.add_new_orderings(new_orderings);
        Ok(())
    }

    /// Updates the ordering equivalence group within assuming that the table
    /// is re-sorted according to the argument `sort_exprs`. Note that constants
    /// and equivalence classes are unchanged as they are unaffected by a re-sort.
    /// If the given ordering is already satisfied, the function does nothing.
    pub fn with_reorder(mut self, sort_exprs: LexOrdering) -> Self {
        // Filter out constant expressions as they don't affect ordering
        let filtered_exprs = LexOrdering::new(
            sort_exprs
                .into_iter()
                .filter(|expr| !self.is_expr_constant(&expr.expr))
                .collect(),
        );

        if filtered_exprs.is_empty() {
            return self;
        }

        let mut new_orderings = vec![filtered_exprs.clone()];

        // Preserve valid suffixes from existing orderings
        let oeq_class = mem::take(&mut self.oeq_class);
        for existing in oeq_class {
            if self.is_prefix_of(&filtered_exprs, &existing) {
                let mut extended = filtered_exprs.clone();
                extended.extend(existing.into_iter().skip(filtered_exprs.len()));
                new_orderings.push(extended);
            }
        }

        self.oeq_class = OrderingEquivalenceClass::new(new_orderings);
        self
    }

    /// Checks if the new ordering matches a prefix of the existing ordering
    /// (considering expression equivalences)
    fn is_prefix_of(&self, new_order: &LexOrdering, existing: &LexOrdering) -> bool {
        // Check if new order is longer than existing - can't be a prefix
        if new_order.len() > existing.len() {
            return false;
        }

        // Check if new order matches existing prefix (considering equivalences)
        new_order.iter().zip(existing).all(|(new, existing)| {
            self.eq_group.exprs_equal(&new.expr, &existing.expr)
                && new.options == existing.options
        })
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
    fn normalize_sort_exprs(&self, sort_exprs: &LexOrdering) -> LexOrdering {
        // Convert sort expressions to sort requirements:
        let sort_reqs = LexRequirement::from(sort_exprs.clone());
        // Normalize the requirements:
        let normalized_sort_reqs = self.normalize_sort_requirements(&sort_reqs);
        // Convert sort requirements back to sort expressions:
        LexOrdering::from(normalized_sort_reqs)
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
    fn normalize_sort_requirements(&self, sort_reqs: &LexRequirement) -> LexRequirement {
        let normalized_sort_reqs = self.eq_group.normalize_sort_requirements(sort_reqs);
        let mut constant_exprs = vec![];
        constant_exprs.extend(
            self.constants
                .iter()
                .map(|const_expr| Arc::clone(const_expr.expr())),
        );
        let constants_normalized = self.eq_group.normalize_exprs(constant_exprs);
        // Prune redundant sections in the requirement:
        normalized_sort_reqs
            .iter()
            .filter(|&order| !physical_exprs_contains(&constants_normalized, &order.expr))
            .cloned()
            .collect::<LexRequirement>()
            .collapse()
    }

    /// Checks whether the given ordering is satisfied by any of the existing
    /// orderings.
    pub fn ordering_satisfy(&self, given: &LexOrdering) -> bool {
        // Convert the given sort expressions to sort requirements:
        let sort_requirements = LexRequirement::from(given.clone());
        self.ordering_satisfy_requirement(&sort_requirements)
    }

    /// Checks whether the given sort requirements are satisfied by any of the
    /// existing orderings.
    pub fn ordering_satisfy_requirement(&self, reqs: &LexRequirement) -> bool {
        let mut eq_properties = self.clone();
        // First, standardize the given requirement:
        let normalized_reqs = eq_properties.normalize_sort_requirements(reqs);

        // Check whether given ordering is satisfied by constraints first
        if self.satisfied_by_constraints(&normalized_reqs) {
            return true;
        }

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

    /// Checks if the sort requirements are satisfied by any of the table constraints (primary key or unique).
    /// Returns true if any constraint fully satisfies the requirements.
    fn satisfied_by_constraints(
        &self,
        normalized_reqs: &[PhysicalSortRequirement],
    ) -> bool {
        self.constraints.iter().any(|constraint| match constraint {
            Constraint::PrimaryKey(indices) | Constraint::Unique(indices) => self
                .satisfied_by_constraint(
                    normalized_reqs,
                    indices,
                    matches!(constraint, Constraint::Unique(_)),
                ),
        })
    }

    /// Checks if sort requirements are satisfied by a constraint (primary key or unique).
    /// Returns true if the constraint indices form a valid prefix of an existing ordering
    /// that matches the requirements. For unique constraints, also verifies nullable columns.
    fn satisfied_by_constraint(
        &self,
        normalized_reqs: &[PhysicalSortRequirement],
        indices: &[usize],
        check_null: bool,
    ) -> bool {
        // Requirements must contain indices
        if indices.len() > normalized_reqs.len() {
            return false;
        }

        // Iterate over all orderings
        self.oeq_class.iter().any(|ordering| {
            if indices.len() > ordering.len() {
                return false;
            }

            // Build a map of column positions in the ordering
            let mut col_positions = HashMap::with_capacity(ordering.len());
            for (pos, req) in ordering.iter().enumerate() {
                if let Some(col) = req.expr.as_any().downcast_ref::<Column>() {
                    col_positions.insert(
                        col.index(),
                        (pos, col.nullable(&self.schema).unwrap_or(true)),
                    );
                }
            }

            // Check if all constraint indices appear in valid positions
            if !indices.iter().all(|&idx| {
                col_positions
                    .get(&idx)
                    .map(|&(pos, nullable)| {
                        // For unique constraints, verify column is not nullable if it's first/last
                        !check_null
                            || (pos != 0 && pos != ordering.len() - 1)
                            || !nullable
                    })
                    .unwrap_or(false)
            }) {
                return false;
            }

            // Check if this ordering matches requirements prefix
            let ordering_len = ordering.len();
            normalized_reqs.len() >= ordering_len
                && normalized_reqs[..ordering_len].iter().zip(ordering).all(
                    |(req, existing)| {
                        req.expr.eq(&existing.expr)
                            && req
                                .options
                                .is_none_or(|req_opts| req_opts == existing.options)
                    },
                )
        })
    }

    /// Determines whether the ordering specified by the given sort requirement
    /// is satisfied based on the orderings within, equivalence classes, and
    /// constant expressions.
    ///
    /// # Parameters
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

    /// Checks whether the `given` sort requirements are equal or more specific
    /// than the `reference` sort requirements.
    pub fn requirements_compatible(
        &self,
        given: &LexRequirement,
        reference: &LexRequirement,
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
        lhs: &LexOrdering,
        rhs: &LexOrdering,
    ) -> Option<LexOrdering> {
        // Convert the given sort expressions to sort requirements:
        let lhs = LexRequirement::from(lhs.clone());
        let rhs = LexRequirement::from(rhs.clone());
        let finer = self.get_finer_requirement(&lhs, &rhs);
        // Convert the chosen sort requirements back to sort expressions:
        finer.map(LexOrdering::from)
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
        req1: &LexRequirement,
        req2: &LexRequirement,
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
        sort_expr: &LexOrdering,
    ) -> Result<Vec<LexOrdering>> {
        let new_orderings = sort_expr
            .iter()
            .map(|sort_expr| {
                let referring_exprs: Vec<_> = mapping
                    .iter()
                    .map(|(source, _target)| source)
                    .filter(|source| expr_refers(source, &sort_expr.expr))
                    .cloned()
                    .collect();
                let mut res = LexOrdering::new(vec![sort_expr.clone()]);
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
            .map(LexOrdering::new)
            .collect::<Vec<_>>();
        Ok(res)
    }

    /// In projection, supposed we have a input function 'A DESC B DESC' and the output shares the same expression
    /// with A and B, we could surely use the ordering of the original ordering, However, if the A has been changed,
    /// for example, A-> Cast(A, Int64) or any other form, it is invalid if we continue using the original ordering
    /// Since it would cause bug in dependency constructions, we should substitute the input order in order to get correct
    /// dependency map, happen in issue 8838: <https://github.com/apache/datafusion/issues/8838>
    pub fn substitute_oeq_class(&mut self, mapping: &ProjectionMapping) -> Result<()> {
        let new_order = self
            .oeq_class
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
    /// \[`DependencyNode`\] contains dependencies for the key [`PhysicalSortExpr`].
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
                prefixes = vec![LexOrdering::default()];
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
            .map(|lex_ordering| lex_ordering.collapse())
            .collect()
    }

    /// Projects constants based on the provided `ProjectionMapping`.
    ///
    /// This function takes a `ProjectionMapping` and identifies/projects
    /// constants based on the existing constants and the mapping. It ensures
    /// that constants are appropriately propagated through the projection.
    ///
    /// # Parameters
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
                const_expr
                    .map(|expr| self.eq_group.project_expr(mapping, expr))
                    .map(|projected_expr| {
                        projected_expr
                            .with_across_partitions(const_expr.across_partitions())
                    })
            })
            .collect::<Vec<_>>();

        // Add projection expressions that are known to be constant:
        for (source, target) in mapping.iter() {
            if self.is_expr_constant(source)
                && !const_exprs_contains(&projected_constants, target)
            {
                if self.is_expr_constant_across_partitions(source) {
                    projected_constants.push(
                        ConstExpr::from(target)
                            .with_across_partitions(self.get_expr_constant_value(source)),
                    )
                } else {
                    projected_constants.push(
                        ConstExpr::from(target)
                            .with_across_partitions(AcrossPartitions::Heterogeneous),
                    )
                }
            }
        }
        projected_constants
    }

    /// Projects constraints according to the given projection mapping.
    ///
    /// This function takes a projection mapping and extracts the column indices of the target columns.
    /// It then projects the constraints to only include relationships between
    /// columns that exist in the projected output.
    ///
    /// # Arguments
    ///
    /// * `mapping` - A reference to `ProjectionMapping` that defines how expressions are mapped
    ///               in the projection operation
    ///
    /// # Returns
    ///
    /// Returns a new `Constraints` object containing only the constraints
    /// that are valid for the projected columns.
    fn projected_constraints(&self, mapping: &ProjectionMapping) -> Option<Constraints> {
        let indices = mapping
            .iter()
            .filter_map(|(_, target)| target.as_any().downcast_ref::<Column>())
            .map(|col| col.index())
            .collect::<Vec<_>>();
        debug_assert_eq!(mapping.map.len(), indices.len());
        self.constraints.project(&indices)
    }

    /// Projects the equivalences within according to `mapping`
    /// and `output_schema`.
    pub fn project(&self, mapping: &ProjectionMapping, output_schema: SchemaRef) -> Self {
        let eq_group = self.eq_group.project(mapping);
        let oeq_class = OrderingEquivalenceClass::new(self.projected_orderings(mapping));
        let constants = self.projected_constants(mapping);
        let constraints = self
            .projected_constraints(mapping)
            .unwrap_or_else(Constraints::empty);
        Self {
            schema: output_schema,
            eq_group,
            oeq_class,
            constants,
            constraints,
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
        let (left, right) = result.into_iter().unzip();
        (LexOrdering::new(left), right)
    }

    /// This function determines whether the provided expression is constant
    /// based on the known constants.
    ///
    /// # Parameters
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

    /// This function determines whether the provided expression is constant
    /// across partitions based on the known constants.
    ///
    /// # Parameters
    ///
    /// - `expr`: A reference to a `Arc<dyn PhysicalExpr>` representing the
    ///   expression to be checked.
    ///
    /// # Returns
    ///
    /// Returns `true` if the expression is constant across all partitions according
    /// to equivalence group, `false` otherwise
    #[deprecated(
        since = "45.0.0",
        note = "Use [`is_expr_constant_across_partitions`] instead"
    )]
    pub fn is_expr_constant_accross_partitions(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> bool {
        self.is_expr_constant_across_partitions(expr)
    }

    /// This function determines whether the provided expression is constant
    /// across partitions based on the known constants.
    ///
    /// # Parameters
    ///
    /// - `expr`: A reference to a `Arc<dyn PhysicalExpr>` representing the
    ///   expression to be checked.
    ///
    /// # Returns
    ///
    /// Returns `true` if the expression is constant across all partitions according
    /// to equivalence group, `false` otherwise.
    pub fn is_expr_constant_across_partitions(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> bool {
        // As an example, assume that we know columns `a` and `b` are constant.
        // Then, `a`, `b` and `a + b` will all return `true` whereas `c` will
        // return `false`.
        let const_exprs = self
            .constants
            .iter()
            .filter_map(|const_expr| {
                if matches!(
                    const_expr.across_partitions(),
                    AcrossPartitions::Uniform { .. }
                ) {
                    Some(Arc::clone(const_expr.expr()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let normalized_constants = self.eq_group.normalize_exprs(const_exprs);
        let normalized_expr = self.eq_group.normalize_expr(Arc::clone(expr));
        is_constant_recurse(&normalized_constants, &normalized_expr)
    }

    /// Retrieves the constant value of a given physical expression, if it exists.
    ///
    /// Normalizes the input expression and checks if it matches any known constants
    /// in the current context. Returns whether the expression has a uniform value,
    /// varies across partitions, or is not constant.
    ///
    /// # Parameters
    /// - `expr`: A reference to the physical expression to evaluate.
    ///
    /// # Returns
    /// - `AcrossPartitions::Uniform(value)`: If the expression has the same value across partitions.
    /// - `AcrossPartitions::Heterogeneous`: If the expression varies across partitions.
    /// - `None`: If the expression is not recognized as constant.
    pub fn get_expr_constant_value(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> AcrossPartitions {
        let normalized_expr = self.eq_group.normalize_expr(Arc::clone(expr));

        if let Some(lit) = normalized_expr.as_any().downcast_ref::<Literal>() {
            return AcrossPartitions::Uniform(Some(lit.value().clone()));
        }

        for const_expr in self.constants.iter() {
            if normalized_expr.eq(const_expr.expr()) {
                return const_expr.across_partitions();
            }
        }

        AcrossPartitions::Heterogeneous
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
        for ordering in self.oeq_class {
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
        for eq_class in self.eq_group {
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    let oeq_class = eq_properties.normalized_oeq_class();
    if eq_properties.is_expr_constant(&normalized_expr)
        || oeq_class.is_expr_partial_const(&normalized_expr)
    {
        node.data.sort_properties = SortProperties::Singleton;
    } else if let Some(options) = oeq_class.get_options(&normalized_expr) {
        node.data.sort_properties = SortProperties::Ordered(options);
    }
    Ok(Transformed::yes(node))
}

/// This function determines whether the provided expression is constant
/// based on the known constants.
///
/// # Parameters
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
            preserves_lex_ordering: false,
        })
    } else if expr.as_any().downcast_ref::<Column>().is_some() {
        Ok(ExprProperties {
            sort_properties: SortProperties::Unordered,
            range: Interval::make_unbounded(&expr.data_type(schema)?)?,
            preserves_lex_ordering: false,
        })
    } else if let Some(literal) = expr.as_any().downcast_ref::<Literal>() {
        Ok(ExprProperties {
            sort_properties: SortProperties::Singleton,
            range: Interval::try_new(literal.value().clone(), literal.value().clone())?,
            preserves_lex_ordering: true,
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
mod tests {

    use super::*;
    use crate::expressions::{col, BinaryExpr};

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_expr::Operator;

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
}
