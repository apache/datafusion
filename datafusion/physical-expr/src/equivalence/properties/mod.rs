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

pub use joins::*;
pub use union::*;

use std::fmt::Display;
use std::sync::Arc;
use std::{fmt, mem};

use self::dependency::{
    construct_prefix_orderings, generate_dependency_orderings, referred_dependencies,
    Dependencies, DependencyMap,
};
use crate::equivalence::class::AcrossPartitions;
use crate::equivalence::{EquivalenceGroup, OrderingEquivalenceClass, ProjectionMapping};
use crate::expressions::{with_new_schema, CastExpr, Column, Literal};
use crate::{
    ConstExpr, LexOrdering, LexRequirement, PhysicalExpr, PhysicalSortExpr,
    PhysicalSortRequirement,
};

use arrow::datatypes::SchemaRef;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{plan_err, Constraint, Constraints, HashMap, Result};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_physical_expr_common::utils::ExprPropertiesNode;

use indexmap::IndexSet;
use itertools::Itertools;

/// `EquivalenceProperties` stores information about the output of a plan node
/// that can be used to optimize the plan. Currently, it keeps track of:
/// - Sort expressions (orderings),
/// - Equivalent expressions; i.e. expressions known to have the same value.
/// - Constants expressions; i.e. expressions known to contain a single constant
///   value.
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
/// `EquivalenceProperties` tracks these different valid sort expressions and
/// treat `a ASC` and `b DESC` on an equal footing. For example, if the query
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
/// In this case,  columns `a` and `b` always have the same value. With this
/// information, Datafusion can optimize various operations. For example, if
/// the partition requirement is `Hash(a)` and output partitioning is
/// `Hash(b)`, then DataFusion avoids repartitioning the data as the existing
/// partitioning satisfies the requirement.
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
/// let mut eq_properties = EquivalenceProperties::new(schema);
/// eq_properties.add_constants(vec![ConstExpr::from(col_b)]);
/// eq_properties.add_ordering([
///   PhysicalSortExpr::new_default(col_a).asc(),
///   PhysicalSortExpr::new_default(col_c).desc(),
/// ]);
///
/// assert_eq!(eq_properties.to_string(), "order: [[a@0 ASC, c@2 DESC]], eq: [{members: [b@1], constant: (heterogeneous)}]");
/// ```
#[derive(Debug, Clone)]
pub struct EquivalenceProperties {
    /// Distinct equivalence classes (i.e. expressions with the same value).
    eq_group: EquivalenceGroup,
    /// Equivalent sort expressions (i.e. those define the same ordering).
    oeq_class: OrderingEquivalenceClass,
    /// Table constraints that factor in equivalence calculations.
    constraints: Constraints,
    /// Schema associated with this object.
    schema: SchemaRef,
}

impl EquivalenceProperties {
    /// Creates an empty `EquivalenceProperties` object.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            eq_group: EquivalenceGroup::default(),
            oeq_class: OrderingEquivalenceClass::default(),
            constraints: Constraints::default(),
            schema,
        }
    }

    /// Adds constraints to the properties.
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = constraints;
        self
    }

    /// Creates a new `EquivalenceProperties` object with the given orderings.
    pub fn new_with_orderings(
        schema: SchemaRef,
        orderings: impl IntoIterator<Item = impl IntoIterator<Item = PhysicalSortExpr>>,
    ) -> Self {
        Self {
            eq_group: EquivalenceGroup::default(),
            oeq_class: OrderingEquivalenceClass::new(orderings),
            constraints: Constraints::default(),
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

    /// Returns a reference to the constraints within.
    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    /// Returns all the known constants expressions.
    pub fn constants(&self) -> Vec<ConstExpr> {
        self.eq_group
            .iter()
            .filter_map(|c| {
                c.constant.as_ref().and_then(|across| {
                    c.canonical_expr()
                        .map(|expr| ConstExpr::new(Arc::clone(expr), across.clone()))
                })
            })
            .collect()
    }

    /// Returns the output ordering of the properties.
    pub fn output_ordering(&self) -> Option<LexOrdering> {
        let mut sort_exprs: Vec<_> = self.oeq_class().output_ordering()?.into();
        // Prune out constant expressions:
        sort_exprs.retain(|sort_expr| {
            self.eq_group
                .get_equivalence_class(&sort_expr.expr)
                .is_none_or(|cls| cls.constant.is_none())
        });
        LexOrdering::new(sort_exprs)
    }

    /// Returns the normalized version of the ordering equivalence class within.
    /// Normalization removes constants and duplicates as well as standardizing
    /// expressions according to the equivalence group within.
    pub fn normalized_oeq_class(&self) -> OrderingEquivalenceClass {
        self.oeq_class
            .iter()
            .cloned()
            .filter_map(|ordering| self.normalize_sort_exprs(ordering))
            .collect::<Vec<_>>()
            .into()
    }

    /// Extends this `EquivalenceProperties` with the `other` object.
    pub fn extend(mut self, other: Self) -> Self {
        self.eq_group.extend(other.eq_group);
        self.oeq_class.extend(other.oeq_class);
        self.constraints.extend(other.constraints);
        self
    }

    /// Clears (empties) the ordering equivalence class within this object.
    /// Call this method when existing orderings are invalidated.
    pub fn clear_orderings(&mut self) {
        self.oeq_class.clear();
    }

    /// Removes constant expressions that may change across partitions.
    /// This method should be used when merging data from different partitions.
    pub fn clear_per_partition_constants(&mut self) {
        self.eq_group.clear_per_partition_constants();
    }

    /// Extends this `EquivalenceProperties` by adding the orderings inside
    /// collection `other`.
    pub fn extend_orderings(&mut self, other: impl IntoIterator<Item = LexOrdering>) {
        self.oeq_class.extend(other);
    }

    /// Adds new orderings into the existing ordering equivalence class.
    pub fn add_orderings(
        &mut self,
        orderings: impl IntoIterator<Item = impl IntoIterator<Item = PhysicalSortExpr>>,
    ) {
        self.oeq_class.add_orderings(orderings);
    }

    /// Adds a single ordering to the existing ordering equivalence class.
    pub fn add_ordering(&mut self, ordering: impl IntoIterator<Item = PhysicalSortExpr>) {
        self.add_orderings(std::iter::once(ordering));
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
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
    ) -> Result<()> {
        // Add equal expressions to the state:
        self.eq_group.add_equal_conditions(Arc::clone(&left), right);
        // Discover any new orderings:
        self.discover_new_orderings(left)
    }

    /// Track/register physical expressions with constant values.
    pub fn add_constants(&mut self, constants: impl IntoIterator<Item = ConstExpr>) {
        let c = constants.into_iter().collect::<Vec<_>>();
        let constants = c.into_iter();
        // Add the new constant to the equivalence group:
        for constant in constants {
            self.eq_group.add_constant(constant);
        }
        // Discover any new orderings based on the constants
        for ordering in self.normalized_oeq_class().iter() {
            self.discover_new_orderings(Arc::clone(&ordering[0].expr))
                .unwrap();
        }
    }

    // Discover new valid orderings in light of a new equality.
    // Accepts a single argument (`expr`) which is used to determine
    // which orderings should be updated.
    // When constants or equivalence classes are changed, there may be new orderings
    // that can be discovered with the new equivalence properties.
    // For a discussion, see: https://github.com/apache/datafusion/issues/9812
    fn discover_new_orderings(&mut self, expr: Arc<dyn PhysicalExpr>) -> Result<()> {
        let normalized_expr = self.eq_group().normalize_expr(expr);
        let eq_class = self
            .eq_group
            .iter()
            .find(|class| class.contains(&normalized_expr))
            .map(|class| class.clone().into())
            .unwrap_or_else(|| vec![Arc::clone(&normalized_expr)]);

        let mut new_orderings = vec![];
        for ordering in self.normalized_oeq_class() {
            if !ordering[0].expr.eq(&normalized_expr) {
                continue;
            }

            let leading_ordering_options = ordering[0].options;

            'exprs: for equivalent_expr in &eq_class {
                let children = equivalent_expr.children();
                if children.is_empty() {
                    continue;
                }
                // Check if all children match the next expressions in the ordering:
                let mut child_properties = vec![];
                // Build properties for each child based on the next expression:
                for (i, child) in children.into_iter().enumerate() {
                    let Some(next) = ordering.get(i + 1) else {
                        break 'exprs;
                    };
                    if !next.expr.eq(child) {
                        break 'exprs;
                    }
                    let data_type = child.data_type(&self.schema)?;
                    child_properties.push(ExprProperties {
                        sort_properties: SortProperties::Ordered(next.options),
                        range: Interval::make_unbounded(&data_type)?,
                        preserves_lex_ordering: true,
                    });
                }
                // Check if the expression is monotonic in all arguments:
                let expr_properties =
                    equivalent_expr.get_properties(&child_properties)?;
                if expr_properties.preserves_lex_ordering
                    && SortProperties::Ordered(leading_ordering_options)
                        == expr_properties.sort_properties
                {
                    // Assume existing ordering is `[c ASC, a ASC, b ASC]`. When
                    // equality `c = f(a, b)` is given, the ordering `[a ASC, b ASC]`,
                    // implies the ordering `[f(a, b) ASC]`. Thus, we can deduce that
                    // ordering `[a ASC, b ASC]` is also valid.
                    new_orderings.push(ordering[1..].to_vec());
                    break;
                }
            }
        }

        self.oeq_class.add_orderings(new_orderings);
        Ok(())
    }

    /// Updates the ordering equivalence group within assuming that the table
    /// is re-sorted according to the argument `ordering`. Note that constants
    /// and equivalence classes are unchanged as they are unaffected by a re-sort.
    /// If the given ordering is already satisfied, the function does nothing.
    pub fn with_reorder(
        mut self,
        ordering: impl IntoIterator<Item = PhysicalSortExpr>,
    ) -> Self {
        // Filter out constant expressions as they don't affect ordering
        let filtered_exprs = ordering
            .into_iter()
            .filter(|expr| self.is_expr_constant(&expr.expr).is_none())
            .collect::<Vec<_>>();

        if let Some(filtered_exprs) = LexOrdering::new(filtered_exprs) {
            // Preserve valid suffixes from existing orderings:
            let oeq_class = mem::take(&mut self.oeq_class);
            let mut new_orderings = oeq_class
                .into_iter()
                .filter(|existing| self.is_prefix_of(&filtered_exprs, existing))
                .collect::<Vec<_>>();
            if new_orderings.is_empty() {
                new_orderings.push(filtered_exprs);
            }
            self.oeq_class = new_orderings.into();
        }
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
    /// equivalence group and the ordering equivalence class within. Returns
    /// a `LexOrdering` instance if the expressions define a proper lexicographical
    /// ordering. It works by:
    /// - Removing expressions that have a constant value from the given expressions.
    /// - Replacing sections that belong to some equivalence class in the equivalence
    ///   group with the first entry in the matching equivalence class.
    ///
    /// Assume that `self.eq_group` states column `a` and `b` are aliases. Also
    /// assume that `self.oeq_class` contains equivalent orderings `d ASC` and
    /// `a ASC, c ASC` (in the sense that both describe the ordering of the
    /// table). If `sort_exprs` were `[b ASC, c ASC, a ASC]`, then this function
    /// would return `[a ASC, c ASC]`. Internally, it would first normalize to
    /// `[a ASC, c ASC, a ASC]` and end up with the final result after deduplication.
    pub fn normalize_sort_exprs(
        &self,
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    ) -> Option<LexOrdering> {
        // Prune redundant sections in the ordering:
        let sort_exprs = sort_exprs
            .into_iter()
            .map(|sort_expr| self.eq_group.normalize_sort_expr(sort_expr))
            .filter(|order| self.is_expr_constant(&order.expr).is_none());
        LexOrdering::new(sort_exprs).map(|o| o.collapse())
    }

    /// Normalizes the given sort requirements (i.e. `sort_reqs`) using the
    /// equivalence group and the ordering equivalence class within. Returns
    /// a `LexRequirement` instance if the expressions define a proper lexicographical
    /// ordering requirement. It works by:
    /// - Removing expressions that have a constant value from the given requirements.
    /// - Replacing sections that belong to some equivalence class in the equivalence
    ///   group with the first entry in the matching equivalence class.
    ///
    /// Assume that `self.eq_group` states column `a` and `b` are aliases. Also
    /// assume that `self.oeq_class` contains equivalent orderings `d ASC` and
    /// `a ASC, c ASC` (in the sense that both describe the ordering of the
    /// table). If `sort_exprs` were `[b ASC, c ASC, a ASC]`, then this function
    /// would return `[a ASC, c ASC]`. Internally, it would first normalize to
    /// `[a ASC, c ASC, a ASC]` and end up with the final result after deduplication.
    fn normalize_sort_requirements(
        &self,
        sort_reqs: impl IntoIterator<Item = PhysicalSortRequirement>,
    ) -> Option<LexRequirement> {
        // Prune redundant sections in the requirement:
        let reqs = sort_reqs
            .into_iter()
            .map(|req| self.eq_group.normalize_sort_requirement(req))
            .filter(|order| self.is_expr_constant(&order.expr).is_none());
        LexRequirement::new(reqs).map(|r| r.collapse())
    }

    /// Checks whether the given ordering is satisfied by any of the existing
    /// orderings.
    pub fn ordering_satisfy(
        &self,
        given: impl IntoIterator<Item = PhysicalSortExpr>,
    ) -> bool {
        // First, standardize the given ordering:
        let Some(normalized_ordering) = self.normalize_sort_exprs(given) else {
            // If the ordering vanishes after normalization, it is satisfied:
            return true;
        };
        let length = normalized_ordering.len();
        self.common_sort_prefix_length(normalized_ordering) == length
    }

    /// Checks whether the given sort requirements are satisfied by any of the
    /// existing orderings.
    pub fn ordering_satisfy_requirement(
        &self,
        given: impl IntoIterator<Item = PhysicalSortRequirement>,
    ) -> bool {
        // First, standardize the given requirement:
        let Some(normalized_reqs) = self.normalize_sort_requirements(given) else {
            // If the requirement vanishes after normalization, it is satisfied:
            return true;
        };
        // Then, check whether given requirement is satisfied by constraints:
        if self.satisfied_by_constraints(&normalized_reqs) {
            return true;
        }
        let schema = self.schema();
        let mut eq_properties = self.clone();
        for element in normalized_reqs {
            // Check whether given requirement is satisfied:
            let ExprProperties {
                sort_properties, ..
            } = eq_properties.get_expr_properties(Arc::clone(&element.expr));
            let satisfy = match sort_properties {
                SortProperties::Ordered(options) => {
                    let sort_expr = PhysicalSortExpr {
                        expr: Arc::clone(&element.expr),
                        options,
                    };
                    sort_expr.satisfy(&element, schema)
                }
                // Singleton expressions satisfies any requirement.
                SortProperties::Singleton => true,
                SortProperties::Unordered => false,
            };
            if !satisfy {
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
            let const_expr = ConstExpr::from(element.expr);
            eq_properties.add_constants(std::iter::once(const_expr));
        }
        true
    }

    /// Returns the number of consecutive sort expressions (starting from the
    /// left) that are satisfied by the existing ordering.
    fn common_sort_prefix_length(&self, normalized_ordering: LexOrdering) -> usize {
        let full_length = normalized_ordering.len();
        // Check whether the given ordering is satisfied by constraints:
        if self.satisfied_by_constraints_ordering(&normalized_ordering) {
            // If constraints satisfy all sort expressions, return the full
            // length:
            return full_length;
        }
        let schema = self.schema();
        let mut eq_properties = self.clone();
        for (idx, element) in normalized_ordering.into_iter().enumerate() {
            // Check whether given ordering is satisfied:
            let ExprProperties {
                sort_properties, ..
            } = eq_properties.get_expr_properties(Arc::clone(&element.expr));
            let satisfy = match sort_properties {
                SortProperties::Ordered(options) => {
                    let sort_expr = PhysicalSortExpr {
                        expr: Arc::clone(&element.expr),
                        options,
                    };
                    sort_expr.satisfy_expr(&element, schema)
                }
                // Singleton expressions satisfies any ordering.
                SortProperties::Singleton => true,
                SortProperties::Unordered => false,
            };
            if !satisfy {
                // As soon as one sort expression is unsatisfied, return how
                // many we've satisfied so far:
                return idx;
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
            let const_expr = ConstExpr::from(element.expr);
            eq_properties.add_constants(std::iter::once(const_expr));
        }

        // All sort expressions are satisfied, return full length:
        full_length
    }

    /// Determines the longest normalized prefix of `ordering` satisfied by the
    /// existing ordering. Returns that prefix as a new `LexOrdering`, and a
    /// boolean indicating whether all the sort expressions are satisfied.
    pub fn extract_common_sort_prefix(
        &self,
        ordering: LexOrdering,
    ) -> (Vec<PhysicalSortExpr>, bool) {
        // First, standardize the given ordering:
        let Some(normalized_ordering) = self.normalize_sort_exprs(ordering) else {
            // If the ordering vanishes after normalization, it is satisfied:
            return (vec![], true);
        };
        let prefix_len = self.common_sort_prefix_length(normalized_ordering.clone());
        let flag = prefix_len == normalized_ordering.len();
        let mut sort_exprs: Vec<_> = normalized_ordering.into();
        if !flag {
            sort_exprs.truncate(prefix_len);
        }
        (sort_exprs, flag)
    }

    /// Checks if the sort expressions are satisfied by any of the table
    /// constraints (primary key or unique). Returns true if any constraint
    /// fully satisfies the expressions (i.e. constraint indices form a valid
    /// prefix of an existing ordering that matches the expressions). For
    /// unique constraints, also verifies nullable columns.
    fn satisfied_by_constraints_ordering(
        &self,
        normalized_exprs: &[PhysicalSortExpr],
    ) -> bool {
        self.constraints.iter().any(|constraint| match constraint {
            Constraint::PrimaryKey(indices) | Constraint::Unique(indices) => {
                let check_null = matches!(constraint, Constraint::Unique(_));
                let normalized_size = normalized_exprs.len();
                indices.len() <= normalized_size
                    && self.oeq_class.iter().any(|ordering| {
                        let length = ordering.len();
                        if indices.len() > length || normalized_size < length {
                            return false;
                        }
                        // Build a map of column positions in the ordering:
                        let mut col_positions = HashMap::with_capacity(length);
                        for (pos, req) in ordering.iter().enumerate() {
                            if let Some(col) = req.expr.as_any().downcast_ref::<Column>()
                            {
                                let nullable = col.nullable(&self.schema).unwrap_or(true);
                                col_positions.insert(col.index(), (pos, nullable));
                            }
                        }
                        // Check if all constraint indices appear in valid positions:
                        if !indices.iter().all(|idx| {
                            col_positions.get(idx).is_some_and(|&(pos, nullable)| {
                                // For unique constraints, verify column is not nullable if it's first/last:
                                !check_null
                                    || !nullable
                                    || (pos != 0 && pos != length - 1)
                            })
                        }) {
                            return false;
                        }
                        // Check if this ordering matches the prefix:
                        normalized_exprs
                            .iter()
                            .zip(ordering)
                            .all(|(given, existing)| {
                                existing.satisfy_expr(given, &self.schema)
                            })
                    })
            }
        })
    }

    /// Checks if the sort requirements are satisfied by any of the table
    /// constraints (primary key or unique). Returns true if any constraint
    /// fully satisfies the requirements (i.e. constraint indices form a valid
    /// prefix of an existing ordering that matches the requirements). For
    /// unique constraints, also verifies nullable columns.
    fn satisfied_by_constraints(
        &self,
        normalized_reqs: &[PhysicalSortRequirement],
    ) -> bool {
        self.constraints.iter().any(|constraint| match constraint {
            Constraint::PrimaryKey(indices) | Constraint::Unique(indices) => {
                let check_null = matches!(constraint, Constraint::Unique(_));
                let normalized_size = normalized_reqs.len();
                indices.len() <= normalized_size
                    && self.oeq_class.iter().any(|ordering| {
                        let length = ordering.len();
                        if indices.len() > length || normalized_size < length {
                            return false;
                        }
                        // Build a map of column positions in the ordering:
                        let mut col_positions = HashMap::with_capacity(length);
                        for (pos, req) in ordering.iter().enumerate() {
                            if let Some(col) = req.expr.as_any().downcast_ref::<Column>()
                            {
                                let nullable = col.nullable(&self.schema).unwrap_or(true);
                                col_positions.insert(col.index(), (pos, nullable));
                            }
                        }
                        // Check if all constraint indices appear in valid positions:
                        if !indices.iter().all(|idx| {
                            col_positions.get(idx).is_some_and(|&(pos, nullable)| {
                                // For unique constraints, verify column is not nullable if it's first/last:
                                !check_null
                                    || !nullable
                                    || (pos != 0 && pos != length - 1)
                            })
                        }) {
                            return false;
                        }
                        // Check if this ordering matches the prefix:
                        normalized_reqs
                            .iter()
                            .zip(ordering)
                            .all(|(given, existing)| {
                                existing.satisfy(given, &self.schema)
                            })
                    })
            }
        })
    }

    /// Checks whether the `given` sort requirements are equal or more specific
    /// than the `reference` sort requirements.
    pub fn requirements_compatible(
        &self,
        given: LexRequirement,
        reference: LexRequirement,
    ) -> bool {
        let Some(normalized_given) = self.normalize_sort_requirements(given) else {
            return true;
        };
        let Some(normalized_reference) = self.normalize_sort_requirements(reference)
        else {
            return true;
        };

        (normalized_reference.len() <= normalized_given.len())
            && normalized_reference
                .into_iter()
                .zip(normalized_given)
                .all(|(reference, given)| given.compatible(&reference))
    }

    /// Modify existing orderings by substituting sort expressions with appropriate
    /// targets from the projection mapping. We substitute a sort expression when
    /// its physical expression has a one-to-one functional relationship with a
    /// target expression in the mapping.
    ///
    /// After substitution, we may generate more than one `LexOrdering` for each
    /// existing equivalent ordering. For example, `[a ASC, b ASC]` will turn
    /// into `[CAST(a) ASC, b ASC]` and `[a ASC, b ASC]` when applying projection
    /// expressions `a, b, CAST(a)`.
    ///
    /// TODO: Handle all scenarios that allow substitution; e.g. when `x` is
    ///       sorted, `atan(x + 1000)` should also be substituted. For now, we
    ///       only consider single-column `CAST` expressions.
    fn substitute_oeq_class(
        &self,
        oeq_class: OrderingEquivalenceClass,
        mapping: &ProjectionMapping,
    ) -> OrderingEquivalenceClass {
        let new_orderings = oeq_class.into_iter().flat_map(|order| {
            // Modify/expand existing orderings by substituting sort
            // expressions with appropriate targets from the mapping:
            order
                .into_iter()
                .map(|sort_expr| {
                    let referring_exprs = mapping
                        .iter()
                        .map(|(source, _target)| source)
                        .filter(|source| expr_refers(source, &sort_expr.expr))
                        .cloned();
                    let mut result = vec![];
                    // The sort expression comes from this schema, so the
                    // following call to `unwrap` is safe.
                    let expr_type = sort_expr.expr.data_type(&self.schema).unwrap();
                    // TODO: Add one-to-one analysis for ScalarFunctions.
                    for r_expr in referring_exprs {
                        // We check whether this expression is substitutable.
                        if let Some(cast_expr) =
                            r_expr.as_any().downcast_ref::<CastExpr>()
                        {
                            // For casts, we need to know whether the cast
                            // expression matches:
                            if cast_expr.expr.eq(&sort_expr.expr)
                                && cast_expr.is_bigger_cast(&expr_type)
                            {
                                result.push(PhysicalSortExpr::new(
                                    r_expr,
                                    sort_expr.options,
                                ));
                            }
                        }
                    }
                    result.push(sort_expr);
                    result
                })
                // Generate all valid orderings given substituted expressions:
                .multi_cartesian_product()
        });
        OrderingEquivalenceClass::new(new_orderings)
    }

    /// Projects argument `expr` according to the projection described by
    /// `mapping`, taking equivalences into account.
    ///
    /// For example, assume that columns `a` and `c` are always equal, and that
    /// the projection described by `mapping` encodes the following:
    ///
    /// ```text
    /// a -> a1
    /// b -> b1
    /// ```
    ///
    /// Then, this function projects `a + b` to `Some(a1 + b1)`, `c + b` to
    /// `Some(a1 + b1)` and `d` to `None`, meaning that it is not projectable.
    pub fn project_expr(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        mapping: &ProjectionMapping,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        self.eq_group.project_expr(mapping, expr)
    }

    /// Projects the given `expressions` according to the projection described
    /// by `mapping`, taking equivalences into account. This function is similar
    /// to [`Self::project_expr`], but projects multiple expressions at once
    /// more efficiently than calling `project_expr` for each expression.
    pub fn project_expressions<'a>(
        &'a self,
        expressions: impl IntoIterator<Item = &'a Arc<dyn PhysicalExpr>> + 'a,
        mapping: &'a ProjectionMapping,
    ) -> impl Iterator<Item = Option<Arc<dyn PhysicalExpr>>> + 'a {
        self.eq_group.project_expressions(mapping, expressions)
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
    fn construct_dependency_map(
        &self,
        oeq_class: OrderingEquivalenceClass,
        mapping: &ProjectionMapping,
    ) -> DependencyMap {
        let mut map = DependencyMap::default();
        for ordering in oeq_class.into_iter() {
            // Previous expression is a dependency. Note that there is no
            // dependency for the leading expression.
            if !self.insert_to_dependency_map(
                mapping,
                ordering[0].clone(),
                None,
                &mut map,
            ) {
                continue;
            }
            for (dependency, sort_expr) in ordering.into_iter().tuple_windows() {
                if !self.insert_to_dependency_map(
                    mapping,
                    sort_expr,
                    Some(dependency),
                    &mut map,
                ) {
                    // If we can't project, stop constructing the dependency map
                    // as remaining dependencies will be invalid post projection.
                    break;
                }
            }
        }
        map
    }

    /// Projects the sort expression according to the projection mapping and
    /// inserts it into the dependency map with the given dependency. Returns
    /// a boolean flag indicating whether the given expression is projectable.
    fn insert_to_dependency_map(
        &self,
        mapping: &ProjectionMapping,
        sort_expr: PhysicalSortExpr,
        dependency: Option<PhysicalSortExpr>,
        map: &mut DependencyMap,
    ) -> bool {
        let target_sort_expr = self
            .project_expr(&sort_expr.expr, mapping)
            .map(|expr| PhysicalSortExpr::new(expr, sort_expr.options));
        let projectable = target_sort_expr.is_some();
        if projectable
            || mapping
                .iter()
                .any(|(source, _)| expr_refers(source, &sort_expr.expr))
        {
            // Add sort expressions that can be projected or referred to
            // by any of the projection expressions to the dependency map:
            map.insert(sort_expr, target_sort_expr, dependency);
        }
        projectable
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
        mapping
            .iter()
            .map(|(source, target)| {
                let normalized_source = self.eq_group.normalize_expr(Arc::clone(source));
                (normalized_source, target.clone())
            })
            .collect::<ProjectionMapping>()
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
        // Normalize source expressions in the mapping:
        let mapping = self.normalized_mapping(mapping);
        // Get dependency map for existing orderings:
        let mut oeq_class = self.normalized_oeq_class();
        oeq_class = self.substitute_oeq_class(oeq_class, &mapping);
        let dependency_map = self.construct_dependency_map(oeq_class, &mapping);
        let orderings = mapping.iter().flat_map(|(source, targets)| {
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
                    // Generate dependent orderings (i.e. prefixes for targets):
                    let dependency_orderings =
                        generate_dependency_orderings(&relevant_deps, &dependency_map);
                    let sort_exprs = targets.iter().map(|(target, _)| {
                        PhysicalSortExpr::new(Arc::clone(target), options)
                    });
                    if dependency_orderings.is_empty() {
                        sort_exprs.map(|sort_expr| [sort_expr].into()).collect()
                    } else {
                        sort_exprs
                            .flat_map(|sort_expr| {
                                let mut result = dependency_orderings.clone();
                                for ordering in result.iter_mut() {
                                    ordering.push(sort_expr.clone());
                                }
                                result
                            })
                            .collect::<Vec<_>>()
                    }
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
                if let Some(target) = &node.target {
                    prefixes.push([target.clone()].into());
                }
            } else {
                // Append current ordering on top its dependencies:
                for ordering in prefixes.iter_mut() {
                    if let Some(target) = &node.target {
                        ordering.push(target.clone())
                    }
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

    /// Projects constraints according to the given projection mapping.
    ///
    /// This function takes a projection mapping and extracts the column indices of the target columns.
    /// It then projects the constraints to only include relationships between
    /// columns that exist in the projected output.
    ///
    /// # Arguments
    ///
    /// * `mapping` - A reference to `ProjectionMapping` that defines how expressions are mapped
    ///   in the projection operation
    ///
    /// # Returns
    ///
    /// Returns a new `Constraints` object containing only the constraints
    /// that are valid for the projected columns.
    fn projected_constraints(&self, mapping: &ProjectionMapping) -> Option<Constraints> {
        let indices = mapping
            .iter()
            .flat_map(|(_, targets)| {
                targets.iter().flat_map(|(target, _)| {
                    target.as_any().downcast_ref::<Column>().map(|c| c.index())
                })
            })
            .collect::<Vec<_>>();
        self.constraints.project(&indices)
    }

    /// Projects the equivalences within according to `mapping`
    /// and `output_schema`.
    pub fn project(&self, mapping: &ProjectionMapping, output_schema: SchemaRef) -> Self {
        let eq_group = self.eq_group.project(mapping);
        let oeq_class = self.projected_orderings(mapping).into();
        let constraints = self.projected_constraints(mapping).unwrap_or_default();
        Self {
            schema: output_schema,
            eq_group,
            oeq_class,
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
    ) -> (Vec<PhysicalSortExpr>, Vec<usize>) {
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
        for _ in 0..exprs.len() {
            // Get ordered expressions with their indices.
            let ordered_exprs = search_indices
                .iter()
                .filter_map(|&idx| {
                    let ExprProperties {
                        sort_properties, ..
                    } = eq_properties.get_expr_properties(Arc::clone(&exprs[idx]));
                    match sort_properties {
                        SortProperties::Ordered(options) => {
                            let expr = Arc::clone(&exprs[idx]);
                            Some((PhysicalSortExpr::new(expr, options), idx))
                        }
                        SortProperties::Singleton => {
                            // Assign default ordering to constant expressions:
                            let expr = Arc::clone(&exprs[idx]);
                            Some((PhysicalSortExpr::new_default(expr), idx))
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
                let const_expr = ConstExpr::from(Arc::clone(expr));
                eq_properties.add_constants(std::iter::once(const_expr));
                search_indices.shift_remove(idx);
            }
            // Add new ordered section to the state.
            result.extend(ordered_exprs);
        }
        result.into_iter().unzip()
    }

    /// This function determines whether the provided expression is constant
    /// based on the known constants. For example, if columns `a` and `b` are
    /// constant, then expressions `a`, `b` and `a + b` will all return `true`
    /// whereas expression `c` will return `false`.
    ///
    /// # Parameters
    ///
    /// - `expr`: A reference to a `Arc<dyn PhysicalExpr>` representing the
    ///   expression to be checked.
    ///
    /// # Returns
    ///
    /// Returns a `Some` value if the expression is constant according to
    /// equivalence group, and `None` otherwise. The `Some` variant contains
    /// an `AcrossPartitions` value indicating whether the expression is
    /// constant across partitions, and its actual value (if available).
    pub fn is_expr_constant(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Option<AcrossPartitions> {
        self.eq_group.is_expr_constant(expr)
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
    pub fn with_new_schema(mut self, schema: SchemaRef) -> Result<Self> {
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
                "Schemas have to be aligned to rewrite equivalences:\n Old schema: {:?}\n New schema: {:?}",
                self.schema,
                schema
            );
        }

        // Rewrite equivalence classes according to the new schema:
        let mut eq_classes = vec![];
        for mut eq_class in self.eq_group {
            // Rewrite the expressions in the equivalence class:
            eq_class.exprs = eq_class
                .exprs
                .into_iter()
                .map(|expr| with_new_schema(expr, &schema))
                .collect::<Result<_>>()?;
            // Rewrite the constant value (if available and known):
            let data_type = eq_class
                .canonical_expr()
                .map(|e| e.data_type(&schema))
                .transpose()?;
            if let (Some(data_type), Some(AcrossPartitions::Uniform(Some(value)))) =
                (data_type, &mut eq_class.constant)
            {
                *value = value.cast_to(&data_type)?;
            }
            eq_classes.push(eq_class);
        }

        // Rewrite orderings according to new schema:
        let new_orderings = self
            .oeq_class
            .into_iter()
            .map(|ordering| {
                ordering
                    .into_iter()
                    .map(|mut sort_expr| {
                        sort_expr.expr = with_new_schema(sort_expr.expr, &schema)?;
                        Ok(sort_expr)
                    })
                    .collect::<Result<Vec<_>>>()
                    // The following `unwrap` is safe because the vector will always
                    // be non-empty.
                    .map(|v| LexOrdering::new(v).unwrap())
            })
            .collect::<Result<Vec<_>>>()?;

        // Update the schema, the equivalence group and the ordering equivalence
        // class:
        self.schema = schema;
        self.eq_group = EquivalenceGroup::new(eq_classes);
        self.oeq_class = new_orderings.into();
        Ok(self)
    }
}

impl From<EquivalenceProperties> for OrderingEquivalenceClass {
    fn from(eq_properties: EquivalenceProperties) -> Self {
        eq_properties.oeq_class
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
        if self.eq_group.is_empty() && self.oeq_class.is_empty() {
            return write!(f, "No properties");
        }
        if !self.oeq_class.is_empty() {
            write!(f, "order: {}", self.oeq_class)?;
        }
        if !self.eq_group.is_empty() {
            write!(f, ", eq: {}", self.eq_group)?;
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
    if eq_properties.is_expr_constant(&normalized_expr).is_some()
        || oeq_class.is_expr_partial_const(&normalized_expr)
    {
        node.data.sort_properties = SortProperties::Singleton;
    } else if let Some(options) = oeq_class.get_options(&normalized_expr) {
        node.data.sort_properties = SortProperties::Ordered(options);
    }
    Ok(Transformed::yes(node))
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
            range: literal.value().into(),
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
