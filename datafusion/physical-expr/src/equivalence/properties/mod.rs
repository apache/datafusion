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

use std::fmt::{self, Display};
use std::mem;
use std::sync::Arc;

use self::dependency::{
    Dependencies, DependencyMap, construct_prefix_orderings,
    generate_dependency_orderings, referred_dependencies,
};
use crate::equivalence::{
    AcrossPartitions, EquivalenceGroup, GroupingEquivalenceClass,
    OrderingEquivalenceClass, ProjectionMapping,
};
use crate::expressions::{Column, Literal, with_new_schema};
use crate::{
    ConstExpr, LexOrdering, LexRequirement, PhysicalExpr, PhysicalSortExpr,
    PhysicalSortRequirement,
};

use arrow::datatypes::SchemaRef;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Constraint, Constraints, HashMap, HashSet, Result, plan_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_physical_expr_common::sort_expr::options_compatible;
use datafusion_physical_expr_common::utils::ExprPropertiesNode;

use indexmap::IndexSet;
use itertools::Itertools;

/// `EquivalenceProperties` stores information about the output of a plan node
/// that can be used to optimize the plan. Currently, it keeps track of:
/// - Sort expressions (orderings),
/// - Complete expression tuples whose equal values are contiguous,
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
///     PhysicalSortExpr::new_default(col_a).asc(),
///     PhysicalSortExpr::new_default(col_c).desc(),
/// ]);
///
/// assert_eq!(
///     eq_properties.to_string(),
///     "order: [[a@0 ASC, c@2 DESC]], eq: [{members: [b@1], constant: (heterogeneous)}]"
/// );
/// ```
#[derive(Clone, Debug)]
pub struct EquivalenceProperties {
    /// Distinct equivalence classes (i.e. expressions with the same value).
    eq_group: EquivalenceGroup,
    /// Equivalent sort expressions (i.e. those define the same ordering).
    oeq_class: OrderingEquivalenceClass,
    /// Complete expression tuples whose equal values occupy one contiguous run
    /// in each output partition.
    geq_class: GroupingEquivalenceClass,
    /// Cache storing equivalent sort expressions in normal form (i.e. without
    /// constants/duplicates and in standard form) and a map associating leading
    /// terms with full sort expressions.
    oeq_cache: OrderingEquivalenceCache,
    /// Grouping expressions in normal form (i.e. without constants,
    /// duplicates, or non-canonical equivalent expressions).
    geq_cache: GroupingEquivalenceClass,
    /// Table constraints that factor in equivalence calculations.
    constraints: Constraints,
    /// Schema associated with this object.
    schema: SchemaRef,
}

/// This object serves as a cache for storing equivalent sort expressions
/// in normal form, and a map associating leading sort expressions with
/// full lexicographical orderings. With this information, DataFusion can
/// efficiently determine whether a given ordering is satisfied by the
/// existing orderings, and discover new orderings based on the existing
/// equivalence properties.
#[derive(Clone, Debug, Default)]
struct OrderingEquivalenceCache {
    /// Equivalent sort expressions in normal form.
    normal_cls: OrderingEquivalenceClass,
    /// Map associating leading sort expressions with full lexicographical
    /// orderings. Values are indices into `normal_cls`.
    leading_map: HashMap<Arc<dyn PhysicalExpr>, Vec<usize>>,
}

impl OrderingEquivalenceCache {
    /// Creates a new `OrderingEquivalenceCache` object with the given
    /// equivalent orderings, which should be in normal form.
    pub fn new(
        orderings: impl IntoIterator<Item = impl IntoIterator<Item = PhysicalSortExpr>>,
    ) -> Self {
        let mut cache = Self {
            normal_cls: OrderingEquivalenceClass::new(orderings),
            leading_map: HashMap::new(),
        };
        cache.update_map();
        cache
    }

    /// Updates/reconstructs the leading expression map according to the normal
    /// ordering equivalence class within.
    pub fn update_map(&mut self) {
        self.leading_map.clear();
        for (idx, ordering) in self.normal_cls.iter().enumerate() {
            let expr = Arc::clone(&ordering.first().expr);
            self.leading_map.entry(expr).or_default().push(idx);
        }
    }

    /// Clears the cache, removing all orderings and leading expressions.
    pub fn clear(&mut self) {
        self.normal_cls.clear();
        self.leading_map.clear();
    }
}

/// Counts how often [`EquivalenceProperties::project_reusing`] reused a cached
/// equivalence group instead of reprojecting it.
///
/// Thread local rather than a global counter: the test binary runs tests in
/// parallel, and a shared count would let one test observe another's hits.
#[cfg(test)]
mod eq_group_reuse_probe {
    use std::cell::Cell;

    thread_local! {
        static HITS: Cell<usize> = const { Cell::new(0) };
    }

    pub(super) fn record_hit() {
        HITS.with(|h| h.set(h.get() + 1));
    }

    /// Runs `f` and reports how many reuses happened while it did.
    pub(super) fn count<T>(f: impl FnOnce() -> T) -> (T, usize) {
        let before = HITS.with(Cell::get);
        let value = f();
        (value, HITS.with(Cell::get) - before)
    }
}

impl EquivalenceProperties {
    /// Helper used by the ordering equivalence rule when considering whether
    /// an expression can replace an existing sort key without invalidating
    /// the ordering.
    ///
    /// The substitution is only allowed when, treating the sort key as the
    /// only ordered input, the expression reports the same ordering *and*
    /// that it is a one-to-one, order-preserving function of it (see
    /// [`ExprProperties::strictly_order_preserving`]). For example, a
    /// widening `CAST` of the sort key qualifies, while a narrowing one does
    /// not, as it could collapse distinct values and violate the existing
    /// sort order.
    fn substitute_order_preserving_ordering(
        r_expr: Arc<dyn PhysicalExpr>,
        sort_expr: &PhysicalSortExpr,
        schema: &SchemaRef,
    ) -> Option<PhysicalSortExpr> {
        if r_expr.eq(&sort_expr.expr) {
            // No point in substituting an expression with itself.
            return None;
        }
        let dependencies = Dependencies::new(std::iter::once(sort_expr.clone()));
        let properties = get_expr_properties(&r_expr, &dependencies, schema).ok()?;
        (properties.strictly_order_preserving
            && properties.sort_properties == SortProperties::Ordered(sort_expr.options))
        .then(|| PhysicalSortExpr::new(r_expr, sort_expr.options))
    }

    /// Creates an empty `EquivalenceProperties` object.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            eq_group: EquivalenceGroup::default(),
            oeq_class: OrderingEquivalenceClass::default(),
            geq_class: GroupingEquivalenceClass::default(),
            oeq_cache: OrderingEquivalenceCache::default(),
            geq_cache: GroupingEquivalenceClass::default(),
            constraints: Constraints::default(),
            schema,
        }
    }

    /// Adds constraints to the properties.
    pub fn set_constraints(&mut self, constraints: Constraints) {
        self.constraints = constraints;
    }

    /// Adds constraints to the properties.
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.set_constraints(constraints);
        self
    }

    /// Creates a new `EquivalenceProperties` object with the given orderings.
    pub fn new_with_orderings(
        schema: SchemaRef,
        orderings: impl IntoIterator<Item = impl IntoIterator<Item = PhysicalSortExpr>>,
    ) -> Self {
        let eq_group = EquivalenceGroup::default();
        let oeq_class = OrderingEquivalenceClass::new(orderings);
        // Here, we can avoid performing a full normalization, and get by with
        // only removing constants because the equivalence group is empty.
        let normal_orderings = oeq_class.iter().cloned().map(|o| {
            o.into_iter()
                .filter(|sort_expr| eq_group.is_expr_constant(&sort_expr.expr).is_none())
        });
        Self {
            oeq_cache: OrderingEquivalenceCache::new(normal_orderings),
            oeq_class,
            geq_class: GroupingEquivalenceClass::default(),
            geq_cache: GroupingEquivalenceClass::default(),
            eq_group,
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

    /// Returns a reference to the grouping equivalence class within.
    pub fn geq_class(&self) -> &GroupingEquivalenceClass {
        &self.geq_class
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
            .flat_map(|c| {
                c.iter().filter_map(|expr| {
                    c.constant
                        .as_ref()
                        .map(|across| ConstExpr::new(Arc::clone(expr), across.clone()))
                })
            })
            .collect()
    }

    /// Returns the output ordering of the properties.
    pub fn output_ordering(&self) -> Option<LexOrdering> {
        let concat = self.oeq_class.iter().flat_map(|o| o.iter().cloned());
        self.normalize_sort_exprs(concat)
    }

    /// Extends this `EquivalenceProperties` with the `other` object.
    pub fn extend(mut self, other: Self) -> Result<Self> {
        self.constraints.extend(other.constraints);
        self.add_equivalence_group(other.eq_group)?;
        self.add_orderings(other.oeq_class);
        self.add_groupings(other.geq_class);
        Ok(self)
    }

    /// Clears (empties) the ordering equivalence class within this object.
    /// Call this method when existing orderings are invalidated.
    pub fn clear_orderings(&mut self) {
        self.oeq_class.clear();
        self.oeq_cache.clear();
    }

    /// Clears grouping information invalidated by an operation that changes
    /// row sequence or combines input partitions.
    pub fn clear_groupings(&mut self) {
        self.geq_class.clear();
        self.geq_cache.clear();
    }

    /// Removes constant expressions that may change across partitions.
    /// This method should be used when merging data from different partitions.
    pub fn clear_per_partition_constants(&mut self) {
        if self.eq_group.clear_per_partition_constants() {
            // Renormalize orderings if the equivalence group changes:
            let normal_orderings = self
                .oeq_class
                .iter()
                .cloned()
                .map(|o| self.eq_group.normalize_sort_exprs(o));
            self.oeq_cache = OrderingEquivalenceCache::new(normal_orderings);
            self.update_geq_cache();
        }
    }

    /// Adds new orderings into the existing ordering equivalence class.
    pub fn add_orderings(
        &mut self,
        orderings: impl IntoIterator<Item = impl IntoIterator<Item = PhysicalSortExpr>>,
    ) {
        let orderings: Vec<_> =
            orderings.into_iter().filter_map(LexOrdering::new).collect();
        let normal_orderings: Vec<_> = orderings
            .iter()
            .cloned()
            .filter_map(|o| self.normalize_sort_exprs(o))
            .collect();
        if !normal_orderings.is_empty() {
            self.oeq_class.extend(orderings);
            // Normalize given orderings to update the cache:
            self.oeq_cache.normal_cls.extend(normal_orderings);
            // TODO: If no ordering is found to be redundant during extension, we
            //       can use a shortcut algorithm to update the leading map.
            self.oeq_cache.update_map();
        }
    }

    /// Adds a single ordering to the existing ordering equivalence class.
    pub fn add_ordering(&mut self, ordering: impl IntoIterator<Item = PhysicalSortExpr>) {
        self.add_orderings(std::iter::once(ordering));
    }

    /// Adds complete grouping tuples whose equal values are contiguous within
    /// every output partition.
    ///
    /// This is a correctness assertion. DataFusion does not verify the row
    /// layout, and consumers may act on a tuple as soon as its values change.
    /// Callers must therefore only add tuples that hold for every output
    /// partition.
    ///
    /// Expression order within a tuple is not significant. A tuple only
    /// describes its complete set of expressions: grouping `[a, b]` does not
    /// imply grouping `[a]` or `[b]`.
    pub fn add_groupings(
        &mut self,
        groupings: impl IntoIterator<Item = impl IntoIterator<Item = Arc<dyn PhysicalExpr>>>,
    ) {
        for grouping in GroupingEquivalenceClass::new(groupings) {
            let normal_grouping = self.normalize_grouping(grouping.iter().cloned());
            self.geq_class.add_groupings(std::iter::once(grouping));
            if !normal_grouping.is_empty() {
                self.geq_cache
                    .add_groupings(std::iter::once(normal_grouping));
            }
        }
    }

    /// Adds one complete grouping tuple.
    pub fn add_grouping(
        &mut self,
        grouping: impl IntoIterator<Item = Arc<dyn PhysicalExpr>>,
    ) {
        self.add_groupings(std::iter::once(grouping));
    }

    fn normalize_grouping(
        &self,
        grouping: impl IntoIterator<Item = Arc<dyn PhysicalExpr>>,
    ) -> Vec<Arc<dyn PhysicalExpr>> {
        let mut seen = HashSet::new();
        grouping
            .into_iter()
            .map(|expr| self.eq_group.normalize_expr(expr))
            .filter(|expr| self.eq_group.is_expr_constant(expr).is_none())
            .filter(|expr| seen.insert(Arc::clone(expr)))
            .collect()
    }

    fn update_geq_cache(&mut self) {
        let groupings = self
            .geq_class
            .iter()
            .map(|grouping| self.normalize_grouping(grouping.iter().cloned()))
            .collect::<Vec<_>>();
        self.geq_cache = GroupingEquivalenceClass::new(groupings);
    }

    fn update_oeq_cache(&mut self) -> Result<()> {
        // Renormalize orderings if the equivalence group changes:
        let normal_cls = mem::take(&mut self.oeq_cache.normal_cls);
        let normal_orderings = normal_cls
            .into_iter()
            .map(|o| self.eq_group.normalize_sort_exprs(o));
        self.oeq_cache.normal_cls = OrderingEquivalenceClass::new(normal_orderings);
        self.oeq_cache.update_map();
        // Discover any new orderings based on the new equivalence classes:
        let leading_exprs: Vec<_> = self.oeq_cache.leading_map.keys().cloned().collect();
        for expr in leading_exprs {
            self.discover_new_orderings(expr)?;
        }
        Ok(())
    }

    /// Incorporates the given equivalence group to into the existing
    /// equivalence group within.
    pub fn add_equivalence_group(
        &mut self,
        other_eq_group: EquivalenceGroup,
    ) -> Result<()> {
        if !other_eq_group.is_empty() {
            self.eq_group.extend(other_eq_group);
            self.update_oeq_cache()?;
            self.update_geq_cache();
        }
        Ok(())
    }

    /// Returns the ordering equivalence class within in normal form.
    /// Normalization standardizes expressions according to the equivalence
    /// group within, and removes constants/duplicates.
    pub fn normalized_oeq_class(&self) -> OrderingEquivalenceClass {
        self.oeq_class
            .iter()
            .cloned()
            .filter_map(|ordering| self.normalize_sort_exprs(ordering))
            .collect::<Vec<_>>()
            .into()
    }

    /// Returns the grouping equivalence class within in normal form.
    pub fn normalized_geq_class(&self) -> GroupingEquivalenceClass {
        self.geq_cache.clone()
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
        if self.eq_group.add_equal_conditions(left, right) {
            self.update_oeq_cache()?;
        }
        self.update_oeq_cache()?;
        self.update_geq_cache();
        Ok(())
    }

    /// Track/register physical expressions with constant values.
    pub fn add_constants(
        &mut self,
        constants: impl IntoIterator<Item = ConstExpr>,
    ) -> Result<()> {
        // Add the new constant to the equivalence group:
        for constant in constants {
            self.eq_group.add_constant(constant);
        }
        // Renormalize the orderings after adding new constants by removing
        // the constants from existing orderings:
        let normal_cls = mem::take(&mut self.oeq_cache.normal_cls);
        let normal_orderings = normal_cls.into_iter().map(|ordering| {
            ordering.into_iter().filter(|sort_expr| {
                self.eq_group.is_expr_constant(&sort_expr.expr).is_none()
            })
        });
        self.oeq_cache.normal_cls = OrderingEquivalenceClass::new(normal_orderings);
        self.oeq_cache.update_map();
        self.update_geq_cache();
        // Discover any new orderings based on the constants:
        let leading_exprs: Vec<_> = self.oeq_cache.leading_map.keys().cloned().collect();
        for expr in leading_exprs {
            self.discover_new_orderings(expr)?;
        }
        Ok(())
    }

    /// Discover new valid orderings in light of a new equality. Accepts a single
    /// argument (`expr`) which is used to determine the orderings to update.
    /// When constants or equivalence classes change, there may be new orderings
    /// that can be discovered with the new equivalence properties.
    /// For a discussion, see: <https://github.com/apache/datafusion/issues/9812>
    fn discover_new_orderings(
        &mut self,
        normal_expr: Arc<dyn PhysicalExpr>,
    ) -> Result<()> {
        let Some(ordering_idxs) = self.oeq_cache.leading_map.get(&normal_expr) else {
            return Ok(());
        };
        let eq_class = self
            .eq_group
            .get_equivalence_class(&normal_expr)
            .map_or_else(|| vec![normal_expr], |class| class.clone().into());

        let mut new_orderings = vec![];
        for idx in ordering_idxs {
            let ordering = &self.oeq_cache.normal_cls[*idx];
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
                        strictly_order_preserving: true,
                    });
                }
                // Check if the expression is monotonic in all arguments:
                let expr_properties =
                    equivalent_expr.get_properties(&child_properties)?;
                if expr_properties.preserves_lex_ordering
                    && expr_properties.sort_properties
                        == SortProperties::Ordered(leading_ordering_options)
                {
                    // Assume that `[c ASC, a ASC, b ASC]` is among existing
                    // orderings. If equality `c = f(a, b)` is given, ordering
                    // `[a ASC, b ASC]` implies the ordering `[c ASC]`. Thus,
                    // ordering `[a ASC, b ASC]` is also a valid ordering.
                    new_orderings.push(ordering[1..].to_vec());
                    break;
                }
            }
        }

        if !new_orderings.is_empty() {
            self.add_orderings(new_orderings);
        }
        Ok(())
    }

    /// Updates the ordering equivalence class assuming that the table is
    /// re-sorted according to `ordering`, and returns whether the ordering
    /// class changed. Equivalence classes and constants are unaffected by a
    /// re-sort. Explicit grouping assertions are cleared because a sort may
    /// rearrange rows that compare equally, even when `ordering` was already
    /// satisfied.
    pub fn reorder(
        &mut self,
        ordering: impl IntoIterator<Item = PhysicalSortExpr>,
    ) -> Result<bool> {
        // A sort may reorder rows that compare equally under `ordering`, so
        // explicit grouping assertions do not necessarily survive even when
        // the requested ordering is already satisfied. Groupings implied by
        // the output ordering remain discoverable through `oeq_class`.
        self.clear_groupings();
        let (ordering, ordering_tee) = ordering.into_iter().tee();
        // First, standardize the given ordering:
        let Some(normal_ordering) = self.normalize_sort_exprs(ordering) else {
            // If the ordering vanishes after normalization, it is satisfied:
            return Ok(false);
        };
        if normal_ordering.len() != self.common_sort_prefix_length(&normal_ordering)? {
            // If the ordering is unsatisfied, the re-sort invalidates existing
            // orderings.
            self.clear_orderings();
            self.add_ordering(ordering_tee);
            return Ok(true);
        }
        Ok(false)
    }

    /// Normalizes the given sort expressions (i.e. `sort_exprs`) using the
    /// equivalence group within. Returns a `LexOrdering` instance if the
    /// expressions define a proper lexicographical ordering. For more details,
    /// see [`EquivalenceGroup::normalize_sort_exprs`].
    pub fn normalize_sort_exprs(
        &self,
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    ) -> Option<LexOrdering> {
        LexOrdering::new(self.eq_group.normalize_sort_exprs(sort_exprs))
    }

    /// Normalizes the given sort requirements (i.e. `sort_reqs`) using the
    /// equivalence group within. Returns a `LexRequirement` instance if the
    /// expressions define a proper lexicographical requirement. For more
    /// details, see [`EquivalenceGroup::normalize_sort_exprs`].
    pub fn normalize_sort_requirements(
        &self,
        sort_reqs: impl IntoIterator<Item = PhysicalSortRequirement>,
    ) -> Option<LexRequirement> {
        LexRequirement::new(self.eq_group.normalize_sort_requirements(sort_reqs))
    }

    /// Returns whether the given complete expression tuple is known to be
    /// grouped within every output partition.
    ///
    /// A tuple is grouped when all rows with the same values for the complete
    /// tuple occur in one contiguous run. Expression order is immaterial. A
    /// lexicographical ordering also satisfies grouping for each of its
    /// prefixes, regardless of sort direction.
    pub fn grouping_satisfy(
        &self,
        given: impl IntoIterator<Item = Arc<dyn PhysicalExpr>>,
    ) -> Result<bool> {
        let normal_grouping = self.normalize_grouping(given);
        if normal_grouping.is_empty() || self.geq_cache.contains(&normal_grouping) {
            return Ok(true);
        }

        let (_, indices) = self.find_longest_permutation(&normal_grouping)?;
        Ok(indices.len() == normal_grouping.len())
    }

    /// Iteratively checks whether the given ordering is satisfied by any of
    /// the existing orderings. See [`Self::ordering_satisfy_requirement`] for
    /// more details and examples.
    pub fn ordering_satisfy(
        &self,
        given: impl IntoIterator<Item = PhysicalSortExpr>,
    ) -> Result<bool> {
        // First, standardize the given ordering:
        let Some(normal_ordering) = self.normalize_sort_exprs(given) else {
            // If the ordering vanishes after normalization, it is satisfied:
            return Ok(true);
        };
        Ok(normal_ordering.len() == self.common_sort_prefix_length(&normal_ordering)?)
    }

    /// Iteratively checks whether the given sort requirement is satisfied by
    /// any of the existing orderings.
    ///
    /// ### Example Scenarios
    ///
    /// In these scenarios, assume that all expressions share the same sort
    /// properties.
    ///
    /// #### Case 1: Sort Requirement `[a, c]`
    ///
    /// **Existing orderings:** `[[a, b, c], [a, d]]`, **constants:** `[]`
    /// 1. The function first checks the leading requirement `a`, which is
    ///    satisfied by `[a, b, c].first()`.
    /// 2. `a` is added as a constant for the next iteration.
    /// 3. Normal orderings become `[[b, c], [d]]`.
    /// 4. The function fails for `c` in the second iteration, as neither
    ///    `[b, c]` nor `[d]` satisfies `c`.
    ///
    /// #### Case 2: Sort Requirement `[a, d]`
    ///
    /// **Existing orderings:** `[[a, b, c], [a, d]]`, **constants:** `[]`
    /// 1. The function first checks the leading requirement `a`, which is
    ///    satisfied by `[a, b, c].first()`.
    /// 2. `a` is added as a constant for the next iteration.
    /// 3. Normal orderings become `[[b, c], [d]]`.
    /// 4. The function returns `true` as `[d]` satisfies `d`.
    pub fn ordering_satisfy_requirement(
        &self,
        given: impl IntoIterator<Item = PhysicalSortRequirement>,
    ) -> Result<bool> {
        // First, standardize the given requirement:
        let Some(normal_reqs) = self.normalize_sort_requirements(given) else {
            // If the requirement vanishes after normalization, it is satisfied:
            return Ok(true);
        };
        // Then, check whether given requirement is satisfied by constraints:
        if self.satisfied_by_constraints(&normal_reqs) {
            return Ok(true);
        }
        let schema = self.schema();
        // Registering satisfied keys as constants mutates the state, so it
        // needs an owned copy -- but only from the second requirement onwards.
        // Single-element requirements (the common case) never pay for the clone.
        let last_idx = normal_reqs.len() - 1;
        let mut owned = None::<Self>;
        for (idx, element) in normal_reqs.into_iter().enumerate() {
            let eq_properties = owned.as_ref().unwrap_or(self);
            // Check whether given requirement is satisfied:
            let ExprProperties {
                sort_properties, ..
            } = eq_properties.get_expr_properties(Arc::clone(&element.expr));
            let satisfy = match sort_properties {
                SortProperties::Ordered(options) => element.options.is_none_or(|opts| {
                    let nullable = element.expr.nullable(schema).unwrap_or(true);
                    options_compatible(&options, &opts, nullable)
                }),
                // Singleton expressions satisfy any requirement.
                SortProperties::Singleton => true,
                SortProperties::Grouped | SortProperties::Unordered => false,
            };
            if !satisfy {
                return Ok(false);
            }
            if idx == last_idx {
                // Nothing left to check, so no need to update the state:
                break;
            }
            // Treat satisfied keys (and the sub-expressions they pin down) as
            // constants in subsequent iterations. See
            // [`Self::add_satisfied_key_constants`] for the rationale.
            owned
                .get_or_insert_with(|| self.clone())
                .add_satisfied_key_constants(element.expr)?;
        }
        Ok(true)
    }

    /// Registers a satisfied sort key as a constant for subsequent iterations
    /// of the ordering satisfaction checks. We can do this because the "next"
    /// key only matters in a lexicographical ordering when the keys to its
    /// left have the same values (i.e. within a single tie group). Note that
    /// these expressions are not properly "constants"; this is just an
    /// implementation strategy confined to the satisfaction checks.
    ///
    /// For example, assume that the requirement is `[a ASC, (b + c) ASC]`,
    /// and existing equivalent orderings are `[a ASC, b ASC]` and `[c ASC]`.
    /// Once we deduce that `[a ASC]` is satisfied, we add column `a` as a
    /// constant to the algorithm state. This enables us to deduce that
    /// `(b + c) ASC` is satisfied, given `a` is constant.
    ///
    /// In addition to the key itself, this also registers any sub-expressions
    /// whose values the key pins down: if an expression is strictly
    /// order-preserving, equal outputs imply equal values of its ordered
    /// children, so within a tie group of the key those children are constant
    /// as well. For example, if data is sorted by `[a, b]`, the requirement
    /// `[CAST(a AS BIGINT) ASC, b ASC]` is satisfied: `a` is constant within
    /// each group of equal `CAST(a AS BIGINT)` values, and hence `b` is
    /// sorted within each such group.
    fn add_satisfied_key_constants(&mut self, expr: Arc<dyn PhysicalExpr>) -> Result<()> {
        let mut stack = vec![expr];
        while let Some(expr) = stack.pop() {
            let properties = self.get_expr_properties(Arc::clone(&expr));
            if properties.strictly_order_preserving {
                for child in expr.children() {
                    let child_properties = self.get_expr_properties(Arc::clone(child));
                    if matches!(
                        child_properties.sort_properties,
                        SortProperties::Ordered(_)
                    ) {
                        stack.push(Arc::clone(child));
                    }
                }
            }
            self.add_constants(std::iter::once(ConstExpr::from(expr)))?;
        }
        Ok(())
    }

    /// Returns the number of consecutive sort expressions (starting from the
    /// left) that are satisfied by the existing ordering.
    fn common_sort_prefix_length(&self, normal_ordering: &LexOrdering) -> Result<usize> {
        let full_length = normal_ordering.len();
        // Check whether the given ordering is satisfied by constraints:
        if self.satisfied_by_constraints_ordering(normal_ordering) {
            // If constraints satisfy all sort expressions, return the full
            // length:
            return Ok(full_length);
        }
        let schema = self.schema();
        // Registering satisfied keys as constants mutates the state, so it
        // needs an owned copy -- but only from the second sort expression
        // onwards. Single-element orderings never pay for the clone.
        let mut owned = None::<Self>;
        for (idx, element) in normal_ordering.into_iter().enumerate() {
            let eq_properties = owned.as_ref().unwrap_or(self);
            // Check whether given ordering is satisfied:
            let ExprProperties {
                sort_properties, ..
            } = eq_properties.get_expr_properties(Arc::clone(&element.expr));
            let satisfy = match sort_properties {
                SortProperties::Ordered(options) => options_compatible(
                    &options,
                    &element.options,
                    element.expr.nullable(schema).unwrap_or(true),
                ),
                // Singleton expressions satisfy any ordering.
                SortProperties::Singleton => true,
                SortProperties::Grouped | SortProperties::Unordered => false,
            };
            if !satisfy {
                // As soon as one sort expression is unsatisfied, return how
                // many we've satisfied so far:
                return Ok(idx);
            }
            if idx + 1 == full_length {
                // Nothing left to check, so no need to update the state:
                break;
            }
            // Treat satisfied keys (and the sub-expressions they pin down) as
            // constants in subsequent iterations. See
            // [`Self::add_satisfied_key_constants`] for the rationale.
            owned
                .get_or_insert_with(|| self.clone())
                .add_satisfied_key_constants(Arc::clone(&element.expr))?;
        }
        // All sort expressions are satisfied, return full length:
        Ok(full_length)
    }

    /// Determines the longest normal prefix of `ordering` satisfied by the
    /// existing ordering. Returns that prefix as a new `LexOrdering`, and a
    /// boolean indicating whether all the sort expressions are satisfied.
    pub fn extract_common_sort_prefix(
        &self,
        ordering: LexOrdering,
    ) -> Result<(Vec<PhysicalSortExpr>, bool)> {
        // First, standardize the given ordering:
        let Some(normal_ordering) = self.normalize_sort_exprs(ordering) else {
            // If the ordering vanishes after normalization, it is satisfied:
            return Ok((vec![], true));
        };
        let prefix_len = self.common_sort_prefix_length(&normal_ordering)?;
        let flag = prefix_len == normal_ordering.len();
        let mut sort_exprs: Vec<_> = normal_ordering.into();
        if !flag {
            sort_exprs.truncate(prefix_len);
        }
        Ok((sort_exprs, flag))
    }

    /// Checks if the sort expressions are satisfied by any of the table
    /// constraints (primary key or unique). Returns true if any constraint
    /// fully satisfies the expressions (i.e. constraint indices form a valid
    /// prefix of an existing ordering that matches the expressions). For
    /// unique constraints, also verifies nullable columns.
    fn satisfied_by_constraints_ordering(
        &self,
        normal_exprs: &[PhysicalSortExpr],
    ) -> bool {
        self.constraints.iter().any(|constraint| match constraint {
            Constraint::PrimaryKey(indices) | Constraint::Unique(indices) => {
                let check_null = matches!(constraint, Constraint::Unique(_));
                let normalized_size = normal_exprs.len();
                indices.len() <= normalized_size
                    && self.oeq_class.iter().any(|ordering| {
                        let length = ordering.len();
                        if indices.len() > length || normalized_size < length {
                            return false;
                        }
                        // Build a map of column positions in the ordering:
                        let mut col_positions = HashMap::with_capacity(length);
                        for (pos, req) in ordering.iter().enumerate() {
                            if let Some(col) = req.expr.downcast_ref::<Column>() {
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
                        normal_exprs.iter().zip(ordering).all(|(given, existing)| {
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
    fn satisfied_by_constraints(&self, normal_reqs: &[PhysicalSortRequirement]) -> bool {
        self.constraints.iter().any(|constraint| match constraint {
            Constraint::PrimaryKey(indices) | Constraint::Unique(indices) => {
                let check_null = matches!(constraint, Constraint::Unique(_));
                let normalized_size = normal_reqs.len();
                indices.len() <= normalized_size
                    && self.oeq_class.iter().any(|ordering| {
                        let length = ordering.len();
                        if indices.len() > length || normalized_size < length {
                            return false;
                        }
                        // Build a map of column positions in the ordering:
                        let mut col_positions = HashMap::with_capacity(length);
                        for (pos, req) in ordering.iter().enumerate() {
                            if let Some(col) = req.expr.downcast_ref::<Column>() {
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
                        normal_reqs.iter().zip(ordering).all(|(given, existing)| {
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
        let Some(normal_given) = self.normalize_sort_requirements(given) else {
            return true;
        };
        let Some(normal_reference) = self.normalize_sort_requirements(reference) else {
            return true;
        };

        (normal_reference.len() <= normal_given.len())
            && normal_reference
                .into_iter()
                .zip(normal_given)
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
    ///       consider widening `CAST` expressions and single-child expressions
    ///       that declare themselves one-to-one order-preserving via
    ///       [`ExprProperties::strictly_order_preserving`].
    fn substitute_oeq_class(
        schema: &SchemaRef,
        mapping: &ProjectionMapping,
        oeq_class: OrderingEquivalenceClass,
    ) -> OrderingEquivalenceClass {
        let new_orderings = oeq_class.into_iter().flat_map(|order| {
            // Modify/expand existing orderings by substituting sort
            // expressions with appropriate targets from the mapping:
            order
                .into_iter()
                .map(|sort_expr| {
                    let original_sort_expr = sort_expr.clone();
                    mapping
                        .iter()
                        .map(|(source, _target)| source)
                        .filter(|source| expr_refers(source, &original_sort_expr.expr))
                        .cloned()
                        .filter_map(|r_expr| {
                            Self::substitute_order_preserving_ordering(
                                r_expr,
                                &original_sort_expr,
                                schema,
                            )
                        })
                        .chain(std::iter::once(sort_expr))
                        .collect::<Vec<_>>()
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

    /// Returns a new `ProjectionMapping` where source expressions are in normal
    /// form. Normalization ensures that source expressions are transformed into
    /// a consistent representation, which is beneficial for algorithms that rely
    /// on exact equalities, as it allows for more precise and reliable comparisons.
    ///
    /// # Parameters
    ///
    /// - `mapping`: A reference to the original `ProjectionMapping` to normalize.
    ///
    /// # Returns
    ///
    /// A new `ProjectionMapping` with source expressions in normal form.
    fn normalize_mapping(&self, mapping: &ProjectionMapping) -> ProjectionMapping {
        mapping
            .iter()
            .map(|(source, target)| {
                let normal_source = self.eq_group.normalize_expr(Arc::clone(source));
                (normal_source, target.clone())
            })
            .collect()
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
    /// - `oeq_class`: The `OrderingEquivalenceClass` containing the orderings
    ///   to project.
    ///
    /// # Returns
    ///
    /// A vector of all valid (but not in normal form) orderings after projection.
    fn projected_orderings(
        &self,
        mapping: &ProjectionMapping,
        mut oeq_class: OrderingEquivalenceClass,
    ) -> Vec<LexOrdering> {
        // Normalize source expressions in the mapping:
        let mapping = self.normalize_mapping(mapping);
        // Get dependency map for existing orderings:
        oeq_class = Self::substitute_oeq_class(&self.schema, &mapping, oeq_class);
        let dependency_map = self.construct_dependency_map(oeq_class, &mapping);
        let orderings = mapping.iter().flat_map(|(source, targets)| {
            referred_dependencies(&dependency_map, source)
                .into_iter()
                .filter_map(|deps| {
                    let ep = get_expr_properties(source, &deps, &self.schema);
                    let sort_properties = ep.map(|prop| prop.sort_properties);
                    if let Ok(SortProperties::Ordered(options)) = sort_properties {
                        Some((options, deps))
                    } else {
                        // Do not consider unordered cases.
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
                        ordering.push(target.clone());
                    }
                }
            }
            prefixes
        });

        // Simplify each ordering by removing redundant sections:
        orderings.chain(projected_orderings).collect()
    }

    /// Projects grouping tuples through `mapping`, dropping a tuple unless all
    /// of its expressions can be represented by the projection.
    fn projected_groupings(
        &self,
        mapping: &ProjectionMapping,
    ) -> Vec<Vec<Arc<dyn PhysicalExpr>>> {
        let mut groupings = self
            .geq_cache
            .iter()
            .filter_map(|grouping| {
                self.project_expressions(grouping.iter(), mapping)
                    .collect::<Option<Vec<_>>>()
            })
            .collect::<Vec<_>>();

        // A projection expression that safely preserves a one-expression
        // grouping establishes the same property for its output column.
        for (source, targets) in mapping.iter() {
            if self.get_expr_properties(Arc::clone(source)).sort_properties
                == SortProperties::Grouped
            {
                groupings
                    .extend(targets.iter().map(|(target, _)| vec![Arc::clone(target)]));
            }
        }
        groupings
    }

    /// Projects constraints according to the given projection mapping.
    ///
    /// This function takes a projection mapping and extracts column indices of
    /// target columns. It then projects the constraints to only include
    /// relationships between columns that exist in the projected output.
    ///
    /// # Parameters
    ///
    /// * `mapping` - A reference to the `ProjectionMapping` that defines the
    ///   projection operation.
    ///
    /// # Returns
    ///
    /// Returns an optional `Constraints` object containing only the constraints
    /// that are valid for the projected columns (if any exists).
    fn projected_constraints(&self, mapping: &ProjectionMapping) -> Option<Constraints> {
        let indices = mapping
            .iter()
            .flat_map(|(_, targets)| {
                targets.iter().filter_map(|(target, _)| {
                    target.downcast_ref::<Column>().map(|c| c.index())
                })
            })
            .collect::<Vec<_>>();
        self.constraints.project(&indices)
    }

    /// Projects these equivalence properties onto `output_schema` according to
    /// `mapping`, returning the properties that still hold for the projected
    /// output.
    ///
    /// `mapping` maps each source expression (evaluated against the current
    /// schema) to the output column(s) it produces. This is more than a
    /// column-index remap: the orderings, the equivalence group and the
    /// constraints are all carried through `mapping`, keeping only what the
    /// projected schema can still express.
    ///
    /// - Orderings: an existing ordering is carried to a target only when the
    ///   mapping expression is order-preserving for it, as determined from the
    ///   expression's [`SortProperties`]. For example, an ordering on `c` is
    ///   preserved through `c + 1` but dropped through `abs(c)`. Orderings
    ///   implied by the mapping are also derived, e.g. an ordering on `a + b`
    ///   yields one on the projected `a_new + b_new`.
    /// - Groupings: a complete grouping tuple is carried only when every
    ///   expression in the tuple can be represented in the output.
    /// - Equivalence group: each class is re-expressed on the output columns.
    /// - Constraints: projected onto the surviving column indices.
    ///
    /// Expressions and orderings not representable in `output_schema` are
    /// dropped.
    pub fn project(&self, mapping: &ProjectionMapping, output_schema: SchemaRef) -> Self {
        let eq_group = self.eq_group.project(mapping);
        // Built here, so it satisfies the precondition by construction; going
        // through the checked entry point would reproject it under
        // `debug_assertions` for nothing.
        self.project_with_eq_group_unchecked(mapping, output_schema, eq_group)
    }

    /// Projects `self`, reusing `cached`'s already-projected equivalence group
    /// when `self`'s group is unchanged from `previous`'s.
    ///
    /// [`EquivalenceGroup::project`] is a pure function of the group and the
    /// mapping, so when the group is unchanged the previous result can be handed
    /// back rather than recomputed. Orderings are still derived here: they are
    /// precisely what changes when a sort is introduced below this node.
    ///
    /// Falls back to a full projection when the groups differ, so there is no
    /// precondition to violate. The caller does still have to pass a `cached`
    /// that came from projecting `previous` through this same `mapping`; that
    /// part cannot be checked here, and in practice it holds because the caller
    /// carries its projection over untouched.
    pub fn project_reusing(
        &self,
        mapping: &ProjectionMapping,
        output_schema: SchemaRef,
        previous: &EquivalenceProperties,
        cached: &EquivalenceProperties,
    ) -> Self {
        let eq_group = if self.eq_group.has_same_classes(&previous.eq_group) {
            #[cfg(test)]
            eq_group_reuse_probe::record_hit();
            cached.eq_group.clone()
        } else {
            self.eq_group.project(mapping)
        };
        self.project_with_eq_group_unchecked(mapping, output_schema, eq_group)
    }

    fn project_with_eq_group_unchecked(
        &self,
        mapping: &ProjectionMapping,
        output_schema: SchemaRef,
        eq_group: EquivalenceGroup,
    ) -> Self {
        let orderings =
            self.projected_orderings(mapping, self.oeq_cache.normal_cls.clone());
        let groupings = self.projected_groupings(mapping);
        let normal_orderings = orderings
            .iter()
            .cloned()
            .map(|o| eq_group.normalize_sort_exprs(o));
        let mut result = Self {
            oeq_cache: OrderingEquivalenceCache::new(normal_orderings),
            oeq_class: OrderingEquivalenceClass::new(orderings),
            geq_class: GroupingEquivalenceClass::default(),
            geq_cache: GroupingEquivalenceClass::default(),
            constraints: self.projected_constraints(mapping).unwrap_or_default(),
            schema: output_schema,
            eq_group,
        };
        result.add_groupings(groupings);
        result
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
    ) -> Result<(Vec<PhysicalSortExpr>, Vec<usize>)> {
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
                        SortProperties::Grouped | SortProperties::Unordered => None,
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
                eq_properties.add_constants(std::iter::once(const_expr))?;
                search_indices.shift_remove(idx);
            }
            // Add new ordered section to the state.
            result.extend(ordered_exprs);
        }
        Ok(result.into_iter().unzip())
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
            .unwrap_or_else(|_| ExprProperties::new_unknown())
    }

    /// Transforms this `EquivalenceProperties` by mapping columns in the
    /// original schema to columns in the new schema by index.
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
                "Schemas have to be aligned to rewrite equivalences:\n Old schema: {}\n New schema: {}",
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
                match value.cast_to(&data_type) {
                    Ok(cast_value) => *value = cast_value,
                    Err(_) => {
                        // This is optimizer metadata. If a stale constant
                        // value cannot be represented after schema rewrite,
                        // drop the constant instead of failing planning.
                        eq_class.constant = None;
                    }
                }
            }
            if eq_class.is_trivial() {
                continue;
            }
            eq_classes.push(eq_class);
        }
        self.eq_group = eq_classes.into();

        // Rewrite orderings according to new schema:
        self.oeq_class = self.oeq_class.with_new_schema(&schema)?;
        self.oeq_cache.normal_cls = self.oeq_cache.normal_cls.with_new_schema(&schema)?;

        // Rewrite grouping expressions according to new schema and rebuild
        // their normalized form against the rewritten equivalence group.
        self.geq_class = self.geq_class.with_new_schema(&schema)?;
        self.update_geq_cache();

        // Update the schema:
        self.schema = schema;

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
/// order: [[b@1 ASC NULLS LAST]], group: [[a@0, b@1]], eq: [{members: [a@0], constant: (heterogeneous)}]
/// ```
impl Display for EquivalenceProperties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let empty_eq_group = self.eq_group.is_empty();
        let empty_oeq_class = self.oeq_class.is_empty();
        let empty_geq_class = self.geq_class.is_empty();
        if empty_oeq_class && empty_geq_class && empty_eq_group {
            return write!(f, "No properties");
        }

        let mut separator = "";
        if !empty_oeq_class {
            write!(f, "order: {}", self.oeq_class)?;
            separator = ", ";
        }
        if !empty_geq_class {
            write!(f, "{separator}group: {}", self.geq_class)?;
            separator = ", ";
        }
        if !empty_eq_group {
            write!(f, "{separator}eq: {}", self.eq_group)?;
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
    } else if node.expr.is::<Literal>() {
        // We have a Literal, which is one of the two possible leaf node types:
        node.data = node.expr.get_properties(&[])?;
    } else if node.expr.is::<Column>() {
        // We have a Column, which is the other possible leaf node type:
        node.data.range =
            Interval::make_unbounded(&node.expr.data_type(eq_properties.schema())?)?;
        // A column is the identity mapping of itself, which is trivially
        // strict:
        node.data.strictly_order_preserving = true;
    }
    // Now, check what we know about orderings:
    let normal_expr = eq_properties
        .eq_group
        .normalize_expr(Arc::clone(&node.expr));
    let oeq_class = &eq_properties.oeq_cache.normal_cls;
    if eq_properties.is_expr_constant(&normal_expr).is_some()
        || oeq_class.is_expr_partial_const(&normal_expr)
    {
        node.data.sort_properties = SortProperties::Singleton;
    } else if let Some(options) = oeq_class.get_options(&normal_expr) {
        node.data.sort_properties = SortProperties::Ordered(options);
    } else if eq_properties
        .geq_cache
        .contains(std::slice::from_ref(&normal_expr))
    {
        node.data.sort_properties = SortProperties::Grouped;
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
        // If exact match is found, return its ordering. This is a base case
        // of the recursion: the expression is treated as an atomic ordered
        // input from here on, so `strictly_order_preserving` states only that
        // it is a one-to-one mapping *of itself* (the identity), which holds
        // for any expression. It makes no claim about the expression being
        // one-to-one in its own inputs (e.g. `floor(x)` as a sort key), and
        // it does not need to: parent expressions are substituted for this
        // sort key, so their strictness only has to be relative to it.
        Ok(ExprProperties {
            sort_properties: SortProperties::Ordered(column_order.options),
            range: Interval::make_unbounded(&expr.data_type(schema)?)?,
            preserves_lex_ordering: false,
            strictly_order_preserving: true,
        })
    } else if expr.downcast_ref::<Column>().is_some() {
        Ok(ExprProperties {
            sort_properties: SortProperties::Unordered,
            range: Interval::make_unbounded(&expr.data_type(schema)?)?,
            preserves_lex_ordering: false,
            // A base case of the recursion: a column is the identity mapping
            // of itself, which is trivially one-to-one.
            strictly_order_preserving: true,
        })
    } else if let Some(literal) = expr.downcast_ref::<Literal>() {
        Ok(ExprProperties {
            sort_properties: SortProperties::Singleton,
            range: literal.value().into(),
            preserves_lex_ordering: true,
            // Vacuously true: a literal has no ordered inputs.
            strictly_order_preserving: true,
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::equivalence::tests::create_test_params;
    use crate::expressions::col;

    use arrow::datatypes::{DataType, Field, Schema};

    /// Renames `a`, `b`, `c` and `d` so the projection is non-trivial while
    /// still carrying every ordering and the `a = c` class across.
    fn renaming_mapping(
        schema: &SchemaRef,
        output_schema: &SchemaRef,
    ) -> Result<ProjectionMapping> {
        [
            ("a", "a1", 0),
            ("b", "b1", 1),
            ("c", "c1", 2),
            ("d", "d1", 3),
        ]
        .into_iter()
        .map(|(source, target, index)| {
            Ok((
                col(source, schema)?,
                vec![(col(target, output_schema)?, index)].into(),
            ))
        })
        .collect::<Result<Vec<_>>>()
        .map(|entries| entries.into_iter().collect())
    }

    fn renamed_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a1", DataType::Int32, true),
            Field::new("b1", DataType::Int32, true),
            Field::new("c1", DataType::Int32, true),
            Field::new("d1", DataType::Int32, true),
        ]))
    }

    #[test]
    fn test_project_reusing_matches_project() -> Result<()> {
        let (schema, eq_properties) = create_test_params()?;
        let output_schema = renamed_schema();
        let mapping = renaming_mapping(&schema, &output_schema)?;

        let baseline = eq_properties.project(&mapping, Arc::clone(&output_schema));

        // Reusing the projection of an identical group must reproduce it exactly.
        let (reused, hits) = eq_group_reuse_probe::count(|| {
            eq_properties.project_reusing(
                &mapping,
                Arc::clone(&output_schema),
                &eq_properties,
                &baseline,
            )
        });
        assert_eq!(
            hits, 1,
            "the reuse path was not taken, so this compares nothing"
        );

        // Guard against a vacuous comparison.
        assert!(
            !baseline.eq_group().is_empty(),
            "the projection dropped the equivalence class, nothing is being tested"
        );
        assert!(
            !baseline.oeq_class().is_empty(),
            "the projection dropped every ordering, nothing is being tested"
        );

        assert!(
            baseline.eq_group().has_same_classes(reused.eq_group()),
            "equivalence group"
        );
        assert_eq!(baseline.oeq_class(), reused.oeq_class(), "orderings");
        assert_eq!(baseline.constraints(), reused.constraints(), "constraints");
        assert_eq!(baseline.schema(), reused.schema(), "schema");

        Ok(())
    }

    #[test]
    fn test_project_reusing_falls_back_when_the_group_moved() -> Result<()> {
        // A `previous` whose group differs must not have its projection carried
        // over. There is no precondition to violate here: the fallback is what
        // keeps a mismatched pair from producing properties that disagree with
        // the projection.
        let (schema, eq_properties) = create_test_params()?;
        let output_schema = renamed_schema();
        let mapping = renaming_mapping(&schema, &output_schema)?;

        let baseline = eq_properties.project(&mapping, Arc::clone(&output_schema));
        let unrelated = EquivalenceProperties::new(Arc::clone(&schema));
        assert!(
            !eq_properties
                .eq_group()
                .has_same_classes(unrelated.eq_group()),
            "the two groups were meant to differ"
        );

        let (recomputed, hits) = eq_group_reuse_probe::count(|| {
            eq_properties.project_reusing(
                &mapping,
                Arc::clone(&output_schema),
                &unrelated,
                &baseline,
            )
        });
        assert_eq!(hits, 0, "a mismatched group was carried over anyway");

        // Falling back must land on exactly what `project` would have produced.
        assert!(
            baseline.eq_group().has_same_classes(recomputed.eq_group()),
            "equivalence group"
        );
        assert_eq!(baseline.oeq_class(), recomputed.oeq_class(), "orderings");

        Ok(())
    }
}
