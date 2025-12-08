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

use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;
use std::vec::IntoIter;

use super::ProjectionMapping;
use crate::expressions::Literal;
use crate::physical_expr::add_offset_to_expr;
use crate::projection::ProjectionTargets;
use crate::{PhysicalExpr, PhysicalExprRef, PhysicalSortExpr, PhysicalSortRequirement};

use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{HashMap, JoinType, Result, ScalarValue};
use datafusion_physical_expr_common::physical_expr::format_physical_expr_list;

use indexmap::{IndexMap, IndexSet};

/// Represents whether a constant expression's value is uniform or varies across
/// partitions. Has two variants:
/// - `Heterogeneous`: The constant expression may have different values for
///   different partitions.
/// - `Uniform(Option<ScalarValue>)`: The constant expression has the same value
///   across all partitions, or is `None` if the value is unknown.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum AcrossPartitions {
    #[default]
    Heterogeneous,
    Uniform(Option<ScalarValue>),
}

impl Display for AcrossPartitions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AcrossPartitions::Heterogeneous => write!(f, "(heterogeneous)"),
            AcrossPartitions::Uniform(value) => {
                if let Some(val) = value {
                    write!(f, "(uniform: {val})")
                } else {
                    write!(f, "(uniform: unknown)")
                }
            }
        }
    }
}

/// A structure representing a expression known to be constant in a physical
/// execution plan.
///
/// The `ConstExpr` struct encapsulates an expression that is constant during
/// the execution of a query. For example if a filter like `A = 5` appears
/// earlier in the plan, `A` would become a constant in subsequent operations.
///
/// # Fields
///
/// - `expr`: Constant expression for a node in the physical plan.
/// - `across_partitions`: A boolean flag indicating whether the constant
///   expression is the same across partitions. If set to `true`, the constant
///   expression has same value for all partitions. If set to `false`, the
///   constant expression may have different values for different partitions.
///
/// # Example
///
/// ```rust
/// # use datafusion_physical_expr::ConstExpr;
/// # use datafusion_physical_expr::expressions::lit;
/// let col = lit(5);
/// // Create a constant expression from a physical expression:
/// let const_expr = ConstExpr::from(col);
/// ```
#[derive(Clone, Debug)]
pub struct ConstExpr {
    /// The expression that is known to be constant (e.g. a `Column`).
    pub expr: Arc<dyn PhysicalExpr>,
    /// Indicates whether the constant have the same value across all partitions.
    pub across_partitions: AcrossPartitions,
}
// TODO: The `ConstExpr` definition above can be in an inconsistent state where
//       `expr` is a literal but `across_partitions` is not `Uniform`. Consider
//       a refactor to ensure that `ConstExpr` is always in a consistent state
//       (either by changing type definition, or by API constraints).

impl ConstExpr {
    /// Create a new constant expression from a physical expression, specifying
    /// whether the constant expression is the same across partitions.
    ///
    /// Note that you can also use `ConstExpr::from` to create a constant
    /// expression from just a physical expression, with the *safe* assumption
    /// of heterogenous values across partitions unless the expression is a
    /// literal.
    pub fn new(expr: Arc<dyn PhysicalExpr>, across_partitions: AcrossPartitions) -> Self {
        let mut result = ConstExpr::from(expr);
        // Override the across partitions specification if the expression is not
        // a literal.
        if result.across_partitions == AcrossPartitions::Heterogeneous {
            result.across_partitions = across_partitions;
        }
        result
    }

    /// Returns a [`Display`]able list of `ConstExpr`.
    pub fn format_list(input: &[ConstExpr]) -> impl Display + '_ {
        struct DisplayableList<'a>(&'a [ConstExpr]);
        impl Display for DisplayableList<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                let mut first = true;
                for const_expr in self.0 {
                    if first {
                        first = false;
                    } else {
                        write!(f, ",")?;
                    }
                    write!(f, "{const_expr}")?;
                }
                Ok(())
            }
        }
        DisplayableList(input)
    }
}

impl PartialEq for ConstExpr {
    fn eq(&self, other: &Self) -> bool {
        self.across_partitions == other.across_partitions && self.expr.eq(&other.expr)
    }
}

impl Display for ConstExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.expr)?;
        write!(f, "{}", self.across_partitions)
    }
}

impl From<Arc<dyn PhysicalExpr>> for ConstExpr {
    fn from(expr: Arc<dyn PhysicalExpr>) -> Self {
        // By default, assume constant expressions are not same across partitions.
        // However, if we have a literal, it will have a single value that is the
        // same across all partitions.
        let across = if let Some(lit) = expr.as_any().downcast_ref::<Literal>() {
            AcrossPartitions::Uniform(Some(lit.value().clone()))
        } else {
            AcrossPartitions::Heterogeneous
        };
        Self {
            expr,
            across_partitions: across,
        }
    }
}

/// An `EquivalenceClass` is a set of [`Arc<dyn PhysicalExpr>`]s that are known
/// to have the same value for all tuples in a relation. These are generated by
/// equality predicates (e.g. `a = b`), typically equi-join conditions and
/// equality conditions in filters.
///
/// Two `EquivalenceClass`es are equal if they contains the same expressions in
/// without any ordering.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EquivalenceClass {
    /// The expressions in this equivalence class. The order doesn't matter for
    /// equivalence purposes.
    pub(crate) exprs: IndexSet<Arc<dyn PhysicalExpr>>,
    /// Indicates whether the expressions in this equivalence class have a
    /// constant value. A `Some` value indicates constant-ness.
    pub(crate) constant: Option<AcrossPartitions>,
}

impl EquivalenceClass {
    // Create a new equivalence class from a pre-existing collection.
    pub fn new(exprs: impl IntoIterator<Item = Arc<dyn PhysicalExpr>>) -> Self {
        let mut class = Self::default();
        for expr in exprs {
            class.push(expr);
        }
        class
    }

    /// Return the "canonical" expression for this class (the first element)
    /// if non-empty.
    pub fn canonical_expr(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.exprs.iter().next()
    }

    /// Insert the expression into this class, meaning it is known to be equal to
    /// all other expressions in this class.
    pub fn push(&mut self, expr: Arc<dyn PhysicalExpr>) {
        if let Some(lit) = expr.as_any().downcast_ref::<Literal>() {
            let expr_across = AcrossPartitions::Uniform(Some(lit.value().clone()));
            if let Some(across) = self.constant.as_mut() {
                // TODO: Return an error if constant values do not agree.
                if *across == AcrossPartitions::Heterogeneous {
                    *across = expr_across;
                }
            } else {
                self.constant = Some(expr_across);
            }
        }
        self.exprs.insert(expr);
    }

    /// Inserts all the expressions from other into this class.
    pub fn extend(&mut self, other: Self) {
        self.exprs.extend(other.exprs);
        match (&self.constant, &other.constant) {
            (Some(across), Some(_)) => {
                // TODO: Return an error if constant values do not agree.
                if across == &AcrossPartitions::Heterogeneous {
                    self.constant = other.constant;
                }
            }
            (None, Some(_)) => self.constant = other.constant,
            (_, None) => {}
        }
    }

    /// Returns whether this equivalence class has any entries in common with
    /// `other`.
    pub fn contains_any(&self, other: &Self) -> bool {
        self.exprs.intersection(&other.exprs).next().is_some()
    }

    /// Returns whether this equivalence class is trivial, meaning that it is
    /// either empty, or contains a single expression that is not a constant.
    /// Such classes are not useful, and can be removed from equivalence groups.
    pub fn is_trivial(&self) -> bool {
        self.exprs.is_empty() || (self.exprs.len() == 1 && self.constant.is_none())
    }

    /// Adds the given offset to all columns in the expressions inside this
    /// class. This is used when schemas are appended, e.g. in joins.
    pub fn try_with_offset(&self, offset: isize) -> Result<Self> {
        let mut cls = Self::default();
        for expr_result in self
            .exprs
            .iter()
            .cloned()
            .map(|e| add_offset_to_expr(e, offset))
        {
            cls.push(expr_result?);
        }
        Ok(cls)
    }
}

impl Deref for EquivalenceClass {
    type Target = IndexSet<Arc<dyn PhysicalExpr>>;

    fn deref(&self) -> &Self::Target {
        &self.exprs
    }
}

impl IntoIterator for EquivalenceClass {
    type Item = Arc<dyn PhysicalExpr>;
    type IntoIter = <IndexSet<Self::Item> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.exprs.into_iter()
    }
}

impl Display for EquivalenceClass {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{{")?;
        write!(f, "members: {}", format_physical_expr_list(&self.exprs))?;
        if let Some(across) = &self.constant {
            write!(f, ", constant: {across}")?;
        }
        write!(f, "}}")
    }
}

impl From<EquivalenceClass> for Vec<Arc<dyn PhysicalExpr>> {
    fn from(cls: EquivalenceClass) -> Self {
        cls.exprs.into_iter().collect()
    }
}

type AugmentedMapping<'a> = IndexMap<
    &'a Arc<dyn PhysicalExpr>,
    (&'a ProjectionTargets, Option<&'a EquivalenceClass>),
>;

/// A collection of distinct `EquivalenceClass`es. This object supports fast
/// lookups of expressions and their equivalence classes.
#[derive(Clone, Debug, Default)]
pub struct EquivalenceGroup {
    /// A mapping from expressions to their equivalence class key.
    map: HashMap<Arc<dyn PhysicalExpr>, usize>,
    /// The equivalence classes in this group.
    classes: Vec<EquivalenceClass>,
}

impl EquivalenceGroup {
    /// Creates an equivalence group from the given equivalence classes.
    pub fn new(classes: impl IntoIterator<Item = EquivalenceClass>) -> Self {
        classes.into_iter().collect::<Vec<_>>().into()
    }

    /// Adds `expr` as a constant expression to this equivalence group.
    pub fn add_constant(&mut self, const_expr: ConstExpr) {
        // If the expression is already in an equivalence class, we should
        // adjust the constant-ness of the class if necessary:
        if let Some(idx) = self.map.get(&const_expr.expr) {
            let cls = &mut self.classes[*idx];
            if let Some(across) = cls.constant.as_mut() {
                // TODO: Return an error if constant values do not agree.
                if *across == AcrossPartitions::Heterogeneous {
                    *across = const_expr.across_partitions;
                }
            } else {
                cls.constant = Some(const_expr.across_partitions);
            }
            return;
        }
        // If the expression is not in any equivalence class, but has the same
        // constant value with some class, add it to that class:
        if let AcrossPartitions::Uniform(_) = &const_expr.across_partitions {
            for (idx, cls) in self.classes.iter_mut().enumerate() {
                if cls
                    .constant
                    .as_ref()
                    .is_some_and(|across| const_expr.across_partitions.eq(across))
                {
                    self.map.insert(Arc::clone(&const_expr.expr), idx);
                    cls.push(const_expr.expr);
                    return;
                }
            }
        }
        // Otherwise, create a new class with the expression as the only member:
        let mut new_class = EquivalenceClass::new(std::iter::once(const_expr.expr));
        if new_class.constant.is_none() {
            new_class.constant = Some(const_expr.across_partitions);
        }
        Self::update_lookup_table(&mut self.map, &new_class, self.classes.len());
        self.classes.push(new_class);
    }

    /// Removes constant expressions that may change across partitions.
    /// This method should be used when merging data from different partitions.
    /// Returns whether any change was made to the equivalence group.
    pub fn clear_per_partition_constants(&mut self) -> bool {
        let (mut idx, mut change) = (0, false);
        while idx < self.classes.len() {
            let cls = &mut self.classes[idx];
            if let Some(AcrossPartitions::Heterogeneous) = cls.constant {
                change = true;
                if cls.len() == 1 {
                    // If this class becomes trivial, remove it entirely:
                    self.remove_class_at_idx(idx);
                    continue;
                } else {
                    cls.constant = None;
                }
            }
            idx += 1;
        }
        change
    }

    /// Adds the equality `left` = `right` to this equivalence group. New
    /// equality conditions often arise after steps like `Filter(a = b)`,
    /// `Alias(a, a as b)` etc. Returns whether the given equality defines
    /// a new equivalence class.
    pub fn add_equal_conditions(
        &mut self,
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
    ) -> bool {
        let first_class = self.map.get(&left).copied();
        let second_class = self.map.get(&right).copied();
        match (first_class, second_class) {
            (Some(mut first_idx), Some(mut second_idx)) => {
                // If the given left and right sides belong to different classes,
                // we should unify/bridge these classes.
                match first_idx.cmp(&second_idx) {
                    // The equality is already known, return and signal this:
                    std::cmp::Ordering::Equal => return false,
                    // Swap indices to ensure `first_idx` is the lesser index.
                    std::cmp::Ordering::Greater => {
                        std::mem::swap(&mut first_idx, &mut second_idx);
                    }
                    _ => {}
                }
                // Remove the class at `second_idx` and merge its values with
                // the class at `first_idx`. The convention above makes sure
                // that `first_idx` is still valid after removing `second_idx`.
                let other_class = self.remove_class_at_idx(second_idx);
                // Update the lookup table for the second class:
                Self::update_lookup_table(&mut self.map, &other_class, first_idx);
                self.classes[first_idx].extend(other_class);
            }
            (Some(group_idx), None) => {
                // Right side is new, extend left side's class:
                self.map.insert(Arc::clone(&right), group_idx);
                self.classes[group_idx].push(right);
            }
            (None, Some(group_idx)) => {
                // Left side is new, extend right side's class:
                self.map.insert(Arc::clone(&left), group_idx);
                self.classes[group_idx].push(left);
            }
            (None, None) => {
                // None of the expressions is among existing classes.
                // Create a new equivalence class and extend the group.
                let class = EquivalenceClass::new([left, right]);
                Self::update_lookup_table(&mut self.map, &class, self.classes.len());
                self.classes.push(class);
                return true;
            }
        }
        false
    }

    /// Removes the equivalence class at the given index from this group.
    fn remove_class_at_idx(&mut self, idx: usize) -> EquivalenceClass {
        // Remove the class at the given index:
        let cls = self.classes.swap_remove(idx);
        // Remove its entries from the lookup table:
        for expr in cls.iter() {
            self.map.remove(expr);
        }
        // Update the lookup table for the moved class:
        if idx < self.classes.len() {
            Self::update_lookup_table(&mut self.map, &self.classes[idx], idx);
        }
        cls
    }

    /// Updates the entry in lookup table for the given equivalence class with
    /// the given index.
    fn update_lookup_table(
        map: &mut HashMap<Arc<dyn PhysicalExpr>, usize>,
        cls: &EquivalenceClass,
        idx: usize,
    ) {
        for expr in cls.iter() {
            map.insert(Arc::clone(expr), idx);
        }
    }

    /// Removes redundant entries from this group. Returns whether any change
    /// was made to the equivalence group.
    fn remove_redundant_entries(&mut self) -> bool {
        // First, remove trivial equivalence classes:
        let mut change = false;
        for idx in (0..self.classes.len()).rev() {
            if self.classes[idx].is_trivial() {
                self.remove_class_at_idx(idx);
                change = true;
            }
        }
        // Then, unify/bridge groups that have common expressions:
        self.bridge_classes() || change
    }

    /// This utility function unifies/bridges classes that have common expressions.
    /// For example, assume that we have [`EquivalenceClass`]es `[a, b]` and `[b, c]`.
    /// Since both classes contain `b`, columns `a`, `b` and `c` are actually all
    /// equal and belong to one class. This utility converts merges such classes.
    /// Returns whether any change was made to the equivalence group.
    fn bridge_classes(&mut self) -> bool {
        let (mut idx, mut change) = (0, false);
        'scan: while idx < self.classes.len() {
            for other_idx in (idx + 1..self.classes.len()).rev() {
                if self.classes[idx].contains_any(&self.classes[other_idx]) {
                    let extension = self.remove_class_at_idx(other_idx);
                    Self::update_lookup_table(&mut self.map, &extension, idx);
                    self.classes[idx].extend(extension);
                    change = true;
                    continue 'scan;
                }
            }
            idx += 1;
        }
        change
    }

    /// Extends this equivalence group with the `other` equivalence group.
    /// Returns whether any equivalence classes were unified/bridged as a
    /// result of the extension process.
    pub fn extend(&mut self, other: Self) -> bool {
        for (idx, cls) in other.classes.iter().enumerate() {
            // Update the lookup table for the new class:
            Self::update_lookup_table(&mut self.map, cls, idx);
        }
        self.classes.extend(other.classes);
        self.bridge_classes()
    }

    /// Normalizes the given physical expression according to this group. The
    /// expression is replaced with the first (canonical) expression in the
    /// equivalence class it matches with (if any).
    pub fn normalize_expr(&self, expr: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        expr.transform(|expr| {
            let cls = self.get_equivalence_class(&expr);
            let Some(canonical) = cls.and_then(|cls| cls.canonical_expr()) else {
                return Ok(Transformed::no(expr));
            };
            Ok(Transformed::yes(Arc::clone(canonical)))
        })
        .data()
        .unwrap()
        // The unwrap above is safe because the closure always returns `Ok`.
    }

    /// Normalizes the given sort expression according to this group. The
    /// underlying physical expression is replaced with the first expression in
    /// the equivalence class it matches with (if any). If the underlying
    /// expression does not belong to any equivalence class in this group,
    /// returns the sort expression as is.
    pub fn normalize_sort_expr(
        &self,
        mut sort_expr: PhysicalSortExpr,
    ) -> PhysicalSortExpr {
        sort_expr.expr = self.normalize_expr(sort_expr.expr);
        sort_expr
    }

    /// Normalizes the given sort expressions (i.e. `sort_exprs`) by:
    /// - Replacing sections that belong to some equivalence class in the
    ///   with the first entry in the matching equivalence class.
    /// - Removing expressions that have a constant value.
    ///
    /// If columns `a` and `b` are known to be equal, `d` is known to be a
    /// constant, and `sort_exprs` is `[b ASC, d DESC, c ASC, a ASC]`, this
    /// function would return `[a ASC, c ASC, a ASC]`.
    pub fn normalize_sort_exprs<'a>(
        &'a self,
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr> + 'a,
    ) -> impl Iterator<Item = PhysicalSortExpr> + 'a {
        sort_exprs
            .into_iter()
            .map(|sort_expr| self.normalize_sort_expr(sort_expr))
            .filter(|sort_expr| self.is_expr_constant(&sort_expr.expr).is_none())
    }

    /// Normalizes the given sort requirement according to this group. The
    /// underlying physical expression is replaced with the first expression in
    /// the equivalence class it matches with (if any). If the underlying
    /// expression does not belong to any equivalence class in this group,
    /// returns the given sort requirement as is.
    pub fn normalize_sort_requirement(
        &self,
        mut sort_requirement: PhysicalSortRequirement,
    ) -> PhysicalSortRequirement {
        sort_requirement.expr = self.normalize_expr(sort_requirement.expr);
        sort_requirement
    }

    /// Normalizes the given sort requirements (i.e. `sort_reqs`) by:
    /// - Replacing sections that belong to some equivalence class in the
    ///   with the first entry in the matching equivalence class.
    /// - Removing expressions that have a constant value.
    ///
    /// If columns `a` and `b` are known to be equal, `d` is known to be a
    /// constant, and `sort_reqs` is `[b ASC, d DESC, c ASC, a ASC]`, this
    /// function would return `[a ASC, c ASC, a ASC]`.
    pub fn normalize_sort_requirements<'a>(
        &'a self,
        sort_reqs: impl IntoIterator<Item = PhysicalSortRequirement> + 'a,
    ) -> impl Iterator<Item = PhysicalSortRequirement> + 'a {
        sort_reqs
            .into_iter()
            .map(|req| self.normalize_sort_requirement(req))
            .filter(|req| self.is_expr_constant(&req.expr).is_none())
    }

    /// Perform an indirect projection of `expr` by consulting the equivalence
    /// classes.
    fn project_expr_indirect(
        aug_mapping: &AugmentedMapping,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        // Literals don't need to be projected
        if expr.as_any().downcast_ref::<Literal>().is_some() {
            return Some(Arc::clone(expr));
        }

        // The given expression is not inside the mapping, so we try to project
        // indirectly using equivalence classes.
        for (targets, eq_class) in aug_mapping.values() {
            // If we match an equivalent expression to a source expression in
            // the mapping, then we can project. For example, if we have the
            // mapping `(a as a1, a + c)` and the equivalence `a == b`,
            // expression `b` projects to `a1`.
            if eq_class.as_ref().is_some_and(|cls| cls.contains(expr)) {
                let (target, _) = targets.first();
                return Some(Arc::clone(target));
            }
        }
        // Project a non-leaf expression by projecting its children.
        let children = expr.children();
        if children.is_empty() {
            // A leaf expression should be inside the mapping.
            return None;
        }
        children
            .into_iter()
            .map(|child| {
                // First, we try to project children with an exact match. If
                // we are unable to do this, we consult equivalence classes.
                if let Some((targets, _)) = aug_mapping.get(child) {
                    // If we match the source, we can project directly:
                    let (target, _) = targets.first();
                    Some(Arc::clone(target))
                } else {
                    Self::project_expr_indirect(aug_mapping, child)
                }
            })
            .collect::<Option<Vec<_>>>()
            .map(|children| Arc::clone(expr).with_new_children(children).unwrap())
    }

    fn augment_projection_mapping<'a>(
        &'a self,
        mapping: &'a ProjectionMapping,
    ) -> AugmentedMapping<'a> {
        mapping
            .iter()
            .map(|(k, v)| {
                let eq_class = self.get_equivalence_class(k);
                (k, (v, eq_class))
            })
            .collect()
    }

    /// Projects `expr` according to the given projection mapping.
    /// If the resulting expression is invalid after projection, returns `None`.
    pub fn project_expr(
        &self,
        mapping: &ProjectionMapping,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        if let Some(targets) = mapping.get(expr) {
            // If we match the source, we can project directly:
            let (target, _) = targets.first();
            Some(Arc::clone(target))
        } else {
            let aug_mapping = self.augment_projection_mapping(mapping);
            Self::project_expr_indirect(&aug_mapping, expr)
        }
    }

    /// Projects `expressions` according to the given projection mapping.
    /// This function is similar to [`Self::project_expr`], but projects multiple
    /// expressions at once more efficiently than calling `project_expr` for each
    /// expression.
    pub fn project_expressions<'a>(
        &'a self,
        mapping: &'a ProjectionMapping,
        expressions: impl IntoIterator<Item = &'a Arc<dyn PhysicalExpr>> + 'a,
    ) -> impl Iterator<Item = Option<Arc<dyn PhysicalExpr>>> + 'a {
        let mut aug_mapping = None;
        expressions.into_iter().map(move |expr| {
            if let Some(targets) = mapping.get(expr) {
                // If we match the source, we can project directly:
                let (target, _) = targets.first();
                Some(Arc::clone(target))
            } else {
                let aug_mapping = aug_mapping
                    .get_or_insert_with(|| self.augment_projection_mapping(mapping));
                Self::project_expr_indirect(aug_mapping, expr)
            }
        })
    }

    /// Projects this equivalence group according to the given projection mapping.
    pub fn project(&self, mapping: &ProjectionMapping) -> Self {
        let projected_classes = self.iter().map(|cls| {
            let new_exprs = self.project_expressions(mapping, cls.iter());
            EquivalenceClass::new(new_exprs.flatten())
        });

        // The key is the source expression, and the value is the equivalence
        // class that contains the corresponding target expression.
        let mut new_constants = vec![];
        let mut new_classes = IndexMap::<_, EquivalenceClass>::new();
        for (source, targets) in mapping.iter() {
            // We need to find equivalent projected expressions. For example,
            // consider a table with columns `[a, b, c]` with `a` == `b`, and
            // projection `[a + c, b + c]`. To conclude that `a + c == b + c`,
            // we first normalize all source expressions in the mapping, then
            // merge all equivalent expressions into the classes.
            let normalized_expr = self.normalize_expr(Arc::clone(source));
            let cls = new_classes.entry(normalized_expr).or_default();
            for (target, _) in targets.iter() {
                cls.push(Arc::clone(target));
            }
            // Save new constants arising from the projection:
            if let Some(across) = self.is_expr_constant(source) {
                for (target, _) in targets.iter() {
                    let const_expr = ConstExpr::new(Arc::clone(target), across.clone());
                    new_constants.push(const_expr);
                }
            }
        }

        // Union projected classes with new classes to make up the result:
        let classes = projected_classes
            .chain(new_classes.into_values())
            .filter(|cls| !cls.is_trivial());
        let mut result = Self::new(classes);
        // Add new constants arising from the projection to the equivalence group:
        for constant in new_constants {
            result.add_constant(constant);
        }
        result
    }

    /// Returns a `Some` value if the expression is constant according to
    /// equivalence group, and `None` otherwise. The `Some` variant contains
    /// an `AcrossPartitions` value indicating whether the expression is
    /// constant across partitions, and its actual value (if available).
    pub fn is_expr_constant(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Option<AcrossPartitions> {
        if let Some(lit) = expr.as_any().downcast_ref::<Literal>() {
            return Some(AcrossPartitions::Uniform(Some(lit.value().clone())));
        }
        if let Some(cls) = self.get_equivalence_class(expr)
            && cls.constant.is_some()
        {
            return cls.constant.clone();
        }
        // TODO: This function should be able to return values of non-literal
        //       complex constants as well; e.g. it should return `8` for the
        //       expression `3 + 5`, not an unknown `heterogenous` value.
        let children = expr.children();
        if children.is_empty() {
            return None;
        }
        for child in children {
            self.is_expr_constant(child)?;
        }
        Some(AcrossPartitions::Heterogeneous)
    }

    /// Returns the equivalence class containing `expr`. If no equivalence class
    /// contains `expr`, returns `None`.
    pub fn get_equivalence_class(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Option<&EquivalenceClass> {
        self.map.get(expr).map(|idx| &self.classes[*idx])
    }

    /// Combine equivalence groups of the given join children.
    pub fn join(
        &self,
        right_equivalences: &Self,
        join_type: &JoinType,
        left_size: usize,
        on: &[(PhysicalExprRef, PhysicalExprRef)],
    ) -> Result<Self> {
        let group = match join_type {
            JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right => {
                let mut result = Self::new(
                    self.iter().cloned().chain(
                        right_equivalences
                            .iter()
                            .map(|cls| cls.try_with_offset(left_size as _))
                            .collect::<Result<Vec<_>>>()?,
                    ),
                );
                // In we have an inner join, expressions in the "on" condition
                // are equal in the resulting table.
                if join_type == &JoinType::Inner {
                    for (lhs, rhs) in on.iter() {
                        let new_lhs = Arc::clone(lhs);
                        // Rewrite rhs to point to the right side of the join:
                        let new_rhs =
                            add_offset_to_expr(Arc::clone(rhs), left_size as _)?;
                        result.add_equal_conditions(new_lhs, new_rhs);
                    }
                }
                result
            }
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => self.clone(),
            JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
                right_equivalences.clone()
            }
        };
        Ok(group)
    }

    /// Checks if two expressions are equal directly or through equivalence
    /// classes. For complex expressions (e.g. `a + b`), checks that the
    /// expression trees are structurally identical and their leaf nodes are
    /// equivalent either directly or through equivalence classes.
    pub fn exprs_equal(
        &self,
        left: &Arc<dyn PhysicalExpr>,
        right: &Arc<dyn PhysicalExpr>,
    ) -> bool {
        // Direct equality check
        if left.eq(right) {
            return true;
        }

        // Check if expressions are equivalent through equivalence classes
        // We need to check both directions since expressions might be in different classes
        if let Some(left_class) = self.get_equivalence_class(left)
            && left_class.contains(right)
        {
            return true;
        }
        if let Some(right_class) = self.get_equivalence_class(right)
            && right_class.contains(left)
        {
            return true;
        }

        // For non-leaf nodes, check structural equality
        let left_children = left.children();
        let right_children = right.children();

        // If either expression is a leaf node and we haven't found equality yet,
        // they must be different
        if left_children.is_empty() || right_children.is_empty() {
            return false;
        }

        // Type equality check through reflection
        if left.as_any().type_id() != right.as_any().type_id() {
            return false;
        }

        // Check if the number of children is the same
        if left_children.len() != right_children.len() {
            return false;
        }

        // Check if all children are equal
        left_children
            .into_iter()
            .zip(right_children)
            .all(|(left_child, right_child)| self.exprs_equal(left_child, right_child))
    }
}

impl Deref for EquivalenceGroup {
    type Target = [EquivalenceClass];

    fn deref(&self) -> &Self::Target {
        &self.classes
    }
}

impl IntoIterator for EquivalenceGroup {
    type Item = EquivalenceClass;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.classes.into_iter()
    }
}

impl Display for EquivalenceGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[")?;
        let mut iter = self.iter();
        if let Some(cls) = iter.next() {
            write!(f, "{cls}")?;
        }
        for cls in iter {
            write!(f, ", {cls}")?;
        }
        write!(f, "]")
    }
}

impl From<Vec<EquivalenceClass>> for EquivalenceGroup {
    fn from(classes: Vec<EquivalenceClass>) -> Self {
        let mut result = Self {
            map: classes
                .iter()
                .enumerate()
                .flat_map(|(idx, cls)| {
                    cls.iter().map(move |expr| (Arc::clone(expr), idx))
                })
                .collect(),
            classes,
        };
        result.remove_redundant_entries();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::equivalence::tests::create_test_params;
    use crate::expressions::{BinaryExpr, Column, Literal, binary, col, lit};
    use arrow::datatypes::{DataType, Field, Schema};

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::Operator;

    #[test]
    fn test_bridge_groups() -> Result<()> {
        // First entry in the tuple is argument, second entry is the bridged result
        let test_cases = vec![
            // ------- TEST CASE 1 -----------//
            (
                vec![vec![1, 2, 3], vec![2, 4, 5], vec![11, 12, 9], vec![7, 6, 5]],
                // Expected is compared with set equality. Order of the specific results may change.
                vec![vec![1, 2, 3, 4, 5, 6, 7], vec![9, 11, 12]],
            ),
            // ------- TEST CASE 2 -----------//
            (
                vec![vec![1, 2, 3], vec![3, 4, 5], vec![9, 8, 7], vec![7, 6, 5]],
                // Expected
                vec![vec![1, 2, 3, 4, 5, 6, 7, 8, 9]],
            ),
        ];
        for (entries, expected) in test_cases {
            let entries = entries
                .into_iter()
                .map(|entry| {
                    entry.into_iter().map(|idx| {
                        let c = Column::new(format!("col_{idx}").as_str(), idx);
                        Arc::new(c) as _
                    })
                })
                .map(EquivalenceClass::new)
                .collect::<Vec<_>>();
            let expected = expected
                .into_iter()
                .map(|entry| {
                    entry.into_iter().map(|idx| {
                        let c = Column::new(format!("col_{idx}").as_str(), idx);
                        Arc::new(c) as _
                    })
                })
                .map(EquivalenceClass::new)
                .collect::<Vec<_>>();
            let eq_groups: EquivalenceGroup = entries.clone().into();
            let eq_groups = eq_groups.classes;
            let err_msg = format!(
                "error in test entries: {entries:?}, expected: {expected:?}, actual:{eq_groups:?}"
            );
            assert_eq!(eq_groups.len(), expected.len(), "{err_msg}");
            for idx in 0..eq_groups.len() {
                assert_eq!(&eq_groups[idx], &expected[idx], "{err_msg}");
            }
        }
        Ok(())
    }

    #[test]
    fn test_remove_redundant_entries_eq_group() -> Result<()> {
        let c = |idx| Arc::new(Column::new(format!("col_{idx}").as_str(), idx)) as _;
        let entries = [
            EquivalenceClass::new([c(1), c(1), lit(20)]),
            EquivalenceClass::new([lit(30), lit(30)]),
            EquivalenceClass::new([c(2), c(3), c(4)]),
        ];
        // Given equivalences classes are not in succinct form.
        // Expected form is the most plain representation that is functionally same.
        let expected = [
            EquivalenceClass::new([c(1), lit(20)]),
            EquivalenceClass::new([lit(30)]),
            EquivalenceClass::new([c(2), c(3), c(4)]),
        ];
        let eq_groups = EquivalenceGroup::new(entries);
        assert_eq!(eq_groups.classes, expected);
        Ok(())
    }

    #[test]
    fn test_schema_normalize_expr_with_equivalence() -> Result<()> {
        let col_a = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let col_b = Arc::new(Column::new("b", 1)) as _;
        let col_c = Arc::new(Column::new("c", 2)) as _;
        // Assume that column a and c are aliases.
        let (_, eq_properties) = create_test_params()?;
        // Test cases for equivalence normalization. First entry in the tuple is
        // the argument, second entry is expected result after normalization.
        let expressions = vec![
            // Normalized version of the column a and c should go to a
            // (by convention all the expressions inside equivalence class are mapped to the first entry
            // in this case a is the first entry in the equivalence class.)
            (Arc::clone(&col_a), Arc::clone(&col_a)),
            (col_c, col_a),
            // Cannot normalize column b
            (Arc::clone(&col_b), Arc::clone(&col_b)),
        ];
        let eq_group = eq_properties.eq_group();
        for (expr, expected_eq) in expressions {
            assert!(expected_eq.eq(&eq_group.normalize_expr(expr)));
        }

        Ok(())
    }

    #[test]
    fn test_contains_any() {
        let lit_true = Arc::new(Literal::new(ScalarValue::from(true))) as _;
        let lit_false = Arc::new(Literal::new(ScalarValue::from(false))) as _;
        let col_a_expr = Arc::new(Column::new("a", 0)) as _;
        let col_b_expr = Arc::new(Column::new("b", 1)) as _;
        let col_c_expr = Arc::new(Column::new("c", 2)) as _;

        let cls1 = EquivalenceClass::new([Arc::clone(&lit_true), col_a_expr]);
        let cls2 = EquivalenceClass::new([lit_true, col_b_expr]);
        let cls3 = EquivalenceClass::new([col_c_expr, lit_false]);

        // lit_true is common
        assert!(cls1.contains_any(&cls2));
        // there is no common entry
        assert!(!cls1.contains_any(&cls3));
        assert!(!cls2.contains_any(&cls3));
    }

    #[test]
    fn test_exprs_equal() -> Result<()> {
        struct TestCase {
            left: Arc<dyn PhysicalExpr>,
            right: Arc<dyn PhysicalExpr>,
            expected: bool,
            description: &'static str,
        }

        // Create test columns
        let col_a = Arc::new(Column::new("a", 0)) as _;
        let col_b = Arc::new(Column::new("b", 1)) as _;
        let col_x = Arc::new(Column::new("x", 2)) as _;
        let col_y = Arc::new(Column::new("y", 3)) as _;

        // Create test literals
        let lit_1 = Arc::new(Literal::new(ScalarValue::from(1))) as _;
        let lit_2 = Arc::new(Literal::new(ScalarValue::from(2))) as _;

        // Create equivalence group with classes (a = x) and (b = y)
        let eq_group = EquivalenceGroup::new([
            EquivalenceClass::new([Arc::clone(&col_a), Arc::clone(&col_x)]),
            EquivalenceClass::new([Arc::clone(&col_b), Arc::clone(&col_y)]),
        ]);

        let test_cases = vec![
            // Basic equality tests
            TestCase {
                left: Arc::clone(&col_a),
                right: Arc::clone(&col_a),
                expected: true,
                description: "Same column should be equal",
            },
            // Equivalence class tests
            TestCase {
                left: Arc::clone(&col_a),
                right: Arc::clone(&col_x),
                expected: true,
                description: "Columns in same equivalence class should be equal",
            },
            TestCase {
                left: Arc::clone(&col_b),
                right: Arc::clone(&col_y),
                expected: true,
                description: "Columns in same equivalence class should be equal",
            },
            TestCase {
                left: Arc::clone(&col_a),
                right: Arc::clone(&col_b),
                expected: false,
                description: "Columns in different equivalence classes should not be equal",
            },
            // Literal tests
            TestCase {
                left: Arc::clone(&lit_1),
                right: Arc::clone(&lit_1),
                expected: true,
                description: "Same literal should be equal",
            },
            TestCase {
                left: Arc::clone(&lit_1),
                right: Arc::clone(&lit_2),
                expected: false,
                description: "Different literals should not be equal",
            },
            // Complex expression tests
            TestCase {
                left: Arc::new(BinaryExpr::new(
                    Arc::clone(&col_a),
                    Operator::Plus,
                    Arc::clone(&col_b),
                )) as _,
                right: Arc::new(BinaryExpr::new(
                    Arc::clone(&col_x),
                    Operator::Plus,
                    Arc::clone(&col_y),
                )) as _,
                expected: true,
                description: "Binary expressions with equivalent operands should be equal",
            },
            TestCase {
                left: Arc::new(BinaryExpr::new(
                    Arc::clone(&col_a),
                    Operator::Plus,
                    Arc::clone(&col_b),
                )) as _,
                right: Arc::new(BinaryExpr::new(
                    Arc::clone(&col_x),
                    Operator::Plus,
                    Arc::clone(&col_a),
                )) as _,
                expected: false,
                description: "Binary expressions with non-equivalent operands should not be equal",
            },
            TestCase {
                left: Arc::new(BinaryExpr::new(
                    Arc::clone(&col_a),
                    Operator::Plus,
                    Arc::clone(&lit_1),
                )) as _,
                right: Arc::new(BinaryExpr::new(
                    Arc::clone(&col_x),
                    Operator::Plus,
                    Arc::clone(&lit_1),
                )) as _,
                expected: true,
                description: "Binary expressions with equivalent column and same literal should be equal",
            },
            TestCase {
                left: Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::clone(&col_a),
                        Operator::Plus,
                        Arc::clone(&col_b),
                    )),
                    Operator::Multiply,
                    Arc::clone(&lit_1),
                )) as _,
                right: Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::clone(&col_x),
                        Operator::Plus,
                        Arc::clone(&col_y),
                    )),
                    Operator::Multiply,
                    Arc::clone(&lit_1),
                )) as _,
                expected: true,
                description: "Nested binary expressions with equivalent operands should be equal",
            },
        ];

        for TestCase {
            left,
            right,
            expected,
            description,
        } in test_cases
        {
            let actual = eq_group.exprs_equal(&left, &right);
            assert_eq!(
                actual, expected,
                "{description}: Failed comparing {left:?} and {right:?}, expected {expected}, got {actual}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_project_classes() -> Result<()> {
        // - columns: [a, b, c].
        // - "a" and "b" in the same equivalence class.
        // - then after a+c, b+c projection col(0) and col(1) must be
        // in the same class too.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));
        let mut group = EquivalenceGroup::default();
        group.add_equal_conditions(col("a", &schema)?, col("b", &schema)?);

        let projected_schema = Arc::new(Schema::new(vec![
            Field::new("a+c", DataType::Int32, false),
            Field::new("b+c", DataType::Int32, false),
        ]));

        let mapping = [
            (
                binary(
                    col("a", &schema)?,
                    Operator::Plus,
                    col("c", &schema)?,
                    &schema,
                )?,
                vec![(col("a+c", &projected_schema)?, 0)].into(),
            ),
            (
                binary(
                    col("b", &schema)?,
                    Operator::Plus,
                    col("c", &schema)?,
                    &schema,
                )?,
                vec![(col("b+c", &projected_schema)?, 1)].into(),
            ),
        ]
        .into_iter()
        .collect::<ProjectionMapping>();

        let projected = group.project(&mapping);

        assert!(!projected.is_empty());
        let first_normalized = projected.normalize_expr(col("a+c", &projected_schema)?);
        let second_normalized = projected.normalize_expr(col("b+c", &projected_schema)?);

        assert!(first_normalized.eq(&second_normalized));

        Ok(())
    }
}
