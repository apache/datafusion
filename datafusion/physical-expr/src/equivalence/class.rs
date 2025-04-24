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
use std::sync::Arc;
use std::vec::IntoIter;

use super::{add_offset_to_expr, ProjectionMapping};
use crate::expressions::{Column, Literal};
use crate::{PhysicalExpr, PhysicalExprRef, PhysicalSortExpr, PhysicalSortRequirement};

use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{JoinType, ScalarValue};
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
                    write!(f, "(uniform: {})", val)
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
    /// The expression that is known to be constant (e.g. a `Column`)
    pub expr: Arc<dyn PhysicalExpr>,
    /// Does the constant have the same value across all partitions? See
    /// struct docs for more details
    pub across_partitions: AcrossPartitions,
}

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
        // TODO: Consider dropping across partitions from the constructor.
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
                    write!(f, "{}", const_expr)?;
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

    /// Returns true if this equivalence class contains the given expression.
    pub fn contains(&self, expr: &Arc<dyn PhysicalExpr>) -> bool {
        self.exprs.contains(expr)
    }

    /// Returns true if this equivalence class has any entries in common with
    /// `other`.
    pub fn contains_any(&self, other: &Self) -> bool {
        self.exprs.intersection(&other.exprs).next().is_some()
    }

    /// Returns the number of items in this equivalence class.
    pub fn len(&self) -> usize {
        self.exprs.len()
    }

    /// Returns whether this equivalence class is empty.
    pub fn is_empty(&self) -> bool {
        self.exprs.is_empty()
    }

    /// Returns whether this equivalence class is trivial, meaning that it is
    /// either empty, or contains a single expression that is not a constant.
    /// Such classes are not useful, and can be removed from equivalence groups.
    pub fn is_trivial(&self) -> bool {
        self.exprs.is_empty() || (self.exprs.len() == 1 && self.constant.is_none())
    }

    /// Iterate over all elements in this class (in some arbitrary order).
    pub fn iter(&self) -> impl Iterator<Item = &Arc<dyn PhysicalExpr>> {
        self.exprs.iter()
    }

    /// Return a new equivalence class that have the specified offset added to
    /// each expression (used when schemas are appended such as in joins)
    pub fn with_offset(&self, offset: usize) -> Self {
        let new_exprs = self
            .exprs
            .iter()
            .cloned()
            .map(|e| add_offset_to_expr(e, offset));
        Self::new(new_exprs)
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
            write!(f, ", constant: {}", across)?;
        }
        write!(f, "}}")
    }
}

impl From<EquivalenceClass> for Vec<Arc<dyn PhysicalExpr>> {
    fn from(cls: EquivalenceClass) -> Self {
        cls.exprs.into_iter().collect()
    }
}

/// A collection of distinct `EquivalenceClass`es
#[derive(Clone, Debug, Default)]
pub struct EquivalenceGroup {
    classes: Vec<EquivalenceClass>,
}

impl EquivalenceGroup {
    /// Creates an equivalence group from the given equivalence classes.
    pub fn new(classes: impl IntoIterator<Item = EquivalenceClass>) -> Self {
        let mut result = Self {
            classes: classes.into_iter().collect(),
        };
        result.remove_redundant_entries();
        result
    }

    /// Returns how many equivalence classes there are in this group.
    pub fn len(&self) -> usize {
        self.classes.len()
    }

    /// Checks whether this equivalence group is empty.
    pub fn is_empty(&self) -> bool {
        self.classes.is_empty()
    }

    /// Returns an iterator over the equivalence classes in this group.
    pub fn iter(&self) -> impl Iterator<Item = &EquivalenceClass> {
        self.classes.iter()
    }

    /// Returns an iterator over the equivalence classes in this group.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut EquivalenceClass> {
        self.classes.iter_mut()
    }

    /// Adds `expr` as a constant expression to this equivalence group.
    pub fn add_constant(&mut self, const_expr: ConstExpr) {
        for cls in self.classes.iter_mut() {
            if cls.contains(&const_expr.expr) {
                // If the expression is already in an equivalence class, we
                // should adjust the constant-ness of the class if necessary:
                if let Some(across) = cls.constant.as_mut() {
                    // TODO: Return an error if constant values do not agree.
                    if *across == AcrossPartitions::Heterogeneous {
                        *across = const_expr.across_partitions;
                    }
                } else {
                    cls.constant = Some(const_expr.across_partitions);
                }
                return;
            } else if let Some(across @ AcrossPartitions::Uniform(_)) = &cls.constant {
                // If the expression is not in some equivalence class, but has
                // the same constant value with it, add it to that class:
                if const_expr.across_partitions.eq(across) {
                    cls.push(const_expr.expr);
                    return;
                }
            }
        }
        // If the expression is not in *any* equivalence class, create a new
        // one with the expression as the only member:
        let mut new_class = EquivalenceClass::new(std::iter::once(const_expr.expr));
        if new_class.constant.is_none() {
            new_class.constant = Some(const_expr.across_partitions);
        }
        self.classes.push(new_class);
    }

    /// Removes constant expressions that may change across partitions.
    /// This method should be used when merging data from different partitions.
    pub fn clear_per_partition_constants(&mut self) {
        let mut idx = 0;
        while idx < self.classes.len() {
            let cls = &mut self.classes[idx];
            if let Some(AcrossPartitions::Heterogeneous) = cls.constant {
                if cls.len() == 1 {
                    // If this class becomes trivial, remove it entirely:
                    self.classes.swap_remove(idx);
                    continue;
                } else {
                    cls.constant = None;
                }
            }
            idx += 1;
        }
    }

    /// Adds the equality `left` = `right` to this equivalence group.
    /// New equality conditions often arise after steps like `Filter(a = b)`,
    /// `Alias(a, a as b)` etc.
    pub fn add_equal_conditions(
        &mut self,
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
    ) {
        let mut idx = 0;
        let size = self.classes.len();
        let mut first_class = None;
        let mut second_class = None;
        while (idx < size) && (first_class.is_none() || second_class.is_none()) {
            let cls = &self.classes[idx];
            if first_class.is_none() && cls.contains(&left) {
                first_class = Some(idx);
            }
            if second_class.is_none() && cls.contains(&right) {
                second_class = Some(idx);
            }
            idx += 1;
        }
        match (first_class, second_class) {
            (Some(mut first_idx), Some(mut second_idx)) => {
                // If the given left and right sides belong to different classes,
                // we should unify/bridge these classes.
                if first_idx != second_idx {
                    // By convention, make sure `second_idx` is larger than `first_idx`.
                    if first_idx > second_idx {
                        (first_idx, second_idx) = (second_idx, first_idx);
                    }
                    // Remove the class at `second_idx` and merge its values with
                    // the class at `first_idx`. The convention above makes sure
                    // that `first_idx` is still valid after removing `second_idx`.
                    let other_class = self.classes.swap_remove(second_idx);
                    self.classes[first_idx].extend(other_class);
                }
            }
            (Some(group_idx), None) => {
                // Right side is new, extend left side's class:
                self.classes[group_idx].push(right);
            }
            (None, Some(group_idx)) => {
                // Left side is new, extend right side's class:
                self.classes[group_idx].push(left);
            }
            (None, None) => {
                // None of the expressions is among existing classes.
                // Create a new equivalence class and extend the group.
                self.classes.push(EquivalenceClass::new([left, right]));
            }
        }
    }

    /// Removes redundant entries from this group.
    fn remove_redundant_entries(&mut self) {
        // Remove duplicate entries from each equivalence class:
        self.classes.retain(|cls| !cls.is_trivial());
        // Unify/bridge groups that have common expressions:
        self.bridge_classes()
    }

    /// This utility function unifies/bridges classes that have common expressions.
    /// For example, assume that we have [`EquivalenceClass`]es `[a, b]` and `[b, c]`.
    /// Since both classes contain `b`, columns `a`, `b` and `c` are actually all
    /// equal and belong to one class. This utility converts merges such classes.
    fn bridge_classes(&mut self) {
        let mut idx = 0;
        while idx < self.classes.len() {
            let mut next_idx = idx + 1;
            let start_size = self.classes[idx].len();
            while next_idx < self.classes.len() {
                if self.classes[idx].contains_any(&self.classes[next_idx]) {
                    let extension = self.classes.swap_remove(next_idx);
                    self.classes[idx].extend(extension);
                } else {
                    next_idx += 1;
                }
            }
            if self.classes[idx].len() > start_size {
                continue;
            }
            idx += 1;
        }
    }

    /// Extends this equivalence group with the `other` equivalence group.
    pub fn extend(&mut self, other: Self) {
        self.classes.extend(other.classes);
        self.remove_redundant_entries();
    }

    /// Normalizes the given physical expression according to this group.
    /// The expression is replaced with the first expression in the equivalence
    /// class it matches with (if any).
    pub fn normalize_expr(&self, expr: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        expr.transform(|expr| {
            for cls in self.iter() {
                // If the equivalence class is non-empty, and it contains this
                // expression, use its canonical version:
                if let Some(canonical) = cls.canonical_expr() {
                    if cls.contains(&expr) {
                        return Ok(Transformed::yes(Arc::clone(canonical)));
                    }
                }
            }
            Ok(Transformed::no(expr))
        })
        .data()
        .unwrap()
        // The unwrap above is safe because the closure always returns `Ok`.
    }

    /// Normalizes the given sort expression according to this group.
    /// The underlying physical expression is replaced with the first expression
    /// in the equivalence class it matches with (if any). If the underlying
    /// expression does not belong to any equivalence class in this group, returns
    /// the sort expression as is.
    pub fn normalize_sort_expr(
        &self,
        mut sort_expr: PhysicalSortExpr,
    ) -> PhysicalSortExpr {
        sort_expr.expr = self.normalize_expr(sort_expr.expr);
        sort_expr
    }

    /// Normalizes the given sort requirement according to this group.
    /// The underlying physical expression is replaced with the first expression
    /// in the equivalence class it matches with (if any). If the underlying
    /// expression does not belong to any equivalence class in this group, returns
    /// the given sort requirement as is.
    pub fn normalize_sort_requirement(
        &self,
        mut sort_requirement: PhysicalSortRequirement,
    ) -> PhysicalSortRequirement {
        sort_requirement.expr = self.normalize_expr(sort_requirement.expr);
        sort_requirement
    }

    /// Projects `expr` according to the given projection mapping.
    /// If the resulting expression is invalid after projection, returns `None`.
    ///
    /// TODO: Write a multiple `expr` version to avoid searching for equivalence
    ///       classes for every source expression in `mapping` multiple times.
    pub fn project_expr(
        &self,
        mapping: &ProjectionMapping,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        // First, we try to project expressions with an exact match. If we are
        // unable to do this, we consult equivalence classes.
        if let Some(target) = mapping.target_expr(expr) {
            // If we match the source, we can project directly:
            return Some(target);
        } else {
            // If the given expression is not inside the mapping, try to project
            // expressions considering the equivalence classes.
            for (source, target) in mapping.iter() {
                // If we match an equivalent expression to `source`, then we can
                // project. For example, if we have the mapping `(a as a1, a + c)`
                // and the equivalence class `(a, b)`, expression `b` projects to `a1`.
                let eq_class = self.get_equivalence_class(source);
                if eq_class.is_some_and(|group| group.contains(expr)) {
                    return Some(Arc::clone(target));
                }
            }
        }
        // Project a non-leaf expression by projecting its children.
        let children = expr.children();
        if children.is_empty() {
            // Leaf expression should be inside mapping.
            return None;
        }
        children
            .into_iter()
            .map(|child| self.project_expr(mapping, child))
            .collect::<Option<Vec<_>>>()
            .map(|children| Arc::clone(expr).with_new_children(children).unwrap())
    }

    /// Projects this equivalence group according to the given projection mapping.
    pub fn project(&self, mapping: &ProjectionMapping) -> Self {
        let projected_classes = self.iter().map(|cls| {
            let new_exprs = cls
                .iter()
                .filter_map(|expr| self.project_expr(mapping, expr));
            EquivalenceClass::new(new_exprs)
        });

        // The key is the source expression, and the value is the equivalence
        // class that contains the corresponding target expression.
        let mut new_constants = vec![];
        let mut new_classes = IndexMap::<_, EquivalenceClass>::new();
        for (source, target) in mapping.iter() {
            // We need to find equivalent projected expressions. For example,
            // consider a table with columns `[a, b, c]` with `a` == `b`, and
            // projection `[a + c, b + c]`. To conclude that `a + c == b + c`,
            // we first normalize all source expressions in the mapping, then
            // merge all equivalent expressions into the classes.
            let normalized_expr = self.normalize_expr(Arc::clone(source));
            new_classes
                .entry(normalized_expr)
                .or_default()
                .push(Arc::clone(target));
            // Save new constants arising from the projection:
            if let Some(across) = self.is_expr_constant(source) {
                let const_expr = ConstExpr::new(Arc::clone(target), across);
                new_constants.push(const_expr);
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
        for cls in self.iter() {
            if cls.constant.is_some() && cls.contains(expr) {
                return cls.constant.clone();
            }
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
        self.iter().find(|cls| cls.contains(expr))
    }

    /// Combine equivalence groups of the given join children.
    pub fn join(
        &self,
        right_equivalences: &Self,
        join_type: &JoinType,
        left_size: usize,
        on: &[(PhysicalExprRef, PhysicalExprRef)],
    ) -> Self {
        match join_type {
            JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right => {
                let mut result = Self::new(
                    self.iter().cloned().chain(
                        right_equivalences
                            .iter()
                            .map(|cls| cls.with_offset(left_size)),
                    ),
                );
                // In we have an inner join, expressions in the "on" condition
                // are equal in the resulting table.
                if join_type == &JoinType::Inner {
                    for (lhs, rhs) in on.iter() {
                        let new_lhs = Arc::clone(lhs);
                        // Rewrite rhs to point to the right side of the join:
                        let new_rhs = Arc::clone(rhs)
                            .transform(|expr| {
                                if let Some(column) =
                                    expr.as_any().downcast_ref::<Column>()
                                {
                                    let new_column = Arc::new(Column::new(
                                        column.name(),
                                        column.index() + left_size,
                                    ));
                                    return Ok(Transformed::yes(new_column as _));
                                }

                                Ok(Transformed::no(expr))
                            })
                            .data()
                            .unwrap();
                        result.add_equal_conditions(new_lhs, new_rhs);
                    }
                }
                result
            }
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => self.clone(),
            JoinType::RightSemi | JoinType::RightAnti => right_equivalences.clone(),
        }
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
        if let Some(left_class) = self.get_equivalence_class(left) {
            if left_class.contains(right) {
                return true;
            }
        }
        if let Some(right_class) = self.get_equivalence_class(right) {
            if right_class.contains(left) {
                return true;
            }
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
            write!(f, "{}", cls)?;
        }
        for cls in iter {
            write!(f, ", {}", cls)?;
        }
        write!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::equivalence::tests::create_test_params;
    use crate::expressions::{binary, col, lit, BinaryExpr, Literal};
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
                        let c = Column::new(format!("col_{}", idx).as_str(), idx);
                        Arc::new(c) as _
                    })
                })
                .map(EquivalenceClass::new)
                .collect::<Vec<_>>();
            let expected = expected
                .into_iter()
                .map(|entry| {
                    entry.into_iter().map(|idx| {
                        let c = Column::new(format!("col_{}", idx).as_str(), idx);
                        Arc::new(c) as _
                    })
                })
                .map(EquivalenceClass::new)
                .collect::<Vec<_>>();
            let mut eq_groups = EquivalenceGroup::new(entries.clone());
            eq_groups.bridge_classes();
            let eq_groups = eq_groups.classes;
            let err_msg = format!(
                "error in test entries: {:?}, expected: {:?}, actual:{:?}",
                entries, expected, eq_groups
            );
            assert_eq!(eq_groups.len(), expected.len(), "{}", err_msg);
            for idx in 0..eq_groups.len() {
                assert_eq!(&eq_groups[idx], &expected[idx], "{}", err_msg);
            }
        }
        Ok(())
    }

    #[test]
    fn test_remove_redundant_entries_eq_group() -> Result<()> {
        let c = |idx| Arc::new(Column::new(format!("col_{}", idx).as_str(), idx)) as _;
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
        let mut eq_groups = EquivalenceGroup::new(entries);
        eq_groups.remove_redundant_entries();
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
                description:
                    "Columns in different equivalence classes should not be equal",
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
                description:
                    "Binary expressions with equivalent operands should be equal",
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
                description:
                    "Binary expressions with non-equivalent operands should not be equal",
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
                "{}: Failed comparing {:?} and {:?}, expected {}, got {}",
                description, left, right, expected, actual
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

        let mapping = ProjectionMapping {
            map: vec![
                (
                    binary(
                        col("a", &schema)?,
                        Operator::Plus,
                        col("c", &schema)?,
                        &schema,
                    )?,
                    col("a+c", &projected_schema)?,
                ),
                (
                    binary(
                        col("b", &schema)?,
                        Operator::Plus,
                        col("c", &schema)?,
                        &schema,
                    )?,
                    col("b+c", &projected_schema)?,
                ),
            ],
        };

        let projected = group.project(&mapping);

        assert!(!projected.is_empty());
        let first_normalized = projected.normalize_expr(col("a+c", &projected_schema)?);
        let second_normalized = projected.normalize_expr(col("b+c", &projected_schema)?);

        assert!(first_normalized.eq(&second_normalized));

        Ok(())
    }
}
