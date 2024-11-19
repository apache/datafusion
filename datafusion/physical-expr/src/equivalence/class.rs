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

use super::{add_offset_to_expr, collapse_lex_req, ProjectionMapping};
use crate::{
    expressions::Column, LexOrdering, LexRequirement, PhysicalExpr, PhysicalExprRef,
    PhysicalSortExpr, PhysicalSortRequirement,
};
use std::fmt::Display;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{JoinType, ScalarValue};
use datafusion_physical_expr_common::physical_expr::format_physical_expr_list;

use indexmap::{IndexMap, IndexSet};

/// A structure representing a expression known to be constant in a physical execution plan.
///
/// The `ConstExpr` struct encapsulates an expression that is constant during the execution
/// of a query. For example if a predicate like `A = 5` applied earlier in the plan `A` would
/// be known constant
///
/// # Fields
///
/// - `expr`: Constant expression for a node in the physical plan.
///
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
/// // Create a constant expression from a physical expression ref
/// let const_expr = ConstExpr::from(&col);
/// // create a constant expression from a physical expression
/// let const_expr = ConstExpr::from(col);
/// ```
#[derive(Debug, Clone)]
pub struct ConstExpr {
    /// The  expression that is known to be constant (e.g. a `Column`)
    expr: Arc<dyn PhysicalExpr>,
    /// Does the constant have the same value across all partitions? See
    /// struct docs for more details
    across_partitions: bool,
    value: Option<ScalarValue>
}

impl PartialEq for ConstExpr {
    fn eq(&self, other: &Self) -> bool {
        self.across_partitions == other.across_partitions && self.expr.eq(&other.expr)
    }
}

impl ConstExpr {
    /// Create a new constant expression from a physical expression.
    ///
    /// Note you can also use `ConstExpr::from` to create a constant expression
    /// from a reference as well
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            expr,
            // By default, assume constant expressions are not same across partitions.
            across_partitions: false,
            value: None,
        }
    }

    pub fn with_value(mut self, value: ScalarValue) -> Self {
        self.value = Some(value);
        self
    }

    /// Set the `across_partitions` flag
    ///
    /// See struct docs for more details
    pub fn with_across_partitions(mut self, across_partitions: bool) -> Self {
        self.across_partitions = across_partitions;
        self
    }

    /// Is the  expression the same across all partitions?
    ///
    /// See struct docs for more details
    pub fn across_partitions(&self) -> bool {
        self.across_partitions
    }

    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    pub fn owned_expr(self) -> Arc<dyn PhysicalExpr> {
        self.expr
    }

    pub fn value(&self) -> Option<&ScalarValue> {
        self.value.as_ref()
    }

    pub fn map<F>(&self, f: F) -> Option<Self>
    where
        F: Fn(&Arc<dyn PhysicalExpr>) -> Option<Arc<dyn PhysicalExpr>>,
    {
        let maybe_expr = f(&self.expr);
        maybe_expr.map(|expr| Self {
            expr,
            across_partitions: self.across_partitions,
            value: self.value.clone(),
        })
    }

    /// Returns true if this constant expression is equal to the given expression
    pub fn eq_expr(&self, other: impl AsRef<dyn PhysicalExpr>) -> bool {
        self.expr.as_ref() == other.as_ref()
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

/// Display implementation for `ConstExpr`
///
/// Example `c` or `c(across_partitions)`
impl Display for ConstExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.expr)?;
        if self.across_partitions {
            write!(f, "(across_partitions)")?;
        }
        if let Some(value) = self.value.as_ref() {
            write!(f, "({})", value)?;
        }
        Ok(())
    }
}

impl From<Arc<dyn PhysicalExpr>> for ConstExpr {
    fn from(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self::new(expr)
    }
}

impl From<&Arc<dyn PhysicalExpr>> for ConstExpr {
    fn from(expr: &Arc<dyn PhysicalExpr>) -> Self {
        Self::new(Arc::clone(expr))
    }
}

/// Checks whether `expr` is among in the `const_exprs`.
pub fn const_exprs_contains(
    const_exprs: &[ConstExpr],
    expr: &Arc<dyn PhysicalExpr>,
) -> bool {
    const_exprs
        .iter()
        .any(|const_expr| const_expr.expr.eq(expr))
}

/// An `EquivalenceClass` is a set of [`Arc<dyn PhysicalExpr>`]s that are known
/// to have the same value for all tuples in a relation. These are generated by
/// equality predicates (e.g. `a = b`), typically equi-join conditions and
/// equality conditions in filters.
///
/// Two `EquivalenceClass`es are equal if they contains the same expressions in
/// without any ordering.
#[derive(Debug, Clone)]
pub struct EquivalenceClass {
    /// The expressions in this equivalence class. The order doesn't
    /// matter for equivalence purposes
    ///
    exprs: IndexSet<Arc<dyn PhysicalExpr>>,
}

impl PartialEq for EquivalenceClass {
    /// Returns true if other is equal in the sense
    /// of bags (multi-sets), disregarding their orderings.
    fn eq(&self, other: &Self) -> bool {
        self.exprs.eq(&other.exprs)
    }
}

impl EquivalenceClass {
    /// Create a new empty equivalence class
    pub fn new_empty() -> Self {
        Self {
            exprs: IndexSet::new(),
        }
    }

    // Create a new equivalence class from a pre-existing `Vec`
    pub fn new(exprs: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            exprs: exprs.into_iter().collect(),
        }
    }

    /// Return the inner vector of expressions
    pub fn into_vec(self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.exprs.into_iter().collect()
    }

    /// Return the "canonical" expression for this class (the first element)
    /// if any
    fn canonical_expr(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.exprs.iter().next().cloned()
    }

    /// Insert the expression into this class, meaning it is known to be equal to
    /// all other expressions in this class
    pub fn push(&mut self, expr: Arc<dyn PhysicalExpr>) {
        self.exprs.insert(expr);
    }

    /// Inserts all the expressions from other into this class
    pub fn extend(&mut self, other: Self) {
        for expr in other.exprs {
            // use push so entries are deduplicated
            self.push(expr);
        }
    }

    /// Returns true if this equivalence class contains t expression
    pub fn contains(&self, expr: &Arc<dyn PhysicalExpr>) -> bool {
        self.exprs.contains(expr)
    }

    /// Returns true if this equivalence class has any entries in common with `other`
    pub fn contains_any(&self, other: &Self) -> bool {
        self.exprs.iter().any(|e| other.contains(e))
    }

    /// return the number of items in this class
    pub fn len(&self) -> usize {
        self.exprs.len()
    }

    /// return true if this class is empty
    pub fn is_empty(&self) -> bool {
        self.exprs.is_empty()
    }

    /// Iterate over all elements in this class, in some arbitrary order
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
            .map(|e| add_offset_to_expr(e, offset))
            .collect();
        Self::new(new_exprs)
    }
}

impl Display for EquivalenceClass {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{}]", format_physical_expr_list(&self.exprs))
    }
}

/// An `EquivalenceGroup` is a collection of `EquivalenceClass`es where each
/// class represents a distinct equivalence class in a relation.
#[derive(Debug, Clone)]
pub struct EquivalenceGroup {
    pub classes: Vec<EquivalenceClass>,
}

impl EquivalenceGroup {
    /// Creates an empty equivalence group.
    pub fn empty() -> Self {
        Self { classes: vec![] }
    }

    /// Creates an equivalence group from the given equivalence classes.
    pub fn new(classes: Vec<EquivalenceClass>) -> Self {
        let mut result = Self { classes };
        result.remove_redundant_entries();
        result
    }

    /// Returns how many equivalence classes there are in this group.
    pub fn len(&self) -> usize {
        self.classes.len()
    }

    /// Checks whether this equivalence group is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an iterator over the equivalence classes in this group.
    pub fn iter(&self) -> impl Iterator<Item = &EquivalenceClass> {
        self.classes.iter()
    }

    /// Adds the equality `left` = `right` to this equivalence group.
    /// New equality conditions often arise after steps like `Filter(a = b)`,
    /// `Alias(a, a as b)` etc.
    pub fn add_equal_conditions(
        &mut self,
        left: &Arc<dyn PhysicalExpr>,
        right: &Arc<dyn PhysicalExpr>,
    ) {
        let mut first_class = None;
        let mut second_class = None;
        for (idx, cls) in self.classes.iter().enumerate() {
            if cls.contains(left) {
                first_class = Some(idx);
            }
            if cls.contains(right) {
                second_class = Some(idx);
            }
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
                self.classes[group_idx].push(Arc::clone(right));
            }
            (None, Some(group_idx)) => {
                // Left side is new, extend right side's class:
                self.classes[group_idx].push(Arc::clone(left));
            }
            (None, None) => {
                // None of the expressions is among existing classes.
                // Create a new equivalence class and extend the group.
                self.classes.push(EquivalenceClass::new(vec![
                    Arc::clone(left),
                    Arc::clone(right),
                ]));
            }
        }
    }

    /// Removes redundant entries from this group.
    fn remove_redundant_entries(&mut self) {
        // Remove duplicate entries from each equivalence class:
        self.classes.retain_mut(|cls| {
            // Keep groups that have at least two entries as singleton class is
            // meaningless (i.e. it contains no non-trivial information):
            cls.len() > 1
        });
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
        Arc::clone(&expr)
            .transform(|expr| {
                for cls in self.iter() {
                    if cls.contains(&expr) {
                        return Ok(Transformed::yes(cls.canonical_expr().unwrap()));
                    }
                }
                Ok(Transformed::no(expr))
            })
            .data()
            .unwrap_or(expr)
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

    /// This function applies the `normalize_expr` function for all expressions
    /// in `exprs` and returns the corresponding normalized physical expressions.
    pub fn normalize_exprs(
        &self,
        exprs: impl IntoIterator<Item = Arc<dyn PhysicalExpr>>,
    ) -> Vec<Arc<dyn PhysicalExpr>> {
        exprs
            .into_iter()
            .map(|expr| self.normalize_expr(expr))
            .collect()
    }

    /// This function applies the `normalize_sort_expr` function for all sort
    /// expressions in `sort_exprs` and returns the corresponding normalized
    /// sort expressions.
    pub fn normalize_sort_exprs(&self, sort_exprs: &LexOrdering) -> LexOrdering {
        // Convert sort expressions to sort requirements:
        let sort_reqs = LexRequirement::from(sort_exprs.clone());
        // Normalize the requirements:
        let normalized_sort_reqs = self.normalize_sort_requirements(&sort_reqs);
        // Convert sort requirements back to sort expressions:
        LexOrdering::from(normalized_sort_reqs)
    }

    /// This function applies the `normalize_sort_requirement` function for all
    /// requirements in `sort_reqs` and returns the corresponding normalized
    /// sort requirements.
    pub fn normalize_sort_requirements(
        &self,
        sort_reqs: &LexRequirement,
    ) -> LexRequirement {
        collapse_lex_req(LexRequirement::new(
            sort_reqs
                .iter()
                .map(|sort_req| self.normalize_sort_requirement(sort_req.clone()))
                .collect(),
        ))
    }

    /// Projects `expr` according to the given projection mapping.
    /// If the resulting expression is invalid after projection, returns `None`.
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
                if self
                    .get_equivalence_class(source)
                    .is_some_and(|group| group.contains(expr))
                {
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
        let projected_classes = self.iter().filter_map(|cls| {
            let new_class = cls
                .iter()
                .filter_map(|expr| self.project_expr(mapping, expr))
                .collect::<Vec<_>>();
            (new_class.len() > 1).then_some(EquivalenceClass::new(new_class))
        });
        // the key is the source expression and the value is the EquivalenceClass that contains the target expression of the source expression.
        let mut new_classes: IndexMap<Arc<dyn PhysicalExpr>, EquivalenceClass> =
            IndexMap::new();
        mapping.iter().for_each(|(source, target)| {
            new_classes
                .entry(Arc::clone(source))
                .or_insert_with(EquivalenceClass::new_empty)
                .push(Arc::clone(target));
        });
        // Only add equivalence classes with at least two members as singleton
        // equivalence classes are meaningless.
        let new_classes = new_classes
            .into_iter()
            .filter_map(|(_, cls)| (cls.len() > 1).then_some(cls));

        let classes = projected_classes.chain(new_classes).collect();
        Self::new(classes)
    }

    /// Returns the equivalence class containing `expr`. If no equivalence class
    /// contains `expr`, returns `None`.
    fn get_equivalence_class(
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
                    self.iter()
                        .cloned()
                        .chain(
                            right_equivalences
                                .iter()
                                .map(|cls| cls.with_offset(left_size)),
                        )
                        .collect(),
                );
                // In we have an inner join, expressions in the "on" condition
                // are equal in the resulting table.
                if join_type == &JoinType::Inner {
                    for (lhs, rhs) in on.iter() {
                        let new_lhs = Arc::clone(lhs) as _;
                        // Rewrite rhs to point to the right side of the join:
                        let new_rhs = Arc::clone(rhs)
                            .transform(|expr| {
                                if let Some(column) =
                                    expr.as_any().downcast_ref::<Column>()
                                {
                                    let new_column = Arc::new(Column::new(
                                        column.name(),
                                        column.index() + left_size,
                                    ))
                                        as _;
                                    return Ok(Transformed::yes(new_column));
                                }

                                Ok(Transformed::no(expr))
                            })
                            .data()
                            .unwrap();
                        result.add_equal_conditions(&new_lhs, &new_rhs);
                    }
                }
                result
            }
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => self.clone(),
            JoinType::RightSemi | JoinType::RightAnti => right_equivalences.clone(),
        }
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
    use crate::expressions::{lit, Literal};

    use datafusion_common::{Result, ScalarValue};

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
                .map(|entry| entry.into_iter().map(lit).collect::<Vec<_>>())
                .map(EquivalenceClass::new)
                .collect::<Vec<_>>();
            let expected = expected
                .into_iter()
                .map(|entry| entry.into_iter().map(lit).collect::<Vec<_>>())
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
        let entries = [
            EquivalenceClass::new(vec![lit(1), lit(1), lit(2)]),
            // This group is meaningless should be removed
            EquivalenceClass::new(vec![lit(3), lit(3)]),
            EquivalenceClass::new(vec![lit(4), lit(5), lit(6)]),
        ];
        // Given equivalences classes are not in succinct form.
        // Expected form is the most plain representation that is functionally same.
        let expected = [
            EquivalenceClass::new(vec![lit(1), lit(2)]),
            EquivalenceClass::new(vec![lit(4), lit(5), lit(6)]),
        ];
        let mut eq_groups = EquivalenceGroup::new(entries.to_vec());
        eq_groups.remove_redundant_entries();

        let eq_groups = eq_groups.classes;
        assert_eq!(eq_groups.len(), expected.len());
        assert_eq!(eq_groups.len(), 2);

        assert_eq!(eq_groups[0], expected[0]);
        assert_eq!(eq_groups[1], expected[1]);
        Ok(())
    }

    #[test]
    fn test_schema_normalize_expr_with_equivalence() -> Result<()> {
        let col_a = &Column::new("a", 0);
        let col_b = &Column::new("b", 1);
        let col_c = &Column::new("c", 2);
        // Assume that column a and c are aliases.
        let (_test_schema, eq_properties) = create_test_params()?;

        let col_a_expr = Arc::new(col_a.clone()) as Arc<dyn PhysicalExpr>;
        let col_b_expr = Arc::new(col_b.clone()) as Arc<dyn PhysicalExpr>;
        let col_c_expr = Arc::new(col_c.clone()) as Arc<dyn PhysicalExpr>;
        // Test cases for equivalence normalization,
        // First entry in the tuple is argument, second entry is expected result after normalization.
        let expressions = vec![
            // Normalized version of the column a and c should go to a
            // (by convention all the expressions inside equivalence class are mapped to the first entry
            // in this case a is the first entry in the equivalence class.)
            (&col_a_expr, &col_a_expr),
            (&col_c_expr, &col_a_expr),
            // Cannot normalize column b
            (&col_b_expr, &col_b_expr),
        ];
        let eq_group = eq_properties.eq_group();
        for (expr, expected_eq) in expressions {
            assert!(
                expected_eq.eq(&eq_group.normalize_expr(Arc::clone(expr))),
                "error in test: expr: {expr:?}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_contains_any() {
        let lit_true = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
            as Arc<dyn PhysicalExpr>;
        let lit_false = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))
            as Arc<dyn PhysicalExpr>;
        let lit2 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))) as Arc<dyn PhysicalExpr>;
        let lit1 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
        let col_b_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;

        let cls1 =
            EquivalenceClass::new(vec![Arc::clone(&lit_true), Arc::clone(&lit_false)]);
        let cls2 =
            EquivalenceClass::new(vec![Arc::clone(&lit_true), Arc::clone(&col_b_expr)]);
        let cls3 = EquivalenceClass::new(vec![Arc::clone(&lit2), Arc::clone(&lit1)]);

        // lit_true is common
        assert!(cls1.contains_any(&cls2));
        // there is no common entry
        assert!(!cls1.contains_any(&cls3));
        assert!(!cls2.contains_any(&cls3));
    }
}
