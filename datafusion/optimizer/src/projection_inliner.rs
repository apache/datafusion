// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! A single check for rewriting expressions through a projection.
//!
//! Many rules move an expression that is written over the output of a
//! projection below that projection, or merge it into the projection. To do
//! this, they replace each column reference with the expression that defines
//! the column. This is called *inlining*.
//!
//! Inlining can make the plan evaluate a definition more times than before:
//!
//! - The consumer can reference the column more than one time.
//! - The projection can stay in the plan and still compute the definition.
//!
//! When the definition is volatile, this changes the query result. That is
//! never allowed.
//!
//! Otherwise a second evaluation only costs time, and DataFusion does not
//! know what an expression costs. So only a definition that
//! [`should_push_to_leaves`] is evaluated more than one time: a column, or a
//! call such as struct field access that asks to move towards the leaves.
//! This is the merge guard of `OptimizeProjections`
//! (<https://github.com/apache/datafusion/issues/8296>).
//!
//! [`should_push_to_leaves`]: datafusion_expr::ExpressionPlacement::should_push_to_leaves
//! [`ProjectionInliner`] is the one place that makes this decision. Rules call
//! it instead of doing the substitution themselves.
//!

use std::collections::{HashMap, HashSet};

use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{Column, Result};
use datafusion_expr::{Expr, Projection};

/// Returns `true` if a second evaluation of `expr` can change the query
/// result.
///
/// This is checked before placement. A volatile function is free to report
/// [`MoveTowardsLeafNodes`](datafusion_expr::ExpressionPlacement::MoveTowardsLeafNodes),
/// and `file_row_index()` does.
fn must_not_duplicate(expr: &Expr) -> bool {
    expr.is_volatile()
}

/// Returns `true` if `expr` may be evaluated more than one time.
fn may_duplicate(expr: &Expr) -> bool {
    !must_not_duplicate(expr) && expr.placement().should_push_to_leaves()
}

/// Definitions that [`ProjectionInliner::pinned`] refuses to inline.
#[derive(Debug, Default)]
pub(crate) struct PinnedDefinitions(HashSet<usize>);

impl PinnedDefinitions {
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Rewrites expressions over the output of a projection into expressions over
/// its input, and decides if that is possible without evaluating a definition
/// too many times.
///
/// The rule is: after the rewrite, a definition that is evaluated more than
/// one time must be a column or a leaf expression, and must not be volatile.
/// See [`ProjectionInliner::pinned`].
#[derive(Debug)]
pub(crate) struct ProjectionInliner<'a> {
    /// Definitions without top-level alias, in output column order.
    definitions: Vec<&'a Expr>,
    /// Output column -> index into `definitions`
    by_column: HashMap<Column, usize>,
    /// Output column name -> index into `definitions`, for unqualified
    /// references. `None` if the name is ambiguous.
    by_name: HashMap<&'a str, Option<usize>>,
}

impl<'a> ProjectionInliner<'a> {
    /// Creates an inliner for the output columns of `projection`.
    pub(crate) fn new(projection: &'a Projection) -> Self {
        let mut inliner = Self {
            definitions: Vec::with_capacity(projection.expr.len()),
            by_column: HashMap::with_capacity(projection.expr.len()),
            by_name: HashMap::with_capacity(projection.expr.len()),
        };
        let columns = projection.schema.iter();
        for (idx, ((qualifier, field), expr)) in
            columns.zip(projection.expr.iter()).enumerate()
        {
            inliner.definitions.push(unalias(expr));
            inliner
                .by_column
                .insert(Column::new(qualifier.cloned(), field.name()), idx);
            inliner
                .by_name
                .entry(field.name().as_str())
                .and_modify(|found| *found = None)
                .or_insert(Some(idx));
        }
        inliner
    }

    /// Resolves `column` like [`DFSchema::index_of_column`] does: an
    /// unqualified reference matches a qualified output column with the same
    /// name, if there is only one.
    ///
    /// [`DFSchema::index_of_column`]: datafusion_common::DFSchema::index_of_column
    fn index_of(&self, column: &Column) -> Option<usize> {
        match self.by_column.get(column) {
            Some(idx) => Some(*idx),
            None if column.relation.is_none() => {
                self.by_name.get(column.name.as_str()).copied().flatten()
            }
            None => None,
        }
    }

    /// Returns the definitions that must not be inlined into `inlined`.
    ///
    /// - `inlined`: the expressions whose column references will be replaced
    ///   with their definitions. A definition is evaluated one time for each
    ///   reference.
    /// - `others`: the other known consumers of the projection. They keep
    ///   their column references, so a definition that they reference is
    ///   evaluated one more time, in the projection.
    ///
    /// A definition that nothing else references is assumed to be removed
    /// from the projection (for example by `OptimizeProjections`).
    ///
    /// A definition that `inlined` references is pinned if it is then
    /// evaluated more than one time and [`may_duplicate`] does not hold.
    pub(crate) fn pinned<'i, 'o>(
        &self,
        inlined: impl IntoIterator<Item = &'i Expr>,
        others: impl IntoIterator<Item = &'o Expr>,
    ) -> PinnedDefinitions {
        self.pinned_impl(inlined, others, false)
    }

    /// Returns [`Self::pinned`], and also pins every definition that
    /// `inlined` references and that [`must_not_duplicate`].
    ///
    /// Use this when the projection can stay in the plan for consumers that
    /// the caller can not see. A copy of a forbidden definition would then
    /// be evaluated independently of the original, and change the query
    /// result.
    pub(crate) fn pinned_with_unknown_consumers<'i, 'o>(
        &self,
        inlined: impl IntoIterator<Item = &'i Expr>,
        others: impl IntoIterator<Item = &'o Expr>,
    ) -> PinnedDefinitions {
        self.pinned_impl(inlined, others, true)
    }

    fn pinned_impl<'i, 'o>(
        &self,
        inlined: impl IntoIterator<Item = &'i Expr>,
        others: impl IntoIterator<Item = &'o Expr>,
        pin_forbidden: bool,
    ) -> PinnedDefinitions {
        let inlined_references = self.count_references(inlined);
        if inlined_references.is_empty() {
            return PinnedDefinitions::default();
        }
        let other_references = self.count_references(others);
        let pinned = inlined_references
            .into_iter()
            .filter(|(idx, count)| {
                let evaluations = count + usize::from(other_references.contains_key(idx));
                if evaluations <= 1 && !pin_forbidden {
                    return false;
                }
                let definition = self.definitions[*idx];
                (pin_forbidden && must_not_duplicate(definition))
                    || (evaluations > 1 && !may_duplicate(definition))
            })
            .map(|(idx, _)| idx)
            .collect();
        PinnedDefinitions(pinned)
    }

    /// Returns `true` if `expr` references a definition in `pinned`.
    pub(crate) fn references_pinned(
        &self,
        expr: &Expr,
        pinned: &PinnedDefinitions,
    ) -> bool {
        !pinned.is_empty()
            && expr
                .exists(|e| {
                    Ok(matches!(e, Expr::Column(col) if self.is_pinned(col, pinned)))
                })
                .expect("exists closure is infallible")
    }

    /// Returns `true` if the definition of `column` is in `pinned`.
    pub(crate) fn is_pinned(&self, column: &Column, pinned: &PinnedDefinitions) -> bool {
        self.index_of(column)
            .is_some_and(|idx| pinned.0.contains(&idx))
    }

    /// Returns the definition of `column`, without its top-level alias.
    pub(crate) fn definition(&self, column: &Column) -> Option<&'a Expr> {
        self.index_of(column).map(|idx| self.definitions[idx])
    }

    /// Replaces each column reference in `expr` with its definition, without a
    /// check. Callers must check [`Self::pinned`] first.
    pub(crate) fn substitute(&self, expr: Expr) -> Result<Expr> {
        expr.transform_up(|e| {
            if let Expr::Column(col) = &e
                && let Some(definition) = self.definition(col)
            {
                Ok(Transformed::yes(definition.clone()))
            } else {
                Ok(Transformed::no(e))
            }
        })
        .data()
    }

    /// Counts the references to each definition in `exprs`.
    fn count_references<'e>(
        &self,
        exprs: impl IntoIterator<Item = &'e Expr>,
    ) -> HashMap<usize, usize> {
        let mut references: HashMap<usize, usize> = HashMap::new();
        for expr in exprs {
            expr.apply(|e| {
                if let Expr::Column(col) = e
                    && let Some(idx) = self.index_of(col)
                {
                    *references.entry(idx).or_default() += 1;
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("traversal is infallible");
        }
        references
    }
}

/// Removes all top-level aliases.
pub(crate) fn unalias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => unalias(&alias.expr),
        _ => expr,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::test_table_scan;
    use crate::test::udfs::{PlacementTestUDF, get_field_like};
    use arrow::datatypes::DataType;
    use datafusion_expr::{
        ExpressionPlacement, LogicalPlan, LogicalPlanBuilder, ScalarUDF, Volatility,
        cast, col, lit,
    };

    fn udf(placement: ExpressionPlacement, volatility: Volatility) -> ScalarUDF {
        ScalarUDF::new_from_impl(
            PlacementTestUDF::new()
                .with_placement(placement)
                .with_volatility(volatility),
        )
    }

    fn keep_in_place(arg: Expr) -> Expr {
        udf(ExpressionPlacement::KeepInPlace, Volatility::Immutable).call(vec![arg])
    }

    fn volatile(arg: Expr) -> Expr {
        udf(ExpressionPlacement::KeepInPlace, Volatility::Volatile).call(vec![arg])
    }

    /// A volatile function that reports
    /// [`ExpressionPlacement::MoveTowardsLeafNodes`], as `file_row_index()`
    /// does.
    fn volatile_leaf(arg: Expr) -> Expr {
        udf(
            ExpressionPlacement::MoveTowardsLeafNodes,
            Volatility::Volatile,
        )
        .call(vec![arg])
    }

    fn projection(exprs: Vec<Expr>) -> Projection {
        let plan = LogicalPlanBuilder::from(test_table_scan().unwrap())
            .project(exprs)
            .unwrap()
            .build()
            .unwrap();
        let LogicalPlan::Projection(projection) = plan else {
            unreachable!()
        };
        projection
    }

    #[test]
    fn may_duplicate_columns_and_leaf_expressions() {
        assert!(may_duplicate(&col("a")));
        assert!(may_duplicate(&get_field_like(col("a"), "x")));
        assert!(!may_duplicate(&lit(1)));
        assert!(!may_duplicate(&(col("a") * lit(2))));
        assert!(!may_duplicate(&cast(col("a"), DataType::Int64)));
        assert!(!may_duplicate(&keep_in_place(col("a"))));
        assert!(!may_duplicate(&get_field_like(volatile(col("a")), "x")));
    }

    /// Volatility beats placement. A volatile function that reports
    /// `MoveTowardsLeafNodes` still gives two different answers.
    #[test]
    fn a_volatile_leaf_function_must_not_be_duplicated() {
        assert!(!may_duplicate(&volatile_leaf(col("a"))));
        assert!(!may_duplicate(&volatile_leaf(col("a")).alias("x")));
        assert!(may_duplicate(
            &udf(
                ExpressionPlacement::MoveTowardsLeafNodes,
                Volatility::Immutable
            )
            .call(vec![col("a")])
        ));
    }

    #[test]
    fn pinned_counts_evaluations() {
        let p = projection(vec![col("a"), keep_in_place(col("c")).alias("k")]);
        let inliner = ProjectionInliner::new(&p);
        let no_others: [&Expr; 0] = [];

        // One reference: allowed.
        let one = get_field_like(col("k"), "x");
        assert!(inliner.pinned([&one], no_others).is_empty());
        // The same reference when another consumer keeps the column: refused.
        assert!(!inliner.pinned([&one], [&col("k")]).is_empty());
        // Two references in one expression: refused.
        let two = col("k").is_not_null().and(get_field_like(col("k"), "x"));
        assert!(!inliner.pinned([&two], no_others).is_empty());
        // References are counted across all inlined expressions.
        assert!(!inliner.pinned([&col("k"), &col("k")], no_others).is_empty());
        // A column is always allowed.
        assert!(
            inliner
                .pinned([&col("a"), &col("a")], [&col("a")])
                .is_empty()
        );
        // An unqualified reference resolves to a qualified output column.
        assert_eq!(inliner.index_of(&Column::from_name("a")), Some(0));
        assert_eq!(
            inliner.index_of(&Column::from_qualified_name("test.a")),
            Some(0)
        );
        assert_eq!(inliner.index_of(&Column::from_qualified_name("t.a")), None);
    }

    #[test]
    fn forbidden_definitions_are_pinned_when_duplicated() {
        let p = projection(vec![volatile(col("a")).alias("r")]);
        let inliner = ProjectionInliner::new(&p);
        let no_others: [&Expr; 0] = [];
        assert!(inliner.pinned([&col("r")], no_others).is_empty());
        assert!(
            !inliner
                .pinned([&(col("r") + col("r"))], no_others)
                .is_empty()
        );
        assert!(!inliner.pinned([&col("r")], [&col("r")]).is_empty());
        // Without all the consumers, a single reference is pinned as well.
        assert!(
            !inliner
                .pinned_with_unknown_consumers([&col("r")], no_others)
                .is_empty()
        );
    }

    #[test]
    fn substitute_and_references_pinned() -> Result<()> {
        let p = projection(vec![
            col("a"),
            keep_in_place(col("c")).alias("k"),
            (col("b") + lit(1)).alias("b1"),
        ]);
        let inliner = ProjectionInliner::new(&p);
        let no_others: [&Expr; 0] = [];

        let twice = col("k").is_not_null().and(get_field_like(col("k"), "x"));
        let other = col("b1").gt(col("a"));
        let pinned = inliner.pinned_with_unknown_consumers([&twice, &other], no_others);
        assert!(inliner.references_pinned(&twice, &pinned));
        assert!(!inliner.references_pinned(&other, &pinned));

        assert_eq!(
            inliner.definition(&Column::from_name("k")),
            Some(&keep_in_place(col("test.c")))
        );
        assert_eq!(
            inliner.substitute(other)?,
            (col("test.b") + lit(1)).gt(col("test.a"))
        );
        Ok(())
    }
}
