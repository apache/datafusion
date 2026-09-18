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

//! A single, cost-aware check for rewriting expressions through a projection.
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
//! When the definition is volatile, this changes the query result. When the
//! definition is expensive, this makes the query slower. A typical example is a
//! column that [`CommonSubexprEliminate`] created on purpose, so that an
//! expensive expression is computed one time only.
//!
//! [`ProjectionInliner`] is the one place that makes this decision.
//!
//! [`CommonSubexprEliminate`]: crate::common_subexpr_eliminate::CommonSubexprEliminate

use std::collections::{HashMap, HashSet};

use datafusion_common::Column;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::{Expr, ExpressionPlacement, Projection};

/// The cost of one more evaluation of an expression.
///
/// The variants are in increasing order of cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum DuplicationCost {
    /// Column references and expressions that report
    /// [`ExpressionPlacement::MoveTowardsLeafNodes`] (for example struct field
    /// access). These are free, or almost free, to evaluate again.
    Free,
    /// Built-in operators over free or cheap operands: arithmetic,
    /// comparisons, `CAST`, `CASE`, `IS NULL`, and similar. They are
    /// vectorized kernels, so an extra evaluation costs little.
    ///
    /// A bare literal is also in this class. This keeps the merge guard of
    /// `OptimizeProjections` (issue #8296) unchanged.
    Cheap,
    /// Contains a scalar function that reports
    /// [`ExpressionPlacement::KeepInPlace`] for column arguments. This is the
    /// default for a user-defined function, so the optimizer must assume that
    /// it can be arbitrarily expensive.
    Expensive,
    /// Must never be evaluated more than one time: the expression is volatile
    /// (for example `random()`) or contains a subquery.
    Forbidden,
}

impl DuplicationCost {
    /// Returns the cost of one more evaluation of `expr`.
    fn of(expr: &Expr) -> Self {
        let expr = unalias(expr);
        if expr.placement().should_push_to_leaves() {
            return Self::Free;
        }

        let mut cost = Self::Cheap;
        expr.apply(|e| {
            let node_cost = match e {
                Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::SetComparison(_) => Self::Forbidden,
                Expr::ScalarFunction(_) if e.is_volatile_node() => Self::Forbidden,
                Expr::ScalarFunction(func) => {
                    // Ask the function for its own placement, as if its
                    // arguments were plain columns and literals. This isolates
                    // the cost of the function from the cost of its arguments
                    // (the walk visits the arguments separately). For example
                    // `get_field(CAST(s AS ...), 'f')` is a cheap field access
                    // over a cheap cast.
                    let arg_placements: Vec<_> = func
                        .args
                        .iter()
                        .map(|arg| match arg.placement() {
                            ExpressionPlacement::Literal => ExpressionPlacement::Literal,
                            _ => ExpressionPlacement::Column,
                        })
                        .collect();
                    match func.func.placement(&arg_placements) {
                        ExpressionPlacement::KeepInPlace => Self::Expensive,
                        _ => Self::Cheap,
                    }
                }
                _ => Self::Cheap,
            };
            cost = cost.max(node_cost);
            Ok(if cost == Self::Forbidden {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })
        .expect("traversal is infallible");
        cost
    }
}

/// Definitions that [`ProjectionInliner::pinned`] refuses to inline.
#[derive(Debug, Default)]
pub(crate) struct PinnedDefinitions(HashSet<usize>);

impl PinnedDefinitions {
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Decides if the column references of expressions over the output of a
/// projection can be replaced with their definitions, without evaluating a
/// definition too many times.
///
/// The rule is: after the rewrite, a definition that is evaluated more than
/// one time must have a [`DuplicationCost`] of at most the `max_cost` that the
/// caller gives. See [`ProjectionInliner::pinned`].
#[derive(Debug)]
pub(crate) struct ProjectionInliner<'a> {
    /// (definition without top-level alias, cost), in output column order
    definitions: Vec<(&'a Expr, DuplicationCost)>,
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
            let expr = unalias(expr);
            inliner.definitions.push((expr, DuplicationCost::of(expr)));
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
    /// evaluated more than one time and its [`DuplicationCost`] is more than
    /// `max_cost`.
    pub(crate) fn pinned<'i, 'o>(
        &self,
        inlined: impl IntoIterator<Item = &'i Expr>,
        others: impl IntoIterator<Item = &'o Expr>,
        max_cost: DuplicationCost,
    ) -> PinnedDefinitions {
        debug_assert!(
            max_cost < DuplicationCost::Forbidden,
            "a forbidden definition must never be duplicated"
        );
        let inlined_references = self.count_references(inlined);
        if inlined_references.is_empty() {
            return PinnedDefinitions::default();
        }
        let other_references = self.count_references(others);
        let pinned = inlined_references
            .into_iter()
            .filter(|(idx, count)| {
                let evaluations = count + usize::from(other_references.contains_key(idx));
                evaluations > 1 && self.definitions[*idx].1 > max_cost
            })
            .map(|(idx, _)| idx)
            .collect();
        PinnedDefinitions(pinned)
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
fn unalias(expr: &Expr) -> &Expr {
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
        LogicalPlan, LogicalPlanBuilder, ScalarUDF, Volatility, cast, col, lit,
    };

    fn udf(placement: ExpressionPlacement, volatility: Volatility) -> ScalarUDF {
        ScalarUDF::new_from_impl(
            PlacementTestUDF::new()
                .with_placement(placement)
                .with_volatility(volatility),
        )
    }

    fn expensive(arg: Expr) -> Expr {
        udf(ExpressionPlacement::KeepInPlace, Volatility::Immutable).call(vec![arg])
    }

    fn volatile(arg: Expr) -> Expr {
        udf(ExpressionPlacement::KeepInPlace, Volatility::Volatile).call(vec![arg])
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
    fn duplication_cost_classification() {
        use DuplicationCost::*;
        assert_eq!(DuplicationCost::of(&col("a")), Free);
        assert_eq!(DuplicationCost::of(&get_field_like(col("a"), "x")), Free);
        assert_eq!(DuplicationCost::of(&lit(1)), Cheap);
        assert_eq!(DuplicationCost::of(&(col("a") * lit(2))), Cheap);
        assert_eq!(
            DuplicationCost::of(&cast(col("a"), DataType::Int64).alias("x")),
            Cheap
        );
        // A field access over a cheap operand is still cheap.
        assert_eq!(
            DuplicationCost::of(&get_field_like(col("a") + lit(1), "x")),
            Cheap
        );
        assert_eq!(DuplicationCost::of(&expensive(col("a"))), Expensive);
        assert_eq!(
            DuplicationCost::of(&get_field_like(expensive(col("a")), "x")),
            Expensive
        );
        assert_eq!(
            DuplicationCost::of(&(expensive(col("a")) + lit(1))),
            Expensive
        );
        assert_eq!(
            DuplicationCost::of(&(volatile(col("a")) + lit(1))),
            Forbidden
        );
    }

    #[test]
    fn pinned_counts_evaluations() {
        use DuplicationCost::*;
        let p = projection(vec![
            col("a"),
            (col("b") + lit(1)).alias("cheap"),
            expensive(col("c")).alias("exp"),
        ]);
        let inliner = ProjectionInliner::new(&p);
        let no_others: [&Expr; 0] = [];

        // One reference of an expensive definition: allowed.
        let one = get_field_like(col("exp"), "x");
        assert!(inliner.pinned([&one], no_others, Free).is_empty());
        // The same reference when another consumer keeps the column: refused,
        // unless the caller accepts expensive duplicates.
        assert!(!inliner.pinned([&one], [&col("exp")], Cheap).is_empty());
        assert!(inliner.pinned([&one], [&col("exp")], Expensive).is_empty());
        // Two references in one expression: refused unless allowed.
        let two = col("exp")
            .is_not_null()
            .and(get_field_like(col("exp"), "x"));
        assert!(!inliner.pinned([&two], no_others, Cheap).is_empty());
        assert!(inliner.pinned([&two], no_others, Expensive).is_empty());
        // References are counted across all inlined expressions.
        assert!(
            !inliner
                .pinned([&col("exp"), &col("exp")], no_others, Cheap)
                .is_empty()
        );
        // Cheap definitions follow `max_cost`.
        let cheap = col("cheap").gt(lit(5));
        assert!(inliner.pinned([&cheap], [&col("cheap")], Cheap).is_empty());
        assert!(!inliner.pinned([&cheap], [&col("cheap")], Free).is_empty());
        // Free definitions are always allowed.
        assert!(
            inliner
                .pinned([&col("a"), &col("a")], [&col("a")], Free)
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
        let max = DuplicationCost::Expensive;
        assert!(inliner.pinned([&col("r")], no_others, max).is_empty());
        assert!(
            !inliner
                .pinned([&(col("r") + col("r"))], no_others, max)
                .is_empty()
        );
        assert!(!inliner.pinned([&col("r")], [&col("r")], max).is_empty());
    }
}
