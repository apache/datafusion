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

//! Reorder conjunctive (`AND`) predicates so that cheap predicates run before
//! expensive ones.
//!
//! DataFusion's `AND` evaluator short-circuits the right-hand side when the
//! left-hand side keeps few rows, so leading with a cheap predicate shrinks
//! the batch that expensive ones see.
//!
//! The cost of evaluating a predicate is assessed with a simple, conservative
//! heuristic: we define an allow-list of cheap operations, and consider an
//! expression to be cheap if it consists ONLY of cheap operations; everything
//! else is considered expensive. The sort of stable, so order within each
//! class is preserved.
//!
//! This reordering scheme is intentionally simple; many enhancements are
//! possible (e.g., consider both cost and selectivity, build a more complex
//! cost model, add estimated evaluation cost for individual UDFs).

use datafusion_common::tree_node::TreeNode;
use datafusion_expr::{BinaryExpr, Expr, Operator};

/// Stable partition of `predicates`: cheap first, then expensive.
///
/// Returns `(predicates, changed)`. When `changed` is `false` the input was
/// already cheap-first and the caller can skip rebuilding the conjunction.
pub(crate) fn reorder_predicates(predicates: Vec<Expr>) -> (Vec<Expr>, bool) {
    if predicates.len() <= 1 {
        return (predicates, false);
    }

    // Volatile predicates may have observable side-effects and reordering
    // conjuncts can change how many times they evaluate.  Preserve user order
    // if any predicate contains a volatile expression.
    if predicates.iter().any(Expr::is_volatile) {
        return (predicates, false);
    }

    let classes: Vec<bool> = predicates.iter().map(is_cheap_predicate).collect();

    // A reorder is needed iff an expensive predicate precedes a cheap one
    let needs_reorder = classes.windows(2).any(|w| !w[0] && w[1]);
    if !needs_reorder {
        return (predicates, false);
    }

    let mut cheap = Vec::with_capacity(predicates.len());
    let mut expensive = Vec::new();
    for (p, is_cheap) in predicates.into_iter().zip(classes) {
        if is_cheap {
            cheap.push(p);
        } else {
            expensive.push(p);
        }
    }
    cheap.extend(expensive);
    (cheap, true)
}

/// Returns true if every node in `expr`'s tree is cheap.
fn is_cheap_predicate(expr: &Expr) -> bool {
    !expr
        .exists(|node| Ok(!is_cheap_node(node)))
        .expect("is_cheap_node is infallible")
}

/// Returns true if `expr` is itself cheap.
///
/// We use a simple, conservative heuristic to determine if an expression is
/// cheap to evaluate: we enumerate known-cheap operations (e.g., equality
/// comparisons, negations, casts), and consider anything outside this list to
/// be expensive. New/unrecognized expressions therefore default to being
/// expensive.
fn is_cheap_node(expr: &Expr) -> bool {
    match expr {
        // Direct reads and literals.
        Expr::Column(_)
        | Expr::Literal(_, _)
        | Expr::ScalarVariable(_, _)
        | Expr::Placeholder(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::LambdaVariable(_)
        // Wrappers; children are walked separately by `is_cheap_predicate`.
        | Expr::Alias(_)
        // Single-row unary predicates and arithmetic negation.
        | Expr::Not(_)
        | Expr::Negative(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_)
        // Composite cheap forms; child expressions are walked separately.
        | Expr::Between(_)
        | Expr::Case(_)
        | Expr::Cast(_)
        | Expr::TryCast(_)
        | Expr::InList(_) => true,
        // BinaryExpr is cheap unless the operator is LIKE or regexp matching.
        Expr::BinaryExpr(BinaryExpr { op, .. }) => !matches!(
            op,
            Operator::LikeMatch
                | Operator::ILikeMatch
                | Operator::NotLikeMatch
                | Operator::NotILikeMatch
                | Operator::RegexMatch
                | Operator::RegexIMatch
                | Operator::RegexNotMatch
                | Operator::RegexNotIMatch
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::{col, lit};

    #[test]
    fn like_predicate_moves_after_equality() {
        let cheap = col("a").eq(lit(1));
        let expensive = col("b").like(lit("%foo%"));
        let (out, changed) = reorder_predicates(vec![expensive.clone(), cheap.clone()]);
        assert_eq!(out, vec![cheap, expensive]);
        assert!(changed);
    }

    #[test]
    fn order_among_cheap_predicates_is_preserved() {
        let p1 = col("a").eq(lit(1));
        let p2 = col("b").eq(lit(2));
        let p3 = col("c").eq(lit(3));
        let input = vec![p1.clone(), p2.clone(), p3.clone()];
        let (out, changed) = reorder_predicates(input.clone());
        assert_eq!(out, input);
        assert!(!changed);
    }

    #[test]
    fn order_among_expensive_predicates_is_preserved() {
        let p1 = col("a").like(lit("%a%"));
        let p2 = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("b")),
            Operator::RegexMatch,
            Box::new(lit("foo")),
        ));
        let p3 = col("c").like(lit("%c%"));
        let input = vec![p1.clone(), p2.clone(), p3.clone()];
        let (out, changed) = reorder_predicates(input.clone());
        assert_eq!(out, input);
        assert!(!changed);
    }

    #[test]
    fn already_cheap_first_reports_no_change() {
        let cheap = col("a").eq(lit(1));
        let expensive = col("b").like(lit("%a%"));
        let input = vec![cheap.clone(), expensive.clone()];
        let (out, changed) = reorder_predicates(input.clone());
        assert_eq!(out, input);
        assert!(!changed);
    }

    #[test]
    fn nested_expensive_under_not_is_expensive() {
        // The top node is `Not`, which is on the cheap allow-list. The walk
        // must descend into the `Like` to flag this predicate as expensive.
        let cheap = col("a").eq(lit(1));
        let nested = Expr::Not(Box::new(col("b").like(lit("%foo%"))));
        let (out, changed) = reorder_predicates(vec![nested.clone(), cheap.clone()]);
        assert_eq!(out, vec![cheap, nested]);
        assert!(changed);
    }
}
