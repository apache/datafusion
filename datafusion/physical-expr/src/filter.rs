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

//! [`PhysicalFilter`]: a filter that an operator applies to its rows.
//!
//! Many operators (`FilterExec`, file scans, joins) apply a boolean
//! [`PhysicalExpr`] as a filter. A [`PhysicalFilter`] holds that filter as an
//! ordered list of [`FilterConjunct`]s. Each conjunct carries the properties
//! that a consumer needs to decide how to apply it (for example, whether it
//! is required for correctness).
//!
//! A [`PhysicalFilter`] is *not* a [`PhysicalExpr`]. Use
//! [`PhysicalFilter::to_expr`] to get one expression (the `AND` of all
//! conjuncts) for code that only accepts expressions.

use std::fmt;
use std::sync::Arc;

use datafusion_common::Result;

use crate::PhysicalExpr;
use crate::utils::{conjunction, conjunction_opt, split_conjunction};

/// One conjunct of a [`PhysicalFilter`].
///
/// A row passes the filter only if it passes all *required* conjuncts. A
/// consumer can skip an *optional* conjunct (for example, a dynamic filter
/// from a hash join) without an effect on the result, because another
/// operator removes the same rows again.
#[derive(Debug, Clone)]
pub struct FilterConjunct {
    expr: Arc<dyn PhysicalExpr>,
    optional: bool,
}

impl FilterConjunct {
    /// A conjunct that the consumer must apply.
    pub fn required(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            expr,
            optional: false,
        }
    }

    /// A conjunct that the consumer can skip without an effect on the result.
    pub fn optional(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            expr,
            optional: true,
        }
    }

    /// The boolean expression of this conjunct.
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// Consume this conjunct and return its expression.
    pub fn into_expr(self) -> Arc<dyn PhysicalExpr> {
        self.expr
    }

    /// `true` if a consumer can skip this conjunct.
    pub fn is_optional(&self) -> bool {
        self.optional
    }

    /// Replace the expression (for example, after a column remap) and keep
    /// all other properties.
    pub fn with_expr(self, expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr, ..self }
    }
}

impl From<Arc<dyn PhysicalExpr>> for FilterConjunct {
    fn from(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self::required(expr)
    }
}

impl fmt::Display for FilterConjunct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)
    }
}

/// A filter: an ordered list of [`FilterConjunct`]s. A row passes the filter
/// when it passes all conjuncts.
///
/// The order is the order in which the filter was built. Consumers can
/// change the evaluation order.
///
/// An empty filter lets all rows pass.
#[derive(Debug, Clone, Default)]
pub struct PhysicalFilter {
    conjuncts: Vec<FilterConjunct>,
}

impl PhysicalFilter {
    /// Create a filter from a list of conjuncts.
    pub fn new(conjuncts: impl IntoIterator<Item = FilterConjunct>) -> Self {
        Self {
            conjuncts: conjuncts.into_iter().collect(),
        }
    }

    /// Adapter: a filter with `expr` as its single *required* conjunct.
    ///
    /// The expression is not split, so [`Self::to_expr`] returns `expr`
    /// unchanged. Use [`Self::split`] to get one conjunct for each term of
    /// the root `AND` chain.
    pub fn from_expr(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self::new([FilterConjunct::required(expr)])
    }

    /// A filter with one *required* conjunct for each term of the root `AND`
    /// chain of `expr`.
    pub fn split(expr: &Arc<dyn PhysicalExpr>) -> Self {
        Self::new(
            split_conjunction(expr)
                .into_iter()
                .map(|e| FilterConjunct::required(Arc::clone(e))),
        )
    }

    /// All conjuncts, in order.
    pub fn conjuncts(&self) -> &[FilterConjunct] {
        &self.conjuncts
    }

    /// Consume the filter and return its conjuncts.
    pub fn into_conjuncts(self) -> Vec<FilterConjunct> {
        self.conjuncts
    }

    /// `true` if the filter has no conjuncts (all rows pass).
    pub fn is_empty(&self) -> bool {
        self.conjuncts.is_empty()
    }

    /// The required conjuncts, in order.
    pub fn required(&self) -> impl Iterator<Item = &FilterConjunct> {
        self.conjuncts.iter().filter(|c| !c.is_optional())
    }

    /// The optional conjuncts, in order.
    pub fn optional(&self) -> impl Iterator<Item = &FilterConjunct> {
        self.conjuncts.iter().filter(|c| c.is_optional())
    }

    /// Add conjuncts at the end.
    pub fn extend(&mut self, conjuncts: impl IntoIterator<Item = FilterConjunct>) {
        self.conjuncts.extend(conjuncts);
    }

    /// Apply `f` to the expression of each conjunct and keep all other
    /// properties (for example, to remap columns to a different schema).
    pub fn try_map_exprs(
        self,
        mut f: impl FnMut(Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>>,
    ) -> Result<Self> {
        let conjuncts = self
            .conjuncts
            .into_iter()
            .map(|c| {
                let expr = f(Arc::clone(&c.expr))?;
                Ok(c.with_expr(expr))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { conjuncts })
    }

    /// The `AND` of all conjuncts, or `None` if the filter is empty.
    ///
    /// This is the expression that gives the same result as the filter when
    /// the consumer applies all conjuncts.
    pub fn to_expr_opt(&self) -> Option<Arc<dyn PhysicalExpr>> {
        conjunction_opt(self.conjuncts.iter().map(|c| Arc::clone(&c.expr)))
    }

    /// The `AND` of all conjuncts, or `true` if the filter is empty.
    pub fn to_expr(&self) -> Arc<dyn PhysicalExpr> {
        conjunction(self.conjuncts.iter().map(|c| Arc::clone(&c.expr)))
    }

    /// One optional flag for each term of the root `AND` chain of
    /// [`Self::to_expr`] (the terms of [`split_conjunction`]), in order.
    ///
    /// The optional flag of a conjunct applies to each of its terms: if a
    /// consumer can skip `a AND b`, it can also skip `a` or `b`. Use
    /// [`Self::from_split_flags`] to rebuild the filter, for example after
    /// serialization.
    pub fn split_optional_flags(&self) -> Vec<bool> {
        self.conjuncts
            .iter()
            .flat_map(|c| {
                std::iter::repeat_n(c.is_optional(), split_conjunction(&c.expr).len())
            })
            .collect()
    }

    /// The inverse of [`Self::split_optional_flags`]: one conjunct for each
    /// term of the root `AND` chain of `expr`, with the flag at the same
    /// position.
    ///
    /// If `flags` is empty, or its length is not the number of terms, the
    /// result is [`Self::from_expr`]: all terms are required. Thus a missing
    /// or damaged flag list makes the filter stricter, never less strict.
    pub fn from_split_flags(expr: Arc<dyn PhysicalExpr>, flags: &[bool]) -> Self {
        let terms = split_conjunction(&expr);
        if flags.is_empty() || flags.len() != terms.len() {
            return Self::from_expr(expr);
        }
        Self::new(terms.into_iter().zip(flags).map(|(term, optional)| {
            let term = Arc::clone(term);
            if *optional {
                FilterConjunct::optional(term)
            } else {
                FilterConjunct::required(term)
            }
        }))
    }

    /// The `AND` of the required conjuncts, or `true` if there are none.
    ///
    /// Use this expression for guarantees that must hold for every output
    /// row (for example, equivalence classes and constants).
    pub fn required_expr(&self) -> Arc<dyn PhysicalExpr> {
        conjunction(self.required().map(|c| Arc::clone(&c.expr)))
    }
}

impl From<Arc<dyn PhysicalExpr>> for PhysicalFilter {
    fn from(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self::from_expr(expr)
    }
}

impl FromIterator<FilterConjunct> for PhysicalFilter {
    fn from_iter<T: IntoIterator<Item = FilterConjunct>>(iter: T) -> Self {
        Self::new(iter)
    }
}

impl fmt::Display for PhysicalFilter {
    /// Displays the same text as [`Self::to_expr`], so that plans do not
    /// change when an operator stores a [`PhysicalFilter`].
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_expr())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{BinaryExpr, col};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::Operator;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("c", DataType::Boolean, true),
        ])
    }

    #[test]
    fn from_expr_is_identity() -> Result<()> {
        let schema = schema();
        let a_and_b: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            col("a", &schema)?,
            Operator::And,
            col("b", &schema)?,
        ));
        let filter = PhysicalFilter::from_expr(Arc::clone(&a_and_b));
        assert_eq!(filter.conjuncts().len(), 1);
        assert!(Arc::ptr_eq(&filter.to_expr(), &a_and_b));
        assert_eq!(PhysicalFilter::split(&a_and_b).conjuncts().len(), 2);
        Ok(())
    }

    #[test]
    fn optional_conjuncts() -> Result<()> {
        let schema = schema();
        let [a, b, c] = ["a", "b", "c"].map(|n| col(n, &schema).unwrap());
        let filter = PhysicalFilter::new([
            FilterConjunct::required(Arc::clone(&a)),
            FilterConjunct::optional(Arc::clone(&b)),
            FilterConjunct::required(Arc::clone(&c)),
        ]);
        assert_eq!(filter.to_string(), "a@0 AND b@1 AND c@2");
        assert_eq!(filter.required_expr().to_string(), "a@0 AND c@2");
        let optional: Vec<_> = filter.optional().map(|c| c.to_string()).collect();
        assert_eq!(optional, vec!["b@1"]);

        // Remapping keeps the optional flag.
        let remapped = filter.try_map_exprs(Ok)?;
        assert_eq!(remapped.optional().count(), 1);
        Ok(())
    }

    #[test]
    fn split_flags_round_trip() -> Result<()> {
        let schema = schema();
        let [a, b, c] = ["a", "b", "c"].map(|n| col(n, &schema).unwrap());
        let b_and_c: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&b),
            Operator::And,
            Arc::clone(&c),
        ));
        let filter = PhysicalFilter::new([
            FilterConjunct::required(Arc::clone(&a)),
            FilterConjunct::optional(b_and_c),
        ]);
        // The flag of the optional conjunct `b AND c` applies to each term.
        let flags = filter.split_optional_flags();
        assert_eq!(flags, vec![false, true, true]);

        let decoded = PhysicalFilter::from_split_flags(filter.to_expr(), &flags);
        let conjuncts: Vec<_> = decoded
            .conjuncts()
            .iter()
            .map(|c| (c.to_string(), c.is_optional()))
            .collect();
        assert_eq!(
            conjuncts,
            vec![
                ("a@0".to_string(), false),
                ("b@1".to_string(), true),
                ("c@2".to_string(), true),
            ]
        );
        assert_eq!(decoded.required_expr().to_string(), "a@0");

        // No flags or a wrong number of flags: all terms are required.
        for flags in [vec![], vec![true, true]] {
            let decoded = PhysicalFilter::from_split_flags(filter.to_expr(), &flags);
            assert_eq!(decoded.optional().count(), 0);
            assert_eq!(decoded.to_string(), "a@0 AND b@1 AND c@2");
        }
        Ok(())
    }
}
