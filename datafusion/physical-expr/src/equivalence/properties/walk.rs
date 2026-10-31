// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use super::EquivalenceProperties;
use crate::equivalence::ordering::is_partial_const;
use crate::expressions::{Column, Literal};
use crate::{LexOrdering, PhysicalExpr, PhysicalSortExpr};

use arrow::compute::SortOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{HashSet, Result};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_physical_expr_common::sort_expr::options_compatible;
use datafusion_physical_expr_common::utils::ExprPropertiesNode;

use itertools::Itertools;

/// The state of one walk along a sequence of sort keys, such as the keys of a
/// required ordering.
///
/// A sort key only matters among the rows that tie on the keys before it. So
/// once a key is satisfied, the walk *pins* it: for the rest of the walk, the
/// key has one value, like a constant. An expression is *fixed* if it has one
/// value among those rows: it is a literal, a constant, pinned, a leaf that
/// reports itself as a singleton (such as an uncorrelated scalar subquery), or
/// not volatile and computed only from fixed expressions. The *head* of an
/// ordering is its first expression that is not fixed; an ordering whose
/// expressions are all fixed has no head. With orderings `[a, b]` and
/// `[a, c]`, the requirement `[a, c]` is satisfied, because pinning `a` makes
/// `c` the head of the second ordering.
///
/// Pins live in the walk; the equivalence properties it reads do not change.
pub(super) struct SortKeyWalk<'a> {
    props: &'a EquivalenceProperties,
    /// Pinned expressions. Pinning a member of an equivalence class pins all
    /// of its members.
    pinned: HashSet<Arc<dyn PhysicalExpr>>,
    /// Orderings that hold among the rows that tie on the pinned expressions,
    /// found by [`EquivalenceProperties::implied_ordering`] as the walk goes.
    implied: Vec<LexOrdering>,
    /// For each ordering (the normal orderings of `props`, then `implied`),
    /// the index of its head. Empty until the first pin, so that a walk with
    /// nothing pinned allocates nothing; until then every head is at index 0.
    head_idxs: Vec<usize>,
}

impl<'a> SortKeyWalk<'a> {
    pub(super) fn new(props: &'a EquivalenceProperties) -> Self {
        Self {
            props,
            pinned: HashSet::new(),
            implied: vec![],
            head_idxs: vec![],
        }
    }

    /// Returns how many leading keys (in normal form) the existing orderings
    /// satisfy, pinning each satisfied key for the keys after it. A key
    /// without sort options only needs to be ordered in some direction.
    pub(super) fn sorted_prefix_len<'k>(
        mut self,
        keys: impl ExactSizeIterator<Item = (&'k Arc<dyn PhysicalExpr>, Option<SortOptions>)>,
    ) -> Result<usize> {
        let schema = self.props.schema();
        let len = keys.len();
        for (idx, (expr, options)) in keys.enumerate() {
            let satisfy = match self.expr_properties(Arc::clone(expr)).sort_properties {
                SortProperties::Ordered(existing) => options.is_none_or(|options| {
                    let nullable = expr.nullable(schema).unwrap_or(true);
                    // `existing` comes from the first head that `expr` (or,
                    // for a computed `expr`, its arguments) matches; another
                    // head may be `expr` itself in the required direction.
                    options_compatible(&existing, &options, nullable)
                        || self.heads().any(|head| {
                            options_compatible(&head.options, &options, nullable)
                                && head.expr.eq(expr)
                        })
                }),
                // Singleton expressions satisfy any ordering.
                SortProperties::Singleton => true,
                SortProperties::Unordered => false,
            };
            if !satisfy {
                return Ok(idx);
            }
            if idx + 1 < len {
                self.pin_key(Arc::clone(expr))?;
            }
        }
        Ok(len)
    }

    /// Pins a satisfied sort key, and the sub-expressions it pins down: if an
    /// expression is strictly order-preserving, equal outputs imply equal
    /// values of its ordered children, so within a tie group of the key those
    /// children have one value as well. For example, if data is sorted by
    /// `[a, b]`, the requirement `[CAST(a AS BIGINT) ASC, b ASC]` is
    /// satisfied: `a` has one value within each group of equal
    /// `CAST(a AS BIGINT)` values, and hence `b` is sorted within each such
    /// group.
    fn pin_key(&mut self, key: Arc<dyn PhysicalExpr>) -> Result<()> {
        let mut stack = vec![key];
        while let Some(expr) = stack.pop() {
            let properties = self.expr_properties(Arc::clone(&expr));
            if properties.strictly_order_preserving {
                for child in expr.children() {
                    let child_properties = self.expr_properties(Arc::clone(child));
                    if matches!(
                        child_properties.sort_properties,
                        SortProperties::Ordered(_)
                    ) {
                        stack.push(Arc::clone(child));
                    }
                }
            }
            self.pin(expr)?;
        }
        Ok(())
    }

    /// Pins `expr` and the rest of its equivalence class (but, unlike
    /// [`Self::pin_key`], not its children), then moves the head of each
    /// ordering past the expressions that are now fixed, and adds the
    /// orderings that the heads imply, until no new ordering is found.
    pub(super) fn pin(&mut self, expr: Arc<dyn PhysicalExpr>) -> Result<()> {
        match self.props.eq_group.get_equivalence_class(&expr) {
            Some(class) => self.pinned.extend(class.iter().cloned()),
            None => {
                self.pinned.insert(expr);
            }
        }
        let ordering_count = self.props.oeq_cache.normal_cls.len() + self.implied.len();
        self.head_idxs.resize(ordering_count, 0);
        // Pins do not change below, so after the first round only the
        // orderings found in the previous round can imply new ones.
        let mut first = 0;
        loop {
            let count = self.head_idxs.len();
            let mut found = vec![];
            for idx in first..count {
                let ordering = self.ordering(idx);
                let mut head_idx = self.head_idxs[idx];
                while head_idx < ordering.len() && self.is_fixed(&ordering[head_idx].expr)
                {
                    head_idx += 1;
                }
                if let Some(head) = ordering.get(head_idx) {
                    let rest = ordering[head_idx + 1..]
                        .iter()
                        .filter(|sort_expr| !self.is_fixed(&sort_expr.expr));
                    found.extend(self.props.implied_ordering(head, rest)?);
                }
                self.head_idxs[idx] = head_idx;
            }
            for ordering in found.into_iter().filter_map(LexOrdering::new) {
                let known =
                    (0..self.head_idxs.len()).any(|idx| *self.ordering(idx) == ordering);
                if !known {
                    self.implied.push(ordering);
                    self.head_idxs.push(0);
                }
            }
            // No new ordering:
            if self.head_idxs.len() == count {
                return Ok(());
            }
            first = count;
        }
    }

    /// Returns the properties of `expr` among the rows that tie on the
    /// pinned expressions.
    pub(super) fn expr_properties(&self, expr: Arc<dyn PhysicalExpr>) -> ExprProperties {
        ExprPropertiesNode::new_unknown(expr)
            .transform_up(|node| self.update_properties(node))
            .data()
            .map(|node| node.data)
            .unwrap_or_else(|_| ExprProperties::new_unknown())
    }

    /// Calculates the properties of a given [`ExprPropertiesNode`], whose
    /// children are already done.
    ///
    /// - An intermediate node takes its order and range from its children,
    ///   through [`PhysicalExpr::get_properties`]. A `Literal` leaf is a
    ///   singleton with a point range. A `Column` leaf is unordered, with an
    ///   unbounded range of its data type.
    /// - Then, whatever its children said: a node that is fixed or partially
    ///   constant is a singleton, and a node that heads an ordering is ordered
    ///   like the first ordering it heads.
    fn update_properties(
        &self,
        mut node: ExprPropertiesNode,
    ) -> Result<Transformed<ExprPropertiesNode>> {
        // First, try to gather the information from the children:
        if !node.expr.children().is_empty() {
            // We have an intermediate (non-leaf) node, account for its children:
            let children_props =
                node.children.iter().map(|c| c.data.clone()).collect_vec();
            // Properties that cannot be derived from the children, for example
            // of a function whose input may be outside its domain, are
            // unknown. The ordering of the node itself can still be known, so
            // do not fail here, and keep the type of its range so that parents
            // can derive from it.
            node.data = node.expr.get_properties(&children_props).or_else(|_| {
                Interval::make_unbounded(&node.expr.data_type(self.props.schema())?)
                    .map(|range| ExprProperties::new_unknown().with_range(range))
            })?;
        } else if node.expr.is::<Literal>() {
            // We have a Literal, which is one of the two possible leaf node types:
            node.data = node.expr.get_properties(&[])?;
        } else if node.expr.is::<Column>() {
            // We have a Column, which is the other possible leaf node type:
            node.data.range =
                Interval::make_unbounded(&node.expr.data_type(self.props.schema())?)?;
            // A column is the identity mapping of itself, which is trivially
            // strict:
            node.data.strictly_order_preserving = true;
        }
        // Now, check what we know about orderings. A whole-partition window
        // such as `SUM(x) OVER (PARTITION BY k)` adds `[k, w]` in all four
        // directions of `w` (see `OrderingEquivalenceClass::is_expr_partial_const`).
        // Once `k` is pinned, `w` heads orderings in opposite directions, so it
        // has one value among the rows that tie on `k`:
        let normal_expr = self.props.eq_group.normalize_expr(Arc::clone(&node.expr));
        if self.is_fixed(&normal_expr) || is_partial_const(self.heads(), &normal_expr) {
            node.data.sort_properties = SortProperties::Singleton;
        } else if let Some(head) = self.heads().find(|head| head.expr.eq(&normal_expr)) {
            node.data.sort_properties = SortProperties::Ordered(head.options);
        }
        Ok(Transformed::yes(node))
    }

    /// Whether `expr` is fixed (see [`SortKeyWalk`]). Callers pass `expr` in
    /// normal form. Pins are stored as given, so a pinned expression that is
    /// not in normal form is found only through its equivalence class, whose
    /// members are all pinned.
    fn is_fixed(&self, expr: &Arc<dyn PhysicalExpr>) -> bool {
        if expr.is::<Literal>() || self.pinned.contains(expr) {
            return true;
        }
        let class = self.props.eq_group.get_equivalence_class(expr);
        if class.is_some_and(|class| class.constant.is_some()) {
            return true;
        }
        // A volatile expression can change from row to row, even when all of
        // its inputs are fixed:
        if expr.is_volatile_node() {
            return false;
        }
        let children = expr.children();
        if children.is_empty() {
            // A leaf that reads no column, such as an uncorrelated scalar
            // subquery, is fixed if it reports itself as a singleton:
            return expr
                .get_properties(&[])
                .is_ok_and(|props| props.sort_properties == SortProperties::Singleton);
        }
        children.into_iter().all(|child| self.is_fixed(child))
    }

    /// The ordering at `idx`: the normal orderings of `props`, then
    /// `implied`.
    fn ordering(&self, idx: usize) -> &LexOrdering {
        let normal = &self.props.oeq_cache.normal_cls;
        normal
            .get(idx)
            .unwrap_or_else(|| &self.implied[idx - normal.len()])
    }

    /// The head of each ordering that has one.
    fn heads(&self) -> impl Iterator<Item = &PhysicalSortExpr> + Clone {
        let orderings = self.props.oeq_cache.normal_cls.iter().chain(&self.implied);
        orderings.enumerate().filter_map(|(idx, ordering)| {
            ordering.get(self.head_idxs.get(idx).copied().unwrap_or(0))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::EquivalenceProperties;
    use crate::expressions::{CastExpr, col};
    use crate::{PhysicalExpr, PhysicalSortExpr};

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_common::Result;

    /// Pinning a key can move the head of an ordering to an expression that
    /// is equal to a function of the expressions after it, which implies a
    /// new ordering. With ordering `[k, a, c]` and `CAST(c AS DATE) = a`,
    /// pinning `k` leaves `[a, c]`, which implies `[c]`, so `[k, c]` holds.
    #[test]
    fn ordering_found_after_pinning() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("a", DataType::Date32, true),
            Field::new("c", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let [k, a, c] = ["k", "a", "c"].map(|name| col(name, &schema).unwrap());
        let cast_c = Arc::new(CastExpr::new_with_target_field(
            Arc::clone(&c),
            Arc::new(Field::new("c", DataType::Date32, true)),
            None,
        )) as _;

        let mut properties = EquivalenceProperties::new(Arc::clone(&schema));
        properties.add_ordering(
            [&k, &a, &c].map(|expr| PhysicalSortExpr::new_default(Arc::clone(expr))),
        );
        properties.add_equal_conditions(cast_c, Arc::clone(&a))?;

        let required =
            [&k, &c].map(|expr| PhysicalSortExpr::new_default(Arc::clone(expr)));
        assert!(properties.ordering_satisfy(required)?);
        // Without `k` first, nothing is known about `c`:
        assert!(!properties.ordering_satisfy([PhysicalSortExpr::new_default(c)])?);
        Ok(())
    }

    /// An ordering found after a pin can imply another one. With ordering
    /// `[k, x, y, z]`, `x = CAST(y AS BIGINT)` and `y = CAST(z AS INT)`,
    /// pinning `k` leaves `[x, y, z]`, which implies `[y, z]`, which implies
    /// `[z]`, so `[k, z]` holds.
    #[test]
    fn chained_orderings_found_after_pinning() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Int32, true),
            Field::new("z", DataType::Int16, true),
        ]));
        let [k, x, y, z] = ["k", "x", "y", "z"].map(|name| col(name, &schema).unwrap());
        let cast = |expr: &Arc<dyn PhysicalExpr>, data_type| {
            Arc::new(CastExpr::new_with_target_field(
                Arc::clone(expr),
                Arc::new(Field::new("cast", data_type, true)),
                None,
            )) as Arc<dyn PhysicalExpr>
        };

        let mut properties = EquivalenceProperties::new(Arc::clone(&schema));
        properties.add_equal_conditions(Arc::clone(&y), cast(&z, DataType::Int32))?;
        properties.add_equal_conditions(Arc::clone(&x), cast(&y, DataType::Int64))?;
        properties.add_ordering(
            [&k, &x, &y, &z].map(|expr| PhysicalSortExpr::new_default(Arc::clone(expr))),
        );

        let required =
            [&k, &z].map(|expr| PhysicalSortExpr::new_default(Arc::clone(expr)));
        assert!(properties.ordering_satisfy(required)?);
        Ok(())
    }

    /// After pinning `k`, `a` heads three orderings, in two directions that
    /// are not opposite. The requirement `[k, a ASC NULLS FIRST]` holds by the
    /// last ordering, even though the first one heads `a` in another
    /// direction.
    #[test]
    fn any_head_in_required_direction() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("k", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]));
        let [a, k, c] = ["a", "k", "c"].map(|name| col(name, &schema).unwrap());
        let sort = |expr: &Arc<dyn PhysicalExpr>, descending| {
            PhysicalSortExpr::new(
                Arc::clone(expr),
                SortOptions {
                    descending,
                    nulls_first: true,
                },
            )
        };

        let mut properties = EquivalenceProperties::new(Arc::clone(&schema));
        properties.add_orderings([
            vec![sort(&a, true), sort(&k, false), sort(&c, false)],
            vec![sort(&k, false), sort(&a, true), sort(&c, false)],
            vec![sort(&k, false), sort(&a, false)],
        ]);

        assert!(properties.ordering_satisfy([sort(&k, false), sort(&a, false)])?);
        Ok(())
    }
}
