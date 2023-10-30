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

use std::{ops::Neg, sync::Arc};

use crate::expressions::Column;
use crate::utils::get_indices_of_matching_sort_exprs_with_order_eq;
use crate::{
    EquivalenceProperties, OrderingEquivalenceProperties, PhysicalExpr, PhysicalSortExpr,
};

use arrow_schema::SortOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, VisitRecursion};
use datafusion_common::Result;

use itertools::Itertools;

/// To propagate [`SortOptions`] across the [`PhysicalExpr`], it is insufficient
/// to simply use `Option<SortOptions>`: There must be a differentiation between
/// unordered columns and literal values, since literals may not break the ordering
/// when they are used as a child of some binary expression when the other child has
/// some ordering. On the other hand, unordered columns cannot maintain ordering when
/// they take part in such operations.
///
/// Example: ((a_ordered + b_unordered) + c_ordered) expression cannot end up with
/// sorted data; however the ((a_ordered + 999) + c_ordered) expression can. Therefore,
/// we need two different variants for literals and unordered columns as literals are
/// often more ordering-friendly under most mathematical operations.
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum SortProperties {
    /// Use the ordinary [`SortOptions`] struct to represent ordered data:
    Ordered(SortOptions),
    // This alternative represents unordered data:
    Unordered,
    // Singleton is used for single-valued literal numbers:
    Singleton,
}

impl SortProperties {
    pub fn add(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Singleton, _) => *rhs,
            (_, Self::Singleton) => *self,
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.descending == rhs.descending =>
            {
                Self::Ordered(SortOptions {
                    descending: lhs.descending,
                    nulls_first: lhs.nulls_first || rhs.nulls_first,
                })
            }
            _ => Self::Unordered,
        }
    }

    pub fn sub(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Singleton, Self::Singleton) => Self::Singleton,
            (Self::Singleton, Self::Ordered(rhs)) => Self::Ordered(SortOptions {
                descending: !rhs.descending,
                nulls_first: rhs.nulls_first,
            }),
            (_, Self::Singleton) => *self,
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.descending != rhs.descending =>
            {
                Self::Ordered(SortOptions {
                    descending: lhs.descending,
                    nulls_first: lhs.nulls_first || rhs.nulls_first,
                })
            }
            _ => Self::Unordered,
        }
    }

    pub fn gt_or_gteq(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Singleton, Self::Ordered(rhs)) => Self::Ordered(SortOptions {
                descending: !rhs.descending,
                nulls_first: rhs.nulls_first,
            }),
            (_, Self::Singleton) => *self,
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.descending != rhs.descending =>
            {
                *self
            }
            _ => Self::Unordered,
        }
    }

    pub fn and(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.descending == rhs.descending =>
            {
                Self::Ordered(SortOptions {
                    descending: lhs.descending,
                    nulls_first: lhs.nulls_first || rhs.nulls_first,
                })
            }
            (Self::Ordered(opt), Self::Singleton)
            | (Self::Singleton, Self::Ordered(opt)) => Self::Ordered(SortOptions {
                descending: opt.descending,
                nulls_first: opt.nulls_first,
            }),
            (Self::Singleton, Self::Singleton) => Self::Singleton,
            _ => Self::Unordered,
        }
    }
}

impl Neg for SortProperties {
    type Output = Self;

    fn neg(self) -> Self::Output {
        match self {
            SortProperties::Ordered(SortOptions {
                descending,
                nulls_first,
            }) => SortProperties::Ordered(SortOptions {
                descending: !descending,
                nulls_first,
            }),
            SortProperties::Singleton => SortProperties::Singleton,
            SortProperties::Unordered => SortProperties::Unordered,
        }
    }
}

/// The `ExprOrdering` struct is designed to aid in the determination of ordering (represented
/// by [`SortProperties`]) for a given [`PhysicalExpr`]. When analyzing the orderings
/// of a [`PhysicalExpr`], the process begins by assigning the ordering of its leaf nodes.
/// By propagating these leaf node orderings upwards in the expression tree, the overall
/// ordering of the entire [`PhysicalExpr`] can be derived.
///
/// This struct holds the necessary state information for each expression in the [`PhysicalExpr`].
/// It encapsulates the orderings (`state`) associated with the expression (`expr`), and
/// orderings of the children expressions (`children_states`). The [`ExprOrdering`] of a parent
/// expression is determined based on the [`ExprOrdering`] states of its children expressions.
#[derive(Debug)]
pub struct ExprOrdering {
    pub expr: Arc<dyn PhysicalExpr>,
    pub state: SortProperties,
    pub children_states: Vec<SortProperties>,
}

impl ExprOrdering {
    /// Creates a new [`ExprOrdering`] having [`SortProperties::Unordered`] order, and
    /// [`SortProperties::Unordered`] orders for its child expressions.
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        let size = expr.children().len();
        Self {
            expr,
            state: SortProperties::Unordered,
            children_states: vec![SortProperties::Unordered; size],
        }
    }

    /// Updates the [`ExprOrdering`]'s children order with the given orderings.
    pub fn with_new_children(mut self, children_states: Vec<SortProperties>) -> Self {
        assert_eq!(self.children_states.len(), children_states.len());
        self.children_states = children_states;
        self
    }

    /// Creates new [`ExprOrdering`]'s for each child of an expression.
    pub fn new_expr_orderings_for_children_exprs(&self) -> Vec<ExprOrdering> {
        self.expr
            .children()
            .into_iter()
            .map(ExprOrdering::new)
            .collect()
    }
}

impl TreeNode for ExprOrdering {
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        for child in self.new_expr_orderings_for_children_exprs() {
            match op(&child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }
        Ok(VisitRecursion::Continue)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        if self.children_states.is_empty() {
            Ok(self)
        } else {
            let child_expr_orderings = self.new_expr_orderings_for_children_exprs();
            Ok(self.with_new_children(
                child_expr_orderings
                    .into_iter()
                    // updates the children states after this transformation
                    .map(transform)
                    // extract the state (order) information of the children
                    .map_ok(|c| c.state)
                    .collect::<Result<Vec<_>>>()?,
            ))
            // after the mapping of children, the children's states are set for
            // the current node, and the last step of transform_up calculates
            // the current node's state with update_ordering().
        }
    }
}

/// Calculates the [`SortProperties`] of a given [`ExprOrdering`] node.
/// The node is either a leaf node, or an intermediate node:
/// - If it is a leaf node, the children states are `None`. We directly find
/// the order of the node by looking at the given sort expression and equivalence
/// properties if it is a `Column` leaf, or we mark it as unordered. In the case
/// of a `Literal` leaf, we mark it as singleton so that it can cooperate with
/// some ordered columns at the upper steps.
/// - If it is an intermediate node, the children states matter. Each `PhysicalExpr`
/// and operator has its own rules about how to propagate the children orderings.
/// However, before the children order propagation, it is checked that whether
/// the intermediate node can be directly matched with the sort expression. If there
/// is a match, the sort expression emerges at that node immediately, discarding
/// the order coming from the children.
pub fn update_ordering(
    mut node: ExprOrdering,
    sort_expr: &PhysicalSortExpr,
    equal_properties: &EquivalenceProperties,
    ordering_equal_properties: &OrderingEquivalenceProperties,
) -> Result<Transformed<ExprOrdering>> {
    // If we can directly match a sort expr with the current node, we can set
    // its state and return early.
    // TODO: If there is a PhysicalExpr other than a Column at this node (e.g.
    //       a BinaryExpr like a + b), and there is an ordering equivalence of
    //       it (let's say like c + d), we actually can find it at this step.
    if sort_expr.expr.eq(&node.expr) {
        node.state = SortProperties::Ordered(sort_expr.options);
        return Ok(Transformed::Yes(node));
    }

    if !node.expr.children().is_empty() {
        // We have an intermediate (non-leaf) node, account for its children:
        node.state = node.expr.get_ordering(&node.children_states);
    } else if let Some(column) = node.expr.as_any().downcast_ref::<Column>() {
        // We have a Column, which is one of the two possible leaf node types:
        node.state = get_indices_of_matching_sort_exprs_with_order_eq(
            &[sort_expr.clone()],
            &[column.clone()],
            equal_properties,
            ordering_equal_properties,
        )
        .map(|(sort_options, _)| {
            SortProperties::Ordered(SortOptions {
                descending: sort_options[0].descending,
                nulls_first: sort_options[0].nulls_first,
            })
        })
        .unwrap_or(SortProperties::Unordered);
    } else {
        // We have a Literal, which is the other possible leaf node type:
        node.state = node.expr.get_ordering(&[]);
    }
    Ok(Transformed::Yes(node))
}
