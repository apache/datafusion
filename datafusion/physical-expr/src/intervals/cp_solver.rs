// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Constraint propagator/solver for custom PhysicalExpr graphs.

use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::type_coercion::binary::get_result_type;
use datafusion_expr::Operator;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::{DefaultIx, StableGraph};
use petgraph::visit::{Bfs, Dfs, DfsPostOrder, EdgeRef};
use petgraph::Outgoing;

use crate::expressions::{BinaryExpr, CastExpr, Column, Literal};
use crate::intervals::interval_aritmetic::{
    apply_operator, is_operator_supported, Interval,
};
use crate::utils::{build_dag, ExprTreeNode};
use crate::PhysicalExpr;

use super::IntervalBound;

// Interval arithmetic provides a way to perform mathematical operations on
// intervals, which represent a range of possible values rather than a single
// point value. This allows for the propagation of ranges through mathematical
// operations, and can be used to compute bounds for a complicated expression.
// The key idea is that by breaking down a complicated expression into simpler
// terms, and then combining the bounds for those simpler terms, one can
// obtain bounds for the overall expression.
//
// For example, consider a mathematical expression such as x^2 + y = 4. Since
// it would be a binary tree in [PhysicalExpr] notation, this type of an
// hierarchical computation is well-suited for a graph based implementation.
// In such an implementation, an equation system f(x) = 0 is represented by a
// directed acyclic expression graph (DAEG).
//
// In order to use interval arithmetic to compute bounds for this expression,
// one would first determine intervals that represent the possible values of x
// and y. Let's say that the interval for x is [1, 2] and the interval for y
// is [-3, 1]. In the chart below, you can see how the computation takes place.
//
// This way of using interval arithmetic to compute bounds for a complex
// expression by combining the bounds for the constituent terms within the
// original expression allows us to reason about the range of possible values
// of the expression. This information later can be used in range pruning of
// the provably unnecessary parts of `RecordBatch`es.
//
// References
// 1 - Kabak, Mehmet Ozan. Analog Circuit Start-Up Behavior Analysis: An Interval
// Arithmetic Based Approach, Chapter 4. Stanford University, 2015.
// 2 - Moore, Ramon E. Interval analysis. Vol. 4. Englewood Cliffs: Prentice-Hall, 1966.
// 3 - F. Messine, "Deterministic global optimization using interval constraint
// propagation techniques," RAIRO-Operations Research, vol. 38, no. 04,
// pp. 277{293, 2004.
//
// ``` text
// Computing bounds for an expression using interval arithmetic.           Constraint propagation through a top-down evaluation of the expression
//                                                                         graph using inverse semantics.
//
//                                                                                 [-2, 5] ∩ [4, 4] = [4, 4]              [4, 4]
//             +-----+                        +-----+                                      +-----+                        +-----+
//        +----|  +  |----+              +----|  +  |----+                            +----|  +  |----+              +----|  +  |----+
//        |    |     |    |              |    |     |    |                            |    |     |    |              |    |     |    |
//        |    +-----+    |              |    +-----+    |                            |    +-----+    |              |    +-----+    |
//        |               |              |               |                            |               |              |               |
//    +-----+           +-----+      +-----+           +-----+                    +-----+           +-----+      +-----+           +-----+
//    |   2 |           |  y  |      |   2 | [1, 4]    |  y  |                    |   2 | [1, 4]    |  y  |      |   2 | [1, 4]    |  y  | [0, 1]*
//    |[.]  |           |     |      |[.]  |           |     |                    |[.]  |           |     |      |[.]  |           |     |
//    +-----+           +-----+      +-----+           +-----+                    +-----+           +-----+      +-----+           +-----+
//       |                              |                                            |              [-3, 1]         |
//       |                              |                                            |                              |
//     +---+                          +---+                                        +---+                          +---+
//     | x | [1, 2]                   | x | [1, 2]                                 | x | [1, 2]                   | x | [1, 2]
//     +---+                          +---+                                        +---+                          +---+
//
//  (a) Bottom-up evaluation: Step1 (b) Bottom up evaluation: Step2             (a) Top-down propagation: Step1 (b) Top-down propagation: Step2
//
//                                        [1 - 3, 4 + 1] = [-2, 5]                                                    [1 - 3, 4 + 1] = [-2, 5]
//             +-----+                        +-----+                                      +-----+                        +-----+
//        +----|  +  |----+              +----|  +  |----+                            +----|  +  |----+              +----|  +  |----+
//        |    |     |    |              |    |     |    |                            |    |     |    |              |    |     |    |
//        |    +-----+    |              |    +-----+    |                            |    +-----+    |              |    +-----+    |
//        |               |              |               |                            |               |              |               |
//    +-----+           +-----+      +-----+           +-----+                    +-----+           +-----+      +-----+           +-----+
//    |   2 |[1, 4]     |  y  |      |   2 |[1, 4]     |  y  |                    |   2 |[3, 4]**   |  y  |      |   2 |[1, 4]     |  y  |
//    |[.]  |           |     |      |[.]  |           |     |                    |[.]  |           |     |      |[.]  |           |     |
//    +-----+           +-----+      +-----+           +-----+                    +-----+           +-----+      +-----+           +-----+
//       |              [-3, 1]         |              [-3, 1]                       |              [0, 1]          |              [-3, 1]
//       |                              |                                            |                              |
//     +---+                          +---+                                        +---+                          +---+
//     | x | [1, 2]                   | x | [1, 2]                                 | x | [1, 2]                   | x | [sqrt(3), 2]***
//     +---+                          +---+                                        +---+                          +---+
//
//  (c) Bottom-up evaluation: Step3 (d) Bottom-up evaluation: Step4             (c) Top-down propagation: Step3  (d) Top-down propagation: Step4
//
//                                                                             * [-3, 1] ∩ ([4, 4] - [1, 4]) = [0, 1]
//                                                                             ** [1, 4] ∩ ([4, 4] - [0, 1]) = [3, 4]
//                                                                             *** [1, 2] ∩ [sqrt(3), sqrt(4)] = [sqrt(3), 2]
// ```

/// This object implements a directed acyclic expression graph (DAEG) that
/// is used to compute ranges for expressions through interval arithmetic.
#[derive(Clone)]
pub struct ExprIntervalGraph {
    graph: StableGraph<ExprIntervalGraphNode, usize>,
    root: NodeIndex,
}

impl ExprIntervalGraph {
    /// Estimate size of bytes including `Self`.
    pub fn size(&self) -> usize {
        let node_memory_usage = self.graph.node_count()
            * (std::mem::size_of::<ExprIntervalGraphNode>()
                + std::mem::size_of::<NodeIndex>());
        let edge_memory_usage = self.graph.edge_count()
            * (std::mem::size_of::<usize>() + std::mem::size_of::<NodeIndex>() * 2);

        std::mem::size_of_val(self) + node_memory_usage + edge_memory_usage
    }
}

/// This object encapsulates all possible constraint propagation results.
#[derive(PartialEq, Debug)]
pub enum PropagationResult {
    CannotPropagate,
    Infeasible,
    Success,
}

/// This is a node in the DAEG; it encapsulates a reference to the actual
/// [PhysicalExpr] as well as an interval containing expression bounds.
#[derive(Clone, Debug)]
pub struct ExprIntervalGraphNode {
    expr: Arc<dyn PhysicalExpr>,
    interval: Interval,
}

impl Display for ExprIntervalGraphNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.expr)
    }
}

impl ExprIntervalGraphNode {
    /// Constructs a new DAEG node with an [-∞, ∞] range.
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        ExprIntervalGraphNode {
            expr,
            interval: Interval::default(),
        }
    }

    /// Constructs a new DAEG node with the given range.
    pub fn new_with_interval(expr: Arc<dyn PhysicalExpr>, interval: Interval) -> Self {
        ExprIntervalGraphNode { expr, interval }
    }

    /// Get the interval object representing the range of the expression.
    pub fn interval(&self) -> &Interval {
        &self.interval
    }

    /// This function creates a DAEG node from Datafusion's [ExprTreeNode]
    /// object. Literals are created with definite, singleton intervals while
    /// any other expression starts with an indefinite interval ([-∞, ∞]).
    pub fn make_node(node: &ExprTreeNode<NodeIndex>) -> ExprIntervalGraphNode {
        let expr = node.expression().clone();
        if let Some(literal) = expr.as_any().downcast_ref::<Literal>() {
            let value = literal.value();
            let interval = Interval::new(
                IntervalBound::new(value.clone(), false),
                IntervalBound::new(value.clone(), false),
            );
            ExprIntervalGraphNode::new_with_interval(expr, interval)
        } else {
            ExprIntervalGraphNode::new(expr)
        }
    }
}

impl PartialEq for ExprIntervalGraphNode {
    fn eq(&self, other: &ExprIntervalGraphNode) -> bool {
        self.expr.eq(&other.expr)
    }
}

// This function returns the inverse operator of the given operator.
fn get_inverse_op(op: Operator) -> Operator {
    match op {
        Operator::Plus => Operator::Minus,
        Operator::Minus => Operator::Plus,
        _ => unreachable!(),
    }
}

/// This function refines intervals `left_child` and `right_child` by applying
/// constraint propagation through `parent` via operation. The main idea is
/// that we can shrink ranges of variables x and y using parent interval p.
///
/// Assuming that x,y and p has ranges [xL, xU], [yL, yU], and [pL, pU], we
/// apply the following operations:
/// - For plus operation, specifically, we would first do
///     - [xL, xU] <- ([pL, pU] - [yL, yU]) ∩ [xL, xU], and then
///     - [yL, yU] <- ([pL, pU] - [xL, xU]) ∩ [yL, yU].
/// - For minus operation, specifically, we would first do
///     - [xL, xU] <- ([yL, yU] + [pL, pU]) ∩ [xL, xU], and then
///     - [yL, yU] <- ([xL, xU] - [pL, pU]) ∩ [yL, yU].
pub fn propagate_arithmetic(
    op: &Operator,
    parent: &Interval,
    left_child: &Interval,
    right_child: &Interval,
) -> Result<(Option<Interval>, Option<Interval>)> {
    let inverse_op = get_inverse_op(*op);
    // First, propagate to the left:
    match apply_operator(&inverse_op, parent, right_child)?.intersect(left_child)? {
        // Left is feasible:
        Some(value) => {
            // Propagate to the right using the new left.
            let right = match op {
                Operator::Minus => apply_operator(op, &value, parent),
                Operator::Plus => apply_operator(&inverse_op, parent, &value),
                _ => unreachable!(),
            }?
            .intersect(right_child)?;
            // Return intervals for both children:
            Ok((Some(value), right))
        }
        // If the left child is infeasible, short-circuit.
        None => Ok((None, None)),
    }
}

/// This function provides a target parent interval for comparison operators.
/// If we have expression > 0, expression must have the range [0, ∞].
/// If we have expression < 0, expression must have the range [-∞, 0].
/// Currently, we only support strict inequalities since open/closed intervals
/// are not implemented yet.
fn comparison_operator_target(
    left_datatype: &DataType,
    op: &Operator,
    right_datatype: &DataType,
) -> Result<Interval> {
    let datatype = get_result_type(left_datatype, &Operator::Minus, right_datatype)?;
    let unbounded = IntervalBound::make_unbounded(&datatype)?;
    let zero = ScalarValue::new_zero(&datatype)?;
    Ok(match *op {
        Operator::GtEq => Interval::new(IntervalBound::new(zero, false), unbounded),
        Operator::Gt => Interval::new(IntervalBound::new(zero, true), unbounded),
        Operator::LtEq => Interval::new(unbounded, IntervalBound::new(zero, false)),
        Operator::Lt => Interval::new(unbounded, IntervalBound::new(zero, true)),
        _ => unreachable!(),
    })
}

/// This function propagates constraints arising from comparison operators.
/// The main idea is that we can analyze an inequality like x > y through the
/// equivalent inequality x - y > 0. Assuming that x and y has ranges [xL, xU]
/// and [yL, yU], we simply apply constraint propagation across [xL, xU],
/// [yL, yH] and [0, ∞]. Specifically, we would first do
///     - [xL, xU] <- ([yL, yU] + [0, ∞]) ∩ [xL, xU], and then
///     - [yL, yU] <- ([xL, xU] - [0, ∞]) ∩ [yL, yU].
pub fn propagate_comparison(
    op: &Operator,
    left_child: &Interval,
    right_child: &Interval,
) -> Result<(Option<Interval>, Option<Interval>)> {
    let parent = comparison_operator_target(
        &left_child.get_datatype()?,
        op,
        &right_child.get_datatype()?,
    )?;
    propagate_arithmetic(&Operator::Minus, &parent, left_child, right_child)
}

impl ExprIntervalGraph {
    pub fn try_new(expr: Arc<dyn PhysicalExpr>) -> Result<Self> {
        // Build the full graph:
        let (root, graph) = build_dag(expr, &ExprIntervalGraphNode::make_node)?;
        Ok(Self { graph, root })
    }

    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    // Sometimes, we do not want to calculate and/or propagate intervals all
    // way down to leaf expressions. For example, assume that we have a
    // `SymmetricHashJoin` which has a child with an output ordering like:
    //
    // PhysicalSortExpr {
    //     expr: BinaryExpr('a', +, 'b'),
    //     sort_option: ..
    // }
    //
    // i.e. its output order comes from a clause like "ORDER BY a + b". In such
    // a case, we must calculate the interval for the BinaryExpr('a', +, 'b')
    // instead of the columns inside this BinaryExpr, because this interval
    // decides whether we prune or not. Therefore, children `PhysicalExpr`s of
    // this `BinaryExpr` may be pruned for performance. The figure below
    // explains this example visually.
    //
    // Note that we just remove the nodes from the DAEG, do not make any change
    // to the plan itself.
    //
    // ```text
    //
    //                                  +-----+                                          +-----+
    //                                  | GT  |                                          | GT  |
    //                         +--------|     |-------+                         +--------|     |-------+
    //                         |        +-----+       |                         |        +-----+       |
    //                         |                      |                         |                      |
    //                      +-----+                   |                      +-----+                   |
    //                      |Cast |                   |                      |Cast |                   |
    //                      |     |                   |             --\      |     |                   |
    //                      +-----+                   |       ----------     +-----+                   |
    //                         |                      |             --/         |                      |
    //                         |                      |                         |                      |
    //                      +-----+                +-----+                   +-----+                +-----+
    //                   +--|Plus |--+          +--|Plus |--+                |Plus |             +--|Plus |--+
    //                   |  |     |  |          |  |     |  |                |     |             |  |     |  |
    //  Prune from here  |  +-----+  |          |  +-----+  |                +-----+             |  +-----+  |
    //  ------------------------------------    |           |                                    |           |
    //                   |           |          |           |                                    |           |
    //                +-----+     +-----+    +-----+     +-----+                              +-----+     +-----+
    //                | a   |     |  b  |    |  c  |     |  2  |                              |  c  |     |  2  |
    //                |     |     |     |    |     |     |     |                              |     |     |     |
    //                +-----+     +-----+    +-----+     +-----+                              +-----+     +-----+
    //
    // ```

    /// This function associates stable node indices with [PhysicalExpr]s so
    /// that we can match `Arc<dyn PhysicalExpr>` and NodeIndex objects during
    /// membership tests.
    pub fn gather_node_indices(
        &mut self,
        exprs: &[Arc<dyn PhysicalExpr>],
    ) -> Vec<(Arc<dyn PhysicalExpr>, usize)> {
        let graph = &self.graph;
        let mut bfs = Bfs::new(graph, self.root);
        // We collect the node indices (usize) of [PhysicalExpr]s in the order
        // given by argument `exprs`. To preserve this order, we initialize each
        // expression's node index with usize::MAX, and then find the corresponding
        // node indices by traversing the graph.
        let mut removals = vec![];
        let mut expr_node_indices = exprs
            .iter()
            .map(|e| (e.clone(), usize::MAX))
            .collect::<Vec<_>>();
        while let Some(node) = bfs.next(graph) {
            // Get the plan corresponding to this node:
            let expr = &graph[node].expr;
            // If the current expression is among `exprs`, slate its children
            // for removal:
            if let Some(value) = exprs.iter().position(|e| expr.eq(e)) {
                // Update the node index of the associated `PhysicalExpr`:
                expr_node_indices[value].1 = node.index();
                for edge in graph.edges_directed(node, Outgoing) {
                    // Slate the child for removal, do not remove immediately.
                    removals.push(edge.id());
                }
            }
        }
        for edge_idx in removals {
            self.graph.remove_edge(edge_idx);
        }
        // Get the set of node indices reachable from the root node:
        let connected_nodes = self.connected_nodes();
        // Remove nodes not connected to the root node:
        self.graph
            .retain_nodes(|_, index| connected_nodes.contains(&index));
        expr_node_indices
    }

    /// Returns the set of node indices reachable from the root node via a
    /// simple depth-first search.
    fn connected_nodes(&self) -> HashSet<NodeIndex> {
        let mut nodes = HashSet::new();
        let mut dfs = Dfs::new(&self.graph, self.root);
        while let Some(node) = dfs.next(&self.graph) {
            nodes.insert(node);
        }
        nodes
    }

    /// This function assigns given ranges to expressions in the DAEG.
    /// The argument `assignments` associates indices of sought expressions
    /// with their corresponding new ranges.
    pub fn assign_intervals(&mut self, assignments: &[(usize, Interval)]) {
        for (index, interval) in assignments {
            let node_index = NodeIndex::from(*index as DefaultIx);
            self.graph[node_index].interval = interval.clone();
        }
    }

    /// This function fetches ranges of expressions from the DAEG. The argument
    /// `assignments` associates indices of sought expressions with their ranges,
    /// which this function modifies to reflect the intervals in the DAEG.
    pub fn update_intervals(&self, assignments: &mut [(usize, Interval)]) {
        for (index, interval) in assignments.iter_mut() {
            let node_index = NodeIndex::from(*index as DefaultIx);
            *interval = self.graph[node_index].interval.clone();
        }
    }

    /// Computes bounds for an expression using interval arithmetic via a
    /// bottom-up traversal.
    ///
    /// # Arguments
    /// * `leaf_bounds` - &[(usize, Interval)]. Provide NodeIndex, Interval tuples for leaf variables.
    ///
    /// # Examples
    ///
    /// ```
    ///  use std::sync::Arc;
    ///  use datafusion_common::ScalarValue;
    ///  use datafusion_expr::Operator;
    ///  use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
    ///  use datafusion_physical_expr::intervals::{Interval, IntervalBound, ExprIntervalGraph};
    ///  use datafusion_physical_expr::PhysicalExpr;
    ///  let expr = Arc::new(BinaryExpr::new(
    ///             Arc::new(Column::new("gnz", 0)),
    ///             Operator::Plus,
    ///             Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
    ///         ));
    ///  let mut graph = ExprIntervalGraph::try_new(expr).unwrap();
    ///  // Do it once, while constructing.
    ///  let node_indices = graph
    ///     .gather_node_indices(&[Arc::new(Column::new("gnz", 0))]);
    ///  let left_index = node_indices.get(0).unwrap().1;
    ///  // Provide intervals for leaf variables (here, there is only one).
    ///  let intervals = vec![(
    ///     left_index,
    ///     Interval::make(Some(10), Some(20), (true, true)),
    ///  )];
    ///  // Evaluate bounds for the composite expression:
    ///  graph.assign_intervals(&intervals);
    ///  assert_eq!(
    ///     graph.evaluate_bounds().unwrap(),
    ///     &Interval::make(Some(20), Some(30), (true, true)),
    ///  )
    ///
    /// ```
    pub fn evaluate_bounds(&mut self) -> Result<&Interval> {
        let mut dfs = DfsPostOrder::new(&self.graph, self.root);
        while let Some(node) = dfs.next(&self.graph) {
            let neighbors = self.graph.neighbors_directed(node, Outgoing);
            let mut children_intervals = neighbors
                .map(|child| self.graph[child].interval())
                .collect::<Vec<_>>();
            // If the current expression is a leaf, its interval should already
            // be set externally, just continue with the evaluation procedure:
            if !children_intervals.is_empty() {
                // Reverse to align with [PhysicalExpr]'s children:
                children_intervals.reverse();
                self.graph[node].interval =
                    self.graph[node].expr.evaluate_bounds(&children_intervals)?;
            }
        }
        Ok(&self.graph[self.root].interval)
    }

    /// Updates/shrinks bounds for leaf expressions using interval arithmetic
    /// via a top-down traversal.
    fn propagate_constraints(&mut self) -> Result<PropagationResult> {
        let mut bfs = Bfs::new(&self.graph, self.root);
        while let Some(node) = bfs.next(&self.graph) {
            let neighbors = self.graph.neighbors_directed(node, Outgoing);
            let mut children = neighbors.collect::<Vec<_>>();
            // If the current expression is a leaf, its range is now final.
            // So, just continue with the propagation procedure:
            if children.is_empty() {
                continue;
            }
            // Reverse to align with [PhysicalExpr]'s children:
            children.reverse();
            let children_intervals = children
                .iter()
                .map(|child| self.graph[*child].interval())
                .collect::<Vec<_>>();
            let node_interval = self.graph[node].interval();
            let propagated_intervals = self.graph[node]
                .expr
                .propagate_constraints(node_interval, &children_intervals)?;
            for (child, interval) in children.into_iter().zip(propagated_intervals) {
                if let Some(interval) = interval {
                    self.graph[child].interval = interval;
                } else {
                    // The constraint is infeasible, report:
                    return Ok(PropagationResult::Infeasible);
                }
            }
        }
        Ok(PropagationResult::Success)
    }

    /// Updates intervals for all expressions in the DAEG by successive
    /// bottom-up and top-down traversals.
    pub fn update_ranges(
        &mut self,
        leaf_bounds: &mut [(usize, Interval)],
    ) -> Result<PropagationResult> {
        self.assign_intervals(leaf_bounds);
        let bounds = self.evaluate_bounds()?;
        if bounds == &Interval::CERTAINLY_FALSE {
            Ok(PropagationResult::Infeasible)
        } else if bounds == &Interval::UNCERTAIN {
            let result = self.propagate_constraints();
            self.update_intervals(leaf_bounds);
            result
        } else {
            Ok(PropagationResult::CannotPropagate)
        }
    }
}

/// Indicates whether interval arithmetic is supported for the given expression.
/// Currently, we do not support all [`PhysicalExpr`]s for interval calculations.
/// We do not support every type of [`Operator`]s either. Over time, this check
/// will relax as more types of `PhysicalExpr`s and `Operator`s are supported.
/// Currently, [`CastExpr`], [`BinaryExpr`], [`Column`] and [`Literal`] are supported.
pub fn check_support(expr: &Arc<dyn PhysicalExpr>) -> bool {
    let expr_any = expr.as_any();
    let expr_supported = if let Some(binary_expr) = expr_any.downcast_ref::<BinaryExpr>()
    {
        is_operator_supported(binary_expr.op())
    } else {
        expr_any.is::<Column>() || expr_any.is::<Literal>() || expr_any.is::<CastExpr>()
    };
    expr_supported && expr.children().iter().all(check_support)
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;

    use crate::expressions::{BinaryExpr, Column};
    use crate::intervals::test_utils::gen_conjunctive_numerical_expr;
    use datafusion_common::ScalarValue;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use rstest::*;

    fn experiment(
        expr: Arc<dyn PhysicalExpr>,
        exprs_with_interval: (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>),
        left_interval: Interval,
        right_interval: Interval,
        left_expected: Interval,
        right_expected: Interval,
        result: PropagationResult,
    ) -> Result<()> {
        let col_stats = vec![
            (exprs_with_interval.0.clone(), left_interval),
            (exprs_with_interval.1.clone(), right_interval),
        ];
        let expected = vec![
            (exprs_with_interval.0.clone(), left_expected),
            (exprs_with_interval.1.clone(), right_expected),
        ];
        let mut graph = ExprIntervalGraph::try_new(expr)?;
        let expr_indexes = graph
            .gather_node_indices(&col_stats.iter().map(|(e, _)| e.clone()).collect_vec());

        let mut col_stat_nodes = col_stats
            .iter()
            .zip(expr_indexes.iter())
            .map(|((_, interval), (_, index))| (*index, interval.clone()))
            .collect_vec();
        let expected_nodes = expected
            .iter()
            .zip(expr_indexes.iter())
            .map(|((_, interval), (_, index))| (*index, interval.clone()))
            .collect_vec();

        let exp_result = graph.update_ranges(&mut col_stat_nodes[..])?;
        assert_eq!(exp_result, result);
        col_stat_nodes.iter().zip(expected_nodes.iter()).for_each(
            |((_, calculated_interval_node), (_, expected))| {
                // NOTE: These randomized tests only check for conservative containment,
                // not openness/closedness of endpoints.
                assert!(calculated_interval_node.lower.value <= expected.lower.value);
                assert!(calculated_interval_node.upper.value >= expected.upper.value);
            },
        );
        Ok(())
    }

    macro_rules! generate_cases {
        ($FUNC_NAME:ident, $TYPE:ty, $SCALAR:ident) => {
            fn $FUNC_NAME<const ASC: bool>(
                expr: Arc<dyn PhysicalExpr>,
                left_col: Arc<dyn PhysicalExpr>,
                right_col: Arc<dyn PhysicalExpr>,
                seed: u64,
                expr_left: $TYPE,
                expr_right: $TYPE,
            ) -> Result<()> {
                let mut r = StdRng::seed_from_u64(seed);

                let (left_given, right_given, left_expected, right_expected) = if ASC {
                    let left = r.gen_range((0 as $TYPE)..(1000 as $TYPE));
                    let right = r.gen_range((0 as $TYPE)..(1000 as $TYPE));
                    (
                        (Some(left), None),
                        (Some(right), None),
                        (Some(<$TYPE>::max(left, right + expr_left)), None),
                        (Some(<$TYPE>::max(right, left + expr_right)), None),
                    )
                } else {
                    let left = r.gen_range((0 as $TYPE)..(1000 as $TYPE));
                    let right = r.gen_range((0 as $TYPE)..(1000 as $TYPE));
                    (
                        (None, Some(left)),
                        (None, Some(right)),
                        (None, Some(<$TYPE>::min(left, right + expr_left))),
                        (None, Some(<$TYPE>::min(right, left + expr_right))),
                    )
                };

                experiment(
                    expr,
                    (left_col, right_col),
                    Interval::make(left_given.0, left_given.1, (true, true)),
                    Interval::make(right_given.0, right_given.1, (true, true)),
                    Interval::make(left_expected.0, left_expected.1, (true, true)),
                    Interval::make(right_expected.0, right_expected.1, (true, true)),
                    PropagationResult::Success,
                )
            }
        };
    }
    generate_cases!(generate_case_i32, i32, Int32);
    generate_cases!(generate_case_i64, i64, Int64);
    generate_cases!(generate_case_f32, f32, Float32);
    generate_cases!(generate_case_f64, f64, Float64);

    #[test]
    fn testing_not_possible() -> Result<()> {
        let left_col = Arc::new(Column::new("left_watermark", 0));
        let right_col = Arc::new(Column::new("right_watermark", 0));

        // left_watermark > right_watermark + 5
        let left_and_1 = Arc::new(BinaryExpr::new(
            left_col.clone(),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(5)))),
        ));
        let expr = Arc::new(BinaryExpr::new(left_and_1, Operator::Gt, right_col.clone()));
        experiment(
            expr,
            (left_col, right_col),
            Interval::make(Some(10), Some(20), (true, true)),
            Interval::make(Some(100), None, (true, true)),
            Interval::make(Some(10), Some(20), (true, true)),
            Interval::make(Some(100), None, (true, true)),
            PropagationResult::Infeasible,
        )
    }

    macro_rules! integer_float_case_1 {
        ($TEST_FUNC_NAME:ident, $GENERATE_CASE_FUNC_NAME:ident, $TYPE:ty, $SCALAR:ident) => {
            #[rstest]
            #[test]
            fn $TEST_FUNC_NAME(
                #[values(0, 1, 2, 3, 4, 12, 32, 314, 3124, 123, 125, 211, 215, 4123)]
                seed: u64,
                #[values(Operator::Gt, Operator::GtEq)] greater_op: Operator,
                #[values(Operator::Lt, Operator::LtEq)] less_op: Operator,
            ) -> Result<()> {
                let left_col = Arc::new(Column::new("left_watermark", 0));
                let right_col = Arc::new(Column::new("right_watermark", 0));

                // left_watermark + 1 > right_watermark + 11 AND left_watermark + 3 < right_watermark + 33
                let expr = gen_conjunctive_numerical_expr(
                    left_col.clone(),
                    right_col.clone(),
                    (
                        Operator::Plus,
                        Operator::Plus,
                        Operator::Plus,
                        Operator::Plus,
                    ),
                    ScalarValue::$SCALAR(Some(1 as $TYPE)),
                    ScalarValue::$SCALAR(Some(11 as $TYPE)),
                    ScalarValue::$SCALAR(Some(3 as $TYPE)),
                    ScalarValue::$SCALAR(Some(33 as $TYPE)),
                    (greater_op, less_op),
                );
                // l > r + 10 AND r > l - 30
                let l_gt_r = 10 as $TYPE;
                let r_gt_l = -30 as $TYPE;
                $GENERATE_CASE_FUNC_NAME::<true>(
                    expr.clone(),
                    left_col.clone(),
                    right_col.clone(),
                    seed,
                    l_gt_r,
                    r_gt_l,
                )?;
                // Descending tests
                // r < l - 10 AND l < r + 30
                let r_lt_l = -l_gt_r;
                let l_lt_r = -r_gt_l;
                $GENERATE_CASE_FUNC_NAME::<false>(
                    expr, left_col, right_col, seed, l_lt_r, r_lt_l,
                )
            }
        };
    }

    integer_float_case_1!(case_1_i32, generate_case_i32, i32, Int32);
    integer_float_case_1!(case_1_i64, generate_case_i64, i64, Int64);
    integer_float_case_1!(case_1_f64, generate_case_f64, f64, Float64);
    integer_float_case_1!(case_1_f32, generate_case_f32, f32, Float32);

    macro_rules! integer_float_case_2 {
        ($TEST_FUNC_NAME:ident, $GENERATE_CASE_FUNC_NAME:ident, $TYPE:ty, $SCALAR:ident) => {
            #[rstest]
            #[test]
            fn $TEST_FUNC_NAME(
                #[values(0, 1, 2, 3, 4, 12, 32, 314, 3124, 123, 125, 211, 215, 4123)]
                seed: u64,
                #[values(Operator::Gt, Operator::GtEq)] greater_op: Operator,
                #[values(Operator::Lt, Operator::LtEq)] less_op: Operator,
            ) -> Result<()> {
                let left_col = Arc::new(Column::new("left_watermark", 0));
                let right_col = Arc::new(Column::new("right_watermark", 0));

                // left_watermark - 1 > right_watermark + 5 AND left_watermark + 3 < right_watermark + 10
                let expr = gen_conjunctive_numerical_expr(
                    left_col.clone(),
                    right_col.clone(),
                    (
                        Operator::Minus,
                        Operator::Plus,
                        Operator::Plus,
                        Operator::Plus,
                    ),
                    ScalarValue::$SCALAR(Some(1 as $TYPE)),
                    ScalarValue::$SCALAR(Some(5 as $TYPE)),
                    ScalarValue::$SCALAR(Some(3 as $TYPE)),
                    ScalarValue::$SCALAR(Some(10 as $TYPE)),
                    (greater_op, less_op),
                );
                // l > r + 6 AND r > l - 7
                let l_gt_r = 6 as $TYPE;
                let r_gt_l = -7 as $TYPE;
                $GENERATE_CASE_FUNC_NAME::<true>(
                    expr.clone(),
                    left_col.clone(),
                    right_col.clone(),
                    seed,
                    l_gt_r,
                    r_gt_l,
                )?;
                // Descending tests
                // r < l - 6 AND l < r + 7
                let r_lt_l = -l_gt_r;
                let l_lt_r = -r_gt_l;
                $GENERATE_CASE_FUNC_NAME::<false>(
                    expr, left_col, right_col, seed, l_lt_r, r_lt_l,
                )
            }
        };
    }

    integer_float_case_2!(case_2_i32, generate_case_i32, i32, Int32);
    integer_float_case_2!(case_2_i64, generate_case_i64, i64, Int64);
    integer_float_case_2!(case_2_f64, generate_case_f64, f64, Float64);
    integer_float_case_2!(case_2_f32, generate_case_f32, f32, Float32);

    macro_rules! integer_float_case_3 {
        ($TEST_FUNC_NAME:ident, $GENERATE_CASE_FUNC_NAME:ident, $TYPE:ty, $SCALAR:ident) => {
            #[rstest]
            #[test]
            fn $TEST_FUNC_NAME(
                #[values(0, 1, 2, 3, 4, 12, 32, 314, 3124, 123, 125, 211, 215, 4123)]
                seed: u64,
                #[values(Operator::Gt, Operator::GtEq)] greater_op: Operator,
                #[values(Operator::Lt, Operator::LtEq)] less_op: Operator,
            ) -> Result<()> {
                let left_col = Arc::new(Column::new("left_watermark", 0));
                let right_col = Arc::new(Column::new("right_watermark", 0));

                // left_watermark - 1 > right_watermark + 5 AND left_watermark - 3 < right_watermark + 10
                let expr = gen_conjunctive_numerical_expr(
                    left_col.clone(),
                    right_col.clone(),
                    (
                        Operator::Minus,
                        Operator::Plus,
                        Operator::Minus,
                        Operator::Plus,
                    ),
                    ScalarValue::$SCALAR(Some(1 as $TYPE)),
                    ScalarValue::$SCALAR(Some(5 as $TYPE)),
                    ScalarValue::$SCALAR(Some(3 as $TYPE)),
                    ScalarValue::$SCALAR(Some(10 as $TYPE)),
                    (greater_op, less_op),
                );
                // l > r + 6 AND r > l - 13
                let l_gt_r = 6 as $TYPE;
                let r_gt_l = -13 as $TYPE;
                $GENERATE_CASE_FUNC_NAME::<true>(
                    expr.clone(),
                    left_col.clone(),
                    right_col.clone(),
                    seed,
                    l_gt_r,
                    r_gt_l,
                )?;
                // Descending tests
                // r < l - 6 AND l < r + 13
                let r_lt_l = -l_gt_r;
                let l_lt_r = -r_gt_l;
                $GENERATE_CASE_FUNC_NAME::<false>(
                    expr, left_col, right_col, seed, l_lt_r, r_lt_l,
                )
            }
        };
    }

    integer_float_case_3!(case_3_i32, generate_case_i32, i32, Int32);
    integer_float_case_3!(case_3_i64, generate_case_i64, i64, Int64);
    integer_float_case_3!(case_3_f64, generate_case_f64, f64, Float64);
    integer_float_case_3!(case_3_f32, generate_case_f32, f32, Float32);

    macro_rules! integer_float_case_4 {
        ($TEST_FUNC_NAME:ident, $GENERATE_CASE_FUNC_NAME:ident, $TYPE:ty, $SCALAR:ident) => {
            #[rstest]
            #[test]
            fn $TEST_FUNC_NAME(
                #[values(0, 1, 2, 3, 4, 12, 32, 314, 3124, 123, 125, 211, 215, 4123)]
                seed: u64,
                #[values(Operator::Gt, Operator::GtEq)] greater_op: Operator,
                #[values(Operator::Lt, Operator::LtEq)] less_op: Operator,
            ) -> Result<()> {
                let left_col = Arc::new(Column::new("left_watermark", 0));
                let right_col = Arc::new(Column::new("right_watermark", 0));

                // left_watermark - 10 > right_watermark - 5 AND left_watermark - 30 < right_watermark - 3
                let expr = gen_conjunctive_numerical_expr(
                    left_col.clone(),
                    right_col.clone(),
                    (
                        Operator::Minus,
                        Operator::Minus,
                        Operator::Minus,
                        Operator::Plus,
                    ),
                    ScalarValue::$SCALAR(Some(10 as $TYPE)),
                    ScalarValue::$SCALAR(Some(5 as $TYPE)),
                    ScalarValue::$SCALAR(Some(3 as $TYPE)),
                    ScalarValue::$SCALAR(Some(10 as $TYPE)),
                    (greater_op, less_op),
                );
                // l > r + 5 AND r > l - 13
                let l_gt_r = 5 as $TYPE;
                let r_gt_l = -13 as $TYPE;
                $GENERATE_CASE_FUNC_NAME::<true>(
                    expr.clone(),
                    left_col.clone(),
                    right_col.clone(),
                    seed,
                    l_gt_r,
                    r_gt_l,
                )?;
                // Descending tests
                // r < l - 5 AND l < r + 13
                let r_lt_l = -l_gt_r;
                let l_lt_r = -r_gt_l;
                $GENERATE_CASE_FUNC_NAME::<false>(
                    expr, left_col, right_col, seed, l_lt_r, r_lt_l,
                )
            }
        };
    }

    integer_float_case_4!(case_4_i32, generate_case_i32, i32, Int32);
    integer_float_case_4!(case_4_i64, generate_case_i64, i64, Int64);
    integer_float_case_4!(case_4_f64, generate_case_f64, f64, Float64);
    integer_float_case_4!(case_4_f32, generate_case_f32, f32, Float32);

    macro_rules! integer_float_case_5 {
        ($TEST_FUNC_NAME:ident, $GENERATE_CASE_FUNC_NAME:ident, $TYPE:ty, $SCALAR:ident) => {
            #[rstest]
            #[test]
            fn $TEST_FUNC_NAME(
                #[values(0, 1, 2, 3, 4, 12, 32, 314, 3124, 123, 125, 211, 215, 4123)]
                seed: u64,
                #[values(Operator::Gt, Operator::GtEq)] greater_op: Operator,
                #[values(Operator::Lt, Operator::LtEq)] less_op: Operator,
            ) -> Result<()> {
                let left_col = Arc::new(Column::new("left_watermark", 0));
                let right_col = Arc::new(Column::new("right_watermark", 0));

                // left_watermark - 10 > right_watermark - 5 AND left_watermark - 30 < right_watermark - 3
                let expr = gen_conjunctive_numerical_expr(
                    left_col.clone(),
                    right_col.clone(),
                    (
                        Operator::Minus,
                        Operator::Minus,
                        Operator::Minus,
                        Operator::Minus,
                    ),
                    ScalarValue::$SCALAR(Some(10 as $TYPE)),
                    ScalarValue::$SCALAR(Some(5 as $TYPE)),
                    ScalarValue::$SCALAR(Some(30 as $TYPE)),
                    ScalarValue::$SCALAR(Some(3 as $TYPE)),
                    (greater_op, less_op),
                );
                // l > r + 5 AND r > l - 27
                let l_gt_r = 5 as $TYPE;
                let r_gt_l = -27 as $TYPE;
                $GENERATE_CASE_FUNC_NAME::<true>(
                    expr.clone(),
                    left_col.clone(),
                    right_col.clone(),
                    seed,
                    l_gt_r,
                    r_gt_l,
                )?;
                // Descending tests
                // r < l - 5 AND l < r + 27
                let r_lt_l = -l_gt_r;
                let l_lt_r = -r_gt_l;
                $GENERATE_CASE_FUNC_NAME::<false>(
                    expr, left_col, right_col, seed, l_lt_r, r_lt_l,
                )
            }
        };
    }

    integer_float_case_5!(case_5_i32, generate_case_i32, i32, Int32);
    integer_float_case_5!(case_5_i64, generate_case_i64, i64, Int64);
    integer_float_case_5!(case_5_f64, generate_case_f64, f64, Float64);
    integer_float_case_5!(case_5_f32, generate_case_f32, f32, Float32);

    #[test]
    fn test_gather_node_indices_dont_remove() -> Result<()> {
        // Expression: a@0 + b@1 + 1 > a@0 - b@1, given a@0 + b@1.
        // Do not remove a@0 or b@1, only remove edges since a@0 - b@1 also
        // depends on leaf nodes a@0 and b@1.
        let left_expr = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Plus,
                Arc::new(Column::new("b", 1)),
            )),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ));

        let right_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Minus,
            Arc::new(Column::new("b", 1)),
        ));
        let expr = Arc::new(BinaryExpr::new(left_expr, Operator::Gt, right_expr));
        let mut graph = ExprIntervalGraph::try_new(expr).unwrap();
        // Define a test leaf node.
        let leaf_node = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        ));
        // Store the current node count.
        let prev_node_count = graph.node_count();
        // Gather the index of node in the expression graph that match the test leaf node.
        graph.gather_node_indices(&[leaf_node]);
        // Store the final node count.
        let final_node_count = graph.node_count();
        // Assert that the final node count is equal the previous node count.
        // This means we did not remove any node.
        assert_eq!(prev_node_count, final_node_count);
        Ok(())
    }

    #[test]
    fn test_gather_node_indices_remove() -> Result<()> {
        // Expression: a@0 + b@1 + 1 > y@0 - z@1, given a@0 + b@1.
        // We expect to remove two nodes since we do not need a@ and b@.
        let left_expr = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Plus,
                Arc::new(Column::new("b", 1)),
            )),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ));

        let right_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("y", 0)),
            Operator::Minus,
            Arc::new(Column::new("z", 1)),
        ));
        let expr = Arc::new(BinaryExpr::new(left_expr, Operator::Gt, right_expr));
        let mut graph = ExprIntervalGraph::try_new(expr).unwrap();
        // Define a test leaf node.
        let leaf_node = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        ));
        // Store the current node count.
        let prev_node_count = graph.node_count();
        // Gather the index of node in the expression graph that match the test leaf node.
        graph.gather_node_indices(&[leaf_node]);
        // Store the final node count.
        let final_node_count = graph.node_count();
        // Assert that the final node count is two less than the previous node
        // count; i.e. that we did remove two nodes.
        assert_eq!(prev_node_count, final_node_count + 2);
        Ok(())
    }

    #[test]
    fn test_gather_node_indices_remove_one() -> Result<()> {
        // Expression: a@0 + b@1 + 1 > a@0 - z@1, given a@0 + b@1.
        // We expect to remove one nodesince we still need a@ but not b@.
        let left_expr = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Plus,
                Arc::new(Column::new("b", 1)),
            )),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ));

        let right_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Minus,
            Arc::new(Column::new("z", 1)),
        ));
        let expr = Arc::new(BinaryExpr::new(left_expr, Operator::Gt, right_expr));
        let mut graph = ExprIntervalGraph::try_new(expr).unwrap();
        // Define a test leaf node.
        let leaf_node = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        ));
        // Store the current node count.
        let prev_node_count = graph.node_count();
        // Gather the index of node in the expression graph that match the test leaf node.
        graph.gather_node_indices(&[leaf_node]);
        // Store the final node count.
        let final_node_count = graph.node_count();
        // Assert that the final node count is one less than the previous node
        // count; i.e. that we did remove two nodes.
        assert_eq!(prev_node_count, final_node_count + 1);
        Ok(())
    }

    #[test]
    fn test_gather_node_indices_cannot_provide() -> Result<()> {
        // Expression: a@0 + 1 + b@1 > y@0 - z@1 -> provide a@0 + b@1
        // TODO: We expect nodes a@0 and b@1 to be pruned, and intervals to be provided from the a@0 + b@1 node.
        //  However, we do not have an exact node for a@0 + b@1 due to the binary tree structure of the expressions.
        //  Pruning and interval providing for BinaryExpr expressions are more challenging without exact matches.
        //  Currently, we only support exact matches for BinaryExprs, but we plan to extend support beyond exact matches in the future.
        let left_expr = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Plus,
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            )),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        ));

        let right_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("y", 0)),
            Operator::Minus,
            Arc::new(Column::new("z", 1)),
        ));
        let expr = Arc::new(BinaryExpr::new(left_expr, Operator::Gt, right_expr));
        let mut graph = ExprIntervalGraph::try_new(expr).unwrap();
        // Define a test leaf node.
        let leaf_node = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        ));
        // Store the current node count.
        let prev_node_count = graph.node_count();
        // Gather the index of node in the expression graph that match the test leaf node.
        graph.gather_node_indices(&[leaf_node]);
        // Store the final node count.
        let final_node_count = graph.node_count();
        // Assert that the final node count is equal the previous node count (i.e., no node was pruned).
        assert_eq!(prev_node_count, final_node_count);
        Ok(())
    }
}
