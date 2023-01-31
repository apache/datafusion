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

use std::fmt::{Display, Formatter};
use std::ops::{Index, IndexMut};
use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Operator;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use petgraph::visit::{Bfs, DfsPostOrder};
use petgraph::Outgoing;

use crate::expressions::{BinaryExpr, CastExpr, Literal};
use crate::intervals::interval_aritmetics::{apply_operator, Interval};
use crate::utils::{build_physical_expr_graph, ExprTreeNode};
use crate::PhysicalExpr;

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
pub struct ExprIntervalGraph(
    NodeIndex,
    StableGraph<ExprIntervalGraphNode, usize>,
    pub Vec<(Arc<dyn PhysicalExpr>, usize)>,
);

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
    pub fn make_node(node: Arc<ExprTreeNode>) -> ExprIntervalGraphNode {
        let expr = node.expr();
        if let Some(literal) = expr.as_any().downcast_ref::<Literal>() {
            let value = literal.value();
            let interval = Interval {
                lower: value.clone(),
                upper: value.clone(),
            };
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

/// This function refines the interval `left_child` by propagating intervals
/// `parent` and `right_child` through the application of `inverse_op`, which
/// is the inverse operation of the operator in the corresponding DAEG node.
fn propagate_left(
    inverse_op: &Operator,
    parent: &Interval,
    left_child: &Interval,
    right_child: &Interval,
) -> Result<Interval> {
    let interv = apply_operator(inverse_op, parent, right_child)?;
    match interv.intersect(left_child) {
        Ok(Some(val)) => Ok(val),
        // TODO: We should not have this condition realize in practice. If this
        //       happens, it would mean the constraint you are propagating will
        //       never get satisfied (there is no satisfying value). When solving
        //       equations, that means we should stop and report there is no solution.
        //       For joins, I think it means we will never have a match. Therefore,
        //       I think we should just propagate this out in the return value all
        //       the way up and handle it properly so that we don't lose generality.
        //       Just returning the left interval without change is not wrong (i.e.
        //       it is conservatively true), but loses very useful information.
        Ok(None) => Ok(left_child.clone()),
        Err(e) => Err(e),
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

/// This function refines intervals `left_child` and `right_child` by
/// applying constraint propagation through `parent` via `inverse_op`, which
/// is the inverse operation of the operator in the corresponding DAEG node.
pub fn propagate(
    op: &Operator,
    parent: &Interval,
    left_child: &Interval,
    right_child: &Interval,
) -> Result<(Interval, Interval)> {
    let inverse_op = get_inverse_op(*op);
    let new_right = propagate_left(&inverse_op, parent, right_child, left_child)?;
    let new_left = propagate_left(&inverse_op, parent, left_child, &new_right)?;
    Ok((new_left, new_right))
}

/// This function provides a target parent interval for comparison operators.
/// If we have expression > 0, expression must have the range [0, ∞].
/// If we have expression < 0, expression must have the range [-∞, 0].
/// Currently, we only support strict inequalities since open/closed intervals
/// are not implemented yet.
fn comparison_operator_target(datatype: &DataType, op: &Operator) -> Result<Interval> {
    let unbounded = ScalarValue::try_from(datatype)?;
    // TODO: Going through a string zero to a numeric zero is very inefficient.
    //       Let's find an efficient way to initialize a zero value with given data type.
    let zero = ScalarValue::try_from_string("0".to_string(), datatype)?;
    Ok(match *op {
        Operator::Gt => Interval {
            lower: zero,
            upper: unbounded,
        },
        Operator::Lt => Interval {
            lower: unbounded,
            upper: zero,
        },
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
pub fn propagate_comparison_operators(
    op: &Operator,
    left_child: &Interval,
    right_child: &Interval,
) -> Result<(Interval, Interval)> {
    let parent = comparison_operator_target(&left_child.get_datatype(), op)?;
    // TODO: Same issue with propagate_left, we should report empty intervals
    //       and handle this outside.
    let new_left = right_child
        .add(&parent)?
        .intersect(left_child)?
        .unwrap_or_else(|| left_child.clone());
    let new_right = left_child
        .sub(&parent)?
        .intersect(right_child)?
        .unwrap_or_else(|| right_child.clone());
    Ok((new_left, new_right))
}

impl ExprIntervalGraph {
    /// Constructs a new ExprIntervalGraph.
    pub fn try_new(
        expr: Arc<dyn PhysicalExpr>,
        provided_expr: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Self> {
        // Build the full graph:
        let (root_node, mut graph) =
            build_physical_expr_graph(expr, &ExprIntervalGraphNode::make_node)?;
        let mut bfs = Bfs::new(&graph, root_node);
        // TODO: I don't understand what this comment means. Write a clearer
        //       comment and you should probably find a better name for `provided_expr`
        //       too.
        // We preserve the order with Vec<SortedFilterExpr> in SymmetricHashJoin.
        let mut expr_node_indices: Vec<(Arc<dyn PhysicalExpr>, usize)> = provided_expr
            .iter()
            .map(|e| (e.clone(), usize::MAX))
            .collect();
        let mut removals = vec![];
        while let Some(node) = bfs.next(&graph) {
            // Get the plan corresponding to this node:
            let input = graph.index(node);
            let expr = input.expr.clone();
            // If the current expression is among `provided_exprs`, slate its
            // children for removal.
            // TODO: Complete the following comment with a clear explanation:
            // We remove the children because ...............................
            if let Some(value) = provided_expr.iter().position(|e| expr.eq(e)) {
                expr_node_indices[value].1 = node.index();
                let mut edges = graph.neighbors_directed(node, Outgoing).detach();
                while let Some(n_index) = edges.next_node(&graph) {
                    // Slate the child for removal, do not remove immediately.
                    removals.push(n_index);
                }
            }
        }
        for node in removals {
            graph.remove_node(node);
        }
        Ok(Self(root_node, graph, expr_node_indices))
    }

    /// Computes bounds for an expression using interval arithmetic via a
    /// bottom-up traversal.
    fn evaluate_bounds(&mut self, expr_stats: &[(usize, Interval)]) -> Result<()> {
        let mut dfs = DfsPostOrder::new(&self.1, self.0);
        while let Some(node) = dfs.next(&self.1) {
            // Outgoing (Children) edges
            let mut edges = self.1.neighbors_directed(node, Outgoing).detach();
            // Get PhysicalExpr
            let expr = self.1[node].expr.clone();
            // Check if we have a interval information about given PhysicalExpr, if so, directly
            // propagate it to the upper.
            if let Some((_, interval)) =
                expr_stats.iter().find(|(e, _)| *e == node.index())
            {
                let input = self.1.index_mut(node);
                input.interval = interval.clone();
                continue;
            }

            let expr_any = expr.as_any();
            if let Some(binary) = expr_any.downcast_ref::<BinaryExpr>() {
                // Access left child immutable
                let second_child_node_index = edges.next_node(&self.1).unwrap();
                // Access left child immutable
                let first_child_node_index = edges.next_node(&self.1).unwrap();
                // Do not use any reference from graph, MUST clone here.
                let left_interval =
                    self.1.index(first_child_node_index).interval().clone();
                let right_interval =
                    self.1.index(second_child_node_index).interval().clone();
                // Since we release the reference, we can get mutable reference.
                let input = self.1.index_mut(node);
                // Calculate and replace the interval
                input.interval =
                    apply_operator(binary.op(), &left_interval, &right_interval)?;
            } else if let Some(CastExpr {
                cast_type,
                cast_options,
                ..
            }) = expr_any.downcast_ref::<CastExpr>()
            {
                // Access the child immutable
                let child_index = edges.next_node(&self.1).unwrap();
                let child = self.1.index(child_index);
                // Cast the interval
                let new_interval = child.interval.cast_to(cast_type, cast_options)?;
                // Update the interval
                let input = self.1.index_mut(node);
                input.interval = new_interval;
            }
        }
        Ok(())
    }

    /// Updates/shrinks bounds for leaf expressions using interval arithmetic
    /// via a top-down traversal.
    pub fn propagate_constraints(
        &mut self,
        expr_stats: &mut [(usize, Interval)],
    ) -> Result<()> {
        let mut bfs = Bfs::new(&self.1, self.0);
        while let Some(node) = bfs.next(&self.1) {
            // Get plan
            let input = self.1.index(node);
            let expr = input.expr.clone();
            // Get calculated interval. BinaryExpr will propagate the interval according to
            // this.
            let node_interval = input.interval().clone();
            if let Some((_, interval)) =
                expr_stats.iter_mut().find(|(e, _)| *e == node.index())
            {
                *interval = node_interval;
                continue;
            }

            // Outgoing (Children) edges
            let mut edges = self.1.neighbors_directed(node, Outgoing).detach();

            let expr_any = expr.as_any();
            if let Some(binary) = expr_any.downcast_ref::<BinaryExpr>() {
                // Get right node.
                let second_child_node_index = edges.next_node(&self.1).unwrap();
                let second_child_interval =
                    self.1.index(second_child_node_index).interval();
                // Get left node.
                let first_child_node_index = edges.next_node(&self.1).unwrap();
                let first_child_interval =
                    self.1.index(first_child_node_index).interval();

                let (shrink_left_interval, shrink_right_interval) =
                    // There is no propagation process for logical operators.
                    if binary.op().is_logic_operator(){
                        continue
                    } else if binary.op().is_comparison_operator() {
                        // If comparison is strictly false, there is nothing to do for shrink.
                        if let Interval{ lower: ScalarValue::Boolean(Some(false)), upper: ScalarValue::Boolean(Some(false))} = node_interval { continue }
                        // Propagate the comparison operator.
                        propagate_comparison_operators(
                            binary.op(),
                            first_child_interval,
                            second_child_interval,
                        )?
                    } else {
                        // Propagate the arithmetic operator.
                        propagate(binary.op(),
                                  &node_interval,
                                  first_child_interval,
                                  second_child_interval)?
                    };
                let mutable_first_child = self.1.index_mut(first_child_node_index);
                mutable_first_child.interval = shrink_left_interval.clone();
                let mutable_second_child = self.1.index_mut(second_child_node_index);
                mutable_second_child.interval = shrink_right_interval.clone();
            } else if let Some(cast) = expr_any.downcast_ref::<CastExpr>() {
                // Calculate new interval
                let child_index = edges.next_node(&self.1).unwrap();
                let child = self.1.index(child_index);
                // Get child's internal datatype.
                let cast_type = child.interval().get_datatype();
                let cast_options = cast.cast_options();
                let new_child_interval =
                    node_interval.cast_to(&cast_type, cast_options)?;
                let mutable_child = self.1.index_mut(child_index);
                mutable_child.interval = new_child_interval.clone();
            }
        }
        Ok(())
    }

    /// Updates intervals for all expressions in the DAEG by successive
    /// bottom-up and top-down traversals.
    pub fn update_intervals(
        &mut self,
        expr_stats: &mut [(usize, Interval)],
    ) -> Result<()> {
        self.evaluate_bounds(expr_stats)
            .and(self.propagate_constraints(expr_stats))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::filter_numeric_expr_generation;
    use itertools::Itertools;

    use crate::expressions::Column;
    use datafusion_common::ScalarValue;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use rstest::*;

    fn experiment(
        expr: Arc<dyn PhysicalExpr>,
        exprs: (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>),
        left_interval: (Option<i32>, Option<i32>),
        right_interval: (Option<i32>, Option<i32>),
        left_waited: (Option<i32>, Option<i32>),
        right_waited: (Option<i32>, Option<i32>),
    ) -> Result<()> {
        let col_stats = vec![
            (
                exprs.0.clone(),
                Interval {
                    lower: ScalarValue::Int32(left_interval.0),
                    upper: ScalarValue::Int32(left_interval.1),
                },
            ),
            (
                exprs.1.clone(),
                Interval {
                    lower: ScalarValue::Int32(right_interval.0),
                    upper: ScalarValue::Int32(right_interval.1),
                },
            ),
        ];
        let expected = vec![
            (
                exprs.0.clone(),
                Interval {
                    lower: ScalarValue::Int32(left_waited.0),
                    upper: ScalarValue::Int32(left_waited.1),
                },
            ),
            (
                exprs.1.clone(),
                Interval {
                    lower: ScalarValue::Int32(right_waited.0),
                    upper: ScalarValue::Int32(right_waited.1),
                },
            ),
        ];
        let mut graph = ExprIntervalGraph::try_new(
            expr,
            &col_stats.iter().map(|(e, _)| e.clone()).collect_vec(),
        )?;
        let mut col_stat_nodes = col_stats
            .iter()
            .zip(graph.2.iter())
            .map(|((_, interval), (_, index))| (*index, interval.clone()))
            .collect_vec();
        let expected_nodes = expected
            .iter()
            .zip(graph.2.iter())
            .map(|((_, interval), (_, index))| (*index, interval.clone()))
            .collect_vec();

        graph.update_intervals(&mut col_stat_nodes[..])?;
        col_stat_nodes
            .iter()
            .zip(expected_nodes.iter())
            .for_each(|((_, res), (_, expected))| assert_eq!(res, expected));
        Ok(())
    }

    fn generate_case<const ASC: bool>(
        expr: Arc<dyn PhysicalExpr>,
        left_col: Arc<dyn PhysicalExpr>,
        right_col: Arc<dyn PhysicalExpr>,
        seed: u64,
        expr_left: i32,
        expr_right: i32,
    ) -> Result<()> {
        let mut r = StdRng::seed_from_u64(seed);

        let (left_interval, right_interval, left_waited, right_waited) = if ASC {
            let left = (Some(r.gen_range(0..1000)), None);
            let right = (Some(r.gen_range(0..1000)), None);
            (
                left,
                right,
                (
                    Some(std::cmp::max(left.0.unwrap(), right.0.unwrap() + expr_left)),
                    None,
                ),
                (
                    Some(std::cmp::max(
                        right.0.unwrap(),
                        left.0.unwrap() + expr_right,
                    )),
                    None,
                ),
            )
        } else {
            let left = (None, Some(r.gen_range(0..1000)));
            let right = (None, Some(r.gen_range(0..1000)));
            (
                left,
                right,
                (
                    None,
                    Some(std::cmp::min(left.1.unwrap(), right.1.unwrap() + expr_left)),
                ),
                (
                    None,
                    Some(std::cmp::min(
                        right.1.unwrap(),
                        left.1.unwrap() + expr_right,
                    )),
                ),
            )
        };
        experiment(
            expr,
            (left_col, right_col),
            left_interval,
            right_interval,
            left_waited,
            right_waited,
        )?;
        Ok(())
    }

    #[test]
    fn particular() -> Result<()> {
        let seed = 0;
        let left_col = Arc::new(Column::new("left_watermark", 0));
        let right_col = Arc::new(Column::new("right_watermark", 0));
        // left_watermark - 1 > right_watermark + 5 AND left_watermark + 3 < right_watermark + 10
        let expr = filter_numeric_expr_generation(
            left_col.clone(),
            right_col.clone(),
            Operator::Minus,
            Operator::Plus,
            Operator::Plus,
            Operator::Plus,
            1,
            5,
            3,
            10,
        );
        // l > r + 6 AND r > l - 7
        let l_gt_r = 6;
        let r_gt_l = -7;
        generate_case::<true>(
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
        generate_case::<false>(expr, left_col, right_col, seed, l_lt_r, r_lt_l)?;

        Ok(())
    }

    #[rstest]
    #[test]
    fn case_1(
        #[values(0, 1, 2, 3, 4, 12, 32, 314, 3124, 123, 123, 4123)] seed: u64,
    ) -> Result<()> {
        let left_col = Arc::new(Column::new("left_watermark", 0));
        let right_col = Arc::new(Column::new("right_watermark", 0));
        // left_watermark + 1 > right_watermark + 11 AND left_watermark + 3 < right_watermark + 33
        let expr = filter_numeric_expr_generation(
            left_col.clone(),
            right_col.clone(),
            Operator::Plus,
            Operator::Plus,
            Operator::Plus,
            Operator::Plus,
            1,
            11,
            3,
            33,
        );
        // l > r + 10 AND r > l - 30
        let l_gt_r = 10;
        let r_gt_l = -30;
        generate_case::<true>(
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
        generate_case::<false>(expr, left_col, right_col, seed, l_lt_r, r_lt_l)?;

        Ok(())
    }
    #[rstest]
    #[test]
    fn case_2(
        #[values(0, 1, 2, 3, 4, 12, 32, 314, 3124, 123, 123, 4123)] seed: u64,
    ) -> Result<()> {
        let left_col = Arc::new(Column::new("left_watermark", 0));
        let right_col = Arc::new(Column::new("right_watermark", 0));
        // left_watermark - 1 > right_watermark + 5 AND left_watermark + 3 < right_watermark + 10
        let expr = filter_numeric_expr_generation(
            left_col.clone(),
            right_col.clone(),
            Operator::Minus,
            Operator::Plus,
            Operator::Plus,
            Operator::Plus,
            1,
            5,
            3,
            10,
        );
        // l > r + 6 AND r > l - 7
        let l_gt_r = 6;
        let r_gt_l = -7;
        generate_case::<true>(
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
        generate_case::<false>(expr, left_col, right_col, seed, l_lt_r, r_lt_l)?;

        Ok(())
    }

    #[rstest]
    #[test]
    fn case_3(
        #[values(0, 1, 2, 3, 4, 12, 32, 314, 3124, 123, 123, 4123)] seed: u64,
    ) -> Result<()> {
        let left_col = Arc::new(Column::new("left_watermark", 0));
        let right_col = Arc::new(Column::new("right_watermark", 0));
        // left_watermark - 1 > right_watermark + 5 AND left_watermark - 3 < right_watermark + 10
        let expr = filter_numeric_expr_generation(
            left_col.clone(),
            right_col.clone(),
            Operator::Minus,
            Operator::Plus,
            Operator::Minus,
            Operator::Plus,
            1,
            5,
            3,
            10,
        );
        // l > r + 6 AND r > l - 13
        let l_gt_r = 6;
        let r_gt_l = -13;
        generate_case::<true>(
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
        generate_case::<false>(expr, left_col, right_col, seed, l_lt_r, r_lt_l)?;

        Ok(())
    }
    #[rstest]
    #[test]
    fn case_4(
        #[values(0, 1, 2, 3, 4, 12, 32, 314, 3124, 123, 123, 4123)] seed: u64,
    ) -> Result<()> {
        let left_col = Arc::new(Column::new("left_watermark", 0));
        let right_col = Arc::new(Column::new("right_watermark", 0));
        // left_watermark - 10 > right_watermark - 5 AND left_watermark - 3 < right_watermark + 10
        let expr = filter_numeric_expr_generation(
            left_col.clone(),
            right_col.clone(),
            Operator::Minus,
            Operator::Minus,
            Operator::Minus,
            Operator::Plus,
            10,
            5,
            3,
            10,
        );
        // l > r + 5 AND r > l - 13
        let l_gt_r = 5;
        let r_gt_l = -13;
        generate_case::<true>(
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
        generate_case::<false>(expr, left_col, right_col, seed, l_lt_r, r_lt_l)?;
        Ok(())
    }

    #[rstest]
    #[test]
    fn case_5(
        #[values(0, 1, 2, 3, 4, 12, 32, 314, 3124, 123, 123, 4123)] seed: u64,
    ) -> Result<()> {
        let left_col = Arc::new(Column::new("left_watermark", 0));
        let right_col = Arc::new(Column::new("right_watermark", 0));
        // left_watermark - 10 > right_watermark - 5 AND left_watermark - 30 < right_watermark - 3

        let expr = filter_numeric_expr_generation(
            left_col.clone(),
            right_col.clone(),
            Operator::Minus,
            Operator::Minus,
            Operator::Minus,
            Operator::Minus,
            10,
            5,
            30,
            3,
        );
        // l > r + 5 AND r > l - 27
        let l_gt_r = 5;
        let r_gt_l = -27;
        generate_case::<true>(
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
        generate_case::<false>(expr, left_col, right_col, seed, l_lt_r, r_lt_l)?;
        Ok(())
    }
}
