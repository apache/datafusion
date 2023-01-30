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

//! It solves constraint propagation problem for the custom PhysicalExpr
//!
use crate::expressions::{BinaryExpr, CastExpr, Literal};
use crate::intervals::interval_aritmetics::{
    apply_operator, negate_ops, propagate_logical_operators, Interval,
};
use crate::utils::{build_physical_expr_graph, ExprTreeNode};
use crate::PhysicalExpr;
use std::fmt::{Display, Formatter};

use datafusion_common::Result;
use datafusion_expr::Operator;
use petgraph::graph::NodeIndex;
use petgraph::visit::{Bfs, DfsPostOrder};
use petgraph::Outgoing;

use petgraph::stable_graph::StableGraph;
use std::ops::{Index, IndexMut};
use std::sync::Arc;

///
/// Interval arithmetic provides a way to perform mathematical operations on intervals,
/// which represents a range of possible values rather than a single point value.
/// This allows for the propagation of ranges through mathematical operations,
/// and can be used to compute bounds for a complicated expression.The key idea is that by
/// breaking down a complicated expression into simpler terms, and then combining the bounds for
/// those simpler terms, one can obtain bounds for the overall expression.
///
/// For example, consider a mathematical expression such as x^2 + y = 4. Since it would be
/// a binary tree in [PhysicalExpr] notation, this type of an hierarchical
/// computation is well-suited for a graph based implementation. In such an implementation,
/// an equation system f(x) = 0 is represented by a directed acyclic expression
/// graph (DAEG).
///
/// To use interval arithmetic to compute bounds for this expression, one would first determine
/// intervals that represent the possible values of x and y.
/// Let's say that the interval for x is [1, 2] and the interval for y is [-3, 1]. Below, you can
/// see how the computation happens.
///
/// This way of using interval arithmetic to compute bounds for a complicated expression by
/// combining the bounds for the simpler terms within the original expression allows us to
/// reason about the range of possible values of the expression. This information later can be
/// used in range pruning of the unused parts of the [RecordBatch]es.
///
/// References
/// 1 - Kabak, Mehmet Ozan. Analog Circuit Start-Up Behavior Analysis: An Interval
/// Arithmetic Based Approach. Stanford University, 2015.
/// 2 - Moore, Ramon E. Interval analysis. Vol. 4. Englewood Cliffs: Prentice-Hall, 1966.
/// 3 - F. Messine, "Deterministic global optimization using interval constraint
/// propagation techniques," RAIRO-Operations Research, vol. 38, no. 04,
/// pp. 277{293, 2004.
///
/// ``` text
/// Computing bounds for an expression using interval arithmetic.           Constraint propagation through a top-down evaluation of the expression
///                                                                         graph using inverse semantics.
///
///                                                                                 [-2, 5] ∩ [4, 4] = [4, 4]              [4, 4]
///             +-----+                        +-----+                                      +-----+                        +-----+
///        +----|  +  |----+              +----|  +  |----+                            +----|  +  |----+              +----|  +  |----+
///        |    |     |    |              |    |     |    |                            |    |     |    |              |    |     |    |
///        |    +-----+    |              |    +-----+    |                            |    +-----+    |              |    +-----+    |
///        |               |              |               |                            |               |              |               |
///    +-----+           +-----+      +-----+           +-----+                    +-----+           +-----+      +-----+           +-----+
///    |   2 |           |  y  |      |   2 | [1, 4]    |  y  |                    |   2 | [1, 4]    |  y  |      |   2 | [1, 4]    |  y  | [0, 1]*
///    |[.]  |           |     |      |[.]  |           |     |                    |[.]  |           |     |      |[.]  |           |     |
///    +-----+           +-----+      +-----+           +-----+                    +-----+           +-----+      +-----+           +-----+
///       |                              |                                            |              [-3, 1]         |
///       |                              |                                            |                              |
///     +---+                          +---+                                        +---+                          +---+
///     | x | [1, 2]                   | x | [1, 2]                                 | x | [1, 2]                   | x | [1, 2]
///     +---+                          +---+                                        +---+                          +---+
///
///  (a) Bottom-up evaluation: Step1 (b) Bottom up evaluation: Step2             (a) Top-down propagation: Step1 (b) Top-down propagation: Step2
///
///                                        [1 - 3, 4 + 1] = [-2, 5]                                                    [1 - 3, 4 + 1] = [-2, 5]
///             +-----+                        +-----+                                      +-----+                        +-----+
///        +----|  +  |----+              +----|  +  |----+                            +----|  +  |----+              +----|  +  |----+
///        |    |     |    |              |    |     |    |                            |    |     |    |              |    |     |    |
///        |    +-----+    |              |    +-----+    |                            |    +-----+    |              |    +-----+    |
///        |               |              |               |                            |               |              |               |
///    +-----+           +-----+      +-----+           +-----+                    +-----+           +-----+      +-----+           +-----+
///    |   2 |[1, 4]     |  y  |      |   2 |[1, 4]     |  y  |                    |   2 |[3, 4]**   |  y  |      |   2 |[1, 4]     |  y  |
///    |[.]  |           |     |      |[.]  |           |     |                    |[.]  |           |     |      |[.]  |           |     |
///    +-----+           +-----+      +-----+           +-----+                    +-----+           +-----+      +-----+           +-----+
///       |              [-3, 1]         |              [-3, 1]                       |              [0, 1]          |              [-3, 1]
///       |                              |                                            |                              |
///     +---+                          +---+                                        +---+                          +---+
///     | x | [1, 2]                   | x | [1, 2]                                 | x | [1, 2]                   | x | [sqrt(3), 2]***
///     +---+                          +---+                                        +---+                          +---+
///
///  (c) Bottom-up evaluation: Step3 (d) Bottom-up evaluation: Step4             (c) Top-down propagation: Step3  (d) Top-down propagation: Step4
///
///                                                                             * [-3, 1] ∩ ([4, 4] - [1, 4]) = [0, 1]
///                                                                             ** [1, 4] ∩ ([4, 4] - [0, 1]) = [3, 4]
///                                                                             *** [1, 2] ∩ [sqrt(3), sqrt(4)] = [sqrt(3), 2]
/// ```
///
#[derive(Clone)]
pub struct ExprIntervalGraph(
    NodeIndex,
    StableGraph<ExprIntervalGraphNode, usize>,
    pub Vec<(Arc<dyn PhysicalExpr>, usize)>,
);

/// This function calculates the current node interval
fn calculate_node_interval(
    parent: &Interval,
    current_child_interval: &Interval,
    negated_op: Operator,
    other_child_interval: &Interval,
) -> Result<Option<Interval>> {
    let interv = apply_operator(parent, &negated_op, other_child_interval)?;
    interv.intersect(current_child_interval)
}

impl ExprIntervalGraph {
    /// Constructs ExprIntervalGraph
    ///
    /// # Arguments
    /// * `expr` - Arc<dyn PhysicalExpr>. The complex expression that we compute bounds on by
    /// traversing the graph.
    /// * `provided_expr` - Arc<dyn PhysicalExpr> that is used for child nodes. If one of them is
    /// a Arc<dyn PhysicalExpr> with multiple child, the child nodes of this node
    /// will be pruned, since we do not need them while traversing.
    ///
    /// # Examples
    ///
    /// ```
    ///  use std::sync::Arc;
    ///  use datafusion_common::ScalarValue;
    ///  use datafusion_expr::Operator;
    ///  use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
    ///  use datafusion_physical_expr::intervals::ExprIntervalGraph;
    ///  use datafusion_physical_expr::PhysicalExpr;
    ///  // syn > gnz + 1 AND syn < gnz + 10
    ///  let syn = Arc::new(Column::new("syn", 0));
    ///  let left_and_2 = Arc::new(BinaryExpr::new(
    ///  Arc::new(Column::new("gnz", 0)),
    ///  Operator::Plus,
    ///  Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
    ///  ));
    ///  let right_and_2 = Arc::new(BinaryExpr::new(
    ///  Arc::new(Column::new("gnz", 0)),
    ///  Operator::Plus,
    ///  Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
    ///  ));
    ///  let left_expr = Arc::new(BinaryExpr::new(syn.clone(), Operator::Gt, left_and_2.clone()));
    ///  let right_expr = Arc::new(BinaryExpr::new(syn, Operator::Lt, right_and_2));
    ///  let expr = Arc::new(BinaryExpr::new(left_expr, Operator::And, right_expr));
    ///  let provided_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![left_and_2];
    ///  // You can provide exprs for child prunning.
    ///  let graph = ExprIntervalGraph::try_new(expr, provided_exprs.as_slice());
    /// ```
    pub fn try_new(
        expr: Arc<dyn PhysicalExpr>,
        provided_expr: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Self> {
        let (root_node, mut graph) =
            build_physical_expr_graph(expr, &ExprIntervalGraphNode::expr_node_builder)?;
        let mut bfs = Bfs::new(&graph, root_node);
        let mut will_be_removed = vec![];
        // We preserve the order with Vec<SortedFilterExpr> in SymmetricHashJoin.
        let mut expr_node_indices: Vec<(Arc<dyn PhysicalExpr>, usize)> = provided_expr
            .iter()
            .map(|e| (e.clone(), usize::MAX))
            .collect();
        while let Some(node) = bfs.next(&graph) {
            // Get plan
            let input = graph.index(node);
            let expr = input.expr.clone();
            // If we find a expr, remove the childs since we could not propagate the intervals there.
            if let Some(value) = provided_expr.iter().position(|e| expr.eq(e)) {
                // Remove all child nodes here, since we have information about them. If there is no child,
                // iterator will not have anything.
                let expr_node_index = expr_node_indices.get_mut(value).unwrap();
                *expr_node_index = (expr_node_index.0.clone(), node.index());
                let mut edges = graph.neighbors_directed(node, Outgoing).detach();
                while let Some(n_index) = edges.next_node(&graph) {
                    // We will not delete while traversing
                    will_be_removed.push(n_index);
                }
            }
        }
        for node in will_be_removed {
            graph.remove_node(node);
        }
        Ok(Self(root_node, graph, expr_node_indices))
    }
    /// Calculates new intervals and mutate the vector without changing the order.
    ///
    /// # Arguments
    ///
    /// * `expr_stats` - PhysicalExpr intervals. They are the child nodes for the interval calculation.
    pub fn calculate_new_intervals(
        &mut self,
        expr_stats: &mut [(usize, Interval)],
    ) -> Result<()> {
        self.post_order_interval_calculation(expr_stats)?;
        self.pre_order_interval_propagation(expr_stats)?;
        Ok(())
    }
    /// Computing bounds for an expression using interval arithmetic.
    fn post_order_interval_calculation(
        &mut self,
        expr_stats: &[(usize, Interval)],
    ) -> Result<()> {
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
                    apply_operator(&left_interval, binary.op(), &right_interval)?;
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

    pub fn pre_order_interval_propagation(
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
                    if node_interval.is_boolean() {
                        propagate_logical_operators(
                            first_child_interval,
                            binary.op(),
                            second_child_interval,
                        )?
                    } else {
                        let negated_op = negate_ops(*binary.op());
                        let new_right_operator = calculate_node_interval(
                            &node_interval,
                            second_child_interval,
                            negated_op,
                            first_child_interval,
                        )?
                        // TODO: Fix this unwrap
                        .unwrap();
                        let new_left_operator = calculate_node_interval(
                            &node_interval,
                            first_child_interval,
                            negated_op,
                            &new_right_operator,
                        )?
                        // TODO: Fix this unwrap
                        .unwrap();
                        (new_left_operator, new_right_operator)
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
}

#[derive(Clone, Debug)]
/// Graph nodes contains interval information.
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
    // Construct ExprIntervalGraphNode
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        ExprIntervalGraphNode {
            expr,
            interval: Interval::default(),
        }
    }
    /// Specify interval
    pub fn new_with_interval(expr: Arc<dyn PhysicalExpr>, interval: Interval) -> Self {
        ExprIntervalGraphNode { expr, interval }
    }
    /// Get interval
    pub fn interval(&self) -> &Interval {
        &self.interval
    }

    /// Within this static method, one can customize how PhysicalExpr generator constructs its nodes
    pub fn expr_node_builder(input: Arc<ExprTreeNode>) -> ExprIntervalGraphNode {
        let binding = input.expr();
        let plan_any = binding.as_any();
        let clone_expr = input.expr().clone();
        if let Some(literal) = plan_any.downcast_ref::<Literal>() {
            // Create interval
            let value = literal.value();
            let interval = Interval {
                lower: value.clone(),
                upper: value.clone(),
            };
            ExprIntervalGraphNode::new_with_interval(clone_expr, interval)
        } else {
            ExprIntervalGraphNode::new(clone_expr)
        }
    }
}

impl PartialEq for ExprIntervalGraphNode {
    fn eq(&self, other: &ExprIntervalGraphNode) -> bool {
        self.expr.eq(&other.expr)
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

        graph.calculate_new_intervals(&mut col_stat_nodes[..])?;
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
