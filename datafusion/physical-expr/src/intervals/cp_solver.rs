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
pub struct ExprIntervalGraph(NodeIndex, StableGraph<ExprIntervalGraphNode, usize>);

/// Result corresponding the 'update_interval' call.
#[derive(PartialEq, Debug)]
pub enum OptimizationResult {
    CannotPropagate,
    UnfeasibleSolution,
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

// This function returns the inverse operator of the given operator.
fn get_inverse_op(op: Operator) -> Operator {
    match op {
        Operator::Plus => Operator::Minus,
        Operator::Minus => Operator::Plus,
        _ => unreachable!(),
    }
}

/// This function refines intervals `left_child` and `right_child` by
/// applying constraint propagation through `parent` via operation.
///
/// The main idea is that we can shrink the domains of variables x and y by using
/// parent interval p.
///
/// Assuming that x,y and p has ranges [xL, xU], [yL, yU], and [pL, pU], we simply apply
/// constraint propagation across [xL, xU], [yL, yH] and [pL, pU].
/// - For minus operation, specifically, we would first do
///     - [xL, xU] <- ([yL, yU] + [pL, pU]) ∩ [xL, xU], and then
///     - [yL, yU] <- ([xL, xU] - [pL, pU]) ∩ [yL, yU].
/// - For plus operation, specifically, we would first do
///     - [xL, xU] <- ([pL, pU] - [yL, yU]) ∩ [xL, xU], and then
///     - [yL, yU] <- ([pL, pU] - [xL, xU]) ∩ [yL, yU].
pub fn propagate(
    op: &Operator,
    parent: &Interval,
    left_child: &Interval,
    right_child: &Interval,
) -> Result<(Option<Interval>, Option<Interval>)> {
    let inverse_op = get_inverse_op(*op);
    Ok(
        match apply_operator(&inverse_op, parent, right_child)?.intersect(left_child)? {
            // Left is feasible
            Some(value) => {
                // Adjust the propagation operator and parent-child order.
                let (o, l, r) = match op {
                    Operator::Minus => (op, &value, parent),
                    Operator::Plus => (&inverse_op, parent, &value),
                    _ => unreachable!(),
                };
                // Calculate right child with new left child.
                let right = apply_operator(o, l, r)?.intersect(right_child)?;
                // Return intervals for both children.
                (Some(value), right)
            }
            // If left child is not feasible, return both childs None.
            None => (None, None),
        },
    )
}

fn get_zero_scalar(datatype: &DataType) -> ScalarValue {
    assert!(datatype.is_primitive());
    match datatype {
        DataType::Int8 => ScalarValue::Int8(Some(0)),
        DataType::Int16 => ScalarValue::Int16(Some(0)),
        DataType::Int32 => ScalarValue::Int32(Some(0)),
        DataType::Int64 => ScalarValue::Int64(Some(0)),
        DataType::UInt8 => ScalarValue::UInt8(Some(0)),
        DataType::UInt16 => ScalarValue::UInt16(Some(0)),
        DataType::UInt32 => ScalarValue::UInt32(Some(0)),
        DataType::UInt64 => ScalarValue::UInt64(Some(0)),
        _ => unreachable!(),
    }
}

/// This function provides a target parent interval for comparison operators.
/// If we have expression > 0, expression must have the range [0, ∞].
/// If we have expression < 0, expression must have the range [-∞, 0].
/// Currently, we only support strict inequalities since open/closed intervals
/// are not implemented yet.
fn comparison_operator_target(datatype: &DataType, op: &Operator) -> Result<Interval> {
    let unbounded = ScalarValue::try_from(datatype)?;
    let zero = get_zero_scalar(datatype);
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
/// This means [x1, y1] > [x2, y2] can be rewritten as [xL, xU] - [yL, yU] = [0, ∞]
/// and [xL, xU] < [yL, yU] can be rewritten as [xL, xU] - [yL, yU] = [-∞, 0].
pub fn propagate_comparison_operators(
    op: &Operator,
    left_child: &Interval,
    right_child: &Interval,
) -> Result<(Option<Interval>, Option<Interval>)> {
    let parent = comparison_operator_target(&left_child.get_datatype(), op)?;
    propagate(&Operator::Minus, &parent, left_child, right_child)
}

impl ExprIntervalGraph {
    pub fn try_new(expr: Arc<dyn PhysicalExpr>) -> Result<Self> {
        // Build the full graph:
        let (root_node, graph) =
            build_physical_expr_graph(expr, &ExprIntervalGraphNode::make_node)?;
        Ok(Self(root_node, graph))
    }

    ///
    /// ``` text
    ///
    ///
    /// If we want to calculate intervals starting from and propagate up to BinaryExpr('a', +, 'b')
    /// instead of col('a') and col('b'), we prune under BinaryExpr('a', +, 'b') since we will
    /// never visit there.
    ///
    /// One of the reasons behind pruning the expression interval graph is that if we provide
    ///
    /// PhysicalSortExpr {
    ///     expr: BinaryExpr('a', +, 'b'),
    ///     sort_option: ..
    /// }
    ///
    /// in output_order of one of the SymmetricHashJoin's childs. In this case, we must calculate
    /// the interval for the BinaryExpr('a', +, 'b') instead of the columns inside the BinaryExpr. Thus,
    /// the child PhysicalExprs of the BinaryExpr may be pruned for the performance.
    ///
    /// We do not change the Arc<dyn PhysicalExpr> inside Arc<dyn PhysicalExpr>, just remove the nodes
    /// corresponding that Arc<dyn PhysicalExpr>.
    ///
    ///
    ///                                  +-----+                                          +-----+
    ///                                  | GT  |                                          | GT  |
    ///                         +--------|     |-------+                         +--------|     |-------+
    ///                         |        +-----+       |                         |        +-----+       |
    ///                         |                      |                         |                      |
    ///                      +-----+                   |                      +-----+                   |
    ///                      |Cast |                   |                      |Cast |                   |
    ///                      |     |                   |             --\      |     |                   |
    ///                      +-----+                   |       ----------     +-----+                   |
    ///                         |                      |             --/         |                      |
    ///                         |                      |                         |                      |
    ///                      +-----+                +-----+                   +-----+                +-----+
    ///                   +--|Plus |--+          +--|Plus |--+                |Plus |             +--|Plus |--+
    ///                   |  |     |  |          |  |     |  |                |     |             |  |     |  |
    ///  Prune from here  |  +-----+  |          |  +-----+  |                +-----+             |  +-----+  |
    ///  ------------------------------------    |           |                                    |           |
    ///                   |           |          |           |                                    |           |
    ///                +-----+     +-----+    +-----+     +-----+                              +-----+     +-----+
    ///                | a   |     |  b  |    |  c  |     |  2  |                              |  c  |     |  2  |
    ///                |     |     |     |    |     |     |     |                              |     |     |     |
    ///                +-----+     +-----+    +-----+     +-----+                              +-----+     +-----+
    ///
    /// This operation mutates the underline graph, so use it after constructor.
    /// ```
    ///
    /// Since graph is stable, the NodeIndex will stay same even if we delete a node. We exploit
    /// this stability by matching Arc<dyn PhysicalExpr> and NodeIndex for membership tests.
    pub fn pair_node_indices_with_interval_providers(
        &mut self,
        exprs: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Vec<(Arc<dyn PhysicalExpr>, usize)>> {
        let mut bfs = Bfs::new(&self.1, self.0);
        // We collect the node indexes  (usize) of the PhysicalExprs, with the same order
        // with `exprs`. To preserve order, we initiate each expr's node index with usize::MAX,
        // then find the corresponding node indexes by traversing the graph.
        let mut removals = vec![];
        let mut expr_node_indices: Vec<(Arc<dyn PhysicalExpr>, usize)> =
            exprs.iter().map(|e| (e.clone(), usize::MAX)).collect();
        while let Some(node) = bfs.next(&self.1) {
            // Get the plan corresponding to this node:
            let input = self.1.index(node);
            let expr = input.expr.clone();
            // If the current expression is among `exprs`, slate its
            // children for removal.
            if let Some(value) = exprs.iter().position(|e| expr.eq(e)) {
                // Update NodeIndex of the PhysicalExpr
                expr_node_indices[value].1 = node.index();
                let mut edges = self.1.neighbors_directed(node, Outgoing).detach();
                while let Some(n_index) = edges.next_node(&self.1) {
                    // Slate the child for removal, do not remove immediately.
                    removals.push(n_index);
                }
            }
        }
        for node in removals {
            self.1.remove_node(node);
        }
        Ok(expr_node_indices)
    }

    /// Computes bounds for an expression using interval arithmetic via a
    /// bottom-up traversal.
    /// It returns root node's interval.
    ///
    /// # Arguments
    /// * `expr_stats` - &[(usize, Interval)]. Provide NodeIndex, Interval tuples for bound evaluation.
    ///
    /// # Examples
    ///
    /// ```
    ///  use std::sync::Arc;
    ///  use datafusion_common::ScalarValue;
    ///  use datafusion_expr::Operator;
    ///  use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
    ///  use datafusion_physical_expr::intervals::ExprIntervalGraph;
    ///  use datafusion_physical_expr::intervals::interval_aritmetics::Interval;
    ///  use datafusion_physical_expr::PhysicalExpr;
    ///  let expr = Arc::new(BinaryExpr::new(
    ///             Arc::new(Column::new("gnz", 0)),
    ///             Operator::Plus,
    ///             Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
    ///         ));
    ///  let mut graph = ExprIntervalGraph::try_new(expr).unwrap();
    ///  // Do it once, while constructing.
    ///  let node_indices = graph
    ///     .pair_node_indices_with_interval_providers(&[Arc::new(Column::new("gnz", 0))])
    ///     .unwrap();
    ///  let left_index = node_indices.get(0).unwrap().1;
    ///  // Provide intervals
    ///  let intervals = vec![(
    ///     left_index,
    ///     Interval {
    ///         lower: ScalarValue::Int32(Some(10)),
    ///         upper: ScalarValue::Int32(Some(20)),
    ///         },
    ///     )];
    ///  // Evaluate bounds
    ///  assert_eq!(
    ///     graph.evaluate_bounds(&intervals).unwrap(),
    ///     Interval {
    ///         lower: ScalarValue::Int32(Some(20)),
    ///         upper: ScalarValue::Int32(Some(30))
    ///     }
    ///  )
    ///
    /// ```
    pub fn evaluate_bounds(
        &mut self,
        expr_stats: &[(usize, Interval)],
    ) -> Result<Interval> {
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
        let root_interval = self.1.index(self.0).interval.clone();
        Ok(root_interval)
    }

    /// Updates/shrinks bounds for leaf expressions using interval arithmetic
    /// via a top-down traversal.
    /// If return false, it means we have a unfeasible solution.
    fn propagate_constraints(
        &mut self,
        expr_stats: &mut [(usize, Interval)],
    ) -> Result<OptimizationResult> {
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

                if let (Some(shrink_left_interval), Some(shrink_right_interval)) =
                    // There is no propagation process for logical operators.
                    if binary.op().is_logic_operator() {
                            continue;
                        } else if binary.op().is_comparison_operator() {
                            // If comparison is strictly false, there is nothing to do for shrink.
                            if let Interval {
                                lower: ScalarValue::Boolean(Some(false)),
                                upper: ScalarValue::Boolean(Some(false)),
                            } = node_interval
                            {
                                continue;
                            }
                            // Propagate the comparison operator.
                            propagate_comparison_operators(
                                binary.op(),
                                first_child_interval,
                                second_child_interval,
                            )?
                        } else {
                            // Propagate the arithmetic operator.
                            propagate(
                                binary.op(),
                                &node_interval,
                                first_child_interval,
                                second_child_interval,
                            )?
                        }
                {
                    let mutable_first_child = self.1.index_mut(first_child_node_index);
                    mutable_first_child.interval = shrink_left_interval.clone();
                    let mutable_second_child = self.1.index_mut(second_child_node_index);
                    mutable_second_child.interval = shrink_right_interval.clone();
                } else {
                    return Ok(OptimizationResult::UnfeasibleSolution);
                };
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
        Ok(OptimizationResult::Success)
    }

    /// Updates intervals for all expressions in the DAEG by successive
    /// bottom-up and top-down traversals.
    pub fn update_intervals(
        &mut self,
        expr_stats: &mut [(usize, Interval)],
    ) -> Result<OptimizationResult> {
        self.evaluate_bounds(expr_stats)
            .and_then(|interval| match interval {
                Interval {
                    upper: ScalarValue::Boolean(Some(true)),
                    ..
                } => self.propagate_constraints(expr_stats),
                _ => Ok(OptimizationResult::CannotPropagate),
            })
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
        exprs_with_interval: (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>),
        left_interval: (Option<i32>, Option<i32>),
        right_interval: (Option<i32>, Option<i32>),
        left_waited: (Option<i32>, Option<i32>),
        right_waited: (Option<i32>, Option<i32>),
        result: OptimizationResult,
    ) -> Result<()> {
        let col_stats = vec![
            (
                exprs_with_interval.0.clone(),
                Interval {
                    lower: ScalarValue::Int32(left_interval.0),
                    upper: ScalarValue::Int32(left_interval.1),
                },
            ),
            (
                exprs_with_interval.1.clone(),
                Interval {
                    lower: ScalarValue::Int32(right_interval.0),
                    upper: ScalarValue::Int32(right_interval.1),
                },
            ),
        ];
        let expected = vec![
            (
                exprs_with_interval.0.clone(),
                Interval {
                    lower: ScalarValue::Int32(left_waited.0),
                    upper: ScalarValue::Int32(left_waited.1),
                },
            ),
            (
                exprs_with_interval.1.clone(),
                Interval {
                    lower: ScalarValue::Int32(right_waited.0),
                    upper: ScalarValue::Int32(right_waited.1),
                },
            ),
        ];
        let mut graph = ExprIntervalGraph::try_new(expr)?;
        let expr_indexes = graph.pair_node_indices_with_interval_providers(
            &col_stats.iter().map(|(e, _)| e.clone()).collect_vec(),
        )?;

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

        let exp_result = graph.update_intervals(&mut col_stat_nodes[..])?;
        assert_eq!(exp_result, result);
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
            OptimizationResult::Success,
        )?;
        Ok(())
    }

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
            (Some(10), Some(20)),
            (Some(100), None),
            (Some(10), Some(20)),
            (Some(100), None),
            OptimizationResult::CannotPropagate,
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
