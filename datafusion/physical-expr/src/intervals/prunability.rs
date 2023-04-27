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

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow_schema::{Schema, SortOptions};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Operator;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use petgraph::visit::{DfsPostOrder, EdgeRef};
use petgraph::Direction::Incoming;
use petgraph::Outgoing;

use crate::expressions::{BinaryExpr, Column, Literal};
use crate::utils::{build_dag, ExprTreeNode};
use crate::{PhysicalExpr, PhysicalSortExpr};

/// This object implements a directed acyclic expression graph (DAEG) that
/// is used to determine a [PhysicalExpr] is prunable or not, with some [PhysicalSortExpr].
#[derive(Clone)]
pub struct ExprPrunabilityGraph {
    graph: StableGraph<ExprPrunabilityGraphNode, usize>,
    root: NodeIndex,
}

/// This object is stored in nodes to move the prunable table side information to the root node.
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum TableSide {
    Left,
    Right,
    Both,
}

/// This is a node in the DAEG; it encapsulates a reference to the actual [PhysicalExpr].
/// It also stores a Option<(TableSide, SortOptions)>. It is updated by bottom-up traversal.
/// The operation at the parent node and the children nodes determines the parent's Option<(TableSide, SortOptions)>,.
/// Unordered columns are None, and ordered columns are Some() with corresponding direction.
/// TableSide shows which table the corresponding column of SortOptions comes from.
#[derive(Clone, Debug)]
pub struct ExprPrunabilityGraphNode {
    expr: Arc<dyn PhysicalExpr>,
    order: Option<(TableSide, SortOptions)>,
}

impl Display for ExprPrunabilityGraphNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Expr:{}, Order:{:?}", self.expr, self.order)
    }
}

impl ExprPrunabilityGraphNode {
    /// Constructs a new DAEG node with an unordered SortOptions.
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        ExprPrunabilityGraphNode { expr, order: None }
    }

    /// This function returns the sort type of the node, if it has any.
    pub fn order(&self) -> &Option<(TableSide, SortOptions)> {
        &self.order
    }

    /// This function creates a DAEG node from Datafusion's [ExprTreeNode] object.
    pub fn make_node(node: &ExprTreeNode<NodeIndex>) -> ExprPrunabilityGraphNode {
        let expr = node.expression().clone();
        ExprPrunabilityGraphNode::new(expr)
    }
}

impl PartialEq for ExprPrunabilityGraphNode {
    fn eq(&self, other: &ExprPrunabilityGraphNode) -> bool {
        self.expr.eq(&other.expr) && self.order.eq(&other.order)
    }
}

impl ExprPrunabilityGraph {
    pub fn try_new(expr: Arc<dyn PhysicalExpr>) -> Result<Self> {
        // Build the full graph:
        let (root, graph) = build_dag(expr, &ExprPrunabilityGraphNode::make_node)?;
        Ok(Self { graph, root })
    }

    /// This function determines the prunability of the given
    /// ExprPrunabilityGraph, with PhysicalSortExpr's of some columns.
    /// The result is returned with a tuple, the first one is the prunability
    /// of the first table, and the second one is of the second table.
    pub fn is_prunable(
        &mut self,
        sort_exprs: &[&PhysicalSortExpr],
        left_schema: &Schema,
    ) -> Result<(bool, bool)> {
        // Dfs Post Order traversal is used, since the children nodes determine the parent's order.
        let mut dfs = DfsPostOrder::new(&self.graph, self.root);
        while let Some(node) = dfs.next(&self.graph) {
            let children = self.graph.neighbors_directed(node, Outgoing);
            let mut children_order = children
                .map(|child| &self.graph[child].order)
                .collect::<Vec<_>>();

            // Leaf nodes
            if children_order.is_empty() {
                // Set initially as unordered
                self.graph[node].order = None;

                // If a column is a leaf, compare it with the elements of [PhysicalSortExpr]
                self.update_node_with_sort_information(sort_exprs, node, left_schema);
            }
            // intermediate nodes
            else {
                children_order.reverse();
                let physical_expr = self.graph[node].expr.clone();
                let binary_expr = physical_expr.as_any().downcast_ref::<BinaryExpr>().ok_or_else(|| {
                        DataFusionError::Internal("PhysicalExpr under investigation for prunability should be BinaryExpr.".to_string())
                    })?;
                match (
                    binary_expr.left().as_any().downcast_ref::<Literal>(),
                    binary_expr.right().as_any().downcast_ref::<Literal>(),
                    binary_expr.op(),
                ) {
                    // If both children are some Literal, then replace that arithmetic op node
                    // by a new literal node calculated by the BinaryExpr of original node.
                    (Some(left_child), Some(right_child), op) => {
                        if *op == Operator::Minus || *op == Operator::Plus {
                            self.reduce_literals(node, left_child, right_child, op)?;
                        } else {
                            return Err(DataFusionError::Internal(
                                "BinaryExpr has an unsupported arithmetic operator for prunability."
                                    .to_string(),
                            ));
                        }
                    }
                    (_, _, op) => {
                        if op.is_numerical_operators() {
                            self.graph[node].order =
                                numeric_node_order(children_order, binary_expr);
                        } else if op.is_comparison_operator() {
                            self.graph[node].order = comparison_node_order(
                                children_order,
                                binary_expr,
                                sort_exprs,
                                left_schema,
                            );
                        } else if *op == Operator::And {
                            self.graph[node].order = logical_node_order(children_order);
                        } else {
                            return Err(DataFusionError::Internal(
                                "BinaryExpr has an unknown logical operator for prunability."
                                    .to_string(),
                            ));
                        }
                    }
                }
            }
        }
        if let Some(res) = &self.graph[self.root].order {
            match res.0 {
                TableSide::Both => Ok((true, true)),
                TableSide::Left => Ok((true, false)),
                TableSide::Right => Ok((false, true)),
            }
        } else {
            Ok((false, false))
        }
    }

    /// This function takes a node index and the corresponging column at that node,
    /// then it scans the PhysicalSortExpr's if there is a match with that column.
    /// If a match is found, which table the column resides in is stored in the node.
    fn update_node_with_sort_information(
        &mut self,
        sort_exprs: &[&PhysicalSortExpr],
        node: NodeIndex,
        left_schema: &Schema,
    ) {
        if let Some(column) = self.graph[node].expr.as_any().downcast_ref::<Column>() {
            for sort_expr in sort_exprs {
                if let Some(sorted) = sort_expr.expr.as_any().downcast_ref::<Column>() {
                    if column == sorted {
                        let order = sort_expr.options;
                        let from = if left_schema.fields[column.index()].name()
                            == column.name()
                        {
                            TableSide::Left
                        } else {
                            TableSide::Right
                        };
                        self.graph[node].order = Some((from, order));
                        break;
                    }
                }
            }
        }
    }

    /// This function takes one node, that is an + or - node,
    /// and 2 child nodes of that arithmetic node, those are Literal values.
    /// It evaluates the operation, and replace the arithmetic node with
    /// a new Literal node. It both modifies the expression and interval value of the node.
    /// Then, if there does not exist any edge incoming to the replaced nodes,
    /// they would be removed. The edges, in both cases, are removed.
    pub fn reduce_literals(
        &mut self,
        node: NodeIndex,
        left_child: &Literal,
        right_child: &Literal,
        op: &Operator,
    ) -> Result<()> {
        let children = self
            .graph
            .neighbors_directed(node, Outgoing)
            .collect::<Vec<_>>();
        if *op == Operator::Plus {
            self.graph[node].expr =
                Arc::new(Literal::new(left_child.value().add(right_child.value())?));
        } else if *op == Operator::Minus {
            self.graph[node].expr =
                Arc::new(Literal::new(left_child.value().sub(right_child.value())?));
        } else {
            return Err(DataFusionError::Internal(
                "BinaryExpr has an unknown Literal operator for prunability.".to_string(),
            ));
        }
        let mut removals = vec![];
        for edge in self.graph.edges_directed(node, Outgoing) {
            removals.push(edge.id());
        }
        for edge_idx in removals {
            self.graph.remove_edge(edge_idx);
        }

        if self
            .graph
            .edges_directed(children[0], Incoming)
            .collect::<Vec<_>>()
            .is_empty()
        {
            self.graph.remove_node(children[0]);
        }
        if self
            .graph
            .edges_directed(children[1], Incoming)
            .collect::<Vec<_>>()
            .is_empty()
        {
            self.graph.remove_node(children[1]);
        }

        Ok(())
    }
}

fn numeric_node_order(
    children_order: Vec<&Option<(TableSide, SortOptions)>>,
    binary_expr: &BinaryExpr,
) -> Option<(TableSide, SortOptions)> {
    match (
        binary_expr.left().as_any().downcast_ref::<Literal>(),
        binary_expr.right().as_any().downcast_ref::<Literal>(),
        binary_expr.op(),
    ) {
        // Literal + some column
        (Some(_), _, Operator::Plus) => *children_order[1],
        // Literal - some column
        (Some(_), _, Operator::Minus) => {
            // if ordered column, reverse the order, otherwise unordered column
            children_order[1].as_ref().map(|(side, sort_options)| {
                (
                    *side,
                    SortOptions {
                        descending: !sort_options.descending,
                        nulls_first: sort_options.nulls_first,
                    },
                )
            })
        }
        // Some column + - literal
        (_, Some(_), Operator::Minus | Operator::Plus) => *children_order[0],
        // Column + - column
        (_, _, op) => match (children_order[0], children_order[1], op) {
            // Ordered + ordered column
            (Some((_, left_ordering)), Some((_, right_ordering)), Operator::Plus)
                if left_ordering == right_ordering =>
            {
                Some((TableSide::Both, *left_ordering))
            }
            // Ordered - ordered column
            (Some((_, left_ordering)), Some((_, right_ordering)), Operator::Minus)
                if (left_ordering.descending != right_ordering.descending)
                    && (left_ordering.nulls_first == right_ordering.nulls_first) =>
            {
                Some((TableSide::Both, *left_ordering))
            }
            // Unordered + - ordered column
            // Ordered + - unordered column
            (_, _, _) => None,
        },
    }
}

fn comparison_node_order(
    children_order: Vec<&Option<(TableSide, SortOptions)>>,
    binary_expr: &BinaryExpr,
    sort_exprs: &[&PhysicalSortExpr],
    left_schema: &Schema,
) -> Option<(TableSide, SortOptions)> {
    // There may be SortOptions like (a + b) sorted.
    // This part handles such expressions.
    for sort_expr in sort_exprs {
        if let Some(binary_expr_sorted) =
            sort_expr.expr.as_any().downcast_ref::<BinaryExpr>()
        {
            if binary_expr.eq(binary_expr_sorted) {
                let order = sort_expr.options;
                if let Some(column_left) =
                    binary_expr.left().as_any().downcast_ref::<Column>()
                {
                    if left_schema.fields[column_left.index()].name()
                        == column_left.name()
                    {
                        if let Some(column_right) =
                            binary_expr.right().as_any().downcast_ref::<Column>()
                        {
                            if left_schema.fields[column_right.index()].name()
                                == column_right.name()
                            {
                                // In this case, it means that a and b are from
                                // different tables, prunability is not possible.
                                return None;
                            } else {
                                // a and b are coming from the same table.
                                return Some((TableSide::Left, order));
                            }
                        }
                    } else if let Some(column_right) =
                        binary_expr.right().as_any().downcast_ref::<Column>()
                    {
                        {
                            if left_schema.fields[column_right.index()].name()
                                == column_right.name()
                            {
                                // a and b are coming from the same table.
                                return Some((TableSide::Left, order));
                            } else {
                                // In this case, it means that a and b are from
                                // different tables, prunability is not possible.
                                return None;
                            }
                        }
                    }
                };
            }
        }
    }
    match (
        binary_expr.left().as_any().downcast_ref::<Literal>(),
        binary_expr.right().as_any().downcast_ref::<Literal>(),
        binary_expr.op(),
    ) {
        // Literal cmp some column or Some column cmp literal
        (Some(_), _, _) | (_, Some(_), _) => None,
        // Column cmp column
        (_, _, op) => match (children_order[0], children_order[1], op) {
            // Ordered + ordered column
            (Some(left), Some(right), op) => {
                match (left.1.descending, right.1.descending, op) {
                    // In these matches, TableSide is important rather than SortOptions.
                    // Left and Right returns are assigned accordingly.
                    (false, false, Operator::Gt)
                    | (false, false, Operator::GtEq)
                    | (true, true, Operator::Lt)
                    | (true, true, Operator::LtEq) => Some(*left),
                    (true, true, Operator::Gt)
                    | (true, true, Operator::GtEq)
                    | (false, false, Operator::Lt)
                    | (false, false, Operator::LtEq) => Some(*right),
                    (_, _, _) => None,
                }
            }
            // Unordered cmp ordered column
            // Ordered cmp unordered column
            (_, _, _) => None,
        },
    }
}

fn logical_node_order(
    children_order: Vec<&Option<(TableSide, SortOptions)>>,
) -> Option<(TableSide, SortOptions)> {
    match (children_order[0], children_order[1]) {
        (None, Some(right)) => Some(*right),
        (Some(left), None) => Some(*left),
        (Some(left), Some(right)) => {
            if (left.0 == TableSide::Left && right.0 == TableSide::Right)
                || (left.0 == TableSide::Right && right.0 == TableSide::Left)
            {
                Some((TableSide::Both, left.1))
            } else {
                Some(*left)
            }
        }
        (_, _) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, BinaryExpr};
    use arrow_schema::{DataType, Field};
    use datafusion_common::ScalarValue;
    use std::ops::Not;

    #[test]
    fn test_reduce_literals() -> Result<()> {
        // Create a new ExprPrunabilityGraph
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(Literal::new(ScalarValue::Int32(Some(5)))),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        ));
        let mut graph = ExprPrunabilityGraph::try_new(expr)?;

        // Reduce the Plus node
        graph.reduce_literals(
            graph.root,
            &Literal::new(ScalarValue::Int32(Some(5))),
            &Literal::new(ScalarValue::Int32(Some(10))),
            &Operator::Plus,
        )?;

        // Check that the Plus node has been replaced with a new Literal node with value 15
        let literal_result = graph.graph[graph.root]
            .expr
            .as_any()
            .downcast_ref::<Literal>();
        assert_eq!(
            literal_result,
            Some(&(Literal::new(ScalarValue::Int32(Some(15)))))
        );
        assert_eq!(graph.graph.edge_count(), 0);
        assert_eq!(graph.graph.node_count(), 1);

        Ok(())
    }

    #[test]
    fn test_reduce_literals_shared_child() -> Result<()> {
        // Create a new ExprPrunabilityGraph with an additional edge
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(Literal::new(ScalarValue::Int32(Some(5)))),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        ));
        let mut graph = ExprPrunabilityGraph::try_new(expr)?;
        if let Some(index_child) =
            DfsPostOrder::new(&graph.graph, graph.root).next(&graph.graph)
        {
            let new_node = graph.graph.add_node(ExprPrunabilityGraphNode::new(Arc::new(
                Literal::new(ScalarValue::Int32(Some(5))),
            )));
            graph.graph.add_edge(new_node, index_child, 0);
        }

        // Reduce the Plus node
        graph.reduce_literals(
            graph.root,
            &Literal::new(ScalarValue::Int32(Some(5))),
            &Literal::new(ScalarValue::Int32(Some(10))),
            &Operator::Plus,
        )?;

        // Check that the Plus node has been replaced with a new Literal node with value 15
        let literal_result = graph.graph[graph.root]
            .expr
            .as_any()
            .downcast_ref::<Literal>();
        assert_eq!(
            literal_result,
            Some(&(Literal::new(ScalarValue::Int32(Some(15)))))
        );
        // The edge added later will remain
        assert_eq!(graph.graph.edge_count(), 1);
        // Shared node is not removed
        assert_eq!(graph.graph.node_count(), 3);

        Ok(())
    }

    fn experiment_prunability(
        schema_left: &Schema,
        schema_right: &Schema,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<()> {
        let left_sorted_asc = PhysicalSortExpr {
            expr: col("left_column", schema_left)?,
            options: SortOptions::default(),
        };
        let right_sorted_asc = PhysicalSortExpr {
            expr: col("right_column", schema_right)?,
            options: SortOptions::default(),
        };
        let left_sorted_desc = PhysicalSortExpr {
            expr: col("left_column", schema_left)?,
            options: SortOptions::default().not(),
        };
        let right_sorted_desc = PhysicalSortExpr {
            expr: col("right_column", schema_right)?,
            options: SortOptions::default().not(),
        };

        let mut graph = ExprPrunabilityGraph::try_new(expr)?;

        assert_eq!(
            (true, true),
            graph.is_prunable(&[&left_sorted_asc, &right_sorted_asc], schema_left)?
        );
        assert_eq!(
            (true, true),
            graph.is_prunable(&[&left_sorted_desc, &right_sorted_desc], schema_left)?
        );
        assert_eq!(
            (false, false),
            graph.is_prunable(&[&left_sorted_asc], schema_left)?
        );
        assert_eq!(
            (false, false),
            graph.is_prunable(&[&left_sorted_asc, &right_sorted_desc], schema_left)?
        );
        assert_eq!((false, false), graph.is_prunable(&[], schema_left)?);

        Ok(())
    }

    fn experiment_prunability_four_columns(
        schema_left: &Schema,
        schema_right: &Schema,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<()> {
        let left_sorted1_asc = PhysicalSortExpr {
            expr: col("left_column1", schema_left)?,
            options: SortOptions::default(),
        };
        let right_sorted1_asc = PhysicalSortExpr {
            expr: col("right_column1", schema_right)?,
            options: SortOptions::default(),
        };
        let left_sorted1_desc = PhysicalSortExpr {
            expr: col("left_column1", schema_left)?,
            options: SortOptions::default().not(),
        };
        let right_sorted1_desc = PhysicalSortExpr {
            expr: col("right_column1", schema_right)?,
            options: SortOptions::default().not(),
        };
        let left_sorted2_asc = PhysicalSortExpr {
            expr: col("left_column2", schema_left)?,
            options: SortOptions::default(),
        };
        let right_sorted2_asc = PhysicalSortExpr {
            expr: col("right_column2", schema_right)?,
            options: SortOptions::default(),
        };
        let left_sorted2_desc = PhysicalSortExpr {
            expr: col("left_column2", schema_left)?,
            options: SortOptions::default().not(),
        };
        let right_sorted2_desc = PhysicalSortExpr {
            expr: col("right_column2", schema_right)?,
            options: SortOptions::default().not(),
        };

        let mut graph = ExprPrunabilityGraph::try_new(expr)?;

        assert_eq!(
            (true, true),
            graph.is_prunable(
                &[
                    &left_sorted1_asc,
                    &right_sorted1_desc,
                    &left_sorted2_asc,
                    &right_sorted2_asc
                ],
                schema_left
            )?
        );
        assert_eq!(
            (true, false),
            graph.is_prunable(&[&left_sorted1_asc, &right_sorted1_desc,], schema_left)?
        );
        assert_eq!(
            (false, true),
            graph.is_prunable(&[&left_sorted2_asc, &right_sorted2_asc,], schema_left)?
        );
        assert_eq!(
            (true, false),
            graph
                .is_prunable(&[&left_sorted2_desc, &right_sorted2_desc,], schema_left)?
        );
        assert_eq!(
            (false, true),
            graph.is_prunable(&[&left_sorted1_desc, &right_sorted1_asc,], schema_left)?
        );
        assert_eq!(
            (false, true),
            graph.is_prunable(
                &[
                    &left_sorted1_asc,
                    &right_sorted1_asc,
                    &left_sorted2_asc,
                    &right_sorted2_asc
                ],
                schema_left
            )?
        );
        assert_eq!(
            (true, false),
            graph.is_prunable(
                &[
                    &left_sorted1_desc,
                    &right_sorted1_desc,
                    &left_sorted2_desc,
                    &right_sorted2_desc
                ],
                schema_left
            )?
        );
        assert_eq!(
            (false, false),
            graph.is_prunable(
                &[
                    &left_sorted1_desc,
                    &right_sorted1_desc,
                    &left_sorted2_asc,
                    &right_sorted2_desc
                ],
                schema_left
            )?
        );
        assert_eq!(
            (false, false),
            graph.is_prunable(
                &[
                    &left_sorted1_asc,
                    &right_sorted1_asc,
                    &left_sorted2_asc,
                    &right_sorted2_desc
                ],
                schema_left
            )?
        );
        assert_eq!((false, false), graph.is_prunable(&[], schema_left)?);

        Ok(())
    }

    #[test]
    fn test_prunability_symmetric_graph() -> Result<()> {
        // Create 2 schemas having an interger column
        let schema_left =
            Schema::new(vec![Field::new("left_column", DataType::Int32, true)]);
        let schema_right =
            Schema::new(vec![Field::new("right_column", DataType::Int32, true)]);

        // ( (left_column + 1) > (right_column + 2) ) AND ( (left_column + 3) < (right_column + 4) )
        let left_col = col("left_column", &schema_left)?;
        let right_col = col("right_column", &schema_right)?;
        let left_and_1 = Arc::new(BinaryExpr::new(
            left_col.clone(),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ));
        let left_and_2 = Arc::new(BinaryExpr::new(
            right_col.clone(),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
        ));
        let right_and_1 = Arc::new(BinaryExpr::new(
            left_col,
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(3)))),
        ));
        let right_and_2 = Arc::new(BinaryExpr::new(
            right_col,
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(4)))),
        ));
        let left_expr = Arc::new(BinaryExpr::new(left_and_1, Operator::Gt, left_and_2));
        let right_expr =
            Arc::new(BinaryExpr::new(right_and_1, Operator::Lt, right_and_2));
        let expr = Arc::new(BinaryExpr::new(left_expr, Operator::And, right_expr));

        experiment_prunability(&schema_left, &schema_right, expr)?;

        Ok(())
    }

    #[test]
    fn test_prunability_asymmetric_graph() -> Result<()> {
        // Create 2 schemas having an interger column
        let schema_left =
            Schema::new(vec![Field::new("left_column", DataType::Int32, true)]);
        let schema_right =
            Schema::new(vec![Field::new("right_column", DataType::Int32, true)]);

        // ( ((left_column + 1) + 3) >= ((right_column + 2) + 4) ) AND ( (left_column) <= (right_column) )
        let left_col = col("left_column", &schema_left)?;
        let right_col = col("right_column", &schema_right)?;
        let left_and_1_inner = Arc::new(BinaryExpr::new(
            left_col.clone(),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ));
        let left_and_1 = Arc::new(BinaryExpr::new(
            left_and_1_inner,
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(3)))),
        ));
        let left_and_2_inner = Arc::new(BinaryExpr::new(
            right_col.clone(),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
        ));
        let left_and_2 = Arc::new(BinaryExpr::new(
            left_and_2_inner,
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(4)))),
        ));
        let left_expr = Arc::new(BinaryExpr::new(left_and_1, Operator::GtEq, left_and_2));
        let right_expr = Arc::new(BinaryExpr::new(left_col, Operator::LtEq, right_col));
        let expr = Arc::new(BinaryExpr::new(left_expr, Operator::And, right_expr));

        experiment_prunability(&schema_left, &schema_right, expr)?;

        Ok(())
    }

    #[test]
    fn test_prunability_more_columns() -> Result<()> {
        // Create 2 schemas having two interger columns
        let schema_left = Schema::new(vec![
            Field::new("left_column1", DataType::Int32, true),
            Field::new("left_column2", DataType::Int32, true),
        ]);
        let schema_right = Schema::new(vec![
            Field::new("right_column1", DataType::Int32, true),
            Field::new("right_column2", DataType::Int32, true),
        ]);

        // ( (left_column1 + 1) > (2 - right_column1) ) AND ( (left_column2 + 3) < (right_column2 - 4) )
        let left_col1 = col("left_column1", &schema_left)?;
        let right_col1 = col("right_column1", &schema_right)?;
        let left_col2 = col("left_column2", &schema_left)?;
        let right_col2 = col("right_column2", &schema_right)?;
        let left_and_1 = Arc::new(BinaryExpr::new(
            left_col1.clone(),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ));
        let left_and_2 = Arc::new(BinaryExpr::new(
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
            Operator::Minus,
            right_col1.clone(),
        ));
        let right_and_1 = Arc::new(BinaryExpr::new(
            left_col2.clone(),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(3)))),
        ));
        let right_and_2 = Arc::new(BinaryExpr::new(
            right_col2.clone(),
            Operator::Minus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(4)))),
        ));
        let left_expr = Arc::new(BinaryExpr::new(left_and_1, Operator::Gt, left_and_2));
        let right_expr =
            Arc::new(BinaryExpr::new(right_and_1, Operator::Lt, right_and_2));
        let expr = Arc::new(BinaryExpr::new(left_expr, Operator::And, right_expr));

        experiment_prunability_four_columns(&schema_left, &schema_right, expr)?;

        Ok(())
    }
}
