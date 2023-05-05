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

use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Operator;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use petgraph::visit::DfsPostOrder;
use petgraph::Outgoing;

use crate::expressions::{BinaryExpr, Column};
use crate::utils::{build_dag, ExprTreeNode};
use crate::{PhysicalExpr, PhysicalSortExpr};

/// This object implements a directed acyclic expression graph (DAEG) that
/// is used to determine a [PhysicalExpr] is prunable or not, with some [PhysicalSortExpr].
/// If it is so, prunable table side can be inferred. Non-prunability implies that the overall
/// binary expression may be monotonically increasing or decreasing. Null placement of binary
/// expression can be examined, too.
#[derive(Clone)]
pub struct ExprPrunabilityGraph {
    graph: StableGraph<ExprPrunabilityGraphNode, usize>,
    root: NodeIndex,
}

/// This object is stored in nodes to move the prunable table side information to the root node.
/// The column leafs are assigned with TableSide info. If there is not any prunability blocking
/// conditions at the parent nodes, this info reaches the root node having overall expression.
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum TableSide {
    None,
    Left,
    Right,
    Both,
}

/// This object is stored in nodes to find the root binary expression's
/// monotonicity according to the columns on its leaf nodes.
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum Monotonicity {
    Asc,
    Desc,
    Unordered,
    Singleton,
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub struct SortInfo {
    dir: Monotonicity,
    nulls_first: bool,
    nulls_last: bool,
}

#[derive(Clone, Debug)]
pub struct ExprPrunabilityGraphNode {
    expr: Arc<dyn PhysicalExpr>,
    table_side: TableSide,
    sort_info: SortInfo,
}

impl Display for ExprPrunabilityGraphNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Expr:{}, TableSide:{:?}, SortInfo:{:?}",
            self.expr, self.table_side, self.sort_info
        )
    }
}

impl ExprPrunabilityGraphNode {
    /// Constructs a new DAEG node with an unordered SortInfo.
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        ExprPrunabilityGraphNode {
            expr,
            table_side: TableSide::None,
            sort_info: SortInfo {
                dir: Monotonicity::Unordered,
                nulls_first: false,
                nulls_last: false,
            },
        }
    }

    /// This function creates a DAEG node from Datafusion's [ExprTreeNode] object.
    pub fn make_node(node: &ExprTreeNode<NodeIndex>) -> ExprPrunabilityGraphNode {
        let expr = node.expression().clone();
        ExprPrunabilityGraphNode::new(expr)
    }
}

impl PartialEq for ExprPrunabilityGraphNode {
    fn eq(&self, other: &ExprPrunabilityGraphNode) -> bool {
        self.expr.eq(&other.expr)
            && self.table_side.eq(&other.table_side)
            && self.sort_info.eq(&other.sort_info)
    }
}

impl ExprPrunabilityGraph {
    pub fn try_new(expr: Arc<dyn PhysicalExpr>) -> Result<Self> {
        // Build the full graph:
        let (root, graph) = build_dag(expr, &ExprPrunabilityGraphNode::make_node)?;
        Ok(Self { graph, root })
    }

    /// This function traverse the whole graph. It returns the prunable table sides,
    /// and sort information of binary expression that builds the graph.
    pub fn analyze_prunability(
        &mut self,
        left_sort_expr: &Option<PhysicalSortExpr>,
        right_sort_expr: &Option<PhysicalSortExpr>,
    ) -> Result<(TableSide, SortInfo)> {
        // Dfs Post Order traversal is used, since the children nodes determine the parent's order.
        let mut dfs = DfsPostOrder::new(&self.graph, self.root);
        while let Some(node) = dfs.next(&self.graph) {
            let children = self.graph.neighbors_directed(node, Outgoing);
            let mut children_prunability = children
                .map(|child| {
                    (&self.graph[child].table_side, &self.graph[child].sort_info)
                })
                .collect::<Vec<_>>();

            // Leaf nodes
            if children_prunability.is_empty() {
                // Set initially as unordered column with undefined table side
                self.graph[node].table_side = TableSide::None;
                self.graph[node].sort_info.dir = Monotonicity::Unordered;
                self.graph[node].sort_info.nulls_first = false;
                self.graph[node].sort_info.nulls_last = false;

                self.update_node_with_sort_information(
                    left_sort_expr,
                    right_sort_expr,
                    node,
                );
            }
            // intermediate nodes
            else {
                // Children are indexed as 1.->right child, 2.->left child.
                // To have more natural placement, they are reversed.
                children_prunability.reverse();
                let physical_expr = self.graph[node].expr.clone();
                let binary_expr = physical_expr.as_any().downcast_ref::<BinaryExpr>().ok_or_else(|| {
                        DataFusionError::Internal("PhysicalExpr under investigation for prunability should be BinaryExpr.".to_string())
                    })?;
                if binary_expr.op().is_numerical_operators() {
                    (self.graph[node].table_side, self.graph[node].sort_info) =
                        numeric_node_order(
                            &children_prunability,
                            binary_expr,
                            left_sort_expr,
                            right_sort_expr,
                        );
                } else if binary_expr.op().is_comparison_operator() {
                    (self.graph[node].table_side, self.graph[node].sort_info) =
                        comparison_node_order(&children_prunability, binary_expr.op());
                } else if *binary_expr.op() == Operator::And {
                    (self.graph[node].table_side, self.graph[node].sort_info) =
                        logical_node_order(&children_prunability);
                } else {
                    return Err(DataFusionError::Internal(
                        "BinaryExpr has an unknown logical operator for prunability."
                            .to_string(),
                    ));
                }
            }
        }
        Ok((
            self.graph[self.root].table_side,
            self.graph[self.root].sort_info,
        ))
    }

    /// This function takes a node index, possibly there is a column at that node.
    /// Then, it scans the PhysicalSortExpr's if there is a match with that column.
    /// If a match is found, TableSide and SortInfo of the node is set accordingly.
    fn update_node_with_sort_information(
        &mut self,
        left_sort_expr: &Option<PhysicalSortExpr>,
        right_sort_expr: &Option<PhysicalSortExpr>,
        node: NodeIndex,
    ) {
        if let Some(column) = self.clone().graph[node]
            .expr
            .as_any()
            .downcast_ref::<Column>()
        {
            for (i, sort_expr) in [left_sort_expr, right_sort_expr]
                .into_iter()
                .flatten()
                .enumerate()
            {
                // SortExpr's in the form of BinaryExpr is handled in intermediate nodes.
                // We need only to check Column-wise SortExpr's.
                if let Some(sorted) = sort_expr.expr.as_any().downcast_ref::<Column>() {
                    if *column == *sorted {
                        if i == 0 {
                            self.graph[node].table_side = TableSide::Left
                        } else {
                            self.graph[node].table_side = TableSide::Right
                        };
                        match sort_expr.options.descending {
                            false => {
                                self.graph[node].sort_info = SortInfo {
                                    dir: Monotonicity::Asc,
                                    nulls_first: sort_expr.options.nulls_first,
                                    nulls_last: !sort_expr.options.nulls_first,
                                };
                            }
                            true => {
                                self.graph[node].sort_info = SortInfo {
                                    dir: Monotonicity::Desc,
                                    nulls_first: sort_expr.options.nulls_first,
                                    nulls_last: !sort_expr.options.nulls_first,
                                };
                            }
                        }
                        break;
                    }
                    self.graph[node].sort_info = SortInfo {
                        dir: Monotonicity::Unordered,
                        // default of nulls first
                        nulls_first: true,
                        nulls_last: false,
                    };
                }
            }
        } else {
            // If the leaf node is not a column, it must be a literal.
            // Its table side is initialized as None, no need to change it.
            self.graph[node].sort_info = SortInfo {
                dir: Monotonicity::Singleton,
                nulls_first: false,
                nulls_last: false,
            };
        }
    }
}

fn numeric_node_order(
    children: &[(&TableSide, &SortInfo)],
    binary_expr: &BinaryExpr,
    left_sort_expr: &Option<PhysicalSortExpr>,
    right_sort_expr: &Option<PhysicalSortExpr>,
) -> (TableSide, SortInfo) {
    // There may be SortOptions for BinaryExpr's like (a + b) sorted.
    // This part handles such expressions.
    for (i, sort_expr) in [left_sort_expr, right_sort_expr]
        .into_iter()
        .flatten()
        .enumerate()
    {
        if let Some(binary_expr_sorted) =
            sort_expr.expr.as_any().downcast_ref::<BinaryExpr>()
        {
            if binary_expr.eq(binary_expr_sorted) {
                let from = if i == 0 {
                    TableSide::Left
                } else {
                    TableSide::Right
                };
                let dir = if sort_expr.options.descending {
                    Monotonicity::Desc
                } else {
                    Monotonicity::Asc
                };
                let nulls_first = sort_expr.options.nulls_first;
                return (
                    from,
                    SortInfo {
                        dir,
                        nulls_first,
                        nulls_last: !nulls_first,
                    },
                );
            }
        }
    }
    match (children[0].1.dir, children[1].1.dir, binary_expr.op()) {
        // Literal + - Literal
        (Monotonicity::Singleton, Monotonicity::Singleton, _) => (
            TableSide::None,
            SortInfo {
                dir: Monotonicity::Singleton,
                nulls_first: false,
                nulls_last: false,
            },
        ),
        // Literal + some column
        (Monotonicity::Singleton, _, Operator::Plus) => (*children[1].0, *children[1].1),
        // Literal - some column
        (Monotonicity::Singleton, _, Operator::Minus) => {
            // if ordered column, reverse the order, otherwise unordered column
            let order = if children[1].1.dir == Monotonicity::Asc {
                Monotonicity::Desc
            } else if children[1].1.dir == Monotonicity::Desc {
                Monotonicity::Asc
            } else {
                Monotonicity::Unordered
            };
            (
                *children[1].0,
                SortInfo {
                    dir: order,
                    nulls_first: children[1].1.nulls_first,
                    nulls_last: children[1].1.nulls_last,
                },
            )
        }
        // Some column + - literal
        (_, Monotonicity::Singleton, Operator::Minus | Operator::Plus) => {
            (*children[0].0, *children[0].1)
        }
        // Column + - column
        (_, _, op) => {
            let from = match (children[0].0, children[1].0) {
                (left_child_side, right_child_side)
                    if left_child_side == right_child_side =>
                {
                    *left_child_side
                }
                (_, _) => TableSide::None,
            };
            let dir = match (children[0].1.dir, children[1].1.dir, op) {
                (Monotonicity::Asc, Monotonicity::Asc, Operator::Plus)
                | (Monotonicity::Asc, Monotonicity::Desc, Operator::Minus) => {
                    Monotonicity::Asc
                }
                (Monotonicity::Desc, Monotonicity::Desc, Operator::Plus)
                | (Monotonicity::Desc, Monotonicity::Asc, Operator::Minus) => {
                    Monotonicity::Desc
                }
                (_, _, _) => Monotonicity::Unordered,
            };
            let nulls_first = children[0].1.nulls_first || children[1].1.nulls_first;
            let nulls_last = children[0].1.nulls_last || children[1].1.nulls_last;
            (
                from,
                SortInfo {
                    dir,
                    nulls_first,
                    nulls_last,
                },
            )
        }
    }
}

fn comparison_node_order(
    children: &[(&TableSide, &SortInfo)],
    binary_expr_op: &Operator,
) -> (TableSide, SortInfo) {
    match (children[0].1.dir, children[1].1.dir, binary_expr_op) {
        // Literal > (>=) some column
        (Monotonicity::Singleton, _, Operator::Gt)
        | (Monotonicity::Singleton, _, Operator::GtEq) => {
            let dir = match children[1].1.dir {
                Monotonicity::Asc => Monotonicity::Desc,
                Monotonicity::Desc => Monotonicity::Asc,
                _ => Monotonicity::Unordered,
            };
            (
                TableSide::None,
                SortInfo {
                    dir,
                    nulls_first: children[1].1.nulls_last,
                    nulls_last: children[1].1.nulls_last,
                },
            )
        }
        // Literal < (<=) some column
        (Monotonicity::Singleton, _, Operator::Lt)
        | (Monotonicity::Singleton, _, Operator::LtEq) => {
            let dir = match children[1].1.dir {
                Monotonicity::Asc => Monotonicity::Asc,
                Monotonicity::Desc => Monotonicity::Desc,
                _ => Monotonicity::Unordered,
            };
            (
                TableSide::None,
                SortInfo {
                    dir,
                    nulls_first: children[1].1.nulls_last,
                    nulls_last: children[1].1.nulls_last,
                },
            )
        }
        // Some column > (>=) literal
        (_, Monotonicity::Singleton, Operator::Gt)
        | (_, Monotonicity::Singleton, Operator::GtEq) => {
            let dir = match children[1].1.dir {
                Monotonicity::Asc => Monotonicity::Asc,
                Monotonicity::Desc => Monotonicity::Desc,
                _ => Monotonicity::Unordered,
            };
            (
                TableSide::None,
                SortInfo {
                    dir,
                    nulls_first: children[1].1.nulls_last,
                    nulls_last: children[1].1.nulls_last,
                },
            )
        }
        // Some column < (<=) literal
        (_, Monotonicity::Singleton, Operator::Lt)
        | (_, Monotonicity::Singleton, Operator::LtEq) => {
            let dir = match children[1].1.dir {
                Monotonicity::Asc => Monotonicity::Desc,
                Monotonicity::Desc => Monotonicity::Asc,
                _ => Monotonicity::Unordered,
            };
            (
                TableSide::None,
                SortInfo {
                    dir,
                    nulls_first: children[1].1.nulls_last,
                    nulls_last: children[1].1.nulls_last,
                },
            )
        }
        // Column cmp column
        (_, _, op) => {
            let nulls_first = children[0].1.nulls_first | children[1].1.nulls_first;
            let nulls_last = children[0].1.nulls_last | children[1].1.nulls_last;
            let table_side = match (children[0].1.dir, children[1].1.dir, op) {
                (Monotonicity::Asc, Monotonicity::Asc, Operator::Gt)
                | (Monotonicity::Asc, Monotonicity::Asc, Operator::GtEq)
                | (Monotonicity::Desc, Monotonicity::Desc, Operator::Lt)
                | (Monotonicity::Desc, Monotonicity::Desc, Operator::LtEq) => {
                    *children[0].0
                }
                (Monotonicity::Asc, Monotonicity::Asc, Operator::Lt)
                | (Monotonicity::Asc, Monotonicity::Asc, Operator::LtEq)
                | (Monotonicity::Desc, Monotonicity::Desc, Operator::Gt)
                | (Monotonicity::Desc, Monotonicity::Desc, Operator::GtEq) => {
                    *children[1].0
                }
                (_, _, _) => TableSide::None,
            };
            match (children[0].1.dir, children[1].1.dir, op) {
                (Monotonicity::Asc, Monotonicity::Desc, Operator::Gt)
                | (Monotonicity::Asc, Monotonicity::Desc, Operator::GtEq)
                | (Monotonicity::Desc, Monotonicity::Asc, Operator::Lt)
                | (Monotonicity::Desc, Monotonicity::Asc, Operator::LtEq) => (
                    table_side,
                    SortInfo {
                        dir: Monotonicity::Asc,
                        nulls_first,
                        nulls_last,
                    },
                ),
                (Monotonicity::Asc, Monotonicity::Desc, Operator::Lt)
                | (Monotonicity::Asc, Monotonicity::Desc, Operator::LtEq)
                | (Monotonicity::Desc, Monotonicity::Asc, Operator::Gt)
                | (Monotonicity::Desc, Monotonicity::Asc, Operator::GtEq) => (
                    table_side,
                    SortInfo {
                        dir: Monotonicity::Desc,
                        nulls_first,
                        nulls_last,
                    },
                ),
                (_, _, _) => (
                    table_side,
                    SortInfo {
                        dir: Monotonicity::Unordered,
                        nulls_first,
                        nulls_last,
                    },
                ),
            }
        }
    }
}

fn logical_node_order(children: &[(&TableSide, &SortInfo)]) -> (TableSide, SortInfo) {
    let from = match (children[0].0, children[1].0) {
        (TableSide::Left, TableSide::Right) | (TableSide::Right, TableSide::Left) => {
            TableSide::Both
        }
        (TableSide::Left, _) | (_, TableSide::Left) => TableSide::Left,
        (TableSide::Right, _) | (_, TableSide::Right) => TableSide::Right,
        (_, _) => TableSide::None,
    };
    match (children[0].1.dir, children[1].1.dir) {
        (Monotonicity::Asc, Monotonicity::Asc)
        | (Monotonicity::Asc, Monotonicity::Singleton)
        | (Monotonicity::Singleton, Monotonicity::Asc) => (
            from,
            SortInfo {
                dir: Monotonicity::Asc,
                nulls_first: children[0].1.nulls_first | children[1].1.nulls_first,
                nulls_last: children[0].1.nulls_last | children[1].1.nulls_last,
            },
        ),
        (Monotonicity::Desc, Monotonicity::Desc)
        | (Monotonicity::Desc, Monotonicity::Singleton)
        | (Monotonicity::Singleton, Monotonicity::Desc) => (
            from,
            SortInfo {
                dir: Monotonicity::Desc,
                nulls_first: children[0].1.nulls_first | children[1].1.nulls_first,
                nulls_last: children[0].1.nulls_last | children[1].1.nulls_last,
            },
        ),
        (Monotonicity::Singleton, Monotonicity::Singleton) => (
            from,
            SortInfo {
                dir: Monotonicity::Singleton,
                nulls_first: children[0].1.nulls_first | children[1].1.nulls_first,
                nulls_last: children[0].1.nulls_last | children[1].1.nulls_last,
            },
        ),
        (_, _) => (
            from,
            SortInfo {
                dir: Monotonicity::Unordered,
                nulls_first: children[0].1.nulls_first | children[1].1.nulls_first,
                nulls_last: children[0].1.nulls_last | children[1].1.nulls_last,
            },
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Not;

    use super::*;
    use crate::expressions::{col, BinaryExpr, Literal};
    use arrow_schema::{DataType, Field, Schema, SortOptions};
    use datafusion_common::ScalarValue;

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
            (
                TableSide::Both,
                SortInfo {
                    dir: Monotonicity::Unordered,
                    nulls_first: true,
                    nulls_last: false
                }
            ),
            graph.analyze_prunability(
                &Some(left_sorted_asc.clone()),
                &Some(right_sorted_asc.clone()),
            )?
        );
        assert_eq!(
            (
                TableSide::None,
                SortInfo {
                    dir: Monotonicity::Unordered,
                    nulls_first: true,
                    nulls_last: true
                }
            ),
            graph.analyze_prunability(
                &Some(left_sorted_asc.clone()),
                &Some(right_sorted_desc.clone()),
            )?
        );
        assert_eq!(
            (
                TableSide::None,
                SortInfo {
                    dir: Monotonicity::Unordered,
                    nulls_first: true,
                    nulls_last: true
                }
            ),
            graph.analyze_prunability(
                &Some(left_sorted_desc.clone()),
                &Some(right_sorted_asc),
            )?
        );
        assert_eq!(
            (
                TableSide::Both,
                SortInfo {
                    dir: Monotonicity::Unordered,
                    nulls_first: false,
                    nulls_last: true
                }
            ),
            graph
                .analyze_prunability(&Some(left_sorted_desc), &Some(right_sorted_desc),)?
        );
        assert_eq!(
            (
                TableSide::None,
                SortInfo {
                    dir: Monotonicity::Unordered,
                    nulls_first: true,
                    nulls_last: false
                }
            ),
            graph.analyze_prunability(&Some(left_sorted_asc), &None,)?
        );

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
            (
                TableSide::Left,
                SortInfo {
                    dir: Monotonicity::Unordered,
                    nulls_first: true,
                    nulls_last: true
                }
            ),
            graph.analyze_prunability(
                &Some(left_sorted1_asc),
                &Some(right_sorted1_desc),
            )?
        );
        assert_eq!(
            (
                TableSide::None,
                SortInfo {
                    dir: Monotonicity::Unordered,
                    nulls_first: true,
                    nulls_last: true
                }
            ),
            graph.analyze_prunability(
                &Some(left_sorted2_desc.clone()),
                &Some(right_sorted2_asc.clone()),
            )?
        );
        assert_eq!(
            (
                TableSide::Left,
                SortInfo {
                    dir: Monotonicity::Unordered,
                    nulls_first: true,
                    nulls_last: true
                }
            ),
            graph.analyze_prunability(
                &Some(left_sorted2_desc),
                &Some(right_sorted2_desc),
            )?
        );
        assert_eq!(
            (
                TableSide::Right,
                SortInfo {
                    dir: Monotonicity::Unordered,
                    nulls_first: true,
                    nulls_last: false
                }
            ),
            graph
                .analyze_prunability(&Some(left_sorted2_asc), &Some(right_sorted2_asc),)?
        );
        assert_eq!(
            (
                TableSide::Right,
                SortInfo {
                    dir: Monotonicity::Unordered,
                    nulls_first: true,
                    nulls_last: true
                }
            ),
            graph.analyze_prunability(
                &Some(left_sorted1_desc),
                &Some(right_sorted1_asc),
            )?
        );

        Ok(())
    }

    fn experiment_sort_info(
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
            (
                TableSide::None,
                SortInfo {
                    dir: Monotonicity::Asc,
                    nulls_first: true,
                    nulls_last: true
                }
            ),
            graph
                .analyze_prunability(&Some(left_sorted_asc), &Some(right_sorted_desc),)?
        );

        assert_eq!(
            (
                TableSide::None,
                SortInfo {
                    dir: Monotonicity::Desc,
                    nulls_first: true,
                    nulls_last: true
                }
            ),
            graph
                .analyze_prunability(&Some(left_sorted_desc), &Some(right_sorted_asc),)?
        );

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

    #[test]
    fn test_sort_info() -> Result<()> {
        // Create 2 schemas having two interger columns
        let schema_left =
            Schema::new(vec![Field::new("left_column", DataType::Int32, true)]);
        let schema_right =
            Schema::new(vec![Field::new("right_column", DataType::Int32, true)]);

        // ( (left_column + 1) >= (right_column + 2) ) AND ( (right_column + 3) <= (left_column - 4) )
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
            right_col,
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(3)))),
        ));
        let right_and_2 = Arc::new(BinaryExpr::new(
            left_col,
            Operator::Minus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(4)))),
        ));
        let left_expr = Arc::new(BinaryExpr::new(left_and_1, Operator::GtEq, left_and_2));
        let right_expr =
            Arc::new(BinaryExpr::new(right_and_1, Operator::LtEq, right_and_2));
        let expr = Arc::new(BinaryExpr::new(left_expr, Operator::And, right_expr));

        experiment_sort_info(&schema_left, &schema_right, expr)?;

        Ok(())
    }
}
