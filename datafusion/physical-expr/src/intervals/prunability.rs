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

use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use petgraph::visit::DfsPostOrder;
use petgraph::Outgoing;

use crate::expressions::{BinaryExpr, Column};
use crate::utils::{build_dag, ExprTreeNode};
use crate::{PhysicalExpr, PhysicalSortExpr};

use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Operator;

/// This object implements a directed acyclic expression graph (DAEG) that
/// is used to determine whether a [`PhysicalExpr`] is prunable given some
/// [`PhysicalSortExpr`]. If so, the prunable table side can be inferred.
/// To decide prunability, we consider monotonicity properties of expressions
/// as well as their orderings and their null placement properties.
#[derive(Clone)]
pub struct ExprPrunabilityGraph {
    graph: StableGraph<ExprPrunabilityGraphNode, usize>,
    root: NodeIndex,
}

/// This object acts as a propagator of monotonicity information as we traverse
/// the DAEG bottom-up.
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum Monotonicity {
    Asc,
    Desc,
    Unordered,
    Singleton,
}

/// This object acts as a propagator of "prunable table side" information
/// as we traverse the DAEG bottom-up. Column leaves are assigned a `TableSide`,
/// and we carry this information upwards according to expression properties.
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum TableSide {
    None,
    Left,
    Right,
    Both,
}

/// This object encapsulates monotonicity properties together with NULL
/// placement and acts as a joint propagator during bottom-up traversals
/// of the DAEG.
#[derive(PartialEq, Debug, Clone, Copy)]
pub struct SortInfo {
    dir: Monotonicity,
    nulls_first: bool,
    nulls_last: bool,
}

fn get_tableside_at_numeric(
    left_dir: &Monotonicity,
    right_dir: &Monotonicity,
    left_tableside: &TableSide,
    right_tableside: &TableSide,
) -> TableSide {
    match (left_dir, right_dir) {
        (Monotonicity::Singleton, Monotonicity::Singleton) => TableSide::None,
        (Monotonicity::Singleton, _) => *right_tableside,
        (_, Monotonicity::Singleton) => *left_tableside,
        (_, _) => {
            if left_tableside == right_tableside {
                *left_tableside
            } else {
                TableSide::None
            }
        }
    }
}
fn get_tableside_at_gt_or_gteq(
    left_dir: &Monotonicity,
    right_dir: &Monotonicity,
    left_tableside: &TableSide,
    right_tableside: &TableSide,
) -> TableSide {
    match (left_dir, right_dir) {
        (Monotonicity::Asc, Monotonicity::Asc) => *left_tableside,
        (Monotonicity::Desc, Monotonicity::Desc) => *right_tableside,
        (_, _) => TableSide::None,
    }
}
fn get_tableside_at_and(
    left_tableside: &TableSide,
    right_tableside: &TableSide,
) -> TableSide {
    match (left_tableside, right_tableside) {
        (TableSide::Left, TableSide::Right) | (TableSide::Right, TableSide::Left) => {
            TableSide::Both
        }
        (TableSide::Left, _) | (_, TableSide::Left) => TableSide::Left,
        (TableSide::Right, _) | (_, TableSide::Right) => TableSide::Right,
        (_, _) => TableSide::None,
    }
}

impl Monotonicity {
    fn add(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Monotonicity::Singleton, Monotonicity::Singleton) => Monotonicity::Singleton,
            (Monotonicity::Singleton, rhs) => *rhs,
            (lhs, Monotonicity::Singleton) => *lhs,
            (Monotonicity::Asc, Monotonicity::Asc) => Monotonicity::Asc,
            (Monotonicity::Desc, Monotonicity::Desc) => Monotonicity::Desc,
            (_, _) => Monotonicity::Unordered,
        }
    }

    fn sub(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Monotonicity::Singleton, Monotonicity::Singleton) => Monotonicity::Singleton,
            (Monotonicity::Singleton, rhs) => {
                if *rhs == Monotonicity::Asc {
                    Monotonicity::Desc
                } else if *rhs == Monotonicity::Desc {
                    Monotonicity::Asc
                } else {
                    Monotonicity::Unordered
                }
            }
            (lhs, Monotonicity::Singleton) => *lhs,
            (Monotonicity::Asc, Monotonicity::Desc) => Monotonicity::Asc,
            (Monotonicity::Desc, Monotonicity::Asc) => Monotonicity::Desc,
            (_, _) => Monotonicity::Unordered,
        }
    }

    fn gt_or_gteq(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Monotonicity::Singleton, rhs) => {
                if *rhs == Monotonicity::Asc {
                    Monotonicity::Desc
                } else if *rhs == Monotonicity::Desc {
                    Monotonicity::Asc
                } else {
                    Monotonicity::Unordered
                }
            }
            (lhs, Monotonicity::Singleton) => *lhs,
            (Monotonicity::Asc, Monotonicity::Desc) => Monotonicity::Asc,
            (Monotonicity::Desc, Monotonicity::Asc) => Monotonicity::Desc,
            (_, _) => Monotonicity::Unordered,
        }
    }

    fn and(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Monotonicity::Asc, Monotonicity::Asc)
            | (Monotonicity::Asc, Monotonicity::Singleton)
            | (Monotonicity::Singleton, Monotonicity::Asc) => Monotonicity::Asc,
            (Monotonicity::Desc, Monotonicity::Desc)
            | (Monotonicity::Desc, Monotonicity::Singleton)
            | (Monotonicity::Singleton, Monotonicity::Desc) => Monotonicity::Desc,
            (Monotonicity::Singleton, Monotonicity::Singleton) => Monotonicity::Singleton,
            (_, _) => Monotonicity::Unordered,
        }
    }
}

macro_rules! sortinfo_op {
    ($OP_NAME:ident) => {
        fn $OP_NAME(&self, rhs: &Self) -> Self {
            SortInfo {
                dir: self.dir.$OP_NAME(&rhs.dir),
                nulls_first: self.nulls_first || rhs.nulls_first,
                nulls_last: self.nulls_last || rhs.nulls_last,
            }
        }
    };
}

impl SortInfo {
    sortinfo_op!(add);
    sortinfo_op!(sub);
    sortinfo_op!(gt_or_gteq);
    sortinfo_op!(and);
}

/// This object represents a node in the DAEG we use for monotonicity analysis.
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
    /// Constructs a new DAEG node with an unordered [`SortInfo`].
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

    /// This function creates a DAEG node from Datafusion's [`ExprTreeNode`] object.
    pub fn make_node(node: &ExprTreeNode<NodeIndex>) -> ExprPrunabilityGraphNode {
        ExprPrunabilityGraphNode::new(node.expression().clone())
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

    /// This function traverses the whole graph. It returns prunable table sides
    /// and sort information of the binary expression that generates the graph.
    pub fn analyze_prunability(
        &mut self,
        left_sort_expr: &Option<PhysicalSortExpr>,
        right_sort_expr: &Option<PhysicalSortExpr>,
    ) -> Result<(TableSide, SortInfo)> {
        let mut includes_filter = false;
        // Since children nodes determine their parent's properties, we use DFS:
        let mut dfs = DfsPostOrder::new(&self.graph, self.root);
        while let Some(node) = dfs.next(&self.graph) {
            let children = self.graph.neighbors_directed(node, Outgoing);
            let mut children_prunability = children
                .map(|child| {
                    (&self.graph[child].table_side, &self.graph[child].sort_info)
                })
                .collect::<Vec<_>>();

            // Handle leaf nodes:
            if children_prunability.is_empty() {
                // Initialize as an unordered column with an undefined table side:
                self.graph[node].table_side = TableSide::None;
                self.graph[node].sort_info.dir = Monotonicity::Unordered;
                // By convention, we initially set nulls_first and nulls_last to false.
                self.graph[node].sort_info.nulls_first = false;
                self.graph[node].sort_info.nulls_last = false;

                self.update_node_with_sort_information(
                    left_sort_expr,
                    right_sort_expr,
                    node,
                );
            }
            // Handle intermediate nodes:
            else {
                // The graph library gives children in the reverse order, so we
                // "fix" the order by the following reversal:
                children_prunability.reverse();
                let binary_expr = self.graph[node].expr.as_any().downcast_ref::<BinaryExpr>().ok_or_else(|| {
                    DataFusionError::Internal("PhysicalExpr under investigation for prunability should be BinaryExpr.".to_string())
                })?;
                (self.graph[node].table_side, self.graph[node].sort_info) =
                    if binary_expr.op().is_numerical_operators() {
                        numeric_node_order(
                            &children_prunability,
                            binary_expr,
                            left_sort_expr,
                            right_sort_expr,
                        )?
                    } else if binary_expr.op().is_comparison_operator() {
                        includes_filter = true;
                        comparison_node_order(&children_prunability, binary_expr.op())?
                    } else if *binary_expr.op() == Operator::And {
                        logical_node_order(&children_prunability)
                    } else {
                        return Err(DataFusionError::NotImplemented(format!(
                        "Prunability is not supported yet for the logical operator {}",
                        *binary_expr.op()
                    )));
                    };
            }
        }
        Ok(if includes_filter {
            (
                self.graph[self.root].table_side,
                self.graph[self.root].sort_info,
            )
        } else {
            (TableSide::None, self.graph[self.root].sort_info)
        })
    }

    /// This function takes the index of a leaf node, which may contain a column
    /// or a literal. If it does contain a column, it scans the `PhysicalSortExpr`s
    /// and checks if there is a match with that column. If so, `TableSide` and
    /// `SortInfo` of the node is set accordingly.
    fn update_node_with_sort_information(
        &mut self,
        left_sort_expr: &Option<PhysicalSortExpr>,
        right_sort_expr: &Option<PhysicalSortExpr>,
        node: NodeIndex,
    ) {
        let current = &mut self.graph[node];
        if let Some(column) = current.expr.as_any().downcast_ref::<Column>() {
            for (table_side, sort_expr) in [
                (TableSide::Left, left_sort_expr),
                (TableSide::Right, right_sort_expr),
            ]
            .into_iter()
            {
                // We handle non-column sort expressions in intermediate nodes.
                // Thus, we need only to check column sort expressions:
                if let Some(sort_expr) = sort_expr {
                    if let Some(sorted) = sort_expr.expr.as_any().downcast_ref::<Column>()
                    {
                        if column == sorted {
                            current.table_side = table_side;
                            current.sort_info.dir = if sort_expr.options.descending {
                                Monotonicity::Desc
                            } else {
                                Monotonicity::Asc
                            };
                            let nulls_first = sort_expr.options.nulls_first;
                            current.sort_info.nulls_first = nulls_first;
                            current.sort_info.nulls_last = !nulls_first;
                            break;
                        }
                    }
                }
            }
        } else {
            // If the leaf node is not a column, it must be a literal. Its
            // table side is already initialized as None, no need to change it.
            current.sort_info.dir = Monotonicity::Singleton;
            current.sort_info.nulls_first = false;
            current.sort_info.nulls_last = false;
        }
    }
}

fn numeric_node_order(
    children: &[(&TableSide, &SortInfo)],
    binary_expr: &BinaryExpr,
    left_sort_expr: &Option<PhysicalSortExpr>,
    right_sort_expr: &Option<PhysicalSortExpr>,
) -> Result<(TableSide, SortInfo)> {
    // There may be SortOptions for BinaryExpr's like a + b. We handle such
    // situations here:
    for (table_side, sort_expr) in [
        (TableSide::Left, left_sort_expr),
        (TableSide::Right, right_sort_expr),
    ]
    .into_iter()
    {
        if let Some(sort_expr) = sort_expr {
            if let Some(binary_expr_sorted) =
                sort_expr.expr.as_any().downcast_ref::<BinaryExpr>()
            {
                if binary_expr.eq(binary_expr_sorted) {
                    let dir = if sort_expr.options.descending {
                        Monotonicity::Desc
                    } else {
                        Monotonicity::Asc
                    };
                    let nulls_first = sort_expr.options.nulls_first;
                    return Ok((
                        table_side,
                        SortInfo {
                            dir,
                            nulls_first,
                            nulls_last: !nulls_first,
                        },
                    ));
                }
            }
        }
    }
    let ((left_ts, left_si), (right_ts, right_si)) = (children[0], children[1]);
    let from = get_tableside_at_numeric(&left_si.dir, &right_si.dir, left_ts, right_ts);
    match binary_expr.op() {
        Operator::Plus => Ok((from, left_si.add(right_si))),
        Operator::Minus => Ok((from, left_si.sub(right_si))),
        op => Err(DataFusionError::NotImplemented(format!(
            "Prunability is not supported yet for binary expressions having the {op} operator"
        )))
    }
}

fn comparison_node_order(
    children: &[(&TableSide, &SortInfo)],
    binary_expr_op: &Operator,
) -> Result<(TableSide, SortInfo)> {
    let ((left_ts, left_si), (right_ts, right_si)) = (children[0], children[1]);
    match binary_expr_op {
        Operator::Gt | Operator::GtEq => Ok((
            get_tableside_at_gt_or_gteq(&left_si.dir, &right_si.dir, left_ts, right_ts),
            left_si.gt_or_gteq(right_si),
        )),
        Operator::Lt | Operator::LtEq => Ok((
            get_tableside_at_gt_or_gteq(&right_si.dir, &left_si.dir, right_ts, left_ts),
            right_si.gt_or_gteq(left_si),
        )),
        op => Err(DataFusionError::NotImplemented(format!(
            "Prunability is not supported yet for binary expressions having the {op} operator"
                ),
        ))
    }
}

fn logical_node_order(children: &[(&TableSide, &SortInfo)]) -> (TableSide, SortInfo) {
    let ((left_ts, left_si), (right_ts, right_si)) = (children[0], children[1]);
    (
        get_tableside_at_and(left_ts, right_ts),
        left_si.and(right_si),
    )
}

#[cfg(test)]
mod tests {
    use std::ops::Not;

    use super::*;
    use crate::expressions::{col, BinaryExpr, Literal};
    use arrow_schema::{DataType, Field, Schema, SortOptions};
    use datafusion_common::ScalarValue;

    // This experiment expects its input expression to be in the form:
    // ( (left_column + a) > (right_column + b) ) AND ( (left_column + c) < (right_column + d) )
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

    // This experiment expects its input expression to be in the form:
    // ( (left_column1 + a) > (b - right_column1) ) AND ( (left_column2 + c) < (right_column2 - d) )
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
                    nulls_first: false,
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

    // This experiment expects its input expression to be in the form:
    // ( (left_column + a) >= (right_column + b) ) AND ( (right_column + c) <= (left_column - d) )
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
        // Create 2 schemas having two integer columns
        let schema_left =
            Schema::new(vec![Field::new("left_column", DataType::Int32, true)]);
        let schema_right =
            Schema::new(vec![Field::new("right_column", DataType::Int32, true)]);

        // ( (left_column + 1) >= (right_column + 2) ) AND ( (right_column + 3) <= (left_column - 4) )
        let left_col = col("left_column", &schema_left)?;
        let right_col = col("right_column", &schema_right)?;
        let expr1 = Arc::new(BinaryExpr::new(
            left_col.clone(),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ));
        let expr2 = Arc::new(BinaryExpr::new(
            right_col.clone(),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
        ));
        let expr3 = Arc::new(BinaryExpr::new(
            right_col,
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(3)))),
        ));
        let expr4 = Arc::new(BinaryExpr::new(
            left_col,
            Operator::Minus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(4)))),
        ));
        let left_expr = Arc::new(BinaryExpr::new(expr1, Operator::GtEq, expr2));
        let right_expr = Arc::new(BinaryExpr::new(expr3, Operator::LtEq, expr4));
        let expr = Arc::new(BinaryExpr::new(left_expr, Operator::And, right_expr));

        experiment_sort_info(&schema_left, &schema_right, expr)?;

        Ok(())
    }
}
