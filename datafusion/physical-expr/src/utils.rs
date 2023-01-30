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

use crate::equivalence::EquivalentClass;
use crate::expressions::Column;
use crate::expressions::UnKnownColumn;
use crate::expressions::{BinaryExpr, Literal};
use crate::rewrite::TreeNodeRewritable;
use crate::PhysicalSortExpr;
use arrow::datatypes::SchemaRef;
use datafusion_common::{Result, ScalarValue};
use crate::{EquivalenceProperties, PhysicalExpr};
use datafusion_expr::Operator;

use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use std::collections::HashMap;
use std::sync::Arc;

/// Compare the two expr lists are equal no matter the order.
/// For example two InListExpr can be considered to be equals no matter the order:
///
/// In('a','b','c') == In('c','b','a')
pub fn expr_list_eq_any_order(
    list1: &[Arc<dyn PhysicalExpr>],
    list2: &[Arc<dyn PhysicalExpr>],
) -> bool {
    if list1.len() == list2.len() {
        let mut expr_vec1 = list1.to_vec();
        let mut expr_vec2 = list2.to_vec();
        while let Some(expr1) = expr_vec1.pop() {
            if let Some(idx) = expr_vec2.iter().position(|expr2| expr1.eq(expr2)) {
                expr_vec2.swap_remove(idx);
            } else {
                break;
            }
        }
        expr_vec1.is_empty() && expr_vec2.is_empty()
    } else {
        false
    }
}

/// Strictly compare the two expr lists are equal in the given order.
pub fn expr_list_eq_strict_order(
    list1: &[Arc<dyn PhysicalExpr>],
    list2: &[Arc<dyn PhysicalExpr>],
) -> bool {
    list1.len() == list2.len() && list1.iter().zip(list2.iter()).all(|(e1, e2)| e1.eq(e2))
}

/// Strictly compare the two sort expr lists in the given order.
///
/// For Physical Sort Exprs, the order matters:
///
/// SortExpr('a','b','c') != SortExpr('c','b','a')
#[allow(dead_code)]
pub fn sort_expr_list_eq_strict_order(
    list1: &[PhysicalSortExpr],
    list2: &[PhysicalSortExpr],
) -> bool {
    list1.len() == list2.len() && list1.iter().zip(list2.iter()).all(|(e1, e2)| e1.eq(e2))
}

/// Assume the predicate is in the form of CNF, split the predicate to a Vec of PhysicalExprs.
///
/// For example, split "a1 = a2 AND b1 <= b2 AND c1 != c2" into ["a1 = a2", "b1 <= b2", "c1 != c2"]
pub fn split_conjunction(
    predicate: &Arc<dyn PhysicalExpr>,
) -> Vec<&Arc<dyn PhysicalExpr>> {
    split_conjunction_impl(predicate, vec![])
}

fn split_conjunction_impl<'a>(
    predicate: &'a Arc<dyn PhysicalExpr>,
    mut exprs: Vec<&'a Arc<dyn PhysicalExpr>>,
) -> Vec<&'a Arc<dyn PhysicalExpr>> {
    match predicate.as_any().downcast_ref::<BinaryExpr>() {
        Some(binary) => match binary.op() {
            Operator::And => {
                let exprs = split_conjunction_impl(binary.left(), exprs);
                split_conjunction_impl(binary.right(), exprs)
            }
            _ => {
                exprs.push(predicate);
                exprs
            }
        },
        None => {
            exprs.push(predicate);
            exprs
        }
    }
}

/// Normalize the output expressions based on Alias Map and SchemaRef.
///
/// 1) If there is mapping in Alias Map, replace the Column in the output expressions with the 1st Column in Alias Map
/// 2) If the Column is invalid for the current Schema, replace the Column with a place holder UnKnownColumn
///
pub fn normalize_out_expr_with_alias_schema(
    expr: Arc<dyn PhysicalExpr>,
    alias_map: &HashMap<Column, Vec<Column>>,
    schema: &SchemaRef,
) -> Arc<dyn PhysicalExpr> {
    let expr_clone = expr.clone();
    expr_clone
        .transform(&|expr| {
            let normalized_form: Option<Arc<dyn PhysicalExpr>> =
                match expr.as_any().downcast_ref::<Column>() {
                    Some(column) => {
                        let out = alias_map
                            .get(column)
                            .map(|c| {
                                let out_col: Arc<dyn PhysicalExpr> =
                                    Arc::new(c[0].clone());
                                out_col
                            })
                            .or_else(|| match schema.index_of(column.name()) {
                                // Exactly matching, return None, no need to do the transform
                                Ok(idx) if column.index() == idx => None,
                                _ => {
                                    let out_col: Arc<dyn PhysicalExpr> =
                                        Arc::new(UnKnownColumn::new(column.name()));
                                    Some(out_col)
                                }
                            });
                        out
                    }
                    None => None,
                };
            Ok(normalized_form)
        })
        .unwrap_or(expr)
}

pub fn normalize_expr_with_equivalence_properties(
    expr: Arc<dyn PhysicalExpr>,
    eq_properties: &[EquivalentClass],
) -> Arc<dyn PhysicalExpr> {
    let expr_clone = expr.clone();
    expr_clone
        .transform(&|expr| match expr.as_any().downcast_ref::<Column>() {
            Some(column) => {
                let mut normalized: Option<Arc<dyn PhysicalExpr>> = None;
                for class in eq_properties {
                    if class.contains(column) {
                        normalized = Some(Arc::new(class.head().clone()));
                        break;
                    }
                }
                Ok(normalized)
            }
            None => Ok(None),
        })
        .unwrap_or(expr)
}

pub fn normalize_sort_expr_with_equivalence_properties(
    sort_expr: PhysicalSortExpr,
    eq_properties: &[EquivalentClass],
) -> PhysicalSortExpr {
    let normalized_expr =
        normalize_expr_with_equivalence_properties(sort_expr.expr.clone(), eq_properties);

    if sort_expr.expr.ne(&normalized_expr) {
        PhysicalSortExpr {
            expr: normalized_expr,
            options: sort_expr.options,
        }
    } else {
        sort_expr
    }
}

#[derive(Clone, Debug)]
pub struct ExprTreeNode {
    expr: Arc<dyn PhysicalExpr>,
    node: Option<NodeIndex>,
    child_nodes: Vec<Arc<ExprTreeNode>>,
}

impl PartialEq for ExprTreeNode {
    fn eq(&self, other: &ExprTreeNode) -> bool {
        self.expr.eq(&other.expr)
    }
}

impl ExprTreeNode {
    pub fn new(plan: Arc<dyn PhysicalExpr>) -> Self {
        ExprTreeNode {
            expr: plan,
            node: None,
            child_nodes: vec![],
        }
    }

    pub fn node(&self) -> &Option<NodeIndex> {
        &self.node
    }

    pub fn child_nodes(&self) -> &Vec<Arc<ExprTreeNode>> {
        &self.child_nodes
    }

    pub fn expr(&self) -> Arc<dyn PhysicalExpr> {
        self.expr.clone()
    }

    pub fn children(&self) -> Vec<Arc<ExprTreeNode>> {
        let plan_children = self.expr.children();
        let child_intervals: Vec<Arc<ExprTreeNode>> = self.child_nodes.to_vec();
        if plan_children.len() == child_intervals.len() {
            child_intervals
        } else {
            plan_children
                .into_iter()
                .map(|child| {
                    Arc::new(ExprTreeNode {
                        expr: child.clone(),
                        node: None,
                        child_nodes: vec![],
                    })
                })
                .collect()
/// Checks whether given ordering requirements are satisfied by provided [PhysicalSortExpr]s.
pub fn ordering_satisfy<F: FnOnce() -> EquivalenceProperties>(
    provided: Option<&[PhysicalSortExpr]>,
    required: Option<&[PhysicalSortExpr]>,
    equal_properties: F,
) -> bool {
    match (provided, required) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(provided), Some(required)) => {
            ordering_satisfy_concrete(provided, required, equal_properties)
        }
    }
}

impl TreeNodeRewritable for Arc<ExprTreeNode> {
    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if !children.is_empty() {
            let new_children: Vec<Arc<ExprTreeNode>> = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<Arc<ExprTreeNode>>>>()
                .unwrap();
            Ok(Arc::new(ExprTreeNode {
                expr: self.expr.clone(),
                node: None,
                child_nodes: new_children,
            }))
        } else {
            Ok(self)
        }
    }
}

fn add_physical_expr_to_graph<T>(
    input: Arc<ExprTreeNode>,
    visited: &mut Vec<(Arc<dyn PhysicalExpr>, NodeIndex)>,
    graph: &mut StableGraph<T, usize>,
    input_node: T,
) -> NodeIndex {
    // If we visited the node before, we just return it. That means, this node will be have multiple
    // parents.
    match visited
        .iter()
        .find(|(visited_expr, _node)| visited_expr.eq(&input.expr()))
    {
        Some((_, idx)) => *idx,
        None => {
            let node_idx = graph.add_node(input_node);
            visited.push((input.expr().clone(), node_idx));
            input.child_nodes().iter().for_each(|expr_node| {
                graph.add_edge(node_idx, expr_node.node().unwrap(), 0);
            });
            node_idx
        }
    }
}

fn post_order_tree_traverse_graph_create<T, F>(
    input: Arc<ExprTreeNode>,
    graph: &mut StableGraph<T, usize>,
    constructor: F,
    visited: &mut Vec<(Arc<dyn PhysicalExpr>, NodeIndex)>,
) -> Result<Option<Arc<ExprTreeNode>>>
where
    F: Fn(Arc<ExprTreeNode>) -> T,
{
    let node = constructor(input.clone());
    let node_idx = add_physical_expr_to_graph(input.clone(), visited, graph, node);
    Ok(Some(Arc::new(ExprTreeNode {
        expr: input.expr.clone(),
        node: Some(node_idx),
        child_nodes: input.child_nodes.to_vec(),
    })))
}

pub fn build_physical_expr_graph<T, F>(
    expr: Arc<dyn PhysicalExpr>,
    constructor: &F,
) -> Result<(NodeIndex, StableGraph<T, usize>)>
where
    F: Fn(Arc<ExprTreeNode>) -> T,
{
    let init = Arc::new(ExprTreeNode::new(expr.clone()));
    let mut graph: StableGraph<T, usize> = StableGraph::new();
    let mut visited_plans: Vec<(Arc<dyn PhysicalExpr>, NodeIndex)> = vec![];
    // Create a graph
    let root_tree_node = init.mutable_transform_up(&mut |expr| {
        post_order_tree_traverse_graph_create(
            expr,
            &mut graph,
            constructor,
            &mut visited_plans,
        )
    })?;
    Ok((root_tree_node.node.unwrap(), graph))
}

#[allow(clippy::too_many_arguments)]
/// left_col (op_1) a  > right_col (op_2) b AND left_col (op_3) c < right_col (op_4) d
pub fn filter_numeric_expr_generation(
    left_col: Arc<dyn PhysicalExpr>,
    right_col: Arc<dyn PhysicalExpr>,
    op_1: Operator,
    op_2: Operator,
    op_3: Operator,
    op_4: Operator,
    a: i32,
    b: i32,
    c: i32,
    d: i32,
) -> Arc<dyn PhysicalExpr> {
    let left_and_1 = Arc::new(BinaryExpr::new(
        left_col.clone(),
        op_1,
        Arc::new(Literal::new(ScalarValue::Int32(Some(a)))),
    ));
    let left_and_2 = Arc::new(BinaryExpr::new(
        right_col.clone(),
        op_2,
        Arc::new(Literal::new(ScalarValue::Int32(Some(b)))),
    ));

    let right_and_1 = Arc::new(BinaryExpr::new(
        left_col.clone(),
        op_3,
        Arc::new(Literal::new(ScalarValue::Int32(Some(c)))),
    ));
    let right_and_2 = Arc::new(BinaryExpr::new(
        right_col.clone(),
        op_4,
        Arc::new(Literal::new(ScalarValue::Int32(Some(d)))),
    ));
    let left_expr = Arc::new(BinaryExpr::new(left_and_1, Operator::Gt, left_and_2));
    let right_expr = Arc::new(BinaryExpr::new(right_and_1, Operator::Lt, right_and_2));
    Arc::new(BinaryExpr::new(left_expr, Operator::And, right_expr))
}

pub fn ordering_satisfy_concrete<F: FnOnce() -> EquivalenceProperties>(
    provided: &[PhysicalSortExpr],
    required: &[PhysicalSortExpr],
    equal_properties: F,
) -> bool {
    if required.len() > provided.len() {
        false
    } else if required
        .iter()
        .zip(provided.iter())
        .all(|(order1, order2)| order1.eq(order2))
    {
        true
    } else if let eq_classes @ [_, ..] = equal_properties().classes() {
        let normalized_required_exprs = required
            .iter()
            .map(|e| {
                normalize_sort_expr_with_equivalence_properties(e.clone(), eq_classes)
            })
            .collect::<Vec<_>>();
        let normalized_provided_exprs = provided
            .iter()
            .map(|e| {
                normalize_sort_expr_with_equivalence_properties(e.clone(), eq_classes)
            })
            .collect::<Vec<_>>();
        normalized_required_exprs
            .iter()
            .zip(normalized_provided_exprs.iter())
            .all(|(order1, order2)| order1.eq(order2))
    } else {
        false
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::expressions::Column;
    use crate::PhysicalSortExpr;
    use arrow::compute::SortOptions;
    use datafusion_common::Result;

    use arrow_schema::Schema;
    use std::sync::Arc;

    #[test]
    fn expr_list_eq_test() -> Result<()> {
        let list1: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
        ];
        let list2: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("a", 0)),
        ];
        assert!(!expr_list_eq_any_order(list1.as_slice(), list2.as_slice()));
        assert!(!expr_list_eq_any_order(list2.as_slice(), list1.as_slice()));

        assert!(!expr_list_eq_strict_order(
            list1.as_slice(),
            list2.as_slice()
        ));
        assert!(!expr_list_eq_strict_order(
            list2.as_slice(),
            list1.as_slice()
        ));

        let list3: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
        ];
        let list4: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("a", 0)),
        ];
        assert!(expr_list_eq_any_order(list3.as_slice(), list4.as_slice()));
        assert!(expr_list_eq_any_order(list4.as_slice(), list3.as_slice()));
        assert!(expr_list_eq_any_order(list3.as_slice(), list3.as_slice()));
        assert!(expr_list_eq_any_order(list4.as_slice(), list4.as_slice()));

        assert!(!expr_list_eq_strict_order(
            list3.as_slice(),
            list4.as_slice()
        ));
        assert!(!expr_list_eq_strict_order(
            list4.as_slice(),
            list3.as_slice()
        ));
        assert!(expr_list_eq_any_order(list3.as_slice(), list3.as_slice()));
        assert!(expr_list_eq_any_order(list4.as_slice(), list4.as_slice()));

        Ok(())
    }

    #[test]
    fn sort_expr_list_eq_strict_order_test() -> Result<()> {
        let list1: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
        ];

        let list2: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
        ];

        assert!(!sort_expr_list_eq_strict_order(
            list1.as_slice(),
            list2.as_slice()
        ));
        assert!(!sort_expr_list_eq_strict_order(
            list2.as_slice(),
            list1.as_slice()
        ));

        let list3: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options: SortOptions::default(),
            },
        ];
        let list4: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options: SortOptions::default(),
            },
        ];

        assert!(sort_expr_list_eq_strict_order(
            list3.as_slice(),
            list4.as_slice()
        ));
        assert!(sort_expr_list_eq_strict_order(
            list4.as_slice(),
            list3.as_slice()
        ));
        assert!(sort_expr_list_eq_strict_order(
            list3.as_slice(),
            list3.as_slice()
        ));
        assert!(sort_expr_list_eq_strict_order(
            list4.as_slice(),
            list4.as_slice()
        ));

        Ok(())
    }

    #[test]
    fn test_ordering_satisfy() -> Result<()> {
        let crude = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("a", 0)),
            options: SortOptions::default(),
        }];
        let crude = Some(&crude[..]);
        let finer = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
        ];
        let finer = Some(&finer[..]);
        let empty_schema = &Arc::new(Schema {
            fields: vec![],
            metadata: Default::default(),
        });
        assert!(ordering_satisfy(finer, crude, || {
            EquivalenceProperties::new(empty_schema.clone())
        }));
        assert!(!ordering_satisfy(crude, finer, || {
            EquivalenceProperties::new(empty_schema.clone())
        }));
        Ok(())
    }
}
