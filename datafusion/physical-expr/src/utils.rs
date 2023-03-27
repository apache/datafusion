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
use crate::expressions::{BinaryExpr, Column, UnKnownColumn};
use crate::{
    EquivalenceProperties, PhysicalExpr, PhysicalSortExpr, PhysicalSortRequirement,
};
use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_expr::Operator;

use arrow_schema::SortOptions;
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRewriter, VisitRecursion,
};
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use std::collections::HashMap;
use std::collections::HashSet;
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
    expr.clone()
        .transform(&|expr| {
            let normalized_form: Option<Arc<dyn PhysicalExpr>> = match expr
                .as_any()
                .downcast_ref::<Column>()
            {
                Some(column) => {
                    alias_map
                        .get(column)
                        .map(|c| Arc::new(c[0].clone()) as _)
                        .or_else(|| match schema.index_of(column.name()) {
                            // Exactly matching, return None, no need to do the transform
                            Ok(idx) if column.index() == idx => None,
                            _ => Some(Arc::new(UnKnownColumn::new(column.name())) as _),
                        })
                }
                None => None,
            };
            Ok(if let Some(normalized_form) = normalized_form {
                Transformed::Yes(normalized_form)
            } else {
                Transformed::No(expr)
            })
        })
        .unwrap_or(expr)
}

pub fn normalize_expr_with_equivalence_properties(
    expr: Arc<dyn PhysicalExpr>,
    eq_properties: &[EquivalentClass],
) -> Arc<dyn PhysicalExpr> {
    expr.clone()
        .transform(&|expr| {
            let normalized_form: Option<Arc<dyn PhysicalExpr>> =
                match expr.as_any().downcast_ref::<Column>() {
                    Some(column) => {
                        let mut normalized: Option<Arc<dyn PhysicalExpr>> = None;
                        for class in eq_properties {
                            if class.contains(column) {
                                normalized = Some(Arc::new(class.head().clone()));
                                break;
                            }
                        }
                        normalized
                    }
                    None => None,
                };
            Ok(if let Some(normalized_form) = normalized_form {
                Transformed::Yes(normalized_form)
            } else {
                Transformed::No(expr)
            })
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

pub fn normalize_sort_requirement_with_equivalence_properties(
    sort_requirement: PhysicalSortRequirement,
    eq_properties: &[EquivalentClass],
) -> PhysicalSortRequirement {
    let normalized_expr = normalize_expr_with_equivalence_properties(
        sort_requirement.expr.clone(),
        eq_properties,
    );
    if sort_requirement.expr.ne(&normalized_expr) {
        PhysicalSortRequirement {
            expr: normalized_expr,
            options: sort_requirement.options,
        }
    } else {
        sort_requirement
    }
}

/// Checks whether the required [`PhysicalSortExpr`]s are satisfied by the
/// provided [`PhysicalSortExpr`]s.
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

/// Checks whether the required [`PhysicalSortExpr`]s are satisfied by the
/// provided [`PhysicalSortExpr`]s.
fn ordering_satisfy_concrete<F: FnOnce() -> EquivalenceProperties>(
    provided: &[PhysicalSortExpr],
    required: &[PhysicalSortExpr],
    equal_properties: F,
) -> bool {
    if required.len() > provided.len() {
        false
    } else if required
        .iter()
        .zip(provided.iter())
        .all(|(req, given)| req.eq(given))
    {
        true
    } else if let eq_classes @ [_, ..] = equal_properties().classes() {
        required
            .iter()
            .map(|e| {
                normalize_sort_expr_with_equivalence_properties(e.clone(), eq_classes)
            })
            .zip(provided.iter().map(|e| {
                normalize_sort_expr_with_equivalence_properties(e.clone(), eq_classes)
            }))
            .all(|(req, given)| req.eq(&given))
    } else {
        false
    }
}

/// Checks whether the given [`PhysicalSortRequirement`]s are satisfied by the
/// provided [`PhysicalSortExpr`]s.
pub fn ordering_satisfy_requirement<F: FnOnce() -> EquivalenceProperties>(
    provided: Option<&[PhysicalSortExpr]>,
    required: Option<&[PhysicalSortRequirement]>,
    equal_properties: F,
) -> bool {
    match (provided, required) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(provided), Some(required)) => {
            ordering_satisfy_requirement_concrete(provided, required, equal_properties)
        }
    }
}

/// Checks whether the given [`PhysicalSortRequirement`]s are satisfied by the
/// provided [`PhysicalSortExpr`]s.
pub fn ordering_satisfy_requirement_concrete<F: FnOnce() -> EquivalenceProperties>(
    provided: &[PhysicalSortExpr],
    required: &[PhysicalSortRequirement],
    equal_properties: F,
) -> bool {
    if required.len() > provided.len() {
        false
    } else if required
        .iter()
        .zip(provided.iter())
        .all(|(req, given)| given.satisfy(req))
    {
        true
    } else if let eq_classes @ [_, ..] = equal_properties().classes() {
        required
            .iter()
            .map(|e| {
                normalize_sort_requirement_with_equivalence_properties(
                    e.clone(),
                    eq_classes,
                )
            })
            .zip(provided.iter().map(|e| {
                normalize_sort_expr_with_equivalence_properties(e.clone(), eq_classes)
            }))
            .all(|(req, given)| given.satisfy(&req))
    } else {
        false
    }
}

/// Checks whether the given [`PhysicalSortRequirement`]s are equal or more
/// specific than the provided [`PhysicalSortRequirement`]s.
pub fn requirements_compatible<F: FnOnce() -> EquivalenceProperties>(
    provided: Option<&[PhysicalSortRequirement]>,
    required: Option<&[PhysicalSortRequirement]>,
    equal_properties: F,
) -> bool {
    match (provided, required) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(provided), Some(required)) => {
            requirements_compatible_concrete(provided, required, equal_properties)
        }
    }
}

/// Checks whether the given [`PhysicalSortRequirement`]s are equal or more
/// specific than the provided [`PhysicalSortRequirement`]s.
fn requirements_compatible_concrete<F: FnOnce() -> EquivalenceProperties>(
    provided: &[PhysicalSortRequirement],
    required: &[PhysicalSortRequirement],
    equal_properties: F,
) -> bool {
    if required.len() > provided.len() {
        false
    } else if required
        .iter()
        .zip(provided.iter())
        .all(|(req, given)| given.compatible(req))
    {
        true
    } else if let eq_classes @ [_, ..] = equal_properties().classes() {
        required
            .iter()
            .map(|e| {
                normalize_sort_requirement_with_equivalence_properties(
                    e.clone(),
                    eq_classes,
                )
            })
            .zip(provided.iter().map(|e| {
                normalize_sort_requirement_with_equivalence_properties(
                    e.clone(),
                    eq_classes,
                )
            }))
            .all(|(req, given)| given.compatible(&req))
    } else {
        false
    }
}

/// This function maps back requirement after ProjectionExec
/// to the Executor for its input.
// Specifically, `ProjectionExec` changes index of `Column`s in the schema of its input executor.
// This function changes requirement given according to ProjectionExec schema to the requirement
// according to schema of input executor to the ProjectionExec.
// For instance, Column{"a", 0} would turn to Column{"a", 1}. Please note that this function assumes that
// name of the Column is unique. If we have a requirement such that Column{"a", 0}, Column{"a", 1}.
// This function will produce incorrect result (It will only emit single Column as a result).
pub fn map_columns_before_projection(
    parent_required: &[Arc<dyn PhysicalExpr>],
    proj_exprs: &[(Arc<dyn PhysicalExpr>, String)],
) -> Vec<Arc<dyn PhysicalExpr>> {
    let column_mapping = proj_exprs
        .iter()
        .filter_map(|(expr, name)| {
            expr.as_any()
                .downcast_ref::<Column>()
                .map(|column| (name.clone(), column.clone()))
        })
        .collect::<HashMap<_, _>>();
    parent_required
        .iter()
        .filter_map(|r| {
            if let Some(column) = r.as_any().downcast_ref::<Column>() {
                column_mapping.get(column.name())
            } else {
                None
            }
        })
        .map(|e| Arc::new(e.clone()) as _)
        .collect()
}

/// This function converts `PhysicalSortRequirement` to `PhysicalSortExpr`
/// for each entry in the input. If required ordering is None for an entry
/// default ordering `ASC, NULLS LAST` if given.
pub fn make_sort_exprs_from_requirements(
    required: &[PhysicalSortRequirement],
) -> Vec<PhysicalSortExpr> {
    required
        .iter()
        .map(|requirement| {
            if let Some(options) = requirement.options {
                PhysicalSortExpr {
                    expr: requirement.expr.clone(),
                    options,
                }
            } else {
                PhysicalSortExpr {
                    expr: requirement.expr.clone(),
                    options: SortOptions {
                        // By default, create sort key with ASC is true and NULLS LAST to be consistent with
                        // PostgreSQL's rule: https://www.postgresql.org/docs/current/queries-order.html
                        descending: false,
                        nulls_first: false,
                    },
                }
            }
        })
        .collect()
}

#[derive(Clone, Debug)]
pub struct ExprTreeNode<T> {
    expr: Arc<dyn PhysicalExpr>,
    data: Option<T>,
    child_nodes: Vec<ExprTreeNode<T>>,
}

impl<T> ExprTreeNode<T> {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        ExprTreeNode {
            expr,
            data: None,
            child_nodes: vec![],
        }
    }

    pub fn expression(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    pub fn children(&self) -> Vec<ExprTreeNode<T>> {
        self.expr
            .children()
            .into_iter()
            .map(ExprTreeNode::new)
            .collect()
    }
}

impl<T: Clone> TreeNode for ExprTreeNode<T> {
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        for child in self.children() {
            match op(&child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }

        Ok(VisitRecursion::Continue)
    }

    fn map_children<F>(mut self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        self.child_nodes = self
            .children()
            .into_iter()
            .map(transform)
            .collect::<Result<Vec<_>>>()?;
        Ok(self)
    }
}

/// This struct facilitates the [TreeNodeRewriter] mechanism to convert a
/// [PhysicalExpr] tree into a DAEG (i.e. an expression DAG) by collecting
/// identical expressions in one node. Caller specifies the node type in the
/// DAEG via the `constructor` argument, which constructs nodes in the DAEG
/// from the [ExprTreeNode] ancillary object.
struct PhysicalExprDAEGBuilder<'a, T, F: Fn(&ExprTreeNode<NodeIndex>) -> T> {
    // The resulting DAEG (expression DAG).
    graph: StableGraph<T, usize>,
    // A vector of visited expression nodes and their corresponding node indices.
    visited_plans: Vec<(Arc<dyn PhysicalExpr>, NodeIndex)>,
    // A function to convert an input expression node to T.
    constructor: &'a F,
}

impl<'a, T, F: Fn(&ExprTreeNode<NodeIndex>) -> T> TreeNodeRewriter
    for PhysicalExprDAEGBuilder<'a, T, F>
{
    type N = ExprTreeNode<NodeIndex>;
    // This method mutates an expression node by transforming it to a physical expression
    // and adding it to the graph. The method returns the mutated expression node.
    fn mutate(
        &mut self,
        mut node: ExprTreeNode<NodeIndex>,
    ) -> Result<ExprTreeNode<NodeIndex>> {
        // Get the expression associated with the input expression node.
        let expr = &node.expr;

        // Check if the expression has already been visited.
        let node_idx = match self.visited_plans.iter().find(|(e, _)| expr.eq(e)) {
            // If the expression has been visited, return the corresponding node index.
            Some((_, idx)) => *idx,
            // If the expression has not been visited, add a new node to the graph and
            // add edges to its child nodes. Add the visited expression to the vector
            // of visited expressions and return the newly created node index.
            None => {
                let node_idx = self.graph.add_node((self.constructor)(&node));
                for expr_node in node.child_nodes.iter() {
                    self.graph.add_edge(node_idx, expr_node.data.unwrap(), 0);
                }
                self.visited_plans.push((expr.clone(), node_idx));
                node_idx
            }
        };
        // Set the data field of the input expression node to the corresponding node index.
        node.data = Some(node_idx);
        // Return the mutated expression node.
        Ok(node)
    }
}

// A function that builds a directed acyclic graph of physical expression trees.
pub fn build_dag<T, F>(
    expr: Arc<dyn PhysicalExpr>,
    constructor: &F,
) -> Result<(NodeIndex, StableGraph<T, usize>)>
where
    F: Fn(&ExprTreeNode<NodeIndex>) -> T,
{
    // Create a new expression tree node from the input expression.
    let init = ExprTreeNode::new(expr);
    // Create a new `PhysicalExprDAEGBuilder` instance.
    let mut builder = PhysicalExprDAEGBuilder {
        graph: StableGraph::<T, usize>::new(),
        visited_plans: Vec::<(Arc<dyn PhysicalExpr>, NodeIndex)>::new(),
        constructor,
    };
    // Use the builder to transform the expression tree node into a DAG.
    let root = init.rewrite(&mut builder)?;
    // Return a tuple containing the root node index and the DAG.
    Ok((root.data.unwrap(), builder.graph))
}

/// Recursively extract referenced [`Column`]s within a [`PhysicalExpr`].
pub fn collect_columns(expr: &Arc<dyn PhysicalExpr>) -> HashSet<Column> {
    let mut columns = HashSet::<Column>::new();
    expr.apply(&mut |expr| {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            if !columns.iter().any(|c| c.eq(column)) {
                columns.insert(column.clone());
            }
        }
        Ok(VisitRecursion::Continue)
    })
    // pre_visit always returns OK, so this will always too
    .expect("no way to return error during recursion");
    columns
}

/// Re-assign column indices referenced in predicate according to given schema.
/// This may be helpful when dealing with projections.
pub fn reassign_predicate_columns(
    pred: Arc<dyn PhysicalExpr>,
    schema: &SchemaRef,
    ignore_not_found: bool,
) -> Result<Arc<dyn PhysicalExpr>> {
    pred.transform(&|expr| {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            let index = match schema.index_of(column.name()) {
                Ok(idx) => idx,
                Err(_) if ignore_not_found => usize::MAX,
                Err(e) => return Err(e.into()),
            };
            return Ok(Transformed::Yes(Arc::new(Column::new(
                column.name(),
                index,
            ))));
        }

        Ok(Transformed::No(expr))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{binary, cast, col, lit, Column, Literal};
    use crate::PhysicalSortExpr;
    use arrow::compute::SortOptions;
    use datafusion_common::{Result, ScalarValue};
    use std::fmt::{Display, Formatter};

    use arrow_schema::{DataType, Field, Schema};
    use petgraph::visit::Bfs;
    use std::sync::Arc;

    #[derive(Clone)]
    struct DummyProperty {
        expr_type: String,
    }

    /// This is a dummy node in the DAEG; it stores a reference to the actual
    /// [PhysicalExpr] as well as a dummy property.
    #[derive(Clone)]
    struct PhysicalExprDummyNode {
        pub expr: Arc<dyn PhysicalExpr>,
        pub property: DummyProperty,
    }

    impl Display for PhysicalExprDummyNode {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.expr)
        }
    }

    fn make_dummy_node(node: &ExprTreeNode<NodeIndex>) -> PhysicalExprDummyNode {
        let expr = node.expression().clone();
        let dummy_property = if expr.as_any().is::<BinaryExpr>() {
            "Binary"
        } else if expr.as_any().is::<Column>() {
            "Column"
        } else if expr.as_any().is::<Literal>() {
            "Literal"
        } else {
            "Other"
        }
        .to_owned();
        PhysicalExprDummyNode {
            expr,
            property: DummyProperty {
                expr_type: dummy_property,
            },
        }
    }

    #[test]
    fn test_build_dag() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
            Field::new("2", DataType::Int32, true),
        ]);
        let expr = binary(
            cast(
                binary(
                    col("0", &schema)?,
                    Operator::Plus,
                    col("1", &schema)?,
                    &schema,
                )?,
                &schema,
                DataType::Int64,
            )?,
            Operator::Gt,
            binary(
                cast(col("2", &schema)?, &schema, DataType::Int64)?,
                Operator::Plus,
                lit(ScalarValue::Int64(Some(10))),
                &schema,
            )?,
            &schema,
        )?;
        let mut vector_dummy_props = vec![];
        let (root, graph) = build_dag(expr, &make_dummy_node)?;
        let mut bfs = Bfs::new(&graph, root);
        while let Some(node_index) = bfs.next(&graph) {
            let node = &graph[node_index];
            vector_dummy_props.push(node.property.clone());
        }

        assert_eq!(
            vector_dummy_props
                .iter()
                .filter(|property| property.expr_type == "Binary")
                .count(),
            3
        );
        assert_eq!(
            vector_dummy_props
                .iter()
                .filter(|property| property.expr_type == "Column")
                .count(),
            3
        );
        assert_eq!(
            vector_dummy_props
                .iter()
                .filter(|property| property.expr_type == "Literal")
                .count(),
            1
        );
        assert_eq!(
            vector_dummy_props
                .iter()
                .filter(|property| property.expr_type == "Other")
                .count(),
            2
        );
        Ok(())
    }

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
