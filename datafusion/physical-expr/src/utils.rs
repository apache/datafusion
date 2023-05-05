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

use crate::equivalence::{
    EquivalenceProperties, EquivalentClass, OrderedColumn, OrderingEquivalenceProperties,
    OrderingEquivalentClass,
};
use crate::expressions::{BinaryExpr, Column, UnKnownColumn};
use crate::{PhysicalExpr, PhysicalSortExpr, PhysicalSortRequirement};

use arrow::datatypes::SchemaRef;
use arrow_schema::SortOptions;
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRewriter, VisitRecursion,
};
use datafusion_common::Result;
use datafusion_expr::Operator;

use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use std::borrow::Borrow;
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

/// Normalize the output expressions based on Columns Map.
///
/// If there is a mapping in Columns Map, replace the Column in the output expressions with the 1st Column in the Columns Map.
/// Otherwise, replace the Column with a place holder of [UnKnownColumn]
///
pub fn normalize_out_expr_with_columns_map(
    expr: Arc<dyn PhysicalExpr>,
    columns_map: &HashMap<Column, Vec<Column>>,
) -> Arc<dyn PhysicalExpr> {
    expr.clone()
        .transform(&|expr| {
            let normalized_form: Option<Arc<dyn PhysicalExpr>> = match expr
                .as_any()
                .downcast_ref::<Column>()
            {
                Some(column) => columns_map
                    .get(column)
                    .map(|c| Arc::new(c[0].clone()) as _)
                    .or_else(|| Some(Arc::new(UnKnownColumn::new(column.name())) as _)),
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
                expr.as_any().downcast_ref::<Column>().and_then(|column| {
                    for class in eq_properties {
                        if class.contains(column) {
                            return Some(Arc::new(class.head().clone()) as _);
                        }
                    }
                    None
                });
            Ok(if let Some(normalized_form) = normalized_form {
                Transformed::Yes(normalized_form)
            } else {
                Transformed::No(expr)
            })
        })
        .unwrap_or(expr)
}

fn normalize_sort_expr_with_equivalence_properties(
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

fn normalize_sort_requirement_with_equivalence_properties(
    sort_requirement: PhysicalSortRequirement,
    eq_properties: &[EquivalentClass],
) -> PhysicalSortRequirement {
    let normalized_expr = normalize_expr_with_equivalence_properties(
        sort_requirement.expr().clone(),
        eq_properties,
    );
    if sort_requirement.expr().ne(&normalized_expr) {
        sort_requirement.with_expr(normalized_expr)
    } else {
        sort_requirement
    }
}

fn normalize_expr_with_ordering_equivalence_properties(
    expr: Arc<dyn PhysicalExpr>,
    sort_options: Option<SortOptions>,
    eq_properties: &[OrderingEquivalentClass],
) -> Arc<dyn PhysicalExpr> {
    expr.clone()
        .transform(&|expr| {
            let normalized_form =
                expr.as_any().downcast_ref::<Column>().and_then(|column| {
                    if let Some(options) = sort_options {
                        for class in eq_properties {
                            let ordered_column = OrderedColumn {
                                col: column.clone(),
                                options,
                            };
                            if class.contains(&ordered_column) {
                                return Some(class.head().clone());
                            }
                        }
                    }
                    None
                });
            Ok(if let Some(normalized_form) = normalized_form {
                Transformed::Yes(Arc::new(normalized_form.col) as _)
            } else {
                Transformed::No(expr)
            })
        })
        .unwrap_or(expr)
}

fn normalize_sort_expr_with_ordering_equivalence_properties(
    sort_expr: PhysicalSortExpr,
    eq_properties: &[OrderingEquivalentClass],
) -> PhysicalSortExpr {
    let requirement = normalize_sort_requirement_with_ordering_equivalence_properties(
        PhysicalSortRequirement::from(sort_expr),
        eq_properties,
    );
    requirement.into_sort_expr()
}

fn normalize_sort_requirement_with_ordering_equivalence_properties(
    sort_requirement: PhysicalSortRequirement,
    eq_properties: &[OrderingEquivalentClass],
) -> PhysicalSortRequirement {
    let normalized_expr = normalize_expr_with_ordering_equivalence_properties(
        sort_requirement.expr().clone(),
        sort_requirement.options(),
        eq_properties,
    );
    if sort_requirement.expr().eq(&normalized_expr) {
        sort_requirement
    } else {
        let mut options = sort_requirement.options();
        if let Some(col) = normalized_expr.as_any().downcast_ref::<Column>() {
            for eq_class in eq_properties.iter() {
                let head = eq_class.head();
                if head.col.eq(col) {
                    // If there is a requirement, update it with the requirement of its normalized version.
                    if let Some(options) = &mut options {
                        *options = head.options;
                    }
                    break;
                }
            }
        }
        PhysicalSortRequirement::new(normalized_expr, options)
    }
}

pub fn normalize_sort_expr(
    sort_expr: PhysicalSortExpr,
    eq_properties: &[EquivalentClass],
    ordering_eq_properties: &[OrderingEquivalentClass],
) -> PhysicalSortExpr {
    let normalized =
        normalize_sort_expr_with_equivalence_properties(sort_expr, eq_properties);
    normalize_sort_expr_with_ordering_equivalence_properties(
        normalized,
        ordering_eq_properties,
    )
}

pub fn normalize_sort_requirement(
    sort_requirement: PhysicalSortRequirement,
    eq_classes: &[EquivalentClass],
    ordering_eq_classes: &[OrderingEquivalentClass],
) -> PhysicalSortRequirement {
    let normalized = normalize_sort_requirement_with_equivalence_properties(
        sort_requirement,
        eq_classes,
    );
    normalize_sort_requirement_with_ordering_equivalence_properties(
        normalized,
        ordering_eq_classes,
    )
}

/// Checks whether given ordering requirements are satisfied by provided [PhysicalSortExpr]s.
pub fn ordering_satisfy<
    F: FnOnce() -> EquivalenceProperties,
    F2: FnOnce() -> OrderingEquivalenceProperties,
>(
    provided: Option<&[PhysicalSortExpr]>,
    required: Option<&[PhysicalSortExpr]>,
    equal_properties: F,
    ordering_equal_properties: F2,
) -> bool {
    match (provided, required) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(provided), Some(required)) => ordering_satisfy_concrete(
            provided,
            required,
            equal_properties,
            ordering_equal_properties,
        ),
    }
}

/// Checks whether the required [`PhysicalSortExpr`]s are satisfied by the
/// provided [`PhysicalSortExpr`]s.
fn ordering_satisfy_concrete<
    F: FnOnce() -> EquivalenceProperties,
    F2: FnOnce() -> OrderingEquivalenceProperties,
>(
    provided: &[PhysicalSortExpr],
    required: &[PhysicalSortExpr],
    equal_properties: F,
    ordering_equal_properties: F2,
) -> bool {
    if required.len() > provided.len() {
        false
    } else if required
        .iter()
        .zip(provided.iter())
        .all(|(req, given)| req.eq(given))
    {
        true
    } else {
        let oeq_properties = ordering_equal_properties();
        let ordering_eq_classes = oeq_properties.classes();
        let eq_properties = equal_properties();
        let eq_classes = eq_properties.classes();
        required
            .iter()
            .map(|e| normalize_sort_expr(e.clone(), eq_classes, ordering_eq_classes))
            .zip(
                provided.iter().map(|e| {
                    normalize_sort_expr(e.clone(), eq_classes, ordering_eq_classes)
                }),
            )
            .all(|(req, given)| req == given)
    }
}

/// Checks whether the given [`PhysicalSortRequirement`]s are satisfied by the
/// provided [`PhysicalSortExpr`]s.
pub fn ordering_satisfy_requirement<
    F: FnOnce() -> EquivalenceProperties,
    F2: FnOnce() -> OrderingEquivalenceProperties,
>(
    provided: Option<&[PhysicalSortExpr]>,
    required: Option<&[PhysicalSortRequirement]>,
    equal_properties: F,
    ordering_equal_properties: F2,
) -> bool {
    match (provided, required) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(provided), Some(required)) => ordering_satisfy_requirement_concrete(
            provided,
            required,
            equal_properties,
            ordering_equal_properties,
        ),
    }
}

/// Checks whether the given [`PhysicalSortRequirement`]s are satisfied by the
/// provided [`PhysicalSortExpr`]s.
pub fn ordering_satisfy_requirement_concrete<
    F: FnOnce() -> EquivalenceProperties,
    F2: FnOnce() -> OrderingEquivalenceProperties,
>(
    provided: &[PhysicalSortExpr],
    required: &[PhysicalSortRequirement],
    equal_properties: F,
    ordering_equal_properties: F2,
) -> bool {
    if required.len() > provided.len() {
        false
    } else if required
        .iter()
        .zip(provided.iter())
        .all(|(req, given)| given.satisfy(req))
    {
        true
    } else {
        let oeq_properties = ordering_equal_properties();
        let ordering_eq_classes = oeq_properties.classes();
        let eq_properties = equal_properties();
        let eq_classes = eq_properties.classes();
        required
            .iter()
            .map(|e| {
                normalize_sort_requirement(e.clone(), eq_classes, ordering_eq_classes)
            })
            .zip(
                provided.iter().map(|e| {
                    normalize_sort_expr(e.clone(), eq_classes, ordering_eq_classes)
                }),
            )
            .all(|(req, given)| given.satisfy(&req))
    }
}

/// Checks whether the given [`PhysicalSortRequirement`]s are equal or more
/// specific than the provided [`PhysicalSortRequirement`]s.
pub fn requirements_compatible<
    F: FnOnce() -> OrderingEquivalenceProperties,
    F2: FnOnce() -> EquivalenceProperties,
>(
    provided: Option<&[PhysicalSortRequirement]>,
    required: Option<&[PhysicalSortRequirement]>,
    ordering_equal_properties: F,
    equal_properties: F2,
) -> bool {
    match (provided, required) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(provided), Some(required)) => requirements_compatible_concrete(
            provided,
            required,
            ordering_equal_properties,
            equal_properties,
        ),
    }
}

/// Checks whether the given [`PhysicalSortRequirement`]s are equal or more
/// specific than the provided [`PhysicalSortRequirement`]s.
fn requirements_compatible_concrete<
    F: FnOnce() -> OrderingEquivalenceProperties,
    F2: FnOnce() -> EquivalenceProperties,
>(
    provided: &[PhysicalSortRequirement],
    required: &[PhysicalSortRequirement],
    ordering_equal_properties: F,
    equal_properties: F2,
) -> bool {
    if required.len() > provided.len() {
        false
    } else if required
        .iter()
        .zip(provided.iter())
        .all(|(req, given)| given.compatible(req))
    {
        true
    } else {
        let oeq_properties = ordering_equal_properties();
        let ordering_eq_classes = oeq_properties.classes();
        let eq_properties = equal_properties();
        let eq_classes = eq_properties.classes();
        required
            .iter()
            .map(|e| {
                normalize_sort_requirement(e.clone(), eq_classes, ordering_eq_classes)
            })
            .zip(provided.iter().map(|e| {
                normalize_sort_requirement(e.clone(), eq_classes, ordering_eq_classes)
            }))
            .all(|(req, given)| given.compatible(&req))
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
            r.as_any()
                .downcast_ref::<Column>()
                .and_then(|c| column_mapping.get(c.name()))
        })
        .map(|e| Arc::new(e.clone()) as _)
        .collect()
}

/// This function returns all `Arc<dyn PhysicalExpr>`s inside the given
/// `PhysicalSortExpr` sequence.
pub fn convert_to_expr<T: Borrow<PhysicalSortExpr>>(
    sequence: impl IntoIterator<Item = T>,
) -> Vec<Arc<dyn PhysicalExpr>> {
    sequence
        .into_iter()
        .map(|elem| elem.borrow().expr.clone())
        .collect()
}

/// This function finds the indices of `targets` within `items`, taking into
/// account equivalences according to `equal_properties`.
pub fn get_indices_of_matching_exprs<
    T: Borrow<Arc<dyn PhysicalExpr>>,
    F: FnOnce() -> EquivalenceProperties,
>(
    targets: impl IntoIterator<Item = T>,
    items: &[Arc<dyn PhysicalExpr>],
    equal_properties: F,
) -> Vec<usize> {
    if let eq_classes @ [_, ..] = equal_properties().classes() {
        let normalized_targets = targets.into_iter().map(|e| {
            normalize_expr_with_equivalence_properties(e.borrow().clone(), eq_classes)
        });
        let normalized_items = items
            .iter()
            .map(|e| normalize_expr_with_equivalence_properties(e.clone(), eq_classes))
            .collect::<Vec<_>>();
        get_indices_of_exprs_strict(normalized_targets, &normalized_items)
    } else {
        get_indices_of_exprs_strict(targets, items)
    }
}

/// This function finds the indices of `targets` within `items` using strict
/// equality.
fn get_indices_of_exprs_strict<T: Borrow<Arc<dyn PhysicalExpr>>>(
    targets: impl IntoIterator<Item = T>,
    items: &[Arc<dyn PhysicalExpr>],
) -> Vec<usize> {
    targets
        .into_iter()
        .filter_map(|target| items.iter().position(|e| e.eq(target.borrow())))
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
    pred.transform_down(&|expr| {
        let expr_any = expr.as_any();

        if let Some(column) = expr_any.downcast_ref::<Column>() {
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
    use crate::expressions::{binary, cast, col, in_list, lit, Column, Literal};
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

    // Generate a schema which consists of 5 columns (a, b, c, d, e)
    fn create_test_schema() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, true);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, true);
        let e = Field::new("e", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e]));

        Ok(schema)
    }

    fn create_test_params() -> Result<(
        SchemaRef,
        EquivalenceProperties,
        OrderingEquivalenceProperties,
    )> {
        // Assume schema satisfies ordering a ASC NULLS LAST
        // and d ASC NULLS LAST and e DESC NULLS FIRST
        // Assume that column a and c are aliases.
        let col_a = &Column::new("a", 0);
        let _col_b = &Column::new("b", 1);
        let col_c = &Column::new("c", 2);
        let col_d = &Column::new("d", 3);
        let col_e = &Column::new("e", 4);
        let option1 = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option2 = SortOptions {
            descending: true,
            nulls_first: true,
        };
        let test_schema = create_test_schema()?;
        let mut eq_properties = EquivalenceProperties::new(test_schema.clone());
        eq_properties.add_equal_conditions((col_a, col_c));
        let mut ordering_eq_properties =
            OrderingEquivalenceProperties::new(test_schema.clone());
        ordering_eq_properties.add_equal_conditions((
            &OrderedColumn::new(col_a.clone(), option1),
            &OrderedColumn::new(col_d.clone(), option1),
        ));
        ordering_eq_properties.add_equal_conditions((
            &OrderedColumn::new(col_a.clone(), option1),
            &OrderedColumn::new(col_e.clone(), option2),
        ));
        Ok((test_schema, eq_properties, ordering_eq_properties))
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
    fn test_convert_to_expr() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::UInt64, false)]);
        let sort_expr = vec![PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: Default::default(),
        }];
        assert!(convert_to_expr(&sort_expr)[0].eq(&sort_expr[0].expr));
        Ok(())
    }

    #[test]
    fn test_get_indices_of_matching_exprs() {
        let empty_schema = &Arc::new(Schema::empty());
        let equal_properties = || EquivalenceProperties::new(empty_schema.clone());
        let list1: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("d", 3)),
        ];
        let list2: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("a", 0)),
        ];
        assert_eq!(
            get_indices_of_matching_exprs(&list1, &list2, equal_properties),
            vec![2, 0, 1]
        );
        assert_eq!(
            get_indices_of_matching_exprs(&list2, &list1, equal_properties),
            vec![1, 2, 0]
        );
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
        let empty_schema = &Arc::new(Schema::empty());
        assert!(ordering_satisfy(
            finer,
            crude,
            || { EquivalenceProperties::new(empty_schema.clone()) },
            || { OrderingEquivalenceProperties::new(empty_schema.clone()) },
        ));
        assert!(!ordering_satisfy(
            crude,
            finer,
            || { EquivalenceProperties::new(empty_schema.clone()) },
            || { OrderingEquivalenceProperties::new(empty_schema.clone()) },
        ));
        Ok(())
    }

    #[test]
    fn test_ordering_satisfy_with_equivalence() -> Result<()> {
        let col_a = &Column::new("a", 0);
        let col_b = &Column::new("b", 1);
        let col_c = &Column::new("c", 2);
        let col_d = &Column::new("d", 3);
        let col_e = &Column::new("e", 4);
        let option1 = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option2 = SortOptions {
            descending: true,
            nulls_first: true,
        };
        // The schema is ordered by a ASC NULLS LAST, b ASC NULLS LAST
        let provided = vec![
            PhysicalSortExpr {
                expr: Arc::new(col_a.clone()),
                options: option1,
            },
            PhysicalSortExpr {
                expr: Arc::new(col_b.clone()),
                options: option1,
            },
        ];
        let provided = Some(&provided[..]);
        let (_test_schema, eq_properties, ordering_eq_properties) = create_test_params()?;
        // First element in the tuple stores vector of requirement, second element is the expected return value for ordering_satisfy function
        let requirements = vec![
            // `a ASC NULLS LAST`, expects `ordering_satisfy` to be `true`, since existing ordering `a ASC NULLS LAST, b ASC NULLS LAST` satisfies it
            (vec![(col_a, option1)], true),
            (vec![(col_a, option2)], false),
            // Test whether equivalence works as expected
            (vec![(col_c, option1)], true),
            (vec![(col_c, option2)], false),
            // Test whether ordering equivalence works as expected
            (vec![(col_d, option1)], true),
            (vec![(col_d, option2)], false),
            (vec![(col_e, option2)], true),
            (vec![(col_e, option1)], false),
        ];
        for (cols, expected) in requirements {
            let err_msg = format!("Error in test case:{cols:?}");
            let required = cols
                .into_iter()
                .map(|(col, options)| PhysicalSortExpr {
                    expr: Arc::new(col.clone()),
                    options,
                })
                .collect::<Vec<_>>();

            let required = Some(&required[..]);
            assert_eq!(
                ordering_satisfy(
                    provided,
                    required,
                    || eq_properties.clone(),
                    || ordering_eq_properties.clone(),
                ),
                expected,
                "{err_msg}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_reassign_predicate_columns_in_list() {
        let int_field = Field::new("should_not_matter", DataType::Int64, true);
        let dict_field = Field::new(
            "id",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        );
        let schema_small = Arc::new(Schema::new(vec![dict_field.clone()]));
        let schema_big = Arc::new(Schema::new(vec![int_field, dict_field]));
        let pred = in_list(
            Arc::new(Column::new_with_schema("id", &schema_big).unwrap()),
            vec![lit(ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::from("2")),
            ))],
            &false,
            &schema_big,
        )
        .unwrap();

        let actual = reassign_predicate_columns(pred, &schema_small, false).unwrap();

        let expected = in_list(
            Arc::new(Column::new_with_schema("id", &schema_small).unwrap()),
            vec![lit(ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::from("2")),
            ))],
            &false,
            &schema_small,
        )
        .unwrap();

        assert_eq!(actual.as_ref(), expected.as_any());
    }

    #[test]
    fn test_normalize_expr_with_equivalence() -> Result<()> {
        let col_a = &Column::new("a", 0);
        let _col_b = &Column::new("b", 1);
        let col_c = &Column::new("c", 2);
        let col_d = &Column::new("d", 3);
        let col_e = &Column::new("e", 4);
        let option1 = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option2 = SortOptions {
            descending: true,
            nulls_first: true,
        };
        // Assume schema satisfies ordering a ASC NULLS LAST
        // and d ASC NULLS LAST and e DESC NULLS FIRST
        // Assume that column a and c are aliases.
        let (_test_schema, eq_properties, ordering_eq_properties) = create_test_params()?;

        let col_a_expr = Arc::new(col_a.clone()) as Arc<dyn PhysicalExpr>;
        let col_c_expr = Arc::new(col_c.clone()) as Arc<dyn PhysicalExpr>;
        let col_d_expr = Arc::new(col_d.clone()) as Arc<dyn PhysicalExpr>;
        let col_e_expr = Arc::new(col_e.clone()) as Arc<dyn PhysicalExpr>;
        // Test cases for equivalence normalization,
        // First entry in the tuple is argument, second entry is expected result after normalization.
        let expressions = vec![(&col_a_expr, &col_a_expr), (&col_c_expr, &col_a_expr)];
        for (expr, expected_eq) in expressions {
            assert!(
                expected_eq.eq(&normalize_expr_with_equivalence_properties(
                    expr.clone(),
                    eq_properties.classes()
                )),
                "error in test: expr: {expr:?}"
            );
        }

        // Test cases for ordering equivalence normalization
        // First entry in the tuple is PhysicalExpr, second entry is its ordering, third entry is result after normalization.
        let expressions = vec![
            (&col_d_expr, option1, &col_a_expr),
            (&col_e_expr, option2, &col_a_expr),
            // Cannot normalize, hence should return itself.
            (&col_e_expr, option1, &col_e_expr),
        ];
        for (expr, sort_options, expected_ordering_eq) in expressions {
            assert!(
                expected_ordering_eq.eq(
                    &normalize_expr_with_ordering_equivalence_properties(
                        expr.clone(),
                        Some(sort_options),
                        ordering_eq_properties.classes()
                    )
                ),
                "error in test: expr: {expr:?}, sort_options: {sort_options:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_normalize_sort_expr_with_equivalence() -> Result<()> {
        let col_a = &Column::new("a", 0);
        let _col_b = &Column::new("b", 1);
        let col_c = &Column::new("c", 2);
        let col_d = &Column::new("d", 3);
        let col_e = &Column::new("e", 4);
        let option1 = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option2 = SortOptions {
            descending: true,
            nulls_first: true,
        };
        // Assume schema satisfies ordering a ASC NULLS LAST
        // and d ASC NULLS LAST and e DESC NULLS FIRST
        // Assume that column a and c are aliases.
        let (_test_schema, eq_properties, ordering_eq_properties) = create_test_params()?;

        // Test cases for equivalence normalization
        // First entry in the tuple is PhysicalExpr, second entry is its ordering, third entry is result after normalization.
        let expressions = vec![
            (&col_a, option1, &col_a, option1),
            (&col_c, option1, &col_a, option1),
            // Cannot normalize column d, since it is not in equivalence properties.
            (&col_d, option1, &col_d, option1),
        ];
        for (expr, sort_options, expected_col, expected_options) in
            expressions.into_iter()
        {
            let expected = PhysicalSortExpr {
                expr: Arc::new((*expected_col).clone()) as _,
                options: expected_options,
            };
            let arg = PhysicalSortExpr {
                expr: Arc::new((*expr).clone()) as _,
                options: sort_options,
            };
            assert!(
                expected.eq(&normalize_sort_expr_with_equivalence_properties(
                    arg.clone(),
                    eq_properties.classes()
                )),
                "error in test: expr: {expr:?}, sort_options: {sort_options:?}"
            );
        }

        // Test cases for ordering equivalence normalization
        // First entry in the tuple is PhysicalExpr, second entry is its ordering, third entry is result after normalization.
        let expressions = vec![
            (&col_d, option1, &col_a, option1),
            (&col_e, option2, &col_a, option1),
        ];
        for (expr, sort_options, expected_col, expected_options) in
            expressions.into_iter()
        {
            let expected = PhysicalSortExpr {
                expr: Arc::new((*expected_col).clone()) as _,
                options: expected_options,
            };
            let arg = PhysicalSortExpr {
                expr: Arc::new((*expr).clone()) as _,
                options: sort_options,
            };
            assert!(
                expected.eq(&normalize_sort_expr_with_ordering_equivalence_properties(
                    arg.clone(),
                    ordering_eq_properties.classes()
                )),
                "error in test: expr: {expr:?}, sort_options: {sort_options:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_normalize_sort_requirement_with_equivalence() -> Result<()> {
        let col_a = &Column::new("a", 0);
        let _col_b = &Column::new("b", 1);
        let col_c = &Column::new("c", 2);
        let col_d = &Column::new("d", 3);
        let col_e = &Column::new("e", 4);
        let option1 = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option2 = SortOptions {
            descending: true,
            nulls_first: true,
        };
        // Assume schema satisfies ordering a ASC NULLS LAST
        // and d ASC NULLS LAST and e DESC NULLS FIRST
        // Assume that column a and c are aliases.
        let (_test_schema, eq_properties, ordering_eq_properties) = create_test_params()?;

        // Test cases for equivalence normalization
        // First entry in the tuple is PhysicalExpr, second entry is its ordering, third entry is result after normalization.
        let expressions = vec![
            (&col_a, Some(option1), &col_a, Some(option1)),
            (&col_c, Some(option1), &col_a, Some(option1)),
            (&col_c, None, &col_a, None),
            // Cannot normalize column d, since it is not in equivalence properties.
            (&col_d, Some(option1), &col_d, Some(option1)),
        ];
        for (expr, sort_options, expected_col, expected_options) in
            expressions.into_iter()
        {
            let expected = PhysicalSortRequirement::new(
                Arc::new((*expected_col).clone()) as _,
                expected_options,
            );
            let arg = PhysicalSortRequirement::new(
                Arc::new((*expr).clone()) as _,
                sort_options,
            );
            assert!(
                expected.eq(&normalize_sort_requirement_with_equivalence_properties(
                    arg.clone(),
                    eq_properties.classes()
                )),
                "error in test: expr: {expr:?}, sort_options: {sort_options:?}"
            );
        }

        // Test cases for ordering equivalence normalization
        // First entry in the tuple is PhysicalExpr, second entry is its ordering, third entry is result after normalization.
        let expressions = vec![
            (&col_d, Some(option1), &col_a, Some(option1)),
            (&col_e, Some(option2), &col_a, Some(option1)),
        ];
        for (expr, sort_options, expected_col, expected_options) in
            expressions.into_iter()
        {
            let expected = PhysicalSortRequirement::new(
                Arc::new((*expected_col).clone()) as _,
                expected_options,
            );
            let arg = PhysicalSortRequirement::new(
                Arc::new((*expr).clone()) as _,
                sort_options,
            );
            assert!(
                expected.eq(
                    &normalize_sort_requirement_with_ordering_equivalence_properties(
                        arg.clone(),
                        ordering_eq_properties.classes()
                    )
                ),
                "error in test: expr: {expr:?}, sort_options: {sort_options:?}"
            );
        }
        Ok(())
    }
}
