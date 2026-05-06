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

use std::sync::Arc;

use datafusion_common::{DataFusionError, NullEquality, Result, plan_err};
use datafusion_expr::{
    Expr, JoinType, LogicalPlan,
    utils::{check_all_columns_from_schema, split_conjunction_owned},
};

pub type NodeId = usize;

pub struct Node {
    pub plan: Arc<LogicalPlan>,
    pub(crate) connections: Vec<EdgeId>,
}

impl Node {
    pub fn connections(&self) -> &[EdgeId] {
        &self.connections
    }

    pub(crate) fn connection_with<'graph>(
        &self,
        node_id: NodeId,
        join_graph: &'graph JoinGraph,
    ) -> Option<&'graph Edge> {
        self.connections
            .iter()
            .filter_map(|edge_id| join_graph.get_edge(*edge_id))
            .find(move |x| x.nodes.contains(&node_id))
    }

    pub fn neighbours(&self, node_id: NodeId, join_graph: &JoinGraph) -> Vec<NodeId> {
        self.connections
            .iter()
            .filter_map(|edge_id| join_graph.get_edge(*edge_id))
            .flat_map(|edge| edge.nodes)
            .filter(|&id| id != node_id)
            .collect()
    }
}

pub type EdgeId = usize;

pub struct Edge {
    pub nodes: [NodeId; 2],
    pub on: Vec<(Expr, Expr)>,
    pub join_type: JoinType,
    pub null_equality: NullEquality,
}

pub struct JoinGraph {
    pub(crate) nodes: VecMap<Node>,
    edges: VecMap<Edge>,
    /// Non-equi predicates hoisted out of decomposed `Join.filter` clauses
    /// and out of `LogicalPlan::Filter` nodes that sit between joins.
    /// The enumerator must reapply these on top of the reordered plan.
    filters: Vec<Expr>,
}

impl JoinGraph {
    pub fn try_from_logical_plan(
        value: LogicalPlan,
    ) -> Result<(JoinGraph, Vec<LogicalPlan>), DataFusionError> {
        // First, extract the join subtree from any wrapper operators
        let (join_subtree, wrappers) = extract_join_subtree(value)?;

        // Now convert only the join subtree to a query graph
        let mut join_graph = JoinGraph::new();
        flatten_joins_recursive(join_subtree, &mut join_graph)?;
        Ok((join_graph, wrappers))
    }

    pub(crate) fn new() -> Self {
        Self {
            nodes: VecMap::new(),
            edges: VecMap::new(),
            filters: Vec::new(),
        }
    }
    pub fn filters(&self) -> &[Expr] {
        &self.filters
    }

    pub(crate) fn add_filter(&mut self, expr: Expr) {
        self.filters.push(expr);
    }

    pub(crate) fn add_node(&mut self, node_data: Arc<LogicalPlan>) -> NodeId {
        self.nodes.insert(Node {
            plan: node_data,
            connections: Vec::new(),
        })
    }

    pub fn add_node_with_edge(
        &mut self,
        other: NodeId,
        node_data: Arc<LogicalPlan>,
        on: Vec<(Expr, Expr)>,
        join_type: JoinType,
        null_equality: NullEquality,
    ) -> Option<NodeId> {
        if self.nodes.contains_key(other) {
            let new_id = self.nodes.insert(Node {
                plan: node_data,
                connections: Vec::new(),
            });
            self.add_edge(new_id, other, on, join_type, null_equality);
            Some(new_id)
        } else {
            None
        }
    }

    fn add_edge(
        &mut self,
        from: NodeId,
        to: NodeId,
        on: Vec<(Expr, Expr)>,
        join_type: JoinType,
        null_equality: NullEquality,
    ) -> Option<EdgeId> {
        if self.nodes.contains_key(from) && self.nodes.contains_key(to) {
            let edge_id = self.edges.insert(Edge {
                nodes: [from, to],
                on,
                join_type,
                null_equality,
            });
            if let Some(from) = self.nodes.get_mut(from) {
                from.connections.push(edge_id);
            }
            if let Some(to) = self.nodes.get_mut(to) {
                to.connections.push(edge_id);
            }
            Some(edge_id)
        } else {
            None
        }
    }

    pub fn remove_node(&mut self, node_id: NodeId) -> Option<Arc<LogicalPlan>> {
        if let Some(node) = self.nodes.remove(node_id) {
            // Remove all edges connected to this node
            for edge_id in &node.connections {
                if let Some(edge) = self.edges.remove(*edge_id) {
                    // Remove the edge from the other node's connections
                    for other_node_id in edge.nodes {
                        if other_node_id != node_id
                            && let Some(other_node) = self.nodes.get_mut(other_node_id)
                        {
                            other_node.connections.retain(|id| id != edge_id);
                        }
                    }
                }
            }
            Some(node.plan)
        } else {
            None
        }
    }

    pub fn remove_edge(&mut self, edge_id: EdgeId) -> Option<Edge> {
        if let Some(edge) = self.edges.remove(edge_id) {
            // Remove the edge from both nodes' connections
            for node_id in edge.nodes {
                if let Some(node) = self.nodes.get_mut(node_id) {
                    node.connections.retain(|id| *id != edge_id);
                }
            }
            Some(edge)
        } else {
            None
        }
    }

    pub(crate) fn nodes(&self) -> impl Iterator<Item = (NodeId, &Node)> {
        self.nodes.iter()
    }

    pub(crate) fn get_node(&self, key: NodeId) -> Option<&Node> {
        self.nodes.get(key)
    }

    pub(crate) fn get_edge(&self, key: EdgeId) -> Option<&Edge> {
        self.edges.get(key)
    }
}

/// Extracts the join subtree from a logical plan, separating it from wrapper operators.
///
/// This function traverses the plan tree from the root downward, collecting all non-join
/// operators until it finds the topmost join node. The join subtree (all consecutive joins)
/// is extracted and returned separately from the wrapper operators.
///
/// # Arguments
///
/// * `plan` - The logical plan to extract from
///
/// # Returns
///
/// Returns a tuple of (join_subtree, wrapper_operators) where:
/// - `join_subtree` is the topmost join and all joins beneath it
/// - `wrapper_operators` is a vector of non-join operators above the joins, in order from root to join
///
/// # Errors
///
/// Returns an error if the plan doesn't contain any joins.
pub(crate) fn extract_join_subtree(
    plan: LogicalPlan,
) -> Result<(LogicalPlan, Vec<LogicalPlan>)> {
    let mut wrappers = Vec::new();
    let mut current = plan;
    let original_display = current.display().to_string();

    // Descend through single-input non-join nodes until we find a join.
    // Wrappers that sit *between* joins are no longer rejected here; they are
    // handled inside `flatten_joins_recursive` (absorbed as opaque leaves or,
    // for `Filter` directly above a decomposable join, hoisted to the
    // side-channel). This pass only strips wrappers above the topmost join.
    loop {
        match current {
            LogicalPlan::Join(_) => {
                // Found the join subtree root
                return Ok((current, wrappers));
            }
            other => {
                let inputs = other.inputs();
                if inputs.is_empty() {
                    return plan_err!(
                        "Plan does not contain any join nodes: {}",
                        original_display
                    );
                }
                if inputs.len() != 1 {
                    return plan_err!(
                        "Join extraction only supports single-input operators, found {} inputs in: {}",
                        inputs.len(),
                        other.display()
                    );
                }

                let next = (*inputs[0]).clone();
                wrappers.push(other.clone());
                current = next;
            }
        }
    }
}

/// Reconstructs a logical plan by wrapping an optimized join plan with the original wrapper operators.
///
/// This function takes an optimized join plan and re-applies the wrapper operators (Filter, Sort,
/// Aggregate, etc.) that were removed during extraction. The wrappers are applied in reverse order
/// (innermost to outermost) to reconstruct the original plan structure.
///
/// # Arguments
///
/// * `join_plan` - The optimized join plan to wrap
/// * `wrappers` - Vector of wrapper operators in order from outermost to innermost (root to join)
///
/// # Returns
///
/// Returns the fully reconstructed logical plan with all wrapper operators reapplied.
///
/// # Errors
///
/// Returns an error if reconstructing any wrapper operator fails.
pub fn reconstruct_plan(
    join_plan: LogicalPlan,
    wrappers: Vec<LogicalPlan>,
) -> Result<LogicalPlan> {
    let mut current = join_plan;

    // Apply wrappers in reverse order (from innermost to outermost)
    for wrapper in wrappers.into_iter().rev() {
        // Use with_new_exprs to reconstruct the wrapper with the new input
        current = wrapper.with_new_exprs(wrapper.expressions(), vec![current])?;
    }

    Ok(current)
}

fn flatten_joins_recursive(
    plan: LogicalPlan,
    join_graph: &mut JoinGraph,
) -> Result<()> {
    match plan {
        // Inner joins decompose into the graph. (Cross joins are encoded as
        // Inner with an empty `on` list, which is also handled here: the
        // equi-key loop simply runs zero iterations and the children are
        // joined by absence of edges, matching cross-product connectivity.)
        // The join's `filter` clause is hoisted into the side-channel so the
        // enumerator can reapply it after reordering.
        LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {
            if let Some(filter) = join.filter.clone() {
                for conj in split_conjunction_owned(filter) {
                    join_graph.add_filter(conj);
                }
            }

            flatten_joins_recursive(
                Arc::unwrap_or_clone(Arc::clone(&join.left)),
                join_graph,
            )?;
            flatten_joins_recursive(
                Arc::unwrap_or_clone(Arc::clone(&join.right)),
                join_graph,
            )?;

            // Process each equijoin predicate to find which nodes it connects
            for (left_key, right_key) in &join.on {
                // Extract column references from both join keys
                let left_columns = left_key.column_refs();
                let right_columns = right_key.column_refs();

                // Filter nodes by checking which ones contain the columns from each expression
                let matching_nodes: Vec<NodeId> = join_graph
                    .nodes()
                    .filter_map(|(node_id, node)| {
                        let schema = node.plan.schema();
                        // Check if this node's schema contains columns from either left or right key
                        let has_left =
                            check_all_columns_from_schema(&left_columns, schema.as_ref())
                                .unwrap_or(false);
                        let has_right = check_all_columns_from_schema(
                            &right_columns,
                            schema.as_ref(),
                        )
                        .unwrap_or(false);

                        // Include node if it contains columns from either key (but not both, as that would be invalid)
                        if (has_left && !has_right) || (!has_left && has_right) {
                            Some(node_id)
                        } else {
                            None
                        }
                    })
                    .collect();

                // We should have exactly two nodes: one with left_key columns, one with right_key columns
                if matching_nodes.len() != 2 {
                    return plan_err!(
                        "Could not find exactly two nodes for join predicate: {} = {} (found {} nodes)",
                        left_key,
                        right_key,
                        matching_nodes.len()
                    );
                }

                let node_id_a = matching_nodes[0];
                let node_id_b = matching_nodes[1];

                // Add an edge if one doesn't exist yet
                if let Some(node_a) = join_graph.get_node(node_id_a)
                    && node_a.connection_with(node_id_b, join_graph).is_none()
                {
                    join_graph.add_edge(
                        node_id_a,
                        node_id_b,
                        join.on.clone(),
                        join.join_type,
                        join.null_equality,
                    );
                }
            }

            Ok(())
        }
        // Non-inner joins (Left/Right/Full/Semi/Anti/Mark) are not freely
        // reorderable, so the entire join subtree becomes one opaque leaf.
        LogicalPlan::Join(join) => {
            join_graph.add_node(Arc::new(LogicalPlan::Join(join)));
            Ok(())
        }
        // A `Filter` directly above a decomposable join is part of the join
        // region: hoist its conjuncts to the side-channel and recurse into
        // the join.
        LogicalPlan::Filter(filter)
            if matches!(
                filter.input.as_ref(),
                LogicalPlan::Join(j) if j.join_type == JoinType::Inner
            ) =>
        {
            for conj in split_conjunction_owned(filter.predicate) {
                join_graph.add_filter(conj);
            }
            let inner = Arc::unwrap_or_clone(filter.input);
            flatten_joins_recursive(inner, join_graph)
        }
        // Anything else (Aggregate, Projection, Sort, Limit, Window, Filter
        // not over a decomposable join, base scans, ...) is absorbed as an
        // opaque leaf. Joins nested inside such a wrapper are intentionally
        // hidden from the enumerator (matches Databend's dphyp behavior).
        other => {
            join_graph.add_node(Arc::new(other));
            Ok(())
        }
    }
}

/// A simple Vec-based map that uses Option<T> for sparse storage
/// Keys are never reused once removed
pub(crate) struct VecMap<V>(Vec<Option<V>>);

impl<V> VecMap<V> {
    pub(crate) fn new() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn insert(&mut self, value: V) -> usize {
        let idx = self.0.len();
        self.0.push(Some(value));
        idx
    }

    pub(crate) fn get(&self, key: usize) -> Option<&V> {
        self.0.get(key)?.as_ref()
    }

    pub(crate) fn get_mut(&mut self, key: usize) -> Option<&mut V> {
        self.0.get_mut(key)?.as_mut()
    }

    pub(crate) fn remove(&mut self, key: usize) -> Option<V> {
        self.0.get_mut(key)?.take()
    }

    pub(crate) fn contains_key(&self, key: usize) -> bool {
        self.0.get(key).and_then(|v| v.as_ref()).is_some()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (usize, &V)> {
        self.0
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| slot.as_ref().map(|v| (idx, v)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_expr::logical_plan::JoinType;
    use datafusion_expr::{LogicalPlanBuilder, col, lit};
    use datafusion_functions_aggregate::expr_fn::sum;

    fn edge_has_keys(edge: &Edge, key_a: &str, key_b: &str) -> bool {
        edge.on.iter().any(|(l, r)| {
            let s = format!("{l}{r}");
            s.contains(key_a) && s.contains(key_b)
        })
    }

    /// Test converting a three-way join with filter into a JoinGraph
    #[test]
    fn test_try_from_three_way_join_with_filter() -> Result<(), DataFusionError> {
        // Create three-way join: customer JOIN orders JOIN lineitem
        // with a filter on the orders-lineitem join
        let customer = scan_tpch_table("customer");
        let orders = scan_tpch_table("orders");
        let lineitem = scan_tpch_table("lineitem");

        let plan = LogicalPlanBuilder::from(customer.clone())
            .join(
                orders.clone(),
                JoinType::Inner,
                (vec!["c_custkey"], vec!["o_custkey"]),
                None,
            )
            .unwrap()
            .join_with_expr_keys(
                lineitem.clone(),
                JoinType::Inner,
                (vec![col("o_orderkey")], vec![col("l_orderkey")]),
                Some(col("l_quantity").gt(lit(10.0))),
            )
            .unwrap()
            .build()
            .unwrap();

        // Convert to JoinGraph
        let join_graph = JoinGraph::try_from_logical_plan(plan)?.0;

        // Verify structure: 3 nodes, 2 edges
        assert_eq!(join_graph.nodes().count(), 3);
        assert_eq!(join_graph.edges.iter().count(), 2);

        // Verify connectivity: one node has 2 connections (orders), two nodes have 1
        let mut connections: Vec<usize> = join_graph
            .nodes()
            .map(|(_, node)| node.connections().len())
            .collect();
        connections.sort();
        assert_eq!(connections, vec![1, 1, 2]);

        // Verify edges have correct join predicates
        let edges: Vec<&Edge> = join_graph.edges.iter().map(|(_, e)| e).collect();
        assert!(
            edges
                .iter()
                .any(|e| edge_has_keys(e, "c_custkey", "o_custkey")),
            "Missing customer-orders join"
        );
        assert!(
            edges
                .iter()
                .any(|e| edge_has_keys(e, "o_orderkey", "l_orderkey")),
            "Missing orders-lineitem join"
        );

        // The non-equi join.filter should now live in the side-channel
        assert_eq!(join_graph.filters().len(), 1);
        let f = format!("{}", join_graph.filters()[0]);
        assert!(
            f.contains("l_quantity"),
            "expected l_quantity in side-channel filter, got: {f}"
        );

        Ok(())
    }

    /// A Filter sitting between two inner joins should be hoisted to the
    /// side-channel and the joins on either side should still decompose.
    #[test]
    fn test_filter_between_inner_joins() -> Result<(), DataFusionError> {
        let customer = scan_tpch_table("customer");
        let orders = scan_tpch_table("orders");
        let lineitem = scan_tpch_table("lineitem");

        let plan = LogicalPlanBuilder::from(customer)
            .join(
                orders,
                JoinType::Inner,
                (vec!["c_custkey"], vec!["o_custkey"]),
                None,
            )
            .unwrap()
            .filter(col("o_totalprice").gt(lit(100.0)))
            .unwrap()
            .join(
                lineitem,
                JoinType::Inner,
                (vec!["o_orderkey"], vec!["l_orderkey"]),
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        let join_graph = JoinGraph::try_from_logical_plan(plan)?.0;

        assert_eq!(join_graph.nodes().count(), 3);
        assert_eq!(join_graph.edges.iter().count(), 2);
        assert_eq!(join_graph.filters().len(), 1);
        let f = format!("{}", join_graph.filters()[0]);
        assert!(
            f.contains("o_totalprice"),
            "expected o_totalprice in side-channel filter, got: {f}"
        );

        Ok(())
    }

    /// An Aggregate sitting between two inner joins is opaque: the entire
    /// subtree below it becomes one leaf Node and reordering only crosses
    /// the aggregate boundary, not through it.
    #[test]
    fn test_aggregate_between_inner_joins() -> Result<(), DataFusionError> {
        let customer = scan_tpch_table("customer");
        let orders = scan_tpch_table("orders");
        let lineitem = scan_tpch_table("lineitem");

        // (customer JOIN orders) -> Aggregate(group=o_orderkey) -> JOIN lineitem
        let plan = LogicalPlanBuilder::from(customer)
            .join(
                orders,
                JoinType::Inner,
                (vec!["c_custkey"], vec!["o_custkey"]),
                None,
            )
            .unwrap()
            .aggregate(vec![col("o_orderkey")], vec![sum(col("o_totalprice"))])
            .unwrap()
            .join(
                lineitem,
                JoinType::Inner,
                (vec!["o_orderkey"], vec!["l_orderkey"]),
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        let join_graph = JoinGraph::try_from_logical_plan(plan)?.0;

        // Two leaves: the aggregated subtree (opaque) and lineitem.
        assert_eq!(join_graph.nodes().count(), 2);
        assert_eq!(join_graph.edges.iter().count(), 1);
        assert_eq!(join_graph.filters().len(), 0);

        // One leaf must be a LogicalPlan::Aggregate.
        let has_aggregate_leaf = join_graph
            .nodes()
            .any(|(_, n)| matches!(n.plan.as_ref(), LogicalPlan::Aggregate(_)));
        assert!(
            has_aggregate_leaf,
            "expected an opaque-leaf Node whose plan is LogicalPlan::Aggregate"
        );

        Ok(())
    }

    /// A non-inner join nested inside an inner join should be wrapped as one
    /// opaque leaf, leaving only the surrounding inner-join structure visible
    /// to the enumerator.
    #[test]
    fn test_left_join_inside_inner_chain() -> Result<(), DataFusionError> {
        let customer = scan_tpch_table("customer");
        let orders = scan_tpch_table("orders");
        let lineitem = scan_tpch_table("lineitem");

        // (customer LEFT orders) INNER lineitem
        let plan = LogicalPlanBuilder::from(customer)
            .join(
                orders,
                JoinType::Left,
                (vec!["c_custkey"], vec!["o_custkey"]),
                None,
            )
            .unwrap()
            .join(
                lineitem,
                JoinType::Inner,
                (vec!["o_orderkey"], vec!["l_orderkey"]),
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        let join_graph = JoinGraph::try_from_logical_plan(plan)?.0;

        // Two leaves: the LEFT-join subtree (opaque) and lineitem.
        assert_eq!(join_graph.nodes().count(), 2);
        assert_eq!(join_graph.edges.iter().count(), 1);

        // The opaque-leaf node's plan should be a LogicalPlan::Join with Left type.
        let has_left_join_leaf = join_graph.nodes().any(|(_, n)| {
            matches!(
                n.plan.as_ref(),
                LogicalPlan::Join(j) if j.join_type == JoinType::Left
            )
        });
        assert!(
            has_left_join_leaf,
            "expected an opaque-leaf Node whose plan is a LEFT Join"
        );

        Ok(())
    }

    /// If the root of the plan is a non-inner join, the whole plan collapses
    /// into a single opaque leaf.
    #[test]
    fn test_root_non_inner_join_is_single_node() -> Result<(), DataFusionError> {
        let customer = scan_tpch_table("customer");
        let orders = scan_tpch_table("orders");

        let plan = LogicalPlanBuilder::from(customer)
            .join(
                orders,
                JoinType::Left,
                (vec!["c_custkey"], vec!["o_custkey"]),
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        let join_graph = JoinGraph::try_from_logical_plan(plan)?.0;

        assert_eq!(join_graph.nodes().count(), 1);
        assert_eq!(join_graph.edges.iter().count(), 0);
        assert_eq!(join_graph.filters().len(), 0);

        let only_node = join_graph.nodes().next().unwrap().1;
        assert!(matches!(
            only_node.plan.as_ref(),
            LogicalPlan::Join(j) if j.join_type == JoinType::Left
        ));

        Ok(())
    }
}
