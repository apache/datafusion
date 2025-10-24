use std::sync::Arc;

use datafusion_common::{
    plan_err,
    tree_node::{TreeNode, TreeNodeRecursion},
    DataFusionError, Result,
};
use datafusion_expr::{utils::check_all_columns_from_schema, Join, LogicalPlan};

pub type NodeId = usize;

pub struct Node {
    pub plan: Arc<LogicalPlan>,
    pub(crate) connections: Vec<EdgeId>,
}

impl Node {
    pub(crate) fn connections(&self) -> &[EdgeId] {
        &self.connections
    }

    pub(crate) fn connection_with<'graph>(
        &self,
        node_id: NodeId,
        query_graph: &'graph QueryGraph,
    ) -> Option<&'graph Edge> {
        self.connections
            .iter()
            .filter_map(|edge_id| query_graph.get_edge(*edge_id))
            .find(move |x| x.nodes.contains(&node_id))
    }

    pub(crate) fn neighbours(
        &self,
        node_id: NodeId,
        query_graph: &QueryGraph,
    ) -> Vec<NodeId> {
        self.connections
            .iter()
            .filter_map(|edge_id| query_graph.get_edge(*edge_id))
            .flat_map(|edge| edge.nodes)
            .filter(|&id| id != node_id)
            .collect()
    }
}

pub type EdgeId = usize;

pub struct Edge {
    pub nodes: [NodeId; 2],
    pub join: Join,
}

pub struct QueryGraph {
    pub(crate) nodes: VecMap<Node>,
    edges: VecMap<Edge>,
}

impl QueryGraph {
    pub(crate) fn new() -> Self {
        Self {
            nodes: VecMap::new(),
            edges: VecMap::new(),
        }
    }

    pub(crate) fn add_node(&mut self, node_data: Arc<LogicalPlan>) -> NodeId {
        self.nodes.insert(Node {
            plan: node_data,
            connections: Vec::new(),
        })
    }

    pub(crate) fn add_node_with_edge(
        &mut self,
        other: NodeId,
        node_data: Arc<LogicalPlan>,
        edge_data: Join,
    ) -> Option<NodeId> {
        if self.nodes.contains_key(other) {
            let new_id = self.nodes.insert(Node {
                plan: node_data,
                connections: Vec::new(),
            });
            self.add_edge(new_id, other, edge_data);
            Some(new_id)
        } else {
            None
        }
    }

    fn add_edge(&mut self, from: NodeId, to: NodeId, data: Join) -> Option<EdgeId> {
        if self.nodes.contains_key(from) && self.nodes.contains_key(to) {
            let edge_id = self.edges.insert(Edge {
                nodes: [from, to],
                join: data,
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

    pub(crate) fn remove_node(&mut self, node_id: NodeId) -> Option<Arc<LogicalPlan>> {
        if let Some(node) = self.nodes.remove(node_id) {
            // Remove all edges connected to this node
            for edge_id in &node.connections {
                if let Some(edge) = self.edges.remove(*edge_id) {
                    // Remove the edge from the other node's connections
                    for other_node_id in edge.nodes {
                        if other_node_id != node_id {
                            if let Some(other_node) = self.nodes.get_mut(other_node_id) {
                                other_node.connections.retain(|id| id != edge_id);
                            }
                        }
                    }
                }
            }
            Some(node.plan)
        } else {
            None
        }
    }

    fn remove_edge(&mut self, edge_id: EdgeId) -> Option<Join> {
        if let Some(edge) = self.edges.remove(edge_id) {
            // Remove the edge from both nodes' connections
            for node_id in edge.nodes {
                if let Some(node) = self.nodes.get_mut(node_id) {
                    node.connections.retain(|id| *id != edge_id);
                }
            }
            Some(edge.join)
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

    // Descend through non-join nodes until we find a join
    loop {
        match current {
            LogicalPlan::Join(_) => {
                // Found the join subtree root
                return Ok((current, wrappers));
            }
            other => {
                // Check if this node contains joins in its children
                if !contains_join(&other) {
                    return plan_err!(
                        "Plan does not contain any join nodes: {}",
                        other.display()
                    );
                }

                // This node is a wrapper - store it and descend to its child
                // For now, we only support single-child wrappers (Filter, Sort, Limit, Aggregate, etc.)
                let inputs = other.inputs();
                if inputs.len() != 1 {
                    return plan_err!(
                        "Join extraction only supports single-input operators, found {} inputs in: {}",
                        inputs.len(),
                        other.display()
                    );
                }

                wrappers.push(other.clone());
                current = (*inputs[0]).clone();
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
pub(crate) fn reconstruct_plan(
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

impl TryFrom<LogicalPlan> for QueryGraph {
    type Error = DataFusionError;

    fn try_from(value: LogicalPlan) -> Result<Self, Self::Error> {
        // First, extract the join subtree from any wrapper operators
        let (join_subtree, _wrappers) = extract_join_subtree(value)?;

        // Now convert only the join subtree to a query graph
        let mut query_graph = QueryGraph::new();
        flatten_joins_recursive(join_subtree, &mut query_graph)?;
        Ok(query_graph)
    }
}

fn flatten_joins_recursive(
    plan: LogicalPlan,
    query_graph: &mut QueryGraph,
) -> Result<()> {
    match plan {
        LogicalPlan::Join(join) => {
            flatten_joins_recursive(
                Arc::unwrap_or_clone(Arc::clone(&join.left)),
                query_graph,
            )?;
            flatten_joins_recursive(
                Arc::unwrap_or_clone(Arc::clone(&join.right)),
                query_graph,
            )?;

            // Process each equijoin predicate to find which nodes it connects
            for (left_key, right_key) in &join.on {
                // Extract column references from both join keys
                let left_columns = left_key.column_refs();
                let right_columns = right_key.column_refs();

                // Filter nodes by checking which ones contain the columns from each expression
                let matching_nodes: Vec<NodeId> = query_graph
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
                if let Some(node_a) = query_graph.get_node(node_id_a) {
                    if node_a.connection_with(node_id_b, query_graph).is_none() {
                        // No edge exists yet, create one with this join
                        query_graph.add_edge(node_id_a, node_id_b, join.clone());
                    }
                }
            }

            Ok(())
        }
        x => {
            if contains_join(&x) {
                plan_err!(
                    "Join reordering requires joins to be consecutive in the plan tree. \
                     Found a non-join node that contains nested joins: {}",
                    x.display()
                )
            } else {
                query_graph.add_node(Arc::new(x));
                Ok(())
            }
        }
    }
}

/// Checks if a LogicalPlan contains any join nodes
///
/// Uses a TreeNode visitor to traverse the plan tree and detect the presence
/// of any `LogicalPlan::Join` nodes.
///
/// # Arguments
///
/// * `plan` - The logical plan to check
///
/// # Returns
///
/// `true` if the plan contains at least one join node, `false` otherwise
pub(crate) fn contains_join(plan: &LogicalPlan) -> bool {
    let mut has_join = false;

    // Use TreeNode's apply method to traverse the plan
    let _ = plan.apply(|node| {
        if matches!(node, LogicalPlan::Join(_)) {
            has_join = true;
            // Stop traversal once we find a join
            Ok(TreeNodeRecursion::Stop)
        } else {
            // Continue traversal
            Ok(TreeNodeRecursion::Continue)
        }
    });

    has_join
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
    use datafusion_expr::{col, LogicalPlanBuilder};

    /// Test converting a three-way join with filter into a QueryGraph
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
                Some(col("l_quantity").gt(datafusion_expr::lit(10.0))),
            )
            .unwrap()
            .build()
            .unwrap();

        // Convert to QueryGraph
        let query_graph = QueryGraph::try_from(plan)?;

        // Verify structure: 3 nodes, 2 edges
        assert_eq!(query_graph.nodes().count(), 3);
        assert_eq!(query_graph.edges.iter().count(), 2);

        // Verify connectivity: one node has 2 connections (orders), two nodes have 1
        let mut connections: Vec<usize> = query_graph
            .nodes()
            .map(|(_, node)| node.connections().len())
            .collect();
        connections.sort();
        assert_eq!(connections, vec![1, 1, 2]);

        // Verify edges have correct join predicates
        let edges: Vec<&Edge> = query_graph.edges.iter().map(|(_, e)| e).collect();

        // One edge should have c_custkey = o_custkey
        let has_customer_orders = edges.iter().any(|e| {
            e.join.on.iter().any(|(l, r)| {
                let s = format!("{l}{r}");
                s.contains("c_custkey") && s.contains("o_custkey")
            })
        });
        assert!(has_customer_orders, "Missing customer-orders join");

        // One edge should have o_orderkey = l_orderkey with a filter
        let has_orders_lineitem = edges.iter().any(|e| {
            e.join.on.iter().any(|(l, r)| {
                let s = format!("{l}{r}");
                s.contains("o_orderkey") && s.contains("l_orderkey")
            }) && e.join.filter.is_some()
        });
        assert!(
            has_orders_lineitem,
            "Missing orders-lineitem join with filter"
        );

        Ok(())
    }
}
