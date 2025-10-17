use std::{collections::HashSet, fmt::Debug, rc::Rc, sync::Arc};

use datafusion_common::{plan_datafusion_err, plan_err, Result};
use datafusion_expr::LogicalPlan;

use crate::reorder_join::{
    cost::JoinCostEstimator,
    query_graph::{NodeId, QueryGraph},
};

/// Generates an optimized left-deep join plan from a logical plan using the Ibaraki-Kameda algorithm.
///
/// This function is the main entry point for join reordering optimization. It takes a logical plan
/// that may contain joins along with wrapper operators (filters, sorts, aggregations, etc.) and
/// produces an optimized plan with reordered joins while preserving the wrapper operators.
///
/// # Algorithm Overview
///
/// The optimization process consists of several steps:
///
/// 1. **Extraction**: Separates the join subtree from wrapper operators (filters, sorts, limits, etc.)
/// 2. **Graph Conversion**: Converts the join subtree into a query graph representation where:
///    - Nodes represent base relations (table scans, subqueries, etc.)
///    - Edges represent join conditions between relations
/// 3. **Optimization**: Uses the Ibaraki-Kameda algorithm to find the optimal left-deep join ordering
///    by trying each node as a potential root and selecting the plan with the lowest estimated cost
/// 4. **Reconstruction**: Rebuilds the complete logical plan by applying the wrapper operators
///    to the optimized join plan
///
/// # Left-Deep Join Plans
///
/// A left-deep join plan is a join tree where:
/// - Each join has a relation or previous join result on the left side
/// - Each join has a single relation on the right side
/// - This creates a linear "chain" of joins processed left-to-right
///
/// Example: `((A ⋈ B) ⋈ C) ⋈ D` is left-deep, while `(A ⋈ B) ⋈ (C ⋈ D)` is not.
///
/// Left-deep plans are preferred because they:
/// - Allow pipelining of intermediate results
/// - Work well with hash join implementations
/// - Have predictable memory usage patterns
///
/// # Arguments
///
/// * `plan` - The logical plan to optimize. Must contain at least one join node.
/// * `cost_estimator` - Cost estimator for calculating join costs, cardinality, and selectivity.
///   Used to compare different join orderings and select the optimal one.
///
/// # Returns
///
/// Returns a `LogicalPlan` with optimized join ordering. The plan structure is:
/// - Wrapper operators (filters, sorts, etc.) in their original positions
/// - Joins reordered to minimize estimated execution cost
/// - Join semantics preserved (same result set as input plan)
///
/// # Errors
///
/// Returns an error if:
/// - The plan does not contain any join nodes
/// - Join extraction fails (e.g., joins are not consecutive in the plan tree)
/// - The query graph cannot be constructed from the join subtree
/// - Join reordering optimization fails (no valid join ordering found)
/// - Plan reconstruction fails
///
/// # Example
///
/// ```ignore
/// use datafusion_optimizer::reorder_join::{optimal_left_deep_join_plan, cost::JoinCostEstimator};
/// use std::rc::Rc;
///
/// // Assume we have a plan with joins: customer ⋈ orders ⋈ lineitem
/// let plan = ...; // Your logical plan
/// let cost_estimator: Rc<dyn JoinCostEstimator> = Rc::new(MyCostEstimator::new());
///
/// // Optimize join ordering
/// let optimized = optimal_left_deep_join_plan(plan, cost_estimator)?;
/// // Result might reorder to: lineitem ⋈ orders ⋈ customer (if this is cheaper)
/// ```
pub fn optimal_left_deep_join_plan(
    plan: LogicalPlan,
    cost_estimator: Rc<dyn JoinCostEstimator>,
) -> Result<LogicalPlan> {
    // Extract the join subtree and wrappers
    let (join_subtree, wrappers) =
        crate::reorder_join::query_graph::extract_join_subtree(plan)?;

    // Convert join subtree to query graph
    let query_graph = QueryGraph::try_from(join_subtree)?;

    // Optimize the joins
    let optimized_joins =
        query_graph_to_optimal_left_deep_join_plan(query_graph, cost_estimator)?;

    // Reconstruct the full plan with wrappers

    crate::reorder_join::query_graph::reconstruct_plan(optimized_joins, wrappers)
}

/// Generates an optimized linear join plan from a query graph using the Ibaraki-Kameda algorithm.
///
/// This function finds the optimal join ordering for a query by:
/// 1. Trying each node in the query graph as a potential root
/// 2. For each root, building a precedence tree and optimizing it through normalization/denormalization
/// 3. Selecting the plan with the lowest estimated cost
///
/// The optimization process uses the Ibaraki-Kameda algorithm, which arranges joins to minimize
/// intermediate result sizes by considering both cardinality and cost estimates.
///
/// # Algorithm Steps
///
/// For each candidate root node:
/// 1. **Construction**: Build a precedence tree from the query graph starting at that node
/// 2. **Normalization**: Transform the tree into a chain structure ordered by rank
/// 3. **Denormalization**: Split merged operations back into individual nodes while maintaining chain structure
/// 4. **Cost Comparison**: Compare the resulting plan's cost against the current best
///
/// # Arguments
///
/// * `query_graph` - The query graph containing logical plan nodes and join specifications
/// * `cost_estimator` - The cost estimator to use for calculating cardinality, selectivity, and cost
///
/// # Returns
///
/// Returns a `LogicalPlan` representing the optimal join ordering with the lowest estimated cost.
///
/// # Errors
///
/// Returns an error if:
/// - The query graph is empty or invalid
/// - Tree construction, normalization, or denormalization fails
/// - No valid precedence graph can be generated
pub fn query_graph_to_optimal_left_deep_join_plan(
    query_graph: QueryGraph,
    cost_estimator: Rc<dyn JoinCostEstimator>,
) -> Result<LogicalPlan> {
    let mut best_graph: Option<PrecedenceTreeNode> = None;

    for (node_id, _) in query_graph.nodes() {
        let mut precedence_graph = PrecedenceTreeNode::from_query_graph(
            &query_graph,
            node_id,
            Rc::clone(&cost_estimator),
        )?;
        precedence_graph.normalize();
        precedence_graph.denormalize()?;

        best_graph = match best_graph.take() {
            Some(current) => {
                let new_cost = precedence_graph.cost()?;
                if new_cost < current.cost()? {
                    Some(precedence_graph)
                } else {
                    Some(current)
                }
            }
            None => Some(precedence_graph),
        };
    }

    best_graph
        .ok_or_else(|| plan_datafusion_err!("No valid precedence graph found"))?
        .into_logical_plan(&query_graph)
}

#[derive(Debug)]
struct QueryNode {
    node_id: NodeId,
    // T in [IbarakiKameda84]
    selectivity: f64,
    // C in [IbarakiKameda84]
    cost: f64,
}

impl QueryNode {
    fn rank(&self) -> f64 {
        (self.selectivity - 1.0) / self.cost
    }
}

/// A node in the precedence tree for query optimization.
///
/// The precedence tree is a data structure used by the Ibaraki-Kameda algorithm for
/// optimizing join ordering in database queries. It can represent both arbitrary tree
/// structures and linear chain structures (where each node has at most one child).
///
/// # Lifecycle
///
/// A typical precedence tree goes through three phases:
///
/// 1. **Construction** ([`from_query_graph`](Self::from_query_graph)): Build an initial tree
///    from a query graph, creating nodes with cost/cardinality estimates
/// 2. **Normalization** ([`normalize`](Self::normalize)): Transform the tree into a chain
///    where nodes are ordered by rank, potentially merging multiple query operations into
///    single nodes
/// 3. **Denormalization** ([`denormalize`](Self::denormalize)): Split merged operations back
///    into individual nodes while maintaining the optimized chain structure
///
/// The result is a linear execution order that minimizes intermediate result sizes.
///
/// # Fields
///
/// * `query_nodes` - Vector of query operations with cost estimates. In an initial tree,
///   contains one operation. After normalization, may contain multiple merged operations.
///   After denormalization, contains exactly one operation.
/// * `children` - Child nodes in the tree. In a normalized/denormalized chain, contains
///   at most one child. In an arbitrary tree, may contain multiple children.
/// * `query_graph` - Reference to the original query graph, used for accessing node
///   relationships and metadata during tree transformations.
struct PrecedenceTreeNode<'graph> {
    query_nodes: Vec<QueryNode>,
    children: Vec<PrecedenceTreeNode<'graph>>,
    query_graph: &'graph QueryGraph,
}

impl Debug for PrecedenceTreeNode<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrecedenceTreeNode")
            .field("query_nodes", &self.query_nodes)
            .field("children", &self.children)
            .finish()
    }
}

impl<'graph> PrecedenceTreeNode<'graph> {
    /// Creates a precedence tree from a query graph.
    ///
    /// This is the main entry point for transforming a query graph into a precedence tree
    /// structure. The tree represents an initial join ordering with cost and cardinality
    /// estimates for query optimization.
    ///
    /// The function performs a depth-first traversal starting from the root node,
    /// building a tree where:
    /// - Each node contains cost/cardinality estimates for a query operation
    /// - Children represent connected query nodes (joins, filters, etc.)
    /// - The root node starts with selectivity of 1.0 (no filtering)
    ///
    /// # Arguments
    ///
    /// * `graph` - The query graph to transform into a precedence tree
    /// * `root_id` - The ID of the node to use as the root of the tree
    /// * `cost_estimator` - The cost estimator to use for calculating cardinality, selectivity, and cost
    ///
    /// # Returns
    ///
    /// Returns a `PrecedenceTreeNode` representing the entire query graph as a tree structure,
    /// with the specified root node at the top.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The `root_id` is not found in the query graph
    /// - Any connected node cannot be found during traversal
    pub(crate) fn from_query_graph(
        graph: &'graph QueryGraph,
        root_id: NodeId,
        cost_estimator: Rc<dyn JoinCostEstimator>,
    ) -> Result<Self> {
        let mut remaining: HashSet<NodeId> = graph.nodes().map(|(x, _)| x).collect();
        remaining.remove(&root_id);
        PrecedenceTreeNode::from_query_node(
            root_id,
            1.0,
            graph,
            &mut remaining,
            cost_estimator,
            true,
        )
    }

    /// Recursively constructs a precedence tree node from a query graph node.
    ///
    /// This function builds a tree structure by:
    /// 1. Creating a node with cost and cardinality estimates for the current query node
    /// 2. Recursively processing all connected unvisited nodes as children
    /// 3. Removing visited nodes from the `remaining` set to avoid cycles
    ///
    /// # Arguments
    ///
    /// * `node_id` - The ID of the query graph node to process
    /// * `selectivity` - The selectivity factor from the parent edge (1.0 for root)
    /// * `query_graph` - Reference to the query graph being transformed
    /// * `remaining` - Mutable set of node IDs not yet visited (updated during traversal)
    /// * `cost_estimator` - The cost estimator to use for calculating cardinality, selectivity, and cost
    ///
    /// # Returns
    ///
    /// Returns a `PrecedenceTreeNode` containing:
    /// - A single `NodeEstimates` with cardinality and cost based on input cardinality and selectivity
    /// - Child nodes for each connected unvisited neighbor in the query graph
    ///
    /// # Errors
    ///
    /// Returns an error if the specified `node_id` is not found in the query graph.
    fn from_query_node(
        node_id: NodeId,
        selectivity: f64,
        query_graph: &'graph QueryGraph,
        remaining: &mut HashSet<NodeId>,
        cost_estimator: Rc<dyn JoinCostEstimator>,
        is_root: bool,
    ) -> Result<Self> {
        let node = query_graph
            .get_node(node_id)
            .ok_or_else(|| plan_datafusion_err!("Root node not found"))?;
        let input_cardinality = cost_estimator.cardinality(&node.plan).unwrap_or(1.0);

        let children = node
            .connections()
            .iter()
            .filter_map(|edge_id| {
                let edge = query_graph.get_edge(*edge_id)?;
                let other = edge
                    .nodes
                    .into_iter()
                    .find(|x| *x != node_id && remaining.contains(x))?;

                remaining.remove(&other);
                let child_selectivity = cost_estimator.selectivity(&edge.join);
                Some(PrecedenceTreeNode::from_query_node(
                    other,
                    child_selectivity,
                    query_graph,
                    remaining,
                    Rc::clone(&cost_estimator),
                    false,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(PrecedenceTreeNode {
            query_nodes: vec![QueryNode {
                node_id,
                selectivity: (selectivity * input_cardinality),
                cost: if is_root {
                    0.0
                } else {
                    cost_estimator.cost(selectivity, input_cardinality)
                },
            }],
            children,
            query_graph,
        })
    }

    /// Rank function according to IbarakiKameda84
    fn rank(&self) -> f64 {
        let (cardinality, cost) =
            self.query_nodes
                .iter()
                .fold((1.0, 0.0), |(cardinality, cost), node| {
                    let cost = cost + cardinality * node.cost;
                    let cardinality = cardinality * node.selectivity;
                    (cardinality, cost)
                });
        if cost == 0.0 {
            0.0
        } else {
            (cardinality - 1.0) as f64 / cost
        }
    }

    /// Normalizes the precedence tree into a linear chain structure.
    ///
    /// This transformation converts the tree into a normalized form where each node
    /// has at most one child, creating a linear sequence of query nodes. The normalization
    /// process uses the rank function to determine optimal ordering according to the
    /// Ibaraki-Kameda algorithm.
    ///
    /// The normalization handles three cases:
    /// - **Leaf nodes (0 children)**: Already normalized, no action needed
    /// - **Single child (1 child)**: If the child has lower rank than current node, merge
    ///   the child's query nodes into the current node, creating a sequence. Otherwise,
    ///   recursively normalize the child.
    /// - **Multiple children (2+ children)**: Recursively normalize all children into chains,
    ///   then merge all child chains into a single chain using the merge operation.
    ///
    /// After normalization, the tree becomes a chain where nodes are ordered by their
    /// rank values, with each node containing one or more query operations in sequence.
    ///
    /// # Algorithm
    ///
    /// Based on the Ibaraki-Kameda join ordering algorithm, which optimizes query
    /// execution by arranging operations to minimize intermediate result sizes.
    fn normalize(&mut self) {
        match self.children.len() {
            0 => (),
            1 => {
                // If child has lower rank, merge it into current node
                if self.children[0].rank() < self.rank() {
                    let mut child = self.children.pop().unwrap();
                    self.query_nodes.append(&mut child.query_nodes);
                    self.children = child.children;
                    self.normalize();
                } else {
                    self.children[0].normalize();
                }
            }
            _ => {
                // Normalize all child trees into chains, then merge them
                for child in &mut self.children {
                    child.normalize();
                }
                let child = std::mem::take(&mut self.children)
                    .into_iter()
                    .reduce(Self::merge)
                    .unwrap();
                self.children = vec![child];
            }
        }
    }

    /// Merges two precedence tree chains into a single chain.
    ///
    /// This operation combines two normalized tree chains (each with at most one child)
    /// into a single chain, preserving rank ordering. The chain with the lower rank becomes
    /// the parent, and the higher-ranked chain is attached as a descendant.
    ///
    /// The merge strategy depends on whether the lower-ranked chain has children:
    /// - **No children**: The higher-ranked chain becomes the direct child
    /// - **Has child**: Recursively merge the higher-ranked chain with the child,
    ///   maintaining the chain structure
    ///
    /// This ensures the resulting chain maintains proper rank ordering from root to leaf,
    /// which is essential for the Ibaraki-Kameda optimization algorithm.
    ///
    /// # Arguments
    ///
    /// * `self` - The first tree chain to merge
    /// * `other` - The second tree chain to merge
    ///
    /// # Returns
    ///
    /// Returns a merged `PrecedenceTreeNode` chain with both input chains combined,
    /// ordered by rank values.
    ///
    /// # Panics
    ///
    /// May panic if called on non-normalized trees (trees with multiple children).
    fn merge(self, other: PrecedenceTreeNode<'graph>) -> Self {
        let (mut first, second) = if self.rank() < other.rank() {
            (self, other)
        } else {
            (other, self)
        };
        if first.children.is_empty() {
            first.children = vec![second];
        } else {
            first.children = vec![first.children.pop().unwrap().merge(second)];
        }
        first
    }

    /// Denormalizes a normalized precedence tree by splitting merged query nodes.
    ///
    /// This is the inverse operation of normalization, but with a critical property:
    /// **the result is still a chain structure** (each node has at most one child).
    /// It converts a normalized chain where nodes contain multiple query operations
    /// into a longer chain where each node contains exactly one query operation.
    ///
    /// The denormalization process:
    /// 1. **Validates input**: Ensures the tree is normalized (0 or 1 children per node)
    /// 2. **Recursively processes children**: Denormalizes the child chain first
    /// 3. **Splits merged nodes**: For nodes with multiple query operations, iteratively
    ///    extracts operations one at a time based on neighbor relationships with the child
    /// 4. **Maintains ordering**: Uses rank-based selection to determine which query node
    ///    to extract next, choosing the highest-ranked neighbor of the child node
    ///
    /// **Key property**: After denormalization, the result remains a chain (not a tree with
    /// branches). Each node contains exactly one query operation, but the chain structure
    /// is preserved. This is the essence of the normalize-denormalize algorithm: transforming
    /// an arbitrary tree into an optimized chain while respecting query dependencies.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The tree is not normalized (has more than one child at any level)
    ///
    /// # Algorithm
    ///
    /// The splitting process uses the query graph's neighbor relationships to determine
    /// which nodes should be adjacent in the chain, maintaining logical dependencies
    /// between query operations while producing a linear execution order.
    fn denormalize(&mut self) -> Result<()> {
        // Normalized trees must have 0 or 1 children
        match self.children.len() {
            0 => (),
            1 => self.children[0].denormalize()?,
            _ => return plan_err!("Tree is not normalized"),
        }

        // Split query nodes into a chain based on neighbor relationships
        while self.query_nodes.len() > 1 {
            if self.children.is_empty() {
                let highest_rank_idx = self
                    .query_nodes
                    .iter()
                    .enumerate()
                    .max_by(|(_, a), (_, b)| a.rank().partial_cmp(&b.rank()).unwrap())
                    .map(|(idx, _)| idx)
                    .unwrap();

                let node = self.query_nodes.remove(highest_rank_idx);

                self.children.push(PrecedenceTreeNode {
                    query_nodes: vec![node],
                    children: Vec::new(),
                    query_graph: self.query_graph,
                });
            } else {
                let child_id = self.children[0].query_nodes[0].node_id;
                let child_node = self.query_graph.get_node(child_id).unwrap();
                let neighbours = child_node.neighbours(child_id, self.query_graph);

                // Find the highest-ranked neighbor node
                let highest_rank_idx = self
                    .query_nodes
                    .iter()
                    .enumerate()
                    .filter(|(_, node)| neighbours.contains(&node.node_id))
                    .max_by(|(_, a), (_, b)| a.rank().partial_cmp(&b.rank()).unwrap())
                    .map(|(idx, _)| idx)
                    .unwrap();

                let node = self.query_nodes.remove(highest_rank_idx);

                let child = std::mem::replace(
                    &mut self.children[0],
                    PrecedenceTreeNode {
                        query_nodes: vec![node],
                        children: Vec::new(),
                        query_graph: self.query_graph,
                    },
                );
                self.children[0].children = vec![child];
            };

            // Insert the node between current and its child
        }
        Ok(())
    }

    /// Converts the precedence tree chain into a DataFusion `LogicalPlan`.
    ///
    /// This method walks down the optimized chain structure, building a left-deep join tree
    /// by repeatedly joining the accumulated result with the next node in the chain.
    ///
    /// # Algorithm
    ///
    /// 1. Start with the first node's `LogicalPlan` from the query graph
    /// 2. For each subsequent node in the chain:
    ///    - Get the node's `LogicalPlan` from the query graph
    ///    - Find the edge connecting the current and next nodes
    ///    - Create a join using the edge's join specification
    ///    - The accumulated plan becomes the left side of the join
    /// 3. Return the final joined `LogicalPlan`
    ///
    /// # Arguments
    ///
    /// * `query_graph` - The query graph containing the logical plans and join specifications
    ///
    /// # Returns
    ///
    /// Returns a `LogicalPlan` representing the optimized join execution order.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A node or edge is missing from the query graph
    /// - The precedence tree is not in the expected chain format
    pub(crate) fn into_logical_plan(
        self,
        query_graph: &QueryGraph,
    ) -> Result<LogicalPlan> {
        // Get the first node's logical plan
        let current_node_id = self.query_nodes[0].node_id;
        let mut current_plan = query_graph
            .get_node(current_node_id)
            .ok_or_else(|| plan_datafusion_err!("Node {:?} not found", current_node_id))?
            .plan
            .as_ref()
            .clone();

        // Track all processed nodes in order
        let mut processed_nodes = vec![current_node_id];

        // Walk down the chain, joining each subsequent node
        let mut current_chain = &self;

        while !current_chain.children.is_empty() {
            let child = &current_chain.children[0];
            let next_node_id = child.query_nodes[0].node_id;

            // Get the next node's logical plan
            let next_plan = query_graph
                .get_node(next_node_id)
                .ok_or_else(|| plan_datafusion_err!("Node {:?} not found", next_node_id))?
                .plan
                .as_ref()
                .clone();

            // Find the edge connecting next_node to any processed node
            let next_node = query_graph.get_node(next_node_id).ok_or_else(|| {
                plan_datafusion_err!("Node {:?} not found", next_node_id)
            })?;

            let edge = processed_nodes
                .iter()
                .rev()
                .find_map(|&processed_id| {
                    next_node.connection_with(processed_id, query_graph)
                })
                .ok_or_else(|| {
                    plan_datafusion_err!(
                        "No edge found between {:?} and any processed nodes {:?}",
                        next_node_id,
                        processed_nodes
                    )
                })?;

            // Determine if the join order was swapped compared to the original edge.
            // We check if the qualified columns (relation + name) from the join expressions
            // match the schemas. This handles all cases including when multiple tables
            // have columns with the same name.
            let current_schema = current_plan.schema();
            let next_schema = next_plan.schema();

            let join_order_swapped = if !edge.join.on.is_empty() {
                // Extract columns from the first join condition
                let (left_expr, right_expr) = &edge.join.on[0];
                let left_columns = left_expr.column_refs();
                let right_columns = right_expr.column_refs();

                // Helper to check if a qualified column exists in a schema
                let column_in_schema = |col: &datafusion_common::Column,
                                         schema: &datafusion_common::DFSchema|
                 -> bool {
                    if let Some(relation) = &col.relation {
                        // Column has a table qualifier - must match exactly (relation + name)
                        schema.iter().any(|(qualifier, field)| {
                            qualifier == Some(relation) && field.name() == col.name()
                        })
                    } else {
                        // Unqualified column - check if the name exists anywhere in schema
                        schema.field_with_unqualified_name(&col.name).is_ok()
                    }
                };

                // Check which schema each expression's columns belong to
                let left_in_current =
                    left_columns.iter().all(|c| column_in_schema(c, current_schema.as_ref()));
                let right_in_next =
                    right_columns.iter().all(|c| column_in_schema(c, next_schema.as_ref()));
                let left_in_next =
                    left_columns.iter().all(|c| column_in_schema(c, next_schema.as_ref()));
                let right_in_current =
                    right_columns.iter().all(|c| column_in_schema(c, current_schema.as_ref()));

                // Determine swap based on where the qualified columns are found
                if left_in_current && right_in_next {
                    // Left expression belongs to current, right to next → no swap
                    false
                } else if left_in_next && right_in_current {
                    // Left expression belongs to next, right to current → swap
                    true
                } else {
                    // Ambiguous or error case - default to no swap to preserve original order
                    // This shouldn't happen with properly qualified columns
                    false
                }
            } else {
                // If there are no join conditions, we can't determine swap status
                // This shouldn't happen in practice for equi-joins
                false
            };

            // When the join order is swapped, we need to adjust the on conditions and join type
            // to maintain correct semantics. For example:
            // - Original: A LeftSemi B ON A.x = B.y
            // - After swap: B RightSemi A ON B.y = A.x
            let (on, join_type) = if join_order_swapped {
                let swapped_on = edge
                    .join
                    .on
                    .iter()
                    .map(|(left, right)| (right.clone(), left.clone()))
                    .collect();
                (swapped_on, edge.join.join_type.swap())
            } else {
                (edge.join.on.clone(), edge.join.join_type)
            };

            // Create the join plan
            current_plan = LogicalPlan::Join(datafusion_expr::Join {
                left: Arc::new(current_plan),
                right: Arc::new(next_plan),
                on,
                filter: edge.join.filter.clone(),
                join_type,
                join_constraint: edge.join.join_constraint,
                schema: Arc::clone(&edge.join.schema),
                null_equality: edge.join.null_equality,
            });

            // Move to the next node in the chain
            processed_nodes.push(next_node_id);
            current_chain = child;
        }

        Ok(current_plan)
    }

    fn cost(&self) -> Result<f64> {
        self.cost_recursive(self.query_nodes[0].selectivity, 0.0)
    }

    fn cost_recursive(&self, cardinality: f64, cost: f64) -> Result<f64> {
        let cost = match self.children.len() {
            0 => cost + cardinality * self.query_nodes[0].cost,
            1 => self.children[0].cost_recursive(
                cardinality * self.query_nodes[0].selectivity,
                cost + cardinality * self.query_nodes[0].cost,
            )?,
            _ => {
                return plan_err!(
                    "Cost calculation requires normalized tree with 0 or 1 children"
                )
            }
        };
        Ok(cost)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
    use crate::eliminate_filter::EliminateFilter;
    use crate::extract_equijoin_predicate::ExtractEquijoinPredicate;
    use crate::filter_null_join_keys::FilterNullJoinKeys;
    use crate::optimizer::{Optimizer, OptimizerContext};
    use crate::push_down_filter::PushDownFilter;
    use crate::reorder_join::cost::JoinCostEstimator;
    use crate::scalar_subquery_to_join::ScalarSubqueryToJoin;
    use crate::simplify_expressions::SimplifyExpressions;
    use crate::test::*;
    use datafusion_expr::logical_plan::JoinType;
    use datafusion_expr::LogicalPlanBuilder;

    /// A simple cost estimator for testing
    #[derive(Debug)]
    struct TestCostEstimator;

    impl JoinCostEstimator for TestCostEstimator {}

    /// A simple TableSource implementation for testing join ordering with statistics
    #[derive(Debug)]
    struct JoinSource {
        schema: arrow::datatypes::SchemaRef,
        num_rows: usize,
    }

    impl datafusion_expr::TableSource for JoinSource {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn statistics(&self) -> Option<datafusion_common::Statistics> {
            use datafusion_common::stats::Precision;
            Some(
                datafusion_common::Statistics::new_unknown(&self.schema)
                    .with_num_rows(Precision::Exact(self.num_rows)),
            )
        }
    }

    /// Create a table scan with statistics for testing join ordering
    fn scan_tpch_table_with_stats(table: &str, num_rows: usize) -> LogicalPlan {
        let schema = Arc::new(get_tpch_table_schema(table));
        let table_source: Arc<dyn datafusion_expr::TableSource> = Arc::new(JoinSource {
            schema: Arc::clone(&schema),
            num_rows,
        });
        LogicalPlanBuilder::scan(table, table_source, None)
            .unwrap()
            .build()
            .unwrap()
    }

    /// Test three-way join: customer -> orders -> lineitem
    #[test]
    fn test_three_way_join_customer_orders_lineitem() -> Result<()> {
        use datafusion_expr::test::function_stub::sum;
        use datafusion_expr::{col, in_subquery, lit};
        // Create the base table scans with statistics
        // Create the base table scans with statistics
        let customer = scan_tpch_table_with_stats("customer", 150);
        let orders = scan_tpch_table_with_stats("orders", 1_500);
        let lineitem = scan_tpch_table_with_stats("lineitem", 6_000);

        // Step 1: Build the subquery
        // SELECT l_orderkey FROM lineitem
        // GROUP BY l_orderkey
        // HAVING sum(l_quantity) > 300
        let subquery = LogicalPlanBuilder::from(lineitem.clone())
            .aggregate(vec![col("l_orderkey")], vec![sum(col("l_quantity"))])?
            .filter(sum(col("l_quantity")).gt(lit(300)))?
            .project(vec![col("l_orderkey")])?
            .build()?;

        // Step 2: Build the main query with joins
        let plan = LogicalPlanBuilder::from(customer.clone())
            .join(
                orders.clone(),
                JoinType::Inner,
                (vec!["c_custkey"], vec!["o_custkey"]),
                None,
            )?
            .join(
                lineitem.clone(),
                JoinType::Inner,
                (vec!["o_orderkey"], vec!["l_orderkey"]),
                None,
            )?
            // Step 3: Apply the IN subquery filter
            .filter(in_subquery(col("o_orderkey"), Arc::new(subquery)))?
            // Step 4: Aggregate
            .aggregate(
                vec![
                    col("c_name"),
                    col("c_custkey"),
                    col("o_orderkey"),
                    col("o_totalprice"),
                ],
                vec![sum(col("l_quantity"))],
            )?
            // Step 5: Sort
            .sort(vec![col("o_totalprice").sort(false, true)])?
            // Step 6: Limit
            .limit(0, Some(100))?
            .build()?;

        println!("{}", plan.display_indent());

        // Optimize the plan with custom optimizer before join reordering
        // We exclude OptimizeProjections to keep joins consecutive
        let config = OptimizerContext::new().with_skip_failing_rules(false);
        let optimizer = Optimizer::with_rules(vec![
            Arc::new(SimplifyExpressions::new()),
            Arc::new(DecorrelatePredicateSubquery::new()),
            Arc::new(ScalarSubqueryToJoin::new()),
            Arc::new(ExtractEquijoinPredicate::new()),
            Arc::new(EliminateFilter::new()),
            Arc::new(FilterNullJoinKeys::default()),
            Arc::new(PushDownFilter::new()),
            // Note: OptimizeProjections is intentionally excluded to keep joins consecutive
        ]);
        let plan = optimizer.optimize(plan, &config, |_, _| {}).unwrap();

        println!("After standard optimization:");
        println!("{}", plan.display_indent());

        let optimized_plan =
            optimal_left_deep_join_plan(plan, Rc::new(TestCostEstimator)).unwrap();

        println!("Optimized Plan:");
        println!("{}", optimized_plan.display_indent());

        // Verify the plan structure
        assert!(matches!(optimized_plan, LogicalPlan::Limit(_)));

        Ok(())
    }
}
