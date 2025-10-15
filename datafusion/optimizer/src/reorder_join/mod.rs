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

//! Optimizer rule for reordering joins to minimize query execution cost

use std::rc::Rc;

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::Result;
use datafusion_expr::LogicalPlan;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

pub mod cost;
pub mod left_deep_join_plan;
pub mod query_graph;

use cost::JoinCostEstimator;
use left_deep_join_plan::optimal_left_deep_join_plan;
use query_graph::{contains_join, QueryGraph};

/// Optimizer rule that reorders joins to minimize query execution cost.
///
/// This rule identifies consecutive join operations in a query plan and reorders
/// them using the Ibaraki-Kameda algorithm. The algorithm:
///
/// 1. Converts a join subtree into a query graph representation
/// 2. Builds precedence trees to explore different join orderings
/// 3. Normalizes and denormalizes the trees to find optimal orderings
/// 4. Selects the ordering with the lowest estimated cost
///
/// The rule only reorders inner joins and requires all joins to be consecutive
/// in the plan tree (no other operations between them).
///
/// # Example
///
/// Given a query plan like:
/// ```text
/// Join(customer.c_custkey = orders.o_custkey)
///   Join(orders.o_orderkey = lineitem.l_orderkey)
///     TableScan(customer)
///     TableScan(orders)
///   TableScan(lineitem)
/// ```
///
/// The optimizer will evaluate different join orderings and select the one
/// that minimizes intermediate result sizes and overall execution cost.
#[derive(Debug)]
pub struct JoinReorder {
    cost_estimator: Rc<dyn JoinCostEstimator>,
}

impl JoinReorder {
    /// Creates a new join reorder optimizer rule with the given cost estimator
    pub fn new(cost_estimator: Rc<dyn JoinCostEstimator>) -> Self {
        Self { cost_estimator }
    }
}

impl Default for JoinReorder {
    fn default() -> Self {
        Self {
            cost_estimator: Rc::new(cost::DefaultCostEstimator),
        }
    }
}

impl OptimizerRule for JoinReorder {
    fn name(&self) -> &str {
        "join_reorder"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // We need bottom-up traversal to process join subtrees from leaves to root
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Only try to reorder if this is a join node
        if !matches!(plan, LogicalPlan::Join(_)) {
            return Ok(Transformed::no(plan));
        }

        // Check if this join is the root of a consecutive join subtree
        // (i.e., all its children are either joins or leaf nodes)
        if !is_join_subtree_root(&plan) {
            return Ok(Transformed::no(plan));
        }

        // Try to convert the join subtree to a query graph and optimize it
        match optimize_join_subtree(plan.clone(), Rc::clone(&self.cost_estimator)) {
            Ok(optimized_plan) => Ok(Transformed::yes(optimized_plan)),
            Err(_) => {
                // If optimization fails (e.g., unsupported join type), return original plan
                Ok(Transformed::no(plan))
            }
        }
    }
}

/// Checks if a plan node is the root of a consecutive join subtree.
///
/// A node is considered a join subtree root if:
/// - It is a Join node
/// - All its descendants are either Join nodes or don't contain any joins
///
/// This ensures we only try to optimize complete join subtrees that can be
/// safely reordered without breaking other operators.
fn is_join_subtree_root(plan: &LogicalPlan) -> bool {
    if !matches!(plan, LogicalPlan::Join(_)) {
        return false;
    }

    // Check if all children either are joins themselves or don't contain any joins
    let mut all_valid = true;
    let _ = plan.apply_children(|child| {
        if matches!(child, LogicalPlan::Join(_)) {
            // This child is a join, continue checking down the tree
            Ok(TreeNodeRecursion::Continue)
        } else if !contains_join(child) {
            // This child doesn't contain any joins - it's a leaf subtree
            Ok(TreeNodeRecursion::Continue)
        } else {
            // Found a non-join node that contains joins - this breaks the consecutive join pattern
            all_valid = false;
            Ok(TreeNodeRecursion::Stop)
        }
    });

    all_valid
}

/// Optimizes a join subtree by converting it to a query graph and finding
/// the optimal join ordering.
///
/// # Arguments
///
/// * `plan` - The join subtree to optimize (must be a Join node at the root)
/// * `cost_estimator` - The cost estimator to use for optimization
///
/// # Returns
///
/// Returns an optimized LogicalPlan with joins reordered for minimal cost.
///
/// # Errors
///
/// Returns an error if:
/// - The plan cannot be converted to a query graph
/// - The optimization algorithm fails
fn optimize_join_subtree(
    plan: LogicalPlan,
    cost_estimator: Rc<dyn JoinCostEstimator>,
) -> Result<LogicalPlan> {
    // Convert the join subtree to a query graph
    let query_graph = QueryGraph::try_from(plan)?;

    // Use the Ibaraki-Kameda algorithm to find the optimal join ordering
    optimal_left_deep_join_plan(query_graph, cost_estimator)
}
