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

use crate::expressions::Literal;
use crate::intervals::cp_solver::PropagationResult;
use crate::physical_expr::PhysicalExpr;
use crate::utils::{ExprTreeNode, build_dag};

use arrow::datatypes::{DataType, Schema};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::statistics::Distribution;
use datafusion_expr_common::interval_arithmetic::Interval;

use petgraph::Outgoing;
use petgraph::adj::DefaultIx;
use petgraph::prelude::Bfs;
use petgraph::stable_graph::{NodeIndex, StableGraph};
use petgraph::visit::DfsPostOrder;

/// This object implements a directed acyclic expression graph (DAEG) that
/// is used to compute statistics/distributions for expressions hierarchically.
#[derive(Clone, Debug)]
pub struct ExprStatisticsGraph {
    graph: StableGraph<ExprStatisticsGraphNode, usize>,
    root: NodeIndex,
}

/// This is a node in the DAEG; it encapsulates a reference to the actual
/// [`PhysicalExpr`] as well as its statistics/distribution.
#[derive(Clone, Debug)]
pub struct ExprStatisticsGraphNode {
    expr: Arc<dyn PhysicalExpr>,
    dist: Distribution,
}

impl ExprStatisticsGraphNode {
    /// Constructs a new DAEG node based on the given interval with a
    /// `Uniform` distribution.
    fn new_uniform(expr: Arc<dyn PhysicalExpr>, interval: Interval) -> Result<Self> {
        Distribution::new_uniform(interval)
            .map(|dist| ExprStatisticsGraphNode { expr, dist })
    }

    /// Constructs a new DAEG node with a `Bernoulli` distribution having an
    /// unknown success probability.
    fn new_bernoulli(expr: Arc<dyn PhysicalExpr>) -> Result<Self> {
        Distribution::new_bernoulli(ScalarValue::Float64(None))
            .map(|dist| ExprStatisticsGraphNode { expr, dist })
    }

    /// Constructs a new DAEG node with a `Generic` distribution having no
    /// definite summary statistics.
    fn new_generic(expr: Arc<dyn PhysicalExpr>, dt: &DataType) -> Result<Self> {
        let interval = Interval::make_unbounded(dt)?;
        let dist = Distribution::new_from_interval(interval)?;
        Ok(ExprStatisticsGraphNode { expr, dist })
    }

    /// Get the [`Distribution`] object representing the statistics of the
    /// expression.
    pub fn distribution(&self) -> &Distribution {
        &self.dist
    }

    /// This function creates a DAEG node from DataFusion's [`ExprTreeNode`]
    /// object. Literals are created with `Uniform` distributions with a
    /// definite, singleton interval. Expressions with a `Boolean` data type
    /// result in a`Bernoulli` distribution with an unknown success probability.
    /// Any other expression starts with an `Unknown` distribution with an
    /// indefinite range (i.e. `[-∞, ∞]`).
    pub fn make_node(node: &ExprTreeNode<NodeIndex>, schema: &Schema) -> Result<Self> {
        let expr = Arc::clone(&node.expr);
        if let Some(literal) = expr.as_any().downcast_ref::<Literal>() {
            let value = literal.value();
            Interval::try_new(value.clone(), value.clone())
                .and_then(|interval| Self::new_uniform(expr, interval))
        } else {
            expr.data_type(schema).and_then(|dt| {
                if dt.eq(&DataType::Boolean) {
                    Self::new_bernoulli(expr)
                } else {
                    Self::new_generic(expr, &dt)
                }
            })
        }
    }
}

impl ExprStatisticsGraph {
    pub fn try_new(expr: Arc<dyn PhysicalExpr>, schema: &Schema) -> Result<Self> {
        // Build the full graph:
        let (root, graph) = build_dag(expr, &|node| {
            ExprStatisticsGraphNode::make_node(node, schema)
        })?;
        Ok(Self { graph, root })
    }

    /// This function assigns given distributions to expressions in the DAEG.
    /// The argument `assignments` associates indices of sought expressions
    /// with their corresponding new distributions.
    pub fn assign_statistics(&mut self, assignments: &[(usize, Distribution)]) {
        for (index, stats) in assignments {
            let node_index = NodeIndex::from(*index as DefaultIx);
            self.graph[node_index].dist = stats.clone();
        }
    }

    /// Computes statistics/distributions for an expression via a bottom-up
    /// traversal.
    pub fn evaluate_statistics(&mut self) -> Result<&Distribution> {
        let mut dfs = DfsPostOrder::new(&self.graph, self.root);
        while let Some(idx) = dfs.next(&self.graph) {
            let neighbors = self.graph.neighbors_directed(idx, Outgoing);
            let mut children_statistics = neighbors
                .map(|child| self.graph[child].distribution())
                .collect::<Vec<_>>();
            // Note that all distributions are assumed to be independent.
            if !children_statistics.is_empty() {
                // Reverse to align with `PhysicalExpr`'s children:
                children_statistics.reverse();
                self.graph[idx].dist = self.graph[idx]
                    .expr
                    .evaluate_statistics(&children_statistics)?;
            }
        }
        Ok(self.graph[self.root].distribution())
    }

    /// Runs a propagation mechanism in a top-down manner to update statistics
    /// of leaf nodes.
    pub fn propagate_statistics(
        &mut self,
        given_stats: Distribution,
    ) -> Result<PropagationResult> {
        // Adjust the root node with the given statistics:
        let root_range = self.graph[self.root].dist.range()?;
        let given_range = given_stats.range()?;
        if let Some(interval) = root_range.intersect(&given_range)? {
            if interval != root_range {
                // If the given statistics enable us to obtain a more precise
                // range for the root, update it:
                let subset = root_range.contains(given_range)?;
                self.graph[self.root].dist = if subset == Interval::TRUE {
                    // Given statistics is strictly more informative, use it as is:
                    given_stats
                } else {
                    // Intersecting ranges gives us a more precise range:
                    Distribution::new_from_interval(interval)?
                };
            }
        } else {
            return Ok(PropagationResult::Infeasible);
        }

        let mut bfs = Bfs::new(&self.graph, self.root);

        while let Some(node) = bfs.next(&self.graph) {
            let neighbors = self.graph.neighbors_directed(node, Outgoing);
            let mut children = neighbors.collect::<Vec<_>>();
            // If the current expression is a leaf, its statistics is now final.
            // So, just continue with the propagation procedure:
            if children.is_empty() {
                continue;
            }
            // Reverse to align with `PhysicalExpr`'s children:
            children.reverse();
            let children_stats = children
                .iter()
                .map(|child| self.graph[*child].distribution())
                .collect::<Vec<_>>();
            let node_statistics = self.graph[node].distribution();
            let propagated_statistics = self.graph[node]
                .expr
                .propagate_statistics(node_statistics, &children_stats)?;
            if let Some(propagated_stats) = propagated_statistics {
                for (child_idx, stats) in children.into_iter().zip(propagated_stats) {
                    self.graph[child_idx].dist = stats;
                }
            } else {
                // The constraint is infeasible, report:
                return Ok(PropagationResult::Infeasible);
            }
        }
        Ok(PropagationResult::Success)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::expressions::{Column, binary, try_cast};
    use crate::intervals::cp_solver::PropagationResult;
    use crate::statistics::stats_solver::ExprStatisticsGraph;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr_common::interval_arithmetic::Interval;
    use datafusion_expr_common::operator::Operator;
    use datafusion_expr_common::statistics::Distribution;
    use datafusion_expr_common::type_coercion::binary::BinaryTypeCoercer;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

    pub fn binary_expr(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
        schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let left_type = left.data_type(schema)?;
        let right_type = right.data_type(schema)?;
        let binary_type_coercer = BinaryTypeCoercer::new(&left_type, &op, &right_type);
        let (lhs, rhs) = binary_type_coercer.get_input_types()?;

        let left_expr = try_cast(left, schema, lhs)?;
        let right_expr = try_cast(right, schema, rhs)?;
        binary(left_expr, op, right_expr, schema)
    }

    #[test]
    fn test_stats_integration() -> Result<()> {
        let schema = &Schema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Float64, false),
            Field::new("d", DataType::Float64, false),
        ]);

        let a = Arc::new(Column::new("a", 0)) as _;
        let b = Arc::new(Column::new("b", 1)) as _;
        let c = Arc::new(Column::new("c", 2)) as _;
        let d = Arc::new(Column::new("d", 3)) as _;

        let left = binary_expr(a, Operator::Plus, b, schema)?;
        let right = binary_expr(c, Operator::Minus, d, schema)?;
        let expr = binary_expr(left, Operator::Eq, right, schema)?;

        let mut graph = ExprStatisticsGraph::try_new(expr, schema)?;
        // 2, 5 and 6 are BinaryExpr
        graph.assign_statistics(&[
            (
                0usize,
                Distribution::new_uniform(Interval::make(Some(0.), Some(1.))?)?,
            ),
            (
                1usize,
                Distribution::new_uniform(Interval::make(Some(0.), Some(2.))?)?,
            ),
            (
                3usize,
                Distribution::new_uniform(Interval::make(Some(1.), Some(3.))?)?,
            ),
            (
                4usize,
                Distribution::new_uniform(Interval::make(Some(1.), Some(5.))?)?,
            ),
        ]);
        let ev_stats = graph.evaluate_statistics()?;
        assert_eq!(
            ev_stats,
            &Distribution::new_bernoulli(ScalarValue::Float64(None))?
        );

        let one = ScalarValue::new_one(&DataType::Float64)?;
        assert_eq!(
            graph.propagate_statistics(Distribution::new_bernoulli(one)?)?,
            PropagationResult::Success
        );
        Ok(())
    }
}
