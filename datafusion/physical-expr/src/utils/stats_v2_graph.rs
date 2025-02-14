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
use crate::utils::{build_dag, ExprTreeNode};

use arrow::datatypes::{DataType, Schema};
use arrow_array::ArrowNativeTypeOp;
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr_common::interval_arithmetic::{apply_operator, Interval};
use datafusion_expr_common::operator::Operator;
use datafusion_physical_expr_common::stats_v2::StatisticsV2::{self, Gaussian, Uniform};
use datafusion_physical_expr_common::stats_v2::{get_one, get_zero};

use log::debug;
use petgraph::adj::DefaultIx;
use petgraph::prelude::Bfs;
use petgraph::stable_graph::{NodeIndex, StableGraph};
use petgraph::visit::DfsPostOrder;
use petgraph::Outgoing;

/// This object implements a directed acyclic expression graph (DAEG) that
/// is used to compute statistics for expressions hierarchically.
#[derive(Clone, Debug)]
pub struct ExprStatisticGraph {
    graph: StableGraph<ExprStatisticGraphNode, usize>,
    root: NodeIndex,
}

/// This is a node in the DAEG; it encapsulates a reference to the actual
/// [`PhysicalExpr`] as well as its statistics.
#[derive(Clone, Debug)]
pub struct ExprStatisticGraphNode {
    expr: Arc<dyn PhysicalExpr>,
    statistics: StatisticsV2,
}

impl ExprStatisticGraphNode {
    /// Constructs a new DAEG node based on the given interval with a
    /// [`Uniform`] distribution.
    fn new_uniform(expr: Arc<dyn PhysicalExpr>, interval: Interval) -> Result<Self> {
        StatisticsV2::new_uniform(interval)
            .map(|statistics| ExprStatisticGraphNode { expr, statistics })
    }

    /// Constructs a new DAEG node with a `Bernoulli` distribution having an
    /// unknown success probability.
    fn new_bernoulli(expr: Arc<dyn PhysicalExpr>) -> Result<Self> {
        StatisticsV2::new_bernoulli(ScalarValue::Float64(None))
            .map(|statistics| ExprStatisticGraphNode { expr, statistics })
    }

    /// Constructs a new DAEG node with an `Unknown` distribution having no
    /// definite summary statistics.
    fn new_unknown(expr: Arc<dyn PhysicalExpr>, dt: &DataType) -> Result<Self> {
        let interval = Interval::make_unbounded(dt)?;
        let statistics = StatisticsV2::new_from_interval(interval)?;
        Ok(ExprStatisticGraphNode { expr, statistics })
    }

    /// Get the [`StatisticsV2`] object representing the statistics of the expression.
    pub fn statistics(&self) -> &StatisticsV2 {
        &self.statistics
    }

    /// This function creates a DAEG node from DataFusion's [`ExprTreeNode`]
    /// object. Literals are created with [`Uniform`] distributions with a
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
                    Self::new_unknown(expr, &dt)
                }
            })
        }
    }
}

impl ExprStatisticGraph {
    pub fn try_new(expr: Arc<dyn PhysicalExpr>, schema: &Schema) -> Result<Self> {
        // Build the full graph:
        let (root, graph) = build_dag(expr, &|node| {
            ExprStatisticGraphNode::make_node(node, schema)
        })?;
        Ok(Self { graph, root })
    }

    /// This function assigns given statistics to expressions in the DAEG.
    /// The argument `assignments` associates indices of sought expressions
    /// with their corresponding new statistics.
    pub fn assign_statistics(&mut self, assignments: &[(usize, StatisticsV2)]) {
        for (index, stats) in assignments {
            let node_index = NodeIndex::from(*index as DefaultIx);
            self.graph[node_index].statistics = stats.clone();
        }
    }

    /// Computes statistics for an expression via a bottom-up traversal.
    pub fn evaluate_statistics(&mut self) -> Result<&StatisticsV2> {
        let mut dfs = DfsPostOrder::new(&self.graph, self.root);
        while let Some(idx) = dfs.next(&self.graph) {
            let neighbors = self.graph.neighbors_directed(idx, Outgoing);
            let mut children_statistics = neighbors
                .map(|child| self.graph[child].statistics())
                .collect::<Vec<_>>();
            // Note that all distributions are assumed to be independent.
            if !children_statistics.is_empty() {
                // Reverse to align with `PhysicalExpr`'s children:
                children_statistics.reverse();
                self.graph[idx].statistics = self.graph[idx]
                    .expr
                    .evaluate_statistics(&children_statistics)?;
            }
        }
        Ok(self.graph[self.root].statistics())
    }

    /// Runs a propagation mechanism in a top-down manner to update statistics
    /// of leaf nodes.
    pub fn propagate_statistics(
        &mut self,
        given_stats: StatisticsV2,
    ) -> Result<PropagationResult> {
        // Adjust the root node with the given range:
        if let Some(interval) = self.graph[self.root]
            .statistics
            .range()?
            .intersect(given_stats.range()?)?
        {
            if interval == Interval::CERTAINLY_TRUE {
                self.graph[self.root].statistics =
                    StatisticsV2::new_bernoulli(get_one().clone())?;
            } else if interval == Interval::CERTAINLY_FALSE {
                self.graph[self.root].statistics =
                    StatisticsV2::new_bernoulli(get_zero().clone())?;
            }
            // If interval == Interval::UNCERTAIN => do nothing to the root
            else if interval != Interval::UNCERTAIN {
                // This case is for numeric quantities
                self.graph[self.root].statistics =
                    StatisticsV2::new_from_interval(interval)?;
            }
        } else {
            return Ok(PropagationResult::Infeasible);
        }

        let mut bfs = Bfs::new(&self.graph, self.root);

        while let Some(node) = bfs.next(&self.graph) {
            let neighbors = self.graph.neighbors_directed(node, Outgoing);
            let mut children = neighbors.collect::<Vec<_>>();
            // If the current expression is a leaf, its statistic is now final. Stop here.
            if children.is_empty() {
                continue;
            }
            // Reverse to align with `PhysicalExpr`'s children:
            children.reverse();
            let children_stats = children
                .iter()
                .map(|child| self.graph[*child].statistics())
                .collect::<Vec<_>>();
            let node_statistics = self.graph[node].statistics();
            let propagated_statistics = self.graph[node]
                .expr
                .propagate_statistics(node_statistics, &children_stats)?;
            if let Some(propagated_stats) = propagated_statistics {
                for (child_idx, stat) in children.into_iter().zip(propagated_stats) {
                    self.graph[child_idx].statistics = stat;
                }
            } else {
                // The constraint is infeasible, report:
                return Ok(PropagationResult::Infeasible);
            }
        }
        Ok(PropagationResult::Success)
    }
}

/// Creates a new statistic with `Bernoulli` distribution by computing the
/// resulting probability. Expects `op` to be a comparison operator, with
/// `left` and `right` having numeric distributions. The resulting distribution
/// has the `Float64` data type.
pub fn create_bernoulli_from_comparison(
    op: &Operator,
    left: &StatisticsV2,
    right: &StatisticsV2,
) -> Result<StatisticsV2> {
    match (left, right) {
        (Uniform(left), Uniform(right)) => {
            match op {
                Operator::Eq | Operator::NotEq => {
                    let (li, ri) = (left.range(), right.range());
                    if let Some(intersection) = li.intersect(ri)? {
                        // If the ranges are not disjoint, calculate the probability
                        // of equality using cardinalities:
                        if let (Some(lc), Some(rc), Some(ic)) = (
                            li.cardinality(),
                            ri.cardinality(),
                            intersection.cardinality(),
                        ) {
                            // Avoid overflow by widening the type temporarily:
                            let pairs = ((lc as u128) * (rc as u128)) as f64;
                            let p = (ic as f64).div_checked(pairs)?;
                            return if op == &Operator::Eq {
                                StatisticsV2::new_bernoulli(ScalarValue::from(p))
                            } else {
                                StatisticsV2::new_bernoulli(ScalarValue::from(1.0 - p))
                            };
                        }
                    } else if op == &Operator::Eq {
                        // If the ranges are disjoint, probability of equality is 0.
                        return StatisticsV2::new_bernoulli(ScalarValue::from(0.0));
                    } else {
                        // If the ranges are disjoint, probability of not-equality is 1.
                        return StatisticsV2::new_bernoulli(ScalarValue::from(1.0));
                    }
                }
                Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                    // TODO: We can handle inequality operators and calculate a
                    // `p` value instead of falling back to an unknown Bernoulli
                    // distribution. Note that the strict and non-strict inequalities
                    // may require slightly different logic in case of real vs.
                    // integral data types.
                }
                _ => {}
            }
        }
        (Gaussian(_), Gaussian(_)) => {
            // TODO: We can handle Gaussian comparisons and calculate a `p` value
            //       instead of falling back to an unknown Bernoulli distribution.
        }
        _ => {}
    }
    let (li, ri) = (left.range()?, right.range()?);
    let range_evaluation = apply_operator(op, &li, &ri)?;
    if range_evaluation.eq(&Interval::CERTAINLY_FALSE) {
        StatisticsV2::new_bernoulli(ScalarValue::from(0.0))
    } else if range_evaluation.eq(&Interval::CERTAINLY_TRUE) {
        StatisticsV2::new_bernoulli(ScalarValue::from(1.0))
    } else if range_evaluation.eq(&Interval::UNCERTAIN) {
        StatisticsV2::new_bernoulli(ScalarValue::try_from(&DataType::Float64)?)
    } else {
        internal_err!("This function must be called with a comparison operator")
    }
}

/// Creates a new statistic with `Unknown` distribution, and tries to compute
/// mean, median and variance if possible.
pub fn new_unknown_from_binary_expr(
    op: &Operator,
    left: &StatisticsV2,
    right: &StatisticsV2,
) -> Result<StatisticsV2> {
    StatisticsV2::new_unknown(
        compute_mean(op, left, right)?,
        compute_median(op, left, right)?,
        compute_variance(op, left, right)?,
        apply_operator(op, &left.range()?, &right.range()?)?,
    )
}

/// Computes the mean value for the result of the given binary operation on
/// two statistics.
pub fn compute_mean(
    op: &Operator,
    left: &StatisticsV2,
    right: &StatisticsV2,
) -> Result<ScalarValue> {
    let (left_mean, right_mean) = (left.mean()?, right.mean()?);

    match op {
        Operator::Plus => return left_mean.add_checked(right_mean),
        Operator::Minus => return left_mean.sub_checked(right_mean),
        // Note the independence assumption below:
        Operator::Multiply => return left_mean.mul_checked(right_mean),
        // TODO: We can calculate the mean for division when we support reciprocals,
        // or know the distributions of the operands. For details, see:
        //
        // <https://en.wikipedia.org/wiki/Algebra_of_random_variables>
        // <https://stats.stackexchange.com/questions/185683/distribution-of-ratio-between-two-independent-uniform-random-variables>
        Operator::Divide => {
            // Fall back to an unknown mean value for division:
            debug!("Division is not yet supported for mean calculations");
        }
        // Fall back to an unknown mean value for other cases:
        _ => {
            debug!("Unsupported operator {op} for mean calculations");
        }
    }
    let target_type = StatisticsV2::target_type(&[&left_mean, &right_mean])?;
    ScalarValue::try_from(target_type)
}

/// Computes the median value for the result of the given binary operation on
/// two statistics. Currently, the median is calculable only for addition and
/// subtraction operations on:
/// - [`Uniform`] and [`Uniform`] distributions, and
/// - [`Gaussian`] and [`Gaussian`] distributions.
pub fn compute_median(
    op: &Operator,
    left: &StatisticsV2,
    right: &StatisticsV2,
) -> Result<ScalarValue> {
    match (left, right) {
        (Uniform(lu), Uniform(ru)) => {
            let (left_median, right_median) = (lu.median()?, ru.median()?);
            // Under the independence assumption, the result is a symmetric
            // triangular distribution, so we can simply add/subtract the
            // median values:
            match op {
                Operator::Plus => return left_median.add_checked(right_median),
                Operator::Minus => return left_median.sub_checked(right_median),
                // Fall back to an unknown median value for other cases:
                _ => {}
            }
        }
        // Under the independence assumption, the result is another Gaussian
        // distribution, so we can simply add/subtract the median values:
        (Gaussian(lg), Gaussian(rg)) => match op {
            Operator::Plus => return lg.mean().add_checked(rg.mean()),
            Operator::Minus => return lg.mean().sub_checked(rg.mean()),
            // Fall back to an unknown median value for other cases:
            _ => {}
        },
        // Fall back to an unknown median value for other cases:
        _ => {}
    }

    let (left_median, right_median) = (left.median()?, right.median()?);
    let target_type = StatisticsV2::target_type(&[&left_median, &right_median])?;
    ScalarValue::try_from(target_type)
}

/// Computes the variance value for the result of the given binary operation on
/// two statistics.
pub fn compute_variance(
    op: &Operator,
    left: &StatisticsV2,
    right: &StatisticsV2,
) -> Result<ScalarValue> {
    let (left_variance, right_variance) = (left.variance()?, right.variance()?);

    match op {
        // Note the independence assumption below:
        Operator::Plus => return left_variance.add_checked(right_variance),
        // Note the independence assumption below:
        Operator::Minus => return left_variance.add_checked(right_variance),
        // Note the independence assumption below:
        Operator::Multiply => {
            // For more details, along with an explanation of the formula below, see:
            //
            // <https://en.wikipedia.org/wiki/Distribution_of_the_product_of_two_random_variables>
            let (left_mean, right_mean) = (left.mean()?, right.mean()?);
            let left_mean_sq = left_mean.mul_checked(&left_mean)?;
            let right_mean_sq = right_mean.mul_checked(&right_mean)?;
            let left_sos = left_variance.add_checked(&left_mean_sq)?;
            let right_sos = right_variance.add_checked(&right_mean_sq)?;
            let pos = left_mean_sq.mul_checked(right_mean_sq)?;
            return left_sos.mul_checked(right_sos)?.sub_checked(pos);
        }
        // TODO: We can calculate the variance for division when we support reciprocals,
        // or know the distributions of the operands. For details, see:
        //
        // <https://en.wikipedia.org/wiki/Algebra_of_random_variables>
        // <https://stats.stackexchange.com/questions/185683/distribution-of-ratio-between-two-independent-uniform-random-variables>
        Operator::Divide => {
            // Fall back to an unknown variance value for division:
            debug!("Division is not yet supported for variance calculations");
        }
        // Fall back to an unknown variance value for other cases:
        _ => {
            debug!("Unsupported operator {op} for variance calculations");
        }
    }
    let target_type = StatisticsV2::target_type(&[&left_variance, &right_variance])?;
    ScalarValue::try_from(target_type)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::expressions::{binary, try_cast, BinaryExpr, Column};
    use crate::intervals::cp_solver::PropagationResult;
    use crate::utils::stats_v2_graph::{
        compute_mean, compute_median, compute_variance, create_bernoulli_from_comparison,
        get_one, new_unknown_from_binary_expr, ExprStatisticGraph,
    };

    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::ScalarValue::Float64;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr_common::interval_arithmetic::{apply_operator, Interval};
    use datafusion_expr_common::operator::Operator;
    use datafusion_expr_common::operator::Operator::{
        Eq, Gt, GtEq, Lt, LtEq, Minus, Multiply, Plus,
    };
    use datafusion_expr_common::type_coercion::binary::BinaryTypeCoercer;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_expr_common::stats_v2::StatisticsV2;

    type Actual = ScalarValue;
    type Expected = ScalarValue;

    pub fn binary_expr(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
        schema: &Schema,
    ) -> Result<BinaryExpr> {
        let left_type = left.data_type(schema)?;
        let right_type = right.data_type(schema)?;
        let binary_type_coercer = BinaryTypeCoercer::new(&left_type, &op, &right_type);
        let (lhs, rhs) = binary_type_coercer.get_input_types()?;

        let left_expr = try_cast(left, schema, lhs)?;
        let right_expr = try_cast(right, schema, rhs)?;
        let b = binary(left_expr, op, right_expr, schema);
        Ok(b?.as_any().downcast_ref::<BinaryExpr>().unwrap().clone())
    }

    // Expected test results were calculated in Wolfram Mathematica, by using
    // *METHOD_NAME*[TransformedDistribution[x op y, {x ~ *DISTRIBUTION_X*[..], y ~ *DISTRIBUTION_Y*[..]}]]
    #[test]
    fn test_calculate_unknown_properties_uniform_uniform() -> Result<()> {
        let stat_a = StatisticsV2::new_uniform(Interval::make(Some(0.), Some(12.))?)?;
        let stat_b = StatisticsV2::new_uniform(Interval::make(Some(12.), Some(36.))?)?;

        let test_data: Vec<(Actual, Expected)> = vec![
            // mean
            (compute_mean(&Plus, &stat_a, &stat_b)?, Float64(Some(30.))),
            (compute_mean(&Minus, &stat_a, &stat_b)?, Float64(Some(-18.))),
            (
                compute_mean(&Multiply, &stat_a, &stat_b)?,
                Float64(Some(144.)),
            ),
            // median
            (compute_median(&Plus, &stat_a, &stat_b)?, Float64(Some(30.))),
            (
                compute_median(&Minus, &stat_a, &stat_b)?,
                Float64(Some(-18.)),
            ),
            // FYI: median of combined distributions for mul, div and mod ops doesn't exist.

            // variance
            (
                compute_variance(&Plus, &stat_a, &stat_b)?,
                Float64(Some(60.)),
            ),
            (
                compute_variance(&Minus, &stat_a, &stat_b)?,
                Float64(Some(60.)),
            ),
            // (compute_variance(&Operator::Multiply, &stat_a, &stat_b), Some(Float64(Some(9216.)))),
        ];
        for (actual, expected) in test_data {
            assert_eq!(actual, expected);
        }

        Ok(())
    }

    #[test]
    fn test_calculate_unknown_properties_gauss_gauss() -> Result<()> {
        let stat_a = StatisticsV2::new_gaussian(
            ScalarValue::from(Some(10.)),
            ScalarValue::from(Some(0.0)),
        )?;
        let stat_b = StatisticsV2::new_gaussian(
            ScalarValue::from(Some(20.)),
            ScalarValue::from(Some(0.0)),
        )?;

        let test_data: Vec<(Actual, Expected)> = vec![
            // mean
            (compute_mean(&Plus, &stat_a, &stat_b)?, Float64(Some(30.))),
            (compute_mean(&Minus, &stat_a, &stat_b)?, Float64(Some(-10.))),
            // median
            (compute_median(&Plus, &stat_a, &stat_b)?, Float64(Some(30.))),
            (
                compute_median(&Minus, &stat_a, &stat_b)?,
                Float64(Some(-10.)),
            ),
        ];
        for (actual, expected) in test_data {
            assert_eq!(actual, expected);
        }

        Ok(())
    }

    /// Test for Uniform-Uniform, Uniform-Unknown, Unknown-Uniform, Unknown-Unknown pairs,
    /// where range is always present.
    #[test]
    fn test_compute_range_where_present() -> Result<()> {
        let a = &Interval::make(Some(0.), Some(12.0))?;
        let b = &Interval::make(Some(0.), Some(12.0))?;
        let mean = Float64(Some(6.0));
        for (stat_a, stat_b) in [
            (
                StatisticsV2::new_uniform(a.clone())?,
                StatisticsV2::new_uniform(b.clone())?,
            ),
            (
                StatisticsV2::new_unknown(
                    mean.clone(),
                    mean.clone(),
                    Float64(None),
                    a.clone(),
                )?,
                StatisticsV2::new_uniform(b.clone())?,
            ),
            (
                StatisticsV2::new_uniform(a.clone())?,
                StatisticsV2::new_unknown(
                    mean.clone(),
                    mean.clone(),
                    Float64(None),
                    b.clone(),
                )?,
            ),
            (
                StatisticsV2::new_unknown(
                    mean.clone(),
                    mean.clone(),
                    Float64(None),
                    a.clone(),
                )?,
                StatisticsV2::new_unknown(
                    mean.clone(),
                    mean.clone(),
                    Float64(None),
                    b.clone(),
                )?,
            ),
        ] {
            for op in [Plus, Minus, Multiply] {
                assert_eq!(
                    new_unknown_from_binary_expr(&op, &stat_a, &stat_b)?.range()?,
                    apply_operator(&op, a, b)?,
                    "Failed for {:?} {op} {:?}",
                    stat_a,
                    stat_b
                );
            }
            for op in [Gt, GtEq, Lt, LtEq, Eq] {
                assert_eq!(
                    create_bernoulli_from_comparison(&op, &stat_a, &stat_b)?.range()?,
                    apply_operator(&op, a, b)?,
                    "Failed for {:?} {op} {:?}",
                    stat_a,
                    stat_b
                );
            }
        }

        Ok(())
    }

    #[test]
    fn test_stats_v2_integration() -> Result<()> {
        let schema = &Schema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Float64, false),
            Field::new("d", DataType::Float64, false),
        ]);

        let a: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));
        let b: Arc<dyn PhysicalExpr> = Arc::new(Column::new("b", 1));
        let c: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c", 2));
        let d: Arc<dyn PhysicalExpr> = Arc::new(Column::new("d", 3));

        let left: Arc<dyn PhysicalExpr> =
            Arc::new(binary_expr(Arc::clone(&a), Plus, Arc::clone(&b), schema)?);
        let right: Arc<dyn PhysicalExpr> =
            Arc::new(binary_expr(Arc::clone(&c), Minus, Arc::clone(&d), schema)?);
        let expr: Arc<dyn PhysicalExpr> = Arc::new(binary_expr(
            Arc::clone(&left),
            Eq,
            Arc::clone(&right),
            schema,
        )?);

        let mut graph = ExprStatisticGraph::try_new(expr, schema)?;
        // 2, 5 and 6 are BinaryExpr
        graph.assign_statistics(&[
            (
                0usize,
                StatisticsV2::new_uniform(Interval::make(Some(0.), Some(1.))?)?,
            ),
            (
                1usize,
                StatisticsV2::new_uniform(Interval::make(Some(0.), Some(2.))?)?,
            ),
            (
                3usize,
                StatisticsV2::new_uniform(Interval::make(Some(1.), Some(3.))?)?,
            ),
            (
                4usize,
                StatisticsV2::new_uniform(Interval::make(Some(1.), Some(5.))?)?,
            ),
        ]);
        let ev_stats = graph.evaluate_statistics()?;
        assert_eq!(ev_stats, &StatisticsV2::new_bernoulli(Float64(None))?);

        assert_eq!(
            graph
                .propagate_statistics(StatisticsV2::new_bernoulli(get_one().clone())?)?,
            PropagationResult::Success
        );
        Ok(())
    }
}
