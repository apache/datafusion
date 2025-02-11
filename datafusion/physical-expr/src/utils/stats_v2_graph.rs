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
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr_common::interval_arithmetic::{apply_operator, Interval};
use datafusion_expr_common::operator::Operator;
use datafusion_physical_expr_common::stats_v2::StatisticsV2::{
    Exponential, Gaussian, Uniform, Unknown,
};
use datafusion_physical_expr_common::stats_v2::{get_one, get_zero, StatisticsV2};

use log::debug;
use petgraph::adj::DefaultIx;
use petgraph::prelude::Bfs;
use petgraph::stable_graph::{NodeIndex, StableGraph};
use petgraph::visit::DfsPostOrder;
use petgraph::Outgoing;

#[derive(Clone, Debug)]
pub struct ExprStatisticGraphNode {
    expr: Arc<dyn PhysicalExpr>,
    statistics: StatisticsV2,
}

impl ExprStatisticGraphNode {
    /// Creates a DAEG node from DataFusion's [`ExprTreeNode`] object. Literals are creating
    /// [`Uniform`] distribution kind of statistic with definite, singleton intervals.
    /// Otherwise, create [`Unknown`] statistic with an unbounded interval, by default.
    pub fn make_node(node: &ExprTreeNode<NodeIndex>, schema: &Schema) -> Result<Self> {
        let expr = Arc::clone(&node.expr);
        if let Some(literal) = expr.as_any().downcast_ref::<Literal>() {
            let value = literal.value();
            Interval::try_new(value.clone(), value.clone())
                .map(|interval| Self::new_uniform(expr, interval))
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

    /// Creates a new graph node with statistic based on a given interval as [`Uniform`] distribution
    fn new_uniform(expr: Arc<dyn PhysicalExpr>, interval: Interval) -> Self {
        ExprStatisticGraphNode {
            expr,
            statistics: StatisticsV2::new_uniform(interval).unwrap(),
        }
    }

    /// Creates a new graph node as `Bernoulli` distribution with uncertain probability.
    fn new_bernoulli(expr: Arc<dyn PhysicalExpr>) -> Result<Self> {
        Ok(ExprStatisticGraphNode {
            expr,
            statistics: StatisticsV2::new_bernoulli(ScalarValue::Float64(None))?,
        })
    }

    /// Creates a new graph node with [`Unknown`] statistic.
    fn new_unknown(expr: Arc<dyn PhysicalExpr>, dt: &DataType) -> Result<Self> {
        Ok(ExprStatisticGraphNode {
            expr,
            statistics: StatisticsV2::new_unknown(
                ScalarValue::try_from(dt)?,
                ScalarValue::try_from(dt)?,
                ScalarValue::try_from(dt)?,
                Interval::make_unbounded(dt)?,
            )?,
        })
    }

    pub fn statistic(&self) -> &StatisticsV2 {
        &self.statistics
    }

    pub fn expression(&self) -> Arc<dyn PhysicalExpr> {
        Arc::clone(&self.expr)
    }
}

#[derive(Clone, Debug)]
pub struct ExprStatisticGraph {
    graph: StableGraph<ExprStatisticGraphNode, usize>,
    root: NodeIndex,
}

impl ExprStatisticGraph {
    pub fn try_new(expr: Arc<dyn PhysicalExpr>, schema: &Schema) -> Result<Self> {
        // Build the full graph:
        let (root, graph) = build_dag(expr, &|node| {
            ExprStatisticGraphNode::make_node(node, schema)
        })?;
        Ok(Self { graph, root })
    }

    pub fn assign_statistics(&mut self, assignments: &[(usize, StatisticsV2)]) {
        for (index, stats) in assignments {
            let node_index = NodeIndex::from(*index as DefaultIx);
            self.graph[node_index].statistics = stats.clone();
        }
    }

    /// Runs a statistics evaluation mechanism in a bottom-up manner,
    /// to calculate a root expression statistic.
    /// Returns a calculated root expression statistic.
    pub fn evaluate_statistics(&mut self) -> Result<&StatisticsV2> {
        let mut dfs = DfsPostOrder::new(&self.graph, self.root);
        while let Some(idx) = dfs.next(&self.graph) {
            let neighbors = self.graph.neighbors_directed(idx, Outgoing);
            let mut children_statistics = neighbors
                .map(|child| &self.graph[child].statistics)
                .collect::<Vec<_>>();
            // Note: all distributions are recognized as independent, by default.
            if !children_statistics.is_empty() {
                // Reverse to align with `PhysicalExpr`'s children:
                children_statistics.reverse();
                self.graph[idx].statistics = self.graph[idx]
                    .expr
                    .evaluate_statistics(&children_statistics)?;
            }
        }

        Ok(&self.graph[self.root].statistics)
    }

    /// Runs a propagation mechanism in a top-down manner to define a statistics for a leaf nodes.
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
                    StatisticsV2::new_from_interval(&interval)?;
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
                .map(|child| self.graph[*child].statistic())
                .collect::<Vec<_>>();
            let node_statistics = self.graph[node].statistic();
            let propagated_statistics = self.graph[node]
                .expression()
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

/// Creates a new statistic with [`Unknown`] distribution, and tries to compute
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
        compute_range(op, left, right)?,
    )
}

/// Creates a new statistic with `Bernoulli` distribution by computing the
/// resulting probability. Expects `op` to be a comparison operator, with `left`
/// and `right` having numeric, continuous distributions.
pub fn create_bernoulli_from_comparison(
    op: &Operator,
    left: &StatisticsV2,
    right: &StatisticsV2,
) -> Result<StatisticsV2> {
    let (li, ri) = (left.range()?, right.range()?);
    match (left, right) {
        (Uniform(_), Uniform(_)) => {
            match op {
                Operator::Eq | Operator::NotEq => {
                    return if let Some(intersection) = li.intersect(&ri)? {
                        let union = li.union(&ri)?;
                        let union_width = union.width()?;
                        // We know that the intersection and the union have the same data types,
                        // it is sufficient to check one of them:
                        let p = if union_width.data_type().is_integer() {
                            intersection
                                .width()?
                                .cast_to(&DataType::Float64)?
                                .div(union_width.cast_to(&DataType::Float64)?)?
                        } else {
                            intersection.width()?.div(union_width)?
                        };
                        if op == &Operator::Eq {
                            StatisticsV2::new_bernoulli(p)
                        } else {
                            let one = ScalarValue::new_one(&li.data_type())?;
                            StatisticsV2::new_bernoulli(one.sub(p)?)
                        }
                    } else if op == &Operator::Eq {
                        StatisticsV2::new_bernoulli(ScalarValue::new_zero(
                            &li.data_type(),
                        )?)
                    } else {
                        StatisticsV2::new_bernoulli(ScalarValue::new_one(
                            &li.data_type(),
                        )?)
                    };
                }
                Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                    // TODO: We can handle inequality operators and calculate a `p` value
                    //       instead of falling back to an unknown Bernoulli distribution.
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
    let range_evaluation = apply_operator(op, &li, &ri)?;
    if range_evaluation.eq(&Interval::CERTAINLY_FALSE) {
        StatisticsV2::new_bernoulli(ScalarValue::new_zero(&li.data_type())?)
    } else if range_evaluation.eq(&Interval::CERTAINLY_TRUE) {
        StatisticsV2::new_bernoulli(ScalarValue::new_one(&li.data_type())?)
    } else if range_evaluation.eq(&Interval::UNCERTAIN) {
        StatisticsV2::new_bernoulli(ScalarValue::try_from(&li.data_type())?)
    } else {
        internal_err!("This function must be called with a comparison operator")
    }
}

/// Computes a mean value for a given binary operator and two statistics.
/// The result is calculated based on the operator type for any statistics kind.
pub fn compute_mean(
    op: &Operator,
    left_stat: &StatisticsV2,
    right_stat: &StatisticsV2,
) -> Result<ScalarValue> {
    let (left_mean, right_mean) = (left_stat.mean()?, right_stat.mean()?);

    match op {
        Operator::Plus => left_mean.add_checked(right_mean),
        Operator::Minus => left_mean.sub_checked(right_mean),
        Operator::Multiply => left_mean.mul_checked(right_mean),
        Operator::Divide => {
            // TODO: We can calculate the mean for division when we know the
            //       distributions of the operands. For example, see:
            //
            // <https://stats.stackexchange.com/questions/185683/distribution-of-ratio-between-two-independent-uniform-random-variables>
            Ok(ScalarValue::Null)
        }
        _ => {
            debug!("Unsupported operator {op} for mean calculations");
            Ok(ScalarValue::Null)
        }
    }
}

/// Computes a median value for a given binary operator and two statistics.
/// The median is calculable only between:
/// [`Uniform`] and [`Uniform`] distributions,
/// [`Gaussian`] and [`Gaussian`] distributions,
/// and only for addition/subtraction.
pub fn compute_median(
    op: &Operator,
    left_stat: &StatisticsV2,
    right_stat: &StatisticsV2,
) -> Result<ScalarValue> {
    let (left_median, right_median) = (left_stat.median()?, right_stat.median()?);
    let target_type = StatisticsV2::target_type(&[&left_median, &right_median])?;

    match (left_stat, right_stat) {
        (Uniform(_), Uniform(_)) => {
            if left_median.is_null() || right_median.is_null() {
                Ok(ScalarValue::try_from(target_type)?)
            } else {
                match op {
                    Operator::Plus => left_median.add_checked(right_median),
                    Operator::Minus => left_median.sub_checked(right_median),
                    _ => Ok(ScalarValue::try_from(target_type)?),
                }
            }
        }
        (Gaussian(lg), Gaussian(rg)) => match op {
            Operator::Plus => lg.mean().add_checked(rg.mean()),
            Operator::Minus => lg.mean().sub_checked(rg.mean()),
            _ => Ok(ScalarValue::try_from(target_type)?),
        },
        // Any
        _ => Ok(ScalarValue::try_from(target_type)?),
    }
}

/// Computes a variance value for a given binary operator and two statistics.
pub fn compute_variance(
    op: &Operator,
    left_stat: &StatisticsV2,
    right_stat: &StatisticsV2,
) -> Result<ScalarValue> {
    let (left_variance, right_variance) = (left_stat.variance()?, right_stat.variance()?);
    let target_type = StatisticsV2::target_type(&[&left_variance, &right_variance])?;

    match (left_stat, right_stat) {
        (Uniform { .. }, Uniform { .. }) => {
            match op {
                Operator::Plus | Operator::Minus => {
                    left_variance.add_checked(right_variance)
                }
                Operator::Multiply => {
                    // TODO: the formula is giga-giant, skipping for now.
                    debug!(
                        "Multiply operator is not supported for variance computation yet"
                    );
                    ScalarValue::try_from(target_type)
                }
                _ => {
                    // Note: mod and div are not supported for any distribution combination pair
                    debug!("Operator {op} cannot be supported for variance computation");
                    ScalarValue::try_from(target_type)
                }
            }
        }
        (Uniform(u), Exponential(e)) | (Exponential(e), Uniform(u)) => {
            let (left_variance, right_variance) = (left_stat.mean()?, right_stat.mean()?);
            let target_type =
                StatisticsV2::target_type(&[&left_variance, &right_variance])?;

            match op {
                Operator::Plus | Operator::Minus => {
                    left_variance.add_checked(right_variance)
                }
                Operator::Multiply => {
                    // (5 * lower^2 + 2 * lower * upper + 5 * upper^2) / 12 * λ^2
                    let five = &ScalarValue::Float64(Some(5.));
                    // 5 * lower^2
                    let interval_lower_sq = u
                        .range()
                        .lower()
                        .mul_checked(u.range().lower())?
                        .cast_to(&DataType::Float64)?
                        .mul_checked(five)?;
                    // 5 * upper^2
                    let interval_upper_sq = u
                        .range()
                        .upper()
                        .mul_checked(u.range().upper())?
                        .cast_to(&DataType::Float64)?
                        .mul_checked(five)?;
                    // 2 * lower * upper
                    let middle = u
                        .range()
                        .upper()
                        .mul_checked(u.range().lower())?
                        .cast_to(&DataType::Float64)?
                        .mul_checked(ScalarValue::Float64(Some(2.)))?;

                    let numerator = interval_lower_sq
                        .add_checked(interval_upper_sq)?
                        .add_checked(middle)?
                        .cast_to(&DataType::Float64)?;

                    let f_rate = &e.rate().cast_to(&DataType::Float64)?;
                    let denominator = ScalarValue::Float64(Some(12.))
                        .mul_checked(f_rate.mul(f_rate)?)?;

                    numerator.div(denominator)
                }
                _ => {
                    // Note: mod and div are not supported for any distribution combination pair
                    debug!("Unsupported operator {op} for variance computation");
                    ScalarValue::try_from(target_type)
                }
            }
        }
        (_, _) => ScalarValue::try_from(target_type),
    }
}

/// Computes range based on input statistics, where it is possible to compute.
/// Otherwise, returns an unbounded interval.
pub fn compute_range(
    op: &Operator,
    left_stat: &StatisticsV2,
    right_stat: &StatisticsV2,
) -> Result<Interval> {
    let (left_range, right_range) = (left_stat.range()?, right_stat.range()?);
    match (left_stat, right_stat) {
        (Uniform(_), Uniform(_))
        | (Uniform(_), Unknown(_))
        | (Unknown(_), Uniform(_))
        | (Unknown(_), Unknown(_)) => match op {
            Operator::Plus
            | Operator::Minus
            | Operator::Multiply
            | Operator::Gt
            | Operator::GtEq
            | Operator::Lt
            | Operator::LtEq => apply_operator(op, &left_range, &right_range),
            Operator::Eq => {
                if let Some(intersection) = left_range.intersect(right_range)? {
                    Ok(intersection)
                } else {
                    Interval::make_zero(&left_stat.data_type())
                }
            }
            _ => Interval::make_unbounded(&DataType::Float64),
        },
        (_, _) => Interval::make_unbounded(&DataType::Float64),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::expressions::{binary, try_cast, BinaryExpr, Column};
    use crate::intervals::cp_solver::PropagationResult;
    use crate::utils::stats_v2_graph::{
        compute_mean, compute_median, compute_range, compute_variance, get_one,
        ExprStatisticGraph,
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
                    ScalarValue::Null,
                    a.clone(),
                )?,
                StatisticsV2::new_uniform(b.clone())?,
            ),
            (
                StatisticsV2::new_uniform(a.clone())?,
                StatisticsV2::new_unknown(
                    mean.clone(),
                    mean.clone(),
                    ScalarValue::Null,
                    b.clone(),
                )?,
            ),
            (
                StatisticsV2::new_unknown(
                    mean.clone(),
                    mean.clone(),
                    ScalarValue::Null,
                    a.clone(),
                )?,
                StatisticsV2::new_unknown(
                    mean.clone(),
                    mean.clone(),
                    ScalarValue::Null,
                    b.clone(),
                )?,
            ),
        ] {
            // range
            for op in [Plus, Minus, Multiply, Gt, GtEq, Lt, LtEq] {
                assert_eq!(
                    compute_range(&op, &stat_a, &stat_b)?,
                    apply_operator(&op, a, b)?,
                    "Failed for {:?} {op} {:?}",
                    stat_a,
                    stat_b
                );
            }

            assert_eq!(
                compute_range(&Eq, &stat_a, &stat_b)?,
                Interval::make(Some(0.0), Some(12.0))?,
            );
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
