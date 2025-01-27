use crate::expressions::Literal;
use crate::physical_expr::PhysicalExpr;
use crate::utils::{build_dag, ExprTreeNode};
use arrow::datatypes::{DataType, Schema};
use datafusion_common::ScalarValue::Float64;
use datafusion_common::{internal_err, ScalarValue};
use datafusion_expr_common::interval_arithmetic::{
    apply_operator, max_of_bounds, min_of_bounds, Interval,
};
use datafusion_expr_common::operator::Operator;
use datafusion_physical_expr_common::stats::StatisticsV2;
use datafusion_physical_expr_common::stats::StatisticsV2::{
    Bernoulli, Exponential, Uniform, Unknown,
};
use log::debug;
use petgraph::adj::DefaultIx;
use petgraph::prelude::Bfs;
use petgraph::stable_graph::{NodeIndex, StableGraph};
use petgraph::visit::DfsPostOrder;
use petgraph::Outgoing;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ExprStatisticGraphNode {
    expr: Arc<dyn PhysicalExpr>,
    statistics: StatisticsV2,
}

impl ExprStatisticGraphNode {
    /// Creates a DAEG node from DataFusion's [`ExprTreeNode`] object. Literals are creating
    /// [`Uniform`] distribution kind of statistic with definite, singleton intervals.
    /// Otherwise, create [`Unknown`] statistic with an unbounded interval.
    pub fn make_node(
        node: &ExprTreeNode<NodeIndex>,
        schema: &Schema,
    ) -> datafusion_common::Result<Self> {
        let expr = Arc::clone(&node.expr);
        if let Some(literal) = expr.as_any().downcast_ref::<Literal>() {
            let value = literal.value();
            Interval::try_new(value.clone(), value.clone())
                .map(|interval| Self::new_uniform(expr, interval))
        } else {
            expr.data_type(schema)
                .and_then(|dt| Self::new_unknown(expr, &dt))
        }
    }

    /// Creates a DAEG node from DataFusion's [`ExprTreeNode`] object. Literals are creating
    /// [`Uniform`] distribution kind of statistic with definite, singleton intervals.
    /// Otherwise, create [`Unknown`] statistic with an unbounded interval.
    pub fn make_node_with_stats(
        node: &ExprTreeNode<NodeIndex>,
        stats: StatisticsV2,
    ) -> Self {
        Self::new(Arc::clone(&node.expr), stats)
    }

    /// Creates a new graph node with prepared statistics
    fn new(expr: Arc<dyn PhysicalExpr>, stats: StatisticsV2) -> Self {
        ExprStatisticGraphNode {
            expr,
            statistics: stats,
        }
    }

    /// Creates a new graph node with statistic based on a given interval as [`Uniform`] distribution
    fn new_uniform(expr: Arc<dyn PhysicalExpr>, interval: Interval) -> Self {
        ExprStatisticGraphNode {
            expr,
            statistics: Uniform { interval },
        }
    }

    /// Creates a new graph node with [`Unknown`] statistic.
    fn new_unknown(
        expr: Arc<dyn PhysicalExpr>,
        dt: &DataType,
    ) -> datafusion_common::Result<Self> {
        Ok(ExprStatisticGraphNode {
            expr,
            statistics: Unknown {
                mean: None,
                median: None,
                variance: None,
                range: Interval::make_unbounded(dt)?,
            },
        })
    }

    pub fn statistic(&self) -> &StatisticsV2 {
        &self.statistics
    }
}

#[derive(Clone, Debug)]
pub struct ExprStatisticGraph {
    graph: StableGraph<ExprStatisticGraphNode, usize>,
    root: NodeIndex,
}

impl ExprStatisticGraph {
    pub fn try_new(
        expr: Arc<dyn PhysicalExpr>,
        schema: &Schema,
    ) -> datafusion_common::Result<Self> {
        // Build the full graph:
        let (root, graph) = build_dag(expr, &|node| {
            ExprStatisticGraphNode::make_node(node, schema)
        })?;
        Ok(Self { graph, root })
    }

    pub fn assign_statistic(&mut self, idx: usize, stats: StatisticsV2) {
        self.graph[NodeIndex::from(idx as DefaultIx)].statistics = stats;
    }

    pub fn assign_statistics(&mut self, assignments: &[(usize, StatisticsV2)]) {
        for (index, stats) in assignments {
            let node_index = NodeIndex::from(*index as DefaultIx);
            self.graph[node_index].statistics = stats.clone();
        }
    }

    /// Runs a propagation mechanism in a top-down manner to define a statistics for a leaf nodes.
    /// Returns false, if propagation was infeasible, true otherwise.
    pub fn propagate(&mut self) -> datafusion_common::Result<bool> {
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
            let propagated_statistics = self.graph[node]
                .expr
                .propagate_statistics(self.graph[node].statistic(), &children_stats)?;

            if let Some(propagated_stats) = propagated_statistics {
                for (child_idx, stat) in children.into_iter().zip(propagated_stats) {
                    self.graph[child_idx].statistics = stat;
                }
            } else {
                // The constraint is infeasible, report:
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Runs a statistics evaluation mechanism in a bottom-up manner,
    /// to calculate a root expression statistic.
    /// Returns a calculated root expression statistic.
    pub fn evaluate(&mut self) -> datafusion_common::Result<&StatisticsV2> {
        let mut dfs = DfsPostOrder::new(&self.graph, self.root);

        while let Some(idx) = dfs.next(&self.graph) {
            let neighbors = self.graph.neighbors_directed(idx, Outgoing);
            let children_statistics = neighbors
                .map(|child| &self.graph[child].statistics)
                .collect::<Vec<_>>();

            // Note: all distributions are recognized as independent, by default.
            if !children_statistics.is_empty() {
                self.graph[idx].statistics = self.graph[idx]
                    .expr
                    .evaluate_statistics(&children_statistics)?;
            }
        }

        Ok(&self.graph[self.root].statistics)
    }
}

/// Creates a new [`Unknown`] statistics instance with a given range.
/// It makes its best to infer mean, median and variance, if it is possible.
/// This builder is moved here due to original package visibility limitations.
pub fn new_unknown_from_interval(
    range: &Interval,
) -> datafusion_common::Result<StatisticsV2> {
    // Note: to avoid code duplication for mean/median/variance computation, we wrap
    // existing range in temporary uniform distribution and compute all these properties.
    let fake_uniform = Uniform {
        interval: range.clone(),
    };

    Ok(Unknown {
        mean: fake_uniform.mean()?,
        median: fake_uniform.median()?,
        variance: fake_uniform.variance()?,
        range: range.clone(),
    })
}

/// Creates a new [`Unknown`] distribution, and tries to compute
/// mean/median/variance if it is calculable.
pub fn new_unknown_from_binary_expr(
    op: &Operator,
    left: &StatisticsV2,
    right: &StatisticsV2,
) -> datafusion_common::Result<StatisticsV2> {
    Ok(Unknown {
        mean: compute_mean(op, left, right)?,
        median: compute_median(op, left, right)?,
        variance: compute_variance(op, left, right)?,
        range: compute_range(op, left, right)?,
    })
}

//noinspection DuplicatedCode
/// Tries to create a new [`Bernoulli`] distribution, by computing the result probability,
/// specifically, for Eq and NotEq operators with range-contained distributions.
/// If not being able to compute a probability, returns an [`Unknown`] distribution.
pub fn new_bernoulli_from_binary_expr(
    op: &Operator,
    left: &StatisticsV2,
    right: &StatisticsV2,
) -> datafusion_common::Result<StatisticsV2> {
    match op {
        Operator::Eq | Operator::NotEq => match (left, right) {
            (Uniform { interval: li }, Uniform { interval: ri })
            | (Uniform { interval: li }, Unknown { range: ri, .. })
            | (Unknown { range: li, .. }, Uniform { interval: ri })
            | (Unknown { range: li, .. }, Unknown { range: ri, .. }) => {
                // Note: unbounded intervals will be caught in `intersect` method.
                if let Some(intersection) = li.intersect(ri)? {
                    if li.data_type().is_numeric() {
                        let overall_spread = max_of_bounds(li.upper(), ri.upper())
                            .sub_checked(min_of_bounds(li.lower(), ri.lower()))?;
                        let intersection_spread =
                            intersection.upper().sub_checked(intersection.lower())?;

                        let p = intersection_spread
                            .cast_to(&DataType::Float64)?
                            .div(overall_spread.cast_to(&DataType::Float64)?)?;

                        if op == &Operator::Eq {
                            Ok(Bernoulli { p })
                        } else {
                            Ok(Bernoulli {
                                p: Float64(Some(1.)).sub(p)?,
                            })
                        }
                    } else {
                        internal_err!("Cannot compute non-numeric probability")
                    }
                } else {
                    new_unknown_from_binary_expr(op, left, right)
                }
            }
            // TODO: handle inequalities, temporarily returns [`Unknown`]
            _ => new_unknown_from_binary_expr(op, left, right),
        },
        _ => new_unknown_from_binary_expr(op, left, right),
    }
}

/// Computes a mean value for a given binary operator and two statistics.
/// The result is calculated based on the operator type for any statistics kind.
pub fn compute_mean(
    op: &Operator,
    left_stat: &StatisticsV2,
    right_stat: &StatisticsV2,
) -> datafusion_common::Result<Option<ScalarValue>> {
    if let (Some(l_mean), Some(r_mean)) = (left_stat.mean()?, right_stat.mean()?) {
        match op {
            Operator::Plus => Ok(Some(l_mean.add_checked(r_mean)?)),
            Operator::Minus => Ok(Some(l_mean.sub_checked(r_mean)?)),
            Operator::Multiply => Ok(Some(l_mean.mul_checked(r_mean)?)),
            Operator::Divide => {
                // ((l_lower + l_upper) (log[r_lower] - log[r_upper)) / 2(c-d)
                debug!("Division is not supported for mean computation; log() for ScalarValue is not supported");
                Ok(None)
            }
            _ => {
                debug!("Unsupported operator {op} for mean computation");
                Ok(None)
            }
        }
    } else {
        Ok(None)
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
) -> datafusion_common::Result<Option<ScalarValue>> {
    match (left_stat, right_stat) {
        (Uniform { .. }, Uniform { .. }) => {
            if let (Some(l_median), Some(r_median)) =
                (left_stat.median()?, right_stat.median()?)
            {
                match op {
                    Operator::Plus => Ok(Some(l_median.add_checked(r_median)?)),
                    Operator::Minus => Ok(Some(l_median.sub_checked(r_median)?)),
                    Operator::Multiply | Operator::Divide | Operator::Modulo => Ok(None),
                    _ => Ok(None),
                }
            } else {
                Ok(None)
            }
        }
        // Any
        _ => Ok(None),
    }
}

/// Computes a variance value for a given binary operator and two statistics.
///
pub fn compute_variance(
    op: &Operator,
    left_stat: &StatisticsV2,
    right_stat: &StatisticsV2,
) -> datafusion_common::Result<Option<ScalarValue>> {
    match (left_stat, right_stat) {
        (Uniform { .. }, Uniform { .. }) => {
            if let (Some(l_variance), Some(r_variance)) =
                (left_stat.variance()?, right_stat.variance()?)
            {
                match op {
                    Operator::Plus | Operator::Minus => {
                        Ok(Some(l_variance.add_checked(r_variance)?))
                    }
                    Operator::Multiply => {
                        // TODO: the formula is giga-giant, skipping for now.
                        debug!("Multiply operator is not supported for variance computation yet");
                        Ok(None)
                    }
                    _ => {
                        // Note: mod and div are not supported for any distribution combination pair
                        debug!(
                            "Operator {op} cannot be supported for variance computation"
                        );
                        Ok(None)
                    }
                }
            } else {
                Ok(None)
            }
        }
        (Uniform { interval }, Exponential { rate, .. })
        | (Exponential { rate, .. }, Uniform { interval }) => {
            if let (Some(l_variance), Some(r_variance)) =
                (left_stat.mean()?, right_stat.mean()?)
            {
                match op {
                    Operator::Plus | Operator::Minus => {
                        Ok(Some(l_variance.add_checked(r_variance)?))
                    }
                    Operator::Multiply => {
                        // (5 * lower^2 + 2 * lower * upper + 5 * upper^2) / 12 * λ^2
                        let five = &Float64(Some(5.));
                        // 5 * lower^2
                        let interval_lower_sq = interval
                            .lower()
                            .mul_checked(interval.lower())?
                            .cast_to(&DataType::Float64)?
                            .mul_checked(five)?;
                        // 5 * upper^2
                        let interval_upper_sq = interval
                            .upper()
                            .mul_checked(interval.upper())?
                            .cast_to(&DataType::Float64)?
                            .mul_checked(five)?;
                        // 2 * lower * upper
                        let middle = interval
                            .upper()
                            .mul_checked(interval.lower())?
                            .cast_to(&DataType::Float64)?
                            .mul_checked(Float64(Some(2.)))?;

                        let numerator = interval_lower_sq
                            .add_checked(interval_upper_sq)?
                            .add_checked(middle)?;
                        let denominator = Float64(Some(12.))
                            .mul_checked(rate.mul(rate)?.cast_to(&DataType::Float64)?)?;

                        Ok(Some(numerator.div(denominator)?))
                    }
                    _ => {
                        // Note: mod and div are not supported for any distribution combination pair
                        debug!("Unsupported operator {op} for variance computation");
                        Ok(None)
                    }
                }
            } else {
                Ok(None)
            }
        }
        (_, _) => Ok(None),
    }
}

/// Computes range based on input statistics, where it is possible to compute.
/// Otherwise, returns an unbounded interval.
pub fn compute_range(
    op: &Operator,
    left_stat: &StatisticsV2,
    right_stat: &StatisticsV2,
) -> datafusion_common::Result<Interval> {
    if !left_stat.is_valid() || !right_stat.is_valid() {
        return Interval::make_unbounded(&DataType::Float64);
    }
    match (left_stat, right_stat) {
        (Uniform { interval: l }, Uniform { interval: r })
        | (Uniform { interval: l }, Unknown { range: r, .. })
        | (Unknown { range: l, .. }, Uniform { interval: r })
        | (Unknown { range: l, .. }, Unknown { range: r, .. }) => match op {
            Operator::Plus
            | Operator::Minus
            | Operator::Multiply
            | Operator::Gt
            | Operator::GtEq
            | Operator::Lt
            | Operator::LtEq => apply_operator(op, l, r),
            Operator::Eq => {
                if let Some(intersection) = l.intersect(r)? {
                    Ok(intersection)
                } else if let Some(data_type) = left_stat.data_type() {
                    Interval::make_zero(&data_type)
                } else {
                    internal_err!("Invariant violation: data_type cannot be None here")
                }
            }
            _ => Interval::make_unbounded(&DataType::Float64),
        },
        (_, _) => Interval::make_unbounded(&DataType::Float64),
    }
}

#[cfg(test)]
mod tests {
    use crate::expressions::{binary, try_cast, BinaryExpr, Column};
    use crate::utils::stats::{
        compute_mean, compute_median, compute_range, compute_variance, ExprStatisticGraph,
    };
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_common::ScalarValue::Float64;
    use datafusion_expr_common::interval_arithmetic::{apply_operator, Interval};
    use datafusion_expr_common::operator::Operator;
    use datafusion_expr_common::operator::Operator::{
        Eq, Gt, GtEq, Lt, LtEq, Minus, Multiply, Plus,
    };
    use datafusion_expr_common::type_coercion::binary::get_input_types;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_expr_common::stats::StatisticsV2::{
        Bernoulli, Uniform, Unknown,
    };
    use std::sync::Arc;

    type Actual = Option<ScalarValue>;
    type Expected = Option<ScalarValue>;

    pub fn binary_expr(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
        schema: &Schema,
    ) -> datafusion_common::Result<BinaryExpr> {
        let left_type = left.data_type(schema)?;
        let right_type = right.data_type(schema)?;
        let (lhs, rhs) = get_input_types(&left_type, &op, &right_type)?;

        let left_expr = try_cast(left, schema, lhs)?;
        let right_expr = try_cast(right, schema, rhs)?;
        let b = binary(left_expr, op, right_expr, schema);
        Ok(b?.as_any().downcast_ref::<BinaryExpr>().unwrap().clone())
    }

    // Expected test results were calculated in Wolfram Mathematica, by using
    // *METHOD_NAME*[TransformedDistribution[x op y, {x ~ *DISTRIBUTION_X*[..], y ~ *DISTRIBUTION_Y*[..]}]]
    #[test]
    fn test_unknown_properties_uniform_uniform() -> datafusion_common::Result<()> {
        let stat_a = Uniform {
            interval: Interval::make(Some(0.), Some(12.0))?,
        };

        let stat_b = Uniform {
            interval: Interval::make(Some(12.0), Some(36.0))?,
        };

        let test_data: Vec<(Actual, Expected)> = vec![
            // mean
            (
                compute_mean(&Plus, &stat_a, &stat_b)?,
                Some(Float64(Some(30.))),
            ),
            (
                compute_mean(&Minus, &stat_a, &stat_b)?,
                Some(Float64(Some(-18.))),
            ),
            (
                compute_mean(&Multiply, &stat_a, &stat_b)?,
                Some(Float64(Some(144.))),
            ),
            // median
            (
                compute_median(&Plus, &stat_a, &stat_b)?,
                Some(Float64(Some(30.))),
            ),
            (
                compute_median(&Minus, &stat_a, &stat_b)?,
                Some(Float64(Some(-18.))),
            ),
            // FYI: median of combined distributions for mul, div and mod ops doesn't exist.

            // variance
            (
                compute_variance(&Plus, &stat_a, &stat_b)?,
                Some(Float64(Some(60.))),
            ),
            (
                compute_variance(&Minus, &stat_a, &stat_b)?,
                Some(Float64(Some(60.))),
            ),
            // (compute_variance(&Operator::Multiply, &stat_a, &stat_b), Some(Float64(Some(9216.)))),
        ];
        for (actual, expected) in test_data {
            assert_eq!(actual, expected);
        }

        Ok(())
    }

    /// Test for Uniform-Uniform, Uniform-Unknown, Unknown-Uniform, Unknown-Unknown pairs,
    /// where range is always present.
    #[test]
    fn test_compute_range_where_present() -> datafusion_common::Result<()> {
        let a = &Interval::make(Some(0.), Some(12.0))?;
        let b = &Interval::make(Some(0.), Some(12.0))?;
        let _mean = Some(Float64(Some(6.0)));
        for (stat_a, stat_b) in [
            (
                Uniform {
                    interval: a.clone(),
                },
                Uniform {
                    interval: b.clone(),
                },
            ),
            (
                Unknown {
                    mean: _mean.clone(),
                    median: _mean.clone(),
                    variance: None,
                    range: a.clone(),
                },
                Uniform {
                    interval: b.clone(),
                },
            ),
            (
                Uniform {
                    interval: a.clone(),
                },
                Unknown {
                    mean: _mean.clone(),
                    median: _mean.clone(),
                    variance: None,
                    range: b.clone(),
                },
            ),
            (
                Unknown {
                    mean: _mean.clone(),
                    median: _mean.clone(),
                    variance: None,
                    range: a.clone(),
                },
                Unknown {
                    mean: _mean.clone(),
                    median: _mean.clone(),
                    variance: None,
                    range: b.clone(),
                },
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
    fn test_stats_v2_integration() -> datafusion_common::Result<()> {
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
                Uniform {
                    interval: Interval::make(Some(0), Some(1))?,
                },
            ),
            (
                1usize,
                Uniform {
                    interval: Interval::make(Some(0), Some(2))?,
                },
            ),
            (
                3usize,
                Uniform {
                    interval: Interval::make(Some(1), Some(3))?,
                },
            ),
            (
                4usize,
                Uniform {
                    interval: Interval::make(Some(1), Some(5))?,
                },
            ),
        ]);
        let ev_stats = graph.evaluate()?;
        assert_eq!(
            ev_stats,
            &Bernoulli {
                p: Float64(Some(0.5))
            }
        );

        graph.assign_statistic(
            6,
            Uniform {
                interval: Interval::CERTAINLY_TRUE,
            },
        );
        assert!(graph.propagate()?);
        Ok(())
    }
}

/*
StableGraph { Ty: "Directed", node_count: 7, edge_count: 6, edges: (2, 0), (2, 1), (5, 3), (5, 4), (6, 2), (6, 5), node weights: {
0: ExprStatisticGraphNode { expr: Column { name: "a", index: 0 }, statistics: Unknown { mean: None, median: None, variance: None, range: Interval { lower: Float64(NULL), upper: Float64(NULL) } } },
1: ExprStatisticGraphNode { expr: Column { name: "b", index: 1 }, statistics: Unknown { mean: None, median: None, variance: None, range: Interval { lower: Float64(NULL), upper: Float64(NULL) } } },
2: ExprStatisticGraphNode { expr: BinaryExpr { left: Column { name: "a", index: 0 }, op: Plus, right: Column { name: "b", index: 1 }, fail_on_overflow: false }, statistics: Unknown { mean: None, median: None, variance: None, range: Interval { lower: Float64(NULL), upper: Float64(NULL) } } },
3: ExprStatisticGraphNode { expr: Column { name: "c", index: 2 }, statistics: Unknown { mean: None, median: None, variance: None, range: Interval { lower: Float64(NULL), upper: Float64(NULL) } } },
4: ExprStatisticGraphNode { expr: Column { name: "d", index: 3 }, statistics: Unknown { mean: None, median: None, variance: None, range: Interval { lower: Float64(NULL), upper: Float64(NULL) } } },
5: ExprStatisticGraphNode { expr: BinaryExpr { left: Column { name: "c", index: 2 }, op: Minus, right: Column { name: "d", index: 3 }, fail_on_overflow: false }, statistics: Unknown { mean: None, median: None, variance: None, range: Interval { lower: Float64(NULL), upper: Float64(NULL) } } },
6: ExprStatisticGraphNode { expr: BinaryExpr { left: BinaryExpr { left: Column { name: "a", index: 0 }, op: Plus, right: Column { name: "b", index: 1 }, fail_on_overflow: false }, op: Eq, right: BinaryExpr { left: Column { name: "c", index: 2 }, op: Minus, right: Column { name: "d", index: 3 }, fail_on_overflow: false }, fail_on_overflow: false }, statistics: Unknown { mean: None, median: None, variance: None, range: Interval { lower: Boolean(false), upper: Boolean(true) } } }}, edge weights: {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0}, free_node: NodeIndex(4294967295), free_edge: EdgeIndex(4294967295) }

 */
