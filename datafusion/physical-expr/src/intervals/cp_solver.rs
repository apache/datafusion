use crate::expressions::{BinaryExpr, CastExpr, Literal};
use crate::intervals::interval_aritmetics::{
    apply_operator, negate_ops, propagate_logical_operators, Interval,
};
use crate::utils::{build_physical_expr_graph, ExprTreeNode};
use crate::PhysicalExpr;
use std::fmt::{Display, Formatter};

use datafusion_common::Result;
use datafusion_expr::Operator;
use petgraph::graph::NodeIndex;
use petgraph::visit::{Bfs, DfsPostOrder};
use petgraph::Outgoing;

use petgraph::stable_graph::StableGraph;
use std::ops::{Index, IndexMut};
use std::sync::Arc;

#[derive(Clone, Debug)]
/// Graph nodes contains interval information.
pub struct ExprIntervalGraphNode {
    expr: Arc<dyn PhysicalExpr>,
    interval: Option<Interval>,
}

impl Display for ExprIntervalGraphNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.expr)
    }
}

impl ExprIntervalGraphNode {
    // Construct ExprIntervalGraphNode
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        ExprIntervalGraphNode {
            expr,
            interval: None,
        }
    }
    /// Specify interval
    pub fn with_interval(mut self, interval: Interval) -> Self {
        self.interval = Some(interval);
        self
    }
    /// Get interval
    pub fn interval(&self) -> &Option<Interval> {
        &self.interval
    }

    /// Within this static method, one can customize how PhysicalExpr generator constructs its nodes
    pub fn expr_node_builder(input: Arc<ExprTreeNode>) -> ExprIntervalGraphNode {
        let binding = input.expr();
        let plan_any = binding.as_any();
        if let Some(literal) = plan_any.downcast_ref::<Literal>() {
            // Create interval
            let interval = Interval::Singleton(literal.value().clone());
            ExprIntervalGraphNode::new(input.expr().clone()).with_interval(interval)
        } else {
            ExprIntervalGraphNode::new(input.expr().clone())
        }
    }
}

impl PartialEq for ExprIntervalGraphNode {
    fn eq(&self, other: &ExprIntervalGraphNode) -> bool {
        self.expr.eq(&other.expr)
    }
}

pub struct ExprIntervalGraph(
    NodeIndex,
    StableGraph<ExprIntervalGraphNode, usize>,
    Vec<(Arc<dyn PhysicalExpr>, usize)>,
);

/// This function calculates the current node interval
fn calculate_node_interval(
    parent: &Interval,
    current_child_interval: &Interval,
    negated_op: Operator,
    other_child_interval: &Interval,
) -> Result<Interval> {
    Ok(if current_child_interval.is_singleton() {
        current_child_interval.clone()
    } else {
        apply_operator(parent, &negated_op, other_child_interval)?
            .intersect(current_child_interval)?
    })
}

impl ExprIntervalGraph {
    /// We initiate [ExprIntervalGraph] with [PhysicalExpr] and the ones with we provide internals.
    /// Let's say we have "a@0 + b@1 > 3" expression.
    /// We can provide initial interval information for "a@0" and "b@1". Also, we can provide
    /// information
    pub fn try_new(
        expr: Arc<dyn PhysicalExpr>,
        provided_expr: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Self> {
        let (root_node, mut graph) =
            build_physical_expr_graph(expr, &ExprIntervalGraphNode::expr_node_builder)?;
        let mut bfs = Bfs::new(&graph, root_node);
        let mut will_be_removed = vec![];
        let mut expr_node_indices: Vec<(Arc<dyn PhysicalExpr>, usize)> = provided_expr
            .iter()
            .map(|e| (e.clone(), usize::MAX))
            .collect();
        while let Some(node) = bfs.next(&graph) {
            // Get plan
            let input = graph.index(node);
            let expr = input.expr.clone();
            // If we find a expr, remove the childs since we could not propagate the intervals there.
            if let Some(value) = provided_expr.iter().position(|e| expr.eq(e)) {
                // Remove all child nodes here, since we have information about them. If there is no child,
                // iterator will not have anything.
                let expr_node_index = expr_node_indices.get_mut(value).unwrap();
                *expr_node_index = (expr_node_index.0.clone(), node.index());
                let mut edges = graph.neighbors_directed(node, Outgoing).detach();
                while let Some(n_index) = edges.next_node(&graph) {
                    // We will not delete while traversing
                    will_be_removed.push(n_index);
                }
            }
        }
        for node in will_be_removed {
            graph.remove_node(node);
        }
        Ok(Self(root_node, graph, expr_node_indices))
    }

    pub fn calculate_new_intervals(
        &mut self,
        expr_stats: &mut [(Arc<dyn PhysicalExpr>, Interval)],
    ) -> Result<()> {
        self.post_order_interval_calculation(expr_stats)?;
        self.pre_order_interval_propagation(expr_stats)?;
        Ok(())
    }

    fn post_order_interval_calculation(
        &mut self,
        expr_stats: &[(Arc<dyn PhysicalExpr>, Interval)],
    ) -> Result<()> {
        let mut dfs = DfsPostOrder::new(&self.1, self.0);
        while let Some(node) = dfs.next(&self.1) {
            // Outgoing (Children) edges
            let mut edges = self.1.neighbors_directed(node, Outgoing).detach();
            // Get PhysicalExpr
            let expr = self.1[node].expr.clone();
            // Check if we have a interval information about given PhysicalExpr, if so, directly
            // propagate it to the upper.
            if let Some((_, interval)) =
                expr_stats.iter().find(|(e, _interval)| expr.eq(e))
            {
                let input = self.1.index_mut(node);
                input.interval = Some(interval.clone());
                continue;
            }

            let expr_any = expr.as_any();
            if let Some(binary) = expr_any.downcast_ref::<BinaryExpr>() {
                // Access left child immutable
                let second_child_node_index = edges.next_node(&self.1).unwrap();
                // Access left child immutable
                let first_child_node_index = edges.next_node(&self.1).unwrap();
                // Do not use any reference from graph, MUST clone here.
                let left_interval =
                    match self.1.index(first_child_node_index).interval().as_ref() {
                        Some(v) => v.clone(),
                        None => continue,
                    };
                let right_interval =
                    match self.1.index(second_child_node_index).interval().as_ref() {
                        Some(v) => v.clone(),
                        None => continue,
                    };
                // Since we release the reference, we can get mutable reference.
                let input = self.1.index_mut(node);
                // Calculate and replace the interval
                input.interval = Some(apply_operator(
                    &left_interval,
                    binary.op(),
                    &right_interval,
                )?);
            } else if let Some(CastExpr {
                cast_type,
                cast_options,
                ..
            }) = expr_any.downcast_ref::<CastExpr>()
            {
                // Access the child immutable
                let child_index = edges.next_node(&self.1).unwrap();
                let child = self.1.index(child_index);
                // Cast the interval
                let new_interval = match &child.interval {
                    Some(interval) => interval.cast_to(cast_type, cast_options)?,
                    // If there is no child, continue
                    None => continue,
                };
                // Update the interval
                let input = self.1.index_mut(node);
                input.interval = Some(new_interval);
            }
        }
        Ok(())
    }

    pub fn pre_order_interval_propagation(
        &mut self,
        expr_stats: &mut [(Arc<dyn PhysicalExpr>, Interval)],
    ) -> Result<()> {
        let mut bfs = Bfs::new(&self.1, self.0);
        while let Some(node) = bfs.next(&self.1) {
            // Get plan
            let input = self.1.index(node);
            let expr = input.expr.clone();
            // Get calculated interval. BinaryExpr will propagate the interval according to
            // this.
            let parent_calculated_interval = match input.interval().as_ref() {
                Some(i) => i.clone(),
                None => continue,
            };

            if let Some(index) = expr_stats.iter().position(|(e, _interval)| expr.eq(e)) {
                let col_interval = expr_stats.get_mut(index).unwrap();
                *col_interval = (col_interval.0.clone(), parent_calculated_interval);
                continue;
            }

            // Outgoing (Children) edges
            let mut edges = self.1.neighbors_directed(node, Outgoing).detach();

            let expr_any = expr.as_any();
            if let Some(binary) = expr_any.downcast_ref::<BinaryExpr>() {
                // Get right node.
                let second_child_node_index = edges.next_node(&self.1).unwrap();
                let second_child_interval =
                    match self.1.index(second_child_node_index).interval().as_ref() {
                        Some(v) => v,
                        None => continue,
                    };
                // Get left node.
                let first_child_node_index = edges.next_node(&self.1).unwrap();
                let first_child_interval =
                    match self.1.index(first_child_node_index).interval().as_ref() {
                        Some(v) => v,
                        None => continue,
                    };

                let (shrink_left_interval, shrink_right_interval) =
                    if parent_calculated_interval.is_boolean() {
                        propagate_logical_operators(
                            first_child_interval,
                            binary.op(),
                            second_child_interval,
                        )?
                    } else {
                        let negated_op = negate_ops(*binary.op());
                        let new_right_operator = calculate_node_interval(
                            &parent_calculated_interval,
                            second_child_interval,
                            negated_op,
                            first_child_interval,
                        )?;
                        let new_left_operator = calculate_node_interval(
                            &parent_calculated_interval,
                            first_child_interval,
                            negated_op,
                            &new_right_operator,
                        )?;
                        (new_left_operator, new_right_operator)
                    };
                let mutable_first_child = self.1.index_mut(first_child_node_index);
                mutable_first_child.interval = Some(shrink_left_interval.clone());
                let mutable_second_child = self.1.index_mut(second_child_node_index);
                mutable_second_child.interval = Some(shrink_right_interval.clone())
            } else if let Some(cast) = expr_any.downcast_ref::<CastExpr>() {
                // Calculate new interval
                let child_index = edges.next_node(&self.1).unwrap();
                let child = self.1.index(child_index);
                // Get child's internal datatype. If None, propagate None.
                let cast_type = match child.interval() {
                    Some(interval) => interval.get_datatype(),
                    None => continue,
                };
                let cast_options = cast.cast_options();
                let new_child_interval = match &child.interval {
                    Some(_) => {
                        parent_calculated_interval.cast_to(&cast_type, cast_options)?
                    }
                    None => continue,
                };
                let mutable_child = self.1.index_mut(child_index);
                mutable_child.interval = Some(new_child_interval.clone());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::filter_numeric_expr_generation;
    use itertools::Itertools;

    use crate::expressions::Column;
    use crate::intervals::interval_aritmetics::Range;
    use datafusion_common::ScalarValue;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use rstest::*;

    fn experiment(
        expr: Arc<dyn PhysicalExpr>,
        exprs: (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>),
        left_interval: (Option<i32>, Option<i32>),
        right_interval: (Option<i32>, Option<i32>),
        left_waited: (Option<i32>, Option<i32>),
        right_waited: (Option<i32>, Option<i32>),
    ) -> Result<()> {
        let mut col_stats = vec![
            (
                exprs.0.clone(),
                Interval::Range(Range {
                    lower: ScalarValue::Int32(left_interval.0),
                    upper: ScalarValue::Int32(left_interval.1),
                }),
            ),
            (
                exprs.1.clone(),
                Interval::Range(Range {
                    lower: ScalarValue::Int32(right_interval.0),
                    upper: ScalarValue::Int32(right_interval.1),
                }),
            ),
        ];
        let expected = vec![
            (
                exprs.0.clone(),
                Interval::Range(Range {
                    lower: ScalarValue::Int32(left_waited.0),
                    upper: ScalarValue::Int32(left_waited.1),
                }),
            ),
            (
                exprs.1.clone(),
                Interval::Range(Range {
                    lower: ScalarValue::Int32(right_waited.0),
                    upper: ScalarValue::Int32(right_waited.1),
                }),
            ),
        ];
        let mut graph = ExprIntervalGraph::try_new(
            expr,
            &col_stats.iter().map(|(e, _)| e.clone()).collect_vec(),
        )?;
        graph.calculate_new_intervals(&mut col_stats)?;
        col_stats
            .iter()
            .zip(expected.iter())
            .for_each(|((_, res), (_, expected))| assert_eq!(res, expected));
        Ok(())
    }

    fn generate_case<const ASC: bool>(
        expr: Arc<dyn PhysicalExpr>,
        left_col: Arc<dyn PhysicalExpr>,
        right_col: Arc<dyn PhysicalExpr>,
        seed: u64,
        expr_left: i32,
        expr_right: i32,
    ) -> Result<()> {
        let mut r = StdRng::seed_from_u64(seed);

        let (left_interval, right_interval, left_waited, right_waited) = if ASC {
            let left = (Some(r.gen_range(0..1000)), None);
            let right = (Some(r.gen_range(0..1000)), None);
            (
                left,
                right,
                (
                    Some(std::cmp::max(left.0.unwrap(), right.0.unwrap() + expr_left)),
                    None,
                ),
                (
                    Some(std::cmp::max(
                        right.0.unwrap(),
                        left.0.unwrap() + expr_right,
                    )),
                    None,
                ),
            )
        } else {
            let left = (None, Some(r.gen_range(0..1000)));
            let right = (None, Some(r.gen_range(0..1000)));
            (
                left,
                right,
                (
                    None,
                    Some(std::cmp::min(left.1.unwrap(), right.1.unwrap() + expr_left)),
                ),
                (
                    None,
                    Some(std::cmp::min(
                        right.1.unwrap(),
                        left.1.unwrap() + expr_right,
                    )),
                ),
            )
        };
        experiment(
            expr,
            (left_col, right_col),
            left_interval,
            right_interval,
            left_waited,
            right_waited,
        )?;
        Ok(())
    }
    #[rstest]
    #[test]
    fn case_1(
        #[values(0, 1, 2, 3, 4, 12, 32, 314, 3124, 123, 123, 4123)] seed: u64,
    ) -> Result<()> {
        let left_col = Arc::new(Column::new("left_watermark", 0));
        let right_col = Arc::new(Column::new("right_watermark", 0));
        // left_watermark + 1 > right_watermark + 11 AND left_watermark + 3 < right_watermark + 33
        let expr = filter_numeric_expr_generation(
            left_col.clone(),
            right_col.clone(),
            Operator::Plus,
            Operator::Plus,
            Operator::Plus,
            Operator::Plus,
            1,
            11,
            3,
            33,
        );
        // l > r + 10 AND r > l - 30
        let l_gt_r = 10;
        let r_gt_l = -30;
        generate_case::<true>(
            expr.clone(),
            left_col.clone(),
            right_col.clone(),
            seed,
            l_gt_r,
            r_gt_l,
        )?;
        // Descending tests
        // r < l - 10 AND l < r + 30
        let r_lt_l = -l_gt_r;
        let l_lt_r = -r_gt_l;
        generate_case::<false>(expr, left_col, right_col, seed, l_lt_r, r_lt_l)?;

        Ok(())
    }
    #[rstest]
    #[test]
    fn case_2(
        #[values(0, 1, 2, 3, 4, 12, 32, 314, 3124, 123, 123, 4123)] seed: u64,
    ) -> Result<()> {
        let left_col = Arc::new(Column::new("left_watermark", 0));
        let right_col = Arc::new(Column::new("right_watermark", 0));
        // left_watermark - 1 > right_watermark + 5 AND left_watermark + 3 < right_watermark + 10
        let expr = filter_numeric_expr_generation(
            left_col.clone(),
            right_col.clone(),
            Operator::Minus,
            Operator::Plus,
            Operator::Plus,
            Operator::Plus,
            1,
            5,
            3,
            10,
        );
        // l > r + 6 AND r > l - 7
        let l_gt_r = 6;
        let r_gt_l = -7;
        generate_case::<true>(
            expr.clone(),
            left_col.clone(),
            right_col.clone(),
            seed,
            l_gt_r,
            r_gt_l,
        )?;
        // Descending tests
        // r < l - 6 AND l < r + 7
        let r_lt_l = -l_gt_r;
        let l_lt_r = -r_gt_l;
        generate_case::<false>(expr, left_col, right_col, seed, l_lt_r, r_lt_l)?;

        Ok(())
    }

    #[rstest]
    #[test]
    fn case_3(
        #[values(0, 1, 2, 3, 4, 12, 32, 314, 3124, 123, 123, 4123)] seed: u64,
    ) -> Result<()> {
        let left_col = Arc::new(Column::new("left_watermark", 0));
        let right_col = Arc::new(Column::new("right_watermark", 0));
        // left_watermark - 1 > right_watermark + 5 AND left_watermark - 3 < right_watermark + 10
        let expr = filter_numeric_expr_generation(
            left_col.clone(),
            right_col.clone(),
            Operator::Minus,
            Operator::Plus,
            Operator::Minus,
            Operator::Plus,
            1,
            5,
            3,
            10,
        );
        // l > r + 6 AND r > l - 13
        let l_gt_r = 6;
        let r_gt_l = -13;
        generate_case::<true>(
            expr.clone(),
            left_col.clone(),
            right_col.clone(),
            seed,
            l_gt_r,
            r_gt_l,
        )?;
        // Descending tests
        // r < l - 6 AND l < r + 13
        let r_lt_l = -l_gt_r;
        let l_lt_r = -r_gt_l;
        generate_case::<false>(expr, left_col, right_col, seed, l_lt_r, r_lt_l)?;

        Ok(())
    }
    #[rstest]
    #[test]
    fn case_4(
        #[values(0, 1, 2, 3, 4, 12, 32, 314, 3124, 123, 123, 4123)] seed: u64,
    ) -> Result<()> {
        let left_col = Arc::new(Column::new("left_watermark", 0));
        let right_col = Arc::new(Column::new("right_watermark", 0));
        // left_watermark - 10 > right_watermark - 5 AND left_watermark - 3 < right_watermark + 10
        let expr = filter_numeric_expr_generation(
            left_col.clone(),
            right_col.clone(),
            Operator::Minus,
            Operator::Minus,
            Operator::Minus,
            Operator::Plus,
            10,
            5,
            3,
            10,
        );
        // l > r + 5 AND r > l - 13
        let l_gt_r = 5;
        let r_gt_l = -13;
        generate_case::<true>(
            expr.clone(),
            left_col.clone(),
            right_col.clone(),
            seed,
            l_gt_r,
            r_gt_l,
        )?;
        // Descending tests
        // r < l - 5 AND l < r + 13
        let r_lt_l = -l_gt_r;
        let l_lt_r = -r_gt_l;
        generate_case::<false>(expr, left_col, right_col, seed, l_lt_r, r_lt_l)?;
        Ok(())
    }

    #[rstest]
    #[test]
    fn case_5(
        #[values(0, 1, 2, 3, 4, 12, 32, 314, 3124, 123, 123, 4123)] seed: u64,
    ) -> Result<()> {
        let left_col = Arc::new(Column::new("left_watermark", 0));
        let right_col = Arc::new(Column::new("right_watermark", 0));
        // left_watermark - 10 > right_watermark - 5 AND left_watermark - 30 < right_watermark - 3

        let expr = filter_numeric_expr_generation(
            left_col.clone(),
            right_col.clone(),
            Operator::Minus,
            Operator::Minus,
            Operator::Minus,
            Operator::Minus,
            10,
            5,
            30,
            3,
        );
        // l > r + 5 AND r > l - 27
        let l_gt_r = 5;
        let r_gt_l = -27;
        generate_case::<true>(
            expr.clone(),
            left_col.clone(),
            right_col.clone(),
            seed,
            l_gt_r,
            r_gt_l,
        )?;
        // Descending tests
        // r < l - 5 AND l < r + 27
        let r_lt_l = -l_gt_r;
        let l_lt_r = -r_gt_l;
        generate_case::<false>(expr, left_col, right_col, seed, l_lt_r, r_lt_l)?;
        Ok(())
    }
}
