use crate::physical_optimizer::sort_enforcement::ExecTree;
use crate::physical_optimizer::utils::is_repartition;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::union::{can_interleave, InterleaveExec, UnionExec};
use crate::physical_plan::{
    with_new_children_if_necessary, Distribution, ExecutionPlan, Partitioning,
};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, VisitRecursion};
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::PhysicalExpr;
use itertools::izip;
use std::sync::Arc;
use crate::physical_plan::projection::ProjectionExec;

#[derive(Default)]
pub struct EnforceDistributionV2 {}

impl EnforceDistributionV2 {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for EnforceDistributionV2 {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let target_partitions = config.execution.target_partitions;
        let repartition_context = RepartitionContext::new(plan);
        // let res = repartition_context.transform_up(&|plan_with_pipeline_fixer| {ensure_distribution(repartition_context, target_partitions)})?;

        // Distribution enforcement needs to be applied bottom-up.
        let updated_plan = repartition_context.transform_up(&|repartition_context| {
            ensure_distribution(repartition_context, target_partitions)
        })?;

        Ok(updated_plan.plan)
    }

    fn name(&self) -> &str {
        "EnforceDistributionV2"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn are_exprs_equal(lhs: &[Arc<dyn PhysicalExpr>], rhs: &[Arc<dyn PhysicalExpr>]) -> bool {
    if lhs.len() != rhs.len() {
        false
    } else {
        izip!(lhs.iter(), rhs.iter()).all(|(lhs, rhs)| lhs.eq(rhs))
    }
}

fn add_roundrobin_on_top(
    input: Arc<dyn ExecutionPlan>,
    n_target: usize,
    repartition_onward: &mut Option<ExecTree>,
) -> Result<(Arc<dyn ExecutionPlan>, bool)> {
    Ok(
        if input.output_partitioning().partition_count() < n_target {
            let new_plan = Arc::new(RepartitionExec::try_new(
                input,
                Partitioning::RoundRobinBatch(n_target),
            )?) as Arc<dyn ExecutionPlan>;
            if let Some(exec_tree) = repartition_onward {
                panic!(
                    "exectree should have been empty, exec tree:{:?} ",
                    exec_tree
                );
            }
            // Initialize new onward
            *repartition_onward = Some(ExecTree::new(new_plan.clone(), 0, vec![]));
            (new_plan, true)
        } else {
            (input, false)
        },
    )
}

fn add_hash_on_top(
    input: Arc<dyn ExecutionPlan>,
    hash_exprs: Vec<Arc<dyn PhysicalExpr>>,
    n_target: usize,
    repartition_onward: &mut Option<ExecTree>,
) -> Result<(Arc<dyn ExecutionPlan>, bool)> {
    if n_target == 1 {
        return Ok((input, false));
    }
    if let Partitioning::Hash(exprs, n_partition) = input.output_partitioning() {
        if are_exprs_equal(&exprs, &hash_exprs) && n_partition == n_target {
            return Ok((input, false));
        }
    }
    let new_plan = Arc::new(RepartitionExec::try_new(
        input,
        Partitioning::Hash(hash_exprs, n_target),
    )?) as Arc<dyn ExecutionPlan>;
    if let Some(exec_tree) = repartition_onward {
        *exec_tree = ExecTree::new(new_plan.clone(), 0, vec![exec_tree.clone()]);
    } else {
        // Initialize new onward
        *repartition_onward = Some(ExecTree::new(new_plan.clone(), 0, vec![]));
    }
    Ok((new_plan, true))
}

fn update_repartition_from_context(
    repartition_context: &RepartitionContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut new_children = repartition_context.plan.children();
    let benefits_from_partitioning = repartition_context.plan.benefits_from_input_partitioning();
    for (child, repartition_onwards, benefits) in izip!(
        new_children.iter_mut(),
        repartition_context.repartition_onwards.iter(),
        benefits_from_partitioning.into_iter()
    ) {
        if !benefits {
            if let Some(exec_tree) = repartition_onwards {
                *child = remove_parallelization(exec_tree)?;
            }
        }
    }
    repartition_context
        .plan
        .clone()
        .with_new_children(new_children)
}

fn remove_parallelization(exec_tree: &ExecTree) -> Result<Arc<dyn ExecutionPlan>> {
    let mut updated_children = exec_tree.plan.children();
    for child in &exec_tree.children {
        let child_idx = child.idx;
        let new_child = remove_parallelization(&child)?;
        updated_children[child_idx] = new_child;
    }
    if let Some(repartition) = exec_tree.plan.as_any().downcast_ref::<RepartitionExec>() {
        // Leaf node
        if let Partitioning::RoundRobinBatch(_n_target) = repartition.partitioning() {
            return Ok(repartition.input().clone());
        }
    }
    exec_tree.plan.clone().with_new_children(updated_children)
}

fn print_plan(plan: &Arc<dyn ExecutionPlan>) -> () {
    let formatted = crate::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    println!("{:#?}", actual);
}

fn ensure_distribution(
    repartition_context: RepartitionContext,
    target_partitions: usize,
) -> Result<Transformed<RepartitionContext>> {
    // let plan = &repartition_context.plan;
    // let mut updated_repartition_onwards = repartition_context.repartition_onwards.clone();
    if repartition_context.plan.children().is_empty() {
        return Ok(Transformed::No(repartition_context));
    }
    let would_benefit = repartition_context.plan.benefits_from_input_partitioning().iter().any(|item|*item);
    let mut is_updated = false;
    // println!("start");
    // print_plan(&repartition_context.plan);
    // println!("WOULD BENEFIT:{:?}", repartition_context.plan.benefits_from_input_partitioning());
    let (plan, mut updated_repartition_onwards) = if !would_benefit && !repartition_context.plan.as_any().is::<ProjectionExec>() {
        // println!("start");
        // print_plan(&repartition_context.plan);
        let new_plan = update_repartition_from_context(&repartition_context)?;
        // print_plan(&new_plan);
        // println!("end");
        let n_child = new_plan.children().len();
        is_updated = true;
        (new_plan, vec![None; n_child])
    } else {
        (
            repartition_context.plan.clone(),
            repartition_context.repartition_onwards.clone(),
        )
    };
    let new_children_with_flags = izip!(
        plan.children().iter(),
        plan.required_input_distribution().iter(),
        plan.required_input_ordering().iter(),
        plan.maintains_input_order().iter(),
        updated_repartition_onwards.iter_mut()
    )
    .map(
        |(child, requirement, required_input_ordering, maintains, repartition_onward)| {
            let mut new_child = child.clone();
            let mut is_changed = false;

            // We can reorder a child if:
            //   - It has no ordering to preserve, or
            //   - Its parent has no required input ordering and does not
            //     maintain input ordering.
            // Check if this condition holds:
            let can_reorder = child.output_ordering().is_none()
                || (!required_input_ordering.is_some() && !maintains);

            if would_benefit && can_reorder {
                (new_child, is_changed) = add_roundrobin_on_top(
                    new_child,
                    target_partitions,
                    repartition_onward,
                )?;
            }

            match requirement {
                Distribution::SinglePartition => {
                    if child.output_partitioning().partition_count() > 1 {
                        new_child = Arc::new(CoalescePartitionsExec::new(new_child));
                        is_changed = true;
                    }
                }
                Distribution::HashPartitioned(exprs) => {
                    (new_child, is_changed) = add_hash_on_top(
                        new_child,
                        exprs.to_vec(),
                        target_partitions,
                        repartition_onward,
                    )?;
                }
                Distribution::UnspecifiedDistribution => {}
            };
            Ok((new_child, is_changed))
        },
    )
    .collect::<Result<Vec<_>>>()?;
    let (new_children, changed_flags): (Vec<_>, Vec<_>) =
        new_children_with_flags.into_iter().unzip();

    // special case for UnionExec: We want to "bubble up" hash-partitioned data. So instead of:
    //
    // Agg:
    //   Repartition (hash):
    //     Union:
    //       - Agg:
    //           Repartition (hash):
    //             Data
    //       - Agg:
    //           Repartition (hash):
    //             Data
    //
    // We can use:
    //
    // Agg:
    //   Interleave:
    //     - Agg:
    //         Repartition (hash):
    //           Data
    //     - Agg:
    //         Repartition (hash):
    //           Data
    if plan.as_any().is::<UnionExec>() {
        if can_interleave(&new_children) {
            let plan = Arc::new(InterleaveExec::try_new(new_children)?) as _;
            let new_repartition_context = RepartitionContext {
                plan,
                repartition_onwards: updated_repartition_onwards,
            };
            return Ok(Transformed::Yes(new_repartition_context))
        }
    }

    if is_updated || changed_flags.iter().any(|item| *item) {
        let new_plan = plan.clone().with_new_children(new_children)?;
        let new_repartition_context = RepartitionContext {
            plan: new_plan,
            repartition_onwards: updated_repartition_onwards,
        };
        Ok(Transformed::Yes(new_repartition_context))
    } else {
        Ok(Transformed::No(repartition_context))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RepartitionContext {
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    repartition_onwards: Vec<Option<ExecTree>>,
}

impl RepartitionContext {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let length = plan.children().len();
        RepartitionContext {
            plan,
            repartition_onwards: vec![None; length],
        }
    }

    pub fn new_from_children_nodes(
        children_nodes: Vec<RepartitionContext>,
        parent_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let children_plans = children_nodes
            .iter()
            .map(|item| item.plan.clone())
            .collect();
        let repartition_onwards = children_nodes
            .into_iter()
            .enumerate()
            .map(|(idx, item)| {
                // `ordering_onwards` tree keeps track of executors that maintain
                // ordering, (or that can maintain ordering with the replacement of
                // its variant)
                let plan = item.plan;
                let repartition_onwards = item.repartition_onwards;
                if plan.children().is_empty() {
                    // Plan has no children, there is nothing to propagate.
                    None
                } else if is_repartition(&plan) && repartition_onwards[0].is_none() {
                    Some(ExecTree::new(plan, idx, vec![]))
                } else {
                    let mut new_repartition_onwards = vec![];
                    for (required_dist, repartition_onwards, would_benefit) in izip!(
                        plan.required_input_distribution().iter(),
                        repartition_onwards.into_iter(),
                        plan.benefits_from_input_partitioning().into_iter()
                    ) {
                        if let Some(repartition_onwards) = repartition_onwards {
                            if let Distribution::UnspecifiedDistribution = required_dist {
                                if would_benefit {
                                    new_repartition_onwards.push(repartition_onwards);
                                }
                            }
                        }
                    }
                    if new_repartition_onwards.is_empty() {
                        None
                    } else {
                        Some(ExecTree::new(plan, idx, new_repartition_onwards))
                    }
                }
            })
            .collect();
        let plan = with_new_children_if_necessary(parent_plan, children_plans)?.into();
        Ok(RepartitionContext {
            plan,
            repartition_onwards,
        })
    }

    /// Computes order-preservation contexts for every child of the plan.
    pub fn children(&self) -> Vec<RepartitionContext> {
        self.plan
            .children()
            .into_iter()
            .map(|child| RepartitionContext::new(child))
            .collect()
    }
}

impl TreeNode for RepartitionContext {
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

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if children.is_empty() {
            Ok(self)
        } else {
            let children_nodes = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<_>>>()?;
            RepartitionContext::new_from_children_nodes(children_nodes, self.plan)
        }
    }
}
