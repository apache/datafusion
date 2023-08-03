use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::{Distribution, ExecutionPlan, Partitioning};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::PhysicalExpr;
use itertools::izip;
use std::sync::Arc;

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
        // Distribution enforcement needs to be applied bottom-up.
        plan.transform_up(&|plan| ensure_distribution(plan, target_partitions))
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
) -> Result<(Arc<dyn ExecutionPlan>, bool)> {
    Ok(
        if input.output_partitioning().partition_count() < n_target {
            (Arc::new(RepartitionExec::try_new(
                input,
                Partitioning::RoundRobinBatch(n_target),
            )?), true)
        } else {
            (input, false)
        },
    )
}

fn add_hash_on_top(
    input: Arc<dyn ExecutionPlan>,
    hash_exprs: Vec<Arc<dyn PhysicalExpr>>,
    n_target: usize,
) -> Result<(Arc<dyn ExecutionPlan>, bool)> {
    if n_target == 1 {
        return Ok((input, false));
    }
    if let Partitioning::Hash(exprs, n_partition) = input.output_partitioning() {
        if are_exprs_equal(&exprs, &hash_exprs) && n_partition == n_target {
            return Ok((input, false));
        }
    }
    Ok((Arc::new(RepartitionExec::try_new(
        input,
        Partitioning::Hash(hash_exprs, n_target),
    )?), true))
}

fn ensure_distribution(
    plan: Arc<dyn ExecutionPlan>,
    target_partitions: usize,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if plan.children().is_empty() {
        return Ok(Transformed::No(plan));
    }
    let would_benefit = plan.benefits_from_input_partitioning();
    let new_children_with_flags = izip!(
        plan.children().iter(),
        plan.required_input_distribution().iter(),
        plan.required_input_ordering().iter(),
        plan.maintains_input_order().iter(),
    )
    .map(|(child, requirement, required_input_ordering, maintains)| {
        let mut new_child = child.clone();
        let mut is_changed = false;

        // We can reorder a child if:
        //   - It has no ordering to preserve, or
        //   - Its parent has no required input ordering and does not
        //     maintain input ordering.
        // Check if this condition holds:
        let can_reorder =
            child.output_ordering().is_none() || (!required_input_ordering.is_some() && !maintains);

        if would_benefit && can_reorder {
            (new_child, is_changed) = add_roundrobin_on_top(new_child, target_partitions)?;
        }
        match requirement {
            Distribution::SinglePartition => {
                if child.output_partitioning().partition_count() > 1 {
                    new_child = Arc::new(CoalescePartitionsExec::new(new_child));
                    is_changed = true;
                }
            }
            Distribution::HashPartitioned(exprs) => {
                (new_child, is_changed) = add_hash_on_top(new_child, exprs.to_vec(), target_partitions)?;
            }
            Distribution::UnspecifiedDistribution => {},
        };
        Ok((new_child, is_changed))
    })
    .collect::<Result<Vec<_>>>()?;
    let (new_children, changed_flags): (Vec<_>, Vec<_>) = new_children_with_flags.into_iter().unzip();
    if changed_flags.iter().any(|item| *item) {
        Ok(Transformed::Yes(plan.with_new_children(new_children)?))
    }else{
        Ok(Transformed::No(plan))
    }

}
