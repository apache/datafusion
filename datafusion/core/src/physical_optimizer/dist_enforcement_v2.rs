use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::{Distribution, ExecutionPlan, Partitioning};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, Result};
use itertools::izip;
use std::sync::Arc;
use datafusion_physical_expr::PhysicalExpr;

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

fn are_exprs_equal(lhs: &[Arc<dyn PhysicalExpr>], rhs: &[Arc<dyn PhysicalExpr>]) -> bool{
    if lhs.len()!=rhs.len(){
        false
    } else{
        izip!(lhs.iter(), rhs.iter()).all(|(lhs, rhs)| lhs.eq(rhs))
    }
}

fn add_roundrobin_on_top(input: Arc<dyn ExecutionPlan>, n_target: usize) -> Result<Arc<dyn ExecutionPlan>>{
    Ok(if input.output_partitioning().partition_count() < n_target {
        Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(n_target))?)
    } else{
        input
    })
}

fn add_hash_on_top(input: Arc<dyn ExecutionPlan>, hash_exprs: Vec<Arc<dyn PhysicalExpr>>, n_target: usize) -> Result<Arc<dyn ExecutionPlan>>{
    if n_target == 1{
        return Ok(input);
    }
    if let Partitioning::Hash(exprs, n_partition) = input.output_partitioning() {
        if are_exprs_equal(&exprs, &hash_exprs) && n_partition == n_target{
            return Ok(input);
        }
    }
    Ok(Arc::new(RepartitionExec::try_new(input, Partitioning::Hash(hash_exprs, n_target))?))
}

fn ensure_distribution(
    plan: Arc<dyn ExecutionPlan>,
    target_partitions: usize,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if plan.children().is_empty() {
        return Ok(Transformed::No(plan));
    }
    let would_benefit = plan.benefits_from_input_partitioning();
    let new_children = izip!(
        plan.children().iter(),
        plan.required_input_distribution().iter()
    )
    .map(|(child, requirement)| {
        let mut new_child = child.clone();
        if would_benefit {
            new_child = add_roundrobin_on_top(new_child, target_partitions)?;
        }
        let new_child = match requirement {
            Distribution::SinglePartition => {
                if child.output_partitioning().partition_count() > 1 {
                    Arc::new(CoalescePartitionsExec::new(new_child))
                } else {
                    new_child
                }
            }
            Distribution::HashPartitioned(exprs) => {
                add_hash_on_top(new_child, exprs.to_vec(), target_partitions)?
            },
            Distribution::UnspecifiedDistribution => new_child,
        };

        Ok(new_child)
    })
    .collect::<Result<Vec<_>>>()?;
    Ok(Transformed::Yes(plan.with_new_children(new_children)?))
}
