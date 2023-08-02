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

fn ensure_distribution(
    plan: Arc<dyn ExecutionPlan>,
    target_partitions: usize,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if plan.children().is_empty() {
        return Ok(Transformed::No(plan));
    }
    let new_children = izip!(
        plan.children().iter(),
        plan.required_input_distribution().iter()
    )
    .map(|(child, requirement)| {
        let new_child = match requirement {
            Distribution::SinglePartition => {
                if child.output_partitioning().partition_count() > 1 {
                    Arc::new(CoalescePartitionsExec::new(child.clone()))
                } else {
                    child.clone()
                }
            }
            Distribution::HashPartitioned(exprs) => match child.output_partitioning() {
                Partitioning::RoundRobinBatch(_) => Arc::new(RepartitionExec::try_new(
                    child.clone(),
                    Partitioning::Hash(exprs.to_vec(), target_partitions),
                )?),
                Partitioning::Hash(out_exprs, n_partition) => {
                    if are_exprs_equal(&out_exprs, &exprs)
                        && n_partition == target_partitions
                    {
                        child.clone()
                    } else {
                        Arc::new(RepartitionExec::try_new(
                            child.clone(),
                            Partitioning::Hash(exprs.to_vec(), target_partitions),
                        )?)
                    }
                }
                Partitioning::UnknownPartitioning(_n_partition) => {
                    Arc::new(RepartitionExec::try_new(
                        child.clone(),
                        Partitioning::Hash(exprs.to_vec(), target_partitions),
                    )?)
                }
            },
            Distribution::UnspecifiedDistribution => child.clone(),
        };

        Ok(new_child)
    })
    .collect::<Result<Vec<_>>>()?;
    Ok(Transformed::Yes(plan.with_new_children(new_children)?))
}
