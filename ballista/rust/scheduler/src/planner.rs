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

//! Distributed query execution
//!
//! This code is EXPERIMENTAL and still under development

use std::collections::HashMap;
use std::sync::Arc;

use ballista_core::datasource::DfTableAdapter;
use ballista_core::error::{BallistaError, Result};
use ballista_core::{
    execution_plans::{ShuffleReaderExec, ShuffleWriterExec, UnresolvedShuffleExec},
    serde::scheduler::PartitionLocation,
};
use datafusion::execution::context::ExecutionContext;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::windows::WindowAggExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use log::info;

type PartialQueryStageResult = (Arc<dyn ExecutionPlan>, Vec<Arc<ShuffleWriterExec>>);

pub struct DistributedPlanner {
    next_stage_id: usize,
}

impl DistributedPlanner {
    pub fn new() -> Self {
        Self { next_stage_id: 0 }
    }
}

impl Default for DistributedPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl DistributedPlanner {
    /// Returns a vector of ExecutionPlans, where the root node is a [ShuffleWriterExec].
    /// Plans that depend on the input of other plans will have leaf nodes of type [UnresolvedShuffleExec].
    /// A [ShuffleWriterExec] is created whenever the partitioning changes.
    ///
    /// Returns an empty vector if the execution_plan doesn't need to be sliced into several stages.
    pub fn plan_query_stages(
        &mut self,
        job_id: &str,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<Arc<ShuffleWriterExec>>> {
        info!("planning query stages");
        let (new_plan, mut stages) =
            self.plan_query_stages_internal(job_id, execution_plan)?;
        stages.push(create_shuffle_writer(
            job_id,
            self.next_stage_id(),
            new_plan,
            None,
        )?);
        Ok(stages)
    }

    /// Returns a potentially modified version of the input execution_plan along with the resulting query stages.
    /// This function is needed because the input execution_plan might need to be modified, but it might not hold a
    /// complete query stage (its parent might also belong to the same stage)
    fn plan_query_stages_internal(
        &mut self,
        job_id: &str,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<PartialQueryStageResult> {
        // recurse down and replace children
        if execution_plan.children().is_empty() {
            return Ok((execution_plan, vec![]));
        }

        let mut stages = vec![];
        let mut children = vec![];
        for child in execution_plan.children() {
            let (new_child, mut child_stages) =
                self.plan_query_stages_internal(job_id, child.clone())?;
            children.push(new_child);
            stages.append(&mut child_stages);
        }

        if let Some(adapter) = execution_plan.as_any().downcast_ref::<DfTableAdapter>() {
            let ctx = ExecutionContext::new();
            Ok((ctx.create_physical_plan(&adapter.logical_plan)?, stages))
        } else if let Some(coalesce) = execution_plan
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
        {
            let query_stage = create_shuffle_writer(
                job_id,
                self.next_stage_id(),
                coalesce.children()[0].clone(),
                None,
            )?;
            let unresolved_shuffle = Arc::new(UnresolvedShuffleExec::new(
                vec![query_stage.stage_id()],
                query_stage.schema(),
                query_stage.output_partitioning().partition_count(),
            ));
            stages.push(query_stage);
            Ok((
                coalesce.with_new_children(vec![unresolved_shuffle])?,
                stages,
            ))
        } else if let Some(repart) =
            execution_plan.as_any().downcast_ref::<RepartitionExec>()
        {
            let query_stage = create_shuffle_writer(
                job_id,
                self.next_stage_id(),
                repart.children()[0].clone(),
                Some(repart.partitioning().to_owned()),
            )?;
            let unresolved_shuffle = Arc::new(UnresolvedShuffleExec::new(
                vec![query_stage.stage_id()],
                query_stage.schema(),
                query_stage.output_partitioning().partition_count(),
            ));
            stages.push(query_stage);
            Ok((unresolved_shuffle, stages))
        } else if let Some(window) =
            execution_plan.as_any().downcast_ref::<WindowAggExec>()
        {
            Err(BallistaError::NotImplemented(format!(
                "WindowAggExec with window {:?}",
                window
            )))
        } else {
            Ok((execution_plan.with_new_children(children)?, stages))
        }
    }

    /// Generate a new stage ID
    fn next_stage_id(&mut self) -> usize {
        self.next_stage_id += 1;
        self.next_stage_id
    }
}

pub fn remove_unresolved_shuffles(
    stage: &dyn ExecutionPlan,
    partition_locations: &HashMap<usize, Vec<Vec<PartitionLocation>>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut new_children: Vec<Arc<dyn ExecutionPlan>> = vec![];
    for child in stage.children() {
        if let Some(unresolved_shuffle) =
            child.as_any().downcast_ref::<UnresolvedShuffleExec>()
        {
            let mut relevant_locations = vec![];
            for id in &unresolved_shuffle.query_stage_ids {
                relevant_locations.append(
                    &mut partition_locations
                        .get(id)
                        .ok_or_else(|| {
                            BallistaError::General(
                                "Missing partition location. Could not remove unresolved shuffles"
                                    .to_owned(),
                            )
                        })?
                        .clone(),
                );
            }
            new_children.push(Arc::new(ShuffleReaderExec::try_new(
                relevant_locations,
                unresolved_shuffle.schema().clone(),
            )?))
        } else {
            new_children.push(remove_unresolved_shuffles(
                child.as_ref(),
                partition_locations,
            )?);
        }
    }
    Ok(stage.with_new_children(new_children)?)
}

fn create_shuffle_writer(
    job_id: &str,
    stage_id: usize,
    plan: Arc<dyn ExecutionPlan>,
    partitioning: Option<Partitioning>,
) -> Result<Arc<ShuffleWriterExec>> {
    Ok(Arc::new(ShuffleWriterExec::try_new(
        job_id.to_owned(),
        stage_id,
        plan,
        "".to_owned(), // executor will decide on the work_dir path
        partitioning,
    )?))
}

#[cfg(test)]
mod test {
    use crate::planner::DistributedPlanner;
    use crate::test_utils::datafusion_test_context;
    use ballista_core::error::BallistaError;
    use ballista_core::execution_plans::UnresolvedShuffleExec;
    use ballista_core::serde::protobuf;
    use datafusion::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
    use datafusion::physical_plan::sort::SortExec;
    use datafusion::physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, projection::ProjectionExec,
    };
    use datafusion::physical_plan::{displayable, ExecutionPlan};
    use std::convert::TryInto;
    use std::sync::Arc;
    use uuid::Uuid;

    macro_rules! downcast_exec {
        ($exec: expr, $ty: ty) => {
            $exec.as_any().downcast_ref::<$ty>().unwrap()
        };
    }

    #[test]
    fn distributed_hash_aggregate_plan() -> Result<(), BallistaError> {
        let mut ctx = datafusion_test_context("testdata")?;

        // simplified form of TPC-H query 1
        let df = ctx.sql(
            "select l_returnflag, sum(l_extendedprice * 1) as sum_disc_price
            from lineitem
            group by l_returnflag
            order by l_returnflag",
        )?;

        let plan = df.to_logical_plan();
        let plan = ctx.optimize(&plan)?;
        let plan = ctx.create_physical_plan(&plan)?;

        let mut planner = DistributedPlanner::new();
        let job_uuid = Uuid::new_v4();
        let stages = planner.plan_query_stages(&job_uuid.to_string(), plan)?;
        for stage in &stages {
            println!("{}", displayable(stage.as_ref()).indent().to_string());
        }

        /* Expected result:

        ShuffleWriterExec: Some(Hash([Column { name: "l_returnflag", index: 0 }], 2))
          HashAggregateExec: mode=Partial, gby=[l_returnflag@1 as l_returnflag], aggr=[SUM(l_extendedprice Multiply Int64(1))]
            CsvExec: source=Path(testdata/lineitem: [testdata/lineitem/partition0.tbl,testdata/lineitem/partition1.tbl]), has_header=false

        ShuffleWriterExec: None
          ProjectionExec: expr=[l_returnflag@0 as l_returnflag, SUM(lineitem.l_extendedprice Multiply Int64(1))@1 as sum_disc_price]
            HashAggregateExec: mode=FinalPartitioned, gby=[l_returnflag@0 as l_returnflag], aggr=[SUM(l_extendedprice Multiply Int64(1))]
              CoalesceBatchesExec: target_batch_size=4096
                RepartitionExec: partitioning=Hash([Column { name: "l_returnflag", index: 0 }], 2)
                  HashAggregateExec: mode=Partial, gby=[l_returnflag@1 as l_returnflag], aggr=[SUM(l_extendedprice Multiply Int64(1))]
                    CsvExec: source=Path(testdata/lineitem: [testdata/lineitem/partition0.tbl,testdata/lineitem/partition1.tbl]), has_header=false

        ShuffleWriterExec: None
          SortExec: [l_returnflag@0 ASC]
            CoalescePartitionsExec
              UnresolvedShuffleExec
        */

        assert_eq!(3, stages.len());

        // verify stage 0
        let stage0 = stages[0].children()[0].clone();
        let partial_hash = downcast_exec!(stage0, HashAggregateExec);
        assert!(*partial_hash.mode() == AggregateMode::Partial);

        // verify stage 1
        let stage1 = stages[1].children()[0].clone();
        let projection = downcast_exec!(stage1, ProjectionExec);
        let final_hash = projection.children()[0].clone();
        let final_hash = downcast_exec!(final_hash, HashAggregateExec);
        assert!(*final_hash.mode() == AggregateMode::FinalPartitioned);

        // verify stage 2
        let stage2 = stages[2].children()[0].clone();
        let sort = downcast_exec!(stage2, SortExec);
        let coalesce_partitions = sort.children()[0].clone();
        let coalesce_partitions =
            downcast_exec!(coalesce_partitions, CoalescePartitionsExec);
        let unresolved_shuffle = coalesce_partitions.children()[0].clone();
        let unresolved_shuffle =
            downcast_exec!(unresolved_shuffle, UnresolvedShuffleExec);
        assert_eq!(unresolved_shuffle.query_stage_ids, vec![2]);

        Ok(())
    }

    #[test]
    fn roundtrip_serde_hash_aggregate() -> Result<(), BallistaError> {
        let mut ctx = datafusion_test_context("testdata")?;

        // simplified form of TPC-H query 1
        let df = ctx.sql(
            "select l_returnflag, sum(l_extendedprice * 1) as sum_disc_price
            from lineitem
            group by l_returnflag
            order by l_returnflag",
        )?;

        let plan = df.to_logical_plan();
        let plan = ctx.optimize(&plan)?;
        let plan = ctx.create_physical_plan(&plan)?;

        let mut planner = DistributedPlanner::new();
        let job_uuid = Uuid::new_v4();
        let stages = planner.plan_query_stages(&job_uuid.to_string(), plan)?;

        let partial_hash = stages[0].children()[0].clone();
        let partial_hash_serde = roundtrip_operator(partial_hash.clone())?;

        let partial_hash = downcast_exec!(partial_hash, HashAggregateExec);
        let partial_hash_serde = downcast_exec!(partial_hash_serde, HashAggregateExec);

        assert_eq!(
            format!("{:?}", partial_hash),
            format!("{:?}", partial_hash_serde)
        );

        Ok(())
    }

    fn roundtrip_operator(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, BallistaError> {
        let proto: protobuf::PhysicalPlanNode = plan.clone().try_into()?;
        let result_exec_plan: Arc<dyn ExecutionPlan> = (&proto).try_into()?;
        Ok(result_exec_plan)
    }
}
