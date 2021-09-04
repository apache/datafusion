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
            let shuffle_writer = create_shuffle_writer(
                job_id,
                self.next_stage_id(),
                children[0].clone(),
                None,
            )?;
            let unresolved_shuffle = Arc::new(UnresolvedShuffleExec::new(
                shuffle_writer.stage_id(),
                shuffle_writer.schema(),
                shuffle_writer.output_partitioning().partition_count(),
                shuffle_writer
                    .shuffle_output_partitioning()
                    .map(|p| p.partition_count())
                    .unwrap_or_else(|| {
                        shuffle_writer.output_partitioning().partition_count()
                    }),
            ));
            stages.push(shuffle_writer);
            Ok((
                coalesce.with_new_children(vec![unresolved_shuffle])?,
                stages,
            ))
        } else if let Some(repart) =
            execution_plan.as_any().downcast_ref::<RepartitionExec>()
        {
            match repart.output_partitioning() {
                Partitioning::Hash(_, _) => {
                    let shuffle_writer = create_shuffle_writer(
                        job_id,
                        self.next_stage_id(),
                        children[0].clone(),
                        Some(repart.partitioning().to_owned()),
                    )?;
                    let unresolved_shuffle = Arc::new(UnresolvedShuffleExec::new(
                        shuffle_writer.stage_id(),
                        shuffle_writer.schema(),
                        shuffle_writer.output_partitioning().partition_count(),
                        shuffle_writer
                            .shuffle_output_partitioning()
                            .map(|p| p.partition_count())
                            .unwrap_or_else(|| {
                                shuffle_writer.output_partitioning().partition_count()
                            }),
                    ));
                    stages.push(shuffle_writer);
                    Ok((unresolved_shuffle, stages))
                }
                _ => {
                    // remove any non-hash repartition from the distributed plan
                    Ok((children[0].clone(), stages))
                }
            }
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
    partition_locations: &HashMap<usize, HashMap<usize, Vec<PartitionLocation>>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut new_children: Vec<Arc<dyn ExecutionPlan>> = vec![];
    for child in stage.children() {
        if let Some(unresolved_shuffle) =
            child.as_any().downcast_ref::<UnresolvedShuffleExec>()
        {
            let mut relevant_locations = vec![];
            let p = partition_locations
                .get(&unresolved_shuffle.stage_id)
                .ok_or_else(|| {
                    BallistaError::General(
                        "Missing partition location. Could not remove unresolved shuffles"
                            .to_owned(),
                    )
                })?
                .clone();

            for i in 0..unresolved_shuffle.output_partition_count {
                if let Some(x) = p.get(&i) {
                    relevant_locations.push(x.to_owned());
                } else {
                    relevant_locations.push(vec![]);
                }
            }
            info!(
                "Creating shuffle reader: {}",
                relevant_locations
                    .iter()
                    .map(|c| c
                        .iter()
                        .map(|l| l.path.clone())
                        .collect::<Vec<_>>()
                        .join(", "))
                    .collect::<Vec<_>>()
                    .join("\n")
            );
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
    use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
    use datafusion::physical_plan::hash_join::HashJoinExec;
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
                UnresolvedShuffleExec

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
        let coalesce = final_hash.children()[0].clone();
        let coalesce = downcast_exec!(coalesce, CoalesceBatchesExec);
        let unresolved_shuffle = coalesce.children()[0].clone();
        let unresolved_shuffle =
            downcast_exec!(unresolved_shuffle, UnresolvedShuffleExec);
        assert_eq!(unresolved_shuffle.stage_id, 1);
        assert_eq!(unresolved_shuffle.input_partition_count, 2);
        assert_eq!(unresolved_shuffle.output_partition_count, 2);

        // verify stage 2
        let stage2 = stages[2].children()[0].clone();
        let sort = downcast_exec!(stage2, SortExec);
        let coalesce_partitions = sort.children()[0].clone();
        let coalesce_partitions =
            downcast_exec!(coalesce_partitions, CoalescePartitionsExec);
        assert_eq!(
            coalesce_partitions.output_partitioning().partition_count(),
            1
        );
        let unresolved_shuffle = coalesce_partitions.children()[0].clone();
        let unresolved_shuffle =
            downcast_exec!(unresolved_shuffle, UnresolvedShuffleExec);
        assert_eq!(unresolved_shuffle.stage_id, 2);
        assert_eq!(unresolved_shuffle.input_partition_count, 2);
        assert_eq!(unresolved_shuffle.output_partition_count, 2);

        Ok(())
    }

    #[test]
    fn distributed_join_plan() -> Result<(), BallistaError> {
        let mut ctx = datafusion_test_context("testdata")?;

        // simplified form of TPC-H query 12
        let df = ctx.sql(
            "select
    l_shipmode,
    sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
    sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
from
    lineitem
        join
    orders
    on
            l_orderkey = o_orderkey
where
        l_shipmode in ('MAIL', 'SHIP')
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1994-01-01'
  and l_receiptdate < date '1995-01-01'
group by
    l_shipmode
order by
    l_shipmode;
",
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

        ShuffleWriterExec: Some(Hash([Column { name: "l_orderkey", index: 0 }], 2))
          CoalesceBatchesExec: target_batch_size=4096
            FilterExec: l_shipmode@4 IN ([Literal { value: Utf8("MAIL") }, Literal { value: Utf8("SHIP") }]) AND l_commitdate@2 < l_receiptdate@3 AND l_shipdate@1 < l_commitdate@2 AND l_receiptdate@3 >= 8766 AND l_receiptdate@3 < 9131
              CsvExec: source=Path(testdata/lineitem: [testdata/lineitem/partition0.tbl,testdata/lineitem/partition1.tbl]), has_header=false

        ShuffleWriterExec: Some(Hash([Column { name: "o_orderkey", index: 0 }], 2))
          CsvExec: source=Path(testdata/orders: [testdata/orders/orders.tbl]), has_header=false

        ShuffleWriterExec: Some(Hash([Column { name: "l_shipmode", index: 0 }], 2))
          HashAggregateExec: mode=Partial, gby=[l_shipmode@4 as l_shipmode], aggr=[SUM(CASE WHEN #orders.o_orderpriority Eq Utf8("1-URGENT") Or #orders.o_orderpriority Eq Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END), SUM(CASE WHEN #orders.o_orderpriority NotEq Utf8("1-URGENT") And #orders.o_orderpriority NotEq Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)]
            CoalesceBatchesExec: target_batch_size=4096
              HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: "l_orderkey", index: 0 }, Column { name: "o_orderkey", index: 0 })]
                CoalesceBatchesExec: target_batch_size=4096
                  UnresolvedShuffleExec
                CoalesceBatchesExec: target_batch_size=4096
                  UnresolvedShuffleExec

        ShuffleWriterExec: None
          ProjectionExec: expr=[l_shipmode@0 as l_shipmode, SUM(CASE WHEN #orders.o_orderpriority Eq Utf8("1-URGENT") Or #orders.o_orderpriority Eq Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)@1 as high_line_count, SUM(CASE WHEN #orders.o_orderpriority NotEq Utf8("1-URGENT") And #orders.o_orderpriority NotEq Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)@2 as low_line_count]
            HashAggregateExec: mode=FinalPartitioned, gby=[l_shipmode@0 as l_shipmode], aggr=[SUM(CASE WHEN #orders.o_orderpriority Eq Utf8("1-URGENT") Or #orders.o_orderpriority Eq Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END), SUM(CASE WHEN #orders.o_orderpriority NotEq Utf8("1-URGENT") And #orders.o_orderpriority NotEq Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)]
              CoalesceBatchesExec: target_batch_size=4096
                UnresolvedShuffleExec

        ShuffleWriterExec: None
          SortExec: [l_shipmode@0 ASC]
            CoalescePartitionsExec
              UnresolvedShuffleExec
        */

        assert_eq!(5, stages.len());

        // verify partitioning for each stage

        // csv "lineitem" (2 files)
        assert_eq!(
            2,
            stages[0].children()[0]
                .output_partitioning()
                .partition_count()
        );
        assert_eq!(
            2,
            stages[0]
                .shuffle_output_partitioning()
                .unwrap()
                .partition_count()
        );

        // csv "orders" (1 file)
        assert_eq!(
            1,
            stages[1].children()[0]
                .output_partitioning()
                .partition_count()
        );
        assert_eq!(
            2,
            stages[1]
                .shuffle_output_partitioning()
                .unwrap()
                .partition_count()
        );

        // join and partial hash aggregate
        let input = stages[2].children()[0].clone();
        assert_eq!(2, input.output_partitioning().partition_count());
        assert_eq!(
            2,
            stages[2]
                .shuffle_output_partitioning()
                .unwrap()
                .partition_count()
        );

        let hash_agg = downcast_exec!(input, HashAggregateExec);

        let coalesce_batches = hash_agg.children()[0].clone();
        let coalesce_batches = downcast_exec!(coalesce_batches, CoalesceBatchesExec);

        let join = coalesce_batches.children()[0].clone();
        let join = downcast_exec!(join, HashJoinExec);

        let join_input_1 = join.children()[0].clone();
        // skip CoalesceBatches
        let join_input_1 = join_input_1.children()[0].clone();
        let unresolved_shuffle_reader_1 =
            downcast_exec!(join_input_1, UnresolvedShuffleExec);
        assert_eq!(unresolved_shuffle_reader_1.input_partition_count, 2); // lineitem
        assert_eq!(unresolved_shuffle_reader_1.output_partition_count, 2);

        let join_input_2 = join.children()[1].clone();
        // skip CoalesceBatches
        let join_input_2 = join_input_2.children()[0].clone();
        let unresolved_shuffle_reader_2 =
            downcast_exec!(join_input_2, UnresolvedShuffleExec);
        assert_eq!(unresolved_shuffle_reader_2.input_partition_count, 1); //orders
        assert_eq!(unresolved_shuffle_reader_2.output_partition_count, 2);

        // final partitioned hash aggregate
        assert_eq!(
            2,
            stages[3].children()[0]
                .output_partitioning()
                .partition_count()
        );
        assert!(stages[3].shuffle_output_partitioning().is_none());

        // coalesce partitions and sort
        assert_eq!(
            1,
            stages[4].children()[0]
                .output_partitioning()
                .partition_count()
        );
        assert!(stages[4].shuffle_output_partitioning().is_none());

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
