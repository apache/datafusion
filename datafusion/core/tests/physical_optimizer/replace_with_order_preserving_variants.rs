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

use crate::physical_optimizer::test_utils::{
    check_integrity, coalesce_partitions_exec, create_test_schema3,
    parquet_exec_with_sort, sort_exec, sort_exec_with_preserve_partitioning,
    sort_preserving_merge_exec, sort_preserving_merge_exec_with_fetch,
    stream_exec_ordered_with_projection,
};

use datafusion::prelude::SessionContext;
use arrow::array::{ArrayRef, Int32Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use insta::{allow_duplicates, assert_snapshot};
use datafusion_common::tree_node::{TransformedResult, TreeNode};
use datafusion_common::{assert_contains, NullEquality, Result};
use datafusion_common::config::ConfigOptions;
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::TaskContext;
use datafusion_expr::{JoinType, Operator};
use datafusion_physical_expr::expressions::{self, col, Column};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_optimizer::enforce_sorting::replace_with_order_preserving_variants::{
    plan_with_order_breaking_variants, plan_with_order_preserving_variants, replace_with_order_preserving_variants, OrderPreservationContext
};
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::{
    collect, displayable, ExecutionPlan, Partitioning,
};

use object_store::ObjectStore;
use object_store::memory::InMemory;
use rstest::rstest;
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Boundedness {
    Unbounded,
    Bounded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortPreference {
    PreserveOrder,
    MaximizeParallelism,
}

struct ReplaceTest {
    plan: Arc<dyn ExecutionPlan>,
    boundedness: Boundedness,
    sort_preference: SortPreference,
}

impl ReplaceTest {
    fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            plan,
            boundedness: Boundedness::Bounded,
            sort_preference: SortPreference::MaximizeParallelism,
        }
    }

    fn with_boundedness(mut self, boundedness: Boundedness) -> Self {
        self.boundedness = boundedness;
        self
    }

    fn with_sort_preference(mut self, sort_preference: SortPreference) -> Self {
        self.sort_preference = sort_preference;
        self
    }

    async fn execute_plan(&self) -> String {
        let mut config = ConfigOptions::new();
        config.optimizer.prefer_existing_sort =
            self.sort_preference == SortPreference::PreserveOrder;

        let plan_with_pipeline_fixer = OrderPreservationContext::new_default(
            self.plan.clone().reset_state().unwrap(),
        );

        let parallel = plan_with_pipeline_fixer
            .transform_up(|plan_with_pipeline_fixer| {
                replace_with_order_preserving_variants(
                    plan_with_pipeline_fixer,
                    false,
                    false,
                    &config,
                )
            })
            .data()
            .and_then(check_integrity)
            .unwrap();

        let optimized_physical_plan = parallel.plan;
        let optimized_plan_string = displayable(optimized_physical_plan.as_ref())
            .indent(true)
            .to_string();

        if self.boundedness == Boundedness::Bounded {
            let ctx = SessionContext::new();
            let object_store = InMemory::new();
            object_store
                .put(
                    &object_store::path::Path::from("file_path"),
                    bytes::Bytes::from("").into(),
                )
                .await
                .expect("could not create object store");
            ctx.register_object_store(
                &Url::parse("test://").unwrap(),
                Arc::new(object_store),
            );
            let task_ctx = Arc::new(TaskContext::from(&ctx));
            let res = collect(optimized_physical_plan, task_ctx).await;
            assert!(
                res.is_ok(),
                "Some errors occurred while executing the optimized physical plan: {:?}\nPlan: {}",
                res.unwrap_err(),
                optimized_plan_string
            );
        }

        optimized_plan_string
    }

    async fn run(&self) -> String {
        let input_plan_string = displayable(self.plan.as_ref()).indent(true).to_string();

        let optimized = self.execute_plan().await;

        if input_plan_string == optimized {
            format!("Input / Optimized:\n{input_plan_string}")
        } else {
            format!("Input:\n{input_plan_string}\nOptimized:\n{optimized}")
        }
    }
}

#[rstest]
#[tokio::test]
// Searches for a simple sort and a repartition just after it, the second repartition with 1 input partition should not be affected
async fn test_replace_multiple_input_repartition_1(
    #[values(Boundedness::Unbounded, Boundedness::Bounded)] boundedness: Boundedness,
    #[values(SortPreference::PreserveOrder, SortPreference::MaximizeParallelism)]
    sort_pref: SortPreference,
) -> Result<()> {
    let schema = create_test_schema()?;
    let sort_exprs: LexOrdering = [sort_expr("a", &schema)].into();
    let source = match boundedness {
        Boundedness::Unbounded => {
            stream_exec_ordered_with_projection(&schema, sort_exprs.clone())
        }
        Boundedness::Bounded => memory_exec_sorted(&schema, sort_exprs.clone()),
    };
    let repartition = repartition_exec_hash(repartition_exec_round_robin(source));
    let sort = sort_exec_with_preserve_partitioning(sort_exprs.clone(), repartition);
    let physical_plan = sort_preserving_merge_exec(sort_exprs, sort);

    let run = ReplaceTest::new(physical_plan)
        .with_boundedness(boundedness)
        .with_sort_preference(sort_pref);

    let physical_plan = run.run().await;

    allow_duplicates! {
    match (boundedness, sort_pref) {
        (Boundedness::Bounded, SortPreference::MaximizeParallelism) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        },
        (Boundedness::Unbounded, _) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]

            Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST
                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                  StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]
            ");
        },
        (Boundedness::Bounded, SortPreference::PreserveOrder) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST

            Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST
                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                  DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        }
    }
    }

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_with_inter_children_change_only(
    #[values(Boundedness::Unbounded, Boundedness::Bounded)] boundedness: Boundedness,
    #[values(SortPreference::PreserveOrder, SortPreference::MaximizeParallelism)]
    sort_pref: SortPreference,
) -> Result<()> {
    let schema = create_test_schema()?;
    let ordering: LexOrdering = [sort_expr_default("a", &schema)].into();
    let source = match boundedness {
        Boundedness::Unbounded => {
            stream_exec_ordered_with_projection(&schema, ordering.clone())
        }
        Boundedness::Bounded => memory_exec_sorted(&schema, ordering.clone()),
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let repartition_hash = repartition_exec_hash(repartition_rr);
    let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
    let sort = sort_exec(ordering.clone(), coalesce_partitions);
    let repartition_rr2 = repartition_exec_round_robin(sort);
    let repartition_hash2 = repartition_exec_hash(repartition_rr2);
    let filter = filter_exec(repartition_hash2);
    let sort2 = sort_exec_with_preserve_partitioning(ordering.clone(), filter);

    let physical_plan = sort_preserving_merge_exec(ordering, sort2);

    let run = ReplaceTest::new(physical_plan)
        .with_boundedness(boundedness)
        .with_sort_preference(sort_pref);

    let physical_plan = run.run().await;

    allow_duplicates! {
    match (boundedness, sort_pref) {
        (Boundedness::Unbounded, _) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortPreservingMergeExec: [a@0 ASC]
              SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
                        CoalescePartitionsExec
                          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                              StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC]

            Optimized:
            SortPreservingMergeExec: [a@0 ASC]
              FilterExec: c@1 > 3
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    SortPreservingMergeExec: [a@0 ASC]
                      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC
                        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                          StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC]
            ");
        },
        (Boundedness::Bounded, SortPreference::MaximizeParallelism) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            SortPreservingMergeExec: [a@0 ASC]
              SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
                        CoalescePartitionsExec
                          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                              DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC
            ");
        },
        (Boundedness::Bounded, SortPreference::PreserveOrder) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortPreservingMergeExec: [a@0 ASC]
              SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
                        CoalescePartitionsExec
                          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                              DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC

            Optimized:
            SortPreservingMergeExec: [a@0 ASC]
              FilterExec: c@1 > 3
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    SortPreservingMergeExec: [a@0 ASC]
                      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC
                        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                          DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC
            ");
        }
    }
    }

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_replace_multiple_input_repartition_2(
    #[values(Boundedness::Unbounded, Boundedness::Bounded)] boundedness: Boundedness,
    #[values(SortPreference::PreserveOrder, SortPreference::MaximizeParallelism)]
    sort_pref: SortPreference,
) -> Result<()> {
    let schema = create_test_schema()?;
    let ordering: LexOrdering = [sort_expr("a", &schema)].into();
    let source = match boundedness {
        Boundedness::Unbounded => {
            stream_exec_ordered_with_projection(&schema, ordering.clone())
        }
        Boundedness::Bounded => memory_exec_sorted(&schema, ordering.clone()),
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let filter = filter_exec(repartition_rr);
    let repartition_hash = repartition_exec_hash(filter);
    let sort = sort_exec_with_preserve_partitioning(ordering.clone(), repartition_hash);
    let physical_plan = sort_preserving_merge_exec(ordering, sort);

    let run = ReplaceTest::new(physical_plan)
        .with_boundedness(boundedness)
        .with_sort_preference(sort_pref);

    let physical_plan = run.run().await;

    allow_duplicates! {
    match (boundedness, sort_pref) {
        (Boundedness::Unbounded, _) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  FilterExec: c@1 > 3
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]

            Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]
            ");
        },
        (Boundedness::Bounded, SortPreference::MaximizeParallelism) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  FilterExec: c@1 > 3
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        },
        (Boundedness::Bounded, SortPreference::PreserveOrder) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  FilterExec: c@1 > 3
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST

            Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        }
    }
    }

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_replace_multiple_input_repartition_with_extra_steps(
    #[values(Boundedness::Unbounded, Boundedness::Bounded)] boundedness: Boundedness,
    #[values(SortPreference::PreserveOrder, SortPreference::MaximizeParallelism)]
    sort_pref: SortPreference,
) -> Result<()> {
    let schema = create_test_schema()?;
    let ordering: LexOrdering = [sort_expr("a", &schema)].into();
    let source = match boundedness {
        Boundedness::Unbounded => {
            stream_exec_ordered_with_projection(&schema, ordering.clone())
        }
        Boundedness::Bounded => memory_exec_sorted(&schema, ordering.clone()),
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let repartition_hash = repartition_exec_hash(repartition_rr);
    let filter = filter_exec(repartition_hash);
    let sort = sort_exec_with_preserve_partitioning(ordering.clone(), filter);
    let physical_plan = sort_preserving_merge_exec(ordering, sort);

    let run = ReplaceTest::new(physical_plan)
        .with_boundedness(boundedness)
        .with_sort_preference(sort_pref);

    let physical_plan = run.run().await;

    allow_duplicates! {
    match (boundedness, sort_pref) {
        (Boundedness::Unbounded, _) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]

            Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              FilterExec: c@1 > 3
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]
            ");
        },
        (Boundedness::Bounded, SortPreference::MaximizeParallelism) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        },
        (Boundedness::Bounded, SortPreference::PreserveOrder) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST

            Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              FilterExec: c@1 > 3
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        }
    }
    }

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_replace_multiple_input_repartition_with_extra_steps_2(
    #[values(Boundedness::Unbounded, Boundedness::Bounded)] boundedness: Boundedness,
    #[values(SortPreference::PreserveOrder, SortPreference::MaximizeParallelism)]
    sort_pref: SortPreference,
) -> Result<()> {
    let schema = create_test_schema()?;
    let ordering: LexOrdering = [sort_expr("a", &schema)].into();
    let source = match boundedness {
        Boundedness::Unbounded => {
            stream_exec_ordered_with_projection(&schema, ordering.clone())
        }
        Boundedness::Bounded => memory_exec_sorted(&schema, ordering.clone()),
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let repartition_hash = repartition_exec_hash(repartition_rr);
    let filter = filter_exec(repartition_hash);
    let sort = sort_exec_with_preserve_partitioning(ordering.clone(), filter);
    let physical_plan = sort_preserving_merge_exec(ordering, sort);

    let run = ReplaceTest::new(physical_plan)
        .with_boundedness(boundedness)
        .with_sort_preference(sort_pref);

    let physical_plan = run.run().await;

    allow_duplicates! {
    match (boundedness, sort_pref) {
        (Boundedness::Unbounded, _) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]

            Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              FilterExec: c@1 > 3
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]
            ");
        },
        (Boundedness::Bounded, SortPreference::MaximizeParallelism) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        },
        (Boundedness::Bounded, SortPreference::PreserveOrder) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST

            Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              FilterExec: c@1 > 3
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        }
    }
    }

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_not_replacing_when_no_need_to_preserve_sorting(
    #[values(Boundedness::Unbounded, Boundedness::Bounded)] boundedness: Boundedness,
    #[values(SortPreference::PreserveOrder, SortPreference::MaximizeParallelism)]
    sort_pref: SortPreference,
) -> Result<()> {
    let schema = create_test_schema()?;
    let ordering: LexOrdering = [sort_expr("a", &schema)].into();
    let source = match boundedness {
        Boundedness::Unbounded => stream_exec_ordered_with_projection(&schema, ordering),
        Boundedness::Bounded => memory_exec_sorted(&schema, ordering),
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let repartition_hash = repartition_exec_hash(repartition_rr);
    let filter = filter_exec(repartition_hash);
    let physical_plan = coalesce_partitions_exec(filter);

    let run = ReplaceTest::new(physical_plan)
        .with_boundedness(boundedness)
        .with_sort_preference(sort_pref);

    let physical_plan = run.run().await;

    allow_duplicates! {
    match (boundedness, sort_pref) {
        (Boundedness::Unbounded, _) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            CoalescePartitionsExec
              FilterExec: c@1 > 3
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]
            ");
        },
        (Boundedness::Bounded, SortPreference::MaximizeParallelism) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            CoalescePartitionsExec
              FilterExec: c@1 > 3
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
                // Expected bounded results same with and without flag, because there is no executor  with ordering requirement
        },
        (Boundedness::Bounded, SortPreference::PreserveOrder) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            CoalescePartitionsExec
              FilterExec: c@1 > 3
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        }
    }
    }

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_with_multiple_replaceable_repartitions(
    #[values(Boundedness::Unbounded, Boundedness::Bounded)] boundedness: Boundedness,
    #[values(SortPreference::PreserveOrder, SortPreference::MaximizeParallelism)]
    sort_pref: SortPreference,
) -> Result<()> {
    let schema = create_test_schema()?;
    let ordering: LexOrdering = [sort_expr("a", &schema)].into();
    let source = match boundedness {
        Boundedness::Unbounded => {
            stream_exec_ordered_with_projection(&schema, ordering.clone())
        }
        Boundedness::Bounded => memory_exec_sorted(&schema, ordering.clone()),
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let repartition_hash = repartition_exec_hash(repartition_rr);
    let filter = filter_exec(repartition_hash);
    let repartition_hash_2 = repartition_exec_hash(filter);
    let sort = sort_exec_with_preserve_partitioning(ordering.clone(), repartition_hash_2);
    let physical_plan = sort_preserving_merge_exec(ordering, sort);

    let run = ReplaceTest::new(physical_plan)
        .with_boundedness(boundedness)
        .with_sort_preference(sort_pref);

    let physical_plan = run.run().await;

    allow_duplicates! {
    match (boundedness, sort_pref) {
        (Boundedness::Unbounded, _) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  FilterExec: c@1 > 3
                    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                        StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]

            Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]
            ");
        },
        (Boundedness::Bounded, SortPreference::MaximizeParallelism) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  FilterExec: c@1 > 3
                    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                        DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        },
        (Boundedness::Bounded, SortPreference::PreserveOrder) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  FilterExec: c@1 > 3
                    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                        DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST

            Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        }
    }
    }

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_not_replace_with_different_orderings(
    #[values(Boundedness::Unbounded, Boundedness::Bounded)] boundedness: Boundedness,
    #[values(SortPreference::PreserveOrder, SortPreference::MaximizeParallelism)]
    sort_pref: SortPreference,
) -> Result<()> {
    use datafusion_physical_expr::LexOrdering;

    let schema = create_test_schema()?;
    let ordering_a = [sort_expr("a", &schema)].into();
    let source = match boundedness {
        Boundedness::Unbounded => {
            stream_exec_ordered_with_projection(&schema, ordering_a)
        }
        Boundedness::Bounded => memory_exec_sorted(&schema, ordering_a),
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let repartition_hash = repartition_exec_hash(repartition_rr);
    let ordering_c: LexOrdering =
        [sort_expr_default("c", &repartition_hash.schema())].into();
    let sort = sort_exec_with_preserve_partitioning(ordering_c.clone(), repartition_hash);
    let physical_plan = sort_preserving_merge_exec(ordering_c, sort);

    let run = ReplaceTest::new(physical_plan)
        .with_boundedness(boundedness)
        .with_sort_preference(sort_pref);

    let physical_plan = run.run().await;

    allow_duplicates! {
    match (boundedness, sort_pref) {
        (Boundedness::Unbounded, _) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            SortPreservingMergeExec: [c@1 ASC]
              SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]
            ");
        },
        (Boundedness::Bounded, SortPreference::MaximizeParallelism) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            SortPreservingMergeExec: [c@1 ASC]
              SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
                // Expected bounded results same with and without flag, because ordering requirement of the executor is
                // different from the existing ordering.
        },
        (Boundedness::Bounded, SortPreference::PreserveOrder) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            SortPreservingMergeExec: [c@1 ASC]
              SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        }
    }
    }

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_with_lost_ordering(
    #[values(Boundedness::Unbounded, Boundedness::Bounded)] boundedness: Boundedness,
    #[values(SortPreference::PreserveOrder, SortPreference::MaximizeParallelism)]
    sort_pref: SortPreference,
) -> Result<()> {
    let schema = create_test_schema()?;
    let ordering: LexOrdering = [sort_expr("a", &schema)].into();
    let source = match boundedness {
        Boundedness::Unbounded => {
            stream_exec_ordered_with_projection(&schema, ordering.clone())
        }
        Boundedness::Bounded => memory_exec_sorted(&schema, ordering.clone()),
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let repartition_hash = repartition_exec_hash(repartition_rr);
    let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
    let physical_plan = sort_exec(ordering, coalesce_partitions);

    let run = ReplaceTest::new(physical_plan)
        .with_boundedness(boundedness)
        .with_sort_preference(sort_pref);

    let physical_plan = run.run().await;

    allow_duplicates! {
    match (boundedness, sort_pref) {
        (Boundedness::Unbounded, _) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[false]
              CoalescePartitionsExec
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]

            Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST
                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                  StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]
            ");
        },
        (Boundedness::Bounded, SortPreference::MaximizeParallelism) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[false]
              CoalescePartitionsExec
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        },
        (Boundedness::Bounded, SortPreference::PreserveOrder) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[false]
              CoalescePartitionsExec
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST

            Optimized:
            SortPreservingMergeExec: [a@0 ASC NULLS LAST]
              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST
                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                  DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        }
    }
    }

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_with_lost_and_kept_ordering(
    #[values(Boundedness::Unbounded, Boundedness::Bounded)] boundedness: Boundedness,
    #[values(SortPreference::PreserveOrder, SortPreference::MaximizeParallelism)]
    sort_pref: SortPreference,
) -> Result<()> {
    use datafusion_physical_expr::LexOrdering;

    let schema = create_test_schema()?;
    let ordering_a = [sort_expr("a", &schema)].into();
    let source = match boundedness {
        Boundedness::Unbounded => {
            stream_exec_ordered_with_projection(&schema, ordering_a)
        }
        Boundedness::Bounded => memory_exec_sorted(&schema, ordering_a),
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let repartition_hash = repartition_exec_hash(repartition_rr);
    let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
    let ordering_c: LexOrdering =
        [sort_expr_default("c", &coalesce_partitions.schema())].into();
    let sort = sort_exec(ordering_c.clone(), coalesce_partitions);
    let repartition_rr2 = repartition_exec_round_robin(sort);
    let repartition_hash2 = repartition_exec_hash(repartition_rr2);
    let filter = filter_exec(repartition_hash2);
    let sort2 = sort_exec_with_preserve_partitioning(ordering_c.clone(), filter);
    let physical_plan = sort_preserving_merge_exec(ordering_c, sort2);

    let run = ReplaceTest::new(physical_plan)
        .with_boundedness(boundedness)
        .with_sort_preference(sort_pref);

    let physical_plan = run.run().await;

    allow_duplicates! {
    match (boundedness, sort_pref) {
        (Boundedness::Unbounded, _) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortPreservingMergeExec: [c@1 ASC]
              SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      SortExec: expr=[c@1 ASC], preserve_partitioning=[false]
                        CoalescePartitionsExec
                          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                              StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]

            Optimized:
            SortPreservingMergeExec: [c@1 ASC]
              FilterExec: c@1 > 3
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=c@1 ASC
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    SortExec: expr=[c@1 ASC], preserve_partitioning=[false]
                      CoalescePartitionsExec
                        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                            StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]
            ");
        },
        (Boundedness::Bounded, SortPreference::MaximizeParallelism) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            SortPreservingMergeExec: [c@1 ASC]
              SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      SortExec: expr=[c@1 ASC], preserve_partitioning=[false]
                        CoalescePartitionsExec
                          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                              DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        },
        (Boundedness::Bounded, SortPreference::PreserveOrder) => {
            assert_snapshot!(physical_plan, @r"
            Input:
            SortPreservingMergeExec: [c@1 ASC]
              SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
                FilterExec: c@1 > 3
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      SortExec: expr=[c@1 ASC], preserve_partitioning=[false]
                        CoalescePartitionsExec
                          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                              DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST

            Optimized:
            SortPreservingMergeExec: [c@1 ASC]
              FilterExec: c@1 > 3
                RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=c@1 ASC
                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                    SortExec: expr=[c@1 ASC], preserve_partitioning=[false]
                      CoalescePartitionsExec
                        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                            DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
        }
    }
    }

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_with_multiple_child_trees(
    #[values(Boundedness::Unbounded, Boundedness::Bounded)] boundedness: Boundedness,
    #[values(SortPreference::PreserveOrder, SortPreference::MaximizeParallelism)]
    sort_pref: SortPreference,
) -> Result<()> {
    let schema = create_test_schema()?;

    let left_ordering = [sort_expr("a", &schema)].into();
    let left_source = match boundedness {
        Boundedness::Unbounded => {
            stream_exec_ordered_with_projection(&schema, left_ordering)
        }
        Boundedness::Bounded => memory_exec_sorted(&schema, left_ordering),
    };
    let left_repartition_rr = repartition_exec_round_robin(left_source);
    let left_repartition_hash = repartition_exec_hash(left_repartition_rr);

    let right_ordering = [sort_expr("a", &schema)].into();
    let right_source = match boundedness {
        Boundedness::Unbounded => {
            stream_exec_ordered_with_projection(&schema, right_ordering)
        }
        Boundedness::Bounded => memory_exec_sorted(&schema, right_ordering),
    };
    let right_repartition_rr = repartition_exec_round_robin(right_source);
    let right_repartition_hash = repartition_exec_hash(right_repartition_rr);

    let hash_join_exec = hash_join_exec(left_repartition_hash, right_repartition_hash);
    let ordering: LexOrdering = [sort_expr_default("a", &hash_join_exec.schema())].into();
    let sort = sort_exec_with_preserve_partitioning(ordering.clone(), hash_join_exec);
    let physical_plan = sort_preserving_merge_exec(ordering, sort);

    let run = ReplaceTest::new(physical_plan)
        .with_boundedness(boundedness)
        .with_sort_preference(sort_pref);

    let physical_plan = run.run().await;

    allow_duplicates! {
    match (boundedness, sort_pref) {
        (Boundedness::Unbounded, _) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            SortPreservingMergeExec: [a@0 ASC]
              SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, c@1)]
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]
            ");
        },
        (Boundedness::Bounded, _) => {
            assert_snapshot!(physical_plan, @r"
            Input / Optimized:
            SortPreservingMergeExec: [a@0 ASC]
              SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, c@1)]
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
                  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8
                    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1, maintains_sort_order=true
                      DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST
            ");
                // Expected bounded results same with and without flag, because ordering get lost during intermediate executor anyway.
                //  Hence, no need to preserve existing ordering.
        }
    }
    }

    Ok(())
}

fn sort_expr(name: &str, schema: &Schema) -> PhysicalSortExpr {
    let sort_opts = SortOptions {
        nulls_first: false,
        descending: false,
    };
    sort_expr_options(name, schema, sort_opts)
}

fn sort_expr_default(name: &str, schema: &Schema) -> PhysicalSortExpr {
    let sort_opts = SortOptions::default();
    sort_expr_options(name, schema, sort_opts)
}

fn sort_expr_options(
    name: &str,
    schema: &Schema,
    options: SortOptions,
) -> PhysicalSortExpr {
    PhysicalSortExpr {
        expr: col(name, schema).unwrap(),
        options,
    }
}

fn repartition_exec_round_robin(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(8)).unwrap())
}

fn repartition_exec_hash(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let input_schema = input.schema();
    Arc::new(
        RepartitionExec::try_new(
            input,
            Partitioning::Hash(vec![col("c", &input_schema).unwrap()], 8),
        )
        .unwrap(),
    )
}

fn filter_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let input_schema = input.schema();
    let predicate = expressions::binary(
        col("c", &input_schema).unwrap(),
        Operator::Gt,
        expressions::lit(3i32),
        &input_schema,
    )
    .unwrap();
    Arc::new(FilterExec::try_new(predicate, input).unwrap())
}

fn hash_join_exec(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let left_on = col("c", &left.schema()).unwrap();
    let right_on = col("c", &right.schema()).unwrap();
    let left_col = left_on.as_any().downcast_ref::<Column>().unwrap();
    let right_col = right_on.as_any().downcast_ref::<Column>().unwrap();
    Arc::new(
        HashJoinExec::try_new(
            left,
            right,
            vec![(Arc::new(left_col.clone()), Arc::new(right_col.clone()))],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
        )
        .unwrap(),
    )
}

fn create_test_schema() -> Result<SchemaRef> {
    let column_a = Field::new("a", DataType::Int32, false);
    let column_b = Field::new("b", DataType::Int32, false);
    let column_c = Field::new("c", DataType::Int32, false);
    let column_d = Field::new("d", DataType::Int32, false);
    let schema = Arc::new(Schema::new(vec![column_a, column_b, column_c, column_d]));

    Ok(schema)
}

// creates a memory exec source for the test purposes
// projection parameter is given static due to testing needs
fn memory_exec_sorted(
    schema: &SchemaRef,
    ordering: LexOrdering,
) -> Arc<dyn ExecutionPlan> {
    pub fn make_partition(schema: &SchemaRef, sz: i32) -> RecordBatch {
        let values = (0..sz).collect::<Vec<_>>();
        let arr = Arc::new(Int32Array::from(values));
        let arr = arr as ArrayRef;

        RecordBatch::try_new(
            schema.clone(),
            vec![arr.clone(), arr.clone(), arr.clone(), arr],
        )
        .unwrap()
    }

    let rows = 5;
    let partitions = 1;
    Arc::new({
        let data: Vec<Vec<_>> = (0..partitions)
            .map(|_| vec![make_partition(schema, rows)])
            .collect();
        let projection: Vec<usize> = vec![0, 2, 3];
        DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&data, schema.clone(), Some(projection))
                .unwrap()
                .try_with_sort_information(vec![ordering])
                .unwrap(),
        ))
    })
}

#[test]
fn test_plan_with_order_preserving_variants_preserves_fetch() -> Result<()> {
    // Create a schema
    let schema = create_test_schema3()?;
    let parquet_sort_exprs = vec![[sort_expr("a", &schema)].into()];
    let parquet_exec = parquet_exec_with_sort(schema, parquet_sort_exprs);
    let coalesced = coalesce_partitions_exec(parquet_exec.clone())
        .with_fetch(Some(10))
        .unwrap();

    // Test sort's fetch is greater than coalesce fetch, return error because it's not reasonable
    let requirements = OrderPreservationContext::new(
        coalesced.clone(),
        false,
        vec![OrderPreservationContext::new(
            parquet_exec.clone(),
            false,
            vec![],
        )],
    );
    let res = plan_with_order_preserving_variants(requirements, false, true, Some(15));
    assert_contains!(
        res.unwrap_err().to_string(),
        "CoalescePartitionsExec fetch [10] should be greater than or equal to SortExec fetch [15]"
    );

    // Test sort is without fetch, expected to get the fetch value from the coalesced
    let requirements = OrderPreservationContext::new(
        coalesced.clone(),
        false,
        vec![OrderPreservationContext::new(
            parquet_exec.clone(),
            false,
            vec![],
        )],
    );
    let res = plan_with_order_preserving_variants(requirements, false, true, None)?;
    assert_eq!(res.plan.fetch(), Some(10),);

    // Test sort's fetch is less than coalesces fetch, expected to get the fetch value from the sort
    let requirements = OrderPreservationContext::new(
        coalesced,
        false,
        vec![OrderPreservationContext::new(parquet_exec, false, vec![])],
    );
    let res = plan_with_order_preserving_variants(requirements, false, true, Some(5))?;
    assert_eq!(res.plan.fetch(), Some(5),);
    Ok(())
}

#[test]
fn test_plan_with_order_breaking_variants_preserves_fetch() -> Result<()> {
    let schema = create_test_schema3()?;
    let parquet_sort_exprs: LexOrdering = [sort_expr("a", &schema)].into();
    let parquet_exec = parquet_exec_with_sort(schema, vec![parquet_sort_exprs.clone()]);
    let spm = sort_preserving_merge_exec_with_fetch(
        parquet_sort_exprs,
        parquet_exec.clone(),
        10,
    );
    let requirements = OrderPreservationContext::new(
        spm,
        true,
        vec![OrderPreservationContext::new(
            parquet_exec.clone(),
            true,
            vec![],
        )],
    );
    let res = plan_with_order_breaking_variants(requirements)?;
    assert_eq!(res.plan.fetch(), Some(10));
    Ok(())
}
