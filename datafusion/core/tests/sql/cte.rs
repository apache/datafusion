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

use super::*;
use datafusion::catalog::MemTable;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::materialized_cte::{
    MaterializedCteExec, MaterializedCteReaderExec,
};
use datafusion::physical_plan::{collect_partitioned, visit_execution_plan};
use datafusion_common::assert_batches_eq;
use datafusion_common::stats::Precision;

#[tokio::test]
async fn multi_reference_cte_materialization_heuristic() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE cte_scan_source AS VALUES (1), (2)")
        .await?
        .collect()
        .await?;

    let reused_scan = ctx
        .sql(
            "WITH t AS (SELECT column1 AS a FROM cte_scan_source) \
             SELECT count(*) FROM t l JOIN t r ON l.a = r.a",
        )
        .await?;
    let physical_plan = reused_scan.create_physical_plan().await?;
    let plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert_contains!(&plan, "MaterializedCteExec");
    assert_contains!(&plan, "MaterializedCteReaderExec");

    let cheap_literal = ctx
        .sql(
            "WITH t AS (SELECT 1 AS a) \
             SELECT count(*) FROM t l JOIN t r ON l.a = r.a",
        )
        .await?;
    let physical_plan = cheap_literal.create_physical_plan().await?;
    let plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert_not_contains!(&plan, "MaterializedCteExec");
    assert_not_contains!(&plan, "MaterializedCteReaderExec");

    let limited_reuse = ctx
        .sql(
            "WITH t AS (SELECT column1 AS a FROM cte_scan_source) \
             SELECT * FROM t l JOIN t r ON l.a = r.a LIMIT 1",
        )
        .await?;
    let physical_plan = limited_reuse.create_physical_plan().await?;
    let plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert_not_contains!(&plan, "MaterializedCteExec");
    assert_not_contains!(&plan, "MaterializedCteReaderExec");

    Ok(())
}

#[tokio::test]
async fn materialized_cte_reader_preserves_input_partitions() -> Result<()> {
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
    let partitions = (0..4)
        .map(|partition| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![partition]))],
            )
            .map(|batch| vec![batch])
        })
        .collect::<arrow::error::Result<Vec<_>>>()?;
    let provider = MemTable::try_new(Arc::clone(&schema), partitions)?;
    ctx.register_table("cte_partition_source", Arc::new(provider))?;

    let df = ctx
        .sql(
            "WITH t AS (SELECT i FROM cte_partition_source) \
             SELECT count(*) FROM t l JOIN t r ON l.i = r.i",
        )
        .await?;
    let physical_plan = df.create_physical_plan().await?;

    struct PartitionVisitor {
        producer_partitions: Vec<usize>,
        reader_partitions: Vec<usize>,
    }

    impl ExecutionPlanVisitor for PartitionVisitor {
        type Error = std::convert::Infallible;

        fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            if plan.is::<MaterializedCteExec>() {
                self.producer_partitions
                    .push(plan.output_partitioning().partition_count());
            }
            if plan.is::<MaterializedCteReaderExec>() {
                self.reader_partitions
                    .push(plan.output_partitioning().partition_count());
            }
            Ok(true)
        }
    }

    let mut visitor = PartitionVisitor {
        producer_partitions: vec![],
        reader_partitions: vec![],
    };
    visit_execution_plan(physical_plan.as_ref(), &mut visitor).unwrap();

    assert_eq!(visitor.producer_partitions, vec![1]);
    assert_eq!(visitor.reader_partitions, vec![4, 4]);

    let results = df.collect().await?;
    let expected = [
        "+----------+",
        "| count(*) |",
        "+----------+",
        "| 4        |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn materialized_cte_partitioned_continuation_executes_partitions_once() -> Result<()>
{
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
    let partitions = (0..4)
        .map(|partition| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![partition]))],
            )
            .map(|batch| vec![batch])
        })
        .collect::<arrow::error::Result<Vec<_>>>()?;
    let provider = MemTable::try_new(Arc::clone(&schema), partitions)?;
    ctx.register_table("cte_repartition_source", Arc::new(provider))?;

    let df = ctx
        .sql(
            "WITH t AS (SELECT i FROM cte_repartition_source) \
             SELECT l.i FROM t l JOIN t r ON l.i = r.i",
        )
        .await?;
    let physical_plan = df.create_physical_plan().await?;

    assert_eq!(physical_plan.output_partitioning().partition_count(), 4);
    let results = collect_partitioned(physical_plan, ctx.task_ctx()).await?;
    assert_eq!(
        results
            .iter()
            .flatten()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        4
    );

    Ok(())
}

#[tokio::test]
async fn materialized_cte_cache_is_per_physical_plan() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE cte_cache_source AS VALUES (1), (2)")
        .await?
        .collect()
        .await?;

    let first = ctx
        .sql(
            "WITH t AS (SELECT column1 AS a FROM cte_cache_source WHERE column1 = 1) \
             SELECT l.a FROM t l JOIN t r ON l.a = r.a",
        )
        .await?;
    let physical_plan = first.create_physical_plan().await?;
    let plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert_contains!(&plan, "MaterializedCteExec");
    let results = first.collect().await?;
    let expected = ["+---+", "| a |", "+---+", "| 1 |", "+---+"];
    assert_batches_eq!(expected, &results);

    let second = ctx
        .sql(
            "WITH t AS (SELECT column1 AS a FROM cte_cache_source WHERE column1 = 2) \
             SELECT l.a FROM t l JOIN t r ON l.a = r.a",
        )
        .await?;
    let physical_plan = second.create_physical_plan().await?;
    let plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert_contains!(&plan, "MaterializedCteExec");
    let results = second.collect().await?;
    let expected = ["+---+", "| a |", "+---+", "| 2 |", "+---+"];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn materialized_cte_reader_preserves_producer_statistics() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE cte_cross_source AS VALUES (1), (2), (3), (4)")
        .await?
        .collect()
        .await?;

    let df = ctx
        .sql(
            "WITH scalar_cte AS ( \
                SELECT max(column1) AS max_value FROM cte_cross_source \
             ) \
             SELECT l.max_value \
             FROM scalar_cte l JOIN scalar_cte r ON l.max_value = r.max_value",
        )
        .await?;
    let physical_plan = df.create_physical_plan().await?;

    struct StatisticsVisitor {
        reader_rows: Vec<Precision<usize>>,
    }

    impl ExecutionPlanVisitor for StatisticsVisitor {
        type Error = datafusion::error::DataFusionError;

        fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            if plan.is::<MaterializedCteReaderExec>() {
                self.reader_rows
                    .push(plan.partition_statistics(None)?.num_rows.clone());
            }

            Ok(true)
        }
    }

    let mut visitor = StatisticsVisitor {
        reader_rows: vec![],
    };
    visit_execution_plan(physical_plan.as_ref(), &mut visitor)?;

    assert_eq!(
        visitor.reader_rows,
        vec![Precision::Exact(1), Precision::Exact(1)]
    );

    let results = df.collect().await?;
    let expected = [
        "+-----------+",
        "| max_value |",
        "+-----------+",
        "| 4         |",
        "+-----------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}
