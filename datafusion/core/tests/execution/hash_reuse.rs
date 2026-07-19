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

use datafusion::error::Result;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use insta::assert_snapshot;

#[tokio::test]
async fn grouped_tpch_aggregate_reuses_hashes_after_repartition() -> Result<()> {
    let plan_with_hash_metrics = HashReuseTest::new(
        r"SELECT l_returnflag, l_linestatus, COUNT(*) AS row_count
         FROM lineitem
         GROUP BY l_returnflag, l_linestatus",
    )
    .run()
    .await?;

    assert_snapshot!(plan_with_hash_metrics, @r"
    ProjectionExec: expr=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus, count(Int64(1))@2 as row_count], metrics=[]
      AggregateExec: mode=FinalPartitioned, gby=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus], aggr=[count(Int64(1))], metrics=[hash_rows_computed=0, hash_rows_reused=3]
        RepartitionExec: partitioning=Hash([l_returnflag@0, l_linestatus@1], 3), input_partitions=1, metrics=[hash_rows_computed=0, hash_rows_reused=3]
          AggregateExec: mode=Partial, gby=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus], aggr=[count(Int64(1))], metrics=[hash_rows_computed=20, hash_rows_reused=0]
            DataSourceExec: file_groups={1 group: [[$DATAFUSION_CORE/tests/data/tpch_lineitem_small.parquet]]}, projection=[l_returnflag, l_linestatus], file_type=parquet, metrics=[]
    ");

    Ok(())
}

#[tokio::test]
async fn legacy_grouped_tpch_aggregate_reuses_hashes_after_repartition() -> Result<()> {
    let plan_with_hash_metrics = HashReuseTest::new(
        r"SELECT l_returnflag, l_linestatus, COUNT(*) AS row_count
             FROM lineitem
             GROUP BY l_returnflag, l_linestatus",
    )
    .with_migration_aggregate(false)
    .run()
    .await?;

    assert_snapshot!(plan_with_hash_metrics, @r"
    ProjectionExec: expr=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus, count(Int64(1))@2 as row_count], metrics=[]
      AggregateExec: mode=FinalPartitioned, gby=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus], aggr=[count(Int64(1))], metrics=[hash_rows_computed=0, hash_rows_reused=3]
        RepartitionExec: partitioning=Hash([l_returnflag@0, l_linestatus@1], 3), input_partitions=1, metrics=[hash_rows_computed=0, hash_rows_reused=3]
          AggregateExec: mode=Partial, gby=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus], aggr=[count(Int64(1))], metrics=[hash_rows_computed=20, hash_rows_reused=0]
            DataSourceExec: file_groups={1 group: [[$DATAFUSION_CORE/tests/data/tpch_lineitem_small.parquet]]}, projection=[l_returnflag, l_linestatus], file_type=parquet, metrics=[]
    ");

    Ok(())
}

#[tokio::test]
async fn chunked_partial_tpch_aggregate_reuses_hashes_after_repartition() -> Result<()> {
    let plan_with_hash_metrics = HashReuseTest::new(
        r"SELECT l_returnflag, l_linestatus, COUNT(*) AS row_count
         FROM lineitem
         GROUP BY l_returnflag, l_linestatus",
    )
    .with_batch_size(1)
    .run()
    .await?;

    assert_snapshot!(plan_with_hash_metrics, @r"
    ProjectionExec: expr=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus, count(Int64(1))@2 as row_count], metrics=[]
      AggregateExec: mode=FinalPartitioned, gby=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus], aggr=[count(Int64(1))], metrics=[hash_rows_computed=0, hash_rows_reused=9]
        RepartitionExec: partitioning=Hash([l_returnflag@0, l_linestatus@1], 3), input_partitions=3, metrics=[hash_rows_computed=0, hash_rows_reused=9]
          AggregateExec: mode=Partial, gby=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus], aggr=[count(Int64(1))], metrics=[hash_rows_computed=20, hash_rows_reused=0]
            RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1, metrics=[]
              DataSourceExec: file_groups={1 group: [[$DATAFUSION_CORE/tests/data/tpch_lineitem_small.parquet]]}, projection=[l_returnflag, l_linestatus], file_type=parquet, metrics=[]
    ");

    Ok(())
}

#[tokio::test]
async fn expression_grouped_tpch_aggregate_reuses_hashes_after_repartition() -> Result<()>
{
    let plan_with_hash_metrics = HashReuseTest::new(
        r"SELECT upper(l_returnflag) AS returnflag, COUNT(*) AS row_count
         FROM lineitem
         GROUP BY upper(l_returnflag)",
    )
    .run()
    .await?;

    assert_snapshot!(plan_with_hash_metrics, @r"
    ProjectionExec: expr=[upper(lineitem.l_returnflag)@0 as returnflag, count(Int64(1))@1 as row_count], metrics=[]
      AggregateExec: mode=FinalPartitioned, gby=[upper(lineitem.l_returnflag)@0 as upper(lineitem.l_returnflag)], aggr=[count(Int64(1))], metrics=[hash_rows_computed=0, hash_rows_reused=3]
        RepartitionExec: partitioning=Hash([upper(lineitem.l_returnflag)@0], 3), input_partitions=1, metrics=[hash_rows_computed=0, hash_rows_reused=3]
          AggregateExec: mode=Partial, gby=[upper(l_returnflag@0) as upper(lineitem.l_returnflag)], aggr=[count(Int64(1))], metrics=[hash_rows_computed=20, hash_rows_reused=0]
            DataSourceExec: file_groups={1 group: [[$DATAFUSION_CORE/tests/data/tpch_lineitem_small.parquet]]}, projection=[l_returnflag], file_type=parquet, metrics=[]
    ");

    Ok(())
}

#[tokio::test]
async fn grouping_sets_tpch_aggregate_reuses_hashes_after_repartition() -> Result<()> {
    let plan_with_hash_metrics = HashReuseTest::new(
        r"SELECT l_returnflag, l_linestatus, COUNT(*) AS row_count
         FROM lineitem
         GROUP BY GROUPING SETS ((l_returnflag), (l_linestatus))",
    )
    .run()
    .await?;

    assert_snapshot!(plan_with_hash_metrics, @r"
    ProjectionExec: expr=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus, count(Int64(1))@3 as row_count], metrics=[]
      AggregateExec: mode=FinalPartitioned, gby=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus, __grouping_id@2 as __grouping_id], aggr=[count(Int64(1))], metrics=[hash_rows_computed=0, hash_rows_reused=5]
        RepartitionExec: partitioning=Hash([l_returnflag@0, l_linestatus@1, __grouping_id@2], 3), input_partitions=1, metrics=[hash_rows_computed=0, hash_rows_reused=5]
          AggregateExec: mode=Partial, gby=[(l_returnflag@0 as l_returnflag, NULL as l_linestatus), (NULL as l_returnflag, l_linestatus@1 as l_linestatus)], aggr=[count(Int64(1))], metrics=[hash_rows_computed=40, hash_rows_reused=0]
            DataSourceExec: file_groups={1 group: [[$DATAFUSION_CORE/tests/data/tpch_lineitem_small.parquet]]}, projection=[l_returnflag, l_linestatus], file_type=parquet, metrics=[]
    ");

    Ok(())
}

#[tokio::test]
async fn distinct_tpch_aggregate_reuses_hashes_after_repartition() -> Result<()> {
    let plan_with_hash_metrics =
        HashReuseTest::new("SELECT DISTINCT l_returnflag, l_linestatus FROM lineitem")
            .run()
            .await?;

    assert_snapshot!(plan_with_hash_metrics, @r"
    AggregateExec: mode=FinalPartitioned, gby=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus], aggr=[], metrics=[hash_rows_computed=0, hash_rows_reused=3]
      RepartitionExec: partitioning=Hash([l_returnflag@0, l_linestatus@1], 3), input_partitions=1, metrics=[hash_rows_computed=0, hash_rows_reused=3]
        AggregateExec: mode=Partial, gby=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus], aggr=[], metrics=[hash_rows_computed=20, hash_rows_reused=0]
          DataSourceExec: file_groups={1 group: [[$DATAFUSION_CORE/tests/data/tpch_lineitem_small.parquet]]}, projection=[l_returnflag, l_linestatus], file_type=parquet, metrics=[]
    ");

    Ok(())
}

#[tokio::test]
async fn ordered_tpch_aggregate_reuses_hashes_after_repartition() -> Result<()> {
    let plan_with_hash_metrics = HashReuseTest::new(
        r"SELECT l_returnflag, max(row_num) AS max_row_num
         FROM (
           SELECT l_returnflag, row_number() OVER (ORDER BY l_returnflag) AS row_num
           FROM lineitem
         )
         GROUP BY l_returnflag",
    )
    .run()
    .await?;

    assert_snapshot!(plan_with_hash_metrics, @r##"
    ProjectionExec: expr=[l_returnflag@0 as l_returnflag, max(row_num)@1 as max_row_num], metrics=[]
      AggregateExec: mode=FinalPartitioned, gby=[l_returnflag@0 as l_returnflag], aggr=[max(row_num)], ordering_mode=Sorted, metrics=[hash_rows_computed=0, hash_rows_reused=3]
        RepartitionExec: partitioning=Hash([l_returnflag@0], 3), input_partitions=1, maintains_sort_order=true, metrics=[hash_rows_computed=0, hash_rows_reused=3]
          AggregateExec: mode=Partial, gby=[l_returnflag@0 as l_returnflag], aggr=[max(row_num)], ordering_mode=Sorted, metrics=[hash_rows_computed=20, hash_rows_reused=0]
            ProjectionExec: expr=[l_returnflag@0 as l_returnflag, row_number() ORDER BY [lineitem.l_returnflag ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@1 as row_num], metrics=[]
              BoundedWindowAggExec: wdw=[row_number() ORDER BY [lineitem.l_returnflag ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: Field { "row_number() ORDER BY [lineitem.l_returnflag ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW": UInt64 }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted], metrics=[]
                SortExec: expr=[l_returnflag@0 ASC NULLS LAST], preserve_partitioning=[false], metrics=[]
                  DataSourceExec: file_groups={1 group: [[$DATAFUSION_CORE/tests/data/tpch_lineitem_small.parquet]]}, projection=[l_returnflag], file_type=parquet, sort_order_for_reorder=[l_returnflag@0 ASC NULLS LAST], metrics=[]
    "##);

    Ok(())
}

#[tokio::test]
async fn summed_tpch_aggregate_reuses_hashes_after_repartition() -> Result<()> {
    let plan_with_hash_metrics = HashReuseTest::new(
        r"SELECT l_returnflag, sum(l_quantity) AS total_quantity
         FROM lineitem
         GROUP BY l_returnflag",
    )
    .run()
    .await?;

    assert_snapshot!(plan_with_hash_metrics, @r"
    ProjectionExec: expr=[l_returnflag@0 as l_returnflag, sum(lineitem.l_quantity)@1 as total_quantity], metrics=[]
      AggregateExec: mode=FinalPartitioned, gby=[l_returnflag@0 as l_returnflag], aggr=[sum(lineitem.l_quantity)], metrics=[hash_rows_computed=0, hash_rows_reused=3]
        RepartitionExec: partitioning=Hash([l_returnflag@0], 3), input_partitions=1, metrics=[hash_rows_computed=0, hash_rows_reused=3]
          AggregateExec: mode=Partial, gby=[l_returnflag@1 as l_returnflag], aggr=[sum(lineitem.l_quantity)], metrics=[hash_rows_computed=20, hash_rows_reused=0]
            DataSourceExec: file_groups={1 group: [[$DATAFUSION_CORE/tests/data/tpch_lineitem_small.parquet]]}, projection=[l_quantity, l_returnflag], file_type=parquet, metrics=[]
    ");

    Ok(())
}

struct HashReuseTest<'a> {
    sql: &'a str,
    batch_size: usize,
    enable_migration_aggregate: bool,
}

impl<'a> HashReuseTest<'a> {
    fn new(sql: &'a str) -> Self {
        Self {
            sql,
            batch_size: 64,
            enable_migration_aggregate: true,
        }
    }

    fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    fn with_migration_aggregate(mut self, enable: bool) -> Self {
        self.enable_migration_aggregate = enable;
        self
    }

    async fn run(self) -> Result<String> {
        let config = SessionConfig::new()
            .with_target_partitions(3)
            .with_batch_size(self.batch_size)
            .set_bool(
                "datafusion.execution.enable_migration_aggregate",
                self.enable_migration_aggregate,
            );
        let ctx = SessionContext::new_with_config(config);
        ctx.register_parquet(
            "lineitem",
            "tests/data/tpch_lineitem_small.parquet",
            ParquetReadOptions::default(),
        )
        .await?;
        let dataframe = ctx.sql(self.sql).await?;
        let plan = dataframe.create_physical_plan().await?;
        let output = collect(plan.clone(), ctx.task_ctx()).await?;
        assert!(!output.is_empty());

        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        Ok(DisplayableExecutionPlan::with_metrics(plan.as_ref())
            .set_metric_names(vec![
                "hash_rows_reused".into(),
                "hash_rows_computed".into(),
            ])
            .indent(true)
            .to_string()
            .replace(manifest_dir, "$DATAFUSION_CORE")
            .replace(manifest_dir.trim_start_matches('/'), "$DATAFUSION_CORE"))
    }
}
