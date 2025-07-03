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

use insta::assert_snapshot;

use datafusion::assert_batches_eq;
use datafusion::datasource::stream::{FileStreamProvider, StreamConfig, StreamTable};
use datafusion::test_util::register_unbounded_file_with_ordering;

use super::*;

#[tokio::test]
async fn join_change_in_planner() -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(8);
    let ctx = SessionContext::new_with_config(config);
    let tmp_dir = TempDir::new().unwrap();
    let left_file_path = tmp_dir.path().join("left.csv");
    File::create(left_file_path.clone()).unwrap();
    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::UInt32, false),
        Field::new("a2", DataType::UInt32, false),
    ]));
    // Specify the ordering:
    let file_sort_order = vec![[col("a1")]
        .into_iter()
        .map(|e| {
            let ascending = true;
            let nulls_first = false;
            e.sort(ascending, nulls_first)
        })
        .collect::<Vec<_>>()];
    register_unbounded_file_with_ordering(
        &ctx,
        schema.clone(),
        &left_file_path,
        "left",
        file_sort_order.clone(),
    )?;
    let right_file_path = tmp_dir.path().join("right.csv");
    File::create(right_file_path.clone()).unwrap();
    register_unbounded_file_with_ordering(
        &ctx,
        schema,
        &right_file_path,
        "right",
        file_sort_order,
    )?;
    let sql = "SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10";
    let dataframe = ctx.sql(sql).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
    let actual = formatted.trim();

    assert_snapshot!(
        actual,
        @r"
    SymmetricHashJoinExec: mode=Partitioned, join_type=Full, on=[(a2@1, a2@1)], filter=CAST(a1@0 AS Int64) > CAST(a1@1 AS Int64) + 3 AND CAST(a1@0 AS Int64) < CAST(a1@1 AS Int64) + 10
      CoalesceBatchesExec: target_batch_size=8192
        RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a1@0 ASC NULLS LAST
          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1
            StreamingTableExec: partition_sizes=1, projection=[a1, a2], infinite_source=true, output_ordering=[a1@0 ASC NULLS LAST]
      CoalesceBatchesExec: target_batch_size=8192
        RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a1@0 ASC NULLS LAST
          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1
            StreamingTableExec: partition_sizes=1, projection=[a1, a2], infinite_source=true, output_ordering=[a1@0 ASC NULLS LAST]
    "
    );
    Ok(())
}

#[tokio::test]
async fn join_no_order_on_filter() -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(8);
    let ctx = SessionContext::new_with_config(config);
    let tmp_dir = TempDir::new().unwrap();
    let left_file_path = tmp_dir.path().join("left.csv");
    File::create(left_file_path.clone()).unwrap();
    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::UInt32, false),
        Field::new("a2", DataType::UInt32, false),
        Field::new("a3", DataType::UInt32, false),
    ]));
    // Specify the ordering:
    let file_sort_order = vec![[col("a1")]
        .into_iter()
        .map(|e| {
            let ascending = true;
            let nulls_first = false;
            e.sort(ascending, nulls_first)
        })
        .collect::<Vec<_>>()];
    register_unbounded_file_with_ordering(
        &ctx,
        schema.clone(),
        &left_file_path,
        "left",
        file_sort_order.clone(),
    )?;
    let right_file_path = tmp_dir.path().join("right.csv");
    File::create(right_file_path.clone()).unwrap();
    register_unbounded_file_with_ordering(
        &ctx,
        schema,
        &right_file_path,
        "right",
        file_sort_order,
    )?;
    let sql = "SELECT * FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a3 > t2.a3 + 3 AND t1.a3 < t2.a3 + 10";
    let dataframe = ctx.sql(sql).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
    let actual = formatted.trim();

    assert_snapshot!(
        actual,
        @r"
    SymmetricHashJoinExec: mode=Partitioned, join_type=Full, on=[(a2@1, a2@1)], filter=CAST(a3@0 AS Int64) > CAST(a3@1 AS Int64) + 3 AND CAST(a3@0 AS Int64) < CAST(a3@1 AS Int64) + 10
      CoalesceBatchesExec: target_batch_size=8192
        RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=8
          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1
            StreamingTableExec: partition_sizes=1, projection=[a1, a2, a3], infinite_source=true, output_ordering=[a1@0 ASC NULLS LAST]
      CoalesceBatchesExec: target_batch_size=8192
        RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=8
          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1
            StreamingTableExec: partition_sizes=1, projection=[a1, a2, a3], infinite_source=true, output_ordering=[a1@0 ASC NULLS LAST]
    "
    );
    Ok(())
}

#[tokio::test]
async fn join_change_in_planner_without_sort() -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(8);
    let ctx = SessionContext::new_with_config(config);
    let tmp_dir = TempDir::new()?;
    let left_file_path = tmp_dir.path().join("left.csv");
    File::create(left_file_path.clone())?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::UInt32, false),
        Field::new("a2", DataType::UInt32, false),
    ]));
    let left_source = FileStreamProvider::new_file(schema.clone(), left_file_path);
    let left = StreamConfig::new(Arc::new(left_source));
    ctx.register_table("left", Arc::new(StreamTable::new(Arc::new(left))))?;

    let right_file_path = tmp_dir.path().join("right.csv");
    File::create(right_file_path.clone())?;
    let right_source = FileStreamProvider::new_file(schema, right_file_path);
    let right = StreamConfig::new(Arc::new(right_source));
    ctx.register_table("right", Arc::new(StreamTable::new(Arc::new(right))))?;
    let sql = "SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10";
    let dataframe = ctx.sql(sql).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
    let actual = formatted.trim();

    assert_snapshot!(
        actual,
        @r"
    SymmetricHashJoinExec: mode=Partitioned, join_type=Full, on=[(a2@1, a2@1)], filter=CAST(a1@0 AS Int64) > CAST(a1@1 AS Int64) + 3 AND CAST(a1@0 AS Int64) < CAST(a1@1 AS Int64) + 10
      CoalesceBatchesExec: target_batch_size=8192
        RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=8
          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1
            StreamingTableExec: partition_sizes=1, projection=[a1, a2], infinite_source=true
      CoalesceBatchesExec: target_batch_size=8192
        RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=8
          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1
            StreamingTableExec: partition_sizes=1, projection=[a1, a2], infinite_source=true
    "
    );
    Ok(())
}

#[tokio::test]
async fn join_change_in_planner_without_sort_not_allowed() -> Result<()> {
    let config = SessionConfig::new()
        .with_target_partitions(8)
        .with_allow_symmetric_joins_without_pruning(false);
    let ctx = SessionContext::new_with_config(config);
    let tmp_dir = TempDir::new()?;
    let left_file_path = tmp_dir.path().join("left.csv");
    File::create(left_file_path.clone())?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::UInt32, false),
        Field::new("a2", DataType::UInt32, false),
    ]));
    let left_source = FileStreamProvider::new_file(schema.clone(), left_file_path);
    let left = StreamConfig::new(Arc::new(left_source));
    ctx.register_table("left", Arc::new(StreamTable::new(Arc::new(left))))?;
    let right_file_path = tmp_dir.path().join("right.csv");
    File::create(right_file_path.clone())?;
    let right_source = FileStreamProvider::new_file(schema.clone(), right_file_path);
    let right = StreamConfig::new(Arc::new(right_source));
    ctx.register_table("right", Arc::new(StreamTable::new(Arc::new(right))))?;
    let df = ctx.sql("SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10").await?;
    match df.create_physical_plan().await {
        Ok(_) => panic!("Expecting error."),
        Err(e) => {
            assert_eq!(e.strip_backtrace(), "SanityCheckPlan\ncaused by\nError during planning: Join operation cannot operate on a non-prunable stream without enabling the 'allow_symmetric_joins_without_pruning' configuration flag")
        }
    }
    Ok(())
}

#[tokio::test]
async fn join_using_uppercase_column() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "UPPER",
        DataType::UInt32,
        false,
    )]));
    let tmp_dir = TempDir::new()?;
    let file_path = tmp_dir.path().join("uppercase-column.csv");
    let mut file = File::create(file_path.clone())?;
    file.write_all("0".as_bytes())?;
    drop(file);

    let ctx = SessionContext::new();
    ctx.register_csv(
        "test",
        file_path.to_str().unwrap(),
        CsvReadOptions::new().schema(&schema).has_header(false),
    )
    .await?;

    let dataframe = ctx
        .sql(
            r#"
        SELECT test."UPPER" FROM "test"
        INNER JOIN (
            SELECT test."UPPER" FROM "test"
        ) AS selection USING ("UPPER")
        ;
        "#,
        )
        .await?;

    assert_batches_eq!(
        [
            "+-------+",
            "| UPPER |",
            "+-------+",
            "| 0     |",
            "+-------+",
        ],
        &dataframe.collect().await?
    );

    Ok(())
}
