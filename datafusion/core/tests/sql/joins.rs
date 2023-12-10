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

use datafusion::test_util::register_unbounded_file_with_ordering;

use super::*;

#[tokio::test]
#[ignore]
/// TODO: need to repair. Wrong Test: ambiguous column name: a
async fn nestedjoin_with_alias() -> Result<()> {
    // repro case for https://github.com/apache/arrow-datafusion/issues/2867
    let sql = "select * from ((select 1 as a, 2 as b) c INNER JOIN (select 1 as a, 3 as d) e on c.a = e.a) f;";
    let expected = [
        "+---+---+---+---+",
        "| a | b | a | d |",
        "+---+---+---+---+",
        "| 1 | 2 | 1 | 3 |",
        "+---+---+---+---+",
    ];
    let ctx = SessionContext::new();
    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn join_partitioned() -> Result<()> {
    // self join on partition id (workaround for duplicate column name)
    let results = execute_with_partition(
        "SELECT 1 FROM test JOIN (SELECT c1 AS id1 FROM test) AS a ON c1=id1",
        4,
    )
    .await?;

    assert_eq!(
        results.iter().map(|b| b.num_rows()).sum::<usize>(),
        4 * 10 * 10
    );

    Ok(())
}

#[tokio::test]
#[ignore = "Test ignored, will be enabled after fixing the NAAJ bug"]
// https://github.com/apache/arrow-datafusion/issues/4211
async fn null_aware_left_anti_join() -> Result<()> {
    let test_repartition_joins = vec![true, false];
    for repartition_joins in test_repartition_joins {
        let ctx = create_left_semi_anti_join_context_with_null_ids(
            "t1_id",
            "t2_id",
            repartition_joins,
        )
        .unwrap();

        let sql = "SELECT t1_id, t1_name FROM t1 WHERE t1_id NOT IN (SELECT t2_id FROM t2) ORDER BY t1_id";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = ["++", "++"];
        assert_batches_eq!(expected, &actual);
    }

    Ok(())
}

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
    let file_sort_order = vec![[datafusion_expr::col("a1")]
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
        true,
    )
    .await?;
    let right_file_path = tmp_dir.path().join("right.csv");
    File::create(right_file_path.clone()).unwrap();
    register_unbounded_file_with_ordering(
        &ctx,
        schema,
        &right_file_path,
        "right",
        file_sort_order,
        true,
    )
    .await?;
    let sql = "SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10";
    let dataframe = ctx.sql(sql).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
    let expected = {
        [
            "SymmetricHashJoinExec: mode=Partitioned, join_type=Full, on=[(a2@1, a2@1)], filter=CAST(a1@0 AS Int64) > CAST(a1@1 AS Int64) + 3 AND CAST(a1@0 AS Int64) < CAST(a1@1 AS Int64) + 10",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            // "     CsvExec: file_groups={1 group: [[tempdir/left.csv]]}, projection=[a1, a2], has_header=false",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            // "     CsvExec: file_groups={1 group: [[tempdir/right.csv]]}, projection=[a1, a2], has_header=false"
        ]
    };
    let mut actual: Vec<&str> = formatted.trim().lines().collect();
    // Remove CSV lines
    actual.remove(4);
    actual.remove(7);

    assert_eq!(
        expected,
        actual[..],
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
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
    ctx.register_csv(
        "left",
        left_file_path.as_os_str().to_str().unwrap(),
        CsvReadOptions::new().schema(&schema).mark_infinite(true),
    )
    .await?;
    let right_file_path = tmp_dir.path().join("right.csv");
    File::create(right_file_path.clone())?;
    ctx.register_csv(
        "right",
        right_file_path.as_os_str().to_str().unwrap(),
        CsvReadOptions::new().schema(&schema).mark_infinite(true),
    )
    .await?;
    let sql = "SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10";
    let dataframe = ctx.sql(sql).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
    let expected = {
        [
            "SymmetricHashJoinExec: mode=Partitioned, join_type=Full, on=[(a2@1, a2@1)], filter=CAST(a1@0 AS Int64) > CAST(a1@1 AS Int64) + 3 AND CAST(a1@0 AS Int64) < CAST(a1@1 AS Int64) + 10",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            // "     CsvExec: file_groups={1 group: [[tempdir/left.csv]]}, projection=[a1, a2], has_header=false",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    RepartitionExec: partitioning=Hash([a2@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            // "     CsvExec: file_groups={1 group: [[tempdir/right.csv]]}, projection=[a1, a2], has_header=false"
        ]
    };
    let mut actual: Vec<&str> = formatted.trim().lines().collect();
    // Remove CSV lines
    actual.remove(4);
    actual.remove(7);

    assert_eq!(
        expected,
        actual[..],
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
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
    ctx.register_csv(
        "left",
        left_file_path.as_os_str().to_str().unwrap(),
        CsvReadOptions::new().schema(&schema).mark_infinite(true),
    )
    .await?;
    let right_file_path = tmp_dir.path().join("right.csv");
    File::create(right_file_path.clone())?;
    ctx.register_csv(
        "right",
        right_file_path.as_os_str().to_str().unwrap(),
        CsvReadOptions::new().schema(&schema).mark_infinite(true),
    )
    .await?;
    let df = ctx.sql("SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10").await?;
    match df.create_physical_plan().await {
        Ok(_) => panic!("Expecting error."),
        Err(e) => {
            assert_eq!(e.strip_backtrace(), "PipelineChecker\ncaused by\nError during planning: Join operation cannot operate on a non-prunable stream without enabling the 'allow_symmetric_joins_without_pruning' configuration flag")
        }
    }
    Ok(())
}
