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

#[tokio::test]
#[ignore]
/// TODO: need to repair. Wrong Test: ambiguous column name: a
async fn nestedjoin_with_alias() -> Result<()> {
    // repro case for https://github.com/apache/arrow-datafusion/issues/2867
    let sql = "select * from ((select 1 as a, 2 as b) c INNER JOIN (select 1 as a, 3 as d) e on c.a = e.a) f;";
    let expected = vec![
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
async fn join_timestamp() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_timestamps()).unwrap();

    let expected = vec![
        "+-------------------------------+----------------------------+-------------------------+---------------------+-------+-------------------------------+----------------------------+-------------------------+---------------------+-------+",
        "| nanos                         | micros                     | millis                  | secs                | name  | nanos                         | micros                     | millis                  | secs                | name  |",
        "+-------------------------------+----------------------------+-------------------------+---------------------+-------+-------------------------------+----------------------------+-------------------------+---------------------+-------+",
        "| 2011-12-13T11:13:10.123450    | 2011-12-13T11:13:10.123450 | 2011-12-13T11:13:10.123 | 2011-12-13T11:13:10 | Row 1 | 2011-12-13T11:13:10.123450    | 2011-12-13T11:13:10.123450 | 2011-12-13T11:13:10.123 | 2011-12-13T11:13:10 | Row 1 |",
        "| 2018-11-13T17:11:10.011375885 | 2018-11-13T17:11:10.011375 | 2018-11-13T17:11:10.011 | 2018-11-13T17:11:10 | Row 0 | 2018-11-13T17:11:10.011375885 | 2018-11-13T17:11:10.011375 | 2018-11-13T17:11:10.011 | 2018-11-13T17:11:10 | Row 0 |",
        "| 2021-01-01T05:11:10.432       | 2021-01-01T05:11:10.432    | 2021-01-01T05:11:10.432 | 2021-01-01T05:11:10 | Row 3 | 2021-01-01T05:11:10.432       | 2021-01-01T05:11:10.432    | 2021-01-01T05:11:10.432 | 2021-01-01T05:11:10 | Row 3 |",
        "+-------------------------------+----------------------------+-------------------------+---------------------+-------+-------------------------------+----------------------------+-------------------------+---------------------+-------+",
    ];

    let results = execute_to_batches(
        &ctx,
        "SELECT * FROM t as t1  \
         JOIN (SELECT * FROM t) as t2 \
         ON t1.nanos = t2.nanos",
    )
    .await;

    assert_batches_sorted_eq!(expected, &results);

    let results = execute_to_batches(
        &ctx,
        "SELECT * FROM t as t1  \
         JOIN (SELECT * FROM t) as t2 \
         ON t1.micros = t2.micros",
    )
    .await;

    assert_batches_sorted_eq!(expected, &results);

    let results = execute_to_batches(
        &ctx,
        "SELECT * FROM t as t1  \
         JOIN (SELECT * FROM t) as t2 \
         ON t1.millis = t2.millis",
    )
    .await;

    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn left_join_using_2() -> Result<()> {
    let results = execute_with_partition(
        "SELECT t1.c1, t2.c2 FROM test t1 JOIN test t2 USING (c2) ORDER BY t2.c2",
        1,
    )
    .await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+----+----+",
        "| c1 | c2 |",
        "+----+----+",
        "| 0  | 1  |",
        "| 0  | 2  |",
        "| 0  | 3  |",
        "| 0  | 4  |",
        "| 0  | 5  |",
        "| 0  | 6  |",
        "| 0  | 7  |",
        "| 0  | 8  |",
        "| 0  | 9  |",
        "| 0  | 10 |",
        "+----+----+",
    ];

    assert_batches_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn left_join_using_join_key_projection() -> Result<()> {
    let results = execute_with_partition(
        "SELECT t1.c1, t1.c2, t2.c2 FROM test t1 JOIN test t2 USING (c2) ORDER BY t2.c2",
        1,
    )
    .await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+----+----+----+",
        "| c1 | c2 | c2 |",
        "+----+----+----+",
        "| 0  | 1  | 1  |",
        "| 0  | 2  | 2  |",
        "| 0  | 3  | 3  |",
        "| 0  | 4  | 4  |",
        "| 0  | 5  | 5  |",
        "| 0  | 6  | 6  |",
        "| 0  | 7  | 7  |",
        "| 0  | 8  | 8  |",
        "| 0  | 9  | 9  |",
        "| 0  | 10 | 10 |",
        "+----+----+----+",
    ];

    assert_batches_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn left_join_2() -> Result<()> {
    let results = execute_with_partition(
        "SELECT t1.c1, t1.c2, t2.c2 FROM test t1 JOIN test t2 ON t1.c2 = t2.c2 ORDER BY t1.c2",
        1,
    )
        .await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+----+----+----+",
        "| c1 | c2 | c2 |",
        "+----+----+----+",
        "| 0  | 1  | 1  |",
        "| 0  | 2  | 2  |",
        "| 0  | 3  | 3  |",
        "| 0  | 4  | 4  |",
        "| 0  | 5  | 5  |",
        "| 0  | 6  | 6  |",
        "| 0  | 7  | 7  |",
        "| 0  | 8  | 8  |",
        "| 0  | 9  | 9  |",
        "| 0  | 10 | 10 |",
        "+----+----+----+",
    ];

    assert_batches_eq!(expected, &results);
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
async fn hash_join_with_date32() -> Result<()> {
    let ctx = create_hashjoin_datatype_context()?;

    // inner join on hash supported data type (Date32)
    let sql = "select * from t1 join t2 on t1.c1 = t2.c1";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(&("explain ".to_owned() + sql)).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Inner Join: t1.c1 = t2.c1 [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N, c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "    TableScan: t1 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "    TableScan: t2 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let expected = vec![
        "+------------+---------------------+---------+-----+------------+---------------------+---------+-----+",
        "| c1         | c2                  | c3      | c4  | c1         | c2                  | c3      | c4  |",
        "+------------+---------------------+---------+-----+------------+---------------------+---------+-----+",
        "| 1970-01-02 | 1970-01-02T00:00:00 | 1.23    | abc | 1970-01-02 | 1970-01-02T00:00:00 | -123.12 | abc |",
        "| 1970-01-04 |                     | -123.12 | jkl | 1970-01-04 |                     | 789.00  |     |",
        "+------------+---------------------+---------+-----+------------+---------------------+---------+-----+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn hash_join_with_date64() -> Result<()> {
    let ctx = create_hashjoin_datatype_context()?;

    // left join on hash supported data type (Date64)
    let sql = "select * from t1 left join t2 on t1.c2 = t2.c2";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(&("explain ".to_owned() + sql)).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Left Join: t1.c2 = t2.c2 [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N, c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "    TableScan: t1 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "    TableScan: t2 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let expected = vec![
        "+------------+---------------------+---------+-----+------------+---------------------+---------+--------+",
        "| c1         | c2                  | c3      | c4  | c1         | c2                  | c3      | c4     |",
        "+------------+---------------------+---------+-----+------------+---------------------+---------+--------+",
        "|            | 1970-01-04T00:00:00 | 789.00  | ghi |            | 1970-01-04T00:00:00 | 0.00    | qwerty |",
        "| 1970-01-02 | 1970-01-02T00:00:00 | 1.23    | abc | 1970-01-02 | 1970-01-02T00:00:00 | -123.12 | abc    |",
        "| 1970-01-03 | 1970-01-03T00:00:00 | 456.00  | def |            |                     |         |        |",
        "| 1970-01-04 |                     | -123.12 | jkl |            |                     |         |        |",
        "+------------+---------------------+---------+-----+------------+---------------------+---------+--------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn hash_join_with_decimal() -> Result<()> {
    let ctx = create_hashjoin_datatype_context()?;

    // right join on hash supported data type (Decimal)
    let sql = "select * from t1 right join t2 on t1.c3 = t2.c3";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(&("explain ".to_owned() + sql)).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Right Join: CAST(t1.c3 AS Decimal128(10, 2)) = t2.c3 [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N, c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "    TableScan: t1 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "    TableScan: t2 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let expected = vec![
        "+------------+---------------------+---------+-----+------------+---------------------+-----------+---------+",
        "| c1         | c2                  | c3      | c4  | c1         | c2                  | c3        | c4      |",
        "+------------+---------------------+---------+-----+------------+---------------------+-----------+---------+",
        "|            |                     |         |     |            |                     | 100000.00 | abcdefg |",
        "|            |                     |         |     |            | 1970-01-04T00:00:00 | 0.00      | qwerty  |",
        "|            | 1970-01-04T00:00:00 | 789.00  | ghi | 1970-01-04 |                     | 789.00    |         |",
        "| 1970-01-04 |                     | -123.12 | jkl | 1970-01-02 | 1970-01-02T00:00:00 | -123.12   | abc     |",
        "+------------+---------------------+---------+-----+------------+---------------------+-----------+---------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn hash_join_with_dictionary() -> Result<()> {
    let ctx = create_hashjoin_datatype_context()?;

    // inner join on hash supported data type (Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)))
    let sql = "select * from t1 join t2 on t1.c4 = t2.c4";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(&("explain ".to_owned() + sql)).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Inner Join: t1.c4 = t2.c4 [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N, c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "    TableScan: t1 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "    TableScan: t2 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let expected = vec![
        "+------------+---------------------+------+-----+------------+---------------------+---------+-----+",
        "| c1         | c2                  | c3   | c4  | c1         | c2                  | c3      | c4  |",
        "+------------+---------------------+------+-----+------------+---------------------+---------+-----+",
        "| 1970-01-02 | 1970-01-02T00:00:00 | 1.23 | abc | 1970-01-02 | 1970-01-02T00:00:00 | -123.12 | abc |",
        "+------------+---------------------+------+-----+------------+---------------------+---------+-----+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn sort_merge_join_on_date32() -> Result<()> {
    let ctx = create_sort_merge_join_datatype_context()?;

    // inner sort merge join on data type (Date32)
    let sql = "select * from t1 join t2 on t1.c1 = t2.c1";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let expected = vec![
        "SortMergeJoin: join_type=Inner, on=[(Column { name: \"c1\", index: 0 }, Column { name: \"c1\", index: 0 })]",
        "  SortExec: expr=[c1@0 ASC]",
        "    CoalesceBatchesExec: target_batch_size=4096",
        "      RepartitionExec: partitioning=Hash([Column { name: \"c1\", index: 0 }], 2), input_partitions=2",
        "        RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
        "          MemoryExec: partitions=1, partition_sizes=[1]",
        "  SortExec: expr=[c1@0 ASC]",
        "    CoalesceBatchesExec: target_batch_size=4096",
        "      RepartitionExec: partitioning=Hash([Column { name: \"c1\", index: 0 }], 2), input_partitions=2",
        "        RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
        "          MemoryExec: partitions=1, partition_sizes=[1]",
    ];
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let expected = vec![
        "+------------+---------------------+---------+-----+------------+---------------------+---------+-----+",
        "| c1         | c2                  | c3      | c4  | c1         | c2                  | c3      | c4  |",
        "+------------+---------------------+---------+-----+------------+---------------------+---------+-----+",
        "| 1970-01-02 | 1970-01-02T00:00:00 | 1.23    | abc | 1970-01-02 | 1970-01-02T00:00:00 | -123.12 | abc |",
        "| 1970-01-04 |                     | -123.12 | jkl | 1970-01-04 |                     | 789.00  |     |",
        "+------------+---------------------+---------+-----+------------+---------------------+---------+-----+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn sort_merge_join_on_decimal() -> Result<()> {
    let ctx = create_sort_merge_join_datatype_context()?;

    // right join on data type (Decimal)
    let sql = "select * from t1 right join t2 on t1.c3 = t2.c3";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let expected = vec![
        "ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3, c4@3 as c4, c1@5 as c1, c2@6 as c2, c3@7 as c3, c4@8 as c4]",
        "  SortMergeJoin: join_type=Right, on=[(Column { name: \"CAST(t1.c3 AS Decimal128(10, 2))\", index: 4 }, Column { name: \"c3\", index: 2 })]",
        "    SortExec: expr=[CAST(t1.c3 AS Decimal128(10, 2))@4 ASC]",
        "      CoalesceBatchesExec: target_batch_size=4096",
        "        RepartitionExec: partitioning=Hash([Column { name: \"CAST(t1.c3 AS Decimal128(10, 2))\", index: 4 }], 2), input_partitions=2",
        "          ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3, c4@3 as c4, CAST(c3@2 AS Decimal128(10, 2)) as CAST(t1.c3 AS Decimal128(10, 2))]",
        "            RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
        "              MemoryExec: partitions=1, partition_sizes=[1]",
        "    SortExec: expr=[c3@2 ASC]",
        "      CoalesceBatchesExec: target_batch_size=4096",
        "        RepartitionExec: partitioning=Hash([Column { name: \"c3\", index: 2 }], 2), input_partitions=2",
        "          RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
        "            MemoryExec: partitions=1, partition_sizes=[1]",
    ];
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let expected = vec![
        "+------------+---------------------+---------+-----+------------+---------------------+-----------+---------+",
        "| c1         | c2                  | c3      | c4  | c1         | c2                  | c3        | c4      |",
        "+------------+---------------------+---------+-----+------------+---------------------+-----------+---------+",
        "|            |                     |         |     |            |                     | 100000.00 | abcdefg |",
        "|            |                     |         |     |            | 1970-01-04T00:00:00 | 0.00      | qwerty  |",
        "|            | 1970-01-04T00:00:00 | 789.00  | ghi | 1970-01-04 |                     | 789.00    |         |",
        "| 1970-01-04 |                     | -123.12 | jkl | 1970-01-02 | 1970-01-02T00:00:00 | -123.12   | abc     |",
        "+------------+---------------------+---------+-----+------------+---------------------+-----------+---------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn left_semi_join() -> Result<()> {
    let test_repartition_joins = vec![true, false];
    for repartition_joins in test_repartition_joins {
        let ctx = create_left_semi_anti_join_context_with_null_ids(
            "t1_id",
            "t2_id",
            repartition_joins,
        )
        .unwrap();

        let sql = "SELECT t1_id, t1_name FROM t1 WHERE t1_id IN (SELECT t2_id FROM t2) ORDER BY t1_id";
        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let expected = if repartition_joins {
            vec![
                "SortPreservingMergeExec: [t1_id@0 ASC NULLS LAST]",
                "  SortExec: expr=[t1_id@0 ASC NULLS LAST]",
                "    CoalesceBatchesExec: target_batch_size=4096",
                "      HashJoinExec: mode=Partitioned, join_type=LeftSemi, on=[(Column { name: \"t1_id\", index: 0 }, Column { name: \"t2_id\", index: 0 })]",
                "        CoalesceBatchesExec: target_batch_size=4096",
                "          RepartitionExec: partitioning=Hash([Column { name: \"t1_id\", index: 0 }], 2), input_partitions=2",
                "            RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
                "              MemoryExec: partitions=1, partition_sizes=[1]",
                "        CoalesceBatchesExec: target_batch_size=4096",
                "          RepartitionExec: partitioning=Hash([Column { name: \"t2_id\", index: 0 }], 2), input_partitions=2",
                "            RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
                "              MemoryExec: partitions=1, partition_sizes=[1]",
            ]
        } else {
            vec![
                "SortExec: expr=[t1_id@0 ASC NULLS LAST]",
                "  CoalesceBatchesExec: target_batch_size=4096",
                "    HashJoinExec: mode=CollectLeft, join_type=LeftSemi, on=[(Column { name: \"t1_id\", index: 0 }, Column { name: \"t2_id\", index: 0 })]",
                "      MemoryExec: partitions=1, partition_sizes=[1]",
                "      MemoryExec: partitions=1, partition_sizes=[1]",
            ]
        };
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+-------+---------+",
            "| t1_id | t1_name |",
            "+-------+---------+",
            "| 11    | a       |",
            "| 11    | a       |",
            "| 22    | b       |",
            "| 44    | d       |",
            "+-------+---------+",
        ];
        assert_batches_eq!(expected, &actual);

        let sql = "SELECT t1_id, t1_name FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t1_id = t2_id) ORDER BY t1_id";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+-------+---------+",
            "| t1_id | t1_name |",
            "+-------+---------+",
            "| 11    | a       |",
            "| 11    | a       |",
            "| 22    | b       |",
            "| 44    | d       |",
            "+-------+---------+",
        ];
        assert_batches_eq!(expected, &actual);

        let sql = "SELECT t1_id FROM t1 INTERSECT SELECT t2_id FROM t2 ORDER BY t1_id";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+-------+",
            "| t1_id |",
            "+-------+",
            "| 11    |",
            "| 22    |",
            "| 44    |",
            "|       |",
            "+-------+",
        ];
        assert_batches_eq!(expected, &actual);

        let sql = "SELECT t1_id, t1_name FROM t1 LEFT SEMI JOIN t2 ON (t1_id = t2_id) ORDER BY t1_id";
        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let expected = if repartition_joins {
            vec![
                "SortPreservingMergeExec: [t1_id@0 ASC NULLS LAST]",
                "  SortExec: expr=[t1_id@0 ASC NULLS LAST]",
                "    CoalesceBatchesExec: target_batch_size=4096",
                "      HashJoinExec: mode=Partitioned, join_type=LeftSemi, on=[(Column { name: \"t1_id\", index: 0 }, Column { name: \"t2_id\", index: 0 })]",
                "        CoalesceBatchesExec: target_batch_size=4096",
                "          RepartitionExec: partitioning=Hash([Column { name: \"t1_id\", index: 0 }], 2), input_partitions=2",
                "            RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
                "              MemoryExec: partitions=1, partition_sizes=[1]",
                "        CoalesceBatchesExec: target_batch_size=4096",
                "          RepartitionExec: partitioning=Hash([Column { name: \"t2_id\", index: 0 }], 2), input_partitions=2",
                "            RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
                "              MemoryExec: partitions=1, partition_sizes=[1]",
            ]
        } else {
            vec![
                "SortExec: expr=[t1_id@0 ASC NULLS LAST]",
                "  CoalesceBatchesExec: target_batch_size=4096",
                "    HashJoinExec: mode=CollectLeft, join_type=LeftSemi, on=[(Column { name: \"t1_id\", index: 0 }, Column { name: \"t2_id\", index: 0 })]",
                "      MemoryExec: partitions=1, partition_sizes=[1]",
                "      MemoryExec: partitions=1, partition_sizes=[1]",
            ]
        };
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+-------+---------+",
            "| t1_id | t1_name |",
            "+-------+---------+",
            "| 11    | a       |",
            "| 11    | a       |",
            "| 22    | b       |",
            "| 44    | d       |",
            "+-------+---------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

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
        let expected = vec!["++", "++"];
        assert_batches_eq!(expected, &actual);
    }

    Ok(())
}

#[tokio::test]
async fn right_semi_join() -> Result<()> {
    let test_repartition_joins = vec![true, false];
    for repartition_joins in test_repartition_joins {
        let ctx = create_right_semi_anti_join_context_with_null_ids(
            "t1_id",
            "t2_id",
            repartition_joins,
        )
        .unwrap();

        let sql = "SELECT t1_id, t1_name, t1_int FROM t1 WHERE EXISTS (SELECT * FROM t2 where t2.t2_id = t1.t1_id and t2.t2_name <> t1.t1_name) ORDER BY t1_id";
        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let expected = if repartition_joins {
            vec![
                "SortPreservingMergeExec: [t1_id@0 ASC NULLS LAST]",
                 "  SortExec: expr=[t1_id@0 ASC NULLS LAST]",
                 "    CoalesceBatchesExec: target_batch_size=4096",
                 "      HashJoinExec: mode=Partitioned, join_type=RightSemi, on=[(Column { name: \"t2_id\", index: 0 }, Column { name: \"t1_id\", index: 0 })], filter=t2_name@1 != t1_name@0",
                 "        CoalesceBatchesExec: target_batch_size=4096",
                 "          RepartitionExec: partitioning=Hash([Column { name: \"t2_id\", index: 0 }], 2), input_partitions=2",
                 "            RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
                 "              MemoryExec: partitions=1, partition_sizes=[1]",
                 "        CoalesceBatchesExec: target_batch_size=4096",
                 "          RepartitionExec: partitioning=Hash([Column { name: \"t1_id\", index: 0 }], 2), input_partitions=2",
                 "            RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
                 "              MemoryExec: partitions=1, partition_sizes=[1]",
            ]
        } else {
            vec![
                "SortExec: expr=[t1_id@0 ASC NULLS LAST]",
                "  CoalesceBatchesExec: target_batch_size=4096",
                "    HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(Column { name: \"t2_id\", index: 0 }, Column { name: \"t1_id\", index: 0 })], filter=t2_name@1 != t1_name@0",
                "      MemoryExec: partitions=1, partition_sizes=[1]",
                "      MemoryExec: partitions=1, partition_sizes=[1]",
            ]
        };
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+-------+---------+--------+",
            "| t1_id | t1_name | t1_int |",
            "+-------+---------+--------+",
            "| 11    | a       | 1      |",
            "+-------+---------+--------+",
        ];
        assert_batches_eq!(expected, &actual);

        let sql = "SELECT t1_id, t1_name, t1_int FROM t2 RIGHT SEMI JOIN t1 on (t2.t2_id = t1.t1_id and t2.t2_name <> t1.t1_name) ORDER BY t1_id";
        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let expected = if repartition_joins {
            vec![
                "SortPreservingMergeExec: [t1_id@0 ASC NULLS LAST]",
                "  SortExec: expr=[t1_id@0 ASC NULLS LAST]",
                "    CoalesceBatchesExec: target_batch_size=4096",
                "      HashJoinExec: mode=Partitioned, join_type=RightSemi, on=[(Column { name: \"t2_id\", index: 0 }, Column { name: \"t1_id\", index: 0 })], filter=t2_name@0 != t1_name@1",
                "        CoalesceBatchesExec: target_batch_size=4096",
                "          RepartitionExec: partitioning=Hash([Column { name: \"t2_id\", index: 0 }], 2), input_partitions=2",
                "            RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
                "              MemoryExec: partitions=1, partition_sizes=[1]",
                "        CoalesceBatchesExec: target_batch_size=4096",
                "          RepartitionExec: partitioning=Hash([Column { name: \"t1_id\", index: 0 }], 2), input_partitions=2",
                "            RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
                "              MemoryExec: partitions=1, partition_sizes=[1]",
            ]
        } else {
            vec![
                "SortExec: expr=[t1_id@0 ASC NULLS LAST]",
                "  CoalesceBatchesExec: target_batch_size=4096",
                "    HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(Column { name: \"t2_id\", index: 0 }, Column { name: \"t1_id\", index: 0 })], filter=t2_name@0 != t1_name@1",
                "      MemoryExec: partitions=1, partition_sizes=[1]",
                "      MemoryExec: partitions=1, partition_sizes=[1]",
            ]
        };
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+-------+---------+--------+",
            "| t1_id | t1_name | t1_int |",
            "+-------+---------+--------+",
            "| 11    | a       | 1      |",
            "+-------+---------+--------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    Ok(())
}

#[tokio::test]
async fn right_as_inner_table_nested_loop_join() -> Result<()> {
    let ctx = create_nested_loop_join_context()?;

    // Distribution: left is `UnspecifiedDistribution`, right is `SinglePartition`.
    let sql = "SELECT t1.t1_id, t2.t2_id
                     FROM t1 INNER JOIN t2 ON t1.t1_id > t2.t2_id
                     WHERE t1.t1_id > 10 AND t2.t2_int > 1";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;

    // right is single partition side, so it will be visited many times.
    let expected = vec![
        "NestedLoopJoinExec: join_type=Inner, filter=BinaryExpr { left: Column { name: \"t1_id\", index: 0 }, op: Gt, right: Column { name: \"t2_id\", index: 1 } }",
        "  CoalesceBatchesExec: target_batch_size=4096",
        "    FilterExec: t1_id@0 > 10",
        "      RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1",
        "        MemoryExec: partitions=1, partition_sizes=[1]",
        "  CoalescePartitionsExec",
        "    ProjectionExec: expr=[t2_id@0 as t2_id]",
        "      CoalesceBatchesExec: target_batch_size=4096",
        "        FilterExec: t2_int@1 > 1",
        "          RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1",
        "            MemoryExec: partitions=1, partition_sizes=[1]",
    ];
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let expected = vec![
        "+-------+-------+",
        "| t1_id | t2_id |",
        "+-------+-------+",
        "| 22    | 11    |",
        "| 33    | 11    |",
        "| 44    | 11    |",
        "+-------+-------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn left_as_inner_table_nested_loop_join() -> Result<()> {
    let ctx = create_nested_loop_join_context()?;

    // Distribution: left is `SinglePartition`, right is `UnspecifiedDistribution`.
    let sql = "SELECT t1.t1_id,t2.t2_id FROM (select t1_id from t1 where t1.t1_id > 22) as t1
                                                 RIGHT JOIN (select t2_id from t2 where t2.t2_id > 11) as t2
                                                 ON t1.t1_id < t2.t2_id";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;

    // left is single partition side, so it will be visited many times.
    let expected = vec![
        "NestedLoopJoinExec: join_type=Right, filter=BinaryExpr { left: Column { name: \"t1_id\", index: 0 }, op: Lt, right: Column { name: \"t2_id\", index: 1 } }",
        "  CoalescePartitionsExec",
        "    CoalesceBatchesExec: target_batch_size=4096",
        "      FilterExec: t1_id@0 > 22",
        "        RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1",
        "          MemoryExec: partitions=1, partition_sizes=[1]",
        "  CoalesceBatchesExec: target_batch_size=4096",
        "    FilterExec: t2_id@0 > 11",
        "      RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1",
        "        MemoryExec: partitions=1, partition_sizes=[1]",
    ];
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();

    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let expected = vec![
        "+-------+-------+",
        "| t1_id | t2_id |",
        "+-------+-------+",
        "|       | 22    |",
        "| 33    | 44    |",
        "| 33    | 55    |",
        "| 44    | 55    |",
        "+-------+-------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn exists_distinct_subquery_to_join() -> Result<()> {
    let test_repartition_joins = vec![true, false];
    for repartition_joins in test_repartition_joins {
        let ctx = create_join_context("t1_id", "t2_id", repartition_joins)?;

        let sql = "SELECT * FROM t1 WHERE NOT EXISTS(SELECT DISTINCT t2_int FROM t2 WHERE t1.t1_id + 1 > t2.t2_id * 2)";
        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(&("explain ".to_owned() + sql)).await.expect(&msg);
        let plan = dataframe.into_optimized_plan()?;

        let expected = vec![
            "Explain [plan_type:Utf8, plan:Utf8]",
            "  LeftAnti Join:  Filter: CAST(t1.t1_id AS Int64) + Int64(1) > CAST(__correlated_sq_1.t2_id AS Int64) * Int64(2) [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
            "    TableScan: t1 projection=[t1_id, t1_name, t1_int] [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
            "    SubqueryAlias: __correlated_sq_1 [t2_id:UInt32;N]",
            "      Aggregate: groupBy=[[t2.t2_id]], aggr=[[]] [t2_id:UInt32;N]",
            "        TableScan: t2 projection=[t2_id] [t2_id:UInt32;N]",
        ];
        let formatted = plan.display_indent_schema().to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        let expected = vec![
            "+-------+---------+--------+",
            "| t1_id | t1_name | t1_int |",
            "+-------+---------+--------+",
            "| 11    | a       | 1      |",
            "+-------+---------+--------+",
        ];

        let results = execute_to_batches(&ctx, sql).await;
        assert_batches_sorted_eq!(expected, &results);
    }

    Ok(())
}

#[tokio::test]
async fn exists_distinct_subquery_to_join_with_expr() -> Result<()> {
    let test_repartition_joins = vec![true, false];
    for repartition_joins in test_repartition_joins {
        let ctx = create_join_context("t1_id", "t2_id", repartition_joins)?;

        // `t2_id + t2_int` is in the subquery project.
        let sql = "SELECT * FROM t1 WHERE NOT EXISTS(SELECT DISTINCT t2_id + t2_int, t2_int FROM t2 WHERE t1.t1_id + 1 > t2.t2_id * 2)";
        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(&("explain ".to_owned() + sql)).await.expect(&msg);
        let plan = dataframe.into_optimized_plan()?;

        let expected = vec![
            "Explain [plan_type:Utf8, plan:Utf8]",
            "  LeftAnti Join:  Filter: CAST(t1.t1_id AS Int64) + Int64(1) > CAST(__correlated_sq_1.t2_id AS Int64) * Int64(2) [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
            "    TableScan: t1 projection=[t1_id, t1_name, t1_int] [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
            "    SubqueryAlias: __correlated_sq_1 [t2_id:UInt32;N]",
            "      Aggregate: groupBy=[[t2.t2_id]], aggr=[[]] [t2_id:UInt32;N]",
            "        TableScan: t2 projection=[t2_id] [t2_id:UInt32;N]",
        ];
        let formatted = plan.display_indent_schema().to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        let expected = vec![
            "+-------+---------+--------+",
            "| t1_id | t1_name | t1_int |",
            "+-------+---------+--------+",
            "| 11    | a       | 1      |",
            "+-------+---------+--------+",
        ];

        let results = execute_to_batches(&ctx, sql).await;
        assert_batches_sorted_eq!(expected, &results);
    }

    Ok(())
}
