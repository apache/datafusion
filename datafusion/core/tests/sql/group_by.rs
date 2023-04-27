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
use datafusion::test_util::get_test_context2;

#[tokio::test]
async fn group_by_date_trunc() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("c2", DataType::UInt64, false),
        Field::new(
            "t1",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));

    // generate a partitioned file
    for partition in 0..4 {
        let filename = format!("partition-{}.{}", partition, "csv");
        let file_path = tmp_dir.path().join(filename);
        let mut file = File::create(file_path)?;

        // generate some data
        for i in 0..10 {
            let data = format!("{},2020-12-{}T00:00:00.000Z\n", i, i + 10);
            file.write_all(data.as_bytes())?;
        }
    }

    ctx.register_csv(
        "test",
        tmp_dir.path().to_str().unwrap(),
        CsvReadOptions::new().schema(&schema).has_header(false),
    )
    .await?;

    let results = plan_and_collect(
        &ctx,
        "SELECT date_trunc('week', t1) as week, SUM(c2) FROM test GROUP BY date_trunc('week', t1)",
    ).await?;

    let expected = vec![
        "+---------------------+--------------+",
        "| week                | SUM(test.c2) |",
        "+---------------------+--------------+",
        "| 2020-12-07T00:00:00 | 24           |",
        "| 2020-12-14T00:00:00 | 156          |",
        "+---------------------+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn group_by_dictionary() {
    async fn run_test_case<K: ArrowDictionaryKeyType>() {
        let ctx = SessionContext::new();

        // input data looks like:
        // A, 1
        // B, 2
        // A, 2
        // A, 4
        // C, 1
        // A, 1

        let dict_array: DictionaryArray<K> =
            vec!["A", "B", "A", "A", "C", "A"].into_iter().collect();
        let dict_array = Arc::new(dict_array);

        let val_array: Int64Array = vec![1, 2, 2, 4, 1, 1].into();
        let val_array = Arc::new(val_array);

        let schema = Arc::new(Schema::new(vec![
            Field::new("dict", dict_array.data_type().clone(), false),
            Field::new("val", val_array.data_type().clone(), false),
        ]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![dict_array, val_array]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        let results =
            plan_and_collect(&ctx, "SELECT dict, count(val) FROM t GROUP BY dict")
                .await
                .expect("ran plan correctly");

        let expected = vec![
            "+------+--------------+",
            "| dict | COUNT(t.val) |",
            "+------+--------------+",
            "| A    | 4            |",
            "| B    | 1            |",
            "| C    | 1            |",
            "+------+--------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        // Now, use dict as an aggregate
        let results =
            plan_and_collect(&ctx, "SELECT val, count(dict) FROM t GROUP BY val")
                .await
                .expect("ran plan correctly");

        let expected = vec![
            "+-----+---------------+",
            "| val | COUNT(t.dict) |",
            "+-----+---------------+",
            "| 1   | 3             |",
            "| 2   | 2             |",
            "| 4   | 1             |",
            "+-----+---------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        // Now, use dict as an aggregate
        let results = plan_and_collect(
            &ctx,
            "SELECT val, count(distinct dict) FROM t GROUP BY val",
        )
        .await
        .expect("ran plan correctly");

        let expected = vec![
            "+-------+------------------------+",
            "| t.val | COUNT(DISTINCT t.dict) |",
            "+-------+------------------------+",
            "| 1     | 2                      |",
            "| 2     | 2                      |",
            "| 4     | 1                      |",
            "+-------+------------------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);
    }

    run_test_case::<Int8Type>().await;
    run_test_case::<Int16Type>().await;
    run_test_case::<Int32Type>().await;
    run_test_case::<Int64Type>().await;
    run_test_case::<UInt8Type>().await;
    run_test_case::<UInt16Type>().await;
    run_test_case::<UInt32Type>().await;
    run_test_case::<UInt64Type>().await;
}

#[tokio::test]
async fn test_source_sorted_groupby() -> Result<()> {
    let tmpdir = TempDir::new().unwrap();
    let session_config = SessionConfig::new().with_target_partitions(1);
    let ctx = get_test_context2(&tmpdir, true, session_config).await?;

    let sql = "SELECT a, b,
           SUM(c) as summation1
           FROM annotated_data
           GROUP BY b, a";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    let expected = {
        vec![
            "ProjectionExec: expr=[a@1 as a, b@0 as b, SUM(annotated_data.c)@2 as summation1]",
            "  AggregateExec: mode=Single, gby=[b@1 as b, a@0 as a], aggr=[SUM(annotated_data.c)]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---+---+------------+",
        "| a | b | summation1 |",
        "+---+---+------------+",
        "| 0 | 0 | 300        |",
        "| 0 | 1 | 925        |",
        "| 1 | 2 | 1550       |",
        "| 1 | 3 | 2175       |",
        "+---+---+------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_source_sorted_groupby2() -> Result<()> {
    let tmpdir = TempDir::new().unwrap();
    let session_config = SessionConfig::new().with_target_partitions(1);
    let ctx = get_test_context2(&tmpdir, true, session_config).await?;

    let sql = "SELECT a, d,
           SUM(c) as summation1
           FROM annotated_data
           GROUP BY d, a";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    let expected = {
        vec![
            "ProjectionExec: expr=[a@1 as a, d@0 as d, SUM(annotated_data.c)@2 as summation1]",
            "  AggregateExec: mode=Single, gby=[d@2 as d, a@0 as a], aggr=[SUM(annotated_data.c)]",
        ]
    };

    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len - 1];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---+---+------------+",
        "| a | d | summation1 |",
        "+---+---+------------+",
        "| 0 | 0 | 292        |",
        "| 0 | 2 | 196        |",
        "| 0 | 1 | 315        |",
        "| 0 | 4 | 164        |",
        "| 0 | 3 | 258        |",
        "| 1 | 0 | 622        |",
        "| 1 | 3 | 299        |",
        "| 1 | 1 | 1043       |",
        "| 1 | 4 | 913        |",
        "| 1 | 2 | 848        |",
        "+---+---+------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
