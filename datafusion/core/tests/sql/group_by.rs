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
use arrow::util::pretty::pretty_format_batches;
use arrow_schema::{DataType, TimeUnit};

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

    let expected = [
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
async fn group_by_limit() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let ctx = create_groupby_context(&tmp_dir).await?;

    let sql = "SELECT trace_id, MAX(ts) from traces group by trace_id order by MAX(ts) desc limit 4";
    let dataframe = ctx.sql(sql).await?;

    // ensure we see `lim=[4]`
    let physical_plan = dataframe.create_physical_plan().await?;
    let mut expected_physical_plan = r#"
GlobalLimitExec: skip=0, fetch=4
  SortExec: fetch=4, expr=[MAX(traces.ts)@1 DESC]
    AggregateExec: mode=Single, gby=[trace_id@0 as trace_id], aggr=[MAX(traces.ts)], lim=[4]
    "#.trim().to_string();
    let actual_phys_plan =
        format_plan(physical_plan.clone(), &mut expected_physical_plan);
    assert_eq!(actual_phys_plan, expected_physical_plan);

    let batches = collect(physical_plan, ctx.task_ctx()).await?;
    let expected = r#"
+----------+----------------------+
| trace_id | MAX(traces.ts)       |
+----------+----------------------+
| 9        | 2020-12-01T00:00:18Z |
| 8        | 2020-12-01T00:00:17Z |
| 7        | 2020-12-01T00:00:16Z |
| 6        | 2020-12-01T00:00:15Z |
+----------+----------------------+
"#
    .trim();
    let actual = format!("{}", pretty_format_batches(&batches)?);
    assert_eq!(actual, expected);

    Ok(())
}

fn format_plan(
    physical_plan: Arc<dyn ExecutionPlan>,
    expected_phys_plan: &mut String,
) -> String {
    let actual_phys_plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    let last_line = actual_phys_plan
        .as_str()
        .lines()
        .last()
        .expect("Plan should not be empty");

    expected_phys_plan.push('\n');
    expected_phys_plan.push_str(last_line);
    expected_phys_plan.push('\n');
    actual_phys_plan
}

async fn create_groupby_context(tmp_dir: &TempDir) -> Result<SessionContext> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
    ]));

    // generate a file
    let filename = "traces.csv";
    let file_path = tmp_dir.path().join(filename);
    let mut file = File::create(file_path)?;

    // generate some data
    for trace_id in 0..10 {
        for ts in 0..10 {
            let ts = trace_id + ts;
            let data = format!("\"{trace_id}\",2020-12-01T00:00:{ts:02}.000Z\n");
            file.write_all(data.as_bytes())?;
        }
    }

    let cfg = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(cfg);
    ctx.register_csv(
        "traces",
        tmp_dir.path().to_str().unwrap(),
        CsvReadOptions::new().schema(&schema).has_header(false),
    )
    .await?;
    Ok(ctx)
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

        let expected = [
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

        let expected = [
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

        let expected = [
            "+-----+------------------------+",
            "| val | COUNT(DISTINCT t.dict) |",
            "+-----+------------------------+",
            "| 1   | 2                      |",
            "| 2   | 2                      |",
            "| 4   | 1                      |",
            "+-----+------------------------+",
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
