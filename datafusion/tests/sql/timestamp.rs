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
use datafusion::from_slice::FromSlice;

#[tokio::test]
async fn query_cast_timestamp_millis() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![
            1235865600000,
            1235865660000,
            1238544000000,
        ]))],
    )?;
    let t1_table = MemTable::try_new(t1_schema, vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let sql = "SELECT to_timestamp_millis(ts) FROM t1 LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+--------------------------+",
        "| totimestampmillis(t1.ts) |",
        "+--------------------------+",
        "| 2009-03-01 00:00:00      |",
        "| 2009-03-01 00:01:00      |",
        "| 2009-04-01 00:00:00      |",
        "+--------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_cast_timestamp_micros() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![
            1235865600000000,
            1235865660000000,
            1238544000000000,
        ]))],
    )?;
    let t1_table = MemTable::try_new(t1_schema, vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let sql = "SELECT to_timestamp_micros(ts) FROM t1 LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+--------------------------+",
        "| totimestampmicros(t1.ts) |",
        "+--------------------------+",
        "| 2009-03-01 00:00:00      |",
        "| 2009-03-01 00:01:00      |",
        "| 2009-04-01 00:00:00      |",
        "+--------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_cast_timestamp_seconds() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![
            1235865600, 1235865660, 1238544000,
        ]))],
    )?;
    let t1_table = MemTable::try_new(t1_schema, vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let sql = "SELECT to_timestamp_seconds(ts) FROM t1 LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+---------------------------+",
        "| totimestampseconds(t1.ts) |",
        "+---------------------------+",
        "| 2009-03-01 00:00:00       |",
        "| 2009-03-01 00:01:00       |",
        "| 2009-04-01 00:00:00       |",
        "+---------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_cast_timestamp_nanos_to_others() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table("ts_data", make_timestamp_nano_table()?)?;

    // Original column is nanos, convert to millis and check timestamp
    let sql = "SELECT to_timestamp_millis(ts) FROM ts_data LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-------------------------------+",
        "| totimestampmillis(ts_data.ts) |",
        "+-------------------------------+",
        "| 2020-09-08 13:42:29.190       |",
        "| 2020-09-08 12:42:29.190       |",
        "| 2020-09-08 11:42:29.190       |",
        "+-------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT to_timestamp_micros(ts) FROM ts_data LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-------------------------------+",
        "| totimestampmicros(ts_data.ts) |",
        "+-------------------------------+",
        "| 2020-09-08 13:42:29.190855    |",
        "| 2020-09-08 12:42:29.190855    |",
        "| 2020-09-08 11:42:29.190855    |",
        "+-------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT to_timestamp_seconds(ts) FROM ts_data LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+--------------------------------+",
        "| totimestampseconds(ts_data.ts) |",
        "+--------------------------------+",
        "| 2020-09-08 13:42:29            |",
        "| 2020-09-08 12:42:29            |",
        "| 2020-09-08 11:42:29            |",
        "+--------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn query_cast_timestamp_seconds_to_others() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table("ts_secs", make_timestamp_table::<TimestampSecondType>()?)?;

    // Original column is seconds, convert to millis and check timestamp
    let sql = "SELECT to_timestamp_millis(ts) FROM ts_secs LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------------+",
        "| totimestampmillis(ts_secs.ts) |",
        "+-------------------------------+",
        "| 2020-09-08 13:42:29           |",
        "| 2020-09-08 12:42:29           |",
        "| 2020-09-08 11:42:29           |",
        "+-------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);

    // Original column is seconds, convert to micros and check timestamp
    let sql = "SELECT to_timestamp_micros(ts) FROM ts_secs LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------------+",
        "| totimestampmicros(ts_secs.ts) |",
        "+-------------------------------+",
        "| 2020-09-08 13:42:29           |",
        "| 2020-09-08 12:42:29           |",
        "| 2020-09-08 11:42:29           |",
        "+-------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // to nanos
    let sql = "SELECT to_timestamp(ts) FROM ts_secs LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------+",
        "| totimestamp(ts_secs.ts) |",
        "+-------------------------+",
        "| 2020-09-08 13:42:29     |",
        "| 2020-09-08 12:42:29     |",
        "| 2020-09-08 11:42:29     |",
        "+-------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_cast_timestamp_micros_to_others() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table(
        "ts_micros",
        make_timestamp_table::<TimestampMicrosecondType>()?,
    )?;

    // Original column is micros, convert to millis and check timestamp
    let sql = "SELECT to_timestamp_millis(ts) FROM ts_micros LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------------------------------+",
        "| totimestampmillis(ts_micros.ts) |",
        "+---------------------------------+",
        "| 2020-09-08 13:42:29.190         |",
        "| 2020-09-08 12:42:29.190         |",
        "| 2020-09-08 11:42:29.190         |",
        "+---------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // Original column is micros, convert to seconds and check timestamp
    let sql = "SELECT to_timestamp_seconds(ts) FROM ts_micros LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------------------------------+",
        "| totimestampseconds(ts_micros.ts) |",
        "+----------------------------------+",
        "| 2020-09-08 13:42:29              |",
        "| 2020-09-08 12:42:29              |",
        "| 2020-09-08 11:42:29              |",
        "+----------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // Original column is micros, convert to nanos and check timestamp
    let sql = "SELECT to_timestamp(ts) FROM ts_micros LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------------------------+",
        "| totimestamp(ts_micros.ts)  |",
        "+----------------------------+",
        "| 2020-09-08 13:42:29.190855 |",
        "| 2020-09-08 12:42:29.190855 |",
        "| 2020-09-08 11:42:29.190855 |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn to_timestamp() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table("ts_data", make_timestamp_nano_table()?)?;

    let sql = "SELECT COUNT(*) FROM ts_data where ts > to_timestamp('2020-09-08T12:00:00+00:00')";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 2               |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn to_timestamp_millis() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table(
        "ts_data",
        make_timestamp_table::<TimestampMillisecondType>()?,
    )?;

    let sql = "SELECT COUNT(*) FROM ts_data where ts > to_timestamp_millis('2020-09-08T12:00:00+00:00')";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 2               |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn to_timestamp_micros() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table(
        "ts_data",
        make_timestamp_table::<TimestampMicrosecondType>()?,
    )?;

    let sql = "SELECT COUNT(*) FROM ts_data where ts > to_timestamp_micros('2020-09-08T12:00:00+00:00')";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 2               |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn to_timestamp_seconds() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table("ts_data", make_timestamp_table::<TimestampSecondType>()?)?;

    let sql = "SELECT COUNT(*) FROM ts_data where ts > to_timestamp_seconds('2020-09-08T12:00:00+00:00')";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 2               |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn count_distinct_timestamps() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table("ts_data", make_timestamp_nano_table()?)?;

    let sql = "SELECT COUNT(DISTINCT(ts)) FROM ts_data";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+----------------------------+",
        "| COUNT(DISTINCT ts_data.ts) |",
        "+----------------------------+",
        "| 3                          |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_current_timestamp_expressions() -> Result<()> {
    let t1 = chrono::Utc::now().timestamp();
    let mut ctx = ExecutionContext::new();
    let actual = execute(&mut ctx, "SELECT NOW(), NOW() as t2").await;
    let res1 = actual[0][0].as_str();
    let res2 = actual[0][1].as_str();
    let t3 = chrono::Utc::now().timestamp();
    let t2_naive =
        chrono::NaiveDateTime::parse_from_str(res1, "%Y-%m-%d %H:%M:%S%.6f").unwrap();

    let t2 = t2_naive.timestamp();
    assert!(t1 <= t2 && t2 <= t3);
    assert_eq!(res2, res1);

    Ok(())
}

#[tokio::test]
async fn test_current_timestamp_expressions_non_optimized() -> Result<()> {
    let t1 = chrono::Utc::now().timestamp();
    let ctx = ExecutionContext::new();
    let sql = "SELECT NOW(), NOW() as t2";

    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx.create_logical_plan(sql).expect(&msg);

    let msg = format!("Creating physical plan for '{}': {:?}", sql, plan);
    let plan = ctx.create_physical_plan(&plan).await.expect(&msg);

    let msg = format!("Executing physical plan for '{}': {:?}", sql, plan);
    let runtime = ctx.state.lock().runtime_env.clone();
    let res = collect(plan, runtime).await.expect(&msg);
    let actual = result_vec(&res);

    let res1 = actual[0][0].as_str();
    let res2 = actual[0][1].as_str();
    let t3 = chrono::Utc::now().timestamp();
    let t2_naive =
        chrono::NaiveDateTime::parse_from_str(res1, "%Y-%m-%d %H:%M:%S%.6f").unwrap();

    let t2 = t2_naive.timestamp();
    assert!(t1 <= t2 && t2 <= t3);
    assert_eq!(res2, res1);

    Ok(())
}

#[tokio::test]
async fn timestamp_minmax() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let table_a = make_timestamp_tz_table::<TimestampMillisecondType>(None)?;
    let table_b =
        make_timestamp_tz_table::<TimestampNanosecondType>(Some("UTC".to_owned()))?;
    ctx.register_table("table_a", table_a)?;
    ctx.register_table("table_b", table_b)?;

    let sql = "SELECT MIN(table_a.ts), MAX(table_b.ts) FROM table_a, table_b";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------+----------------------------+",
        "| MIN(table_a.ts)         | MAX(table_b.ts)            |",
        "+-------------------------+----------------------------+",
        "| 2020-09-08 11:42:29.190 | 2020-09-08 13:42:29.190855 |",
        "+-------------------------+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn timestamp_coercion() -> Result<()> {
    {
        let mut ctx = ExecutionContext::new();
        let table_a =
            make_timestamp_tz_table::<TimestampSecondType>(Some("UTC".to_owned()))?;
        let table_b =
            make_timestamp_tz_table::<TimestampMillisecondType>(Some("UTC".to_owned()))?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------------------+-------------------------+--------------------------+",
            "| ts                  | ts                      | table_a.ts Eq table_b.ts |",
            "+---------------------+-------------------------+--------------------------+",
            "| 2020-09-08 13:42:29 | 2020-09-08 13:42:29.190 | true                     |",
            "| 2020-09-08 13:42:29 | 2020-09-08 12:42:29.190 | false                    |",
            "| 2020-09-08 13:42:29 | 2020-09-08 11:42:29.190 | false                    |",
            "| 2020-09-08 12:42:29 | 2020-09-08 13:42:29.190 | false                    |",
            "| 2020-09-08 12:42:29 | 2020-09-08 12:42:29.190 | true                     |",
            "| 2020-09-08 12:42:29 | 2020-09-08 11:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 13:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 12:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 11:42:29.190 | true                     |",
            "+---------------------+-------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampSecondType>()?;
        let table_b = make_timestamp_table::<TimestampMicrosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------------------+----------------------------+--------------------------+",
            "| ts                  | ts                         | table_a.ts Eq table_b.ts |",
            "+---------------------+----------------------------+--------------------------+",
            "| 2020-09-08 13:42:29 | 2020-09-08 13:42:29.190855 | true                     |",
            "| 2020-09-08 13:42:29 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 13:42:29 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29 | 2020-09-08 12:42:29.190855 | true                     |",
            "| 2020-09-08 12:42:29 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 11:42:29.190855 | true                     |",
            "+---------------------+----------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampSecondType>()?;
        let table_b = make_timestamp_table::<TimestampNanosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------------------+----------------------------+--------------------------+",
            "| ts                  | ts                         | table_a.ts Eq table_b.ts |",
            "+---------------------+----------------------------+--------------------------+",
            "| 2020-09-08 13:42:29 | 2020-09-08 13:42:29.190855 | true                     |",
            "| 2020-09-08 13:42:29 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 13:42:29 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29 | 2020-09-08 12:42:29.190855 | true                     |",
            "| 2020-09-08 12:42:29 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 11:42:29.190855 | true                     |",
            "+---------------------+----------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampMillisecondType>()?;
        let table_b = make_timestamp_table::<TimestampSecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+-------------------------+---------------------+--------------------------+",
            "| ts                      | ts                  | table_a.ts Eq table_b.ts |",
            "+-------------------------+---------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 13:42:29 | true                     |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 12:42:29 | false                    |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 11:42:29 | false                    |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 13:42:29 | false                    |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 12:42:29 | true                     |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 11:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 13:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 12:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 11:42:29 | true                     |",
            "+-------------------------+---------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampMillisecondType>()?;
        let table_b = make_timestamp_table::<TimestampMicrosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+-------------------------+----------------------------+--------------------------+",
            "| ts                      | ts                         | table_a.ts Eq table_b.ts |",
            "+-------------------------+----------------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 13:42:29.190855 | true                     |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 12:42:29.190855 | true                     |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 11:42:29.190855 | true                     |",
            "+-------------------------+----------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampMillisecondType>()?;
        let table_b = make_timestamp_table::<TimestampNanosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+-------------------------+----------------------------+--------------------------+",
            "| ts                      | ts                         | table_a.ts Eq table_b.ts |",
            "+-------------------------+----------------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 13:42:29.190855 | true                     |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 12:42:29.190855 | true                     |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 11:42:29.190855 | true                     |",
            "+-------------------------+----------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampMicrosecondType>()?;
        let table_b = make_timestamp_table::<TimestampSecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+----------------------------+---------------------+--------------------------+",
            "| ts                         | ts                  | table_a.ts Eq table_b.ts |",
            "+----------------------------+---------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29 | true                     |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29 | false                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29 | true                     |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29 | true                     |",
            "+----------------------------+---------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampMicrosecondType>()?;
        let table_b = make_timestamp_table::<TimestampMillisecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+----------------------------+-------------------------+--------------------------+",
            "| ts                         | ts                      | table_a.ts Eq table_b.ts |",
            "+----------------------------+-------------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29.190 | true                     |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29.190 | false                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29.190 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29.190 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29.190 | true                     |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29.190 | true                     |",
            "+----------------------------+-------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampMicrosecondType>()?;
        let table_b = make_timestamp_table::<TimestampNanosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+----------------------------+----------------------------+--------------------------+",
            "| ts                         | ts                         | table_a.ts Eq table_b.ts |",
            "+----------------------------+----------------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29.190855 | true                     |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29.190855 | true                     |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29.190855 | true                     |",
            "+----------------------------+----------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampNanosecondType>()?;
        let table_b = make_timestamp_table::<TimestampSecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+----------------------------+---------------------+--------------------------+",
            "| ts                         | ts                  | table_a.ts Eq table_b.ts |",
            "+----------------------------+---------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29 | true                     |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29 | false                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29 | true                     |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29 | true                     |",
            "+----------------------------+---------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampNanosecondType>()?;
        let table_b = make_timestamp_table::<TimestampMillisecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+----------------------------+-------------------------+--------------------------+",
            "| ts                         | ts                      | table_a.ts Eq table_b.ts |",
            "+----------------------------+-------------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29.190 | true                     |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29.190 | false                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29.190 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29.190 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29.190 | true                     |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29.190 | true                     |",
            "+----------------------------+-------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampNanosecondType>()?;
        let table_b = make_timestamp_table::<TimestampMicrosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+----------------------------+----------------------------+--------------------------+",
            "| ts                         | ts                         | table_a.ts Eq table_b.ts |",
            "+----------------------------+----------------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29.190855 | true                     |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29.190855 | true                     |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29.190855 | true                     |",
            "+----------------------------+----------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    Ok(())
}

#[tokio::test]
async fn group_by_timestamp_millis() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("count", DataType::Int32, false),
    ]));
    let base_dt = Utc.ymd(2018, 7, 1).and_hms(6, 0, 0); // 2018-Jul-01 06:00
    let hour1 = Duration::hours(1);
    let timestamps = vec![
        base_dt.timestamp_millis(),
        (base_dt + hour1).timestamp_millis(),
        base_dt.timestamp_millis(),
        base_dt.timestamp_millis(),
        (base_dt + hour1).timestamp_millis(),
        (base_dt + hour1).timestamp_millis(),
    ];
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(timestamps)),
            Arc::new(Int32Array::from_slice(&[10, 20, 30, 40, 50, 60])),
        ],
    )?;
    let t1_table = MemTable::try_new(schema, vec![vec![data]])?;
    ctx.register_table("t1", Arc::new(t1_table)).unwrap();

    let sql =
        "SELECT timestamp, SUM(count) FROM t1 GROUP BY timestamp ORDER BY timestamp ASC";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------------------+---------------+",
        "| timestamp           | SUM(t1.count) |",
        "+---------------------+---------------+",
        "| 2018-07-01 06:00:00 | 80            |",
        "| 2018-07-01 07:00:00 | 130           |",
        "+---------------------+---------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
