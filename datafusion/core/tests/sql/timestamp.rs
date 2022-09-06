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
use std::ops::Add;

#[tokio::test]
async fn query_cast_timestamp_millis() -> Result<()> {
    let ctx = SessionContext::new();

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
    let actual = execute_to_batches(&ctx, sql).await;

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
    let ctx = SessionContext::new();

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
    let actual = execute_to_batches(&ctx, sql).await;

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
    let ctx = SessionContext::new();

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
    let actual = execute_to_batches(&ctx, sql).await;

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
    let ctx = SessionContext::new();
    ctx.register_table("ts_data", make_timestamp_nano_table()?)?;

    // Original column is nanos, convert to millis and check timestamp
    let sql = "SELECT to_timestamp_millis(ts) FROM ts_data LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;

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
    let actual = execute_to_batches(&ctx, sql).await;

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
    let actual = execute_to_batches(&ctx, sql).await;
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
    let ctx = SessionContext::new();
    ctx.register_table("ts_secs", make_timestamp_table::<TimestampSecondType>()?)?;

    // Original column is seconds, convert to millis and check timestamp
    let sql = "SELECT to_timestamp_millis(ts) FROM ts_secs LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;
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
    let actual = execute_to_batches(&ctx, sql).await;
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
    let actual = execute_to_batches(&ctx, sql).await;
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
    let ctx = SessionContext::new();
    ctx.register_table(
        "ts_micros",
        make_timestamp_table::<TimestampMicrosecondType>()?,
    )?;

    // Original column is micros, convert to millis and check timestamp
    let sql = "SELECT to_timestamp_millis(ts) FROM ts_micros LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;
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
    let actual = execute_to_batches(&ctx, sql).await;
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
    let actual = execute_to_batches(&ctx, sql).await;
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
async fn query_cast_timestamp_from_unixtime() -> Result<()> {
    let ctx = SessionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![
            1235865600, 1235865660, 1238544000,
        ]))],
    )?;
    let t1_table = MemTable::try_new(t1_schema, vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let sql = "SELECT from_unixtime(ts) FROM t1 LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------------------+",
        "| fromunixtime(t1.ts) |",
        "+---------------------+",
        "| 2009-03-01 00:00:00 |",
        "| 2009-03-01 00:01:00 |",
        "| 2009-04-01 00:00:00 |",
        "+---------------------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn to_timestamp() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("ts_data", make_timestamp_nano_table()?)?;

    let sql = "SELECT COUNT(*) FROM ts_data where ts > to_timestamp('2020-09-08T12:00:00+00:00')";
    let actual = execute_to_batches(&ctx, sql).await;

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
    let ctx = SessionContext::new();
    ctx.register_table(
        "ts_data",
        make_timestamp_table::<TimestampMillisecondType>()?,
    )?;

    let sql = "SELECT COUNT(*) FROM ts_data where ts > to_timestamp_millis('2020-09-08T12:00:00+00:00')";
    let actual = execute_to_batches(&ctx, sql).await;
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
    let ctx = SessionContext::new();
    ctx.register_table(
        "ts_data",
        make_timestamp_table::<TimestampMicrosecondType>()?,
    )?;

    let sql = "SELECT COUNT(*) FROM ts_data where ts > to_timestamp_micros('2020-09-08T12:00:00+00:00')";
    let actual = execute_to_batches(&ctx, sql).await;

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
    let ctx = SessionContext::new();
    ctx.register_table("ts_data", make_timestamp_table::<TimestampSecondType>()?)?;

    let sql = "SELECT COUNT(*) FROM ts_data where ts > to_timestamp_seconds('2020-09-08T12:00:00+00:00')";
    let actual = execute_to_batches(&ctx, sql).await;

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
async fn from_unixtime() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("ts_data", make_timestamp_table::<TimestampSecondType>()?)?;

    let sql = "SELECT COUNT(*) FROM ts_data where ts > from_unixtime(1599566400)"; // '2020-09-08T12:00:00+00:00'
    let actual = execute_to_batches(&ctx, sql).await;

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
    let ctx = SessionContext::new();
    ctx.register_table("ts_data", make_timestamp_nano_table()?)?;

    let sql = "SELECT COUNT(DISTINCT(ts)) FROM ts_data";
    let actual = execute_to_batches(&ctx, sql).await;

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
    let ctx = SessionContext::new();
    let actual = execute(&ctx, "SELECT NOW(), NOW() as t2").await;
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
async fn test_now_in_same_stmt_using_sql_function() -> Result<()> {
    let ctx = SessionContext::new();

    let df1 = ctx.sql("select now(), now() as now2").await?;
    let result = result_vec(&df1.collect().await?);
    assert_eq!(result[0][0], result[0][1]);

    Ok(())
}

#[tokio::test]
async fn test_now_across_statements() -> Result<()> {
    let ctx = SessionContext::new();

    let actual1 = execute(&ctx, "SELECT NOW()").await;
    let res1 = actual1[0][0].as_str();

    let actual2 = execute(&ctx, "SELECT NOW()").await;
    let res2 = actual2[0][0].as_str();

    assert!(res1 < res2);

    Ok(())
}

#[tokio::test]
async fn test_now_across_statements_using_sql_function() -> Result<()> {
    let ctx = SessionContext::new();

    let df1 = ctx.sql("select now()").await?;
    let rb1 = df1.collect().await?;
    let result1 = result_vec(&rb1);
    let res1 = result1[0][0].as_str();

    let df2 = ctx.sql("select now()").await?;
    let rb2 = df2.collect().await?;
    let result2 = result_vec(&rb2);
    let res2 = result2[0][0].as_str();

    assert!(res1 < res2);

    Ok(())
}

#[tokio::test]
async fn test_now_dataframe_api() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.sql("select 1").await?; // use this to get a DataFrame
    let df = df.select(vec![now(), now().alias("now2")])?;
    let result = result_vec(&df.collect().await?);
    assert_eq!(result[0][0], result[0][1]);

    Ok(())
}

#[tokio::test]
async fn test_now_dataframe_api_across_statements() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.sql("select 1").await?; // use this to get a DataFrame
    let df = df.select(vec![now()])?;
    let result = result_vec(&df.collect().await?);

    let df = ctx.sql("select 1").await?;
    let df = df.select(vec![now()])?;
    let result2 = result_vec(&df.collect().await?);

    assert_ne!(result[0][0], result2[0][0]);

    Ok(())
}

#[tokio::test]
async fn test_now_in_view() -> Result<()> {
    let ctx = SessionContext::new();
    let _df = ctx
        .sql("create or replace view test_now as select now()")
        .await?
        .collect()
        .await?;

    let df = ctx.sql("select * from test_now").await?;
    let result = result_vec(&df.collect().await?);

    let df1 = ctx.sql("select * from test_now").await?;
    let result2 = result_vec(&df1.collect().await?);

    assert_ne!(result[0][0], result2[0][0]);

    Ok(())
}

#[tokio::test]
async fn timestamp_minmax() -> Result<()> {
    let ctx = SessionContext::new();
    let table_a = make_timestamp_tz_table::<TimestampMillisecondType>(None)?;
    let table_b =
        make_timestamp_tz_table::<TimestampNanosecondType>(Some("UTC".to_owned()))?;
    ctx.register_table("table_a", table_a)?;
    ctx.register_table("table_b", table_b)?;

    let sql = "SELECT MIN(table_a.ts), MAX(table_b.ts) FROM table_a, table_b";
    let actual = execute_to_batches(&ctx, sql).await;
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
        let ctx = SessionContext::new();
        let table_a =
            make_timestamp_tz_table::<TimestampSecondType>(Some("UTC".to_owned()))?;
        let table_b =
            make_timestamp_tz_table::<TimestampMillisecondType>(Some("UTC".to_owned()))?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+---------------------+-------------------------+-------------------------+",
            "| ts                  | ts                      | table_a.ts = table_b.ts |",
            "+---------------------+-------------------------+-------------------------+",
            "| 2020-09-08 13:42:29 | 2020-09-08 13:42:29.190 | true                    |",
            "| 2020-09-08 13:42:29 | 2020-09-08 12:42:29.190 | false                   |",
            "| 2020-09-08 13:42:29 | 2020-09-08 11:42:29.190 | false                   |",
            "| 2020-09-08 12:42:29 | 2020-09-08 13:42:29.190 | false                   |",
            "| 2020-09-08 12:42:29 | 2020-09-08 12:42:29.190 | true                    |",
            "| 2020-09-08 12:42:29 | 2020-09-08 11:42:29.190 | false                   |",
            "| 2020-09-08 11:42:29 | 2020-09-08 13:42:29.190 | false                   |",
            "| 2020-09-08 11:42:29 | 2020-09-08 12:42:29.190 | false                   |",
            "| 2020-09-08 11:42:29 | 2020-09-08 11:42:29.190 | true                    |",
            "+---------------------+-------------------------+-------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let ctx = SessionContext::new();
        let table_a = make_timestamp_table::<TimestampSecondType>()?;
        let table_b = make_timestamp_table::<TimestampMicrosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+---------------------+----------------------------+-------------------------+",
            "| ts                  | ts                         | table_a.ts = table_b.ts |",
            "+---------------------+----------------------------+-------------------------+",
            "| 2020-09-08 13:42:29 | 2020-09-08 13:42:29.190855 | true                    |",
            "| 2020-09-08 13:42:29 | 2020-09-08 12:42:29.190855 | false                   |",
            "| 2020-09-08 13:42:29 | 2020-09-08 11:42:29.190855 | false                   |",
            "| 2020-09-08 12:42:29 | 2020-09-08 13:42:29.190855 | false                   |",
            "| 2020-09-08 12:42:29 | 2020-09-08 12:42:29.190855 | true                    |",
            "| 2020-09-08 12:42:29 | 2020-09-08 11:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29 | 2020-09-08 13:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29 | 2020-09-08 12:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29 | 2020-09-08 11:42:29.190855 | true                    |",
            "+---------------------+----------------------------+-------------------------+",

        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let ctx = SessionContext::new();
        let table_a = make_timestamp_table::<TimestampSecondType>()?;
        let table_b = make_timestamp_table::<TimestampNanosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+---------------------+----------------------------+-------------------------+",
            "| ts                  | ts                         | table_a.ts = table_b.ts |",
            "+---------------------+----------------------------+-------------------------+",
            "| 2020-09-08 13:42:29 | 2020-09-08 13:42:29.190855 | true                    |",
            "| 2020-09-08 13:42:29 | 2020-09-08 12:42:29.190855 | false                   |",
            "| 2020-09-08 13:42:29 | 2020-09-08 11:42:29.190855 | false                   |",
            "| 2020-09-08 12:42:29 | 2020-09-08 13:42:29.190855 | false                   |",
            "| 2020-09-08 12:42:29 | 2020-09-08 12:42:29.190855 | true                    |",
            "| 2020-09-08 12:42:29 | 2020-09-08 11:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29 | 2020-09-08 13:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29 | 2020-09-08 12:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29 | 2020-09-08 11:42:29.190855 | true                    |",
            "+---------------------+----------------------------+-------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let ctx = SessionContext::new();
        let table_a = make_timestamp_table::<TimestampMillisecondType>()?;
        let table_b = make_timestamp_table::<TimestampSecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+-------------------------+---------------------+-------------------------+",
            "| ts                      | ts                  | table_a.ts = table_b.ts |",
            "+-------------------------+---------------------+-------------------------+",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 13:42:29 | true                    |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 12:42:29 | false                   |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 11:42:29 | false                   |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 13:42:29 | false                   |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 12:42:29 | true                    |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 11:42:29 | false                   |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 13:42:29 | false                   |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 12:42:29 | false                   |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 11:42:29 | true                    |",
            "+-------------------------+---------------------+-------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let ctx = SessionContext::new();
        let table_a = make_timestamp_table::<TimestampMillisecondType>()?;
        let table_b = make_timestamp_table::<TimestampMicrosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+-------------------------+----------------------------+-------------------------+",
            "| ts                      | ts                         | table_a.ts = table_b.ts |",
            "+-------------------------+----------------------------+-------------------------+",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 13:42:29.190855 | true                    |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 12:42:29.190855 | false                   |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 11:42:29.190855 | false                   |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 13:42:29.190855 | false                   |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 12:42:29.190855 | true                    |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 11:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 13:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 12:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 11:42:29.190855 | true                    |",
            "+-------------------------+----------------------------+-------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let ctx = SessionContext::new();
        let table_a = make_timestamp_table::<TimestampMillisecondType>()?;
        let table_b = make_timestamp_table::<TimestampNanosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+-------------------------+----------------------------+-------------------------+",
            "| ts                      | ts                         | table_a.ts = table_b.ts |",
            "+-------------------------+----------------------------+-------------------------+",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 13:42:29.190855 | true                    |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 12:42:29.190855 | false                   |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 11:42:29.190855 | false                   |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 13:42:29.190855 | false                   |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 12:42:29.190855 | true                    |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 11:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 13:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 12:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 11:42:29.190855 | true                    |",
            "+-------------------------+----------------------------+-------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let ctx = SessionContext::new();
        let table_a = make_timestamp_table::<TimestampMicrosecondType>()?;
        let table_b = make_timestamp_table::<TimestampSecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+----------------------------+---------------------+-------------------------+",
            "| ts                         | ts                  | table_a.ts = table_b.ts |",
            "+----------------------------+---------------------+-------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29 | true                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29 | false                   |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29 | false                   |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29 | false                   |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29 | true                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29 | true                    |",
            "+----------------------------+---------------------+-------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let ctx = SessionContext::new();
        let table_a = make_timestamp_table::<TimestampMicrosecondType>()?;
        let table_b = make_timestamp_table::<TimestampMillisecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+----------------------------+-------------------------+-------------------------+",
            "| ts                         | ts                      | table_a.ts = table_b.ts |",
            "+----------------------------+-------------------------+-------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29.190 | true                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29.190 | false                   |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29.190 | false                   |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29.190 | false                   |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29.190 | true                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29.190 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29.190 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29.190 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29.190 | true                    |",
            "+----------------------------+-------------------------+-------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let ctx = SessionContext::new();
        let table_a = make_timestamp_table::<TimestampMicrosecondType>()?;
        let table_b = make_timestamp_table::<TimestampNanosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+----------------------------+----------------------------+-------------------------+",
            "| ts                         | ts                         | table_a.ts = table_b.ts |",
            "+----------------------------+----------------------------+-------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29.190855 | true                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29.190855 | false                   |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29.190855 | false                   |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29.190855 | false                   |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29.190855 | true                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29.190855 | true                    |",
            "+----------------------------+----------------------------+-------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let ctx = SessionContext::new();
        let table_a = make_timestamp_table::<TimestampNanosecondType>()?;
        let table_b = make_timestamp_table::<TimestampSecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+----------------------------+---------------------+-------------------------+",
            "| ts                         | ts                  | table_a.ts = table_b.ts |",
            "+----------------------------+---------------------+-------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29 | true                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29 | false                   |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29 | false                   |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29 | false                   |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29 | true                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29 | true                    |",
            "+----------------------------+---------------------+-------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let ctx = SessionContext::new();
        let table_a = make_timestamp_table::<TimestampNanosecondType>()?;
        let table_b = make_timestamp_table::<TimestampMillisecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+----------------------------+-------------------------+-------------------------+",
            "| ts                         | ts                      | table_a.ts = table_b.ts |",
            "+----------------------------+-------------------------+-------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29.190 | true                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29.190 | false                   |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29.190 | false                   |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29.190 | false                   |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29.190 | true                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29.190 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29.190 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29.190 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29.190 | true                    |",
            "+----------------------------+-------------------------+-------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let ctx = SessionContext::new();
        let table_a = make_timestamp_table::<TimestampNanosecondType>()?;
        let table_b = make_timestamp_table::<TimestampMicrosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+----------------------------+----------------------------+-------------------------+",
            "| ts                         | ts                         | table_a.ts = table_b.ts |",
            "+----------------------------+----------------------------+-------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29.190855 | true                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29.190855 | false                   |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29.190855 | false                   |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29.190855 | false                   |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29.190855 | true                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29.190855 | false                   |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29.190855 | true                    |",
            "+----------------------------+----------------------------+-------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    Ok(())
}

#[tokio::test]
async fn group_by_timestamp_millis() -> Result<()> {
    let ctx = SessionContext::new();

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
    let actual = execute_to_batches(&ctx, sql).await;
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

#[tokio::test]
async fn interval_year() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select date '1994-01-01' + interval '1' year as date;";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+------------+",
        "| date       |",
        "+------------+",
        "| 1995-01-01 |",
        "+------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn add_interval_month() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select date '1994-01-31' + interval '1' month as date;";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+------------+",
        "| date       |",
        "+------------+",
        "| 1994-02-28 |",
        "+------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn sub_interval_month() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select date '1994-03-31' - interval '1' month as date;";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+------------+",
        "| date       |",
        "+------------+",
        "| 1994-02-28 |",
        "+------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn sub_month_wrap() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select date '1994-01-15' - interval '1' month as date;";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+------------+",
        "| date       |",
        "+------------+",
        "| 1993-12-15 |",
        "+------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn add_interval_day() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select date '1994-01-15' + interval '1' day as date;";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+------------+",
        "| date       |",
        "+------------+",
        "| 1994-01-16 |",
        "+------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn sub_interval_day() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select date '1994-01-01' - interval '1' day as date;";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+------------+",
        "| date       |",
        "+------------+",
        "| 1993-12-31 |",
        "+------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn cast_string_to_time() {
    let ctx = SessionContext::new();

    let sql = "select \
        time '08:09:10.123456789' as time_nano, \
        time '13:14:15.123456'    as time_micro,\
        time '13:14:15.123'       as time_milli,\
        time '13:14:15'           as time;";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+--------------------+-----------------+--------------+----------+",
        "| time_nano          | time_micro      | time_milli   | time     |",
        "+--------------------+-----------------+--------------+----------+",
        "| 08:09:10.123456789 | 13:14:15.123456 | 13:14:15.123 | 13:14:15 |",
        "+--------------------+-----------------+--------------+----------+",
    ];
    assert_batches_eq!(expected, &results);

    // Fallible cases

    let sql = "SELECT TIME 'not a time' as time;";
    let result = try_execute_to_batches(&ctx, sql).await;
    assert_eq!(
        result.err().unwrap().to_string(),
        "Arrow error: Cast error: Cannot cast string 'not a time' to value of Time64(Nanosecond) type"
    );

    // An invalid time
    let sql = "SELECT TIME '24:01:02' as time;";
    let result = try_execute_to_batches(&ctx, sql).await;
    assert_eq!(
        result.err().unwrap().to_string(),
        "Arrow error: Cast error: Cannot cast string '24:01:02' to value of Time64(Nanosecond) type"
    );
}

#[tokio::test]
async fn cast_to_timestamp_twice() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select to_timestamp(a) from (select to_timestamp(1) as a)A;";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+-------------------------------+",
        "| totimestamp(a.a)              |",
        "+-------------------------------+",
        "| 1970-01-01 00:00:00.000000001 |",
        "+-------------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn cast_to_timestamp_seconds_twice() -> Result<()> {
    let ctx = SessionContext::new();

    let sql =
        "select to_timestamp_seconds(a) from (select to_timestamp_seconds(1) as a)A;";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+-------------------------+",
        "| totimestampseconds(a.a) |",
        "+-------------------------+",
        "| 1970-01-01 00:00:01     |",
        "+-------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn cast_to_timestamp_millis_twice() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select to_timestamp_millis(a) from (select to_timestamp_millis(1) as a)A;";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+-------------------------+",
        "| totimestampmillis(a.a)  |",
        "+-------------------------+",
        "| 1970-01-01 00:00:00.001 |",
        "+-------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn cast_to_timestamp_micros_twice() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select to_timestamp_micros(a) from (select to_timestamp_micros(1) as a)A;";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+----------------------------+",
        "| totimestampmicros(a.a)     |",
        "+----------------------------+",
        "| 1970-01-01 00:00:00.000001 |",
        "+----------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn to_timestamp_i32() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select to_timestamp(cast (1 as int));";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+-------------------------------+",
        "| totimestamp(Int64(1))         |",
        "+-------------------------------+",
        "| 1970-01-01 00:00:00.000000001 |",
        "+-------------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn to_timestamp_micros_i32() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select to_timestamp_micros(cast (1 as int));";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+-----------------------------+",
        "| totimestampmicros(Int64(1)) |",
        "+-----------------------------+",
        "| 1970-01-01 00:00:00.000001  |",
        "+-----------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn to_timestamp_millis_i32() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select to_timestamp_millis(cast (1 as int));";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+-----------------------------+",
        "| totimestampmillis(Int64(1)) |",
        "+-----------------------------+",
        "| 1970-01-01 00:00:00.001     |",
        "+-----------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn to_timestamp_seconds_i32() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select to_timestamp_seconds(cast (1 as int));";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+------------------------------+",
        "| totimestampseconds(Int64(1)) |",
        "+------------------------------+",
        "| 1970-01-01 00:00:01          |",
        "+------------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn date_bin() {
    let ctx = SessionContext::new();

    let sql = "SELECT DATE_BIN(INTERVAL '15 minutes', TIMESTAMP '2022-08-03 14:38:50Z', TIMESTAMP '1970-01-01T00:00:00Z') AS res";
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------+",
        "| res                 |",
        "+---------------------+",
        "| 2022-08-03 14:30:00 |",
        "+---------------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Shift forward by 5 minutes
    let sql = "SELECT DATE_BIN(INTERVAL '15 minutes', TIMESTAMP '2022-08-03 14:38:50Z', TIMESTAMP '1970-01-01T00:05:00Z') AS res";
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------+",
        "| res                 |",
        "+---------------------+",
        "| 2022-08-03 14:35:00 |",
        "+---------------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Shift backward by 5 minutes
    let sql = "SELECT DATE_BIN(INTERVAL '15 minutes', TIMESTAMP '2022-08-03 14:38:50Z', TIMESTAMP '1970-01-01T23:55:00Z') AS res";
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------+",
        "| res                 |",
        "+---------------------+",
        "| 2022-08-03 14:25:00 |",
        "+---------------------+",
    ];
    assert_batches_eq!(expected, &results);

    // origin after source, timestamp in previous bucket
    let sql = "SELECT DATE_BIN(INTERVAL '15 minutes', TIMESTAMP '2022-08-03 14:38:50Z', TIMESTAMP '2022-08-03 14:40:00Z') AS res";
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------+",
        "| res                 |",
        "+---------------------+",
        "| 2022-08-03 14:25:00 |",
        "+---------------------+",
    ];
    assert_batches_eq!(expected, &results);

    // stride by 7 days
    let sql = "SELECT DATE_BIN(INTERVAL '7 days', TIMESTAMP '2022-08-03 14:38:50Z', TIMESTAMP '1970-01-01 00:00:00Z') AS res";
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------+",
        "| res                 |",
        "+---------------------+",
        "| 2022-07-28 00:00:00 |",
        "+---------------------+",
    ];
    assert_batches_eq!(expected, &results);

    // origin shifts bins forward 1 day
    let sql = "SELECT DATE_BIN(INTERVAL '7 days', TIMESTAMP '2022-08-03 14:38:50Z', TIMESTAMP '1970-01-02 00:00:00Z') AS res";
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------+",
        "| res                 |",
        "+---------------------+",
        "| 2022-07-29 00:00:00 |",
        "+---------------------+",
    ];
    assert_batches_eq!(expected, &results);

    // following test demonstrates array values for the source argument
    let sql = "SELECT
      DATE_BIN(INTERVAL '15' minute, time, TIMESTAMP '2001-01-01T00:00:00Z') AS time,
      val
    FROM (
      VALUES
        (TIMESTAMP '2021-06-10 17:05:00Z', 0.5),
        (TIMESTAMP '2021-06-10 17:19:10Z', 0.3)
      ) as t (time, val)";
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------+-----+",
        "| time                | val |",
        "+---------------------+-----+",
        "| 2021-06-10 17:00:00 | 0.5 |",
        "| 2021-06-10 17:15:00 | 0.3 |",
        "+---------------------+-----+",
    ];
    assert_batches_eq!(expected, &results);

    // following test demonstrates array values for the origin argument are not currently supported
    let sql = "SELECT
      DATE_BIN(INTERVAL '15' minute, time, origin) AS time,
      val
    FROM (
      VALUES
        (TIMESTAMP '2021-06-10 17:05:00Z', TIMESTAMP '2001-01-01T00:00:00Z', 0.5),
        (TIMESTAMP '2021-06-10 17:19:10Z', TIMESTAMP '2001-01-01T00:00:00Z', 0.3)
      ) as t (time, origin, val)";
    let result = try_execute_to_batches(&ctx, sql).await;
    assert_eq!(
        result.err().unwrap().to_string(),
        "Arrow error: External error: This feature is not implemented: DATE_BIN only supports literal values for the origin argument, not arrays"
    );
}

#[tokio::test]
async fn timestamp_add_interval_second() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "SELECT NOW(), NOW() + INTERVAL '1' SECOND;";
    let results = execute_to_batches(&ctx, sql).await;
    let actual = result_vec(&results);

    let res1 = actual[0][0].as_str();
    let res2 = actual[0][1].as_str();

    let format = "%Y-%m-%d %H:%M:%S%.6f";
    let t1_naive = chrono::NaiveDateTime::parse_from_str(res1, format).unwrap();
    let t2_naive = chrono::NaiveDateTime::parse_from_str(res2, format).unwrap();

    assert_eq!(t1_naive.add(Duration::seconds(1)), t2_naive);
    Ok(())
}

#[tokio::test]
async fn timestamp_sub_interval_days() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "SELECT NOW(), NOW() - INTERVAL '8' DAY;";
    let results = execute_to_batches(&ctx, sql).await;
    let actual = result_vec(&results);

    let res1 = actual[0][0].as_str();
    let res2 = actual[0][1].as_str();

    let format = "%Y-%m-%d %H:%M:%S%.6f";
    let t1_naive = chrono::NaiveDateTime::parse_from_str(res1, format).unwrap();
    let t2_naive = chrono::NaiveDateTime::parse_from_str(res2, format).unwrap();

    assert_eq!(t1_naive.sub(Duration::days(8)), t2_naive);
    Ok(())
}

#[tokio::test]
#[ignore] // https://github.com/apache/arrow-datafusion/issues/3327
async fn timestamp_add_interval_months() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "SELECT NOW(), NOW() + INTERVAL '17' MONTH;";
    let results = execute_to_batches(&ctx, sql).await;
    let actual = result_vec(&results);

    let res1 = actual[0][0].as_str();
    let res2 = actual[0][1].as_str();

    let format = "%Y-%m-%d %H:%M:%S%.6f";
    let t1_naive = chrono::NaiveDateTime::parse_from_str(res1, format).unwrap();
    let t2_naive = chrono::NaiveDateTime::parse_from_str(res2, format).unwrap();

    let year = t1_naive.year() + (t1_naive.month() as i32 + 17) / 12;
    let month = (t1_naive.month() + 17) % 12;

    assert_eq!(
        t1_naive.with_year(year).unwrap().with_month(month).unwrap(),
        t2_naive
    );
    Ok(())
}

#[tokio::test]
async fn timestamp_sub_interval_years() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "SELECT NOW(), NOW() - INTERVAL '16' YEAR;";
    let results = execute_to_batches(&ctx, sql).await;
    let actual = result_vec(&results);

    let res1 = actual[0][0].as_str();
    let res2 = actual[0][1].as_str();

    let format = "%Y-%m-%d %H:%M:%S%.6f";
    let t1_naive = chrono::NaiveDateTime::parse_from_str(res1, format).unwrap();
    let t2_naive = chrono::NaiveDateTime::parse_from_str(res2, format).unwrap();

    assert_eq!(t1_naive.with_year(t1_naive.year() - 16).unwrap(), t2_naive);
    Ok(())
}

#[tokio::test]
async fn timestamp_array_add_interval() -> Result<()> {
    let ctx = SessionContext::new();
    let table_a = make_timestamp_table::<TimestampNanosecondType>()?;
    let table_b = make_timestamp_table::<TimestampMicrosecondType>()?;
    ctx.register_table("table_a", table_a)?;
    ctx.register_table("table_b", table_b)?;

    let sql = "SELECT ts, ts - INTERVAL '8' MILLISECONDS FROM table_a";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+-----------------------------------+",
        "| ts                         | table_a.ts - IntervalDayTime(\"8\") |",
        "+----------------------------+-----------------------------------+",
        "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29.182855        |",
        "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29.182855        |",
        "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29.182855        |",
        "+----------------------------+-----------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT ts, ts + INTERVAL '1' SECOND FROM table_b";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+--------------------------------------+",
        "| ts                         | table_b.ts + IntervalDayTime(\"1000\") |",
        "+----------------------------+--------------------------------------+",
        "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:30.190855           |",
        "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:30.190855           |",
        "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:30.190855           |",
        "+----------------------------+--------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT ts, ts + INTERVAL '2' MONTH FROM table_b";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+-------------------------------------+",
        "| ts                         | table_b.ts + IntervalYearMonth(\"2\") |",
        "+----------------------------+-------------------------------------+",
        "| 2020-09-08 13:42:29.190855 | 2020-11-08 13:42:29.190855          |",
        "| 2020-09-08 12:42:29.190855 | 2020-11-08 12:42:29.190855          |",
        "| 2020-09-08 11:42:29.190855 | 2020-11-08 11:42:29.190855          |",
        "+----------------------------+-------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT ts, ts - INTERVAL '16' YEAR FROM table_b";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+---------------------------------------+",
        "| ts                         | table_b.ts - IntervalYearMonth(\"192\") |",
        "+----------------------------+---------------------------------------+",
        "| 2020-09-08 13:42:29.190855 | 2004-09-08 13:42:29.190855            |",
        "| 2020-09-08 12:42:29.190855 | 2004-09-08 12:42:29.190855            |",
        "| 2020-09-08 11:42:29.190855 | 2004-09-08 11:42:29.190855            |",
        "+----------------------------+---------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn cast_timestamp_before_1970() -> Result<()> {
    // this is a repro for issue #3082
    let ctx = SessionContext::new();

    let sql = "select cast('1969-01-01T00:00:00Z' as timestamp);";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------------------------------+",
        "| Utf8(\"1969-01-01T00:00:00Z\") |",
        "+------------------------------+",
        "| 1969-01-01 00:00:00          |",
        "+------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);

    let sql = "select cast('1969-01-01T00:00:00.1Z' as timestamp);";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------------------------+",
        "| Utf8(\"1969-01-01T00:00:00.1Z\") |",
        "+--------------------------------+",
        "| 1969-01-01 00:00:00.100        |",
        "+--------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);

    Ok(())
}
