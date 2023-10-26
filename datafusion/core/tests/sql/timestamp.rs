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
use std::ops::Add;

#[tokio::test]
async fn test_current_timestamp_expressions() -> Result<()> {
    let t1 = chrono::Utc::now().timestamp();
    let ctx = SessionContext::new();
    let actual = execute(&ctx, "SELECT NOW(), NOW() as t2").await;
    let res1 = actual[0][0].as_str();
    let res2 = actual[0][1].as_str();
    let t3 = Utc::now().timestamp();
    let t2_naive = DateTime::parse_from_rfc3339(res1).unwrap();

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
        make_timestamp_tz_table::<TimestampNanosecondType>(Some("+00:00".into()))?;
    ctx.register_table("table_a", table_a)?;
    ctx.register_table("table_b", table_b)?;

    let sql = "SELECT MIN(table_a.ts), MAX(table_b.ts) FROM table_a, table_b";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = [
        "+-------------------------+-----------------------------+",
        "| MIN(table_a.ts)         | MAX(table_b.ts)             |",
        "+-------------------------+-----------------------------+",
        "| 2020-09-08T11:42:29.190 | 2020-09-08T13:42:29.190855Z |",
        "+-------------------------+-----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn timestamp_coercion() -> Result<()> {
    {
        let ctx = SessionContext::new();
        let table_a =
            make_timestamp_tz_table::<TimestampSecondType>(Some("+00:00".into()))?;
        let table_b =
            make_timestamp_tz_table::<TimestampMillisecondType>(Some("+00:00".into()))?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b order by table_a.ts desc, table_b.ts desc";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+----------------------+--------------------------+-------------------------+",
            "| ts                   | ts                       | table_a.ts = table_b.ts |",
            "+----------------------+--------------------------+-------------------------+",
            "| 2020-09-08T13:42:29Z | 2020-09-08T13:42:29.190Z | true                    |",
            "| 2020-09-08T13:42:29Z | 2020-09-08T12:42:29.190Z | false                   |",
            "| 2020-09-08T13:42:29Z | 2020-09-08T11:42:29.190Z | false                   |",
            "| 2020-09-08T12:42:29Z | 2020-09-08T13:42:29.190Z | false                   |",
            "| 2020-09-08T12:42:29Z | 2020-09-08T12:42:29.190Z | true                    |",
            "| 2020-09-08T12:42:29Z | 2020-09-08T11:42:29.190Z | false                   |",
            "| 2020-09-08T11:42:29Z | 2020-09-08T13:42:29.190Z | false                   |",
            "| 2020-09-08T11:42:29Z | 2020-09-08T12:42:29.190Z | false                   |",
            "| 2020-09-08T11:42:29Z | 2020-09-08T11:42:29.190Z | true                    |",
            "+----------------------+--------------------------+-------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let ctx = SessionContext::new();
        let table_a = make_timestamp_table::<TimestampSecondType>()?;
        let table_b = make_timestamp_table::<TimestampMicrosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b order by table_a.ts desc, table_b.ts desc";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+---------------------+----------------------------+-------------------------+",
            "| ts                  | ts                         | table_a.ts = table_b.ts |",
            "+---------------------+----------------------------+-------------------------+",
            "| 2020-09-08T13:42:29 | 2020-09-08T13:42:29.190855 | true                    |",
            "| 2020-09-08T13:42:29 | 2020-09-08T12:42:29.190855 | false                   |",
            "| 2020-09-08T13:42:29 | 2020-09-08T11:42:29.190855 | false                   |",
            "| 2020-09-08T12:42:29 | 2020-09-08T13:42:29.190855 | false                   |",
            "| 2020-09-08T12:42:29 | 2020-09-08T12:42:29.190855 | true                    |",
            "| 2020-09-08T12:42:29 | 2020-09-08T11:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29 | 2020-09-08T13:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29 | 2020-09-08T12:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29 | 2020-09-08T11:42:29.190855 | true                    |",
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

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b order by table_a.ts desc, table_b.ts desc";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+---------------------+----------------------------+-------------------------+",
            "| ts                  | ts                         | table_a.ts = table_b.ts |",
            "+---------------------+----------------------------+-------------------------+",
            "| 2020-09-08T13:42:29 | 2020-09-08T13:42:29.190855 | true                    |",
            "| 2020-09-08T13:42:29 | 2020-09-08T12:42:29.190855 | false                   |",
            "| 2020-09-08T13:42:29 | 2020-09-08T11:42:29.190855 | false                   |",
            "| 2020-09-08T12:42:29 | 2020-09-08T13:42:29.190855 | false                   |",
            "| 2020-09-08T12:42:29 | 2020-09-08T12:42:29.190855 | true                    |",
            "| 2020-09-08T12:42:29 | 2020-09-08T11:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29 | 2020-09-08T13:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29 | 2020-09-08T12:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29 | 2020-09-08T11:42:29.190855 | true                    |",
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

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b order by table_a.ts desc, table_b.ts desc";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+-------------------------+---------------------+-------------------------+",
            "| ts                      | ts                  | table_a.ts = table_b.ts |",
            "+-------------------------+---------------------+-------------------------+",
            "| 2020-09-08T13:42:29.190 | 2020-09-08T13:42:29 | true                    |",
            "| 2020-09-08T13:42:29.190 | 2020-09-08T12:42:29 | false                   |",
            "| 2020-09-08T13:42:29.190 | 2020-09-08T11:42:29 | false                   |",
            "| 2020-09-08T12:42:29.190 | 2020-09-08T13:42:29 | false                   |",
            "| 2020-09-08T12:42:29.190 | 2020-09-08T12:42:29 | true                    |",
            "| 2020-09-08T12:42:29.190 | 2020-09-08T11:42:29 | false                   |",
            "| 2020-09-08T11:42:29.190 | 2020-09-08T13:42:29 | false                   |",
            "| 2020-09-08T11:42:29.190 | 2020-09-08T12:42:29 | false                   |",
            "| 2020-09-08T11:42:29.190 | 2020-09-08T11:42:29 | true                    |",
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

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b order by table_a.ts desc, table_b.ts desc";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+-------------------------+----------------------------+-------------------------+",
            "| ts                      | ts                         | table_a.ts = table_b.ts |",
            "+-------------------------+----------------------------+-------------------------+",
            "| 2020-09-08T13:42:29.190 | 2020-09-08T13:42:29.190855 | true                    |",
            "| 2020-09-08T13:42:29.190 | 2020-09-08T12:42:29.190855 | false                   |",
            "| 2020-09-08T13:42:29.190 | 2020-09-08T11:42:29.190855 | false                   |",
            "| 2020-09-08T12:42:29.190 | 2020-09-08T13:42:29.190855 | false                   |",
            "| 2020-09-08T12:42:29.190 | 2020-09-08T12:42:29.190855 | true                    |",
            "| 2020-09-08T12:42:29.190 | 2020-09-08T11:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29.190 | 2020-09-08T13:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29.190 | 2020-09-08T12:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29.190 | 2020-09-08T11:42:29.190855 | true                    |",
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

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b order by table_a.ts desc, table_b.ts desc";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+-------------------------+----------------------------+-------------------------+",
            "| ts                      | ts                         | table_a.ts = table_b.ts |",
            "+-------------------------+----------------------------+-------------------------+",
            "| 2020-09-08T13:42:29.190 | 2020-09-08T13:42:29.190855 | true                    |",
            "| 2020-09-08T13:42:29.190 | 2020-09-08T12:42:29.190855 | false                   |",
            "| 2020-09-08T13:42:29.190 | 2020-09-08T11:42:29.190855 | false                   |",
            "| 2020-09-08T12:42:29.190 | 2020-09-08T13:42:29.190855 | false                   |",
            "| 2020-09-08T12:42:29.190 | 2020-09-08T12:42:29.190855 | true                    |",
            "| 2020-09-08T12:42:29.190 | 2020-09-08T11:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29.190 | 2020-09-08T13:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29.190 | 2020-09-08T12:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29.190 | 2020-09-08T11:42:29.190855 | true                    |",
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

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b order by table_a.ts desc, table_b.ts desc";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+----------------------------+---------------------+-------------------------+",
            "| ts                         | ts                  | table_a.ts = table_b.ts |",
            "+----------------------------+---------------------+-------------------------+",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T13:42:29 | true                    |",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T12:42:29 | false                   |",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T11:42:29 | false                   |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T13:42:29 | false                   |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T12:42:29 | true                    |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T11:42:29 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T13:42:29 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T12:42:29 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T11:42:29 | true                    |",
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

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b order by table_a.ts desc, table_b.ts desc";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+----------------------------+-------------------------+-------------------------+",
            "| ts                         | ts                      | table_a.ts = table_b.ts |",
            "+----------------------------+-------------------------+-------------------------+",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T13:42:29.190 | true                    |",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T12:42:29.190 | false                   |",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T11:42:29.190 | false                   |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T13:42:29.190 | false                   |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T12:42:29.190 | true                    |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T11:42:29.190 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T13:42:29.190 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T12:42:29.190 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T11:42:29.190 | true                    |",
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

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b order by table_a.ts desc, table_b.ts desc";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+----------------------------+----------------------------+-------------------------+",
            "| ts                         | ts                         | table_a.ts = table_b.ts |",
            "+----------------------------+----------------------------+-------------------------+",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T13:42:29.190855 | true                    |",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T12:42:29.190855 | false                   |",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T11:42:29.190855 | false                   |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T13:42:29.190855 | false                   |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T12:42:29.190855 | true                    |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T11:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T13:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T12:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T11:42:29.190855 | true                    |",
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

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b order by table_a.ts desc, table_b.ts desc";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+----------------------------+---------------------+-------------------------+",
            "| ts                         | ts                  | table_a.ts = table_b.ts |",
            "+----------------------------+---------------------+-------------------------+",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T13:42:29 | true                    |",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T12:42:29 | false                   |",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T11:42:29 | false                   |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T13:42:29 | false                   |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T12:42:29 | true                    |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T11:42:29 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T13:42:29 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T12:42:29 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T11:42:29 | true                    |",
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

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b order by table_a.ts desc, table_b.ts desc";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+----------------------------+-------------------------+-------------------------+",
            "| ts                         | ts                      | table_a.ts = table_b.ts |",
            "+----------------------------+-------------------------+-------------------------+",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T13:42:29.190 | true                    |",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T12:42:29.190 | false                   |",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T11:42:29.190 | false                   |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T13:42:29.190 | false                   |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T12:42:29.190 | true                    |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T11:42:29.190 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T13:42:29.190 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T12:42:29.190 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T11:42:29.190 | true                    |",
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

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b order by table_a.ts desc, table_b.ts desc";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+----------------------------+----------------------------+-------------------------+",
            "| ts                         | ts                         | table_a.ts = table_b.ts |",
            "+----------------------------+----------------------------+-------------------------+",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T13:42:29.190855 | true                    |",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T12:42:29.190855 | false                   |",
            "| 2020-09-08T13:42:29.190855 | 2020-09-08T11:42:29.190855 | false                   |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T13:42:29.190855 | false                   |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T12:42:29.190855 | true                    |",
            "| 2020-09-08T12:42:29.190855 | 2020-09-08T11:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T13:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T12:42:29.190855 | false                   |",
            "| 2020-09-08T11:42:29.190855 | 2020-09-08T11:42:29.190855 | true                    |",
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
    let base_dt = Utc.with_ymd_and_hms(2018, 7, 1, 6, 0, 0).unwrap(); // 2018-Jul-01 06:00
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
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60])),
        ],
    )?;
    ctx.register_batch("t1", data).unwrap();

    let sql =
        "SELECT timestamp, SUM(count) FROM t1 GROUP BY timestamp ORDER BY timestamp ASC";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = [
        "+---------------------+---------------+",
        "| timestamp           | SUM(t1.count) |",
        "+---------------------+---------------+",
        "| 2018-07-01T06:00:00 | 80            |",
        "| 2018-07-01T07:00:00 | 130           |",
        "+---------------------+---------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn timestamp_add_interval_second() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "SELECT NOW(), NOW() + INTERVAL '1' SECOND;";
    let results = execute_to_batches(&ctx, sql).await;
    let actual = result_vec(&results);

    let res1 = actual[0][0].as_str();
    let res2 = actual[0][1].as_str();

    let t1_naive = DateTime::parse_from_rfc3339(res1).unwrap();
    let t2_naive = DateTime::parse_from_rfc3339(res2).unwrap();

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

    let t1_naive = DateTime::parse_from_rfc3339(res1).unwrap();
    let t2_naive = chrono::DateTime::parse_from_rfc3339(res2).unwrap();

    assert_eq!(t1_naive.sub(Duration::days(8)), t2_naive);
    Ok(())
}

#[tokio::test]
async fn timestamp_add_interval_months() -> Result<()> {
    let ctx = SessionContext::new();
    let table_a =
        make_timestamp_tz_table::<TimestampNanosecondType>(Some("+00:00".into()))?;
    ctx.register_table("table_a", table_a)?;

    let sql = "SELECT ts, ts + INTERVAL '17' MONTH FROM table_a;";
    let results = execute_to_batches(&ctx, sql).await;
    let actual_vec = result_vec(&results);

    for actual in actual_vec {
        let res1 = actual[0].as_str();
        let res2 = actual[1].as_str();

        let format = "%Y-%m-%dT%H:%M:%S%.6fZ";
        let t1_naive = NaiveDateTime::parse_from_str(res1, format).unwrap();
        let t2_naive = NaiveDateTime::parse_from_str(res2, format).unwrap();

        let year = t1_naive.year() + (t1_naive.month0() as i32 + 17) / 12;
        let month = (t1_naive.month0() + 17) % 12 + 1;

        assert_eq!(
            t1_naive.with_year(year).unwrap().with_month(month).unwrap(),
            t2_naive
        );
    }
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

    let t1_naive = DateTime::parse_from_rfc3339(res1).unwrap();
    let t2_naive = DateTime::parse_from_rfc3339(res2).unwrap();

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
    let expected = [
        "+----------------------------+----------------------------------------------+",
        "| ts                         | table_a.ts - IntervalMonthDayNano(\"8000000\") |",
        "+----------------------------+----------------------------------------------+",
        "| 2020-09-08T13:42:29.190855 | 2020-09-08T13:42:29.182855                   |",
        "| 2020-09-08T12:42:29.190855 | 2020-09-08T12:42:29.182855                   |",
        "| 2020-09-08T11:42:29.190855 | 2020-09-08T11:42:29.182855                   |",
        "+----------------------------+----------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT ts, ts + INTERVAL '1' SECOND FROM table_b";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = ["+----------------------------+-------------------------------------------------+",
        "| ts                         | table_b.ts + IntervalMonthDayNano(\"1000000000\") |",
        "+----------------------------+-------------------------------------------------+",
        "| 2020-09-08T13:42:29.190855 | 2020-09-08T13:42:30.190855                      |",
        "| 2020-09-08T12:42:29.190855 | 2020-09-08T12:42:30.190855                      |",
        "| 2020-09-08T11:42:29.190855 | 2020-09-08T11:42:30.190855                      |",
        "+----------------------------+-------------------------------------------------+"];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT ts, ts + INTERVAL '2' MONTH FROM table_b";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = ["+----------------------------+---------------------------------------------------------------------+",
        "| ts                         | table_b.ts + IntervalMonthDayNano(\"158456325028528675187087900672\") |",
        "+----------------------------+---------------------------------------------------------------------+",
        "| 2020-09-08T13:42:29.190855 | 2020-11-08T13:42:29.190855                                          |",
        "| 2020-09-08T12:42:29.190855 | 2020-11-08T12:42:29.190855                                          |",
        "| 2020-09-08T11:42:29.190855 | 2020-11-08T11:42:29.190855                                          |",
        "+----------------------------+---------------------------------------------------------------------+"];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT ts, ts - INTERVAL '16' YEAR FROM table_b";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = ["+----------------------------+-----------------------------------------------------------------------+",
        "| ts                         | table_b.ts - IntervalMonthDayNano(\"15211807202738752817960438464512\") |",
        "+----------------------------+-----------------------------------------------------------------------+",
        "| 2020-09-08T13:42:29.190855 | 2004-09-08T13:42:29.190855                                            |",
        "| 2020-09-08T12:42:29.190855 | 2004-09-08T12:42:29.190855                                            |",
        "| 2020-09-08T11:42:29.190855 | 2004-09-08T11:42:29.190855                                            |",
        "+----------------------------+-----------------------------------------------------------------------+"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn cast_timestamp_before_1970() -> Result<()> {
    // this is a repro for issue #3082
    let ctx = SessionContext::new();

    let sql = "select cast('1969-01-01T00:00:00Z' as timestamp);";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = [
        "+------------------------------+",
        "| Utf8(\"1969-01-01T00:00:00Z\") |",
        "+------------------------------+",
        "| 1969-01-01T00:00:00          |",
        "+------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);

    let sql = "select cast('1969-01-01T00:00:00.1Z' as timestamp);";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = [
        "+--------------------------------+",
        "| Utf8(\"1969-01-01T00:00:00.1Z\") |",
        "+--------------------------------+",
        "| 1969-01-01T00:00:00.100        |",
        "+--------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_arrow_typeof() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select arrow_typeof(date_trunc('minute', to_timestamp_seconds(61)));";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = [
        "+--------------------------------------------------------------------------+",
        "| arrow_typeof(date_trunc(Utf8(\"minute\"),to_timestamp_seconds(Int64(61)))) |",
        "+--------------------------------------------------------------------------+",
        "| Timestamp(Second, None)                                                  |",
        "+--------------------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "select arrow_typeof(date_trunc('second', to_timestamp_millis(61)));";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = [
        "+-------------------------------------------------------------------------+",
        "| arrow_typeof(date_trunc(Utf8(\"second\"),to_timestamp_millis(Int64(61)))) |",
        "+-------------------------------------------------------------------------+",
        "| Timestamp(Millisecond, None)                                            |",
        "+-------------------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "select arrow_typeof(date_trunc('millisecond', to_timestamp_micros(61)));";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = ["+------------------------------------------------------------------------------+",
        "| arrow_typeof(date_trunc(Utf8(\"millisecond\"),to_timestamp_micros(Int64(61)))) |",
        "+------------------------------------------------------------------------------+",
        "| Timestamp(Microsecond, None)                                                 |",
        "+------------------------------------------------------------------------------+"];
    assert_batches_eq!(expected, &actual);

    let sql = "select arrow_typeof(date_trunc('microsecond', to_timestamp(61)));";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = [
        "+-----------------------------------------------------------------------+",
        "| arrow_typeof(date_trunc(Utf8(\"microsecond\"),to_timestamp(Int64(61)))) |",
        "+-----------------------------------------------------------------------+",
        "| Timestamp(Second, None)                                               |",
        "+-----------------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn cast_timestamp_to_timestamptz() -> Result<()> {
    let ctx = SessionContext::new();
    let table_a = make_timestamp_table::<TimestampNanosecondType>()?;

    ctx.register_table("table_a", table_a)?;

    let sql = "SELECT ts::timestamptz, arrow_typeof(ts::timestamptz) FROM table_a;";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = [
        "+-----------------------------+---------------------------------------+",
        "| table_a.ts                  | arrow_typeof(table_a.ts)              |",
        "+-----------------------------+---------------------------------------+",
        "| 2020-09-08T13:42:29.190855Z | Timestamp(Nanosecond, Some(\"+00:00\")) |",
        "| 2020-09-08T12:42:29.190855Z | Timestamp(Nanosecond, Some(\"+00:00\")) |",
        "| 2020-09-08T11:42:29.190855Z | Timestamp(Nanosecond, Some(\"+00:00\")) |",
        "+-----------------------------+---------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_cast_to_time() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT 0::TIME";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = [
        "+----------+",
        "| Int64(0) |",
        "+----------+",
        "| 00:00:00 |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_cast_to_time_with_time_zone_should_not_work() -> Result<()> {
    // this should not work until we implement tz for DataType::Time64
    let ctx = SessionContext::new();
    let sql = "SELECT 0::TIME WITH TIME ZONE";
    let results = plan_and_collect(&ctx, sql).await.unwrap_err();

    assert_eq!(
        results.strip_backtrace(),
        "This feature is not implemented: Unsupported SQL type Time(None, WithTimeZone)"
    );

    Ok(())
}

#[tokio::test]
async fn test_cast_to_time_without_time_zone() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT 0::TIME WITHOUT TIME ZONE";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = [
        "+----------+",
        "| Int64(0) |",
        "+----------+",
        "| 00:00:00 |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_cast_to_timetz_should_not_work() -> Result<()> {
    // this should not work until we implement tz for DataType::Time64
    let ctx = SessionContext::new();
    let sql = "SELECT 0::TIMETZ";
    let results = plan_and_collect(&ctx, sql).await.unwrap_err();

    assert_eq!(
        results.strip_backtrace(),
        "This feature is not implemented: Unsupported SQL type Time(None, Tz)"
    );
    Ok(())
}

#[tokio::test]
async fn test_current_date() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select current_date() dt";
    let results = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        results[0]
            .schema()
            .field_with_name("dt")
            .unwrap()
            .data_type()
            .to_owned(),
        DataType::Date32
    );

    let sql = "select case when current_date() = cast(now() as date) then 'OK' else 'FAIL' end result";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = [
        "+--------+",
        "| result |",
        "+--------+",
        "| OK     |",
        "+--------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn test_current_time() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select current_time() dt";
    let results = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        results[0]
            .schema()
            .field_with_name("dt")
            .unwrap()
            .data_type()
            .to_owned(),
        DataType::Time64(TimeUnit::Nanosecond)
    );

    let sql = "select case when current_time() = (now()::bigint % 86400000000000)::time then 'OK' else 'FAIL' end result";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = [
        "+--------+",
        "| result |",
        "+--------+",
        "| OK     |",
        "+--------+",
    ];

    assert_batches_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn test_ts_dt_binary_ops() -> Result<()> {
    let ctx = SessionContext::new();
    // test cast in where clause
    let sql =
        "select count(1) result from (select now() as n) a where n = '2000-01-01'::date";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = [
        "+--------+",
        "| result |",
        "+--------+",
        "| 0      |",
        "+--------+",
    ];

    assert_batches_eq!(expected, &results);

    // test cast in where ge clause
    let sql =
        "select count(1) result from (select now() as n) a where n >= '2000-01-01'::date";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = [
        "+--------+",
        "| result |",
        "+--------+",
        "| 1      |",
        "+--------+",
    ];

    assert_batches_eq!(expected, &results);

    // test cast in equal select
    let sql = "select now() = '2000-01-01'::date as result";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = [
        "+--------+",
        "| result |",
        "+--------+",
        "| false  |",
        "+--------+",
    ];

    assert_batches_eq!(expected, &results);

    // test cast in gt select
    let sql = "select now() >= '2000-01-01'::date as result";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = [
        "+--------+",
        "| result |",
        "+--------+",
        "| true   |",
        "+--------+",
    ];

    assert_batches_eq!(expected, &results);

    //test cast path timestamp date using literals
    let sql = "select '2000-01-01'::timestamp >= '2000-01-01'::date";
    let df = ctx.sql(sql).await.unwrap();

    let plan = df.explain(true, false)?.collect().await?;
    let batch = &plan[0];
    let mut res: Option<String> = None;
    for row in 0..batch.num_rows() {
        if &array_value_to_string(batch.column(0), row)? == "initial_logical_plan" {
            res = Some(array_value_to_string(batch.column(1), row)?);
            break;
        }
    }
    assert_eq!(res, Some("Projection: CAST(Utf8(\"2000-01-01\") AS Timestamp(Nanosecond, None)) >= CAST(Utf8(\"2000-01-01\") AS Date32)\n  EmptyRelation".to_string()));

    //test cast path timestamp date using function
    let sql = "select now() >= '2000-01-01'::date";
    let df = ctx.sql(sql).await.unwrap();

    let plan = df.explain(true, false)?.collect().await?;
    let batch = &plan[0];
    let mut res: Option<String> = None;
    for row in 0..batch.num_rows() {
        if &array_value_to_string(batch.column(0), row)? == "initial_logical_plan" {
            res = Some(array_value_to_string(batch.column(1), row)?);
            break;
        }
    }
    assert_eq!(
        res,
        Some(
            "Projection: now() >= CAST(Utf8(\"2000-01-01\") AS Date32)\n  EmptyRelation"
                .to_string()
        )
    );

    let sql = "select now() = current_date()";
    let df = ctx.sql(sql).await.unwrap();

    let plan = df.explain(true, false)?.collect().await?;
    let batch = &plan[0];
    let mut res: Option<String> = None;
    for row in 0..batch.num_rows() {
        if &array_value_to_string(batch.column(0), row)? == "initial_logical_plan" {
            res = Some(array_value_to_string(batch.column(1), row)?);
            break;
        }
    }
    assert_eq!(
        res,
        Some("Projection: now() = current_date()\n  EmptyRelation".to_string())
    );

    Ok(())
}

// Cannot remove to sqllogictest, timezone support is not ready there.
#[tokio::test]
async fn timestamp_sub_with_tz() -> Result<()> {
    let ctx = SessionContext::new();
    let table_a = make_timestamp_tz_sub_table::<TimestampMillisecondType>(
        Some("America/Los_Angeles".into()),
        Some("Europe/Istanbul".into()),
    )?;
    ctx.register_table("table_a", table_a)?;

    let sql = "SELECT val, ts1 - ts2 AS ts_diff FROM table_a ORDER BY ts2 - ts1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = [
        "+-----+-----------------------------------+",
        "| val | ts_diff                           |",
        "+-----+-----------------------------------+",
        "| 3   | 0 days 0 hours 0 mins 30.000 secs |",
        "| 1   | 0 days 0 hours 0 mins 20.000 secs |",
        "| 2   | 0 days 0 hours 0 mins 10.000 secs |",
        "+-----+-----------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}
