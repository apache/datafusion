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

use datafusion::{
    assert_batches_eq,
    prelude::{ParquetReadOptions, SessionContext},
};
use datafusion_common::{test_util::parquet_test_data, Result};

#[tokio::test]
async fn describe() -> Result<()> {
    let ctx = parquet_context().await;

    let describe_record_batch = ctx
        .table("alltypes_tiny_pages")
        .await?
        .describe()
        .await?
        .collect()
        .await?;

    #[rustfmt::skip]
    let expected = [
        "+------------+-------------------+----------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-----------------+------------+-------------------------+--------------------+-------------------+",
        "| describe   | id                | bool_col | tinyint_col        | smallint_col       | int_col            | bigint_col         | float_col          | double_col         | date_string_col | string_col | timestamp_col           | year               | month             |",
        "+------------+-------------------+----------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-----------------+------------+-------------------------+--------------------+-------------------+",
        "| count      | 7300.0            | 7300     | 7300.0             | 7300.0             | 7300.0             | 7300.0             | 7300.0             | 7300.0             | 7300            | 7300       | 7300                    | 7300.0             | 7300.0            |",
        "| null_count | 0.0               | 0        | 0.0                | 0.0                | 0.0                | 0.0                | 0.0                | 0.0                | 0               | 0          | 0                       | 0.0                | 0.0               |",
        "| mean       | 3649.5            | null     | 4.5                | 4.5                | 4.5                | 45.0               | 4.949999964237213  | 45.45              | null            | null       | null                    | 2009.5             | 6.526027397260274 |",
        "| std        | 2107.472815166704 | null     | 2.8724780750809518 | 2.8724780750809518 | 2.8724780750809518 | 28.724780750809533 | 3.1597258182544645 | 29.012028558317645 | null            | null       | null                    | 0.5000342500942125 | 3.44808750051728  |",
        "| min        | 0.0               | null     | 0.0                | 0.0                | 0.0                | 0.0                | 0.0                | 0.0                | 01/01/09        | 0          | 2008-12-31T23:00:00     | 2009.0             | 1.0               |",
        "| max        | 7299.0            | null     | 9.0                | 9.0                | 9.0                | 90.0               | 9.899999618530273  | 90.89999999999999  | 12/31/10        | 9          | 2010-12-31T04:09:13.860 | 2010.0             | 12.0              |",
        "| median     | 3649.0            | null     | 4.0                | 4.0                | 4.0                | 45.0               | 4.949999809265137  | 45.45              | null            | null       | null                    | 2009.0             | 7.0               |",
        "+------------+-------------------+----------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-----------------+------------+-------------------------+--------------------+-------------------+",
    ];
    assert_batches_eq!(expected, &describe_record_batch);
    Ok(())
}

#[tokio::test]
async fn describe_boolean_binary() -> Result<()> {
    let ctx = parquet_context().await;

    //add test case for only boolean boolean/binary column
    let result = ctx
        .sql("select 'a' as a,true as b")
        .await?
        .describe()
        .await?
        .collect()
        .await?;
    #[rustfmt::skip]
    let expected = [
        "+------------+------+------+",
        "| describe   | a    | b    |",
        "+------------+------+------+",
        "| count      | 1    | 1    |",
        "| null_count | 0    | 0    |",
        "| mean       | null | null |",
        "| std        | null | null |",
        "| min        | a    | null |",
        "| max        | a    | null |",
        "| median     | null | null |",
        "+------------+------+------+"
    ];
    assert_batches_eq!(expected, &result);
    Ok(())
}

#[tokio::test]
async fn describe_null() -> Result<()> {
    let ctx = parquet_context().await;

    //add test case for only boolean boolean/binary column
    let result = ctx
        .sql("select 'a' as a, null as b")
        .await?
        .describe()
        .await?
        .collect()
        .await?;
    #[rustfmt::skip]
    let expected = [
        "+------------+------+------+",
        "| describe   | a    | b    |",
        "+------------+------+------+",
        "| count      | 1    | 0    |",
        "| null_count | 0    | 1    |",
        "| mean       | null | null |",
        "| std        | null | null |",
        "| min        | a    | null |",
        "| max        | a    | null |",
        "| median     | null | null |",
        "+------------+------+------+"
    ];
    assert_batches_eq!(expected, &result);
    Ok(())
}

/// Return a SessionContext with parquet file registered
async fn parquet_context() -> SessionContext {
    let ctx = SessionContext::new();
    let testdata = parquet_test_data();
    ctx.register_parquet(
        "alltypes_tiny_pages",
        &format!("{testdata}/alltypes_tiny_pages.parquet"),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    ctx
}
