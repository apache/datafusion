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

use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_common::test_util::batches_to_string;
use datafusion_common::{Result, test_util::parquet_test_data};
use insta::assert_snapshot;
use std::sync::Arc;

use arrow::array::{FixedSizeBinaryArray, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};

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

    assert_snapshot!(
        batches_to_string(&describe_record_batch),
        @r"
    +------------+-------------------+----------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-----------------+------------+-------------------------+--------------------+-------------------+
    | describe   | id                | bool_col | tinyint_col        | smallint_col       | int_col            | bigint_col         | float_col          | double_col         | date_string_col | string_col | timestamp_col           | year               | month             |
    +------------+-------------------+----------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-----------------+------------+-------------------------+--------------------+-------------------+
    | count      | 7300.0            | 7300     | 7300.0             | 7300.0             | 7300.0             | 7300.0             | 7300.0             | 7300.0             | 7300            | 7300       | 7300                    | 7300.0             | 7300.0            |
    | null_count | 0.0               | 0        | 0.0                | 0.0                | 0.0                | 0.0                | 0.0                | 0.0                | 0               | 0          | 0                       | 0.0                | 0.0               |
    | mean       | 3649.5            | null     | 4.5                | 4.5                | 4.5                | 45.0               | 4.949999964237213  | 45.45              | null            | null       | null                    | 2009.5             | 6.526027397260274 |
    | std        | 2107.472815166704 | null     | 2.8724780750809518 | 2.8724780750809518 | 2.8724780750809518 | 28.724780750809533 | 3.1597258182544645 | 29.012028558317645 | null            | null       | null                    | 0.5000342500942125 | 3.44808750051728  |
    | min        | 0.0               | null     | 0.0                | 0.0                | 0.0                | 0.0                | 0.0                | 0.0                | 01/01/09        | 0          | 2008-12-31T23:00:00     | 2009.0             | 1.0               |
    | max        | 7299.0            | null     | 9.0                | 9.0                | 9.0                | 90.0               | 9.899999618530273  | 90.89999999999999  | 12/31/10        | 9          | 2010-12-31T04:09:13.860 | 2010.0             | 12.0              |
    | median     | 3649.0            | null     | 4.0                | 4.0                | 4.0                | 45.0               | 4.949999809265137  | 45.45              | null            | null       | null                    | 2009.0             | 7.0               |
    +------------+-------------------+----------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-----------------+------------+-------------------------+--------------------+-------------------+
    ");
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

    assert_snapshot!(
        batches_to_string(&result),
        @r"
    +------------+------+------+
    | describe   | a    | b    |
    +------------+------+------+
    | count      | 1    | 1    |
    | null_count | 0    | 0    |
    | mean       | null | null |
    | std        | null | null |
    | min        | a    | null |
    | max        | a    | null |
    | median     | null | null |
    +------------+------+------+
    ");
    Ok(())
}

#[tokio::test]
async fn describe_fixed_size_binary() -> Result<()> {
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "fsb",
            DataType::FixedSizeBinary(3),
            true,
        )])),
        vec![Arc::new(FixedSizeBinaryArray::from(vec![
            Some(&[1_u8, 2, 3][..]),
            None,
            Some(&[4_u8, 5, 6][..]),
        ]))],
    )?;
    ctx.register_batch("test", batch)?;

    let result = ctx.table("test").await?.describe().await?.collect().await?;

    assert_snapshot!(
        batches_to_string(&result),
        @r"
    +------------+------+
    | describe   | fsb  |
    +------------+------+
    | count      | 2    |
    | null_count | 1    |
    | mean       | null |
    | std        | null |
    | min        | null |
    | max        | null |
    | median     | null |
    +------------+------+
    "
    );
    Ok(())
}

#[tokio::test]
async fn describe_mixed_numeric_and_fixed_size_binary() -> Result<()> {
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("num", DataType::Int32, true),
            Field::new("fsb", DataType::FixedSizeBinary(3), true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![Some(10), Some(20), Some(30)])),
            Arc::new(FixedSizeBinaryArray::from(vec![
                Some(&[1_u8, 2, 3][..]),
                None,
                Some(&[4_u8, 5, 6][..]),
            ])),
        ],
    )?;
    ctx.register_batch("test_mixed", batch)?;

    let result = ctx
        .table("test_mixed")
        .await?
        .describe()
        .await?
        .collect()
        .await?;

    // num is numeric so min/max/mean/median/std are computed;
    // fsb is FixedSizeBinary so it is filtered out of min/max but still
    // appears in count/null_count. This exercises the filter path (partial
    // column list in the aggregate) rather than the empty-aggregate fallback.
    assert_snapshot!(
        batches_to_string(&result),
        @r"
    +------------+------+------+
    | describe   | num  | fsb  |
    +------------+------+------+
    | count      | 3.0  | 2    |
    | null_count | 0.0  | 1    |
    | mean       | 20.0 | null |
    | std        | 10.0 | null |
    | min        | 10.0 | null |
    | max        | 30.0 | null |
    | median     | 20.0 | null |
    +------------+------+------+
    "
    );
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

    assert_snapshot!(
        batches_to_string(&result),
        @r"
    +------------+------+------+
    | describe   | a    | b    |
    +------------+------+------+
    | count      | 1    | 0    |
    | null_count | 0    | 1    |
    | mean       | null | null |
    | std        | null | null |
    | min        | a    | null |
    | max        | a    | null |
    | median     | null | null |
    +------------+------+------+
    ");
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
