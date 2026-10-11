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

//! Regression tests for field-metadata consistency under `UNION ALL` when
//! expression simplification rewrites a cast child from a metadata-free
//! function result into a metadata-carrying column.
//!
//! See <https://github.com/apache/datafusion/issues/26178>.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::assert_batches_eq;
use datafusion::prelude::*;
use datafusion_common::Result;

fn table_batch(with_field_metadata: bool) -> Result<RecordBatch> {
    let mut field = Field::new("b", DataType::Int64, false);
    if with_field_metadata {
        field = field.with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "1".to_string(),
        )]));
    }
    let schema = Arc::new(Schema::new(vec![field]));
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )?)
}

fn register_t(ctx: &SessionContext, with_field_metadata: bool) -> Result<()> {
    ctx.register_batch("t", table_batch(with_field_metadata)?)?;
    Ok(())
}

async fn assert_max_count(
    ctx: &SessionContext,
    sql: &str,
    expected_count: i64,
) -> Result<()> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let expected = [
        "+---+---+".to_string(),
        "| m | c |".to_string(),
        "+---+---+".to_string(),
        format!("| 3 | {expected_count} |"),
        "+---+---+".to_string(),
    ];
    let expected_refs: Vec<&str> = expected.iter().map(String::as_str).collect();
    assert_batches_eq!(&expected_refs, &batches);
    Ok(())
}

const COALESCE_CAST_UNION_AGG: &str = "\
SELECT max(x) AS m, count(x) AS c FROM (\
  SELECT CAST(coalesce(b, 7) AS BIGINT) AS x FROM t \
  UNION ALL \
  SELECT CAST(coalesce(b, 7) AS BIGINT) AS x FROM t\
) q";

const CAST_COLUMN_UNION_AGG: &str = "\
SELECT max(x) AS m, count(x) AS c FROM (\
  SELECT CAST(b AS BIGINT) AS x FROM t \
  UNION ALL \
  SELECT CAST(b AS BIGINT) AS x FROM t\
) q";

const COALESCE_CAST_AGG_NO_UNION: &str = "\
SELECT max(x) AS m, count(x) AS c FROM (\
  SELECT CAST(coalesce(b, 7) AS BIGINT) AS x FROM t\
) q";

const DIRECT_COLUMN_UNION_AGG: &str = "\
SELECT max(x) AS m, count(x) AS c FROM (\
  SELECT b AS x FROM t \
  UNION ALL \
  SELECT b AS x FROM t\
) q";

/// Controls that isolate the failure mode reported in #26178.
#[tokio::test]
async fn coalesce_cast_union_aggregate_controls() -> Result<()> {
    let ctx = SessionContext::new();
    register_t(&ctx, false)?;
    assert_max_count(&ctx, COALESCE_CAST_UNION_AGG, 6).await?;

    let ctx = SessionContext::new();
    register_t(&ctx, true)?;
    assert_max_count(&ctx, CAST_COLUMN_UNION_AGG, 6).await?;
    // Metadata-bearing direct column (no coalesce rewrite): Union schema has
    // field metadata from construction and must keep agreeing with physical.
    assert_max_count(&ctx, DIRECT_COLUMN_UNION_AGG, 6).await?;
    // Without UNION ALL the three input rows are not doubled.
    assert_max_count(&ctx, COALESCE_CAST_AGG_NO_UNION, 3).await?;
    Ok(())
}

/// `CAST(coalesce(non_null_col, lit))` under `UNION ALL` + aggregate must plan
/// when the column carries Arrow field metadata.
///
/// `b` is `NOT NULL`, so `coalesce(b, 7)` simplifies to `b`. The Union's
/// logical schema must be refreshed from the simplified children so it stays
/// consistent with the physical plan's inherited column metadata.
#[tokio::test]
async fn coalesce_cast_union_aggregate_preserves_field_metadata() -> Result<()> {
    let ctx = SessionContext::new();
    register_t(&ctx, true)?;
    assert_max_count(&ctx, COALESCE_CAST_UNION_AGG, 6).await
}

/// One leg with field metadata and one without: empty metadata is skipped in
/// the Union intersection, so planning under an aggregate still succeeds after
/// coalesce simplification on both legs.
#[tokio::test]
async fn coalesce_cast_union_aggregate_mixed_field_metadata() -> Result<()> {
    let ctx = SessionContext::new();

    let with_meta = Field::new("b", DataType::Int64, false).with_metadata(HashMap::from(
        [("PARQUET:field_id".to_string(), "1".to_string())],
    ));
    let without_meta = Field::new("b", DataType::Int64, false);
    ctx.register_batch(
        "t1",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![with_meta])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?,
    )?;
    ctx.register_batch(
        "t2",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![without_meta])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?,
    )?;

    assert_max_count(
        &ctx,
        "SELECT max(x) AS m, count(x) AS c FROM (\
           SELECT CAST(coalesce(b, 7) AS BIGINT) AS x FROM t1 \
           UNION ALL \
           SELECT CAST(coalesce(b, 7) AS BIGINT) AS x FROM t2\
         ) q",
        6,
    )
    .await
}
