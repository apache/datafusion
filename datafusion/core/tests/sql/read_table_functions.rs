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

//! Tests for built-in `read_parquet`, `read_csv`, and `read_json` table functions.

use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::{assert_batches_eq, assert_contains};

use tempfile::TempDir;

// ---------------------------------------------------------------------------
// read_parquet tests
// ---------------------------------------------------------------------------

/// Create a temporary parquet file with known data and return the path.
async fn write_test_parquet(dir: &TempDir) -> Result<String> {
    let ctx = SessionContext::new();
    let path = dir
        .path()
        .join("test.parquet")
        .to_str()
        .unwrap()
        .to_string();

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0])),
        ],
    )?;

    ctx.read_batch(batch)?
        .write_parquet(
            &path,
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await?;

    Ok(path)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_parquet_basic() -> Result<()> {
    let tmp = TempDir::new()?;
    let path = write_test_parquet(&tmp).await?;

    let ctx = SessionContext::new();
    let results = ctx
        .sql(&format!("SELECT * FROM read_parquet('{path}') ORDER BY id"))
        .await?
        .collect()
        .await?;

    #[cfg_attr(any(), rustfmt::skip)]
    assert_batches_eq!(&[
        "+----+------+-------+",
        "| id | name | value |",
        "+----+------+-------+",
        "| 1  | a    | 1.0   |",
        "| 2  | b    | 2.0   |",
        "| 3  | c    | 3.0   |",
        "| 4  | d    | 4.0   |",
        "| 5  | e    | 5.0   |",
        "+----+------+-------+",
    ], &results);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_parquet_with_filter() -> Result<()> {
    let tmp = TempDir::new()?;
    let path = write_test_parquet(&tmp).await?;

    let ctx = SessionContext::new();
    let results = ctx
        .sql(&format!(
            "SELECT id, name FROM read_parquet('{path}') WHERE id > 3 ORDER BY id"
        ))
        .await?
        .collect()
        .await?;

    #[cfg_attr(any(), rustfmt::skip)]
    assert_batches_eq!(&[
        "+----+------+",
        "| id | name |",
        "+----+------+",
        "| 4  | d    |",
        "| 5  | e    |",
        "+----+------+",
    ], &results);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_parquet_projection() -> Result<()> {
    let tmp = TempDir::new()?;
    let path = write_test_parquet(&tmp).await?;

    let ctx = SessionContext::new();
    let results = ctx
        .sql(&format!(
            "SELECT name FROM read_parquet('{path}') ORDER BY name"
        ))
        .await?
        .collect()
        .await?;

    #[cfg_attr(any(), rustfmt::skip)]
    assert_batches_eq!(&[
        "+------+",
        "| name |",
        "+------+",
        "| a    |",
        "| b    |",
        "| c    |",
        "| d    |",
        "| e    |",
        "+------+",
    ], &results);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_parquet_count() -> Result<()> {
    let tmp = TempDir::new()?;
    let path = write_test_parquet(&tmp).await?;

    let ctx = SessionContext::new();
    let results = ctx
        .sql(&format!(
            "SELECT count(*) AS cnt FROM read_parquet('{path}')"
        ))
        .await?
        .collect()
        .await?;

    #[cfg_attr(any(), rustfmt::skip)]
    assert_batches_eq!(&[
        "+-----+",
        "| cnt |",
        "+-----+",
        "| 5   |",
        "+-----+",
    ], &results);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_parquet_glob() -> Result<()> {
    let tmp = TempDir::new()?;
    let ctx = SessionContext::new();

    let batch1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )?;
    let batch2 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![3, 4]))],
    )?;

    let path1 = tmp
        .path()
        .join("part1.parquet")
        .to_str()
        .unwrap()
        .to_string();
    let path2 = tmp
        .path()
        .join("part2.parquet")
        .to_str()
        .unwrap()
        .to_string();

    ctx.read_batch(batch1)?
        .write_parquet(
            &path1,
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await?;
    ctx.read_batch(batch2)?
        .write_parquet(
            &path2,
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await?;

    let glob_path = tmp.path().join("*.parquet").to_str().unwrap().to_string();
    let results = ctx
        .sql(&format!(
            "SELECT count(*) AS cnt FROM read_parquet('{glob_path}')"
        ))
        .await?
        .collect()
        .await?;

    #[cfg_attr(any(), rustfmt::skip)]
    assert_batches_eq!(&[
        "+-----+",
        "| cnt |",
        "+-----+",
        "| 4   |",
        "+-----+",
    ], &results);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_parquet_no_args_error() -> Result<()> {
    let ctx = SessionContext::new();
    let result = ctx.sql("SELECT * FROM read_parquet()").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert_contains!(err, "read_parquet requires exactly 1 argument");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_parquet_filter_pushdown() -> Result<()> {
    let tmp = TempDir::new()?;
    let path = write_test_parquet(&tmp).await?;

    let ctx = SessionContext::new();
    let df = ctx
        .sql(&format!(
            "EXPLAIN SELECT * FROM read_parquet('{path}') WHERE id > 3"
        ))
        .await?;
    let results = df.collect().await?;

    // The filter should be pushed down — look for it in the physical plan
    let plan_str = arrow::util::pretty::pretty_format_batches(&results)?.to_string();
    // Filter pushdown means the predicate appears inside the DataSourceExec scan
    assert_contains!(plan_str, "predicate=id@0 > 3");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_parquet_projection_pushdown() -> Result<()> {
    let tmp = TempDir::new()?;
    let path = write_test_parquet(&tmp).await?;

    let ctx = SessionContext::new();
    let df = ctx
        .sql(&format!("EXPLAIN SELECT name FROM read_parquet('{path}')"))
        .await?;
    let results = df.collect().await?;

    let plan_str = arrow::util::pretty::pretty_format_batches(&results)?.to_string();
    // Projection pushdown means only 'name' column is read — the projection
    // should appear in the scan node
    assert_contains!(plan_str, "projection=[name]");
    Ok(())
}

// ---------------------------------------------------------------------------
// read_csv tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_csv_basic() -> Result<()> {
    let tmp = TempDir::new()?;
    let path = tmp.path().join("test.csv").to_str().unwrap().to_string();

    std::fs::write(&path, "id,name,value\n1,a,1.0\n2,b,2.0\n3,c,3.0\n")?;

    let ctx = SessionContext::new();
    let results = ctx
        .sql(&format!("SELECT * FROM read_csv('{path}') ORDER BY id"))
        .await?
        .collect()
        .await?;

    #[cfg_attr(any(), rustfmt::skip)]
    assert_batches_eq!(&[
        "+----+------+-------+",
        "| id | name | value |",
        "+----+------+-------+",
        "| 1  | a    | 1.0   |",
        "| 2  | b    | 2.0   |",
        "| 3  | c    | 3.0   |",
        "+----+------+-------+",
    ], &results);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_csv_with_filter() -> Result<()> {
    let tmp = TempDir::new()?;
    let path = tmp.path().join("test.csv").to_str().unwrap().to_string();

    std::fs::write(&path, "id,name,value\n1,a,1.0\n2,b,2.0\n3,c,3.0\n")?;

    let ctx = SessionContext::new();
    let results = ctx
        .sql(&format!(
            "SELECT id, name FROM read_csv('{path}') WHERE id > 1 ORDER BY id"
        ))
        .await?
        .collect()
        .await?;

    #[cfg_attr(any(), rustfmt::skip)]
    assert_batches_eq!(&[
        "+----+------+",
        "| id | name |",
        "+----+------+",
        "| 2  | b    |",
        "| 3  | c    |",
        "+----+------+",
    ], &results);

    Ok(())
}

// ---------------------------------------------------------------------------
// read_json tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_json_basic() -> Result<()> {
    let tmp = TempDir::new()?;
    let path = tmp.path().join("test.json").to_str().unwrap().to_string();

    std::fs::write(
        &path,
        r#"{"id":1,"name":"a"}
{"id":2,"name":"b"}
{"id":3,"name":"c"}
"#,
    )?;

    let ctx = SessionContext::new();
    let results = ctx
        .sql(&format!("SELECT * FROM read_json('{path}') ORDER BY id"))
        .await?
        .collect()
        .await?;

    #[cfg_attr(any(), rustfmt::skip)]
    assert_batches_eq!(&[
        "+----+------+",
        "| id | name |",
        "+----+------+",
        "| 1  | a    |",
        "| 2  | b    |",
        "| 3  | c    |",
        "+----+------+",
    ], &results);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_json_with_filter() -> Result<()> {
    let tmp = TempDir::new()?;
    let path = tmp.path().join("test.json").to_str().unwrap().to_string();

    std::fs::write(
        &path,
        r#"{"id":1,"name":"a"}
{"id":2,"name":"b"}
{"id":3,"name":"c"}
"#,
    )?;

    let ctx = SessionContext::new();
    let results = ctx
        .sql(&format!(
            "SELECT name FROM read_json('{path}') WHERE id >= 2 ORDER BY name"
        ))
        .await?
        .collect()
        .await?;

    #[cfg_attr(any(), rustfmt::skip)]
    assert_batches_eq!(&[
        "+------+",
        "| name |",
        "+------+",
        "| b    |",
        "| c    |",
        "+------+",
    ], &results);

    Ok(())
}

// ---------------------------------------------------------------------------
// Error-path tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_csv_no_args_error() -> Result<()> {
    let ctx = SessionContext::new();
    let result = ctx.sql("SELECT * FROM read_csv()").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert_contains!(err, "read_csv requires exactly 1 argument");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_json_no_args_error() -> Result<()> {
    let ctx = SessionContext::new();
    let result = ctx.sql("SELECT * FROM read_json()").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert_contains!(err, "read_json requires exactly 1 argument");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_parquet_wrong_arg_type() -> Result<()> {
    let ctx = SessionContext::new();
    let result = ctx.sql("SELECT * FROM read_parquet(42)").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert_contains!(err, "read_parquet requires a string literal path argument");
    Ok(())
}

// ---------------------------------------------------------------------------
// Single-threaded runtime tests — verify no panic on current_thread runtime
// ---------------------------------------------------------------------------

#[tokio::test]
async fn read_parquet_single_threaded_runtime() -> Result<()> {
    let tmp = TempDir::new()?;
    let path = write_test_parquet(&tmp).await?;

    let ctx = SessionContext::new();
    let results = ctx
        .sql(&format!(
            "SELECT count(*) AS cnt FROM read_parquet('{path}')"
        ))
        .await?
        .collect()
        .await?;

    #[cfg_attr(any(), rustfmt::skip)]
    assert_batches_eq!(&[
        "+-----+",
        "| cnt |",
        "+-----+",
        "| 5   |",
        "+-----+",
    ], &results);

    Ok(())
}
