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

//! Write-time integer type narrowing via embedded `ARROW:schema` metadata.

use std::fs;
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use bytes::Bytes;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use datafusion_common::config::TableParquetOptions;
use parquet::arrow::{ARROW_SCHEMA_META_KEY, encode_arrow_schema};
use parquet::file::reader::{FileReader, SerializedFileReader};
use tempfile::TempDir;

/// Last `ARROW:schema` value in file KV metadata (appended narrowed hint wins).
fn last_arrow_schema_kv(path: &std::path::Path) -> String {
    let reader = SerializedFileReader::new(Bytes::from(fs::read(path).unwrap())).unwrap();
    let kvs = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("expected key_value_metadata");

    kvs.iter()
        .rev()
        .find(|kv| kv.key == ARROW_SCHEMA_META_KEY)
        .and_then(|kv| kv.value.clone())
        .expect("ARROW:schema missing")
}

async fn write_batch(batch: RecordBatch, opts: TableParquetOptions) -> TempDir {
    let tmp = TempDir::new().unwrap();
    let out = tmp.path().join("data.parquet");
    let ctx = SessionContext::new();
    ctx.read_batch(batch)
        .unwrap()
        .write_parquet(
            out.to_str().unwrap(),
            DataFrameWriteOptions::new().with_single_file_output(true),
            Some(opts),
        )
        .await
        .unwrap();
    tmp
}

fn written_file(tmp: &TempDir) -> std::path::PathBuf {
    let mut files: Vec<_> = fs::read_dir(tmp.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .collect();
    assert_eq!(files.len(), 1, "expected one parquet file, got {files:?}");
    files.pop().unwrap()
}

#[tokio::test]
async fn write_narrows_int64_arrow_schema_to_uint8() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2i64, 19, 7]))],
    )
    .unwrap();

    let mut opts = TableParquetOptions::new();
    opts.global.allow_single_file_parallelism = false;

    let tmp = write_batch(batch, opts).await;
    let expected =
        encode_arrow_schema(&Schema::new(vec![Field::new("id", DataType::UInt8, true)]));
    assert_eq!(last_arrow_schema_kv(&written_file(&tmp)), expected);
}

#[tokio::test]
async fn write_narrows_with_parallel_writer() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1i64, 2, 3, 4]))],
    )
    .unwrap();

    let mut opts = TableParquetOptions::new();
    opts.global.allow_single_file_parallelism = true;

    let tmp = write_batch(batch, opts).await;
    let expected =
        encode_arrow_schema(&Schema::new(vec![Field::new("id", DataType::UInt8, true)]));
    assert_eq!(last_arrow_schema_kv(&written_file(&tmp)), expected);
}

#[tokio::test]
async fn write_merges_stats_across_row_groups() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
    // Per-RG domains fit in UInt8, but merged max needs UInt16.
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 300, 301]))],
    )
    .unwrap();

    let mut opts = TableParquetOptions::new();
    opts.global.allow_single_file_parallelism = false;
    opts.global.max_row_group_size = 2;

    let tmp = write_batch(batch, opts).await;
    let path = written_file(&tmp);
    let reader =
        SerializedFileReader::new(Bytes::from(fs::read(&path).unwrap())).unwrap();
    assert!(
        reader.metadata().num_row_groups() >= 2,
        "expected multiple row groups"
    );

    let expected =
        encode_arrow_schema(&Schema::new(vec![Field::new("id", DataType::UInt16, true)]));
    assert_eq!(last_arrow_schema_kv(&path), expected);
}

#[tokio::test]
async fn write_narrows_nested_struct_integer() {
    let n_field = Field::new("n", DataType::Int32, true);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Struct(Fields::from(vec![n_field.clone()])),
        true,
    )]));
    let struct_array = StructArray::from(vec![(
        Arc::new(n_field.clone()),
        Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
    )]);
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(struct_array)]).unwrap();

    let mut opts = TableParquetOptions::new();
    opts.global.allow_single_file_parallelism = false;

    let tmp = write_batch(batch, opts).await;
    let expected = encode_arrow_schema(&Schema::new(vec![Field::new(
        "s",
        DataType::Struct(Fields::from(vec![Field::new("n", DataType::UInt8, true)])),
        true,
    )]));
    assert_eq!(last_arrow_schema_kv(&written_file(&tmp)), expected);
}

#[tokio::test]
async fn skip_arrow_metadata_skips_narrowing_rewrite() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2i64, 19]))],
    )
    .unwrap();

    let mut opts = TableParquetOptions::new();
    opts.global.allow_single_file_parallelism = false;
    opts.global.skip_arrow_metadata = true;

    let tmp = write_batch(batch, opts).await;
    let reader =
        SerializedFileReader::new(Bytes::from(fs::read(written_file(&tmp)).unwrap()))
            .unwrap();
    let kvs = reader.metadata().file_metadata().key_value_metadata();
    let has_arrow_schema = kvs
        .map(|kvs| kvs.iter().any(|kv| kv.key == ARROW_SCHEMA_META_KEY))
        .unwrap_or(false);
    assert!(
        !has_arrow_schema,
        "ARROW:schema should be absent when skip_arrow_metadata is set"
    );
}
