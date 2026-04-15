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

//! Tests for parquet content-defined chunking (CDC).
//!
//! These tests verify that CDC options are correctly wired through to the
//! parquet writer by inspecting file metadata (compressed sizes, page
//! boundaries) on the written files.

use arrow::array::{AsArray, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_common::config::{CdcOptions, TableParquetOptions};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;
use tempfile::NamedTempFile;

/// Create a RecordBatch with enough data to exercise CDC chunking.
fn make_test_batch(num_rows: usize) -> RecordBatch {
    let ids: Vec<i32> = (0..num_rows as i32).collect();
    // ~100 bytes per row to generate enough data for CDC page splits
    let payloads: Vec<String> = (0..num_rows)
        .map(|i| format!("row-{i:06}-payload-{}", "x".repeat(80)))
        .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(payloads)),
        ],
    )
    .unwrap()
}

/// Build WriterProperties from TableParquetOptions, exercising the same
/// code path that DataFusion's parquet sink uses.
fn writer_props(
    opts: &mut TableParquetOptions,
    schema: &Arc<Schema>,
) -> WriterProperties {
    opts.arrow_schema(schema);
    parquet::file::properties::WriterPropertiesBuilder::try_from(
        opts as &TableParquetOptions,
    )
    .unwrap()
    .build()
}

/// Write a batch to a temp parquet file and return the file handle.
fn write_parquet_file(batch: &RecordBatch, props: WriterProperties) -> NamedTempFile {
    let tmp = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let mut writer =
        ArrowWriter::try_new(tmp.reopen().unwrap(), batch.schema(), Some(props)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    tmp
}

/// Read parquet metadata from a file.
fn read_metadata(file: &NamedTempFile) -> parquet::file::metadata::ParquetMetaData {
    let f = File::open(file.path()).unwrap();
    let reader_meta = ArrowReaderMetadata::load(&f, Default::default()).unwrap();
    reader_meta.metadata().as_ref().clone()
}

/// Write parquet with CDC enabled, read it back via DataFusion, and verify
/// the data round-trips correctly.
#[tokio::test]
async fn cdc_data_round_trip() {
    let batch = make_test_batch(5000);

    let mut opts = TableParquetOptions::default();
    opts.global.use_content_defined_chunking = Some(CdcOptions::default());
    let props = writer_props(&mut opts, &batch.schema());

    let tmp = write_parquet_file(&batch, props);

    // Read back via DataFusion and verify row count
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "data",
        tmp.path().to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();

    let result = ctx
        .sql("SELECT COUNT(*), MIN(id), MAX(id) FROM data")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let row = &result[0];
    let count = row.column(0).as_primitive::<Int64Type>().value(0);
    let min_id = row.column(1).as_primitive::<Int32Type>().value(0);
    let max_id = row.column(2).as_primitive::<Int32Type>().value(0);

    assert_eq!(count, 5000);
    assert_eq!(min_id, 0);
    assert_eq!(max_id, 4999);
}

/// Verify that CDC options are reflected in the parquet file metadata.
/// With small chunk sizes, CDC should produce different page boundaries
/// compared to default (no CDC) writing.
#[tokio::test]
async fn cdc_affects_page_boundaries() {
    let batch = make_test_batch(5000);

    // Write WITHOUT CDC
    let mut no_cdc_opts = TableParquetOptions::default();
    let no_cdc_file =
        write_parquet_file(&batch, writer_props(&mut no_cdc_opts, &batch.schema()));
    let no_cdc_meta = read_metadata(&no_cdc_file);

    // Write WITH CDC using small chunk sizes to maximize effect
    let mut cdc_opts = TableParquetOptions::default();
    cdc_opts.global.use_content_defined_chunking = Some(CdcOptions {
        min_chunk_size: 512,
        max_chunk_size: 2048,
        norm_level: 0,
    });
    let cdc_file =
        write_parquet_file(&batch, writer_props(&mut cdc_opts, &batch.schema()));
    let cdc_meta = read_metadata(&cdc_file);

    // Both files should have the same number of rows
    assert_eq!(
        no_cdc_meta.file_metadata().num_rows(),
        cdc_meta.file_metadata().num_rows(),
    );

    // Compare the uncompressed sizes of columns across all row groups.
    // CDC with small chunk sizes should produce different page boundaries.
    let no_cdc_sizes: Vec<i64> = no_cdc_meta
        .row_groups()
        .iter()
        .flat_map(|rg| rg.columns().iter().map(|c| c.uncompressed_size()))
        .collect();

    let cdc_sizes: Vec<i64> = cdc_meta
        .row_groups()
        .iter()
        .flat_map(|rg| rg.columns().iter().map(|c| c.uncompressed_size()))
        .collect();

    assert_ne!(
        no_cdc_sizes, cdc_sizes,
        "CDC with small chunk sizes should produce different page layouts \
         than default writing. no_cdc={no_cdc_sizes:?}, cdc={cdc_sizes:?}"
    );
}
