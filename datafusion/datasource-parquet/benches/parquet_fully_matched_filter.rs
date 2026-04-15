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

//! Benchmark for skipping filter evaluation on fully matched row groups.
//!
//! This benchmark measures the performance improvement from skipping
//! RowFilter evaluation when row group statistics prove that all rows
//! in a row group satisfy the predicate.
//!
//! Dataset layout:
//! - 20 row groups, each with 50_000 rows
//! - Column `x`: i64, values in range [0, 100) for all row groups
//! - Column `payload`: Utf8, 1 KB string (makes filter column decoding cost visible)
//!
//! Predicate: `x < 200`
//! - ALL row groups are fully matched (max(x) < 200 for every row group)
//! - Without the optimization: RowFilter decodes `x` and evaluates predicate for every row
//! - With the optimization: RowFilter is skipped entirely (statistics prove all rows match)
//!
//! Uses `ParquetPushDecoder` directly to exercise the exact code path
//! that DataFusion's async opener uses.

use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use arrow::array::{Int64Array, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_datasource_parquet::{ParquetFileMetrics, build_row_filter};
use datafusion_expr::{Expr, col};
use datafusion_physical_expr::planner::logical2physical;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use parquet::DecodeResult;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use parquet::file::properties::WriterProperties;
use parquet::{arrow::ArrowWriter, file::metadata::ParquetMetaData};
use tempfile::TempDir;

const ROW_GROUP_SIZE: usize = 50_000;
const NUM_ROW_GROUPS: usize = 20;
const TOTAL_ROWS: usize = ROW_GROUP_SIZE * NUM_ROW_GROUPS;
const PAYLOAD_LEN: usize = 1024;

struct BenchmarkDataset {
    _tempdir: TempDir,
    file_path: PathBuf,
    file_bytes: Bytes,
    metadata: Arc<ParquetMetaData>,
    schema: SchemaRef,
}

static DATASET: LazyLock<BenchmarkDataset> = LazyLock::new(|| {
    create_dataset().expect("failed to prepare parquet benchmark dataset")
});

fn bench_fully_matched_filter(c: &mut Criterion) {
    let dataset = &*DATASET;
    let mut group = c.benchmark_group("parquet_fully_matched_filter");
    group.throughput(Throughput::Elements(TOTAL_ROWS as u64));

    let metrics = ExecutionPlanMetricsSet::new();
    let file_metrics =
        ParquetFileMetrics::new(0, &dataset.file_path.display().to_string(), &metrics);

    // Build the row filter (shared across benchmarks)
    let predicate_expr = logical2physical(&x_lt(200), &dataset.schema);
    let row_filter = build_row_filter(
        &predicate_expr,
        &dataset.schema,
        &dataset.metadata,
        false,
        &file_metrics,
    )
    .expect("build row filter")
    .expect("row filter should be Some");

    // All row group indices
    let all_rg_indices: Vec<usize> = (0..NUM_ROW_GROUPS).collect();

    // Scenario 1: with pushdown, WITHOUT fully_matched optimization
    // (RowFilter evaluated on every row group)
    group.bench_function("all_fully_matched/pushdown_no_skip", |b| {
        b.iter(|| {
            // Rebuild row filter each iteration (it's consumed by the builder)
            let rf = rebuild_row_filter(dataset, &file_metrics);
            let reader_metadata = ArrowReaderMetadata::try_new(
                Arc::clone(&dataset.metadata),
                Default::default(),
            )
            .unwrap();
            let decoder = ParquetPushDecoderBuilder::new_with_metadata(reader_metadata)
                .with_batch_size(8192)
                .with_row_filter(rf)
                .with_row_groups(all_rg_indices.clone())
                // No fully_matched_row_groups: filter runs on all row groups
                .build()
                .unwrap();
            let rows = run_push_decoder(decoder, &dataset.file_bytes);
            assert_eq!(rows, TOTAL_ROWS);
        });
    });

    // Scenario 2: with pushdown, WITH fully_matched optimization
    // (RowFilter skipped for all row groups)
    group.bench_function("all_fully_matched/pushdown_with_skip", |b| {
        b.iter(|| {
            let rf = rebuild_row_filter(dataset, &file_metrics);
            let reader_metadata = ArrowReaderMetadata::try_new(
                Arc::clone(&dataset.metadata),
                Default::default(),
            )
            .unwrap();
            let decoder = ParquetPushDecoderBuilder::new_with_metadata(reader_metadata)
                .with_batch_size(8192)
                .with_row_filter(rf)
                .with_row_groups(all_rg_indices.clone())
                .with_fully_matched_row_groups(all_rg_indices.clone())
                .build()
                .unwrap();
            let rows = run_push_decoder(decoder, &dataset.file_bytes);
            assert_eq!(rows, TOTAL_ROWS);
        });
    });

    // Scenario 3: no pushdown at all (no RowFilter set)
    group.bench_function("all_fully_matched/no_pushdown", |b| {
        b.iter(|| {
            let reader_metadata = ArrowReaderMetadata::try_new(
                Arc::clone(&dataset.metadata),
                Default::default(),
            )
            .unwrap();
            let decoder = ParquetPushDecoderBuilder::new_with_metadata(reader_metadata)
                .with_batch_size(8192)
                .with_row_groups(all_rg_indices.clone())
                .build()
                .unwrap();
            let rows = run_push_decoder(decoder, &dataset.file_bytes);
            assert_eq!(rows, TOTAL_ROWS);
        });
    });

    drop(row_filter);
    group.finish();
}

fn rebuild_row_filter(
    dataset: &BenchmarkDataset,
    file_metrics: &ParquetFileMetrics,
) -> parquet::arrow::arrow_reader::RowFilter {
    let predicate_expr = logical2physical(&x_lt(200), &dataset.schema);
    build_row_filter(
        &predicate_expr,
        &dataset.schema,
        &dataset.metadata,
        false,
        file_metrics,
    )
    .expect("build row filter")
    .expect("row filter should be Some")
}

fn run_push_decoder(
    mut decoder: parquet::arrow::push_decoder::ParquetPushDecoder,
    file_bytes: &Bytes,
) -> usize {
    let mut total_rows = 0;
    loop {
        match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData(ranges) => {
                let data: Vec<Bytes> = ranges
                    .iter()
                    .map(|r| {
                        let start = r.start as usize;
                        let end = r.end as usize;
                        file_bytes.slice(start..end)
                    })
                    .collect();
                decoder.push_ranges(ranges, data).unwrap();
            }
            DecodeResult::Data(batch) => {
                total_rows += batch.num_rows();
            }
            DecodeResult::Finished => break,
        }
    }
    total_rows
}

/// `x < value`
fn x_lt(value: i64) -> Expr {
    col("x").lt(Expr::Literal(ScalarValue::Int64(Some(value)), None))
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn create_dataset() -> datafusion_common::Result<BenchmarkDataset> {
    let tempdir = TempDir::new()?;
    let file_path = tempdir.path().join("fully_matched_filter.parquet");

    let schema = schema();
    let writer_props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROW_GROUP_SIZE))
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
        .build();

    let file = std::fs::File::create(&file_path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_props))?;

    for _rg in 0..NUM_ROW_GROUPS {
        let x_values: Vec<i64> = (0..ROW_GROUP_SIZE as i64).map(|i| i % 100).collect();
        let x_array = Int64Array::from(x_values);

        let mut payload_builder =
            StringBuilder::with_capacity(ROW_GROUP_SIZE, ROW_GROUP_SIZE * PAYLOAD_LEN);
        for _ in 0..ROW_GROUP_SIZE {
            let s: String = (0..PAYLOAD_LEN)
                .map(|i| (b'A' + (i % 26) as u8) as char)
                .collect();
            payload_builder.append_value(&s);
        }
        let payload_array = payload_builder.finish();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(x_array), Arc::new(payload_array)],
        )?;
        writer.write(&batch)?;
    }

    writer.close()?;

    // Read the file into memory for push decoder benchmarks
    let file_bytes = Bytes::from(std::fs::read(&file_path)?);

    // Parse metadata
    let file = std::fs::File::open(&file_path)?;
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata().clone();
    let arrow_schema = Arc::clone(builder.schema());

    Ok(BenchmarkDataset {
        _tempdir: tempdir,
        file_path,
        file_bytes,
        metadata,
        schema: arrow_schema,
    })
}

criterion_group!(benches, bench_fully_matched_filter);
criterion_main!(benches);
