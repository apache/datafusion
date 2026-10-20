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

//! Scans an RLE dictionary-encoded Parquet byte-array column into `Utf8View`
//! and `Dictionary<Int32, Utf8>` arrays through `DataSourceExec`.
//!
//! The annotated UTF-8 source is a control for direct dictionary decoding. The
//! unannotated binary source matches ClickBench, where `binary_as_string`
//! exposes the column as a string while the physical file remains binary.

use std::fs::File;
use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{RecordBatch, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::instant::Instant;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding as ParquetEncoding};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::ColumnPath;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const ROWS: usize = 1_000_000;
const CARDINALITIES: &[usize] = &[1, 10, 100, 1_000, 10_000, 100_000, 500_000];
const STRING_LENGTH: usize = 16;
const BATCH_SIZE: usize = 8192;
const ROW_GROUP_SIZE: usize = 500_000;
const SEED: u64 = 42;

#[derive(Clone, Copy)]
enum OutputEncoding {
    Utf8View,
    Dictionary,
}

struct Scan {
    context: SessionContext,
}

fn expected_type(encoding: OutputEncoding) -> DataType {
    match encoding {
        OutputEncoding::Utf8View => DataType::Utf8View,
        OutputEncoding::Dictionary => {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        }
    }
}

fn source_batches(ndv: usize) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
    let values = (0..ndv)
        .map(|value| format!("{value:0STRING_LENGTH$x}"))
        .collect::<Vec<_>>();
    let mut keys = StdRng::seed_from_u64(SEED ^ 0x4B45_5953_4B45_5953);

    (0..ROWS)
        .step_by(BATCH_SIZE)
        .map(|start| {
            let end = (start + BATCH_SIZE).min(ROWS);
            let array = StringArray::from_iter_values(
                (start..end).map(|_| values[keys.random_range(0..ndv)].as_str()),
            );
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).unwrap()
        })
        .collect()
}

fn as_unannotated_binary(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Binary, false)]));
    batches
        .iter()
        .map(|batch| {
            let array = cast(batch.column(0), &DataType::Binary).unwrap();
            RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap()
        })
        .collect()
}

fn write_dictionary_file(path: &Path, batches: &[RecordBatch], ndv: usize) {
    let key_path = ColumnPath::from("k");
    let dictionary_page_size = ndv.saturating_mul(STRING_LENGTH + 8).max(1024 * 1024);
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_column_dictionary_enabled(key_path.clone(), true)
        .set_column_dictionary_page_size_limit(key_path, dictionary_page_size)
        .set_max_row_group_row_count(Some(ROW_GROUP_SIZE))
        .build();

    let file = File::create(path).unwrap();
    let mut writer =
        ArrowWriter::try_new(file, batches[0].schema(), Some(properties)).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.close().unwrap();

    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
    for (row_group_index, row_group) in reader.metadata().row_groups().iter().enumerate()
    {
        let encodings = row_group.column(0).page_encoding_stats_mask().unwrap();
        assert!(
            encodings.is_only(ParquetEncoding::RLE_DICTIONARY),
            "row group {row_group_index} has data-page encodings {encodings:?}"
        );
    }
}

fn make_session(encoding: OutputEncoding) -> SessionContext {
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(BATCH_SIZE);
    let parquet = &mut config.options_mut().execution.parquet;
    parquet.schema_force_view_types = matches!(encoding, OutputEncoding::Utf8View);
    parquet.enable_rle_to_dictionary = matches!(encoding, OutputEncoding::Dictionary);
    SessionContext::new_with_config(config)
}

async fn prepare_scan(path: &Path, encoding: OutputEncoding) -> Scan {
    let context = make_session(encoding);
    let schema = Schema::new(vec![Field::new("k", expected_type(encoding), false)]);
    context
        .register_parquet(
            "strings",
            path.to_str().unwrap(),
            ParquetReadOptions::default()
                .schema(&schema)
                .skip_metadata(true),
        )
        .await
        .unwrap();

    let scan = Scan { context };
    let plan = build_plan(&scan).await;
    assert_eq!(plan.schema().field(0).data_type(), &expected_type(encoding));
    let displayed_plan = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(displayed_plan.contains("DataSourceExec"));
    assert!(
        !displayed_plan.contains("CAST("),
        "schema adaptation must remain inside DataSourceExec:\n{displayed_plan}"
    );
    scan
}

async fn build_plan(scan: &Scan) -> Arc<dyn ExecutionPlan> {
    scan.context
        .sql("SELECT k FROM strings")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

async fn scan_once(scan: &Scan, plan: Arc<dyn ExecutionPlan>) {
    let mut stream = execute_stream(plan, scan.context.task_ctx()).unwrap();
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        rows += batch.num_rows();
        black_box(batch.column(0));
    }
    assert_eq!(rows, ROWS);
}

fn criterion_benchmark(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    for &ndv in CARDINALITIES {
        let utf8_batches = source_batches(ndv);
        let binary_batches = as_unannotated_binary(&utf8_batches);
        let directory = tempfile::tempdir().unwrap();

        for (source_name, benchmark_name, batches) in [
            (
                "annotated_utf8",
                "parquet_rle_dictionary_scan",
                utf8_batches.as_slice(),
            ),
            (
                "unannotated_binary",
                "parquet_binary_rle_dictionary_scan",
                binary_batches.as_slice(),
            ),
        ] {
            let path = directory.path().join(format!("{source_name}.parquet"));
            write_dictionary_file(&path, batches, ndv);

            let view = runtime.block_on(prepare_scan(&path, OutputEncoding::Utf8View));
            let dictionary =
                runtime.block_on(prepare_scan(&path, OutputEncoding::Dictionary));
            let mut group = criterion
                .benchmark_group(format!("{benchmark_name}/rows={ROWS}/ndv={ndv}"));
            group.throughput(Throughput::Elements(ROWS as u64));

            group.bench_function("utf8_view", |bencher| {
                bencher.to_async(&runtime).iter_custom(|iterations| {
                    let scan = &view;
                    async move {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            // Parquet plans contain one-shot stream state. Re-plan outside
                            // the measured interval so every timed drain has a fresh plan.
                            let plan = build_plan(scan).await;
                            let start = Instant::now();
                            scan_once(scan, plan).await;
                            elapsed += start.elapsed();
                        }
                        elapsed
                    }
                });
            });
            group.bench_function("dictionary", |bencher| {
                bencher.to_async(&runtime).iter_custom(|iterations| {
                    let scan = &dictionary;
                    async move {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            let plan = build_plan(scan).await;
                            let start = Instant::now();
                            scan_once(scan, plan).await;
                            elapsed += start.elapsed();
                        }
                        elapsed
                    }
                });
            });
            group.finish();
        }
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(20);
    targets = criterion_benchmark
}
criterion_main!(benches);
