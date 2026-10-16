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

//! Compares scanning an RLE dictionary-encoded Parquet string column into
//! `Utf8View` and `Dictionary<Int32, Utf8>` arrays.

use std::fs::File;
use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanVisitor, accept, execute_stream,
};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::instant::Instant;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use rand::{Rng, SeedableRng, rngs::StdRng};

const ROW_COUNTS: &[usize] = &[1_000_000, 5_000_000];
const CARDINALITIES: &[usize] =
    &[10, 100, 500, 1_000, 2_000, 2_500, 3_000, 5_000, 20_000];
const AGGREGATE_CARDINALITIES: &[usize] =
    &[10, 100, 500, 1_000, 2_000, 2_500, 3_000, 5_000];
const FILTER_AGGREGATE_CARDINALITIES: &[usize] = &[500, 2_500];
const STRING_LENGTH: usize = 24;
const BATCH_SIZE: usize = 8192;
const SEED: u64 = 0xC0FFEE;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadAs {
    Utf8View,
    Dictionary,
}

impl ReadAs {
    fn expected_type(self) -> DataType {
        match self {
            Self::Utf8View => DataType::Utf8View,
            Self::Dictionary => {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            }
        }
    }
}

struct Scan {
    context: SessionContext,
}

struct ParquetScanFinder(bool);

impl ExecutionPlanVisitor for ParquetScanFinder {
    type Error = std::convert::Infallible;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        if let Some(scan) = plan.downcast_ref::<DataSourceExec>()
            && scan.downcast_to_file_source::<ParquetSource>().is_some()
        {
            self.0 = true;
        }
        Ok(!self.0)
    }
}

fn make_string_array(rows: usize, cardinality: usize) -> StringArray {
    let values: Vec<String> = (0..cardinality)
        .map(|value| format!("{value:0STRING_LENGTH$}"))
        .collect();
    let mut rng = StdRng::seed_from_u64(SEED ^ rows as u64 ^ cardinality as u64);
    StringArray::from_iter_values(
        (0..rows).map(|_| values[rng.random_range(0..cardinality)].as_str()),
    )
}

fn write_parquet(path: &Path, values: StringArray) {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values) as ArrayRef])
            .unwrap();
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties))
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
    assert!(
        reader.metadata().row_groups().iter().all(|row_group| {
            row_group
                .column(0)
                .encodings()
                .into_iter()
                .any(|encoding| encoding == Encoding::RLE_DICTIONARY)
        }),
        "benchmark input is not RLE_DICTIONARY encoded"
    );
}

async fn prepare_scan(path: &Path, read_as: ReadAs) -> Scan {
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(BATCH_SIZE);
    config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = true;
    config
        .options_mut()
        .execution
        .parquet
        .enable_rle_to_dictionary = read_as == ReadAs::Dictionary;

    let context = SessionContext::new_with_config(config);
    context
        .register_parquet(
            "strings",
            path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
    let scan = Scan { context };
    let plan = build_plan(&scan, "SELECT value FROM strings").await;
    assert_eq!(plan.schema().field(0).data_type(), &read_as.expected_type());

    let mut finder = ParquetScanFinder(false);
    accept(plan.as_ref(), &mut finder).unwrap();
    assert!(finder.0, "physical plan does not contain a Parquet scan");

    scan
}

async fn build_plan(scan: &Scan, query: &str) -> Arc<dyn ExecutionPlan> {
    scan.context
        .sql(query)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

async fn execute_once(scan: &Scan, plan: Arc<dyn ExecutionPlan>, expected_rows: usize) {
    let mut stream = execute_stream(plan, scan.context.task_ctx()).unwrap();
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        rows += batch.num_rows();
        black_box(batch);
    }
    assert_eq!(rows, expected_rows);
}

fn criterion_benchmark(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    for &rows in ROW_COUNTS {
        for &cardinality in CARDINALITIES {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("strings.parquet");
            write_parquet(&path, make_string_array(rows, cardinality));

            let view = runtime.block_on(prepare_scan(&path, ReadAs::Utf8View));
            let dictionary = runtime.block_on(prepare_scan(&path, ReadAs::Dictionary));

            let mut group = criterion.benchmark_group(format!(
                "parquet_rle_dictionary_scan/rows={rows}/cardinality={cardinality}"
            ));
            group.throughput(Throughput::Elements(rows as u64));
            group.bench_function("utf8_view", |bencher| {
                bencher.to_async(&runtime).iter_custom(|iterations| {
                    let scan = &view;
                    async move {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            let plan =
                                build_plan(scan, "SELECT value FROM strings").await;
                            let start = Instant::now();
                            execute_once(scan, plan, rows).await;
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
                            let plan =
                                build_plan(scan, "SELECT value FROM strings").await;
                            let start = Instant::now();
                            execute_once(scan, plan, rows).await;
                            elapsed += start.elapsed();
                        }
                        elapsed
                    }
                });
            });
            group.finish();

            if AGGREGATE_CARDINALITIES.contains(&cardinality) {
                let query = "SELECT value, COUNT(*) FROM strings GROUP BY value";
                let mut group = criterion.benchmark_group(format!(
                    "parquet_rle_dictionary_aggregate/rows={rows}/cardinality={cardinality}"
                ));
                group.throughput(Throughput::Elements(rows as u64));
                group.bench_function("utf8_view", |bencher| {
                    bencher.to_async(&runtime).iter_custom(|iterations| {
                        let scan = &view;
                        async move {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iterations {
                                let plan = build_plan(scan, query).await;
                                let start = Instant::now();
                                execute_once(scan, plan, cardinality).await;
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
                                let plan = build_plan(scan, query).await;
                                let start = Instant::now();
                                execute_once(scan, plan, cardinality).await;
                                elapsed += start.elapsed();
                            }
                            elapsed
                        }
                    });
                });
                group.finish();
            }

            if FILTER_AGGREGATE_CARDINALITIES.contains(&cardinality) {
                let cutoff = format!("{:0STRING_LENGTH$}", cardinality / 2);
                let query = format!(
                    "SELECT value, COUNT(*) FROM strings WHERE value < '{cutoff}' GROUP BY value"
                );
                let expected_rows = cardinality / 2;
                let mut group = criterion.benchmark_group(format!(
                    "parquet_rle_dictionary_filter_aggregate/rows={rows}/cardinality={cardinality}"
                ));
                group.throughput(Throughput::Elements(rows as u64));
                group.bench_function("utf8_view", |bencher| {
                    bencher.to_async(&runtime).iter_custom(|iterations| {
                        let scan = &view;
                        let query = &query;
                        async move {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iterations {
                                let plan = build_plan(scan, query).await;
                                let start = Instant::now();
                                execute_once(scan, plan, expected_rows).await;
                                elapsed += start.elapsed();
                            }
                            elapsed
                        }
                    });
                });
                group.bench_function("dictionary", |bencher| {
                    bencher.to_async(&runtime).iter_custom(|iterations| {
                        let scan = &dictionary;
                        let query = &query;
                        async move {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iterations {
                                let plan = build_plan(scan, query).await;
                                let start = Instant::now();
                                execute_once(scan, plan, expected_rows).await;
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
