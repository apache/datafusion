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

//! Benchmarks end-to-end Parquet scan cost for a cache-favourable workload.
//!
//! The 128 files share one physical schema and use the same predicate and target
//! partition count, so pruning setup is reusable in a cache-enabled comparison
//! branch. The predicate matches every file, so each scan must adapt the
//! predicate and build (or reuse) pruning setup for all files.
//!
//! This is a focused baseline for comparing cache-disabled and cache-enabled
//! branches. It is not a general DataFusion benchmark and must not be
//! interpreted as a ClickBench performance result.

use std::{fs::File, sync::Arc};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::{SessionConfig, SessionContext};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

const FILES: usize = 128;
const ROWS_PER_FILE: usize = 128;

fn write_files() -> TempDir {
    let directory = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    for file_index in 0..FILES {
        let start = i64::try_from(file_index * ROWS_PER_FILE).unwrap();
        let values = Int64Array::from_iter_values(
            start..start + i64::try_from(ROWS_PER_FILE).unwrap(),
        );
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)]).unwrap();
        let file =
            File::create(directory.path().join(format!("{file_index}.parquet"))).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    directory
}

fn criterion_benchmark(criterion: &mut Criterion) {
    let directory = write_files();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let context = runtime.block_on(async {
        let context = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(1),
        );
        context
            .register_parquet("t", directory.path().to_str().unwrap(), Default::default())
            .await
            .unwrap();
        context
    });

    runtime.block_on(async {
        let batches = context
            .sql("SELECT id FROM t WHERE id >= 0")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            FILES * ROWS_PER_FILE
        );
    });

    criterion.bench_function(
        "parquet_pruning_setup_cache/same_schema_files",
        |bencher| {
            bencher.to_async(&runtime).iter(|| async {
                context
                    .sql("SELECT id FROM t WHERE id >= 0")
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap()
            });
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
