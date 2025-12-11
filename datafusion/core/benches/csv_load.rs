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

#[macro_use]
extern crate criterion;
extern crate arrow;
extern crate datafusion;

mod data_utils;

use crate::criterion::Criterion;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::CsvReadOptions;
use datafusion::test_util::csv::TestCsvFile;
use parking_lot::Mutex;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use test_utils::AccessLogGenerator;
use tokio::runtime::Runtime;

#[expect(clippy::needless_pass_by_value)]
fn load_csv(
    ctx: Arc<Mutex<SessionContext>>,
    rt: &Runtime,
    path: &str,
    options: CsvReadOptions,
) {
    let df = rt.block_on(ctx.lock().read_csv(path, options)).unwrap();
    black_box(rt.block_on(df.collect()).unwrap());
}

fn create_context() -> Result<Arc<Mutex<SessionContext>>> {
    let ctx = SessionContext::new();
    Ok(Arc::new(Mutex::new(ctx)))
}

fn generate_test_file() -> TestCsvFile {
    let write_location = std::env::current_dir()
        .unwrap()
        .join("benches")
        .join("data");

    // Make sure the write directory exists.
    std::fs::create_dir_all(&write_location).unwrap();
    let file_path = write_location.join("logs.csv");

    let generator = AccessLogGenerator::new().with_include_nulls(true);
    let num_batches = 2;
    TestCsvFile::try_new(file_path.clone(), generator.take(num_batches as usize))
        .expect("Failed to create test file.")
}

fn criterion_benchmark(c: &mut Criterion) {
    let ctx = create_context().unwrap();
    let rt = Runtime::new().unwrap();
    let test_file = generate_test_file();

    let mut group = c.benchmark_group("load csv testing");
    group.measurement_time(Duration::from_secs(20));

    group.bench_function("default csv read options", |b| {
        b.iter(|| {
            load_csv(
                ctx.clone(),
                &rt,
                test_file.path().to_str().unwrap(),
                CsvReadOptions::default(),
            )
        })
    });

    group.bench_function("null regex override", |b| {
        b.iter(|| {
            load_csv(
                ctx.clone(),
                &rt,
                test_file.path().to_str().unwrap(),
                CsvReadOptions::default().null_regex(Some("^NULL$|^$".to_string())),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
