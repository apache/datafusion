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
use criterion::Criterion;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable};
use datafusion::datasource::object_store::local::LocalFileSystem;

use parking_lot::Mutex;
use std::sync::Arc;

extern crate arrow;
extern crate datafusion;

use arrow::datatypes::{DataType, Field, Schema};

use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext;

use tokio::runtime::Runtime;

fn query(ctx: Arc<Mutex<ExecutionContext>>, sql: &str) {
    let rt = Runtime::new().unwrap();

    // execute the query
    let df = rt.block_on(ctx.lock().sql(sql)).unwrap();
    rt.block_on(df.collect()).unwrap();
}

fn create_context() -> Arc<Mutex<ExecutionContext>> {
    // define schema for data source (csv file)
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::UInt32, false),
        Field::new("c3", DataType::Int8, false),
        Field::new("c4", DataType::Int16, false),
        Field::new("c5", DataType::Int32, false),
        Field::new("c6", DataType::Int64, false),
        Field::new("c7", DataType::UInt8, false),
        Field::new("c8", DataType::UInt16, false),
        Field::new("c9", DataType::UInt32, false),
        Field::new("c10", DataType::UInt64, false),
        Field::new("c11", DataType::Float32, false),
        Field::new("c12", DataType::Float64, false),
        Field::new("c13", DataType::Utf8, false),
    ]));

    let testdata = datafusion::test_util::arrow_test_data();

    // create CSV data source
    let listing_options = ListingOptions::new(Arc::new(CsvFormat::default()));
    let csv = ListingTable::new(
        Arc::new(LocalFileSystem {}),
        format!("{}/csv/aggregate_test_100.csv", testdata),
        schema,
        listing_options,
    );

    let rt = Runtime::new().unwrap();

    let ctx_holder: Arc<Mutex<Vec<Arc<Mutex<ExecutionContext>>>>> =
        Arc::new(Mutex::new(vec![]));

    let partitions = 16;

    rt.block_on(async {
        // create local execution context
        let mut ctx = ExecutionContext::new();
        ctx.state.lock().config.target_partitions = 1;
        let runtime = ctx.state.lock().runtime_env.clone();

        let mem_table = MemTable::load(Arc::new(csv), Some(partitions), runtime)
            .await
            .unwrap();
        ctx.register_table("aggregate_test_100", Arc::new(mem_table))
            .unwrap();
        ctx_holder.lock().push(Arc::new(Mutex::new(ctx)))
    });

    let ctx = ctx_holder.lock().get(0).unwrap().clone();
    ctx
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("sort_and_limit_by_int", |b| {
        let ctx = create_context();
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT c1, c13, c6, c10 \
                 FROM aggregate_test_100 \
                 ORDER BY c6
                 LIMIT 10",
            )
        })
    });

    c.bench_function("sort_and_limit_by_float", |b| {
        let ctx = create_context();
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT c1, c13, c12 \
                 FROM aggregate_test_100 \
                 ORDER BY c13
                 LIMIT 10",
            )
        })
    });

    c.bench_function("sort_and_limit_lex_by_int", |b| {
        let ctx = create_context();
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT c1, c13, c6, c10 \
                 FROM aggregate_test_100 \
                 ORDER BY c6 DESC, c10 DESC
                 LIMIT 10",
            )
        })
    });

    c.bench_function("sort_and_limit_lex_by_string", |b| {
        let ctx = create_context();
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT c1, c13, c6, c10 \
                 FROM aggregate_test_100 \
                 ORDER BY c1, c13
                 LIMIT 10",
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
