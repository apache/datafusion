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

//! Benchmarks consuming a Substrait `ReadRel` over a wide table, which exercises
//! the schema compatibility check and projection in the consumer.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::empty::EmptyTable;
use datafusion::prelude::SessionContext;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;

fn wide_table_context(num_columns: usize) -> SessionContext {
    let fields: Vec<Field> = (0..num_columns)
        .map(|i| Field::new(format!("c{i}"), DataType::Int64, true))
        .collect();
    let ctx = SessionContext::new();
    ctx.register_table(
        "wide",
        Arc::new(EmptyTable::new(Arc::new(Schema::new(fields)))),
    )
    .unwrap();
    ctx
}

fn consume_wide_read(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("consume_wide_read");
    for num_columns in [100, 1_000, 10_000] {
        let ctx = wide_table_context(num_columns);
        // The producer emits the full table schema as `base_schema` and a mask
        // selecting the single projected column
        let plan = rt.block_on(async {
            let df = ctx.sql("SELECT c0 FROM wide").await.unwrap();
            let logical_plan = df.into_optimized_plan().unwrap();
            to_substrait_plan(&logical_plan, &ctx.state()).unwrap()
        });
        let state = ctx.state();

        group.bench_with_input(
            BenchmarkId::from_parameter(num_columns),
            &plan,
            |b, plan| {
                b.to_async(&rt).iter(|| async {
                    black_box(from_substrait_plan(&state, plan).await.unwrap())
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, consume_wide_read);
criterion_main!(benches);
