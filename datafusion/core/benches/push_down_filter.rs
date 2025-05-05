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

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use bytes::{BufMut, BytesMut};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::config::ConfigOptions;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_optimizer::push_down_filter::PushdownFilter;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::ExecutionPlan;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use std::sync::Arc;

async fn create_plan() -> Arc<dyn ExecutionPlan> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::UInt16, true),
        Field::new("salary", DataType::Float64, true),
    ]));
    let batch = RecordBatch::new_empty(schema);

    let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let data = {
        let out = BytesMut::new();
        let mut writer =
            ArrowWriter::try_new(out.writer(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        writer.into_inner().unwrap().into_inner().freeze()
    };
    store
        .put(&Path::from("test.parquet"), data.into())
        .await
        .unwrap();
    ctx.register_object_store(
        ObjectStoreUrl::parse("memory://").unwrap().as_ref(),
        store,
    );

    ctx.register_parquet("t", "memory://", ParquetReadOptions::default())
        .await
        .unwrap();

    let df = ctx
        .sql(
            r"
        WITH brackets AS (
            SELECT age % 10 AS age_bracket
            FROM t
            GROUP BY age % 10
            HAVING COUNT(*) > 10
        )
        SELECT id, name, age, salary
        FROM t
        JOIN brackets ON t.age % 10 = brackets.age_bracket
        WHERE age > 20 AND data.salary > 1000
        ORDER BY data.salary DESC
        LIMIT 100
    ",
        )
        .await
        .unwrap();

    df.create_physical_plan().await.unwrap()
}

#[derive(Clone)]
struct BenchmarkPlan {
    plan: Arc<dyn ExecutionPlan>,
    config: ConfigOptions,
}

impl std::fmt::Display for BenchmarkPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BenchmarkPlan")
    }
}

fn bench_push_down_filter(c: &mut Criterion) {
    // Create a relatively complex plan
    let plan = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(create_plan());
    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    let plan = BenchmarkPlan { plan, config };

    c.bench_with_input(
        BenchmarkId::new("push_down_filter", plan.clone()),
        &plan,
        |b, plan| {
            b.iter(|| {
                let optimizer = PushdownFilter::new();
                optimizer
                    .optimize(Arc::clone(&plan.plan), &plan.config)
                    .unwrap();
            });
        },
    );
}

criterion_group!(benches, bench_push_down_filter);
criterion_main!(benches);
