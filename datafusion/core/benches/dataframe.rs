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

extern crate arrow;
#[macro_use]
extern crate criterion;
extern crate datafusion;

use arrow_schema::{DataType, Field, Schema};
use criterion::Criterion;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_expr::col;
use datafusion_functions::expr_fn::btrim;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn create_context(field_count: u32) -> datafusion_common::Result<Arc<SessionContext>> {
    let mut fields = vec![];
    for i in 0..field_count {
        fields.push(Field::new(format!("str{}", i), DataType::Utf8, true))
    }

    let schema = Arc::new(Schema::new(fields));
    let ctx = SessionContext::new();
    let table = MemTable::try_new(Arc::clone(&schema), vec![vec![]])?;

    ctx.register_table("t", Arc::new(table))?;

    Ok(Arc::new(ctx))
}

fn run(column_count: u32, ctx: Arc<SessionContext>) {
    let rt = Runtime::new().unwrap();

    criterion::black_box(rt.block_on(async {
        let mut data_frame = ctx.table("t").await.unwrap();

        for i in 0..column_count {
            let field_name = &format!("str{}", i);
            let new_field_name = &format!("newstr{}", i);

            data_frame = data_frame
                .with_column_renamed(field_name, new_field_name)
                .unwrap();
            data_frame = data_frame
                .with_column(new_field_name, btrim(vec![col(new_field_name)]))
                .unwrap();
        }

        Some(true)
    }))
    .unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    // 500 takes far too long right now
    for column_count in [10, 100, 200 /* 500 */] {
        let ctx = create_context(column_count).unwrap();

        c.bench_function(&format!("with_column_{column_count}"), |b| {
            b.iter(|| run(column_count, ctx.clone()))
        });
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);
