// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_expr::col;
use datafusion_functions::expr_fn::btrim;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn create_context(field_count: u32) -> datafusion_common::Result<Arc<SessionContext>> {
    let mut fields = vec![];
    for i in 0..field_count {
        fields.push(Field::new(format!("str{i}"), DataType::Utf8, true))
    }

    let schema = Arc::new(Schema::new(fields));
    let ctx = SessionContext::new();
    let table = MemTable::try_new(Arc::clone(&schema), vec![vec![]])?;

    ctx.register_table("t", Arc::new(table))?;

    Ok(Arc::new(ctx))
}

#[expect(clippy::needless_pass_by_value)]
fn run(column_count: u32, ctx: Arc<SessionContext>, rt: &Runtime) {
    black_box(rt.block_on(async {
        let mut data_frame = ctx.table("t").await.unwrap();

        for i in 0..column_count {
            let field_name = &format!("str{i}");
            let new_field_name = &format!("newstr{i}");

            data_frame = data_frame
                .with_column_renamed(field_name, new_field_name)
                .unwrap()
                .with_column(new_field_name, btrim(vec![col(new_field_name)]))
                .unwrap();
        }

        Some(true)
    }))
    .unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    for column_count in [10, 100, 200, 500] {
        let ctx = create_context(column_count).unwrap();

        c.bench_function(&format!("with_column_{column_count}"), |b| {
            b.iter(|| run(column_count, ctx.clone(), &rt))
        });
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);
