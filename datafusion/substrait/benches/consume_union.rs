// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Benchmarks for consuming Substrait plans with wide `UNION ALL`s, as produced by
//! queries generated over many tables.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use tokio::runtime::Builder;

const COLUMNS: usize = 20;

/// Registers `inputs` distinct tables and returns a `UNION ALL` over all of them. Distinct
/// tables matter: inputs that share a qualifier would collapse when schemas are merged.
fn register_wide_union(ctx: &SessionContext, inputs: usize) -> String {
    let fields = (0..COLUMNS)
        .map(|i| Field::new(format!("c{i}"), DataType::Int32, true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    (0..inputs)
        .map(|i| {
            let table = format!("t{i}");
            let provider = MemTable::try_new(Arc::clone(&schema), vec![vec![]]).unwrap();
            ctx.register_table(table.as_str(), Arc::new(provider))
                .unwrap();
            format!("SELECT * FROM {table}")
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

fn consume_wide_union(c: &mut Criterion) {
    let rt = Builder::new_current_thread().build().unwrap();
    for inputs in [50, 200] {
        let ctx = SessionContext::new();
        let sql = register_wide_union(&ctx, inputs);
        let plan = rt
            .block_on(async { ctx.sql(&sql).await?.into_optimized_plan() })
            .unwrap();
        let state = ctx.state();
        let proto = to_substrait_plan(&plan, &state).unwrap();

        c.bench_function(
            &format!("consume_and_optimize_wide_union_all_{inputs}"),
            |b| {
                b.iter(|| {
                    let consumed =
                        rt.block_on(from_substrait_plan(&state, &proto)).unwrap();
                    black_box(state.optimize(&consumed).unwrap())
                })
            },
        );
    }
}

criterion_group!(benches, consume_wide_union);
criterion_main!(benches);
