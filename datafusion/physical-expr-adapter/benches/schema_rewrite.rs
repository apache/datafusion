// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::hint::black_box;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_adapter::{DefaultPhysicalExprAdapter, PhysicalExprAdapter};

fn bench_schema_rewrite(c: &mut Criterion) {
    for field_count in [16, 128] {
        let physical_fields: Vec<_> = (0..field_count)
            .map(|index| Field::new(format!("field_{index}"), DataType::Utf8, true))
            .collect();
        let mut logical_fields = physical_fields.clone();
        logical_fields.push(Field::new("optional_new_field", DataType::Utf8, true));

        let adapter = DefaultPhysicalExprAdapter::new(
            Arc::new(Schema::new(logical_fields)),
            Arc::new(Schema::new(physical_fields)),
        );
        let missing = Arc::new(Column::new("optional_new_field", field_count));
        let present = Arc::new(Column::new("field_0", 0));

        c.bench_function(&format!("schema_rewrite/missing/{field_count}"), |b| {
            b.iter(|| black_box(adapter.rewrite(black_box(missing.clone())).unwrap()))
        });
        c.bench_function(&format!("schema_rewrite/present/{field_count}"), |b| {
            b.iter(|| black_box(adapter.rewrite(black_box(present.clone())).unwrap()))
        });
    }
}

criterion_group!(benches, bench_schema_rewrite);
criterion_main!(benches);
