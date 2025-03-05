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

use arrow::array::{builder::Int32Builder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_physical_expr::expressions::{Column, IsNotNullExpr, IsNullExpr};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    // create input data
    let mut c1 = Int32Builder::new();
    let mut c2 = Int32Builder::new();
    let mut c3 = Int32Builder::new();
    for i in 0..1000 {
        // c1 is always null
        c1.append_null();
        // c2 is never null
        c2.append_value(i);
        // c3 is a mix of values and nulls
        if i % 7 == 0 {
            c3.append_null();
        } else {
            c3.append_value(i);
        }
    }
    let c1 = Arc::new(c1.finish());
    let c2 = Arc::new(c2.finish());
    let c3 = Arc::new(c3.finish());
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, false),
        Field::new("c3", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![c1, c2, c3]).unwrap();

    c.bench_function("is_null: column is all nulls", |b| {
        let expr = is_null("c1", 0);
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });

    c.bench_function("is_null: column is never null", |b| {
        let expr = is_null("c2", 1);
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });

    c.bench_function("is_null: column is mix of values and nulls", |b| {
        let expr = is_null("c3", 2);
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });

    c.bench_function("is_not_null: column is all nulls", |b| {
        let expr = is_not_null("c1", 0);
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });

    c.bench_function("is_not_null: column is never null", |b| {
        let expr = is_not_null("c2", 1);
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });

    c.bench_function("is_not_null: column is mix of values and nulls", |b| {
        let expr = is_not_null("c3", 2);
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });
}

fn is_null(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(IsNullExpr::new(Arc::new(Column::new(name, index))))
}

fn is_not_null(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(IsNotNullExpr::new(Arc::new(Column::new(name, index))))
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
