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

use arrow::array::builder::{Int32Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::ScalarValue;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, CaseExpr, Column, Literal};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::sync::Arc;

fn make_col(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(name, index))
}

fn make_lit_i32(n: i32) -> Arc<dyn PhysicalExpr> {
    Arc::new(Literal::new(ScalarValue::Int32(Some(n))))
}

fn criterion_benchmark(c: &mut Criterion) {
    // create input data
    let mut c1 = Int32Builder::new();
    let mut c2 = StringBuilder::new();
    let mut c3 = StringBuilder::new();
    for i in 0..1000 {
        c1.append_value(i);
        if i % 7 == 0 {
            c2.append_null();
        } else {
            c2.append_value(format!("string {i}"));
        }
        if i % 9 == 0 {
            c3.append_null();
        } else {
            c3.append_value(format!("other string {i}"));
        }
    }
    let c1 = Arc::new(c1.finish());
    let c2 = Arc::new(c2.finish());
    let c3 = Arc::new(c3.finish());
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Utf8, true),
        Field::new("c3", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![c1, c2, c3]).unwrap();

    // use same predicate for all benchmarks
    let predicate = Arc::new(BinaryExpr::new(
        make_col("c1", 0),
        Operator::LtEq,
        make_lit_i32(500),
    ));

    // CASE WHEN c1 <= 500 THEN 1 ELSE 0 END
    c.bench_function("case_when: scalar or scalar", |b| {
        let expr = Arc::new(
            CaseExpr::try_new(
                None,
                vec![(predicate.clone(), make_lit_i32(1))],
                Some(make_lit_i32(0)),
            )
            .unwrap(),
        );
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });

    // CASE WHEN c1 <= 500 THEN c2 [ELSE NULL] END
    c.bench_function("case_when: column or null", |b| {
        let expr = Arc::new(
            CaseExpr::try_new(None, vec![(predicate.clone(), make_col("c2", 1))], None)
                .unwrap(),
        );
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });

    // CASE WHEN c1 <= 500 THEN c2 ELSE c3 END
    c.bench_function("case_when: expr or expr", |b| {
        let expr = Arc::new(
            CaseExpr::try_new(
                None,
                vec![(predicate.clone(), make_col("c2", 1))],
                Some(make_col("c3", 2)),
            )
            .unwrap(),
        );
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });

    // CASE c1 WHEN 1 THEN c2 WHEN 2 THEN c3 END
    c.bench_function("case_when: CASE expr", |b| {
        let expr = Arc::new(
            CaseExpr::try_new(
                Some(make_col("c1", 0)),
                vec![
                    (make_lit_i32(1), make_col("c2", 1)),
                    (make_lit_i32(2), make_col("c3", 2)),
                ],
                None,
            )
            .unwrap(),
        );
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
