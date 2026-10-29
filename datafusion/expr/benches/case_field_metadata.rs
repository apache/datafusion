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

use std::collections::HashMap;
use std::hint::black_box;

use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::DFSchema;
use datafusion_expr::{Expr, ExprSchemable, col, lit, when};

fn literal_case(width: usize) -> Expr {
    let mut builder = when(lit(true), lit(0_i32));
    for i in 1..width {
        builder = builder.when(lit(false), lit(i32::try_from(i).unwrap()));
    }
    builder.end().unwrap()
}

fn column_case(width: usize, marked_last: bool) -> (DFSchema, Expr) {
    let fields = (0..width)
        .map(|i| {
            let field = Field::new(format!("c{i}"), DataType::Int32, false);
            if marked_last && i == width - 1 {
                field.with_metadata(HashMap::from([(
                    "source".to_owned(),
                    "marked".to_owned(),
                )]))
            } else {
                field
            }
        })
        .collect::<Vec<_>>();
    let schema =
        DFSchema::from_unqualified_fields(fields.into(), HashMap::new()).unwrap();
    let mut builder = when(lit(true), col("c0"));
    for i in 1..width {
        builder = builder.when(lit(false), col(format!("c{i}")));
    }
    (schema, builder.end().unwrap())
}

fn case_field_metadata(c: &mut Criterion) {
    let mut group = c.benchmark_group("case_field_metadata");
    for width in [10, 100, 1000] {
        let literal = literal_case(width);
        let empty_schema = DFSchema::empty();
        group.bench_with_input(
            BenchmarkId::new("plain_literals", width),
            &width,
            |b, _| {
                b.iter(|| black_box(literal.to_field(black_box(&empty_schema)).unwrap()))
            },
        );

        for (name, marked_last) in [("plain_columns", false), ("marked_last", true)] {
            let (schema, expression) = column_case(width, marked_last);
            group.bench_with_input(BenchmarkId::new(name, width), &width, |b, _| {
                b.iter(|| black_box(expression.to_field(black_box(&schema)).unwrap()))
            });
        }
    }
    group.finish();
}

criterion_group!(benches, case_field_metadata);
criterion_main!(benches);
