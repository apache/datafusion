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
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::DFSchema;
use datafusion_common::metadata::FieldMetadata;
use datafusion_expr::{ExprSchemable, col};

fn nested_alias_to_field(c: &mut Criterion) {
    let source_metadata = HashMap::from([
        ("keep".to_string(), "base".to_string()),
        ("override".to_string(), "base".to_string()),
    ]);
    let schema = DFSchema::from_unqualified_fields(
        vec![Arc::new(
            Field::new("source", DataType::Int32, false).with_metadata(source_metadata),
        )]
        .into(),
        HashMap::new(),
    )
    .unwrap();

    let mut group = c.benchmark_group("nested_alias_to_field");
    for depth in [0, 1, 4, 8, 12] {
        for with_metadata in [false, true] {
            let mut expr = col("source");
            for index in 0..depth {
                expr = if with_metadata {
                    expr.alias_with_metadata(
                        format!("alias{index}"),
                        Some(FieldMetadata::from(HashMap::from([(
                            "override".to_string(),
                            index.to_string(),
                        )]))),
                    )
                } else {
                    expr.alias(format!("alias{index}"))
                };
            }
            // Rust API builds directly nested aliases, avoiding SQL's surface syntax limitations.
            let mode = if with_metadata { "metadata" } else { "plain" };
            group.bench_with_input(BenchmarkId::new(mode, depth), &expr, |b, expr| {
                b.iter(|| {
                    black_box(black_box(expr).to_field(black_box(&schema)).unwrap())
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, nested_alias_to_field);
criterion_main!(benches);
