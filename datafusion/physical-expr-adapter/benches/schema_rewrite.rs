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
