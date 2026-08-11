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

//! Interning cost for a single integer GROUP BY column, via the production
//! `new_group_values` dispatch. On a branch with the direct-index flat grouper
//! (#19938) this measures the flat path; on `main` it measures the hash path
//! (`GroupValuesPrimitive`) — run on both to A/B. Each arm is warmed to steady
//! state (all groups created), then re-interns the same batch.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_physical_plan::aggregates::group_values::new_group_values;
use datafusion_physical_plan::aggregates::order::GroupOrdering;

const BATCH: usize = 8192;

fn int64_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]))
}

fn bench_intern(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_group_by_intern_int64");
    for card in [100i64, 1000, 10_000] {
        let col: ArrayRef = Arc::new(Int64Array::from_iter_values(
            (0..BATCH as i64).map(|i| i % card),
        ));
        let cols = std::slice::from_ref(&col);
        group.bench_with_input(BenchmarkId::from_parameter(card), &card, |b, _| {
            let mut gv = new_group_values(int64_schema(), &GroupOrdering::None).unwrap();
            let mut groups = Vec::with_capacity(BATCH);
            gv.intern(cols, &mut groups).unwrap(); // warm to steady state
            b.iter(|| {
                gv.intern(black_box(cols), &mut groups).unwrap();
                black_box(&groups);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_intern);
criterion_main!(benches);
