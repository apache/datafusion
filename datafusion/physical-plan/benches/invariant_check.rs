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

use arrow::datatypes::{Schema, SchemaRef};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::execution_plan::{
    ExecutionPlan, InvariantLevel, check_default_invariants,
};
use datafusion_physical_plan::union::UnionExec;

fn empty_exec(schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
    Arc::new(EmptyExec::new(Arc::clone(schema)))
}

fn bench_invariant_checks(c: &mut Criterion) {
    let schema = Arc::new(Schema::empty());
    let leaf = EmptyExec::new(Arc::clone(&schema));
    let union =
        UnionExec::try_new((0..4).map(|_| empty_exec(&schema)).collect::<Vec<_>>())
            .unwrap();

    let mut group = c.benchmark_group("check_default_invariants");
    group.bench_function("leaf", |b| {
        b.iter(|| {
            black_box(check_default_invariants(
                black_box(&leaf),
                InvariantLevel::Always,
            ))
        });
    });
    group.bench_function("four_children", |b| {
        b.iter(|| {
            black_box(check_default_invariants(
                black_box(union.as_ref()),
                InvariantLevel::Always,
            ))
        });
    });
    group.finish();
}

criterion_group!(benches, bench_invariant_checks);
criterion_main!(benches);
