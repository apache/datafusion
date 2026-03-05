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

use arrow::array::{ArrayRef, Int32Array, Scalar, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_expr_common::operator::Operator;
use datafusion_physical_expr_common::datum::compare_op_for_nested;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

/// Build a StructArray with fields {x: Int32, y: Utf8}.
fn make_struct_array(num_rows: usize, rng: &mut StdRng) -> ArrayRef {
    let ints: Int32Array = (0..num_rows).map(|_| Some(rng.random::<i32>())).collect();

    let strings: StringArray = (0..num_rows)
        .map(|_| {
            let s: String =
                (0..12).map(|_| rng.random_range(b'a'..=b'z') as char).collect();
            Some(s)
        })
        .collect();

    let fields = Fields::from(vec![
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::Utf8, false),
    ]);

    Arc::new(
        StructArray::try_new(fields, vec![Arc::new(ints), Arc::new(strings)], None)
            .unwrap(),
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    let num_rows = 8192;
    let mut rng = StdRng::seed_from_u64(42);

    let lhs = make_struct_array(num_rows, &mut rng);
    let rhs_array = make_struct_array(num_rows, &mut rng);
    let rhs_scalar = Scalar::new(make_struct_array(1, &mut rng));

    c.bench_function("compare_nested array_array", |b| {
        b.iter(|| {
            black_box(
                compare_op_for_nested(Operator::Eq, &lhs, &rhs_array).unwrap(),
            )
        })
    });

    c.bench_function("compare_nested array_scalar", |b| {
        b.iter(|| {
            black_box(
                compare_op_for_nested(Operator::Eq, &lhs, &rhs_scalar).unwrap(),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
