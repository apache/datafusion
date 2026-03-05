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
fn make_struct_array(num_rows: usize, null_fraction: f64, rng: &mut StdRng) -> ArrayRef {
    let ints: Int32Array = (0..num_rows)
        .map(|_| {
            if null_fraction > 0.0 && rng.random::<f64>() < null_fraction {
                None
            } else {
                Some(rng.random::<i32>())
            }
        })
        .collect();

    let strings: StringArray = (0..num_rows)
        .map(|_| {
            if null_fraction > 0.0 && rng.random::<f64>() < null_fraction {
                None
            } else {
                let s: String = (0..12).map(|_| rng.random_range(b'a'..=b'z') as char).collect();
                Some(s)
            }
        })
        .collect();

    let fields = Fields::from(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Utf8, true),
    ]);

    Arc::new(StructArray::try_new(fields, vec![Arc::new(ints), Arc::new(strings)], None).unwrap())
}

fn criterion_benchmark(c: &mut Criterion) {
    for num_rows in [4096, 8192] {
        for null_pct in [0, 10] {
            let null_fraction = null_pct as f64 / 100.0;
            let mut rng = StdRng::seed_from_u64(42);

            let lhs = make_struct_array(num_rows, null_fraction, &mut rng);
            let rhs_array = make_struct_array(num_rows, null_fraction, &mut rng);
            let rhs_scalar_array = make_struct_array(1, 0.0, &mut rng);
            let rhs_scalar = Scalar::new(rhs_scalar_array);

            c.bench_function(
                &format!("compare_nested array_array rows={num_rows} nulls={null_pct}%"),
                |b| {
                    b.iter(|| {
                        black_box(
                            compare_op_for_nested(
                                Operator::Eq,
                                &lhs,
                                &rhs_array,
                            )
                            .unwrap(),
                        )
                    })
                },
            );

            c.bench_function(
                &format!("compare_nested array_scalar rows={num_rows} nulls={null_pct}%"),
                |b| {
                    b.iter(|| {
                        black_box(
                            compare_op_for_nested(
                                Operator::Eq,
                                &lhs,
                                &rhs_scalar,
                            )
                            .unwrap(),
                        )
                    })
                },
            );
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
