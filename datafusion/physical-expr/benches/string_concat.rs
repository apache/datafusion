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

use arrow::array::StringViewArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{BinaryExpr, Column};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 8192;
const SEED: u64 = 42;

fn create_string_view_array(
    num_rows: usize,
    str_len: usize,
    null_density: f64,
    seed: u64,
) -> StringViewArray {
    let mut rng = StdRng::seed_from_u64(seed);
    let values: Vec<Option<String>> = (0..num_rows)
        .map(|_| {
            if rng.random::<f64>() < null_density {
                None
            } else {
                let s: String = (0..str_len)
                    .map(|_| rng.random_range(b'a'..=b'z') as char)
                    .collect();
                Some(s)
            }
        })
        .collect();
    StringViewArray::from_iter(values)
}

fn bench_concat_utf8view(c: &mut Criterion) {
    let mut group = c.benchmark_group("concat_utf8view");

    let schema = Arc::new(Schema::new(vec![
        Field::new("left", DataType::Utf8View, true),
        Field::new("right", DataType::Utf8View, true),
    ]));

    // left || right
    let expr = BinaryExpr::new(
        Arc::new(Column::new("left", 0)),
        Operator::StringConcat,
        Arc::new(Column::new("right", 1)),
    );

    for null_density in [0.0, 0.1, 0.5] {
        let left = create_string_view_array(NUM_ROWS, 16, null_density, SEED);
        let right = create_string_view_array(NUM_ROWS, 16, null_density, SEED + 1);

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(left), Arc::new(right)])
                .unwrap();

        let label = format!("nulls_{}", (null_density * 100.0) as u32);
        group.bench_with_input(
            BenchmarkId::new("concat", &label),
            &null_density,
            |b, _| {
                b.iter(|| {
                    black_box(expr.evaluate(black_box(&batch)).unwrap());
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_concat_utf8view);
criterion_main!(benches);
