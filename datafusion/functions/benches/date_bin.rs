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

extern crate criterion;

use std::sync::Arc;

use arrow::array::{ArrayRef, TimestampSecondArray};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::ScalarValue;
use rand::rngs::ThreadRng;
use rand::Rng;

use datafusion_expr::ColumnarValue;
use datafusion_functions::datetime::date_bin;

fn timestamps(rng: &mut ThreadRng) -> TimestampSecondArray {
    let mut seconds = vec![];
    for _ in 0..1000 {
        seconds.push(rng.gen_range(0..1_000_000));
    }

    TimestampSecondArray::from(seconds)
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("date_bin_1000", |b| {
        let mut rng = rand::thread_rng();
        let interval = ColumnarValue::Scalar(ScalarValue::new_interval_dt(0, 1_000_000));
        let timestamps = ColumnarValue::Array(Arc::new(timestamps(&mut rng)) as ArrayRef);
        let udf = date_bin();

        b.iter(|| {
            black_box(
                udf.invoke(&[interval.clone(), timestamps.clone()])
                    .expect("date_bin should work on valid values"),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
