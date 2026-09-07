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

use arrow::array::{ArrayRef, StringArray};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion_physical_expr_common::binary_map::{
    ArrowBytesMap, INITIAL_MAP_CAPACITY, OutputType,
};
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 8192;

fn make_short_strings(cardinality: usize) -> ArrayRef {
    let values = (0..NUM_ROWS).map(|index| format!("{:04x}", index % cardinality));
    Arc::new(StringArray::from_iter_values(values))
}

fn make_long_strings(cardinality: usize) -> ArrayRef {
    let values = (0..NUM_ROWS).map(|index| {
        let value = (index % cardinality) as u32;
        format!(
            "{value:08x}{:08x}{:08x}{:08x}",
            value.wrapping_mul(17),
            value.wrapping_mul(31),
            value.wrapping_mul(127)
        )
    });
    Arc::new(StringArray::from_iter_values(values))
}

fn bench_arrow_bytes_map(c: &mut Criterion) {
    let cases = [
        // Exercises inline entry storage while still growing the output buffer.
        ("short_unique", make_short_strings(NUM_ROWS)),
        // Exercises repeated buffer growth and out-of-line entry storage.
        ("long_unique", make_long_strings(NUM_ROWS)),
        // Fits the distinct values in the initial buffer and repeats comparisons.
        ("long_low_cardinality", make_long_strings(128)),
    ];

    let mut group = c.benchmark_group("arrow_bytes_map");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));

    for (name, values) in cases {
        group.bench_function(name, |b| {
            b.iter(|| {
                // The `long_low_cardinality` case is defined by the distinct
                // values fitting in the pre-allocated buffer, so this benchmark
                // measures the pre-allocating constructor.
                let mut map = ArrowBytesMap::<i32, usize>::with_capacity(
                    OutputType::Utf8,
                    INITIAL_MAP_CAPACITY,
                );
                let mut next_payload = 0;
                map.insert_if_new(
                    &values,
                    |_| {
                        let payload = next_payload;
                        next_payload += 1;
                        payload
                    },
                    |payload| {
                        black_box(payload);
                    },
                );
                black_box(map.into_state())
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_arrow_bytes_map);
criterion_main!(benches);
