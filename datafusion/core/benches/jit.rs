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

#[macro_use]
extern crate criterion;
extern crate arrow;
extern crate datafusion;

mod data_utils;
use crate::criterion::Criterion;
use crate::data_utils::{create_record_batches, create_schema};
use datafusion::row::jit::writer::bench_write_batch_jit;
use datafusion::row::writer::bench_write_batch;
use datafusion::row::RowType;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let partitions_len = 8;
    let array_len = 32768 * 1024; // 2^25
    let batch_size = 2048; // 2^11

    let schema = Arc::new(create_schema());
    let batches =
        create_record_batches(schema.clone(), array_len, partitions_len, batch_size);

    c.bench_function("compact row serializer", |b| {
        b.iter(|| {
            criterion::black_box(
                bench_write_batch(&batches, schema.clone(), RowType::Compact).unwrap(),
            )
        })
    });

    c.bench_function("word aligned row serializer", |b| {
        b.iter(|| {
            criterion::black_box(
                bench_write_batch(&batches, schema.clone(), RowType::WordAligned)
                    .unwrap(),
            )
        })
    });

    c.bench_function("compact row serializer jit", |b| {
        b.iter(|| {
            criterion::black_box(
                bench_write_batch_jit(&batches, schema.clone(), RowType::Compact)
                    .unwrap(),
            )
        })
    });

    c.bench_function("word aligned row serializer jit", |b| {
        b.iter(|| {
            criterion::black_box(
                bench_write_batch_jit(&batches, schema.clone(), RowType::WordAligned)
                    .unwrap(),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
