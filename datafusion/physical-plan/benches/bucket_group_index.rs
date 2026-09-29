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

//! Run the same bucket group interning workload in separate processes with
//! DATAFUSION_BUCKET_INDEX unset, hashbrown-half, and linear.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_physical_plan::aggregates::group_values::GroupValues;
use datafusion_physical_plan::aggregates::group_values::multi_group_by::GroupValuesColumn;

const BATCH_SIZE: usize = 8192;

#[derive(Clone, Copy)]
enum KeyKind {
    Integers,
    ShortText,
    LongText,
}

impl KeyKind {
    fn name(self) -> &'static str {
        match self {
            Self::Integers => "integers",
            Self::ShortText => "short_text",
            Self::LongText => "long_text",
        }
    }
}

fn input(
    kind: KeyKind,
    groups: usize,
    repeats: usize,
    hot: bool,
) -> (SchemaRef, Vec<Vec<ArrayRef>>) {
    let fields = match kind {
        KeyKind::Integers => vec![
            Field::new("key1", DataType::Int64, false),
            Field::new("key2", DataType::Int64, false),
        ],
        KeyKind::ShortText | KeyKind::LongText => vec![
            Field::new("key1", DataType::Utf8View, false),
            Field::new("key2", DataType::Int64, false),
        ],
    };
    let schema = Arc::new(Schema::new(fields));
    let rows = groups * repeats;
    let batches = (0..rows)
        .step_by(BATCH_SIZE)
        .map(|start| {
            let end = (start + BATCH_SIZE).min(rows);
            let keys: Vec<usize> = (start..end)
                .map(|row| {
                    // A fixed odd multiplier spreads neighboring keys while
                    // keeping repeated keys identical across input batches.
                    if hot && row % repeats != 0 {
                        0
                    } else {
                        ((row / repeats) * 0x9e3779b1) % groups
                    }
                })
                .collect();
            let second: ArrayRef = Arc::new(Int64Array::from(
                keys.iter().map(|&key| key as i64).collect::<Vec<_>>(),
            ));
            let first: ArrayRef = match kind {
                KeyKind::Integers => Arc::new(Int64Array::from(
                    keys.iter()
                        .map(|&key| (key as i64) ^ 0x5a5a5a5a)
                        .collect::<Vec<_>>(),
                )),
                KeyKind::ShortText => {
                    let strings = keys
                        .iter()
                        .map(|key| format!("k{key:08x}"))
                        .collect::<Vec<_>>();
                    Arc::new(StringViewArray::from_iter_values(
                        strings.iter().map(String::as_str),
                    ))
                }
                KeyKind::LongText => {
                    let strings = keys
                        .iter()
                        .map(|key| format!("a-long-group-key-{key:08x}-suffix"))
                        .collect::<Vec<_>>();
                    Arc::new(StringViewArray::from_iter_values(
                        strings.iter().map(String::as_str),
                    ))
                }
            };
            vec![first, second]
        })
        .collect();
    (schema, batches)
}

fn intern(
    values: &mut GroupValuesColumn<false>,
    batches: &[Vec<ArrayRef>],
    groups: &mut Vec<usize>,
) {
    for batch in batches {
        values.intern(batch, groups).unwrap();
    }
    black_box(values.len());
    black_box(groups);
}

fn bench_bucket_index(c: &mut Criterion) {
    let mut suite = c.benchmark_group("bucket_group_index");
    suite.sample_size(10);

    for (kind, groups, repeats, hot) in [
        (KeyKind::Integers, 768, 1, false),
        (KeyKind::Integers, 8_192, 8, false),
        (KeyKind::Integers, 24_576, 1, false),
        (KeyKind::Integers, 98_304, 1, false),
        (KeyKind::Integers, 393_216, 1, false),
        (KeyKind::ShortText, 6_144, 8, false),
        (KeyKind::LongText, 6_144, 8, false),
        (KeyKind::Integers, 6_144, 10, true),
    ] {
        let (schema, batches) = input(kind, groups, repeats, hot);
        let id = format!(
            "{}_groups_{groups}_repeats_{repeats}_{}",
            kind.name(),
            if hot { "hot" } else { "uniform" }
        );
        let mut sample =
            GroupValuesColumn::<false>::try_new_with_borrow(Arc::clone(&schema), true)
                .unwrap();
        intern(&mut sample, &batches, &mut Vec::new());
        eprintln!("WORKSET case={id} reported_bytes={}", sample.size());
        suite.bench_with_input(BenchmarkId::new("build", &id), &batches, |b, batches| {
            b.iter_batched(
                || {
                    (
                        GroupValuesColumn::<false>::try_new_with_borrow(
                            Arc::clone(&schema),
                            true,
                        )
                        .unwrap(),
                        Vec::new(),
                    )
                },
                |(mut values, mut group_ids)| {
                    intern(&mut values, batches, &mut group_ids)
                },
                criterion::BatchSize::LargeInput,
            )
        });
        suite.bench_with_input(BenchmarkId::new("reuse", &id), &batches, |b, batches| {
            b.iter_batched(
                || {
                    let mut values = GroupValuesColumn::<false>::try_new_with_borrow(
                        Arc::clone(&schema),
                        true,
                    )
                    .unwrap();
                    let mut group_ids = Vec::new();
                    intern(&mut values, batches, &mut group_ids);
                    values.clear_shrink(groups);
                    (values, group_ids)
                },
                |(mut values, mut group_ids)| {
                    intern(&mut values, batches, &mut group_ids)
                },
                criterion::BatchSize::LargeInput,
            )
        });
    }
    suite.finish();
}

criterion_group!(benches, bench_bucket_index);
criterion_main!(benches);
