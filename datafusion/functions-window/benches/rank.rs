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
use std::ops::Range;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_expr::{PartitionEvaluator, WindowUDFImpl};
use datafusion_functions_window::cume_dist::CumeDist;
use datafusion_functions_window::rank::Rank;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;

const NUM_ROWS: usize = 8192;

fn make_peer_ranges(group_size: usize) -> Vec<Range<usize>> {
    let mut ranges = Vec::with_capacity(NUM_ROWS.div_ceil(group_size));
    let mut start = 0;
    while start < NUM_ROWS {
        let end = (start + group_size).min(NUM_ROWS);
        ranges.push(start..end);
        start = end;
    }
    ranges
}

fn create_rank_evaluator(rank: &Rank) -> Box<dyn PartitionEvaluator> {
    rank.partition_evaluator(PartitionEvaluatorArgs::default())
        .unwrap()
}

fn create_cume_dist_evaluator() -> Box<dyn PartitionEvaluator> {
    CumeDist::new()
        .partition_evaluator(PartitionEvaluatorArgs::default())
        .unwrap()
}

fn bench_rank(c: &mut Criterion) {
    let mut group = c.benchmark_group("rank");

    for group_size in [1, 8, 64] {
        let ranges = make_peer_ranges(group_size);
        let range_label = format!("group_{group_size}");

        group.bench_function(BenchmarkId::new("rank", &range_label), |b| {
            b.iter(|| {
                let evaluator = create_rank_evaluator(&Rank::basic());
                black_box(
                    evaluator
                        .evaluate_all_with_rank(NUM_ROWS, black_box(&ranges))
                        .unwrap(),
                );
            })
        });

        group.bench_function(BenchmarkId::new("dense_rank", &range_label), |b| {
            b.iter(|| {
                let evaluator = create_rank_evaluator(&Rank::dense_rank());
                black_box(
                    evaluator
                        .evaluate_all_with_rank(NUM_ROWS, black_box(&ranges))
                        .unwrap(),
                );
            })
        });

        group.bench_function(BenchmarkId::new("percent_rank", &range_label), |b| {
            b.iter(|| {
                let evaluator = create_rank_evaluator(&Rank::percent_rank());
                black_box(
                    evaluator
                        .evaluate_all_with_rank(NUM_ROWS, black_box(&ranges))
                        .unwrap(),
                );
            })
        });

        group.bench_function(BenchmarkId::new("cume_dist", &range_label), |b| {
            b.iter(|| {
                let evaluator = create_cume_dist_evaluator();
                black_box(
                    evaluator
                        .evaluate_all_with_rank(NUM_ROWS, black_box(&ranges))
                        .unwrap(),
                );
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_rank);
criterion_main!(benches);
