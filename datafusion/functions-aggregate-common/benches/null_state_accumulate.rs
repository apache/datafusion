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

use arrow::array::ArrowNativeTypeOp;
use arrow::{
    array::{ArrayRef, AsArray, BooleanArray, Int64Array},
    datatypes::Int64Type,
};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::group_index_operations::FlatGroupIndexOperations;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::{
    accumulate::{self, accumulate_indices, NullStateAdapter},
    blocks::GeneralBlocks,
    group_index_operations::{BlockedGroupIndexOperations, GroupIndexOperations},
};

fn generate_group_indices(len: usize) -> Vec<usize> {
    (0..len).collect()
}

fn generate_values(len: usize, has_null: bool) -> ArrayRef {
    if has_null {
        let values = (0..len)
            .map(|i| if i % 7 == 0 { None } else { Some(i as i64) })
            .collect::<Vec<_>>();
        Arc::new(Int64Array::from(values))
    } else {
        let values = (0..len).map(|i| Some(i as i64)).collect::<Vec<_>>();
        Arc::new(Int64Array::from(values))
    }
}

fn generate_filter(len: usize) -> Option<BooleanArray> {
    let values = (0..len)
        .map(|i| {
            if i % 7 == 0 {
                None
            } else if i % 5 == 0 {
                Some(false)
            } else {
                Some(true)
            }
        })
        .collect::<Vec<_>>();
    Some(BooleanArray::from(values))
}

fn criterion_benchmark(c: &mut Criterion) {
    let batch_size = 8192;
    let len = batch_size * 4096;
    let group_indices = generate_group_indices(len);
    let rows_count = group_indices.len();
    let values = generate_values(len, false);
    let opt_filter = generate_filter(len);
    let prim_op = |x: &mut i64, y: i64| *x = x.add_wrapping(y);

    let mut num_chunks = len.div_ceil(batch_size);
    let last_chunk_size = len % batch_size;
    let last_chunk_size = if last_chunk_size > 0 {
        last_chunk_size
    } else {
        batch_size
    };

    let group_indices_chunks = group_indices
        .chunks(batch_size)
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<_>>();

    let mut values_chunks = vec![];
    for chunk_idx in 0..num_chunks {
        let chunk_size = if chunk_idx == num_chunks - 1 {
            last_chunk_size
        } else {
            batch_size
        };
        let chunk = values.slice(chunk_idx * batch_size, chunk_size);

        values_chunks.push(chunk);
    }

    let mut total_num_groups_chunks = vec![];
    for group_indices in &group_indices_chunks {
        let total_num_groups = *group_indices.iter().max().unwrap() + 1;
        total_num_groups_chunks.push(total_num_groups);
    }

    let mode = std::env::var("ACC_MODE").unwrap_or("all".to_string());
    let block_factor = std::env::var("BLOCK_FACTOR")
        .unwrap_or("1".to_string())
        .parse::<usize>()
        .unwrap();

    if &mode == "blocked" || &mode == "all" {
        c.bench_function("Blocked accumulate", |b| {
            b.iter(|| {
                let block_size = block_factor * batch_size;
                let mut blocks = GeneralBlocks::<i64>::new(Some(block_size));
                let group_index_operation = BlockedGroupIndexOperations::new(block_size);

                let group_indices_iter = group_indices_chunks.iter();
                let values_iter = values_chunks.iter();
                let total_num_groups_iter = total_num_groups_chunks.iter();
                let iter = group_indices_iter
                    .zip(values_iter)
                    .zip(total_num_groups_iter);
                for ((group_indices, values), &total_num_groups) in iter {
                    let values = values.as_primitive::<Int64Type>();
                    blocks.expand(total_num_groups, 0);

                    // let mut value_fn = |block_id, block_offset, new_value| {
                    //     let value = blocks.get_mut(block_id, block_offset);
                    //     prim_op(value, new_value);
                    // };

                    accumulate::accumulate(
                        group_indices,
                        values,
                        None,
                        |group_index, value| {
                            let block_id =
                                group_index_operation.get_block_id(group_index);
                            let block_offset =
                                group_index_operation.get_block_offset(group_index);
                            sum(&mut blocks, block_id, block_offset, value);
                        },
                    );
                }
            })
        });
    }

    if &mode == "flat" || &mode == "all" {
        c.bench_function("Flat accumulate", |b| {
            b.iter(|| {
                let mut blocks = GeneralBlocks::<i64>::new(None);
                let group_index_operation = FlatGroupIndexOperations;

                let group_indices_iter = group_indices_chunks.iter();
                let values_iter = values_chunks.iter();
                let total_num_groups_iter = total_num_groups_chunks.iter();
                let iter = group_indices_iter
                    .zip(values_iter)
                    .zip(total_num_groups_iter);
                for ((group_indices, values), &total_num_groups) in iter {
                    let values = values.as_primitive::<Int64Type>();
                    blocks.expand(total_num_groups, 0);

                    // let mut value_fn = |block_id, block_offset, new_value| {
                    //     let value = blocks.get_mut(block_id, block_offset);
                    //     prim_op(value, new_value);
                    // };

                    accumulate::accumulate(
                        group_indices,
                        values,
                        None,
                        |group_index, value| {
                            let block_id =
                                group_index_operation.get_block_id(group_index);
                            let block_offset =
                                group_index_operation.get_block_offset(group_index);
                            sum(&mut blocks, block_id, block_offset, value);
                        },
                    );
                }
            })
        });
    }
}

#[inline]
fn sum(
    blocks: &mut GeneralBlocks<i64>,
    block_id: usize,
    block_offset: usize,
    new_value: i64,
) {
    let value = blocks.get_mut(block_id, block_offset);
    *value = value.add_wrapping(new_value);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
