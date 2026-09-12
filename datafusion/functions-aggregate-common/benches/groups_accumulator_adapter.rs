// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you
// under the Apache License, Version 2.0 (the
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

//! Benchmarks the uninstrumented `GroupsAccumulatorAdapter` update path.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_expr_common::groups_accumulator::GroupsAccumulator;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::GroupsAccumulatorAdapter;
use datafusion_functions_aggregate_common::min_max::MaxAccumulator;

const NUM_GROUPS: usize = 8_192;

fn groups_accumulator_adapter(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..NUM_GROUPS as i64));
    let group_indices: Vec<_> = (0..NUM_GROUPS).collect();

    c.bench_function("groups_accumulator_adapter/update_batch/8192_groups", |b| {
        b.iter_batched(
            || {
                GroupsAccumulatorAdapter::new(|| {
                    Ok(Box::new(MaxAccumulator::try_new(&DataType::Int64)?)
                        as Box<dyn Accumulator>)
                })
            },
            |mut accumulator| {
                accumulator
                    .update_batch(
                        &[Arc::clone(&values)],
                        &group_indices,
                        None,
                        NUM_GROUPS,
                    )
                    .unwrap();
                black_box(accumulator);
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, groups_accumulator_adapter);
criterion_main!(benches);
