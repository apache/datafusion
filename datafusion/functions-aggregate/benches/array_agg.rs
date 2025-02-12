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

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, ListArray, NullBufferBuilder,
};
use arrow::datatypes::{Field, Int64Type};
use arrow::util::bench_util::create_primitive_array;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_expr::Accumulator;
use datafusion_functions_aggregate::array_agg::ArrayAggAccumulator;

use arrow::buffer::OffsetBuffer;
use arrow::util::test_util::seedable_rng;
use rand::distributions::{Distribution, Standard};
use rand::Rng;

fn merge_batch_bench(c: &mut Criterion, name: &str, values: ArrayRef) {
    let list_item_data_type = values.as_list::<i32>().values().data_type().clone();
    c.bench_function(name, |b| {
        b.iter(|| {
            #[allow(clippy::unit_arg)]
            black_box(
                ArrayAggAccumulator::try_new(&list_item_data_type)
                    .unwrap()
                    .merge_batch(&[values.clone()])
                    .unwrap(),
            )
        })
    });
}

/// Create List array with the given item data type, null density, null locations and zero length lists density
/// Creates an random (but fixed-seeded) array of a given size and null density
pub fn create_list_array<T>(
    size: usize,
    null_density: f32,
    zero_length_lists_probability: f32,
) -> ListArray
where
    T: ArrowPrimitiveType,
    Standard: Distribution<T::Native>,
{
    let mut nulls_builder = NullBufferBuilder::new(size);
    let mut rng = seedable_rng();

    let offsets = OffsetBuffer::from_lengths((0..size).map(|_| {
        let is_null = rng.gen::<f32>() < null_density;

        let mut length = rng.gen_range(1..10);

        if is_null {
            nulls_builder.append_null();

            if rng.gen::<f32>() <= zero_length_lists_probability {
                length = 0;
            }
        } else {
            nulls_builder.append_non_null();
        }

        length
    }));

    let length = *offsets.last().unwrap() as usize;

    let values = create_primitive_array::<T>(length, 0.0);

    let field = Field::new_list_field(T::DATA_TYPE, true);

    ListArray::new(
        Arc::new(field),
        offsets,
        Arc::new(values),
        nulls_builder.finish(),
    )
}

fn array_agg_benchmark(c: &mut Criterion) {
    let values = Arc::new(create_list_array::<Int64Type>(8192, 0.0, 1.0)) as ArrayRef;
    merge_batch_bench(c, "array_agg i64 merge_batch no nulls", values);

    let values = Arc::new(create_list_array::<Int64Type>(8192, 1.0, 1.0)) as ArrayRef;
    merge_batch_bench(
        c,
        "array_agg i64 merge_batch all nulls, 100% of nulls point to a zero length array",
        values,
    );

    let values = Arc::new(create_list_array::<Int64Type>(8192, 1.0, 0.9)) as ArrayRef;
    merge_batch_bench(
        c,
        "array_agg i64 merge_batch all nulls, 90% of nulls point to a zero length array",
        values,
    );

    // All nulls point to a 0 length array

    let values = Arc::new(create_list_array::<Int64Type>(8192, 0.3, 1.0)) as ArrayRef;
    merge_batch_bench(
        c,
        "array_agg i64 merge_batch 30% nulls, 100% of nulls point to a zero length array",
        values,
    );

    let values = Arc::new(create_list_array::<Int64Type>(8192, 0.7, 1.0)) as ArrayRef;
    merge_batch_bench(
        c,
        "array_agg i64 merge_batch 70% nulls, 100% of nulls point to a zero length array",
        values,
    );

    let values = Arc::new(create_list_array::<Int64Type>(8192, 0.3, 0.99)) as ArrayRef;
    merge_batch_bench(
        c,
        "array_agg i64 merge_batch 30% nulls, 99% of nulls point to a zero length array",
        values,
    );

    let values = Arc::new(create_list_array::<Int64Type>(8192, 0.7, 0.99)) as ArrayRef;
    merge_batch_bench(
        c,
        "array_agg i64 merge_batch 70% nulls, 99% of nulls point to a zero length array",
        values,
    );

    let values = Arc::new(create_list_array::<Int64Type>(8192, 0.3, 0.9)) as ArrayRef;
    merge_batch_bench(
        c,
        "array_agg i64 merge_batch 30% nulls, 90% of nulls point to a zero length array",
        values,
    );

    let values = Arc::new(create_list_array::<Int64Type>(8192, 0.7, 0.9)) as ArrayRef;
    merge_batch_bench(
        c,
        "array_agg i64 merge_batch 70% nulls, 90% of nulls point to a zero length array",
        values,
    );

    let values = Arc::new(create_list_array::<Int64Type>(8192, 0.3, 0.50)) as ArrayRef;
    merge_batch_bench(
        c,
        "array_agg i64 merge_batch 30% nulls, 50% of nulls point to a zero length array",
        values,
    );

    let values = Arc::new(create_list_array::<Int64Type>(8192, 0.7, 0.50)) as ArrayRef;
    merge_batch_bench(
        c,
        "array_agg i64 merge_batch 70% nulls, 50% of nulls point to a zero length array",
        values,
    );

    let values = Arc::new(create_list_array::<Int64Type>(8192, 0.3, 0.0)) as ArrayRef;
    merge_batch_bench(
        c,
        "array_agg i64 merge_batch 30% nulls, 0% of nulls point to a zero length array",
        values,
    );

    let values = Arc::new(create_list_array::<Int64Type>(8192, 0.7, 0.0)) as ArrayRef;
    merge_batch_bench(
        c,
        "array_agg i64 merge_batch 70% nulls, 0% of nulls point to a zero length array",
        values,
    );
}

criterion_group!(benches, array_agg_benchmark);
criterion_main!(benches);
