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
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, ListArray, NullBufferBuilder,
    StringArray,
};
use arrow::datatypes::{DataType, Field, Int64Type};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_expr::Accumulator;
use datafusion_functions_aggregate::array_agg::{
    ArrayAggAccumulator, DistinctArrayAggAccumulator,
};

use arrow::buffer::OffsetBuffer;
use arrow::util::bench_util::create_primitive_array;
use rand::Rng;
use rand::SeedableRng;
use rand::distr::{Distribution, StandardUniform};
use rand::prelude::StdRng;

/// Returns fixed seedable RNG
pub fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

#[expect(clippy::needless_pass_by_value)]
fn merge_batch_bench(c: &mut Criterion, name: &str, values: ArrayRef) {
    let list_item_data_type = values.as_list::<i32>().values().data_type().clone();
    c.bench_function(name, |b| {
        b.iter(|| {
            #[expect(clippy::unit_arg)]
            black_box(
                ArrayAggAccumulator::try_new(&list_item_data_type, false)
                    .unwrap()
                    .merge_batch(std::slice::from_ref(&values))
                    .unwrap(),
            )
        })
    });
}

/// Create List array with the given item data type, null density, null locations and zero length lists density
/// Creates a random (but fixed-seeded) array of a given size and null density
pub fn create_list_array<T>(
    size: usize,
    null_density: f32,
    zero_length_lists_probability: f32,
) -> ListArray
where
    T: ArrowPrimitiveType,
    StandardUniform: Distribution<T::Native>,
{
    let mut nulls_builder = NullBufferBuilder::new(size);
    let mut rng = StdRng::seed_from_u64(42);

    let offsets = OffsetBuffer::from_lengths((0..size).map(|_| {
        let is_null = rng.random::<f32>() < null_density;

        let mut length = rng.random_range(1..10);

        if is_null {
            nulls_builder.append_null();

            if rng.random::<f32>() <= zero_length_lists_probability {
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

/// A realistic pool of database names with variable lengths.
const DB_NAMES: &[&str] = &[
    "postgres",
    "mysql",
    "oracle",
    "mssql",
    "mongodb",
    "redis",
    "elasticsearch",
    "cassandra",
    "dynamodb",
    "bigquery",
    "snowflake",
    "redshift",
    "databricks",
    "clickhouse",
    "duckdb",
    "cockroachdb",
    "tidb",
    "mariadb",
    "sqlite",
    "neo4j",
    "influxdb",
    "timescaledb",
    "yugabytedb",
    "planetscale",
    "singlestore",
];

/// Low-cardinality: every row is drawn uniformly from `DB_NAMES` (~25 distinct
/// values across 8 192 rows). Exercises the hot duplicate path.
fn create_string_array_low_cardinality(size: usize) -> StringArray {
    let mut rng = StdRng::seed_from_u64(42);
    StringArray::from_iter_values(
        (0..size).map(|_| DB_NAMES[rng.random_range(0..DB_NAMES.len())]),
    )
}

/// High-cardinality: `db_name_pct` fraction of rows are drawn from `DB_NAMES`;
/// the rest are near-unique random hex strings ("id_XXXXXXXX").
/// With 8 192 rows and a 32-bit space the collision probability among the
/// random strings is < 1 %, giving ~7 800 distinct values in total.
fn create_string_array_high_cardinality(size: usize, db_name_pct: f32) -> StringArray {
    let mut rng = StdRng::seed_from_u64(42);
    let strings: Vec<String> = (0..size)
        .map(|_| {
            if rng.random::<f32>() < db_name_pct {
                DB_NAMES[rng.random_range(0..DB_NAMES.len())].to_string()
            } else {
                format!("id_{:08x}", rng.random::<u32>())
            }
        })
        .collect();
    StringArray::from_iter_values(strings.iter().map(String::as_str))
}

fn distinct_update_batch_bench(
    c: &mut Criterion,
    name: &str,
    values: &ArrayRef,
    ignore_nulls: bool,
) {
    c.bench_function(name, |b| {
        b.iter(|| {
            DistinctArrayAggAccumulator::try_new(&DataType::Utf8, None, ignore_nulls)
                .unwrap()
                .update_batch(std::slice::from_ref(values))
                .unwrap()
        })
    });
}

fn distinct_array_agg_benchmark(c: &mut Criterion) {
    // --- Low cardinality: ~25 distinct DB names in 8 192 rows ---------------
    // Realistic production scenario: most rows are duplicates, the HashSet
    // saturates quickly and the rest of the batch is pure dedup overhead.
    let values = Arc::new(create_string_array_low_cardinality(8192)) as ArrayRef;
    distinct_update_batch_bench(
        c,
        "distinct_array_agg utf8 low cardinality (~25 distinct)",
        &values,
        false,
    );

    // --- High cardinality: ~5 % DB names, ~95 % near-unique random strings --
    // Worst-case scenario: almost every row is a new distinct value, so the
    // accumulator pays the full insertion cost for nearly every row.
    let values = Arc::new(create_string_array_high_cardinality(8192, 0.05)) as ArrayRef;
    distinct_update_batch_bench(
        c,
        "distinct_array_agg utf8 high cardinality (~7800 distinct, 5% db names)",
        &values,
        false,
    );
}

criterion_group!(benches, array_agg_benchmark, distinct_array_agg_benchmark);
criterion_main!(benches);
