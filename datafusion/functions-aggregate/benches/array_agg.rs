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

use arrow::array::Int64Array;
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, ListArray, NullBufferBuilder,
    StringArray,
};
use arrow::datatypes::{DataType, Field, FieldRef, Int64Type, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{Accumulator, AggregateUDFImpl};
use datafusion_functions_aggregate::array_agg::{
    ArrayAgg, ArrayAggAccumulator, DistinctArrayAggAccumulator,
};
use datafusion_physical_expr::{PhysicalSortExpr, expressions::col};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

use arrow::buffer::OffsetBuffer;
use arrow::util::bench_util::create_primitive_array;
use rand::Rng;
use rand::SeedableRng;
use rand::prelude::StdRng;
use rand::seq::SliceRandom;

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
    T: ArrowPrimitiveType<Native = i64>,
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

    let length = offsets.last() as usize;

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

/// Precomputes the schema, physical expressions, sort expression, and aggregate
/// metadata so each benchmark iteration measures only accumulator creation,
/// `update_batch()`, and `evaluate()`.
struct OrderedArrayAggBenchFixture {
    schema: Schema,
    value_expr: Arc<dyn PhysicalExpr>,
    value_field: FieldRef,
    order_by: PhysicalSortExpr,
    array_agg: Arc<dyn AggregateUDFImpl>,
    return_field: FieldRef,
}

impl OrderedArrayAggBenchFixture {
    fn new(input_preordered: bool) -> Self {
        let schema = Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("ordering", DataType::Int64, false),
        ]);

        let value_expr = col("value", &schema).unwrap();
        let ordering_expr = col("ordering", &schema).unwrap();

        let value_field = value_expr.return_field(&schema).unwrap();

        let order_by = PhysicalSortExpr::new(
            ordering_expr,
            arrow::compute::SortOptions {
                descending: false,
                nulls_first: false,
            },
        );

        let array_agg = Arc::new(ArrayAgg::default())
            .with_beneficial_ordering(input_preordered)
            .unwrap()
            .unwrap();

        let return_field = Field::new(
            "array_agg",
            DataType::List(Field::new_list_field(DataType::Int64, true).into()),
            true,
        )
        .into();

        Self {
            schema,
            value_expr,
            value_field,
            order_by,
            array_agg,
            return_field,
        }
    }
    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        self.array_agg
            .accumulator(AccumulatorArgs {
                return_field: Arc::clone(&self.return_field),
                schema: &self.schema,
                expr_fields: std::slice::from_ref(&self.value_field),
                ignore_nulls: false,
                order_bys: std::slice::from_ref(&self.order_by),
                is_reversed: false,
                name: "array_agg(value ORDER BY ordering)",
                is_distinct: false,
                exprs: std::slice::from_ref(&self.value_expr),
            })
            .unwrap()
    }
}

const ORDERED_ARRAY_AGG_ROWS: usize = 2048;

fn create_ordered_array_agg_batches(
    rows_per_batch: usize,
    input_preordered: bool,
) -> Vec<[ArrayRef; 2]> {
    assert!(
        rows_per_batch > 0,
        "rows_per_batch must be greater than zero"
    );

    let upper_value: i64 = ORDERED_ARRAY_AGG_ROWS.try_into().unwrap();
    let mut values = (0..upper_value).collect::<Vec<i64>>();

    if !input_preordered {
        let mut rng = StdRng::seed_from_u64(42);
        values.shuffle(&mut rng);
    }

    values
        .chunks(rows_per_batch)
        .map(|batch_values| {
            let values = Arc::new(Int64Array::from(batch_values.to_vec())) as ArrayRef;

            [
                Arc::clone(&values),
                values, // Reuse the payload values as ordering keys.
            ]
        })
        .collect()
}

fn ordered_array_agg_bench(
    c: &mut Criterion,
    name: &str,
    batches: &[[ArrayRef; 2]],
    input_preordered: bool,
) {
    c.bench_function(name, |b| {
        let fixture = OrderedArrayAggBenchFixture::new(input_preordered);
        b.iter(|| {
            let mut accumulator = fixture.create_accumulator();

            for batch in batches {
                accumulator
                    .update_batch(batch)
                    .expect("update_batch should succeed");
            }

            let result = accumulator.evaluate().expect("evaluate should succeed");

            black_box(result);
        })
    });
}

fn ordered_array_agg_benchmark(c: &mut Criterion) {
    for rows_per_batch in [1, 8, 64, ORDERED_ARRAY_AGG_ROWS] {
        let ordered_batches = create_ordered_array_agg_batches(rows_per_batch, true);
        ordered_array_agg_bench(
            c,
            &format!(
                "ordered_array_agg i64 ordered input, \
                   {rows_per_batch} rows per update_batch"
            ),
            &ordered_batches,
            true,
        );
        let shuffled_batches = create_ordered_array_agg_batches(rows_per_batch, false);
        ordered_array_agg_bench(
            c,
            &format!(
                "ordered_array_agg i64 random input, \
                   {rows_per_batch} rows per update_batch"
            ),
            &shuffled_batches,
            false,
        );
    }
}
criterion_group!(
    benches,
    array_agg_benchmark,
    distinct_array_agg_benchmark,
    ordered_array_agg_benchmark
);
criterion_main!(benches);
