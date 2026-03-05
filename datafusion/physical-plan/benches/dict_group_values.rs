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

//! Benchmarks for GROUP BY on dictionary-encoded columns.
//!
//! Compares three paths:
//! - `column_utf8`: GroupValuesColumn with plain Utf8 (fast-path baseline)
//! - `column_dict`: GroupValuesColumn with Dictionary(Int32, Utf8) (new path)
//! - `rows_dict`:   GroupValuesRows with Dictionary(Int32, Utf8) (old fallback)

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{
    BenchmarkId, Criterion, criterion_group, criterion_main,
};
use datafusion_physical_plan::aggregates::group_values::multi_group_by::GroupValuesColumn;
use datafusion_physical_plan::aggregates::group_values::row::GroupValuesRows;
use datafusion_physical_plan::aggregates::group_values::GroupValues;
use rand::Rng;
use rand::rngs::StdRng;
use rand::SeedableRng;

const CARDINALITIES: [usize; 3] = [50, 1_000, 10_000];
const BATCH_SIZES: [usize; 2] = [8_192, 65_536];
const NUM_BATCHES: usize = 10;

/// Generate `num_rows` random string values chosen from `cardinality` distinct strings,
/// returned as both plain Utf8 and Dictionary(Int32, Utf8) arrays.
fn generate_string_batches(
    num_rows: usize,
    cardinality: usize,
    seed: u64,
) -> (ArrayRef, ArrayRef) {
    let mut rng = StdRng::seed_from_u64(seed);

    // Build a pool of distinct strings
    let pool: Vec<String> = (0..cardinality)
        .map(|i| format!("group_value_{i:06}"))
        .collect();

    let values: Vec<&str> = (0..num_rows)
        .map(|_| pool[rng.random_range(0..cardinality)].as_str())
        .collect();

    let utf8_array: ArrayRef = Arc::new(StringArray::from(values));
    let dict_array = cast(&utf8_array, &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)))
        .expect("cast to dictionary");

    (utf8_array, dict_array)
}

fn bench_dict_group_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("dict_group_values");

    for &cardinality in &CARDINALITIES {
        for &batch_size in &BATCH_SIZES {
            // Pre-generate batches (both utf8 and dict variants)
            let batches: Vec<(ArrayRef, ArrayRef)> = (0..NUM_BATCHES as u64)
                .map(|seed| generate_string_batches(batch_size, cardinality, seed))
                .collect();

            let param = format!("card_{cardinality}/batch_{batch_size}");

            // ---- column_utf8: GroupValuesColumn with plain Utf8 (baseline fast path) ----
            {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("key", DataType::Utf8, false),
                ]));
                let id = BenchmarkId::new("column_utf8", &param);
                group.bench_function(id, |b| {
                    b.iter(|| {
                        let mut gv = GroupValuesColumn::<false>::try_new(Arc::clone(&schema)).unwrap();
                        let mut groups = Vec::new();
                        for (utf8, _dict) in &batches {
                            gv.intern(&[Arc::clone(utf8)], &mut groups).unwrap();
                        }
                    });
                });
            }

            // ---- column_dict: GroupValuesColumn with Dictionary(Int32, Utf8) (new path) ----
            {
                let schema = Arc::new(Schema::new(vec![
                    Field::new(
                        "key",
                        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                        false,
                    ),
                ]));
                let id = BenchmarkId::new("column_dict", &param);
                group.bench_function(id, |b| {
                    b.iter(|| {
                        let mut gv = GroupValuesColumn::<false>::try_new(Arc::clone(&schema)).unwrap();
                        let mut groups = Vec::new();
                        for (_utf8, dict) in &batches {
                            gv.intern(&[Arc::clone(dict)], &mut groups).unwrap();
                        }
                    });
                });
            }

            // ---- rows_dict: GroupValuesRows with Dictionary(Int32, Utf8) (old fallback) ----
            {
                let schema = Arc::new(Schema::new(vec![
                    Field::new(
                        "key",
                        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                        false,
                    ),
                ]));
                let id = BenchmarkId::new("rows_dict", &param);
                group.bench_function(id, |b| {
                    b.iter(|| {
                        let mut gv = GroupValuesRows::try_new(Arc::clone(&schema)).unwrap();
                        let mut groups = Vec::new();
                        for (_utf8, dict) in &batches {
                            gv.intern(&[Arc::clone(dict)], &mut groups).unwrap();
                        }
                    });
                });
            }
        }
    }

    group.finish();
}

criterion_group!(benches, bench_dict_group_values);
criterion_main!(benches);
