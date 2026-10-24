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

use arrow::array::{ArrayRef, Int32Array, ListArray, StringArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{Bencher, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::sort::ArraySort;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

const SEED: u64 = 42;
const NUM_ROWS: usize = 8192;
/// Each case sorts the next of its arrays in each iteration, cycling through
/// arrays that hold at least this many elements in total.
const CYCLE_ELEMENTS: usize = 1 << 20;

/// The arrays a case cycles through, each built by `make` from its own seed:
/// enough to hold `CYCLE_ELEMENTS` elements, and at least two.
fn inputs(
    elements_per_row: usize,
    make: impl Fn(&mut StdRng) -> ArrayRef,
) -> Vec<ArrayRef> {
    let count = CYCLE_ELEMENTS.div_ceil(NUM_ROWS * elements_per_row).max(2);
    (0..count as u64)
        .map(|i| make(&mut StdRng::seed_from_u64(SEED + i)))
        .collect()
}

/// Offsets for `NUM_ROWS` rows of 1 to `2 * elements_per_row - 1` elements,
/// `elements_per_row` on average.
fn random_offsets(rng: &mut StdRng, elements_per_row: usize) -> OffsetBuffer<i32> {
    OffsetBuffer::from_lengths(
        (0..NUM_ROWS).map(|_| rng.random_range(1..2 * elements_per_row)),
    )
}

/// Validity for `len` slots, each NULL with probability `null_rate`, or `None`
/// if `null_rate` is zero.
fn random_nulls(rng: &mut StdRng, len: usize, null_rate: f64) -> Option<NullBuffer> {
    (null_rate > 0.0)
        .then(|| NullBuffer::from_iter((0..len).map(|_| !rng.random_bool(null_rate))))
}

/// A list of distinct `Int32` values in random order, where each row is NULL
/// with probability `null_row_rate` and each element with probability
/// `null_element_rate`.
fn create_int32_list_array(
    rng: &mut StdRng,
    elements_per_row: usize,
    null_row_rate: f64,
    null_element_rate: f64,
) -> ArrayRef {
    let offsets = random_offsets(rng, elements_per_row);
    let mut values: Vec<i32> = (0..offsets.last()).collect();
    values.shuffle(rng);
    let element_nulls = random_nulls(rng, values.len(), null_element_rate);
    let list_nulls = random_nulls(rng, NUM_ROWS, null_row_rate);

    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        offsets,
        Arc::new(Int32Array::new(values.into(), element_nulls)),
        list_nulls,
    ))
}

fn create_string_list_array(rng: &mut StdRng, elements_per_row: usize) -> ArrayRef {
    let offsets = random_offsets(rng, elements_per_row);
    let mut indices: Vec<i32> = (0..offsets.last()).collect();
    indices.shuffle(rng);
    let values =
        StringArray::from_iter_values(indices.iter().map(|i| format!("value_{i:06}")));

    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        offsets,
        Arc::new(values),
        None,
    ))
}

/// Calls `array_sort` on `array`, followed by `options`: the sort order
/// (`"ASC"` / `"DESC"`), then where NULLs go (`"NULLS FIRST"` / `"NULLS LAST"`).
/// Options left out take their defaults, ascending with NULLs first.
fn invoke_array_sort(
    udf: &ArraySort,
    array: &ArrayRef,
    options: &[&str],
) -> ColumnarValue {
    let mut args = vec![ColumnarValue::Array(Arc::clone(array))];
    let mut arg_fields = vec![Field::new("arr", array.data_type().clone(), true).into()];
    for (name, &option) in ["order", "nulls"].into_iter().zip(options) {
        args.push(ColumnarValue::Scalar(ScalarValue::from(option)));
        arg_fields.push(Field::new(name, DataType::Utf8, true).into());
    }
    udf.invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields,
        number_rows: array.len(),
        return_field: Field::new("result", array.data_type().clone(), true).into(),
        config_options: Arc::new(ConfigOptions::default()),
    })
    .unwrap()
}

/// Sorts the next of `arrays` with `options` in each iteration.
fn sort_each<'a>(
    udf: &'a ArraySort,
    arrays: &'a [ArrayRef],
    options: &'a [&'a str],
) -> impl FnMut(&mut Bencher) + 'a {
    let mut arrays = arrays.iter().cycle();
    move |b| b.iter(|| black_box(invoke_array_sort(udf, arrays.next().unwrap(), options)))
}

/// Vary the average elements per row over [5, 20, 100, 1000]: for small
/// arrays, per-row overhead dominates, whereas for larger arrays the sort
/// kernel dominates.
fn bench_array_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_sort");
    let udf = ArraySort::new();

    // Int32 arrays, ascending and descending: `sort_row` sorts ascending and
    // reverses, so the descending case measures the cost of that extra pass.
    for &elements_per_row in &[5, 20, 100, 1000] {
        let arrays = inputs(elements_per_row, |rng| {
            create_int32_list_array(rng, elements_per_row, 0.0, 0.0)
        });
        group.bench_function(
            BenchmarkId::new("int32", elements_per_row),
            sort_each(&udf, &arrays, &[]),
        );
        group.bench_function(
            BenchmarkId::new("int32_desc", elements_per_row),
            sort_each(&udf, &arrays, &["DESC"]),
        );
    }

    // Int32 with nulls in the outer list (10% null rows), single size
    {
        let arrays = inputs(50, |rng| create_int32_list_array(rng, 50, 0.1, 0.0));
        group.bench_function("int32_with_nulls", sort_each(&udf, &arrays, &[]));
    }

    // Int32 with null elements (10% of elements), placed first in ascending
    // and descending order, and last in ascending order
    for &elements_per_row in &[5, 20, 100, 1000] {
        let arrays = inputs(elements_per_row, |rng| {
            create_int32_list_array(rng, elements_per_row, 0.0, 0.1)
        });
        let cases: [(&str, &[&str]); 3] = [
            ("int32_null_elements", &[]),
            ("int32_null_elements_desc", &["DESC"]),
            ("int32_null_elements_nulls_last", &["ASC", "NULLS LAST"]),
        ];
        for (name, options) in cases {
            group.bench_function(
                BenchmarkId::new(name, elements_per_row),
                sort_each(&udf, &arrays, options),
            );
        }
    }

    // String arrays
    for &elements_per_row in &[5, 20, 100, 1000] {
        let arrays = inputs(elements_per_row, |rng| {
            create_string_list_array(rng, elements_per_row)
        });
        group.bench_function(
            BenchmarkId::new("string", elements_per_row),
            sort_each(&udf, &arrays, &[]),
        );
    }

    group.finish();
}

criterion_group!(benches, bench_array_sort);
criterion_main!(benches);
