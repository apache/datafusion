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

use arrow::array::{
    Array, ArrayRef, Float32Array, Int16Array, Int32Array, StringArray, StringViewArray,
    TimestampNanosecondArray, UInt8Array,
};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{col, in_list, lit};
use rand::distr::Alphanumeric;
use rand::prelude::*;
use std::any::TypeId;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

/// Measures how long `in_list(col("a"), exprs)` takes to evaluate against a single RecordBatch.
fn do_bench(c: &mut Criterion, name: &str, values: ArrayRef, exprs: &[ScalarValue]) {
    let schema = Schema::new(vec![Field::new("a", values.data_type().clone(), true)]);
    let exprs = exprs.iter().map(|s| lit(s.clone())).collect();
    let expr = in_list(col("a", &schema).unwrap(), exprs, &false, &schema).unwrap();
    let batch = RecordBatch::try_new(Arc::new(schema), vec![values]).unwrap();

    c.bench_function(name, |b| {
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });
}

/// Generates a random alphanumeric string of the specified length.
fn random_string(rng: &mut StdRng, len: usize) -> String {
    let value = rng.sample_iter(&Alphanumeric).take(len).collect();
    String::from_utf8(value).unwrap()
}

const IN_LIST_LENGTHS: [usize; 4] = [3, 8, 28, 100];
const LIST_WITH_COLUMNS_LENGTHS: [usize; 3] = [3, 8, 28];
const NULL_PERCENTS: [f64; 2] = [0., 0.2];
const MATCH_PERCENTS: [f64; 3] = [0.0, 0.5, 1.0];
const STRING_LENGTHS: [usize; 3] = [3, 12, 100];
const ARRAY_LENGTH: usize = 8192;

/// Mixed string lengths for realistic benchmarks.
/// ~50% short (≤12 bytes), ~50% long (>12 bytes).
const MIXED_STRING_LENGTHS: &[usize] = &[3, 6, 9, 12, 16, 20, 25, 30];

/// Returns a friendly type name for the array type.
fn array_type_name<A: 'static>() -> &'static str {
    let id = TypeId::of::<A>();
    if id == TypeId::of::<StringArray>() {
        "Utf8"
    } else if id == TypeId::of::<StringViewArray>() {
        "Utf8View"
    } else if id == TypeId::of::<Float32Array>() {
        "Float32"
    } else if id == TypeId::of::<Int16Array>() {
        "Int16"
    } else if id == TypeId::of::<Int32Array>() {
        "Int32"
    } else if id == TypeId::of::<TimestampNanosecondArray>() {
        "TimestampNs"
    } else if id == TypeId::of::<UInt8Array>() {
        "UInt8"
    } else {
        "Unknown"
    }
}

/// Builds a benchmark name from array type, list size, and null percentage.
fn bench_name<A: 'static>(in_list_length: usize, null_percent: f64) -> String {
    format!(
        "in_list/{}/list={in_list_length}/nulls={}%",
        array_type_name::<A>(),
        (null_percent * 100.0) as u32
    )
}

/// Runs in_list benchmarks for a string array type across all list-size × null-ratio × string-length combinations.
fn bench_string_type<A>(
    c: &mut Criterion,
    rng: &mut StdRng,
    make_scalar: fn(String) -> ScalarValue,
) where
    A: Array + FromIterator<Option<String>> + 'static,
{
    for in_list_length in IN_LIST_LENGTHS {
        for null_percent in NULL_PERCENTS {
            for string_length in STRING_LENGTHS {
                let values: A = (0..ARRAY_LENGTH)
                    .map(|_| {
                        rng.random_bool(1.0 - null_percent)
                            .then(|| random_string(rng, string_length))
                    })
                    .collect();

                let in_list: Vec<_> = (0..in_list_length)
                    .map(|_| make_scalar(random_string(rng, string_length)))
                    .collect();

                do_bench(
                    c,
                    &format!(
                        "{}/str={string_length}",
                        bench_name::<A>(in_list_length, null_percent)
                    ),
                    Arc::new(values),
                    &in_list,
                )
            }
        }
    }
}

/// Runs in_list benchmarks for a numeric array type across all list-size × null-ratio combinations.
fn bench_numeric_type<T, A>(
    c: &mut Criterion,
    rng: &mut StdRng,
    mut gen_value: impl FnMut(&mut StdRng) -> T,
    make_scalar: fn(T) -> ScalarValue,
) where
    A: Array + FromIterator<Option<T>> + 'static,
{
    for in_list_length in IN_LIST_LENGTHS {
        for null_percent in NULL_PERCENTS {
            let values: A = (0..ARRAY_LENGTH)
                .map(|_| rng.random_bool(1.0 - null_percent).then(|| gen_value(rng)))
                .collect();

            let in_list: Vec<_> = (0..in_list_length)
                .map(|_| make_scalar(gen_value(rng)))
                .collect();

            do_bench(
                c,
                &bench_name::<A>(in_list_length, null_percent),
                Arc::new(values),
                &in_list,
            );
        }
    }
}

/// Generates a random string with a length chosen from MIXED_STRING_LENGTHS.
fn random_mixed_length_string(rng: &mut StdRng) -> String {
    let len = *MIXED_STRING_LENGTHS.choose(rng).unwrap();
    random_string(rng, len)
}

/// Benchmarks realistic mixed-length IN list scenario.
///
/// Tests with:
/// - Mixed short (≤12 bytes) and long (>12 bytes) strings in the IN list
/// - Varying prefixes (fully random strings)
/// - Configurable match rate (% of values that are in the IN list)
/// - Various IN list sizes (3, 8, 28, 100)
fn bench_realistic_mixed_strings<A>(
    c: &mut Criterion,
    rng: &mut StdRng,
    make_scalar: fn(String) -> ScalarValue,
) where
    A: Array + FromIterator<Option<String>> + 'static,
{
    for in_list_length in IN_LIST_LENGTHS {
        for match_percent in [0.0, 0.25, 0.75] {
            for null_percent in NULL_PERCENTS {
                // Generate IN list with mixed-length random strings
                let in_list_strings: Vec<String> = (0..in_list_length)
                    .map(|_| random_mixed_length_string(rng))
                    .collect();

                let in_list: Vec<_> = in_list_strings
                    .iter()
                    .map(|s| make_scalar(s.clone()))
                    .collect();

                // Generate values array with controlled match rate
                let values: A = (0..ARRAY_LENGTH)
                    .map(|_| {
                        if !rng.random_bool(1.0 - null_percent) {
                            None
                        } else if rng.random_bool(match_percent) {
                            // Pick from IN list (will match)
                            Some(in_list_strings.choose(rng).unwrap().clone())
                        } else {
                            // Generate new random string (unlikely to match)
                            Some(random_mixed_length_string(rng))
                        }
                    })
                    .collect();

                do_bench(
                    c,
                    &format!(
                        "in_list/{}/mixed/list={}/match={}%/nulls={}%",
                        array_type_name::<A>(),
                        in_list_length,
                        (match_percent * 100.0) as u32,
                        (null_percent * 100.0) as u32
                    ),
                    Arc::new(values),
                    &in_list,
                );
            }
        }
    }
}

/// Benchmarks the column-reference evaluation path (no static filter) by including
/// a column reference in the IN list, which prevents static filter creation.
///
/// This simulates SQL like:
/// ```sql
/// CREATE TABLE t (a INT, b0 INT, b1 INT, b2 INT);
/// SELECT * FROM t WHERE a IN (b0, b1, b2);
/// ```
///
/// - `values`: the "needle" column (`a`)
/// - `list_cols`: the "haystack" columns (`b0`, `b1`, …)
fn do_bench_with_columns(
    c: &mut Criterion,
    name: &str,
    values: ArrayRef,
    list_cols: &[ArrayRef],
) {
    let mut fields = vec![Field::new("a", values.data_type().clone(), true)];
    let mut columns: Vec<ArrayRef> = vec![values];

    // Build list expressions: column refs (forces non-constant evaluation path)
    let schema_fields: Vec<Field> = list_cols
        .iter()
        .enumerate()
        .map(|(i, col_arr)| {
            let name = format!("b{i}");
            fields.push(Field::new(&name, col_arr.data_type().clone(), true));
            columns.push(Arc::clone(col_arr));
            Field::new(&name, col_arr.data_type().clone(), true)
        })
        .collect();

    let schema = Schema::new(fields);
    let list_exprs: Vec<Arc<dyn PhysicalExpr>> = schema_fields
        .iter()
        .map(|f| col(f.name(), &schema).unwrap())
        .collect();

    let expr = in_list(col("a", &schema).unwrap(), list_exprs, &false, &schema).unwrap();
    let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();

    c.bench_function(name, |b| {
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });
}

/// Benchmarks the IN list path with column references for Int32 arrays.
///
/// Equivalent SQL:
/// ```sql
/// CREATE TABLE t (a INT, b0 INT, b1 INT, ...);
/// SELECT * FROM t WHERE a IN (b0, b1, ...);
/// ```
fn bench_with_columns_int32(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);

    for list_size in LIST_WITH_COLUMNS_LENGTHS {
        for match_percent in MATCH_PERCENTS {
            for null_percent in NULL_PERCENTS {
                // Generate the "needle" column
                let values: Int32Array = (0..ARRAY_LENGTH)
                    .map(|_| {
                        rng.random_bool(1.0 - null_percent)
                            .then(|| rng.random_range(0..1000))
                    })
                    .collect();

                // Generate list columns with controlled match rate
                let list_cols: Vec<ArrayRef> = (0..list_size)
                    .map(|_| {
                        let col: Int32Array = (0..ARRAY_LENGTH)
                            .map(|row| {
                                if rng.random_bool(1.0 - null_percent) {
                                    if rng.random_bool(match_percent) {
                                        // Copy from values to create a match
                                        if values.is_null(row) {
                                            Some(rng.random_range(0..1000))
                                        } else {
                                            Some(values.value(row))
                                        }
                                    } else {
                                        // Random value (unlikely to match)
                                        Some(rng.random_range(1000..2000))
                                    }
                                } else {
                                    None
                                }
                            })
                            .collect();
                        Arc::new(col) as ArrayRef
                    })
                    .collect();

                do_bench_with_columns(
                    c,
                    &format!(
                        "in_list_cols/Int32/list={}/match={}%/nulls={}%",
                        list_size,
                        (match_percent * 100.0) as u32,
                        (null_percent * 100.0) as u32
                    ),
                    Arc::new(values),
                    &list_cols,
                );
            }
        }
    }
}

/// Benchmarks the IN list path with column references for Utf8 arrays.
///
/// Equivalent SQL:
/// ```sql
/// CREATE TABLE t (a VARCHAR, b0 VARCHAR, b1 VARCHAR, ...);
/// SELECT * FROM t WHERE a IN (b0, b1, ...);
/// ```
fn bench_with_columns_utf8(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(99);

    for list_size in LIST_WITH_COLUMNS_LENGTHS {
        for match_percent in MATCH_PERCENTS {
            // Generate the "needle" column
            let value_strings: Vec<Option<String>> = (0..ARRAY_LENGTH)
                .map(|_| rng.random_bool(0.8).then(|| random_string(&mut rng, 12)))
                .collect();
            let values: StringArray =
                value_strings.iter().map(|s| s.as_deref()).collect();

            // Generate list columns with controlled match rate
            let list_cols: Vec<ArrayRef> = (0..list_size)
                .map(|_| {
                    let col: StringArray = (0..ARRAY_LENGTH)
                        .map(|row| {
                            if rng.random_bool(match_percent) {
                                // Copy from values to create a match
                                value_strings[row].as_deref()
                            } else {
                                Some("no_match_value_xyz")
                            }
                        })
                        .collect();
                    Arc::new(col) as ArrayRef
                })
                .collect();

            do_bench_with_columns(
                c,
                &format!(
                    "in_list_cols/Utf8/list={}/match={}%",
                    list_size,
                    (match_percent * 100.0) as u32,
                ),
                Arc::new(values),
                &list_cols,
            );
        }
    }
}

/// Entry point: registers in_list benchmarks for string and numeric array types.
fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(120320);

    // Benchmarks for string array types (Utf8, Utf8View)
    bench_string_type::<StringArray>(c, &mut rng, |s| ScalarValue::Utf8(Some(s)));
    bench_string_type::<StringViewArray>(c, &mut rng, |s| ScalarValue::Utf8View(Some(s)));

    // Realistic mixed-length string benchmarks (TPC-H style)
    bench_realistic_mixed_strings::<StringArray>(c, &mut rng, |s| {
        ScalarValue::Utf8(Some(s))
    });
    bench_realistic_mixed_strings::<StringViewArray>(c, &mut rng, |s| {
        ScalarValue::Utf8View(Some(s))
    });

    // Benchmarks for numeric types
    bench_numeric_type::<u8, UInt8Array>(
        c,
        &mut rng,
        |rng| rng.random(),
        |v| ScalarValue::UInt8(Some(v)),
    );
    bench_numeric_type::<i16, Int16Array>(
        c,
        &mut rng,
        |rng| rng.random(),
        |v| ScalarValue::Int16(Some(v)),
    );
    bench_numeric_type::<f32, Float32Array>(
        c,
        &mut rng,
        |rng| rng.random(),
        |v| ScalarValue::Float32(Some(v)),
    );
    bench_numeric_type::<i32, Int32Array>(
        c,
        &mut rng,
        |rng| rng.random(),
        |v| ScalarValue::Int32(Some(v)),
    );
    bench_numeric_type::<i64, TimestampNanosecondArray>(
        c,
        &mut rng,
        |rng| rng.random(),
        |v| ScalarValue::TimestampNanosecond(Some(v), None),
    );

    // Column-reference path benchmarks (non-constant list expressions)
    bench_with_columns_int32(c);
    bench_with_columns_utf8(c);
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_millis(500));
    targets = criterion_benchmark
}
criterion_main!(benches);
