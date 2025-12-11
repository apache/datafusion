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
    Array, ArrayRef, Float32Array, Int32Array, StringArray, StringViewArray,
};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::expressions::{col, in_list, lit};
use rand::distr::Alphanumeric;
use rand::prelude::*;
use std::any::TypeId;
use std::hint::black_box;
use std::sync::Arc;

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

const IN_LIST_LENGTHS: [usize; 3] = [3, 8, 100];
const NULL_PERCENTS: [f64; 2] = [0., 0.2];
const STRING_LENGTHS: [usize; 3] = [3, 12, 100];
const ARRAY_LENGTH: usize = 1024;

/// Returns a friendly type name for the array type.
fn array_type_name<A: 'static>() -> &'static str {
    let id = TypeId::of::<A>();
    if id == TypeId::of::<StringArray>() {
        "Utf8"
    } else if id == TypeId::of::<StringViewArray>() {
        "Utf8View"
    } else if id == TypeId::of::<Float32Array>() {
        "Float32"
    } else if id == TypeId::of::<Int32Array>() {
        "Int32"
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

/// Entry point: registers in_list benchmarks for Utf8, Utf8View, Float32, and Int32 arrays.
fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(120320);

    // Benchmarks for string array types (Utf8, Utf8View)
    bench_string_type::<StringArray>(c, &mut rng, |s| ScalarValue::Utf8(Some(s)));
    bench_string_type::<StringViewArray>(c, &mut rng, |s| ScalarValue::Utf8View(Some(s)));

    // Benchmarks for numeric types
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
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
