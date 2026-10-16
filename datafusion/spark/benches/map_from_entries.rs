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

//! Benchmarks for Spark's `map_from_entries`, which returns NULL for a row whose
//! list of entries contains a NULL entry.

use arrow::array::{Array, ArrayRef, Int32Array, ListArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
};
use datafusion_spark::function::map::map_from_entries::MapFromEntries;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::hint::black_box;
use std::sync::Arc;

const ROWS: usize = 8192;

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    // 10% of rows contain one NULL entry. Keys are unique within each row.
    for width in [4, 16, 64] {
        let len = ROWS * width;
        let mut valid = vec![true; len];
        for row in 0..ROWS {
            if rng.random_bool(0.1) {
                valid[row * width + rng.random_range(0..width)] = false;
            }
        }
        let entries = StructArray::new(
            Fields::from(vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Int32, true),
            ]),
            vec![
                Arc::new(Int32Array::from_iter_values(
                    (0..len).map(|i| (i % width) as i32),
                )),
                Arc::new(Int32Array::from_iter_values(
                    (0..len).map(|_| rng.random_range(0..100)),
                )),
            ],
            Some(NullBuffer::from(valid)),
        );
        let list = ListArray::new(
            Arc::new(Field::new_list_field(entries.data_type().clone(), true)),
            OffsetBuffer::from_repeated_length(width, ROWS),
            Arc::new(entries),
            None,
        );
        bench(c, &format!("width_{width}"), Arc::new(list));
    }
}

fn bench(c: &mut Criterion, name: &str, entries: ArrayRef) {
    let udf = MapFromEntries::new();
    let arg_fields = vec![Arc::new(Field::new(
        "entries",
        entries.data_type().clone(),
        true,
    ))];
    let return_field = udf
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None],
        })
        .unwrap();
    let args = ScalarFunctionArgs {
        number_rows: entries.len(),
        args: vec![ColumnarValue::Array(entries)],
        arg_fields,
        return_field,
        config_options: Arc::new(ConfigOptions::default()),
    };
    c.bench_function(&format!("map_from_entries/{name}"), |b| {
        b.iter(|| black_box(udf.invoke_with_args(args.clone()).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
