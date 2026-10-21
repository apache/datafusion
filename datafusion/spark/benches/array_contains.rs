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

//! Benchmarks for Spark's `array_contains`, which returns NULL instead of false
//! when the value is not found and the list contains a NULL element.

use arrow::array::{ArrayRef, FixedSizeListArray, Int32Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_spark::function::array::array_contains::SparkArrayContains;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::hint::black_box;
use std::sync::Arc;

const ROWS: usize = 8192;

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    // 10% of elements are NULL. The needle never occurs, so every row checks
    // whether its list contains a NULL element.
    for width in [4, 16, 128] {
        let values = int_values(&mut rng, ROWS * width);
        let list = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::from_repeated_length(width, ROWS),
            values,
            None,
        );
        bench(c, &format!("list/width_{width}"), Arc::new(list));
    }
    let width = 16;
    let list = FixedSizeListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        width as i32,
        int_values(&mut rng, ROWS * width),
        None,
    );
    bench(c, &format!("fixed_size_list/width_{width}"), Arc::new(list));
}

fn int_values(rng: &mut StdRng, len: usize) -> ArrayRef {
    let values = (0..len)
        .map(|_| rng.random_range(0..100))
        .collect::<Vec<_>>();
    let nulls = NullBuffer::from_iter((0..len).map(|_| !rng.random_bool(0.1)));
    Arc::new(Int32Array::new(values.into(), Some(nulls)))
}

fn bench(c: &mut Criterion, name: &str, haystack: ArrayRef) {
    let udf = SparkArrayContains::new();
    let args = ScalarFunctionArgs {
        number_rows: haystack.len(),
        arg_fields: vec![
            Arc::new(Field::new("haystack", haystack.data_type().clone(), true)),
            Arc::new(Field::new("needle", DataType::Int32, true)),
        ],
        args: vec![
            ColumnarValue::Array(haystack),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(-1))),
        ],
        return_field: Arc::new(Field::new("result", DataType::Boolean, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    c.bench_function(&format!("array_contains/{name}"), |b| {
        b.iter(|| black_box(udf.invoke_with_args(args.clone()).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
