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

extern crate criterion;

use arrow::array::{Int32Array, ListArray, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datafusion_functions::core::{make_map, map};
use rand::prelude::ThreadRng;
use rand::Rng;
use std::sync::Arc;

fn keys(rng: &mut ThreadRng) -> Vec<String> {
    let mut keys = vec![];
    for _ in 0..1000 {
        keys.push(rng.gen_range(0..9999).to_string());
    }
    keys
}

fn values(rng: &mut ThreadRng) -> Vec<i32> {
    let mut values = vec![];
    for _ in 0..1000 {
        values.push(rng.gen_range(0..9999));
    }
    values
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("make_map_1000", |b| {
        let mut rng = rand::thread_rng();
        let keys = keys(&mut rng);
        let values = values(&mut rng);
        let mut buffer = Vec::new();
        for i in 0..1000 {
            buffer.push(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                keys[i].clone(),
            ))));
            buffer.push(ColumnarValue::Scalar(ScalarValue::Int32(Some(values[i]))));
        }

        b.iter(|| {
            black_box(
                make_map()
                    .invoke(&buffer)
                    .expect("map should work on valid values"),
            );
        });
    });

    c.bench_function("map_1000", |b| {
        let mut rng = rand::thread_rng();
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 1000]));
        let key_list = ListArray::new(
            field,
            offsets,
            Arc::new(StringArray::from(keys(&mut rng))),
            None,
        );
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 1000]));
        let value_list = ListArray::new(
            field,
            offsets,
            Arc::new(Int32Array::from(values(&mut rng))),
            None,
        );
        let keys = ColumnarValue::Scalar(ScalarValue::List(Arc::new(key_list)));
        let values = ColumnarValue::Scalar(ScalarValue::List(Arc::new(value_list)));

        b.iter(|| {
            black_box(
                map()
                    .invoke(&[keys.clone(), values.clone()])
                    .expect("map should work on valid values"),
            );
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
