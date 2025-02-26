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
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::prelude::ThreadRng;
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::ScalarValue;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::{ColumnarValue, Expr};
use datafusion_functions_nested::map::map_udf;
use datafusion_functions_nested::planner::NestedFunctionPlanner;

fn keys(rng: &mut ThreadRng) -> Vec<String> {
    let mut keys = HashSet::with_capacity(1000);

    while keys.len() < 1000 {
        keys.insert(rng.gen_range(0..10000).to_string());
    }

    keys.into_iter().collect()
}

fn values(rng: &mut ThreadRng) -> Vec<i32> {
    let mut values = HashSet::with_capacity(1000);

    while values.len() < 1000 {
        values.insert(rng.gen_range(0..10000));
    }
    values.into_iter().collect()
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("make_map_1000", |b| {
        let mut rng = rand::thread_rng();
        let keys = keys(&mut rng);
        let values = values(&mut rng);
        let mut buffer = Vec::new();
        for i in 0..1000 {
            buffer.push(Expr::Literal(ScalarValue::Utf8(Some(keys[i].clone()))));
            buffer.push(Expr::Literal(ScalarValue::Int32(Some(values[i]))));
        }

        let planner = NestedFunctionPlanner {};

        b.iter(|| {
            black_box(
                planner
                    .plan_make_map(buffer.clone())
                    .expect("map should work on valid values"),
            );
        });
    });

    c.bench_function("map_1000", |b| {
        let mut rng = rand::thread_rng();
        let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 1000]));
        let key_list = ListArray::new(
            field,
            offsets,
            Arc::new(StringArray::from(keys(&mut rng))),
            None,
        );
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
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
                // TODO use invoke_with_args
                map_udf()
                    .invoke_batch(&[keys.clone(), values.clone()], 1)
                    .expect("map should work on valid values"),
            );
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
