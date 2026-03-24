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

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::min_max::ArrayMax;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const NUM_ROWS: usize = 8192;
const SEED: u64 = 42;
const LIST_NULL_DENSITY: f64 = 0.1;
const ELEMENT_NULL_DENSITY: f64 = 0.1;

fn create_int64_list_array(
    num_rows: usize,
    list_size: usize,
    element_null_density: f64,
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let total_values = num_rows * list_size;

    if element_null_density > 0.0 {
        let values: Vec<Option<i64>> = (0..total_values)
            .map(|_| {
                if rng.random::<f64>() < element_null_density {
                    None
                } else {
                    Some(rng.random::<i64>() % 10_000)
                }
            })
            .collect();
        let values_array = Arc::new(Int64Array::from(values));

        let offsets: Vec<i32> = (0..=num_rows).map(|i| (i * list_size) as i32).collect();
        let nulls: Vec<bool> = (0..num_rows)
            .map(|_| rng.random::<f64>() >= LIST_NULL_DENSITY)
            .collect();

        Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(offsets.into()),
            values_array,
            Some(NullBuffer::from(nulls)),
        ))
    } else {
        // No element nulls — values array has no null buffer
        let values: Vec<i64> = (0..total_values)
            .map(|_| rng.random::<i64>() % 10_000)
            .collect();
        let values_array = Arc::new(Int64Array::from(values));

        let offsets: Vec<i32> = (0..=num_rows).map(|i| (i * list_size) as i32).collect();
        let nulls: Vec<bool> = (0..num_rows)
            .map(|_| rng.random::<f64>() >= LIST_NULL_DENSITY)
            .collect();

        Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, false)),
            OffsetBuffer::new(offsets.into()),
            values_array,
            Some(NullBuffer::from(nulls)),
        ))
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let udf = ArrayMax::new();
    let config_options = Arc::new(ConfigOptions::default());

    for list_size in [10, 100, 1000] {
        for (label, null_density) in [("nulls", ELEMENT_NULL_DENSITY), ("no_nulls", 0.0)]
        {
            let list_array = create_int64_list_array(NUM_ROWS, list_size, null_density);
            let args = vec![ColumnarValue::Array(Arc::clone(&list_array))];
            let arg_fields =
                vec![Field::new("arg_0", list_array.data_type().clone(), true).into()];
            let return_field: Arc<Field> = Field::new("f", DataType::Int64, true).into();

            c.bench_with_input(
                BenchmarkId::new("array_max", format!("{label}/list_size={list_size}")),
                &list_array,
                |b, _| {
                    b.iter(|| {
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: NUM_ROWS,
                            return_field: return_field.clone(),
                            config_options: config_options.clone(),
                        })
                        .unwrap()
                    });
                },
            );
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
