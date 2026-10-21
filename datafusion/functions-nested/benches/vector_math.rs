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

//! Benchmarks for `inner_product`, `cosine_distance`, and `array_normalize`,
//! which compute over each row of `Float64` lists.

use arrow::array::{ArrayRef, Float64Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::array_normalize::ArrayNormalize;
use datafusion_functions_nested::cosine_distance::CosineDistance;
use datafusion_functions_nested::inner_product::InnerProduct;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::hint::black_box;
use std::sync::Arc;

// Every input has the same number of elements, so narrower rows mean more rows.
const ELEMENTS: usize = 1 << 17;

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    for width in [4, 32, 256] {
        // With "null_elements", 10% of rows contain one NULL element, which
        // makes the result for that row NULL.
        for (nulls, null_rows) in [("no_nulls", 0.0), ("null_elements", 0.1)] {
            let left = float_lists(&mut rng, width, null_rows);
            let right = float_lists(&mut rng, width, null_rows);
            let case = format!("width_{width}/{nulls}");
            bench(
                c,
                &format!("inner_product/{case}"),
                &InnerProduct::new(),
                vec![Arc::clone(&left), Arc::clone(&right)],
            );
            bench(
                c,
                &format!("cosine_distance/{case}"),
                &CosineDistance::new(),
                vec![Arc::clone(&left), right],
            );
            bench(
                c,
                &format!("array_normalize/{case}"),
                &ArrayNormalize::new(),
                vec![left],
            );
        }
    }
}

fn bench(c: &mut Criterion, name: &str, udf: &dyn ScalarUDFImpl, inputs: Vec<ArrayRef>) {
    let arg_types = inputs
        .iter()
        .map(|input| input.data_type().clone())
        .collect::<Vec<_>>();
    let arg_fields = arg_types
        .iter()
        .enumerate()
        .map(|(i, data_type)| {
            Arc::new(Field::new(format!("arg_{i}"), data_type.clone(), true))
        })
        .collect();
    let return_type = udf.return_type(&arg_types).unwrap();
    let args = ScalarFunctionArgs {
        number_rows: inputs[0].len(),
        args: inputs.into_iter().map(ColumnarValue::Array).collect(),
        arg_fields,
        return_field: Arc::new(Field::new("result", return_type, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    c.bench_function(name, |b| {
        b.iter(|| black_box(udf.invoke_with_args(args.clone()).unwrap()))
    });
}

fn float_lists(rng: &mut StdRng, width: usize, null_rows: f64) -> ArrayRef {
    let rows = ELEMENTS / width;
    let values = (0..ELEMENTS)
        .map(|_| rng.random_range(-1.0..1.0))
        .collect::<Vec<f64>>();
    let nulls = (null_rows > 0.0).then(|| {
        let mut valid = vec![true; ELEMENTS];
        for row in 0..rows {
            if rng.random_bool(null_rows) {
                valid[row * width + rng.random_range(0..width)] = false;
            }
        }
        NullBuffer::from(valid)
    });
    Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Float64, true)),
        OffsetBuffer::from_repeated_length(width, rows),
        Arc::new(Float64Array::new(values.into(), nulls)),
        None,
    ))
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
