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
    Int64Array, ListArray, ListViewArray, NullBufferBuilder, PrimitiveArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Int64Type};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions_nested::extract::array_slice_udf;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

fn create_inputs(
    rng: &mut StdRng,
    size: usize,
    child_array_size: usize,
    null_density: f32,
) -> (ListArray, ListViewArray) {
    let mut nulls_builder = NullBufferBuilder::new(size);
    let mut sizes = Vec::with_capacity(size);

    for _ in 0..size {
        if rng.random::<f32>() < null_density {
            nulls_builder.append_null();
        } else {
            nulls_builder.append_non_null();
        }
        sizes.push(rng.random_range(1..child_array_size));
    }
    let nulls = nulls_builder.finish();

    let length = sizes.iter().sum();
    let values: PrimitiveArray<Int64Type> =
        (0..length).map(|_| Some(rng.random())).collect();
    let values = Arc::new(values);

    let offsets = OffsetBuffer::from_lengths(sizes.clone());
    let list_array = ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int64, true)),
        offsets.clone(),
        values.clone(),
        nulls.clone(),
    );

    let offsets = ScalarBuffer::from(offsets.slice(0, size - 1));
    let sizes = ScalarBuffer::from_iter(sizes.into_iter().map(|v| v as i32));
    let list_view_array = ListViewArray::new(
        Arc::new(Field::new_list_field(DataType::Int64, true)),
        offsets,
        sizes,
        values,
        nulls,
    );

    (list_array, list_view_array)
}

/// Create `from`, `to`, and `stride` from an array of strides.
fn random_from_to_stride(
    rng: &mut StdRng,
    size: i64,
    null_density: f32,
    stride_choices: &[Option<i64>],
) -> (Option<i64>, Option<i64>, Option<i64>) {
    let from = if rng.random::<f32>() < null_density {
        None
    } else {
        Some(rng.random_range(1..=size))
    };

    let to = if rng.random::<f32>() < null_density {
        None
    } else {
        match from {
            Some(from) => Some(rng.random_range(from..=size)),
            None => Some(rng.random_range(1..=size)),
        }
    };

    let stride = stride_choices.choose(rng).cloned().unwrap_or(None);

    if from.is_none() || to.is_none() || stride.is_none_or(|s| s > 0) {
        (from, to, stride)
    } else {
        // stride < 0, swap from and to
        (to, from, stride)
    }
}

fn array_slice_benchmark(
    name: &str,
    input: ColumnarValue,
    mut args: Vec<ColumnarValue>,
    c: &mut Criterion,
    size: usize,
) {
    args.insert(0, input);

    let array_slice = array_slice_udf();
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| {
            <Arc<Field>>::from(Field::new(format!("arg_{idx}"), arg.data_type(), true))
        })
        .collect::<Vec<_>>();
    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                array_slice
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new_list_field(args[0].data_type(), true)
                            .into(),
                        config_options: Arc::new(ConfigOptions::default()),
                    })
                    .unwrap(),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let rng = &mut StdRng::seed_from_u64(42);
    let size = 1_000_000;
    let child_array_size = 100;
    let null_density = 0.1;

    let (list_array, list_view_array) =
        create_inputs(rng, size, child_array_size, null_density);

    let mut array_from = Vec::with_capacity(size);
    let mut array_to = Vec::with_capacity(size);
    let mut array_stride = Vec::with_capacity(size);
    for child_array_size in list_array.offsets().lengths() {
        let (from, to, stride) = random_from_to_stride(
            rng,
            child_array_size as i64,
            null_density,
            &[None, Some(-2), Some(-1), Some(1), Some(2)],
        );
        array_from.push(from);
        array_to.push(to);
        array_stride.push(stride);
    }

    // input
    let list_array = ColumnarValue::Array(Arc::new(list_array));
    let list_view_array = ColumnarValue::Array(Arc::new(list_view_array));

    // args
    let array_from = ColumnarValue::Array(Arc::new(Int64Array::from(array_from)));
    let array_to = ColumnarValue::Array(Arc::new(Int64Array::from(array_to)));
    let array_stride = ColumnarValue::Array(Arc::new(Int64Array::from(array_stride)));
    let scalar_from = ColumnarValue::Scalar(ScalarValue::from(1i64));
    let scalar_to = ColumnarValue::Scalar(ScalarValue::from(child_array_size as i64 / 2));

    for input in [list_array, list_view_array] {
        let input_type = input.data_type().to_string();

        array_slice_benchmark(
            &format!("array_slice: input {input_type}, array args"),
            input.clone(),
            vec![array_from.clone(), array_to.clone(), array_stride.clone()],
            c,
            size,
        );

        array_slice_benchmark(
            &format!("array_slice: input {input_type}, array args, no stride"),
            input.clone(),
            vec![array_from.clone(), array_to.clone()],
            c,
            size,
        );

        array_slice_benchmark(
            &format!("array_slice: input {input_type}, scalar args, no stride"),
            input.clone(),
            vec![scalar_from.clone(), scalar_to.clone()],
            c,
            size,
        );

        for stride in [-2i64, -1i64, 1i64, 2i64] {
            // swap from and to if stride < 0
            let (scalar_from, scalar_to) = if stride > 0 {
                (scalar_from.clone(), scalar_to.clone())
            } else {
                (scalar_to.clone(), scalar_from.clone())
            };
            let scalar_stride = ColumnarValue::Scalar(ScalarValue::from(stride));
            array_slice_benchmark(
                &format!("array_slice: input {input_type}, scalar args, stride={stride}"),
                input.clone(),
                vec![scalar_from, scalar_to, scalar_stride],
                c,
                size,
            );
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
