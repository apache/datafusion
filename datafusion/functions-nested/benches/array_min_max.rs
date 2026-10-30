// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Int64Type};
use arrow::util::bench_util::create_primitive_list_array_with_seed;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::min_max::ArrayMax;

const NUM_ROWS: usize = 8192;
const SEED: u64 = 42;
const LIST_NULL_DENSITY: f64 = 0.1;
const ELEMENT_NULL_DENSITY: f64 = 0.1;

fn criterion_benchmark(c: &mut Criterion) {
    let udf = ArrayMax::new();
    let config_options = Arc::new(ConfigOptions::default());

    for list_size in [10, 100, 1000] {
        for (label, null_density) in [("nulls", ELEMENT_NULL_DENSITY), ("no_nulls", 0.0)]
        {
            let list_array: ArrayRef =
                Arc::new(create_primitive_list_array_with_seed::<i32, Int64Type>(
                    NUM_ROWS,
                    LIST_NULL_DENSITY as f32,
                    null_density as f32,
                    list_size,
                    SEED,
                ));
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
