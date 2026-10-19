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
    Array, ArrayRef, GenericListArray, Int32Array, MapArray, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions_nested::cardinality::cardinality_udf;
use std::hint::black_box;
use std::sync::Arc;

fn list_array<O: arrow::array::OffsetSizeTrait>(
    values: ArrayRef,
    rows: usize,
    width: usize,
    nulls: Option<NullBuffer>,
) -> ArrayRef {
    Arc::new(GenericListArray::<O>::new(
        Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows)),
        values,
        nulls,
    ))
}

fn bench_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("cardinality");
    let udf = cardinality_udf();
    let return_field = Arc::new(Field::new("cardinality", DataType::UInt64, true));
    let config_options = Arc::new(ConfigOptions::default());

    let rows = 8192;
    let width = 32;
    let values = Arc::new(Int32Array::from_iter_values(
        (0..rows * width).map(|i| i as i32),
    )) as ArrayRef;
    let flat = list_array::<i32>(Arc::clone(&values), rows, width, None);
    let large = list_array::<i64>(
        Arc::clone(&values),
        rows,
        width,
        Some(NullBuffer::from(
            (0..rows).map(|row| row % 5 != 0).collect::<Vec<_>>(),
        )),
    );
    let entries = StructArray::from(vec![
        (
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::clone(&values),
        ),
        (
            Arc::new(Field::new("value", DataType::Int32, true)),
            Arc::clone(&values),
        ),
    ]);
    let map = Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows)),
        entries,
        None,
        false,
    )) as ArrayRef;
    // Nested lists exercise recursive cardinality: four lists of eight elements.
    let children = list_array::<i32>(values, rows * 4, 8, None);
    let nested = list_array::<i32>(children, rows, 4, None);

    for (name, array) in [
        ("list/valid", Arc::clone(&flat)),
        ("large_list/nullable", large),
        ("map/valid", map),
        ("list/valid", flat.slice(0, 1)),
        ("nested_list/valid", nested),
    ] {
        let number_rows = array.len();
        let id = BenchmarkId::new(name, format!("{number_rows}x{width}"));
        let arg_fields = vec![Arc::new(Field::new(
            "array",
            array.data_type().clone(),
            true,
        ))];
        let input = ColumnarValue::Array(array);
        group.bench_function(id, |b| {
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: vec![input.clone()],
                        arg_fields: arg_fields.clone(),
                        number_rows,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
                )
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_cardinality);
criterion_main!(benches);
