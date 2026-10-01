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
    ArrayRef, ArrowPrimitiveType, DictionaryArray, FixedSizeListArray, Int32Array,
    Int64Array, ListArray, ListViewArray, MapArray, NullArray, PrimitiveArray, RunArray,
    StringArray, StringViewArray, StructArray, UnionArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Int64Type, UnionFields};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::hash_utils::{RandomState, create_hashes};
use rand::Rng;
use rand::SeedableRng;
use rand::distr::{Distribution, StandardUniform};
use rand::prelude::StdRng;
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;
const ELEMENTS_PER_ROW: usize = 5;

fn make_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

fn bench_create_hashes(c: &mut Criterion, name: &str, array: &ArrayRef) {
    let state = RandomState::default();
    let mut hashes = vec![0u64; BATCH_SIZE];
    c.bench_function(name, |b| {
        b.iter(|| {
            create_hashes(std::slice::from_ref(array), &state, &mut hashes).unwrap();
        })
    });
}

fn primitive_array<T>() -> ArrayRef
where
    T: ArrowPrimitiveType,
    StandardUniform: Distribution<T::Native>,
{
    let mut rng = make_rng();
    Arc::new(
        (0..BATCH_SIZE)
            .map(|_| Some(rng.random::<T::Native>()))
            .collect::<PrimitiveArray<T>>(),
    )
}

fn null_array() -> ArrayRef {
    Arc::new(NullArray::new(BATCH_SIZE))
}

fn utf8_array() -> ArrayRef {
    let mut rng = make_rng();
    Arc::new(StringArray::from_iter_values((0..BATCH_SIZE).map(|_| {
        let len = rng.random_range(1usize..=32);
        (0..len).map(|_| rng.random::<char>()).collect::<String>()
    })))
}

fn string_view_array() -> ArrayRef {
    let mut rng = make_rng();
    Arc::new(StringViewArray::from_iter_values((0..BATCH_SIZE).map(
        |_| {
            let len = rng.random_range(1usize..=32);
            (0..len).map(|_| rng.random::<char>()).collect::<String>()
        },
    )))
}

fn dictionary_array() -> ArrayRef {
    let pool: Vec<String> = (0..100).map(|i| format!("value_{i}")).collect();
    let mut rng = make_rng();
    Arc::new(DictionaryArray::<Int32Type>::from_iter(
        (0..BATCH_SIZE).map(|_| Some(pool[rng.random_range(0..pool.len())].as_str())),
    ))
}

fn struct_array() -> ArrayRef {
    Arc::new(StructArray::new(
        Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int32, false),
        ]),
        vec![
            primitive_array::<Int64Type>(),
            primitive_array::<Int32Type>(),
        ],
        None,
    ))
}

fn list_array() -> ArrayRef {
    let mut rng = make_rng();
    let total = BATCH_SIZE * ELEMENTS_PER_ROW;
    let values: Int64Array = (0..total).map(|_| Some(rng.random::<i64>())).collect();
    let offsets: Vec<i32> = (0..=BATCH_SIZE)
        .map(|i| (i * ELEMENTS_PER_ROW) as i32)
        .collect();
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(values),
        None,
    ))
}

fn list_view_array() -> ArrayRef {
    let mut rng = make_rng();
    let total = BATCH_SIZE * ELEMENTS_PER_ROW;
    let values: Int64Array = (0..total).map(|_| Some(rng.random::<i64>())).collect();
    let offsets: ScalarBuffer<i32> = (0..BATCH_SIZE)
        .map(|i| (i * ELEMENTS_PER_ROW) as i32)
        .collect();
    let sizes: ScalarBuffer<i32> =
        (0..BATCH_SIZE).map(|_| ELEMENTS_PER_ROW as i32).collect();
    Arc::new(ListViewArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        offsets,
        sizes,
        Arc::new(values),
        None,
    ))
}

fn map_array() -> ArrayRef {
    let mut rng = make_rng();
    let total = BATCH_SIZE * ELEMENTS_PER_ROW;
    let keys: Int32Array = (0..total).map(|_| Some(rng.random::<i32>())).collect();
    let vals: Int64Array = (0..total).map(|_| Some(rng.random::<i64>())).collect();
    let offsets: Vec<i32> = (0..=BATCH_SIZE)
        .map(|i| (i * ELEMENTS_PER_ROW) as i32)
        .collect();
    let entries = StructArray::try_new(
        Fields::from(vec![
            Field::new("keys", DataType::Int32, false),
            Field::new("values", DataType::Int64, true),
        ]),
        vec![Arc::new(keys), Arc::new(vals)],
        None,
    )
    .unwrap();
    Arc::new(MapArray::new(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Int32, false),
                Field::new("values", DataType::Int64, true),
            ])),
            false,
        )),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        entries,
        None,
        false,
    ))
}

fn fixed_size_list_array() -> ArrayRef {
    let list_size = 4i32;
    Arc::new(FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        list_size,
        primitive_array::<Int64Type>(),
        None,
    ))
}

fn union_array() -> ArrayRef {
    let mut rng = make_rng();
    let num_types: i8 = 3;
    let type_ids: Vec<i8> = (0..BATCH_SIZE)
        .map(|_| rng.random_range(0..num_types))
        .collect();
    let (fields, children): (Vec<_>, Vec<_>) = (0..num_types)
        .map(|i| {
            (
                (
                    i,
                    Arc::new(Field::new(format!("f{i}"), DataType::Int64, true)),
                ),
                primitive_array::<Int64Type>(),
            )
        })
        .unzip();
    Arc::new(
        UnionArray::try_new(
            UnionFields::from_iter(fields),
            ScalarBuffer::from(type_ids),
            None,
            children,
        )
        .unwrap(),
    )
}

fn run_array() -> ArrayRef {
    let mut rng = make_rng();
    let mut run_ends = Vec::new();
    let mut values = Vec::new();
    let mut pos = 0;
    while pos < BATCH_SIZE {
        let run_len = rng.random_range(1..=50).min(BATCH_SIZE - pos);
        pos += run_len;
        run_ends.push(pos as i32);
        values.push(Some(rng.random::<i64>()));
    }
    Arc::new(
        RunArray::try_new(
            &Int32Array::from(run_ends),
            &values.into_iter().collect::<Int64Array>(),
        )
        .unwrap(),
    )
}

fn bench_create_hashes_multi(c: &mut Criterion, name: &str, arrays: &[ArrayRef]) {
    let state = RandomState::default();
    let mut hashes = vec![0u64; BATCH_SIZE];
    c.bench_function(name, |b| {
        b.iter(|| {
            create_hashes(arrays, &state, &mut hashes).unwrap();
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_create_hashes(c, "create_hashes: null", &null_array());
    bench_create_hashes(c, "create_hashes: int64", &primitive_array::<Int64Type>());
    bench_create_hashes(c, "create_hashes: utf8", &utf8_array());
    bench_create_hashes(c, "create_hashes: utf8_view", &string_view_array());
    bench_create_hashes(c, "create_hashes: dictionary", &dictionary_array());
    bench_create_hashes(c, "create_hashes: struct", &struct_array());
    bench_create_hashes(c, "create_hashes: list", &list_array());
    bench_create_hashes(c, "create_hashes: list_view", &list_view_array());
    bench_create_hashes(c, "create_hashes: map", &map_array());
    bench_create_hashes(
        c,
        "create_hashes: fixed_size_list",
        &fixed_size_list_array(),
    );
    bench_create_hashes(c, "create_hashes: union", &union_array());
    bench_create_hashes(c, "create_hashes: run_end_encoded", &run_array());

    let int64 = primitive_array::<Int64Type>();
    let utf8 = utf8_array();
    let utf8_view = string_view_array();
    let dict = dictionary_array();
    bench_create_hashes_multi(
        c,
        "create_hashes: 2 columns int64",
        &[int64.clone(), int64.clone()],
    );
    bench_create_hashes_multi(
        c,
        "create_hashes: 3 columns mixed",
        &[int64.clone(), utf8.clone(), utf8_view.clone()],
    );
    bench_create_hashes_multi(
        c,
        "create_hashes: 5 columns mixed",
        &[
            int64.clone(),
            utf8.clone(),
            utf8_view.clone(),
            dict.clone(),
            int64.clone(),
        ],
    );
    bench_create_hashes_multi(
        c,
        "create_hashes: 7 columns mixed",
        &[
            int64.clone(),
            utf8.clone(),
            utf8_view.clone(),
            dict.clone(),
            int64.clone(),
            utf8.clone(),
            dict,
        ],
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
