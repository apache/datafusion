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

use arrow::array::{Array, ArrayRef, Int32Array, MapArray, StringViewArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, FieldRef};
use criterion::{Bencher, BenchmarkId, Criterion, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::core::get_field;
use std::hint::black_box;
use std::sync::Arc;

const ROWS: usize = 1024;

/// Map key types covered by the benchmarks. Struct keys are nested, so their
/// lookups can never switch from the comparator to the vectorized `eq`.
#[derive(Clone, Copy)]
enum KeyType {
    Int32,
    Utf8View,
    Struct,
}

impl KeyType {
    fn name(self) -> &'static str {
        match self {
            KeyType::Int32 => "int32",
            KeyType::Utf8View => "utf8_view",
            KeyType::Struct => "struct",
        }
    }

    /// Builds a key array holding one key per element of `keys`.
    fn make_keys(self, keys: &[i32]) -> ArrayRef {
        match self {
            KeyType::Int32 => Arc::new(Int32Array::from(keys.to_vec())),
            KeyType::Utf8View => Arc::new(StringViewArray::from_iter_values(
                keys.iter().map(|key| format!("key_{key:016}")),
            )),
            KeyType::Struct => Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("a", DataType::Int32, false)),
                Arc::new(Int32Array::from(keys.to_vec())) as ArrayRef,
            )])),
        }
    }
}

/// A map array with `size` rows, each holding `entries` key/value pairs whose
/// keys are `0..entries`. Every tenth row is null and has no entries. With
/// `shuffled`, each row's entries are rotated by the row number, so a given
/// key sits at a different position in every row.
fn map_array(key_type: KeyType, size: usize, entries: usize, shuffled: bool) -> ArrayRef {
    let mut keys = Vec::with_capacity(size * entries);
    let mut values = Vec::with_capacity(size * entries);
    let mut lengths = Vec::with_capacity(size);
    let mut valid = Vec::with_capacity(size);
    for row in 0..size {
        let is_null = row % 10 == 0;
        valid.push(!is_null);
        if is_null {
            lengths.push(0);
            continue;
        }
        lengths.push(entries);
        for position in 0..entries {
            let key = if shuffled {
                (position + row) % entries
            } else {
                position
            };
            keys.push(key as i32);
            values.push((row * position) as i32);
        }
    }
    let keys = key_type.make_keys(&keys);
    let entries = StructArray::from(vec![
        (
            Arc::new(Field::new("keys", keys.data_type().clone(), false)),
            keys,
        ),
        (
            Arc::new(Field::new("values", DataType::Int32, true)),
            Arc::new(Int32Array::from(values)) as ArrayRef,
        ),
    ]);
    Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::from_lengths(lengths),
        entries,
        Some(NullBuffer::from(valid)),
        false,
    ))
}

/// Runs `get_field(map, key)` over a map built by [`map_array`]. `lookup`
/// picks the key: the first or last entry of every row, a key that every row
/// holds at a different position (`shuffled`), or one present in no row.
fn bench_get_field(b: &mut Bencher<'_>, key_type: KeyType, entries: usize, lookup: &str) {
    let (key, shuffled) = match lookup {
        "first" => (0, false),
        "last" => (entries as i32 - 1, false),
        "shuffled" => (0, true),
        "missing" => (entries as i32, false),
        _ => unreachable!(),
    };
    let map = map_array(key_type, ROWS, entries, shuffled);
    let lookup = ScalarValue::try_from_array(&key_type.make_keys(&[key]), 0)
        .expect("lookup key should convert to a scalar");
    let arg_fields: Vec<FieldRef> = vec![
        Field::new("map", map.data_type().clone(), true).into(),
        Field::new("key", lookup.data_type(), false).into(),
    ];
    let args = vec![ColumnarValue::Array(map), ColumnarValue::Scalar(lookup)];
    let udf = get_field();
    let config_options = Arc::new(ConfigOptions::default());
    let return_field: FieldRef = Field::new("f", DataType::Int32, true).into();
    b.iter(|| {
        black_box(
            udf.invoke_with_args(ScalarFunctionArgs {
                args: args.clone(),
                arg_fields: arg_fields.clone(),
                number_rows: ROWS,
                return_field: Arc::clone(&return_field),
                config_options: Arc::clone(&config_options),
            })
            .unwrap(),
        )
    })
}

fn criterion_benchmark(c: &mut Criterion) {
    // Cases are named `{key type}/{lookup}/{rows}x{entries}`.
    let mut group = c.benchmark_group("get_field_map");
    let shapes: &[(usize, &[&str])] = &[
        (4, &["last", "shuffled", "missing"]),
        (32, &["first", "last", "shuffled", "missing"]),
    ];
    for key_type in [KeyType::Int32, KeyType::Utf8View, KeyType::Struct] {
        for &(entries, lookups) in shapes {
            for &lookup in lookups {
                group.bench_function(
                    BenchmarkId::new(
                        format!("{}/{lookup}", key_type.name()),
                        format!("{ROWS}x{entries}"),
                    ),
                    |b| bench_get_field(b, key_type, entries, lookup),
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
