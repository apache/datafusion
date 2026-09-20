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
    Array, ArrayRef, BooleanArray, DictionaryArray, FixedSizeListArray, Int32Array,
    ListArray, MapArray, NullArray, PrimitiveRunBuilder, RunArray, StringArray,
    StringViewArray, StructArray, UnionArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Int32Type, UnionFields};
use criterion::{
    BenchmarkGroup, Criterion, SamplingMode, criterion_group, criterion_main,
    measurement::WallTime,
};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::array_compact::ArrayCompact;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::hint::black_box;
use std::sync::Arc;

const ROWS: usize = 256;
const WIDTH: usize = 8;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_compact");
    group.sampling_mode(SamplingMode::Flat);

    // Keep the visible rows fixed while varying the retained child size.
    for kind in ["primitive", "dictionary"] {
        for (pattern, hidden_only) in [("mixed_nulls", false), ("hidden_nulls", true)] {
            for backing_rows in [ROWS, 131_072] {
                bench_input(
                    &mut group,
                    &format!("{kind}/{pattern}/{}", backing_rows * WIDTH),
                    sliced_input(kind, backing_rows, hidden_only),
                );
            }
        }
    }

    // Cover the primitive, gather, and span-copy paths, including nested types
    // whose Arrow kernels can have different costs for mixed and sparse nulls.
    for kind in [
        "int64",
        "decimal128",
        "boolean",
        "utf8",
        "utf8_view",
        "nested",
        "nested_nonnull",
        "map",
        "dictionary_keys",
        "run",
        "struct",
        "struct_nested",
        "fixed",
        "fixed_nested",
        "union_sparse",
        "union_dense",
        "union_nested",
        "dictionary_nested",
        "run_nested",
        "nested_long",
        "map_long",
    ] {
        for pattern in ["mixed", "sparse"] {
            bench_input(
                &mut group,
                &format!("types/{kind}/{pattern}"),
                fixed_width_input(kind, pattern),
            );
        }
    }
    for pattern in [
        "sparse",
        "alternating",
        "dense",
        "clustered",
        "random",
        "all",
    ] {
        bench_input(
            &mut group,
            &format!("nulls/{pattern}"),
            fixed_width_input("primitive", pattern),
        );
    }
    for width in [1, 128, 2048] {
        bench_input(
            &mut group,
            &format!("shape/width_{width}"),
            list_input(
                "primitive",
                vec![width; ROWS * WIDTH / width],
                "mixed",
                None,
                0,
            ),
        );
    }
    for kind in ["primitive", "utf8"] {
        let lengths = (0..ROWS).map(|i| (i * 17) % 33).collect();
        bench_input(
            &mut group,
            &format!("shape/variable/{kind}"),
            list_input(kind, lengths, "random", None, 0),
        );
        bench_input(&mut group, &format!("large_list/{kind}"), {
            let input = fixed_width_input(kind, "mixed");
            let DataType::List(field) = input.data_type() else {
                unreachable!()
            };
            cast(&input, &DataType::LargeList(Arc::clone(field))).unwrap()
        });
        // The child bitmap starts three bits into its backing buffer.
        bench_input(
            &mut group,
            &format!("child_slice/{kind}"),
            list_input(kind, vec![WIDTH; ROWS], "mixed", None, 3),
        );
    }
    bench_input(
        &mut group,
        "shape/8192_rows",
        list_input("primitive", vec![WIDTH; 8192], "mixed", None, 0),
    );
    for kind in ["primitive", "utf8", "dictionary", "nested"] {
        for (name, stride, mostly_null) in [("some", 4, false), ("mostly", 100, true)] {
            let nulls = NullBuffer::from_iter(
                (0..ROWS).map(|i| (i % stride == 0) == mostly_null),
            );
            bench_input(
                &mut group,
                &format!("parent_nulls/{kind}/{name}"),
                list_input(kind, vec![256; ROWS], "mixed", Some(nulls), 0),
            );
        }
    }
    for (name, lengths, nulls) in [
        ("zero_rows", vec![], None),
        ("empty_lists", vec![0; ROWS], None),
        (
            "null_lists",
            vec![WIDTH; ROWS],
            Some(NullBuffer::new_null(ROWS)),
        ),
    ] {
        bench_input(
            &mut group,
            &format!("empty/{name}"),
            list_input("primitive", lengths, "mixed", nulls, 0),
        );
    }
    for kind in ["utf8_short", "utf8_long"] {
        bench_input(
            &mut group,
            &format!("strings/{kind}"),
            fixed_width_input(kind, "mixed"),
        );
    }
    bench_input(&mut group, "types/null", fixed_width_input("null", "all"));
    // Seeded random patterns supplement periodic fixtures: branch predictability
    // and contiguous copy spans depend on the distribution as well as density.
    for kind in [
        "primitive",
        "utf8",
        "utf8_view",
        "nested",
        "map",
        "dictionary",
        "run",
    ] {
        for pattern in ["random_sparse", "random", "random_half", "random_dense"] {
            bench_input(
                &mut group,
                &format!("random/{kind}/{pattern}"),
                fixed_width_input(kind, pattern),
            );
        }
    }
    for kind in ["utf8", "nested", "dictionary", "run"] {
        bench_input(
            &mut group,
            &format!("all_null/{kind}"),
            fixed_width_input(kind, "all"),
        );
    }
    for kind in ["nested", "map", "utf8"] {
        for width in [1, 128] {
            bench_input(
                &mut group,
                &format!("shape/{kind}/width_{width}"),
                list_input(kind, vec![width; ROWS * WIDTH / width], "sparse", None, 0),
            );
        }
    }
    group.finish();
}

fn bench_input(group: &mut BenchmarkGroup<WallTime>, name: &str, input: ArrayRef) {
    let udf = ArrayCompact::new();
    let field = Arc::new(Field::new("array", input.data_type().clone(), true));
    let args = ScalarFunctionArgs {
        number_rows: input.len(),
        args: vec![ColumnarValue::Array(input)],
        arg_fields: vec![Arc::clone(&field)],
        return_field: field,
        config_options: Arc::new(ConfigOptions::default()),
    };
    group.bench_function(name, |b| {
        b.iter(|| black_box(udf.invoke_with_args(args.clone()).unwrap()))
    });
}

fn child(kind: &str, values: Vec<Option<i32>>) -> ArrayRef {
    match kind {
        "primitive" => Arc::new(Int32Array::from(values)),
        "int64" => cast(&Int32Array::from(values), &DataType::Int64).unwrap(),
        "decimal128" => {
            cast(&Int32Array::from(values), &DataType::Decimal128(20, 4)).unwrap()
        }
        "boolean" => Arc::new(BooleanArray::from_iter(
            values.into_iter().map(|v| v.map(|v| v % 2 == 0)),
        )),
        "utf8" | "utf8_short" | "utf8_long" | "utf8_view" => {
            let len = match kind {
                "utf8_short" => 8,
                "utf8_long" => 256,
                _ => 32,
            };
            let text = "x".repeat(len);
            let strings = values.iter().map(|v| v.map(|_| text.as_str()));
            if kind == "utf8_view" {
                Arc::new(StringViewArray::from_iter(strings))
            } else {
                Arc::new(StringArray::from_iter(strings))
            }
        }
        "struct" | "struct_nested" => {
            let nulls = Some(NullBuffer::from_iter(values.iter().map(Option::is_some)));
            let inner = child(
                if kind == "struct" {
                    "primitive"
                } else {
                    "nested"
                },
                values,
            );
            Arc::new(StructArray::new(
                vec![Field::new("value", inner.data_type().clone(), true)].into(),
                vec![inner],
                nulls,
            ))
        }
        "fixed" | "fixed_nested" => {
            let nulls = Some(NullBuffer::from_iter(values.iter().map(Option::is_some)));
            let inner = child(
                if kind == "fixed" {
                    "primitive"
                } else {
                    "nested"
                },
                values.into_iter().flat_map(|v| [v, None, v]).collect(),
            );
            Arc::new(FixedSizeListArray::new(
                Arc::new(Field::new_list_field(inner.data_type().clone(), true)),
                3,
                inner,
                nulls,
            ))
        }
        "union_sparse" | "union_dense" | "union_nested" => {
            let len = values.len();
            let type_ids = (0..len).map(|i| (i % 2) as i8).collect::<Vec<_>>();
            let dense = kind != "union_sparse";
            let children = (0..2)
                .map(|id| {
                    child(
                        if kind == "union_nested" {
                            "nested"
                        } else {
                            "primitive"
                        },
                        values
                            .iter()
                            .enumerate()
                            .filter_map(|(i, v)| (!dense || i % 2 == id).then_some(*v))
                            .collect(),
                    )
                })
                .collect::<Vec<_>>();
            let fields = UnionFields::try_new(
                [0, 1],
                children
                    .iter()
                    .enumerate()
                    .map(|(i, c)| Field::new(i.to_string(), c.data_type().clone(), true)),
            )
            .unwrap();
            let offsets = dense
                .then(|| (0..len).map(|i| (i / 2) as i32).collect::<Vec<_>>().into());
            Arc::new(
                UnionArray::try_new(fields, type_ids.into(), offsets, children).unwrap(),
            )
        }
        "dictionary_nested" => Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from_iter_values(0..values.len() as i32),
                child("nested", values),
            )
            .unwrap(),
        ),
        "run_nested" => Arc::new(
            RunArray::<Int32Type>::try_new(
                &Int32Array::from_iter_values(1..=values.len() as i32),
                child("nested", values).as_ref(),
            )
            .unwrap(),
        ),
        "nested_long" => Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            values.into_iter().map(|v| {
                v.map(|v| {
                    (0..128)
                        .map(|i| if i % 4 == 0 { None } else { Some(v) })
                        .collect::<Vec<_>>()
                })
            }),
        )),
        "nested" | "nested_nonnull" => {
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                values.into_iter().map(|v| {
                    v.map(|v| {
                        vec![
                            Some(v),
                            if kind == "nested" { None } else { Some(v + 2) },
                            Some(v + 1),
                        ]
                    })
                }),
            ))
        }
        "map" | "map_long" => {
            let nulls = Some(NullBuffer::from_iter(values.iter().map(Option::is_some)));
            let rows = values.len();
            let width = if kind == "map_long" { 128 } else { 1 };
            let len = rows * width;
            let entries = StructArray::new(
                vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new("value", DataType::Int32, true),
                ]
                .into(),
                vec![
                    Arc::new(Int32Array::from_iter_values(
                        (0..len).map(|i| (i % width + 1) as i32),
                    )),
                    Arc::new(Int32Array::from_iter(
                        values
                            .into_iter()
                            .flat_map(|v| std::iter::repeat_n(v, width)),
                    )),
                ],
                None,
            );
            Arc::new(MapArray::new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::from_repeated_length(width, rows),
                entries,
                nulls,
                true,
            ))
        }
        "dictionary" => Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from_iter_values(
                    values.iter().map(|v| i32::from(v.is_some())),
                ),
                Arc::new(Int32Array::from(vec![None, Some(42)])),
            )
            .unwrap(),
        ),
        "dictionary_keys" => Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from_iter(values.iter().map(|v| v.map(|_| 0))),
                Arc::new(Int32Array::from(vec![42])),
            )
            .unwrap(),
        ),
        "run" => {
            let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
            builder.extend(values);
            Arc::new(builder.finish())
        }
        "null" => Arc::new(NullArray::new(values.len())),
        _ => unreachable!("unknown child type {kind}"),
    }
}

// `ROWS` lists of `WIDTH` elements, with no null lists and an unsliced child.
fn fixed_width_input(kind: &str, pattern: &str) -> ArrayRef {
    list_input(kind, vec![WIDTH; ROWS], pattern, None, 0)
}

fn list_input(
    kind: &str,
    lengths: Vec<usize>,
    pattern: &str,
    nulls: Option<NullBuffer>,
    child_offset: usize,
) -> ArrayRef {
    let len = lengths.iter().sum::<usize>();
    let mut rng = StdRng::seed_from_u64(42);
    let values = (0..len + child_offset)
        .map(|i| {
            let is_null = match pattern {
                "mixed" => i % 4 == 0,
                "sparse" => i % 100 == 0,
                "alternating" => i % 2 == 0,
                "dense" => i % 20 != 0,
                "clustered" => i % 256 < 64,
                "random" => rng.random_bool(0.25),
                "random_sparse" => rng.random_bool(0.01),
                "random_half" => rng.random_bool(0.5),
                "random_dense" => rng.random_bool(0.95),
                "all" => true,
                _ => unreachable!("unknown null pattern {pattern}"),
            };
            if is_null { None } else { Some(42) }
        })
        .collect();
    let child = child(kind, values).slice(child_offset, len);
    Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(child.data_type().clone(), true)),
        OffsetBuffer::from_lengths(lengths),
        child,
        nulls,
    ))
}

fn sliced_input(kind: &str, backing_rows: usize, hidden_only: bool) -> ArrayRef {
    let first_row = (backing_rows - ROWS) / 2;
    let selected = first_row * WIDTH..(first_row + ROWS) * WIDTH;
    let values = (0..backing_rows * WIDTH)
        .map(|i| {
            let is_null = if hidden_only {
                !selected.contains(&i)
            } else {
                i % 4 == 0
            };
            if is_null { None } else { Some(42) }
        })
        .collect();
    let child = child(kind, values);
    let lists = ListArray::new(
        Arc::new(Field::new_list_field(child.data_type().clone(), true)),
        OffsetBuffer::from_repeated_length(WIDTH, backing_rows),
        child,
        None,
    );
    Arc::new(lists.slice(first_row, ROWS))
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
