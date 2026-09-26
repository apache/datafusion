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
    ListArray, MapArray, PrimitiveRunBuilder, StringArray, StringViewArray, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Int32Type};
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

    // Vary null density for the common element types. Seeded random patterns
    // avoid the predictable branches and copy spans of periodic null patterns.
    // The four densities are 1%, 25%, 50%, and 95%, respectively.
    for kind in ["primitive", "utf8", "utf8_view"] {
        for pattern in ["random_sparse", "random", "random_half", "random_dense"] {
            bench_input(
                &mut group,
                &format!("random/{kind}/{pattern}"),
                fixed_width_input(kind, pattern),
            );
        }
    }

    // At 25% nulls, contrast periodic and clustered patterns with random nulls.
    // The periodic primitive case is already covered by mixed_nulls above.
    bench_input(
        &mut group,
        "types/utf8/mixed",
        fixed_width_input("utf8", "mixed"),
    );
    for (kind, name) in [
        ("primitive", "nulls/clustered"),
        ("utf8", "nulls/utf8/clustered"),
    ] {
        bench_input(&mut group, name, fixed_width_input(kind, "clustered"));
    }

    // Representative less common types: sparse nulls expose the cost of copying
    // many surviving nested values. Avoid a full type-by-null-density matrix.
    for (kind, pattern) in [
        ("boolean", "mixed"),
        ("decimal128", "mixed"),
        ("nested", "sparse"),
        ("struct_nested", "sparse"),
        ("fixed_nested", "sparse"),
        ("map", "sparse"),
        ("run", "mixed"),
    ] {
        bench_input(
            &mut group,
            &format!("types/{kind}/{pattern}"),
            fixed_width_input(kind, pattern),
        );
    }

    // Compare list lengths of 0–32 with lengths of 15–16 at both 25% and 1%
    // random nulls. Each pair has 256 lists and the same 4,044 element slots;
    // list_input's fixed seed gives the pair identical values and null bits.
    let variable_lengths = (0..ROWS).map(|i| (i * 17) % 33).collect::<Vec<_>>();
    let total = variable_lengths.iter().sum::<usize>();
    let near_uniform_lengths = (0..ROWS)
        .map(|i| total / ROWS + usize::from(i < total % ROWS))
        .collect::<Vec<_>>();
    for (pattern, name) in [("random", "utf8"), ("random_sparse", "utf8_sparse")] {
        for (shape, lengths) in [
            ("variable", &variable_lengths),
            ("near_uniform", &near_uniform_lengths),
        ] {
            bench_input(
                &mut group,
                &format!("shape/{shape}/{name}"),
                list_input("utf8", lengths.clone(), pattern, None, 0),
            );
        }
    }
    for kind in ["utf8_short", "utf8_long"] {
        bench_input(
            &mut group,
            &format!("strings/{kind}"),
            fixed_width_input(kind, "mixed"),
        );
    }
    for (kind, pattern, name) in [
        ("primitive", "mixed", "shape/width_128"),
        ("utf8", "sparse", "shape/utf8/width_128"),
    ] {
        bench_input(
            &mut group,
            name,
            list_input(kind, vec![128; ROWS * WIDTH / 128], pattern, None, 0),
        );
    }
    bench_input(
        &mut group,
        "shape/8192_rows",
        list_input("primitive", vec![WIDTH; 8192], "mixed", None, 0),
    );

    // Null list rows still retain 256 element slots each. "some" makes 25% of
    // lists null; "mostly" leaves only one list in every 100 valid.
    for (kind, name, stride, mostly_null) in [
        ("primitive", "mostly", 100, true),
        ("utf8", "some", 4, false),
        ("utf8", "mostly", 100, true),
        ("nested", "mostly", 100, true),
    ] {
        let nulls =
            NullBuffer::from_iter((0..ROWS).map(|i| (i % stride == 0) == mostly_null));
        bench_input(
            &mut group,
            &format!("parent_nulls/{kind}/{name}"),
            list_input(kind, vec![256; ROWS], "mixed", Some(nulls), 0),
        );
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
    for (kind, name) in [("primitive", "nulls/all"), ("utf8", "all_null/utf8")] {
        bench_input(&mut group, name, fixed_width_input(kind, "all"));
    }

    bench_input(&mut group, "large_list/utf8", {
        let input = fixed_width_input("utf8", "mixed");
        let DataType::List(field) = input.data_type() else {
            unreachable!()
        };
        cast(&input, &DataType::LargeList(Arc::clone(field))).unwrap()
    });
    // The element bitmap starts three bits into its backing buffer.
    bench_input(
        &mut group,
        "child_slice/utf8",
        list_input("utf8", vec![WIDTH; ROWS], "mixed", None, 3),
    );
    // Unlike the backing-size controls above, these large inputs expose
    // 1,048,576 elements. Random nulls make primitive branches unpredictable.
    for (kind, pattern, name) in [
        ("primitive", "random_tenth", "large/int32/random_tenth"),
        ("primitive", "random_half", "large/int32/random_half"),
        ("utf8", "random_tenth", "large/utf8/random_tenth"),
        ("utf8", "random_half", "large/utf8/random_half"),
        ("utf8_long", "random_half", "large/utf8_long/random_half"),
        ("decimal256", "random_half", "large/decimal256/random_half"),
        (
            "decimal256",
            "random_dense",
            "large/decimal256/random_dense",
        ),
        (
            "decimal256",
            "random_dense",
            "small/decimal256/random_dense",
        ),
        ("float64", "random_half", "large/float64/random_half"),
        ("binary", "random_sparse", "binary/random_sparse"),
        ("binary", "random_half", "binary/random_half"),
        ("large_binary", "random_half", "large_binary/random_half"),
    ] {
        let rows = if name.starts_with("large/") {
            65_536
        } else {
            ROWS
        };
        bench_input(
            &mut group,
            name,
            list_input(kind, vec![16; rows], pattern, None, 0),
        );
    }
    for (name, stride) in [
        ("large/int32/null_rows_half", 2),
        ("large/int32/null_rows_mostly", 100),
    ] {
        let nulls = NullBuffer::from_iter((0..65_536).map(|i| i % stride == 0));
        bench_input(
            &mut group,
            name,
            list_input("primitive", vec![16; 65_536], "random_half", Some(nulls), 0),
        );
    }
    // Store bytes even in NULL element slots to measure allocation tradeoffs.
    for (name, parent_nulls) in [
        ("strings/null_payload", false),
        ("strings/null_payload_null_rows", true),
    ] {
        let strings =
            StringArray::from_iter_values(std::iter::repeat_n("x".repeat(256), 8192));
        let strings = StringArray::new(
            strings.offsets().clone(),
            strings.values().clone(),
            Some(NullBuffer::from_iter((0..8192).map(|i| i % 100 == 0))),
        );
        let input = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
            OffsetBuffer::from_repeated_length(16, 512),
            Arc::new(strings),
            parent_nulls.then(|| NullBuffer::from_iter((0..512).map(|i| i % 100 == 0))),
        );
        bench_input(&mut group, name, Arc::new(input));
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
        "decimal256" => {
            cast(&Int32Array::from(values), &DataType::Decimal256(40, 4)).unwrap()
        }
        "float64" => cast(&Int32Array::from(values), &DataType::Float64).unwrap(),
        "binary" | "large_binary" => {
            let strings = child("utf8", values);
            cast(
                strings.as_ref(),
                &if kind == "binary" {
                    DataType::Binary
                } else {
                    DataType::LargeBinary
                },
            )
            .unwrap()
        }
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
        "struct_nested" => {
            let nulls = Some(NullBuffer::from_iter(values.iter().map(Option::is_some)));
            let inner = child("nested", values);
            Arc::new(StructArray::new(
                vec![Field::new("value", inner.data_type().clone(), true)].into(),
                vec![inner],
                nulls,
            ))
        }
        "fixed_nested" => {
            let nulls = Some(NullBuffer::from_iter(values.iter().map(Option::is_some)));
            let inner = child(
                "nested",
                values.into_iter().flat_map(|v| [v, None, v]).collect(),
            );
            Arc::new(FixedSizeListArray::new(
                Arc::new(Field::new_list_field(inner.data_type().clone(), true)),
                3,
                inner,
                nulls,
            ))
        }
        "nested" => Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            values
                .into_iter()
                .map(|v| v.map(|v| vec![Some(v), None, Some(v + 1)])),
        )),
        "map" => {
            let nulls = Some(NullBuffer::from_iter(values.iter().map(Option::is_some)));
            let rows = values.len();
            let entries = StructArray::new(
                vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new("value", DataType::Int32, true),
                ]
                .into(),
                vec![
                    Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(1, rows))),
                    Arc::new(Int32Array::from(values)),
                ],
                None,
            );
            Arc::new(MapArray::new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::from_repeated_length(1, rows),
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
        "run" => {
            let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
            builder.extend(values);
            Arc::new(builder.finish())
        }
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
                "clustered" => i % 256 < 64,
                "random" => rng.random_bool(0.25),
                "random_tenth" => rng.random_bool(0.10),
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
