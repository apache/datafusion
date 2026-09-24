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

use std::{hint::black_box, sync::Arc};

use arrow::array::{
    Array, ArrayRef, Int32Array, ListArray, RecordBatch, StringArray, StringViewArray,
    UInt64Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::{cast, take};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{
    Expr, col, execution_props::ExecutionProps, expr::LambdaVariable, lambda, lit,
    physical_planning_context::PhysicalPlanningContext,
};
use datafusion_functions_nested::expr_fn::array_filter;
use datafusion_physical_expr::create_physical_expr;
use rand::{Rng, SeedableRng, rngs::StdRng};

// Inputs are built by hand rather than from SQL literals so the bench can cover
// sliced arrays and elements stored under NULL list rows.
// Defaults: 256 rows of 16 elements, seeded random values, no NULL list rows.
struct Case {
    kind: DataType,
    rows: usize,
    width: usize,
    element_nulls: f64,
    row_nulls: f64,
    // NULL list rows keep their elements, which evaluation must strip.
    hidden_storage: bool,
    predicate: Predicate,
    variable: bool,
    sliced: bool,
    large_list: bool,
}

#[derive(Clone, Copy)]
enum Predicate {
    LessThan(i32),
    Scalar(Option<bool>),
    IsNull,
    Captured,
}

impl Default for Case {
    fn default() -> Self {
        Self {
            kind: DataType::Int32,
            rows: 256,
            width: 16,
            element_nulls: 0.0,
            row_nulls: 0.0,
            hidden_storage: false,
            predicate: Predicate::LessThan(50),
            variable: false,
            sliced: false,
            large_list: false,
        }
    }
}

fn string_value(value: i32) -> String {
    format!("{value:03}-array-filter-value")
}

// Evaluate a planned projection, including lambda evaluation, on an in-memory
// batch. Constructing inputs and planning are outside the timed loop.
fn bench_case(c: &mut Criterion, name: &str, case: &Case) {
    let mut rng = StdRng::seed_from_u64(42);
    let row_valid = (0..case.rows)
        .map(|_| !rng.random_bool(case.row_nulls))
        .collect::<Vec<_>>();
    let lengths = (0..case.rows)
        .map(|row| {
            if !row_valid[row] && !case.hidden_storage {
                0
            } else if case.variable {
                (row * 17) % (2 * case.width + 1)
            } else {
                case.width
            }
        })
        .collect::<Vec<_>>();
    let values = (0..lengths.iter().sum())
        .map(|_| {
            let value = rng.random_range(0..100);
            (!rng.random_bool(case.element_nulls)).then_some(value)
        })
        .collect::<Vec<_>>();
    let elements: ArrayRef = match case.kind {
        DataType::Int32 => Arc::new(Int32Array::from(values.clone())),
        DataType::Utf8 => Arc::new(StringArray::from_iter(
            values.iter().map(|v| v.map(string_value)),
        )),
        DataType::Utf8View => Arc::new(StringViewArray::from_iter(
            values.iter().map(|v| v.map(string_value)),
        )),
        _ => unreachable!(),
    };
    let row_nulls = (case.row_nulls != 0.0).then(|| NullBuffer::from(row_valid.clone()));
    let field = Arc::new(Field::new_list_field(case.kind.clone(), true));
    let list = ListArray::new(
        Arc::clone(&field),
        OffsetBuffer::from_lengths(lengths),
        Arc::clone(&elements),
        row_nulls,
    );
    let list = if case.sliced {
        list.slice(1, case.rows - 2)
    } else {
        list
    };
    let first_row = usize::from(case.sliced);
    let thresholds = (first_row..first_row + list.len())
        .map(|row| (row % 100) as i32)
        .collect::<Vec<_>>();

    // Independently check logical output before timing, including NULL elements
    // deliberately retained by IS NULL and values hidden by NULL list rows.
    let mut selected = Vec::new();
    let mut result_lengths = Vec::new();
    for (row, offsets) in list.offsets().windows(2).enumerate() {
        let before = selected.len();
        if row_valid[first_row + row] {
            let (start, end) = (offsets[0] as usize, offsets[1] as usize);
            for (index, value) in (start..).zip(&values[start..end]) {
                let keep = match case.predicate {
                    Predicate::LessThan(limit) => value.is_some_and(|v| v < limit),
                    Predicate::Scalar(value) => value == Some(true),
                    Predicate::IsNull => value.is_none(),
                    Predicate::Captured => value.is_some_and(|v| v < thresholds[row]),
                };
                if keep {
                    selected.push(index as u64);
                }
            }
        }
        result_lengths.push(selected.len() - before);
    }
    let expected = ListArray::new(
        Arc::clone(&field),
        OffsetBuffer::<i32>::from_lengths(result_lengths),
        take(elements.as_ref(), &UInt64Array::from(selected), None).unwrap(),
        list.nulls().cloned(),
    );
    let mut input = Arc::new(list) as ArrayRef;
    let mut expected = Arc::new(expected) as ArrayRef;
    if case.large_list {
        let data_type = DataType::LargeList(Arc::clone(&field));
        input = cast(&input, &data_type).unwrap();
        expected = cast(&expected, &data_type).unwrap();
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("list", input.data_type().clone(), true),
        Field::new("threshold", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![input, Arc::new(Int32Array::from(thresholds))],
    )
    .unwrap();
    let v = Expr::LambdaVariable(LambdaVariable::new("v".into(), Some(field)));
    let body = match case.predicate {
        Predicate::LessThan(limit) => {
            let limit = if case.kind == DataType::Int32 {
                lit(limit)
            } else {
                lit(ScalarValue::new_utf8(string_value(limit))
                    .cast_to(&case.kind)
                    .unwrap())
            };
            v.lt(limit)
        }
        Predicate::Scalar(value) => lit(ScalarValue::Boolean(value)),
        Predicate::IsNull => v.is_null(),
        Predicate::Captured => v.lt(col("threshold")),
    };
    let expression = create_physical_expr(
        &array_filter(col("list"), lambda(["v"], body)),
        &DFSchema::try_from(schema).unwrap(),
        &ExecutionProps::new(),
        &PhysicalPlanningContext::default(),
    )
    .unwrap();
    let output = expression
        .evaluate(&batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    assert_eq!(output.as_ref(), expected.as_ref(), "{name}");
    output.to_data().validate_full().unwrap();

    let mut group = c.benchmark_group("array_filter");
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function(name, |b| {
        b.iter(|| black_box(expression.evaluate(black_box(&batch)).unwrap()))
    });
    group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    for (kind, name) in [
        (DataType::Int32, "int32"),
        (DataType::Utf8, "utf8"),
        (DataType::Utf8View, "utf8_view"),
    ] {
        for keep in [10, 50, 90] {
            bench_case(
                c,
                &format!("{name}/keep{keep}/no_nulls"),
                &Case {
                    kind: kind.clone(),
                    predicate: Predicate::LessThan(keep),
                    ..Case::default()
                },
            );
        }
        // 25% NULL elements make comparison results nullable. Half of the
        // remaining elements pass, so approximately 37.5% of all slots survive.
        bench_case(
            c,
            &format!("{name}/keep50/null_elements25"),
            &Case {
                kind: kind.clone(),
                element_nulls: 0.25,
                ..Case::default()
            },
        );
        // IS NULL is cheap to evaluate, so list reconstruction dominates.
        bench_case(
            c,
            &format!("{name}/keep_null_elements"),
            &Case {
                kind: kind.clone(),
                predicate: Predicate::IsNull,
                element_nulls: 0.25,
                ..Case::default()
            },
        );
        for percent in [50, 99] {
            bench_case(
                c,
                &format!("{name}/keep50/null_rows{percent}"),
                &Case {
                    kind: kind.clone(),
                    row_nulls: f64::from(percent) / 100.0,
                    element_nulls: 0.25,
                    ..Case::default()
                },
            );
        }
    }
    for (name, case) in [
        (
            "utf8/wide_128",
            Case {
                kind: DataType::Utf8,
                rows: 32,
                width: 128,
                ..Case::default()
            },
        ),
        (
            "utf8/variable_0_to_32",
            Case {
                kind: DataType::Utf8,
                variable: true,
                ..Case::default()
            },
        ),
        (
            "int32/large_1m/null_elements25",
            Case {
                rows: 65_536,
                element_nulls: 0.25,
                ..Case::default()
            },
        ),
        (
            "scalar/true",
            Case {
                predicate: Predicate::Scalar(Some(true)),
                ..Case::default()
            },
        ),
        (
            "scalar/false",
            Case {
                predicate: Predicate::Scalar(Some(false)),
                ..Case::default()
            },
        ),
        (
            "scalar/null",
            Case {
                predicate: Predicate::Scalar(None),
                ..Case::default()
            },
        ),
        (
            "int32/keep_all",
            Case {
                predicate: Predicate::LessThan(100),
                ..Case::default()
            },
        ),
        (
            "int32/keep_none",
            Case {
                predicate: Predicate::LessThan(0),
                ..Case::default()
            },
        ),
        (
            "int32/captured_column",
            Case {
                predicate: Predicate::Captured,
                ..Case::default()
            },
        ),
        (
            "int32/keep50/null_rows50/hidden_storage",
            Case {
                row_nulls: 0.5,
                hidden_storage: true,
                element_nulls: 0.25,
                ..Case::default()
            },
        ),
        (
            "utf8/sliced",
            Case {
                kind: DataType::Utf8,
                sliced: true,
                element_nulls: 0.25,
                ..Case::default()
            },
        ),
        (
            "utf8/large_list",
            Case {
                kind: DataType::Utf8,
                large_list: true,
                element_nulls: 0.25,
                ..Case::default()
            },
        ),
    ] {
        bench_case(c, name, &case);
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
