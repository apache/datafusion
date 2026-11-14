// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Scalar functions over dictionary-encoded columns, measured at two layers.
//!
//! `string/*` calls the function directly, so it measures whatever
//! dictionary handling the function itself has: `reverse` and the trim family
//! peel the dictionary by hand, others see it materialized. These groups
//! moved here from `datafusion-functions` (#23930) unchanged, so their
//! numbers stay comparable, and now sit beside the expression-layer groups
//! they are the baseline for.
//!
//! `expression/*` evaluates through [`ScalarFunctionExpr`], the layer that
//! decides what the function receives. `cold` gives every batch its own
//! dictionary, as a projection building one per batch does; `warm` shares one
//! across batches, as a Parquet scan does within a column chunk; `flat` is the
//! same rows with the encoding materialized; `cast_away` is what a function
//! without encoding preservation pays, since coercion casts the dictionary
//! away before the call.
//!
//! `expression/decode` derives its physical casts from the function's
//! coercion result. Its dictionary cases therefore measure materialization on
//! main and can measure dictionary preservation later without changing the
//! benchmark inputs or expression construction.

use std::cell::Cell;
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, DictionaryArray, Int32Array, StringArray,
};
use arrow::compute::{cast, take};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

const ROWS: usize = 8192;

/// Distinct values per batch. 10, 100, 1000 and 8192 are the grid #23930 used,
/// kept so its numbers remain comparable; 256 is added because that is where
/// real dictionaries sit — a Parquet page for a categorical column typically
/// holds a few hundred values.
const CARDINALITIES: [usize; 5] = [10, 100, 256, 1000, 8192];

#[derive(Clone, Copy)]
enum InputValues {
    String,
    Binary,
    HexBinary,
}

/// The distinct values a batch draws from, in the type the function receives
/// once coercion has run: `encode` takes binary, the string functions take
/// strings. The textual form is the one #23930 used, so its numbers stay
/// comparable.
fn values_of(distinct: usize, input: InputValues) -> ArrayRef {
    let values: Vec<String> = match input {
        InputValues::String | InputValues::Binary => {
            (0..distinct).map(|i| format!("value_{i:04}")).collect()
        }
        // Every value is 32 hex digits and decodes to 16 bytes. Keep the
        // inputs valid so this benchmark measures decoding rather than the
        // error path.
        InputValues::HexBinary => (0..distinct).map(|i| format!("{i:032x}")).collect(),
    };
    match input {
        InputValues::String => Arc::new(StringArray::from(values)),
        InputValues::Binary | InputValues::HexBinary => Arc::new(BinaryArray::from(
            values.iter().map(|v| v.as_bytes()).collect::<Vec<_>>(),
        )),
    }
}

/// `ROWS` rows cycling through `cardinality` distinct values.
fn create_string_dictionary(cardinality: usize) -> ArrayRef {
    let keys = Int32Array::from(
        (0..ROWS)
            .map(|index| (index % cardinality) as i32)
            .collect::<Vec<_>>(),
    );
    Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            keys,
            values_of(cardinality, InputValues::String),
        )
        .expect("dictionary array"),
    )
}

/// The function's own dictionary handling: the call receives the dictionary
/// exactly as coercion would deliver it.
fn benchmark_function_layer(c: &mut Criterion) {
    let udfs = [
        ("ascii", datafusion_functions::string::ascii()),
        ("bit_length", datafusion_functions::string::bit_length()),
        ("btrim", datafusion_functions::string::btrim()),
        (
            "character_length",
            datafusion_functions::unicode::character_length(),
        ),
        ("initcap", datafusion_functions::unicode::initcap()),
        ("ltrim", datafusion_functions::string::ltrim()),
        ("octet_length", datafusion_functions::string::octet_length()),
        ("reverse", datafusion_functions::unicode::reverse()),
        ("rtrim", datafusion_functions::string::rtrim()),
    ];
    let config_options = Arc::new(ConfigOptions::default());

    for cardinality in CARDINALITIES {
        let dictionary = create_string_dictionary(cardinality);
        let mut group = c.benchmark_group(format!(
            "dictionary_encoding/string/cardinality_{cardinality}"
        ));
        for (name, udf) in &udfs {
            let input_field =
                Field::new("a", dictionary.data_type().clone(), false).into();
            let coerced_field = fields_with_udf(&[input_field], udf.as_ref())
                .unwrap()
                .into_iter()
                .next()
                .unwrap();
            let coerced_type = coerced_field.data_type();
            let return_type =
                udf.return_type(std::slice::from_ref(coerced_type)).unwrap();
            let return_field = Field::new("f", return_type, false).into();
            let input = if dictionary.data_type() == coerced_type {
                Arc::clone(&dictionary)
            } else {
                cast(dictionary.as_ref(), coerced_type).unwrap()
            };

            group.bench_function(*name, |b| {
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: vec![ColumnarValue::Array(Arc::clone(&input))],
                            arg_fields: vec![Arc::clone(&coerced_field)],
                            number_rows: ROWS,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                    )
                })
            });
        }
        group.finish();
    }
}

/// `ROWS` rows drawn from `distinct` values, dictionary-encoded. `shift` moves
/// which value each row lands on, so batches built separately differ in their
/// keys as well as in the memory their values occupy.
fn dictionary_batch(
    distinct: usize,
    shift: usize,
    input: InputValues,
) -> (Schema, RecordBatch) {
    dictionary_batch_with_backing(distinct, distinct, shift, input)
}

/// `ROWS` rows referencing `distinct` values from a values array of
/// `backing_values` entries. This models filtered and sliced dictionaries that
/// retain unreferenced values.
fn dictionary_batch_with_backing(
    distinct: usize,
    backing_values: usize,
    shift: usize,
    input: InputValues,
) -> (Schema, RecordBatch) {
    assert!(distinct <= backing_values);
    let keys = Int32Array::from(
        (0..ROWS)
            .map(|i| ((i + shift) % distinct) as i32)
            .collect::<Vec<_>>(),
    );
    let dict =
        DictionaryArray::<Int32Type>::try_new(keys, values_of(backing_values, input))
            .expect("dictionary array");
    let schema = Schema::new(vec![Field::new("c", dict.data_type().clone(), true)]);
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(dict)])
        .expect("batch");
    (schema, batch)
}

/// The same rows without the encoding — what the function receives when the
/// dictionary is materialized before evaluation.
fn flat_batch(distinct: usize, input: InputValues) -> (Schema, RecordBatch) {
    let keys =
        Int32Array::from((0..ROWS).map(|i| (i % distinct) as i32).collect::<Vec<_>>());
    let array = take(values_of(distinct, input).as_ref(), &keys, None)
        .expect("materialized column");
    let data_type = array.data_type().clone();
    let schema = Schema::new(vec![Field::new("c", data_type, true)]);
    let batch =
        RecordBatch::try_new(Arc::new(schema.clone()), vec![array]).expect("batch");
    (schema, batch)
}

/// Consecutive batches of one column chunk: their own keys, one dictionary.
fn chunk(
    distinct: usize,
    batches: usize,
    input: InputValues,
) -> (Schema, Vec<RecordBatch>) {
    let values = values_of(distinct, input);
    let schema = Schema::new(vec![Field::new(
        "c",
        DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(values.data_type().clone()),
        ),
        true,
    )]);
    let batches = (0..batches)
        .map(|b| {
            let keys = Int32Array::from(
                (0..ROWS)
                    .map(|i| ((i + b * 7) % distinct) as i32)
                    .collect::<Vec<_>>(),
            );
            let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::clone(&values))
                .expect("dictionary array");
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(dict)])
                .expect("batch")
        })
        .collect();
    (schema, batches)
}

/// `batches` batches, each with a dictionary of its own.
fn separate(
    distinct: usize,
    batches: usize,
    input: InputValues,
) -> (Schema, Vec<RecordBatch>) {
    let mut schema = None;
    let batches = (0..batches)
        .map(|b| {
            let (built, batch) = dictionary_batch(distinct, b, input);
            schema.get_or_insert(built);
            batch
        })
        .collect();
    (schema.expect("at least one batch"), batches)
}

fn expr_over(udf: Arc<ScalarUDF>, schema: &Schema, base64: bool) -> ScalarFunctionExpr {
    let mut args: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("c", 0))];
    if base64 {
        args.push(Arc::new(Literal::new(ScalarValue::from("base64"))));
    }
    ScalarFunctionExpr::try_new(udf, args, schema, Arc::new(ConfigOptions::new()))
        .expect("scalar function expr")
}

/// Builds the physical casts selected by the function's coercion rules before
/// constructing the scalar expression. This mirrors the casts inserted during
/// logical expression analysis, while keeping the benchmark at the physical
/// expression layer.
fn coerced_expr_over(
    udf: Arc<ScalarUDF>,
    schema: &Schema,
    encoding: &str,
) -> ScalarFunctionExpr {
    let args: Vec<Arc<dyn PhysicalExpr>> = vec![
        Arc::new(Column::new("c", 0)),
        Arc::new(Literal::new(ScalarValue::from(encoding))),
    ];
    let arg_fields = args
        .iter()
        .map(|arg| arg.return_field(schema).expect("argument field"))
        .collect::<Vec<_>>();
    let coerced_fields = fields_with_udf(&arg_fields, udf.as_ref()).expect("coercion");
    let args = args
        .into_iter()
        .zip(coerced_fields)
        .map(|(arg, field)| {
            let input_type = arg.data_type(schema).expect("argument type");
            if &input_type == field.data_type() {
                arg
            } else {
                Arc::new(CastExpr::new(arg, field.data_type().clone(), None))
                    as Arc<dyn PhysicalExpr>
            }
        })
        .collect();
    ScalarFunctionExpr::try_new(udf, args, schema, Arc::new(ConfigOptions::new()))
        .expect("scalar function expr")
}

/// The expression layer, which decides what the function receives.
fn benchmark_expression_layer(c: &mut Criterion) {
    // (name, function, takes a base64 argument, dictionary-typed calls reach
    // the function today)
    let functions: Vec<(&str, Arc<ScalarUDF>, bool, bool)> = vec![
        (
            "encode",
            datafusion_functions::encoding::encode(),
            true,
            false,
        ),
        (
            "reverse",
            datafusion_functions::unicode::reverse(),
            false,
            true,
        ),
    ];

    for (name, udf, binary, dictionary_calls) in &functions {
        let input = if *binary {
            InputValues::Binary
        } else {
            InputValues::String
        };
        let mut group =
            c.benchmark_group(format!("dictionary_encoding/expression/{name}"));

        if *dictionary_calls {
            // A dictionary of its own per batch: nothing carries over.
            //
            // The cursor lives outside the routine, which criterion calls
            // afresh for every sample: restarted per sample it would revisit
            // the first batches often enough for a result to still be
            // remembered, and the group would quietly measure a warm
            // dictionary under a cold name.
            for distinct in CARDINALITIES {
                let (schema, batches) = separate(distinct, 16, input);
                let expr = expr_over(Arc::clone(udf), &schema, *binary);
                let cursor = Cell::new(0usize);
                group.bench_with_input(
                    BenchmarkId::new("cold", distinct),
                    &batches,
                    |b, batches| {
                        b.iter(|| {
                            cursor.set((cursor.get() + 1) % batches.len());
                            black_box(
                                expr.evaluate(black_box(&batches[cursor.get()])).unwrap(),
                            )
                        })
                    },
                );
            }

            // One dictionary across the batches of a column chunk.
            for distinct in CARDINALITIES {
                let (schema, batches) = chunk(distinct, 8, input);
                let expr = expr_over(Arc::clone(udf), &schema, *binary);
                for batch in &batches {
                    expr.evaluate(batch).unwrap();
                }
                let cursor = Cell::new(0usize);
                group.bench_with_input(
                    BenchmarkId::new("warm", distinct),
                    &batches,
                    |b, batches| {
                        b.iter(|| {
                            cursor.set((cursor.get() + 1) % batches.len());
                            black_box(
                                expr.evaluate(black_box(&batches[cursor.get()])).unwrap(),
                            )
                        })
                    },
                );
            }
        }

        // The same rows with the encoding materialized: one call per row.
        for distinct in [CARDINALITIES[0], ROWS] {
            let (schema, batch) = flat_batch(distinct, input);
            let expr = expr_over(Arc::clone(udf), &schema, *binary);
            group.bench_with_input(
                BenchmarkId::new("flat", distinct),
                &batch,
                |b, batch| b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap())),
            );
        }

        // What a dictionary column costs without encoding preservation, which
        // is what `encode` did before this change and what every function
        // without it still does: coercion casts the dictionary away, and the
        // call sees one row per row. The `flat` rows above are not this — they
        // never were a dictionary and so never pay for materializing one.
        for distinct in CARDINALITIES {
            let (schema, batches) = separate(distinct, 16, input);
            let values_type = match schema.field(0).data_type() {
                DataType::Dictionary(_, values) => values.as_ref().clone(),
                other => other.clone(),
            };
            let cast_expr: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
                Arc::new(Column::new("c", 0)),
                values_type,
                None,
            ));
            let mut args: Vec<Arc<dyn PhysicalExpr>> = vec![cast_expr];
            if *binary {
                args.push(Arc::new(Literal::new(ScalarValue::from("base64"))));
            }
            let expr = ScalarFunctionExpr::try_new(
                Arc::clone(udf),
                args,
                &schema,
                Arc::new(ConfigOptions::new()),
            )
            .expect("scalar function expr");
            let cursor = Cell::new(0usize);
            group.bench_with_input(
                BenchmarkId::new("cast_away", distinct),
                &batches,
                |b, batches| {
                    b.iter(|| {
                        cursor.set((cursor.get() + 1) % batches.len());
                        black_box(
                            expr.evaluate(black_box(&batches[cursor.get()])).unwrap(),
                        )
                    })
                },
            );
        }

        group.finish();
    }
}

/// Baseline for dictionary-aware `decode`. The coercion-derived expression
/// currently materializes each dictionary to Binary. Once `decode` requests
/// dictionary preservation, these same expressions will receive dictionaries
/// with no benchmark changes.
fn benchmark_decode_expression_layer(c: &mut Criterion) {
    let udf = datafusion_functions::encoding::decode();
    let mut group = c.benchmark_group("dictionary_encoding/expression/decode");

    // A dictionary of its own per batch: nothing carries over.
    for distinct in CARDINALITIES {
        let (schema, batches) = separate(distinct, 16, InputValues::HexBinary);
        let expr = coerced_expr_over(Arc::clone(&udf), &schema, "hex");
        let cursor = Cell::new(0usize);
        group.bench_with_input(
            BenchmarkId::new("cold", distinct),
            &batches,
            |b, batches| {
                b.iter(|| {
                    cursor.set((cursor.get() + 1) % batches.len());
                    black_box(expr.evaluate(black_box(&batches[cursor.get()])).unwrap())
                })
            },
        );
    }

    // Consecutive batches sharing the values of one dictionary.
    for distinct in CARDINALITIES {
        let (schema, batches) = chunk(distinct, 8, InputValues::HexBinary);
        let expr = coerced_expr_over(Arc::clone(&udf), &schema, "hex");
        let cursor = Cell::new(0usize);
        group.bench_with_input(
            BenchmarkId::new("warm", distinct),
            &batches,
            |b, batches| {
                b.iter(|| {
                    cursor.set((cursor.get() + 1) % batches.len());
                    black_box(expr.evaluate(black_box(&batches[cursor.get()])).unwrap())
                })
            },
        );
    }

    // Flat inputs bound the cost of decoding the same logical rows without
    // dictionary materialization.
    for distinct in [CARDINALITIES[0], ROWS] {
        let (schema, batch) = flat_batch(distinct, InputValues::HexBinary);
        let expr = coerced_expr_over(Arc::clone(&udf), &schema, "hex");
        group.bench_with_input(BenchmarkId::new("flat", distinct), &batch, |b, batch| {
            b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap()))
        });
    }

    // A filtered dictionary retaining four times as many values as rows.
    // Only 256 of those values are referenced by the visible rows.
    let (schema, batch) =
        dictionary_batch_with_backing(256, ROWS * 4, 0, InputValues::HexBinary);
    let expr = coerced_expr_over(udf, &schema, "hex");
    group.bench_function("retained_4x", |b| {
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_function_layer,
    benchmark_expression_layer,
    benchmark_decode_expression_layer
);
criterion_main!(benches);
