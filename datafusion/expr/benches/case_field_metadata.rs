// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility, col, lit, when,
};

#[derive(Debug, PartialEq, Eq, Hash)]
struct MetadataIdentity {
    signature: Signature,
}

impl ScalarUDFImpl for MetadataIdentity {
    fn name(&self) -> &str {
        "metadata_identity"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::clone(&args.arg_fields[0]))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(args.args[0].clone())
    }
}

fn literal_case(width: usize) -> Expr {
    let mut builder = when(lit(true), lit(0_i32));
    for i in 1..width {
        builder = builder.when(lit(false), lit(i32::try_from(i).unwrap()));
    }
    builder.end().unwrap()
}

fn column_case(width: usize, marked_last: bool) -> (DFSchema, Expr) {
    let fields = (0..width)
        .map(|i| {
            let field = Field::new(format!("c{i}"), DataType::Int32, false);
            if marked_last && i == width - 1 {
                field.with_metadata(HashMap::from([(
                    "source".to_owned(),
                    "marked".to_owned(),
                )]))
            } else {
                field
            }
        })
        .collect::<Vec<_>>();
    let schema =
        DFSchema::from_unqualified_fields(fields.into(), HashMap::new()).unwrap();
    let mut builder = when(lit(true), col("c0"));
    for i in 1..width {
        builder = builder.when(lit(false), col(format!("c{i}")));
    }
    (schema, builder.end().unwrap())
}

fn marked_column_schema() -> DFSchema {
    let metadata = HashMap::from([("source".to_owned(), "marked".to_owned())]);
    DFSchema::from_unqualified_fields(
        vec![
            Field::new("a", DataType::Int32, false).with_metadata(metadata.clone()),
            Field::new("b", DataType::Int32, false).with_metadata(metadata),
        ]
        .into(),
        HashMap::new(),
    )
    .unwrap()
}

fn nested_column_case(depth: usize, wrap: impl Fn(Expr) -> Expr) -> Expr {
    let mut expression = col("a");
    for _ in 0..depth {
        expression = when(lit(true), col("b"))
            .otherwise(wrap(expression))
            .unwrap();
    }
    expression
}

fn alternating_case(depth: usize, identity: &ScalarUDF) -> Expr {
    let mut expression = col("a");
    for _ in 0..depth {
        expression = when(lit(true), identity.call(vec![expression]))
            .otherwise(col("b"))
            .unwrap();
    }
    expression
}

fn case_field_metadata(c: &mut Criterion) {
    let mut group = c.benchmark_group("case_field_metadata");
    for width in [10, 100, 1000] {
        let literal = literal_case(width);
        let empty_schema = DFSchema::empty();
        group.bench_with_input(
            BenchmarkId::new("plain_literals", width),
            &width,
            |b, _| {
                b.iter(|| {
                    black_box(
                        black_box(&literal)
                            .to_field(black_box(&empty_schema))
                            .unwrap(),
                    )
                })
            },
        );

        for (name, marked_last) in [("plain_columns", false), ("marked_last", true)] {
            let (schema, expression) = column_case(width, marked_last);
            group.bench_with_input(BenchmarkId::new(name, width), &width, |b, _| {
                b.iter(|| {
                    black_box(
                        black_box(&expression).to_field(black_box(&schema)).unwrap(),
                    )
                })
            });
        }
    }

    let schema = marked_column_schema();
    for depth in [8, 32, 128] {
        for (name, expression) in [
            ("nested_columns", nested_column_case(depth, |expr| expr)),
            (
                "nested_aliases",
                nested_column_case(depth, |expr| expr.alias("nested")),
            ),
            (
                "nested_casts",
                nested_column_case(depth, |expr| {
                    Expr::Cast(Cast::new(Box::new(expr), DataType::Int32))
                }),
            ),
        ] {
            group.bench_with_input(BenchmarkId::new(name, depth), &depth, |b, _| {
                b.iter(|| {
                    black_box(
                        black_box(&expression).to_field(black_box(&schema)).unwrap(),
                    )
                })
            });
        }
    }

    let identity = ScalarUDF::from(MetadataIdentity {
        signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
    });
    for depth in [2, 4, 6, 8] {
        let expression = alternating_case(depth, &identity);
        group.bench_with_input(
            BenchmarkId::new("alternating_case_udf", depth),
            &depth,
            |b, _| {
                b.iter(|| {
                    black_box(
                        black_box(&expression).to_field(black_box(&schema)).unwrap(),
                    )
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, case_field_metadata);
criterion_main!(benches);
