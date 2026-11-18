// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! The `raw` and `canon` tables. `raw` has some `-0.0` values; `canon` is
//! `raw` with every float normalized by `normalize`, so it has none.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, AsArray, Float32Array, Float64Array, Int64Array, ListArray, RecordBatch,
};
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Schema, SchemaRef};
use datafusion_common::float_semantics::normalize;
use rand::Rng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

/// Both copies of the table, split into the same partitions and batches.
pub struct Dataset {
    pub schema: SchemaRef,
    pub canon: Vec<Vec<RecordBatch>>,
    pub raw: Vec<Vec<RecordBatch>>,
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("g", DataType::Int64, false),
        Field::new("a", DataType::Float64, true),
        Field::new("a32", DataType::Float32, true),
        Field::new_list("k", Field::new_list_field(DataType::Float64, true), true),
    ]))
}

struct Columns {
    id: Vec<i64>,
    b: Vec<i64>,
    g: Vec<i64>,
    a: Vec<Option<f64>>,
    a32: Vec<Option<f32>>,
    k: Vec<Option<Vec<Option<f64>>>>,
}

impl Columns {
    fn into_batch(self, schema: &SchemaRef) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(self.id)),
            Arc::new(Int64Array::from(self.b)),
            Arc::new(Int64Array::from(self.g)),
            Arc::new(Float64Array::from(self.a)),
            Arc::new(Float32Array::from(self.a32)),
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(self.k)),
        ];
        RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
    }
}

/// `raw` with every `-0.0`, also nested ones, replaced by `0.0`.
fn canon_of(raw: &RecordBatch) -> RecordBatch {
    let columns = raw.columns().iter().cloned().map(normalize).collect();
    RecordBatch::try_new(raw.schema(), columns).unwrap()
}

/// Whether `array`, one of the float or list columns of `raw`, has a `-0.0`
/// at any depth. Checks the values directly rather than asking
/// `normalize`, which may change more than `-0.0`. A list is checked
/// through all of its child values, so `array` must not be a slice.
pub fn has_negative_zero(array: &ArrayRef) -> bool {
    match array.data_type() {
        DataType::Float64 => array
            .as_primitive::<Float64Type>()
            .iter()
            .flatten()
            .any(|v| v == 0.0 && v.is_sign_negative()),
        DataType::Float32 => array
            .as_primitive::<Float32Type>()
            .iter()
            .flatten()
            .any(|v| v == 0.0 && v.is_sign_negative()),
        DataType::List(_) => has_negative_zero(array.as_list::<i32>().values()),
        other => panic!("has_negative_zero does not support {other}"),
    }
}

/// The 4-row `raw` table from
/// <https://github.com/apache/datafusion/issues/26138#issuecomment-6497193637>,
/// as a single batch. Every known failure reproduces on it.
pub fn fixed() -> Dataset {
    let schema = schema();
    let z = -0.0;
    let raw = Columns {
        id: vec![1, 2, 3, 4],
        b: vec![1, 0, 2, 3],
        g: vec![1, 1, 1, 1],
        a: vec![Some(z), Some(0.0), Some(1.0), Some(z)],
        a32: vec![Some(-0.0), Some(0.0), Some(1.0), Some(-0.0)],
        k: vec![
            Some(vec![Some(z), Some(1.0)]),
            Some(vec![Some(0.0), Some(0.0)]),
            Some(vec![Some(1.0)]),
            Some(vec![Some(0.0), Some(1.0)]),
        ],
    }
    .into_batch(&schema);
    Dataset {
        canon: vec![vec![canon_of(&raw)]],
        raw: vec![vec![raw]],
        schema,
    }
}

/// A random float from a small pool, so that keys tie often.
///
/// TODO(#26091): add NaN payloads other than the default one once it is
/// decided whether all NaNs are equal.
fn pool_value(rng: &mut impl Rng) -> Option<f64> {
    match rng.random_range(0..10) {
        0..2 => Some(0.0),
        2..4 => Some(-0.0),
        4 => Some(1.0),
        5 => Some(-1.0),
        6 => Some(f64::INFINITY),
        7 => Some(f64::NAN),
        _ => None,
    }
}

/// `num_rows` random rows, split into random batches over 1 to 3 partitions.
/// Retries until every float column of `raw` (`a`, `a32` and the lists in
/// `k`) has a `-0.0`.
pub fn random(rng: &mut StdRng, num_rows: usize) -> Dataset {
    let schema = schema();
    let raw = loop {
        let mut b: Vec<i64> = (1..=num_rows as i64).collect();
        b.shuffle(rng);
        let raw = Columns {
            id: (1..=num_rows as i64).collect(),
            b,
            g: (0..num_rows).map(|_| rng.random_range(0..3)).collect(),
            a: (0..num_rows).map(|_| pool_value(rng)).collect(),
            a32: (0..num_rows)
                .map(|_| pool_value(rng).map(|v| v as f32))
                .collect(),
            k: (0..num_rows)
                .map(|_| {
                    let len = rng.random_range(0..5);
                    // Length 4 stands for a NULL list.
                    (len < 4).then(|| (0..len).map(|_| pool_value(rng)).collect())
                })
                .collect(),
        }
        .into_batch(&schema);
        if ["a", "a32", "k"]
            .iter()
            .all(|name| has_negative_zero(raw.column_by_name(name).unwrap()))
        {
            break raw;
        }
    };
    let (canon, raw) = split(rng, &canon_of(&raw), &raw);
    Dataset { schema, canon, raw }
}

/// Splits both batches at the same random offsets into partitions of batches.
fn split(
    rng: &mut StdRng,
    canon: &RecordBatch,
    raw: &RecordBatch,
) -> (Vec<Vec<RecordBatch>>, Vec<Vec<RecordBatch>>) {
    let num_partitions = rng.random_range(1..=3);
    let mut canon_parts = vec![vec![]; num_partitions];
    let mut raw_parts = vec![vec![]; num_partitions];
    let mut offset = 0;
    while offset < canon.num_rows() {
        let len = rng.random_range(1..=16).min(canon.num_rows() - offset);
        let partition = rng.random_range(0..num_partitions);
        canon_parts[partition].push(canon.slice(offset, len));
        raw_parts[partition].push(raw.slice(offset, len));
        offset += len;
    }
    (canon_parts, raw_parts)
}

/// `random` relies on `has_negative_zero` seeing a `-0.0` in every column
/// type of `raw`, and only a `-0.0`.
#[test]
fn has_negative_zero_column_types() {
    let f64s = |v: f64| -> ArrayRef { Arc::new(Float64Array::from(vec![None, Some(v)])) };
    let f32s = |v: f32| -> ArrayRef { Arc::new(Float32Array::from(vec![None, Some(v)])) };
    let list = |v: f64| -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([
            None,
            Some(vec![Some(1.0), Some(v)]),
        ]))
    };
    for v in [0.0, 1.0, f64::NAN] {
        assert!(!has_negative_zero(&f64s(v)), "{v}");
        assert!(!has_negative_zero(&f32s(v as f32)), "{v}");
        assert!(!has_negative_zero(&list(v)), "{v}");
    }
    assert!(has_negative_zero(&f64s(-0.0)));
    assert!(has_negative_zero(&f32s(-0.0)));
    assert!(has_negative_zero(&list(-0.0)));
}
