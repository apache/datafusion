// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! The two tables each query runs on:
//!
//! - `raw` has some `-0.0` values.
//! - `canonical` is the same table with every `-0.0` replaced by `0.0`, so
//!   each zero is written the same way.
//!
//! A query must give the same result on both.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, AsArray, Float16Array, Float32Array, Float64Array, Int64Array, ListArray,
    RecordBatch,
};
use arrow::datatypes::{DataType, Float16Type, Float32Type, Float64Type, SchemaRef};
use datafusion_common::float_semantics::normalize;
use half::f16;
use rand::Rng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

/// Both tables, split into the same pieces.
pub struct Dataset {
    pub schema: SchemaRef,
    pub canonical: Vec<Vec<RecordBatch>>,
    pub raw: Vec<Vec<RecordBatch>>,
}

struct Columns {
    id: Vec<i64>,
    b: Vec<i64>,
    g: Vec<i64>,
    a: Vec<Option<f64>>,
    a32: Vec<Option<f32>>,
    a16: Vec<Option<f16>>,
    k: Vec<Option<Vec<Option<f64>>>>,
}

impl Columns {
    fn into_batch(self) -> RecordBatch {
        let k = ListArray::from_iter_primitive::<Float64Type, _, _>(self.k);
        RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(self.id)) as ArrayRef),
            ("b", Arc::new(Int64Array::from(self.b))),
            ("g", Arc::new(Int64Array::from(self.g))),
            ("a", Arc::new(Float64Array::from(self.a))),
            ("a32", Arc::new(Float32Array::from(self.a32))),
            ("a16", Arc::new(Float16Array::from(self.a16))),
            ("k", Arc::new(k)),
        ])
        .unwrap()
    }
}

fn canonical_of(raw: &RecordBatch) -> RecordBatch {
    let columns = raw.columns().iter().cloned().map(normalize).collect();
    RecordBatch::try_new(raw.schema(), columns).unwrap()
}

/// Whether `array` holds a `-0.0`, also inside lists.
fn has_negative_zero(array: &ArrayRef) -> bool {
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
        DataType::Float16 => array
            .as_primitive::<Float16Type>()
            .iter()
            .flatten()
            .any(|v| v == f16::ZERO && v.is_sign_negative()),
        DataType::List(_) => array
            .as_list::<i32>()
            .iter()
            .flatten()
            .any(|l| has_negative_zero(&l)),
        other => panic!("has_negative_zero does not support {other}"),
    }
}

/// The 4-row table from
/// <https://github.com/apache/datafusion/issues/26138#issuecomment-6497193637>.
/// Every known failure shows up on it.
pub fn fixed() -> Dataset {
    let z = -0.0;
    let raw = Columns {
        id: vec![1, 2, 3, 4],
        b: vec![1, 0, 2, 3],
        g: vec![1, 1, 1, 1],
        a: vec![Some(z), Some(0.0), Some(1.0), Some(z)],
        a32: vec![Some(-0.0), Some(0.0), Some(1.0), Some(-0.0)],
        a16: vec![
            Some(f16::NEG_ZERO),
            Some(f16::ZERO),
            Some(f16::ONE),
            Some(f16::NEG_ZERO),
        ],
        k: vec![
            Some(vec![Some(z), Some(1.0)]),
            Some(vec![Some(0.0), Some(0.0)]),
            Some(vec![Some(1.0)]),
            Some(vec![Some(0.0), Some(1.0)]),
        ],
    }
    .into_batch();
    Dataset {
        schema: raw.schema(),
        canonical: vec![vec![canonical_of(&raw)]],
        raw: vec![vec![raw]],
    }
}

/// A random value from a small set, so that values often repeat.
///
/// TODO(#26091): add other NaN values once it is decided whether all NaNs
/// are equal.
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

/// A random table of 8 to 128 rows, split into 1 to 3 partitions. Tries
/// again until `a`, `a32`, `a16` and `k` each hold a `-0.0`.
pub fn random(rng: &mut StdRng) -> Dataset {
    let num_rows = rng.random_range(8..=128);
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
            a16: (0..num_rows)
                .map(|_| pool_value(rng).map(f16::from_f64))
                .collect(),
            k: (0..num_rows)
                .map(|_| {
                    let len = rng.random_range(0..5);
                    // Length 4 stands for a NULL list.
                    (len < 4).then(|| (0..len).map(|_| pool_value(rng)).collect())
                })
                .collect(),
        }
        .into_batch();
        if ["a", "a32", "a16", "k"]
            .iter()
            .all(|name| has_negative_zero(raw.column_by_name(name).unwrap()))
        {
            break raw;
        }
    };
    let schema = raw.schema();
    let (canonical, raw) = split(rng, &canonical_of(&raw), &raw);
    Dataset {
        schema,
        canonical,
        raw,
    }
}

/// Cuts both tables into the same random pieces of 1 to 16 rows and spreads
/// them over 1 to 3 groups. The two tables still differ only in how their
/// zeros are written.
fn split(
    rng: &mut StdRng,
    canonical: &RecordBatch,
    raw: &RecordBatch,
) -> (Vec<Vec<RecordBatch>>, Vec<Vec<RecordBatch>>) {
    let num_partitions = rng.random_range(1..=3);
    let mut canonical_parts = vec![vec![]; num_partitions];
    let mut raw_parts = vec![vec![]; num_partitions];
    let mut offset = 0;
    while offset < canonical.num_rows() {
        let len = rng.random_range(1..=16).min(canonical.num_rows() - offset);
        let partition = rng.random_range(0..num_partitions);
        canonical_parts[partition].push(canonical.slice(offset, len));
        raw_parts[partition].push(raw.slice(offset, len));
        offset += len;
    }
    (canonical_parts, raw_parts)
}

/// `random` needs `has_negative_zero` to find `-0.0`, and only `-0.0`, in
/// every column type.
#[test]
fn has_negative_zero_column_types() {
    let f64s = |v: f64| -> ArrayRef { Arc::new(Float64Array::from(vec![None, Some(v)])) };
    let f32s = |v: f32| -> ArrayRef { Arc::new(Float32Array::from(vec![None, Some(v)])) };
    let f16s = |v: f64| -> ArrayRef {
        Arc::new(Float16Array::from(vec![None, Some(f16::from_f64(v))]))
    };
    let list = |v: f64| -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([
            None,
            Some(vec![Some(1.0), Some(v)]),
        ]))
    };
    for v in [0.0, 1.0, f64::NAN] {
        assert!(!has_negative_zero(&f64s(v)), "{v}");
        assert!(!has_negative_zero(&f32s(v as f32)), "{v}");
        assert!(!has_negative_zero(&f16s(v)), "{v}");
        assert!(!has_negative_zero(&list(v)), "{v}");
    }
    assert!(has_negative_zero(&f64s(-0.0)));
    assert!(has_negative_zero(&f32s(-0.0)));
    assert!(has_negative_zero(&f16s(-0.0)));
    assert!(has_negative_zero(&list(-0.0)));
}
