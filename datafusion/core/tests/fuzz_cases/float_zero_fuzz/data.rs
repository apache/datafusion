// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! The `canon` and `raw` tables. `canon` contains no `-0.0`; `raw` is `canon`
//! with some of its zeros flipped to `-0.0`.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Float32Array, Float64Array, Int64Array, ListArray,
    RecordBatch, make_array,
};
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Schema, SchemaRef};
use rand::Rng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

/// Probability that a zero in `canon` becomes `-0.0` in `raw`.
const FLIP_PROBABILITY: f64 = 0.5;

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

/// The 4-row tables from
/// <https://github.com/apache/datafusion/issues/26138#issuecomment-6497193637>,
/// as a single batch. Every known failure reproduces on them.
pub fn fixed() -> Dataset {
    let schema = schema();
    let z = -0.0_f64;
    let canon = Columns {
        id: vec![1, 2, 3, 4],
        b: vec![1, 0, 2, 3],
        g: vec![1, 1, 1, 1],
        a: vec![Some(0.0), Some(0.0), Some(1.0), Some(0.0)],
        a32: vec![Some(0.0), Some(0.0), Some(1.0), Some(0.0)],
        k: vec![
            Some(vec![Some(0.0), Some(1.0)]),
            Some(vec![Some(0.0), Some(0.0)]),
            Some(vec![Some(1.0)]),
            Some(vec![Some(0.0), Some(1.0)]),
        ],
    };
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
    };
    Dataset {
        canon: vec![vec![canon.into_batch(&schema)]],
        raw: vec![vec![raw.into_batch(&schema)]],
        schema,
    }
}

/// A random float from a small pool, so that keys tie often.
///
/// TODO(#26091): add NaN payloads other than the default one once it is
/// decided whether all NaNs are equal.
fn pool_value(rng: &mut impl Rng) -> Option<f64> {
    match rng.random_range(0..10) {
        0..4 => Some(0.0),
        4 => Some(1.0),
        5 => Some(-1.0),
        6 => Some(f64::INFINITY),
        7 => Some(f64::NAN),
        _ => None,
    }
}

/// `num_rows` random rows, split into random batches over 1 to 3 partitions.
/// Retries until `raw` has a `-0.0` both at the top level and inside a list.
pub fn random(rng: &mut StdRng, num_rows: usize) -> Dataset {
    let schema = schema();
    loop {
        let mut b: Vec<i64> = (1..=num_rows as i64).collect();
        b.shuffle(rng);
        let canon = Columns {
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

        let raw = RecordBatch::try_new(
            Arc::clone(&schema),
            canon
                .columns()
                .iter()
                .map(|c| flip_zeros(c, rng, FLIP_PROBABILITY))
                .collect(),
        )
        .unwrap();
        let top_level = count_negative_zeros(raw.column_by_name("a").unwrap());
        let nested = count_negative_zeros(raw.column_by_name("k").unwrap());
        if top_level == 0 || nested == 0 {
            continue;
        }

        let (canon, raw) = split(rng, &canon, &raw);
        return Dataset { schema, canon, raw };
    }
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

/// Rewrites each float `0.0` in `array`, at any nesting depth, to `-0.0` with
/// probability `p`. The recursion follows `normalize_floats` in
/// `datafusion_common::float_semantics`.
fn flip_zeros(array: &ArrayRef, rng: &mut impl Rng, p: f64) -> ArrayRef {
    match array.data_type() {
        DataType::Float32 => Arc::new(
            array
                .as_primitive::<Float32Type>()
                .iter()
                .map(|v| {
                    v.map(|v| {
                        if v == 0.0 && rng.random_bool(p) {
                            -0.0
                        } else {
                            v
                        }
                    })
                })
                .collect::<Float32Array>(),
        ),
        DataType::Float64 => Arc::new(
            array
                .as_primitive::<Float64Type>()
                .iter()
                .map(|v| {
                    v.map(|v| {
                        if v == 0.0 && rng.random_bool(p) {
                            -0.0
                        } else {
                            v
                        }
                    })
                })
                .collect::<Float64Array>(),
        ),
        _ => {
            let data = array.to_data();
            if data.child_data().is_empty() {
                return Arc::clone(array);
            }
            let children = data
                .child_data()
                .iter()
                .map(|child| flip_zeros(&make_array(child.clone()), rng, p).to_data())
                .collect::<Vec<_>>();
            make_array(data.into_builder().child_data(children).build().unwrap())
        }
    }
}

/// The number of `-0.0` values in `array`, at any nesting depth.
fn count_negative_zeros(array: &ArrayRef) -> usize {
    match array.data_type() {
        DataType::Float32 => array
            .as_primitive::<Float32Type>()
            .iter()
            .flatten()
            .filter(|v| *v == 0.0 && v.is_sign_negative())
            .count(),
        DataType::Float64 => array
            .as_primitive::<Float64Type>()
            .iter()
            .flatten()
            .filter(|v| *v == 0.0 && v.is_sign_negative())
            .count(),
        _ => array
            .to_data()
            .child_data()
            .iter()
            .map(|child| count_negative_zeros(&make_array(child.clone())))
            .sum(),
    }
}
