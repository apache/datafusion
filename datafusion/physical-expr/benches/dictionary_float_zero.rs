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

//! Dynamic IN over dictionary floats, with and without signed-zero rewriting.
//! An all-valid values bitmap can add work when computing logical nulls.
//! Physical expressions preserve this exact Arrow layout, which SQL does not
//! specify. Float32 suffices to exercise the bitmap mechanism.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, AsArray, BooleanArray, DictionaryArray, Float32Array, Int32Array,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::{
    PhysicalExpr,
    expressions::{InListExpr, col, lit},
};

fn benchmark(c: &mut Criterion) {
    const ROWS: usize = 8192;
    const CARDINALITY: usize = 16;
    const ZERO_KEY: usize = CARDINALITY / 2;

    for negative_zero in [false, true] {
        for all_valid_bitmap in [false, true] {
            let zero = if negative_zero { -0.0_f32 } else { 0.0 };
            let values = Float32Array::new(
                (0..CARDINALITY)
                    .map(|i| if i == ZERO_KEY { zero } else { (i + 1) as f32 })
                    .collect::<Vec<_>>()
                    .into(),
                all_valid_bitmap.then(|| NullBuffer::new_valid(CARDINALITY)),
            );
            let a: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
                Int32Array::from_iter_values((0..ROWS).map(|i| (i % CARDINALITY) as i32)),
                Arc::new(values),
            ));
            // Keep the RHS bitmap-free to isolate the LHS representation.
            let b: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
                Int32Array::from(vec![0; ROWS]),
                Arc::new(Float32Array::from(vec![0.0])),
            ));
            let batch = RecordBatch::try_from_iter([("a", a), ("b", b)]).unwrap();
            let schema = batch.schema();
            // A column forces dynamic evaluation; positive nonzero needles
            // cannot match the negative literals, so all four terms are visited.
            let expr = InListExpr::try_new(
                col("a", &schema).unwrap(),
                vec![
                    col("b", &schema).unwrap(),
                    lit(ScalarValue::Float32(Some(-1.0))),
                    lit(ScalarValue::Float32(Some(-2.0))),
                    lit(ScalarValue::Float32(Some(-3.0))),
                ],
                false,
                &schema,
            )
            .unwrap();
            let expected: BooleanArray = (0..ROWS)
                .map(|i| Some(i % CARDINALITY == ZERO_KEY))
                .collect();
            let result = expr.evaluate(&batch).unwrap().into_array(ROWS).unwrap();
            assert_eq!(result.as_boolean(), &expected);

            let name = format!(
                "dictionary_float_zero/negative_zero={negative_zero}/all_valid_bitmap={all_valid_bitmap}"
            );
            c.bench_function(&name, |b| {
                // Reuse the original input; setup and validation are untimed.
                b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
            });
        }
    }
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
