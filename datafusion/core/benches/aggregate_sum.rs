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

#[macro_use]
extern crate criterion;
extern crate arrow;
extern crate datafusion;

mod data_utils;
use crate::criterion::Criterion;
use arrow_array::{RecordBatch, ArrayRef, Float64Array, PrimitiveArray};
use arrow_schema::{Schema, Field};
use data_utils::create_table_provider;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_expr::AggregateFunction;
use datafusion_expr::type_coercion::aggregates::coerce_types;
use datafusion_physical_expr::{expressions::{create_aggregate_expr, try_cast, col}, AggregateExpr};
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub fn aggregate(
    batch: &RecordBatch,
    agg: Arc<dyn AggregateExpr>,
) -> Result<ScalarValue> {
    let mut accum = agg.create_accumulator()?;
    let expr = agg.expressions();
    let values = expr
        .iter()
        .map(|e| {
            e.evaluate(batch)
                .and_then(|v| v.into_array(batch.num_rows()))
        })
        .collect::<Result<Vec<_>>>()?;
    accum.update_batch(&values)?;
    accum.evaluate()
}

pub fn assert_aggregate(
    array: ArrayRef,
    function: AggregateFunction,
    distinct: bool,
    expected: ScalarValue,
) {
    let data_type = array.data_type();
    let sig = function.signature();
    let coerced = coerce_types(&function, &[data_type.clone()], &sig).unwrap();

    let input_schema = Schema::new(vec![Field::new("a", data_type.clone(), true)]);
    let batch =
        RecordBatch::try_new(Arc::new(input_schema.clone()), vec![array]).unwrap();

    let input = try_cast(
        col("a", &input_schema).unwrap(),
        &input_schema,
        coerced[0].clone(),
    )
    .unwrap();

    let schema = Schema::new(vec![Field::new("a", coerced[0].clone(), true)]);
    let agg =
        create_aggregate_expr(&function, distinct, &[input], &[], &schema, "agg")
            .unwrap();

    let result = aggregate(&batch, agg).unwrap();
    assert_eq!(expected, result);
}

fn query(ctx: Arc<Mutex<SessionContext>>, sql: &str) {
    let rt = Runtime::new().unwrap();
    let df = rt.block_on(ctx.lock().sql(sql)).unwrap();
    criterion::black_box(rt.block_on(df.collect()).unwrap());
}

fn create_context(
    partitions_len: usize,
    array_len: usize,
    batch_size: usize,
) -> Result<Arc<Mutex<SessionContext>>> {
    let ctx = SessionContext::new();
    let provider = create_table_provider(partitions_len, array_len, batch_size)?;
    ctx.register_table("t", provider)?;
    Ok(Arc::new(Mutex::new(ctx)))
}

fn criterion_benchmark(c: &mut Criterion) {
    // let partitions_len = 8;
    // let array_len = 32768 * 2; // 2^16
    // let batch_size = 2048; // 2^11
    // let ctx = create_context(partitions_len, array_len, batch_size).unwrap();

    let n = 1000000000; 
    let vec_of_f64: Vec<f64> = (0..=n as usize).map(|x| 1 as f64).collect();
    let a: ArrayRef = Arc::new(Float64Array::from(vec_of_f64));
    // c.bench_function("sum 1e9", |b| b.iter(|| assert_aggregate(a.clone(), AggregateFunction::Sum, false, criterion::black_box(ScalarValue::List(Arc::new(Float64Array::from(vec![1000000001_f64])))))));
    c.bench_function("sum 1e9", |b| b.iter(|| assert_aggregate(a.clone(), AggregateFunction::Sum, false, criterion::black_box(ScalarValue::from(1000000001_f64)))));

    // c.bench_function("aggregate_query_no_group_by 15 12", |b| {
    //     b.iter(|| {
    //         query(
    //             ctx.clone(),
    //             "SELECT MIN(f64), AVG(f64), COUNT(f64) \
    //              FROM t",
    //         )
    //     })
    // });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
