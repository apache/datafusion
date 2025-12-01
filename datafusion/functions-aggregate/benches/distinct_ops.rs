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

//! Benchmarks for distinct-aware aggregates.

use std::hint::black_box;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::ArrayRef;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int64Type, Schema};
use arrow::util::bench_util::create_primitive_array;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use datafusion_common::{HashSet, ScalarValue};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{Accumulator, AggregateUDFImpl};
use datafusion_functions_aggregate::median::Median;
use datafusion_functions_aggregate::percentile_cont::PercentileCont;
use datafusion_functions_aggregate_common::aggregate::count_distinct::{
    FloatDistinctCountAccumulator, PrimitiveDistinctCountAccumulator,
};
use datafusion_physical_expr::expressions::{col, lit};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

/// Number of input values per benchmark run.
const NUM_VALUES: usize = 64 * 1024;

fn benchmark_count_distinct_int64(c: &mut Criterion) {
    let values =
        Arc::new(create_primitive_array::<Int64Type>(NUM_VALUES, 0.1)) as ArrayRef;
    let dtype = DataType::Int64;

    c.bench_function("count_distinct_int64", |b| {
        b.iter_batched(
            || {
                PrimitiveDistinctCountAccumulator::<Int64Type, HashSet<i64, RandomState>>::new(
                    &dtype,
                )
            },
            |mut acc| {
                acc.update_batch(std::slice::from_ref(&values)).unwrap();
                black_box(acc.evaluate().unwrap());
            },
            BatchSize::SmallInput,
        )
    });
}

fn benchmark_count_distinct_float64(c: &mut Criterion) {
    let values =
        Arc::new(create_primitive_array::<Float64Type>(NUM_VALUES, 0.1)) as ArrayRef;
    let dtype = DataType::Float64;

    c.bench_function("count_distinct_float64", |b| {
        b.iter_batched(
            || FloatDistinctCountAccumulator::<Float64Type>::new(&dtype),
            |mut acc| {
                acc.update_batch(std::slice::from_ref(&values)).unwrap();
                black_box(acc.evaluate().unwrap());
            },
            BatchSize::SmallInput,
        )
    });
}

fn create_distinct_median_accumulator(data_type: &DataType) -> Box<dyn Accumulator> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        data_type.clone(),
        true,
    )]));
    let value_expr = col("value", &schema).unwrap();
    let value_field = value_expr.return_field(&schema).unwrap();
    let exprs = vec![value_expr];
    let expr_fields = vec![value_field];
    let return_field = Arc::new(Field::new("median", data_type.clone(), true));

    let args = AccumulatorArgs {
        return_field,
        schema: &schema,
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name: "median",
        is_distinct: true,
        exprs: &exprs,
        expr_fields: &expr_fields,
    };

    Median::new().accumulator(args).unwrap()
}

fn benchmark_distinct_median(c: &mut Criterion) {
    let values =
        Arc::new(create_primitive_array::<Float64Type>(NUM_VALUES, 0.05)) as ArrayRef;

    c.bench_function("median_distinct_float64", |b| {
        b.iter_batched(
            || create_distinct_median_accumulator(&DataType::Float64),
            |mut acc| {
                acc.update_batch(std::slice::from_ref(&values)).unwrap();
                black_box(acc.evaluate().unwrap());
            },
            BatchSize::SmallInput,
        )
    });
}

fn create_distinct_percentile_accumulator(
    data_type: &DataType,
    percentile: f64,
) -> Box<dyn Accumulator> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", data_type.clone(), true),
        Field::new("percentile", DataType::Float64, false),
    ]));
    let value_expr = col("value", &schema).unwrap();
    let percentile_expr = lit(ScalarValue::Float64(Some(percentile)));
    let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![value_expr.clone(), percentile_expr];

    let value_field = value_expr.return_field(&schema).unwrap();
    let percentile_field: FieldRef =
        Arc::new(Field::new("percentile", DataType::Float64, false));
    let expr_fields = vec![value_field, percentile_field];

    let return_field = Arc::new(Field::new("percentile_cont", data_type.clone(), true));

    let order_expr = PhysicalSortExpr {
        expr: exprs[0].clone(),
        options: SortOptions::default(),
    };
    let order_bys = vec![order_expr];

    let args = AccumulatorArgs {
        return_field,
        schema: &schema,
        ignore_nulls: false,
        order_bys: &order_bys,
        is_reversed: false,
        name: "percentile_cont",
        is_distinct: true,
        exprs: &exprs,
        expr_fields: &expr_fields,
    };

    PercentileCont::new().accumulator(args).unwrap()
}

fn benchmark_distinct_percentile(c: &mut Criterion) {
    let values =
        Arc::new(create_primitive_array::<Float64Type>(NUM_VALUES, 0.05)) as ArrayRef;

    c.bench_function("percentile_cont_distinct_float64", |b| {
        b.iter_batched(
            || create_distinct_percentile_accumulator(&DataType::Float64, 0.75),
            |mut acc| {
                acc.update_batch(std::slice::from_ref(&values)).unwrap();
                black_box(acc.evaluate().unwrap());
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    distinct_benches,
    benchmark_count_distinct_int64,
    benchmark_count_distinct_float64,
    benchmark_distinct_median,
    benchmark_distinct_percentile
);
criterion_main!(distinct_benches);
