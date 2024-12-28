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
// under the License.use arrow::array::{ArrayRef, BooleanBuilder, Int32Builder, RecordBatch, StringBuilder};

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::builder::{Decimal128Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion::execution::TaskContext;
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_spark_expr::AvgDecimal;
use datafusion_comet_spark_expr::SumDecimal;
use datafusion_expr::AggregateUDF;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::Column;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate");
    let num_rows = 8192;
    let batch = create_record_batch(num_rows);
    let mut batches = Vec::new();
    for _ in 0..10 {
        batches.push(batch.clone());
    }
    let partitions = &[batches];
    let c0: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c0", 0));
    let c1: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c1", 1));

    let rt = Runtime::new().unwrap();

    group.bench_function("avg_decimal_datafusion", |b| {
        let datafusion_sum_decimal = avg_udaf();
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                partitions,
                c0.clone(),
                c1.clone(),
                datafusion_sum_decimal.clone(),
                "avg",
            ))
        })
    });

    group.bench_function("avg_decimal_comet", |b| {
        let comet_avg_decimal = Arc::new(AggregateUDF::new_from_impl(AvgDecimal::new(
            DataType::Decimal128(38, 10),
            DataType::Decimal128(38, 10),
        )));
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                partitions,
                c0.clone(),
                c1.clone(),
                comet_avg_decimal.clone(),
                "avg",
            ))
        })
    });

    group.bench_function("sum_decimal_datafusion", |b| {
        let datafusion_sum_decimal = sum_udaf();
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                partitions,
                c0.clone(),
                c1.clone(),
                datafusion_sum_decimal.clone(),
                "sum",
            ))
        })
    });

    group.bench_function("sum_decimal_comet", |b| {
        let comet_sum_decimal = Arc::new(AggregateUDF::new_from_impl(
            SumDecimal::try_new(DataType::Decimal128(38, 10)).unwrap(),
        ));
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                partitions,
                c0.clone(),
                c1.clone(),
                comet_sum_decimal.clone(),
                "sum",
            ))
        })
    });

    group.finish();
}

async fn agg_test(
    partitions: &[Vec<RecordBatch>],
    c0: Arc<dyn PhysicalExpr>,
    c1: Arc<dyn PhysicalExpr>,
    aggregate_udf: Arc<AggregateUDF>,
    alias: &str,
) {
    let schema = &partitions[0][0].schema();
    let scan: Arc<dyn ExecutionPlan> =
        Arc::new(MemoryExec::try_new(partitions, Arc::clone(schema), None).unwrap());
    let aggregate = create_aggregate(scan, c0.clone(), c1.clone(), schema, aggregate_udf, alias);
    let mut stream = aggregate
        .execute(0, Arc::new(TaskContext::default()))
        .unwrap();
    while let Some(batch) = stream.next().await {
        let _batch = batch.unwrap();
    }
}

fn create_aggregate(
    scan: Arc<dyn ExecutionPlan>,
    c0: Arc<dyn PhysicalExpr>,
    c1: Arc<dyn PhysicalExpr>,
    schema: &SchemaRef,
    aggregate_udf: Arc<AggregateUDF>,
    alias: &str,
) -> Arc<AggregateExec> {
    let aggr_expr = AggregateExprBuilder::new(aggregate_udf, vec![c1])
        .schema(schema.clone())
        .alias(alias)
        .with_ignore_nulls(false)
        .with_distinct(false)
        .build()
        .unwrap();

    Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![(c0, "c0".to_string())]),
            vec![aggr_expr.into()],
            vec![None], // no filter expressions
            scan,
            Arc::clone(schema),
        )
        .unwrap(),
    )
}

fn create_record_batch(num_rows: usize) -> RecordBatch {
    let mut decimal_builder = Decimal128Builder::with_capacity(num_rows);
    let mut string_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
    for i in 0..num_rows {
        decimal_builder.append_value(i as i128);
        string_builder.append_value(format!("this is string #{}", i % 1024));
    }
    let decimal_array = Arc::new(decimal_builder.finish());
    let string_array = Arc::new(string_builder.finish());

    let mut fields = vec![];
    let mut columns: Vec<ArrayRef> = vec![];

    // string column
    fields.push(Field::new("c0", DataType::Utf8, false));
    columns.push(string_array);

    // decimal column
    fields.push(Field::new("c1", DataType::Decimal128(38, 10), false));
    columns.push(decimal_array);

    let schema = Schema::new(fields);
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

fn config() -> Criterion {
    Criterion::default()
        .measurement_time(Duration::from_millis(500))
        .warm_up_time(Duration::from_millis(500))
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
