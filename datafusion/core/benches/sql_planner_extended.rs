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

use arrow::array::{ArrayRef, RecordBatch};
use arrow_schema::DataType;
use arrow_schema::TimeUnit::Nanosecond;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_catalog::MemTable;
use datafusion_common::ScalarValue;
use datafusion_expr::Expr::Literal;
use datafusion_expr::{cast, col, lit, not, try_cast, when};
use datafusion_functions::expr_fn::{
    btrim, length, regexp_like, regexp_replace, to_timestamp, upper,
};
use std::hint::black_box;
use std::ops::Rem;
use std::sync::Arc;
use tokio::runtime::Runtime;

// This benchmark suite is designed to test the performance of
// logical planning with a large plan containing unions, many columns
// with a variety of operations in it.
//
// Since it is (currently) very slow to execute it has been separated
// out from the sql_planner benchmark suite to this file.
//
// See https://github.com/apache/datafusion/issues/17261 for details.

/// Registers a table like this:
/// c0,c1,c2...,c99
/// "0","100"..."9900"
/// "0","200"..."19800"
/// "0","300"..."29700"
fn register_string_table(ctx: &SessionContext, num_columns: usize, num_rows: usize) {
    // ("c0", ["0", "0", ...])
    // ("c1": ["100", "200", ...])
    // etc
    let iter = (0..num_columns).map(|i| i as u64).map(|i| {
        let array: ArrayRef = Arc::new(arrow::array::StringViewArray::from_iter_values(
            (0..num_rows)
                .map(|j| format!("c{}", j as u64 * 100 + i))
                .collect::<Vec<_>>(),
        ));
        (format!("c{i}"), array)
    });
    let batch = RecordBatch::try_from_iter(iter).unwrap();
    let schema = batch.schema();
    let partitions = vec![vec![batch]];

    // create the table
    let table = MemTable::try_new(schema, partitions).unwrap();

    ctx.register_table("t", Arc::new(table)).unwrap();
}

/// Build a dataframe for testing logical plan optimization
fn build_test_data_frame(ctx: &SessionContext, rt: &Runtime) -> DataFrame {
    register_string_table(ctx, 100, 1000);

    rt.block_on(async {
        let mut df = ctx.table("t").await.unwrap();
        // add some columns in
        for i in 100..150 {
            df = df
                .with_column(&format!("c{i}"), Literal(ScalarValue::Utf8(None), None))
                .unwrap();
        }
        // add in some columns with string encoded timestamps
        for i in 150..175 {
            df = df
                .with_column(
                    &format!("c{i}"),
                    Literal(ScalarValue::Utf8(Some("2025-08-21 09:43:17".into())), None),
                )
                .unwrap();
        }
        // do a bunch of ops on the columns
        for i in 0..175 {
            // trim the columns
            df = df
                .with_column(&format!("c{i}"), btrim(vec![col(format!("c{i}"))]))
                .unwrap();
        }

        for i in 0..175 {
            let c_name = format!("c{i}");
            let c = col(&c_name);

            // random ops
            if i % 5 == 0 && i < 150 {
                // the actual ops here are largely unimportant as they are just a sample
                // of ops that could occur on a dataframe
                df = df
                    .with_column(&c_name, cast(c.clone(), DataType::Utf8))
                    .unwrap()
                    .with_column(
                        &c_name,
                        when(
                            cast(c.clone(), DataType::Int32).gt(lit(135)),
                            cast(
                                cast(c.clone(), DataType::Int32) - lit(i + 3),
                                DataType::Utf8,
                            ),
                        )
                        .otherwise(c.clone())
                        .unwrap(),
                    )
                    .unwrap()
                    .with_column(
                        &c_name,
                        when(
                            c.clone().is_not_null().and(
                                cast(c.clone(), DataType::Int32)
                                    .between(lit(120), lit(130)),
                            ),
                            Literal(ScalarValue::Utf8(None), None),
                        )
                        .otherwise(
                            when(
                                c.clone().is_not_null().and(regexp_like(
                                    cast(c.clone(), DataType::Utf8View),
                                    lit("[0-9]*"),
                                    None,
                                )),
                                upper(c.clone()),
                            )
                            .otherwise(c.clone())
                            .unwrap(),
                        )
                        .unwrap(),
                    )
                    .unwrap()
                    .with_column(
                        &c_name,
                        when(
                            c.clone().is_not_null().and(
                                cast(c.clone(), DataType::Int32)
                                    .between(lit(90), lit(100)),
                            ),
                            cast(c.clone(), DataType::Utf8View),
                        )
                        .otherwise(Literal(ScalarValue::Date32(None), None))
                        .unwrap(),
                    )
                    .unwrap()
                    .with_column(
                        &c_name,
                        when(
                            c.clone().is_not_null().and(
                                cast(c.clone(), DataType::Int32).rem(lit(10)).gt(lit(7)),
                            ),
                            regexp_replace(
                                cast(c.clone(), DataType::Utf8View),
                                lit("1"),
                                lit("a"),
                                None,
                            ),
                        )
                        .otherwise(Literal(ScalarValue::Date32(None), None))
                        .unwrap(),
                    )
                    .unwrap()
            }
            if i >= 150 {
                df = df
                    .with_column(
                        &c_name,
                        try_cast(
                            to_timestamp(vec![c.clone(), lit("%Y-%m-%d %H:%M:%S")]),
                            DataType::Timestamp(Nanosecond, Some("UTC".into())),
                        ),
                    )
                    .unwrap()
                    .with_column(&c_name, try_cast(c.clone(), DataType::Date32))
                    .unwrap()
            }

            // add in a few unions
            if i % 30 == 0 {
                let df1 = df
                    .clone()
                    .filter(length(c.clone()).gt(lit(2)))
                    .unwrap()
                    .with_column(&format!("c{i}_filtered"), lit(true))
                    .unwrap();
                let df2 = df
                    .filter(not(length(c.clone()).gt(lit(2))))
                    .unwrap()
                    .with_column(&format!("c{i}_filtered"), lit(false))
                    .unwrap();

                df = df1.union_by_name(df2).unwrap()
            }
        }

        df
    })
}

fn criterion_benchmark(c: &mut Criterion) {
    let ctx = SessionContext::new();
    let rt = Runtime::new().unwrap();

    // validate logical plan optimize performance
    // https://github.com/apache/datafusion/issues/17261

    let df = build_test_data_frame(&ctx, &rt);

    c.bench_function("logical_plan_optimize", |b| {
        b.iter(|| {
            let df_clone = df.clone();
            black_box(rt.block_on(async { df_clone.into_optimized_plan().unwrap() }));
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
