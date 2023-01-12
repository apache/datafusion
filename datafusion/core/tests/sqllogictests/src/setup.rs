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

use datafusion::{
    arrow::{
        array::{
            ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
            Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
        },
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    prelude::{CsvReadOptions, SessionContext},
    test_util,
};
use std::sync::Arc;

use crate::utils;

pub async fn register_aggregate_tables(ctx: &SessionContext) {
    register_aggregate_csv_by_sql(ctx).await;
    register_aggregate_test_100(ctx).await;
    register_decimal_table(ctx);
    register_median_test_tables(ctx);
    register_test_data(ctx);
}

fn register_median_test_tables(ctx: &SessionContext) {
    // Register median tables
    let items: Vec<(&str, DataType, ArrayRef)> = vec![
        (
            "i8",
            DataType::Int8,
            Arc::new(Int8Array::from(vec![i8::MIN, i8::MIN, 100, i8::MAX])),
        ),
        (
            "i16",
            DataType::Int16,
            Arc::new(Int16Array::from(vec![i16::MIN, i16::MIN, 100, i16::MAX])),
        ),
        (
            "i32",
            DataType::Int32,
            Arc::new(Int32Array::from(vec![i32::MIN, i32::MIN, 100, i32::MAX])),
        ),
        (
            "i64",
            DataType::Int64,
            Arc::new(Int64Array::from(vec![i64::MIN, i64::MIN, 100, i64::MAX])),
        ),
        (
            "u8",
            DataType::UInt8,
            Arc::new(UInt8Array::from(vec![u8::MIN, u8::MIN, 100, u8::MAX])),
        ),
        (
            "u16",
            DataType::UInt16,
            Arc::new(UInt16Array::from(vec![u16::MIN, u16::MIN, 100, u16::MAX])),
        ),
        (
            "u32",
            DataType::UInt32,
            Arc::new(UInt32Array::from(vec![u32::MIN, u32::MIN, 100, u32::MAX])),
        ),
        (
            "u64",
            DataType::UInt64,
            Arc::new(UInt64Array::from(vec![u64::MIN, u64::MIN, 100, u64::MAX])),
        ),
        (
            "f32",
            DataType::Float32,
            Arc::new(Float32Array::from(vec![1.1, 4.4, 5.5, 3.3, 2.2])),
        ),
        (
            "f64",
            DataType::Float64,
            Arc::new(Float64Array::from(vec![1.1, 4.4, 5.5, 3.3, 2.2])),
        ),
        (
            "f64_nan",
            DataType::Float64,
            Arc::new(Float64Array::from(vec![1.1, f64::NAN, f64::NAN, f64::NAN])),
        ),
    ];

    for (name, data_type, values) in items {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", data_type, false)])),
            vec![values],
        )
        .unwrap();
        let table_name = &format!("median_{name}");
        ctx.register_batch(table_name, batch).unwrap();
    }
}

async fn register_aggregate_csv_by_sql(ctx: &SessionContext) {
    let test_data = datafusion::test_util::arrow_test_data();

    let df = ctx
        .sql(&format!(
            "
    CREATE EXTERNAL TABLE aggregate_test_100_by_sql (
        c1  VARCHAR NOT NULL,
        c2  TINYINT NOT NULL,
        c3  SMALLINT NOT NULL,
        c4  SMALLINT NOT NULL,
        c5  INTEGER NOT NULL,
        c6  BIGINT NOT NULL,
        c7  SMALLINT NOT NULL,
        c8  INT NOT NULL,
        c9  INT UNSIGNED NOT NULL,
        c10 BIGINT UNSIGNED NOT NULL,
        c11 FLOAT NOT NULL,
        c12 DOUBLE NOT NULL,
        c13 VARCHAR NOT NULL
    )
    STORED AS CSV
    WITH HEADER ROW
    LOCATION '{test_data}/csv/aggregate_test_100.csv'
    "
        ))
        .await
        .expect("Creating dataframe for CREATE EXTERNAL TABLE");

    // Mimic the CLI and execute the resulting plan -- even though it
    // is effectively a no-op (returns zero rows)
    let results = df.collect().await.expect("Executing CREATE EXTERNAL TABLE");
    assert!(
        results.is_empty(),
        "Expected no rows from executing CREATE EXTERNAL TABLE"
    );
}

fn register_test_data(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int64, true),
        Field::new("c2", DataType::Int64, true),
    ]));

    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![
                Some(0),
                Some(1),
                None,
                Some(3),
                Some(3),
            ])),
            Arc::new(Int64Array::from(vec![
                None,
                Some(1),
                Some(1),
                Some(2),
                Some(2),
            ])),
        ],
    )
    .unwrap();

    ctx.register_batch("test", data).unwrap();
}

fn register_decimal_table(ctx: &SessionContext) {
    let batch_decimal = utils::make_decimal();
    let schema = batch_decimal.schema();
    let partitions = vec![vec![batch_decimal]];
    let provider = Arc::new(MemTable::try_new(schema, partitions).unwrap());
    ctx.register_table("d_table", provider).unwrap();
}

async fn register_aggregate_test_100(ctx: &SessionContext) {
    let test_data = datafusion::test_util::arrow_test_data();
    let schema = test_util::aggr_test_schema();
    ctx.register_csv(
        "aggregate_test_100",
        &format!("{test_data}/csv/aggregate_test_100.csv"),
        CsvReadOptions::new().schema(&schema),
    )
    .await
    .unwrap();
}
