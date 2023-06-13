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

use arrow_array::types::Int32Type;
use arrow_array::{Date32Array, Date64Array, Decimal128Array, DictionaryArray};
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
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use crate::{utils, TestContext};

#[cfg(feature = "avro")]
pub async fn register_avro_tables(ctx: &mut crate::TestContext) {
    use datafusion::prelude::AvroReadOptions;

    ctx.enable_testdir();

    let table_path = ctx.testdir_path().join("avro");
    std::fs::create_dir(&table_path).expect("failed to create avro table path");

    let testdata = datafusion::test_util::arrow_test_data();
    let alltypes_plain_file = format!("{testdata}/avro/alltypes_plain.avro");
    std::fs::copy(
        &alltypes_plain_file,
        format!("{}/alltypes_plain1.avro", table_path.display()),
    )
    .unwrap();
    std::fs::copy(
        &alltypes_plain_file,
        format!("{}/alltypes_plain2.avro", table_path.display()),
    )
    .unwrap();

    ctx.session_ctx()
        .register_avro(
            "alltypes_plain_multi_files",
            table_path.display().to_string().as_str(),
            AvroReadOptions::default(),
        )
        .await
        .unwrap();
}

pub async fn register_aggregate_tables(ctx: &SessionContext) {
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

pub async fn register_scalar_tables(ctx: &SessionContext) {
    register_nan_table(ctx)
}

/// Register a table with a NaN value (different than NULL, and can
/// not be created via SQL)
fn register_nan_table(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Float64, true)]));

    let data = RecordBatch::try_new(
        schema,
        vec![Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(f64::NAN),
        ]))],
    )
    .unwrap();
    ctx.register_batch("test_float", data).unwrap();
}

/// Generate a partitioned CSV file and register it with an execution context
pub async fn register_partition_table(test_ctx: &mut TestContext) {
    test_ctx.enable_testdir();
    let partition_count = 1;
    let file_extension = "csv";
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::UInt32, false),
        Field::new("c2", DataType::UInt64, false),
        Field::new("c3", DataType::Boolean, false),
    ]));
    // generate a partitioned file
    for partition in 0..partition_count {
        let filename = format!("partition-{partition}.{file_extension}");
        let file_path = test_ctx.testdir_path().join(filename);
        let mut file = File::create(file_path).unwrap();

        // generate some data
        for i in 0..=10 {
            let data = format!("{},{},{}\n", partition, i, i % 2 == 0);
            file.write_all(data.as_bytes()).unwrap()
        }
    }

    // register csv file with the execution context
    test_ctx
        .ctx
        .register_csv(
            "test_partition_table",
            test_ctx.testdir_path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await
        .unwrap();
}

pub async fn register_hashjoin_datatype_table(ctx: &SessionContext) {
    let t1_schema = Schema::new(vec![
        Field::new("c1", DataType::Date32, true),
        Field::new("c2", DataType::Date64, true),
        Field::new("c3", DataType::Decimal128(5, 2), true),
        Field::new(
            "c4",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]);
    let dict1: DictionaryArray<Int32Type> =
        vec!["abc", "def", "ghi", "jkl"].into_iter().collect();
    let t1_data = RecordBatch::try_new(
        Arc::new(t1_schema),
        vec![
            Arc::new(Date32Array::from(vec![Some(1), Some(2), None, Some(3)])),
            Arc::new(Date64Array::from(vec![
                Some(86400000),
                Some(172800000),
                Some(259200000),
                None,
            ])),
            Arc::new(
                Decimal128Array::from_iter_values([123, 45600, 78900, -12312])
                    .with_precision_and_scale(5, 2)
                    .unwrap(),
            ),
            Arc::new(dict1),
        ],
    )
    .unwrap();
    ctx.register_batch("hashjoin_datatype_table_t1", t1_data)
        .unwrap();

    let t2_schema = Schema::new(vec![
        Field::new("c1", DataType::Date32, true),
        Field::new("c2", DataType::Date64, true),
        Field::new("c3", DataType::Decimal128(10, 2), true),
        Field::new(
            "c4",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]);
    let dict2: DictionaryArray<Int32Type> = vec!["abc", "abcdefg", "qwerty", "qwe"]
        .into_iter()
        .collect();
    let t2_data = RecordBatch::try_new(
        Arc::new(t2_schema),
        vec![
            Arc::new(Date32Array::from(vec![Some(1), None, None, Some(3)])),
            Arc::new(Date64Array::from(vec![
                Some(86400000),
                None,
                Some(259200000),
                None,
            ])),
            Arc::new(
                Decimal128Array::from_iter_values([-12312, 10000000, 0, 78900])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(dict2),
        ],
    )
    .unwrap();
    ctx.register_batch("hashjoin_datatype_table_t2", t2_data)
        .unwrap();
}
