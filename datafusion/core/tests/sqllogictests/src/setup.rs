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
        array::Float64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    prelude::{CsvReadOptions, SessionContext},
};
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use crate::TestContext;

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
