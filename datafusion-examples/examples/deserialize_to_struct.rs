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

use datafusion::error::Result;
use datafusion::prelude::*;
use serde::Deserialize;

/// This example shows that it is possible to convert query results into Rust structs .
/// It will collect the query results into RecordBatch, then convert it to serde_json::Value.
/// Then, serde_json::Value is turned into Rust's struct.
/// Any datatype with `Deserialize` implemeneted works.
#[tokio::main]
async fn main() -> Result<()> {
    let data_list = Data::new().await?;
    println!("{:#?}", data_list);
    Ok(())
}

#[derive(Deserialize, Debug)]
struct Data {
    #[allow(dead_code)]
    int_col: i64,
    #[allow(dead_code)]
    double_col: f64,
}

impl Data {
    pub async fn new() -> Result<Vec<Self>> {
        // this group is almost the same as the one you find it in parquet_sql.rs
        let batches = {
            let ctx = SessionContext::new();

            let testdata = datafusion::test_util::parquet_test_data();

            ctx.register_parquet(
                "alltypes_plain",
                &format!("{}/alltypes_plain.parquet", testdata),
                ParquetReadOptions::default(),
            )
            .await?;

            let df = ctx
                .sql("SELECT int_col, double_col FROM alltypes_plain")
                .await?;

            df.show().await?;

            df.collect().await?
        };
        // converts it to serde_json type and then convert that into Rust type
        let list =
            datafusion::arrow::json::writer::record_batches_to_json_rows(&batches[..])?
                .into_iter()
                .map(|val| serde_json::from_value(serde_json::Value::Object(val)))
                .take_while(|val| val.is_ok())
                .map(|val| val.unwrap())
                .collect();

        Ok(list)
    }
}
