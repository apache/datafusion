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

use arrow::array::AsArray;
use arrow::datatypes::{Float64Type, Int32Type};
use datafusion::error::Result;
use datafusion::prelude::*;
use futures::StreamExt;

/// This example shows that it is possible to convert query results into Rust structs .
#[tokio::main]
async fn main() -> Result<()> {
    let data_list = Data::new().await?;
    println!("{data_list:#?}");
    Ok(())
}

#[derive(Debug)]
struct Data {
    #[allow(dead_code)]
    int_col: i32,
    #[allow(dead_code)]
    double_col: f64,
}

impl Data {
    pub async fn new() -> Result<Vec<Self>> {
        // this group is almost the same as the one you find it in parquet_sql.rs
        let ctx = SessionContext::new();

        let testdata = datafusion::test_util::parquet_test_data();

        ctx.register_parquet(
            "alltypes_plain",
            &format!("{testdata}/alltypes_plain.parquet"),
            ParquetReadOptions::default(),
        )
        .await?;

        let df = ctx
            .sql("SELECT int_col, double_col FROM alltypes_plain")
            .await?;

        df.clone().show().await?;

        let mut stream = df.execute_stream().await?;
        let mut list = vec![];
        while let Some(b) = stream.next().await.transpose()? {
            let int_col = b.column(0).as_primitive::<Int32Type>();
            let float_col = b.column(1).as_primitive::<Float64Type>();

            for (i, f) in int_col.values().iter().zip(float_col.values()) {
                list.push(Data {
                    int_col: *i,
                    double_col: *f,
                })
            }
        }

        Ok(list)
    }
}
