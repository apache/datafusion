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

use datafusion::arrow::util::pretty;

use datafusion::error::Result;
use datafusion::physical_plan::avro::AvroReadOptions;
use datafusion::prelude::*;

/// This example demonstrates executing a simple query against an Arrow data source (Avro) and
/// fetching results
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let mut ctx = ExecutionContext::new();

    let testdata = datafusion::arrow::util::test_util::arrow_test_data();

    // register avro file with the execution context
    let avro_file = &format!("{}/avro/alltypes_plain.avro", testdata);
    ctx.register_avro("alltypes_plain", avro_file, AvroReadOptions::default())?;

    // execute the query
    let df = ctx.sql(
        "SELECT int_col, double_col, CAST(date_string_col as VARCHAR) \
        FROM alltypes_plain \
        WHERE id > 1 AND tinyint_col < double_col",
    )?;
    let results = df.collect().await?;

    // print the results
    pretty::print_batches(&results)?;

    Ok(())
}
