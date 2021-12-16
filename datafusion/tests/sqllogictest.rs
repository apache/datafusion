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

// This file does sql logical test
// Ref: https://sqlite.org/sqllogictest/doc/trunk/about.wiki

use std::fs::File;
use std::io::Read;
use std::path::Path;
use sqllogictest::{DB, Runner};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::ExecutionContext;


struct DataFusionDB {}

struct PostgresDB {}

#[async_trait]
impl DB for DataFusionDB {
    type Error = DataFusionError;

    async fn run(&self, sql: &str) -> Result<String> {
        let mut ctx = ExecutionContext::new();
        let df = ctx.sql(sql).await?;
        let res = df.collect().await?;
        dbg!(res);
        Ok("".to_string())
    }
}

#[tokio::test]
async fn main_test() -> Result<()>{
    let mut df_runner = Runner::new(DataFusionDB{});
    let mut file = File::open(Path::new("tests/sqllogictest-proto/select1.txt")).expect("Unable to open the file");
    let mut contents = String::new();
    file.read_to_string(&mut contents).expect("Unable to read the file");
    df_runner.run_script(contents.as_str());
    Ok(())
}