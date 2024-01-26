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

use datafusion::common::Result;
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;

/// Creates synthetic data that mimic's a web server access log
/// dataset
///
/// Example
///
/// dfbench access-log-gen   --path ./data/access_logs --scale-factor 1.0
///
///
/// Generates synthetic dataset in `./data/access_logs`. The size
/// of the dataset can be controlled through the `size_factor`
/// (with the default value of `1.0` generating a ~1GB parquet file).
///
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Path to folder where access log files will be generated
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,

    /// Total size of each file in dataset. The default scale factor of 1.0 will generate roughly 1GB parquet files
    //#[structopt(long = "scale-factor", default_value = "1.0")]
    #[structopt(long = "scale-factor", default_value = "0.1")]
    scale_factor: f32,

    /// How many files should be created? 10 by default
    #[structopt(default_value = "10", short = "n", long = "num-files")]
    num_files: NonZeroUsize,

    /// Should the files be sorted or not? Defaults to false. If true, the files will be sorted by XX, YY, and timestamp.
    #[structopt(short = "s", long = "sorted")]
    sort: bool,
}

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        let Self {
            path,
            scale_factor,
            num_files,
            sort,
        } = self;

        std::fs::create_dir_all(&path)?;

        let mut ctx = SessionContext::new();
        ctx.register_udtf("access_log_gen", Arc::new(AccessLogTable::new()));

        // use the COPY command to write data to the file
        let output_file = path.join("1.parquet");
        let output_filename = output_file.to_str().expect("non utf8 path name");
        let seed = 1;
        let sql = format!(
            "COPY (SELECT * from access_log_gen({scale_factor}, {seed})) TO '{output_filename}'",
        );
        println!("Running sql: {}", sql);
        ctx.sql(&sql).await?.show().await
    }
}

/// User defined table function that generates access logs for a given seed
///
/// Usage:
/// access_log_gen(scale_factor, seed)
struct AccessLogTable {}

impl AccessLogTable {
    fn new() -> Self {
        Self {}
    }
}

impl TableFunctionImpl for AccessLogTable {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        todo!();

        //let generator = AccessLogGenerator::new();
        //let num_batches = 100_f32 * self.scale_factor;
    }
}
