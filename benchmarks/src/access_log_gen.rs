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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::{plan_datafusion_err, plan_err, ScalarValue};
use std::any::Any;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;
use test_utils::AccessLogGenerator;

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
        ctx.register_udtf("access_log_gen", Arc::new(AccessLogTableFunction::new()));

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
struct AccessLogTableFunction {}

impl AccessLogTableFunction {
    fn new() -> Self {
        Self {}
    }
}

impl TableFunctionImpl for AccessLogTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if args.len() != 2 {
            return plan_err!(
                "access_log_gen requires two arguments (scale_factor, seed), {} provided",
                args.len()
            );
        }

        let scale_factor = match &args[0] {
            Expr::Literal(ScalarValue::Float32(Some(v))) => *v as f64,
            Expr::Literal(ScalarValue::Float64(Some(v))) => *v,
            _ => {
                return plan_err!("scale_factor must be a float literal, got {}", args[0])
            }
        };

        let seed = match &args[1] {
            Expr::Literal(ScalarValue::Int64(Some(v))) => *v as u64,
            Expr::Literal(ScalarValue::UInt64(Some(v))) => *v,
            _ => return plan_err!("seed must be a int literal, got {}", args[1]),
        };

        println!("Using scale_factor={} and seed={}", scale_factor, seed);

        let generator = AccessLogGenerator::new();
        //.with_seed(seed);
        let num_batches = (100_f64 * scale_factor) as usize;

        let stream = AccessLogStream::new(generator, num_batches);

        let table =
            StreamingTable::try_new(stream.schema().clone(), vec![Arc::new(stream)])?;

        Ok(Arc::new(table))
    }
}

struct AccessLogStream {
    schema: SchemaRef,
    generator: AccessLogGenerator,
    num_batches: usize,
}

impl AccessLogStream {
    fn new(generator: AccessLogGenerator, num_batches: usize) -> Self {
        Self {
            schema: generator.schema(),
            generator,
            num_batches,
        }
    }
}

impl PartitionStream for AccessLogStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        todo!()
    }
}
