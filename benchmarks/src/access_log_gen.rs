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
use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result};
use datafusion::dataframe::DataFrameWriteOptions;
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
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
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

        let ctx = SessionContext::new();

        // use the COPY command to write data to the file
        let output_file = path.join("1.parquet");
        let output_filename = output_file.to_str().expect("non utf8 path name");
        let options = DataFrameWriteOptions::new().with_single_file_output(true);

        let writer_properties = WriterProperties::builder()
            //.set_data_page_size_limit(1024 * 1024)
            //.set_write_batch_size(1024 * 1024)
            //.set_max_row_group_size(1024 * 1024)
            .build();

        let seed = 1;
        let provider = access_log_provider(scale_factor, seed)?;
        println!("writing to parquet...");
        let res = ctx
            .read_table(provider)?
            .write_parquet(output_filename, options, Some(writer_properties))
            .await?;
        println!("done!");
        println!("{}", pretty_format_batches(&res)?);

        Ok(())
    }
}

fn access_log_provider(scale_factor: f32, seed: u64) -> Result<Arc<dyn TableProvider>> {
    println!("Using scale_factor={} and seed={}", scale_factor, seed);

    let generator = AccessLogGenerator::new();
    //.with_seed(seed);
    let num_batches = (100_f32 * scale_factor) as usize;

    let stream = AccessLogStream::new(generator, num_batches);

    let table = StreamingTable::try_new(stream.schema().clone(), vec![Arc::new(stream)])?;

    Ok(Arc::new(table))
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
