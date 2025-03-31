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

use std::sync::Arc;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::datasource::physical_plan::{FileGroup, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{
    execute_stream, visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor,
};
use futures::StreamExt;

/// Example of collecting metrics after execution by visiting the `ExecutionPlan`
#[tokio::main]
async fn main() {
    let ctx = SessionContext::new();

    let test_data = datafusion::test_util::parquet_test_data();

    // Configure listing options
    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options = ListingOptions::new(Arc::new(file_format));

    // First example were we use an absolute path, which requires no additional setup.
    let _ = ctx
        .register_listing_table(
            "my_table",
            &format!("file://{test_data}/alltypes_plain.parquet"),
            listing_options.clone(),
            None,
            None,
        )
        .await;

    let df = ctx.sql("SELECT * FROM my_table").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();

    // Create empty visitor
    let mut visitor = ParquetExecVisitor {
        file_groups: None,
        bytes_scanned: None,
    };

    // Make sure you execute the plan to collect actual execution statistics.
    // For example, in this example the `file_scan_config` is known without executing
    // but the `bytes_scanned` would be None if we did not execute.
    let mut batch_stream = execute_stream(plan.clone(), ctx.task_ctx()).unwrap();
    while let Some(batch) = batch_stream.next().await {
        println!("Batch rows: {}", batch.unwrap().num_rows());
    }

    visit_execution_plan(plan.as_ref(), &mut visitor).unwrap();

    println!(
        "ParquetExecVisitor bytes_scanned: {:?}",
        visitor.bytes_scanned
    );
    println!(
        "ParquetExecVisitor file_groups: {:?}",
        visitor.file_groups.unwrap()
    );
}

/// Define a struct with fields to hold the execution information you want to
/// collect. In this case, I want information on how many bytes were scanned
/// and `file_groups` from the FileScanConfig.
#[derive(Debug)]
struct ParquetExecVisitor {
    file_groups: Option<Vec<FileGroup>>,
    bytes_scanned: Option<MetricValue>,
}

impl ExecutionPlanVisitor for ParquetExecVisitor {
    type Error = DataFusionError;

    /// This function is called once for every node in the tree.
    /// Based on your needs implement either `pre_visit` (visit each node before its children/inputs)
    /// or `post_visit` (visit each node after its children/inputs)
    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        // If needed match on a specific `ExecutionPlan` node type
        if let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
            if let Some((file_config, _)) =
                data_source_exec.downcast_to_file_source::<ParquetSource>()
            {
                self.file_groups = Some(file_config.file_groups.clone());

                let metrics = match data_source_exec.metrics() {
                    None => return Ok(true),
                    Some(metrics) => metrics,
                };
                self.bytes_scanned = metrics.sum_by_name("bytes_scanned");
            }
        }
        Ok(true)
    }
}
