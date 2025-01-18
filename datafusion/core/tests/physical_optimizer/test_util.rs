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

//! Test utilities for physical optimizer tests

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::stream::{FileStreamProvider, StreamConfig, StreamTable};
use datafusion::error::Result;
use datafusion::prelude::{CsvReadOptions, SessionContext};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::{
    listing::PartitionedFile,
    physical_plan::{FileScanConfig, ParquetExec},
};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_expr_common::sort_expr::LexOrdering;

/// create a single parquet file that is sorted
pub(crate) fn parquet_exec_with_sort(
    output_ordering: Vec<LexOrdering>,
) -> Arc<ParquetExec> {
    ParquetExec::builder(
        FileScanConfig::new(ObjectStoreUrl::parse("test:///").unwrap(), schema())
            .with_file(PartitionedFile::new("x".to_string(), 100))
            .with_output_ordering(output_ordering),
    )
    .build_arc()
}

pub(crate) fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
        Field::new("d", DataType::Int32, true),
        Field::new("e", DataType::Boolean, true),
    ]))
}

pub(crate) fn trim_plan_display(plan: &str) -> Vec<&str> {
    plan.split('\n')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect()
}

async fn register_current_csv(
    ctx: &SessionContext,
    table_name: &str,
    infinite: bool,
) -> Result<()> {
    let testdata = datafusion::test_util::arrow_test_data();
    let schema = datafusion::test_util::aggr_test_schema();
    let path = format!("{testdata}/csv/aggregate_test_100.csv");

    match infinite {
        true => {
            let source = FileStreamProvider::new_file(schema, path.into());
            let config = StreamConfig::new(Arc::new(source));
            ctx.register_table(table_name, Arc::new(StreamTable::new(Arc::new(config))))?;
        }
        false => {
            ctx.register_csv(table_name, &path, CsvReadOptions::new().schema(&schema))
                .await?;
        }
    }

    Ok(())
}

#[derive(Eq, PartialEq, Debug)]
pub enum SourceType {
    Unbounded,
    Bounded,
}

#[async_trait]
pub trait SqlTestCase {
    async fn register_table(&self, ctx: &SessionContext) -> Result<()>;
    fn expect_fail(&self) -> bool;
}

/// [UnaryTestCase] is designed for single input [ExecutionPlan]s.
pub struct UnaryTestCase {
    pub source_type: SourceType,
    pub expect_fail: bool,
}

#[async_trait]
impl SqlTestCase for UnaryTestCase {
    async fn register_table(&self, ctx: &SessionContext) -> Result<()> {
        let table_is_infinite = self.source_type == SourceType::Unbounded;
        register_current_csv(ctx, "test", table_is_infinite).await?;
        Ok(())
    }

    fn expect_fail(&self) -> bool {
        self.expect_fail
    }
}
/// [BinaryTestCase] is designed for binary input [ExecutionPlan]s.
pub struct BinaryTestCase {
    pub source_types: (SourceType, SourceType),
    pub expect_fail: bool,
}

#[async_trait]
impl SqlTestCase for BinaryTestCase {
    async fn register_table(&self, ctx: &SessionContext) -> Result<()> {
        let left_table_is_infinite = self.source_types.0 == SourceType::Unbounded;
        let right_table_is_infinite = self.source_types.1 == SourceType::Unbounded;
        register_current_csv(ctx, "left", left_table_is_infinite).await?;
        register_current_csv(ctx, "right", right_table_is_infinite).await?;
        Ok(())
    }

    fn expect_fail(&self) -> bool {
        self.expect_fail
    }
}

pub struct QueryCase {
    pub sql: String,
    pub cases: Vec<Arc<dyn SqlTestCase>>,
    pub error_operator: String,
}

impl QueryCase {
    /// Run the test cases
    pub async fn run(&self) -> Result<()> {
        for case in &self.cases {
            let ctx = SessionContext::new();
            case.register_table(&ctx).await?;
            let error = if case.expect_fail() {
                Some(&self.error_operator)
            } else {
                None
            };
            self.run_case(ctx, error).await?;
        }
        Ok(())
    }
    async fn run_case(&self, ctx: SessionContext, error: Option<&String>) -> Result<()> {
        let dataframe = ctx.sql(self.sql.as_str()).await?;
        let plan = dataframe.create_physical_plan().await;
        if let Some(error) = error {
            let plan_error = plan.unwrap_err();
            assert!(
                plan_error.to_string().contains(error.as_str()),
                "plan_error: {:?} doesn't contain message: {:?}",
                plan_error,
                error.as_str()
            );
        } else {
            assert!(plan.is_ok())
        }
        Ok(())
    }
}
