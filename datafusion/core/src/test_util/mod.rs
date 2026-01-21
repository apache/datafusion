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

//! Utility functions to make testing DataFusion based crates easier

#[cfg(feature = "parquet")]
pub mod parquet;

pub mod csv;

use futures::Stream;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::catalog::{TableProvider, TableProviderFactory};
use crate::dataframe::DataFrame;
use crate::datasource::stream::{FileStreamProvider, StreamConfig, StreamTable};
use crate::datasource::{empty::EmptyTable, provider_as_source};
use crate::error::Result;
use crate::execution::session_state::CacheFactory;
use crate::logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE};
use crate::physical_plan::ExecutionPlan;
use crate::prelude::{CsvReadOptions, SessionContext};

use crate::execution::{SendableRecordBatchStream, SessionState, SessionStateBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_catalog::Session;
use datafusion_common::{DFSchemaRef, TableReference};
use datafusion_expr::{
    CreateExternalTable, Expr, LogicalPlan, SortExpr, TableType,
    UserDefinedLogicalNodeCore,
};
use std::pin::Pin;

use async_trait::async_trait;

use tempfile::TempDir;
// backwards compatibility
#[cfg(feature = "parquet")]
pub use datafusion_common::test_util::parquet_test_data;
pub use datafusion_common::test_util::{arrow_test_data, get_data_dir};

use crate::execution::RecordBatchStream;

/// Scan an empty data source, mainly used in tests
pub fn scan_empty(
    name: Option<&str>,
    table_schema: &Schema,
    projection: Option<Vec<usize>>,
) -> Result<LogicalPlanBuilder> {
    let table_schema = Arc::new(table_schema.clone());
    let provider = Arc::new(EmptyTable::new(table_schema));
    let name = TableReference::bare(name.unwrap_or(UNNAMED_TABLE));
    LogicalPlanBuilder::scan(name, provider_as_source(provider), projection)
}

/// Scan an empty data source with configured partition, mainly used in tests.
pub fn scan_empty_with_partitions(
    name: Option<&str>,
    table_schema: &Schema,
    projection: Option<Vec<usize>>,
    partitions: usize,
) -> Result<LogicalPlanBuilder> {
    let table_schema = Arc::new(table_schema.clone());
    let provider = Arc::new(EmptyTable::new(table_schema).with_partitions(partitions));
    let name = TableReference::bare(name.unwrap_or(UNNAMED_TABLE));
    LogicalPlanBuilder::scan(name, provider_as_source(provider), projection)
}

/// Get the schema for the aggregate_test_* csv files
pub fn aggr_test_schema() -> SchemaRef {
    let mut f1 = Field::new("c1", DataType::Utf8, false);
    f1.set_metadata(HashMap::from_iter(vec![("testing".into(), "test".into())]));
    let schema = Schema::new(vec![
        f1,
        Field::new("c2", DataType::UInt32, false),
        Field::new("c3", DataType::Int8, false),
        Field::new("c4", DataType::Int16, false),
        Field::new("c5", DataType::Int32, false),
        Field::new("c6", DataType::Int64, false),
        Field::new("c7", DataType::UInt8, false),
        Field::new("c8", DataType::UInt16, false),
        Field::new("c9", DataType::UInt32, false),
        Field::new("c10", DataType::UInt64, false),
        Field::new("c11", DataType::Float32, false),
        Field::new("c12", DataType::Float64, false),
        Field::new("c13", DataType::Utf8, false),
    ]);

    Arc::new(schema)
}

/// Register session context for the aggregate_test_100.csv file
pub async fn register_aggregate_csv(
    ctx: &SessionContext,
    table_name: &str,
) -> Result<()> {
    let schema = aggr_test_schema();
    let testdata = arrow_test_data();
    ctx.register_csv(
        table_name,
        &format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::new().schema(schema.as_ref()),
    )
    .await?;
    Ok(())
}

/// Create a table from the aggregate_test_100.csv file with the specified name
pub async fn test_table_with_name(name: &str) -> Result<DataFrame> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx, name).await?;
    ctx.table(name).await
}

/// Create a table from the aggregate_test_100.csv file with the name "aggregate_test_100"
pub async fn test_table() -> Result<DataFrame> {
    test_table_with_name("aggregate_test_100").await
}

/// Execute SQL and return results
#[cfg(feature = "sql")]
pub async fn plan_and_collect(
    ctx: &SessionContext,
    sql: &str,
) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

/// Generate CSV partitions within the supplied directory
pub fn populate_csv_partitions(
    tmp_dir: &TempDir,
    partition_count: usize,
    file_extension: &str,
) -> Result<SchemaRef> {
    // define schema for data source (csv file)
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::UInt32, false),
        Field::new("c2", DataType::UInt64, false),
        Field::new("c3", DataType::Boolean, false),
    ]));

    // generate a partitioned file
    for partition in 0..partition_count {
        let filename = format!("partition-{partition}.{file_extension}");
        let file_path = tmp_dir.path().join(filename);
        let mut file = File::create(file_path)?;

        // generate some data
        for i in 0..=10 {
            let data = format!("{},{},{}\n", partition, i, i % 2 == 0);
            file.write_all(data.as_bytes())?;
        }
    }

    Ok(schema)
}

/// TableFactory for tests
#[derive(Default, Debug)]
pub struct TestTableFactory {}

#[async_trait]
impl TableProviderFactory for TestTableFactory {
    async fn create(
        &self,
        _: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        Ok(Arc::new(TestTableProvider {
            url: cmd.location.to_string(),
            schema: Arc::clone(cmd.schema.inner()),
        }))
    }
}

/// TableProvider for testing purposes
#[derive(Debug)]
pub struct TestTableProvider {
    /// URL of table files or folder
    pub url: String,
    /// test table schema
    pub schema: SchemaRef,
}

impl TestTableProvider {}

#[async_trait]
impl TableProvider for TestTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        unimplemented!("TestTableProvider is a stub for testing.")
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!("TestTableProvider is a stub for testing.")
    }
}

/// This function creates an unbounded sorted file for testing purposes.
pub fn register_unbounded_file_with_ordering(
    ctx: &SessionContext,
    schema: SchemaRef,
    file_path: &Path,
    table_name: &str,
    file_sort_order: Vec<Vec<SortExpr>>,
) -> Result<()> {
    let source = FileStreamProvider::new_file(schema, file_path.into());
    let config = StreamConfig::new(Arc::new(source)).with_order(file_sort_order);

    // Register table:
    ctx.register_table(table_name, Arc::new(StreamTable::new(Arc::new(config))))?;
    Ok(())
}

/// Creates a bounded stream that emits the same record batch a specified number of times.
/// This is useful for testing purposes.
pub fn bounded_stream(
    record_batch: RecordBatch,
    limit: usize,
) -> SendableRecordBatchStream {
    Box::pin(BoundedStream {
        record_batch,
        count: 0,
        limit,
    })
}

struct BoundedStream {
    record_batch: RecordBatch,
    count: usize,
    limit: usize,
}

impl Stream for BoundedStream {
    type Item = Result<RecordBatch, crate::error::DataFusionError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.count >= self.limit {
            Poll::Ready(None)
        } else {
            self.count += 1;
            Poll::Ready(Some(Ok(self.record_batch.clone())))
        }
    }
}

impl RecordBatchStream for BoundedStream {
    fn schema(&self) -> SchemaRef {
        self.record_batch.schema()
    }
}

#[derive(Hash, Eq, PartialEq, PartialOrd, Debug)]
struct CacheNode {
    input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for CacheNode {
    fn name(&self) -> &str {
        "CacheNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CacheNode")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        Ok(Self {
            input: inputs[0].clone(),
        })
    }
}

#[derive(Debug)]
struct TestCacheFactory {}

impl CacheFactory for TestCacheFactory {
    fn create(
        &self,
        plan: LogicalPlan,
        _session_state: &SessionState,
    ) -> Result<LogicalPlan> {
        Ok(LogicalPlan::Extension(datafusion_expr::Extension {
            node: Arc::new(CacheNode { input: plan }),
        }))
    }
}

/// Create a test table registered to a session context with an associated cache factory
pub async fn test_table_with_cache_factory() -> Result<DataFrame> {
    let session_state = SessionStateBuilder::new()
        .with_cache_factory(Some(Arc::new(TestCacheFactory {})))
        .build();
    let ctx = SessionContext::new_with_state(session_state);
    let name = "aggregate_test_100";
    register_aggregate_csv(&ctx, name).await?;
    ctx.table(name).await
}
