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

pub mod parquet;

use std::any::Any;
use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::datasource::provider::TableProviderFactory;
use crate::datasource::{empty::EmptyTable, provider_as_source, TableProvider};
use crate::error::Result;
use crate::execution::context::{SessionState, TaskContext};
use crate::execution::options::ReadOptions;
use crate::logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE};
use crate::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream,
};
use crate::prelude::{CsvReadOptions, SessionContext};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_common::{Statistics, TableReference};
use datafusion_expr::{CreateExternalTable, Expr, TableType};
use datafusion_physical_expr::PhysicalSortExpr;
use futures::Stream;

// backwards compatibility
pub use datafusion_common::test_util::{
    arrow_test_data, get_data_dir, parquet_test_data,
};

/// Compares formatted output of a record batch with an expected
/// vector of strings, with the result of pretty formatting record
/// batches. This is a macro so errors appear on the correct line
///
/// Designed so that failure output can be directly copy/pasted
/// into the test code as expected results.
///
/// Expects to be called about like this:
///
/// `assert_batch_eq!(expected_lines: &[&str], batches: &[RecordBatch])`
#[macro_export]
macro_rules! assert_batches_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        let expected_lines: Vec<String> =
            $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

        let formatted = $crate::arrow::util::pretty::pretty_format_batches_with_options(
            $CHUNKS,
            &$crate::common::format::DEFAULT_FORMAT_OPTIONS,
        )
        .unwrap()
        .to_string();

        let actual_lines: Vec<&str> = formatted.trim().lines().collect();

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

/// Compares formatted output of a record batch with an expected
/// vector of strings in a way that order does not matter.
/// This is a macro so errors appear on the correct line
///
/// Designed so that failure output can be directly copy/pasted
/// into the test code as expected results.
///
/// Expects to be called about like this:
///
/// `assert_batch_sorted_eq!(expected_lines: &[&str], batches: &[RecordBatch])`
#[macro_export]
macro_rules! assert_batches_sorted_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        let mut expected_lines: Vec<String> =
            $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

        // sort except for header + footer
        let num_lines = expected_lines.len();
        if num_lines > 3 {
            expected_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        let formatted = $crate::arrow::util::pretty::pretty_format_batches_with_options(
            $CHUNKS,
            &$crate::common::format::DEFAULT_FORMAT_OPTIONS,
        )
        .unwrap()
        .to_string();
        // fix for windows: \r\n -->

        let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();

        // sort except for header + footer
        let num_lines = actual_lines.len();
        if num_lines > 3 {
            actual_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

/// Scan an empty data source, mainly used in tests
pub fn scan_empty(
    name: Option<&str>,
    table_schema: &Schema,
    projection: Option<Vec<usize>>,
) -> Result<LogicalPlanBuilder> {
    let table_schema = Arc::new(table_schema.clone());
    let provider = Arc::new(EmptyTable::new(table_schema));
    let name = TableReference::bare(name.unwrap_or(UNNAMED_TABLE).to_string());
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
    let name = TableReference::bare(name.unwrap_or(UNNAMED_TABLE).to_string());
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

/// TableFactory for tests
pub struct TestTableFactory {}

#[async_trait]
impl TableProviderFactory for TestTableFactory {
    async fn create(
        &self,
        _: &SessionState,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        Ok(Arc::new(TestTableProvider {
            url: cmd.location.to_string(),
            schema: Arc::new(cmd.schema.as_ref().into()),
        }))
    }
}

/// TableProvider for testing purposes
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
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        unimplemented!("TestTableProvider is a stub for testing.")
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!("TestTableProvider is a stub for testing.")
    }
}

/// A mock execution plan that simply returns the provided data source characteristic
#[derive(Debug, Clone)]
pub struct UnboundedExec {
    batch_produce: Option<usize>,
    batch: RecordBatch,
    partitions: usize,
}
impl UnboundedExec {
    /// Create new exec that clones the given record batch to its output.
    ///
    /// Set `batch_produce` to `Some(n)` to emit exactly `n` batches per partition.
    pub fn new(
        batch_produce: Option<usize>,
        batch: RecordBatch,
        partitions: usize,
    ) -> Self {
        Self {
            batch_produce,
            batch,
            partitions,
        }
    }
}

impl DisplayAs for UnboundedExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "UnboundableExec: unbounded={}",
                    self.batch_produce.is_none(),
                )
            }
        }
    }
}

impl ExecutionPlan for UnboundedExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions)
    }

    fn unbounded_output(&self, _children: &[bool]) -> Result<bool> {
        Ok(self.batch_produce.is_none())
    }
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(UnboundedStream {
            batch_produce: self.batch_produce,
            count: 0,
            batch: self.batch.clone(),
        }))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[derive(Debug)]
struct UnboundedStream {
    batch_produce: Option<usize>,
    count: usize,
    batch: RecordBatch,
}

impl Stream for UnboundedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(val) = self.batch_produce {
            if val <= self.count {
                return Poll::Ready(None);
            }
        }
        self.count += 1;
        Poll::Ready(Some(Ok(self.batch.clone())))
    }
}

impl RecordBatchStream for UnboundedStream {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
}

/// This function creates an unbounded sorted file for testing purposes.
pub async fn register_unbounded_file_with_ordering(
    ctx: &SessionContext,
    schema: SchemaRef,
    file_path: &Path,
    table_name: &str,
    file_sort_order: Vec<Vec<Expr>>,
    with_unbounded_execution: bool,
) -> Result<()> {
    // Mark infinite and provide schema:
    let fifo_options = CsvReadOptions::new()
        .schema(schema.as_ref())
        .mark_infinite(with_unbounded_execution);
    // Get listing options:
    let options_sort = fifo_options
        .to_listing_options(&ctx.copied_config())
        .with_file_sort_order(file_sort_order);
    // Register table:
    ctx.register_listing_table(
        table_name,
        file_path.as_os_str().to_str().unwrap(),
        options_sort,
        Some(schema),
        None,
    )
    .await?;
    Ok(())
}
