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

use arrow::array::Int32Array;
use std::any::Any;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{env, error::Error, path::PathBuf, sync::Arc};

use crate::datasource::datasource::TableProviderFactory;
use crate::datasource::{empty::EmptyTable, provider_as_source, TableProvider};
use crate::error::Result;
use crate::execution::context::{SessionState, TaskContext};
use crate::execution::options::ReadOptions;
use crate::logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE};
use crate::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream,
};
use crate::prelude::{CsvReadOptions, SessionContext};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_common::from_slice::FromSlice;
use datafusion_common::{Statistics, TableReference};
use datafusion_execution::config::SessionConfig;
use datafusion_expr::{col, CreateExternalTable, Expr, TableType};
use datafusion_physical_expr::PhysicalSortExpr;
use futures::Stream;
use tempfile::TempDir;

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

        let formatted = arrow::util::pretty::pretty_format_batches($CHUNKS)
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

        let formatted = arrow::util::pretty::pretty_format_batches($CHUNKS)
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

/// Returns the arrow test data directory, which is by default stored
/// in a git submodule rooted at `testing/data`.
///
/// The default can be overridden by the optional environment
/// variable `ARROW_TEST_DATA`
///
/// panics when the directory can not be found.
///
/// Example:
/// ```
/// let testdata = datafusion::test_util::arrow_test_data();
/// let csvdata = format!("{}/csv/aggregate_test_100.csv", testdata);
/// assert!(std::path::PathBuf::from(csvdata).exists());
/// ```
pub fn arrow_test_data() -> String {
    match get_data_dir("ARROW_TEST_DATA", "../../testing/data") {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!("failed to get arrow data dir: {err}"),
    }
}

/// Returns the parquet test data directory, which is by default
/// stored in a git submodule rooted at
/// `parquet-testing/data`.
///
/// The default can be overridden by the optional environment variable
/// `PARQUET_TEST_DATA`
///
/// panics when the directory can not be found.
///
/// Example:
/// ```
/// let testdata = datafusion::test_util::parquet_test_data();
/// let filename = format!("{}/binary.parquet", testdata);
/// assert!(std::path::PathBuf::from(filename).exists());
/// ```
pub fn parquet_test_data() -> String {
    match get_data_dir("PARQUET_TEST_DATA", "../../parquet-testing/data") {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!("failed to get parquet data dir: {err}"),
    }
}

/// Returns a directory path for finding test data.
///
/// udf_env: name of an environment variable
///
/// submodule_dir: fallback path (relative to CARGO_MANIFEST_DIR)
///
///  Returns either:
/// The path referred to in `udf_env` if that variable is set and refers to a directory
/// The submodule_data directory relative to CARGO_MANIFEST_PATH
pub fn get_data_dir(
    udf_env: &str,
    submodule_data: &str,
) -> Result<PathBuf, Box<dyn Error>> {
    // Try user defined env.
    if let Ok(dir) = env::var(udf_env) {
        let trimmed = dir.trim().to_string();
        if !trimmed.is_empty() {
            let pb = PathBuf::from(trimmed);
            if pb.is_dir() {
                return Ok(pb);
            } else {
                return Err(format!(
                    "the data dir `{}` defined by env {} not found",
                    pb.display(),
                    udf_env
                )
                .into());
            }
        }
    }

    // The env is undefined or its value is trimmed to empty, let's try default dir.

    // env "CARGO_MANIFEST_DIR" is "the directory containing the manifest of your package",
    // set by `cargo run` or `cargo test`, see:
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html
    let dir = env!("CARGO_MANIFEST_DIR");

    let pb = PathBuf::from(dir).join(submodule_data);
    if pb.is_dir() {
        Ok(pb)
    } else {
        Err(format!(
            "env `{}` is undefined or has empty value, and the pre-defined data dir `{}` not found\n\
             HINT: try running `git submodule update --init`",
            udf_env,
            pb.display(),
        ).into())
    }
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
    f1.set_metadata(HashMap::from_iter(
        vec![("testing".into(), "test".into())].into_iter(),
    ));
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

/// Get the schema for the aggregate_test_* csv files with an additional filed not present in the files.
pub fn aggr_test_schema_with_missing_col() -> SchemaRef {
    let mut f1 = Field::new("c1", DataType::Utf8, false);
    f1.set_metadata(HashMap::from_iter(
        vec![("testing".into(), "test".into())].into_iter(),
    ));
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
        Field::new("missing_col", DataType::Int64, true),
    ]);

    Arc::new(schema)
}

// Return a static RecordBatch and its ordering for tests. RecordBatch is ordered by ts
fn get_test_data() -> Result<(RecordBatch, Vec<Expr>)> {
    let ts_field = Field::new("ts", DataType::Int32, false);
    let inc_field = Field::new("inc_col", DataType::Int32, false);
    let desc_field = Field::new("desc_col", DataType::Int32, false);

    let schema = Arc::new(Schema::new(vec![ts_field, inc_field, desc_field]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_slice([
                1, 1, 5, 9, 10, 11, 16, 21, 22, 26, 26, 28, 31, 33, 38, 42, 47, 51, 53,
                53, 58, 63, 67, 68, 70, 72, 72, 76, 81, 85, 86, 88, 91, 96, 97, 98, 100,
                101, 102, 104, 104, 108, 112, 113, 113, 114, 114, 117, 122, 126, 131,
                131, 136, 136, 136, 139, 141, 146, 147, 147, 152, 154, 159, 161, 163,
                164, 167, 172, 173, 177, 180, 185, 186, 191, 195, 195, 199, 203, 207,
                210, 213, 218, 221, 224, 226, 230, 232, 235, 238, 238, 239, 244, 245,
                247, 250, 254, 258, 262, 264, 264,
            ])),
            Arc::new(Int32Array::from_slice([
                1, 5, 10, 15, 20, 21, 26, 29, 30, 33, 37, 40, 43, 44, 45, 49, 51, 53, 58,
                61, 65, 70, 75, 78, 83, 88, 90, 91, 95, 97, 100, 105, 109, 111, 115, 119,
                120, 124, 126, 129, 131, 135, 140, 143, 144, 147, 148, 149, 151, 155,
                156, 159, 160, 163, 165, 170, 172, 177, 181, 182, 186, 187, 192, 196,
                197, 199, 203, 207, 209, 213, 214, 216, 219, 221, 222, 225, 226, 231,
                236, 237, 242, 245, 247, 248, 253, 254, 259, 261, 266, 269, 272, 275,
                278, 283, 286, 289, 291, 296, 301, 305,
            ])),
            Arc::new(Int32Array::from_slice([
                100, 98, 93, 91, 86, 84, 81, 77, 75, 71, 70, 69, 64, 62, 59, 55, 50, 45,
                41, 40, 39, 36, 31, 28, 23, 22, 17, 13, 10, 6, 5, 2, 1, -1, -4, -5, -6,
                -8, -12, -16, -17, -19, -24, -25, -29, -34, -37, -42, -47, -48, -49, -53,
                -57, -58, -61, -65, -67, -68, -71, -73, -75, -76, -78, -83, -87, -91,
                -95, -98, -101, -105, -106, -111, -114, -116, -120, -125, -128, -129,
                -134, -139, -142, -143, -146, -150, -154, -158, -163, -168, -172, -176,
                -181, -184, -189, -193, -196, -201, -203, -208, -210, -213,
            ])),
        ],
    )?;
    let file_sort_order = vec![col("ts").sort(true, false)];
    Ok((batch, file_sort_order))
}

// Return a static RecordBatch and its ordering for tests. RecordBatch is ordered by a, b, c
fn get_test_data2() -> Result<(RecordBatch, Vec<Expr>)> {
    let a = Field::new("a", DataType::Int32, false);
    let b = Field::new("b", DataType::Int32, false);
    let c = Field::new("c", DataType::Int32, false);
    let d = Field::new("d", DataType::Int32, false);

    let schema = Arc::new(Schema::new(vec![a, b, c, d]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_slice([
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1,
            ])),
            Arc::new(Int32Array::from_slice([
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
                3, 3, 3, 3,
            ])),
            Arc::new(Int32Array::from_slice([
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
                39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56,
                57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74,
                75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92,
                93, 94, 95, 96, 97, 98, 99,
            ])),
            Arc::new(Int32Array::from_slice([
                0, 2, 0, 0, 1, 1, 0, 2, 1, 4, 4, 2, 2, 1, 2, 3, 3, 2, 1, 4, 0, 3, 0, 0,
                4, 0, 2, 0, 1, 1, 3, 4, 2, 2, 4, 0, 1, 4, 0, 1, 1, 3, 3, 2, 3, 0, 0, 1,
                1, 3, 0, 3, 1, 1, 4, 2, 1, 1, 1, 2, 4, 3, 1, 4, 4, 0, 2, 4, 1, 1, 0, 2,
                1, 1, 4, 2, 0, 2, 1, 4, 2, 0, 4, 2, 1, 1, 1, 4, 3, 4, 1, 2, 0, 0, 2, 0,
                4, 2, 4, 3,
            ])),
        ],
    )?;
    let file_sort_order = vec![
        col("a").sort(true, false),
        col("b").sort(true, false),
        col("c").sort(true, false),
    ];
    Ok((batch, file_sort_order))
}

/// Creates a test_context with table name `annotated_data` which has 100 rows.
// Columns in the table are ts, inc_col, desc_col. Source is CsvExec which is ordered by
// ts column.
pub async fn get_test_context(
    tmpdir: &TempDir,
    infinite_source: bool,
    session_config: SessionConfig,
) -> Result<SessionContext> {
    get_test_context_helper(tmpdir, infinite_source, session_config, get_test_data).await
}

/// Creates a test_context with table name `annotated_data`, which has 100 rows.
// Columns in the table are a,b,c,d. Source is CsvExec which is ordered by
// a,b,c column. Column a has cardinality 2, column b has cardinality 4.
// Column c has cardinality 100 (unique entries). Column d has cardinality 5.
pub async fn get_test_context2(
    tmpdir: &TempDir,
    infinite_source: bool,
    session_config: SessionConfig,
) -> Result<SessionContext> {
    get_test_context_helper(tmpdir, infinite_source, session_config, get_test_data2).await
}

async fn get_test_context_helper(
    tmpdir: &TempDir,
    infinite_source: bool,
    session_config: SessionConfig,
    data_receiver: fn() -> Result<(RecordBatch, Vec<Expr>)>,
) -> Result<SessionContext> {
    let ctx = SessionContext::with_config(session_config);

    let csv_read_options = CsvReadOptions::default();
    let (batch, file_sort_order) = data_receiver()?;

    let options_sort = csv_read_options
        .to_listing_options(&ctx.copied_config())
        .with_file_sort_order(Some(file_sort_order))
        .with_infinite_source(infinite_source);

    write_test_data_to_csv(tmpdir, 1, &batch)?;
    let sql_definition = None;
    ctx.register_listing_table(
        "annotated_data",
        tmpdir.path().to_string_lossy(),
        options_sort.clone(),
        Some(batch.schema()),
        sql_definition,
    )
    .await
    .unwrap();
    Ok(ctx)
}

fn write_test_data_to_csv(
    tmpdir: &TempDir,
    n_file: usize,
    batch: &RecordBatch,
) -> Result<()> {
    let n_chunk = batch.num_rows() / n_file;
    for i in 0..n_file {
        let target_file = tmpdir.path().join(format!("{i}.csv"));
        let file = File::create(target_file)?;
        let chunks_start = i * n_chunk;
        let cur_batch = batch.slice(chunks_start, n_chunk);
        let mut writer = arrow::csv::Writer::new(file);
        writer.write(&cur_batch)?;
    }
    Ok(())
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

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "UnboundableExec: unbounded={}",
                    self.batch_produce.is_none(),
                )
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_data_dir() {
        let udf_env = "get_data_dir";
        let cwd = env::current_dir().unwrap();

        let existing_pb = cwd.join("..");
        let existing = existing_pb.display().to_string();
        let existing_str = existing.as_str();

        let non_existing = cwd.join("non-existing-dir").display().to_string();
        let non_existing_str = non_existing.as_str();

        env::set_var(udf_env, non_existing_str);
        let res = get_data_dir(udf_env, existing_str);
        assert!(res.is_err());

        env::set_var(udf_env, "");
        let res = get_data_dir(udf_env, existing_str);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), existing_pb);

        env::set_var(udf_env, " ");
        let res = get_data_dir(udf_env, existing_str);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), existing_pb);

        env::set_var(udf_env, existing_str);
        let res = get_data_dir(udf_env, existing_str);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), existing_pb);

        env::remove_var(udf_env);
        let res = get_data_dir(udf_env, non_existing_str);
        assert!(res.is_err());

        let res = get_data_dir(udf_env, existing_str);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), existing_pb);
    }

    #[test]
    fn test_happy() {
        let res = arrow_test_data();
        assert!(PathBuf::from(res).is_dir());

        let res = parquet_test_data();
        assert!(PathBuf::from(res).is_dir());
    }
}

/// This function creates an unbounded sorted file for testing purposes.
pub async fn register_unbounded_file_with_ordering(
    ctx: &SessionContext,
    schema: SchemaRef,
    file_path: &Path,
    table_name: &str,
    file_sort_order: Option<Vec<Expr>>,
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
