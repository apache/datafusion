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

use std::any::Any;
use std::collections::HashMap;
use std::{env, error::Error, path::PathBuf, sync::Arc};

use crate::datasource::datasource::TableProviderFactory;
use crate::datasource::{empty::EmptyTable, provider_as_source, TableProvider};
use crate::execution::context::SessionState;
use crate::logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE};
use crate::physical_plan::ExecutionPlan;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_common::DataFusionError;
use datafusion_expr::{CreateExternalTable, Expr, TableType};

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
        Err(err) => panic!("failed to get arrow data dir: {}", err),
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
        Err(err) => panic!("failed to get parquet data dir: {}", err),
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
fn get_data_dir(udf_env: &str, submodule_data: &str) -> Result<PathBuf, Box<dyn Error>> {
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
) -> Result<LogicalPlanBuilder, DataFusionError> {
    let table_schema = Arc::new(table_schema.clone());
    let provider = Arc::new(EmptyTable::new(table_schema));
    LogicalPlanBuilder::scan(
        name.unwrap_or(UNNAMED_TABLE),
        provider_as_source(provider),
        projection,
    )
}

/// Scan an empty data source with configured partition, mainly used in tests.
pub fn scan_empty_with_partitions(
    name: Option<&str>,
    table_schema: &Schema,
    projection: Option<Vec<usize>>,
    partitions: usize,
) -> Result<LogicalPlanBuilder, DataFusionError> {
    let table_schema = Arc::new(table_schema.clone());
    let provider = Arc::new(EmptyTable::new(table_schema).with_partitions(partitions));
    LogicalPlanBuilder::scan(
        name.unwrap_or(UNNAMED_TABLE),
        provider_as_source(provider),
        projection,
    )
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

/// TableFactory for tests
pub struct TestTableFactory {}

#[async_trait]
impl TableProviderFactory for TestTableFactory {
    async fn create(
        &self,
        _: &SessionState,
        cmd: &CreateExternalTable,
    ) -> datafusion_common::Result<Arc<dyn TableProvider>> {
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
        _ctx: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        unimplemented!("TestTableProvider is a stub for testing.")
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
