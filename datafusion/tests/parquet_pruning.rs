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

// This file contains an end to end test of parquet pruning. It writes
// data into a parquet file and then
use std::sync::Arc;

use arrow::{
    array::{
        Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    },
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
    util::pretty::pretty_format_batches,
};
use chrono::Duration;
use datafusion::{
    physical_plan::{plan_metrics, SQLMetric},
    prelude::ExecutionContext,
};
use hashbrown::HashMap;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use tempfile::NamedTempFile;

#[tokio::test]
async fn prune_timestamps_nanos() {
    let output = ContextWithParquet::new()
        .await
        .query("SELECT * FROM t where nanos < to_timestamp('2020-01-02 01:01:11Z')")
        .await;
    println!("{}", output.description());
    // This should prune one metrics without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(1));
    assert_eq!(output.result_rows, 10, "{}", output.description());
}

#[tokio::test]
async fn prune_timestamps_micros() {
    let output = ContextWithParquet::new()
        .await
        .query(
            "SELECT * FROM t where micros < to_timestamp_micros('2020-01-02 01:01:11Z')",
        )
        .await;
    println!("{}", output.description());
    // This should prune one metrics without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(1));
    assert_eq!(output.result_rows, 10, "{}", output.description());
}

#[tokio::test]
async fn prune_timestamps_millis() {
    let output = ContextWithParquet::new()
        .await
        .query(
            "SELECT * FROM t where millis < to_timestamp_millis('2020-01-02 01:01:11Z')",
        )
        .await;
    println!("{}", output.description());
    // This should prune one metrics without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(1));
    assert_eq!(output.result_rows, 10, "{}", output.description());
}

#[tokio::test]
async fn prune_timestamps_seconds() {
    let output = ContextWithParquet::new()
        .await
        .query(
            "SELECT * FROM t where seconds < to_timestamp_seconds('2020-01-02 01:01:11Z')",
        )
        .await;
    println!("{}", output.description());
    // This should prune one metrics without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(1));
    assert_eq!(output.result_rows, 10, "{}", output.description());
}

// ----------------------
// Begin test fixture
// ----------------------

/// Test fixture that has an execution context that has an external
/// table "t" registered, pointing at a parquet file made with
/// `make_test_file`
struct ContextWithParquet {
    file: NamedTempFile,
    ctx: ExecutionContext,
}

/// The output of running one of the test cases
struct TestOutput {
    /// The input string
    sql: String,
    /// Normalized metrics (filename replaced by a constant)
    metrics: HashMap<String, SQLMetric>,
    /// number of rows in results
    result_rows: usize,
    /// the contents of the input, as a string
    pretty_input: String,
    /// the raw results, as a string
    pretty_results: String,
}

impl TestOutput {
    /// retrieve the value of the named metric, if any
    fn metric_value(&self, metric_name: &str) -> Option<usize> {
        self.metrics.get(metric_name).map(|m| m.value())
    }

    /// The number of times the pruning predicate evaluation errors
    fn predicate_evaluation_errors(&self) -> Option<usize> {
        self.metric_value("numPredicateEvaluationErrors for PARQUET_FILE")
    }

    /// The number of times the pruning predicate evaluation errors
    fn row_groups_pruned(&self) -> Option<usize> {
        self.metric_value("numRowGroupsPruned for PARQUET_FILE")
    }

    fn description(&self) -> String {
        let metrics = self
            .metrics
            .iter()
            .map(|(name, val)| format!("  {} = {:?}", name, val))
            .collect::<Vec<_>>();

        format!(
            "Input:\n{}\nQuery:\n{}\nOutput:\n{}\nMetrics:\n{}",
            self.pretty_input,
            self.sql,
            self.pretty_results,
            metrics.join("\n")
        )
    }
}

/// Creates an execution context that has an external table "t"
/// registered pointing at a parquet file made with `make_test_file`
impl ContextWithParquet {
    async fn new() -> Self {
        let file = make_test_file().await;

        // now, setup a the file as a data source and run a query against it
        let mut ctx = ExecutionContext::new();
        let parquet_path = file.path().to_string_lossy();
        ctx.register_parquet("t", &parquet_path)
            .expect("registering");

        Self { file, ctx }
    }

    /// Runs the specified SQL query and returns the number of output
    /// rows and normalized execution metrics
    async fn query(&mut self, sql: &str) -> TestOutput {
        println!("Planning sql {}", sql);

        let input = self
            .ctx
            .sql("SELECT * from t")
            .expect("planning")
            .collect()
            .await
            .expect("getting input");
        let pretty_input = pretty_format_batches(&input).unwrap();

        let logical_plan = self.ctx.sql(sql).expect("planning").to_logical_plan();

        let logical_plan = self.ctx.optimize(&logical_plan).expect("optimizing plan");
        let execution_plan = self
            .ctx
            .create_physical_plan(&logical_plan)
            .expect("creating physical plan");

        let results = datafusion::physical_plan::collect(execution_plan.clone())
            .await
            .expect("Running");

        // replace the path name, which varies test to test,a with some
        // constant for test comparisons
        let path = self.file.path();
        let path_name = path.to_string_lossy();
        let metrics = plan_metrics(execution_plan)
            .into_iter()
            .map(|(name, metric)| {
                (name.replace(path_name.as_ref(), "PARQUET_FILE"), metric)
            })
            .collect();

        let result_rows = results.iter().map(|b| b.num_rows()).sum();

        let pretty_results = pretty_format_batches(&results).unwrap();

        let sql = sql.to_string();
        TestOutput {
            sql,
            metrics,
            result_rows,
            pretty_input,
            pretty_results,
        }
    }
}

/// Create a test parquet file with varioud data types
async fn make_test_file() -> NamedTempFile {
    let output_file = tempfile::Builder::new()
        .prefix("parquet_pruning")
        .suffix(".parquet")
        .tempfile()
        .expect("tempfile creation");

    let props = WriterProperties::builder()
        .set_max_row_group_size(5)
        .build();

    let batches = vec![
        make_batch(Duration::seconds(0)),
        make_batch(Duration::seconds(10)),
        make_batch(Duration::minutes(10)),
        make_batch(Duration::days(10)),
    ];
    let schema = batches[0].schema();

    let mut writer = ArrowWriter::try_new(
        output_file
            .as_file()
            .try_clone()
            .expect("cloning file descriptor"),
        schema,
        Some(props),
    )
    .unwrap();

    for batch in batches {
        writer.write(&batch).expect("writing batch");
    }
    writer.close().unwrap();

    output_file
}

/// Return record batch with a few rows of data for all of the supported timestamp types
/// values with the specified offset
///
/// Columns are named:
/// "nanos" --> TimestampNanosecondArray
/// "micros" --> TimestampMicrosecondArray
/// "millis" --> TimestampMillisecondArray
/// "seconds" --> TimestampSecondArray
/// "names" --> StringArray
pub fn make_batch(offset: Duration) -> RecordBatch {
    let ts_strings = vec![
        Some("2020-01-01T01:01:01.0000000000001"),
        Some("2020-01-01T01:02:01.0000000000001"),
        Some("2020-01-01T02:01:01.0000000000001"),
        None,
        Some("2020-01-02T01:01:01.0000000000001"),
    ];

    let offset_nanos = offset.num_nanoseconds().expect("non overflow nanos");

    let ts_nanos = ts_strings
        .into_iter()
        .map(|t| {
            t.map(|t| {
                offset_nanos
                    + t.parse::<chrono::NaiveDateTime>()
                        .unwrap()
                        .timestamp_nanos()
            })
        })
        .collect::<Vec<_>>();

    let ts_micros = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| ts_nanos / 1000))
        .collect::<Vec<_>>();

    let ts_millis = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| ts_nanos / 1000000))
        .collect::<Vec<_>>();

    let ts_seconds = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| ts_nanos / 1000000000))
        .collect::<Vec<_>>();

    let names = ts_nanos
        .iter()
        .enumerate()
        .map(|(i, _)| format!("Row {} + {}", i, offset))
        .collect::<Vec<_>>();

    let arr_nanos = TimestampNanosecondArray::from_opt_vec(ts_nanos, None);
    let arr_micros = TimestampMicrosecondArray::from_opt_vec(ts_micros, None);
    let arr_millis = TimestampMillisecondArray::from_opt_vec(ts_millis, None);
    let arr_seconds = TimestampSecondArray::from_opt_vec(ts_seconds, None);

    let names = names.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let arr_names = StringArray::from(names);

    let schema = Schema::new(vec![
        Field::new("nanos", arr_nanos.data_type().clone(), true),
        Field::new("micros", arr_micros.data_type().clone(), true),
        Field::new("millis", arr_millis.data_type().clone(), true),
        Field::new("seconds", arr_seconds.data_type().clone(), true),
        Field::new("name", arr_names.data_type().clone(), true),
    ]);
    let schema = Arc::new(schema);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arr_nanos),
            Arc::new(arr_micros),
            Arc::new(arr_millis),
            Arc::new(arr_seconds),
            Arc::new(arr_names),
        ],
    )
    .unwrap()
}
