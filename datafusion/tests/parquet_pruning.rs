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
        Array, Date32Array, Date64Array, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    },
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
    util::pretty::pretty_format_batches,
};
use chrono::{Datelike, Duration};
use datafusion::{
    datasource::{parquet::ParquetTable, TableProvider},
    logical_plan::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder},
    physical_plan::{plan_metrics, SQLMetric},
    prelude::ExecutionContext,
    scalar::ScalarValue,
};
use hashbrown::HashMap;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use tempfile::NamedTempFile;

#[tokio::test]
async fn prune_timestamps_nanos() {
    let output = ContextWithParquet::new(Scenario::Timestamps)
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
    let output = ContextWithParquet::new(Scenario::Timestamps)
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
    let output = ContextWithParquet::new(Scenario::Timestamps)
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
    let output = ContextWithParquet::new(Scenario::Timestamps)
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

#[tokio::test]
async fn prune_date32() {
    let output = ContextWithParquet::new(Scenario::Dates)
        .await
        .query("SELECT * FROM t where date32 < cast('2020-01-02' as date)")
        .await;
    println!("{}", output.description());
    // This should prune out groups  without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(3));
    assert_eq!(output.result_rows, 1, "{}", output.description());
}

#[tokio::test]
async fn prune_date64() {
    // work around for not being able to cast Date32 to Date64 automatically
    let date = "2020-01-02"
        .parse::<chrono::NaiveDate>()
        .unwrap()
        .and_time(chrono::NaiveTime::from_hms(0, 0, 0));
    let date = ScalarValue::Date64(Some(date.timestamp_millis()));

    let output = ContextWithParquet::new(Scenario::Dates)
        .await
        .query_with_expr(col("date64").lt(lit(date)))
        // .query(
        //     "SELECT * FROM t where date64 < caste('2020-01-02' as date)",
        // query results in Plan("'Date64 < Date32' can't be evaluated because there isn't a common type to coerce the types to")
        // )
        .await;

    println!("{}", output.description());
    // This should prune out groups  without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(3));
    assert_eq!(output.result_rows, 1, "{}", output.description());
}

// ----------------------
// Begin test fixture
// ----------------------

/// What data to use
enum Scenario {
    Timestamps,
    Dates,
}

/// Test fixture that has an execution context that has an external
/// table "t" registered, pointing at a parquet file made with
/// `make_test_file`
struct ContextWithParquet {
    file: NamedTempFile,
    provider: Arc<dyn TableProvider>,
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
/// and the appropriate scenario
impl ContextWithParquet {
    async fn new(scenario: Scenario) -> Self {
        let file = make_test_file(scenario).await;

        // now, setup a the file as a data source and run a query against it
        let mut ctx = ExecutionContext::new();
        let parquet_path = file.path().to_string_lossy();

        let table = ParquetTable::try_new(parquet_path, 4).unwrap();

        let provider = Arc::new(table);
        ctx.register_table("t", provider.clone()).unwrap();

        Self {
            file,
            provider,
            ctx,
        }
    }

    /// runs a query like "SELECT * from t WHERE <expr> and returns
    /// the number of output rows and normalized execution metrics
    async fn query_with_expr(&mut self, expr: Expr) -> TestOutput {
        let sql = format!("EXPR only: {:?}", expr);
        let logical_plan = LogicalPlanBuilder::scan("t", self.provider.clone(), None)
            .unwrap()
            .filter(expr)
            .unwrap()
            .build()
            .unwrap();
        self.run_test(logical_plan, sql).await
    }

    /// Runs the specified SQL query and returns the number of output
    /// rows and normalized execution metrics
    async fn query(&mut self, sql: &str) -> TestOutput {
        println!("Planning sql {}", sql);
        let logical_plan = self.ctx.sql(sql).expect("planning").to_logical_plan();
        self.run_test(logical_plan, sql).await
    }

    /// runs the logical plan
    async fn run_test(
        &mut self,
        logical_plan: LogicalPlan,
        sql: impl Into<String>,
    ) -> TestOutput {
        let input = self
            .ctx
            .sql("SELECT * from t")
            .expect("planning")
            .collect()
            .await
            .expect("getting input");
        let pretty_input = pretty_format_batches(&input).unwrap();

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

        let sql = sql.into();
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
async fn make_test_file(scenario: Scenario) -> NamedTempFile {
    let output_file = tempfile::Builder::new()
        .prefix("parquet_pruning")
        .suffix(".parquet")
        .tempfile()
        .expect("tempfile creation");

    let props = WriterProperties::builder()
        .set_max_row_group_size(5)
        .build();

    let batches = match scenario {
        Scenario::Timestamps => {
            vec![
                make_timestamp_batch(Duration::seconds(0)),
                make_timestamp_batch(Duration::seconds(10)),
                make_timestamp_batch(Duration::minutes(10)),
                make_timestamp_batch(Duration::days(10)),
            ]
        }
        Scenario::Dates => {
            vec![
                make_date_batch(Duration::days(0)),
                make_date_batch(Duration::days(10)),
                make_date_batch(Duration::days(300)),
                make_date_batch(Duration::days(3600)),
            ]
        }
    };

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
fn make_timestamp_batch(offset: Duration) -> RecordBatch {
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

/// Return record batch with a few rows of data for all of the supported date
/// types with the specified offset (in days)
///
/// Columns are named:
/// "date32" --> Date32Array
/// "date64" --> Date64Array
/// "names" --> StringArray
fn make_date_batch(offset: Duration) -> RecordBatch {
    let date_strings = vec![
        Some("2020-01-01"),
        Some("2020-01-02"),
        Some("2020-01-03"),
        None,
        Some("2020-01-04"),
    ];

    let names = date_strings
        .iter()
        .enumerate()
        .map(|(i, val)| format!("Row {} + {}: {:?}", i, offset, val))
        .collect::<Vec<_>>();

    // Copied from `cast.rs` cast kernel due to lack of temporal kernels
    // https://github.com/apache/arrow-rs/issues/527
    const EPOCH_DAYS_FROM_CE: i32 = 719_163;

    let date_seconds = date_strings
        .iter()
        .map(|t| {
            t.map(|t| {
                let t = t.parse::<chrono::NaiveDate>().unwrap();
                let t = t + offset;
                t.num_days_from_ce() - EPOCH_DAYS_FROM_CE
            })
        })
        .collect::<Vec<_>>();

    let date_millis = date_strings
        .into_iter()
        .map(|t| {
            t.map(|t| {
                let t = t
                    .parse::<chrono::NaiveDate>()
                    .unwrap()
                    .and_time(chrono::NaiveTime::from_hms(0, 0, 0));
                let t = t + offset;
                t.timestamp_millis()
            })
        })
        .collect::<Vec<_>>();

    let arr_date32 = Date32Array::from(date_seconds);
    let arr_date64 = Date64Array::from(date_millis);

    let names = names.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let arr_names = StringArray::from(names);

    let schema = Schema::new(vec![
        Field::new("date32", arr_date32.data_type().clone(), true),
        Field::new("date64", arr_date64.data_type().clone(), true),
        Field::new("name", arr_names.data_type().clone(), true),
    ]);
    let schema = Arc::new(schema);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arr_date32),
            Arc::new(arr_date64),
            Arc::new(arr_names),
        ],
    )
    .unwrap()
}
