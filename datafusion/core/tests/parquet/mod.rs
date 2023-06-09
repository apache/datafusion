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

//! Parquet integration tests
use arrow::array::Decimal128Array;
use arrow::{
    array::{
        Array, ArrayRef, Date32Array, Date64Array, Float64Array, Int32Array, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
    util::pretty::pretty_format_batches,
};
use chrono::{Datelike, Duration};
use datafusion::{
    datasource::{physical_plan::ParquetExec, provider_as_source, TableProvider},
    physical_plan::{accept, metrics::MetricsSet, ExecutionPlan, ExecutionPlanVisitor},
    prelude::{ParquetReadOptions, SessionConfig, SessionContext},
};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tempfile::NamedTempFile;

mod custom_reader;
mod filter_pushdown;
mod page_pruning;
mod row_group_pruning;
mod schema_coercion;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::try_init();
}

// ----------------------
// Begin test fixture
// ----------------------

/// What data to use
enum Scenario {
    Timestamps,
    Dates,
    Int32,
    Float64,
    Decimal,
    DecimalLargePrecision,
    PeriodsInColumnNames,
}

enum Unit {
    RowGroup,
    Page,
}

/// Test fixture that has an execution context that has an external
/// table "t" registered, pointing at a parquet file made with
/// `make_test_file`
struct ContextWithParquet {
    #[allow(dead_code)]
    /// temp file parquet data is written to. The file is cleaned up
    /// when dropped
    file: NamedTempFile,
    provider: Arc<dyn TableProvider>,
    ctx: SessionContext,
}

/// The output of running one of the test cases
struct TestOutput {
    /// The input string
    sql: String,
    /// Execution metrics for the Parquet Scan
    parquet_metrics: MetricsSet,
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
        self.parquet_metrics
            .sum(|metric| metric.value().name() == metric_name)
            .map(|v| v.as_usize())
    }

    /// The number of times the pruning predicate evaluation errors
    fn predicate_evaluation_errors(&self) -> Option<usize> {
        self.metric_value("predicate_evaluation_errors")
    }

    /// The number of times the pruning predicate evaluation errors
    fn row_groups_pruned(&self) -> Option<usize> {
        self.metric_value("row_groups_pruned")
    }

    /// The number of times the pruning predicate evaluation errors
    fn row_pages_pruned(&self) -> Option<usize> {
        self.metric_value("page_index_rows_filtered")
    }

    fn description(&self) -> String {
        format!(
            "Input:\n{}\nQuery:\n{}\nOutput:\n{}\nMetrics:\n{}",
            self.pretty_input, self.sql, self.pretty_results, self.parquet_metrics,
        )
    }
}

/// Creates an execution context that has an external table "t"
/// registered pointing at a parquet file made with `make_test_file`
/// and the appropriate scenario
impl ContextWithParquet {
    async fn new(scenario: Scenario, unit: Unit) -> Self {
        Self::with_config(scenario, unit, SessionConfig::new()).await
    }

    async fn with_config(
        scenario: Scenario,
        unit: Unit,
        mut config: SessionConfig,
    ) -> Self {
        let file = match unit {
            Unit::RowGroup => make_test_file_rg(scenario).await,
            Unit::Page => {
                let config = config.options_mut();
                config.execution.parquet.enable_page_index = true;
                make_test_file_page(scenario).await
            }
        };
        let parquet_path = file.path().to_string_lossy();

        // now, setup a the file as a data source and run a query against it
        let ctx = SessionContext::with_config(config);

        ctx.register_parquet("t", &parquet_path, ParquetReadOptions::default())
            .await
            .unwrap();
        let provider = ctx.deregister_table("t").unwrap().unwrap();
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
        let sql = format!("EXPR only: {expr:?}");
        let logical_plan = LogicalPlanBuilder::scan(
            "t",
            provider_as_source(self.provider.clone()),
            None,
        )
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
        println!("Planning sql {sql}");
        let logical_plan = self
            .ctx
            .sql(sql)
            .await
            .expect("planning")
            .into_unoptimized_plan();
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
            .await
            .expect("planning")
            .collect()
            .await
            .expect("getting input");
        let pretty_input = pretty_format_batches(&input).unwrap().to_string();

        let state = self.ctx.state();
        let logical_plan = state.optimize(&logical_plan).expect("optimizing plan");

        let physical_plan = state
            .create_physical_plan(&logical_plan)
            .await
            .expect("creating physical plan");

        let task_ctx = state.task_ctx();
        let results = datafusion::physical_plan::collect(physical_plan.clone(), task_ctx)
            .await
            .expect("Running");

        // find the parquet metrics
        struct MetricsFinder {
            metrics: Option<MetricsSet>,
        }
        impl ExecutionPlanVisitor for MetricsFinder {
            type Error = std::convert::Infallible;
            fn pre_visit(
                &mut self,
                plan: &dyn ExecutionPlan,
            ) -> Result<bool, Self::Error> {
                if plan.as_any().downcast_ref::<ParquetExec>().is_some() {
                    self.metrics = plan.metrics();
                }
                // stop searching once we have found the metrics
                Ok(self.metrics.is_none())
            }
        }
        let mut finder = MetricsFinder { metrics: None };
        accept(physical_plan.as_ref(), &mut finder).unwrap();
        let parquet_metrics = finder.metrics.unwrap();

        let result_rows = results.iter().map(|b| b.num_rows()).sum();

        let pretty_results = pretty_format_batches(&results).unwrap().to_string();

        let sql = sql.into();
        TestOutput {
            sql,
            parquet_metrics,
            result_rows,
            pretty_input,
            pretty_results,
        }
    }
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
        .map(|(i, _)| format!("Row {i} + {offset}"))
        .collect::<Vec<_>>();

    let arr_nanos = TimestampNanosecondArray::from(ts_nanos);
    let arr_micros = TimestampMicrosecondArray::from(ts_micros);
    let arr_millis = TimestampMillisecondArray::from(ts_millis);
    let arr_seconds = TimestampSecondArray::from(ts_seconds);

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

/// Return record batch with i32 sequence
///
/// Columns are named
/// "i" -> Int32Array
fn make_int32_batch(start: i32, end: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
    let v: Vec<i32> = (start..end).collect();
    let array = Arc::new(Int32Array::from(v)) as ArrayRef;
    RecordBatch::try_new(schema, vec![array.clone()]).unwrap()
}

/// Return record batch with f64 vector
///
/// Columns are named
/// "f" -> Float64Array
fn make_f64_batch(v: Vec<f64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
    let array = Arc::new(Float64Array::from(v)) as ArrayRef;
    RecordBatch::try_new(schema, vec![array.clone()]).unwrap()
}

/// Return record batch with decimal vector
///
/// Columns are named
/// "decimal_col" -> DecimalArray
fn make_decimal_batch(v: Vec<i128>, precision: u8, scale: i8) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "decimal_col",
        DataType::Decimal128(precision, scale),
        true,
    )]));
    let array = Arc::new(
        Decimal128Array::from(v)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    ) as ArrayRef;
    RecordBatch::try_new(schema, vec![array.clone()]).unwrap()
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
        .map(|(i, val)| format!("Row {i} + {offset}: {val:?}"))
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
                    .and_time(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
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

/// returns a batch with two columns (note "service.name" is the name
/// of the column. It is *not* a table named service.name
///
/// name | service.name
fn make_names_batch(name: &str, service_name_values: Vec<&str>) -> RecordBatch {
    let num_rows = service_name_values.len();
    let name: StringArray = std::iter::repeat(Some(name)).take(num_rows).collect();
    let service_name: StringArray = service_name_values.iter().map(Some).collect();

    let schema = Schema::new(vec![
        Field::new("name", name.data_type().clone(), true),
        // note the column name has a period in it!
        Field::new("service.name", service_name.data_type().clone(), true),
    ]);
    let schema = Arc::new(schema);

    RecordBatch::try_new(schema, vec![Arc::new(name), Arc::new(service_name)]).unwrap()
}

fn create_data_batch(scenario: Scenario) -> Vec<RecordBatch> {
    match scenario {
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
        Scenario::Int32 => {
            vec![
                make_int32_batch(-5, 0),
                make_int32_batch(-4, 1),
                make_int32_batch(0, 5),
                make_int32_batch(5, 10),
            ]
        }
        Scenario::Float64 => {
            vec![
                make_f64_batch(vec![-5.0, -4.0, -3.0, -2.0, -1.0]),
                make_f64_batch(vec![-4.0, -3.0, -2.0, -1.0, 0.0]),
                make_f64_batch(vec![0.0, 1.0, 2.0, 3.0, 4.0]),
                make_f64_batch(vec![5.0, 6.0, 7.0, 8.0, 9.0]),
            ]
        }
        Scenario::Decimal => {
            // decimal record batch
            vec![
                make_decimal_batch(vec![100, 200, 300, 400, 600], 9, 2),
                make_decimal_batch(vec![-500, 100, 300, 400, 600], 9, 2),
                make_decimal_batch(vec![2000, 3000, 3000, 4000, 6000], 9, 2),
            ]
        }
        Scenario::DecimalLargePrecision => {
            // decimal record batch with large precision,
            // and the data will stored as FIXED_LENGTH_BYTE_ARRAY
            vec![
                make_decimal_batch(vec![100, 200, 300, 400, 600], 38, 2),
                make_decimal_batch(vec![-500, 100, 300, 400, 600], 38, 2),
                make_decimal_batch(vec![2000, 3000, 3000, 4000, 6000], 38, 2),
            ]
        }
        Scenario::PeriodsInColumnNames => {
            vec![
                // all frontend
                make_names_batch(
                    "HTTP GET / DISPATCH",
                    vec!["frontend", "frontend", "frontend", "frontend", "frontend"],
                ),
                // both frontend and backend
                make_names_batch(
                    "HTTP PUT / DISPATCH",
                    vec!["frontend", "frontend", "backend", "backend", "backend"],
                ),
                // all backend
                make_names_batch(
                    "HTTP GET / DISPATCH",
                    vec!["backend", "backend", "backend", "backend", "backend"],
                ),
            ]
        }
    }
}

/// Create a test parquet file with various data types
async fn make_test_file_rg(scenario: Scenario) -> NamedTempFile {
    let mut output_file = tempfile::Builder::new()
        .prefix("parquet_pruning")
        .suffix(".parquet")
        .tempfile()
        .expect("tempfile creation");

    let props = WriterProperties::builder()
        .set_max_row_group_size(5)
        .build();

    let batches = create_data_batch(scenario);

    let schema = batches[0].schema();

    let mut writer = ArrowWriter::try_new(&mut output_file, schema, Some(props)).unwrap();

    for batch in batches {
        writer.write(&batch).expect("writing batch");
    }
    writer.close().unwrap();

    output_file
}

async fn make_test_file_page(scenario: Scenario) -> NamedTempFile {
    let mut output_file = tempfile::Builder::new()
        .prefix("parquet_page_pruning")
        .suffix(".parquet")
        .tempfile()
        .expect("tempfile creation");

    // set row count to 5, should get same result as rowGroup
    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(5)
        .set_write_batch_size(5)
        .build();

    let batches = create_data_batch(scenario);

    let schema = batches[0].schema();

    let mut writer = ArrowWriter::try_new(&mut output_file, schema, Some(props)).unwrap();

    for batch in batches {
        writer.write(&batch).expect("writing batch");
    }
    writer.close().unwrap();
    output_file
}
