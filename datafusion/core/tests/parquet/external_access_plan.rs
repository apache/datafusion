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

//! Tests for passing user provided [`ParquetAccessPlan`]` to `DataSourceExec`]`

use std::path::Path;
use std::sync::Arc;

use crate::parquet::utils::MetricsFinder;
use crate::parquet::{create_data_batch, Scenario};

use arrow::datatypes::SchemaRef;
use arrow::util::pretty::pretty_format_batches;
use datafusion::common::Result;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::prelude::SessionContext;
use datafusion_common::{assert_contains, DFSchema};
use datafusion_datasource_parquet::{ParquetAccessPlan, RowGroupAccess};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::{col, lit, Expr};
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::ExecutionPlan;

use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::NamedTempFile;

#[tokio::test]
async fn none() {
    // no user defined plan
    Test {
        access_plan: None,
        expected_rows: 10,
    }
    .run_success()
    .await;
}

#[tokio::test]
async fn scan_all() {
    let parquet_metrics = Test {
        access_plan: Some(ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
        ])),
        expected_rows: 10,
    }
    .run_success()
    .await;

    // Verify that some bytes were read
    let bytes_scanned = metric_value(&parquet_metrics, "bytes_scanned").unwrap();
    assert_ne!(bytes_scanned, 0, "metrics : {parquet_metrics:#?}",);
}

#[tokio::test]
async fn skip_all() {
    let parquet_metrics = Test {
        access_plan: Some(ParquetAccessPlan::new(vec![
            RowGroupAccess::Skip,
            RowGroupAccess::Skip,
        ])),
        expected_rows: 0,
    }
    .run_success()
    .await;

    // Verify that skipping all row groups skips reading any data at all
    let bytes_scanned = metric_value(&parquet_metrics, "bytes_scanned").unwrap();
    assert_eq!(bytes_scanned, 0, "metrics : {parquet_metrics:#?}",);
}

#[tokio::test]
async fn skip_one_row_group() {
    let plans = vec![
        ParquetAccessPlan::new(vec![RowGroupAccess::Scan, RowGroupAccess::Skip]),
        ParquetAccessPlan::new(vec![RowGroupAccess::Skip, RowGroupAccess::Scan]),
    ];

    for access_plan in plans {
        Test {
            access_plan: Some(access_plan),
            expected_rows: 5,
        }
        .run_success()
        .await;
    }
}

#[tokio::test]
async fn selection_scan() {
    let plans = vec![
        ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            RowGroupAccess::Selection(select_one_row()),
        ]),
        ParquetAccessPlan::new(vec![
            RowGroupAccess::Selection(select_one_row()),
            RowGroupAccess::Scan,
        ]),
    ];

    for access_plan in plans {
        Test {
            access_plan: Some(access_plan),
            expected_rows: 6,
        }
        .run_success()
        .await;
    }
}

#[tokio::test]
async fn skip_scan() {
    let plans = vec![
        // skip one row group, scan the toehr
        ParquetAccessPlan::new(vec![
            RowGroupAccess::Skip,
            RowGroupAccess::Selection(select_one_row()),
        ]),
        ParquetAccessPlan::new(vec![
            RowGroupAccess::Selection(select_one_row()),
            RowGroupAccess::Skip,
        ]),
    ];

    for access_plan in plans {
        Test {
            access_plan: Some(access_plan),
            expected_rows: 1,
        }
        .run_success()
        .await;
    }
}

#[tokio::test]
async fn plan_and_filter() {
    // show that row group pruning is applied even when an initial plan is supplied

    // No rows match this predicate
    let predicate = col("utf8").eq(lit("z"));

    // user supplied access plan specifies to still read a row group
    let access_plan = Some(ParquetAccessPlan::new(vec![
        // Row group 0 has values a-d
        RowGroupAccess::Skip,
        // Row group 1 has values e-i
        RowGroupAccess::Scan,
    ]));

    // initial
    let parquet_metrics = TestFull {
        access_plan,
        expected_rows: 0,
        predicate: Some(predicate),
    }
    .run()
    .await
    .unwrap();

    // Verify that row group pruning still happens for just that group
    let row_groups_pruned_statistics =
        metric_value(&parquet_metrics, "row_groups_pruned_statistics").unwrap();
    assert_eq!(
        row_groups_pruned_statistics, 1,
        "metrics : {parquet_metrics:#?}",
    );
}

#[tokio::test]
async fn two_selections() {
    let plans = vec![
        ParquetAccessPlan::new(vec![
            RowGroupAccess::Selection(select_one_row()),
            RowGroupAccess::Selection(select_two_rows()),
        ]),
        ParquetAccessPlan::new(vec![
            RowGroupAccess::Selection(select_two_rows()),
            RowGroupAccess::Selection(select_one_row()),
        ]),
    ];

    for access_plan in plans {
        Test {
            access_plan: Some(access_plan),
            expected_rows: 3,
        }
        .run_success()
        .await;
    }
}

#[tokio::test]
async fn bad_row_groups() {
    let err = TestFull {
        access_plan: Some(ParquetAccessPlan::new(vec![
            // file has only 2 row groups, but specify 3
            RowGroupAccess::Scan,
            RowGroupAccess::Skip,
            RowGroupAccess::Scan,
        ])),
        expected_rows: 0,
        predicate: None,
    }
    .run()
    .await
    .unwrap_err();
    let err_string = err.to_string();
    assert_contains!(&err_string, "Invalid ParquetAccessPlan");
    assert_contains!(&err_string, "Specified 3 row groups, but file has 2");
}

#[tokio::test]
async fn bad_selection() {
    let err = TestFull {
        access_plan: Some(ParquetAccessPlan::new(vec![
            // specify fewer rows than are actually in the row group
            RowGroupAccess::Selection(RowSelection::from(vec![
                RowSelector::skip(1),
                RowSelector::select(3),
            ])),
            RowGroupAccess::Skip,
        ])),
        // expects that we hit an error, this should not be run
        expected_rows: 10000,
        predicate: None,
    }
    .run()
    .await
    .unwrap_err();
    let err_string = err.to_string();
    assert_contains!(&err_string, "Internal error: Invalid ParquetAccessPlan Selection. Row group 0 has 5 rows but selection only specifies 4 rows");
}

/// Return a RowSelection of 1 rows from a row group of 5 rows
fn select_one_row() -> RowSelection {
    RowSelection::from(vec![
        RowSelector::skip(2),
        RowSelector::select(1),
        RowSelector::skip(2),
    ])
}
/// Return a RowSelection of 2 rows from a row group of 5 rows
fn select_two_rows() -> RowSelection {
    RowSelection::from(vec![
        RowSelector::skip(1),
        RowSelector::select(1),
        RowSelector::skip(1),
        RowSelector::select(1),
        RowSelector::skip(1),
    ])
}

/// Test for passing user defined ParquetAccessPlans. See [`TestFull`] for details.
#[derive(Debug)]
struct Test {
    access_plan: Option<ParquetAccessPlan>,
    expected_rows: usize,
}

impl Test {
    /// Runs the test case, panic'ing on error.
    ///
    /// Returns the [`MetricsSet`] from the [`DataSourceExec`]
    async fn run_success(self) -> MetricsSet {
        let Self {
            access_plan,
            expected_rows,
        } = self;
        TestFull {
            access_plan,
            expected_rows,
            predicate: None,
        }
        .run()
        .await
        .unwrap()
    }
}

/// Test for passing user defined ParquetAccessPlans:
///
/// 1. Creates a parquet file with 2 row groups, each with 5 rows
/// 2. Reads the parquet file with an optional user provided access plan
/// 3. Verifies that the expected number of rows is read
/// 4. Returns the statistics from running the plan
struct TestFull {
    access_plan: Option<ParquetAccessPlan>,
    expected_rows: usize,
    predicate: Option<Expr>,
}

impl TestFull {
    async fn run(self) -> Result<MetricsSet> {
        let ctx = SessionContext::new();

        let Self {
            access_plan,
            expected_rows,
            predicate,
        } = self;

        let TestData {
            _temp_file: _,
            ref schema,
            ref file_name,
            ref file_size,
        } = get_test_data();

        let new_file_name = if cfg!(target_os = "windows") {
            // Windows path separator is different from Unix
            file_name.replace("\\", "/")
        } else {
            file_name.clone()
        };

        let mut partitioned_file = PartitionedFile::new(new_file_name, *file_size);

        // add the access plan, if any, as an extension
        if let Some(access_plan) = access_plan {
            partitioned_file = partitioned_file.with_extensions(Arc::new(access_plan));
        }

        // Create a DataSourceExec to read the file
        let object_store_url = ObjectStoreUrl::local_filesystem();
        // add the predicate, if requested
        let source = if let Some(predicate) = predicate {
            let df_schema = DFSchema::try_from(schema.clone())?;
            let predicate = ctx.create_physical_expr(predicate, &df_schema)?;
            Arc::new(ParquetSource::default().with_predicate(schema.clone(), predicate))
        } else {
            Arc::new(ParquetSource::default())
        };
        let config = FileScanConfigBuilder::new(object_store_url, schema.clone(), source)
            .with_file(partitioned_file)
            .build();

        let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);

        // run the DataSourceExec and collect the results
        let results =
            datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx()).await?;

        // calculate the total number of rows that came out
        let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
        assert_eq!(
            total_rows,
            expected_rows,
            "results: \n{}",
            pretty_format_batches(&results).unwrap()
        );

        std::fs::remove_file(file_name).unwrap();

        Ok(MetricsFinder::find_metrics(plan.as_ref()).unwrap())
    }
}

// Holds necessary data for these tests to reuse the same parquet file
struct TestData {
    /// Pointer to temporary file storage. Keeping it in scope to prevent temporary folder
    /// to be deleted prematurely
    _temp_file: NamedTempFile,
    schema: SchemaRef,
    file_name: String,
    file_size: u64,
}

/// Return a parquet file with 2 row groups each with 5 rows
fn get_test_data() -> TestData {
    let scenario = Scenario::UTF8;
    let row_per_group = 5;

    let mut temp_file = tempfile::Builder::new()
        .prefix("user_access_plan")
        .suffix(".parquet")
        .tempfile_in(Path::new(""))
        .expect("tempfile creation");

    let props = WriterProperties::builder()
        .set_max_row_group_size(row_per_group)
        .build();

    let batches = create_data_batch(scenario);
    let schema = batches[0].schema();

    let mut writer =
        ArrowWriter::try_new(&mut temp_file, schema.clone(), Some(props)).unwrap();

    for batch in batches {
        writer.write(&batch).expect("writing batch");
    }
    writer.close().unwrap();

    let file_name = temp_file.path().to_string_lossy().to_string();
    let file_size = temp_file.path().metadata().unwrap().len();

    TestData {
        _temp_file: temp_file,
        schema,
        file_name,
        file_size,
    }
}

/// Return the total value of the specified metric name
fn metric_value(parquet_metrics: &MetricsSet, metric_name: &str) -> Option<usize> {
    parquet_metrics
        .sum(|metric| metric.value().name() == metric_name)
        .map(|v| v.as_usize())
}
