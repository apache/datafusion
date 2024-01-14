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

use arrow::{
    array::*, datatypes::*, record_batch::RecordBatch,
    util::display::array_value_to_string,
};

use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::{Aggregate, LogicalPlan, TableScan};
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanVisitor;
use datafusion::prelude::*;
use datafusion::test_util;
use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
use datafusion::{datasource::MemTable, physical_plan::collect};
use datafusion::{execution::context::SessionContext, physical_plan::displayable};
use datafusion_common::{assert_contains, assert_not_contains};
use object_store::path::Path;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use tempfile::TempDir;

/// A macro to assert that some particular line contains two substrings
///
/// Usage: `assert_metrics!(actual, operator_name, metrics)`
///
macro_rules! assert_metrics {
    ($ACTUAL: expr, $OPERATOR_NAME: expr, $METRICS: expr) => {
        let found = $ACTUAL
            .lines()
            .any(|line| line.contains($OPERATOR_NAME) && line.contains($METRICS));
        assert!(
            found,
            "Can not find a line with both '{}' and '{}' in\n\n{}",
            $OPERATOR_NAME, $METRICS, $ACTUAL
        );
    };
}

macro_rules! test_expression {
    ($SQL:expr, $EXPECTED:expr) => {
        println!("Input:\n  {}\nExpected:\n  {}\n", $SQL, $EXPECTED);
        let ctx = SessionContext::new();
        let sql = format!("SELECT {}", $SQL);
        let actual = execute(&ctx, sql.as_str()).await;
        assert_eq!(actual[0][0], $EXPECTED);
    };
}

pub mod aggregates;
pub mod create_drop;
pub mod csv_files;
pub mod explain_analyze;
pub mod expr;
pub mod joins;
pub mod partitioned_csv;
pub mod repartition;
pub mod select;
mod sql_api;

fn create_join_context(
    column_left: &str,
    column_right: &str,
    repartition_joins: bool,
) -> Result<SessionContext> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_repartition_joins(repartition_joins)
            .with_target_partitions(2)
            .with_batch_size(4096),
    );

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new(column_left, DataType::UInt32, true),
        Field::new("t1_name", DataType::Utf8, true),
        Field::new("t1_int", DataType::UInt32, true),
    ]));
    let t1_data = RecordBatch::try_new(
        t1_schema,
        vec![
            Arc::new(UInt32Array::from(vec![11, 22, 33, 44])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
            ])),
            Arc::new(UInt32Array::from(vec![1, 2, 3, 4])),
        ],
    )?;
    ctx.register_batch("t1", t1_data)?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new(column_right, DataType::UInt32, true),
        Field::new("t2_name", DataType::Utf8, true),
        Field::new("t2_int", DataType::UInt32, true),
    ]));
    let t2_data = RecordBatch::try_new(
        t2_schema,
        vec![
            Arc::new(UInt32Array::from(vec![11, 22, 44, 55])),
            Arc::new(StringArray::from(vec![
                Some("z"),
                Some("y"),
                Some("x"),
                Some("w"),
            ])),
            Arc::new(UInt32Array::from(vec![3, 1, 3, 3])),
        ],
    )?;
    ctx.register_batch("t2", t2_data)?;

    Ok(ctx)
}

fn create_left_semi_anti_join_context_with_null_ids(
    column_left: &str,
    column_right: &str,
    repartition_joins: bool,
) -> Result<SessionContext> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_repartition_joins(repartition_joins)
            .with_target_partitions(2)
            .with_batch_size(4096),
    );

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new(column_left, DataType::UInt32, true),
        Field::new("t1_name", DataType::Utf8, true),
        Field::new("t1_int", DataType::UInt32, true),
    ]));
    let t1_data = RecordBatch::try_new(
        t1_schema,
        vec![
            Arc::new(UInt32Array::from(vec![
                Some(11),
                Some(11),
                Some(22),
                Some(33),
                Some(44),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
                Some("e"),
            ])),
            Arc::new(UInt32Array::from(vec![1, 1, 2, 3, 4, 0])),
        ],
    )?;
    ctx.register_batch("t1", t1_data)?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new(column_right, DataType::UInt32, true),
        Field::new("t2_name", DataType::Utf8, true),
        Field::new("t2_int", DataType::UInt32, true),
    ]));
    let t2_data = RecordBatch::try_new(
        t2_schema,
        vec![
            Arc::new(UInt32Array::from(vec![
                Some(11),
                Some(11),
                Some(22),
                Some(44),
                Some(55),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("z"),
                Some("z"),
                Some("y"),
                Some("x"),
                Some("w"),
                Some("v"),
            ])),
            Arc::new(UInt32Array::from(vec![3, 3, 1, 3, 3, 0])),
        ],
    )?;
    ctx.register_batch("t2", t2_data)?;

    Ok(ctx)
}

async fn register_aggregate_csv_by_sql(ctx: &SessionContext) {
    let testdata = datafusion::test_util::arrow_test_data();

    let df = ctx
        .sql(&format!(
            "
    CREATE EXTERNAL TABLE aggregate_test_100 (
        c1  VARCHAR NOT NULL,
        c2  TINYINT NOT NULL,
        c3  SMALLINT NOT NULL,
        c4  SMALLINT NOT NULL,
        c5  INTEGER NOT NULL,
        c6  BIGINT NOT NULL,
        c7  SMALLINT NOT NULL,
        c8  INT NOT NULL,
        c9  INT UNSIGNED NOT NULL,
        c10 BIGINT UNSIGNED NOT NULL,
        c11 FLOAT NOT NULL,
        c12 DOUBLE NOT NULL,
        c13 VARCHAR NOT NULL
    )
    STORED AS CSV
    WITH HEADER ROW
    LOCATION '{testdata}/csv/aggregate_test_100.csv'
    "
        ))
        .await
        .expect("Creating dataframe for CREATE EXTERNAL TABLE");

    // Mimic the CLI and execute the resulting plan -- even though it
    // is effectively a no-op (returns zero rows)
    let results = df.collect().await.expect("Executing CREATE EXTERNAL TABLE");
    assert!(
        results.is_empty(),
        "Expected no rows from executing CREATE EXTERNAL TABLE"
    );
}

async fn register_aggregate_csv(ctx: &SessionContext) -> Result<()> {
    let testdata = datafusion::test_util::arrow_test_data();
    let schema = test_util::aggr_test_schema();
    ctx.register_csv(
        "aggregate_test_100",
        &format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::new().schema(&schema),
    )
    .await?;
    Ok(())
}

/// Execute SQL and return results as a RecordBatch
async fn plan_and_collect(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

/// Execute query and return results as a Vec of RecordBatches
async fn execute_to_batches(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    let df = ctx.sql(sql).await.unwrap();

    // optimize just for check schema don't change during optimization.
    df.clone().into_optimized_plan().unwrap();

    df.collect().await.unwrap()
}

/// Execute query and return result set as 2-d table of Vecs
/// `result[row][column]`
async fn execute(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
    result_vec(&execute_to_batches(ctx, sql).await)
}

/// Execute SQL and return results
async fn execute_with_partition(
    sql: &str,
    partition_count: usize,
) -> Result<Vec<RecordBatch>> {
    let tmp_dir = TempDir::new()?;
    let ctx = create_ctx_with_partition(&tmp_dir, partition_count).await?;
    plan_and_collect(&ctx, sql).await
}

/// Generate a partitioned CSV file and register it with an execution context
async fn create_ctx_with_partition(
    tmp_dir: &TempDir,
    partition_count: usize,
) -> Result<SessionContext> {
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));

    let schema = populate_csv_partitions(tmp_dir, partition_count, ".csv")?;

    // register csv file with the execution context
    ctx.register_csv(
        "test",
        tmp_dir.path().to_str().unwrap(),
        CsvReadOptions::new().schema(&schema),
    )
    .await?;

    Ok(ctx)
}

/// Generate CSV partitions within the supplied directory
fn populate_csv_partitions(
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

/// Specialised String representation
fn col_str(column: &ArrayRef, row_index: usize) -> String {
    // NullArray::is_null() does not work on NullArray.
    // can remove check for DataType::Null when
    // https://github.com/apache/arrow-rs/issues/4835 is fixed
    if column.data_type() == &DataType::Null || column.is_null(row_index) {
        return "NULL".to_string();
    }

    array_value_to_string(column, row_index)
        .ok()
        .unwrap_or_else(|| "???".to_string())
}

/// Converts the results into a 2d array of strings, `result[row][column]`
/// Special cases nulls to NULL for testing
fn result_vec(results: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut result = vec![];
    for batch in results {
        for row_index in 0..batch.num_rows() {
            let row_vec = batch
                .columns()
                .iter()
                .map(|column| col_str(column, row_index))
                .collect();
            result.push(row_vec);
        }
    }
    result
}

async fn register_alltypes_parquet(ctx: &SessionContext) {
    let testdata = datafusion::test_util::parquet_test_data();
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{testdata}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
}

/// Return a new table provider that has a single Int32 column with
/// values between `seq_start` and `seq_end`
pub fn table_with_sequence(
    seq_start: i32,
    seq_end: i32,
) -> Result<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
    let arr = Arc::new(Int32Array::from((seq_start..=seq_end).collect::<Vec<_>>()));
    let partitions = vec![vec![RecordBatch::try_new(
        schema.clone(),
        vec![arr as ArrayRef],
    )?]];
    Ok(Arc::new(MemTable::try_new(schema, partitions)?))
}

pub struct ExplainNormalizer {
    replacements: Vec<(String, String)>,
}

impl ExplainNormalizer {
    fn new() -> Self {
        let mut replacements = vec![];

        let mut push_path = |path: PathBuf, key: &str| {
            // Push path as is
            replacements.push((path.to_string_lossy().to_string(), key.to_string()));

            // Push URL representation of path
            let path = Path::from_filesystem_path(path).unwrap();
            replacements.push((path.to_string(), key.to_string()));
        };

        push_path(test_util::arrow_test_data().into(), "ARROW_TEST_DATA");
        push_path(std::env::current_dir().unwrap(), "WORKING_DIR");

        // convert things like partitioning=RoundRobinBatch(16)
        // to partitioning=RoundRobinBatch(NUM_CORES)
        let needle = format!("RoundRobinBatch({})", num_cpus::get());
        replacements.push((needle, "RoundRobinBatch(NUM_CORES)".to_string()));

        Self { replacements }
    }

    fn normalize(&self, s: impl Into<String>) -> String {
        let mut s = s.into();
        for (from, to) in &self.replacements {
            s = s.replace(from, to);
        }
        s
    }
}

/// Applies normalize_for_explain to every line
fn normalize_vec_for_explain(v: Vec<Vec<String>>) -> Vec<Vec<String>> {
    let normalizer = ExplainNormalizer::new();
    v.into_iter()
        .map(|l| {
            l.into_iter()
                .map(|s| normalizer.normalize(s))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

#[tokio::test]
async fn nyc() -> Result<()> {
    // schema for nyxtaxi csv files
    let schema = Schema::new(vec![
        Field::new("VendorID", DataType::Utf8, true),
        Field::new("tpep_pickup_datetime", DataType::Utf8, true),
        Field::new("tpep_dropoff_datetime", DataType::Utf8, true),
        Field::new("passenger_count", DataType::Utf8, true),
        Field::new("trip_distance", DataType::Float64, true),
        Field::new("RatecodeID", DataType::Utf8, true),
        Field::new("store_and_fwd_flag", DataType::Utf8, true),
        Field::new("PULocationID", DataType::Utf8, true),
        Field::new("DOLocationID", DataType::Utf8, true),
        Field::new("payment_type", DataType::Utf8, true),
        Field::new("fare_amount", DataType::Float64, true),
        Field::new("extra", DataType::Float64, true),
        Field::new("mta_tax", DataType::Float64, true),
        Field::new("tip_amount", DataType::Float64, true),
        Field::new("tolls_amount", DataType::Float64, true),
        Field::new("improvement_surcharge", DataType::Float64, true),
        Field::new("total_amount", DataType::Float64, true),
    ]);

    let ctx = SessionContext::new();
    ctx.register_csv(
        "tripdata",
        "file:///file.csv",
        CsvReadOptions::new().schema(&schema),
    )
    .await?;

    let dataframe = ctx
        .sql(
            "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) \
         FROM tripdata GROUP BY passenger_count",
        )
        .await?;
    let optimized_plan = dataframe.into_optimized_plan().unwrap();

    match &optimized_plan {
        LogicalPlan::Aggregate(Aggregate { input, .. }) => match input.as_ref() {
            LogicalPlan::TableScan(TableScan {
                ref projected_schema,
                ..
            }) => {
                assert_eq!(2, projected_schema.fields().len());
                assert_eq!(projected_schema.field(0).name(), "passenger_count");
                assert_eq!(projected_schema.field(1).name(), "fare_amount");
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }

    Ok(())
}
