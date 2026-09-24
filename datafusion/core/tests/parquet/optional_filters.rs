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

//! End-to-end tests for *optional filters* in the Parquet scan with row
//! level filter pushdown (`pushdown_filters = true`).
//!
//! An optional filter is a predicate conjunct wrapped in an
//! `OptionalFilterPhysicalExpr`. The scan handles it as
//! `datafusion.execution.optional_filter_mode` says:
//!
//! * `always`: like all other filters.
//! * `adaptive`: an `OptionalFilterGate` skips it while it removes too few
//!   rows, or while it costs more than it saves.
//! * `pruning_only`: only statistics pruning uses it.
//!
//! No operator makes optional filters yet, thus these tests put an
//! `Optional(...)` predicate on the `ParquetSource` directly.

use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, RecordBatch};
use arrow::compute::concat_batches;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::{ExecutionPlan, collect, execute_stream};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::ScalarValue;
use datafusion_common::config::OptionalFilterMode;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, OptionalFilterPhysicalExpr, lit,
};
use datafusion_physical_expr::optional_filter_gate::OptionalFilterGateConfig;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::NamedTempFile;

use crate::parquet::utils::MetricsFinder;

const ROW_GROUPS: usize = 10;
const ROWS_PER_ROW_GROUP: usize = 2000;
/// Each row filter evaluation sees one batch of this many rows.
const BATCH_SIZE: usize = 100;

/// A file with `ROW_GROUPS` row groups. Column `a` is `i % 100` (each row
/// group has all values `0..100`, thus statistics cannot prune it), `rg` is
/// the row group index and `v` is the row number.
fn write_file() -> (NamedTempFile, SchemaRef) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("rg", DataType::Int32, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let file = NamedTempFile::new().unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS_PER_ROW_GROUP))
        .build();
    let mut writer =
        ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), Some(props))
            .unwrap();
    for rg in 0..ROW_GROUPS {
        let start = rg * ROWS_PER_ROW_GROUP;
        let rows = start..start + ROWS_PER_ROW_GROUP;
        let a: Int32Array = rows.clone().map(|i| (i % 100) as i32).collect();
        let rg_col = Int32Array::from(vec![rg as i32; ROWS_PER_ROW_GROUP]);
        let v: Int64Array = rows.map(|i| i as i64).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(a), Arc::new(rg_col), Arc::new(v)],
        )
        .unwrap();
        writer.write(&batch).unwrap();
    }
    let metadata = writer.close().unwrap();
    assert_eq!(metadata.num_row_groups(), ROW_GROUPS);
    (file, schema)
}

fn col_a(schema: &Schema) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new_with_schema("a", schema).unwrap())
}

fn a_op(schema: &Schema, op: Operator, value: i32) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(
        col_a(schema),
        op,
        lit(ScalarValue::Int32(Some(value))),
    ))
}

fn optional(inner: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
    Arc::new(OptionalFilterPhysicalExpr::new(inner))
}

/// A scan of all columns of `file`. The gates of the optional filters
/// assume that a removed row saves a very large amount of work, thus the
/// cost check (which uses the wall clock) never pauses a filter that
/// removes rows, and the tests are deterministic. See [`scan_with`].
fn scan(
    file: &NamedTempFile,
    schema: &SchemaRef,
    predicate: Arc<dyn PhysicalExpr>,
    mode: OptionalFilterMode,
) -> Arc<dyn ExecutionPlan> {
    scan_with(file, schema, predicate, mode, 1e9, None)
}

/// A scan of the columns `projection` (all columns if `None`) of `file`,
/// with `min_saving_ns_per_row` for the gates of the optional filters.
fn scan_with(
    file: &NamedTempFile,
    schema: &SchemaRef,
    predicate: Arc<dyn PhysicalExpr>,
    mode: OptionalFilterMode,
    min_saving_ns_per_row: f64,
    projection: Option<Vec<usize>>,
) -> Arc<dyn ExecutionPlan> {
    let source = ParquetSource::new(Arc::clone(schema))
        .with_predicate(predicate)
        .with_pushdown_filters(true)
        .with_optional_filter_mode(mode)
        .with_optional_filter_gate_config(OptionalFilterGateConfig {
            min_saving_ns_per_row,
            ..Default::default()
        });
    let path = file.path().to_str().unwrap().to_string();
    let size = std::fs::metadata(&path).unwrap().len();
    let config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
            .with_file(PartitionedFile::new(path, size))
            .with_projection_indices(projection)
            .unwrap()
            .build();
    DataSourceExec::from_data_source(config)
}

fn session() -> SessionContext {
    SessionContext::new_with_config(SessionConfig::new().with_batch_size(BATCH_SIZE))
}

fn metric(plan: &dyn ExecutionPlan, name: &str) -> usize {
    MetricsFinder::find_metrics(plan)
        .unwrap()
        .sum_by_name(name)
        .map_or(0, |v| v.as_usize())
}

/// Sorted values of column `v`.
fn values(batches: &[RecordBatch]) -> Vec<i64> {
    let mut values: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            let v = batch
                .column_by_name("v")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            v.values().to_vec()
        })
        .collect();
    values.sort_unstable();
    values
}

/// A non-selective optional filter (99% of the rows pass it) is paused by
/// the gate in `adaptive` mode. The consumer of the scan (here a
/// `FilterExec`, like a hash join for a join dynamic filter) applies the
/// filter again, thus the results are the same in all modes.
#[tokio::test]
async fn non_selective_optional_filter_is_paused() {
    let (file, schema) = write_file();
    let total_rows = ROW_GROUPS * ROWS_PER_ROW_GROUP;
    let removed_rows = total_rows / 100;

    let mut results = vec![];
    for mode in [
        OptionalFilterMode::Always,
        OptionalFilterMode::Adaptive,
        OptionalFilterMode::PruningOnly,
    ] {
        let filter = a_op(&schema, Operator::NotEq, 99);
        let scan = scan(&file, &schema, optional(Arc::clone(&filter)), mode);
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(filter, Arc::clone(&scan)).unwrap());
        let batches = collect(plan, session().task_ctx()).await.unwrap();
        let values = values(&batches);
        assert_eq!(values.len(), total_rows - removed_rows, "mode {mode}");

        let pruned = metric(scan.as_ref(), "pushdown_rows_pruned");
        let skipped = metric(scan.as_ref(), "optional_filter_rows_skipped");
        let pauses = metric(scan.as_ref(), "optional_filter_pauses");
        match mode {
            OptionalFilterMode::Always => {
                assert_eq!(pruned, removed_rows);
                assert_eq!(skipped, 0);
                assert_eq!(pauses, 0);
            }
            OptionalFilterMode::Adaptive => {
                assert!(skipped > total_rows / 2, "skipped {skipped} rows");
                assert!(pauses > 0);
                assert!(pruned < removed_rows);
                // Each row is either skipped or evaluated.
                let matched = metric(scan.as_ref(), "pushdown_rows_matched");
                assert_eq!(matched + pruned, total_rows);
            }
            OptionalFilterMode::PruningOnly => {
                // No row filter at all.
                assert_eq!(pruned, 0);
                assert_eq!(metric(scan.as_ref(), "pushdown_rows_matched"), 0);
                assert_eq!(skipped, 0);
                assert_eq!(pauses, 0);
            }
        }
        results.push(values);
    }
    assert_eq!(results[0], results[1]);
    assert_eq!(results[0], results[2]);
}

/// A selective optional filter is never paused, and the scan output is the
/// same as in `always` mode.
#[tokio::test]
async fn selective_optional_filter_is_not_paused() {
    let (file, schema) = write_file();
    let predicate = optional(a_op(&schema, Operator::Lt, 10));
    let mut results = vec![];
    for mode in [OptionalFilterMode::Always, OptionalFilterMode::Adaptive] {
        let scan = scan(&file, &schema, Arc::clone(&predicate), mode);
        let batches = collect(Arc::clone(&scan), session().task_ctx())
            .await
            .unwrap();
        assert_eq!(metric(scan.as_ref(), "optional_filter_rows_skipped"), 0);
        assert_eq!(metric(scan.as_ref(), "optional_filter_pauses"), 0);
        results.push(values(&batches));
    }
    assert_eq!(results[0].len(), ROW_GROUPS * ROWS_PER_ROW_GROUP / 10);
    assert_eq!(results[0], results[1]);
}

/// A selective optional filter is paused when it costs more than it saves.
/// The scan reads only column `a`, which the filter reads too: a removed row
/// saves no decode time. With `min_saving_ns_per_row = 0`, a removed row
/// saves nothing, thus any evaluation time is too much and the gate pauses
/// `a < 10` although it keeps only 10% of the rows. The consumer of the scan
/// applies the filter again, thus the result does not change.
#[tokio::test]
async fn selective_optional_filter_is_paused_when_it_costs_more_than_it_saves() {
    let (file, schema) = write_file();
    let filter = a_op(&schema, Operator::Lt, 10);
    let scan = scan_with(
        &file,
        &schema,
        optional(Arc::clone(&filter)),
        OptionalFilterMode::Adaptive,
        0.0,
        Some(vec![0]),
    );
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(filter, Arc::clone(&scan)).unwrap());
    let batches = collect(plan, session().task_ctx()).await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, ROW_GROUPS * ROWS_PER_ROW_GROUP / 10);
    assert!(metric(scan.as_ref(), "optional_filter_pauses") > 0);
    assert!(metric(scan.as_ref(), "optional_filter_rows_skipped") > 0);
    assert!(metric(scan.as_ref(), "optional_filter_eval_time") > 0);
}

/// A required conjunct stays a normal row filter predicate in all modes.
#[tokio::test]
async fn required_conjunct_is_unchanged() {
    let (file, schema) = write_file();
    // a < 50 AND Optional(a != 99)
    let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        a_op(&schema, Operator::Lt, 50),
        Operator::And,
        optional(a_op(&schema, Operator::NotEq, 99)),
    ));
    for mode in [
        OptionalFilterMode::Always,
        OptionalFilterMode::Adaptive,
        OptionalFilterMode::PruningOnly,
    ] {
        let scan = scan(&file, &schema, Arc::clone(&predicate), mode);
        let batches = collect(Arc::clone(&scan), session().task_ctx())
            .await
            .unwrap();
        assert_eq!(
            values(&batches).len(),
            ROW_GROUPS * ROWS_PER_ROW_GROUP / 2,
            "mode {mode}"
        );
    }
}

/// An optional dynamic filter that changes during the scan: the gate paused
/// the first (non-selective) version of the filter, and evaluates the filter
/// again when the filter changes.
#[tokio::test]
async fn optional_dynamic_filter_update_restarts_evaluation() {
    let (file, schema) = write_file();
    let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![col_a(&schema)],
        a_op(&schema, Operator::NotEq, 99),
    ));
    let scan = scan(
        &file,
        &schema,
        optional(Arc::clone(&dynamic) as Arc<dyn PhysicalExpr>),
        OptionalFilterMode::Adaptive,
    );
    let mut stream = execute_stream(Arc::clone(&scan), session().task_ctx()).unwrap();

    // Read until the scan is in the third row group. The gate paused the
    // non-selective filter in the first row group.
    let mut before_update = vec![];
    let mut max_rg_before_update = 0;
    while max_rg_before_update < 2 {
        let batch = stream.next().await.unwrap().unwrap();
        max_rg_before_update = max_rg_before_update.max(max_rg(&batch));
        before_update.push(batch);
    }
    assert!(metric(scan.as_ref(), "optional_filter_rows_skipped") > 0);
    assert!(metric(scan.as_ref(), "optional_filter_pauses") > 0);

    // The filter becomes selective.
    dynamic.update(a_op(&schema, Operator::Lt, 10)).unwrap();

    let mut after_update = vec![];
    while let Some(batch) = stream.next().await {
        after_update.push(batch.unwrap());
    }
    let after_update = concat_batches(&after_update[0].schema(), &after_update).unwrap();
    // The scan evaluates the filter of a row group before it returns the
    // first batch of that row group. Thus all row groups after the current
    // one use the new filter.
    let a = column_i32(&after_update, "a");
    let rg = column_i32(&after_update, "rg");
    let mut checked_rows = 0;
    for (a, rg) in a.iter().zip(rg.iter()) {
        if *rg > max_rg_before_update {
            assert!(*a < 10, "row with a = {a} in row group {rg} passed");
            checked_rows += 1;
        }
    }
    let later_row_groups = ROW_GROUPS - 1 - max_rg_before_update as usize;
    assert_eq!(checked_rows, later_row_groups * ROWS_PER_ROW_GROUP / 10);
}

fn column_i32<'a>(batch: &'a RecordBatch, name: &str) -> &'a [i32] {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
}

fn max_rg(batch: &RecordBatch) -> i32 {
    column_i32(batch, "rg").iter().copied().max().unwrap_or(0)
}
