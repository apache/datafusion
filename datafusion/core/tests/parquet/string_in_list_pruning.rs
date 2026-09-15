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

//! End-to-end coverage for compact, large IN-list pruning. The string cases
//! exercise detailed edge conditions; representative ordered types verify the
//! same path against real Parquet row-group and page statistics.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, Date64Array, Decimal128Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::config::TableParquetOptions;
use datafusion_common::{ScalarValue, assert_batches_eq};
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_physical_expr::expressions::{col, in_list, lit};
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::metrics::{MetricValue, MetricsSet};
use object_store::path::Path;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tempfile::NamedTempFile;

use super::utils::MetricsFinder;

const ROWS_PER_UNIT: usize = 16;
const UNITS: usize = 4;
const TOTAL_ROWS: usize = ROWS_PER_UNIT * UNITS;
const MATCHING_ROWS: usize = ROWS_PER_UNIT * 2;

fn assert_file_layout(
    metadata: &ParquetMetaData,
    total_rows: usize,
    rows_per_group: usize,
) {
    assert_eq!(metadata.num_row_groups(), total_rows / rows_per_group);
    let offsets = metadata.offset_index().unwrap();
    for row_group in offsets {
        assert_eq!(
            row_group[0].page_locations().len(),
            rows_per_group / ROWS_PER_UNIT
        );
    }
}

/// Write either four row groups or four pages in one row group. Each unit holds
/// a single repeated value, so `NOT IN` can exclude the two units whose value is
/// a list member. The second unit lies in a gap between two members of every
/// test IN list; an enclosing min/max range for the list cannot prune it.
fn make_file(page_pruning: bool) -> NamedTempFile {
    let values = ["v000000", "v000001", "v000010", "v999999"]
        .into_iter()
        .flat_map(|value| std::iter::repeat_n(value, ROWS_PER_UNIT))
        .collect::<Vec<_>>();
    write_file(page_pruning, values)
}

/// Write one mixed unit and one single-valued unit. The mixed unit contains
/// both a list member and a value that satisfies `NOT IN`.
fn make_mixed_not_in_file(page_pruning: bool) -> NamedTempFile {
    let values = std::iter::repeat_n("v000000", ROWS_PER_UNIT / 2)
        .chain(std::iter::repeat_n("v000001", ROWS_PER_UNIT / 2))
        .chain(std::iter::repeat_n("v000010", ROWS_PER_UNIT))
        .collect::<Vec<_>>();
    write_file(page_pruning, values)
}

fn write_file(page_pruning: bool, values: Vec<&str>) -> NamedTempFile {
    write_file_with_truncation(page_pruning, values, None)
}

fn write_file_with_truncation(
    page_pruning: bool,
    values: Vec<&str>,
    truncation_length: Option<usize>,
) -> NamedTempFile {
    let mut file = tempfile::Builder::new()
        .prefix("string_in_list_pruning")
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let total_rows = values.len();
    assert_eq!(total_rows % ROWS_PER_UNIT, 0);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(values))],
    )
    .unwrap();
    let rows_per_group = if page_pruning {
        total_rows
    } else {
        ROWS_PER_UNIT
    };
    let mut properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rows_per_group))
        .set_data_page_row_count_limit(ROWS_PER_UNIT)
        .set_write_batch_size(ROWS_PER_UNIT)
        .set_dictionary_enabled(false)
        .set_bloom_filter_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Page);
    if let Some(truncation_length) = truncation_length {
        properties = properties
            .set_statistics_truncate_length(Some(truncation_length))
            .set_column_index_truncate_length(Some(truncation_length));
    }
    let properties = properties.build();
    let mut writer = ArrowWriter::try_new(&mut file, schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    let metadata = writer.close().unwrap();
    assert_file_layout(&metadata, total_rows, rows_per_group);
    file
}

struct ScanOutput {
    batches: Vec<RecordBatch>,
    plan: String,
    metrics: MetricsSet,
}

impl ScanOutput {
    fn counter(&self, name: &str) -> usize {
        self.metrics
            .sum(|metric| metric.value().name() == name)
            .unwrap_or_else(|| panic!("missing {name}: {}", self.metrics))
            .as_usize()
    }

    fn pruned(&self, name: &str) -> usize {
        let value = self
            .metrics
            .sum(|metric| metric.value().name() == name)
            .unwrap_or_else(|| panic!("missing {name}: {}", self.metrics));
        let MetricValue::PruningMetrics {
            pruning_metrics, ..
        } = value
        else {
            panic!("expected pruning metric {name}: {}", self.metrics);
        };
        pruning_metrics.pruned()
    }

    fn fully_matched(&self, name: &str) -> usize {
        let value = self
            .metrics
            .sum(|metric| metric.value().name() == name)
            .unwrap_or_else(|| panic!("missing {name}: {}", self.metrics));
        let MetricValue::PruningMetrics {
            pruning_metrics, ..
        } = value
        else {
            panic!("expected pruning metric {name}: {}", self.metrics);
        };
        pruning_metrics.fully_matched()
    }

    fn assert_results(&self) {
        assert_batches_eq!(
            [
                "+---------+----+",
                "| value   | n  |",
                "+---------+----+",
                "| v000000 | 16 |",
                "| v000010 | 16 |",
                "+---------+----+",
            ],
            &self.batches
        );
        self.assert_no_filter_interference();
    }

    /// The complement of [`Self::assert_results`]: the two units whose value is
    /// not a list member.
    fn assert_negated_results(&self) {
        assert_batches_eq!(
            [
                "+---------+----+",
                "| value   | n  |",
                "+---------+----+",
                "| v000001 | 16 |",
                "| v999999 | 16 |",
                "+---------+----+",
            ],
            &self.batches
        );
        self.assert_no_filter_interference();
    }

    fn assert_no_filter_interference(&self) {
        assert_eq!(self.counter("predicate_evaluation_errors"), 0);
        assert_eq!(self.counter("pushdown_rows_pruned"), 0);
        assert_eq!(self.pruned("row_groups_pruned_bloom_filter"), 0);
    }
}

async fn scan(
    file: &NamedTempFile,
    list_size: usize,
    max_in_list_size: Option<usize>,
    page_pruning: bool,
    negated: bool,
) -> ScanOutput {
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_parquet_bloom_filter_pruning(false)
        .with_parquet_page_index_pruning(page_pruning);
    config.options_mut().execution.parquet.pushdown_filters = false;
    if let Some(max_in_list_size) = max_in_list_size {
        config.options_mut().execution.parquet.max_in_list_size = max_in_list_size;
    }
    let ctx = SessionContext::new_with_config(config);
    ctx.register_parquet(
        "t",
        file.path().to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    let values = (0..list_size)
        .map(|index| format!("'v{:06}'", index * 10))
        .collect::<Vec<_>>()
        .join(", ");
    let op = if negated { "NOT IN" } else { "IN" };
    let sql = format!(
        "SELECT value, count(*) AS n FROM t \
         WHERE value {op} ({values}) GROUP BY value ORDER BY value"
    );
    let plan = ctx
        .sql(&sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    let metrics = MetricsFinder::find_metrics(plan.as_ref()).unwrap();
    ScanOutput {
        batches,
        plan: plan_text,
        metrics,
    }
}

fn repeated_ordered_values(scale: i64) -> Vec<i64> {
    [0, 1, 2, 100]
        .into_iter()
        .flat_map(|value| std::iter::repeat_n(value * scale, ROWS_PER_UNIT))
        .collect()
}

fn write_ordered_file(
    schema: &Arc<Schema>,
    values: ArrayRef,
    page_pruning: bool,
) -> NamedTempFile {
    let mut file = tempfile::Builder::new()
        .prefix("ordered_in_list_pruning")
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let rows_per_group = if page_pruning {
        TOTAL_ROWS
    } else {
        ROWS_PER_UNIT
    };
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rows_per_group))
        .set_data_page_row_count_limit(ROWS_PER_UNIT)
        .set_write_batch_size(ROWS_PER_UNIT)
        .set_dictionary_enabled(false)
        .set_bloom_filter_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build();
    let batch = RecordBatch::try_new(Arc::clone(schema), vec![values]).unwrap();
    let mut writer =
        ArrowWriter::try_new(&mut file, Arc::clone(schema), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    let metadata = writer.close().unwrap();
    assert_file_layout(&metadata, TOTAL_ROWS, rows_per_group);
    file
}

async fn scan_ordered(
    file: &NamedTempFile,
    schema: &Arc<Schema>,
    list: &[ScalarValue],
    max_in_list_size: usize,
    page_pruning: bool,
    negated: bool,
) -> ScanOutput {
    let predicate = in_list(
        col("value", schema).unwrap(),
        list.iter().cloned().map(lit).collect(),
        &negated,
        schema,
    )
    .unwrap();
    let mut options = TableParquetOptions::default();
    options.global.max_in_list_size = max_in_list_size;
    let source = Arc::new(
        ParquetSource::new(Arc::clone(schema))
            .with_table_parquet_options(options)
            .with_predicate(Arc::clone(&predicate))
            .with_pushdown_filters(false)
            .with_enable_page_index(page_pruning)
            .with_bloom_filter_on_read(false),
    );
    let location = Path::from_filesystem_path(file.path()).unwrap();
    let partitioned_file = PartitionedFile::new(
        location.to_string(),
        file.as_file().metadata().unwrap().len(),
    );
    let config = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
        .with_file(partitioned_file)
        .build();
    let scan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(config)));
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate, scan).unwrap());
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    let metrics = MetricsFinder::find_metrics(plan.as_ref()).unwrap();
    ScanOutput {
        batches,
        plan: plan_text,
        metrics,
    }
}

async fn check_string_in_list_pruning(page_pruning: bool) {
    let file = make_file(page_pruning);
    for list_size in [20, 21, 256, 1024] {
        // A zero cap provides a result-equivalence control that cannot use
        // min/max IN-list pruning at either granularity.
        let unpruned = scan(&file, list_size, Some(0), page_pruning, false).await;
        unpruned.assert_results();
        assert!(!unpruned.plan.contains("IN_SET_INTERSECTS"));
        assert_eq!(unpruned.pruned("row_groups_pruned_statistics"), 0);
        assert_eq!(unpruned.pruned("page_index_rows_pruned"), 0);
        assert_eq!(unpruned.counter("output_rows"), TOTAL_ROWS);

        let output = scan(&file, list_size, Some(list_size), page_pruning, false).await;
        output.assert_results();
        assert_eq!(
            pretty_format_batches(&output.batches).unwrap().to_string(),
            pretty_format_batches(&unpruned.batches)
                .unwrap()
                .to_string()
        );
        assert_eq!(
            output.plan.contains("IN_SET_INTERSECTS"),
            list_size > 20,
            "list_size={list_size}, plan={}",
            output.plan
        );
        assert_eq!(
            output.pruned("row_groups_pruned_statistics"),
            if page_pruning { 0 } else { 2 },
            "list_size={list_size}, metrics={}",
            output.metrics
        );
        assert_eq!(
            output.pruned("page_index_rows_pruned"),
            if page_pruning { MATCHING_ROWS } else { 0 },
            "list_size={list_size}, metrics={}",
            output.metrics
        );
        assert_eq!(output.counter("output_rows"), MATCHING_ROWS);
    }

    // The default remains 20: enabling the compact representation must not
    // silently change the public cap's meaning.
    let default = scan(&file, 21, None, page_pruning, false).await;
    default.assert_results();
    assert!(!default.plan.contains("IN_SET_INTERSECTS"));
    assert_eq!(default.pruned("row_groups_pruned_statistics"), 0);
    assert_eq!(default.pruned("page_index_rows_pruned"), 0);
    assert_eq!(default.counter("output_rows"), TOTAL_ROWS);
}

/// The compact `NOT IN` form must prune exactly the units whose single repeated
/// value is a list member, and nothing else. Overlapping a list member is not
/// enough: units 1 and 3 each sit inside the list's enclosing range.
async fn check_string_not_in_list_pruning(page_pruning: bool) {
    let file = make_file(page_pruning);
    for list_size in [20, 21, 256, 1024] {
        let unpruned = scan(&file, list_size, Some(0), page_pruning, true).await;
        unpruned.assert_negated_results();
        assert!(!unpruned.plan.contains("NOT_IN_SET_MAY_MATCH"));
        assert_eq!(unpruned.pruned("row_groups_pruned_statistics"), 0);
        assert_eq!(unpruned.pruned("page_index_rows_pruned"), 0);
        assert_eq!(unpruned.counter("output_rows"), TOTAL_ROWS);

        let output = scan(&file, list_size, Some(list_size), page_pruning, true).await;
        output.assert_negated_results();
        assert_eq!(
            pretty_format_batches(&output.batches).unwrap().to_string(),
            pretty_format_batches(&unpruned.batches)
                .unwrap()
                .to_string()
        );
        assert_eq!(
            output.plan.contains("NOT_IN_SET_MAY_MATCH"),
            list_size > 20,
            "list_size={list_size}, plan={}",
            output.plan
        );
        // The compact form and the per-value AND chain it replaces prune the
        // same units, so the counts do not depend on which one ran.
        assert_eq!(
            output.pruned("row_groups_pruned_statistics"),
            if page_pruning { 0 } else { 2 },
            "list_size={list_size}, metrics={}",
            output.metrics
        );
        assert_eq!(
            output.pruned("page_index_rows_pruned"),
            if page_pruning { MATCHING_ROWS } else { 0 },
            "list_size={list_size}, metrics={}",
            output.metrics
        );
        assert_eq!(output.counter("output_rows"), MATCHING_ROWS);
    }

    let default = scan(&file, 21, None, page_pruning, true).await;
    default.assert_negated_results();
    assert!(!default.plan.contains("NOT_IN_SET_MAY_MATCH"));
    assert_eq!(default.pruned("row_groups_pruned_statistics"), 0);
    assert_eq!(default.pruned("page_index_rows_pruned"), 0);
    assert_eq!(default.counter("output_rows"), TOTAL_ROWS);

    // A mixed interval that overlaps a list member can still contain matching
    // rows. Only the adjacent single-valued unit can be excluded.
    let mixed_file = make_mixed_not_in_file(page_pruning);
    let unpruned = scan(&mixed_file, 21, Some(0), page_pruning, true).await;
    assert_batches_eq!(
        [
            "+---------+---+",
            "| value   | n |",
            "+---------+---+",
            "| v000001 | 8 |",
            "+---------+---+",
        ],
        &unpruned.batches
    );
    unpruned.assert_no_filter_interference();
    assert_eq!(unpruned.pruned("row_groups_pruned_statistics"), 0);
    assert_eq!(unpruned.pruned("page_index_rows_pruned"), 0);
    assert_eq!(unpruned.counter("output_rows"), ROWS_PER_UNIT * 2);

    let output = scan(&mixed_file, 21, Some(21), page_pruning, true).await;
    assert_eq!(
        pretty_format_batches(&output.batches).unwrap().to_string(),
        pretty_format_batches(&unpruned.batches)
            .unwrap()
            .to_string()
    );
    output.assert_no_filter_interference();
    assert!(output.plan.contains("NOT_IN_SET_MAY_MATCH"));
    assert_eq!(
        output.pruned("row_groups_pruned_statistics"),
        usize::from(!page_pruning)
    );
    assert_eq!(
        output.pruned("page_index_rows_pruned"),
        if page_pruning { ROWS_PER_UNIT } else { 0 }
    );
    assert_eq!(output.counter("output_rows"), ROWS_PER_UNIT);
}

async fn check_string_not_in_list_with_truncated_bounds(page_pruning: bool) {
    // Exact bounds for this singleton are equal and can exclude the unit. With
    // four-byte truncation, Parquet lowers the min and raises the max instead.
    let values = std::iter::repeat_n("v000000", ROWS_PER_UNIT).collect();
    let file = write_file_with_truncation(page_pruning, values, Some(4));
    let output = scan(&file, 21, Some(21), page_pruning, true).await;

    assert!(output.batches.iter().all(|batch| batch.num_rows() == 0));
    output.assert_no_filter_interference();
    assert!(output.plan.contains("NOT_IN_SET_MAY_MATCH"));
    assert_eq!(output.pruned("row_groups_pruned_statistics"), 0);
    assert_eq!(output.pruned("page_index_rows_pruned"), 0);
    assert_eq!(output.counter("output_rows"), ROWS_PER_UNIT);
}

#[tokio::test]
async fn ordered_in_list_parquet_pruning() {
    let decimal_values = Decimal128Array::from_iter_values(
        repeated_ordered_values(1).into_iter().map(i128::from),
    )
    .with_precision_and_scale(18, 2)
    .unwrap();
    let cases = [
        (
            "binary",
            Arc::new(BinaryArray::from_iter_values(
                repeated_ordered_values(1)
                    .into_iter()
                    .map(|value| [0xff, value as u8]),
            )) as ArrayRef,
            (0..21)
                .map(|value| ScalarValue::Binary(Some(vec![0xff, value * 2])))
                .collect::<Vec<_>>(),
        ),
        (
            "int64",
            Arc::new(Int64Array::from(repeated_ordered_values(1))) as ArrayRef,
            (0..21)
                .map(|value| ScalarValue::Int64(Some(value * 2)))
                .collect(),
        ),
        (
            "decimal128",
            Arc::new(decimal_values) as ArrayRef,
            (0..21)
                .map(|value| ScalarValue::Decimal128(Some(value * 2), 18, 2))
                .collect(),
        ),
        (
            "date64",
            Arc::new(Date64Array::from(repeated_ordered_values(86_400_000))) as ArrayRef,
            (0..21)
                .map(|value| ScalarValue::Date64(Some(value * 2 * 86_400_000)))
                .collect(),
        ),
        (
            "timestamp_microsecond_utc",
            Arc::new(
                TimestampMicrosecondArray::from(repeated_ordered_values(1_000_000))
                    .with_timezone("UTC"),
            ) as ArrayRef,
            (0..21)
                .map(|value| {
                    ScalarValue::TimestampMicrosecond(
                        Some(value * 2 * 1_000_000),
                        Some("UTC".into()),
                    )
                })
                .collect(),
        ),
    ];

    for (name, values, list) in cases {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            values.data_type().clone(),
            false,
        )]));
        for page_pruning in [false, true] {
            let file = write_ordered_file(&schema, Arc::clone(&values), page_pruning);
            for negated in [false, true] {
                let control =
                    scan_ordered(&file, &schema, &list, 0, page_pruning, negated).await;
                let output =
                    scan_ordered(&file, &schema, &list, 32, page_pruning, negated).await;
                control.assert_no_filter_interference();
                output.assert_no_filter_interference();
                assert!(!control.plan.contains("IN_SET_INTERSECTS"));
                assert!(!control.plan.contains("NOT_IN_SET_MAY_MATCH"));
                assert_eq!(
                    pretty_format_batches(&output.batches).unwrap().to_string(),
                    pretty_format_batches(&control.batches).unwrap().to_string(),
                    "type={name}, page_pruning={page_pruning}, negated={negated}"
                );
                assert_eq!(
                    output
                        .batches
                        .iter()
                        .map(RecordBatch::num_rows)
                        .sum::<usize>(),
                    MATCHING_ROWS,
                    "type={name}, page_pruning={page_pruning}, negated={negated}"
                );
                assert!(
                    output.plan.contains(if negated {
                        "NOT_IN_SET_MAY_MATCH"
                    } else {
                        "IN_SET_INTERSECTS"
                    }),
                    "type={name}, page_pruning={page_pruning}, negated={negated}, plan={}",
                    output.plan
                );
                assert_eq!(
                    output.pruned("row_groups_pruned_statistics"),
                    if page_pruning { 0 } else { 2 },
                    "type={name}, page_pruning={page_pruning}, negated={negated}"
                );
                assert_eq!(
                    output.pruned("page_index_rows_pruned"),
                    if page_pruning { MATCHING_ROWS } else { 0 },
                    "type={name}, page_pruning={page_pruning}, negated={negated}"
                );
            }
        }
    }
}

#[tokio::test]
async fn string_in_list_row_group_pruning() {
    check_string_in_list_pruning(false).await;
}

#[tokio::test]
async fn string_in_list_page_pruning() {
    check_string_in_list_pruning(true).await;
}

#[tokio::test]
async fn string_not_in_list_row_group_pruning() {
    check_string_not_in_list_pruning(false).await;
}

#[tokio::test]
async fn string_not_in_list_page_pruning() {
    check_string_not_in_list_pruning(true).await;
}

#[tokio::test]
async fn string_not_in_list_with_truncated_bounds() {
    check_string_not_in_list_with_truncated_bounds(false).await;
    check_string_not_in_list_with_truncated_bounds(true).await;
}

async fn check_in_list_with_null_preserves_filter_semantics(
    name: &str,
    values: ArrayRef,
    list: Vec<ScalarValue>,
    expected: ScalarValue,
) {
    let mut file = tempfile::Builder::new()
        .prefix("in_list_null_pruning")
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        values.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values]).unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .set_bloom_filter_enabled(false)
        .build();
    let mut writer =
        ArrowWriter::try_new(&mut file, Arc::clone(&schema), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    assert_eq!(writer.close().unwrap().num_row_groups(), 2);

    // Build the physical source directly so a logical optimizer cannot fold
    // NOT IN (..., NULL) to an empty relation before the scan.
    let list = list.into_iter().map(lit).collect::<Vec<_>>();
    let location = Path::from_filesystem_path(file.path()).unwrap();
    let partitioned_file = PartitionedFile::new(
        location.to_string(),
        file.as_file().metadata().unwrap().len(),
    );
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));

    for negated in [false, true] {
        let predicate = in_list(
            col("value", &schema).unwrap(),
            list.clone(),
            &negated,
            &schema,
        )
        .unwrap();
        for max_in_list_size in [0, 32] {
            let mut options = TableParquetOptions::default();
            options.global.max_in_list_size = max_in_list_size;
            let source = Arc::new(
                ParquetSource::new(Arc::clone(&schema))
                    .with_table_parquet_options(options)
                    .with_predicate(Arc::clone(&predicate))
                    .with_pushdown_filters(true)
                    .with_enable_page_index(false)
                    .with_bloom_filter_on_read(false),
            );
            let config =
                FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
                    .with_file(partitioned_file.clone())
                    .with_limit(Some(1))
                    .build();
            let plan: Arc<dyn ExecutionPlan> =
                Arc::new(DataSourceExec::new(Arc::new(config)));
            let plan_text = displayable(plan.as_ref()).indent(true).to_string();
            assert!(plan_text.contains("IN"), "type={name}, plan={plan_text}");
            assert_eq!(
                plan_text.contains("IN_SET_INTERSECTS"),
                !negated && max_in_list_size == 32,
                "type={name}, negated={negated}, cap={max_in_list_size}, plan={plan_text}"
            );
            let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
            let output = ScanOutput {
                batches,
                plan: plan_text,
                metrics: MetricsFinder::find_metrics(plan.as_ref()).unwrap(),
            };

            if negated {
                assert!(
                    output.batches.iter().all(|batch| batch.num_rows() == 0),
                    "type={name}, cap={max_in_list_size}"
                );
            } else {
                assert_eq!(
                    output
                        .batches
                        .iter()
                        .map(RecordBatch::num_rows)
                        .sum::<usize>(),
                    1,
                    "type={name}, cap={max_in_list_size}"
                );
                let batch = output
                    .batches
                    .iter()
                    .find(|batch| batch.num_rows() > 0)
                    .unwrap();
                assert_eq!(
                    ScalarValue::try_from_array(batch.column(0), 0).unwrap(),
                    expected,
                    "type={name}, cap={max_in_list_size}"
                );
            }
            assert_eq!(
                output.counter("pushdown_rows_pruned"),
                match (negated, max_in_list_size) {
                    (false, 0) => 7,
                    (false, _) => 3,
                    (true, 0) => 8,
                    (true, _) => 0,
                },
                "type={name}, negated={negated}, cap={max_in_list_size}"
            );
            assert_eq!(output.fully_matched("row_groups_pruned_statistics"), 0);
            assert_eq!(
                output.pruned("row_groups_pruned_statistics"),
                if max_in_list_size == 0 {
                    0
                } else if negated {
                    2
                } else {
                    1
                },
                "type={name}, negated={negated}, cap={max_in_list_size}, metrics={}",
                output.metrics
            );
            assert_eq!(output.pruned("limit_pruned_row_groups"), 0);
            assert_eq!(output.counter("predicate_evaluation_errors"), 0);
        }
    }
}

#[tokio::test]
async fn in_list_with_null_preserves_filter_semantics() {
    // The first row group has a known zero null count, and every value lies
    // in a gap in the IN list. The matching value in the second row group is
    // deliberately not first, so incorrectly bypassing the row filter changes
    // the result when the scan has a limit.
    let mut string_list = (0..21)
        .map(|index| ScalarValue::Utf8(Some(format!("v{:06}", index * 10))))
        .collect::<Vec<_>>();
    string_list.push(ScalarValue::Utf8(None));
    check_in_list_with_null_preserves_filter_semantics(
        "string",
        Arc::new(StringArray::from(vec![
            Some("v000001"),
            Some("v000001"),
            Some("v000001"),
            Some("v000001"),
            Some("v000001"),
            Some("v000000"),
            None,
            Some("v999999"),
        ])) as ArrayRef,
        string_list,
        ScalarValue::Utf8(Some("v000000".into())),
    )
    .await;

    let mut int64_list = (0..21)
        .map(|index| ScalarValue::Int64(Some(index * 10)))
        .collect::<Vec<_>>();
    int64_list.push(ScalarValue::Int64(None));
    check_in_list_with_null_preserves_filter_semantics(
        "int64",
        Arc::new(Int64Array::from(vec![
            Some(1),
            Some(1),
            Some(1),
            Some(1),
            Some(1),
            Some(0),
            None,
            Some(999_999),
        ])) as ArrayRef,
        int64_list,
        ScalarValue::Int64(Some(0)),
    )
    .await;
}
