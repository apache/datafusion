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

//! End-to-end tests for scalar functions with the
//! [`NestedInputAccess`] capability: the Parquet scan decodes only the struct
//! fields such a function reads, and results are unchanged.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, Int32Array, Int64Array, RecordBatch,
    StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Int64Type, Schema};
use arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::collect;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, Result, Statistics};
use datafusion_expr::{
    ColumnarValue, ExpressionPlacement, FieldPath, NestedInputAccess, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_physical_plan::metrics::MetricValue;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

use crate::parquet::utils::MetricsFinder;

/// `value_plus_label_len(s)`: `s.value + length(s.label)`, null if `s` or
/// either field is null. It never reads `s.pad`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ValuePlusLabelLen {
    signature: Signature,
    /// Whether to declare the [`NestedInputAccess`] capability.
    declare_access: bool,
}

impl ValuePlusLabelLen {
    fn new(declare_access: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            declare_access,
        }
    }
}

impl NestedInputAccess for ValuePlusLabelLen {
    fn required_fields(&self, _: &ReturnFieldArgs) -> Vec<Option<Vec<FieldPath>>> {
        vec![Some(vec![vec!["value".into()], vec!["label".into()]])]
    }
}

impl ScalarUDFImpl for ValuePlusLabelLen {
    fn name(&self) -> &str {
        "value_plus_label_len"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let s = args.args[0].to_array(args.number_rows)?;
        let s = s.as_struct();
        // Fields are found by name: the scan may have removed `pad`.
        let value = s
            .column_by_name("value")
            .unwrap()
            .as_primitive::<Int32Type>();
        let label = s.column_by_name("label").unwrap().as_string::<i32>();
        let result = (0..s.len())
            .map(|i| {
                (s.is_valid(i) && value.is_valid(i) && label.is_valid(i))
                    .then(|| value.value(i) + label.value(i).len() as i32)
            })
            .collect::<Int32Array>();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        if args.first() == Some(&ExpressionPlacement::Column) {
            ExpressionPlacement::MoveTowardsLeafNodes
        } else {
            ExpressionPlacement::KeepInPlace
        }
    }

    fn as_nested_input_access(&self) -> Option<&dyn NestedInputAccess> {
        self.declare_access
            .then_some(self as &dyn NestedInputAccess)
    }
}

/// Writes `id: Int32, s: Struct{value: Int32, label: Utf8, pad: Utf8}` with a
/// wide `pad`, so reading it dominates the bytes scanned. Every tenth `s`,
/// and every seventh `label`, is null.
fn write_file(dir: &TempDir) -> String {
    let rows = 2_000;
    let struct_fields = Fields::from(vec![
        Field::new("value", DataType::Int32, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("pad", DataType::Utf8, true),
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("s", DataType::Struct(struct_fields.clone()), true),
    ]));
    let ids = Int32Array::from_iter_values(0..rows);
    let values = Int32Array::from_iter_values((0..rows).map(|i| i * 3));
    let labels = (0..rows)
        .map(|i| (i % 7 != 0).then(|| "x".repeat((i % 13) as usize)))
        .collect::<StringArray>();
    let pads = (0..rows)
        .map(|i| Some(format!("{i:0>256}")))
        .collect::<StringArray>();
    let s = StructArray::try_new(
        struct_fields,
        vec![
            Arc::new(values) as ArrayRef,
            Arc::new(labels),
            Arc::new(pads),
        ],
        Some((0..rows).map(|i| i % 10 != 0).collect()),
    )
    .unwrap();
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids), Arc::new(s)])
            .unwrap();

    let path = dir.path().join("t.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    path.to_str().unwrap().to_string()
}

/// Runs `sql` and returns its formatted output and the Parquet scan's
/// `bytes_scanned`.
async fn run(
    path: &str,
    sql: &str,
    declare_access: bool,
    pushdown_filters: bool,
) -> (String, usize) {
    let config = SessionConfig::new().set_bool(
        "datafusion.execution.parquet.pushdown_filters",
        pushdown_filters,
    );
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udf(ScalarUDF::from(ValuePlusLabelLen::new(declare_access)));
    ctx.register_parquet("t", path, ParquetReadOptions::default())
        .await
        .unwrap();

    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    let metrics = MetricsFinder::find_metrics(plan.as_ref()).unwrap();
    let bytes_scanned = metrics
        .sum(|metric| metric.value().name() == "bytes_scanned")
        .map(|value| value.as_usize())
        .unwrap();
    (
        pretty_format_batches(&batches).unwrap().to_string(),
        bytes_scanned,
    )
}

#[tokio::test]
async fn nested_input_access_reads_only_required_fields() {
    let dir = TempDir::new().unwrap();
    let path = write_file(&dir);

    for sql in [
        "SELECT id, value_plus_label_len(s) AS v FROM t ORDER BY id",
        "SELECT id FROM t WHERE value_plus_label_len(s) > 3000 ORDER BY id",
        "SELECT sum(value_plus_label_len(s)) AS total FROM t",
    ] {
        for pushdown_filters in [false, true] {
            let (expected, full_bytes) = run(&path, sql, false, pushdown_filters).await;
            let (actual, narrowed_bytes) = run(&path, sql, true, pushdown_filters).await;

            assert_eq!(
                actual, expected,
                "results differ for {sql} (pushdown_filters={pushdown_filters})"
            );
            // `pad` makes up most of the data, and is no longer read.
            assert!(
                narrowed_bytes * 2 < full_bytes,
                "expected far fewer bytes for {sql} \
                 (pushdown_filters={pushdown_filters}): \
                 {narrowed_bytes} vs {full_bytes}"
            );
        }
    }
}

/// Rows per row group in the row group pruning tests.
const ROW_GROUP_ROWS: usize = 100;

/// Writes `batch` as a file with [`ROW_GROUP_ROWS`] rows per row group.
fn write_row_groups(dir: &TempDir, batch: &RecordBatch) -> String {
    write_with_props(
        dir,
        batch,
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
            .build(),
    )
}

/// Rows per data page in the page pruning tests.
const PAGE_ROWS: usize = 10;

/// Writes `batch` as a single row group with [`PAGE_ROWS`] rows per page.
fn write_pages(dir: &TempDir, batch: &RecordBatch) -> String {
    write_with_props(
        dir,
        batch,
        WriterProperties::builder()
            .set_data_page_row_count_limit(PAGE_ROWS)
            .set_write_batch_size(PAGE_ROWS)
            .build(),
    )
}

fn write_with_props(
    dir: &TempDir,
    batch: &RecordBatch,
    props: WriterProperties,
) -> String {
    let path = dir.path().join("t.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    path.to_str().unwrap().to_string()
}

/// Runs `sql` with `udfs` registered, returning its formatted output and the
/// number of containers pruned according to pruning metric `metric`.
async fn run_pruning(
    path: &str,
    sql: &str,
    udfs: &[ScalarUDF],
    metric: &str,
    pruning: bool,
    pushdown_filters: bool,
) -> (String, usize) {
    let config = SessionConfig::new()
        .set_bool("datafusion.execution.parquet.pruning", pruning)
        .set_bool("datafusion.execution.parquet.enable_page_index", pruning)
        .set_bool(
            "datafusion.execution.parquet.pushdown_filters",
            pushdown_filters,
        );
    let ctx = SessionContext::new_with_config(config);
    for udf in udfs {
        ctx.register_udf(udf.clone());
    }
    ctx.register_parquet("t", path, ParquetReadOptions::default())
        .await
        .unwrap();

    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    let metrics = MetricsFinder::find_metrics(plan.as_ref()).unwrap();
    let pruned = metrics
        .iter()
        .filter(|value| value.value().name() == metric)
        .map(|metric| match metric.value() {
            MetricValue::PruningMetrics {
                pruning_metrics, ..
            } => pruning_metrics.pruned(),
            _ => 0,
        })
        .sum();
    (pretty_format_batches(&batches).unwrap().to_string(), pruned)
}

/// Checks that `sql` prunes `expected_pruned` row groups by statistics and
/// returns the same rows as without pruning.
async fn assert_pruning(
    path: &str,
    sql: &str,
    udfs: &[ScalarUDF],
    expected_pruned: usize,
) {
    assert_pruning_metric(
        path,
        sql,
        udfs,
        "row_groups_pruned_statistics",
        expected_pruned,
    )
    .await
}

/// Checks that `sql` prunes `expected_pruned` containers according to pruning
/// metric `metric`, and returns the same rows as without pruning.
async fn assert_pruning_metric(
    path: &str,
    sql: &str,
    udfs: &[ScalarUDF],
    metric: &str,
    expected_pruned: usize,
) {
    for pushdown_filters in [false, true] {
        let (expected, _) =
            run_pruning(path, sql, udfs, metric, false, pushdown_filters).await;
        let (actual, pruned) =
            run_pruning(path, sql, udfs, metric, true, pushdown_filters).await;
        assert_eq!(
            actual, expected,
            "results differ for {sql} (pushdown_filters={pushdown_filters})"
        );
        assert_eq!(
            pruned, expected_pruned,
            "{metric} for {sql} (pushdown_filters={pushdown_filters})"
        );
    }
}

/// Row group statistics of a struct leaf prune row groups for `get_field`.
#[tokio::test]
async fn get_field_prunes_row_groups() {
    let dir = TempDir::new().unwrap();
    let path = write_row_groups(&dir, &struct_batch());

    // s.a ranges are [0, 99], [100, 199], [200, 299] and [300, 399].
    assert_pruning(
        &path,
        "SELECT id FROM t WHERE s['a'] > 350 ORDER BY id",
        &[],
        3,
    )
    .await;
    assert_pruning(
        &path,
        "SELECT id FROM t WHERE s['a'] BETWEEN 120 AND 130 ORDER BY id",
        &[],
        3,
    )
    .await;
    assert_pruning(&path, "SELECT count(*) FROM t WHERE s['a'] < 0", &[], 4).await;
}

/// Page statistics of a struct leaf prune pages for `get_field`.
#[tokio::test]
async fn get_field_prunes_pages() {
    let dir = TempDir::new().unwrap();
    let path = write_pages(&dir, &struct_batch());

    // 40 pages of 10 rows: only the page holding 120..=129 and the one
    // holding 130 can match, so 380 of 400 rows are skipped.
    assert_pruning_metric(
        &path,
        "SELECT id FROM t WHERE s['a'] BETWEEN 120 AND 130 ORDER BY id",
        &[],
        "page_index_rows_pruned",
        380,
    )
    .await;
}

/// `id: Int64, s: Struct{a: Int64, b: Utf8}` with `s.a = id`, for 400 rows.
fn struct_batch() -> RecordBatch {
    let rows = 4 * ROW_GROUP_ROWS as i64;
    let fields = Fields::from(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    let s = StructArray::try_new(
        fields.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..rows)) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|i| i.to_string()),
            )),
        ],
        None,
    )
    .unwrap();
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..rows)) as ArrayRef,
        ),
        ("s", Arc::new(s) as ArrayRef),
    ])
    .unwrap()
}

/// `variant_like_get(v, 'k')` mimics extracting an Int64 key from a shredded
/// Variant: `v` is `Struct<typed_value: Struct<k: Struct<typed_value: Int64,
/// value: Binary>>>`. A row's value is in `typed_value` when it has the shredded
/// type, and otherwise in the fallback `value`, here as 8 little-endian bytes.
#[derive(Debug, PartialEq, Eq, Hash)]
struct VariantLikeGet {
    signature: Signature,
}

impl VariantLikeGet {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    fn paths(key: &str) -> Vec<FieldPath> {
        let leaf = |name: &str| vec!["typed_value".into(), key.into(), name.into()];
        vec![leaf("typed_value"), leaf("value")]
    }
}

impl NestedInputAccess for VariantLikeGet {
    fn required_fields(&self, args: &ReturnFieldArgs) -> Vec<Option<Vec<FieldPath>>> {
        let Some(Some(key)) = args
            .scalar_arguments
            .get(1)
            .map(|key| key.and_then(|key| key.try_as_str().flatten()))
        else {
            return vec![];
        };
        vec![Some(Self::paths(key))]
    }

    /// The typed leaf's statistics describe the output only where every
    /// value is in it, i.e. the fallback is null in every row.
    fn output_statistics(
        &self,
        _args: &ReturnFieldArgs,
        inputs: &Statistics,
    ) -> Option<ColumnStatistics> {
        let [typed, fallback] = inputs.column_statistics.as_slice() else {
            return None;
        };
        match (fallback.null_count, inputs.num_rows) {
            (Precision::Exact(nulls), Precision::Exact(rows)) if nulls == rows => {
                Some(typed.clone())
            }
            _ => None,
        }
    }
}

impl ScalarUDFImpl for VariantLikeGet {
    fn name(&self) -> &str {
        "variant_like_get"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ColumnarValue::Scalar(key) = &args.args[1] else {
            unimplemented!("constant keys only")
        };
        let key = key.try_as_str().flatten().unwrap();
        let v = args.args[0].to_array(args.number_rows)?;
        let entry = v
            .as_struct()
            .column_by_name("typed_value")
            .unwrap()
            .as_struct()
            .column_by_name(key)
            .unwrap()
            .as_struct();
        let typed = entry
            .column_by_name("typed_value")
            .unwrap()
            .as_primitive::<Int64Type>();
        let fallback = entry.column_by_name("value").unwrap().as_binary::<i32>();
        let result = (0..v.len())
            .map(|i| {
                if typed.is_valid(i) {
                    Some(typed.value(i))
                } else if fallback.is_valid(i) {
                    Some(i64::from_le_bytes(fallback.value(i).try_into().unwrap()))
                } else {
                    None
                }
            })
            .collect::<Int64Array>();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        if args.first() == Some(&ExpressionPlacement::Column)
            && args[1..]
                .iter()
                .all(|arg| *arg == ExpressionPlacement::Literal)
        {
            ExpressionPlacement::MoveTowardsLeafNodes
        } else {
            ExpressionPlacement::KeepInPlace
        }
    }

    fn as_nested_input_access(&self) -> Option<&dyn NestedInputAccess> {
        Some(self)
    }
}

/// Row group pruning through `output_statistics` only skips row groups where
/// every value is in the typed leaf: a row group holding a fallback value
/// outside the typed leaf's range must still be read.
#[tokio::test]
async fn variant_like_get_prunes_only_fully_shredded_row_groups() {
    let dir = TempDir::new().unwrap();
    let path = write_row_groups(&dir, &variant_like_batch());
    let udfs = [ScalarUDF::from(VariantLikeGet::new())];

    // Row groups 0 and 1 are skipped. Row group 2's typed values are at most
    // 299, but it holds the fallback value 10000, so it must be read.
    assert_pruning(
        &path,
        "SELECT id FROM t WHERE variant_like_get(v, 'k') > 350 ORDER BY id",
        &udfs,
        2,
    )
    .await;
    // Only the fallback value matches; only row group 2 can hold it.
    assert_pruning(
        &path,
        "SELECT id FROM t WHERE variant_like_get(v, 'k') = 10000",
        &udfs,
        3,
    )
    .await;
}

/// The same guarantee for pages: the page holding the fallback value is read.
#[tokio::test]
async fn variant_like_get_prunes_only_fully_shredded_pages() {
    let dir = TempDir::new().unwrap();
    let path = write_pages(&dir, &variant_like_batch());
    let udfs = [ScalarUDF::from(VariantLikeGet::new())];

    // Pages hold 10 rows. Rows 351..=399 span five pages and row 250 is in a
    // sixth, so the other 34 pages (340 rows) are skipped.
    assert_pruning_metric(
        &path,
        "SELECT id FROM t WHERE variant_like_get(v, 'k') > 350 ORDER BY id",
        &udfs,
        "page_index_rows_pruned",
        340,
    )
    .await;
}

/// `id: Int64, v: Struct{typed_value: Struct{k: Struct{typed_value: Int64,
/// value: Binary}}}` with 400 rows, where `k` is `id`. Row 250 falls back to
/// the untyped `value` instead, with 10000, far above the typed values around
/// it.
fn variant_like_batch() -> RecordBatch {
    let rows = 4 * ROW_GROUP_ROWS as i64;
    let outlier = 250;
    let typed = (0..rows)
        .map(|i| (i != outlier).then_some(i))
        .collect::<Int64Array>();
    let fallback = (0..rows)
        .map(|i| (i == outlier).then(|| 10_000_i64.to_le_bytes()))
        .collect::<BinaryArray>();
    let entry_fields = Fields::from(vec![
        Field::new("typed_value", DataType::Int64, true),
        Field::new("value", DataType::Binary, true),
    ]);
    let entry = StructArray::try_new(
        entry_fields.clone(),
        vec![Arc::new(typed) as ArrayRef, Arc::new(fallback)],
        None,
    )
    .unwrap();
    let typed_value_fields =
        Fields::from(vec![Field::new("k", DataType::Struct(entry_fields), true)]);
    let typed_value =
        StructArray::try_new(typed_value_fields.clone(), vec![Arc::new(entry)], None)
            .unwrap();
    let v = StructArray::try_new(
        Fields::from(vec![Field::new(
            "typed_value",
            DataType::Struct(typed_value_fields),
            true,
        )]),
        vec![Arc::new(typed_value)],
        None,
    )
    .unwrap();
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..rows)) as ArrayRef,
        ),
        ("v", Arc::new(v) as ArrayRef),
    ])
    .unwrap()
}
