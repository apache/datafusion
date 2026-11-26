// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! A [`ParquetStatisticsResolver`] registered from outside DataFusion prunes
//! row groups for its own function.
//!
//! `fake_variant_get(v, 'a')` stands in for a shredded variant access. The
//! file stores `a` in `v.typed_value.a.typed_value` when it is shredded and
//! in `v.typed_value.a.value` (binary) when it is not:
//!
//! | Row group | `typed_value` | `value`      | Bounds usable? |
//! |-----------|---------------|--------------|----------------|
//! | 0         | 1, 2          | null, null   | yes: [1, 2]    |
//! | 1         | 10, 11        | null, null   | yes: [10, 11]  |
//! | 2         | 3, null       | null, <100>  | no: one row is not shredded |
//!
//! For `fake_variant_get(v, 'a') > 5`, only row group 0 can be pruned.
//!
//! The binding does not implement `bloom_filter_leaf` or `page_index_leaf`.
//! A bloom filter on `typed_value` cannot see values stored in `value`, and
//! its validity check needs a second leaf, whose pages do not line up.

use std::io::Write;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int32Array, Int64Array, RecordBatch,
    StructArray, UInt64Array,
};
use arrow::compute::nullif;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::functions::core::expr_fn::get_field;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::{DFSchema, Result, ScalarValue, internal_err};
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource_parquet::statistics_resolver::{
    BoundExprStatistics, ExprStatisticsKind, ParquetLeafStatistics,
    ParquetStatisticsResolver, leaf_index_by_path,
};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, col, lit,
};
use datafusion_physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, Literal, TryCastExpr,
};
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::metrics::MetricValue;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataWriter};
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::Statistics;
use parquet::file::writer::TrackedWrite;
use parquet::schema::types::SchemaDescriptor;
use tempfile::NamedTempFile;

use crate::parquet::utils::MetricsFinder;

/// Returns `v.typed_value.<key>`. Only its statistics are used here.
#[derive(Debug, PartialEq, Eq, Hash)]
struct FakeVariantGet(Signature);

impl ScalarUDFImpl for FakeVariantGet {
    fn name(&self) -> &str {
        "fake_variant_get"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("the scan only prunes with statistics, it does not filter rows")
    }
}

/// Reads `fake_variant_get(v, 'key')` bounds from the shredded column, but
/// only for containers where no row fell back to the binary `value` column.
#[derive(Debug)]
struct FakeVariantResolver;

impl FakeVariantResolver {
    /// The column name and key of `fake_variant_get(column, 'key')`.
    fn args(expr: &Arc<dyn PhysicalExpr>) -> Option<(&str, &str)> {
        let func = expr.downcast_ref::<ScalarFunctionExpr>()?;
        if func.name() != "fake_variant_get" {
            return None;
        }
        let [column, key] = func.args() else {
            return None;
        };
        let column = column.downcast_ref::<Column>()?;
        let ScalarValue::Utf8(Some(key)) = key.downcast_ref::<Literal>()?.value() else {
            return None;
        };
        Some((column.name(), key.as_str()))
    }
}

impl ParquetStatisticsResolver for FakeVariantResolver {
    fn bind(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        _arrow_schema: &Schema,
        parquet_schema: &SchemaDescriptor,
    ) -> Option<Arc<dyn BoundExprStatistics>> {
        let (column, key) = Self::args(expr)?;
        let typed = leaf_index_by_path(
            parquet_schema,
            &[column, "typed_value", key, "typed_value"],
        )?;
        let fallback =
            leaf_index_by_path(parquet_schema, &[column, "typed_value", key, "value"])?;
        Some(Arc::new(FakeVariantStatistics { typed, fallback }))
    }
}

/// The leaves of one `fake_variant_get(v, 'key')` in one file.
#[derive(Debug)]
struct FakeVariantStatistics {
    typed: usize,
    fallback: usize,
}

impl BoundExprStatistics for FakeVariantStatistics {
    fn statistics(
        &self,
        kind: ExprStatisticsKind,
        statistics: &dyn ParquetLeafStatistics,
    ) -> Option<ArrayRef> {
        let typed_field = Field::new("typed_value", DataType::Int64, true);
        let fallback_field = Field::new("value", DataType::Binary, true);

        let bounds = match kind {
            ExprStatisticsKind::Min => statistics.leaf_mins(self.typed, &typed_field)?,
            ExprStatisticsKind::Max => statistics.leaf_maxes(self.typed, &typed_field)?,
            // A null `typed_value` may hold a value in `value`.
            ExprStatisticsKind::NullCount => return None,
        };
        // The bounds hold only where every row is shredded, which is where
        // the fallback column is null in every row.
        let fallback_nulls =
            statistics.leaf_null_counts(self.fallback, &fallback_field)?;
        let rows = statistics.row_counts()?;
        let fallback_nulls = fallback_nulls.as_any().downcast_ref::<UInt64Array>()?;
        let rows = rows.as_any().downcast_ref::<UInt64Array>()?;
        // Unknown counts are null. Treat them as not shredded.
        let not_shredded: BooleanArray = fallback_nulls
            .iter()
            .zip(rows.iter())
            .map(|(nulls, rows)| Some(nulls.is_none() || nulls != rows))
            .collect();
        nullif(&bounds, &not_shredded).ok()
    }
}

fn shredded_schema() -> SchemaRef {
    let a = Fields::from(vec![
        Field::new("value", DataType::Binary, true),
        Field::new("typed_value", DataType::Int64, true),
    ]);
    let typed_value = Fields::from(vec![Field::new("a", DataType::Struct(a), true)]);
    let v = Fields::from(vec![Field::new(
        "typed_value",
        DataType::Struct(typed_value),
        true,
    )]);
    Arc::new(Schema::new(vec![Field::new(
        "v",
        DataType::Struct(v),
        true,
    )]))
}

/// Writes the file described in the module docs, two rows per row group.
fn write_file(schema: &SchemaRef) -> NamedTempFile {
    let DataType::Struct(v_fields) = schema.field(0).data_type() else {
        unreachable!()
    };
    let DataType::Struct(tv_fields) = v_fields[0].data_type() else {
        unreachable!()
    };
    let DataType::Struct(a_fields) = tv_fields[0].data_type() else {
        unreachable!()
    };
    let value: ArrayRef = Arc::new(BinaryArray::from(vec![
        None,
        None,
        None,
        None,
        None,
        Some(100i64.to_le_bytes().as_slice()),
    ]));
    let typed: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(1),
        Some(2),
        Some(10),
        Some(11),
        Some(3),
        None,
    ]));
    let a = StructArray::new(a_fields.clone(), vec![value, typed], None);
    let tv = StructArray::new(tv_fields.clone(), vec![Arc::new(a)], None);
    let v = StructArray::new(v_fields.clone(), vec![Arc::new(tv)], None);
    let batch = RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(v)]).unwrap();

    let file = NamedTempFile::new().unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer =
        ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(schema), Some(props))
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Scans the file with `fake_variant_get(v, 'a') > 5` and returns the
/// (pruned, matched) row group counts from statistics.
async fn row_groups_pruned(register_resolver: bool) -> (usize, usize) {
    let schema = shredded_schema();
    let file = write_file(&schema);
    let ctx = SessionContext::new();

    let udf = ScalarUDF::from(FakeVariantGet(Signature::any(2, Volatility::Immutable)));
    let predicate = udf.call(vec![col("v"), lit("a")]).gt(lit(5i64));
    let df_schema = DFSchema::try_from(Arc::clone(&schema)).unwrap();
    let predicate = ctx.create_physical_expr(predicate, &df_schema).unwrap();

    let mut source = ParquetSource::new(Arc::clone(&schema)).with_predicate(predicate);
    if register_resolver {
        source = source.with_statistics_resolver(Arc::new(FakeVariantResolver));
    }
    let path = file.path().to_str().unwrap().to_string();
    let size = std::fs::metadata(file.path()).unwrap().len();
    let config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
            .with_file(PartitionedFile::new(path, size))
            .build();
    let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
    datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .unwrap();

    let metrics = MetricsFinder::find_metrics(plan.as_ref()).unwrap();
    let Some(MetricValue::PruningMetrics {
        pruning_metrics, ..
    }) = metrics.sum_by_name("row_groups_pruned_statistics")
    else {
        panic!("metrics: {metrics:#?}");
    };
    (pruning_metrics.pruned(), pruning_metrics.matched())
}

/// Scans the file with a dynamic filter that starts as `true`. After the
/// first batch, the test sets the filter to `fake_variant_get(v, 'a') < 5`.
/// Returns the `row_groups_pruned_dynamic_filter` count.
async fn row_groups_pruned_dynamic(register_resolver: bool) -> usize {
    let schema = shredded_schema();
    let file = write_file(&schema);
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(1));

    let udf = ScalarUDF::from(FakeVariantGet(Signature::any(2, Volatility::Immutable)));
    let df_schema = DFSchema::try_from(Arc::clone(&schema)).unwrap();
    let column = ctx.create_physical_expr(col("v"), &df_schema).unwrap();
    let filter = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![Arc::clone(&column)],
        Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
    ));

    let mut source = ParquetSource::new(Arc::clone(&schema))
        .with_predicate(Arc::clone(&filter) as Arc<dyn PhysicalExpr>);
    if register_resolver {
        source = source.with_statistics_resolver(Arc::new(FakeVariantResolver));
    }
    let path = file.path().to_str().unwrap().to_string();
    let size = std::fs::metadata(file.path()).unwrap().len();
    let config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
            .with_file(PartitionedFile::new(path, size))
            .build();
    let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
    let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
    // The first batch comes from row group 0.
    stream.next().await.unwrap().unwrap();
    let predicate = udf.call(vec![col("v"), lit("a")]).lt(lit(5i64));
    filter
        .update(ctx.create_physical_expr(predicate, &df_schema).unwrap())
        .unwrap();
    while let Some(batch) = stream.next().await {
        batch.unwrap();
    }

    let metrics = MetricsFinder::find_metrics(plan.as_ref()).unwrap();
    metrics
        .sum_by_name("row_groups_pruned_dynamic_filter")
        .unwrap()
        .as_usize()
}

#[tokio::test]
async fn external_resolver_prunes_row_groups_with_a_dynamic_filter() {
    // Row group 1 ([10, 11]) is pruned. Row group 2 is kept because one of
    // its rows is not shredded.
    assert_eq!(row_groups_pruned_dynamic(true).await, 1);
    assert_eq!(row_groups_pruned_dynamic(false).await, 0);
}

#[tokio::test]
async fn external_resolver_prunes_shredded_row_groups() {
    // Row group 0 is pruned. Row group 2 is kept because one of its rows
    // is not shredded, even though its shredded max (3) is below 5.
    assert_eq!(row_groups_pruned(true).await, (1, 2));
}

#[tokio::test]
async fn without_resolver_nothing_is_pruned() {
    assert_eq!(row_groups_pruned(false).await, (0, 3));
}

/// Writes `s: {a: Int64}` with rows `{a: 1}` and `{a: null}` in one row
/// group. The footer keeps the min and max of `s.a`, but not its null count.
fn struct_file_without_null_count() -> NamedTempFile {
    let a = Field::new("a", DataType::Int64, true);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Struct(Fields::from(vec![a.clone()])),
        true,
    )]));
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
    let s = StructArray::new(Fields::from(vec![a]), vec![values], None);
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(s)]).unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
    writer.write(&batch).unwrap();
    let metadata = writer.close().unwrap();

    let group = &metadata.row_groups()[0];
    let column = group.column(0).clone();
    let Statistics::Int64(stats) = column.statistics().unwrap() else {
        panic!("expected Int64 statistics");
    };
    let stats = Statistics::int64(
        stats.min_opt().copied(),
        stats.max_opt().copied(),
        None,
        None,
        false,
    );
    let column = column.into_builder().set_statistics(stats).build().unwrap();
    let group = group
        .clone()
        .into_builder()
        .set_column_metadata(vec![column])
        .build()
        .unwrap();
    let metadata = ParquetMetaData::new(metadata.file_metadata().clone(), vec![group]);

    // Replace the footer. The encoded rows and their offsets do not change.
    let footer_len =
        u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap());
    bytes.truncate(bytes.len() - 8 - footer_len as usize);
    let mut file = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let mut output = TrackedWrite::new(file.as_file_mut());
    output.write_all(&bytes).unwrap();
    ParquetMetaDataWriter::new_with_tracked(output, &metadata)
        .finish()
        .unwrap();
    file
}

#[tokio::test]
async fn missing_struct_field_null_count_keeps_row_group() {
    // A missing null count is unknown, not zero. `s['a'] IS NULL` must not
    // prune the row group.
    let file = struct_file_without_null_count();
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "t",
        file.path().to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    let batches = ctx
        .sql("SELECT count(*) FROM t WHERE s['a'] IS NULL")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
        ScalarValue::Int64(Some(1)),
    );
}

/// Matches `TRY_CAST(s['a'] AS Int64)` and reads its bounds from a sidecar
/// leaf `s_a_int64`. The test file has no such leaf.
#[derive(Debug)]
struct SidecarCastResolver;

impl SidecarCastResolver {
    fn is_cast_of_field(expr: &Arc<dyn PhysicalExpr>) -> bool {
        expr.downcast_ref::<TryCastExpr>().is_some_and(|cast| {
            cast.cast_type() == &DataType::Int64
                && cast
                    .expr()
                    .downcast_ref::<ScalarFunctionExpr>()
                    .is_some_and(|func| func.name() == "get_field")
        })
    }
}

impl ParquetStatisticsResolver for SidecarCastResolver {
    fn bind(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        _arrow_schema: &Schema,
        parquet_schema: &SchemaDescriptor,
    ) -> Option<Arc<dyn BoundExprStatistics>> {
        if !Self::is_cast_of_field(expr) {
            return None;
        }
        let leaf = leaf_index_by_path(parquet_schema, &["s_a_int64"])?;
        Some(Arc::new(SidecarStatistics { leaf }))
    }
}

#[derive(Debug)]
struct SidecarStatistics {
    leaf: usize,
}

impl BoundExprStatistics for SidecarStatistics {
    fn statistics(
        &self,
        kind: ExprStatisticsKind,
        statistics: &dyn ParquetLeafStatistics,
    ) -> Option<ArrayRef> {
        let field = Field::new("s_a_int64", DataType::Int64, true);
        match kind {
            ExprStatisticsKind::Min => statistics.leaf_mins(self.leaf, &field),
            ExprStatisticsKind::Max => statistics.leaf_maxes(self.leaf, &field),
            ExprStatisticsKind::NullCount => {
                statistics.leaf_null_counts(self.leaf, &field)
            }
        }
    }
}

/// Writes `s: {a: Int32}` with rows 1, 2, 10, 11, 3, 4, two rows per row
/// group. With `sidecar`, the file also has the leaf `s_a_int64` with the
/// value 4000000000 in every row. Its bounds differ from those of `s.a`.
fn struct_int32_file(sidecar: bool) -> NamedTempFile {
    let a = Field::new("a", DataType::Int32, true);
    let mut fields = vec![Field::new(
        "s",
        DataType::Struct(Fields::from(vec![a.clone()])),
        true,
    )];
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 10, 11, 3, 4]));
    let mut columns: Vec<ArrayRef> = vec![Arc::new(StructArray::new(
        Fields::from(vec![a]),
        vec![values],
        None,
    ))];
    if sidecar {
        fields.push(Field::new("s_a_int64", DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from(vec![4_000_000_000; 6])));
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let file = NamedTempFile::new().unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer =
        ArrowWriter::try_new(file.reopen().unwrap(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Scans `files` with `TRY_CAST(s['a'] AS Int64) > 3000000000` and
/// `SidecarCastResolver`. Returns the (pruned, matched) row group counts
/// from statistics and the number of rows read. The scan does not filter
/// rows, so the rows read are the rows of the kept row groups.
async fn sidecar_scan(files: &[&NamedTempFile]) -> (usize, usize, usize) {
    let a = Field::new("a", DataType::Int32, true);
    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Struct(Fields::from(vec![a])), true),
        Field::new("s_a_int64", DataType::Int64, true),
    ]));
    let ctx = SessionContext::new();
    let df_schema = DFSchema::try_from(Arc::clone(&schema)).unwrap();
    let field = ctx
        .create_physical_expr(get_field(col("s"), "a"), &df_schema)
        .unwrap();
    // The literal does not fit in Int32, so the simplifier keeps the cast.
    let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(TryCastExpr::new(field, DataType::Int64)),
        Operator::Gt,
        Arc::new(Literal::new(ScalarValue::Int64(Some(3_000_000_000)))),
    ));

    let source = ParquetSource::new(Arc::clone(&schema))
        .with_predicate(predicate)
        .with_statistics_resolver(Arc::new(SidecarCastResolver));
    let mut config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source));
    for file in files {
        let path = file.path().to_str().unwrap().to_string();
        let size = std::fs::metadata(file.path()).unwrap().len();
        config = config.with_file(PartitionedFile::new(path, size));
    }
    let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config.build());
    let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .unwrap();
    let rows = batches.iter().map(RecordBatch::num_rows).sum();

    let metrics = MetricsFinder::find_metrics(plan.as_ref()).unwrap();
    let Some(MetricValue::PruningMetrics {
        pruning_metrics, ..
    }) = metrics.sum_by_name("row_groups_pruned_statistics")
    else {
        panic!("metrics: {metrics:#?}");
    };
    (pruning_metrics.pruned(), pruning_metrics.matched(), rows)
}

/// The resolver matches `TRY_CAST(s['a'] AS Int64)`, but the file has no
/// sidecar leaf for it. The pruning predicate then uses the statistics of
/// the inner `s['a']`, and every row group is pruned.
#[tokio::test]
async fn unbound_outer_expression_falls_back_to_inner_statistics() {
    let file = struct_int32_file(false);
    assert_eq!(sidecar_scan(&[&file]).await, (3, 0, 0));
}

/// The file has the sidecar leaf, so the resolver binds the outer
/// expression. Its bounds (4000000000) keep every row group, although the
/// bounds of `s.a` would prune them all.
#[tokio::test]
async fn bound_outer_expression_uses_its_own_statistics() {
    let file = struct_int32_file(true);
    assert_eq!(sidecar_scan(&[&file]).await, (0, 3, 6));
}

/// Each file binds on its own: the file with the sidecar leaf keeps its
/// row groups, and the file without it is pruned with `s.a`.
#[tokio::test]
async fn each_file_binds_the_outer_expression_on_its_own() {
    let with_leaf = struct_int32_file(true);
    let without_leaf = struct_int32_file(false);
    assert_eq!(sidecar_scan(&[&with_leaf, &without_leaf]).await, (3, 3, 6));
}
