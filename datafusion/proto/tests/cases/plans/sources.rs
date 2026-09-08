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

//! Scans and data sources: file formats, `FileScanConfig`, listing tables
//! and memory sources.

use super::{
    all_types_context, roundtrip_test, roundtrip_test_and_return,
    roundtrip_test_sql_with_context,
};
use arrow::array::RecordBatch;
use arrow::datatypes::Fields;
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl, PartitionedFile,
};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    ArrowSource, CsvSource, FileGroup, FileScanConfig, FileScanConfigBuilder, JsonSource,
    ParquetSource, wrap_partition_type_in_dict, wrap_partition_value_in_dict,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::expressions::{
    BinaryExpr, Column, PhysicalSortExpr, col, lit,
};
use datafusion::physical_plan::filter::FilterExecBuilder;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, PhysicalExpr, RangePartitioning, SplitPoint, Statistics,
    displayable,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::stats::Precision;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::{TableSchema, TableSchemaBuilder};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_proto_converter,
    physical_plan_to_bytes_with_proto_converter,
};
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter,
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::vec;

#[test]
fn roundtrip_parquet_exec_with_pruning_predicate() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));

    let predicate = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("col", 1)),
        Operator::Eq,
        lit("1"),
    ));

    let mut options = TableParquetOptions::new();
    options.global.pushdown_filters = true;

    let file_source = Arc::new(
        ParquetSource::new(Arc::clone(&file_schema))
            .with_table_parquet_options(options)
            .with_predicate(predicate),
    );

    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.parquet".to_string(),
                1024,
            )])])
            .with_statistics(Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(1024),
                column_statistics: Statistics::unknown_column(&Arc::new(Schema::new(
                    vec![Field::new("col", DataType::Utf8, false)],
                ))),
            })
            .build();

    roundtrip_test(DataSourceExec::from_data_source(scan_config))
}

#[tokio::test]
async fn roundtrip_parquet_exec_with_sort_pushdown() -> Result<()> {
    let ctx = all_types_context().await?;
    let plan = ctx
        .sql("SELECT id FROM alltypes_plain ORDER BY id DESC NULLS LAST LIMIT 5")
        .await?
        .create_physical_plan()
        .await?;
    let before = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        before.contains("sort_order_for_reorder=[id@0 DESC NULLS LAST]")
            && before.contains("reverse_row_groups=true"),
        "expected sort pushdown in plan:\n{before}"
    );

    let roundtripped = roundtrip_test_and_return(
        plan,
        &ctx,
        &DefaultPhysicalExtensionCodec {},
        &DefaultPhysicalProtoConverter {},
    )?;
    let after = displayable(roundtripped.as_ref()).indent(true).to_string();
    pretty_assertions::assert_eq!(before, after);
    Ok(())
}

#[test]
fn file_scan_rejects_zero_batch_size() -> Result<()> {
    let schema = Arc::new(Schema::empty());
    let scan_config = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        Arc::new(ParquetSource::new(schema)),
    )
    .build();
    let codec = DefaultPhysicalExtensionCodec {};
    let mut node = PhysicalPlanNode::try_from_physical_plan(
        DataSourceExec::from_data_source(scan_config),
        &codec,
    )?;
    let Some(protobuf::physical_plan_node::PhysicalPlanType::ParquetScan(scan)) =
        node.physical_plan_type.as_mut()
    else {
        return internal_err!("Expected ParquetScan node");
    };
    scan.base_conf
        .as_mut()
        .expect("Parquet scan has a base config")
        .batch_size = Some(0);

    let ctx = SessionContext::new();
    let err = node
        .try_into_physical_plan(ctx.task_ctx().as_ref(), &codec)
        .expect_err("zero file scan batch size must fail");
    assert!(
        err.to_string()
            .contains("FileScanConfig: batch_size must be greater than 0"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[test]
fn roundtrip_parquet_exec_attaches_cached_reader_factory_after_roundtrip() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let file_source = Arc::new(ParquetSource::new(Arc::clone(&file_schema)));
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.parquet".to_string(),
                1024,
            )])])
            .with_statistics(Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(1024),
                column_statistics: Statistics::unknown_column(&file_schema),
            })
            .build();
    let exec_plan = DataSourceExec::from_data_source(scan_config);

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let roundtripped =
        roundtrip_test_and_return(exec_plan, &ctx, &codec, &proto_converter)?;

    let data_source = roundtripped
        .downcast_ref::<DataSourceExec>()
        .ok_or_else(|| {
            internal_datafusion_err!("Expected DataSourceExec after roundtrip")
        })?;
    let file_scan = data_source
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .ok_or_else(|| {
            internal_datafusion_err!("Expected FileScanConfig after roundtrip")
        })?;
    let parquet_source = file_scan
        .file_source()
        .downcast_ref::<ParquetSource>()
        .ok_or_else(|| {
            internal_datafusion_err!("Expected ParquetSource after roundtrip")
        })?;

    assert!(
        parquet_source.parquet_file_reader_factory().is_some(),
        "Parquet reader factory should be attached after decoding from protobuf"
    );
    Ok(())
}

/// Returns `FileSource::file_type` of a `DataSourceExec` file scan, e.g.
/// "arrow" vs "arrow_stream". The two Arrow IPC formats print identically in
/// plan debug output, so roundtrip tests must inspect the source directly.
fn scan_file_type(plan: &Arc<dyn ExecutionPlan>) -> Result<String> {
    let data_source = plan.downcast_ref::<DataSourceExec>().ok_or_else(|| {
        internal_datafusion_err!("Expected DataSourceExec after roundtrip")
    })?;
    let file_scan = data_source
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .ok_or_else(|| {
            internal_datafusion_err!("Expected FileScanConfig after roundtrip")
        })?;
    Ok(file_scan.file_source().file_type().to_string())
}

#[test]
fn roundtrip_arrow_scan() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));

    let table_schema = TableSchema::from(&file_schema);
    let file_source = Arc::new(ArrowSource::new_file_source(table_schema));

    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.arrow".to_string(),
                1024,
            )])])
            .with_statistics(Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(1024),
                column_statistics: Statistics::unknown_column(&file_schema),
            })
            .build();

    let roundtripped = roundtrip_test_and_return(
        DataSourceExec::from_data_source(scan_config),
        &SessionContext::new(),
        &DefaultPhysicalExtensionCodec {},
        &DefaultPhysicalProtoConverter {},
    )?;
    assert_eq!(scan_file_type(&roundtripped)?, "arrow");
    Ok(())
}

#[test]
fn roundtrip_arrow_stream_scan() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let file_source = Arc::new(ArrowSource::new_stream_file_source(TableSchema::from(
        &file_schema,
    )));
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.arrows".to_string(),
                1024,
            )])])
            .build();

    let roundtripped = roundtrip_test_and_return(
        DataSourceExec::from_data_source(scan_config),
        &SessionContext::new(),
        &DefaultPhysicalExtensionCodec {},
        &DefaultPhysicalProtoConverter {},
    )?;
    assert_eq!(scan_file_type(&roundtripped)?, "arrow_stream");
    Ok(())
}

#[test]
fn arrow_scan_without_format_field_decodes_as_file_format() -> Result<()> {
    // Payloads encoded before `ArrowScanExecNode.format` existed carry no
    // format discriminator; they must keep decoding as the IPC file format.
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let file_source = Arc::new(ArrowSource::new_file_source(TableSchema::from(
        &file_schema,
    )));
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.arrow".to_string(),
                1024,
            )])])
            .build();

    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let bytes = physical_plan_to_bytes_with_proto_converter(
        DataSourceExec::from_data_source(scan_config),
        &codec,
        &proto_converter,
    )?;

    let mut node = PhysicalPlanNode::decode(bytes.as_ref()).map_err(|e| {
        internal_datafusion_err!("Failed to decode PhysicalPlanNode: {e}")
    })?;
    match node.physical_plan_type.as_mut() {
        Some(protobuf::physical_plan_node::PhysicalPlanType::ArrowScan(scan)) => {
            scan.format = protobuf::ArrowIpcFormat::File as i32;
        }
        other => return internal_err!("Expected ArrowScan node, got {other:?}"),
    }

    let ctx = SessionContext::new();
    let decoded = physical_plan_from_bytes_with_proto_converter(
        &node.encode_to_vec(),
        ctx.task_ctx().as_ref(),
        &codec,
        &proto_converter,
    )?;
    assert_eq!(scan_file_type(&decoded)?, "arrow");
    Ok(())
}

#[test]
fn roundtrip_json_scan_preserves_format_options() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let file_source = Arc::new(
        JsonSource::new(TableSchema::from(&file_schema)).with_newline_delimited(false),
    );
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.json.gz".to_string(),
                1024,
            )])])
            .with_file_compression_type(FileCompressionType::GZIP)
            .build();

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(scan_config);
    let roundtripped = roundtrip_test_and_return(
        Arc::clone(&plan),
        &ctx,
        &codec,
        &DefaultPhysicalProtoConverter {},
    )?;
    let file_scan = roundtripped
        .downcast_ref::<DataSourceExec>()
        .and_then(|exec| exec.data_source().downcast_ref::<FileScanConfig>())
        .ok_or_else(|| internal_datafusion_err!("Expected FileScanConfig"))?;
    let json_source = file_scan
        .file_source()
        .downcast_ref::<JsonSource>()
        .ok_or_else(|| internal_datafusion_err!("Expected JsonSource"))?;
    assert!(!json_source.is_newline_delimited());
    assert_eq!(file_scan.file_compression_type, FileCompressionType::GZIP);

    // Payloads written before these fields existed must keep their historical
    // defaults: newline-delimited JSON without compression.
    let mut node = PhysicalPlanNode::try_from_physical_plan(plan, &codec)?;
    match node.physical_plan_type.as_mut() {
        Some(protobuf::physical_plan_node::PhysicalPlanType::JsonScan(scan)) => {
            scan.newline_delimited = None;
            scan.base_conf
                .as_mut()
                .expect("JSON scan has a base config")
                .file_compression_type = None;
        }
        other => return internal_err!("Expected JsonScan node, got {other:?}"),
    }
    let decoded = node.try_into_physical_plan(ctx.task_ctx().as_ref(), &codec)?;
    let file_scan = decoded
        .downcast_ref::<DataSourceExec>()
        .and_then(|exec| exec.data_source().downcast_ref::<FileScanConfig>())
        .ok_or_else(|| internal_datafusion_err!("Expected FileScanConfig"))?;
    let json_source = file_scan
        .file_source()
        .downcast_ref::<JsonSource>()
        .ok_or_else(|| internal_datafusion_err!("Expected JsonSource"))?;
    assert!(json_source.is_newline_delimited());
    assert_eq!(
        file_scan.file_compression_type,
        FileCompressionType::UNCOMPRESSED
    );
    Ok(())
}

#[tokio::test]
async fn roundtrip_compressed_json_array_scan_executes() -> Result<()> {
    use datafusion::prelude::JsonReadOptions;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

    let tmp_dir = tempfile::TempDir::new()?;
    let path = tmp_dir.path().join("array.json.gz");
    let file = std::fs::File::create(&path)?;
    let mut encoder = GzEncoder::new(file, Compression::default());
    encoder.write_all(br#"[{"a": 1, "b": "hello"}, {"a": 2, "b": "world"}]"#)?;
    encoder.finish()?;

    let ctx = SessionContext::new();
    let options = JsonReadOptions::default()
        .newline_delimited(false)
        .file_compression_type(FileCompressionType::GZIP)
        .file_extension(".json.gz");
    ctx.register_json("test_table", path.to_string_lossy(), options)
        .await?;

    let initial_plan = ctx
        .sql("SELECT a, b FROM test_table ORDER BY a")
        .await?
        .create_physical_plan()
        .await?;
    let roundtripped = roundtrip_test_and_return(
        initial_plan,
        &ctx,
        &DefaultPhysicalExtensionCodec {},
        &DefaultPhysicalProtoConverter {},
    )?;
    let batches =
        datafusion::physical_plan::collect(roundtripped, ctx.task_ctx()).await?;

    datafusion::assert_batches_eq!(
        &[
            "+---+-------+",
            "| a | b     |",
            "+---+-------+",
            "| 1 | hello |",
            "| 2 | world |",
            "+---+-------+",
        ],
        &batches
    );
    Ok(())
}

#[cfg(feature = "avro")]
#[test]
fn roundtrip_avro_scan() -> Result<()> {
    use datafusion_datasource_avro::source::AvroSource;

    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let file_source = Arc::new(AvroSource::new(TableSchema::from(&file_schema)));
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.avro".to_string(),
                1024,
            )])])
            .build();
    roundtrip_test(DataSourceExec::from_data_source(scan_config))
}

#[test]
fn roundtrip_csv_scan_preserves_format_options() -> Result<()> {
    use datafusion::common::config::CsvOptions;

    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let table_schema = TableSchema::from(&file_schema);
    let file_source =
        Arc::new(CsvSource::new(table_schema).with_csv_options(CsvOptions {
            has_header: Some(false),
            delimiter: b'|',
            quote: b'\'',
            escape: Some(b'\\'),
            comment: Some(b'#'),
            terminator: Some(0xff),
            newlines_in_values: Some(true),
            truncated_rows: Some(true),
            ..Default::default()
        }));

    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.csv.gz".to_string(),
                1024,
            )])])
            .with_file_compression_type(FileCompressionType::GZIP)
            .build();

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(scan_config);
    let roundtripped = roundtrip_test_and_return(
        Arc::clone(&plan),
        &ctx,
        &codec,
        &DefaultPhysicalProtoConverter {},
    )?;
    let data_source = roundtripped
        .downcast_ref::<DataSourceExec>()
        .ok_or_else(|| internal_datafusion_err!("Expected DataSourceExec"))?;
    let file_scan = data_source
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .ok_or_else(|| internal_datafusion_err!("Expected FileScanConfig"))?;
    let csv_source = file_scan
        .file_source()
        .downcast_ref::<CsvSource>()
        .ok_or_else(|| internal_datafusion_err!("Expected CsvSource"))?;

    assert!(!csv_source.has_header());
    assert_eq!(csv_source.delimiter(), b'|');
    assert_eq!(csv_source.quote(), b'\'');
    assert_eq!(csv_source.escape(), Some(b'\\'));
    assert_eq!(csv_source.comment(), Some(b'#'));
    assert_eq!(csv_source.terminator(), Some(0xff));
    assert!(csv_source.newlines_in_values());
    assert!(csv_source.truncate_rows());
    assert_eq!(file_scan.file_compression_type, FileCompressionType::GZIP);

    for invalid_terminator in [vec![], vec![b'\r', b'\n']] {
        let mut node =
            PhysicalPlanNode::try_from_physical_plan(Arc::clone(&plan), &codec)?;
        match node.physical_plan_type.as_mut() {
            Some(protobuf::physical_plan_node::PhysicalPlanType::CsvScan(scan)) => {
                scan.terminator = Some(invalid_terminator);
            }
            other => return internal_err!("Expected CsvScan node, got {other:?}"),
        }
        let err = node
            .try_into_physical_plan(ctx.task_ctx().as_ref(), &codec)
            .expect_err("invalid terminator length must fail");
        assert!(
            err.to_string().contains("expected exactly one byte"),
            "unexpected error: {err}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn roundtrip_parquet_exec_with_table_partition_cols() -> Result<()> {
    let mut file_group =
        PartitionedFile::new("/path/to/part=0/file.parquet".to_string(), 1024);
    file_group.partition_values =
        vec![wrap_partition_value_in_dict(ScalarValue::Int64(Some(0)))];
    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));

    let table_schema = TableSchemaBuilder::from(&schema)
        .with_table_partition_cols(vec![Arc::new(Field::new(
            "part".to_string(),
            wrap_partition_type_in_dict(DataType::Int16),
            false,
        ))])
        .build();

    let file_source = Arc::new(ParquetSource::new(table_schema.clone()));
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_projection_indices(Some(vec![0, 1]))?
            .with_file_group(FileGroup::new(vec![file_group]))
            .build();

    roundtrip_test(DataSourceExec::from_data_source(scan_config))
}

#[test]
fn roundtrip_parquet_exec_with_custom_predicate_expr() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));

    let custom_predicate_expr = Arc::new(CustomPredicateExpr {
        inner: Arc::new(Column::new("col", 1)),
    });

    let file_source = Arc::new(
        ParquetSource::new(Arc::clone(&file_schema))
            .with_predicate(custom_predicate_expr),
    );

    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.parquet".to_string(),
                1024,
            )])])
            .with_statistics(Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(1024),
                column_statistics: Statistics::unknown_column(&Arc::new(Schema::new(
                    vec![Field::new("col", DataType::Utf8, false)],
                ))),
            })
            .build();

    #[derive(Debug, Clone, Eq)]
    struct CustomPredicateExpr {
        inner: Arc<dyn PhysicalExpr>,
    }

    // Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
    impl PartialEq for CustomPredicateExpr {
        fn eq(&self, other: &Self) -> bool {
            self.inner.eq(&other.inner)
        }
    }

    impl std::hash::Hash for CustomPredicateExpr {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.inner.hash(state);
        }
    }

    impl Display for CustomPredicateExpr {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "CustomPredicateExpr")
        }
    }

    impl PhysicalExpr for CustomPredicateExpr {
        fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
            unreachable!()
        }

        fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
            unreachable!()
        }

        fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
            unreachable!()
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![&self.inner]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            Ok(self)
        }

        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            Display::fmt(self, f)
        }
    }

    #[derive(Debug)]
    struct CustomPhysicalExtensionCodec;
    impl PhysicalExtensionCodec for CustomPhysicalExtensionCodec {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unreachable!()
        }

        fn try_encode(
            &self,
            _node: Arc<dyn ExecutionPlan>,
            _buf: &mut Vec<u8>,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<()> {
            unreachable!()
        }

        fn try_decode_expr(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn PhysicalExpr>],
            _ctx: &PhysicalExprDecodeCtx<'_>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            if buf == b"CustomPredicateExpr" {
                Ok(Arc::new(CustomPredicateExpr {
                    inner: inputs[0].clone(),
                }))
            } else {
                internal_err!("Not supported")
            }
        }

        fn try_encode_expr(
            &self,
            node: &Arc<dyn PhysicalExpr>,
            buf: &mut Vec<u8>,
            _ctx: &PhysicalExprEncodeCtx<'_>,
        ) -> Result<()> {
            if node.downcast_ref::<CustomPredicateExpr>().is_some() {
                buf.extend_from_slice(b"CustomPredicateExpr");
                Ok(())
            } else {
                internal_err!("Not supported")
            }
        }
    }

    let exec_plan = DataSourceExec::from_data_source(scan_config);

    let ctx = SessionContext::new();
    roundtrip_test_and_return(
        exec_plan,
        &ctx,
        &CustomPhysicalExtensionCodec {},
        &DefaultPhysicalProtoConverter {},
    )?;
    Ok(())
}

#[tokio::test]
async fn roundtrip_json_source() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_json("t1", "../core/tests/data/1.json", Default::default())
        .await?;
    let plan = ctx.table("t1").await?.create_physical_plan().await?;
    roundtrip_test(plan)
}

#[tokio::test]
async fn roundtrip_coalesce() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table(
        "t",
        Arc::new(EmptyTable::new(Arc::new(Schema::new(Fields::from([
            Arc::new(Field::new("f", DataType::Int64, false)),
        ]))))),
    )?;
    let df = ctx.sql("select coalesce(f) as f from t").await?;
    let plan = df.create_physical_plan().await?;

    let node = PhysicalPlanNode::try_from_physical_plan(
        plan.clone(),
        &DefaultPhysicalExtensionCodec {},
    )?;
    let node = PhysicalPlanNode::decode(node.encode_to_vec().as_slice())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let restored =
        node.try_into_physical_plan(&ctx.task_ctx(), &DefaultPhysicalExtensionCodec {})?;

    assert_eq!(
        plan.schema(),
        restored.schema(),
        "Schema mismatch for plans:\n>> initial:\n{}>> final: \n{}",
        displayable(plan.as_ref())
            .set_show_schema(true)
            .indent(true),
        displayable(restored.as_ref())
            .set_show_schema(true)
            .indent(true),
    );

    Ok(())
}

#[tokio::test]
async fn roundtrip_generate_series() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table(
        "t",
        Arc::new(EmptyTable::new(Arc::new(Schema::new(Fields::from([
            Arc::new(Field::new("f", DataType::Int64, false)),
        ]))))),
    )?;
    let df = ctx.sql("select * from generate_series(1, 10000)").await?;
    let plan = df.create_physical_plan().await?;

    let node = PhysicalPlanNode::try_from_physical_plan(
        plan.clone(),
        &DefaultPhysicalExtensionCodec {},
    )?;
    let node = PhysicalPlanNode::decode(node.encode_to_vec().as_slice())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let restored =
        node.try_into_physical_plan(&ctx.task_ctx(), &DefaultPhysicalExtensionCodec {})?;

    assert_eq!(
        plan.schema(),
        restored.schema(),
        "Schema mismatch for plans:\n>> initial:\n{}>> final: \n{}",
        displayable(plan.as_ref())
            .set_show_schema(true)
            .indent(true),
        displayable(restored.as_ref())
            .set_show_schema(true)
            .indent(true),
    );

    Ok(())
}

#[tokio::test]
async fn roundtrip_projection_source() -> Result<()> {
    let schema = Arc::new(Schema::new(Fields::from([
        Arc::new(Field::new("a", DataType::Utf8, false)),
        Arc::new(Field::new("b", DataType::Utf8, false)),
        Arc::new(Field::new("c", DataType::Int32, false)),
        Arc::new(Field::new("d", DataType::Int32, false)),
    ])));

    let statistics = Statistics::new_unknown(&schema);

    let file_source = Arc::new(ParquetSource::new(Arc::clone(&schema)));
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.parquet".to_string(),
                1024,
            )])])
            .with_statistics(statistics)
            .with_projection_indices(Some(vec![0, 1, 2]))?
            .build();

    let filter = Arc::new(
        FilterExecBuilder::new(
            Arc::new(BinaryExpr::new(col("c", &schema)?, Operator::Eq, lit(1))),
            DataSourceExec::from_data_source(scan_config),
        )
        .apply_projection(Some(vec![0, 1]))?
        .build()?,
    );

    roundtrip_test(filter)
}

#[tokio::test]
async fn roundtrip_parquet_select_star() -> Result<()> {
    let ctx = all_types_context().await?;
    let sql = "select * from alltypes_plain";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_parquet_select_projection() -> Result<()> {
    let ctx = all_types_context().await?;
    let sql = "select string_col, timestamp_col from alltypes_plain";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_parquet_select_star_predicate() -> Result<()> {
    let ctx = all_types_context().await?;
    let sql = "select * from alltypes_plain where id > 4";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_parquet_select_projection_predicate() -> Result<()> {
    let ctx = all_types_context().await?;
    let sql = "select string_col, timestamp_col from alltypes_plain where id > 4";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_empty_projection() -> Result<()> {
    let ctx = all_types_context().await?;
    let sql = "select 1 from alltypes_plain";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_memory_source_empty_projection() -> Result<()> {
    // Memory scan: `Some(vec![])` must not decode back as `None`
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["Tom"])),
            Arc::new(arrow::array::Int64Array::from(vec![18i64])),
        ],
    )?;
    ctx.register_batch("tmem", batch)?;
    let sql = "select 1 from tmem";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_memory_source() -> Result<()> {
    let ctx = SessionContext::new();
    let plan = ctx
        .sql("select * from values ('Tom', 18)")
        .await?
        .create_physical_plan()
        .await?;
    roundtrip_test(plan)
}

#[tokio::test]
async fn roundtrip_memory_source_projected_sort_information_and_fetch() -> Result<()> {
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSource as _;

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["Tom", "Bob"])),
            Arc::new(arrow::array::Int64Array::from(vec![18i64, 21i64])),
        ],
    )?;
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        col("b", &schema)?,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
    let source =
        MemorySourceConfig::try_new(&[vec![batch]], Arc::clone(&schema), Some(vec![1]))?
            .with_limit(Some(1))
            .with_show_sizes(false)
            .try_with_sort_information(vec![ordering])?;
    let exec_plan = DataSourceExec::from_data_source(source.clone());

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let decoded = roundtrip_test_and_return(exec_plan, &ctx, &codec, &proto_converter)?;

    // The string representation does not include every field; check the
    // decoded source directly.
    let decoded = decoded
        .downcast_ref::<DataSourceExec>()
        .expect("expected DataSourceExec");
    let decoded_source = decoded
        .data_source()
        .downcast_ref::<MemorySourceConfig>()
        .expect("expected MemorySourceConfig");
    assert_eq!(decoded_source.partitions(), source.partitions());
    assert_eq!(decoded_source.original_schema(), source.original_schema());
    assert_eq!(decoded_source.projection(), source.projection());
    assert_eq!(decoded_source.sort_information(), source.sort_information());
    assert_eq!(decoded_source.fetch(), Some(1));
    assert!(!decoded_source.show_sizes());
    Ok(())
}

#[tokio::test]
async fn roundtrip_listing_table_with_schema_metadata() -> Result<()> {
    let ctx = SessionContext::new();
    let file_format = JsonFormat::default();
    let table_partition_cols = vec![("part".to_owned(), DataType::Int64)];
    let data = "../core/tests/data/partitioned_table_json";
    let listing_table_url = ListingTableUrl::parse(data)?;
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_table_partition_cols(table_partition_cols);

    let config = ListingTableConfig::new(listing_table_url)
        .with_listing_options(listing_options)
        .infer_schema(&ctx.state())
        .await?;

    // Decorate metadata onto the inferred ListingTable schema
    let schema_with_meta = config
        .file_schema
        .clone()
        .map(|s| {
            let mut meta: HashMap<String, String> = HashMap::new();
            meta.insert("foo.bar".to_string(), "baz".to_string());
            s.as_ref().clone().with_metadata(meta)
        })
        .expect("Must decorate metadata");

    let config = config.with_schema(Arc::new(schema_with_meta));
    ctx.register_table("hive_style", Arc::new(ListingTable::try_new(config)?))?;

    let plan = ctx
        .sql("select * from hive_style limit 1")
        .await?
        .create_physical_plan()
        .await?;

    roundtrip_test(plan)
}

fn roundtrip_file_scan_config(scan_config: FileScanConfig) -> Result<FileScanConfig> {
    let exec_plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(scan_config);
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let result_plan =
        roundtrip_test_and_return(exec_plan, &ctx, &codec, &proto_converter)?;

    let data_source_exec = result_plan
        .downcast_ref::<DataSourceExec>()
        .expect("Expected DataSourceExec");
    let file_scan_config = data_source_exec
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .expect("Expected FileScanConfig");
    Ok(file_scan_config.clone())
}

#[test]
fn roundtrip_parquet_exec_output_partitioning() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let file_source = Arc::new(ParquetSource::new(Arc::clone(&file_schema)));
    let output_partitioning =
        Partitioning::Hash(vec![Arc::new(Column::new("col", 0))], 1);
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.parquet".to_string(),
                1024,
            )])])
            .with_output_partitioning(Some(output_partitioning.clone()))
            .build();

    assert_eq!(
        roundtrip_file_scan_config(scan_config)?.output_partitioning,
        Some(output_partitioning)
    );

    Ok(())
}

#[test]
fn roundtrip_parquet_exec_range_output_partitioning() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));
    let file_source = Arc::new(ParquetSource::new(Arc::clone(&file_schema)));
    let output_partitioning = Partitioning::Range(RangePartitioning::new(
        LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
            "col", 0,
        )))])
        .unwrap(),
        vec![SplitPoint::new(vec![ScalarValue::Int32(Some(10))])],
    ));
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![
                FileGroup::new(vec![PartitionedFile::new(
                    "/path/to/file-1.parquet".to_string(),
                    1024,
                )]),
                FileGroup::new(vec![PartitionedFile::new(
                    "/path/to/file-2.parquet".to_string(),
                    1024,
                )]),
            ])
            .with_output_partitioning(Some(output_partitioning.clone()))
            .build();

    assert_eq!(
        roundtrip_file_scan_config(scan_config)?.output_partitioning,
        Some(output_partitioning)
    );

    Ok(())
}
