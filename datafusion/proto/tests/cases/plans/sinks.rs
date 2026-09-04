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

//! Data sinks and their file sink configurations.

use super::{roundtrip_test, roundtrip_test_and_return};
use arrow::csv::writer::Terminator;
use arrow::csv::{QuoteStyle, WriterBuilder};
use async_trait::async_trait;
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::file_format::csv::CsvSink;
use datafusion::datasource::file_format::json::JsonSink;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    FileGroup, FileOutputMode, FileSink, FileSinkConfig,
};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{LexRequirement, PhysicalSortRequirement};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::proto::ExecutionPlanEncodeCtx;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::file_options::csv_writer::CsvWriterOptions;
use datafusion_common::file_options::json_writer::JsonWriterOptions;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_expr::dml::InsertOp;
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter,
};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::PhysicalPlanNode;
use datafusion_proto_common::protobuf_common::CsvWriterOptions as ProtoCsvWriterOptions;
use std::fmt::Formatter;
use std::sync::Arc;
use std::vec;

#[derive(Debug)]
struct ProtoHookSink {
    schema: SchemaRef,
}

impl DisplayAs for ProtoHookSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ProtoHookSink")
    }
}

#[async_trait]
impl DataSink for ProtoHookSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        _data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        unreachable!("serialization test does not execute the sink")
    }

    fn try_to_proto(
        &self,
        exec: &DataSinkExec,
        ctx: &ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<PhysicalPlanNode>> {
        let input = ctx.encode_child(exec.input())?;
        let sort_order = exec.encode_sort_order(ctx)?;
        assert!(matches!(
            input.physical_plan_type,
            Some(protobuf::physical_plan_node::PhysicalPlanType::PlaceholderRow(_))
        ));
        assert_eq!(
            sort_order
                .as_ref()
                .map(|ordering| ordering.physical_sort_expr_nodes.len()),
            Some(1)
        );
        assert_eq!(exec.schema().fields().len(), 1);

        Ok(Some(PhysicalPlanNode {
            physical_plan_type: Some(
                protobuf::physical_plan_node::PhysicalPlanType::Empty(
                    protobuf::EmptyExecNode {
                        schema: Some(exec.schema().as_ref().try_into()?),
                        partitions: 1,
                    },
                ),
            ),
        }))
    }
}

#[test]
fn data_sink_exec_delegates_to_sink_proto_hook() -> Result<()> {
    let input_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let input = Arc::new(PlaceholderRowExec::new(Arc::clone(&input_schema)));
    let sink = Arc::new(ProtoHookSink {
        schema: Arc::clone(&input_schema),
    });
    let sort_order = [PhysicalSortRequirement::new(
        Arc::new(Column::new("value", 0)),
        Some(SortOptions::default()),
    )]
    .into();
    let plan = Arc::new(DataSinkExec::new(input, sink, Some(sort_order)));

    let node = PhysicalPlanNode::try_from_physical_plan(
        plan,
        &DefaultPhysicalExtensionCodec {},
    )?;

    assert!(matches!(
        node.physical_plan_type,
        Some(protobuf::physical_plan_node::PhysicalPlanType::Empty(_))
    ));
    Ok(())
}

#[test]
fn file_sink_config_roundtrip_preserves_fields() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "partition",
        DataType::Utf8,
        false,
    )]));
    let config = FileSinkConfig {
        original_url: "file:///tmp/output".to_string(),
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_group: FileGroup::new(vec![PartitionedFile::new("/tmp/output", 1)]),
        table_paths: vec![ListingTableUrl::parse("file:///tmp/output")?],
        output_schema: schema,
        table_partition_cols: vec![("partition".to_string(), DataType::Utf8)],
        insert_op: InsertOp::Overwrite,
        keep_partition_by_columns: true,
        file_extension: "parquet".to_string(),
        file_output_mode: FileOutputMode::Directory,
    };

    let encoded = protobuf::FileSinkConfig::try_from(&config)?;
    assert_eq!(encoded.insert_op(), protobuf::InsertOp::Overwrite);
    assert_eq!(
        encoded.file_output_mode(),
        protobuf::FileOutputMode::Directory
    );

    let decoded = FileSinkConfig::try_from(&encoded)?;
    assert_eq!(decoded.object_store_url, config.object_store_url);
    assert_eq!(decoded.table_paths, config.table_paths);
    assert_eq!(
        decoded.output_schema.as_ref(),
        config.output_schema.as_ref()
    );
    assert_eq!(decoded.table_partition_cols, config.table_partition_cols);
    assert_eq!(decoded.insert_op, config.insert_op);
    assert_eq!(
        decoded.keep_partition_by_columns,
        config.keep_partition_by_columns
    );
    assert_eq!(decoded.file_extension, config.file_extension);
    assert_eq!(decoded.file_output_mode, config.file_output_mode);

    let [decoded_file] = decoded.file_group.files() else {
        panic!("expected one decoded output file");
    };
    let [config_file] = config.file_group.files() else {
        panic!("expected one configured output file");
    };
    assert_eq!(
        decoded_file.object_meta.location,
        config_file.object_meta.location
    );
    assert_eq!(decoded_file.object_meta.size, config_file.object_meta.size);
    Ok(())
}

#[test]
fn roundtrip_json_sink() -> Result<()> {
    let field_a = Field::new("plan_type", DataType::Utf8, false);
    let field_b = Field::new("plan", DataType::Utf8, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));
    let input = Arc::new(PlaceholderRowExec::new(schema.clone()));

    let file_sink_config = FileSinkConfig {
        original_url: String::default(),
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_group: FileGroup::new(vec![PartitionedFile::new("/tmp".to_string(), 1)]),
        table_paths: vec![ListingTableUrl::parse("file:///")?],
        output_schema: schema.clone(),
        table_partition_cols: vec![("plan_type".to_string(), DataType::Utf8)],
        insert_op: InsertOp::Overwrite,
        keep_partition_by_columns: true,
        file_extension: "json".into(),
        file_output_mode: FileOutputMode::SingleFile,
    };
    let data_sink = Arc::new(JsonSink::new(
        file_sink_config,
        JsonWriterOptions::new_with_level(CompressionTypeVariant::ZSTD, 7),
    ));
    let sort_order: LexRequirement = [PhysicalSortRequirement::new(
        Arc::new(Column::new("plan_type", 0)),
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
    )]
    .into();

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let roundtrip_plan = roundtrip_test_and_return(
        Arc::new(DataSinkExec::new(
            input,
            data_sink,
            Some(sort_order.clone()),
        )),
        &ctx,
        &codec,
        &proto_converter,
    )?;

    let roundtrip_plan =
        roundtrip_plan
            .downcast_ref::<DataSinkExec>()
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!("Expected DataSinkExec")
            })?;
    let json_sink = roundtrip_plan
        .sink()
        .downcast_ref::<JsonSink>()
        .ok_or_else(|| {
            datafusion_common::internal_datafusion_err!("Expected JsonSink")
        })?;
    assert_eq!(json_sink.config().insert_op, InsertOp::Overwrite);
    assert!(json_sink.config().keep_partition_by_columns);
    assert_eq!(
        json_sink.config().file_output_mode,
        FileOutputMode::SingleFile
    );
    assert_eq!(
        json_sink.writer_options().compression,
        CompressionTypeVariant::ZSTD
    );
    assert_eq!(json_sink.writer_options().compression_level, Some(7));
    assert_eq!(roundtrip_plan.sort_order(), &Some(sort_order));
    Ok(())
}

#[test]
fn roundtrip_csv_sink() -> Result<()> {
    let field_a = Field::new("plan_type", DataType::Utf8, false);
    let field_b = Field::new("plan", DataType::Utf8, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));
    let input = Arc::new(PlaceholderRowExec::new(schema.clone()));

    let file_sink_config = FileSinkConfig {
        original_url: String::default(),
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_group: FileGroup::new(vec![PartitionedFile::new("/tmp".to_string(), 1)]),
        table_paths: vec![ListingTableUrl::parse("file:///")?],
        output_schema: schema.clone(),
        table_partition_cols: vec![("plan_type".to_string(), DataType::Utf8)],
        insert_op: InsertOp::Overwrite,
        keep_partition_by_columns: true,
        file_extension: "csv".into(),
        file_output_mode: FileOutputMode::Directory,
    };
    let writer_options = WriterBuilder::default()
        .with_delimiter(b'|')
        .with_header(false)
        .with_quote(b'\'')
        .with_escape(b'!')
        .with_double_quote(false)
        .with_date_format("%Y/%m/%d".into())
        .with_datetime_format("%Y/%m/%d %H:%M:%S".into())
        .with_timestamp_format("%s".into())
        .with_timestamp_tz_format("%Y-%m-%dT%H:%M:%S%:z".into())
        .with_time_format("%H-%M-%S".into())
        .with_null("NULL".into())
        .with_quote_style(QuoteStyle::Always)
        .with_ignore_leading_whitespace(true)
        .with_ignore_trailing_whitespace(true)
        .with_line_terminator(Terminator::CRLF);
    let data_sink = Arc::new(CsvSink::new(
        file_sink_config,
        CsvWriterOptions::new_with_level(writer_options, CompressionTypeVariant::ZSTD, 7),
    ));
    let sort_order: LexRequirement = [PhysicalSortRequirement::new(
        Arc::new(Column::new("plan_type", 0)),
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
    )]
    .into();

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};

    let roundtrip_plan = roundtrip_test_and_return(
        Arc::new(DataSinkExec::new(
            input,
            data_sink,
            Some(sort_order.clone()),
        )),
        &ctx,
        &codec,
        &proto_converter,
    )?;

    let roundtrip_plan =
        roundtrip_plan
            .downcast_ref::<DataSinkExec>()
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!("Expected DataSinkExec")
            })?;
    let csv_sink = roundtrip_plan
        .sink()
        .downcast_ref::<CsvSink>()
        .ok_or_else(|| datafusion_common::internal_datafusion_err!("Expected CsvSink"))?;
    assert_eq!(csv_sink.config().insert_op, InsertOp::Overwrite);
    assert!(csv_sink.config().keep_partition_by_columns);
    assert_eq!(
        csv_sink.config().file_output_mode,
        FileOutputMode::Directory
    );

    let options = csv_sink.writer_options();
    assert_eq!(options.compression, CompressionTypeVariant::ZSTD);
    assert_eq!(options.compression_level, Some(7));
    let writer = &options.writer_options;
    assert_eq!(writer.delimiter(), b'|');
    assert!(!writer.header());
    assert_eq!(writer.quote(), b'\'');
    assert_eq!(writer.escape(), b'!');
    assert!(!writer.double_quote());
    assert_eq!(writer.date_format(), Some("%Y/%m/%d"));
    assert_eq!(writer.datetime_format(), Some("%Y/%m/%d %H:%M:%S"));
    assert_eq!(writer.timestamp_format(), Some("%s"));
    assert_eq!(writer.timestamp_tz_format(), Some("%Y-%m-%dT%H:%M:%S%:z"));
    assert_eq!(writer.time_format(), Some("%H-%M-%S"));
    assert_eq!(writer.null(), "NULL");
    assert!(matches!(writer.quote_style(), QuoteStyle::Always));
    assert!(writer.ignore_leading_whitespace());
    assert!(writer.ignore_trailing_whitespace());
    assert!(matches!(writer.line_terminator(), Terminator::CRLF));
    assert_eq!(roundtrip_plan.sort_order(), &Some(sort_order));

    let unset = CsvWriterOptions::new(
        WriterBuilder::default(),
        CompressionTypeVariant::UNCOMPRESSED,
    );
    let unset = CsvWriterOptions::try_from(&ProtoCsvWriterOptions::try_from(&unset)?)?;
    assert_eq!(unset.writer_options.date_format(), None);
    assert_eq!(unset.writer_options.datetime_format(), None);
    assert_eq!(unset.writer_options.timestamp_format(), None);
    assert_eq!(unset.writer_options.timestamp_tz_format(), None);
    assert_eq!(unset.writer_options.time_format(), None);

    let defaults = CsvWriterOptions::try_from(&ProtoCsvWriterOptions::default())?;
    assert_eq!(defaults.compression_level, None);
    assert!(matches!(
        defaults.writer_options.line_terminator(),
        Terminator::Any(b'\n')
    ));

    let malformed = ProtoCsvWriterOptions {
        terminator: b"\r\r".to_vec(),
        ..Default::default()
    };
    assert!(CsvWriterOptions::try_from(&malformed).is_err());

    Ok(())
}

#[test]
fn roundtrip_parquet_sink() -> Result<()> {
    let field_a = Field::new("plan_type", DataType::Utf8, false);
    let field_b = Field::new("plan", DataType::Utf8, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));
    let input = Arc::new(PlaceholderRowExec::new(schema.clone()));

    let file_sink_config = FileSinkConfig {
        original_url: String::default(),
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_group: FileGroup::new(vec![PartitionedFile::new("/tmp".to_string(), 1)]),
        table_paths: vec![ListingTableUrl::parse("file:///")?],
        output_schema: schema.clone(),
        table_partition_cols: vec![("plan_type".to_string(), DataType::Utf8)],
        insert_op: InsertOp::Overwrite,
        keep_partition_by_columns: true,
        file_extension: "parquet".into(),
        file_output_mode: FileOutputMode::Automatic,
    };
    let data_sink = Arc::new(ParquetSink::new(
        file_sink_config,
        TableParquetOptions::default(),
    ));
    let sort_order = [PhysicalSortRequirement::new(
        Arc::new(Column::new("plan_type", 0)),
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
    )]
    .into();

    roundtrip_test(Arc::new(DataSinkExec::new(
        input,
        data_sink,
        Some(sort_order),
    )))
}
