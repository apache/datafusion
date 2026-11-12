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

//! Built-in scans and sinks encode as `data_source` / `data_sink`, and the
//! deprecated flat variants (`csv_scan`, `json_sink`, …) still decode.

use super::roundtrip_test;
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::csv::CsvSink;
use datafusion::datasource::file_format::json::JsonSink;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    ArrowSource, CsvSource, FileGroup, FileOutputMode, FileScanConfigBuilder,
    FileSinkConfig, FileSource, JsonSource, ParquetSource,
};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::file_options::csv_writer::CsvWriterOptions;
use datafusion_common::file_options::json_writer::JsonWriterOptions;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::TableSchema;
use datafusion_expr::dml::InsertOp;
use datafusion_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
use datafusion_proto::protobuf::{
    self, PhysicalPlanNode, data_sink_node::SinkType, data_source_node::SourceType,
    file_source_node::FileSourceType,
};
use prost::Message;
use std::sync::Arc;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
}

fn file_scan(source: Arc<dyn FileSource>, path: &str) -> Arc<dyn ExecutionPlan> {
    DataSourceExec::from_data_source(
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                path.to_string(),
                1024,
            )])])
            .build(),
    )
}

fn sink_exec(sink: Arc<dyn DataSink>) -> Arc<dyn ExecutionPlan> {
    Arc::new(DataSinkExec::new(
        Arc::new(PlaceholderRowExec::new(schema())),
        sink,
        None,
    ))
}

fn sink_config(extension: &str) -> Result<FileSinkConfig> {
    Ok(FileSinkConfig {
        original_url: String::default(),
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_group: FileGroup::new(vec![PartitionedFile::new("/tmp".to_string(), 1)]),
        table_paths: vec![ListingTableUrl::parse("file:///")?],
        output_schema: schema(),
        table_partition_cols: vec![],
        insert_op: InsertOp::Append,
        keep_partition_by_columns: false,
        file_extension: extension.into(),
        file_output_mode: FileOutputMode::SingleFile,
    })
}

/// One plan per built-in scan and sink.
fn built_in_plans() -> Result<Vec<Arc<dyn ExecutionPlan>>> {
    let table_schema = TableSchema::from(&schema());
    let batch =
        RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(vec![1]))])?;
    let mut plans = vec![
        file_scan(
            Arc::new(ParquetSource::new(table_schema.clone())),
            "/f.parquet",
        ),
        file_scan(Arc::new(CsvSource::new(table_schema.clone())), "/f.csv"),
        file_scan(Arc::new(JsonSource::new(table_schema.clone())), "/f.json"),
        file_scan(
            Arc::new(ArrowSource::new_stream_file_source(table_schema.clone())),
            "/f.arrows",
        ),
        DataSourceExec::from_data_source(MemorySourceConfig::try_new(
            &[vec![batch]],
            schema(),
            None,
        )?),
        sink_exec(Arc::new(JsonSink::new(
            sink_config("json")?,
            JsonWriterOptions::new(CompressionTypeVariant::UNCOMPRESSED),
        ))),
        sink_exec(Arc::new(CsvSink::new(
            sink_config("csv")?,
            CsvWriterOptions::new(
                Default::default(),
                CompressionTypeVariant::UNCOMPRESSED,
            ),
        ))),
        sink_exec(Arc::new(ParquetSink::new(
            sink_config("parquet")?,
            TableParquetOptions::default(),
        ))),
    ];
    #[cfg(feature = "avro")]
    plans.push(file_scan(
        Arc::new(datafusion_datasource_avro::source::AvroSource::new(
            table_schema,
        )),
        "/f.avro",
    ));
    #[cfg(not(feature = "avro"))]
    let _ = &mut plans;
    Ok(plans)
}

/// Rewrite the layered encoding into the deprecated flat variant.
fn downgrade(plan: PhysicalPlanType) -> PhysicalPlanType {
    match plan {
        PhysicalPlanType::DataSource(node) => match node.source.unwrap().source_type {
            Some(SourceType::File(scan)) => {
                let base_conf = scan.base;
                match scan.source.unwrap().file_source_type.unwrap() {
                    FileSourceType::Parquet(s) => {
                        PhysicalPlanType::ParquetScan(protobuf::ParquetScanExecNode {
                            base_conf,
                            predicate: s.predicate,
                            parquet_options: s.parquet_options,
                            sort_order_for_reorder: s.sort_order_for_reorder,
                            reverse_row_groups: s.reverse_row_groups,
                            metadata_size_hint: s.metadata_size_hint,
                        })
                    }
                    FileSourceType::Csv(s) => {
                        use protobuf::{
                            csv_scan_exec_node as old, csv_source_node as new,
                        };
                        PhysicalPlanType::CsvScan(protobuf::CsvScanExecNode {
                            base_conf,
                            has_header: s.has_header,
                            delimiter: s.delimiter,
                            quote: s.quote,
                            optional_escape: s.optional_escape.map(
                                |new::OptionalEscape::Escape(e)| {
                                    old::OptionalEscape::Escape(e)
                                },
                            ),
                            optional_comment: s.optional_comment.map(
                                |new::OptionalComment::Comment(c)| {
                                    old::OptionalComment::Comment(c)
                                },
                            ),
                            newlines_in_values: s.newlines_in_values,
                            truncate_rows: s.truncate_rows,
                            terminator: s.terminator,
                        })
                    }
                    FileSourceType::Json(s) => {
                        PhysicalPlanType::JsonScan(protobuf::JsonScanExecNode {
                            base_conf,
                            newline_delimited: s.newline_delimited,
                        })
                    }
                    FileSourceType::Avro(_) => {
                        PhysicalPlanType::AvroScan(protobuf::AvroScanExecNode {
                            base_conf,
                        })
                    }
                    FileSourceType::Arrow(s) => {
                        PhysicalPlanType::ArrowScan(protobuf::ArrowScanExecNode {
                            base_conf,
                            format: s.format,
                        })
                    }
                    FileSourceType::Extension(_) => unreachable!(),
                }
            }
            Some(SourceType::Memory(scan)) => PhysicalPlanType::MemoryScan(scan),
            other => unreachable!("{other:?}"),
        },
        PhysicalPlanType::DataSink(node) => {
            let protobuf::DataSinkExecNode {
                input,
                sink,
                sort_order,
            } = *node;
            match sink.unwrap().sink_type.unwrap() {
                SinkType::Json(sink) => {
                    PhysicalPlanType::JsonSink(Box::new(protobuf::JsonSinkExecNode {
                        input,
                        sink: Some(sink),
                        sink_schema: None,
                        sort_order,
                    }))
                }
                SinkType::Csv(sink) => {
                    PhysicalPlanType::CsvSink(Box::new(protobuf::CsvSinkExecNode {
                        input,
                        sink: Some(sink),
                        sink_schema: None,
                        sort_order,
                    }))
                }
                SinkType::Parquet(sink) => PhysicalPlanType::ParquetSink(Box::new(
                    protobuf::ParquetSinkExecNode {
                        input,
                        sink: Some(sink),
                        sink_schema: None,
                        sort_order,
                    },
                )),
                SinkType::Extension(_) => unreachable!(),
            }
        }
        other => unreachable!("not a scan or sink: {other:?}"),
    }
}

fn decode(node: &PhysicalPlanNode) -> Result<Arc<dyn ExecutionPlan>> {
    let ctx = SessionContext::new();
    node.try_into_physical_plan(
        ctx.task_ctx().as_ref(),
        &DefaultPhysicalExtensionCodec {},
    )
}

fn encode(plan: Arc<dyn ExecutionPlan>) -> Result<PhysicalPlanNode> {
    PhysicalPlanNode::try_from_physical_plan(plan, &DefaultPhysicalExtensionCodec {})
}

fn decode_err(node: PhysicalPlanNode) -> String {
    decode(&node).unwrap_err().to_string()
}

fn data_source_node(source_type: Option<SourceType>) -> PhysicalPlanNode {
    PhysicalPlanNode {
        physical_plan_type: Some(PhysicalPlanType::DataSource(
            protobuf::DataSourceExecNode {
                source: Some(protobuf::DataSourceNode { source_type }),
            },
        )),
    }
}

fn file_scan_node(
    base: Option<protobuf::FileScanExecConf>,
    file_source_type: Option<FileSourceType>,
) -> PhysicalPlanNode {
    data_source_node(Some(SourceType::File(protobuf::FileScanNode {
        base,
        source: Some(protobuf::FileSourceNode { file_source_type }),
    })))
}

fn csv_base() -> Result<protobuf::FileScanExecConf> {
    let plan = file_scan(
        Arc::new(CsvSource::new(TableSchema::from(&schema()))),
        "/f.csv",
    );
    match encode(plan)?.physical_plan_type {
        Some(PhysicalPlanType::DataSource(protobuf::DataSourceExecNode {
            source:
                Some(protobuf::DataSourceNode {
                    source_type: Some(SourceType::File(scan)),
                }),
        })) => Ok(scan.base.unwrap()),
        other => unreachable!("{other:?}"),
    }
}

fn sink_node(
    input: Option<PhysicalPlanNode>,
    sink: Option<protobuf::DataSinkNode>,
) -> PhysicalPlanNode {
    PhysicalPlanNode {
        physical_plan_type: Some(PhysicalPlanType::DataSink(Box::new(
            protobuf::DataSinkExecNode {
                input: input.map(Box::new),
                sink,
                sort_order: None,
            },
        ))),
    }
}

#[test]
fn built_ins_encode_layered_and_legacy_variants_decode() -> Result<()> {
    for plan in built_in_plans()? {
        roundtrip_test(Arc::clone(&plan))?;
        let layered = encode(Arc::clone(&plan))?;
        assert!(
            matches!(
                layered.physical_plan_type,
                Some(PhysicalPlanType::DataSource(_) | PhysicalPlanType::DataSink(_))
            ),
            "unexpected encoding: {layered:?}"
        );

        // The legacy variant decodes to the same plan and re-encodes in the
        // layered form.
        let legacy = PhysicalPlanNode {
            physical_plan_type: Some(downgrade(
                layered.physical_plan_type.clone().unwrap(),
            )),
        };
        let decoded = decode(&legacy)?;
        assert_eq!(format!("{plan:?}"), format!("{decoded:?}"));
        assert_eq!(
            layered.encode_to_vec(),
            encode(decoded)?.encode_to_vec(),
            "{legacy:?}"
        );
    }
    Ok(())
}

#[test]
fn extension_nodes_without_decoder_error() -> Result<()> {
    let err = decode_err(data_source_node(Some(SourceType::Extension(
        protobuf::ExtensionDataSourceNode {
            name: "my_source".to_string(),
            payload: vec![],
        },
    ))));
    assert!(
        err.contains("No decoder for extension DataSource 'my_source'"),
        "{err}"
    );

    let err = decode_err(file_scan_node(
        Some(csv_base()?),
        Some(FileSourceType::Extension(
            protobuf::ExtensionFileSourceNode {
                name: "my_format".to_string(),
                payload: vec![1, 2, 3],
            },
        )),
    ));
    assert!(
        err.contains("No decoder for extension FileSource 'my_format'"),
        "{err}"
    );

    let input = encode(Arc::new(PlaceholderRowExec::new(schema())))?;
    let err = decode_err(sink_node(
        Some(input),
        Some(protobuf::DataSinkNode {
            sink_type: Some(SinkType::Extension(protobuf::ExtensionDataSinkNode {
                name: "my_sink".to_string(),
                payload: vec![],
            })),
        }),
    ));
    assert!(
        err.contains("No decoder for extension DataSink 'my_sink'"),
        "{err}"
    );
    Ok(())
}

#[test]
fn missing_fields_give_clear_errors() -> Result<()> {
    let missing_source = PhysicalPlanNode {
        physical_plan_type: Some(PhysicalPlanType::DataSource(
            protobuf::DataSourceExecNode { source: None },
        )),
    };
    assert!(
        decode_err(missing_source).contains("DataSourceExecNode is missing its source")
    );
    assert!(
        decode_err(data_source_node(None))
            .contains("DataSourceExecNode is missing its source")
    );
    assert!(
        decode_err(file_scan_node(
            None,
            Some(FileSourceType::Avro(protobuf::AvroSourceNode {}))
        ))
        .contains("FileScanNode is missing its base")
    );
    assert!(
        decode_err(file_scan_node(Some(csv_base()?), None))
            .contains("FileScanNode is missing its source")
    );

    let input = encode(Arc::new(PlaceholderRowExec::new(schema())))?;
    let err = decode_err(sink_node(Some(input.clone()), None));
    assert!(
        err.contains("DataSinkExecNode is missing its sink"),
        "{err}"
    );
    let err = decode_err(sink_node(
        Some(input),
        Some(protobuf::DataSinkNode { sink_type: None }),
    ));
    assert!(
        err.contains("DataSinkExecNode is missing its sink"),
        "{err}"
    );
    let err = decode_err(sink_node(None, None));
    assert!(err.contains("input"), "{err}");

    // A legacy sink with no sink payload names the missing legacy field.
    let input = encode(Arc::new(PlaceholderRowExec::new(schema())))?;
    let legacy = PhysicalPlanNode {
        physical_plan_type: Some(PhysicalPlanType::JsonSink(Box::new(
            protobuf::JsonSinkExecNode {
                input: Some(Box::new(input)),
                sink: None,
                sink_schema: None,
                sort_order: None,
            },
        ))),
    };
    let err = decode_err(legacy);
    assert!(
        err.contains("JsonSinkExecNode is missing required field 'sink'"),
        "{err}"
    );
    Ok(())
}
