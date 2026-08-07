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

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::datatypes::{IntervalMonthDayNanoType, Schema, SchemaRef};
use datafusion_catalog::memory::MemorySourceConfig;
use datafusion_common::config::CsvOptions;
use datafusion_common::{
    DataFusionError, Result, internal_datafusion_err, internal_err, not_impl_err,
};
#[cfg(feature = "parquet")]
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::sink::DataSinkExec;
use datafusion_datasource::source::{DataSource, DataSourceExec};
use datafusion_datasource_arrow::source::ArrowSource;
#[cfg(feature = "avro")]
use datafusion_datasource_avro::source::AvroSource;
use datafusion_datasource_csv::file_format::CsvSink;
use datafusion_datasource_csv::source::CsvSource;
use datafusion_datasource_json::file_format::JsonSink;
use datafusion_datasource_json::source::JsonSource;
#[cfg(feature = "parquet")]
use datafusion_datasource_parquet::CachedParquetFileReaderFactory;
#[cfg(feature = "parquet")]
use datafusion_datasource_parquet::file_format::ParquetSink;
#[cfg(feature = "parquet")]
use datafusion_datasource_parquet::source::ParquetSource;
#[cfg(feature = "parquet")]
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::{FunctionRegistry, TaskContext};
use datafusion_expr::physical_planning_context::ScalarSubqueryResults;
use datafusion_expr::{AggregateUDF, HigherOrderUDF, ScalarUDF, WindowUDF};
use datafusion_functions_table::generate_series::{
    Empty, GenSeriesArgs, GenerateSeriesTable, GenericSeriesState, TimestampValue,
};
use datafusion_physical_expr::LexOrdering;
use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::analyze::AnalyzeExec;
use datafusion_physical_plan::async_func::AsyncFuncExec;
use datafusion_physical_plan::buffer::BufferExec;
#[expect(
    deprecated,
    reason = "`CoalesceBatchesExec` remains supported for protobuf compatibility"
)]
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::coop::CooperativeExec;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::explain::ExplainExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec,
    SymmetricHashJoinExec,
};
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::memory::LazyMemoryExec;
use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::proto::{
    ExecutionPlanDecode, ExecutionPlanDecodeCtx, ExecutionPlanEncode,
    ExecutionPlanEncodeCtx,
};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::scalar_subquery::ScalarSubqueryExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::union::{InterleaveExec, UnionExec};
use datafusion_physical_plan::unnest::UnnestExec;
use datafusion_physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
use prost::Message;
use prost::bytes::BufMut;

use crate::common::{byte_to_string, str_to_byte};
use crate::convert_required;
use crate::physical_plan::from_proto::{
    parse_physical_expr_with_converter, parse_physical_sort_exprs,
    parse_protobuf_file_scan_config, parse_record_batches, parse_table_schema_from_proto,
};
use crate::physical_plan::to_proto::{
    serialize_file_scan_config, serialize_physical_expr_with_converter,
    serialize_physical_sort_exprs, serialize_record_batches,
};
use crate::protobuf::physical_plan_node::PhysicalPlanType;
use crate::protobuf::{self, SortMergeJoinExecNode, proto_error};

pub mod from_proto;
pub mod to_proto;

const HUMAN_DISPLAY_ALIAS_PREFIX: &str = "\u{1f}datafusion_human_display_alias_v1:";

fn encode_human_display_alias(human_display: &str, alias: &str) -> String {
    format!(
        "{HUMAN_DISPLAY_ALIAS_PREFIX}{}:{alias}{human_display}",
        alias.len()
    )
}

#[cfg(test)]
mod file_scan_config_serde {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::{Constraint, Constraints, ScalarValue, Statistics};
    use datafusion_datasource::file::FileSource;
    use datafusion_datasource::file_groups::FileGroup;
    use datafusion_datasource::file_stream::FileOpener;
    use datafusion_datasource::{PartitionedFile, TableSchema};
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::projection::{
        ProjectionExpr as FileProjectionExpr, ProjectionExprs as FileProjectionExprs,
    };
    use datafusion_physical_expr::{
        LexOrdering, Partitioning, PhysicalSortExpr, RangePartitioning, SplitPoint,
    };
    use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
    use object_store::ObjectStore;

    #[derive(Clone)]
    struct SerdeTestSource {
        metrics: ExecutionPlanMetricsSet,
        table_schema: TableSchema,
        projection: Option<FileProjectionExprs>,
    }

    impl SerdeTestSource {
        fn new(
            table_schema: TableSchema,
            projection: Option<FileProjectionExprs>,
        ) -> Self {
            Self {
                metrics: ExecutionPlanMetricsSet::new(),
                table_schema,
                projection,
            }
        }
    }

    impl FileSource for SerdeTestSource {
        fn create_file_opener(
            &self,
            _object_store: Arc<dyn ObjectStore>,
            _base_config: &FileScanConfig,
            _partition: usize,
        ) -> Result<Arc<dyn FileOpener>> {
            internal_err!("not needed for FileScanConfig serde tests")
        }

        fn table_schema(&self) -> &TableSchema {
            &self.table_schema
        }

        fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
            Arc::new(self.clone())
        }

        fn metrics(&self) -> &ExecutionPlanMetricsSet {
            &self.metrics
        }

        fn file_type(&self) -> &str {
            "serde-test"
        }

        fn try_pushdown_projection(
            &self,
            projection: &FileProjectionExprs,
        ) -> Result<Option<Arc<dyn FileSource>>> {
            Ok(Some(Arc::new(Self {
                projection: Some(projection.clone()),
                ..self.clone()
            })))
        }

        fn projection(&self) -> Option<&FileProjectionExprs> {
            self.projection.as_ref()
        }
    }

    fn populated_projection() -> FileProjectionExprs {
        FileProjectionExprs::new(vec![FileProjectionExpr::new(
            Arc::new(Column::new("value", 0)),
            "projected_value",
        )])
    }

    fn test_config(output_partitioning: Option<Partitioning>) -> FileScanConfig {
        test_config_with_projection(output_partitioning, Some(populated_projection()))
    }

    fn test_config_with_projection(
        output_partitioning: Option<Partitioning>,
        projection: Option<FileProjectionExprs>,
    ) -> FileScanConfig {
        let file_schema = Arc::new(
            Schema::new(vec![
                Field::new("value", DataType::Int32, false),
                Field::new("label", DataType::Utf8, true),
            ])
            .with_metadata(HashMap::from([(
                "serde_test_key".to_string(),
                "serde_test_value".to_string(),
            )])),
        );
        let table_schema = TableSchema::builder(Arc::clone(&file_schema))
            .with_table_partition_cols(vec![Arc::new(Field::new(
                "part",
                DataType::Utf8,
                false,
            ))])
            .build();
        let table_statistics = Statistics::new_unknown(table_schema.table_schema());
        let source = Arc::new(SerdeTestSource::new(table_schema, projection));
        let first_file = PartitionedFile::new("data/part=a/file.arrow", 1024)
            .with_partition_values(vec![ScalarValue::Utf8(Some("a".to_string()))])
            .with_range(10, 900)
            .with_arrow_schema(Arc::clone(&file_schema))
            .with_statistics(Arc::new(table_statistics.clone()));
        let second_file = PartitionedFile::new("data/part=b/file.arrow", 2048)
            .with_partition_values(vec![ScalarValue::Utf8(Some("b".to_string()))]);
        let third_file = PartitionedFile::new("data/part=c/file.arrow", 4096)
            .with_partition_values(vec![ScalarValue::Utf8(Some("c".to_string()))])
            .with_arrow_schema(Arc::clone(&file_schema));
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(
            Column::new("value", 0),
        ))])
        .expect("single expression ordering");

        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_file_groups(vec![
                FileGroup::new(vec![first_file, second_file]),
                FileGroup::new(vec![third_file]),
            ])
            .with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![0],
            )]))
            .with_statistics(table_statistics)
            .with_limit(Some(17))
            .with_batch_size(Some(256))
            .with_output_ordering(vec![ordering])
            .with_output_partitioning(output_partitioning)
            .build()
    }

    fn hash_partitioning() -> Partitioning {
        Partitioning::Hash(vec![Arc::new(Column::new("value", 0))], 3)
    }

    fn range_partitioning() -> Partitioning {
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(
            Column::new("value", 0),
        ))])
        .expect("single expression ordering");
        Partitioning::Range(RangePartitioning::new(
            ordering,
            vec![SplitPoint::new(vec![ScalarValue::Int32(Some(10))])],
        ))
    }

    fn decode_source(conf: &protobuf::FileScanExecConf) -> Result<Arc<dyn FileSource>> {
        Ok(Arc::new(SerdeTestSource::new(
            FileScanConfig::parse_table_schema_from_proto(conf)?,
            None,
        )))
    }

    struct FileScanSerdeHarness {
        codec: DefaultPhysicalExtensionCodec,
        converter: DefaultPhysicalProtoConverter,
        task_ctx: TaskContext,
    }

    impl FileScanSerdeHarness {
        fn new() -> Self {
            Self {
                codec: DefaultPhysicalExtensionCodec {},
                converter: DefaultPhysicalProtoConverter {},
                task_ctx: TaskContext::default(),
            }
        }

        fn encode(&self, config: &FileScanConfig) -> Result<protobuf::FileScanExecConf> {
            let encoder = ConverterPlanEncoder {
                codec: &self.codec,
                proto_converter: &self.converter,
            };
            config.try_to_proto(&ExecutionPlanEncodeCtx::new(&encoder))
        }

        fn decode(&self, conf: &protobuf::FileScanExecConf) -> Result<FileScanConfig> {
            self.decode_with_source(conf, decode_source(conf)?)
        }

        fn decode_with_source(
            &self,
            conf: &protobuf::FileScanExecConf,
            file_source: Arc<dyn FileSource>,
        ) -> Result<FileScanConfig> {
            let physical_decode_ctx =
                PhysicalPlanDecodeContext::new(&self.task_ctx, &self.codec);
            let decoder = ConverterPlanDecoder {
                ctx: &physical_decode_ctx,
                proto_converter: &self.converter,
            };
            FileScanConfig::try_from_proto(
                conf,
                &ExecutionPlanDecodeCtx::new(&decoder),
                file_source,
            )
        }
    }

    #[test]
    fn new_file_scan_config_serde_roundtrips_all_partitioning_variants() -> Result<()> {
        let serde = FileScanSerdeHarness::new();

        for config in [
            test_config(None),
            test_config(Some(Partitioning::RoundRobinBatch(2))),
            test_config(Some(hash_partitioning())),
            test_config(Some(range_partitioning())),
            test_config(Some(Partitioning::UnknownPartitioning(4))),
        ] {
            let encoded = serde.encode(&config)?;
            let reencoded = serde.encode(&serde.decode(&encoded)?)?;
            assert_eq!(reencoded.output_partitioning, encoded.output_partitioning);
        }

        Ok(())
    }

    #[test]
    fn new_file_scan_config_serde_preserves_complete_fixture() -> Result<()> {
        let serde = FileScanSerdeHarness::new();
        let config = test_config(None);
        let decoded = serde.decode(&serde.encode(&config)?)?;

        assert_eq!(decoded.constraints, config.constraints);
        assert_eq!(
            decoded.file_schema().metadata,
            config.file_schema().metadata
        );
        assert_eq!(decoded.file_groups.len(), 2);
        assert_eq!(decoded.file_groups[0].len(), 2);
        assert_eq!(decoded.file_groups[1].len(), 1);
        assert!(decoded.file_groups[0].files()[0].arrow_schema.is_some());
        assert!(decoded.file_groups[0].files()[1].arrow_schema.is_none());

        Ok(())
    }

    #[test]
    fn new_file_scan_config_serde_preserves_projection_presence() -> Result<()> {
        let serde = FileScanSerdeHarness::new();

        let absent = serde.encode(&test_config_with_projection(None, None))?;
        assert!(absent.projection_exprs.is_none());
        assert!(serde.decode(&absent)?.file_source().projection().is_none());

        let empty = serde.encode(&test_config_with_projection(
            None,
            Some(FileProjectionExprs::new(vec![])),
        ))?;
        assert!(
            empty
                .projection_exprs
                .as_ref()
                .is_some_and(|projection| projection.projections.is_empty())
        );
        assert!(
            serde
                .decode(&empty)?
                .file_source()
                .projection()
                .is_some_and(|projection| projection.as_ref().is_empty())
        );

        Ok(())
    }

    #[test]
    fn new_file_scan_config_decode_rejects_malformed_required_fields() -> Result<()> {
        let serde = FileScanSerdeHarness::new();
        let valid = serde.encode(&test_config(None))?;
        let file_source = decode_source(&valid)?;

        for (field, malformed) in [
            (
                "schema",
                protobuf::FileScanExecConf {
                    schema: None,
                    ..valid.clone()
                },
            ),
            (
                "constraints",
                protobuf::FileScanExecConf {
                    constraints: None,
                    ..valid.clone()
                },
            ),
            (
                "statistics",
                protobuf::FileScanExecConf {
                    statistics: None,
                    ..valid.clone()
                },
            ),
        ] {
            let err = serde
                .decode_with_source(&malformed, Arc::clone(&file_source))
                .expect_err("missing required field must fail");
            assert!(err.to_string().contains(field), "unexpected error: {err}");
        }

        let mut missing_projection_expr = valid.clone();
        missing_projection_expr
            .projection_exprs
            .as_mut()
            .expect("test config has projection expressions")
            .projections[0]
            .expr = None;
        let err = serde
            .decode_with_source(&missing_projection_expr, file_source)
            .expect_err("missing projection expression must fail");
        assert!(
            err.to_string()
                .contains("ProjectionExpr missing expr field"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[test]
    fn new_file_scan_config_decode_rejects_invalid_range_ordering() -> Result<()> {
        let serde = FileScanSerdeHarness::new();
        let mut proto = serde.encode(&test_config(Some(range_partitioning())))?;

        let mut duplicate_ordering = proto.clone();
        let range = match duplicate_ordering
            .output_partitioning
            .as_mut()
            .and_then(|p| p.partition_method.as_mut())
        {
            Some(protobuf::partitioning::PartitionMethod::Range(range)) => range,
            other => panic!("expected range partitioning, got {other:?}"),
        };
        range.sort_expr.push(range.sort_expr[0].clone());

        let err = serde
            .decode(&duplicate_ordering)
            .expect_err("duplicate range ordering must fail");
        assert!(
            err.to_string().contains("duplicate expressions"),
            "unexpected error: {err}"
        );

        let range = match proto
            .output_partitioning
            .as_mut()
            .and_then(|p| p.partition_method.as_mut())
        {
            Some(protobuf::partitioning::PartitionMethod::Range(range)) => range,
            other => panic!("expected range partitioning, got {other:?}"),
        };
        range.sort_expr.clear();

        let err = serde
            .decode(&proto)
            .expect_err("empty range ordering must fail");
        assert!(
            err.to_string().contains("requires non-empty ordering"),
            "unexpected error: {err}"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unit tests for the bytes-only function serde exposed on
    /// [`ExecutionPlanEncodeCtx`] / [`ExecutionPlanDecodeCtx`] and backed by
    /// [`ConverterPlanEncoder`] / [`ConverterPlanDecoder`]. Function-carrying
    /// plans migrate in follow-up PRs, so these paths have no in-tree plan
    /// caller yet; the tests pin the payload semantics (`None` == encode by
    /// name) and the decode lookup order (payload → codec; else registry →
    /// codec fallback with an empty buffer) that those migrations rely on.
    mod function_serde {
        use super::*;
        use arrow::datatypes::{DataType, Field, FieldRef};
        use datafusion_common::plan_err;
        use datafusion_execution::config::SessionConfig;
        use datafusion_execution::runtime_env::RuntimeEnv;
        use datafusion_expr::function::AccumulatorArgs;
        use datafusion_expr::{
            Accumulator, AggregateUDFImpl, ColumnarValue, PartitionEvaluator,
            ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility, WindowUDFImpl,
        };
        use datafusion_functions_window_common::field::WindowUDFFieldArgs;
        use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;

        #[derive(Debug, PartialEq, Eq, Hash)]
        struct TestUdf {
            signature: Signature,
        }

        impl TestUdf {
            fn new() -> Self {
                Self {
                    signature: Signature::exact(
                        vec![DataType::Int64],
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl ScalarUDFImpl for TestUdf {
            fn name(&self) -> &str {
                "test_udf"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
                Ok(DataType::Int64)
            }
            fn invoke_with_args(
                &self,
                _args: ScalarFunctionArgs,
            ) -> Result<ColumnarValue> {
                plan_err!("test only")
            }
        }

        #[derive(Debug, PartialEq, Eq, Hash)]
        struct TestUdaf {
            signature: Signature,
        }

        impl TestUdaf {
            fn new() -> Self {
                Self {
                    signature: Signature::exact(
                        vec![DataType::Int64],
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl AggregateUDFImpl for TestUdaf {
            fn name(&self) -> &str {
                "test_udaf"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Int64)
            }
            fn accumulator(
                &self,
                _acc_args: AccumulatorArgs,
            ) -> Result<Box<dyn Accumulator>> {
                plan_err!("test only")
            }
        }

        #[derive(Debug, PartialEq, Eq, Hash)]
        struct TestUdwf {
            signature: Signature,
        }

        impl TestUdwf {
            fn new() -> Self {
                Self {
                    signature: Signature::exact(
                        vec![DataType::Int64],
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl WindowUDFImpl for TestUdwf {
            fn name(&self) -> &str {
                "test_udwf"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn partition_evaluator(
                &self,
                _partition_evaluator_args: PartitionEvaluatorArgs,
            ) -> Result<Box<dyn PartitionEvaluator>> {
                plan_err!("test only")
            }
            fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
                Ok(Field::new(field_args.name(), DataType::Int64, true).into())
            }
        }

        /// Codec that encodes every function as its name bytes and decodes by
        /// checking the payload it receives, so tests can observe exactly what
        /// crosses the bytes-only boundary.
        #[derive(Debug)]
        struct PayloadCodec;

        impl PhysicalExtensionCodec for PayloadCodec {
            fn try_decode(
                &self,
                _buf: &[u8],
                _inputs: &[Arc<dyn ExecutionPlan>],
                _ctx: &TaskContext,
                _proto_converter: &dyn PhysicalProtoConverterExtension,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                internal_err!("not needed for these tests")
            }

            fn try_encode(
                &self,
                _node: Arc<dyn ExecutionPlan>,
                _buf: &mut Vec<u8>,
                _proto_converter: &dyn PhysicalProtoConverterExtension,
            ) -> Result<()> {
                internal_err!("not needed for these tests")
            }

            fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
                buf.extend_from_slice(node.name().as_bytes());
                Ok(())
            }

            fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
                assert_eq!(name, "test_udf");
                assert_eq!(buf, name.as_bytes());
                Ok(Arc::new(ScalarUDF::from(TestUdf::new())))
            }

            fn try_encode_udaf(
                &self,
                node: &AggregateUDF,
                buf: &mut Vec<u8>,
            ) -> Result<()> {
                buf.extend_from_slice(node.name().as_bytes());
                Ok(())
            }

            fn try_decode_udaf(
                &self,
                name: &str,
                buf: &[u8],
            ) -> Result<Arc<AggregateUDF>> {
                assert_eq!(name, "test_udaf");
                assert_eq!(buf, name.as_bytes());
                Ok(Arc::new(AggregateUDF::from(TestUdaf::new())))
            }

            fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
                buf.extend_from_slice(node.name().as_bytes());
                Ok(())
            }

            fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
                assert_eq!(name, "test_udwf");
                assert_eq!(buf, name.as_bytes());
                Ok(Arc::new(WindowUDF::from(TestUdwf::new())))
            }
        }

        /// Codec whose decode hooks only accept an empty payload, to pin the
        /// by-name decode fallback (registry miss → codec with `&[]`).
        #[derive(Debug)]
        struct EmptyPayloadOnlyCodec;

        impl PhysicalExtensionCodec for EmptyPayloadOnlyCodec {
            fn try_decode(
                &self,
                _buf: &[u8],
                _inputs: &[Arc<dyn ExecutionPlan>],
                _ctx: &TaskContext,
                _proto_converter: &dyn PhysicalProtoConverterExtension,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                internal_err!("not needed for these tests")
            }

            fn try_encode(
                &self,
                _node: Arc<dyn ExecutionPlan>,
                _buf: &mut Vec<u8>,
                _proto_converter: &dyn PhysicalProtoConverterExtension,
            ) -> Result<()> {
                internal_err!("not needed for these tests")
            }

            fn try_decode_udf(&self, _name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
                assert!(buf.is_empty());
                Ok(Arc::new(ScalarUDF::from(TestUdf::new())))
            }

            fn try_decode_udaf(
                &self,
                _name: &str,
                buf: &[u8],
            ) -> Result<Arc<AggregateUDF>> {
                assert!(buf.is_empty());
                Ok(Arc::new(AggregateUDF::from(TestUdaf::new())))
            }

            fn try_decode_udwf(&self, _name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
                assert!(buf.is_empty());
                Ok(Arc::new(WindowUDF::from(TestUdwf::new())))
            }
        }

        fn encode_ctx_over<'a>(
            codec: &'a dyn PhysicalExtensionCodec,
            proto_converter: &'a dyn PhysicalProtoConverterExtension,
        ) -> ConverterPlanEncoder<'a> {
            ConverterPlanEncoder {
                codec,
                proto_converter,
            }
        }

        #[test]
        fn encode_by_name_functions_produce_no_payload() -> Result<()> {
            let codec = DefaultPhysicalExtensionCodec {};
            let converter = DefaultPhysicalProtoConverter {};
            let encoder = encode_ctx_over(&codec, &converter);
            let ctx = ExecutionPlanEncodeCtx::new(&encoder);

            assert!(ctx.encode_udf(&ScalarUDF::from(TestUdf::new()))?.is_none());
            assert!(
                ctx.encode_udaf(&AggregateUDF::from(TestUdaf::new()))?
                    .is_none()
            );
            assert!(
                ctx.encode_udwf(&WindowUDF::from(TestUdwf::new()))?
                    .is_none()
            );
            Ok(())
        }

        #[test]
        fn encode_functions_surface_codec_payload() -> Result<()> {
            let codec = PayloadCodec;
            let converter = DefaultPhysicalProtoConverter {};
            let encoder = encode_ctx_over(&codec, &converter);
            let ctx = ExecutionPlanEncodeCtx::new(&encoder);

            assert_eq!(
                ctx.encode_udf(&ScalarUDF::from(TestUdf::new()))?.as_deref(),
                Some(b"test_udf".as_slice())
            );
            assert_eq!(
                ctx.encode_udaf(&AggregateUDF::from(TestUdaf::new()))?
                    .as_deref(),
                Some(b"test_udaf".as_slice())
            );
            assert_eq!(
                ctx.encode_udwf(&WindowUDF::from(TestUdwf::new()))?
                    .as_deref(),
                Some(b"test_udwf".as_slice())
            );
            Ok(())
        }

        #[test]
        fn decode_functions_prefer_explicit_payload() -> Result<()> {
            let task_ctx = TaskContext::default();
            let codec = PayloadCodec;
            let decode_context = PhysicalPlanDecodeContext::new(&task_ctx, &codec);
            let converter = DefaultPhysicalProtoConverter {};
            let decoder = ConverterPlanDecoder {
                ctx: &decode_context,
                proto_converter: &converter,
            };
            let ctx = ExecutionPlanDecodeCtx::new(&decoder);

            assert_eq!(
                ctx.decode_udf("test_udf", Some(b"test_udf"))?.name(),
                "test_udf"
            );
            assert_eq!(
                ctx.decode_udaf("test_udaf", Some(b"test_udaf"))?.name(),
                "test_udaf"
            );
            assert_eq!(
                ctx.decode_udwf("test_udwf", Some(b"test_udwf"))?.name(),
                "test_udwf"
            );
            Ok(())
        }

        #[test]
        fn decode_functions_by_name_resolve_from_registry() -> Result<()> {
            let udf = Arc::new(ScalarUDF::from(TestUdf::new()));
            let udaf = Arc::new(AggregateUDF::from(TestUdaf::new()));
            let udwf = Arc::new(WindowUDF::from(TestUdwf::new()));
            let task_ctx = TaskContext::new(
                None,
                "test".to_string(),
                SessionConfig::new(),
                HashMap::from([("test_udf".to_string(), Arc::clone(&udf))]),
                HashMap::new(),
                HashMap::from([("test_udaf".to_string(), Arc::clone(&udaf))]),
                HashMap::from([("test_udwf".to_string(), Arc::clone(&udwf))]),
                Arc::new(RuntimeEnv::default()),
            );
            // The default codec fails any decode, so a success proves the
            // registry satisfied the lookup without a codec fallback.
            let codec = DefaultPhysicalExtensionCodec {};
            let decode_context = PhysicalPlanDecodeContext::new(&task_ctx, &codec);
            let converter = DefaultPhysicalProtoConverter {};
            let decoder = ConverterPlanDecoder {
                ctx: &decode_context,
                proto_converter: &converter,
            };
            let ctx = ExecutionPlanDecodeCtx::new(&decoder);

            assert!(Arc::ptr_eq(&ctx.decode_udf("test_udf", None)?, &udf));
            assert!(Arc::ptr_eq(&ctx.decode_udaf("test_udaf", None)?, &udaf));
            assert!(Arc::ptr_eq(&ctx.decode_udwf("test_udwf", None)?, &udwf));
            assert_eq!(ctx.task_ctx().session_id(), "test");
            Ok(())
        }

        #[test]
        fn decode_functions_by_name_fall_back_to_codec_on_registry_miss() -> Result<()> {
            let task_ctx = TaskContext::default();
            let codec = EmptyPayloadOnlyCodec;
            let decode_context = PhysicalPlanDecodeContext::new(&task_ctx, &codec);
            let converter = DefaultPhysicalProtoConverter {};
            let decoder = ConverterPlanDecoder {
                ctx: &decode_context,
                proto_converter: &converter,
            };
            let ctx = ExecutionPlanDecodeCtx::new(&decoder);

            assert_eq!(ctx.decode_udf("test_udf", None)?.name(), "test_udf");
            assert_eq!(ctx.decode_udaf("test_udaf", None)?.name(), "test_udaf");
            assert_eq!(ctx.decode_udwf("test_udwf", None)?.name(), "test_udwf");
            Ok(())
        }

        #[test]
        fn decode_required_helpers_error_on_missing_fields() {
            let task_ctx = TaskContext::default();
            let codec = DefaultPhysicalExtensionCodec {};
            let decode_context = PhysicalPlanDecodeContext::new(&task_ctx, &codec);
            let converter = DefaultPhysicalProtoConverter {};
            let decoder = ConverterPlanDecoder {
                ctx: &decode_context,
                proto_converter: &converter,
            };
            let ctx = ExecutionPlanDecodeCtx::new(&decoder);

            let err = ctx
                .decode_required_child(None, "FooExec", "input")
                .unwrap_err();
            assert!(
                err.to_string()
                    .contains("FooExec is missing required field 'input'"),
                "unexpected error: {err}"
            );

            let schema = Schema::empty();
            let err = ctx
                .decode_required_expr(None, &schema, "FooExec", "predicate")
                .unwrap_err();
            assert!(
                err.to_string()
                    .contains("FooExec is missing required field 'predicate'"),
                "unexpected error: {err}"
            );
        }

        #[test]
        fn try_from_proto_rejects_wrong_plan_variant() {
            let task_ctx = TaskContext::default();
            let codec = DefaultPhysicalExtensionCodec {};
            let decode_context = PhysicalPlanDecodeContext::new(&task_ctx, &codec);
            let converter = DefaultPhysicalProtoConverter {};
            let decoder = ConverterPlanDecoder {
                ctx: &decode_context,
                proto_converter: &converter,
            };
            let ctx = ExecutionPlanDecodeCtx::new(&decoder);

            let node = protobuf::PhysicalPlanNode {
                physical_plan_type: None,
            };
            let err = ProjectionExec::try_from_proto(&node, &ctx).unwrap_err();
            assert!(
                err.to_string()
                    .contains("PhysicalPlanNode is not a ProjectionExec"),
                "unexpected error: {err}"
            );
        }
    }
}

/// Context threaded through physical-plan deserialization.
///
/// This bundles the stable per-call inputs for deserialization and the
/// per-scope `ScalarSubqueryResults` handle needed while reconstructing
/// `ScalarSubqueryExpr` nodes inside a `ScalarSubqueryExec` input plan.
#[derive(Clone)]
pub struct PhysicalPlanDecodeContext<'a> {
    task_ctx: &'a TaskContext,
    codec: &'a dyn PhysicalExtensionCodec,
    scalar_subquery_results: Option<ScalarSubqueryResults>,
}

impl<'a> PhysicalPlanDecodeContext<'a> {
    /// Creates a new root decode context.
    pub fn new(task_ctx: &'a TaskContext, codec: &'a dyn PhysicalExtensionCodec) -> Self {
        Self {
            task_ctx,
            codec,
            scalar_subquery_results: None,
        }
    }

    /// Returns the task context used for deserialization.
    pub fn task_ctx(&self) -> &'a TaskContext {
        self.task_ctx
    }

    /// Returns the physical extension codec used for deserialization.
    pub fn codec(&self) -> &'a dyn PhysicalExtensionCodec {
        self.codec
    }

    /// Returns the scalar subquery results container for the current scope, if
    /// one is active.
    pub fn scalar_subquery_results(&self) -> Option<&ScalarSubqueryResults> {
        self.scalar_subquery_results.as_ref()
    }

    /// Returns a child context with a different scalar subquery results
    /// container.
    pub fn with_scalar_subquery_results(
        &self,
        scalar_subquery_results: ScalarSubqueryResults,
    ) -> Self {
        Self {
            task_ctx: self.task_ctx,
            codec: self.codec,
            scalar_subquery_results: Some(scalar_subquery_results),
        }
    }
}

impl AsExecutionPlan for protobuf::PhysicalPlanNode {
    fn try_decode(buf: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        protobuf::PhysicalPlanNode::decode(buf).map_err(|e| {
            internal_datafusion_err!("failed to decode physical plan: {e:?}")
        })
    }

    fn try_encode<B>(&self, buf: &mut B) -> Result<()>
    where
        B: BufMut,
        Self: Sized,
    {
        self.encode(buf).map_err(|e| {
            internal_datafusion_err!("failed to encode physical plan: {e:?}")
        })
    }

    fn try_into_physical_plan(
        &self,
        ctx: &TaskContext,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.try_into_physical_plan_with_converter(
            ctx,
            codec,
            &DefaultPhysicalProtoConverter {},
        )
    }

    fn try_from_physical_plan(
        plan: Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        Self::try_from_physical_plan_with_converter(
            plan,
            codec,
            &DefaultPhysicalProtoConverter {},
        )
    }
}

/// Extension methods on [`protobuf::PhysicalPlanNode`].
///
/// The prost-generated `PhysicalPlanNode` struct lives in
/// `datafusion-proto-models`, which is foreign to this crate, so the orphan
/// rule forbids inherent `impl` blocks here. Instead, all (de)serialization
/// helpers are exposed through this trait. Callers can bring it in scope with
/// `use datafusion_proto::physical_plan::PhysicalPlanNodeExt;`.
///
/// Method bodies live in the default trait implementation. To make the trait
/// usable as if it were inherent (i.e. let bodies access fields on `self`),
/// implementors provide [`PhysicalPlanNodeExt::node`] returning a reference
/// back to the concrete `protobuf::PhysicalPlanNode`. Default method bodies
/// then go through `self.node()` to read fields.
pub trait PhysicalPlanNodeExt: Sized {
    /// Returns a reference to the underlying [`protobuf::PhysicalPlanNode`].
    fn node(&self) -> &protobuf::PhysicalPlanNode;

    fn try_into_physical_plan_with_converter(
        &self,
        ctx: &TaskContext,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let decode_ctx = PhysicalPlanDecodeContext::new(ctx, codec);
        self.try_into_physical_plan_with_context(&decode_ctx, proto_converter)
    }

    fn try_into_physical_plan_with_context(
        &self,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.node().physical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "physical_plan::from_proto() Unsupported physical plan '{:?}'",
                self.node(),
            ))
        })?;
        // Decode context for plans migrated to the `try_from_proto` pattern
        // (#22419). Arms for migrated plans are one-liners delegating to the
        // plan's own crate; un-migrated arms keep their inline bodies.
        let plan_decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&plan_decoder);
        match plan {
            PhysicalPlanType::Explain(_) => {
                ExplainExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::Projection(_) => {
                ProjectionExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::Filter(_) => {
                FilterExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::CsvScan(scan) => {
                self.try_into_csv_scan_physical_plan(scan, ctx, proto_converter)
            }
            PhysicalPlanType::JsonScan(scan) => {
                self.try_into_json_scan_physical_plan(scan, ctx, proto_converter)
            }
            PhysicalPlanType::ParquetScan(scan) => {
                self.try_into_parquet_scan_physical_plan(scan, ctx, proto_converter)
            }
            PhysicalPlanType::AvroScan(scan) => {
                self.try_into_avro_scan_physical_plan(scan, ctx, proto_converter)
            }
            PhysicalPlanType::MemoryScan(scan) => {
                self.try_into_memory_scan_physical_plan(scan, ctx, proto_converter)
            }
            PhysicalPlanType::ArrowScan(scan) => {
                self.try_into_arrow_scan_physical_plan(scan, ctx, proto_converter)
            }
            #[expect(
                deprecated,
                reason = "`CoalesceBatchesExec` remains supported for protobuf compatibility"
            )]
            PhysicalPlanType::CoalesceBatches(_) => {
                CoalesceBatchesExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::Merge(_) => {
                CoalescePartitionsExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::Repartition(_) => {
                RepartitionExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::GlobalLimit(_) => {
                GlobalLimitExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::LocalLimit(_) => {
                LocalLimitExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::Window(_) => {
                WindowAggExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::Aggregate(_) => {
                AggregateExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::HashJoin(_) => {
                HashJoinExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::SymmetricHashJoin(_) => {
                SymmetricHashJoinExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::Union(_) => {
                UnionExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::Interleave(_) => {
                InterleaveExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::CrossJoin(_) => {
                CrossJoinExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::Empty(_) => {
                EmptyExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::PlaceholderRow(_) => {
                PlaceholderRowExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::Sort(_) => {
                SortExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::SortPreservingMerge(_) => {
                SortPreservingMergeExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::Extension(extension) => {
                self.try_into_extension_physical_plan(extension, ctx, proto_converter)
            }
            PhysicalPlanType::NestedLoopJoin(_) => {
                NestedLoopJoinExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::Analyze(_) => {
                AnalyzeExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::JsonSink(_) => {
                JsonSink::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::CsvSink(_) => {
                CsvSink::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::ParquetSink(_) => {
                #[cfg(feature = "parquet")]
                {
                    ParquetSink::try_from_proto(self.node(), &decode_ctx)
                }
                #[cfg(not(feature = "parquet"))]
                not_impl_err!("ParquetSink requires the `parquet` feature")
            }
            PhysicalPlanType::Unnest(_) => {
                UnnestExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::Cooperative(_) => {
                CooperativeExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::GenerateSeries(generate_series) => {
                self.try_into_generate_series_physical_plan(generate_series)
            }
            PhysicalPlanType::SortMergeJoin(_) => {
                SortMergeJoinExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::AsyncFunc(_) => {
                AsyncFuncExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::Buffer(_) => {
                BufferExec::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::ScalarSubquery(_) => {
                ScalarSubqueryExec::try_from_proto(self.node(), &decode_ctx)
            }
        }
    }

    fn try_from_physical_plan_with_converter(
        plan: Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let plan_clone = Arc::clone(&plan);
        let mut plan = plan.as_ref();
        // Resolve the downcast identity first so wrapper plans serialize as
        // their delegate, matching how the `downcast_ref` chain below sees
        // them. Without this a wrapper around a migrated plan would hit the
        // wrapper's default `try_to_proto` (`Ok(None)`) and find no fallback
        // arm for the delegate.
        while let Some(delegate) = plan.downcast_delegate() {
            plan = delegate;
        }

        // Self-serializing plans handle themselves via the `try_to_proto` hook
        // (#22419). `Ok(None)` means "not migrated" and falls through to the
        // central downcast chain below.
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        if let Some(node) = plan.try_to_proto(&encode_ctx)? {
            return Ok(node);
        }

        if let Some(data_source_exec) = plan.downcast_ref::<DataSourceExec>()
            && let Some(node) = protobuf::PhysicalPlanNode::try_from_data_source_exec(
                data_source_exec,
                codec,
                proto_converter,
            )?
        {
            return Ok(node);
        }

        if let Some(exec) = plan.downcast_ref::<LazyMemoryExec>()
            && let Some(node) =
                protobuf::PhysicalPlanNode::try_from_lazy_memory_exec(exec)?
        {
            return Ok(node);
        }

        let mut buf: Vec<u8> = vec![];
        match codec.try_encode(Arc::clone(&plan_clone), &mut buf, proto_converter) {
            Ok(_) => {
                let inputs: Vec<protobuf::PhysicalPlanNode> = plan_clone
                    .children()
                    .into_iter()
                    .cloned()
                    .map(|i| {
                        protobuf::PhysicalPlanNode::try_from_physical_plan_with_converter(
                            i,
                            codec,
                            proto_converter,
                        )
                    })
                    .collect::<Result<_>>()?;

                Ok(protobuf::PhysicalPlanNode {
                    physical_plan_type: Some(PhysicalPlanType::Extension(
                        protobuf::PhysicalExtensionNode { node: buf, inputs },
                    )),
                })
            }
            Err(e) => internal_err!(
                "Unsupported plan and extension codec failed with [{e}]. Plan: {plan_clone:?}"
            ),
        }
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `ExplainExec` deserializes itself via `ExplainExec::try_from_proto`"
    )]
    fn try_into_explain_physical_plan(
        &self,
        _explain: &protobuf::ExplainExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan_decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&plan_decoder);
        ExplainExec::try_from_proto(self.node(), &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `ProjectionExec` deserializes itself via `ProjectionExec::try_from_proto`"
    )]
    fn try_into_projection_physical_plan(
        &self,
        projection: &protobuf::ProjectionExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        // `try_from_proto` takes the enclosing `PhysicalPlanNode`, while this
        // deprecated method is driven by the `ProjectionExecNode` argument.
        // Re-wrap the argument so the decoded plan keeps depending on it rather
        // than on `self`, which a caller may not have kept in sync.
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Projection(Box::new(
                projection.clone(),
            ))),
        };
        ProjectionExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `FilterExec` deserializes itself via `FilterExec::try_from_proto`"
    )]
    fn try_into_filter_physical_plan(
        &self,
        filter: &protobuf::FilterExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Filter(Box::new(filter.clone()))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        FilterExec::try_from_proto(&node, &decode_ctx)
    }

    fn try_into_csv_scan_physical_plan(
        &self,
        scan: &protobuf::CsvScanExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let escape =
            if let Some(protobuf::csv_scan_exec_node::OptionalEscape::Escape(escape)) =
                &scan.optional_escape
            {
                Some(str_to_byte(escape, "escape")?)
            } else {
                None
            };

        let comment = if let Some(
            protobuf::csv_scan_exec_node::OptionalComment::Comment(comment),
        ) = &scan.optional_comment
        {
            Some(str_to_byte(comment, "comment")?)
        } else {
            None
        };

        // Parse table schema with partition columns
        let table_schema =
            parse_table_schema_from_proto(scan.base_conf.as_ref().unwrap())?;

        let csv_options = CsvOptions {
            has_header: Some(scan.has_header),
            delimiter: str_to_byte(&scan.delimiter, "delimiter")?,
            quote: str_to_byte(&scan.quote, "quote")?,
            newlines_in_values: Some(scan.newlines_in_values),
            ..Default::default()
        };
        let source = Arc::new(
            CsvSource::new(table_schema)
                .with_csv_options(csv_options)
                .with_escape(escape)
                .with_comment(comment),
        );

        let conf = FileScanConfigBuilder::from(parse_protobuf_file_scan_config(
            scan.base_conf.as_ref().unwrap(),
            ctx,
            proto_converter,
            source,
        )?)
        .with_file_compression_type(FileCompressionType::UNCOMPRESSED)
        .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    fn try_into_json_scan_physical_plan(
        &self,
        scan: &protobuf::JsonScanExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let base_conf = scan.base_conf.as_ref().unwrap();
        let table_schema = parse_table_schema_from_proto(base_conf)?;
        let scan_conf = parse_protobuf_file_scan_config(
            base_conf,
            ctx,
            proto_converter,
            Arc::new(JsonSource::new(table_schema)),
        )?;
        Ok(DataSourceExec::from_data_source(scan_conf))
    }

    fn try_into_arrow_scan_physical_plan(
        &self,
        scan: &protobuf::ArrowScanExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let base_conf = scan.base_conf.as_ref().ok_or_else(|| {
            internal_datafusion_err!("base_conf in ArrowScanExecNode is missing.")
        })?;
        let table_schema = parse_table_schema_from_proto(base_conf)?;
        let scan_conf = parse_protobuf_file_scan_config(
            base_conf,
            ctx,
            proto_converter,
            Arc::new(ArrowSource::new_file_source(table_schema)),
        )?;
        Ok(DataSourceExec::from_data_source(scan_conf))
    }

    #[cfg_attr(not(feature = "parquet"), expect(unused_variables))]
    fn try_into_parquet_scan_physical_plan(
        &self,
        scan: &protobuf::ParquetScanExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        #[cfg(feature = "parquet")]
        {
            let schema = from_proto::parse_protobuf_file_scan_schema(
                scan.base_conf.as_ref().unwrap(),
            )?;

            // Check if there's a projection and use projected schema for predicate parsing
            let base_conf = scan.base_conf.as_ref().unwrap();
            let predicate_schema = if !base_conf.projection.is_empty() {
                // Create projected schema for parsing the predicate
                let projected_fields: Vec<_> = base_conf
                    .projection
                    .iter()
                    .map(|&i| schema.field(i as usize).clone())
                    .collect();
                Arc::new(Schema::new(projected_fields))
            } else {
                schema
            };

            let predicate = scan
                .predicate
                .as_ref()
                .map(|expr| {
                    proto_converter.proto_to_physical_expr(
                        expr,
                        predicate_schema.as_ref(),
                        ctx,
                    )
                })
                .transpose()?;
            let mut options = datafusion_common::config::TableParquetOptions::default();

            if let Some(table_options) = scan.parquet_options.as_ref() {
                options = table_options.try_into()?;
            }

            // Parse table schema with partition columns
            let table_schema = parse_table_schema_from_proto(base_conf)?;
            let object_store_url = match base_conf.object_store_url.is_empty() {
                false => ObjectStoreUrl::parse(&base_conf.object_store_url)?,
                true => ObjectStoreUrl::local_filesystem(),
            };
            let store = ctx
                .task_ctx()
                .runtime_env()
                .object_store(object_store_url)?;
            let metadata_cache = ctx
                .task_ctx()
                .runtime_env()
                .cache_manager
                .get_file_metadata_cache();
            let reader_factory =
                Arc::new(CachedParquetFileReaderFactory::new(store, metadata_cache));

            let mut source = ParquetSource::new(table_schema)
                .with_parquet_file_reader_factory(reader_factory)
                .with_table_parquet_options(options);

            if let Some(predicate) = predicate {
                source = source.with_predicate(predicate);
            }
            let base_config = parse_protobuf_file_scan_config(
                base_conf,
                ctx,
                proto_converter,
                Arc::new(source),
            )?;
            Ok(DataSourceExec::from_data_source(base_config))
        }
        #[cfg(not(feature = "parquet"))]
        panic!(
            "Unable to process a Parquet PhysicalPlan when `parquet` feature is not enabled"
        )
    }

    #[cfg_attr(not(feature = "avro"), expect(unused_variables))]
    fn try_into_avro_scan_physical_plan(
        &self,
        scan: &protobuf::AvroScanExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        #[cfg(feature = "avro")]
        {
            let table_schema =
                parse_table_schema_from_proto(scan.base_conf.as_ref().unwrap())?;
            let conf = parse_protobuf_file_scan_config(
                scan.base_conf.as_ref().unwrap(),
                ctx,
                proto_converter,
                Arc::new(AvroSource::new(table_schema)),
            )?;
            Ok(DataSourceExec::from_data_source(conf))
        }

        #[cfg(not(feature = "avro"))]
        panic!("Unable to process a Avro PhysicalPlan when `avro` feature is not enabled")
    }

    fn try_into_memory_scan_physical_plan(
        &self,
        scan: &protobuf::MemoryScanExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let partitions = scan
            .partitions
            .iter()
            .map(|p| parse_record_batches(p))
            .collect::<Result<Vec<_>>>()?;

        let proto_schema = scan.schema.as_ref().ok_or_else(|| {
            internal_datafusion_err!("schema in MemoryScanExecNode is missing.")
        })?;
        let schema: SchemaRef = SchemaRef::new(proto_schema.try_into()?);

        // Preserve the empty-projection sentinel written by `try_from_data_source_exec`.
        let projection = match scan.projection.as_slice() {
            [] => None,
            [u32::MAX] => Some(Vec::new()),
            indices => Some(indices.iter().map(|i| *i as usize).collect()),
        };

        let mut sort_information = vec![];
        for ordering in &scan.sort_information {
            let sort_exprs = parse_physical_sort_exprs(
                &ordering.physical_sort_expr_nodes,
                ctx,
                &schema,
                proto_converter,
            )?;
            sort_information.extend(LexOrdering::new(sort_exprs));
        }

        let source = MemorySourceConfig::try_new(&partitions, schema, projection)?
            .with_limit(scan.fetch.map(|f| f as usize))
            .with_show_sizes(scan.show_sizes);

        let source = source.try_with_sort_information(sort_information)?;

        Ok(DataSourceExec::from_data_source(source))
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `CoalesceBatchesExec` deserializes itself via `CoalesceBatchesExec::try_from_proto`"
    )]
    fn try_into_coalesce_batches_physical_plan(
        &self,
        coalesce_batches: &protobuf::CoalesceBatchesExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::CoalesceBatches(Box::new(
                coalesce_batches.clone(),
            ))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        #[expect(
            deprecated,
            reason = "`CoalesceBatchesExec` remains supported for protobuf compatibility"
        )]
        CoalesceBatchesExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `CoalescePartitionsExec` deserializes itself via `CoalescePartitionsExec::try_from_proto`"
    )]
    fn try_into_merge_physical_plan(
        &self,
        merge: &protobuf::CoalescePartitionsExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Merge(Box::new(merge.clone()))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        CoalescePartitionsExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `RepartitionExec` deserializes itself via `RepartitionExec::try_from_proto`"
    )]
    fn try_into_repartition_physical_plan(
        &self,
        repart: &protobuf::RepartitionExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Repartition(Box::new(
                repart.clone(),
            ))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        RepartitionExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `GlobalLimitExec` deserializes itself via `GlobalLimitExec::try_from_proto`"
    )]
    fn try_into_global_limit_physical_plan(
        &self,
        limit: &protobuf::GlobalLimitExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::GlobalLimit(Box::new(
                limit.clone(),
            ))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        GlobalLimitExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `LocalLimitExec` deserializes itself via `LocalLimitExec::try_from_proto`"
    )]
    fn try_into_local_limit_physical_plan(
        &self,
        limit: &protobuf::LocalLimitExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::LocalLimit(Box::new(
                limit.clone(),
            ))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        LocalLimitExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; window plans deserialize via `WindowAggExec::try_from_proto`"
    )]
    fn try_into_window_physical_plan(
        &self,
        window_agg: &protobuf::WindowAggExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Window(Box::new(
                window_agg.clone(),
            ))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        WindowAggExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `AggregateExec` deserializes itself via `AggregateExec::try_from_proto`"
    )]
    fn try_into_aggregate_physical_plan(
        &self,
        hash_agg: &protobuf::AggregateExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Aggregate(Box::new(
                hash_agg.clone(),
            ))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        AggregateExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `HashJoinExec` deserializes itself via `HashJoinExec::try_from_proto`"
    )]
    fn try_into_hash_join_physical_plan(
        &self,
        hashjoin: &protobuf::HashJoinExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::HashJoin(Box::new(
                hashjoin.clone(),
            ))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        HashJoinExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `SymmetricHashJoinExec` deserializes itself via `SymmetricHashJoinExec::try_from_proto`"
    )]
    fn try_into_symmetric_hash_join_physical_plan(
        &self,
        sym_join: &protobuf::SymmetricHashJoinExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::SymmetricHashJoin(Box::new(
                sym_join.clone(),
            ))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        SymmetricHashJoinExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `UnionExec` deserializes itself via `UnionExec::try_from_proto`"
    )]
    fn try_into_union_physical_plan(
        &self,
        union: &protobuf::UnionExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Union(union.clone())),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        UnionExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `InterleaveExec` deserializes itself via `InterleaveExec::try_from_proto`"
    )]
    fn try_into_interleave_physical_plan(
        &self,
        interleave: &protobuf::InterleaveExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Interleave(interleave.clone())),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        InterleaveExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `CrossJoinExec` deserializes itself via `CrossJoinExec::try_from_proto`"
    )]
    fn try_into_cross_join_physical_plan(
        &self,
        crossjoin: &protobuf::CrossJoinExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::CrossJoin(Box::new(
                crossjoin.clone(),
            ))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        CrossJoinExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `EmptyExec` deserializes itself via `EmptyExec::try_from_proto`"
    )]
    fn try_into_empty_physical_plan(
        &self,
        empty: &protobuf::EmptyExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Empty(empty.clone())),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        EmptyExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `PlaceholderRowExec` deserializes itself via `PlaceholderRowExec::try_from_proto`"
    )]
    fn try_into_placeholder_row_physical_plan(
        &self,
        placeholder: &protobuf::PlaceholderRowExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::PlaceholderRow(
                placeholder.clone(),
            )),
        };
        let proto_converter = DefaultPhysicalProtoConverter {};
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter: &proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        PlaceholderRowExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `SortExec` deserializes itself via `SortExec::try_from_proto`"
    )]
    fn try_into_sort_physical_plan(
        &self,
        sort: &protobuf::SortExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Sort(Box::new(sort.clone()))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        SortExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `SortPreservingMergeExec` deserializes itself via `SortPreservingMergeExec::try_from_proto`"
    )]
    fn try_into_sort_preserving_merge_physical_plan(
        &self,
        sort: &protobuf::SortPreservingMergeExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::SortPreservingMerge(Box::new(
                sort.clone(),
            ))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        SortPreservingMergeExec::try_from_proto(&node, &decode_ctx)
    }

    fn try_into_extension_physical_plan(
        &self,
        extension: &protobuf::PhysicalExtensionNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inputs: Vec<Arc<dyn ExecutionPlan>> = extension
            .inputs
            .iter()
            .map(|i| proto_converter.proto_to_execution_plan(i, ctx))
            .collect::<Result<_>>()?;

        let extension_node = ctx.codec().try_decode(
            extension.node.as_slice(),
            &inputs,
            ctx.task_ctx(),
            proto_converter,
        )?;

        Ok(extension_node)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `NestedLoopJoinExec` deserializes itself via `NestedLoopJoinExec::try_from_proto`"
    )]
    fn try_into_nested_loop_join_physical_plan(
        &self,
        join: &protobuf::NestedLoopJoinExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::NestedLoopJoin(Box::new(
                join.clone(),
            ))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        NestedLoopJoinExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `AnalyzeExec` deserializes itself via `AnalyzeExec::try_from_proto`"
    )]
    fn try_into_analyze_physical_plan(
        &self,
        _analyze: &protobuf::AnalyzeExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan_decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&plan_decoder);
        AnalyzeExec::try_from_proto(self.node(), &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `JsonSink` deserializes itself via `JsonSink::try_from_proto`"
    )]
    fn try_into_json_sink_physical_plan(
        &self,
        sink: &protobuf::JsonSinkExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::JsonSink(Box::new(sink.clone()))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        JsonSink::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `CsvSink` deserializes itself via `CsvSink::try_from_proto`"
    )]
    fn try_into_csv_sink_physical_plan(
        &self,
        sink: &protobuf::CsvSinkExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::CsvSink(Box::new(sink.clone()))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        CsvSink::try_from_proto(&node, &decode_ctx)
    }

    #[cfg_attr(not(feature = "parquet"), expect(unused_variables))]
    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `ParquetSink` deserializes itself via `ParquetSink::try_from_proto`"
    )]
    fn try_into_parquet_sink_physical_plan(
        &self,
        sink: &protobuf::ParquetSinkExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        #[cfg(feature = "parquet")]
        {
            let node = protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ParquetSink(Box::new(
                    sink.clone(),
                ))),
            };
            let decoder = ConverterPlanDecoder {
                ctx,
                proto_converter,
            };
            let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
            ParquetSink::try_from_proto(&node, &decode_ctx)
        }
        #[cfg(not(feature = "parquet"))]
        not_impl_err!("ParquetSink requires the `parquet` feature")
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `UnnestExec` deserializes itself via `UnnestExec::try_from_proto`"
    )]
    fn try_into_unnest_physical_plan(
        &self,
        unnest: &protobuf::UnnestExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Unnest(Box::new(unnest.clone()))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        UnnestExec::try_from_proto(&node, &decode_ctx)
    }

    fn generate_series_name_to_str(name: protobuf::GenerateSeriesName) -> &'static str {
        match name {
            protobuf::GenerateSeriesName::GsGenerateSeries => "generate_series",
            protobuf::GenerateSeriesName::GsRange => "range",
        }
    }
    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `SortMergeJoinExec` deserializes itself via `SortMergeJoinExec::try_from_proto`"
    )]
    fn try_into_sort_join(
        &self,
        sort_join: &SortMergeJoinExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::SortMergeJoin(Box::new(
                sort_join.clone(),
            ))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        SortMergeJoinExec::try_from_proto(&node, &decode_ctx)
    }

    fn try_into_generate_series_physical_plan(
        &self,
        generate_series: &protobuf::GenerateSeriesNode,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema: SchemaRef = Arc::new(convert_required!(generate_series.schema)?);

        let args = match &generate_series.args {
            Some(protobuf::generate_series_node::Args::ContainsNull(args)) => {
                GenSeriesArgs::ContainsNull {
                    name: protobuf::PhysicalPlanNode::generate_series_name_to_str(
                        args.name(),
                    ),
                }
            }
            Some(protobuf::generate_series_node::Args::Int64Args(args)) => {
                GenSeriesArgs::Int64Args {
                    start: args.start,
                    end: args.end,
                    step: args.step,
                    include_end: args.include_end,
                    name: protobuf::PhysicalPlanNode::generate_series_name_to_str(
                        args.name(),
                    ),
                }
            }
            Some(protobuf::generate_series_node::Args::TimestampArgs(args)) => {
                let step_proto = args.step.as_ref().ok_or_else(|| {
                    internal_datafusion_err!("Missing step in TimestampArgs")
                })?;
                let step = IntervalMonthDayNanoType::make_value(
                    step_proto.months,
                    step_proto.days,
                    step_proto.nanos,
                );
                GenSeriesArgs::TimestampArgs {
                    start: args.start,
                    end: args.end,
                    step,
                    tz: args.tz.as_ref().map(|s| Arc::from(s.as_str())),
                    include_end: args.include_end,
                    name: protobuf::PhysicalPlanNode::generate_series_name_to_str(
                        args.name(),
                    ),
                }
            }
            Some(protobuf::generate_series_node::Args::DateArgs(args)) => {
                let step_proto = args.step.as_ref().ok_or_else(|| {
                    internal_datafusion_err!("Missing step in DateArgs")
                })?;
                let step = IntervalMonthDayNanoType::make_value(
                    step_proto.months,
                    step_proto.days,
                    step_proto.nanos,
                );
                GenSeriesArgs::DateArgs {
                    start: args.start,
                    end: args.end,
                    step,
                    include_end: args.include_end,
                    name: protobuf::PhysicalPlanNode::generate_series_name_to_str(
                        args.name(),
                    ),
                }
            }
            None => return internal_err!("Missing args in GenerateSeriesNode"),
        };

        let table = GenerateSeriesTable::new(Arc::clone(&schema), args);
        let generator = table.as_generator(generate_series.target_batch_size as usize)?;

        Ok(Arc::new(LazyMemoryExec::try_new(schema, vec![generator])?))
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `CooperativeExec` deserializes itself via `CooperativeExec::try_from_proto`"
    )]
    fn try_into_cooperative_physical_plan(
        &self,
        field_stream: &protobuf::CooperativeExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Cooperative(Box::new(
                field_stream.clone(),
            ))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        CooperativeExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `AsyncFuncExec` deserializes itself via `AsyncFuncExec::try_from_proto`"
    )]
    fn try_into_async_func_physical_plan(
        &self,
        async_func: &protobuf::AsyncFuncExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::AsyncFunc(Box::new(
                async_func.clone(),
            ))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        AsyncFuncExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `BufferExec` deserializes itself via `BufferExec::try_from_proto`"
    )]
    fn try_into_buffer_physical_plan(
        &self,
        buffer: &protobuf::BufferExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Buffer(Box::new(buffer.clone()))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        BufferExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `ScalarSubqueryExec` deserializes itself via `ScalarSubqueryExec::try_from_proto`"
    )]
    fn try_into_scalar_subquery_physical_plan(
        &self,
        sq: &protobuf::ScalarSubqueryExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::ScalarSubquery(Box::new(
                sq.clone(),
            ))),
        };
        let decoder = ConverterPlanDecoder {
            ctx,
            proto_converter,
        };
        let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);
        ScalarSubqueryExec::try_from_proto(&node, &decode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `ExplainExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_explain_exec(
        exec: &ExplainExec,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let proto_converter = DefaultPhysicalProtoConverter {};
        let plan_encoder = ConverterPlanEncoder {
            codec,
            proto_converter: &proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&plan_encoder);
        exec.try_to_proto(&encode_ctx)?.ok_or_else(|| {
            internal_datafusion_err!("ExplainExec did not serialize itself")
        })
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `ProjectionExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_projection_exec(
        exec: &ProjectionExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&ctx)?.ok_or_else(|| {
            internal_datafusion_err!("ProjectionExec::try_to_proto returned None")
        })
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `AnalyzeExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_analyze_exec(
        exec: &AnalyzeExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let plan_encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&plan_encoder);
        exec.try_to_proto(&encode_ctx)?.ok_or_else(|| {
            internal_datafusion_err!("AnalyzeExec did not serialize itself")
        })
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `FilterExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_filter_exec(
        exec: &FilterExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?
            .ok_or_else(|| internal_datafusion_err!("FilterExec is not serializable"))
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `GlobalLimitExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_global_limit_exec(
        limit: &GlobalLimitExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        limit.try_to_proto(&encode_ctx)?.ok_or_else(|| {
            internal_datafusion_err!("GlobalLimitExec is not serializable")
        })
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `LocalLimitExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_local_limit_exec(
        limit: &LocalLimitExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        limit
            .try_to_proto(&encode_ctx)?
            .ok_or_else(|| internal_datafusion_err!("LocalLimitExec is not serializable"))
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `HashJoinExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_hash_join_exec(
        exec: &HashJoinExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?
            .ok_or_else(|| internal_datafusion_err!("HashJoinExec is not serializable"))
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `SymmetricHashJoinExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_symmetric_hash_join_exec(
        exec: &SymmetricHashJoinExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?.ok_or_else(|| {
            internal_datafusion_err!("SymmetricHashJoinExec is not serializable")
        })
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `SortMergeJoinExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_sort_merge_join_exec(
        exec: &SortMergeJoinExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?.ok_or_else(|| {
            internal_datafusion_err!("SortMergeJoinExec is not serializable")
        })
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `CrossJoinExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_cross_join_exec(
        exec: &CrossJoinExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?
            .ok_or_else(|| internal_datafusion_err!("CrossJoinExec is not serializable"))
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `AggregateExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_aggregate_exec(
        exec: &AggregateExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?
            .ok_or_else(|| internal_datafusion_err!("AggregateExec is not serializable"))
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `EmptyExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_empty_exec(
        empty: &EmptyExec,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let proto_converter = DefaultPhysicalProtoConverter {};
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter: &proto_converter,
        };
        let ctx = ExecutionPlanEncodeCtx::new(&encoder);
        empty.try_to_proto(&ctx)?.ok_or_else(|| {
            internal_datafusion_err!("EmptyExec::try_to_proto returned None")
        })
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `PlaceholderRowExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_placeholder_row_exec(
        placeholder: &PlaceholderRowExec,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let proto_converter = DefaultPhysicalProtoConverter {};
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter: &proto_converter,
        };
        let ctx = ExecutionPlanEncodeCtx::new(&encoder);
        placeholder.try_to_proto(&ctx)?.ok_or_else(|| {
            internal_datafusion_err!("PlaceholderRowExec::try_to_proto returned None")
        })
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `CoalesceBatchesExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    #[expect(
        deprecated,
        reason = "`CoalesceBatchesExec` remains supported for protobuf compatibility"
    )]
    fn try_from_coalesce_batches_exec(
        coalesce_batches: &CoalesceBatchesExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        coalesce_batches.try_to_proto(&encode_ctx)?.ok_or_else(|| {
            internal_datafusion_err!("CoalesceBatchesExec is not serializable")
        })
    }

    fn try_from_data_source_exec(
        data_source_exec: &DataSourceExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Option<protobuf::PhysicalPlanNode>> {
        let data_source = data_source_exec.data_source();
        if let Some(maybe_csv) = data_source.downcast_ref::<FileScanConfig>() {
            let source = maybe_csv.file_source();
            if let Some(csv_config) = source.downcast_ref::<CsvSource>() {
                return Ok(Some(protobuf::PhysicalPlanNode {
                    physical_plan_type: Some(PhysicalPlanType::CsvScan(
                        protobuf::CsvScanExecNode {
                            base_conf: Some(serialize_file_scan_config(
                                maybe_csv,
                                codec,
                                proto_converter,
                            )?),
                            has_header: csv_config.has_header(),
                            delimiter: byte_to_string(
                                csv_config.delimiter(),
                                "delimiter",
                            )?,
                            quote: byte_to_string(csv_config.quote(), "quote")?,
                            optional_escape: if let Some(escape) = csv_config.escape() {
                                Some(
                                    protobuf::csv_scan_exec_node::OptionalEscape::Escape(
                                        byte_to_string(escape, "escape")?,
                                    ),
                                )
                            } else {
                                None
                            },
                            optional_comment: if let Some(comment) = csv_config.comment()
                            {
                                Some(protobuf::csv_scan_exec_node::OptionalComment::Comment(
                                        byte_to_string(comment, "comment")?,
                                    ))
                            } else {
                                None
                            },
                            newlines_in_values: csv_config.newlines_in_values(),
                            truncate_rows: csv_config.truncate_rows(),
                        },
                    )),
                }));
            }
        }

        if let Some(scan_conf) = data_source.downcast_ref::<FileScanConfig>() {
            let source = scan_conf.file_source();
            if let Some(_json_source) = source.downcast_ref::<JsonSource>() {
                return Ok(Some(protobuf::PhysicalPlanNode {
                    physical_plan_type: Some(PhysicalPlanType::JsonScan(
                        protobuf::JsonScanExecNode {
                            base_conf: Some(serialize_file_scan_config(
                                scan_conf,
                                codec,
                                proto_converter,
                            )?),
                        },
                    )),
                }));
            }
        }

        if let Some(scan_conf) = data_source.downcast_ref::<FileScanConfig>() {
            let source = scan_conf.file_source();
            if let Some(_arrow_source) = source.downcast_ref::<ArrowSource>() {
                return Ok(Some(protobuf::PhysicalPlanNode {
                    physical_plan_type: Some(PhysicalPlanType::ArrowScan(
                        protobuf::ArrowScanExecNode {
                            base_conf: Some(serialize_file_scan_config(
                                scan_conf,
                                codec,
                                proto_converter,
                            )?),
                        },
                    )),
                }));
            }
        }

        #[cfg(feature = "parquet")]
        if let Some((maybe_parquet, conf)) =
            data_source_exec.downcast_to_file_source::<ParquetSource>()
        {
            let predicate = conf
                .filter()
                .map(|pred| proto_converter.physical_expr_to_proto(&pred, codec))
                .transpose()?;
            return Ok(Some(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ParquetScan(
                    protobuf::ParquetScanExecNode {
                        base_conf: Some(serialize_file_scan_config(
                            maybe_parquet,
                            codec,
                            proto_converter,
                        )?),
                        predicate,
                        parquet_options: Some(conf.table_parquet_options().try_into()?),
                    },
                )),
            }));
        }

        #[cfg(feature = "avro")]
        if let Some(maybe_avro) = data_source.downcast_ref::<FileScanConfig>() {
            let source = maybe_avro.file_source();
            if source.downcast_ref::<AvroSource>().is_some() {
                return Ok(Some(protobuf::PhysicalPlanNode {
                    physical_plan_type: Some(PhysicalPlanType::AvroScan(
                        protobuf::AvroScanExecNode {
                            base_conf: Some(serialize_file_scan_config(
                                maybe_avro,
                                codec,
                                proto_converter,
                            )?),
                        },
                    )),
                }));
            }
        }

        if let Some(source_conf) = data_source.downcast_ref::<MemorySourceConfig>() {
            let proto_partitions = source_conf
                .partitions()
                .iter()
                .map(|p| serialize_record_batches(p))
                .collect::<Result<Vec<_>>>()?;

            let proto_schema: protobuf::Schema =
                source_conf.original_schema().as_ref().try_into()?;

            // Proto3 can't tell `None` from `Some(vec![])`; encode the latter
            // as the `[u32::MAX]` sentinel, matching the join/filter nodes.
            let proto_projection = match source_conf.projection().as_ref() {
                None => Vec::new(),
                Some(v) if v.is_empty() => vec![u32::MAX],
                Some(v) => v.iter().map(|x| *x as u32).collect(),
            };

            let proto_sort_information = source_conf
                .sort_information()
                .iter()
                .map(|ordering| {
                    let sort_exprs = serialize_physical_sort_exprs(
                        ordering.to_owned(),
                        codec,
                        proto_converter,
                    )?;
                    Ok::<_, DataFusionError>(protobuf::PhysicalSortExprNodeCollection {
                        physical_sort_expr_nodes: sort_exprs,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            return Ok(Some(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::MemoryScan(
                    protobuf::MemoryScanExecNode {
                        partitions: proto_partitions,
                        schema: Some(proto_schema),
                        projection: proto_projection,
                        sort_information: proto_sort_information,
                        show_sizes: source_conf.show_sizes(),
                        fetch: source_conf.fetch().map(|f| f as u32),
                    },
                )),
            }));
        }

        Ok(None)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `CoalescePartitionsExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_coalesce_partitions_exec(
        exec: &CoalescePartitionsExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?.ok_or_else(|| {
            internal_datafusion_err!("CoalescePartitionsExec is not serializable")
        })
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `RepartitionExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_repartition_exec(
        exec: &RepartitionExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?.ok_or_else(|| {
            internal_datafusion_err!("RepartitionExec is not serializable")
        })
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `SortExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_sort_exec(
        exec: &SortExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?
            .ok_or_else(|| internal_datafusion_err!("SortExec is not serializable"))
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `UnionExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_union_exec(
        union: &UnionExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        union
            .try_to_proto(&encode_ctx)?
            .ok_or_else(|| internal_datafusion_err!("UnionExec is not serializable"))
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `InterleaveExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_interleave_exec(
        interleave: &InterleaveExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        interleave
            .try_to_proto(&encode_ctx)?
            .ok_or_else(|| internal_datafusion_err!("InterleaveExec is not serializable"))
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `SortPreservingMergeExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_sort_preserving_merge_exec(
        exec: &SortPreservingMergeExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?.ok_or_else(|| {
            internal_datafusion_err!("SortPreservingMergeExec is not serializable")
        })
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `NestedLoopJoinExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_nested_loop_join_exec(
        exec: &NestedLoopJoinExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?.ok_or_else(|| {
            internal_datafusion_err!("NestedLoopJoinExec is not serializable")
        })
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `WindowAggExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_window_agg_exec(
        exec: &WindowAggExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?
            .ok_or_else(|| internal_datafusion_err!("WindowAggExec is not serializable"))
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `BoundedWindowAggExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_bounded_window_agg_exec(
        exec: &BoundedWindowAggExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?.ok_or_else(|| {
            internal_datafusion_err!("BoundedWindowAggExec is not serializable")
        })
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `DataSinkExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_data_sink_exec(
        exec: &DataSinkExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Option<protobuf::PhysicalPlanNode>> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `UnnestExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_unnest_exec(
        exec: &UnnestExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?
            .ok_or_else(|| internal_datafusion_err!("UnnestExec is not serializable"))
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `CooperativeExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_cooperative_exec(
        exec: &CooperativeExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?.ok_or_else(|| {
            internal_datafusion_err!("CooperativeExec is not serializable")
        })
    }

    fn str_to_generate_series_name(name: &str) -> Result<protobuf::GenerateSeriesName> {
        match name {
            "generate_series" => Ok(protobuf::GenerateSeriesName::GsGenerateSeries),
            "range" => Ok(protobuf::GenerateSeriesName::GsRange),
            _ => internal_err!("unknown name: {name}"),
        }
    }

    fn try_from_lazy_memory_exec(
        exec: &LazyMemoryExec,
    ) -> Result<Option<protobuf::PhysicalPlanNode>> {
        let generators = exec.generators();

        // ensure we only have one generator
        let [generator] = generators.as_slice() else {
            return Ok(None);
        };

        let generator_guard = generator.read();

        // Try to downcast to different generate_series types
        if let Some(empty_gen) = generator_guard.as_any().downcast_ref::<Empty>() {
            let schema = exec.schema();
            let node = protobuf::GenerateSeriesNode {
                schema: Some(schema.as_ref().try_into()?),
                target_batch_size: 8192, // Default batch size
                args: Some(protobuf::generate_series_node::Args::ContainsNull(
                    protobuf::GenerateSeriesArgsContainsNull {
                        name: protobuf::PhysicalPlanNode::str_to_generate_series_name(
                            empty_gen.name(),
                        )? as i32,
                    },
                )),
            };

            return Ok(Some(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::GenerateSeries(node)),
            }));
        }

        if let Some(int_64) = generator_guard
            .as_any()
            .downcast_ref::<GenericSeriesState<i64>>()
        {
            let schema = exec.schema();
            let node = protobuf::GenerateSeriesNode {
                schema: Some(schema.as_ref().try_into()?),
                target_batch_size: int_64.batch_size() as u32,
                args: Some(protobuf::generate_series_node::Args::Int64Args(
                    protobuf::GenerateSeriesArgsInt64 {
                        start: *int_64.start(),
                        end: *int_64.end(),
                        step: *int_64.step(),
                        include_end: int_64.include_end(),
                        name: protobuf::PhysicalPlanNode::str_to_generate_series_name(
                            int_64.name(),
                        )? as i32,
                    },
                )),
            };

            return Ok(Some(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::GenerateSeries(node)),
            }));
        }

        if let Some(timestamp_args) = generator_guard
            .as_any()
            .downcast_ref::<GenericSeriesState<TimestampValue>>()
        {
            let schema = exec.schema();

            let start = timestamp_args.start().value();
            let end = timestamp_args.end().value();

            let step_value = timestamp_args.step();

            let step = Some(datafusion_proto_common::IntervalMonthDayNanoValue {
                months: step_value.months,
                days: step_value.days,
                nanos: step_value.nanoseconds,
            });
            let include_end = timestamp_args.include_end();
            let name = protobuf::PhysicalPlanNode::str_to_generate_series_name(
                timestamp_args.name(),
            )? as i32;

            let args = match timestamp_args.current().tz_str() {
                Some(tz) => protobuf::generate_series_node::Args::TimestampArgs(
                    protobuf::GenerateSeriesArgsTimestamp {
                        start,
                        end,
                        step,
                        include_end,
                        name,
                        tz: Some(tz.to_string()),
                    },
                ),
                None => protobuf::generate_series_node::Args::DateArgs(
                    protobuf::GenerateSeriesArgsDate {
                        start,
                        end,
                        step,
                        include_end,
                        name,
                    },
                ),
            };

            let node = protobuf::GenerateSeriesNode {
                schema: Some(schema.as_ref().try_into()?),
                target_batch_size: timestamp_args.batch_size() as u32,
                args: Some(args),
            };

            return Ok(Some(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::GenerateSeries(node)),
            }));
        }

        Ok(None)
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `AsyncFuncExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_async_func_exec(
        exec: &AsyncFuncExec,
        extension_codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec: extension_codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?
            .ok_or_else(|| internal_datafusion_err!("AsyncFuncExec is not serializable"))
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `BufferExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_buffer_exec(
        exec: &BufferExec,
        extension_codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec: extension_codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?
            .ok_or_else(|| internal_datafusion_err!("BufferExec is not serializable"))
    }

    #[deprecated(
        since = "55.0.0",
        note = "unused by DataFusion; `ScalarSubqueryExec` serializes itself via `ExecutionPlan::try_to_proto`"
    )]
    fn try_from_scalar_subquery_exec(
        exec: &ScalarSubqueryExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<protobuf::PhysicalPlanNode> {
        let encoder = ConverterPlanEncoder {
            codec,
            proto_converter,
        };
        let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
        exec.try_to_proto(&encode_ctx)?.ok_or_else(|| {
            internal_datafusion_err!("ScalarSubqueryExec is not serializable")
        })
    }
}

impl PhysicalPlanNodeExt for protobuf::PhysicalPlanNode {
    fn node(&self) -> &protobuf::PhysicalPlanNode {
        self
    }
}

pub trait AsExecutionPlan: Debug + Send + Sync + Clone {
    fn try_decode(buf: &[u8]) -> Result<Self>
    where
        Self: Sized;

    fn try_encode<B>(&self, buf: &mut B) -> Result<()>
    where
        B: BufMut,
        Self: Sized;

    fn try_into_physical_plan(
        &self,
        ctx: &TaskContext,

        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn try_from_physical_plan(
        plan: Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Self>
    where
        Self: Sized;
}

pub trait PhysicalExtensionCodec: Debug + Send + Sync + Any {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()>;

    fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        not_impl_err!("PhysicalExtensionCodec is not provided for scalar function {name}")
    }

    fn try_encode_udf(&self, _node: &ScalarUDF, _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn try_decode_higher_order_function(
        &self,
        name: &str,
        _buf: &[u8],
    ) -> Result<Arc<HigherOrderUDF>> {
        not_impl_err!(
            "PhysicalExtensionCodec is not provided for higher order function {name}"
        )
    }

    fn try_encode_higher_order_function(
        &self,
        _node: &HigherOrderUDF,
        _buf: &mut Vec<u8>,
    ) -> Result<()> {
        Ok(())
    }

    /// Decode a custom extension expression from `buf`.
    ///
    /// `inputs` holds the already-decoded children carried in the
    /// `PhysicalExtensionExprNode.inputs` field. If the codec instead embeds
    /// nested `PhysicalExprNode`s *inside* `buf`, decode them through
    /// `ctx.decode(..)` (equivalently [`PhysicalExprDecodeCtx::decode`]) rather
    /// than the free [`parse_physical_expr`] function: `ctx` carries the active
    /// schema and task context (so UDF/column references resolve against the
    /// real registry) and routes through any active `DeduplicatingDeserializer`,
    /// so a shared inner expression (e.g. a `DynamicFilterPhysicalExpr`
    /// referenced both from a `SortExec.filter` and from inside this blob)
    /// cache-hits on its `expr_id` and re-shares one `Arc<dyn PhysicalExpr>`.
    ///
    /// [`parse_physical_expr`]: crate::physical_plan::from_proto::parse_physical_expr
    fn try_decode_expr(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn PhysicalExpr>],
        _ctx: &PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        not_impl_err!("PhysicalExtensionCodec is not provided")
    }

    /// Encode a custom extension expression into `buf`.
    ///
    /// If the codec embeds nested `PhysicalExprNode`s inside `buf`, encode them
    /// through `ctx.encode_child(..)` (equivalently
    /// [`PhysicalExprEncodeCtx::encode_child`]) rather than the free
    /// [`serialize_physical_expr`] function, so an active
    /// `DeduplicatingProtoConverter` stamps matching `expr_id`s for shared
    /// inner expressions. See [`Self::try_decode_expr`].
    ///
    /// [`serialize_physical_expr`]: crate::physical_plan::to_proto::serialize_physical_expr
    fn try_encode_expr(
        &self,
        _node: &Arc<dyn PhysicalExpr>,
        _buf: &mut Vec<u8>,
        _ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> Result<()> {
        not_impl_err!("PhysicalExtensionCodec is not provided")
    }

    fn try_decode_udaf(&self, name: &str, _buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        not_impl_err!(
            "PhysicalExtensionCodec is not provided for aggregate function {name}"
        )
    }

    fn try_encode_udaf(&self, _node: &AggregateUDF, _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn try_decode_udwf(&self, name: &str, _buf: &[u8]) -> Result<Arc<WindowUDF>> {
        not_impl_err!("PhysicalExtensionCodec is not provided for window function {name}")
    }

    fn try_encode_udwf(&self, _node: &WindowUDF, _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct DefaultPhysicalExtensionCodec {}

impl PhysicalExtensionCodec for DefaultPhysicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("PhysicalExtensionCodec is not provided")
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        not_impl_err!("PhysicalExtensionCodec is not provided")
    }
}

/// Controls the conversion of physical plans and expressions to and from their
/// Protobuf variants. Using this trait, users can perform optimizations on the
/// conversion process or collect performance metrics.
pub trait PhysicalProtoConverterExtension {
    fn proto_to_execution_plan(
        &self,
        proto: &protobuf::PhysicalPlanNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn default_proto_to_execution_plan(
        &self,
        proto: &protobuf::PhysicalPlanNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>>
    where
        Self: Sized,
    {
        proto.try_into_physical_plan_with_context(ctx, self)
    }

    fn execution_plan_to_proto(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<protobuf::PhysicalPlanNode>;

    fn proto_to_physical_expr(
        &self,
        proto: &protobuf::PhysicalExprNode,
        input_schema: &Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>>;

    fn default_proto_to_physical_expr(
        &self,
        proto: &protobuf::PhysicalExprNode,
        input_schema: &Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>>
    where
        Self: Sized,
    {
        parse_physical_expr_with_converter(proto, input_schema, ctx, self)
    }

    fn physical_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<protobuf::PhysicalExprNode>;
}

/// DataEncoderTuple captures the position of the encoder
/// in the codec list that was used to encode the data and actual encoded data
#[derive(Clone, PartialEq, prost::Message)]
struct DataEncoderTuple {
    /// The position of encoder used to encode data
    /// (to be used for decoding)
    #[prost(uint32, tag = 1)]
    pub encoder_position: u32,

    #[prost(bytes, tag = 2)]
    pub blob: Vec<u8>,
}

pub struct DefaultPhysicalProtoConverter {}

impl PhysicalProtoConverterExtension for DefaultPhysicalProtoConverter {
    fn proto_to_execution_plan(
        &self,
        proto: &protobuf::PhysicalPlanNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        proto.try_into_physical_plan_with_context(ctx, self)
    }

    fn execution_plan_to_proto(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<protobuf::PhysicalPlanNode>
    where
        Self: Sized,
    {
        protobuf::PhysicalPlanNode::try_from_physical_plan_with_converter(
            Arc::clone(plan),
            codec,
            self,
        )
    }

    fn proto_to_physical_expr(
        &self,
        proto: &protobuf::PhysicalExprNode,
        input_schema: &Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>>
    where
        Self: Sized,
    {
        // Default implementation calls the free function
        parse_physical_expr_with_converter(proto, input_schema, ctx, self)
    }

    fn physical_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<protobuf::PhysicalExprNode> {
        serialize_physical_expr_with_converter(expr, codec, self)
    }
}

/// Internal deserializer that caches expressions by their `expression_id()` so
/// multiple occurrences of the same expression are deduped.
#[derive(Default)]
struct DeduplicatingDeserializer {
    /// Cache mapping expression_id to deserialized expressions.
    cache: RefCell<HashMap<u64, Arc<dyn PhysicalExpr>>>,
}

impl PhysicalProtoConverterExtension for DeduplicatingDeserializer {
    fn proto_to_execution_plan(
        &self,
        proto: &protobuf::PhysicalPlanNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        proto.try_into_physical_plan_with_context(ctx, self)
    }

    fn execution_plan_to_proto(
        &self,
        _plan: &Arc<dyn ExecutionPlan>,
        _codec: &dyn PhysicalExtensionCodec,
    ) -> Result<protobuf::PhysicalPlanNode>
    where
        Self: Sized,
    {
        internal_err!("DeduplicatingDeserializer cannot serialize execution plans")
    }

    fn proto_to_physical_expr(
        &self,
        proto: &protobuf::PhysicalExprNode,
        input_schema: &Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>>
    where
        Self: Sized,
    {
        // `expr_id` is the generic identity slot on `PhysicalExprNode`.
        // The default serializer populates it from `PhysicalExpr::expression_id`.
        // A missing id means this expression type doesn't participate in deduping.
        let Some(id) = proto.expr_id else {
            return parse_physical_expr_with_converter(proto, input_schema, ctx, self);
        };

        let parsed = parse_physical_expr_with_converter(proto, input_schema, ctx, self)?;

        let mut cache = self.cache.borrow_mut();
        if let Some(cached) = cache.get(&id) {
            // Since expressions may manage their own internal state when deriving
            // expressions via `with_new_children`, we use `with_new_children`
            // to opt into the same behavior.
            //
            // For example, one `DynamicFilterPhysicalExpr` may be derived from
            // another resulting in shared references. Using `with_new_children`
            // is meant to preserve those references.
            let children: Vec<_> = parsed.children().into_iter().cloned().collect();
            return Arc::clone(cached).with_new_children(children);
        }

        cache.insert(id, Arc::clone(&parsed));
        Ok(parsed)
    }

    fn physical_expr_to_proto(
        &self,
        _expr: &Arc<dyn PhysicalExpr>,
        _codec: &dyn PhysicalExtensionCodec,
    ) -> Result<protobuf::PhysicalExprNode> {
        internal_err!("DeduplicatingDeserializer cannot serialize physical expressions")
    }
}

/// A proto converter that deduplicates [`PhysicalExpr`] by [`PhysicalExpr::expression_id`].
/// This helps preserve referential integrity when deserializing [`ExecutionPlan`]s
/// which may contain multiple occurrences of the same [`PhysicalExpr`] (ex. when
/// [`DynamicFilterPhysicalExpr`] are pushed down, it is important to preserve
/// referential integrity).
///
///
/// [`DynamicFilterPhysicalExpr`]: https://docs.rs/datafusion-physical-expr/latest/datafusion_physical_expr/expressions/struct.DynamicFilterPhysicalExpr.html
#[derive(Debug, Default, Clone, Copy)]
pub struct DeduplicatingProtoConverter {}

impl PhysicalProtoConverterExtension for DeduplicatingProtoConverter {
    fn proto_to_execution_plan(
        &self,
        proto: &protobuf::PhysicalPlanNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let deserializer = DeduplicatingDeserializer::default();
        proto.try_into_physical_plan_with_context(ctx, &deserializer)
    }

    fn execution_plan_to_proto(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<protobuf::PhysicalPlanNode>
    where
        Self: Sized,
    {
        protobuf::PhysicalPlanNode::try_from_physical_plan_with_converter(
            Arc::clone(plan),
            codec,
            self,
        )
    }

    fn proto_to_physical_expr(
        &self,
        proto: &protobuf::PhysicalExprNode,
        input_schema: &Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>>
    where
        Self: Sized,
    {
        let deserializer = DeduplicatingDeserializer::default();
        deserializer.proto_to_physical_expr(proto, input_schema, ctx)
    }

    fn physical_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<protobuf::PhysicalExprNode> {
        serialize_physical_expr_with_converter(expr, codec, self)
    }
}

/// A PhysicalExtensionCodec that tries one of multiple inner codecs
/// until one works
#[derive(Debug)]
pub struct ComposedPhysicalExtensionCodec {
    codecs: Vec<Arc<dyn PhysicalExtensionCodec>>,
}

impl ComposedPhysicalExtensionCodec {
    // Position in this codecs list is important as it will be used for decoding.
    // If new codec is added it should go to last position.
    pub fn new(codecs: Vec<Arc<dyn PhysicalExtensionCodec>>) -> Self {
        Self { codecs }
    }

    fn decode_protobuf<R>(
        &self,
        buf: &[u8],
        decode: impl FnOnce(&dyn PhysicalExtensionCodec, &[u8]) -> Result<R>,
    ) -> Result<R> {
        let proto =
            DataEncoderTuple::decode(buf).map_err(|e| internal_datafusion_err!("{e}"))?;

        let codec = self.codecs.get(proto.encoder_position as usize).ok_or(
            internal_datafusion_err!("Can't find required codec in codec list"),
        )?;

        decode(codec.as_ref(), &proto.blob)
    }

    fn encode_protobuf(
        &self,
        buf: &mut Vec<u8>,
        mut encode: impl FnMut(&dyn PhysicalExtensionCodec, &mut Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        let mut data = vec![];
        let mut last_err = None;
        let mut encoder_position = None;

        // find the encoder
        for (position, codec) in self.codecs.iter().enumerate() {
            match encode(codec.as_ref(), &mut data) {
                Ok(_) => {
                    encoder_position = Some(position as u32);
                    break;
                }
                Err(err) => last_err = Some(err),
            }
        }

        let encoder_position = encoder_position.ok_or_else(|| {
            last_err.unwrap_or_else(|| {
                DataFusionError::NotImplemented(
                    "Empty list of composed codecs".to_owned(),
                )
            })
        })?;

        // encode with encoder position
        let proto = DataEncoderTuple {
            encoder_position,
            blob: data,
        };
        proto
            .encode(buf)
            .map_err(|e| internal_datafusion_err!("{e}"))
    }
}

impl PhysicalExtensionCodec for ComposedPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.decode_protobuf(buf, |codec, data| {
            codec.try_decode(data, inputs, ctx, proto_converter)
        })
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        self.encode_protobuf(buf, |codec, data| {
            codec.try_encode(Arc::clone(&node), data, proto_converter)
        })
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.decode_protobuf(buf, |codec, data| codec.try_decode_udf(name, data))
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.encode_protobuf(buf, |codec, data| codec.try_encode_udf(node, data))
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        self.decode_protobuf(buf, |codec, data| codec.try_decode_udaf(name, data))
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.encode_protobuf(buf, |codec, data| codec.try_encode_udaf(node, data))
    }
}

/// Adapter backing [`ExecutionPlanEncodeCtx`] for plans migrated to the
/// `try_to_proto` hook (#22419). Routes child-plan and child-expr encoding back
/// through the central converter so nested plans honor their own hooks.
struct ConverterPlanEncoder<'a> {
    codec: &'a dyn PhysicalExtensionCodec,
    proto_converter: &'a dyn PhysicalProtoConverterExtension,
}

impl ExecutionPlanEncode for ConverterPlanEncoder<'_> {
    fn encode_plan(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<protobuf::PhysicalPlanNode> {
        self.proto_converter
            .execution_plan_to_proto(plan, self.codec)
    }

    fn encode_expr(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Result<protobuf::PhysicalExprNode> {
        self.proto_converter
            .physical_expr_to_proto(expr, self.codec)
    }

    // Bytes-only function serde. `(!buf.is_empty()).then_some(buf)` preserves the
    // existing `fun_definition` wire semantics (empty payload == encode-by-name).
    fn encode_udf(&self, udf: &ScalarUDF) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![];
        self.codec.try_encode_udf(udf, &mut buf)?;
        Ok((!buf.is_empty()).then_some(buf))
    }

    fn encode_udaf(&self, udaf: &AggregateUDF) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![];
        self.codec.try_encode_udaf(udaf, &mut buf)?;
        Ok((!buf.is_empty()).then_some(buf))
    }

    fn encode_udwf(&self, udwf: &WindowUDF) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![];
        self.codec.try_encode_udwf(udwf, &mut buf)?;
        Ok((!buf.is_empty()).then_some(buf))
    }
}

/// Adapter backing [`ExecutionPlanDecodeCtx`] for plans migrated to the
/// `try_from_proto` pattern (#22419). Routes child-plan and child-expr decoding
/// back through the central converter, and exposes the session task context
/// (never the extension codec).
struct ConverterPlanDecoder<'a, 'ctx> {
    ctx: &'a PhysicalPlanDecodeContext<'ctx>,
    proto_converter: &'a dyn PhysicalProtoConverterExtension,
}

impl ExecutionPlanDecode for ConverterPlanDecoder<'_, '_> {
    fn decode_plan(
        &self,
        node: &protobuf::PhysicalPlanNode,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.proto_converter.proto_to_execution_plan(node, self.ctx)
    }

    fn decode_plan_with_scalar_subquery_results(
        &self,
        node: &protobuf::PhysicalPlanNode,
        results: ScalarSubqueryResults,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let scoped_ctx = self.ctx.with_scalar_subquery_results(results);
        self.proto_converter
            .proto_to_execution_plan(node, &scoped_ctx)
    }

    fn decode_expr(
        &self,
        node: &protobuf::PhysicalExprNode,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.proto_converter
            .proto_to_physical_expr(node, input_schema, self.ctx)
    }

    fn task_ctx(&self) -> &TaskContext {
        self.ctx.task_ctx()
    }

    // Lookup-order policy, owned here so no plan re-derives it: an explicit
    // payload is decoded by the codec; otherwise resolve by name from the
    // registry, falling back to the codec with an empty buffer.
    fn decode_udf(&self, name: &str, payload: Option<&[u8]>) -> Result<Arc<ScalarUDF>> {
        match payload {
            Some(buf) => self.ctx.codec().try_decode_udf(name, buf),
            None => self
                .ctx
                .task_ctx()
                .udf(name)
                .or_else(|_| self.ctx.codec().try_decode_udf(name, &[])),
        }
    }

    fn decode_udaf(
        &self,
        name: &str,
        payload: Option<&[u8]>,
    ) -> Result<Arc<AggregateUDF>> {
        match payload {
            Some(buf) => self.ctx.codec().try_decode_udaf(name, buf),
            None => self
                .ctx
                .task_ctx()
                .udaf(name)
                .or_else(|_| self.ctx.codec().try_decode_udaf(name, &[])),
        }
    }

    fn decode_udwf(&self, name: &str, payload: Option<&[u8]>) -> Result<Arc<WindowUDF>> {
        match payload {
            Some(buf) => self.ctx.codec().try_decode_udwf(name, buf),
            None => self
                .ctx
                .task_ctx()
                .udwf(name)
                .or_else(|_| self.ctx.codec().try_decode_udwf(name, &[])),
        }
    }
}
