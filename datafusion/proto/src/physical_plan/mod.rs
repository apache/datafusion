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
use datafusion_common::utils::{usize_from_wire, usize_to_wire};
use datafusion_common::{
    DataFusionError, Result, internal_datafusion_err, internal_err, not_impl_err,
    plan_err,
};
use datafusion_datasource_arrow::source::ArrowSource;
#[cfg(feature = "avro")]
use datafusion_datasource_avro::source::AvroSource;
use datafusion_datasource_csv::file_format::CsvSink;
use datafusion_datasource_csv::source::CsvSource;
use datafusion_datasource_json::file_format::JsonSink;
use datafusion_datasource_json::source::JsonSource;
#[cfg(feature = "parquet")]
use datafusion_datasource_parquet::file_format::ParquetSink;
#[cfg(feature = "parquet")]
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_execution::{FunctionRegistry, TaskContext};
use datafusion_expr::physical_planning_context::ScalarSubqueryResults;
use datafusion_expr::{AggregateUDF, HigherOrderUDF, ScalarUDF, WindowUDF};
use datafusion_functions_table::generate_series::{
    Empty, GenSeriesArgs, GenerateSeriesTable, GenericSeriesState, TimestampValue,
};
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
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PiecewiseMergeJoinExec,
    SortMergeJoinExec, SymmetricHashJoinExec,
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
use datafusion_physical_plan::windows::WindowAggExec;
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
use prost::Message;
use prost::bytes::BufMut;

use crate::convert_required;
use crate::physical_plan::from_proto::parse_physical_expr_with_converter;
use crate::physical_plan::to_proto::serialize_physical_expr_with_converter;
use crate::protobuf::physical_plan_node::PhysicalPlanType;
use crate::protobuf::{self, proto_error};

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
    use datafusion_datasource::file_compression_type::FileCompressionType;
    use datafusion_datasource::file_groups::FileGroup;
    use datafusion_datasource::file_scan_config::{
        FileScanConfig, FileScanConfigBuilder,
    };
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

        fn apply_expressions(
            &self,
            f: &mut dyn FnMut(
                &Arc<dyn PhysicalExpr>,
            )
                -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
        ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
            datafusion_physical_plan::apply_expression_roots(
                self.projection.iter().flatten(),
                f,
            )
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
            .with_file_compression_type(FileCompressionType::GZIP)
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
        assert_eq!(decoded.file_compression_type, FileCompressionType::GZIP);

        Ok(())
    }

    #[test]
    fn new_file_scan_config_decode_without_compression_uses_legacy_default() -> Result<()>
    {
        let serde = FileScanSerdeHarness::new();
        let mut encoded = serde.encode(&test_config(None))?;
        assert!(encoded.file_compression_type.is_some());

        encoded.file_compression_type = None;
        let decoded = serde.decode(&encoded)?;
        assert_eq!(
            decoded.file_compression_type,
            FileCompressionType::UNCOMPRESSED
        );
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

        let mut unknown_compression = valid;
        unknown_compression.file_compression_type = Some(i32::MAX);
        let err = serde
            .decode(&unknown_compression)
            .expect_err("unknown compression type must fail");
        assert!(
            err.to_string().contains("Unknown file compression type"),
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
            PhysicalPlanType::CsvScan(_) => {
                CsvSource::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::JsonScan(_) => {
                JsonSource::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::ParquetScan(_) => {
                #[cfg(feature = "parquet")]
                {
                    ParquetSource::try_from_proto(self.node(), &decode_ctx)
                }
                #[cfg(not(feature = "parquet"))]
                not_impl_err!(
                    "Unable to process a Parquet PhysicalPlan when the `parquet` feature is not enabled"
                )
            }
            PhysicalPlanType::AvroScan(_) => {
                #[cfg(feature = "avro")]
                {
                    AvroSource::try_from_proto(self.node(), &decode_ctx)
                }
                #[cfg(not(feature = "avro"))]
                panic!(
                    "Unable to process a Avro PhysicalPlan when `avro` feature is not enabled"
                )
            }
            PhysicalPlanType::MemoryScan(_) => {
                MemorySourceConfig::try_from_proto(self.node(), &decode_ctx)
            }
            PhysicalPlanType::ArrowScan(_) => {
                ArrowSource::try_from_proto(self.node(), &decode_ctx)
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
            PhysicalPlanType::PiecewiseMergeJoin(_) => {
                PiecewiseMergeJoinExec::try_from_proto(self.node(), &decode_ctx)
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

    fn generate_series_name_to_str(name: protobuf::GenerateSeriesName) -> &'static str {
        match name {
            protobuf::GenerateSeriesName::GsGenerateSeries => "generate_series",
            protobuf::GenerateSeriesName::GsRange => "range",
        }
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
        let target_batch_size = usize_from_wire(
            generate_series.target_batch_size,
            "GenerateSeriesNode",
            "target_batch_size",
        )?;
        if target_batch_size == 0 {
            return plan_err!(
                "GenerateSeriesNode: target_batch_size must be greater than 0"
            );
        }
        let generator = table.as_generator(target_batch_size)?;

        Ok(Arc::new(LazyMemoryExec::try_new(schema, vec![generator])?))
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

        let encode_target_batch_size =
            |size| usize_to_wire::<u32>(size, "GenerateSeriesNode", "target_batch_size");
        if let Some(int_64) = generator_guard
            .as_any()
            .downcast_ref::<GenericSeriesState<i64>>()
        {
            let schema = exec.schema();
            let node = protobuf::GenerateSeriesNode {
                schema: Some(schema.as_ref().try_into()?),
                target_batch_size: encode_target_batch_size(int_64.batch_size())?,
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
                target_batch_size: encode_target_batch_size(timestamp_args.batch_size())?,
                args: Some(args),
            };

            return Ok(Some(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::GenerateSeries(node)),
            }));
        }

        Ok(None)
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
