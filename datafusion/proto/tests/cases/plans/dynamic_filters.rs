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

//! Dynamic filter expressions: their deduplication across a plan, and the
//! plans that produce them.

use super::{roundtrip_test_and_return, roundtrip_test_sql_with_context};
use arrow::array::RecordBatch;
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{JoinType, Operator};
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, PhysicalSortExpr, lit,
};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use datafusion_common::config::{ConfigOptions, TableParquetOptions};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{NullEquality, Result, internal_datafusion_err, internal_err};
use datafusion_datasource::file::FileSource;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_proto_converter,
    physical_plan_to_bytes_with_proto_converter,
};
use datafusion_proto::physical_plan::{
    DeduplicatingProtoConverter, DefaultPhysicalExtensionCodec,
    DefaultPhysicalProtoConverter, PhysicalExtensionCodec, PhysicalPlanDecodeContext,
    PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::vec;

/// Create a [`DynamicFilterPhysicalExpr`] with child column expression "a" @ index 0.
fn make_dynamic_filter() -> Arc<dyn PhysicalExpr> {
    Arc::new(DynamicFilterPhysicalExpr::new(
        vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>],
        lit(true),
    )) as Arc<dyn PhysicalExpr>
}

/// Update a [`DynamicFilterPhysicalExpr`]'s children to support child schema "b" @ 0, "a" @ 1.
fn make_reassigned_dynamic_filter(
    filter: Arc<dyn PhysicalExpr>,
) -> Result<(Arc<Schema>, Arc<dyn PhysicalExpr>)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Int64, false),
        Field::new("a", DataType::Int64, false),
    ]));
    let reassigned = reassign_expr_columns(filter, &schema)?;
    Ok((schema, reassigned))
}

/// Extract the expression id from a [`PhysicalExpr`] proto. Populated by the
/// default serializer from `PhysicalExpr::expression_id`.
fn proto_expression_id(expr: &PhysicalExprNode) -> u64 {
    expr.expr_id
        .expect("expected PhysicalExprNode.expr_id to be populated")
}

/// Roundtrip a single physical expression shaped like so:
///
/// ```text
///             BinaryExpr(AND)
///             /             \
///     filter_expr_1     filter_expr_2
/// ```
///
/// Returns filter_expr_1 and filter_expr_2 after deserialization.
fn roundtrip_dynamic_filter_expr_pair(
    filter_expr_1: Arc<dyn PhysicalExpr>,
    filter_expr_2: Arc<dyn PhysicalExpr>,
    schema: Arc<Schema>,
) -> Result<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> {
    let pair_expr = Arc::new(BinaryExpr::new(
        Arc::clone(&filter_expr_1),
        Operator::And,
        Arc::clone(&filter_expr_2),
    )) as Arc<dyn PhysicalExpr>;

    let codec = DefaultPhysicalExtensionCodec {};
    let converter = DeduplicatingProtoConverter {};
    let proto = converter.physical_expr_to_proto(&pair_expr, &codec)?;
    let ctx = SessionContext::new();
    let task_ctx = ctx.task_ctx();
    let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx.as_ref(), &codec);
    let deserialized_expr =
        converter.proto_to_physical_expr(&proto, &schema, &decode_ctx)?;

    let binary = deserialized_expr
        .downcast_ref::<BinaryExpr>()
        .expect("Expected BinaryExpr");

    Ok((Arc::clone(binary.left()), Arc::clone(binary.right())))
}

/// Roundtrip an execution plan shaped like so:
///
/// ```text
/// FilterExec(dynamic_filter_1 on a@0)
///   ProjectionExec(a := Column("a", source_index))
///     DataSourceExec
///       ParquetSource(predicate = dynamic_filter_2)
/// ```
///
/// `dynamic_filter_1` and `dynamic_filter_2` are the same dynamic filter, except with
/// different children.
///
/// Returns
/// - `dynamic_filter_1` before serialization
/// - `dynamic_filter_2` before serialization
/// - `dynamic_filter_1` after serialization
/// - `dynamic_filter_2` after serialization
#[expect(clippy::type_complexity)]
fn roundtrip_dynamic_filter_plan_pair() -> Result<(
    Arc<dyn PhysicalExpr>,
    Arc<dyn PhysicalExpr>,
    Arc<dyn PhysicalExpr>,
    Arc<dyn PhysicalExpr>,
)> {
    let filter_expr_1 = make_dynamic_filter();
    let (data_source_schema, filter_expr_2) =
        make_reassigned_dynamic_filter(Arc::clone(&filter_expr_1))?;
    let left_before = Arc::clone(&filter_expr_1);
    let right_before = Arc::clone(&filter_expr_2);
    let file_source = Arc::new(
        ParquetSource::new(Arc::clone(&data_source_schema))
            .with_predicate(Arc::clone(&filter_expr_2)),
    );
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.parquet".to_string(),
                1024,
            )])])
            .build();
    let data_source_exec =
        DataSourceExec::from_data_source(scan_config) as Arc<dyn ExecutionPlan>;

    let projection_exec = Arc::new(ProjectionExec::try_new(
        vec![ProjectionExpr {
            expr: Arc::new(Column::new("a", 1)) as Arc<dyn PhysicalExpr>,
            alias: "a".to_string(),
        }],
        data_source_exec,
    )?) as Arc<dyn ExecutionPlan>;
    let filter_exec = Arc::new(FilterExec::try_new(
        Arc::clone(&filter_expr_1),
        projection_exec,
    )?) as Arc<dyn ExecutionPlan>;

    let codec = DefaultPhysicalExtensionCodec {};
    let converter = DeduplicatingProtoConverter {};
    let proto = converter.execution_plan_to_proto(&filter_exec, &codec)?;

    let ctx = SessionContext::new();
    let task_ctx = ctx.task_ctx();
    let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx.as_ref(), &codec);
    let deserialized_plan = converter.proto_to_execution_plan(&proto, &decode_ctx)?;

    let outer_filter = deserialized_plan
        .downcast_ref::<FilterExec>()
        .expect("Expected outer FilterExec");
    let left_filter = Arc::clone(outer_filter.predicate());
    let projection = outer_filter.children()[0]
        .downcast_ref::<ProjectionExec>()
        .expect("Expected ProjectionExec");
    let data_source = projection
        .input()
        .downcast_ref::<DataSourceExec>()
        .expect("Expected DataSourceExec");
    let scan_config = data_source
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .expect("Expected FileScanConfig");
    let right_filter = scan_config
        .file_source()
        .filter()
        .expect("Expected pushed-down predicate");

    Ok((left_before, right_before, left_filter, right_filter))
}

/// Takes two [`DynamicFilterPhysicalExpr`] and asserts that updates to one are visible
/// via the other. This helps assert that referential integrity is maintained after
/// deserializing.
fn assert_dynamic_filter_update_is_visible(
    left_filter: &Arc<dyn PhysicalExpr>,
    right_filter: &Arc<dyn PhysicalExpr>,
) -> Result<()> {
    let left_filter = left_filter
        .downcast_ref::<DynamicFilterPhysicalExpr>()
        .expect("Expected dynamic filter");
    let right_filter = right_filter
        .downcast_ref::<DynamicFilterPhysicalExpr>()
        .expect("Expected dynamic filter");

    // Sanity check that the filters have the same generation.
    let original_generation = left_filter.snapshot_generation();
    assert_eq!(original_generation, right_filter.snapshot_generation(),);

    left_filter.update(lit(123_i64))?;

    // Assert that both generations updated.
    assert_eq!(original_generation + 1, right_filter.snapshot_generation(),);
    assert_eq!(
        left_filter.snapshot_generation(),
        right_filter.snapshot_generation(),
    );

    // Ensure both filters have the updated expr.
    let expected_current = r#"Literal { value: Int64(123), field: Field { name: "lit", data_type: Int64 } }"#;
    assert_eq!(expected_current, format!("{:?}", left_filter.current()?));
    assert_eq!(expected_current, format!("{:?}", right_filter.current()?));

    Ok(())
}

/// Extract the dynamic-filter predicate that was pushed down to the parquet
/// scan at the bottom of the plan tree.
fn parquet_source_predicate(child: &Arc<dyn ExecutionPlan>) -> Arc<dyn PhysicalExpr> {
    let data_source = child
        .downcast_ref::<DataSourceExec>()
        .expect("Child should be DataSourceExec");
    let (_, parquet_source) = data_source
        .downcast_to_file_source::<ParquetSource>()
        .expect("Should be ParquetSource");
    parquet_source
        .filter()
        .expect("ParquetSource should have a predicate after roundtrip")
}

/// Assert that two dynamic filters are equal both structurally (Debug output)
/// and by identity (`expression_id`).
fn assert_dynamic_filters_equal(
    expected: &Arc<dyn PhysicalExpr>,
    actual: &Arc<dyn PhysicalExpr>,
) {
    // Structural.
    let expected_dbg = format!("{expected:?}");
    let actual_dbg = format!("{actual:?}");
    if expected_dbg == actual_dbg {
        return;
    }

    // Note that the `DeduplicatingDeserializer` routes every cache hit through
    // `with_new_children`. This produces an equivalent expression, but with
    // remapped children that are equal to the original. Handle that case here.
    let rewritten = Arc::clone(expected)
        .with_new_children(expected.children().iter().map(|c| Arc::clone(c)).collect())
        .expect("with_new_children on a dynamic filter should not fail");
    assert_eq!(format!("{rewritten:?}"), actual_dbg);
}

// Two clones of a dynamic filter expression should be deduped to the exact same expression.
#[test]
fn test_dynamic_filter_roundtrip_dedupe() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let filter_expr_1 = make_dynamic_filter();
    let filter_expr_2 = Arc::clone(&filter_expr_1);

    let (filter_expr_1_after_roundtrip, filter_expr_2_after_roundtrip) =
        roundtrip_dynamic_filter_expr_pair(
            Arc::clone(&filter_expr_1),
            Arc::clone(&filter_expr_2),
            schema,
        )?;

    // Assert the filters are not modified during roundtrip.
    assert_dynamic_filters_equal(&filter_expr_1, &filter_expr_1_after_roundtrip);
    assert_dynamic_filters_equal(&filter_expr_2, &filter_expr_2_after_roundtrip);
    assert_dynamic_filters_equal(
        &filter_expr_1_after_roundtrip,
        &filter_expr_2_after_roundtrip,
    );

    // Assert referential integrity.
    assert_dynamic_filter_update_is_visible(
        &filter_expr_1_after_roundtrip,
        &filter_expr_2_after_roundtrip,
    )?;

    Ok(())
}

/// Roundtrip test for an execution plan where there are multiple instances of a dynamic filter
/// with different children.
#[test]
fn test_dynamic_filter_plan_roundtrip_dedupe() -> Result<()> {
    let (
        filter_expr_1,
        filter_expr_2,
        filter_expr_1_after_roundtrip,
        filter_expr_2_after_roundtrip,
    ) = roundtrip_dynamic_filter_plan_pair()?;

    // Assert the filters are not modified during roundtrip.
    assert_dynamic_filters_equal(&filter_expr_1, &filter_expr_1_after_roundtrip);
    assert_dynamic_filters_equal(&filter_expr_2, &filter_expr_2_after_roundtrip);

    // Assert referential integrity.
    assert_dynamic_filter_update_is_visible(
        &filter_expr_1_after_roundtrip,
        &filter_expr_2_after_roundtrip,
    )?;

    Ok(())
}

#[test]
fn test_dynamic_filter_expression_id_is_stable_between_serializations() -> Result<()> {
    let filter_expr = make_dynamic_filter();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DeduplicatingProtoConverter {};

    let proto1 = proto_converter.physical_expr_to_proto(&filter_expr, &codec)?;
    let expr_id1 = proto_expression_id(&proto1);

    let proto2 = proto_converter.physical_expr_to_proto(&filter_expr, &codec)?;
    let expr_id2 = proto_expression_id(&proto2);

    assert_eq!(
        expr_id1, expr_id2,
        "Expected the same dynamic filter expression id across serializations"
    );

    Ok(())
}

/// Create a DataSourceExec backed by a ParquetSource that accepts filter pushdown,
/// along with a ConfigOptions that enables all dynamic filter pushdown options.
fn datasource_for_dynamic_filter_pushdown(
    schema: &Arc<Schema>,
) -> (Arc<dyn ExecutionPlan>, ConfigOptions) {
    let mut parquet_options = TableParquetOptions::new();
    parquet_options.global.pushdown_filters = true;
    let source = Arc::new(
        ParquetSource::new(Arc::clone(schema))
            .with_table_parquet_options(parquet_options),
    );
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_file(PartitionedFile::new("/path/to/file.parquet", 1024))
            .build();

    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    config.optimizer.enable_join_dynamic_filter_pushdown = true;
    config.optimizer.enable_aggregate_dynamic_filter_pushdown = true;
    config.optimizer.enable_topk_dynamic_filter_pushdown = true;

    (DataSourceExec::from_data_source(scan_config), config)
}

/// Test that plan containing a HashJoinExec with dynamic filter pushdown
/// can be serialized and deserialized while preserving references to the dynamic filter.
#[test]
fn test_hash_join_with_dynamic_filter_roundtrip() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int64, false)]));

    let left_child = Arc::new(EmptyExec::new(Arc::clone(&schema)));
    let (right_child, config) = datasource_for_dynamic_filter_pushdown(&schema);

    let on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> = vec![(
        Arc::new(Column::new("col", 0)),
        Arc::new(Column::new("col", 0)),
    )];

    let hash_join = Arc::new(HashJoinExec::try_new(
        left_child,
        right_child,
        on,
        None,
        &JoinType::Inner,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
        false,
    )?) as Arc<dyn ExecutionPlan>;

    // Run the optimizer rule for filter pushdown.
    let optimizer = FilterPushdown::new_post_optimization();
    let plan = optimizer.optimize(hash_join, &config)?;

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let converter = DeduplicatingProtoConverter {};
    let deserialized = roundtrip_test_and_return(plan, &ctx, &codec, &converter)?;

    // Extract the deserialized HashJoinExec and its dynamic filter.
    let deserialized_join = deserialized
        .downcast_ref::<HashJoinExec>()
        .expect("Should be HashJoinExec");
    let deserialized_hash_join_df = deserialized_join
        .dynamic_expressions_produced()
        .into_iter()
        .next()
        .expect("HashJoinExec should have a dynamic filter after roundtrip");

    // Extract the dynamic filter pushed down to the probe side's ParquetSource.
    let deserialized_predicate = parquet_source_predicate(deserialized_join.right());

    // The HashJoinExec's dynamic filter and the probe side's predicate should
    // refer to the same underlying expression.
    let plan_df = deserialized_hash_join_df;
    assert_dynamic_filters_equal(&plan_df, &deserialized_predicate);
    assert_dynamic_filter_update_is_visible(&plan_df, &deserialized_predicate)?;

    Ok(())
}

/// returns a SessionContext with an empty `netflow` table registered
fn netflow_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("dst_geo_country_name", DataType::Utf8, true),
        Field::new("dst_geo_city_name", DataType::Utf8, true),
        Field::new("packets", DataType::UInt64, true),
        Field::new("src_addr", DataType::Utf8, true),
        Field::new("dst_addr", DataType::Utf8, true),
    ]));

    ctx.register_table("netflow", Arc::new(EmptyTable::new(schema)))?;

    Ok(ctx)
}

/// Regression test for issue #18602:
/// https://github.com/apache/datafusion/issues/18602
///
/// The physical filter expression here contains a long chain of `AND` predicates.
/// Before linearizing `PhysicalBinaryExprNode`, encoding then decoding the protobuf
/// could fail with `DecodeError: recursion limit reached`.
#[tokio::test]
async fn roundtrip_issue_18602_complex_filter_decode_recursion() -> Result<()> {
    let ctx = netflow_context()?;
    let sql = "SELECT \
      dst_geo_country_name AS x_axis_1, \
      dst_geo_city_name AS x_axis_2, \
      sum(packets) AS y_axis_1 \
    FROM netflow \
    WHERE dst_geo_country_name IS NOT NULL \
      AND src_addr NOT LIKE '10.201.%' \
      AND dst_addr NOT LIKE '10.201.%' \
      AND src_addr NOT LIKE '10.202.%' \
      AND dst_addr NOT LIKE '10.202.%' \
      AND src_addr NOT LIKE '10.203.%' \
      AND dst_addr NOT LIKE '10.203.%' \
      AND src_addr NOT LIKE '10.204.%' \
      AND dst_addr NOT LIKE '10.204.%' \
      AND src_addr NOT LIKE '172.16.186.%' \
      AND dst_addr NOT LIKE '172.16.186.%' \
      AND src_addr NOT LIKE '172.16.187.%' \
      AND dst_addr NOT LIKE '172.16.187.%' \
      AND src_addr NOT LIKE '172.16.188.%' \
      AND dst_addr NOT LIKE '172.16.188.%' \
      AND src_addr NOT LIKE '10.102.45.%' \
      AND dst_addr NOT LIKE '10.102.45.%' \
      AND src_addr NOT LIKE '172.25.210.%' \
      AND dst_addr NOT LIKE '172.25.210.%' \
      AND src_addr NOT LIKE '172.25.211.%' \
      AND dst_addr NOT LIKE '172.25.211.%' \
      AND src_addr NOT LIKE '141.226.101.%' \
      AND dst_addr NOT LIKE '141.226.101.%' \
      AND src_addr NOT LIKE '167.86.40.%' \
      AND dst_addr NOT LIKE '167.86.40.%' \
      AND src_addr NOT LIKE '66.22.38.%' \
      AND dst_addr NOT LIKE '66.22.38.%' \
      AND src_addr != '168.143.191.55' \
      AND dst_addr != '168.143.191.55' \
      AND src_addr != '82.112.107.142' \
      AND dst_addr != '82.112.107.142' \
      AND src_addr != '20.76.39.176' \
      AND dst_addr != '20.76.39.176' \
      AND src_addr != '162.159.129.83' \
      AND dst_addr != '162.159.129.83' \
      AND src_addr != '34.201.223.155' \
      AND dst_addr != '34.201.223.155' \
      AND src_addr != '34.201.223.156' \
      AND dst_addr != '34.201.223.156' \
      AND src_addr != '34.201.223.157' \
      AND dst_addr != '34.201.223.157' \
      AND src_addr != '134.201.223.157' \
      AND dst_addr != '134.201.223.157' \
      AND src_addr != '341.201.223.157' \
      AND dst_addr != '341.201.223.157' \
    GROUP BY x_axis_1, x_axis_2 \
    ORDER BY y_axis_1 DESC \
    LIMIT 20";

    roundtrip_test_sql_with_context(sql, &ctx).await
}

/// Test that plan containing a AggregateExec with dynamic filter pushdown
/// can be serialized and deserialized while preserving references to the dynamic filter.
#[test]
fn test_aggregate_with_dynamic_filter_roundtrip() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let col_a: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));

    let (child, config) = datasource_for_dynamic_filter_pushdown(&schema);

    let agg = Arc::new(AggregateExec::try_new(
        AggregateMode::Partial,
        PhysicalGroupBy::new_single(vec![]),
        vec![
            AggregateExprBuilder::new(
                datafusion::functions_aggregate::min_max::min_udaf(),
                vec![Arc::clone(&col_a)],
            )
            .schema(Arc::clone(&schema))
            .alias("min_a")
            .build()
            .map(Arc::new)?,
        ],
        vec![None],
        child,
        Arc::clone(&schema),
    )?) as Arc<dyn ExecutionPlan>;

    // Run the optimizer rule for filter pushdown.
    let optimizer = FilterPushdown::new_post_optimization();
    let plan = optimizer.optimize(agg, &config)?;

    // Roundtrip with deduplication.
    //
    // Note: We don't use `roundtrip_test_and_return` here because there's a
    // pre-existing issue with PhysicalGroupBy serialization where empty groups
    // `[[]]` become `[]` after roundtrip. This behavior is unrelated to this test.
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let converter = DeduplicatingProtoConverter {};
    let bytes = physical_plan_to_bytes_with_proto_converter(
        Arc::clone(&plan),
        &codec,
        &converter,
    )?;
    let deserialized = physical_plan_from_bytes_with_proto_converter(
        bytes.as_ref(),
        ctx.task_ctx().as_ref(),
        &codec,
        &converter,
    )?;

    // Extract the deserialized AggregateExec and its dynamic filter.
    let deserialized_agg = deserialized
        .downcast_ref::<AggregateExec>()
        .expect("Should be AggregateExec");
    let deserialized_agg_df = deserialized_agg
        .dynamic_expressions_produced()
        .into_iter()
        .next()
        .expect("AggregateExec should have a dynamic filter after roundtrip");

    // Extract the dynamic filter pushed down to the child ParquetSource.
    let deserialized_predicate = parquet_source_predicate(deserialized_agg.input());

    // The AggregateExec's dynamic filter and the child's predicate should
    // refer to the same underlying expression.
    let plan_df = deserialized_agg_df;
    assert_dynamic_filters_equal(&plan_df, &deserialized_predicate);
    assert_dynamic_filter_update_is_visible(&plan_df, &deserialized_predicate)?;

    Ok(())
}

#[test]
fn test_aggregate_without_dynamic_filter_roundtrip() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let col_a: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));
    let child = Arc::new(EmptyExec::new(Arc::clone(&schema)));
    let aggregate = Arc::new(AggregateExec::try_new(
        AggregateMode::Partial,
        PhysicalGroupBy::new_single(vec![]),
        vec![
            AggregateExprBuilder::new(
                datafusion::functions_aggregate::min_max::min_udaf(),
                vec![col_a],
            )
            .schema(Arc::clone(&schema))
            .alias("min_a")
            .build()
            .map(Arc::new)?,
        ],
        vec![None],
        child,
        Arc::clone(&schema),
    )?) as Arc<dyn ExecutionPlan>;

    let mut config = ConfigOptions::default();
    config.optimizer.enable_aggregate_dynamic_filter_pushdown = true;
    let optimizer = FilterPushdown::new_post_optimization();
    let plan = optimizer.optimize(aggregate, &config)?;
    assert!(
        plan.downcast_ref::<AggregateExec>()
            .expect("Should be AggregateExec")
            .dynamic_expressions_produced()
            .is_empty()
    );

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let converter = DefaultPhysicalProtoConverter {};
    let bytes = physical_plan_to_bytes_with_proto_converter(plan, &codec, &converter)?;
    let deserialized = physical_plan_from_bytes_with_proto_converter(
        bytes.as_ref(),
        ctx.task_ctx().as_ref(),
        &codec,
        &converter,
    )?;

    assert!(
        deserialized
            .downcast_ref::<AggregateExec>()
            .expect("Should be AggregateExec")
            .dynamic_expressions_produced()
            .is_empty()
    );
    Ok(())
}

/// Test that plan containing a SortExec with dynamic filter pushdown
/// can be serialized and deserialized while preserving references to the dynamic filter.
#[test]
fn test_sort_topk_with_dynamic_filter_roundtrip() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let col_a: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));

    let (child, config) = datasource_for_dynamic_filter_pushdown(&schema);

    let sort = Arc::new(
        SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr {
                expr: Arc::clone(&col_a),
                options: SortOptions::default(),
            }])
            .unwrap(),
            child,
        )
        .with_fetch(Some(10)),
    ) as Arc<dyn ExecutionPlan>;

    // Verify the optimizer kept the dynamic filter on the SortExec.
    let optimizer = FilterPushdown::new_post_optimization();
    let plan = optimizer.optimize(sort, &config)?;

    // Roundtrip with deduplication.
    //
    // Note: We don't use `roundtrip_test_and_return` here because
    // `DeduplicatingDeserializer` rewrites cache hits via `with_new_children`,
    // which sets `remapped_children: Some(...)` on the second encounter of a
    // shared `DynamicFilterPhysicalExpr`. SortExec's `Debug` includes its
    // dynamic filter, so the original-vs-deserialized structural equality check
    // would fail purely on this artifact.
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let converter = DeduplicatingProtoConverter {};
    let bytes = physical_plan_to_bytes_with_proto_converter(
        Arc::clone(&plan),
        &codec,
        &converter,
    )?;
    let deserialized = physical_plan_from_bytes_with_proto_converter(
        bytes.as_ref(),
        ctx.task_ctx().as_ref(),
        &codec,
        &converter,
    )?;

    // Extract the deserialized SortExec and its dynamic filter.
    let deserialized_sort = deserialized
        .downcast_ref::<SortExec>()
        .expect("Should be SortExec");
    let deserialized_sort_df = deserialized_sort
        .dynamic_expressions_produced()
        .into_iter()
        .next()
        .expect("SortExec should have a dynamic filter after roundtrip");

    // Extract the dynamic filter pushed down to the child ParquetSource.
    let deserialized_predicate = parquet_source_predicate(deserialized_sort.input());

    // The SortExec's dynamic filter and the child's predicate should
    // refer to the same underlying expression.
    let plan_df = deserialized_sort_df;
    assert_dynamic_filters_equal(&plan_df, &deserialized_predicate);
    assert_dynamic_filter_update_is_visible(&plan_df, &deserialized_predicate)?;

    Ok(())
}

/// A custom [`ExecutionPlan`] which stores [`PhysicalExpr`]s.
struct CustomExecWithExprs {
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    child: Arc<dyn ExecutionPlan>,
}

#[derive(Clone, PartialEq, Message)]
struct CustomExecWithExprsProto {
    #[prost(message, repeated, tag = "1")]
    exprs: Vec<PhysicalExprNode>,
}

impl std::fmt::Debug for CustomExecWithExprs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomExecWithExprs")
            .field("exprs", &self.exprs)
            .field("child", &self.child)
            .finish()
    }
}

impl CustomExecWithExprs {
    fn new(exprs: Vec<Arc<dyn PhysicalExpr>>, child: Arc<dyn ExecutionPlan>) -> Self {
        Self { exprs, child }
    }
}

impl DisplayAs for CustomExecWithExprs {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CustomExecWithExprs")
    }
}

impl ExecutionPlan for CustomExecWithExprs {
    fn name(&self) -> &str {
        "CustomExecWithExprs"
    }

    fn schema(&self) -> SchemaRef {
        self.child.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.child.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        datafusion_physical_plan::apply_expression_roots(&self.exprs, f)
    }

    fn replace_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unreachable!()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!()
    }
}

/// A [`PhysicalExtensionCodec`] for [`CustomExecWithExprs`].
#[derive(Debug)]
struct CustomExecWithExprsCodec {}

impl PhysicalExtensionCodec for CustomExecWithExprsCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let decode_ctx = PhysicalPlanDecodeContext::new(ctx, self);
        let input_schema = inputs[0].schema();
        let proto = CustomExecWithExprsProto::decode(buf)
            .map_err(|e| internal_datafusion_err!("Failed to decode custom exec: {e}"))?;
        let exprs = proto
            .exprs
            .iter()
            .map(|expr_proto| {
                proto_converter.proto_to_physical_expr(
                    expr_proto,
                    input_schema.as_ref(),
                    &decode_ctx,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Arc::new(CustomExecWithExprs::new(exprs, inputs[0].clone())))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let custom = node
            .downcast_ref::<CustomExecWithExprs>()
            .ok_or_else(|| internal_datafusion_err!("Expected CustomExecWithExprs"))?;
        let proto = CustomExecWithExprsProto {
            exprs: custom
                .exprs
                .iter()
                .map(|expr| proto_converter.physical_expr_to_proto(expr, self))
                .collect::<Result<Vec<_>>>()?,
        };
        proto
            .encode(buf)
            .map_err(|e| internal_datafusion_err!("Failed to encode custom exec: {e}"))?;

        Ok(())
    }
}

/// Tests that a custom [`ExecutionPlan`] with [`PhysicalExpr`] can
/// dedupe dynamic filters by using the proto converter in its
/// [`PhysicalExtensionCodec`] implementation.
#[test]
fn test_custom_node_with_dynamic_filter_dedup_roundtrip() -> Result<()> {
    // Create the plan:
    //
    //   FilterExec(dynamic_filter)
    //     -> CustomExecWithExprs(exprs: [dynamic_filter])
    //         -> EmptyExec
    //
    // The same dynamic filter expression is saved in both the FilterExec and CustomExecWithExprs.
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>],
        lit(true),
    ));
    let dynamic_filter_expr: Arc<dyn PhysicalExpr> = dynamic_filter;

    let empty = Arc::new(EmptyExec::new(Arc::clone(&schema)));
    let custom_exec = Arc::new(CustomExecWithExprs::new(
        vec![Arc::clone(&dynamic_filter_expr)],
        empty,
    ));
    let filter_exec = Arc::new(FilterExec::try_new(
        Arc::clone(&dynamic_filter_expr),
        custom_exec,
    )?) as Arc<dyn ExecutionPlan>;

    // Roundtrip with DeduplicatingProtoConverter
    let codec = CustomExecWithExprsCodec {};
    let converter = DeduplicatingProtoConverter {};

    let bytes = physical_plan_to_bytes_with_proto_converter(
        Arc::clone(&filter_exec),
        &codec,
        &converter,
    )?;

    let ctx = SessionContext::new();
    let deser_converter = DeduplicatingProtoConverter {};
    let deserialized = physical_plan_from_bytes_with_proto_converter(
        bytes.as_ref(),
        ctx.task_ctx().as_ref(),
        &codec,
        &deser_converter,
    )?;

    // Extract the deserialized FilterExec's dynamic filter
    let deser_filter = deserialized
        .downcast_ref::<FilterExec>()
        .expect("Top-level should be FilterExec");
    let deser_filter_df = deser_filter.predicate();

    // Extract the deserialized custom node's dynamic filter
    let deser_custom = deser_filter
        .input()
        .downcast_ref::<CustomExecWithExprs>()
        .expect("FilterExec child should be CustomExecWithExprs");
    assert_eq!(deser_custom.exprs.len(), 1, "Should have one expression");
    let [deser_custom_df] = deser_custom.exprs.as_slice() else {
        return internal_err!("Custom node should have one expression");
    };

    // Pass the un-remapped filter first so the helper's `with_new_children`
    // rewrite can reconstruct the remapped form on the other side.
    assert_dynamic_filters_equal(deser_custom_df, deser_filter_df);
    assert_dynamic_filter_update_is_visible(deser_custom_df, deser_filter_df)?;

    Ok(())
}

/// A custom `PhysicalExpr` whose extension codec embeds a nested
/// `PhysicalExprNode` *inside its own blob* (rather than the standard
/// `PhysicalExtensionExprNode.inputs` field). This is the case that only
/// works if the expr-level codec methods receive the encode/decode context.
#[derive(Debug)]
struct WrapperExpr {
    inner: Arc<dyn PhysicalExpr>,
}

impl Display for WrapperExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WrapperExpr({})", self.inner)
    }
}

impl PartialEq for WrapperExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Eq for WrapperExpr {}

impl std::hash::Hash for WrapperExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl PhysicalExpr for WrapperExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.inner.data_type(input_schema)
    }
    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.inner.nullable(input_schema)
    }
    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        internal_err!("WrapperExpr is not executable in this test")
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(WrapperExpr {
            inner: Arc::clone(&children[0]),
        }))
    }
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

/// Wire layout for [`WrapperExpr`]: a single nested `PhysicalExprNode`.
#[derive(Clone, PartialEq, prost::Message)]
struct WrapperExprProto {
    #[prost(message, optional, boxed, tag = "1")]
    inner: Option<Box<PhysicalExprNode>>,
}

#[derive(Debug)]
struct WrapperCodec;

impl PhysicalExtensionCodec for WrapperCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        internal_err!("not used")
    }
    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        internal_err!("not used")
    }
    fn try_decode_expr(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn PhysicalExpr>],
        ctx: &PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let proto = WrapperExprProto::decode(buf)
            .map_err(|e| internal_datafusion_err!("decode WrapperExprProto: {e}"))?;
        let inner_proto = proto
            .inner
            .ok_or_else(|| internal_datafusion_err!("missing inner"))?;
        // Decode the nested expr through the context so it resolves against
        // the real schema/registry AND participates in dedup — no fabricated
        // `SessionContext` or hard-coded schema required.
        let inner = ctx.decode(&inner_proto)?;
        Ok(Arc::new(WrapperExpr { inner }))
    }
    fn try_encode_expr(
        &self,
        node: &Arc<dyn PhysicalExpr>,
        buf: &mut Vec<u8>,
        ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> Result<()> {
        let wrapper = node
            .downcast_ref::<WrapperExpr>()
            .ok_or_else(|| internal_datafusion_err!("not WrapperExpr"))?;
        // Encode the nested expr through the context so an active
        // `DeduplicatingProtoConverter` stamps a matching `expr_id`.
        let inner_proto = ctx.encode_child(&wrapper.inner)?;
        let proto = WrapperExprProto {
            inner: Some(Box::new(inner_proto)),
        };
        proto
            .encode(buf)
            .map_err(|e| internal_datafusion_err!("encode WrapperExprProto: {e}"))?;
        Ok(())
    }
}

/// A `DynamicFilterPhysicalExpr` referenced both as a bare expression and
/// nested inside a custom expression's codec blob must reconstruct to a
/// single shared `Inner` after roundtrip.
///
/// This exercises the expr-level codec hooks receiving the encode/decode
/// context: `try_encode_expr` routes its nested `PhysicalExprNode` through
/// `ctx.encode_child` and `try_decode_expr` through `ctx.decode`, so the
/// nested filter picks up the same `DeduplicatingProtoConverter` /
/// `DeduplicatingDeserializer` cache as the bare reference. Without the
/// context the nested expr would serialize with `expr_id: None` and decode
/// into a distinct `Inner`, breaking heap-max propagation across the
/// extension boundary in distributed execution.
#[test]
fn extension_codec_expr_participates_in_deduplication() -> Result<()> {
    use prost::Message;

    // A single composite expression holding TWO references to the same
    // dynamic filter: bare on the left of an AND, wrapped on the right.
    let dyn_filter = make_dynamic_filter();
    let wrapper: Arc<dyn PhysicalExpr> = Arc::new(WrapperExpr {
        inner: Arc::clone(&dyn_filter),
    });
    let composite: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::clone(&dyn_filter),
        Operator::And,
        Arc::clone(&wrapper),
    ));

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let codec = WrapperCodec;
    let converter = DeduplicatingProtoConverter {};

    // Encode, then round-trip through prost bytes to mimic the wire.
    let proto = converter.physical_expr_to_proto(&composite, &codec)?;
    let bytes = proto.encode_to_vec();
    let decoded_proto = PhysicalExprNode::decode(bytes.as_slice()).unwrap();

    let ctx = SessionContext::new();
    let task_ctx = ctx.task_ctx();
    let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx.as_ref(), &codec);
    let decoded =
        converter.proto_to_physical_expr(&decoded_proto, &schema, &decode_ctx)?;

    let binary = decoded
        .downcast_ref::<BinaryExpr>()
        .expect("must decode back to BinaryExpr");
    let decoded_left = Arc::clone(binary.left());
    let decoded_right = Arc::clone(binary.right());
    let decoded_wrapper = decoded_right
        .downcast_ref::<WrapperExpr>()
        .expect("right side must decode back to WrapperExpr");

    // The load-bearing check: an `update()` on the bare-side filter must be
    // observable from the wrapped-side filter, proving both refs back the
    // same `Inner`.
    assert_dynamic_filter_update_is_visible(&decoded_left, &decoded_wrapper.inner)?;

    Ok(())
}
