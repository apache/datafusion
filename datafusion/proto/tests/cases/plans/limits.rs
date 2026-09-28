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

//! Plans that carry a row limit or shape the batch pipeline: the limit
//! execs, the coalescing execs, `BufferExec` and `CooperativeExec`.

use super::{roundtrip_test, roundtrip_test_and_return};
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion::physical_plan::buffer::BufferExec;
#[expect(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{PhysicalSortExpr, col};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions,
};
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter,
};
#[cfg(target_pointer_width = "32")]
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::PhysicalPlanNode;
use std::sync::Arc;
use std::vec;

#[test]
fn roundtrip_local_limit() -> Result<()> {
    roundtrip_test(Arc::new(LocalLimitExec::new(
        Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
        25,
    )))
}

#[test]
fn roundtrip_global_limit() -> Result<()> {
    roundtrip_test(Arc::new(GlobalLimitExec::new(
        Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
        0,
        Some(25),
    )))
}

#[test]
fn roundtrip_global_skip_no_limit() -> Result<()> {
    roundtrip_test(Arc::new(GlobalLimitExec::new(
        Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
        10,
        None, // no limit
    )))
}

#[cfg(target_pointer_width = "64")]
#[test]
fn local_limit_rejects_fetch_above_wire_range() {
    let fetch = u32::MAX as usize + 1;
    let plan: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(
        Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
        fetch,
    ));

    let err =
        PhysicalPlanNode::try_from_physical_plan(plan, &DefaultPhysicalExtensionCodec {})
            .unwrap_err();
    assert_eq!(
        err.strip_backtrace(),
        format!(
            "Error during planning: LocalLimitExec: fetch value {fetch} is out of range for the plan wire format"
        )
    );
}

#[cfg(target_pointer_width = "32")]
#[test]
fn global_limit_rejects_fetch_above_usize() -> Result<()> {
    let codec = DefaultPhysicalExtensionCodec {};
    let plan: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(
        Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
        0,
        Some(1),
    ));
    let mut node = PhysicalPlanNode::try_from_physical_plan(plan, &codec)?;
    let Some(protobuf::physical_plan_node::PhysicalPlanType::GlobalLimit(limit)) =
        &mut node.physical_plan_type
    else {
        panic!("expected GlobalLimitExecNode");
    };
    limit.fetch = i64::from(u32::MAX) + 1;

    let ctx = SessionContext::new();
    let err = node
        .try_into_physical_plan(&ctx.task_ctx(), &codec)
        .unwrap_err();
    assert_eq!(
        err.strip_backtrace(),
        "Error during planning: GlobalLimitExec: fetch wire value 4294967296 is out of range for usize"
    );
    Ok(())
}

/// Sort key at index 1, so a decoder that misbinds column name vs index
/// cannot pass.
fn limit_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]))
}

/// Non-default sort options, so a decode that falls back to defaults cannot
/// pass.
fn limit_required_ordering(schema: &Schema) -> Result<Option<LexOrdering>> {
    Ok(LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("b", schema)?,
        options: SortOptions {
            descending: true,
            nulls_first: false,
        },
    }]))
}

#[test]
fn roundtrip_limit_with_required_ordering() -> Result<()> {
    let schema = limit_test_schema();
    let required_ordering = limit_required_ordering(&schema)?;
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};

    let mut global =
        GlobalLimitExec::new(Arc::new(EmptyExec::new(Arc::clone(&schema))), 3, Some(25));
    global.set_required_ordering(required_ordering.clone());
    let decoded =
        roundtrip_test_and_return(Arc::new(global), &ctx, &codec, &proto_converter)?;
    let decoded = decoded
        .downcast_ref::<GlobalLimitExec>()
        .expect("expected GlobalLimitExec");
    assert_eq!(decoded.required_ordering(), &required_ordering);

    let mut local = LocalLimitExec::new(Arc::new(EmptyExec::new(schema)), 25);
    local.set_required_ordering(required_ordering.clone());
    let decoded =
        roundtrip_test_and_return(Arc::new(local), &ctx, &codec, &proto_converter)?;
    let decoded = decoded
        .downcast_ref::<LocalLimitExec>()
        .expect("expected LocalLimitExec");
    assert_eq!(decoded.required_ordering(), &required_ordering);
    Ok(())
}

/// A limit's `required_ordering` is the only record that an `ORDER BY ... LIMIT`
/// whose sort node was optimized away is order-sensitive, so it must survive
/// serde all the way into the scan's `preserve_order` flag.
#[test]
fn roundtrip_limit_required_ordering_reaches_data_source() -> Result<()> {
    let file_schema = limit_test_schema();
    let make_scan = || {
        let file_source = Arc::new(ParquetSource::new(Arc::clone(&file_schema)));
        let scan_config =
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
                .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                    "/path/to/file.parquet".to_string(),
                    1024,
                )])])
                .build();
        DataSourceExec::from_data_source(scan_config)
    };
    let scan_after_limit_pushdown = |limit: GlobalLimitExec| -> Result<FileScanConfig> {
        let ctx = SessionContext::new();
        let codec = DefaultPhysicalExtensionCodec {};
        let proto_converter = DefaultPhysicalProtoConverter {};
        let decoded =
            roundtrip_test_and_return(Arc::new(limit), &ctx, &codec, &proto_converter)?;

        // Child replacement must not erase the decoded ordering before pushdown.
        let rebuilt = decoded.replace_children(
            vec![make_scan()],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;

        let optimized =
            LimitPushdown::new().optimize(rebuilt, &ConfigOptions::default())?;
        let scan = optimized
            .downcast_ref::<DataSourceExec>()
            .expect("limit should be absorbed into the scan");
        Ok(scan
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .expect("expected FileScanConfig")
            .clone())
    };

    let mut limit = GlobalLimitExec::new(make_scan(), 0, Some(10));
    limit.set_required_ordering(limit_required_ordering(&file_schema)?);
    let scan_config = scan_after_limit_pushdown(limit)?;
    assert_eq!(scan_config.limit, Some(10));
    assert!(scan_config.preserve_order);

    let scan_config =
        scan_after_limit_pushdown(GlobalLimitExec::new(make_scan(), 0, Some(10)))?;
    assert_eq!(scan_config.limit, Some(10));
    assert!(!scan_config.preserve_order);
    Ok(())
}

#[test]
fn roundtrip_coalesce_batches_with_fetch() -> Result<()> {
    let field_a = Field::new("a", DataType::Boolean, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    #[expect(deprecated)]
    roundtrip_test(Arc::new(CoalesceBatchesExec::new(
        Arc::new(EmptyExec::new(schema.clone())),
        8096,
    )))?;

    #[expect(deprecated)]
    roundtrip_test(Arc::new(
        CoalesceBatchesExec::new(Arc::new(EmptyExec::new(schema)), 8096)
            .with_fetch(Some(10)),
    ))
}

#[test]
#[expect(deprecated)]
fn coalesce_batches_rejects_zero_batch_size_on_encode() {
    let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalesceBatchesExec::new(
        Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
        0,
    ));

    let err =
        PhysicalPlanNode::try_from_physical_plan(plan, &DefaultPhysicalExtensionCodec {})
            .unwrap_err();
    assert!(
        err.to_string()
            .contains("CoalesceBatchesExec: target_batch_size must be greater than 0")
    );
}

#[test]
fn roundtrip_coalesce_partitions_with_fetch() -> Result<()> {
    let field_a = Field::new("a", DataType::Boolean, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    roundtrip_test(Arc::new(CoalescePartitionsExec::new(Arc::new(
        EmptyExec::new(schema.clone()),
    ))))?;

    roundtrip_test(Arc::new(
        CoalescePartitionsExec::new(Arc::new(EmptyExec::new(schema)))
            .with_fetch(Some(10)),
    ))
}

#[test]
fn roundtrip_cooperative() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, false)]));
    roundtrip_test(Arc::new(CooperativeExec::new(Arc::new(EmptyExec::new(
        schema,
    )))))
}

#[test]
fn roundtrip_buffer() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, false)]));
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let result = roundtrip_test_and_return(
        Arc::new(BufferExec::new(Arc::new(EmptyExec::new(schema)), 4096)),
        &ctx,
        &codec,
        &proto_converter,
    )?;
    let result = result.downcast_ref::<BufferExec>().unwrap();
    assert_eq!(result.capacity(), 4096);
    Ok(())
}
