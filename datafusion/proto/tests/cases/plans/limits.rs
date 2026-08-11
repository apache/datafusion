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
use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter,
};
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
            ReplaceChildrenOptions {
                children_properties: ChildrenPropertiesMode::Recompute,
            },
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
