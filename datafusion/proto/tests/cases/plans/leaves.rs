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

//! Leaf plans: `EmptyExec` and `PlaceholderRowExec`.

use super::{roundtrip_test, roundtrip_test_and_return};
use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter,
};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::PhysicalPlanNode;
use std::sync::Arc;

#[test]
fn roundtrip_empty() -> Result<()> {
    roundtrip_test(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))))
}

#[test]
fn roundtrip_empty_with_partitions() -> Result<()> {
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let plan = Arc::new(EmptyExec::new(Arc::new(Schema::empty())).with_partitions(4));
    let plan = roundtrip_test_and_return(plan, &ctx, &codec, &proto_converter)?;
    assert_eq!(plan.output_partitioning().partition_count(), 4);
    Ok(())
}

#[test]
fn roundtrip_placeholder_row_with_partitions() -> Result<()> {
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let plan =
        Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty())).with_partitions(4));
    let plan = roundtrip_test_and_return(plan, &ctx, &codec, &proto_converter)?;
    assert_eq!(plan.output_partitioning().partition_count(), 4);
    Ok(())
}

#[cfg(target_pointer_width = "64")]
#[test]
fn leaf_plans_reject_partition_counts_above_wire_range() {
    let partitions = u32::MAX as usize + 1;
    let schema = Arc::new(Schema::empty());
    let plans: [(Arc<dyn ExecutionPlan>, &str); 2] = [
        (
            Arc::new(EmptyExec::new(Arc::clone(&schema)).with_partitions(partitions)),
            "EmptyExec",
        ),
        (
            Arc::new(PlaceholderRowExec::new(schema).with_partitions(partitions)),
            "PlaceholderRowExec",
        ),
    ];

    for (plan, name) in plans {
        let err = PhysicalPlanNode::try_from_physical_plan(
            plan,
            &DefaultPhysicalExtensionCodec {},
        )
        .unwrap_err();
        assert!(err.to_string().contains(name));
        assert!(err.to_string().contains("partitions"));
    }
}

/// Plans encoded before `partitions` was added carry no value for it, which
/// decodes as zero and must be treated as the previous default of one.
#[test]
fn decode_empty_and_placeholder_row_without_partitions() -> Result<()> {
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let schema: protobuf::Schema = (&Schema::empty()).try_into()?;

    for physical_plan_type in [
        protobuf::physical_plan_node::PhysicalPlanType::Empty(protobuf::EmptyExecNode {
            schema: Some(schema.clone()),
            partitions: 0,
        }),
        protobuf::physical_plan_node::PhysicalPlanType::PlaceholderRow(
            protobuf::PlaceholderRowExecNode {
                schema: Some(schema.clone()),
                partitions: 0,
            },
        ),
    ] {
        let node = PhysicalPlanNode {
            physical_plan_type: Some(physical_plan_type),
        };
        let plan = node.try_into_physical_plan(ctx.task_ctx().as_ref(), &codec)?;
        assert_eq!(plan.output_partitioning().partition_count(), 1);
    }
    Ok(())
}
