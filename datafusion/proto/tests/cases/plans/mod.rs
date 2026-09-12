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

//! Round trip tests for the physical plan protobuf representation.
//!
//! The tests are grouped by the kind of plan they cover; the shared
//! round trip helpers live here.

use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_common::Result;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_proto_converter,
    physical_plan_to_bytes_with_proto_converter,
};
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
    PhysicalProtoConverterExtension,
};
use std::sync::Arc;

mod aggregates;
mod dispatch;
mod dynamic_filters;
mod expr_registry;
mod exprs;
mod filters;
mod joins;
mod leaves;
mod limits;
mod misc;
mod scalar_subquery;
mod sinks;
mod sorts;
mod sources;
mod tpch;
mod udfs;
mod windows;

/// Perform a serde roundtrip and assert that the string representation of the before and after plans
/// are identical. Note that this often isn't sufficient to guarantee that no information is
/// lost during serde because the string representation of a plan often only shows a subset of state.
fn roundtrip_test(exec_plan: Arc<dyn ExecutionPlan>) -> Result<()> {
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    roundtrip_test_and_return(exec_plan, &ctx, &codec, &proto_converter)?;
    Ok(())
}

/// Perform a serde roundtrip and assert that the string representation of the before and after plans
/// are identical. Note that this often isn't sufficient to guarantee that no information is
/// lost during serde because the string representation of a plan often only shows a subset of state.
///
/// This version of the roundtrip_test method returns the final plan after serde so that it can be inspected
/// farther in tests.
fn roundtrip_test_and_return(
    exec_plan: Arc<dyn ExecutionPlan>,
    ctx: &SessionContext,
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<Arc<dyn ExecutionPlan>> {
    let bytes = physical_plan_to_bytes_with_proto_converter(
        Arc::clone(&exec_plan),
        codec,
        proto_converter,
    )?;
    let result_exec_plan = physical_plan_from_bytes_with_proto_converter(
        bytes.as_ref(),
        ctx.task_ctx().as_ref(),
        codec,
        proto_converter,
    )?;

    pretty_assertions::assert_eq!(
        format!("{exec_plan:?}"),
        format!("{result_exec_plan:?}")
    );
    Ok(result_exec_plan)
}

/// Perform a serde roundtrip and assert that the string representation of the before and after plans
/// are identical. Note that this often isn't sufficient to guarantee that no information is
/// lost during serde because the string representation of a plan often only shows a subset of state.
///
/// This version of the roundtrip_test function accepts a SessionContext, which is required when
/// performing serde on some plans.
fn roundtrip_test_with_context(
    exec_plan: Arc<dyn ExecutionPlan>,
    ctx: &SessionContext,
) -> Result<()> {
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    roundtrip_test_and_return(exec_plan, ctx, &codec, &proto_converter)?;
    Ok(())
}

/// Perform a serde roundtrip for the specified sql query, and  assert that
/// query results are identical.
async fn roundtrip_test_sql_with_context(sql: &str, ctx: &SessionContext) -> Result<()> {
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let initial_plan = ctx.sql(sql).await?.create_physical_plan().await?;

    roundtrip_test_and_return(initial_plan, ctx, &codec, &proto_converter)?;
    Ok(())
}

/// returns a SessionContext with `alltypes_plain` registered
async fn all_types_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();

    let testdata = datafusion::test_util::parquet_test_data();
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{testdata}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    Ok(ctx)
}
