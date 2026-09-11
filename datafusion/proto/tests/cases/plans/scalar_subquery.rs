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

//! `ScalarSubqueryExec` and the results it scopes to its subtree.

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{BinaryExpr, binary, col};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::scalar_subquery::{
    ScalarSubqueryExec, ScalarSubqueryLink,
};
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_expr::physical_planning_context::{ScalarSubqueryResults, SubqueryIndex};
use datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_proto_converter,
    physical_plan_to_bytes_with_proto_converter,
};
use datafusion_proto::physical_plan::{
    DeduplicatingProtoConverter, DefaultPhysicalExtensionCodec,
};
use std::sync::Arc;
use std::vec;

/// Verify that ScalarSubqueryExpr nodes in the input plan are connected to the
/// same shared results container as ScalarSubqueryExec after a proto round-trip.
#[test]
fn roundtrip_scalar_subquery_exec() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let results = ScalarSubqueryResults::new(1);

    // Build the input plan: a filter whose predicate references the
    // scalar subquery result via ScalarSubqueryExpr.
    let sq_expr = Arc::new(ScalarSubqueryExpr::new(
        DataType::Int64,
        true,
        SubqueryIndex::new(0),
        results.clone(),
    ));
    let predicate = binary(col("a", &schema)?, Operator::Eq, sq_expr, &schema)?;
    let filter =
        FilterExec::try_new(predicate, Arc::new(EmptyExec::new(schema.clone())))?;

    // Build a trivial subquery plan.
    let subquery_plan =
        Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Int64,
            true,
        )]))));

    let exec: Arc<dyn ExecutionPlan> = Arc::new(ScalarSubqueryExec::new(
        Arc::new(filter),
        vec![ScalarSubqueryLink {
            plan: subquery_plan,
            index: SubqueryIndex::new(0),
        }],
        results,
    ));

    // Perform the round-trip using DeduplicatingProtoConverter, which
    // creates a DeduplicatingDeserializer that threads scalar subquery
    // results through expression deserialization.
    let codec = DefaultPhysicalExtensionCodec {};
    let converter = DeduplicatingProtoConverter {};
    let bytes = physical_plan_to_bytes_with_proto_converter(
        Arc::clone(&exec),
        &codec,
        &converter,
    )?;
    let ctx = SessionContext::new();
    let deserialized = physical_plan_from_bytes_with_proto_converter(
        bytes.as_ref(),
        ctx.task_ctx().as_ref(),
        &codec,
        &converter,
    )?;

    // Verify the deserialized ScalarSubqueryExec's results container is
    // shared with the ScalarSubqueryExpr in the input plan.
    let sq_exec = deserialized
        .downcast_ref::<ScalarSubqueryExec>()
        .expect("expected ScalarSubqueryExec");
    let exec_results = sq_exec.results();

    // Walk the input plan to find the ScalarSubqueryExpr and verify it
    // points to the same results container.
    let filter_exec = sq_exec
        .input()
        .downcast_ref::<FilterExec>()
        .expect("expected FilterExec");
    let binary_expr = filter_exec
        .predicate()
        .downcast_ref::<BinaryExpr>()
        .expect("expected BinaryExpr");
    let deserialized_sq_expr = binary_expr
        .right()
        .downcast_ref::<ScalarSubqueryExpr>()
        .expect("expected ScalarSubqueryExpr");

    assert!(
        ScalarSubqueryResults::ptr_eq(exec_results, deserialized_sq_expr.results()),
        "ScalarSubqueryExpr should share the same results container as ScalarSubqueryExec"
    );
    Ok(())
}

/// Verify that nested ScalarSubqueryExec nodes deserialize with distinct
/// scoped results containers, and that each ScalarSubqueryExpr is wired to the
/// container for its own surrounding ScalarSubqueryExec.
#[test]
fn roundtrip_nested_scalar_subquery_exec_scopes_results() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let subquery_schema =
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));

    let inner_results = ScalarSubqueryResults::new(1);
    let inner_sq_expr = Arc::new(ScalarSubqueryExpr::new(
        DataType::Int64,
        true,
        SubqueryIndex::new(0),
        inner_results.clone(),
    ));
    let inner_predicate =
        binary(col("a", &schema)?, Operator::Eq, inner_sq_expr, &schema)?;
    let inner_filter = Arc::new(FilterExec::try_new(
        inner_predicate,
        Arc::new(EmptyExec::new(schema.clone())),
    )?);
    let inner_exec: Arc<dyn ExecutionPlan> = Arc::new(ScalarSubqueryExec::new(
        inner_filter,
        vec![ScalarSubqueryLink {
            plan: Arc::new(EmptyExec::new(subquery_schema.clone())),
            index: SubqueryIndex::new(0),
        }],
        inner_results,
    ));

    let outer_results = ScalarSubqueryResults::new(1);
    let outer_sq_expr = Arc::new(ScalarSubqueryExpr::new(
        DataType::Int64,
        true,
        SubqueryIndex::new(0),
        outer_results.clone(),
    ));
    let outer_predicate =
        binary(col("a", &schema)?, Operator::Eq, outer_sq_expr, &schema)?;
    let outer_filter = Arc::new(FilterExec::try_new(outer_predicate, inner_exec)?);
    let outer_exec: Arc<dyn ExecutionPlan> = Arc::new(ScalarSubqueryExec::new(
        outer_filter,
        vec![ScalarSubqueryLink {
            plan: Arc::new(EmptyExec::new(subquery_schema)),
            index: SubqueryIndex::new(0),
        }],
        outer_results,
    ));

    let bytes = datafusion_proto::bytes::physical_plan_to_bytes(Arc::clone(&outer_exec))?;
    let ctx = SessionContext::new();
    let deserialized = datafusion_proto::bytes::physical_plan_from_bytes(
        bytes.as_ref(),
        ctx.task_ctx().as_ref(),
    )?;

    let outer_exec = deserialized
        .downcast_ref::<ScalarSubqueryExec>()
        .expect("expected outer ScalarSubqueryExec");
    let outer_results = outer_exec.results();
    let outer_filter = outer_exec
        .input()
        .downcast_ref::<FilterExec>()
        .expect("expected outer FilterExec");
    let outer_binary = outer_filter
        .predicate()
        .downcast_ref::<BinaryExpr>()
        .expect("expected outer BinaryExpr");
    let outer_sq_expr = outer_binary
        .right()
        .downcast_ref::<ScalarSubqueryExpr>()
        .expect("expected outer ScalarSubqueryExpr");

    let inner_exec = outer_filter
        .input()
        .downcast_ref::<ScalarSubqueryExec>()
        .expect("expected inner ScalarSubqueryExec");
    let inner_results = inner_exec.results();
    let inner_filter = inner_exec
        .input()
        .downcast_ref::<FilterExec>()
        .expect("expected inner FilterExec");
    let inner_binary = inner_filter
        .predicate()
        .downcast_ref::<BinaryExpr>()
        .expect("expected inner BinaryExpr");
    let inner_sq_expr = inner_binary
        .right()
        .downcast_ref::<ScalarSubqueryExpr>()
        .expect("expected inner ScalarSubqueryExpr");

    assert!(
        ScalarSubqueryResults::ptr_eq(outer_results, outer_sq_expr.results()),
        "outer ScalarSubqueryExpr should use outer ScalarSubqueryExec results"
    );
    assert!(
        ScalarSubqueryResults::ptr_eq(inner_results, inner_sq_expr.results()),
        "inner ScalarSubqueryExpr should use inner ScalarSubqueryExec results"
    );
    assert!(
        !ScalarSubqueryResults::ptr_eq(outer_results, inner_results),
        "nested ScalarSubqueryExec nodes should not share results containers"
    );
    assert!(
        !ScalarSubqueryResults::ptr_eq(outer_results, inner_sq_expr.results()),
        "inner ScalarSubqueryExpr must not read from outer results"
    );
    assert!(
        !ScalarSubqueryResults::ptr_eq(inner_results, outer_sq_expr.results()),
        "outer ScalarSubqueryExpr must not read from inner results"
    );

    Ok(())
}

/// Verify that the default physical plan bytes round-trip preserves executable
/// scalar subquery plans.
#[tokio::test]
async fn roundtrip_scalar_subquery_exec_with_default_converter_executes() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT x + (SELECT max(y) FROM (VALUES (10), (20)) AS u(y)) AS s \
               FROM (VALUES (2), (1)) AS t(x) \
               ORDER BY s";

    let initial_plan = ctx.sql(sql).await?.create_physical_plan().await?;
    assert!(
        format!("{initial_plan:?}").contains("ScalarSubqueryExec"),
        "expected ScalarSubqueryExec in plan:\n{initial_plan:?}"
    );

    let bytes =
        datafusion_proto::bytes::physical_plan_to_bytes(Arc::clone(&initial_plan))?;
    let roundtripped = datafusion_proto::bytes::physical_plan_from_bytes(
        bytes.as_ref(),
        ctx.task_ctx().as_ref(),
    )?;
    assert!(
        format!("{roundtripped:?}").contains("ScalarSubqueryExec"),
        "expected ScalarSubqueryExec after roundtrip:\n{roundtripped:?}"
    );

    let batches = datafusion::physical_plan::common::collect(
        roundtripped.execute(0, ctx.task_ctx())?,
    )
    .await?;
    datafusion::assert_batches_eq!(
        &["+----+", "| s  |", "+----+", "| 21 |", "| 22 |", "+----+",],
        &batches
    );

    Ok(())
}
