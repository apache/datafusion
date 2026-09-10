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

//! End to end round trips of the TPC-H queries, plus the human readable
//! display of the plans they produce.

use super::{roundtrip_test_and_return, roundtrip_test_sql_with_context};
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::functions_aggregate::first_last::first_value_udaf;
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{PhysicalSortExpr, col};
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result};
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter,
};
use datafusion_proto::protobuf::PhysicalPlanNode;
use std::sync::Arc;
use std::vec;

/// Helper function to create a SessionContext with all TPC-H tables registered as external tables
async fn tpch_context() -> Result<SessionContext> {
    use datafusion_common::test_util::datafusion_test_data;

    let ctx = SessionContext::new();
    let test_data = datafusion_test_data();

    // TPC-H table names
    let tables = [
        "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation",
        "region",
    ];

    // Create external tables for all TPC-H tables
    for table in &tables {
        let table_sql = format!(
            "CREATE EXTERNAL TABLE {table} STORED AS PARQUET LOCATION '{test_data}/tpch_{table}_small.parquet'"
        );
        ctx.sql(&table_sql).await.map_err(|e| {
            DataFusionError::External(
                format!("Failed to create {table} table: {e}").into(),
            )
        })?;
    }

    Ok(ctx)
}

/// Helper function to get TPC-H query SQL
fn get_tpch_query_sql(query: usize) -> Result<Vec<String>> {
    use std::fs;

    if !(1..=22).contains(&query) {
        return Err(DataFusionError::External(
            format!("Invalid TPC-H query number: {query}").into(),
        ));
    }

    let filename = format!("../../benchmarks/queries/q{query}.sql");
    let contents = fs::read_to_string(&filename).map_err(|e| {
        DataFusionError::External(
            format!("Failed to read query file {filename}: {e}").into(),
        )
    })?;

    Ok(contents
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect())
}

#[tokio::test]
async fn test_serialize_deserialize_tpch_queries() -> Result<()> {
    // Create context with TPC-H tables
    let ctx = tpch_context().await?;

    // repeat to run all 22 queries
    for query in 1..=22 {
        // run all statements in the query
        let sql = get_tpch_query_sql(query)?;
        for stmt in sql {
            let logical_plan = ctx.sql(&stmt).await?.into_unoptimized_plan();
            let optimized_plan = ctx.state().optimize(&logical_plan)?;
            let physical_plan = ctx.state().create_physical_plan(&optimized_plan).await?;

            // serialize the physical plan
            let codec = DefaultPhysicalExtensionCodec {};

            let proto =
                PhysicalPlanNode::try_from_physical_plan(physical_plan.clone(), &codec)?;

            // deserialize the physical plan
            let _deserialized_plan =
                proto.try_into_physical_plan(&ctx.task_ctx(), &codec)?;
        }
    }

    Ok(())
}

// Bugs: https://github.com/apache/datafusion/issues/16772
#[tokio::test]
async fn test_round_trip_tpch_queries() -> Result<()> {
    // Create context with TPC-H tables
    let ctx = tpch_context().await?;

    // repeat to run all 22 queries
    for query in 1..=22 {
        // run all statements in the query
        let sql = get_tpch_query_sql(query)?;
        for stmt in sql {
            roundtrip_test_sql_with_context(&stmt, &ctx).await?;
        }
    }

    Ok(())
}

// Bug 1 of https://github.com/apache/datafusion/issues/16772
/// Test that AggregateFunctionExpr human_display field is correctly preserved
/// during serialization/deserialization roundtrip.
///
/// Test for issue where the human_display field (used for EXPLAIN output)
/// was not being serialized to protobuf, causing it to be lost during roundtrip
/// and resulting in empty or incorrect display strings in query plans.
#[tokio::test]
async fn test_round_trip_human_display() -> Result<()> {
    // Create context with TPC-H tables
    let ctx = tpch_context().await?;

    let sql = "select r_name, count(1) from region group by r_name";
    roundtrip_test_sql_with_context(sql, &ctx).await?;

    let sql = "select r_name, count(*) from region group by r_name";
    roundtrip_test_sql_with_context(sql, &ctx).await?;

    let sql = "select r_name, count(r_name) from region group by r_name";
    roundtrip_test_sql_with_context(sql, &ctx).await?;

    let sql = "select count(*) as count_star from region";
    roundtrip_test_sql_with_context(sql, &ctx).await?;

    Ok(())
}

#[test]
fn test_round_trip_aliased_reverse_human_display() -> Result<()> {
    let aggregate_expr = roundtrip_first_value_aggregate(
        "agg",
        "first_value(b) ORDER BY [b ASC NULLS LAST]",
        Some("agg"),
    )?;
    let reversed = aggregate_expr
        .reverse_expr()
        .expect("expected reverse expr");

    assert_eq!(reversed.name(), "agg");
    assert_eq!(reversed.human_display_alias(), Some("agg"));
    assert_eq!(
        reversed.human_display(),
        Some("last_value(b) ORDER BY [b DESC NULLS FIRST]")
    );

    Ok(())
}

#[test]
fn test_round_trip_human_display_alias_with_colon() -> Result<()> {
    let aggregate_expr = roundtrip_first_value_aggregate(
        "agg:one",
        "first_value(b) ORDER BY [b ASC NULLS LAST]",
        Some("agg:one"),
    )?;

    assert_eq!(aggregate_expr.name(), "agg:one");
    assert_eq!(aggregate_expr.human_display_alias(), Some("agg:one"));
    assert_eq!(
        aggregate_expr.human_display(),
        Some("first_value(b) ORDER BY [b ASC NULLS LAST]")
    );

    Ok(())
}

#[test]
fn test_round_trip_non_aliased_human_display_ending_like_alias() -> Result<()> {
    let aggregate_expr =
        roundtrip_first_value_aggregate("agg", "first_value(b) as agg", None)?;

    assert_eq!(aggregate_expr.name(), "agg");
    assert_eq!(
        aggregate_expr.human_display(),
        Some("first_value(b) as agg")
    );
    assert_eq!(aggregate_expr.human_display_alias(), None);

    Ok(())
}

fn roundtrip_first_value_aggregate(
    alias: &str,
    human_display: &str,
    human_display_alias: Option<&str>,
) -> Result<Arc<AggregateFunctionExpr>> {
    let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, true)]));
    let mut builder =
        AggregateExprBuilder::new(first_value_udaf(), vec![col("b", &schema)?])
            .order_by(vec![PhysicalSortExpr {
                expr: col("b", &schema)?,
                options: SortOptions::new(false, false),
            }])
            .schema(Arc::clone(&schema))
            .alias(alias)
            .human_display(human_display);
    if let Some(human_display_alias) = human_display_alias {
        builder = builder.human_display_alias(human_display_alias);
    }
    let agg_expr = builder.build().map(Arc::new)?;

    let plan = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new(vec![], vec![], vec![], false),
        vec![agg_expr],
        vec![None],
        Arc::new(EmptyExec::new(Arc::clone(&schema))),
        schema,
    )?);

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let roundtrip_plan = roundtrip_test_and_return(plan, &ctx, &codec, &proto_converter)?;
    let aggregate = roundtrip_plan
        .as_ref()
        .downcast_ref::<AggregateExec>()
        .expect("expected AggregateExec after roundtrip");

    Ok(Arc::clone(&aggregate.aggr_expr()[0]))
}

// Bug 2 of https://github.com/apache/datafusion/issues/16772
/// Test that PhysicalGroupBy groups field is correctly serialized/deserialized
/// for simple aggregates (no GROUP BY clause).
///
/// Test for issue where simple aggregates like "SELECT SUM(col1 * col2) FROM table"
/// would incorrectly serialize groups as [[]] instead of [] during roundtrip serialization.
/// The groups field should be empty ([]) when there are no GROUP BY expressions.
#[tokio::test]
async fn test_round_trip_groups_display() -> Result<()> {
    // Create context with TPC-H tables
    let ctx = tpch_context().await?;

    let sql = "select sum(l_extendedprice * l_discount) as revenue from lineitem;";
    roundtrip_test_sql_with_context(sql, &ctx).await?;

    let sql = "select sum(l_extendedprice) as revenue from lineitem;";
    roundtrip_test_sql_with_context(sql, &ctx).await?;

    Ok(())
}

// Bug 3 of https://github.com/apache/datafusion/issues/16772
/// Test that ScalarFunctionExpr return_field name is correctly preserved
/// during serialization/deserialization roundtrip.
///
/// Test for issue where the return_field.name for scalar functions
/// was not being serialized to protobuf, causing it to be lost during roundtrip
/// and defaulting to a generic name like "f" instead of the proper function name.
#[tokio::test]
async fn test_round_trip_date_part_display() -> Result<()> {
    // Create context with TPC-H tables
    let ctx = tpch_context().await?;

    let sql = "select extract(year from l_shipdate) as l_year from lineitem ";
    roundtrip_test_sql_with_context(sql, &ctx).await?;

    let sql = "select extract(month from l_shipdate) as l_year from lineitem ";
    roundtrip_test_sql_with_context(sql, &ctx).await?;

    Ok(())
}

#[tokio::test]
async fn test_tpch_part_in_list_query_with_real_parquet_data() -> Result<()> {
    use datafusion_common::test_util::datafusion_test_data;

    let ctx = SessionContext::new();

    // Register the TPC-H part table using the local test data
    let test_data = datafusion_test_data();
    let table_sql = format!(
        "CREATE EXTERNAL TABLE part STORED AS PARQUET LOCATION '{test_data}/tpch_part_small.parquet'"
    );
    ctx.sql(&table_sql).await.map_err(|e| {
        DataFusionError::External(format!("Failed to create part table: {e}").into())
    })?;

    // Test the exact problematic query
    let sql =
        "SELECT p_size FROM part WHERE p_size IN (14, 6, 5, 31) and p_partkey > 1000";

    let logical_plan = ctx.sql(sql).await?.into_unoptimized_plan();
    let optimized_plan = ctx.state().optimize(&logical_plan)?;
    let physical_plan = ctx.state().create_physical_plan(&optimized_plan).await?;

    // Serialize the physical plan - bug may happen here already but not necessarily manifests
    let codec = DefaultPhysicalExtensionCodec {};

    let proto = PhysicalPlanNode::try_from_physical_plan(physical_plan.clone(), &codec)?;

    // This will fail with the bug, but should succeed when fixed
    let _deserialized_plan = proto.try_into_physical_plan(&ctx.task_ctx(), &codec)?;
    Ok(())
}
