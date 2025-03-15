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

//! TPCH `roundtrip_substrait_tpch` tests
//!
//! This module tests that substrait queries in sql can be read and that the optimized
//! logiccal plans produced remain the same after a round trip to substrait and back.
//!
//!
//! The input sql queries are generated from <https://duckdb.org/docs/extensions/tpch.html>
//!
//!

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::common::{internal_datafusion_err, Result};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::prelude::DataFrame;
use datafusion::sql::TableReference;
use datafusion::{
    execution::runtime_env::RuntimeEnv,
    prelude::{SessionConfig, SessionContext},
};
use datafusion_proto::protobuf;
use std::fs::read_to_string;
use std::sync::Arc;

#[derive(Debug)]
struct FakeTableProvider {
    schema: Arc<Schema>,
}

#[async_trait]
impl TableProvider for FakeTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        println!("uh oh");
        unimplemented!("scan")
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&datafusion::prelude::Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(std::vec![
            TableProviderFilterPushDown::Inexact;
            filters.len()
        ])
    }
}

async fn create_context() -> Result<SessionContext> {
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::default())
        .with_runtime_env(Arc::new(RuntimeEnv::default()))
        .with_default_features()
        .build();

    let ctx = SessionContext::new_with_state(state);

    let tables = [
        "customer", "lineitem", "nation", "orders", "part", "partsupp", "region",
        "supplier",
    ];

    tables.into_iter().try_for_each(|table_name| {
        let schema_path = format!("tests/testdata/tpch_schemas/{table_name}_schema.json");
        let schema_json_data = read_to_string(schema_path)?;

        let proto: protobuf::Schema = serde_json::from_str(&schema_json_data)
            .map_err(|e| internal_datafusion_err!("Error parsing schema JSON: {}", e))?;

        let schema = Schema::try_from(&proto)?;

        let provider = FakeTableProvider {
            schema: Arc::new(schema),
        };
        ctx.register_table(TableReference::bare(table_name), Arc::new(provider))?;

        Ok::<_, DataFusionError>(())
    })?;

    Ok(ctx)
}

async fn get_dataframe(query_id: i32) -> Result<(DataFrame, SessionState)> {
    let path = format!("tests/testdata/tpch_queries/query_{query_id:02}.sql");
    println!("path = {}", path);
    let query = read_to_string(path)?;
    println!("Query: \n{}", query);

    let ctx = create_context().await?;

    Ok((ctx.sql(&query).await?, ctx.state()))
}

#[cfg(test)]
mod tests {
    use datafusion::common::Result;
    use datafusion_substrait::logical_plan::{
        consumer::from_substrait_plan, producer::to_substrait_plan,
    };

    use super::*;

    async fn tpch_round_trip_optimized(query_id: i32) -> Result<()> {
        let (df, state) = get_dataframe(query_id).await?;

        let oplan = df.clone().into_optimized_plan()?;
        println!("Optimized plan:\n{}", oplan.display_indent());

        let ssplan = to_substrait_plan(&oplan, &state)?;
        let roundtrip_plan = from_substrait_plan(&state, &ssplan).await?;
        let roundtrip_plan_optimized = state.optimize(&roundtrip_plan)?;
        println!(
            "Roundtrip optimized plan:\n{}",
            roundtrip_plan_optimized.display_indent()
        );

        assert_eq!(
            oplan.display_indent().to_string(),
            roundtrip_plan_optimized.display_indent().to_string()
        );
        Ok(())
    }

    async fn tpch_round_trip_unoptimized(query_id: i32) -> Result<()> {
        let (df, state) = get_dataframe(query_id).await?;

        let plan = df.logical_plan();
        println!("Logical plan:\n{}", plan.display_indent());

        let ssplan = to_substrait_plan(plan, &state)?;
        let roundtrip_plan = from_substrait_plan(&state, &ssplan).await?;
        println!("Roundtrip plan:\n{}", roundtrip_plan.display_indent());

        assert_eq!(
            plan.display_indent().to_string(),
            roundtrip_plan.display_indent().to_string()
        );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_round_trip_test_optimized_01() -> Result<()> {
        tpch_round_trip_optimized(1).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_02() -> Result<()> {
        tpch_round_trip_optimized(2).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_03() -> Result<()> {
        tpch_round_trip_optimized(3).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_04() -> Result<()> {
        tpch_round_trip_optimized(4).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_05() -> Result<()> {
        tpch_round_trip_optimized(5).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_06() -> Result<()> {
        tpch_round_trip_optimized(6).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_07() -> Result<()> {
        tpch_round_trip_optimized(7).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_08() -> Result<()> {
        tpch_round_trip_optimized(8).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_09() -> Result<()> {
        tpch_round_trip_optimized(9).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_10() -> Result<()> {
        tpch_round_trip_optimized(10).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_11() -> Result<()> {
        tpch_round_trip_optimized(11).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_12() -> Result<()> {
        tpch_round_trip_optimized(12).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_13() -> Result<()> {
        tpch_round_trip_optimized(13).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_14() -> Result<()> {
        tpch_round_trip_optimized(14).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_15() -> Result<()> {
        tpch_round_trip_optimized(15).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_16() -> Result<()> {
        tpch_round_trip_optimized(16).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_17() -> Result<()> {
        tpch_round_trip_optimized(17).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_18() -> Result<()> {
        tpch_round_trip_optimized(18).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_19() -> Result<()> {
        tpch_round_trip_optimized(19).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_20() -> Result<()> {
        tpch_round_trip_optimized(20).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_21() -> Result<()> {
        tpch_round_trip_optimized(21).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_optimized_22() -> Result<()> {
        tpch_round_trip_optimized(22).await
    }

    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_01() -> Result<()> {
        tpch_round_trip_unoptimized(1).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_02() -> Result<()> {
        tpch_round_trip_unoptimized(2).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_03() -> Result<()> {
        tpch_round_trip_unoptimized(3).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_04() -> Result<()> {
        tpch_round_trip_unoptimized(4).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_05() -> Result<()> {
        tpch_round_trip_unoptimized(5).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_06() -> Result<()> {
        tpch_round_trip_unoptimized(6).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_07() -> Result<()> {
        tpch_round_trip_unoptimized(7).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_08() -> Result<()> {
        tpch_round_trip_unoptimized(8).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_09() -> Result<()> {
        tpch_round_trip_unoptimized(9).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_10() -> Result<()> {
        tpch_round_trip_unoptimized(10).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_11() -> Result<()> {
        tpch_round_trip_unoptimized(11).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_12() -> Result<()> {
        tpch_round_trip_unoptimized(12).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_13() -> Result<()> {
        tpch_round_trip_unoptimized(13).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_14() -> Result<()> {
        tpch_round_trip_unoptimized(14).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_15() -> Result<()> {
        tpch_round_trip_unoptimized(15).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_16() -> Result<()> {
        tpch_round_trip_unoptimized(16).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_17() -> Result<()> {
        tpch_round_trip_unoptimized(17).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_18() -> Result<()> {
        tpch_round_trip_unoptimized(18).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_19() -> Result<()> {
        tpch_round_trip_unoptimized(19).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_20() -> Result<()> {
        tpch_round_trip_unoptimized(20).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_21() -> Result<()> {
        tpch_round_trip_unoptimized(21).await
    }
    #[tokio::test]
    async fn tpch_round_trip_test_unoptimized_22() -> Result<()> {
        tpch_round_trip_unoptimized(22).await
    }
}
