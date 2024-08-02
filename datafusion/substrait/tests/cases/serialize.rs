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

#[cfg(test)]
mod tests {
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use datafusion_substrait::logical_plan::producer;
    use datafusion_substrait::serializer;

    use datafusion::error::Result;
    use datafusion::prelude::*;

    use std::fs;

    #[tokio::test]
    async fn serialize_simple_select() -> Result<()> {
        let ctx = create_context().await?;
        let path = "tests/simple_select.bin";
        let sql = "SELECT a, b FROM data";
        // Test reference
        let df_ref = ctx.sql(sql).await?;
        let plan_ref = df_ref.into_optimized_plan()?;
        // Test
        // Write substrait plan to file
        serializer::serialize(sql, &ctx, path).await?;
        // Read substrait plan from file
        let proto = serializer::deserialize(path).await?;
        // Check plan equality
        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str_ref = format!("{plan_ref}");
        let plan_str = format!("{plan}");
        assert_eq!(plan_str_ref, plan_str);
        // Delete test binary file
        fs::remove_file(path)?;

        Ok(())
    }

    #[tokio::test]
    async fn table_scan_without_projection() -> Result<()> {
        let ctx = create_context().await?;
        let table = provider_as_source(ctx.table_provider("data").await?);
        let table_scan = LogicalPlanBuilder::scan("data", table, None)?.build()?;
        let convert_result = producer::to_substrait_plan(&table_scan, &ctx);
        assert!(convert_result.is_ok());

        Ok(())
    }

    async fn create_context() -> Result<SessionContext> {
        let ctx = SessionContext::new();
        ctx.register_csv("data", "tests/testdata/data.csv", CsvReadOptions::new())
            .await?;
        ctx.register_csv("data2", "tests/testdata/data.csv", CsvReadOptions::new())
            .await?;
        Ok(ctx)
    }
}
