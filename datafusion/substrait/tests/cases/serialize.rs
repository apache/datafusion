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
    use datafusion::common::assert_contains;
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use datafusion_substrait::serializer;

    use datafusion::error::Result;
    use datafusion::prelude::*;

    use insta::assert_snapshot;
    use std::fs;
    use substrait::proto::plan_rel::RelType;
    use substrait::proto::rel_common::{Emit, EmitKind};
    use substrait::proto::{rel, RelCommon};

    #[tokio::test]
    async fn serialize_to_file() -> Result<()> {
        let ctx = create_context().await?;
        let path = "tests/serialize_to_file.bin";
        let sql = "SELECT a, b FROM data";

        // Test case 1: serializing to a non-existing file should succeed.
        serializer::serialize(sql, &ctx, path).await?;
        serializer::deserialize(path).await?;

        // Test case 2: serializing to an existing file should fail.
        let got = serializer::serialize(sql, &ctx, path).await.unwrap_err();
        assert_contains!(got.to_string(), "File exists");

        fs::remove_file(path)?;

        Ok(())
    }

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
        let plan = from_substrait_plan(&ctx.state(), &proto).await?;
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
        let convert_result = to_substrait_plan(&table_scan, &ctx.state());
        assert!(convert_result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn include_remaps_for_projects() -> Result<()> {
        let ctx = create_context().await?;
        let df = ctx.sql("SELECT b, a + a, a FROM data").await?;
        let datafusion_plan = df.into_optimized_plan()?;

        assert_snapshot!(
                    format!("{}", datafusion_plan),
                    @r#"
Projection: data.b, data.a + data.a, data.a
  TableScan: data projection=[a, b]
"#
        ,
                );

        let plan = to_substrait_plan(&datafusion_plan, &ctx.state())?
            .as_ref()
            .clone();

        let relation = plan.relations.first().unwrap().rel_type.as_ref();
        let root_rel = match relation {
            Some(RelType::Root(root)) => root.input.as_ref().unwrap(),
            _ => panic!("expected Root"),
        };
        if let Some(rel::RelType::Project(p)) = root_rel.rel_type.as_ref() {
            // The input has 2 columns [a, b], the Projection has 3 expressions [b, a + a, a]
            // The required output mapping is [2,3,4], which skips the 2 input columns.
            assert_emit(p.common.as_ref(), vec![2, 3, 4]);

            if let Some(rel::RelType::Read(r)) =
                p.input.as_ref().unwrap().rel_type.as_ref()
            {
                let mask_expression = r.projection.as_ref().unwrap();
                let select = mask_expression.select.as_ref().unwrap();
                assert_eq!(
                    2,
                    select.struct_items.len(),
                    "Read outputs two columns: a, b"
                );
                return Ok(());
            }
        }
        panic!("plan did not match expected structure")
    }

    #[tokio::test]
    async fn include_remaps_for_windows() -> Result<()> {
        let ctx = create_context().await?;
        // let df = ctx.sql("SELECT a, b, lead(b) OVER (PARTITION BY a) FROM data").await?;
        let df = ctx
            .sql("SELECT b, RANK() OVER (PARTITION BY a), c FROM data;")
            .await?;
        let datafusion_plan = df.into_optimized_plan()?;
        assert_snapshot!(
                    datafusion_plan,
                    @r#"
Projection: data.b, rank() PARTITION BY [data.a] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, data.c
  WindowAggr: windowExpr=[[rank() PARTITION BY [data.a] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
    TableScan: data projection=[a, b, c]
"#
        ,
                );

        let plan = to_substrait_plan(&datafusion_plan, &ctx.state())?
            .as_ref()
            .clone();

        let relation = plan.relations.first().unwrap().rel_type.as_ref();
        let root_rel = match relation {
            Some(RelType::Root(root)) => root.input.as_ref().unwrap(),
            _ => panic!("expected Root"),
        };

        if let Some(rel::RelType::Project(p1)) = root_rel.rel_type.as_ref() {
            // The WindowAggr outputs 4 columns, the Projection has 4 columns
            assert_emit(p1.common.as_ref(), vec![4, 5, 6]);

            if let Some(rel::RelType::Project(p2)) =
                p1.input.as_ref().unwrap().rel_type.as_ref()
            {
                // The input has 3 columns, the WindowAggr has 4 expression
                assert_emit(p2.common.as_ref(), vec![3, 4, 5, 6]);

                if let Some(rel::RelType::Read(r)) =
                    p2.input.as_ref().unwrap().rel_type.as_ref()
                {
                    let mask_expression = r.projection.as_ref().unwrap();
                    let select = mask_expression.select.as_ref().unwrap();
                    assert_eq!(
                        3,
                        select.struct_items.len(),
                        "Read outputs three columns: a, b, c"
                    );
                    return Ok(());
                }
            }
        }
        panic!("plan did not match expected structure")
    }

    fn assert_emit(rel_common: Option<&RelCommon>, output_mapping: Vec<i32>) {
        assert_eq!(
            rel_common.unwrap().emit_kind.clone(),
            Some(EmitKind::Emit(Emit { output_mapping }))
        );
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
