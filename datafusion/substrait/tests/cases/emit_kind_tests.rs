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

//! Tests for Emit Kind usage

#[cfg(test)]
mod tests {
    use crate::utils::test::{add_plan_schemas_to_ctx, read_json};

    use datafusion::common::Result;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use insta::assert_snapshot;

    #[tokio::test]
    async fn project_respects_direct_emit_kind() -> Result<()> {
        let proto_plan = read_json(
            "tests/testdata/test_plans/emit_kind/direct_on_project.substrait.json",
        );
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        @r#"
            Projection: DATA.A AS a, DATA.B AS b, DATA.A + Int64(1) AS add1
              TableScan: DATA
            "#
                );
        Ok(())
    }

    #[tokio::test]
    async fn handle_emit_as_project() -> Result<()> {
        let proto_plan = read_json(
            "tests/testdata/test_plans/emit_kind/emit_on_filter.substrait.json",
        );
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        // Note that duplicate references in the remap are aliased
        @r#"
            Projection: DATA.B, DATA.A AS A1, DATA.A AS DATA.A__temp__0 AS A2
              Filter: DATA.B = Int64(2)
                TableScan: DATA
            "#
                );
        Ok(())
    }

    async fn make_context() -> Result<SessionContext> {
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::default())
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_csv("data", "tests/testdata/data.csv", CsvReadOptions::default())
            .await?;
        Ok(ctx)
    }

    #[tokio::test]
    async fn handle_emit_as_project_with_volatile_expr() -> Result<()> {
        let ctx = make_context().await?;

        let df = ctx
            .sql("SELECT random() AS c1, a + 1 AS c2 FROM data")
            .await?;

        let plan = df.into_unoptimized_plan();
        assert_snapshot!(
            plan,
            @r#"
            Projection: random() AS c1, data.a + Int64(1) AS c2
              TableScan: data
            "#        );

        let proto = to_substrait_plan(&plan, &ctx.state())?;
        let plan2 = from_substrait_plan(&ctx.state(), &proto).await?;
        // note how the Projections are not flattened
        assert_snapshot!(
        plan2,
        @r#"
            Projection: random() AS c1, data.a + Int64(1) AS c2
              Projection: data.a, data.b, data.c, data.d, data.e, data.f, random(), data.a + Int64(1)
                TableScan: data
            "#
                );
        Ok(())
    }

    #[tokio::test]
    async fn handle_emit_as_project_without_volatile_exprs() -> Result<()> {
        let ctx = make_context().await?;
        let df = ctx.sql("SELECT a + 1, b + 2 FROM data").await?;

        let plan = df.into_unoptimized_plan();
        assert_snapshot!(
        plan,
        @r#"
            Projection: data.a + Int64(1), data.b + Int64(2)
              TableScan: data
            "#
                );

        let proto = to_substrait_plan(&plan, &ctx.state())?;
        let plan2 = from_substrait_plan(&ctx.state(), &proto).await?;

        let plan1str = format!("{plan}");
        let plan2str = format!("{plan2}");
        println!("{}", plan1str);
        println!("{}", plan2str);
        assert_eq!(plan1str, plan2str);

        Ok(())
    }
}
