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

//! Tests for reading substrait plans produced by other systems

#[cfg(test)]
mod tests {
    use crate::utils::test::{add_plan_schemas_to_ctx, read_json};
    use datafusion::common::Result;
    use datafusion::dataframe::DataFrame;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use insta::assert_snapshot;

    #[tokio::test]
    async fn scalar_function_compound_signature() -> Result<()> {
        // DataFusion currently produces Substrait that refers to functions only by their name.
        // However, the Substrait spec requires that functions be identified by their compound signature.
        // This test confirms that DataFusion is able to consume plans following the spec, even though
        // we don't yet produce such plans.
        // Once we start producing plans with compound signatures, this test can be replaced by the roundtrip tests.

        // File generated with substrait-java's Isthmus:
        // ./isthmus-cli/build/graal/isthmus --create "create table data (d boolean)" "select not d from data"
        let proto_plan =
            read_json("tests/testdata/test_plans/select_not_bool.substrait.json");
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        @r#"
            Projection: NOT DATA.D AS EXPR$0
              TableScan: DATA
            "#
                );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    // Aggregate function compound signature is tested through TPCH plans

    #[tokio::test]
    async fn window_function_compound_signature() -> Result<()> {
        // DataFusion currently produces Substrait that refers to functions only by their name.
        // However, the Substrait spec requires that functions be identified by their compound signature.
        // This test confirms that DataFusion is able to consume plans following the spec, even though
        // we don't yet produce such plans.
        // Once we start producing plans with compound signatures, this test can be replaced by the roundtrip tests.

        // File generated with substrait-java's Isthmus:
        // ./isthmus-cli/build/graal/isthmus --create "create table data (d int, part int, ord int)" "select sum(d) OVER (PARTITION BY part ORDER BY ord ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS lead_expr from data"
        let proto_plan =
            read_json("tests/testdata/test_plans/select_window.substrait.json");
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        @r#"
            Projection: sum(DATA.D) PARTITION BY [DATA.PART] ORDER BY [DATA.ORD ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING AS LEAD_EXPR
              WindowAggr: windowExpr=[[sum(DATA.D) PARTITION BY [DATA.PART] ORDER BY [DATA.ORD ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING]]
                TableScan: DATA
            "#
                );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    #[tokio::test]
    async fn double_window_function() -> Result<()> {
        // Confirms a WindowExpr can be repeated in the same project.
        // This wouldn't normally happen with DF-created plans since CSE would eliminate the duplicate.

        // File generated with substrait-java's Isthmus:
        // ./isthmus-cli/build/graal/isthmus --create "create table data (a int)" "select ROW_NUMBER() OVER (), ROW_NUMBER() OVER () AS aliased from data";
        let proto_plan =
            read_json("tests/testdata/test_plans/double_window.substrait.json");
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        @r#"
            Projection: row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS EXPR$0, row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW__temp__0 AS ALIASED
              WindowAggr: windowExpr=[[row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
                TableScan: DATA
            "#
                );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    #[tokio::test]
    async fn double_window_function_distinct_windows() -> Result<()> {
        // Confirms a single project can have multiple window functions with separate windows in it.
        // This wouldn't normally happen with DF-created plans since logical optimizer would
        // separate them out.

        // File generated with substrait-java's Isthmus:
        // ./isthmus-cli/build/graal/isthmus --create "create table data (a int)" "select ROW_NUMBER() OVER (), ROW_NUMBER() OVER (PARTITION BY a) from data";
        let proto_plan = read_json(
            "tests/testdata/test_plans/double_window_distinct_windows.substrait.json",
        );
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        @r#"
            Projection: row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS EXPR$0, row_number() PARTITION BY [DATA.A] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS EXPR$1
              WindowAggr: windowExpr=[[row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
                WindowAggr: windowExpr=[[row_number() PARTITION BY [DATA.A] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
                  TableScan: DATA
            "#
                );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_nullable_lists() -> Result<()> {
        // DataFusion's Substrait consumer treats all lists as nullable, even if the Substrait plan specifies them as non-nullable.
        // That's because implementing the non-nullability consistently is non-trivial.
        // This test confirms that reading a plan with non-nullable lists works as expected.
        let proto_plan =
            read_json("tests/testdata/test_plans/non_nullable_lists.substrait.json");
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
                &plan,
            @r#"
        Values: (List([1, 2]))
        "#
        );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    #[tokio::test]
    async fn multilayer_aggregate() -> Result<()> {
        let proto_plan =
            read_json("tests/testdata/test_plans/multilayer_aggregate.substrait.json");
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        @r#"
            Projection: lower(sales.product) AS lower(product), sum(count(sales.product)) AS product_count
              Aggregate: groupBy=[[sales.product]], aggr=[[sum(count(sales.product))]]
                Aggregate: groupBy=[[sales.product]], aggr=[[count(sales.product)]]
                  TableScan: sales
            "#
                );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }
}
