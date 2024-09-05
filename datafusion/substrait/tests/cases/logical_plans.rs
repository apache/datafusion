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
    use datafusion::common::Result;
    use datafusion::dataframe::DataFrame;
    use datafusion::prelude::{CsvReadOptions, SessionContext};
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use std::fs::File;
    use std::io::BufReader;
    use substrait::proto::Plan;

    #[tokio::test]
    async fn scalar_function_compound_signature() -> Result<()> {
        // DataFusion currently produces Substrait that refers to functions only by their name.
        // However, the Substrait spec requires that functions be identified by their compound signature.
        // This test confirms that DataFusion is able to consume plans following the spec, even though
        // we don't yet produce such plans.
        // Once we start producing plans with compound signatures, this test can be replaced by the roundtrip tests.

        let ctx = create_context().await?;

        // File generated with substrait-java's Isthmus:
        // ./isthmus-cli/build/graal/isthmus "select not d from data" -c "create table data (d boolean)"
        let proto = read_json("tests/testdata/test_plans/select_not_bool.substrait.json");

        let plan = from_substrait_plan(&ctx, &proto).await?;

        assert_eq!(
            format!("{}", plan),
            "Projection: NOT DATA.a AS EXPR$0\
            \n  TableScan: DATA projection=[a, b, c, d, e, f]"
        );
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

        let ctx = create_context().await?;

        // File generated with substrait-java's Isthmus:
        // ./isthmus-cli/build/graal/isthmus "select sum(d) OVER (PARTITION BY part ORDER BY ord ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS lead_expr from data" -c "create table data (d int, part int, ord int)"
        let proto = read_json("tests/testdata/test_plans/select_window.substrait.json");

        let plan = from_substrait_plan(&ctx, &proto).await?;

        assert_eq!(
            format!("{}", plan),
            "Projection: sum(DATA.a) PARTITION BY [DATA.b] ORDER BY [DATA.c ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING AS LEAD_EXPR\
            \n  WindowAggr: windowExpr=[[sum(DATA.a) PARTITION BY [DATA.b] ORDER BY [DATA.c ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING]]\
            \n    TableScan: DATA projection=[a, b, c, d, e, f]"
        );
        Ok(())
    }

    #[tokio::test]
    async fn non_nullable_lists() -> Result<()> {
        // DataFusion's Substrait consumer treats all lists as nullable, even if the Substrait plan specifies them as non-nullable.
        // That's because implementing the non-nullability consistently is non-trivial.
        // This test confirms that reading a plan with non-nullable lists works as expected.
        let ctx = create_context().await?;
        let proto =
            read_json("tests/testdata/test_plans/non_nullable_lists.substrait.json");

        let plan = from_substrait_plan(&ctx, &proto).await?;

        assert_eq!(format!("{}", &plan), "Values: (List([1, 2]))");

        // Need to trigger execution to ensure that Arrow has validated the plan
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    fn read_json(path: &str) -> Plan {
        serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json")
    }

    async fn create_context() -> datafusion::common::Result<SessionContext> {
        let ctx = SessionContext::new();
        ctx.register_csv("DATA", "tests/testdata/data.csv", CsvReadOptions::new())
            .await?;
        Ok(ctx)
    }
}
