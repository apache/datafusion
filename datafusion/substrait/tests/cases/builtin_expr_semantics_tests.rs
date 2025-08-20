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

//! There are some Substrait functions that are semantically equivalent to nested built-in expressions, such as xor:bool_bool and and_not:bool_bool
//! This module tests that the semantics of these functions are correct roundtripped

#[cfg(test)]
mod tests {
    use crate::utils::test::add_plan_schemas_to_ctx;
    use datafusion::arrow::util::pretty;
    use datafusion::common::Result;
    use datafusion::prelude::DataFrame;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use std::fs::File;
    use std::io::BufReader;
    use substrait::proto::Plan;

    // Helper function to test scalar function semantics and roundtrip conversion
    async fn test_scalar_fn_semantics(
        file_path: &str,
        expected_results: Vec<&str>,
    ) -> Result<()> {
        let path = format!("tests/testdata/test_plans/{file_path}");
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto)?;
        let plan = from_substrait_plan(&ctx.state(), &proto).await?;

        // Test correct semantics of function
        let df = DataFrame::new(ctx.state().clone(), plan.clone());
        let results = df.collect().await?;
        let pretty_results = pretty::pretty_format_batches(&results)?.to_string();
        assert_eq!(
            pretty_results.trim().lines().collect::<Vec<_>>(),
            expected_results
        );

        // Test roundtrip semantics
        let proto = to_substrait_plan(&plan, &ctx.state())?;
        let plan2 = from_substrait_plan(&ctx.state(), &proto).await?;
        let df2 = DataFrame::new(ctx.state().clone(), plan2.clone());
        let results2 = df2.collect().await?;
        let pretty_results2 = pretty::pretty_format_batches(&results2)?.to_string();
        assert_eq!(
            pretty_results2.trim().lines().collect::<Vec<_>>(),
            expected_results
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_xor_semantics() -> Result<()> {
        let expected = vec![
            "+-------+-------+--------+",
            "| a     | b     | result |",
            "+-------+-------+--------+",
            "| true  | true  | false  |",
            "| true  | false | true   |",
            "| false | true  | true   |",
            "| false | false | false  |",
            "+-------+-------+--------+",
        ];

        test_scalar_fn_semantics(
            "scalar_fn_to_built_in_binary_expr_xor.substrait.json",
            expected,
        )
        .await
    }

    #[tokio::test]
    async fn test_and_not_semantics() -> Result<()> {
        let expected = vec![
            "+-------+-------+--------+",
            "| a     | b     | result |",
            "+-------+-------+--------+",
            "| true  | true  | false  |",
            "| true  | false | true   |",
            "| false | true  | false  |",
            "| false | false | false  |",
            "+-------+-------+--------+",
        ];

        test_scalar_fn_semantics(
            "scalar_fn_to_built_in_binary_expr_and_not.substrait.json",
            expected,
        )
        .await
    }

    #[tokio::test]
    async fn test_logb_semantics() -> Result<()> {
        let expected = vec![
            "+-------+------+--------+",
            "| x     | base | result |",
            "+-------+------+--------+",
            "| 1.0   | 10.0 | 0.0    |",
            "| 100.0 | 10.0 | 2.0    |",
            "+-------+------+--------+",
        ];

        test_scalar_fn_semantics("scalar_fn_logb_expr.substrait.json", expected).await
    }
}
