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

//! TPCH `substrait_consumer` tests
//!
//! This module tests that substrait plans as json encoded protobuf can be
//! correctly read as DataFusion plans.
//!
//! The input data comes from  <https://github.com/substrait-io/consumer-testing/tree/main/substrait_consumer/tests/integration/queries/tpch_substrait_plans>

#[cfg(test)]
mod tests {
    use crate::utils::test::add_plan_schemas_to_ctx;
    use datafusion::common::Result;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use insta::assert_snapshot;
    use std::fs::File;
    use std::io::BufReader;
    use substrait::proto::Plan;

    async fn tpch_plan_to_string(query_id: i32) -> Result<String> {
        let path =
            format!("tests/testdata/tpch_substrait_plans/query_{query_id:02}_plan.json");
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto)?;
        let plan = from_substrait_plan(&ctx.state(), &proto).await?;
        ctx.state().create_physical_plan(&plan).await?;
        Ok(format!("{}", plan))
    }

    #[tokio::test]
    async fn tpch_test_01() -> Result<()> {
        let plan_str = tpch_plan_to_string(1).await?;
        assert_snapshot!("tpch_test_01", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_02() -> Result<()> {
        let plan_str = tpch_plan_to_string(2).await?;
        assert_snapshot!("tpch_test_02", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_03() -> Result<()> {
        let plan_str = tpch_plan_to_string(3).await?;
        assert_snapshot!("tpch_test_03", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_04() -> Result<()> {
        let plan_str = tpch_plan_to_string(4).await?;
        assert_snapshot!("tpch_test_04", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_05() -> Result<()> {
        let plan_str = tpch_plan_to_string(5).await?;
        assert_snapshot!("tpch_test_05", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_06() -> Result<()> {
        let plan_str = tpch_plan_to_string(6).await?;
        assert_snapshot!("tpch_test_06", plan_str);
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn tpch_test_07() -> Result<()> {
        let plan_str = tpch_plan_to_string(7).await?;
        assert_snapshot!("tpch_test_07", plan_str);
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn tpch_test_08() -> Result<()> {
        let plan_str = tpch_plan_to_string(8).await?;
        assert_snapshot!("tpch_test_08", plan_str);
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn tpch_test_09() -> Result<()> {
        let plan_str = tpch_plan_to_string(9).await?;
        assert_snapshot!("tpch_test_09", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_10() -> Result<()> {
        let plan_str = tpch_plan_to_string(10).await?;
        assert_snapshot!("tpch_test_10", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_11() -> Result<()> {
        let plan_str = tpch_plan_to_string(11).await?;
        assert_snapshot!("tpch_test_11", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_12() -> Result<()> {
        let plan_str = tpch_plan_to_string(12).await?;
        assert_snapshot!("tpch_test_12", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_13() -> Result<()> {
        let plan_str = tpch_plan_to_string(13).await?;
        assert_snapshot!("tpch_test_13", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_14() -> Result<()> {
        let plan_str = tpch_plan_to_string(14).await?;
        assert_snapshot!("tpch_test_14", plan_str);
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn tpch_test_15() -> Result<()> {
        let plan_str = tpch_plan_to_string(15).await?;
        assert_snapshot!("tpch_test_15", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_16() -> Result<()> {
        let plan_str = tpch_plan_to_string(16).await?;
        assert_snapshot!("tpch_test_16", plan_str);
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn tpch_test_17() -> Result<()> {
        let plan_str = tpch_plan_to_string(17).await?;
        assert_snapshot!("tpch_test_17", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_18() -> Result<()> {
        let plan_str = tpch_plan_to_string(18).await?;
        assert_snapshot!("tpch_test_18", plan_str);
        Ok(())
    }
    #[tokio::test]
    async fn tpch_test_19() -> Result<()> {
        let plan_str = tpch_plan_to_string(19).await?;
        assert_snapshot!("tpch_test_19", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_20() -> Result<()> {
        let plan_str = tpch_plan_to_string(20).await?;
        assert_snapshot!("tpch_test_20", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_21() -> Result<()> {
        let plan_str = tpch_plan_to_string(21).await?;
        assert_snapshot!("tpch_test_21", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_22() -> Result<()> {
        let plan_str = tpch_plan_to_string(22).await?;
        assert_snapshot!("tpch_test_22", plan_str);
        Ok(())
    }

    async fn test_plan_to_string(name: &str) -> Result<String> {
        let path = format!("tests/testdata/test_plans/{name}");
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto)?;
        let plan = from_substrait_plan(&ctx.state(), &proto).await?;
        ctx.state().create_physical_plan(&plan).await?;
        Ok(format!("{}", plan))
    }

    #[tokio::test]
    async fn test_select_count_from_select_1() -> Result<()> {
        let plan_str =
            test_plan_to_string("select_count_from_select_1.substrait.json").await?;

        assert_snapshot!("test_select_count_from_select_1", plan_str);
        Ok(())
    }

    #[tokio::test]
    async fn test_select_window_count() -> Result<()> {
        let plan_str = test_plan_to_string("select_window_count.substrait.json").await?;

        assert_snapshot!("test_select_window_count", plan_str);
        Ok(())
    }
}
