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

//! Tests for Function Compatibility

#[cfg(test)]
mod tests {
    use crate::utils::test::{add_plan_schemas_to_ctx, read_json};

    use datafusion::common::Result;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use insta::assert_snapshot;

    #[tokio::test]
    async fn contains_function_test() -> Result<()> {
        let proto_plan = read_json("tests/testdata/contains_plan.substrait.json");
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
        plan,
        @r#"
            Projection: nation.n_name
              Filter: contains(nation.n_name, Utf8("IA"))
                TableScan: nation
            "#
                );
        Ok(())
    }
}
