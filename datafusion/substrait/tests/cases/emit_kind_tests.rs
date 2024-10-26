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
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;

    #[tokio::test]
    async fn project_respects_direct_emit_kind() -> Result<()> {
        let proto_plan = read_json(
            "tests/testdata/test_plans/emit_kind/direct_on_project.substrait.json",
        );
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx, &proto_plan).await?;

        let plan_str = format!("{}", plan);

        assert_eq!(
            plan_str,
            "Projection: DATA.A AS a, DATA.B AS b, DATA.A + Int64(1) AS add1\
            \n  TableScan: DATA"
        );
        Ok(())
    }

    #[tokio::test]
    async fn handle_emit_as_project() -> Result<()> {
        let proto_plan = read_json(
            "tests/testdata/test_plans/emit_kind/emit_on_filter.substrait.json",
        );
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx, &proto_plan).await?;

        let plan_str = format!("{}", plan);

        assert_eq!(
            plan_str,
            // Note that duplicate references in the remap are aliased
            "Projection: DATA.B, DATA.A AS A1, DATA.A AS DATA.A__temp__0 AS A2\
             \n  Filter: DATA.B = Int64(2)\
             \n    TableScan: DATA"
        );
        Ok(())
    }
}
