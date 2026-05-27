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

use super::*;

#[tokio::test]
async fn multi_reference_cte_materialization_heuristic() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE cte_scan_source AS VALUES (1), (2)")
        .await?
        .collect()
        .await?;

    let reused_scan = ctx
        .sql(
            "WITH t AS (SELECT column1 AS a FROM cte_scan_source) \
             SELECT count(*) FROM t l JOIN t r ON l.a = r.a",
        )
        .await?;
    let physical_plan = reused_scan.create_physical_plan().await?;
    let plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert_contains!(&plan, "MaterializedCteExec");
    assert_contains!(&plan, "MaterializedCteReaderExec");

    let cheap_literal = ctx
        .sql(
            "WITH t AS (SELECT 1 AS a) \
             SELECT count(*) FROM t l JOIN t r ON l.a = r.a",
        )
        .await?;
    let physical_plan = cheap_literal.create_physical_plan().await?;
    let plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert_not_contains!(&plan, "MaterializedCteExec");
    assert_not_contains!(&plan, "MaterializedCteReaderExec");

    let limited_reuse = ctx
        .sql(
            "WITH t AS (SELECT column1 AS a FROM cte_scan_source) \
             SELECT * FROM t l JOIN t r ON l.a = r.a LIMIT 1",
        )
        .await?;
    let physical_plan = limited_reuse.create_physical_plan().await?;
    let plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert_not_contains!(&plan, "MaterializedCteExec");
    assert_not_contains!(&plan, "MaterializedCteReaderExec");

    Ok(())
}
