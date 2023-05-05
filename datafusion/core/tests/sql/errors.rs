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
async fn unsupported_sql_returns_error() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let state = ctx.state();

    // create view
    let sql = "create view test_view as select * from aggregate_test_100";
    let plan = state.create_logical_plan(sql).await;
    let physical_plan = state.create_physical_plan(&plan.unwrap()).await;
    assert!(physical_plan.is_err());
    assert_eq!(
        format!("{}", physical_plan.unwrap_err()),
        "This feature is not implemented: Unsupported logical plan: CreateView"
    );
    // // drop view
    let sql = "drop view test_view";
    let plan = state.create_logical_plan(sql).await;
    let physical_plan = state.create_physical_plan(&plan.unwrap()).await;
    assert!(physical_plan.is_err());
    assert_eq!(
        format!("{}", physical_plan.unwrap_err()),
        "This feature is not implemented: Unsupported logical plan: DropView"
    );
    // // drop table
    let sql = "drop table aggregate_test_100";
    let plan = state.create_logical_plan(sql).await;
    let physical_plan = state.create_physical_plan(&plan.unwrap()).await;
    assert!(physical_plan.is_err());
    assert_eq!(
        format!("{}", physical_plan.unwrap_err()),
        "This feature is not implemented: Unsupported logical plan: DropTable"
    );
    Ok(())
}
