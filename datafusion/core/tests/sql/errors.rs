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
async fn csv_query_error() -> Result<()> {
    // sin(utf8) should error
    let ctx = create_ctx();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT sin(c1) FROM aggregate_test_100";
    ctx.sql(sql).await.unwrap_err();
    Ok(())
}

#[tokio::test]
async fn test_cast_expressions_error() -> Result<()> {
    // sin(utf8) should error
    let ctx = create_ctx();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT CAST(c1 AS INT) FROM aggregate_test_100";
    let dataframe = ctx.sql(sql).await.unwrap();
    let result = dataframe.collect().await;

    match result {
        Ok(_) => panic!("expected error"),
        Err(e) => {
            assert_contains!(
                e.to_string(),
                "Cannot cast string 'c' to value of Int32 type"
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_aggregation_with_bad_arguments() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT COUNT(DISTINCT) FROM aggregate_test_100";
    let err = ctx.sql(sql).await.unwrap_err().to_string();
    assert_eq!(
        err,
        DataFusionError::Plan(
            "The function Count expects 1 arguments, but 0 were provided".to_string()
        )
        .to_string()
    );
    Ok(())
}

#[tokio::test]
async fn query_cte_incorrect() -> Result<()> {
    let ctx = SessionContext::new();

    // self reference
    let sql = "WITH t AS (SELECT * FROM t) SELECT * from u";
    let err = ctx.sql(sql).await.unwrap_err().to_string();
    assert_eq!(
        err,
        "Error during planning: table 'datafusion.public.t' not found"
    );

    // forward referencing
    let sql = "WITH t AS (SELECT * FROM u), u AS (SELECT 1) SELECT * from u";
    let err = ctx.sql(sql).await.unwrap_err().to_string();
    assert_eq!(
        err,
        "Error during planning: table 'datafusion.public.u' not found"
    );

    // wrapping should hide u
    let sql = "WITH t AS (WITH u as (SELECT 1) SELECT 1) SELECT * from u";
    let err = ctx.sql(sql).await.unwrap_err().to_string();
    assert_eq!(
        err,
        "Error during planning: table 'datafusion.public.u' not found"
    );

    Ok(())
}

#[tokio::test]
async fn test_select_wildcard_without_table() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT * ";
    let actual = ctx.sql(sql).await;
    match actual {
        Ok(_) => panic!("expect err"),
        Err(e) => {
            assert_contains!(
                e.to_string(),
                "Error during planning: SELECT * with no tables specified is not valid"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn invalid_qualified_table_references() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;

    for table_ref in &[
        "nonexistentschema.aggregate_test_100",
        "nonexistentcatalog.public.aggregate_test_100",
        "way.too.many.namespaces.as.ident.prefixes.aggregate_test_100",
    ] {
        let sql = format!("SELECT COUNT(*) FROM {table_ref}");
        let result = ctx.sql(&sql).await;
        assert!(
            matches!(result, Err(DataFusionError::Plan(_))),
            "result was: {result:?}"
        );
    }
    Ok(())
}

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
        "Internal error: Unsupported logical plan: CreateView. \
        This was likely caused by a bug in DataFusion's code and we would welcome that you file an bug report in our issue tracker"
    );
    // // drop view
    let sql = "drop view test_view";
    let plan = state.create_logical_plan(sql).await;
    let physical_plan = state.create_physical_plan(&plan.unwrap()).await;
    assert!(physical_plan.is_err());
    assert_eq!(
        format!("{}", physical_plan.unwrap_err()),
        "Internal error: Unsupported logical plan: DropView. \
        This was likely caused by a bug in DataFusion's code and we would welcome that you file an bug report in our issue tracker"
    );
    // // drop table
    let sql = "drop table aggregate_test_100";
    let plan = state.create_logical_plan(sql).await;
    let physical_plan = state.create_physical_plan(&plan.unwrap()).await;
    assert!(physical_plan.is_err());
    assert_eq!(
        format!("{}", physical_plan.unwrap_err()),
        "Internal error: Unsupported logical plan: DropTable. \
        This was likely caused by a bug in DataFusion's code and we would welcome that you file an bug report in our issue tracker"
    );
    Ok(())
}
